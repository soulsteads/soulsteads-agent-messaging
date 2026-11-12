//! Copilot uses its own ACP server, not a bundled native-protocol adapter.
#[path = "support/copilot_environment.rs"]
mod copilot_environment;
use async_trait::async_trait;
use serde_json::{Value, json};
use soulsteads_messaging::{
    config,
    runtime::{Bridge, Event, Sink},
    store::Store,
};
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    sync::Arc,
};
use tokio::sync::Mutex;

#[derive(Clone, Default)]
struct Recorder(Arc<Mutex<Vec<Value>>>);
#[async_trait]
impl Sink for Recorder {
    async fn emit(&self, event: Value) -> anyhow::Result<()> {
        self.0.lock().await.push(event);
        Ok(())
    }
}

#[cfg(unix)]
fn token_auditing_wrapper(root: &Path) -> PathBuf {
    use std::os::unix::fs::PermissionsExt;

    let wrapper = root.join("copilot-wrapper.sh");
    std::fs::write(
        &wrapper,
        "#!/bin/sh\nprintf '%s' \"$COPILOT_GITHUB_TOKEN\" > \"$COPILOT_TOKEN_AUDIT\"\nexec \"$ACP_FIXTURE_EXECUTABLE\" \"$@\"\n",
    )
    .unwrap();
    std::fs::set_permissions(&wrapper, std::fs::Permissions::from_mode(0o700)).unwrap();
    wrapper
}

#[cfg(unix)]
#[tokio::test]
async fn copilot_token_environment_expands_for_restrictive_child() {
    const TOKEN: &str = "synthetic-copilot-token";

    let root = tempfile::tempdir().unwrap();
    let native_home = root.path().join("copilot");
    std::fs::create_dir(&native_home).unwrap();
    let audit = root.path().join("copilot-token.txt");
    let mut child_environment = copilot_environment::environment(&native_home, true);
    child_environment["ACP_FIXTURE_EXECUTABLE"] = env!("CARGO_BIN_EXE_acp-fixture").into();
    child_environment["COPILOT_TOKEN_AUDIT"] = audit.to_string_lossy().into();
    let configuration = config::load(
        &json!({
            "rateLimitSeconds": 0,
            "sessionStorePath": root.path().join("store.json"),
            "acp": {
                "defaultBackend": "copilot",
                "agentHome": native_home,
                "backends": {"copilot": {
                    "command": token_auditing_wrapper(root.path()),
                    "args": [],
                    "env": child_environment
                }}
            }
        })
        .to_string(),
        &BTreeMap::from([("COPILOT_GITHUB_TOKEN".into(), TOKEN.into())]),
    )
    .unwrap();
    let bridge = Bridge::new(configuration, BTreeMap::new(), root.path().join("sessions")).unwrap();
    bridge
        .handle(Event::web("token", "hello"), &Recorder::default())
        .await
        .unwrap();
    let received = std::fs::read_to_string(audit).unwrap();
    assert!(!received.contains("${COPILOT_GITHUB_TOKEN}"));
    assert_eq!(received, TOKEN);
    bridge.shutdown().await.unwrap();
}

#[tokio::test]
async fn copilot_switch_prompt_restart_and_clear_preserve_other_backend() {
    let root = tempfile::tempdir().unwrap();
    let audit = root.path().join("audit.jsonl");
    let configuration = config::load(
        &json!({
            "rateLimitSeconds":0, "sessionStorePath":root.path().join("store.json"),
            "acp":{
                "command":env!("CARGO_BIN_EXE_acp-fixture"), "args":[],
                "backends":{"copilot":{
                    "command":env!("CARGO_BIN_EXE_acp-fixture"), "args":[],
                    "env":{"ACP_FIXTURE_RPC_AUDIT":audit}
                }}
            }
        })
        .to_string(),
        &BTreeMap::new(),
    )
    .unwrap();
    let workspace = root.path().join("sessions");
    let store = Store::new(root.path().join("store.json"));
    let sink = Recorder::default();
    let bridge = Bridge::new(configuration.clone(), BTreeMap::new(), workspace.clone()).unwrap();
    bridge
        .handle(Event::web("probe", "hello"), &sink)
        .await
        .unwrap();
    let original = store.get("web", "probe", "opencode").unwrap().unwrap();
    bridge
        .handle(Event::web("probe", "/backend copilot"), &sink)
        .await
        .unwrap();
    let selected = store.get("web", "probe", "copilot").unwrap().unwrap();
    assert_eq!(bridge.selected("web", "probe").unwrap(), "copilot");
    bridge
        .handle(Event::web("probe", "copilot prompt"), &sink)
        .await
        .unwrap();
    bridge.shutdown().await.unwrap();

    let restarted = Bridge::new(configuration, BTreeMap::new(), workspace).unwrap();
    restarted
        .handle(Event::web("probe", "after restart"), &sink)
        .await
        .unwrap();
    assert_eq!(
        store
            .get("web", "probe", "copilot")
            .unwrap()
            .unwrap()
            .session_id,
        selected.session_id
    );
    let calls: Vec<Value> = std::fs::read_to_string(audit)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert_eq!(
        calls
            .iter()
            .filter(|call| call["method"] == "session/new")
            .count(),
        1
    );
    assert!(
        calls
            .iter()
            .any(|call| call["method"] == "session/resume" || call["method"] == "session/load")
    );
    assert_eq!(
        calls
            .iter()
            .filter(|call| call["method"] == "session/prompt")
            .count(),
        2
    );
    restarted
        .handle(Event::web("probe", "/clear"), &sink)
        .await
        .unwrap();
    assert!(store.get("web", "probe", "copilot").unwrap().is_none());
    assert_eq!(
        store
            .get("web", "probe", "opencode")
            .unwrap()
            .unwrap()
            .session_id,
        original.session_id
    );
    assert_eq!(
        sink.0
            .lock()
            .await
            .iter()
            .filter(|event| event["type"] == "response" && event["text"] == "hello world")
            .count(),
        3
    );
    restarted.shutdown().await.unwrap();
}

#[test]
fn invalid_copilot_home_is_rejected_before_launch() {
    for home in [
        json!("relative"),
        json!(42),
        json!("/definitely/missing-copilot-home"),
    ] {
        let root = tempfile::tempdir().unwrap();
        let audit = root.path().join("audit.jsonl");
        let configuration = config::load(
            &json!({
                "acp":{"defaultBackend":"copilot","backends":{"copilot":{
                    "command":env!("CARGO_BIN_EXE_acp-fixture"),"args":[],
                    "agentHome":home,"env":{"ACP_FIXTURE_RPC_AUDIT":audit}
                }}}
            })
            .to_string(),
            &BTreeMap::new(),
        )
        .unwrap();
        assert!(Bridge::new(configuration, BTreeMap::new(), root.path().join("sessions")).is_err());
        assert!(!audit.exists());
    }
}

#[cfg(unix)]
#[test]
fn compiled_cli_launches_copilot_from_path_with_default_configuration() {
    use std::os::unix::fs::symlink;
    let root = tempfile::tempdir().unwrap();
    let bin = root.path().join("bin");
    std::fs::create_dir(&bin).unwrap();
    symlink(env!("CARGO_BIN_EXE_acp-fixture"), bin.join("copilot")).unwrap();
    let config_path = root.path().join("config.json");
    std::fs::write(
        &config_path,
        json!({
            "rateLimitSeconds":0,"sessionStorePath":root.path().join("store.json"),
            "acp":{"defaultBackend":"copilot"}
        })
        .to_string(),
    )
    .unwrap();
    let mut paths = vec![bin];
    paths.extend(std::env::split_paths(
        &std::env::var_os("PATH").unwrap_or_default(),
    ));
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_soulsteads-messaging"))
        .current_dir(root.path())
        .env("PATH", std::env::join_paths(paths).unwrap())
        .env("SESSION_BASE_DIR", root.path().join("sessions"))
        .args(["--no-images", "--config"])
        .arg(config_path)
        .arg("hello")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(String::from_utf8_lossy(&output.stdout).contains("hello world"));
    assert!(
        Store::new(root.path().join("store.json"))
            .get("web", "cli", "copilot")
            .unwrap()
            .is_some()
    );
}

#[test]
fn copilot_defaults_use_native_stdio_without_permission_bypass() {
    let defaults = config::defaults();
    assert_eq!(defaults["acp"]["defaultBackend"], "opencode");
    assert_eq!(defaults["acp"]["backends"]["copilot"]["command"], "copilot");
    assert_eq!(
        defaults["acp"]["backends"]["copilot"]["args"],
        serde_json::json!(["--acp", "--stdio"])
    );
}
