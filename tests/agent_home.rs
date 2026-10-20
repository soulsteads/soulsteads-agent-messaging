use async_trait::async_trait;
use serde_json::{Value, json};
use soulsteads_messaging::{
    config,
    runtime::{Bridge, Event, Sink},
    store::Store,
};
use std::{collections::BTreeMap, path::Path, process::Command, sync::Arc};
use tokio::sync::Mutex;

#[derive(Clone, Default)]
struct Recorder(Arc<Mutex<Vec<Value>>>);
#[async_trait]
impl Sink for Recorder {
    async fn emit(&self, value: Value) -> anyhow::Result<()> {
        self.0.lock().await.push(value);
        Ok(())
    }
}

fn configuration(root: &Path, home: Value) -> Value {
    config::load(
        &json!({
            "rateLimitSeconds": 0,
            "sessionStorePath": root.join("store.json"),
            "acp": {
                "command": env!("CARGO_BIN_EXE_acp-fixture"),
                "args": [],
                "agentHome": home,
                "env": {"ACP_FIXTURE_PROFILE_EXPECT": json!({"AGENTS.md":"home instructions"}).to_string()}
            }
        })
        .to_string(),
        &BTreeMap::new(),
    )
    .unwrap()
}

fn home(root: &Path, name: &str) -> std::path::PathBuf {
    let home = root.join(name);
    std::fs::create_dir(&home).unwrap();
    std::fs::write(home.join("AGENTS.md"), "home instructions").unwrap();
    home
}

fn rpc(path: &Path) -> Vec<Value> {
    std::fs::read_to_string(path)
        .unwrap_or_default()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect()
}

#[tokio::test]
async fn configured_agent_home_is_canonical_cwd_for_every_backend_and_override_wins() {
    for backend in ["opencode", "codex", "claude", "pi"] {
        let root = tempfile::tempdir().unwrap();
        let inherited = home(root.path(), "inherited");
        let override_home = home(root.path(), "override");
        let audit = root.path().join("audit.jsonl");
        let mut settings = configuration(root.path(), inherited.to_string_lossy().into());
        settings["acp"]["env"]["ACP_FIXTURE_RPC_AUDIT"] = audit.to_string_lossy().into();
        settings["acp"]["defaultBackend"] = backend.into();
        settings["acp"]["backends"][backend] = json!({
            "command": env!("CARGO_BIN_EXE_acp-fixture"), "args": [],
            "agentHome": override_home.to_string_lossy(),
            "env": {"ACP_FIXTURE_PROFILE_EXPECT": json!({"AGENTS.md":"home instructions"}).to_string(), "ACP_FIXTURE_RPC_AUDIT": audit.to_string_lossy()}
        });
        let bridge = Bridge::new(settings, BTreeMap::new(), root.path().join("sessions")).unwrap();
        bridge
            .handle(Event::web("home", "hello"), &Recorder::default())
            .await
            .unwrap();
        let entry = Store::new(root.path().join("store.json"))
            .get("web", "home", backend)
            .unwrap()
            .unwrap();
        assert_eq!(
            entry.cwd,
            std::fs::canonicalize(&override_home)
                .unwrap()
                .to_string_lossy()
        );
        assert!(rpc(&audit).iter().any(|row| row["method"] == "session/new"));
        assert!(
            !root.path().join("sessions").exists(),
            "configured homes are never copied into session dirs"
        );
        bridge.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn invalid_agent_home_fails_closed_before_fixture_launch() {
    for home in [
        json!("relative"),
        json!(42),
        json!("/definitely/missing-agent-home"),
    ] {
        let root = tempfile::tempdir().unwrap();
        let mut settings = configuration(root.path(), home);
        let audit = root.path().join("audit.jsonl");
        settings["acp"]["env"]["ACP_FIXTURE_RPC_AUDIT"] = audit.to_string_lossy().into();
        assert!(Bridge::new(settings, BTreeMap::new(), root.path().join("sessions")).is_err());
        assert!(!audit.exists());
    }
}

#[tokio::test]
async fn changed_agent_home_refuses_resume_before_launch_and_clear_only_forgets_binding() {
    let root = tempfile::tempdir().unwrap();
    let first = home(root.path(), "first");
    let second = home(root.path(), "second");
    let audit = root.path().join("audit.jsonl");
    let mut settings = configuration(root.path(), first.to_string_lossy().into());
    settings["acp"]["env"]["ACP_FIXTURE_RPC_AUDIT"] = audit.to_string_lossy().into();
    let bridge = Bridge::new(
        settings.clone(),
        BTreeMap::new(),
        root.path().join("sessions"),
    )
    .unwrap();
    bridge
        .handle(Event::web("bound", "hello"), &Recorder::default())
        .await
        .unwrap();
    bridge.shutdown().await.unwrap();
    let before = std::fs::read(root.path().join("store.json")).unwrap();
    let calls = rpc(&audit).len();
    settings["acp"]["agentHome"] = second.to_string_lossy().into();
    let bridge = Bridge::new(settings, BTreeMap::new(), root.path().join("sessions")).unwrap();
    assert!(
        bridge
            .handle(Event::web("bound", "hello"), &Recorder::default())
            .await
            .is_err()
    );
    assert_eq!(
        rpc(&audit).len(),
        calls,
        "mismatch must fail before backend launch"
    );
    assert_eq!(
        std::fs::read(root.path().join("store.json")).unwrap(),
        before
    );
    bridge
        .handle(Event::web("bound", "/clear"), &Recorder::default())
        .await
        .unwrap();
    assert!(
        Store::new(root.path().join("store.json"))
            .get("web", "bound", "opencode")
            .unwrap()
            .is_none()
    );
    assert_eq!(
        rpc(&audit).len(),
        calls,
        "clear must not open or delete foreign sessions"
    );
    bridge.shutdown().await.unwrap();
}

#[tokio::test]
async fn empty_backend_home_disables_inheritance_and_profile_conflicts_fail_at_construction() {
    let root = tempfile::tempdir().unwrap();
    let inherited = home(root.path(), "inherited");
    let mut settings = configuration(root.path(), inherited.to_string_lossy().into());
    settings["acp"]["defaultBackend"] = "codex".into();
    settings["acp"]["backends"]["codex"] =
        json!({"command": env!("CARGO_BIN_EXE_acp-fixture"), "args": [], "agentHome": ""});
    let bridge = Bridge::new(settings, BTreeMap::new(), root.path().join("sessions")).unwrap();
    bridge
        .handle(Event::web("empty", "hello"), &Recorder::default())
        .await
        .unwrap();
    let entry = Store::new(root.path().join("store.json"))
        .get("web", "empty", "codex")
        .unwrap()
        .unwrap();
    assert_ne!(entry.cwd, inherited.to_string_lossy());
    bridge.shutdown().await.unwrap();

    let mut conflict = configuration(root.path(), inherited.to_string_lossy().into());
    conflict["acp"]["backends"]["opencode"] = json!({"profileDir": "/legacy-copy"});
    assert!(
        Bridge::new(
            conflict,
            BTreeMap::new(),
            root.path().join("other-sessions")
        )
        .is_err()
    );
}

#[tokio::test]
async fn disabling_home_with_explicit_backend_id_refuses_old_binding_before_launch() {
    let root = tempfile::tempdir().unwrap();
    let home = home(root.path(), "home");
    let audit = root.path().join("audit.jsonl");
    let mut settings = configuration(root.path(), home.to_string_lossy().into());
    settings["acp"]["backendId"] = "explicit".into();
    settings["acp"]["env"]["ACP_FIXTURE_RPC_AUDIT"] = audit.to_string_lossy().into();
    let bridge = Bridge::new(
        settings.clone(),
        BTreeMap::new(),
        root.path().join("sessions"),
    )
    .unwrap();
    bridge
        .handle(Event::web("identity", "hello"), &Recorder::default())
        .await
        .unwrap();
    bridge.shutdown().await.unwrap();
    let calls = rpc(&audit).len();
    settings["acp"]["agentHome"] = "".into();
    let bridge = Bridge::new(settings, BTreeMap::new(), root.path().join("sessions")).unwrap();
    assert!(
        bridge
            .handle(Event::web("identity", "hello"), &Recorder::default())
            .await
            .is_err()
    );
    assert_eq!(rpc(&audit).len(), calls);
    bridge.shutdown().await.unwrap();
}

#[test]
fn empty_opencode_home_allows_legacy_profile_without_inheriting_global_home() {
    let root = tempfile::tempdir().unwrap();
    let inherited = home(root.path(), "inherited");
    let mut settings = configuration(root.path(), inherited.to_string_lossy().into());
    settings["acp"]["backends"]["opencode"] = json!({
        "command": env!("CARGO_BIN_EXE_acp-fixture"),
        "args": [],
        "agentHome": "",
        "profileDir": root.path().join("legacy-profile")
    });
    assert!(Bridge::new(settings, BTreeMap::new(), root.path().join("sessions")).is_ok());
}

#[tokio::test]
async fn tilde_home_resumes_same_binding_after_restart() {
    let root = tempfile::tempdir().unwrap();
    let agent_home = home(root.path(), "agent");
    let audit = root.path().join("audit.jsonl");
    let mut settings = configuration(root.path(), json!("~/agent"));
    settings["acp"]["env"]["ACP_FIXTURE_RPC_AUDIT"] = audit.to_string_lossy().into();
    let environment = BTreeMap::from([("HOME".into(), root.path().to_string_lossy().into())]);
    for _ in 0..2 {
        let bridge = Bridge::new(
            settings.clone(),
            environment.clone(),
            root.path().join("sessions"),
        )
        .unwrap();
        assert_eq!(
            bridge.working_directory(&Event::web("same", "hello")),
            agent_home
        );
        bridge
            .handle(Event::web("same", "hello"), &Recorder::default())
            .await
            .unwrap();
        bridge.shutdown().await.unwrap();
    }
    let calls = rpc(&audit);
    assert_eq!(
        calls
            .iter()
            .filter(|row| row["method"] == "session/new")
            .count(),
        1
    );
    assert!(calls.iter().any(|row| matches!(
        row["method"].as_str(),
        Some("session/load" | "session/resume")
    )));
}

#[tokio::test]
async fn unconfigured_home_preserves_legacy_command_identity_and_picked_workspace_resume() {
    use sha2::{Digest, Sha256};
    let root = tempfile::tempdir().unwrap();
    let picked = home(root.path(), "picked");
    let mut settings = configuration(root.path(), json!(""));
    settings["acp"]["env"] = json!({});
    let bridge = Bridge::new(
        settings.clone(),
        BTreeMap::new(),
        root.path().join("sessions"),
    )
    .unwrap();
    bridge
        .handle(Event::web("legacy", "hello"), &Recorder::default())
        .await
        .unwrap();
    bridge.shutdown().await.unwrap();
    let store = Store::new(root.path().join("store.json"));
    let mut entry = store.get("web", "legacy", "opencode").unwrap().unwrap();
    let legacy_command = vec![env!("CARGO_BIN_EXE_acp-fixture")];
    assert_eq!(
        entry.backend_id,
        format!(
            "command-sha256:{:x}",
            Sha256::digest(serde_json::to_vec(&legacy_command).unwrap())
        )
    );
    entry.cwd = picked.to_string_lossy().into();
    store.set(entry.clone(), true).unwrap();
    let bridge = Bridge::new(settings, BTreeMap::new(), root.path().join("sessions")).unwrap();
    bridge
        .handle(Event::web("legacy", "hello"), &Recorder::default())
        .await
        .unwrap();
    assert_eq!(
        store.get("web", "legacy", "opencode").unwrap().unwrap().cwd,
        entry.cwd
    );
    bridge.shutdown().await.unwrap();
}

#[tokio::test]
async fn configured_home_removed_after_startup_is_not_recreated_or_launched() {
    let root = tempfile::tempdir().unwrap();
    let agent_home = home(root.path(), "agent");
    let audit = root.path().join("audit.jsonl");
    let mut settings = configuration(root.path(), agent_home.to_string_lossy().into());
    settings["acp"]["env"]["ACP_FIXTURE_RPC_AUDIT"] = audit.to_string_lossy().into();
    let bridge = Bridge::new(settings, BTreeMap::new(), root.path().join("sessions")).unwrap();
    std::fs::rename(&agent_home, root.path().join("moved")).unwrap();
    assert!(
        bridge
            .handle(Event::web("missing", "hello"), &Recorder::default())
            .await
            .is_err()
    );
    assert!(!agent_home.exists());
    assert!(!audit.exists());
    bridge.shutdown().await.unwrap();
}

#[test]
fn check_config_rejects_invalid_agent_home_without_spawning_backend() {
    let root = tempfile::tempdir().unwrap();
    let audit = root.path().join("audit.jsonl");
    std::fs::write(
        root.path().join("soulsteads-messaging.json"),
        json!({
            "acp": {"command": env!("CARGO_BIN_EXE_acp-fixture"), "agentHome": "relative", "env": {"ACP_FIXTURE_RPC_AUDIT": audit}}
        })
        .to_string(),
    )
    .unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_soulsteads-messaging"))
        .arg("--check-config")
        .current_dir(root.path())
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(!audit.exists());
}
