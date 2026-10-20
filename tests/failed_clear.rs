#[cfg(feature = "reference-conformance")]
#[path = "support/reference_source.rs"]
mod reference_source;
use async_trait::async_trait;
use serde_json::{Value, json};
use soulsteads_messaging::{
    config,
    runtime::{Bridge, Event, Sink},
    store::{Entry, Store},
};
use std::{collections::BTreeMap, path::Path};
use tokio::sync::Mutex;

#[derive(Default)]
struct Recorder(Mutex<Vec<Value>>);
#[async_trait]
impl Sink for Recorder {
    async fn emit(&self, event: Value) -> anyhow::Result<()> {
        self.0.lock().await.push(event);
        Ok(())
    }
}
fn configuration(root: &Path) -> Value {
    config::load(&json!({"rateLimitSeconds":0, "sessionStorePath":root.join("store.json"),
        "acp":{"command":"/usr/bin/python3","args":[concat!(env!("CARGO_MANIFEST_DIR"),"/tests/fixtures/failed-clear-peer.py")],
        "env":{"CLEAR_AUDIT":root.join("audit.jsonl")}}}).to_string(), &BTreeMap::new()).unwrap()
}
fn requests(root: &Path) -> Vec<Value> {
    std::fs::read_to_string(root.join("audit.jsonl"))
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect()
}
#[tokio::test]
async fn failed_backend_delete_still_clears_local_mapping_and_next_prompt_is_fresh() {
    let root = tempfile::tempdir().unwrap();
    #[cfg(feature = "reference-conformance")]
    {
        let source_root = root.path().join("source");
        std::fs::create_dir(&source_root).unwrap();
        std::fs::write(
            source_root.join("soulsteads-messaging.json"),
            configuration(&source_root).to_string(),
        )
        .unwrap();
        let spec = json!({"steps":[{"id":"clear","text":"before"},{"id":"clear","text":"/clear"},{"id":"clear","text":"after"}]});
        std::fs::write(source_root.join("specification.json"), spec.to_string()).unwrap();
        let source = reference_source::root();
        let output = std::process::Command::new("bun")
            .arg("--no-env-file")
            .arg(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/tests/fixtures/source-commands.ts"
            ))
            .arg(source)
            .arg(source_root.join("specification.json"))
            .current_dir(&source_root)
            .env_clear()
            .env("PATH", std::env::var("PATH").unwrap())
            .env("HOME", &source_root)
            .env("SESSION_BASE_DIR", source_root.join("sessions"))
            .env("TMPDIR", root.path())
            .env("BUN_RUNTIME_TRANSPILER_CACHE_PATH", "0")
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "Source failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let text = String::from_utf8(output.stdout).unwrap();
        let cases: Vec<Value> = serde_json::from_str(text.lines().last().unwrap()).unwrap();
        assert_eq!(
            cases[1]["output"],
            json!(["Session cleared. Next message will start a fresh session."])
        );
        assert!(cases[1]["stats"].is_null());
        let audit = requests(&source_root);
        assert_eq!(
            audit
                .iter()
                .filter(|r| r["method"] == "session/delete")
                .count(),
            1
        );
        assert_eq!(
            audit
                .iter()
                .filter(|r| r["method"] == "session/new")
                .count(),
            2
        );
        assert!(
            !audit
                .iter()
                .any(|r| r["method"] == "session/load" || r["method"] == "session/resume")
        );
        println!("Actual Source cleared after rejected session/delete and opened fresh: verified");
    }
    let rust_root = root.path().join("rust");
    std::fs::create_dir(&rust_root).unwrap();
    let bridge = Bridge::new(
        configuration(&rust_root),
        BTreeMap::new(),
        rust_root.join("sessions"),
    )
    .unwrap();
    let sink = Recorder::default();
    let store = Store::new(rust_root.join("store.json"));
    bridge
        .handle_web(Event::web("clear", "before"), &sink)
        .await
        .unwrap();
    store
        .set(
            Entry {
                connector: "web".into(),
                thread_id: "clear".into(),
                session_id: "independent".into(),
                cwd: rust_root.to_string_lossy().into(),
                backend_id: "cx".into(),
                updated_at: chrono::Utc::now().to_rfc3339(),
                backend: "codex".into(),
            },
            false,
        )
        .unwrap();
    let result = bridge
        .handle_web(Event::web("clear", "/clear"), &sink)
        .await;
    assert!(
        result.is_ok(),
        "clear must continue local cleanup after backend deletion error: {result:?}"
    );
    assert!(store.get("web", "clear", "opencode").unwrap().is_none());
    assert_eq!(
        store
            .get("web", "clear", "codex")
            .unwrap()
            .unwrap()
            .session_id,
        "independent"
    );
    assert!(
        sink.0
            .lock()
            .await
            .iter()
            .any(|r| r["text"] == "Session cleared. Next message will start a fresh session.")
    );
    bridge
        .handle_web(Event::web("clear", "after"), &sink)
        .await
        .unwrap();
    let audit = requests(&rust_root);
    assert_eq!(
        audit
            .iter()
            .filter(|r| r["method"] == "session/new")
            .count(),
        2
    );
    assert!(
        !audit
            .iter()
            .any(|r| r["method"] == "session/load" || r["method"] == "session/resume")
    );
    bridge.shutdown().await.unwrap();
}
