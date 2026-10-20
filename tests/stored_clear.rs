#[cfg(feature = "reference-conformance")]
#[path = "support/reference_source.rs"]
mod reference_source;
use async_trait::async_trait;
use serde_json::{Value, json};
use soulsteads_messaging::{
    config,
    runtime::{Bridge, Event, Sink},
    store::Store,
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
    config::load(&json!({"rateLimitSeconds":0,"sessionStorePath":root.join("store.json"),
 "acp":{"command":"/usr/bin/python3","args":[concat!(env!("CARGO_MANIFEST_DIR"),"/tests/fixtures/failed-clear-peer.py")],"env":{"CLEAR_AUDIT":root.join("audit.jsonl")}}}).to_string(),&BTreeMap::new()).unwrap()
}
fn assert_cleanup_audit(requests: &[Value]) {
    assert_eq!(
        requests
            .iter()
            .filter(|r| r["method"] == "session/new")
            .count(),
        2
    );
    assert_eq!(
        requests
            .iter()
            .filter(|r| r["method"] == "session/load" || r["method"] == "session/resume")
            .count(),
        1,
        "stored clear must resume its matching session before backend deletion"
    );
    assert_eq!(
        requests
            .iter()
            .filter(|r| r["method"] == "session/delete")
            .count(),
        1,
        "stored clear must attempt backend deletion"
    );
}
#[tokio::test]
async fn stored_only_clear_resumes_matching_binding_then_forgets_it_despite_delete_error() {
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
        let source = reference_source::root();
        let output = std::process::Command::new("bun")
            .arg("--no-env-file")
            .arg(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/tests/fixtures/source-stored-clear.ts"
            ))
            .arg(source)
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
        let stdout = String::from_utf8(output.stdout).unwrap();
        let observed: Value = serde_json::from_str(
            stdout
                .lines()
                .find(|line| line.starts_with("{\"clearEvents\""))
                .expect("Source result JSON missing"),
        )
        .unwrap();
        assert_cleanup_audit(observed["requests"].as_array().unwrap());
        assert!(
            observed["clearEvents"]
                .as_array()
                .unwrap()
                .iter()
                .any(|r| r["text"] == "Session cleared. Next message will start a fresh session.")
        );
        println!(
            "Actual Source stored-only clear resumes and attempts delete, then opens fresh: verified"
        );
    }
    let rust_root = root.path().join("rust");
    std::fs::create_dir(&rust_root).unwrap();
    let cfg = configuration(&rust_root);
    let workspace = rust_root.join("sessions");
    let sink = Recorder::default();
    let bridge = Bridge::new(cfg.clone(), BTreeMap::new(), workspace.clone()).unwrap();
    bridge
        .handle_web(Event::web("stored", "before"), &sink)
        .await
        .unwrap();
    bridge.shutdown().await.unwrap();
    let restarted = Bridge::new(cfg, BTreeMap::new(), workspace).unwrap();
    restarted
        .handle_web(Event::web("stored", "/clear"), &sink)
        .await
        .unwrap();
    assert!(
        Store::new(rust_root.join("store.json"))
            .get("web", "stored", "opencode")
            .unwrap()
            .is_none()
    );
    restarted
        .handle_web(Event::web("stored", "after"), &sink)
        .await
        .unwrap();
    let requests: Vec<Value> = std::fs::read_to_string(rust_root.join("audit.jsonl"))
        .unwrap()
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect();
    assert_cleanup_audit(&requests);
    restarted.shutdown().await.unwrap();
}

#[tokio::test]
async fn stored_clear_never_contacts_backend_for_changed_identity_or_foreign_workspace() {
    for mismatch in ["identity", "cwd"] {
        let root = tempfile::tempdir().unwrap();
        let cfg = configuration(root.path());
        let workspace = root.path().join("sessions");
        let sink = Recorder::default();
        let bridge = Bridge::new(cfg.clone(), BTreeMap::new(), workspace.clone()).unwrap();
        bridge
            .handle_web(Event::web("stored", "before"), &sink)
            .await
            .unwrap();
        bridge.shutdown().await.unwrap();
        let store = Store::new(root.path().join("store.json"));
        let mut entry = store.get("web", "stored", "opencode").unwrap().unwrap();
        if mismatch == "identity" {
            entry.backend_id = "changed-backend-identity".into();
        } else {
            let foreign = root.path().join("foreign");
            std::fs::create_dir(&foreign).unwrap();
            std::fs::write(foreign.join("sentinel"), "preserve").unwrap();
            entry.cwd = foreign.to_string_lossy().into();
        }
        store.set(entry.clone(), false).unwrap();
        let mut independent = entry;
        independent.backend = "codex".into();
        independent.session_id = "independent".into();
        store.set(independent, false).unwrap();
        let audit_path = root.path().join("audit.jsonl");
        let audit_before = std::fs::read(&audit_path).unwrap();
        let restarted = Bridge::new(cfg, BTreeMap::new(), workspace).unwrap();
        restarted
            .handle_web(Event::web("stored", "/reset"), &sink)
            .await
            .unwrap();
        assert_eq!(
            std::fs::read(&audit_path).unwrap(),
            audit_before,
            "mismatched identity/cwd must not launch or delete through backend"
        );
        assert!(store.get("web", "stored", "opencode").unwrap().is_none());
        assert_eq!(
            store
                .get("web", "stored", "codex")
                .unwrap()
                .unwrap()
                .session_id,
            "independent"
        );
        if mismatch == "cwd" {
            assert_eq!(
                std::fs::read_to_string(root.path().join("foreign/sentinel")).unwrap(),
                "preserve"
            );
        }
        restarted.shutdown().await.unwrap();
    }
}
