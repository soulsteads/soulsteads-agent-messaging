use async_trait::async_trait;
use soulsteads_messaging::{
    config,
    runtime::{Bridge, Event, Sink},
    store::Store,
};
use serde_json::{Value, json};
use std::{collections::BTreeMap, time::Duration};

struct Recorder;
#[async_trait]
impl Sink for Recorder {
    async fn emit(&self, _: Value) -> anyhow::Result<()> {
        Ok(())
    }
}

#[tokio::test]
async fn expiry_commits_exact_mapping_removal_and_preserves_workspace() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join(".build-tmp");
    let directory = tempfile::Builder::new()
        .prefix("expiry-runtime-")
        .tempdir_in(root)
        .unwrap();
    let configuration = config::load(
        &json!({"rateLimitSeconds":0,"sessionStorePath":directory.path().join("store.json"),"acp":{"command":env!("CARGO_BIN_EXE_acp-fixture"),"args":[]}}).to_string(),
        &BTreeMap::new(),
    ).unwrap();
    let bridge = Bridge::new(
        configuration,
        BTreeMap::new(),
        directory.path().join("sessions"),
    )
    .unwrap();
    let event = Event::web("expiry", "hello");
    bridge.handle(event.clone(), &Recorder).await.unwrap();
    let store = Store::new(directory.path().join("store.json"));
    let original = store.get("web", "expiry", "opencode").unwrap().unwrap();
    let artifact = std::path::Path::new(&original.cwd).join("user-artifact.txt");
    std::fs::write(&artifact, "preserve me").unwrap();
    let mut independent = original.clone();
    independent.backend = "codex".into();
    independent.session_id = "independent-history".into();
    store.set(independent.clone(), false).unwrap();
    let mut other_connector = independent.clone();
    other_connector.connector = "matrix".into();
    store.set(other_connector.clone(), false).unwrap();
    store.set_selected("web", "expiry", "opencode").unwrap();
    assert_eq!(bridge.expire(Duration::from_secs(3600)).await.unwrap(), 0);
    assert_eq!(
        store.get("web", "expiry", "opencode").unwrap(),
        Some(original)
    );
    assert_eq!(bridge.expire(Duration::from_nanos(1)).await.unwrap(), 1);
    assert!(
        store.get("web", "expiry", "opencode").unwrap().is_none(),
        "expiry must delete persisted mapping before evicting client"
    );
    assert_eq!(
        store.get("web", "expiry", "codex").unwrap(),
        Some(independent)
    );
    assert_eq!(
        store.get("matrix", "expiry", "codex").unwrap(),
        Some(other_connector)
    );
    assert_eq!(
        store.selected("web", "expiry", "codex").unwrap(),
        "opencode"
    );
    assert_eq!(std::fs::read_to_string(&artifact).unwrap(), "preserve me");
    assert_eq!(bridge.expire(Duration::from_nanos(1)).await.unwrap(), 0);
    bridge.handle(event, &Recorder).await.unwrap();
    assert!(store.get("web", "expiry", "opencode").unwrap().is_some());
    bridge.shutdown().await.unwrap();
}

#[tokio::test]
async fn selected_backend_expiry_and_reopen_preserve_independent_histories() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join(".build-tmp");
    let directory = tempfile::Builder::new()
        .prefix("expiry-backend-")
        .tempdir_in(root)
        .unwrap();
    let configuration = config::load(
        &json!({"rateLimitSeconds":0,"sessionStorePath":directory.path().join("store.json"),"acp":{"command":env!("CARGO_BIN_EXE_acp-fixture"),"args":[],"env":{"ACP_FIXTURE_MATRIX_AUDIT":directory.path().join("audit.jsonl")},"backends":{"codex":{"command":env!("CARGO_BIN_EXE_acp-fixture"),"args":[],"env":{"ACP_FIXTURE_MATRIX_AUDIT":directory.path().join("audit.jsonl")}}}}}).to_string(),
        &BTreeMap::new(),
    ).unwrap();
    let workspace = directory.path().join("sessions");
    let bridge = Bridge::new(configuration.clone(), BTreeMap::new(), workspace.clone()).unwrap();
    let store = Store::new(directory.path().join("store.json"));
    bridge
        .handle(Event::web("backend", "hello"), &Recorder)
        .await
        .unwrap();
    let opencode = store.get("web", "backend", "opencode").unwrap().unwrap();
    bridge
        .handle(Event::web("backend", "/backend codex"), &Recorder)
        .await
        .unwrap();
    bridge
        .handle(Event::web("backend", "hello"), &Recorder)
        .await
        .unwrap();
    let codex = store.get("web", "backend", "codex").unwrap().unwrap();
    let history = std::path::Path::new(&codex.cwd).join("backend-owned-history.json");
    std::fs::write(&history, "independent backend artifact").unwrap();
    assert_eq!(bridge.expire(Duration::from_nanos(1)).await.unwrap(), 1);
    assert!(store.get("web", "backend", "codex").unwrap().is_none());
    assert_eq!(
        store.get("web", "backend", "opencode").unwrap(),
        Some(opencode.clone())
    );
    assert_eq!(
        store.selected("web", "backend", "opencode").unwrap(),
        "codex"
    );
    bridge.shutdown().await.unwrap();
    let restarted = Bridge::new(configuration, BTreeMap::new(), workspace).unwrap();
    assert_eq!(restarted.selected("web", "backend").unwrap(), "codex");
    assert_eq!(restarted.expire(Duration::from_nanos(1)).await.unwrap(), 0);
    let audit_before = std::fs::read_to_string(directory.path().join("audit.jsonl"))
        .unwrap()
        .len();
    restarted
        .handle(Event::web("backend", "hello"), &Recorder)
        .await
        .unwrap();
    assert!(store.get("web", "backend", "codex").unwrap().is_some());
    assert_eq!(
        store.get("web", "backend", "opencode").unwrap(),
        Some(opencode)
    );
    let audit = std::fs::read_to_string(directory.path().join("audit.jsonl")).unwrap();
    let methods = audit[audit_before..]
        .lines()
        .map(|line| {
            serde_json::from_str::<Value>(line).unwrap()["method"]
                .as_str()
                .unwrap()
                .to_owned()
        })
        .collect::<Vec<_>>();
    assert!(methods.iter().any(|method| method == "session/new"));
    assert!(!methods.iter().any(|method| matches!(
        method.as_str(),
        "session/load" | "session/resume" | "session/delete"
    )));
    assert!(
        !audit
            .lines()
            .any(|line| serde_json::from_str::<Value>(line).unwrap()["method"] == "session/delete")
    );
    assert_eq!(
        std::fs::read_to_string(history).unwrap(),
        "independent backend artifact"
    );
    restarted.shutdown().await.unwrap();
}
