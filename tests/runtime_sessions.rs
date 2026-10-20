use async_trait::async_trait;
use soulsteads_messaging::{
    config,
    runtime::{Bridge, Event, Sink},
    store::Store,
};
use serde_json::{Value, json};
use std::{collections::BTreeMap, path::Path, sync::Arc};
use tokio::sync::Mutex;

#[derive(Default, Clone)]
struct Recorder(Arc<Mutex<Vec<Value>>>);
#[async_trait]
impl Sink for Recorder {
    fn owned(&self) -> Option<Arc<dyn Sink>> {
        Some(Arc::new(self.clone()))
    }
    async fn emit(&self, event: Value) -> anyhow::Result<()> {
        self.0.lock().await.push(event);
        Ok(())
    }
}

#[tokio::test]
async fn mirror_is_read_only_and_stops_on_user_activity() {
    let directory = tempfile::tempdir().unwrap();
    let project = directory.path().join("project");
    std::fs::create_dir(&project).unwrap();
    let bridge = Bridge::new(
        configuration(directory.path(), &project),
        BTreeMap::new(),
        directory.path().join("sessions"),
    )
    .unwrap();
    let sink = Recorder::default();
    for command in ["/p", "/p 1", "/m 1"] {
        bridge
            .handle(Event::web("mirror", command), &sink)
            .await
            .unwrap();
    }
    assert!(
        sink.0.lock().await.iter().any(|event| event["text"]
            .as_str()
            .is_some_and(|text| text.contains("Mirror mode on:")
                && text.contains("Current tail:")
                && text.contains("User: history")))
    );
    assert!(
        Store::new(directory.path().join("store.json"))
            .get("web", "mirror", "opencode")
            .unwrap()
            .is_none()
    );
    bridge
        .handle(Event::web("mirror", "/status"), &sink)
        .await
        .unwrap();
    assert!(
        sink.0
            .lock()
            .await
            .iter()
            .any(|event| event["text"] == "Mirror mode stopped.")
    );
    bridge.shutdown().await.unwrap();
}

#[tokio::test]
async fn mirror_poll_detects_changes_outside_truncated_display_without_publishing_attachment() {
    let directory = tempfile::tempdir().unwrap();
    let project = directory.path().join("project");
    std::fs::create_dir(&project).unwrap();
    let history_path = project.join("history.txt");
    std::fs::write(&history_path, "a".repeat(1000)).unwrap();
    let mut configuration = configuration(directory.path(), &project);
    configuration["sessionPicker"]["mirrorIntervalSeconds"] = 1.into();
    configuration["acp"]["env"]["ACP_FIXTURE_HISTORY_FILE"] = history_path.to_string_lossy().into();
    let bridge = Bridge::new(
        configuration,
        BTreeMap::new(),
        directory.path().join("sessions"),
    )
    .unwrap();
    let sink = Recorder::default();
    for command in ["/p", "/p 1", "/m 1"] {
        bridge
            .handle(Event::web("mirror", command), &sink)
            .await
            .unwrap();
    }
    assert_eq!(bridge.poll_mirrors().await.unwrap(), 0);
    std::fs::write(&history_path, format!("{}b", "a".repeat(999))).unwrap();
    tokio::time::sleep(std::time::Duration::from_secs(11)).await;
    assert_eq!(bridge.poll_mirrors().await.unwrap(), 1);
    assert!(sink.0.lock().await.iter().any(|event| {
        event["text"]
            .as_str()
            .is_some_and(|text| text.starts_with("Mirror update:"))
    }));
    assert_eq!(bridge.poll_mirrors().await.unwrap(), 0);
    assert!(
        Store::new(directory.path().join("store.json"))
            .get("web", "mirror", "opencode")
            .unwrap()
            .is_none()
    );
    bridge.shutdown().await.unwrap();
}
fn configuration(root: &Path, project: &Path) -> Value {
    config::load(&json!({"rateLimitSeconds":0,"sessionStorePath":root.join("store.json"),"sessionPicker":{"enabled":true},"acp":{"command":env!("CARGO_BIN_EXE_acp-fixture"),"args":[],"env":{"ACP_FIXTURE_PROJECT_DIR":project}}}).to_string(), &BTreeMap::new()).unwrap()
}
#[tokio::test]
async fn picker_listing_is_read_only_attachment_is_durable_and_reload_uses_project_cwd() {
    let directory = tempfile::tempdir().unwrap();
    let project = directory.path().join("project");
    std::fs::create_dir(&project).unwrap();
    let configuration = configuration(directory.path(), &project);
    let bridge = Bridge::new(
        configuration.clone(),
        BTreeMap::new(),
        directory.path().join("sessions"),
    )
    .unwrap();
    let sink = Recorder::default();
    bridge
        .handle(Event::web("picker", "/p"), &sink)
        .await
        .unwrap();
    let store = Store::new(directory.path().join("store.json"));
    assert!(
        store.get("web", "picker", "opencode").unwrap().is_none(),
        "listing must not create or publish an ACP session"
    );
    bridge
        .handle(Event::web("picker", "/p 1"), &sink)
        .await
        .unwrap();
    bridge
        .handle(Event::web("picker", "/s 1"), &sink)
        .await
        .unwrap();
    let entry = store.get("web", "picker", "opencode").unwrap().unwrap();
    assert_eq!(entry.cwd, project.to_string_lossy());
    assert_eq!(entry.session_id, "fixture");
    assert!(
        sink.0.lock().await.iter().any(|event| event["text"]
            .as_str()
            .is_some_and(|text| text.contains("Attached to session: Fixture")
                && text.contains("User: history")))
    );
    bridge.shutdown().await.unwrap();
    let restarted = Bridge::new(
        configuration,
        BTreeMap::new(),
        directory.path().join("sessions"),
    )
    .unwrap();
    restarted
        .handle(Event::web("picker", "/r"), &sink)
        .await
        .unwrap();
    assert!(sink.0.lock().await.iter().any(|event| {
        event["text"]
            .as_str()
            .is_some_and(|text| text.starts_with("Reloaded session:"))
    }));
    restarted.shutdown().await.unwrap();
}
