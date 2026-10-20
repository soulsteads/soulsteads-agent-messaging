use async_trait::async_trait;
use soulsteads_messaging::{
    config,
    runtime::{Bridge, Event, Sink},
    store::Store,
};
use serde_json::{Value, json};
use std::collections::BTreeMap;
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

#[tokio::test]
async fn two_empty_web_attempts_invalidate_the_failed_retry_session() {
    let root = tempfile::tempdir().unwrap();
    let script = root.path().join("script.json");
    std::fs::write(
        &script,
        json!([
            {"stopReason":"end_turn"},
            {"stopReason":"end_turn"}
        ])
        .to_string(),
    )
    .unwrap();
    let store_path = root.path().join("store.json");
    let config = config::load(
        &json!({
            "rateLimitSeconds":0,"sessionStorePath":store_path,
            "acp":{"command":env!("CARGO_BIN_EXE_acp-fixture"),"args":[],
                "env":{"ACP_FIXTURE_MATRIX_SCRIPT":script,
                    "ACP_FIXTURE_MATRIX_COUNTER":root.path().join("counter")}}
        })
        .to_string(),
        &BTreeMap::new(),
    )
    .unwrap();
    let bridge = Bridge::new(config, BTreeMap::new(), root.path().join("sessions")).unwrap();
    let sink = Recorder::default();
    let result = bridge
        .handle_web(Event::web("failed-browser", "query"), &sink)
        .await;
    let remaining = Store::new(&store_path)
        .get("web", "failed-browser", "opencode")
        .unwrap();
    bridge.shutdown().await.unwrap();
    result.unwrap();
    assert_eq!(
        std::fs::read_to_string(root.path().join("counter")).unwrap(),
        "2",
        "The failure path must remain bounded to exactly two attempts"
    );
    assert!(
        remaining.is_none(),
        "Source discards an empty failed retry session; Rust retained {remaining:?}"
    );
    let events = sink.0.lock().await;
    assert!(
        events
            .iter()
            .any(|event| event["type"] == "session_state" && event["state"] == "stale-invalidated"),
        "{events:?}"
    );
    assert!(events.iter().any(|event| event["type"] == "error" && event["message"] == "The AI service completed without returning a usable response. Please try again."), "{events:?}");
}
