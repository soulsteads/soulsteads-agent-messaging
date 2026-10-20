use async_trait::async_trait;
use soulsteads_messaging::{
    config,
    runtime::{Bridge, Event, Sink},
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
async fn web_empty_response_retries_once_with_a_fresh_client_and_session() {
    let root = tempfile::tempdir().unwrap();
    let cfg = config::load(&json!({"rateLimitSeconds":0,
        "sessionStorePath":root.path().join("store.json"),
        "acp":{"command":"/usr/bin/python3","args":[concat!(env!("CARGO_MANIFEST_DIR"),"/tests/fixtures/web-retry-peer.py")],
            "env":{"WEB_RETRY_AUDIT":root.path().join("rpc.jsonl"),"WEB_RETRY_COUNTER":root.path().join("counter")}}
    }).to_string(), &BTreeMap::new()).unwrap();
    let bridge = Bridge::new(cfg, BTreeMap::new(), root.path().join("sessions")).unwrap();
    let sink = Recorder::default();
    let result = bridge
        .handle_web(
            Event {
                platform: "web".into(),
                room: "browser-fixture".into(),
                thread: String::new(),
                id: "request-one".into(),
                user: "browser-fixture".into(),
                text: "query".into(),
                direct: true,
                mentioned: false,
                bot: false,
                images: vec![],
                reply_to: None,
            },
            &sink,
        )
        .await;
    bridge.shutdown().await.unwrap();
    result.unwrap();
    let audit: Vec<Value> = std::fs::read_to_string(root.path().join("rpc.jsonl"))
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    let events = sink.0.lock().await;
    assert_eq!(
        audit
            .iter()
            .filter(|value| value["method"] == "session/prompt")
            .count(),
        2,
        "Source retries a side-effect-free empty response once; events={events:?}; audit={audit:?}"
    );
    assert_eq!(
        audit
            .iter()
            .filter(|value| value["method"] == "initialize")
            .count(),
        2,
        "Retry must use a fresh ACP client"
    );
    assert_eq!(
        audit
            .iter()
            .filter(|value| value["method"] == "session/new")
            .count(),
        2,
        "Retry must create a fresh session"
    );
    assert!(
        events
            .iter()
            .any(|value| value["text"] == "recovered answer"),
        "{events:?}"
    );
    assert!(
        !events.iter().any(|value| value["type"] == "error"),
        "{events:?}"
    );
}
