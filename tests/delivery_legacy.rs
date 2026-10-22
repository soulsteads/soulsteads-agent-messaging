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

impl Recorder {
    async fn take(&self) -> Vec<Value> {
        std::mem::take(&mut *self.0.lock().await)
    }
}

fn configuration(root: &Path) -> Value {
    config::load(
        &json!({
            "rateLimitSeconds": 0,
            "sessionStorePath": root.join("store.json"),
            "acp": {
                "command": env!("CARGO_BIN_EXE_acp-fixture"),
                "args": [],
                "env": {
                    "ACP_FIXTURE_MATRIX_SCRIPT": root.join("script.json"),
                    "ACP_FIXTURE_MATRIX_COUNTER": root.join("counter"),
                    "ACP_FIXTURE_RPC_AUDIT": root.join("rpc.jsonl")
                }
            }
        })
        .to_string(),
        &BTreeMap::new(),
    )
    .unwrap()
}

fn requests(root: &Path) -> Vec<Value> {
    std::fs::read_to_string(root.join("rpc.jsonl"))
        .unwrap_or_default()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect()
}

fn event_types(events: &[Value]) -> Vec<String> {
    events
        .iter()
        .map(|event| event["type"].as_str().unwrap().to_owned())
        .collect()
}

#[test]
fn web_event_constructor_preserves_the_exact_legacy_identity_shape() {
    let event = Event::web("browser-user", "legacy prompt");

    assert_eq!(event.platform, "web");
    assert_eq!(event.room, "browser-user");
    assert_eq!(event.thread, "");
    assert_eq!(event.id, "");
    assert_eq!(event.reply_to, None);
    assert_eq!(event.user, "browser-user");
    assert_eq!(event.text, "legacy prompt");
    assert!(event.direct);
    assert!(!event.mentioned);
    assert!(!event.bot);
    assert!(event.images.is_empty());
}

#[tokio::test]
async fn matrix_thread_isolation_uses_the_legacy_room_colon_thread_store_key() {
    let root = tempfile::tempdir().unwrap();
    std::fs::write(
        root.path().join("script.json"),
        json!([{"text":"matrix answer","stopReason":"end_turn"}]).to_string(),
    )
    .unwrap();
    let mut config = configuration(root.path());
    config["matrix"] = json!({"respondToAllMessages":true,"threadIsolation":true});
    let workspace = root.path().join("sessions");
    let bridge = Bridge::new(config, BTreeMap::new(), workspace).unwrap();
    let sink = Recorder::default();
    let event = Event {
        platform: "matrix".into(),
        room: "!legacy:local".into(),
        thread: "$root:thread".into(),
        id: "legacy-event".into(),
        reply_to: Some("$parent".into()),
        user: "@legacy:local".into(),
        text: "matrix prompt".into(),
        direct: false,
        mentioned: false,
        bot: false,
        images: vec![],
    };
    let context = "!legacy:local:$root:thread";

    assert_eq!(bridge.context(&event), context);
    bridge.handle(event, &sink).await.unwrap();

    let store = Store::new(root.path().join("store.json"));
    let entry = store.get("matrix", context, "opencode").unwrap().unwrap();
    assert_eq!(entry.connector, "matrix");
    assert_eq!(entry.thread_id, context);
    assert_eq!(entry.backend, "opencode");
    assert_eq!(entry.session_id, "fixture");
    assert!(
        store
            .get("matrix", "!legacy:local", "opencode")
            .unwrap()
            .is_none(),
        "thread isolation must not fall back to the room-only binding"
    );
    assert_eq!(
        requests(root.path())
            .iter()
            .filter(|request| request["method"] == "session/prompt")
            .map(|request| request["params"]["prompt"][0]["text"].clone())
            .collect::<Vec<_>>(),
        vec![json!("matrix prompt")]
    );

    bridge.shutdown().await.unwrap();
}

#[tokio::test]
async fn web_prompt_status_and_clear_keep_legacy_routing_terminal_order_and_session_identity() {
    let root = tempfile::tempdir().unwrap();
    std::fs::write(
        root.path().join("script.json"),
        json!([{"text":"legacy answer","stopReason":"end_turn"}]).to_string(),
    )
    .unwrap();
    let bridge = Bridge::new(
        configuration(root.path()),
        BTreeMap::new(),
        root.path().join("sessions"),
    )
    .unwrap();
    let sink = Recorder::default();
    let store = Store::new(root.path().join("store.json"));

    bridge
        .handle_web(Event::web("legacy-browser", "legacy prompt"), &sink)
        .await
        .unwrap();
    let prompt_events = sink.take().await;
    assert_eq!(
        event_types(&prompt_events),
        vec!["thinking", "chunk", "response", "prompt_end", "done"]
    );
    assert_eq!(
        prompt_events[1],
        json!({"type":"chunk","text":"legacy answer"})
    );
    assert_eq!(
        prompt_events[2],
        json!({"type":"response","text":"legacy answer"})
    );
    assert_eq!(prompt_events[3]["failed"], false);
    assert_eq!(prompt_events[4], json!({"type":"done"}));
    let entry = store
        .get("web", "legacy-browser", "opencode")
        .unwrap()
        .unwrap();
    assert_eq!(entry.connector, "web");
    assert_eq!(entry.thread_id, "legacy-browser");
    assert_eq!(entry.backend, "opencode");
    assert_eq!(entry.session_id, "fixture");

    bridge
        .handle_web(Event::web("legacy-browser", "/status"), &sink)
        .await
        .unwrap();
    let status_events = sink.take().await;
    assert_eq!(event_types(&status_events), vec!["response", "done"]);
    assert!(
        status_events[0]["text"]
            .as_str()
            .is_some_and(|text| text.contains("Messages: 1")),
        "{status_events:?}"
    );

    bridge
        .handle_web(Event::web("legacy-browser", "/clear"), &sink)
        .await
        .unwrap();
    let clear_events = sink.take().await;
    assert_eq!(
        clear_events,
        vec![
            json!({"type":"response","text":"Session cleared. Next message will start a fresh session."}),
            json!({"type":"session_state","state":"cleared"}),
            json!({"type":"done"})
        ]
    );
    assert!(
        store
            .get("web", "legacy-browser", "opencode")
            .unwrap()
            .is_none()
    );

    let requests = requests(root.path());
    assert_eq!(
        requests
            .iter()
            .filter(|request| request["method"] == "session/prompt")
            .map(|request| request["params"]["prompt"][0]["text"].clone())
            .collect::<Vec<_>>(),
        vec![json!("legacy prompt")],
        "status and clear must remain local controls, never ACP prompts"
    );
    assert_eq!(
        requests
            .iter()
            .filter(|request| request["method"] == "session/delete")
            .count(),
        1
    );

    bridge.shutdown().await.unwrap();
}

#[tokio::test]
async fn absent_agent_messaging_does_not_create_the_peer_snapshot_or_owner_lock() {
    let root = tempfile::tempdir().unwrap();
    let store_path = root.path().join("store.json");
    let bridge = Bridge::new(
        configuration(root.path()),
        BTreeMap::new(),
        root.path().join("sessions"),
    )
    .unwrap();
    let sink = Recorder::default();

    bridge
        .handle_web(Event::web("legacy-browser", "/status"), &sink)
        .await
        .unwrap();
    assert_eq!(event_types(&sink.take().await), vec!["response", "done"]);
    assert!(
        !root
            .path()
            .join("store.json.peer-delivery.v1.json")
            .exists(),
        "absent agentMessaging must not create a peer snapshot"
    );
    assert!(
        !root
            .path()
            .join("store.json.peer-delivery.v1.json.lock")
            .exists(),
        "absent agentMessaging must not acquire a peer owner lock"
    );
    assert!(
        !store_path.exists(),
        "status must not create legacy session state"
    );

    bridge.shutdown().await.unwrap();
}
