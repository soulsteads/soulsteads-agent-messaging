use async_trait::async_trait;
use soulsteads_messaging::{
    config,
    runtime::{Bridge, Event, Sink},
};
use serde_json::{Value, json};
use std::collections::BTreeMap;
use tokio::sync::Mutex;

#[derive(Clone)]
struct BlockedReceipt {
    events: std::sync::Arc<Mutex<Vec<Value>>>,
    started: std::sync::Arc<tokio::sync::Semaphore>,
    release: std::sync::Arc<tokio::sync::Semaphore>,
}
#[async_trait]
impl Sink for BlockedReceipt {
    fn owned(&self) -> Option<std::sync::Arc<dyn Sink>> {
        Some(std::sync::Arc::new(self.clone()))
    }
    async fn emit(&self, value: Value) -> anyhow::Result<()> {
        let read = value["type"] == "read";
        self.events.lock().await.push(value);
        if read {
            self.started.add_permits(1);
            self.release.acquire().await.unwrap().forget();
            anyhow::bail!("receipt transport timed out");
        }
        Ok(())
    }
}

#[derive(Default)]
struct Recorder(Mutex<Vec<Value>>, std::sync::atomic::AtomicBool);
#[async_trait]
impl Sink for Recorder {
    async fn emit(&self, value: Value) -> anyhow::Result<()> {
        let fail = value["type"] == "read" && self.1.load(std::sync::atomic::Ordering::SeqCst);
        self.0.lock().await.push(value);
        if fail {
            anyhow::bail!("receipt transport failed");
        }
        Ok(())
    }
}
fn configuration(path: &std::path::Path) -> Value {
    config::load(&json!({"rateLimitSeconds":0,"sessionStorePath":path.join("store.json"),"acp":{"command":env!("CARGO_BIN_EXE_acp-fixture"),"args":[]},"matrix":{"userId":"@bot:local","allowedUsers":["@user:local"],"allowedRooms":["!room:local"]}}).to_string(), &BTreeMap::new()).unwrap()
}
fn event(text: &str) -> Event {
    Event {
        platform: "matrix".into(),
        room: "!room:local".into(),
        thread: "$root".into(),
        id: uuid::Uuid::new_v4().to_string(),
        user: "@user:local".into(),
        text: text.into(),
        direct: false,
        mentioned: false,
        bot: false,
        images: vec![],
        reply_to: None,
    }
}

#[tokio::test]
async fn matrix_literal_address_precedence_and_leading_query_separators() {
    for (body, mentioned) in [
        ("@bot:local /status", false),
        ("@bot:local : /status", true),
        ("oc: @bot:local /status", false),
        ("!oc : /status", false),
        ("@OC : /status", true),
    ] {
        let directory = tempfile::tempdir().unwrap();
        let bridge = Bridge::new(
            configuration(directory.path()),
            BTreeMap::new(),
            directory.path().join("sessions"),
        )
        .unwrap();
        let sink = Recorder::default();
        let mut input = event(body);
        input.mentioned = mentioned;
        bridge.handle(input, &sink).await.unwrap();
        let output = sink.0.lock().await;
        let expected_status = !body.starts_with("oc: @bot");
        assert_eq!(
            output.iter().any(|value| value["text"]
                .as_str()
                .is_some_and(|text| text.starts_with("Backend:"))),
            expected_status,
            "wrong query/address precedence for {body}: {output:?}"
        );
        assert!(
            !output.is_empty(),
            "literal address must be admitted: {body}"
        );
        drop(output);
        bridge.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn matrix_address_does_not_bypass_negative_admission() {
    for refusal in [
        "unaddressed",
        "user",
        "ignore_user",
        "room",
        "ignore_room",
        "bot",
        "empty",
    ] {
        let directory = tempfile::tempdir().unwrap();
        let mut config = configuration(directory.path());
        let mut input = event("@bot:local /status");
        match refusal {
            "unaddressed" => input.text = "plain room message".into(),
            "user" => input.user = "@other:local".into(),
            "ignore_user" => config["matrix"]["ignoreUsers"] = json!([input.user]),
            "room" => input.room = "!denied:local".into(),
            "ignore_room" => config["matrix"]["ignoreRooms"] = json!([input.room]),
            "bot" => input.bot = true,
            "empty" => input.text = "@bot:local : ".into(),
            _ => unreachable!(),
        }
        let bridge =
            Bridge::new(config, BTreeMap::new(), directory.path().join("sessions")).unwrap();
        let sink = Recorder::default();
        bridge.handle(input, &sink).await.unwrap();
        assert!(sink.0.lock().await.is_empty(), "refusal failed: {refusal}");
        assert!(!directory.path().join("store.json").exists());
    }
}

#[tokio::test]
async fn matrix_only_admitted_events_receive_best_effort_read_receipts() {
    let directory = tempfile::tempdir().unwrap();
    let mut config = configuration(directory.path());
    config["rateLimitSeconds"] = json!(60);
    let bridge = Bridge::new(config, BTreeMap::new(), directory.path().join("sessions")).unwrap();
    let sink = Recorder::default();
    sink.1.store(true, std::sync::atomic::Ordering::SeqCst);
    let accepted = event("!oc /status");
    bridge.handle(accepted.clone(), &sink).await.unwrap();
    assert!(
        sink.0
            .lock()
            .await
            .iter()
            .any(|value| value["type"] == "read" && value["eventId"] == accepted.id)
    );
    assert!(
        sink.0
            .lock()
            .await
            .iter()
            .any(|value| value["type"] == "response")
    );
    let read_count = sink
        .0
        .lock()
        .await
        .iter()
        .filter(|value| value["type"] == "read")
        .count();
    bridge.handle(accepted, &sink).await.unwrap();
    bridge.handle(event("!oc /status"), &sink).await.unwrap();
    bridge.handle(event("unaddressed"), &sink).await.unwrap();
    let mut denied = event("!oc /status");
    denied.user = "@other:local".into();
    bridge.handle(denied, &sink).await.unwrap();
    assert_eq!(
        sink.0
            .lock()
            .await
            .iter()
            .filter(|value| value["type"] == "read")
            .count(),
        read_count
    );
}

#[tokio::test]
async fn matrix_typing_wraps_failed_initialization() {
    let directory = tempfile::tempdir().unwrap();
    let mut config = configuration(directory.path());
    config["acp"]["command"] = json!("/not-an-acp-executable");
    let bridge = Bridge::new(config, BTreeMap::new(), directory.path().join("sessions")).unwrap();
    let sink = Recorder::default();
    assert!(bridge.handle(event("!oc hello"), &sink).await.is_err());
    let output = sink.0.lock().await;
    assert_eq!(
        output
            .iter()
            .filter(|value| value["type"] == "thinking")
            .count(),
        1,
        "typing must start before initialization: {output:?}"
    );
    assert_eq!(output.last().unwrap()["type"], "done");
}

#[tokio::test]
async fn matrix_implicit_thread_excludes_case_insensitive_trigger_prefixes_after_reload() {
    for reload in [false, true] {
        let directory = tempfile::tempdir().unwrap();
        let config = configuration(directory.path());
        let mut bridge = Bridge::new(
            config.clone(),
            BTreeMap::new(),
            directory.path().join("sessions"),
        )
        .unwrap();
        let sink = Recorder::default();
        bridge.handle(event("!oc hello"), &sink).await.unwrap();
        if reload {
            bridge.shutdown().await.unwrap();
            bridge =
                Bridge::new(config, BTreeMap::new(), directory.path().join("sessions")).unwrap();
        }
        for text in ["!OC hello", "!OcSuffix", "  !oC: hello  ", "   "] {
            let count = sink.0.lock().await.len();
            bridge.handle(event(text), &sink).await.unwrap();
            assert_eq!(
                sink.0.lock().await.len(),
                count,
                "implicit admission must reject {text:?}, reload={reload}"
            );
        }
        bridge
            .handle(event("ordinary thread reply"), &sink)
            .await
            .unwrap();
        bridge
            .handle(event("!oc explicit reply"), &sink)
            .await
            .unwrap();
        bridge
            .handle(event("@bot:local literal mention"), &sink)
            .await
            .unwrap();
        assert!(
            sink.0
                .lock()
                .await
                .iter()
                .any(|value| value["type"] == "response" && value["text"] == "hello world")
        );
        bridge.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn matrix_disabled_implicit_thread_admission_is_not_enabled_by_other_platform_topic_option() {
    let directory = tempfile::tempdir().unwrap();
    let mut config = configuration(directory.path());
    config["matrix"]["respondToThreadReplies"] = json!(false);
    config["matrix"]["respondToImplicitTopicReplies"] = json!(true);
    let bridge = Bridge::new(config, BTreeMap::new(), directory.path().join("sessions")).unwrap();
    let sink = Recorder::default();
    bridge.handle(event("!oc hello"), &sink).await.unwrap();
    let count = sink.0.lock().await.len();
    bridge.handle(event("ordinary reply"), &sink).await.unwrap();
    assert_eq!(sink.0.lock().await.len(), count);
    bridge.shutdown().await.unwrap();
}

#[tokio::test]
async fn matrix_isolation_off_shares_session_but_refuses_unaddressed_thread_replies() {
    let directory = tempfile::tempdir().unwrap();
    let mut config = configuration(directory.path());
    config["matrix"]["threadIsolation"] = json!(false);
    let bridge = Bridge::new(config, BTreeMap::new(), directory.path().join("sessions")).unwrap();
    let sink = Recorder::default();
    let initial = event("!oc hello");
    assert_eq!(bridge.context(&initial), "!room:local");
    bridge.handle(initial, &sink).await.unwrap();
    let count = sink.0.lock().await.len();
    let followup = event("unaddressed thread followup");
    bridge.handle(followup, &sink).await.unwrap();
    assert_eq!(sink.0.lock().await.len(), count);
    let mut addressed = event("!oc /status");
    addressed.thread = "$another".into();
    bridge.handle(addressed, &sink).await.unwrap();
    assert!(sink.0.lock().await.iter().any(|value| {
        value["text"]
            .as_str()
            .is_some_and(|text| text.contains("Messages: 1"))
    }));
    bridge.shutdown().await.unwrap();
}

#[tokio::test]
async fn matrix_slow_failed_receipt_never_blocks_initialization_prompt_or_final_reply() {
    let directory = tempfile::tempdir().unwrap();
    let bridge = std::sync::Arc::new(
        Bridge::new(
            configuration(directory.path()),
            BTreeMap::new(),
            directory.path().join("sessions"),
        )
        .unwrap(),
    );
    let sink = BlockedReceipt {
        events: Default::default(),
        started: std::sync::Arc::new(tokio::sync::Semaphore::new(0)),
        release: std::sync::Arc::new(tokio::sync::Semaphore::new(0)),
    };
    let mut running = {
        let bridge = bridge.clone();
        let sink = sink.clone();
        tokio::spawn(async move { bridge.handle(event("!oc hello"), &sink).await })
    };
    tokio::time::timeout(std::time::Duration::from_secs(2), sink.started.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
    let result = tokio::time::timeout(std::time::Duration::from_millis(800), &mut running).await;
    sink.release.add_permits(1);
    let nonblocking = result.is_ok();
    if let Ok(completed) = result {
        completed.unwrap().unwrap();
    } else {
        running.await.unwrap().unwrap();
    }
    bridge.shutdown().await.unwrap();
    assert!(nonblocking, "a slow receipt blocked the coding request");
    assert!(
        sink.events
            .lock()
            .await
            .iter()
            .any(|value| value["type"] == "response" && value["text"] == "hello world")
    );
}
