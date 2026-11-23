use async_trait::async_trait;
use serde_json::{Value, json};
use soulsteads_messaging::{
    config,
    connectors::http::Platform,
    runtime::{Bridge, Sink},
};
use std::collections::BTreeMap;
use tokio::sync::Mutex;

#[derive(Default)]
struct Capture(Mutex<Vec<Value>>);
#[async_trait]
impl Sink for Capture {
    async fn emit(&self, value: Value) -> anyhow::Result<()> {
        self.0.lock().await.push(value);
        Ok(())
    }
}
fn fixture(mm: Value) -> (tempfile::TempDir, Bridge, Platform) {
    let directory = tempfile::tempdir().unwrap();
    let config = config::load(&json!({"rateLimitSeconds":0,"mattermost":mm,"sessionStorePath":directory.path().join("store.json"),"acp":{"command":env!("CARGO_BIN_EXE_acp-fixture"),"args":[],"env":{"ACP_FIXTURE_RPC_AUDIT":directory.path().join("rpc.jsonl")}}}).to_string(), &BTreeMap::new()).unwrap();
    let bridge = Bridge::new(config, BTreeMap::new(), directory.path().join("sessions")).unwrap();
    (
        directory,
        bridge,
        Platform::new(
            "mattermost",
            "http://127.0.0.1:9",
            "fixture",
            "BOT",
            "bridge",
        ),
    )
}
fn post(id: &str, root: &str, text: &str) -> Value {
    json!({"event":"posted","data":{"post":json!({"id":id,"root_id":root,"channel_id":"channel","user_id":"human","message":text}).to_string(),"channel_type":"P"}})
}
async fn deliver(bridge: &Bridge, platform: &Platform, message: Value, sink: &Capture) {
    if let Some(event) = platform.prepare(bridge, &message).await.unwrap() {
        bridge.handle(event, sink).await.unwrap();
    }
}
#[tokio::test]
async fn all_message_admission_requires_boolean_true_and_keeps_addressed_commands() {
    let cases = [
        ("default", json!({"allowedChannels":["channel"]}), false),
        (
            "false",
            json!({"allowedChannels":["channel"],"respondToAllMessages":false}),
            false,
        ),
        (
            "null",
            json!({"allowedChannels":["channel"],"respondToAllMessages":null}),
            false,
        ),
        (
            "string",
            json!({"allowedChannels":["channel"],"respondToAllMessages":"true"}),
            false,
        ),
        (
            "number",
            json!({"allowedChannels":["channel"],"respondToAllMessages":1}),
            false,
        ),
        (
            "true",
            json!({"allowedChannels":["channel"],"respondToAllMessages":true}),
            true,
        ),
    ];
    for (name, mut mm, admitted) in cases {
        mm["allowedUsers"] = json!(["human"]);
        let (_dir, bridge, platform) = fixture(mm);
        let message = post("hello", "", "hello");
        assert_eq!(
            platform.prepare(&bridge, &message).await.unwrap().is_some(),
            admitted,
            "case {name}"
        );
        bridge.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn all_message_mode_does_not_bypass_user_channel_or_post_restrictions() {
    let (_dir, bridge, platform) = fixture(json!({
        "allowedChannels":["channel"], "allowedUsers":["human"], "respondToAllMessages":true
    }));
    let base = post("rejected", "", "hello");
    let mut cases = Vec::new();
    let mut unauthorized = base.clone();
    unauthorized["data"]["post"] =
        json!({"id":"u","channel_id":"channel","user_id":"intruder","message":"hello"})
            .to_string()
            .into();
    cases.push(("unauthorized", unauthorized));
    let mut wrong_channel = base.clone();
    wrong_channel["data"]["post"] =
        json!({"id":"c","channel_id":"other","user_id":"human","message":"hello"})
            .to_string()
            .into();
    cases.push(("channel", wrong_channel));
    let mut self_post = base.clone();
    self_post["data"]["post"] =
        json!({"id":"s","channel_id":"channel","user_id":"BOT","message":"hello"})
            .to_string()
            .into();
    cases.push(("self", self_post));
    let mut system_post = base.clone();
    system_post["data"]["post"] = json!({"id":"sys","channel_id":"channel","user_id":"human","type":"system_join_channel","message":"hello"}).to_string().into();
    cases.push(("system", system_post));
    for (name, message) in cases {
        assert!(
            platform.prepare(&bridge, &message).await.unwrap().is_none(),
            "case {name}"
        );
    }
    bridge.shutdown().await.unwrap();
}

#[tokio::test]
async fn all_message_mode_admits_slash_commands_through_prepare() {
    let (_dir, bridge, platform) = fixture(json!({
        "allowedChannels":["channel"], "allowedUsers":["human"], "respondToAllMessages":true
    }));
    let event = platform
        .prepare(&bridge, &post("slash", "", "/help"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(event.text, "/help");
    bridge.shutdown().await.unwrap();
}

#[tokio::test]
async fn opt_in_admits_plain_top_level_messages_from_allowed_users() {
    let (_dir, bridge, platform) = fixture(json!({
        "allowedChannels":["channel"],
        "allowedUsers":["human"],
        "respondToAllMessages":true
    }));
    let sink = Capture::default();
    deliver(&bridge, &platform, post("hello", "", "hello"), &sink).await;
    assert!(sink.0.lock().await.iter().any(|v| v["type"] == "response"));
    bridge.shutdown().await.unwrap();

    let (_dir, bridge, platform) = fixture(json!({
        "allowedChannels":["channel"], "allowedUsers":["human"], "respondToAllMessages":false
    }));
    let sink = Capture::default();
    deliver(
        &bridge,
        &platform,
        post("plain-disabled", "", "hello"),
        &sink,
    )
    .await;
    assert!(
        sink.0.lock().await.is_empty(),
        "disabled option started work"
    );
    bridge.shutdown().await.unwrap();
}

#[tokio::test]
async fn channel_admission_requires_a_nonempty_valid_explicit_allowlist() {
    let cases = [
        ("missing", json!({}), false),
        ("null", json!({"allowedChannels":null}), false),
        ("empty", json!({"allowedChannels":[]}), false),
        ("malformed", json!({"allowedChannels":["channel",7]}), false),
        ("unmatched", json!({"allowedChannels":["other"]}), false),
        ("matching", json!({"allowedChannels":["channel"]}), true),
    ];
    for (name, config, admitted) in cases {
        let (_dir, bridge, platform) = fixture(config);
        let sink = Capture::default();
        deliver(&bridge, &platform, post("one", "", "!oc hello"), &sink).await;
        assert_eq!(
            sink.0.lock().await.iter().any(|v| v["type"] == "response"),
            admitted,
            "case {name}"
        );
        bridge.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn surrounding_whitespace_is_trimmed_before_mention_matching() {
    let (_dir, bridge, platform) = fixture(json!({"allowedChannels":["channel"]}));
    let sink = Capture::default();
    deliver(
        &bridge,
        &platform,
        post("one", "", "  @bridge hello  "),
        &sink,
    )
    .await;
    assert!(sink.0.lock().await.iter().any(|v| v["type"] == "response"));
    bridge.shutdown().await.unwrap();
}

#[tokio::test]
async fn disabled_mentions_do_not_start_an_agent_turn() {
    let (_dir, bridge, platform) =
        fixture(json!({"allowedChannels":["channel"],"respondToMentions":false}));
    let sink = Capture::default();
    deliver(&bridge, &platform, post("one", "", "@bridge hello"), &sink).await;
    assert!(
        sink.0.lock().await.is_empty(),
        "disabled mention started work"
    );
    bridge.shutdown().await.unwrap();
}
#[tokio::test]
async fn nonisolated_threads_do_not_implicitly_admit_plain_replies() {
    let (_dir, bridge, platform) =
        fixture(json!({"allowedChannels":["channel"],"threadIsolation":false}));
    let sink = Capture::default();
    deliver(&bridge, &platform, post("one", "", "!oc hello"), &sink).await;
    assert!(sink.0.lock().await.iter().any(|v| v["type"] == "response"));
    sink.0.lock().await.clear();
    deliver(
        &bridge,
        &platform,
        post("two", "one", "plain followup"),
        &sink,
    )
    .await;
    assert!(
        sink.0.lock().await.is_empty(),
        "nonisolated plain reply started work"
    );
    bridge.shutdown().await.unwrap();
}
#[tokio::test]
async fn persisted_thread_after_restart_requires_explicit_addressing() {
    let (directory, bridge, platform) =
        fixture(json!({"allowedChannels":["channel"],"threadIsolation":true}));
    let sink = Capture::default();
    deliver(&bridge, &platform, post("one", "", "!oc hello"), &sink).await;
    assert!(bridge.has_persisted("mattermost", "channel:one").unwrap());
    let config = bridge.config.clone();
    bridge.shutdown().await.unwrap();
    let restarted =
        Bridge::new(config, BTreeMap::new(), directory.path().join("sessions")).unwrap();
    assert!(
        restarted
            .has_persisted("mattermost", "channel:one")
            .unwrap()
    );
    sink.0.lock().await.clear();
    deliver(
        &restarted,
        &platform,
        post("two", "one", "plain followup"),
        &sink,
    )
    .await;
    assert!(
        sink.0.lock().await.is_empty(),
        "persisted-only thread must not implicitly start work"
    );
    deliver(
        &restarted,
        &platform,
        post("three", "one", "!oc resume"),
        &sink,
    )
    .await;
    assert!(sink.0.lock().await.iter().any(|v| v["type"] == "response"));
    restarted.shutdown().await.unwrap();
}

#[tokio::test]
async fn cancel_does_not_open_a_saved_thread() {
    let (directory, bridge, platform) = fixture(json!({
        "allowedChannels":["channel"],"allowedUsers":["human"],"threadIsolation":true
    }));
    let sink = Capture::default();
    deliver(&bridge, &platform, post("root", "", "!oc hello"), &sink).await;
    let config = bridge.config.clone();
    bridge.shutdown().await.unwrap();
    let restarted =
        Bridge::new(config, BTreeMap::new(), directory.path().join("sessions")).unwrap();
    assert!(
        restarted
            .has_persisted("mattermost", "channel:root")
            .unwrap()
    );
    let audit = directory.path().join("rpc.jsonl");
    let before = std::fs::read(&audit).unwrap();
    let store = directory.path().join("store.json");
    let saved = std::fs::read(&store).unwrap();
    sink.0.lock().await.clear();
    deliver(
        &restarted,
        &platform,
        post("cancel", "root", "!oc /cancel"),
        &sink,
    )
    .await;
    restarted.shutdown().await.unwrap();
    assert!(
        sink.0
            .lock()
            .await
            .iter()
            .any(|v| v["text"] == "No open session in this thread.")
    );
    assert_eq!(
        std::fs::read(audit).unwrap(),
        before,
        "an unopened thread received a backend RPC"
    );
    assert_eq!(std::fs::read(store).unwrap(), saved);
}

#[tokio::test]
async fn cancel_targets_only_the_active_thread_and_takes_no_arguments() {
    use std::time::Duration;

    let (directory, bridge, platform) = fixture(json!({
        "allowedChannels":["channel"],"allowedUsers":["human"],"threadIsolation":true
    }));
    let audit = directory.path().join("rpc.jsonl");
    let prompt_sink = Capture::default();
    let control_sink = Capture::default();
    let prompt = deliver(
        &bridge,
        &platform,
        post("root", "", "!oc wait"),
        &prompt_sink,
    );
    let controls = async {
        loop {
            if std::fs::read_to_string(&audit).is_ok_and(|log| {
                log.lines().any(|line| {
                    serde_json::from_str::<Value>(line)
                        .is_ok_and(|v| v["method"] == "session/prompt")
                })
            }) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        deliver(
            &bridge,
            &platform,
            post("foreign", "other", "!oc /cancel"),
            &control_sink,
        )
        .await;
        let unauthorized = json!({"event":"posted","data":{"post":json!({
            "id":"denied","root_id":"root","channel_id":"channel","user_id":"denied","message":"!oc /cancel"
        }).to_string(),"channel_type":"P"}});
        deliver(&bridge, &platform, unauthorized, &control_sink).await;
        deliver(
            &bridge,
            &platform,
            post("with-arg", "root", "!oc /cancel other"),
            &control_sink,
        )
        .await;
        let cancel = post("cancel", "root", "!oc /cancel");
        deliver(&bridge, &platform, cancel.clone(), &control_sink).await;
        deliver(&bridge, &platform, cancel, &control_sink).await;
    };
    let outcome = tokio::time::timeout(Duration::from_secs(5), async {
        tokio::join!(prompt, controls);
    })
    .await;
    bridge.shutdown().await.unwrap();
    outcome.expect("the active ACP prompt did not settle");
    let values = control_sink.0.lock().await;
    let messages: Vec<_> = values.iter().filter_map(|v| v["text"].as_str()).collect();
    assert_eq!(
        messages,
        [
            "No open session in this thread.",
            "Usage: /cancel (without arguments).",
            "Cancellation requested."
        ]
    );
    assert!(
        prompt_sink
            .0
            .lock()
            .await
            .iter()
            .any(|v| v["text"] == "Request cancelled.")
    );
    let calls: Vec<Value> = std::fs::read_to_string(audit)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    let prompt = calls
        .iter()
        .find(|v| v["method"] == "session/prompt")
        .unwrap();
    let cancels: Vec<_> = calls
        .iter()
        .filter(|v| v["method"] == "session/cancel")
        .collect();
    assert_eq!(cancels.len(), 1);
    assert_eq!(
        cancels[0]["params"]["sessionId"],
        prompt["params"]["sessionId"]
    );
}

#[tokio::test]
async fn resume_opens_only_the_saved_thread_without_prompting() {
    let (directory, bridge, platform) = fixture(json!({
        "allowedChannels":["channel"],"allowedUsers":["human"],"threadIsolation":true,
        "respondToThreadReplies":true
    }));
    let sink = Capture::default();
    deliver(&bridge, &platform, post("one", "", "!oc hello"), &sink).await;
    let store = directory.path().join("store.json");
    let before = std::fs::read(&store).unwrap();
    let saved = serde_json::from_slice::<Value>(&before).unwrap();
    let id = &saved["sessions"][0]["sessionId"];
    let config = bridge.config.clone();
    bridge.shutdown().await.unwrap();
    let audit = directory.path().join("rpc.jsonl");
    std::fs::write(&audit, "").unwrap();
    let restarted =
        Bridge::new(config, BTreeMap::new(), directory.path().join("sessions")).unwrap();

    sink.0.lock().await.clear();
    deliver(
        &restarted,
        &platform,
        post("status", "one", "!oc /status"),
        &sink,
    )
    .await;
    assert!(sink.0.lock().await.iter().any(|v| v["text"] == "This thread has a saved session. Address the bot with /resume to reopen it without starting a prompt."));
    assert!(std::fs::read_to_string(&audit).unwrap().is_empty());
    sink.0.lock().await.clear();
    deliver(
        &restarted,
        &platform,
        post("resume", "one", "!oc /resume"),
        &sink,
    )
    .await;
    assert!(
        sink.0
            .lock()
            .await
            .iter()
            .any(|v| v["text"] == "Saved session resumed.")
    );
    assert_eq!(std::fs::read(&store).unwrap(), before);
    let resumed = std::fs::read_to_string(&audit).unwrap();
    let requests: Vec<Value> = resumed
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    let session_ops: Vec<_> = requests
        .iter()
        .filter(|v| {
            v["method"]
                .as_str()
                .is_some_and(|method| method.starts_with("session/"))
        })
        .collect();
    assert_eq!(session_ops.len(), 1, "{session_ops:?}");
    assert!(matches!(
        session_ops[0]["method"].as_str(),
        Some("session/load" | "session/resume")
    ));
    assert_eq!(&session_ops[0]["params"]["sessionId"], id);

    deliver(
        &restarted,
        &platform,
        post("repeat", "one", "!oc /resume"),
        &sink,
    )
    .await;
    assert_eq!(std::fs::read_to_string(&audit).unwrap(), resumed);
    sink.0.lock().await.clear();
    deliver(
        &restarted,
        &platform,
        post("followup", "one", "plain followup"),
        &sink,
    )
    .await;
    assert!(sink.0.lock().await.iter().any(|v| v["type"] == "response"));
    let after = std::fs::read_to_string(&audit).unwrap();
    let prompt: Value = serde_json::from_str(after.lines().last().unwrap()).unwrap();
    assert_eq!(prompt["method"], "session/prompt");
    assert_eq!(&prompt["params"]["sessionId"], id);
    restarted.shutdown().await.unwrap();
}

#[tokio::test]
async fn resume_does_not_launch_or_change_bindings_for_another_thread_or_invalid_arguments() {
    let (directory, bridge, platform) = fixture(json!({
        "allowedChannels":["channel"],"allowedUsers":["human"],"threadIsolation":true
    }));
    let sink = Capture::default();
    deliver(&bridge, &platform, post("one", "", "!oc hello"), &sink).await;
    let store = directory.path().join("store.json");
    let saved = std::fs::read(&store).unwrap();
    let config = bridge.config.clone();
    bridge.shutdown().await.unwrap();
    let audit = directory.path().join("rpc.jsonl");
    std::fs::write(&audit, "").unwrap();
    let restarted = Bridge::new(
        config.clone(),
        BTreeMap::new(),
        directory.path().join("sessions"),
    )
    .unwrap();
    for (event, expected) in [
        (
            post("none", "other", "!oc /resume"),
            "No saved session is bound to this thread.",
        ),
        (
            post("args", "one", "!oc /resume another-session"),
            "Usage: /resume (without a session ID).",
        ),
    ] {
        sink.0.lock().await.clear();
        deliver(&restarted, &platform, event, &sink).await;
        assert!(sink.0.lock().await.iter().any(|v| v["text"] == expected));
    }
    let mut denied = post("denied", "one", "!oc /resume");
    let mut content: Value =
        serde_json::from_str(denied["data"]["post"].as_str().unwrap()).unwrap();
    content["user_id"] = json!("intruder");
    denied["data"]["post"] = json!(content.to_string());
    assert!(
        platform
            .prepare(&restarted, &denied)
            .await
            .unwrap()
            .is_none()
    );
    restarted.shutdown().await.unwrap();
    let mut nonisolated = config;
    nonisolated["mattermost"]["threadIsolation"] = json!(false);
    let restarted = Bridge::new(
        nonisolated,
        BTreeMap::new(),
        directory.path().join("sessions"),
    )
    .unwrap();
    sink.0.lock().await.clear();
    deliver(
        &restarted,
        &platform,
        post("unisolated", "one", "!oc /resume"),
        &sink,
    )
    .await;
    assert!(
        sink.0
            .lock()
            .await
            .iter()
            .any(|v| v["text"] == "Resume requires an isolated Mattermost thread.")
    );
    restarted.shutdown().await.unwrap();
    assert_eq!(std::fs::read(store).unwrap(), saved);
    assert!(std::fs::read_to_string(audit).unwrap().is_empty());
}

#[tokio::test]
async fn resume_refuses_a_changed_backend_identity_without_starting_it() {
    let (directory, bridge, platform) = fixture(json!({
        "allowedChannels":["channel"],"allowedUsers":["human"],"threadIsolation":true
    }));
    let sink = Capture::default();
    deliver(&bridge, &platform, post("one", "", "!oc hello"), &sink).await;
    let store = directory.path().join("store.json");
    let saved = std::fs::read(&store).unwrap();
    let mut config = bridge.config.clone();
    config["acp"]["backendId"] = json!("different-binding");
    bridge.shutdown().await.unwrap();
    let audit = directory.path().join("rpc.jsonl");
    std::fs::write(&audit, "").unwrap();
    let restarted =
        Bridge::new(config, BTreeMap::new(), directory.path().join("sessions")).unwrap();
    let event = platform
        .prepare(&restarted, &post("resume", "one", "!oc /resume"))
        .await
        .unwrap()
        .unwrap();
    let error = restarted.handle(event, &sink).await.unwrap_err();
    assert!(
        error
            .to_string()
            .contains("Stored backend identity or workspace differs")
    );
    assert!(
        !restarted
            .has_active_session("mattermost", "channel:one")
            .await
            .unwrap()
    );
    restarted.shutdown().await.unwrap();
    assert_eq!(std::fs::read(store).unwrap(), saved);
    assert!(std::fs::read_to_string(audit).unwrap().is_empty());
}

#[tokio::test]
async fn isolated_plain_replies_require_an_existing_thread_session() {
    let (_dir, bridge, platform) =
        fixture(json!({"allowedChannels":["channel"],"threadIsolation":true}));
    let sink = Capture::default();
    deliver(
        &bridge,
        &platform,
        post("unknown", "unstarted", "plain followup"),
        &sink,
    )
    .await;
    assert!(sink.0.lock().await.is_empty());
    deliver(&bridge, &platform, post("one", "", "!oc hello"), &sink).await;
    sink.0.lock().await.clear();
    deliver(
        &bridge,
        &platform,
        post("two", "one", "plain followup"),
        &sink,
    )
    .await;
    assert!(sink.0.lock().await.iter().any(|v| v["type"] == "response"));
    bridge.shutdown().await.unwrap();
}
