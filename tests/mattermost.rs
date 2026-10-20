use async_trait::async_trait;
use soulsteads_messaging::{
    config,
    connectors::http::Platform,
    runtime::{Bridge, Sink},
};
use serde_json::{Value, json};
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
    let config = config::load(&json!({"rateLimitSeconds":0,"mattermost":mm,"sessionStorePath":directory.path().join("store.json"),"acp":{"command":env!("CARGO_BIN_EXE_acp-fixture"),"args":[]}}).to_string(), &BTreeMap::new()).unwrap();
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
