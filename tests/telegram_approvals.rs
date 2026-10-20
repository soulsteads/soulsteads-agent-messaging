use async_trait::async_trait;
use soulsteads_messaging::{
    config,
    connectors::http::Platform,
    runtime::{Bridge, Event, Sink},
};
use serde_json::{Value, json};
use std::{collections::BTreeMap, sync::Arc, time::Duration};
use tokio::sync::Mutex;

#[derive(Clone, Default)]
struct Recorder(Arc<Mutex<Vec<Value>>>);
#[async_trait]
impl Sink for Recorder {
    async fn emit(&self, value: Value) -> anyhow::Result<()> {
        self.0.lock().await.push(value);
        Ok(())
    }
}
struct Fixture {
    root: tempfile::TempDir,
    bridge: Arc<Bridge>,
    sink: Recorder,
}
impl Fixture {
    fn new(interactive: Value) -> Self {
        let root = tempfile::tempdir().unwrap();
        let cfg = config::load(&json!({"rateLimitSeconds":0,"sessionStorePath":root.path().join("store.json"),
            "acp":{"command":"/usr/bin/python3","args":[concat!(env!("CARGO_MANIFEST_DIR"),"/tests/fixtures/matrix-approval-peer.py")],
                "env":{"APPROVAL_AUDIT":root.path().join("rpc.jsonl")}},
            "telegram":{"interactivePermissions":interactive,"allowedUsers":["8","9"],"threadIsolation":false}
        }).to_string(), &BTreeMap::new()).unwrap();
        Self {
            bridge: Arc::new(
                Bridge::new(cfg, BTreeMap::new(), root.path().join("sessions")).unwrap(),
            ),
            root,
            sink: Recorder::default(),
        }
    }
    fn start(&self) -> tokio::task::JoinHandle<anyhow::Result<()>> {
        let bridge = self.bridge.clone();
        let sink = self.sink.clone();
        tokio::spawn(async move { bridge.handle(event("work"), &sink).await })
    }
    async fn notice(&self) -> String {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if let Some(text) = self
                    .sink
                    .0
                    .lock()
                    .await
                    .iter()
                    .filter_map(|v| v["text"].as_str())
                    .find(|s| s.contains("/approve "))
                {
                    return text.to_owned();
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("Telegram approval notice")
    }
    fn audit(&self) -> Vec<Value> {
        std::fs::read_to_string(self.root.path().join("rpc.jsonl"))
            .unwrap_or_default()
            .lines()
            .map(|s| serde_json::from_str(s).unwrap())
            .collect()
    }
    fn replies(&self) -> Vec<Value> {
        self.audit()
            .into_iter()
            .filter(|v| v["id"] == 900 && v.get("method").is_none())
            .collect()
    }
    async fn finish(&self, task: tokio::task::JoinHandle<anyhow::Result<()>>) {
        tokio::time::timeout(Duration::from_secs(5), task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        self.bridge.shutdown().await.unwrap();
    }
}
fn event(text: &str) -> Event {
    // Exercise actual Telegram mapping, including chat-scoped message identity.
    use std::sync::atomic::{AtomicI64, Ordering};
    static ID: AtomicI64 = AtomicI64::new(1);
    Platform::new("telegram", "http://127.0.0.1", "fixture", "0", "bridge").map(&json!({
        "message_id":ID.fetch_add(1, Ordering::Relaxed), "chat":{"id":7,"type":"private"},"from":{"id":8},"text":text
    })).unwrap()
}
fn request_id(body: &str) -> &str {
    body.split("/approve ")
        .nth(1)
        .unwrap()
        .split_whitespace()
        .next()
        .unwrap()
}
#[test]
fn telegram_bot_command_suffix_is_normalized_only_for_this_bot() {
    let platform = Platform::new("telegram", "http://127.0.0.1", "fixture", "0", "bridge");
    for (text, expected) in [
        ("/status@bridge", "/status"),
        ("/approve@BRIDGE request-id", "/approve request-id"),
        ("/deny@bridge request-id", "/deny request-id"),
    ] {
        let message =
            json!({"message_id":1,"chat":{"id":7,"type":"supergroup"},"from":{"id":8},"text":text});
        let mapped = platform.map(&message).unwrap();
        assert_eq!(mapped.text, expected);
        assert!(mapped.mentioned);
    }
    let literal_path = json!({"message_id":3,"chat":{"id":7,"type":"private"},"from":{"id":8},"text":"/tmp/user@host data"});
    assert_eq!(
        platform.map(&literal_path).unwrap().text,
        "/tmp/user@host data"
    );
    let other_bot = json!({"message_id":2,"chat":{"id":7,"type":"private"},"from":{"id":8},"text":"/approve@another_bot request-id"});
    assert!(
        platform.map(&other_bot).is_none(),
        "a command addressed to another bot must never become a prompt"
    );
}

#[tokio::test]
async fn telegram_permission_notice_and_decision_cross_the_real_http_sink() {
    http_permission(false).await;
}
#[tokio::test]
async fn telegram_failed_http_preview_cannot_grant_an_operation() {
    http_permission(true).await;
}
async fn http_permission(reject: bool) {
    use axum::{Json, Router, extract::State, routing::post};
    use soulsteads_messaging::connectors::http::PlatformSink;
    use std::future::IntoFuture;
    async fn api(
        State((requests, reject)): State<(Arc<Mutex<Vec<Value>>>, bool)>,
        Json(body): Json<Value>,
    ) -> (axum::http::StatusCode, Json<Value>) {
        let rejected = reject
            && body["text"]
                .as_str()
                .is_some_and(|s| s.contains("/approve "));
        requests.lock().await.push(body);
        if rejected {
            (
                axum::http::StatusCode::BAD_REQUEST,
                Json(json!({"ok":false,"error_code":400})),
            )
        } else {
            (
                axum::http::StatusCode::OK,
                Json(json!({"ok":true,"result":{"message_id":123}})),
            )
        }
    }
    let requests = Arc::new(Mutex::new(Vec::new()));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let platform = Platform::new(
        "telegram",
        &format!("http://{}", listener.local_addr().unwrap()),
        "fixture",
        "0",
        "bridge",
    );
    let server = tokio::spawn(
        axum::serve(
            listener,
            Router::new()
                .fallback(post(api))
                .with_state((requests.clone(), reject)),
        )
        .into_future(),
    );
    let fixture = Fixture::new(json!(true));
    let bridge = fixture.bridge.clone();
    let transport = platform.clone();
    let task = tokio::spawn(async move {
        let origin = event("work");
        bridge
            .handle(origin.clone(), &PlatformSink::new(transport, origin))
            .await
    });
    let notice = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let Some(text) = requests
                .lock()
                .await
                .iter()
                .filter_map(|v| v["text"].as_str())
                .find(|s| s.contains("/approve "))
            {
                break text.to_owned();
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    // A posted preview isn't a grant; wait for its acknowledgement to be consumed.
    tokio::time::sleep(Duration::from_millis(20)).await;
    if reject {
        // The real transport error must have cancelled the permission before
        // a late command can select an allow-once outcome.
        tokio::time::timeout(Duration::from_secs(5), async {
            while fixture.replies().is_empty() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    } else {
        assert!(fixture.replies().is_empty());
    }
    let reply = platform.prepare(&fixture.bridge,&json!({"message_id":9002,"chat":{"id":7,"type":"private"},"from":{"id":8},"text":format!("/approve {}",request_id(&notice))})).await.unwrap().unwrap();
    fixture
        .bridge
        .handle(reply.clone(), &PlatformSink::new(platform, reply))
        .await
        .unwrap();
    fixture.finish(task).await;
    assert_eq!(
        fixture.replies()[0]["result"]["outcome"],
        if reject {
            json!({"outcome":"cancelled"})
        } else {
            json!({"outcome":"selected","optionId":"native-once"})
        }
    );
    let sent = requests.lock().await;
    assert!(sent.iter().any(|v| v["chat_id"] == "7"
        && v["text"]
            == if reject {
                "No matching pending approval for this request."
            } else {
                "Approval decision recorded."
            }));
    assert!(
        sent.iter()
            .filter(|v| v["text"].is_string())
            .all(|v| v.get("parse_mode").is_none()),
        "previews must be literal text, not Telegram markup"
    );
    server.abort();
}

#[tokio::test]
async fn telegram_denial_is_scoped_to_the_requesting_sender_chat_and_topic() {
    let fixture = Fixture::new(json!(true));
    let task = fixture.start();
    let notice = fixture.notice().await;
    let text = format!("/deny {}", request_id(&notice));
    let mut sender = event(&text);
    sender.user = "9".into();
    let mut unauthorized = event(&text);
    unauthorized.user = "10".into();
    let mut room = event(&text);
    room.room = "99".into();
    let mut thread = event(&text);
    thread.thread = "42".into();
    let mut missing_id = event(&text);
    missing_id.id.clear();
    for wrong in [
        sender,
        unauthorized,
        room,
        thread,
        missing_id,
        event("/approve fabricated"),
        event(&format!("{text} extra")),
    ] {
        tokio::time::timeout(
            Duration::from_secs(1),
            fixture.bridge.handle(wrong, &fixture.sink),
        )
        .await
        .unwrap()
        .unwrap();
        assert!(fixture.replies().is_empty());
        assert!(!task.is_finished());
    }
    fixture
        .bridge
        .handle(event(&text), &fixture.sink)
        .await
        .unwrap();
    fixture.finish(task).await;
    assert_eq!(fixture.replies().len(), 1);
    assert_eq!(
        fixture.replies()[0]["result"]["outcome"],
        json!({"outcome":"selected","optionId":"native-deny"})
    );
}

#[tokio::test]
async fn telegram_permission_opt_in_requires_exact_true() {
    for opt_in in [Value::Null, json!(false), json!("true"), json!(1)] {
        let fixture = Fixture::new(opt_in);
        fixture.finish(fixture.start()).await;
        assert_eq!(
            fixture.replies()[0]["result"]["outcome"],
            json!({"outcome":"cancelled"})
        );
        assert!(
            !fixture
                .sink
                .0
                .lock()
                .await
                .iter()
                .any(|v| v["text"].as_str().is_some_and(|s| s.contains("/approve ")))
        );
    }
}

#[tokio::test]
async fn telegram_cancel_invalidates_pending_permission_and_late_approval() {
    let fixture = Fixture::new(json!(true));
    let task = fixture.start();
    let notice = fixture.notice().await;
    fixture
        .bridge
        .handle(event("/cancel"), &fixture.sink)
        .await
        .unwrap();
    fixture.finish(task).await;
    fixture
        .bridge
        .handle(
            event(&format!("/approve {}", request_id(&notice))),
            &fixture.sink,
        )
        .await
        .unwrap();
    assert_eq!(fixture.replies().len(), 1);
    assert_eq!(
        fixture.replies()[0]["result"]["outcome"],
        json!({"outcome":"cancelled"})
    );
    assert_eq!(
        fixture
            .audit()
            .iter()
            .filter(|v| v["method"] == "session/prompt")
            .count(),
        1
    );
}

#[tokio::test]
async fn telegram_expired_permission_rejects_late_approval() {
    let fixture = Fixture::new(json!(true));
    let task = fixture.start();
    let notice = fixture.notice().await;
    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(2700)).await;
    for _ in 0..20 {
        tokio::task::yield_now().await;
    }
    tokio::time::resume();
    fixture.finish(task).await;
    fixture
        .bridge
        .handle(
            event(&format!("/approve {}", request_id(&notice))),
            &fixture.sink,
        )
        .await
        .unwrap();
    assert_eq!(fixture.replies().len(), 1);
    assert_eq!(
        fixture.replies()[0]["result"]["outcome"],
        json!({"outcome":"cancelled"})
    );
}

#[tokio::test]
async fn telegram_approval_is_not_lost_to_the_prompt_rate_limit() {
    let mut fixture = Fixture::new(json!(true));
    Arc::get_mut(&mut fixture.bridge).unwrap().config["rateLimitSeconds"] = json!(60);
    let task = fixture.start();
    let notice = fixture.notice().await;
    let platform = Platform::new("telegram", "http://127.0.0.1", "fixture", "0", "bridge");
    let update = json!({"message_id":9001,"chat":{"id":7,"type":"private"},"from":{"id":8},"text":format!("/approve {}",request_id(&notice))});
    let reply = platform
        .prepare(&fixture.bridge, &update)
        .await
        .unwrap()
        .expect("an approval must not be dropped during prompt cooldown");
    fixture.bridge.handle(reply, &fixture.sink).await.unwrap();
    fixture.finish(task).await;
    assert_eq!(
        fixture.replies()[0]["result"]["outcome"],
        json!({"outcome":"selected","optionId":"native-once"})
    );
}

#[tokio::test]
async fn telegram_group_approval_is_admitted_without_a_trigger_or_mention() {
    let fixture = Fixture::new(json!(true));
    let mut origin = event("!oc work");
    origin.direct = false;
    origin.thread = "42".into();
    let bridge = fixture.bridge.clone();
    let sink = fixture.sink.clone();
    let task = tokio::spawn(async move { bridge.handle(origin, &sink).await });
    let notice = fixture.notice().await;
    let platform = Platform::new("telegram", "http://127.0.0.1", "fixture", "0", "bridge");
    let update = json!({"message_id":9000,"chat":{"id":7,"type":"supergroup"},"message_thread_id":42,"is_topic_message":true,"from":{"id":8},"text":format!("/approve {}",request_id(&notice))});
    let reply = platform
        .prepare(&fixture.bridge, &update)
        .await
        .unwrap()
        .expect("approval controls must pass transport addressing");
    tokio::time::timeout(
        Duration::from_secs(1),
        fixture.bridge.handle(reply, &fixture.sink),
    )
    .await
    .unwrap()
    .unwrap();
    fixture.finish(task).await;
    assert_eq!(
        fixture.replies()[0]["result"]["outcome"],
        json!({"outcome":"selected","optionId":"native-once"})
    );
}

#[tokio::test]
async fn telegram_approval_cannot_be_granted_from_another_platform() {
    let fixture = Fixture::new(json!(true));
    let task = fixture.start();
    let notice = fixture.notice().await;
    let mut wrong = event(&format!("/approve {}", request_id(&notice)));
    wrong.platform = "matrix".into();
    fixture.bridge.handle(wrong, &fixture.sink).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(
        fixture.replies().is_empty(),
        "Matrix must not grant a Telegram operation even when numeric identities match"
    );
    assert!(!task.is_finished());
    fixture
        .bridge
        .handle(
            event(&format!("/deny {}", request_id(&notice))),
            &fixture.sink,
        )
        .await
        .unwrap();
    fixture.finish(task).await;
    assert_eq!(
        fixture.replies()[0]["result"]["outcome"],
        json!({"outcome":"selected","optionId":"native-deny"})
    );
}

#[tokio::test]
async fn telegram_approval_reaches_acp_once_without_starting_another_prompt() {
    let fixture = Fixture::new(json!(true));
    let task = fixture.start();
    let notice = fixture.notice().await;
    let decision = event(&format!("/approve {}", request_id(&notice)));
    tokio::time::timeout(
        Duration::from_secs(1),
        fixture.bridge.handle(decision.clone(), &fixture.sink),
    )
    .await
    .unwrap()
    .unwrap();
    fixture
        .bridge
        .handle(decision, &fixture.sink)
        .await
        .unwrap();
    fixture.finish(task).await;
    assert_eq!(fixture.replies().len(), 1);
    assert_eq!(
        fixture.replies()[0]["result"]["outcome"],
        json!({"outcome":"selected","optionId":"native-once"})
    );
    assert_eq!(
        fixture
            .audit()
            .iter()
            .filter(|v| v["method"] == "session/prompt")
            .count(),
        1
    );
}
