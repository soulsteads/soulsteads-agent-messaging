//! Native outbound HTTP acknowledgment composed with real Matrix ingress and ACP.
use super::*;
use axum::{Json, Router, extract::Request, response::IntoResponse, routing::any};
use std::{
    collections::BTreeMap,
    sync::atomic::{AtomicBool, Ordering},
};

struct DecisionSink {
    raw_thread: &'static str,
    recorded: AtomicBool,
}
#[async_trait]
impl Sink for DecisionSink {
    fn matrix_raw_thread(&self) -> Option<String> {
        Some(self.raw_thread.into())
    }
    async fn emit(&self, event: Value) -> Result<()> {
        if event["text"] == "Approval decision recorded." {
            self.recorded.store(true, Ordering::SeqCst);
        }
        Ok(())
    }
}
fn event(text: String) -> Event {
    Event {
        platform: "matrix".into(),
        room: "!room:local".into(),
        thread: "$root".into(),
        id: uuid::Uuid::new_v4().to_string(),
        user: "@human:local".into(),
        text,
        direct: false,
        mentioned: false,
        bot: false,
        images: vec![],
        reply_to: None,
    }
}
fn decision_event(text: String, raw_thread: &str) -> Event {
    let mut event = event(text);
    event.thread = if raw_thread.is_empty() {
        event.id.clone()
    } else {
        raw_thread.to_owned()
    };
    event
}
fn replies(path: &std::path::Path) -> Vec<Value> {
    std::fs::read_to_string(path)
        .unwrap_or_default()
        .lines()
        .filter_map(|line| serde_json::from_str::<Value>(line).ok())
        .filter(|v| v["id"] == 900 && v.get("method").is_none())
        .collect()
}

#[derive(Clone, Copy)]
enum ThreadMode {
    Explicit,
    Synthesized,
    ExplicitWithSharedSession,
    Room,
}

async fn native_approval_case(fail: bool, cancel: bool, mode: ThreadMode) {
    let notices = Arc::new(tokio::sync::Mutex::new(Vec::<Value>::new()));
    let entered = Arc::new(tokio::sync::Semaphore::new(0));
    let release = Arc::new(tokio::sync::Semaphore::new(0));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let router = Router::new().fallback(any({
        let notices = notices.clone(); let entered = entered.clone(); let release = release.clone();
        move |request: Request| {
            let notices = notices.clone(); let entered = entered.clone(); let release = release.clone();
            async move {
                let path = request.uri().path().to_owned();
                if path.trim_end_matches('/').ends_with("/state/m.room.encryption") {
                    return (axum::http::StatusCode::NOT_FOUND, Json(json!({"errcode":"M_NOT_FOUND","error":"not encrypted"}))).into_response();
                }
                let result = if path.ends_with("versions") {
                    json!({"versions":["v1.12"],"unstable_features":{}})
                } else if path.ends_with("whoami") {
                    json!({"user_id":"@bot:local","device_id":"LOCAL"})
                } else if path.ends_with("sync") {
                    json!({"next_batch":"one","rooms":{"join":{"!room:local":{"state":{"events":[
                        {"type":"m.room.create","event_id":"$create","origin_server_ts":1,"state_key":"","sender":"@bot:local","content":{"creator":"@bot:local","room_version":"10"}},
                        {"type":"m.room.member","event_id":"$member","origin_server_ts":1,"state_key":"@bot:local","sender":"@bot:local","content":{"membership":"join"}}
                    ]},"timeline":{"events":[],"limited":false},"ephemeral":{"events":[]},"account_data":{"events":[]}}}},"device_lists":{"changed":[],"left":[]},"device_one_time_keys_count":{}})
                } else if path.contains("/send/") {
                    let bytes = axum::body::to_bytes(request.into_body(), 1024*1024).await.unwrap();
                    let content: Value = serde_json::from_slice(&bytes).unwrap();
                    if content["body"].as_str().is_some_and(|b| b.contains("/approve ")) {
                        notices.lock().await.push(content);
                        entered.add_permits(1);
                        release.acquire().await.unwrap().forget();
                        if fail {
                            return (axum::http::StatusCode::BAD_REQUEST, Json(json!({"errcode":"M_BAD_JSON","error":"fixture publication failure"}))).into_response();
                        }
                    }
                    json!({"event_id":"$ack"})
                } else { json!({}) };
                Json(result).into_response()
            }
        }
    }));
    let server = tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    let directory = tempfile::tempdir().unwrap();
    let audit = directory.path().join("rpc.jsonl");
    let isolation = matches!(mode, ThreadMode::Explicit | ThreadMode::Synthesized);
    let raw = matches!(
        mode,
        ThreadMode::Explicit | ThreadMode::ExplicitWithSharedSession
    );
    let room_level = matches!(mode, ThreadMode::Room);
    let configuration = crate::config::load(&json!({"rateLimitSeconds":0,"sessionStorePath":directory.path().join("store.json"),
        "toolMessages":{"mode":"off"},
        "acp":{"command":"/usr/bin/python3","args":[concat!(env!("CARGO_MANIFEST_DIR"),"/tests/fixtures/matrix-approval-peer.py")],"env":{"APPROVAL_AUDIT":audit}},
        "matrix":{"homeserver":base,"userId":"@bot:local","accessToken":"fixture","encryption":{"enabled":false},"respondToAllMessages":true,"interactivePermissions":true,"threadIsolation":isolation,"allowedUsers":["@human:local"],"allowedRooms":["!room:local"],"strictAllowedRooms":true}
    }).to_string(), &BTreeMap::new()).unwrap();
    let bridge = Arc::new(
        Bridge::new(
            configuration.clone(),
            BTreeMap::new(),
            directory.path().join("sessions"),
        )
        .unwrap(),
    );
    let client = connect(&configuration["matrix"], &directory.path().join("matrix"))
        .await
        .unwrap();
    client.sync_once(SyncSettings::default()).await.unwrap();
    let room = client.get_room("!room:local".try_into().unwrap()).unwrap();
    let sink = Arc::new(MatrixSink {
        room,
        thread: isolation.then(|| "$root".try_into().unwrap()),
        raw_thread: raw.then(|| "$root".try_into().unwrap()),
        html: false,
        rich_tools: false,
        activities: Default::default(),
        indicators: TypingManager::new(Arc::new(NativeTyping(client)), Duration::from_secs(4)),
        typing: Default::default(),
        incoming: if raw { "$incoming" } else { "$root" }.try_into().unwrap(),
        reply_context: Default::default(),
    });
    let task = {
        let bridge = bridge.clone();
        let sink = sink.clone();
        tokio::spawn(async move {
            let mut work = event("work".into());
            work.id = sink.incoming.to_string();
            bridge.handle(work, sink.as_ref()).await
        })
    };
    tokio::time::timeout(Duration::from_secs(5), entered.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
    let notice = notices.lock().await[0].clone();
    assert_eq!(notice["msgtype"], "m.notice");
    if room_level {
        assert!(notice.get("m.relates_to").is_none());
    } else {
        assert_eq!(notice["m.relates_to"]["rel_type"], "m.thread");
        assert_eq!(notice["m.relates_to"]["event_id"], "$root");
    }
    let id = notice["body"]
        .as_str()
        .unwrap()
        .split("/approve ")
        .nth(1)
        .unwrap()
        .split_whitespace()
        .next()
        .unwrap();
    let approve = format!("/approve {id}");
    let controls = DecisionSink {
        raw_thread: if room_level { "" } else { "$root" },
        recorded: AtomicBool::new(false),
    };
    bridge
        .handle(
            decision_event(approve.clone(), controls.raw_thread),
            &controls,
        )
        .await
        .unwrap();
    assert!(!controls.recorded.load(Ordering::SeqCst));
    assert!(
        replies(&audit).is_empty(),
        "native HTTP arrival is not acknowledgment"
    );
    assert!(!task.is_finished());
    if cancel {
        bridge
            .handle(
                decision_event("/cancel".into(), controls.raw_thread),
                &controls,
            )
            .await
            .unwrap();
    }
    release.add_permits(1);
    if !fail && !cancel {
        // Continue testing wrong scopes after the HTTP response as well as
        // checking the correct scope before acknowledgment. A shared native
        // session must not make unrelated Matrix threads interchangeable.
        let wrong_root = DecisionSink {
            raw_thread: "$elsewhere",
            recorded: AtomicBool::new(false),
        };
        let wrong_level = DecisionSink {
            raw_thread: if room_level { "$root" } else { "" },
            recorded: AtomicBool::new(false),
        };
        // Each attempted input has a fresh event ID; ACK-to-broker scheduling is asynchronous.
        tokio::time::timeout(Duration::from_secs(5), async {
            while replies(&audit).is_empty() {
                for wrong in [&wrong_root, &wrong_level] {
                    bridge
                        .handle(decision_event(approve.clone(), wrong.raw_thread), wrong)
                        .await
                        .unwrap();
                    assert!(!wrong.recorded.load(Ordering::SeqCst));
                }
                bridge
                    .handle(
                        decision_event(approve.clone(), controls.raw_thread),
                        &controls,
                    )
                    .await
                    .unwrap();
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("approval from the exact published destination did not reach ACP");
        assert!(controls.recorded.load(Ordering::SeqCst));
    }
    tokio::time::timeout(Duration::from_secs(5), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    bridge
        .handle(decision_event(approve, controls.raw_thread), &controls)
        .await
        .unwrap();
    let decisions = replies(&audit);
    assert_eq!(decisions.len(), 1, "one grant/denial, no late replay");
    assert_eq!(
        decisions[0]["result"]["outcome"],
        if fail || cancel {
            json!({"outcome":"cancelled"})
        } else {
            json!({"outcome":"selected","optionId":"native-once"})
        }
    );
    let calls = std::fs::read_to_string(&audit).unwrap();
    assert_eq!(
        calls
            .lines()
            .filter(
                |line| serde_json::from_str::<Value>(line).unwrap()["method"] == "session/prompt"
            )
            .count(),
        1
    );
    bridge.shutdown().await.unwrap();
    server.abort();
}
#[tokio::test]
async fn native_notice_http_ack_arms_scoped_approval_once() {
    native_approval_case(false, false, ThreadMode::Explicit).await;
}
#[tokio::test]
async fn native_notice_http_failure_cancels_without_arming() {
    native_approval_case(true, false, ThreadMode::Explicit).await;
}
#[tokio::test]
async fn native_notice_cancellation_before_http_ack_rejects_late_grant() {
    native_approval_case(false, true, ThreadMode::Explicit).await;
}

#[tokio::test]
async fn native_top_level_notice_accepts_approval_in_displayed_thread() {
    native_approval_case(false, false, ThreadMode::Synthesized).await;
}

#[tokio::test]
async fn native_approval_uses_explicit_thread_even_with_shared_session() {
    native_approval_case(false, false, ThreadMode::ExplicitWithSharedSession).await;
}

#[tokio::test]
async fn native_room_level_notice_keeps_room_level_approval_with_shared_session() {
    native_approval_case(false, false, ThreadMode::Room).await;
}
