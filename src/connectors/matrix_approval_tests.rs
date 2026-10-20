//! Native outbound HTTP acknowledgment composed with real Matrix ingress and ACP.
use super::*;
use axum::{Json, Router, extract::Request, response::IntoResponse, routing::any};
use std::collections::BTreeMap;

#[derive(Clone, Default)]
struct Controls;
#[async_trait]
impl Sink for Controls {
    fn matrix_raw_thread(&self) -> Option<String> {
        Some("$root".into())
    }
    async fn emit(&self, _: Value) -> Result<()> {
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
fn replies(path: &std::path::Path) -> Vec<Value> {
    std::fs::read_to_string(path)
        .unwrap_or_default()
        .lines()
        .filter_map(|line| serde_json::from_str::<Value>(line).ok())
        .filter(|v| v["id"] == 900 && v.get("method").is_none())
        .collect()
}

async fn native_approval_case(fail: bool, cancel: bool) {
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
    let configuration = crate::config::load(&json!({"rateLimitSeconds":0,"sessionStorePath":directory.path().join("store.json"),
        "toolMessages":{"mode":"off"},
        "acp":{"command":"/usr/bin/python3","args":[concat!(env!("CARGO_MANIFEST_DIR"),"/tests/fixtures/matrix-approval-peer.py")],"env":{"APPROVAL_AUDIT":audit}},
        "matrix":{"homeserver":base,"userId":"@bot:local","accessToken":"fixture","encryption":{"enabled":false},"respondToAllMessages":true,"interactivePermissions":true,"allowedUsers":["@human:local"],"allowedRooms":["!room:local"],"strictAllowedRooms":true}
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
        thread: Some("$root".try_into().unwrap()),
        raw_thread: Some("$root".try_into().unwrap()),
        html: false,
        rich_tools: false,
        activities: Default::default(),
        indicators: TypingManager::new(Arc::new(NativeTyping(client)), Duration::from_secs(4)),
        typing: Default::default(),
        incoming: "$incoming".try_into().unwrap(),
        reply_context: Default::default(),
    });
    let task = {
        let bridge = bridge.clone();
        let sink = sink.clone();
        tokio::spawn(async move { bridge.handle(event("work".into()), sink.as_ref()).await })
    };
    tokio::time::timeout(Duration::from_secs(5), entered.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
    let notice = notices.lock().await[0].clone();
    assert_eq!(notice["msgtype"], "m.notice");
    assert_eq!(notice["m.relates_to"]["event_id"], "$root");
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
    bridge
        .handle(event(approve.clone()), &Controls)
        .await
        .unwrap();
    assert!(
        replies(&audit).is_empty(),
        "native HTTP arrival is not acknowledgment"
    );
    assert!(!task.is_finished());
    if cancel {
        bridge
            .handle(event("/cancel".into()), &Controls)
            .await
            .unwrap();
    }
    release.add_permits(1);
    if !fail && !cancel {
        // Each attempted input has a fresh event ID; ACK-to-broker scheduling is asynchronous.
        tokio::time::timeout(Duration::from_secs(5), async {
            while replies(&audit).is_empty() {
                bridge
                    .handle(event(approve.clone()), &Controls)
                    .await
                    .unwrap();
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }
    tokio::time::timeout(Duration::from_secs(5), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    bridge.handle(event(approve), &Controls).await.unwrap();
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
    native_approval_case(false, false).await;
}
#[tokio::test]
async fn native_notice_http_failure_cancels_without_arming() {
    native_approval_case(true, false).await;
}
#[tokio::test]
async fn native_notice_cancellation_before_http_ack_rejects_late_grant() {
    native_approval_case(false, true).await;
}
