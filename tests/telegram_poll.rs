use axum::{
    Json, Router,
    extract::{Request, State},
    routing::any,
};
use soulsteads_messaging::{config, connectors::http::Platform, runtime::Bridge};
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    future::IntoFuture,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::sync::{Mutex, watch};

#[derive(Clone, Default)]
struct Fixture {
    requests: Arc<Mutex<Vec<(String, Value)>>>,
    polls: Arc<AtomicUsize>,
}
async fn endpoint(State(state): State<Fixture>, request: Request) -> Json<Value> {
    let path = request.uri().path().to_owned();
    let bytes = axum::body::to_bytes(request.into_body(), 1024 * 1024)
        .await
        .unwrap();
    let body: Value = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
    state
        .requests
        .lock()
        .await
        .push((path.clone(), body.clone()));
    let result = if path.ends_with("getMe") {
        json!({"id":99,"username":"bridge"})
    } else if path.ends_with("getUpdates") {
        match state.polls.fetch_add(1, Ordering::SeqCst) {
            0 => {
                json!([{"update_id":9,"message":{"message_id":9,"chat":{"id":123,"type":"private"},"from":{"id":7},"text":"backlog"}}])
            }
            1 => json!([
                {"update_id":44,"message":{"message_id":44,"chat":{"id":123,"type":"private"},"from":{"id":7},"document":{"file_id":"slow","file_name":"file.txt"}}},
                {"update_id":42,"message":{"message_id":42,"chat":{"id":456,"type":"private"},"from":{"id":7},"text":"hello"}}
            ]),
            _ => {
                tokio::time::sleep(Duration::from_secs(30)).await;
                json!([])
            }
        }
    } else if path.ends_with("getFile") {
        tokio::time::sleep(Duration::from_secs(30)).await;
        json!({"file_path":"document.txt"})
    } else {
        json!({"message_id":17})
    };
    Json(json!({"ok":true,"result":result}))
}
#[tokio::test]
async fn telegram_poll_drops_backlog_advances_monotonically_and_cancels_during_slow_attachment() {
    let fixture = Fixture::default();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(
        axum::serve(
            listener,
            Router::new()
                .fallback(any(endpoint))
                .with_state(fixture.clone()),
        )
        .into_future(),
    );
    let directory = tempfile::tempdir().unwrap();
    let configuration = config::load(&json!({"rateLimitSeconds":0,"sessionStorePath":directory.path().join("store.json"),"acp":{"command":env!("CARGO_BIN_EXE_acp-fixture"),"args":[]}}).to_string(),&BTreeMap::new()).unwrap();
    let bridge = Arc::new(
        Bridge::new(
            configuration,
            BTreeMap::from([("TELEGRAM_DROP_PENDING".into(), "1".into())]),
            directory.path().join("sessions"),
        )
        .unwrap(),
    );
    let (shutdown, receiver) = watch::channel(false);
    let running = tokio::spawn(
        Platform::new("telegram", &base, "fixture-token", "", "").run(bridge.clone(), receiver),
    );
    tokio::time::sleep(Duration::from_secs(2)).await;
    shutdown.send(true).unwrap();
    let completed = tokio::time::timeout(Duration::from_secs(1), running).await;
    let requests = fixture.requests.lock().await;
    assert!(requests.iter().any(
        |(path, body)| path.ends_with("deleteWebhook") && body["drop_pending_updates"] == true
    ));
    let polls: Vec<_> = requests
        .iter()
        .filter(|(path, _)| path.ends_with("getUpdates"))
        .map(|(_, body)| body)
        .collect();
    assert_eq!(polls[0]["timeout"], 0);
    assert_eq!(polls[1]["offset"], 10);
    assert_eq!(polls[2]["offset"], 45);
    let outputs: Vec<_> = requests
        .iter()
        .filter(|(path, _)| path.ends_with("sendMessage"))
        .map(|(_, body)| body)
        .collect();
    assert!(
        outputs
            .iter()
            .any(|body| body["chat_id"] == "456" && body["text"] == "hello world")
    );
    assert!(!outputs.iter().any(|body| {
        body["text"]
            .as_str()
            .is_some_and(|text| text.contains("backlog"))
    }));
    assert!(completed.unwrap().unwrap().is_ok());
    drop(requests);
    bridge.shutdown().await.unwrap();
    server.abort();
}

#[derive(Clone, Default)]
struct SaturatedFixture {
    requests: Arc<Mutex<Vec<(String, Value)>>>,
    polls: Arc<AtomicUsize>,
    approval: Arc<Mutex<String>>,
    control_on_next_poll: bool,
    overload_user: i64,
}

async fn saturated_endpoint(
    State(state): State<SaturatedFixture>,
    request: Request,
) -> Json<Value> {
    let path = request.uri().path().to_owned();
    let bytes = axum::body::to_bytes(request.into_body(), 1024 * 1024)
        .await
        .unwrap();
    let body: Value = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
    state
        .requests
        .lock()
        .await
        .push((path.clone(), body.clone()));
    let result = if path.ends_with("getMe") {
        json!({"id":99,"username":"bridge"})
    } else if path.ends_with("getUpdates") {
        let poll = state.polls.fetch_add(1, Ordering::SeqCst);
        if poll == 0 {
            let mut updates: Vec<Value> = (1..=65)
                .map(|id| {
                    let user = if id == 65 { state.overload_user } else { 8 };
                    json!({"update_id":id,"message":{"message_id":id + 100,"chat":{"id":7,"type":"private"},"from":{"id":user},"document":{"file_id":format!("slow-{id}"),"file_name":"file.txt"}}})
                })
                .collect();
            if !state.control_on_next_poll {
                let approval = state.approval.lock().await.clone();
                updates.push(json!({"update_id":66,"message":{"message_id":166,"chat":{"id":7,"type":"private"},"from":{"id":8},"text":format!("/approve {approval}"),"document":{"file_id":"slow-control","file_name":"control.txt"}}}));
            }
            Value::Array(updates)
        } else if poll == 1 && state.control_on_next_poll {
            let approval = state.approval.lock().await.clone();
            json!([{"update_id":66,"message":{"message_id":166,"chat":{"id":7,"type":"private"},"from":{"id":8},"text":format!("/approve {approval}"),"document":{"file_id":"slow-control","file_name":"control.txt"}}}])
        } else {
            tokio::time::sleep(Duration::from_secs(30)).await;
            json!([])
        }
    } else if path.ends_with("getFile") {
        tokio::time::sleep(Duration::from_secs(30)).await;
        json!({"file_path":"document.txt"})
    } else {
        json!({"message_id":17})
    };
    Json(json!({"ok":true,"result":result}))
}

async fn wait_for_telegram_notice(fixture: &SaturatedFixture) -> String {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let Some(text) = fixture
                .requests
                .lock()
                .await
                .iter()
                .filter(|(path, _)| path.ends_with("sendMessage"))
                .filter_map(|(_, body)| body["text"].as_str())
                .find(|text| text.contains("/approve "))
            {
                return text.to_owned();
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("Telegram approval notice")
}

async fn run_saturated_poll(control_on_next_poll: bool, overload_user: i64) {
    let fixture = SaturatedFixture {
        control_on_next_poll,
        overload_user,
        ..Default::default()
    };
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(
        axum::serve(
            listener,
            Router::new()
                .fallback(any(saturated_endpoint))
                .with_state(fixture.clone()),
        )
        .into_future(),
    );
    let directory = tempfile::tempdir().unwrap();
    let audit = directory.path().join("approval.jsonl");
    let configuration = config::load(&json!({
        "rateLimitSeconds":0,
        "sessionStorePath":directory.path().join("store.json"),
        "acp":{"command":"/usr/bin/python3","args":[concat!(env!("CARGO_MANIFEST_DIR"),"/tests/fixtures/matrix-approval-peer.py")],"env":{"APPROVAL_AUDIT":audit}},
        "telegram":{"interactivePermissions":true,"allowedUsers":["8"],"attachments":{"enabled":true}}
    }).to_string(), &BTreeMap::new()).unwrap();
    let bridge = Arc::new(
        Bridge::new(
            configuration,
            BTreeMap::new(),
            directory.path().join("sessions"),
        )
        .unwrap(),
    );
    let source = Platform::new("telegram", &base, "fixture-token", "99", "bridge");
    let origin = source
        .prepare(
            &bridge,
            &json!({"message_id":1,"chat":{"id":7,"type":"private"},"from":{"id":8},"text":"work"}),
        )
        .await
        .unwrap()
        .unwrap();
    let origin_task = tokio::spawn({
        let bridge = bridge.clone();
        let source = source.clone();
        async move {
            bridge
                .handle(
                    origin.clone(),
                    &soulsteads_messaging::connectors::http::PlatformSink::new(source, origin),
                )
                .await
        }
    });
    let notice = wait_for_telegram_notice(&fixture).await;
    *fixture.approval.lock().await = notice
        .split("/approve ")
        .nth(1)
        .unwrap()
        .split_whitespace()
        .next()
        .unwrap()
        .to_owned();
    let (shutdown, receiver) = watch::channel(false);
    let running = tokio::spawn(
        Platform::new("telegram", &base, "fixture-token", "", "").run(bridge.clone(), receiver),
    );
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let held = fixture
                .requests
                .lock()
                .await
                .iter()
                .filter(|(path, _)| path.ends_with("getFile"))
                .count();
            if held == 64 {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("all 64 ordinary permits must be occupied by held downloads");
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let decided = std::fs::read_to_string(&audit)
                .unwrap_or_default()
                .lines()
                .filter_map(|line| serde_json::from_str::<Value>(line).ok())
                .any(|message| message["id"] == 900 && message.get("method").is_none());
            if decided {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the approval must be delivered despite 64 blocked polling tasks");
    let native_turns = std::fs::read_to_string(&audit)
        .unwrap_or_default()
        .lines()
        .filter_map(|line| serde_json::from_str::<Value>(line).ok())
        .filter(|message| message["method"] == "session/prompt")
        .count();
    assert_eq!(
        native_turns, 1,
        "refused normal work must not start a native turn"
    );
    let requests = fixture.requests.lock().await;
    let downloaded: Vec<_> = requests
        .iter()
        .filter(|(path, _)| path.ends_with("getFile"))
        .filter_map(|(_, body)| body["file_id"].as_str())
        .collect();
    assert_eq!(downloaded.len(), 64, "all ordinary permits must be held");
    assert!(downloaded.iter().all(|id| *id != "slow-65"));
    assert!(
        downloaded.iter().all(|id| *id != "slow-control"),
        "literal controls must reach the broker without attachment I/O"
    );
    let busy: Vec<_> = requests
        .iter()
        .filter(|(path, _)| path.ends_with("sendMessage"))
        .filter_map(|(_, body)| body["text"].as_str())
        .filter(|text| text.contains("not accepted or run"))
        .collect();
    if overload_user == 8 {
        assert_eq!(
            busy.len(),
            1,
            "authorized overloaded work is explicitly refused"
        );
    } else {
        assert!(
            busy.is_empty(),
            "unauthorized overload must not be notified"
        );
    }
    drop(requests);
    shutdown.send(true).unwrap();
    assert!(
        tokio::time::timeout(Duration::from_secs(1), running)
            .await
            .unwrap()
            .unwrap()
            .is_ok()
    );
    assert!(
        tokio::time::timeout(Duration::from_secs(1), origin_task)
            .await
            .unwrap()
            .unwrap()
            .is_ok()
    );
    bridge.shutdown().await.unwrap();
    server.abort();
}

#[tokio::test]
async fn telegram_poll_refuses_authorized_overload_before_a_same_batch_approval() {
    run_saturated_poll(false, 8).await;
}

#[tokio::test]
async fn telegram_poll_refuses_authorized_overload_before_a_next_poll_approval() {
    run_saturated_poll(true, 8).await;
}

#[tokio::test]
async fn telegram_poll_does_not_notify_or_download_for_unauthorized_overload() {
    run_saturated_poll(false, 9).await;
}
