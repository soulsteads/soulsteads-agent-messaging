use axum::{
    Json, Router,
    extract::{
        Request, State, WebSocketUpgrade,
        ws::{Message, WebSocket},
    },
    response::{IntoResponse, Response},
    routing::{any, get},
};
use soulsteads_messaging::{config, connectors::http::Platform, runtime::Bridge};
use futures_util::{SinkExt, StreamExt};
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

#[derive(Clone)]
struct Fixture {
    kind: String,
    admission_probe: bool,
    base: String,
    connections: Arc<AtomicUsize>,
    inputs: Arc<Mutex<Vec<Value>>>,
    outputs: Arc<Mutex<Vec<Value>>>,
}
async fn http(State(state): State<Fixture>, request: Request) -> Json<Value> {
    let path = request.uri().path().to_owned();
    let bytes = axum::body::to_bytes(request.into_body(), 1024 * 1024)
        .await
        .unwrap();
    let body: Value = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
    if path.contains("posts") || path.contains("postMessage") || path.contains("/messages") {
        state.outputs.lock().await.push(body);
    }
    Json(if path.ends_with("gateway/bot") {
        json!({"url":state.base.replace("http:","ws:")+"/socket"})
    } else if path.ends_with("apps.connections.open") {
        json!({"ok":true,"url":state.base.replace("http:","ws:")+"/socket"})
    } else if path.ends_with("auth.test") {
        json!({"ok":true,"user_id":"BOT"})
    } else {
        json!({"id":"BOT","username":"bridge","ok":true,"ts":"42"})
    })
}
async fn upgrade(State(state): State<Fixture>, socket: WebSocketUpgrade) -> Response {
    socket
        .on_upgrade(move |socket| conversation(socket, state))
        .into_response()
}
async fn conversation(mut socket: WebSocket, state: Fixture) {
    let connection = state.connections.fetch_add(1, Ordering::SeqCst);
    if state.kind == "discord" {
        tokio::time::sleep(Duration::from_millis(80)).await;
        socket
            .send(Message::Text(
                json!({"op":10,"d":{"heartbeat_interval":500}})
                    .to_string()
                    .into(),
            ))
            .await
            .unwrap();
    }
    if state.kind != "slack" {
        let first = socket.next().await.unwrap().unwrap();
        state
            .inputs
            .lock()
            .await
            .push(serde_json::from_str(first.to_text().unwrap()).unwrap());
    }
    if state.kind == "discord" {
        socket
            .send(Message::Text(
                json!({"op":0,"t":"READY","s":7,"d":{"session_id":"resume-fixture"}})
                    .to_string()
                    .into(),
            ))
            .await
            .unwrap();
    }
    if state.admission_probe {
        socket.send(Message::Text(json!({"event":"posted","data":{"post":json!({"id":"denied", "channel_id":"DENIED", "user_id":"human", "message":"!oc must not run", "root_id":"DENIED_ROOT"}).to_string(),"channel_type":"O"}}).to_string().into())).await.unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let event = match state.kind.as_str() {
        "mattermost" => {
            json!({"event":"posted","data":{"post":json!({"id":format!("m-{connection}"),"channel_id":"ROOM","user_id":"human","message":"!oc hello","root_id":"ROOT"}).to_string(),"channel_type":"O"}})
        }
        "slack" => {
            json!({"envelope_id":format!("e-{connection}"),"payload":{"team_id":"TEAM","event":{"type":"message","channel":"ROOM","user":"human","text":"!oc hello","ts":format!("{connection}.1"),"thread_ts":"ROOT"}}})
        }
        _ => {
            json!({"op":0,"t":"MESSAGE_CREATE","s":8,"d":{"id":format!("m-{connection}"),"channel_id":"ROOM","author":{"id":"human"},"content":"!oc hello","guild_id":"GUILD"}})
        }
    };
    if state.kind == "mattermost" {
        let denied = json!({"event":"posted","data":{"post":json!({"id":"denied","channel_id":"UNLISTED","user_id":"human","message":"!oc rejected"}).to_string(),"channel_type":"O"}});
        socket
            .send(Message::Text(denied.to_string().into()))
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(80)).await;
    }
    socket
        .send(Message::Text(event.to_string().into()))
        .await
        .unwrap();
    if state.kind == "slack" {
        let ack = socket.next().await.unwrap().unwrap();
        state
            .inputs
            .lock()
            .await
            .push(serde_json::from_str(ack.to_text().unwrap()).unwrap());
    }
    let mut deadline = tokio::time::interval(Duration::from_millis(20));
    loop {
        tokio::select! {
            _=deadline.tick()=>{if state.outputs.lock().await.len()>connection{break;}},
            message=socket.next()=>{
                let Some(Ok(message))=message else{return;};
                if let Message::Text(text)=message{let value:Value=serde_json::from_str(&text).unwrap();state.inputs.lock().await.push(value.clone());if value["op"]==1{let _=socket.send(Message::Text(json!({"op":11}).to_string().into())).await;}}
            }
        }
    }
    if connection == 0 && !state.admission_probe {
        let _ = socket.close().await;
    } else {
        while socket.next().await.is_some() {}
    }
}

#[tokio::test]
async fn gateways_auth_ack_reconnect_resume_dispatch_acp_and_shutdown() {
    for kind in ["mattermost", "slack", "discord"] {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let state = Fixture {
            kind: kind.into(),
            admission_probe: false,
            base: base.clone(),
            connections: Arc::new(AtomicUsize::new(0)),
            inputs: Arc::default(),
            outputs: Arc::default(),
        };
        let router = Router::new()
            .route("/socket", get(upgrade))
            .route("/api/v4/websocket", get(upgrade))
            .fallback(any(http))
            .with_state(state.clone());
        let server = tokio::spawn(axum::serve(listener, router).into_future());
        let directory = tempfile::tempdir().unwrap();
        let config=config::load(&json!({"rateLimitSeconds":0,"mattermost":{"allowedChannels":["ROOM"]},"sessionStorePath":directory.path().join("store.json"),"acp":{"command":env!("CARGO_BIN_EXE_acp-fixture"),"args":[]}}).to_string(),&BTreeMap::new()).unwrap();
        let bridge = Arc::new(
            Bridge::new(
                config,
                BTreeMap::from([("SLACK_APP_TOKEN".into(), "fixture-app-token".into())]),
                directory.path().join("sessions"),
            )
            .unwrap(),
        );
        let (sender, receiver) = watch::channel(false);
        let platform = Platform::new(kind, &base, "fixture-token", "", "");
        let runtime_bridge = bridge.clone();
        let task = tokio::spawn(async move { platform.run(runtime_bridge, receiver).await });
        tokio::time::timeout(Duration::from_secs(12), async {
            while state.outputs.lock().await.len() < 2 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        sender.send(true).unwrap();
        tokio::time::timeout(Duration::from_secs(2), task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let inputs = state.inputs.lock().await;
        let outputs = state.outputs.lock().await;
        assert_eq!(outputs.len(), 2, "{kind}");
        assert!(outputs.iter().all(|output| output[if kind == "slack" {
            "text"
        } else if kind == "discord" {
            "content"
        } else {
            "message"
        }] == "hello world"));
        match kind {
            "mattermost" => {
                assert_eq!(inputs[0]["action"], "authentication_challenge");
                assert_eq!(inputs[0]["data"]["token"], "fixture-token");
                assert_eq!(outputs[0]["root_id"], "ROOT");
            }
            "slack" => {
                assert_eq!(inputs[0]["envelope_id"], "e-0");
                assert_eq!(outputs[0]["thread_ts"], "ROOT");
            }
            _ => {
                assert_eq!(
                    inputs[0]["op"], 2,
                    "Identify must follow HELLO; no premature heartbeat"
                );
                assert!(
                    inputs
                        .iter()
                        .any(|input| input["op"] == 6
                            && input["d"]["session_id"] == "resume-fixture")
                );
            }
        }
        bridge.shutdown().await.unwrap();
        server.abort();
    }
}

#[tokio::test]
async fn mattermost_live_socket_rejects_unlisted_before_acp_and_admits_allowed() {
    use std::os::unix::fs::PermissionsExt;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let state = Fixture {
        kind: "mattermost".into(),
        admission_probe: true,
        base: base.clone(),
        connections: Arc::new(AtomicUsize::new(0)),
        inputs: Arc::default(),
        outputs: Arc::default(),
    };
    let router = Router::new()
        .route("/socket", get(upgrade))
        .route("/api/v4/websocket", get(upgrade))
        .fallback(any(http))
        .with_state(state.clone());
    let server = tokio::spawn(axum::serve(listener, router).into_future());
    let directory = tempfile::tempdir().unwrap();
    let marker = directory.path().join("acp-started");
    let wrapper = directory.path().join("acp-wrapper");
    std::fs::write(
        &wrapper,
        format!(
            "#!/bin/sh\nprintf started >> '{}'\nexec '{}' \"$@\"\n",
            marker.display(),
            env!("CARGO_BIN_EXE_acp-fixture")
        ),
    )
    .unwrap();
    std::fs::set_permissions(&wrapper, std::fs::Permissions::from_mode(0o700)).unwrap();
    let config=config::load(&json!({"rateLimitSeconds":0,"mattermost":{"allowedChannels":["ROOM"]},"sessionStorePath":directory.path().join("store.json"),"acp":{"command":wrapper,"args":[]}}).to_string(),&BTreeMap::new()).unwrap();
    let bridge =
        Arc::new(Bridge::new(config, BTreeMap::new(), directory.path().join("sessions")).unwrap());
    let (sender, receiver) = watch::channel(false);
    let platform = Platform::new("mattermost", &base, "fixture-token", "BOT", "");
    let b = bridge.clone();
    let task = tokio::spawn(async move { platform.run(b, receiver).await });
    // Wait for websocket connection, then the server sends both posts below.
    tokio::time::timeout(Duration::from_secs(5), async {
        while state.connections.load(Ordering::SeqCst) == 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        while !state
            .outputs
            .lock()
            .await
            .iter()
            .any(|post| post["channel_id"] == "ROOM")
        {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(
        state.outputs.lock().await.len(),
        1,
        "only allowed event should post REST response"
    );
    assert!(marker.exists(), "allowed event should spawn ACP");
    let starts = std::fs::read_to_string(&marker).unwrap();
    assert_eq!(
        starts.matches("started").count(),
        1,
        "unlisted event must not spawn ACP"
    );
    sender.send(true).unwrap();
    let _ = tokio::time::timeout(Duration::from_secs(3), task).await;
    bridge.shutdown().await.unwrap();
    server.abort();
}
