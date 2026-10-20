use async_trait::async_trait;
use axum::{
    Router,
    extract::{Request, State},
    response::IntoResponse,
    routing::any,
};
use soulsteads_messaging::{
    config,
    connectors::http::Platform,
    runtime::{Bridge, Sink},
};
use serde_json::{Value, json};
use std::{collections::BTreeMap, future::IntoFuture, sync::Arc};
use tokio::sync::Mutex;

#[derive(Clone, Default)]
struct Server(Arc<Mutex<Vec<(String, String, String)>>>);
async fn endpoint(State(state): State<Server>, request: Request) -> impl IntoResponse {
    let path = request.uri().path().to_owned();
    let host = request
        .headers()
        .get("host")
        .unwrap()
        .to_str()
        .unwrap()
        .to_owned();
    let token = request
        .headers()
        .get("authorization")
        .and_then(|value| value.to_str().ok())
        .unwrap_or("")
        .to_owned();
    let bytes = axum::body::to_bytes(request.into_body(), 25 * 1024 * 1024)
        .await
        .unwrap();
    let body = String::from_utf8_lossy(&bytes).to_string();
    let mut requests = state.0.lock().await;
    let first = !requests
        .iter()
        .any(|(previous, _, _)| previous.contains("sendMessage"));
    requests.push((path.clone(), token, body));
    if first && path.contains("sendMessage") {
        return (
            axum::http::StatusCode::TOO_MANY_REQUESTS,
            axum::Json(json!({"ok":false,"error_code":429,"parameters":{"retry_after":0}})),
        )
            .into_response();
    }
    axum::Json(if path.contains("files.getUploadURLExternal") {
        json!({"ok":true,"file_id":"file-1","upload_url":format!("http://{host}/uploads")})
    } else if path.contains("files") {
        json!({"file_infos":[{"id":"file-1"}]})
    } else if path.contains("send") {
        json!({"ok":true,"result":{"message_id":17}})
    } else if path.contains("chat.") {
        json!({"ok":true,"ts":"17"})
    } else {
        json!({"id":"17"})
    })
    .into_response()
}

struct PlatformSink {
    platform: Platform,
    event: soulsteads_messaging::runtime::Event,
}
#[async_trait]
impl Sink for PlatformSink {
    async fn emit(&self, value: Value) -> anyhow::Result<()> {
        if value["type"] == "response" {
            self.platform
                .send(&self.event, value["text"].as_str().unwrap())
                .await?;
        }
        Ok(())
    }
}
#[tokio::test]
async fn four_adapters_map_real_wire_events_and_auth_send_edit_upload() {
    let server = Server::default();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let task = tokio::spawn(
        axum::serve(
            listener,
            Router::new()
                .fallback(any(endpoint))
                .with_state(server.clone()),
        )
        .into_future(),
    );
    let directory = tempfile::tempdir().unwrap();
    let config = config::load(&json!({"rateLimitSeconds":0,"sessionStorePath":directory.path().join("store.json"),"acp":{"command":env!("CARGO_BIN_EXE_acp-fixture"),"args":[]}}).to_string(),&BTreeMap::new()).unwrap();
    let bridge = Bridge::new(config, BTreeMap::new(), directory.path().join("sessions")).unwrap();
    let cases = [
        (
            "telegram",
            json!({"message":{"message_id":1,"chat":{"id":-123,"type":"supergroup"},"from":{"id":7},"is_topic_message":true,"message_thread_id":9,"text":"!oc hello"}}),
        ),
        (
            "mattermost",
            json!({"event":"posted","data":{"post":json!({"id":"one","channel_id":"room","user_id":"7","root_id":"root","message":"!oc hello"}).to_string(),"channel_type":"O"}}),
        ),
        (
            "discord",
            json!({"t":"MESSAGE_CREATE","d":{"id":"one","channel_id":"room","guild_id":"guild","author":{"id":"7","bot":false},"content":"!oc hello"}}),
        ),
        (
            "slack",
            json!({"payload":{"team_id":"team","event":{"type":"message","user":"7","channel":"room","ts":"1.1","thread_ts":"1.0","text":"!oc hello"}}}),
        ),
    ];
    for (kind, message) in cases {
        let platform = Platform::new(kind, &base, "fake-token", "bot", "bridge");
        let event = platform.map(&message).unwrap();
        if kind != "discord" {
            assert!(!event.thread.is_empty());
        }
        bridge
            .handle(
                event.clone(),
                &PlatformSink {
                    platform: platform.clone(),
                    event: event.clone(),
                },
            )
            .await
            .unwrap();
        platform.edit(&event, "17", "updated").await.unwrap();
        platform
            .upload(&event, "document.txt", b"safe document".to_vec())
            .await
            .unwrap();
    }
    bridge.shutdown().await.unwrap();
    let requests = server.0.lock().await;
    assert!(
        requests
            .iter()
            .any(|(path, _, body)| path.contains("sendMessage")
                && body.contains("hello world")
                && body.contains("message_thread_id"))
    );
    assert!(
        requests
            .iter()
            .any(|(path, token, body)| path == "/api/v4/posts"
                && token == "Bearer fake-token"
                && body.contains("root_id"))
    );
    assert!(requests.iter().any(
        |(path, token, _)| path.contains("channels/room/messages") && token == "Bot fake-token"
    ));
    assert!(
        requests
            .iter()
            .any(|(path, token, _)| path.contains("chat.postMessage")
                && token == "Bearer fake-token")
    );
    assert!(
        requests
            .iter()
            .any(|(_, _, body)| body.contains("safe document"))
    );
    assert_eq!(
        requests
            .iter()
            .filter(|(path, _, _)| path.contains("sendMessage"))
            .count(),
        2
    );
    task.abort();
}

#[tokio::test]
async fn native_adapter_edits_one_cumulative_tool_trace_preserving_initial_names() {
    let fixture = Server::default();
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
    let configuration=config::load(&json!({"rateLimitSeconds":0,"sessionStorePath":directory.path().join("store.json"),"acp":{"command":env!("CARGO_BIN_EXE_acp-fixture"),"args":[]},"toolMessages":{"mode":"trace","showArguments":true}}).to_string(),&BTreeMap::new()).unwrap();
    let bridge = Bridge::new(
        configuration,
        BTreeMap::new(),
        directory.path().join("sessions"),
    )
    .unwrap();
    let platform = Platform::new("mattermost", &base, "fixture-token", "BOT", "bridge");
    let event=platform.map(&json!({"event":"posted","data":{"post":json!({"id":"one","channel_id":"room","user_id":"7","message":"!oc tools"}).to_string(),"channel_type":"O"}})).unwrap();
    let sink = soulsteads_messaging::connectors::http::PlatformSink::new(platform, event.clone());
    bridge.handle(event, &sink).await.unwrap();
    bridge.shutdown().await.unwrap();
    let requests = fixture.0.lock().await;
    let messages: Vec<Value> = requests
        .iter()
        .filter(|(path, _, _)| path.starts_with("/api/v4/posts"))
        .map(|(_, _, body)| serde_json::from_str(body).unwrap())
        .collect();
    assert!(
        messages.iter().any(|body| body["message"]
            .as_str()
            .is_some_and(|text| text.contains("Tool trace (completed)")
                && text.contains("[completed] filePath=notes.txt [read]")
                && text.contains("[completed] pattern=test [grep]")))
    );
    assert_eq!(
        messages
            .iter()
            .filter(|body| body["message"]
                .as_str()
                .is_some_and(|text| text.contains("Tool trace")))
            .count(),
        4
    );
    assert!(
        requests
            .iter()
            .any(|(path, _, _)| path == "/api/v4/posts/17/patch")
    );
    server.abort();
}

#[tokio::test]
async fn mattermost_system_posts_are_rejected_and_allowed_channels_gate_admission() {
    let server = Server::default();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let task = tokio::spawn(
        axum::serve(
            listener,
            Router::new()
                .fallback(any(endpoint))
                .with_state(server.clone()),
        )
        .into_future(),
    );
    let directory = tempfile::tempdir().unwrap();
    let configuration = config::load(&json!({"rateLimitSeconds":0,"mattermost":{"allowedChannels":["permitted"]},"sessionStorePath":directory.path().join("store.json"),"acp":{"command":env!("CARGO_BIN_EXE_acp-fixture"),"args":[]}}).to_string(), &BTreeMap::new()).unwrap();
    let bridge = Bridge::new(
        configuration,
        BTreeMap::new(),
        directory.path().join("sessions"),
    )
    .unwrap();
    let platform = Platform::new("mattermost", &base, "token", "BOT", "bridge");
    let system = json!({"event":"posted","data":{"post":json!({"id":"sys","channel_id":"permitted","user_id":"human","type":"system_join_channel","message":"!oc hello"}).to_string(),"channel_type":"O"}});
    assert!(
        platform.map(&system).is_none(),
        "system post must not enter bridge"
    );
    let denied = json!({"event":"posted","data":{"post":json!({"id":"denied","channel_id":"other","user_id":"human","message":"!oc hello"}).to_string(),"channel_type":"O"}});
    assert!(
        platform.prepare(&bridge, &denied).await.unwrap().is_none(),
        "unlisted channel must fail closed"
    );
    bridge.shutdown().await.unwrap();
    assert!(
        server.0.lock().await.is_empty(),
        "rejected events must not trigger REST or ACP work"
    );
    task.abort();
}
