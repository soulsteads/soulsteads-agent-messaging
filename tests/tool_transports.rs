#[path = "support/reference_source.rs"]
mod reference_source;
use axum::{
    Json, Router,
    extract::{Request, State},
    routing::any,
};
use soulsteads_messaging::{
    connectors::http::{Platform, PlatformSink},
    runtime::{Event, Sink},
};
use serde_json::{Value, json};
use std::{future::IntoFuture, sync::Arc};
use tokio::sync::Mutex;
async fn endpoint(State(state): State<Arc<Mutex<Vec<Value>>>>, request: Request) -> Json<Value> {
    let method = request.method().to_string();
    let path = request.uri().path().to_owned();
    let body: Value = serde_json::from_slice(
        &axum::body::to_bytes(request.into_body(), 1024 * 1024)
            .await
            .unwrap(),
    )
    .unwrap();
    state
        .lock()
        .await
        .push(json!({"method":method,"endpoint":path,"body":body}));
    Json(json!({"ok":true,"id":"100","ts":"100"}))
}
#[tokio::test]
async fn editable_http_tool_notices_execute_source_callbacks_and_scope_their_edits() {
    let directory = tempfile::tempdir().unwrap();
    let source = reference_source::root();
    let oracle = std::process::Command::new("bun")
        .arg("--no-env-file")
        .arg(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/source-tool-transports.ts"
        ))
        .arg(source)
        .current_dir(directory.path())
        .output()
        .unwrap();
    assert!(
        oracle.status.success(),
        "{}",
        String::from_utf8_lossy(&oracle.stderr)
    );
    let cases: Vec<Value> = serde_json::from_slice(&oracle.stdout).unwrap();
    let mut failures = Vec::new();
    for case in cases {
        let requests = Arc::new(Mutex::new(Vec::new()));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let task = tokio::spawn(
            axum::serve(
                listener,
                Router::new()
                    .fallback(any(endpoint))
                    .with_state(requests.clone()),
            )
            .into_future(),
        );
        let kind = case["kind"].as_str().unwrap();
        let platform = Platform::new(kind, &base, "fixture", "BOT", "bridge");
        let mut event = Event::web("user", "query");
        event.platform = kind.into();
        event.room = "room".into();
        event.thread = "root".into();
        event.id = "incoming".into();
        let sink =
            PlatformSink::new(platform, event).with_thread_isolation(case["isolated"] == true);
        sink.emit(json!({"type":"activity","activityId":"tool","message":"answer"}))
            .await
            .unwrap();
        sink.emit(json!({"type":"activity_update","activityId":"tool","message":"updated"}))
            .await
            .unwrap();
        let actual = requests.lock().await;
        for (index, expected) in case["requests"].as_array().unwrap().iter().enumerate() {
            let matches = if kind == "discord" {
                actual[index]["body"]["content"] == expected["text"]
                    && actual[index]["body"].get("message_reference").is_none()
            } else if kind == "mattermost" {
                actual[index]["body"] == expected["body"]
                    && actual[index]["endpoint"]
                        .as_str()
                        .unwrap()
                        .ends_with(expected["endpoint"].as_str().unwrap())
            } else {
                actual[index]["body"] == expected["body"]
            };
            if !matches {
                failures.push(format!(
                    "{kind} operation {index}: {expected} != {}",
                    actual[index]
                ));
            }
        }
        task.abort();
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}
