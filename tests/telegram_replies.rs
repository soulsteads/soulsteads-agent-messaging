#[path = "support/reference_source.rs"]
mod reference_source;
use axum::{Json, Router, extract::State, routing::post};
use soulsteads_messaging::{
    connectors::http::{Platform, PlatformSink},
    runtime::Sink,
};
use serde_json::{Value, json};
use std::{future::IntoFuture, sync::Arc};
use tokio::sync::Mutex;

#[derive(Clone)]
struct Fixture {
    requests: Arc<Mutex<Vec<Value>>>,
    reject_reply: bool,
}
async fn send(
    State(state): State<Fixture>,
    Json(body): Json<Value>,
) -> (axum::http::StatusCode, Json<Value>) {
    let reject = state.reject_reply && body.get("reply_to_message_id").is_some();
    state.requests.lock().await.push(body);
    if reject {
        (
            axum::http::StatusCode::BAD_REQUEST,
            Json(json!({"ok":false,"error_code":400,"description":"reply rejected"})),
        )
    } else {
        (
            axum::http::StatusCode::OK,
            Json(json!({"ok":true,"result":{"message_id":100}})),
        )
    }
}

#[tokio::test]
async fn telegram_real_http_reply_routing_matches_executed_source_and_retries_rejected_parent() {
    let directory = tempfile::tempdir().unwrap();
    let source = reference_source::root();
    let oracle = std::process::Command::new("bun")
        .arg("--no-env-file")
        .arg(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/source-telegram-replies.ts"
        ))
        .arg(source)
        .current_dir(directory.path())
        .env("BUN_RUNTIME_TRANSPILER_CACHE_PATH", "0")
        .output()
        .unwrap();
    assert!(
        oracle.status.success(),
        "{}",
        String::from_utf8_lossy(&oracle.stderr)
    );
    let stdout = String::from_utf8(oracle.stdout).unwrap();
    let cases: Vec<Value> = serde_json::from_str(stdout.lines().last().unwrap()).unwrap();
    assert_eq!(
        cases.len(),
        15,
        "actual source must execute the full reply corpus"
    );
    let mut mismatches = Vec::new();
    for case in cases {
        let requests = Arc::new(Mutex::new(Vec::new()));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let task = tokio::spawn(
            axum::serve(
                listener,
                Router::new().fallback(post(send)).with_state(Fixture {
                    requests: requests.clone(),
                    reject_reply: case["rejectReply"] == true,
                }),
            )
            .into_future(),
        );
        let platform = Platform::new("telegram", &base, "fixture", "0", "bridge");
        let event = platform.map(&case["input"]).unwrap();
        let result = if case["activity"] == true {
            let sink = PlatformSink::new(platform, event);
            sink.emit(json!({"type":"activity","activityId":"tool","message":"answer"}))
                .await
                .unwrap();
            sink.emit(json!({"type":"activity_update","activityId":"tool","message":"updated"}))
                .await
                .map(|_| String::new())
        } else {
            platform
                .send(&event, case["text"].as_str().unwrap_or("answer"))
                .await
        };
        if result.is_err() || *requests.lock().await != *case["requests"].as_array().unwrap() {
            mismatches.push(format!(
                "{}: result={result:?}; source and destination requests differ",
                case["name"]
            ));
        }
        task.abort();
    }
    assert!(mismatches.is_empty(), "{}", mismatches.join("\n"));
}

#[tokio::test]
async fn telegram_split_preserves_valid_unicode_when_source_would_bisect_a_surrogate_pair() {
    let requests = Arc::new(Mutex::new(Vec::new()));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let task = tokio::spawn(
        axum::serve(
            listener,
            Router::new().fallback(post(send)).with_state(Fixture {
                requests: requests.clone(),
                reject_reply: false,
            }),
        )
        .into_future(),
    );
    let platform = Platform::new("telegram", &base, "fixture", "0", "bridge");
    let event = platform.map(&json!({"message_id":20,"chat":{"id":7,"type":"private"},"from":{"id":8},"text":"hello"})).unwrap();
    let prefix = "a".repeat(4095);
    platform
        .send(&event, &format!("{prefix}😀tail"))
        .await
        .unwrap();
    let bodies = requests.lock().await;
    assert_eq!(bodies.len(), 2);
    assert_eq!(bodies[0]["text"], prefix);
    assert_eq!(bodies[1]["text"], "😀tail");
    task.abort();
}
