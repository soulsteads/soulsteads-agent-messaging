use axum::{
    Json, Router,
    extract::{Request, State},
    response::{IntoResponse, Response},
    routing::any,
};
use soulsteads_messaging::{config, connectors::http::Platform, runtime::Bridge};
use serde_json::json;
use std::{collections::BTreeMap, future::IntoFuture, sync::Arc};
use tokio::sync::Mutex;
#[derive(Clone, Default)]
struct Fixture(Arc<Mutex<Vec<String>>>);
async fn handler(State(state): State<Fixture>, request: Request) -> Response {
    let path = request.uri().path().to_owned();
    state.0.lock().await.push(path.clone());
    if path.contains("/file/") {
        return ([("content-type", "text/plain")], "document fixture").into_response();
    }
    Json(json!({"ok":true,"result":{"file_id":"fixture-file","file_size":16,"file_path":"documents/file.txt"}})).into_response()
}
#[tokio::test]
async fn telegram_downloads_after_admission_into_isolated_workspace_with_safe_filename() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let state = Fixture::default();
    let server = tokio::spawn(
        axum::serve(
            listener,
            Router::new()
                .fallback(any(handler))
                .with_state(state.clone()),
        )
        .into_future(),
    );
    let directory = tempfile::tempdir().unwrap();
    let configuration=config::load(&json!({"rateLimitSeconds":0,"sessionStorePath":directory.path().join("store.json"),"telegram":{"allowedUsers":["7"]}}).to_string(),&BTreeMap::new()).unwrap();
    let bridge = Bridge::new(
        configuration,
        BTreeMap::new(),
        directory.path().join("sessions"),
    )
    .unwrap();
    let platform = Platform::new("telegram", &base, "fixture-token", "BOT", "bridge");
    let mut message = json!({"message":{"message_id":1,"chat":{"id":123,"type":"private"},"from":{"id":8},"document":{"file_id":"fixture-file","file_name":"../../untrusted.txt","file_size":16,"mime_type":"text/plain"}}});
    assert!(platform.prepare(&bridge, &message).await.unwrap().is_none());
    assert!(state.0.lock().await.is_empty());
    message["message"]["from"]["id"] = 7.into();
    let event = platform.prepare(&bridge, &message).await.unwrap().unwrap();
    assert!(event.text.contains("[Attached file:"));
    assert!(event.text.contains("User sent a file attachment"));
    let root = bridge.working_directory(&event);
    let files = std::fs::read_dir(root.join("uploads"))
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .collect::<Vec<_>>();
    assert_eq!(files.len(), 1);
    assert_eq!(std::fs::read(&files[0]).unwrap(), b"document fixture");
    assert!(
        files[0]
            .file_name()
            .unwrap()
            .to_string_lossy()
            .ends_with(".txt")
    );
    assert!(
        state
            .0
            .lock()
            .await
            .iter()
            .any(|path| path == "/file/botfixture-token/documents/file.txt")
    );
    server.abort();
}

#[tokio::test]
async fn telegram_rejects_encoded_traversal_before_file_download() {
    let file_requests = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let observed = file_requests.clone();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let router = Router::new().fallback(any(move |request: Request| {
        let observed = observed.clone();
        async move {
            if request.uri().path().ends_with("getFile") {
                Json(json!({"ok":true,"result":{"file_path":"documents/%2e%2e/secret.txt"}}))
                    .into_response()
            } else {
                observed.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                "must not download".into_response()
            }
        }
    }));
    let server = tokio::spawn(axum::serve(listener, router).into_future());
    let directory = tempfile::tempdir().unwrap();
    let configuration = config::load(
        &json!({"rateLimitSeconds":0,"sessionStorePath":directory.path().join("store.json")})
            .to_string(),
        &BTreeMap::new(),
    )
    .unwrap();
    let bridge = Bridge::new(
        configuration,
        BTreeMap::new(),
        directory.path().join("sessions"),
    )
    .unwrap();
    let platform = Platform::new("telegram", &base, "fixture-token", "bot", "bridge");
    let message = json!({"message":{"message_id":1,"chat":{"id":123,"type":"private"},"from":{"id":7},"document":{"file_id":"fixture","file_name":"file.txt"}}});
    let event = platform.prepare(&bridge, &message).await.unwrap().unwrap();
    assert!(!event.text.contains("[Attached file:"));
    assert_eq!(file_requests.load(std::sync::atomic::Ordering::SeqCst), 0);
    server.abort();
}
