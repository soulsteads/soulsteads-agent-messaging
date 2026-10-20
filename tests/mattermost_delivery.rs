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
async fn endpoint(State(posts): State<Arc<Mutex<Vec<Value>>>>, request: Request) -> Json<Value> {
    let files = request.uri().path().ends_with("/files");
    let bytes = axum::body::to_bytes(request.into_body(), 1024 * 1024)
        .await
        .unwrap();
    if files {
        Json(json!({"file_infos":[{"id":"file-one"}]}))
    } else {
        posts
            .lock()
            .await
            .push(serde_json::from_slice(&bytes).unwrap());
        Json(json!({"id":"posted"}))
    }
}
async fn delivery(isolated: bool) -> Vec<Value> {
    let posts = Arc::new(Mutex::new(Vec::new()));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(
        axum::serve(
            listener,
            Router::new()
                .fallback(any(endpoint))
                .with_state(posts.clone()),
        )
        .into_future(),
    );
    let mut event = Event::web("channel", "");
    event.platform = "mattermost".into();
    event.room = "channel".into();
    event.thread = "root".into();
    let sink = PlatformSink::new(
        Platform::new("mattermost", &base, "fixture", "BOT", "bridge"),
        event,
    )
    .with_thread_isolation(isolated);
    sink.emit(json!({"type":"response","text":"reply"}))
        .await
        .unwrap();
    sink.emit(json!({"type":"error","message":"error"}))
        .await
        .unwrap();
    sink.emit(json!({"type":"notice","text":"> Permission denied"}))
        .await
        .unwrap();
    sink.upload("picture.png", "image/png", vec![1, 2, 3])
        .await
        .unwrap();
    let result = posts.lock().await.clone();
    server.abort();
    result
}
#[tokio::test]
async fn nonisolated_replies_errors_and_images_are_channel_posts() {
    let posts = delivery(false).await;
    assert_eq!(posts.len(), 4);
    for post in posts {
        assert!(
            post["root_id"].as_str().unwrap_or("").is_empty(),
            "unexpected thread root: {post}"
        );
    }
}
#[tokio::test]
async fn isolated_replies_errors_and_images_preserve_thread_root() {
    let posts = delivery(true).await;
    assert_eq!(posts.len(), 4);
    for post in posts {
        assert_eq!(post["root_id"], "root");
    }
}
