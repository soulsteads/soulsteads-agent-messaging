use axum::{Json, Router, extract::State, routing::post};
use serde_json::{Value, json};
use soulsteads_messaging::{connectors::http::Platform, runtime::Event};
use std::{fs, future::IntoFuture, path::Path, sync::Arc};
use tokio::sync::Mutex;

fn source(path: &str) -> String {
    fs::read_to_string(Path::new(env!("CARGO_MANIFEST_DIR")).join(path))
        .unwrap_or_else(|error| panic!("{path} must exist: {error}"))
}

#[test]
fn telegram_connector_has_local_inbound_and_outbound_delegates() {
    let module = source("src/connectors/telegram/mod.rs");
    let inbound = source("src/connectors/telegram/inbound.rs");
    let outbound = source("src/connectors/telegram/outbound.rs");
    let http = source("src/connectors/http.rs");

    assert!(module.contains("pub(crate) mod inbound;"));
    assert!(module.contains("pub(crate) mod outbound;"));
    assert!(module.contains("pub(crate) async fn poll"));
    assert!(inbound.contains("pub(crate) fn map"));
    assert!(inbound.contains("pub(crate) async fn admit"));
    assert!(inbound.contains("pub(crate) async fn prepare"));
    assert!(inbound.contains("async fn download"));
    assert!(outbound.contains("pub(crate) async fn send"));
    assert!(outbound.contains("pub(crate) async fn edit"));
    assert!(http.contains("crate::connectors::telegram::inbound::map"));
    assert!(http.contains("crate::connectors::telegram::inbound::admit"));
    assert!(http.contains("crate::connectors::telegram::inbound::prepare"));
    assert!(http.contains("crate::connectors::telegram::outbound::send"));
    assert!(http.contains("crate::connectors::telegram::poll"));

    // Keep Telegram behavior behind the connector module.  The HTTP connector
    // may retain shared low-level API/multipart helpers, but must not retain a
    // second unreachable implementation of Telegram mapping, media, outbound,
    // or polling behavior after dispatching to telegram/.
    for duplicate in [
        "async fn download_telegram",
        r#""sendMessage""#,
        r#""editMessageText""#,
        r#""sendDocument""#,
        r#""deleteWebhook""#,
        r#""getUpdates""#,
        r#""getFile""#,
    ] {
        assert!(
            !http.contains(duplicate),
            "http.rs retains duplicated Telegram implementation: {duplicate}"
        );
    }
}

#[derive(Clone)]
struct RejectedParentFixture {
    requests: Arc<Mutex<Vec<Value>>>,
}

async fn reject_parent(
    State(fixture): State<RejectedParentFixture>,
    Json(body): Json<Value>,
) -> (axum::http::StatusCode, Json<Value>) {
    let rejected = body.get("reply_to_message_id").is_some();
    fixture.requests.lock().await.push(body);
    if rejected {
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
async fn telegram_send_retries_once_without_rejected_parent() {
    let requests = Arc::new(Mutex::new(Vec::new()));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(
        axum::serve(
            listener,
            Router::new()
                .fallback(post(reject_parent))
                .with_state(RejectedParentFixture {
                    requests: requests.clone(),
                }),
        )
        .into_future(),
    );
    let platform = Platform::new("telegram", &base, "fixture", "0", "bridge");
    let mut event = Event::web("7", "message");
    event.platform = "telegram".into();
    event.direct = false;
    event.reply_to = Some("41".into());
    platform.send(&event, "answer").await.unwrap();
    assert_eq!(
        *requests.lock().await,
        vec![
            json!({"chat_id":"7","text":"answer","reply_to_message_id":41}),
            json!({"chat_id":"7","text":"answer"}),
        ]
    );
    server.abort();
}
