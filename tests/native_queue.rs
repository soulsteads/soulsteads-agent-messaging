use axum::{Json, Router, extract::Request, response::IntoResponse, routing::any};
use soulsteads_messaging::native_queue::NativeQueue;
use serde_json::{Value, json};
use std::sync::{Arc, Mutex};
#[derive(Default)]
struct Seen(Mutex<Vec<(String, Value)>>);
async fn api(state: axum::extract::State<Arc<Seen>>, request: Request) -> impl IntoResponse {
    use axum::http::StatusCode;
    let path = request.uri().path().to_owned();
    let auth = request
        .headers()
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default()
        .to_owned();
    if request.method() == axum::http::Method::DELETE {
        state.0.0.lock().unwrap().push((path, json!({})));
        return (StatusCode::NO_CONTENT, Json(Value::Null));
    }
    let is_post = request.method() == axum::http::Method::POST;
    let body = if is_post {
        serde_json::from_slice(
            &axum::body::to_bytes(request.into_body(), 1024 * 1024)
                .await
                .unwrap(),
        )
        .unwrap()
    } else {
        json!({})
    };
    if auth != "Basic b3BlbmNvZGU6c2VjcmV0" {
        return (StatusCode::UNAUTHORIZED, Json(json!({})));
    }
    state.0.0.lock().unwrap().push((path.clone(), body.clone()));
    if path == "/api/session" {
        return (StatusCode::OK, Json(json!({"data":{"id":"ses_mock"}})));
    }
    if path == "/api/session/ses_mock/prompt" {
        return (StatusCode::OK, Json(json!({"data":{"id":body["id"]}})));
    }
    (StatusCode::OK, Json(json!({"data":[]})))
}
#[tokio::test]
async fn authenticated_native_session_admission_and_inbox_contract() {
    let seen = Arc::new(Seen::default());
    let app = Router::new()
        .route("/{*path}", any(api))
        .with_state(seen.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    let client = NativeQueue::new(&format!("http://{address}"), "secret".into()).unwrap();
    let session = client
        .create_session(
            "/tmp/native-fixture",
            Some("build"),
            Some(json!({"providerID":"fixture","id":"fake"})),
        )
        .await
        .unwrap();
    assert_eq!(session, "ses_mock");
    assert_eq!(
        client
            .enqueue(&session, "msg_fixture", "return fixture")
            .await
            .unwrap(),
        "msg_fixture"
    );
    assert!(client.inbox(&session).await.unwrap().is_empty());
    client.cancel(&session, "msg_fixture").await.unwrap();
    let seen = seen.0.lock().unwrap();
    let prompt = seen
        .iter()
        .find(|(path, _)| path.ends_with("/prompt"))
        .unwrap();
    assert_eq!(prompt.1["delivery"], "queue");
    assert_eq!(prompt.1["resume"], true);
    assert_eq!(prompt.1["id"], "msg_fixture");
    drop(seen);
    server.abort();
}
#[test]
fn native_api_refuses_non_loopback_or_uncredentialed_endpoints() {
    assert!(NativeQueue::new("http://127.0.0.1:4096", "secret".into()).is_ok());
    assert!(NativeQueue::new("http://0.0.0.0:4096", "secret".into()).is_err());
    assert!(NativeQueue::new("https://127.0.0.1:4096", "secret".into()).is_err());
    assert!(NativeQueue::new("http://127.0.0.1:4096", String::new()).is_err());
}
