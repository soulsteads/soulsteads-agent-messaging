//! HTTP transports for the owned live MCP server.
use super::live_mcp::{append, response};
use anyhow::Result;
use serde_json::{Value, json};
use std::{
    collections::HashMap,
    path::PathBuf,
    sync::{Arc, Mutex},
};
use tokio::sync::mpsc;
#[derive(Clone)]
struct State {
    audit: PathBuf,
    url: String,
    sessions: Arc<Mutex<HashMap<String, mpsc::UnboundedSender<Value>>>>,
}
pub struct Http {
    pub url: String,
    task: tokio::task::JoinHandle<()>,
}
impl Drop for Http {
    fn drop(&mut self) {
        self.task.abort();
    }
}
impl Http {
    pub async fn start(audit: PathBuf) -> Result<Self> {
        use axum::{
            Router,
            routing::{get, post},
        };
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let url = format!("http://{}", listener.local_addr()?);
        let state = State {
            audit,
            url: url.clone(),
            sessions: Default::default(),
        };
        let app = Router::new()
            .route("/mcp", post(http))
            .route("/sse", get(sse))
            .route("/messages", post(message))
            .with_state(state);
        let task = tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        Ok(Self { url, task })
    }
}
async fn http(
    axum::extract::State(state): axum::extract::State<State>,
    headers: axum::http::HeaderMap,
    axum::Json(request): axum::Json<Value>,
) -> axum::response::Response {
    use axum::response::IntoResponse;
    let _ = append(
        &state.audit,
        &json!({"transport":"http","method":request["method"],"params":request["params"],"probeHeader":headers.get("x-adapter-probe").and_then(|value|value.to_str().ok()),"literalHeader":headers.get("x-adapter-literal").and_then(|value|value.to_str().ok())}),
    );
    match response(&request) {
        Some(response) => axum::Json(response).into_response(),
        None => axum::http::StatusCode::ACCEPTED.into_response(),
    }
}
async fn sse(
    axum::extract::State(state): axum::extract::State<State>,
    headers: axum::http::HeaderMap,
) -> axum::response::Response {
    use axum::response::{
        IntoResponse, Sse,
        sse::{Event, KeepAlive},
    };
    let id = uuid::Uuid::new_v4().to_string();
    let _ = append(
        &state.audit,
        &json!({"transport":"sse_connect","probeHeader":headers.get("x-adapter-probe").and_then(|value|value.to_str().ok())}),
    );
    let (sender, receiver) = mpsc::unbounded_channel();
    state.sessions.lock().unwrap().insert(id.clone(), sender);
    let endpoint = format!("{}/messages?session={id}", state.url);
    let initial = futures::stream::once(async move {
        Ok::<_, std::convert::Infallible>(Event::default().event("endpoint").data(endpoint))
    });
    let messages = futures::stream::unfold(receiver, |mut receiver| async move {
        receiver.recv().await.map(|value| {
            (
                Ok::<_, std::convert::Infallible>(
                    Event::default().event("message").data(value.to_string()),
                ),
                receiver,
            )
        })
    });
    use futures::StreamExt;
    Sse::new(initial.chain(messages))
        .keep_alive(KeepAlive::default())
        .into_response()
}
async fn message(
    axum::extract::State(state): axum::extract::State<State>,
    axum::extract::Query(query): axum::extract::Query<HashMap<String, String>>,
    headers: axum::http::HeaderMap,
    axum::Json(request): axum::Json<Value>,
) -> axum::http::StatusCode {
    let _ = append(
        &state.audit,
        &json!({"transport":"sse","method":request["method"],"params":request["params"],"probeHeader":headers.get("x-adapter-probe").and_then(|value|value.to_str().ok())}),
    );
    if let Some(response) = response(&request)
        && let Some(id) = query.get("session")
        && let Some(sender) = state.sessions.lock().unwrap().get(id)
    {
        let _ = sender.send(response);
    }
    axum::http::StatusCode::ACCEPTED
}
