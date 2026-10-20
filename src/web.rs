use crate::runtime::{Bridge, Event, Sink};
use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use axum::{
    Json, Router,
    extract::{
        Query, Request, State, WebSocketUpgrade,
        ws::{Message, WebSocket},
    },
    http::{HeaderMap, HeaderValue, Method, StatusCode},
    middleware::{self, Next},
    response::{Html, IntoResponse, Response},
    routing::get,
};
use base64::Engine;
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use std::{
    collections::HashMap,
    io::Cursor,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};
use tokio::sync::{Mutex, Semaphore, mpsc};

#[derive(Clone)]
struct WebState {
    bridge: Arc<Bridge>,
    config: Value,
    clients: Arc<AtomicUsize>,
    sockets: Arc<Mutex<SocketRegistry>>,
}
type SocketRegistry = HashMap<String, (uuid::Uuid, mpsc::Sender<Value>)>;

pub fn router(bridge: Arc<Bridge>, config: Value) -> Router {
    let state = WebState {
        bridge,
        config,
        clients: Arc::new(AtomicUsize::new(0)),
        sockets: Arc::default(),
    };
    Router::new()
        .route("/", get(health))
        .route("/health", get(health))
        .route("/ws", get(upgrade))
        .route("/widget.js", get(widget))
        .route("/favicon.svg", get(favicon))
        .route("/chat", get(page))
        .route("/test", get(test_page))
        .route("/test-embedded", get(page))
        .layer(middleware::from_fn_with_state(state.clone(), cors))
        .with_state(state)
}
async fn cors(State(state): State<WebState>, request: Request, next: Next) -> Response {
    let origin = request.headers().get("origin").cloned();
    let allowed = crate::config::strings(&state.config, "allowedOrigins");
    let selected = if allowed.iter().any(|origin| origin == "*") {
        Some(HeaderValue::from_static("*"))
    } else {
        origin.filter(|origin| {
            origin
                .to_str()
                .is_ok_and(|origin| allowed.iter().any(|allowed| allowed == origin))
        })
    };
    let mut response = if request.method() == Method::OPTIONS {
        StatusCode::NO_CONTENT.into_response()
    } else {
        next.run(request).await
    };
    if let Some(origin) = selected {
        response
            .headers_mut()
            .insert("access-control-allow-origin", origin);
        response.headers_mut().insert(
            "access-control-allow-methods",
            HeaderValue::from_static("GET, OPTIONS"),
        );
        response.headers_mut().insert(
            "access-control-allow-headers",
            HeaderValue::from_static("Content-Type"),
        );
        response
            .headers_mut()
            .insert("vary", HeaderValue::from_static("Origin"));
    }
    response
}
async fn health(State(state): State<WebState>) -> Json<Value> {
    Json(json!({"status":"ok","connector":"web","clients":state.clients.load(Ordering::Relaxed)}))
}
async fn widget(State(state): State<WebState>) -> impl IntoResponse {
    let configuration=json!({"title":crate::config::string(&state.bridge.config,"botName"),"attachments":state.config["attachments"]}).to_string().replace('<',"\\u003c").replace('>',"\\u003e").replace('&',"\\u0026");
    (
        [
            ("content-type", "application/javascript; charset=utf-8"),
            ("cache-control", "no-cache"),
        ],
        format!(
            "window.OpenCodeWidgetServerConfig = {configuration};\n{}\n{}\n{}",
            include_str!("../assets/web-widget-state.js"),
            include_str!("../assets/web-message-renderer.js"),
            include_str!("../assets/web-widget.js")
        ),
    )
}
async fn favicon() -> impl IntoResponse {
    (
        [("content-type", "image/svg+xml; charset=utf-8")],
        include_str!("../assets/favicon.svg"),
    )
}
async fn page() -> Html<&'static str> {
    Html(
        "<!doctype html><html><head><meta charset=\"utf-8\"><title>Chat bridge</title><meta name=\"viewport\" content=\"width=device-width,initial-scale=1\"></head><body style=\"margin:0;height:100vh\"><div id=\"chat\" style=\"height:100%\"></div><script>window.OpenCodeWidget = {mode:'embedded',container:'#chat'};</script><script src=\"/widget.js\"></script></body></html>",
    )
}
async fn test_page() -> Html<&'static str> {
    Html(
        "<!doctype html><html><head><meta charset=\"utf-8\"><title>Widget test</title></head><body><h1>Chat bridge widget</h1><script src=\"/widget.js\"></script></body></html>",
    )
}

async fn upgrade(
    State(state): State<WebState>,
    Query(query): Query<HashMap<String, String>>,
    headers: HeaderMap,
    socket: WebSocketUpgrade,
) -> Response {
    let origins = crate::config::strings(&state.config, "allowedOrigins");
    if let Some(origin) = headers.get("origin").and_then(|value| value.to_str().ok())
        && !origins
            .iter()
            .any(|allowed| allowed == "*" || allowed == origin)
    {
        return (StatusCode::FORBIDDEN, "Origin not allowed").into_response();
    }
    let client = query
        .get("clientId")
        .cloned()
        .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
    if client.is_empty() || client.len() > 256 || state.clients.load(Ordering::Relaxed) >= 256 {
        return (
            StatusCode::BAD_REQUEST,
            "Invalid client or capacity exceeded",
        )
            .into_response();
    }
    socket
        .max_message_size(8 * 1024 * 1024)
        .max_frame_size(8 * 1024 * 1024)
        .on_upgrade(move |socket| session(socket, state, client))
}
struct WebSink {
    state: WebState,
    client: String,
    presentation: Arc<Mutex<WebPresentation>>,
}
#[derive(Default)]
struct WebPresentation {
    prompting: bool,
    streamed: String,
    overflow: bool,
}
impl WebSink {
    fn new(state: WebState, client: String) -> Self {
        Self {
            state,
            client,
            presentation: Arc::default(),
        }
    }
}
#[async_trait]
impl Sink for WebSink {
    fn owned(&self) -> Option<Arc<dyn Sink>> {
        Some(Arc::new(Self {
            state: self.state.clone(),
            client: self.client.clone(),
            presentation: self.presentation.clone(),
        }))
    }
    async fn emit(&self, mut event: Value) -> Result<()> {
        let sender = self
            .state
            .sockets
            .lock()
            .await
            .get(&self.client)
            .map(|(_, sender)| sender.clone())
            .ok_or_else(|| anyhow::anyhow!("No active WebSocket"))?;
        if sender.is_closed() {
            bail!("WebSocket output queue closed");
        }
        let mut presentation = self.presentation.lock().await;
        if event["type"] == "response" && presentation.prompting && !presentation.overflow {
            let text = event["text"].as_str().unwrap_or("");
            let clean_stream = crate::core::sanitize(
                &crate::core::media(&crate::core::media(&presentation.streamed, false).1, true).1,
            );
            if text.is_empty() || (!presentation.streamed.is_empty() && text == clean_stream) {
                return Ok(());
            }
            if !presentation.streamed.is_empty()
                && let Some(suffix) = text.strip_prefix(&presentation.streamed)
            {
                event = json!({"type":"chunk","text":suffix});
            } else {
                event = json!({"type":"chunk","text":text});
            }
        }
        let next_stream = if event["type"] == "chunk" {
            event["text"].as_str().map(str::to_owned)
        } else {
            None
        };
        let thinking = event["type"] == "thinking";
        tokio::time::timeout(std::time::Duration::from_secs(5), sender.send(event))
            .await
            .context("WebSocket output queue timed out")?
            .map_err(|_| anyhow::anyhow!("WebSocket output queue closed"))?;
        presentation.prompting |= thinking;
        if let Some(text) = next_stream {
            if presentation.streamed.len().saturating_add(text.len()) <= 2 * 1024 * 1024 {
                presentation.streamed.push_str(&text);
            } else {
                presentation.overflow = true;
            }
        }
        Ok(())
    }
    async fn upload(&self, name: &str, mime: &str, bytes: Vec<u8>) -> Result<()> {
        if mime.starts_with("image/") {
            self.emit(json!({"type":"image","mimeType":mime,"data":base64::engine::general_purpose::STANDARD.encode(bytes),"alt":name})).await?;
        }
        Ok(())
    }
}
async fn session(socket: WebSocket, state: WebState, client: String) {
    state.clients.fetch_add(1, Ordering::Relaxed);
    let (mut writer, mut reader) = socket.split();
    let (sender, mut receiver) = mpsc::channel::<Value>(64);
    let connection_id = uuid::Uuid::new_v4();
    state
        .sockets
        .lock()
        .await
        .insert(client.clone(), (connection_id, sender));
    let sink = Arc::new(WebSink::new(state.clone(), client.clone()));
    let resumed = state.bridge.has_persisted("web", &client).unwrap_or(false);
    let _ = sink.emit(json!({"type":"connected","clientId":client,"hasSession":resumed,"state":if resumed {"resumed"} else {"new"}})).await;
    let output = tokio::spawn(async move {
        while let Some(message) = receiver.recv().await {
            if writer
                .send(Message::Text(message.to_string().into()))
                .await
                .is_err()
            {
                break;
            }
        }
    });
    let capacity = Arc::new(Semaphore::new(8));
    let mut tasks = tokio::task::JoinSet::new();
    while let Some(Ok(message)) = reader.next().await {
        let Message::Text(text) = message else {
            continue;
        };
        let value: Value = match serde_json::from_str(&text) {
            Ok(value) => value,
            Err(_) => {
                let _ = sink
                    .emit(json!({"type":"error","message":"Invalid JSON"}))
                    .await;
                continue;
            }
        };
        if value["type"] != "message" || !value["text"].is_string() {
            continue;
        }
        let images = match validate_images(&value["images"], &state.config["attachments"]) {
            Ok(images) => images,
            Err(error) => {
                let _ = sink
                    .emit(json!({"type":"error","message":error.to_string()}))
                    .await;
                continue;
            }
        };
        let Some(text) = value["text"].as_str() else {
            continue;
        };
        if text.trim().starts_with('/') && !images.is_empty() {
            let _ = sink.emit(json!({"type":"error","message":"Image attachments cannot be sent with commands."})).await;
            continue;
        }
        let permit = match capacity.clone().try_acquire_owned() {
            Ok(permit) => permit,
            Err(_) => {
                let _ = sink
                    .emit(json!({"type":"error","message":"Input queue full"}))
                    .await;
                continue;
            }
        };
        let bridge = state.bridge.clone();
        let sink = WebSink::new(state.clone(), client.clone());
        let mut event = Event::web(&client, text);
        event.images = images;
        tasks.spawn(async move {
            let _permit = permit;
            let _ = bridge.handle_web(event, &sink).await;
        });
        while tasks.try_join_next().is_some() {}
    }
    tasks.detach_all();
    output.abort();
    let mut sockets = state.sockets.lock().await;
    if sockets
        .get(&client)
        .is_some_and(|(id, _)| *id == connection_id)
    {
        sockets.remove(&client);
    }
    state.clients.fetch_sub(1, Ordering::Relaxed);
}

pub fn validate_images(raw: &Value, limits: &Value) -> Result<Vec<Value>> {
    if raw.is_null() {
        return Ok(vec![]);
    }
    let images = raw
        .as_array()
        .ok_or_else(|| anyhow::anyhow!("Images must be an array."))?;
    if images.is_empty() {
        return Ok(vec![]);
    }
    if limits["enabled"] != true {
        bail!("Image attachments are disabled.");
    }
    if images.len() as u64 > limits["maxFilesPerMessage"].as_u64().unwrap_or(1) {
        bail!("Too many image attachments.");
    }
    let maximum = limits["maxFileBytes"].as_u64().unwrap_or(5 * 1024 * 1024);
    let allowed = crate::config::strings(limits, "allowedMimeTypes");
    let mut output = vec![];
    for image in images {
        let mime = image["mimeType"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("Image MIME type is required."))?;
        if !allowed.iter().any(|allowed| allowed == mime) {
            bail!("Unsupported image MIME type.");
        }
        let data = image["data"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("Image data is required."))?;
        if data.len() as u64 > maximum.div_ceil(3) * 4 || data.is_empty() {
            bail!("Image exceeds byte limit.");
        }
        let bytes = base64::engine::general_purpose::STANDARD
            .decode(data)
            .map_err(|_| anyhow::anyhow!("Invalid base64 image."))?;
        if bytes.len() as u64 > maximum {
            bail!("Image exceeds byte limit.");
        }
        let format =
            image::guess_format(&bytes).map_err(|_| anyhow::anyhow!("Invalid image signature."))?;
        if format.to_mime_type() != mime {
            bail!("Image MIME type does not match signature.");
        }
        let (width, height) = image::ImageReader::with_format(Cursor::new(bytes), format)
            .into_dimensions()
            .map_err(|_| anyhow::anyhow!("Invalid image dimensions."))?;
        if width == 0
            || height == 0
            || width as u64 > limits["maxWidth"].as_u64().unwrap_or(4096)
            || height as u64 > limits["maxHeight"].as_u64().unwrap_or(4096)
            || width as u64 * height as u64 > limits["maxPixels"].as_u64().unwrap_or(20_000_000)
        {
            bail!("Image exceeds dimension or pixel limit.");
        }
        output.push(json!({"type":"image","mimeType":mime,"data":data}));
    }
    Ok(output)
}
