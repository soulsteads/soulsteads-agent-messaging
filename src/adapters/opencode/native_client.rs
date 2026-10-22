use crate::{
    backend::{InlineImage, Options, Output, Response},
    native_queue::NativeQueue,
    native_server::NativeServer,
};
use anyhow::{Context, Result, bail};
use futures_util::StreamExt;
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    path::{Path, PathBuf},
    sync::{
        Arc, Weak,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio::sync::{Mutex, broadcast, mpsc, watch};

#[derive(Default)]
struct State {
    session: Option<String>,
    admitted: Vec<String>,
    compactions: Vec<String>,
    baseline: HashSet<String>,
    idle: Value,
    output: Output,
    stop: Option<String>,
    model: Value,
    models: Vec<Value>,
    agents: Vec<Value>,
    commands: Vec<Value>,
    agent: String,
    tools: BTreeMap<String, Value>,
    settled_tools: HashSet<String>,
    command: bool,
    cancelled: bool,
    collector_error: Option<String>,
    partial_text: String,
}
#[derive(Default)]
struct Permissions {
    generation: u64,
    handler: Option<mpsc::Sender<crate::acp::PermissionRequest>>,
    form_handler: Option<mpsc::Sender<FormRequest>>,
    seen: HashSet<String>,
    seen_forms: HashSet<String>,
    pending: HashMap<String, Arc<AtomicBool>>,
    elapsed: Duration,
    since: Option<tokio::time::Instant>,
}
impl Permissions {
    fn clear(&mut self) {
        self.generation += 1;
        self.handler = None;
        self.form_handler = None;
        self.seen_forms.clear();
        self.seen.clear();
        for current in self.pending.values() {
            current.store(false, Ordering::SeqCst);
        }
    }
    fn finish_wait(&mut self, id: &str) {
        self.pending.remove(id);
        if self.pending.is_empty()
            && let Some(since) = self.since.take()
        {
            self.elapsed += since.elapsed();
        }
    }
    fn credit(&self) -> Duration {
        (self.elapsed + self.since.map_or(Duration::ZERO, |since| since.elapsed()))
            .min(Duration::from_secs(45 * 60))
    }
}
#[derive(Clone, Debug)]
pub struct PromptFailure {
    pub text: String,
    pub output: Output,
    pub stop_reason: Option<String>,
    pub message: String,
}
impl std::fmt::Display for PromptFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.message)
    }
}
impl std::error::Error for PromptFailure {}
type Completion = std::result::Result<(String, Output, Option<String>), PromptFailure>;
struct Inner {
    api: NativeQueue,
    server: Mutex<Option<NativeServer>>,
    cwd: PathBuf,
    timeout: Duration,
    active: AtomicBool,
    closed: AtomicBool,
    shutdown_gate: Mutex<()>,
    collector: Mutex<Option<tokio::task::JoinHandle<()>>>,
    state: Mutex<State>,
    admission: Mutex<()>,
    events: broadcast::Sender<Value>,
    completion: watch::Sender<Option<Completion>>,
    permissions: std::sync::Mutex<Permissions>,
    permission_changes: watch::Sender<u64>,
}
#[derive(Clone)]
pub struct Client {
    inner: Arc<Inner>,
}
pub(crate) struct PermissionGuard(Weak<Inner>, u64);
pub(crate) type FormGuard = PermissionGuard;
struct FormWaitGuard(Weak<Inner>, String, u64, Arc<AtomicBool>);
impl Drop for FormWaitGuard {
    fn drop(&mut self) {
        self.3.store(false, Ordering::Release);
        if let Some(inner) = self.0.upgrade() {
            let mut interactions = inner.permissions.lock().unwrap();
            if interactions.generation == self.2 {
                interactions.finish_wait(&self.1);
            }
        }
    }
}
pub(crate) struct FormRequest {
    pub form: Value,
    pub current: Arc<AtomicBool>,
    pub reply: tokio::sync::oneshot::Sender<Option<Value>>,
}
impl Drop for PermissionGuard {
    fn drop(&mut self) {
        if let Some(inner) = self.0.upgrade() {
            let mut permissions = inner.permissions.lock().unwrap();
            if permissions.generation == self.1 {
                permissions.clear();
                inner
                    .permission_changes
                    .send_modify(|version| *version += 1);
            }
        }
    }
}
impl Client {
    pub async fn connect(options: Options, servers: Vec<Value>) -> Result<Self> {
        let servers = mcp_servers(servers)?;
        let cwd = options.cwd.clone();
        let timeout = options.timeout;
        let server = NativeServer::start(options).await?;
        let api = server.client();
        let client = Self {
            inner: Arc::new(Inner {
                api,
                server: Mutex::new(Some(server)),
                cwd,
                timeout,
                active: AtomicBool::new(false),
                closed: AtomicBool::new(false),
                state: Mutex::new(State::default()),
                shutdown_gate: Mutex::new(()),
                collector: Mutex::new(None),
                admission: Mutex::new(()),
                events: broadcast::channel(256).0,
                completion: watch::channel(None).0,
                permissions: std::sync::Mutex::new(Permissions::default()),
                permission_changes: watch::channel(0).0,
            }),
        };
        let registration = async {
            let query = client.location_query()?;
            for (name, config) in &servers {
                client
                    .api(
                        reqwest::Method::PUT,
                        &format!("/api/experimental/mcp/{name}?{query}"),
                        Some(json!({"config":config})),
                    )
                    .await?;
            }
            if !servers.is_empty() {
                let status = client
                    .api(reqwest::Method::GET, &format!("/api/mcp?{query}"), None)
                    .await?;
                if !servers.iter().all(|(name, _)| {
                    status["data"].as_array().is_some_and(|items| {
                        items.iter().any(|item| {
                            item["name"] == *name && item["status"]["status"] == "connected"
                        })
                    })
                }) {
                    bail!("Native MCP registration was not confirmed; session not created");
                }
            }
            Ok::<_, anyhow::Error>(())
        }
        .await;
        if let Err(error) = registration {
            let _ = client.shutdown().await;
            return Err(error);
        }
        Ok(client)
    }
    async fn api(&self, method: reqwest::Method, path: &str, body: Option<Value>) -> Result<Value> {
        if self.inner.closed.load(Ordering::SeqCst) {
            bail!("Native server is closed");
        }
        tokio::time::timeout(
            self.inner.timeout,
            self.inner.api.request(method, path, body),
        )
        .await
        .context("Native API timed out")?
    }
    async fn session(&self) -> Result<String> {
        self.session_id()
            .await
            .context("No native session selected")
    }
    pub fn subscribe(&self) -> broadcast::Receiver<Value> {
        self.inner.events.subscribe()
    }
    pub fn working_directory(&self) -> &Path {
        &self.inner.cwd
    }
    pub fn accepts_followups(&self) -> bool {
        self.inner.active.load(Ordering::SeqCst)
    }
    pub async fn session_id(&self) -> Option<String> {
        self.inner.state.lock().await.session.clone()
    }
    pub async fn inline_images(&self) -> Vec<InlineImage> {
        self.inner.state.lock().await.output.images.clone()
    }
    pub async fn prompt_observation(&self) -> (Output, Option<String>, Value) {
        let state = self.inner.state.lock().await;
        let mut output = state.output.clone();
        output.collector_closed = !self.accepts_followups();
        (output, state.stop.clone(), state.model.clone())
    }
    pub async fn config_options(&self) -> Vec<Value> {
        let state = self.inner.state.lock().await;
        let choices: Vec<Value> = state.models.iter().filter_map(|model| Some(json!({"value":format!("{}/{}", model["providerID"].as_str()?, model["id"].as_str()?),"name":model["name"]}))).collect();
        let mut options = vec![
            json!({"id":"model","category":"model","name":"Model","type":"select","currentValue":model_name(&state.model),"options":choices}),
            json!({"id":"mode","category":"mode","name":"Agent","type":"select","currentValue":state.agent,"options":state.agents.iter().map(|agent| json!({"value":agent["id"],"name":agent["name"]})).collect::<Vec<_>>()}),
        ];
        let variants = state
            .models
            .iter()
            .find(|model| {
                model["id"] == state.model["id"] && model["providerID"] == state.model["providerID"]
            })
            .and_then(|model| model["variants"].as_array())
            .cloned()
            .unwrap_or_default();
        if !variants.is_empty() {
            options.push(json!({"id":"effort","category":"thought_level","name":"Model variant","type":"select","currentValue":state.model["variant"].as_str().unwrap_or("default"),"options":std::iter::once(json!({"value":"default","name":"Default"})).chain(variants.iter().filter_map(|variant| Some(json!({"value":variant["id"].as_str()?,"name":variant["id"]})))).collect::<Vec<_>>()}));
        }
        options
    }
    pub async fn commands(&self) -> Vec<Value> {
        self.inner.state.lock().await.commands.clone()
    }
    pub async fn queued_inputs(&self) -> Result<Vec<Value>> {
        let session = self.session().await?;
        tokio::time::timeout(self.inner.timeout, self.inner.api.inbox(&session))
            .await
            .context("Native inbox read timed out")?
    }
    pub async fn modes(&self) -> Value {
        let state = self.inner.state.lock().await;
        json!({"currentModeId":state.agent,"availableModes":state.agents.iter().map(|agent| json!({"id":agent["id"],"name":agent["name"],"description":agent["description"]})).collect::<Vec<_>>()})
    }
    async fn refresh(&self, session: &str) -> Result<()> {
        self.refresh_for_model(session, None).await
    }
    fn location_query(&self) -> Result<String> {
        Ok(url::form_urlencoded::Serializer::new(String::new())
            .append_pair(
                "location[directory]",
                self.inner.cwd.to_str().context("Invalid native cwd")?,
            )
            .finish())
    }
    async fn refresh_for_model(
        &self,
        session: &str,
        requested: Option<(&str, &str)>,
    ) -> Result<()> {
        tokio::time::timeout(self.inner.timeout, async {
            let query = self.location_query()?;
            let info = self.info(session).await?;
            let (selected, models) = loop {
                let default = if info["model"].is_null() {
                    self
                        .api(
                            reqwest::Method::GET,
                            &format!("/api/model/default?{query}"),
                            None,
                        )
                        .await?["data"].clone()
                } else {
                    info["model"].clone()
                };
                if default.is_null() {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    continue;
                }
                let mut selected = json!({"providerID":default["providerID"].as_str().context("Native default model is unavailable")?,"id":default["id"].as_str().context("Native default model has no ID")?});
                if let Some(variant) = default["variant"].as_str() {
                    selected["variant"] = variant.into();
                }
                let snapshot = self.api(reqwest::Method::GET, &format!("/api/model?{query}"), None).await?;
                let models: Vec<Value> = snapshot["data"].as_array().context("Invalid native models catalog")?.iter().filter(|model| model["enabled"] == true).cloned().collect();
                let current_present = models.iter().any(|model| model["providerID"] == selected["providerID"] && model["id"] == selected["id"]);
                let requested_present = requested.is_none_or(|(provider, name)| models.iter().any(|model| model["providerID"] == provider && model["id"] == name));
                if current_present && requested_present {
                    break (selected, models);
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            };
        let agents = self.api(reqwest::Method::GET, &format!("/api/agent?{query}"), None).await?;
        let commands = self.api(reqwest::Method::GET, &format!("/api/command?{query}"), None).await?;
        let mut state = self.inner.state.lock().await;
        state.models = models;
        state.agents = agents["data"]
            .as_array()
            .context("Invalid native agents catalog")?
            .iter()
            .filter(|agent| agent["hidden"] != true && agent["mode"] != "subagent")
            .cloned()
            .collect();
        state.commands = commands["data"]
            .as_array()
            .context("Invalid native commands catalog")?
            .clone();
        // Compaction is implemented by this adapter, not the server's dynamic
        // command endpoint. Advertise it so public ingress can admit it too.
        if !state.commands.iter().any(|command| command["name"] == "compact") {
            state.commands.push(serde_json::json!({"name":"compact","description":"Compact conversation context"}));
        }
        state.model = selected;
        state.agent = info["agent"].as_str().unwrap_or("").into();
        Ok::<_, anyhow::Error>(())
        }).await.context("Native advertised model catalog did not settle before timeout")?
    }
    pub async fn new_session(&self) -> Result<String> {
        let _gate = self.inner.admission.lock().await;
        if self.inner.closed.load(Ordering::SeqCst) {
            bail!("Native server is closed");
        }
        if self.accepts_followups() {
            bail!("Native session is busy");
        }
        let id = tokio::time::timeout(
            self.inner.timeout,
            self.inner.api.create_session(
                self.inner.cwd.to_str().context("Invalid native cwd")?,
                None,
                None,
            ),
        )
        .await??;
        self.refresh(&id).await?;
        self.inner.state.lock().await.session = Some(id.clone());
        Ok(id)
    }
    async fn info(&self, session: &str) -> Result<Value> {
        if !NativeQueue::valid_id(session, "ses_") {
            bail!("Invalid native session ID");
        }
        let result = self
            .api(
                reqwest::Method::GET,
                &format!("/api/session/{session}"),
                None,
            )
            .await?;
        if result["data"]["id"] != session {
            bail!("Native session identity mismatch");
        }
        Ok(result["data"].clone())
    }
    async fn messages(&self, session: &str) -> Result<Vec<Value>> {
        let mut rows = vec![];
        let mut bytes = 0usize;
        let mut identities = HashSet::new();
        let mut cursor: Option<String> = None;
        let mut cursors = HashSet::new();
        let mut pages = 0usize;
        loop {
            if pages >= 100 {
                bail!("Native history exceeded page limit");
            }
            pages += 1;
            let mut path = format!("/api/session/{session}/message?limit=100");
            if let Some(cursor) = &cursor {
                path.push_str("&cursor=");
                path.extend(url::form_urlencoded::byte_serialize(cursor.as_bytes()));
            } else {
                path.push_str("&order=desc");
            }
            let result = self.api(reqwest::Method::GET, &path, None).await?;
            bytes += serde_json::to_vec(&result)?.len();
            if bytes > 8 * 1024 * 1024 {
                bail!("Native history exceeds byte limit");
            }
            for row in result["data"]
                .as_array()
                .context("Invalid native message list")?
            {
                let id = row["id"]
                    .as_str()
                    .filter(|id| NativeQueue::valid_id(id, "msg_"))
                    .context("Invalid native history message ID")?;
                if !identities.insert(id.to_owned()) {
                    bail!("Duplicate native history message ID");
                }
                match row["type"].as_str() {
                    Some("user") if !row["text"].is_string() => {
                        bail!("Invalid native user history text")
                    }
                    Some("assistant") => {
                        for part in row["content"]
                            .as_array()
                            .context("Invalid native assistant content")?
                        {
                            if part["type"] == "text" && !part["text"].is_string() {
                                bail!("Invalid native assistant text");
                            }
                        }
                    }
                    None => bail!("Missing native history message type"),
                    _ => {}
                }
                rows.push(row.clone());
            }
            if rows.len() > 10000 {
                bail!("Native history exceeds supported bound");
            }
            cursor = Self::next_cursor(&result)?;
            if let Some(cursor) = &cursor {
                if !cursors.insert(cursor.clone()) {
                    bail!("Native history cursor repeated");
                }
            } else {
                break;
            }
        }
        rows.reverse();
        Ok(rows)
    }
    fn next_cursor(snapshot: &Value) -> Result<Option<String>> {
        let cursor = snapshot["cursor"]
            .as_object()
            .context("Invalid native pagination cursor")?;
        match cursor.get("next") {
            None | Some(Value::Null) => Ok(None),
            Some(Value::String(next)) if !next.is_empty() && next.len() <= 16 * 1024 => {
                Ok(Some(next.clone()))
            }
            _ => bail!("Invalid native next-page cursor"),
        }
    }
    pub async fn load_session(&self, session: &str, _resume: bool) -> Result<Vec<Value>> {
        let _gate = self.inner.admission.lock().await;
        if self.accepts_followups() {
            bail!("Native session is busy");
        }
        let info = self.info(session).await?;
        if info["location"]["directory"].as_str() != self.inner.cwd.to_str() {
            bail!("Native session workspace differs");
        }
        self.require_idle(session).await?;
        let rows = self.messages(session).await?;
        self.refresh(session).await?;
        self.inner.state.lock().await.session = Some(session.into());
        Ok(rows
            .iter()
            .filter_map(|row| {
                let kind = match row["type"].as_str()? {
                    "user" => "user_message_chunk",
                    "assistant" => "agent_message_chunk",
                    _ => return None,
                };
                let text = message_text(row);
                Some(json!({"sessionUpdate":kind,"content":{"type":"text","text":text}}))
            })
            .collect())
    }
    pub async fn list_sessions(&self) -> Result<Vec<Value>> {
        let mut sessions = Vec::new();
        let mut seen = HashSet::new();
        let mut cursor = None::<String>;
        let mut bytes = 0usize;
        for _ in 0..100 {
            let query = {
                let mut query = url::form_urlencoded::Serializer::new(String::new());
                query.append_pair("limit", "100");
                if let Some(cursor) = &cursor {
                    query.append_pair("cursor", cursor);
                }
                query.finish()
            };
            let result = self
                .api(reqwest::Method::GET, &format!("/api/session?{query}"), None)
                .await?;
            bytes += serde_json::to_vec(&result)?.len();
            if bytes > 8 * 1024 * 1024 {
                bail!("Native session listing exceeds byte limit");
            }
            for info in result["data"]
                .as_array()
                .context("Invalid native session list")?
            {
                if !info["id"]
                    .as_str()
                    .is_some_and(|id| NativeQueue::valid_id(id, "ses_"))
                {
                    bail!("Invalid native listed session ID");
                }
                sessions.push(json!({"sessionId":info["id"],"title":info["title"],"cwd":info["location"]["directory"],"updatedAt":info["time"]["updated"]}));
            }
            if sessions.len() > 10000 {
                bail!("Native session listing exceeds row limit");
            }
            cursor = Self::next_cursor(&result)?;
            let Some(cursor) = &cursor else {
                return Ok(sessions);
            };
            if !seen.insert(cursor.clone()) {
                bail!("Native session cursor repeated");
            }
        }
        bail!("Native session listing exceeded page limit")
    }
    pub async fn close_session(&self) -> Result<()> {
        self.cancel().await?;
        self.wait_collector().await?;
        self.inner.state.lock().await.session = None;
        Ok(())
    }
    pub async fn delete_session(&self) -> Result<()> {
        self.cancel().await?;
        self.wait_collector().await?;
        let session = self.session().await?;
        self.api(
            reqwest::Method::DELETE,
            &format!("/api/session/{session}"),
            None,
        )
        .await?;
        self.inner.state.lock().await.session = None;
        Ok(())
    }
    async fn admit(&self, text: &str, files: Vec<Value>) -> Result<()> {
        let session = self.session().await?;
        let compact = text.split_whitespace().next() == Some("/compact");
        if compact && (text.trim() != "/compact" || !files.is_empty()) {
            bail!("Native compaction does not accept arguments or images; not submitted");
        }
        if !compact && let Some(command) = text.strip_prefix('/') {
            let (name, arguments) = command
                .split_once(char::is_whitespace)
                .unwrap_or((command, ""));
            if self
                .commands()
                .await
                .iter()
                .any(|item| item["name"] == name)
            {
                {
                    let state = self.inner.state.lock().await;
                    if state.command || !state.admitted.is_empty() || !state.compactions.is_empty()
                    {
                        bail!(
                            "Native command callbacks cannot be safely attributed to a busy turn; not submitted"
                        );
                    }
                }
                tokio::time::timeout(
                    self.inner.timeout,
                    self.inner.api.no_content(
                        reqwest::Method::POST,
                        &format!("/api/session/{session}/command"),
                        Some(
                            json!({"name":name,"text":arguments,"files":files,"delivery":"queue"}),
                        ),
                    ),
                )
                .await
                .context("Native command acknowledgement timed out; not replaying")??;
                self.inner.state.lock().await.command = true;
                return Ok(());
            }
        }
        let id = format!("msg_{}", uuid::Uuid::new_v4().simple());
        {
            let mut state = self.inner.state.lock().await;
            if compact {
                state.compactions.push(id.clone());
            } else {
                state.admitted.push(id.clone());
            }
        }
        let result = if compact {
            self.api(
                reqwest::Method::POST,
                &format!("/api/session/{session}/compact"),
                Some(json!({"id":id,"delivery":"queue"})),
            )
            .await
            .map(|ack| ack["data"]["id"] == id)
        } else {
            tokio::time::timeout(
                self.inner.timeout,
                self.inner.api.enqueue_files(&session, &id, text, files),
            )
            .await
            .context("Native admission timed out")
            .and_then(|result| result.map(|_| true))
        };
        if matches!(result, Ok(true)) {
            return Ok(());
        }
        let inbox = self
            .api(
                reqwest::Method::GET,
                &format!("/api/session/{session}/inbox"),
                None,
            )
            .await?;
        let history = self.messages(&session).await?;
        if inbox["data"]
            .as_array()
            .context("Invalid native inbox")?
            .iter()
            .chain(history.iter())
            .any(|item| item["id"] == id)
        {
            return Ok(());
        }
        self.inner
            .state
            .lock()
            .await
            .admitted
            .retain(|item| item != &id);
        self.inner
            .state
            .lock()
            .await
            .compactions
            .retain(|item| item != &id);
        bail!("Native admission was not confirmed; not replaying uncertain submission")
    }
    pub async fn prompt(&self, text: &str, images: Vec<Value>) -> Result<Response> {
        let files = image_files(images)?;
        let mut completion = self.inner.completion.subscribe();
        {
            let _gate = self.inner.admission.lock().await;
            if self.accepts_followups() {
                bail!("Native session is busy");
            }
            let session = self.session().await?;
            self.require_idle(&session).await?;
            let info = self.info(&session).await?;
            let baseline = self.messages(&session).await?;
            let stream =
                tokio::time::timeout(self.inner.timeout, self.inner.api.events()).await??;
            {
                let mut state = self.inner.state.lock().await;
                state.output = Output::default();
                state.stop = None;
                state.admitted.clear();
                state.compactions.clear();
                state.tools.clear();
                state.settled_tools.clear();
                state.command = false;
                state.cancelled = false;
                state.collector_error = None;
                state.partial_text.clear();
                state.baseline = baseline
                    .iter()
                    .filter_map(|row| row["id"].as_str().map(str::to_owned))
                    .collect();
                state.idle = info["time"]["idle"].clone();
            }
            self.inner.completion.send_replace(None);
            {
                let mut permissions = self.inner.permissions.lock().unwrap();
                permissions.seen.clear();
                permissions.pending.clear();
                permissions.elapsed = Duration::ZERO;
                permissions.since = None;
            }
            self.inner.active.store(true, Ordering::SeqCst);
            if let Err(error) = self.admit(text, files).await {
                self.inner.active.store(false, Ordering::SeqCst);
                self.inner.state.lock().await.output.output_lost = true;
                return Err(error);
            }
            let client = self.clone();
            *self.inner.collector.lock().await = Some(tokio::spawn(async move {
                client.collect(session, stream).await;
            }));
        }
        loop {
            let result = completion.borrow_and_update().clone();
            if let Some(result) = result {
                let (text, output, stop_reason) = result.map_err(anyhow::Error::new)?;
                return Ok(Response {
                    text,
                    output,
                    stop_reason,
                });
            }
            completion
                .changed()
                .await
                .context("Native collector stopped")?;
        }
    }
    pub async fn forward(&self, text: &str) -> Result<()> {
        self.forward_content(text, vec![]).await
    }
    pub async fn forward_content(&self, text: &str, images: Vec<Value>) -> Result<()> {
        let files = image_files(images)?;
        let _gate = self.inner.admission.lock().await;
        if !self.accepts_followups() || self.inner.state.lock().await.cancelled {
            bail!("Native session is not running or was cancelled");
        }
        let state = self.inner.state.lock().await;
        if state.admitted.len() + state.compactions.len() >= 128 {
            bail!("Native queue is full");
        }
        drop(state);
        self.admit(text, files).await
    }
    async fn collect(
        &self,
        session: String,
        mut stream: futures_util::stream::BoxStream<'static, Result<Vec<u8>>>,
    ) {
        let mut decoder = crate::native_events::SseDecoder::new(1024 * 1024);
        let mut projector = crate::native_events::EventProjector::new(&session);
        let mut tick = tokio::time::interval(Duration::from_millis(100));
        let started = tokio::time::Instant::now();
        let deadline = started + self.inner.timeout;
        let result = 'collect: loop {
            tokio::select! {
                item = stream.next() => {
                    match item {
                        Some(Ok(bytes)) => for event in match decoder.feed_checked(&bytes) {
                            Ok(events) => events,
                            Err(_) => { self.inner.state.lock().await.output.output_lost = true; break self.reconcile_until(&session, deadline).await; }
                        } {
                            for update in projector.push(event.clone()) {
                                if matches!(update, crate::native_events::Update::ReplayWindowExceeded | crate::native_events::Update::Reconcile { .. }) {
                                    self.inner.state.lock().await.output.output_lost = true;
                                    break 'collect self.reconcile_until(&session, deadline).await;
                                }
                                if let crate::native_events::Update::TextDelta { text, .. } = &update {
                                    let mut state = self.inner.state.lock().await;
                                    state.output.raw_chars += text.encode_utf16().count(); state.output.chunks += 1;
                                    if state.output.raw_chars > 2 * 1024 * 1024 { state.collector_error = Some("Native text exceeded collector limit".into()); }
                                    else { state.partial_text.push_str(text); }
                                    let _ = self.inner.events.send(json!({"sessionUpdate":"agent_message_chunk","content":{"type":"text","text":text}}));
                                }
                                if let crate::native_events::Update::Tool { kind, data } = update { self.tool(&kind, &data).await; }
                            }
                            if event["type"] == "permission.asked" && event["data"]["sessionID"] == session
                                && let Err(error) = self.permission(&session, event["data"].clone()).await {
                                self.inner.state.lock().await.collector_error = Some(error.to_string());
                            }
                        },
                        _ => { self.inner.state.lock().await.output.output_lost = true; break self.reconcile_until(&session, deadline).await; }
                    }
                },
                _ = tick.tick() => {
                    let deadline = started + self.inner.timeout + self.inner.permissions.lock().unwrap().credit();
                    if tokio::time::Instant::now() >= deadline { break Err(anyhow::anyhow!("Native execution timed out; completion was not confirmed")); }
                    if let Err(error) = self.pending_requests(&session).await { break Err(error); }
                    if let Some(error) = self.inner.state.lock().await.collector_error.take() { break Err(anyhow::Error::msg(error)); }
                    match self.reconcile(&session).await { Ok(Some(text)) => break Ok(text), Ok(None) => {}, Err(error) => break Err(error) }
                },
            }
        };
        if result.is_err() {
            self.inner.state.lock().await.output.output_lost = true;
            let _ = self.cancel().await;
        }
        if result.is_err() {
            self.finish(result).await;
        }
    }
    async fn reconcile_until(
        &self,
        session: &str,
        deadline: tokio::time::Instant,
    ) -> Result<String> {
        loop {
            if tokio::time::Instant::now()
                >= deadline + self.inner.permissions.lock().unwrap().credit()
            {
                bail!("Native disconnected execution timed out");
            }
            self.pending_requests(session).await?;
            if let Some(error) = self.inner.state.lock().await.collector_error.take() {
                bail!(error);
            }
            if let Some(text) = self.reconcile(session).await? {
                return Ok(text);
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
    async fn reconcile(&self, session: &str) -> Result<Option<String>> {
        let _gate = self.inner.admission.lock().await;
        let active = self
            .api(reqwest::Method::GET, "/api/session/active", None)
            .await?;
        if active["data"]
            .as_object()
            .context("Invalid native active sessions")?
            .contains_key(session)
        {
            return Ok(None);
        }
        let info = self.info(session).await?;
        let inbox = self.inner.api.inbox(session).await?;
        let rows = self.messages(session).await?;
        self.recover_tools_from_rows(&rows).await;
        let mut state = self.inner.state.lock().await;
        state.partial_text = rows
            .iter()
            .filter(|row| {
                !state.baseline.contains(row["id"].as_str().unwrap_or(""))
                    && row["type"] == "assistant"
            })
            .map(message_text)
            .collect();
        state.output.raw_chars = state.partial_text.encode_utf16().count();
        if let Some(error) = state.collector_error.take() {
            bail!(error);
        }
        if !info["time"]["idle"].is_null()
            && info["time"]["idle"] != state.idle
            && info["outcome"] == "failed"
        {
            state.stop = Some("failed".into());
            bail!("Native execution failed; partial result retained");
        }
        if state.cancelled && inbox.is_empty() {
            state.stop = Some("cancelled".into());
            let text: String = rows
                .iter()
                .filter(|row| {
                    !state.baseline.contains(row["id"].as_str().unwrap_or(""))
                        && row["type"] == "assistant"
                })
                .map(message_text)
                .collect();
            drop(state);
            self.complete_locked(Ok(text.clone())).await;
            return Ok(Some(text));
        }
        if info["time"]["idle"].is_null() || info["time"]["idle"] == state.idle {
            return Ok(None);
        }
        match info["outcome"].as_str() {
            Some("interrupted") => {
                state.stop = Some("cancelled".into());
                let text: String = rows
                    .iter()
                    .filter(|row| {
                        !state.baseline.contains(row["id"].as_str().unwrap_or(""))
                            && row["type"] == "assistant"
                    })
                    .map(message_text)
                    .collect();
                drop(state);
                self.complete_locked(Ok(text.clone())).await;
                return Ok(Some(text));
            }
            Some("succeeded") => {}
            _ => return Ok(None),
        }
        if state.command {
            if !inbox.is_empty() {
                return Ok(None);
            }
            if !rows.iter().any(|row| {
                row["type"] == "assistant"
                    && !state.baseline.contains(row["id"].as_str().unwrap_or(""))
            }) {
                bail!("Native command was acknowledged but a durable result was not confirmed");
            }
        }
        if state.admitted.iter().any(|id| {
            inbox.iter().any(|row| row["id"] == *id)
                || !rows
                    .iter()
                    .any(|row| row["id"] == *id && row["type"] == "user")
        }) {
            return Ok(None);
        }
        if !state.compactions.is_empty() {
            if state
                .compactions
                .iter()
                .any(|id| inbox.iter().any(|row| row["id"] == *id))
            {
                return Ok(None);
            }
            if state.compactions.iter().any(|id| {
                !rows.iter().any(|row| {
                    row["id"] == *id
                        && !state.baseline.contains(id)
                        && row["type"] == "compaction"
                        && row["reason"] == "manual"
                        && row["status"] == "completed"
                })
            }) {
                bail!("Native compaction completion was not confirmed by durable history");
            }
        }
        let mut text: String = rows
            .iter()
            .filter(|row| {
                !state.baseline.contains(row["id"].as_str().unwrap_or(""))
                    && row["type"] == "assistant"
            })
            .map(message_text)
            .collect();
        if text.is_empty() && !state.compactions.is_empty() {
            // Reached only after successful idle state and durable manual
            // compaction confirmation; ordinary empty prompts stay empty.
            text = "Context compacted".into();
        }
        state.output.raw_chars = text.encode_utf16().count();
        state.stop = Some("end_turn".into());
        drop(state);
        self.complete_locked(Ok(text.clone())).await;
        Ok(Some(text))
    }
    async fn finish(&self, result: Result<String>) {
        let _gate = self.inner.admission.lock().await;
        self.complete_locked(result).await;
    }
    async fn complete_locked(&self, result: Result<String>) {
        self.inner.active.store(false, Ordering::SeqCst);
        self.inner.permissions.lock().unwrap().clear();
        self.inner
            .permission_changes
            .send_modify(|version| *version += 1);
        let mut state = self.inner.state.lock().await;
        state.output.collector_closed = true;
        if result.is_err() && state.stop.is_none() {
            state.stop = Some("failed".into());
        }
        let result = result
            .map(|text| (text, state.output.clone(), state.stop.clone()))
            .map_err(|error| PromptFailure {
                text: state.partial_text.clone(),
                output: state.output.clone(),
                stop_reason: state.stop.clone(),
                message: error.to_string(),
            });
        self.inner.completion.send_replace(Some(result));
    }
    async fn tool(&self, kind: &str, data: &Value) {
        let (Some(message), Some(call)) =
            (data["assistantMessageID"].as_str(), data["id"].as_str())
        else {
            return;
        };
        let identity = format!("{message}:{call}");
        let mut state = self.inner.state.lock().await;
        state.output.had_tool_activity = true;
        if state.settled_tools.contains(&identity) {
            return;
        }
        if state.tools.len() >= 1024 && !state.tools.contains_key(&identity) {
            state.output.output_lost = true;
            state.collector_error = Some("Native tool collector exceeded limit".into());
            return;
        }
        let fresh = !state.tools.contains_key(&identity);
        let tool = state.tools.entry(identity.clone()).or_insert_with(|| json!({"toolCallId":identity,"sessionUpdate":"tool_call","status":"pending","title":"unknown","kind":"other"}));
        match kind {
            "session.tool.input.started" => {
                tool["title"] = data["name"].clone();
                tool["kind"] = if data["name"] == "bash" {
                    "execute"
                } else {
                    "other"
                }
                .into();
            }
            "session.tool.input.ended" => {
                if let Some(text) = data["text"].as_str() {
                    tool["rawInput"] = serde_json::from_str(text).unwrap_or(Value::Null);
                }
            }
            "session.tool.called" => {
                tool["rawInput"] = data["input"].clone();
                tool["status"] = "in_progress".into();
            }
            "session.tool.progress" => {
                tool["rawOutput"] = data["metadata"].clone();
            }
            "session.tool.success" | "session.tool.failed" => {
                tool["status"] = if kind == "session.tool.success" {
                    "completed"
                } else {
                    "failed"
                }
                .into();
                let mut content = vec![];
                for part in data["content"].as_array().into_iter().flatten() {
                    if part["type"] == "text" {
                        content.push(
                            json!({"type":"content","content":{"type":"text","text":part["text"]}}),
                        );
                    }
                }
                if kind == "session.tool.failed" {
                    content.push(json!({"type":"content","content":{"type":"text","text":"Native tool execution failed"}}));
                }
                tool["content"] = content.into();
            }
            _ => return,
        }
        let mut update = tool.clone();
        update["sessionUpdate"] = if fresh {
            "tool_call"
        } else {
            "tool_call_update"
        }
        .into();
        if fresh {
            state.output.tool_calls += 1;
        }
        if kind == "session.tool.success" || kind == "session.tool.failed" {
            update["sessionUpdate"] = "tool_call_update".into();
            state.settled_tools.insert(identity);
            let text: String = data["content"]
                .as_array()
                .into_iter()
                .flatten()
                .filter(|part| part["type"] == "text")
                .filter_map(|part| part["text"].as_str())
                .collect();
            if state.output.tool_results.len() + text.len() <= 2 * 1024 * 1024 {
                state.output.tool_results.push_str(&text);
                state.output.tool_results.push('\n');
            } else {
                state.output.output_lost = true;
                state.collector_error = Some("Native tool output exceeded limit".into());
            }
            for part in data["content"]
                .as_array()
                .into_iter()
                .flatten()
                .filter(|part| part["type"] == "file")
            {
                if state.output.images.len() >= 16 {
                    state.output.output_lost = true;
                    state.collector_error =
                        Some("Native media output exceeded attachment limit".into());
                    break;
                }
                let result = native_file(&self.inner.cwd, part);
                match result {
                    Ok(Some(image)) => {
                        state.output.images.push(image);
                        state.output.image_count = state.output.images.len();
                    }
                    Ok(None) => {
                        if let Some(uri) = part["uri"].as_str()
                            && let Ok(url) = url::Url::parse(uri)
                            && let Ok(path) = url.to_file_path()
                            && let Some(path) = path.to_str()
                            && !path.contains(['[', ']', '\n', '\r'])
                        {
                            state
                                .output
                                .tool_results
                                .push_str(&format!("[DOCLIBRARY_DOC]{path}[/DOCLIBRARY_DOC]\n"));
                        }
                    }
                    Err(_) => {
                        state.output.output_lost = true;
                        state.collector_error =
                            Some("Native media output was rejected by bridge safety policy".into());
                    }
                }
            }
        }
        update["collectorImageCount"] = state.output.image_count.into();
        let _ = self.inner.events.send(update);
    }
    async fn recover_tools(&self, session: &str) -> Result<()> {
        let rows = self.messages(session).await?;
        self.recover_tools_from_rows(&rows).await;
        Ok(())
    }
    async fn recover_tools_from_rows(&self, rows: &[Value]) {
        let baseline = self.inner.state.lock().await.baseline.clone();
        for row in rows.iter().filter(|row| {
            row["type"] == "assistant" && !baseline.contains(row["id"].as_str().unwrap_or(""))
        }) {
            for tool in row["content"]
                .as_array()
                .into_iter()
                .flatten()
                .filter(|part| part["type"] == "tool")
            {
                let base =
                    json!({"assistantMessageID":row["id"],"id":tool["id"],"name":tool["name"]});
                let identity = format!(
                    "{}:{}",
                    row["id"].as_str().unwrap_or(""),
                    tool["id"].as_str().unwrap_or("")
                );
                if !self
                    .inner
                    .state
                    .lock()
                    .await
                    .tools
                    .get(&identity)
                    .is_some_and(|known| known["title"] == tool["name"])
                {
                    self.tool("session.tool.input.started", &base).await;
                }
                let mut data = base;
                data["input"] = tool["state"]["input"].clone();
                if data["input"].is_object() {
                    self.tool("session.tool.called", &data).await;
                }
                data["metadata"] = tool["state"]["metadata"].clone();
                data["content"] = tool["state"]["content"].clone();
                match tool["state"]["status"].as_str() {
                    Some("completed") => self.tool("session.tool.success", &data).await,
                    Some("error") => self.tool("session.tool.failed", &data).await,
                    _ => {}
                }
            }
        }
    }
    async fn pending_requests(&self, session: &str) -> Result<()> {
        self.recover_tools(session).await?;
        let permissions = self
            .api(
                reqwest::Method::GET,
                &format!("/api/session/{session}/permission"),
                None,
            )
            .await?;
        for request in permissions["data"]
            .as_array()
            .context("Invalid native permission list")?
        {
            self.permission(session, request.clone()).await?;
        }
        let forms = self
            .api(
                reqwest::Method::GET,
                &format!("/api/session/{session}/form"),
                None,
            )
            .await?;
        for form in forms["data"]
            .as_array()
            .context("Invalid native form list")?
        {
            self.form(session, form.clone()).await?;
        }
        Ok(())
    }
    async fn form_detail(&self, session: &str, id: &str) -> Result<Value> {
        let result = self
            .api(
                reqwest::Method::GET,
                &format!("/api/session/{session}/form/{id}"),
                None,
            )
            .await?;
        let detail = &result["data"];
        if detail["id"] != id || detail["sessionID"] != session {
            bail!("Native form identity mismatch");
        }
        Ok(detail.clone())
    }
    async fn form(&self, session: &str, form: Value) -> Result<()> {
        let _gate = self.inner.admission.lock().await;
        if form["sessionID"] != session {
            bail!("Foreign native form refused");
        }
        let id = form["id"]
            .as_str()
            .filter(|id| NativeQueue::valid_id(id, "frm_"))
            .context("Invalid native form identity")?
            .to_owned();
        {
            let mut interactions = self.inner.permissions.lock().unwrap();
            if interactions.seen_forms.contains(&id) {
                return Ok(());
            }
            if interactions.seen_forms.len() >= 1024 {
                bail!("Native form collector exceeded limit");
            }
            interactions.seen_forms.insert(id.clone());
        }
        let detail = self.form_detail(session, &id).await?;
        if detail["state"]["status"] != "pending" {
            return Ok(());
        }
        self.recover_tools(session).await?;
        let identity = format!(
            "{}:{}",
            form["metadata"]["tool"]["messageID"].as_str().unwrap_or(""),
            form["metadata"]["tool"]["id"].as_str().unwrap_or("")
        );
        let eligible = {
            let state = self.inner.state.lock().await;
            !state.cancelled
                && self.accepts_followups()
                && state.session.as_deref() == Some(session)
                && form["metadata"]["kind"] == "question"
                && state.tools.get(&identity).is_some_and(|tool| {
                    tool["title"] == "question"
                        && matches!(tool["status"].as_str(), Some("pending" | "in_progress"))
                })
        };
        self.inner.state.lock().await.output.had_server_request = true;
        let current = Arc::new(AtomicBool::new(true));
        let (reply, receiver) = tokio::sync::oneshot::channel();
        let (sent, generation) = {
            let mut interactions = self.inner.permissions.lock().unwrap();
            let generation = interactions.generation;
            let sent = eligible
                && interactions.form_handler.as_ref().is_some_and(|handler| {
                    handler
                        .try_send(FormRequest {
                            form: form.clone(),
                            current: current.clone(),
                            reply,
                        })
                        .is_ok()
                });
            if sent {
                interactions.pending.insert(id.clone(), current.clone());
                interactions
                    .since
                    .get_or_insert_with(tokio::time::Instant::now);
            }
            (sent, generation)
        };
        if !sent {
            current.store(false, Ordering::Release);
            self.settle_form(session, &id, None).await?;
            let _ = self
                .inner
                .events
                .send(json!({"method":"bridge/form", "params":{"status":"cancelled"}}));
            return Ok(());
        }
        let client = self.clone();
        let session = session.to_owned();
        tokio::spawn(async move {
            let _guard = FormWaitGuard(
                Arc::downgrade(&client.inner),
                id.clone(),
                generation,
                current.clone(),
            );
            let mut receiver = receiver;
            let expiry = tokio::time::sleep(Duration::from_secs(45 * 60));
            tokio::pin!(expiry);
            let mut changed = client.inner.permission_changes.subscribe();
            let result = async {
                let answer = loop {
                    if !current.load(Ordering::Acquire) { return Ok(()); }
                    tokio::select! {
                        response = &mut receiver => break response.ok().flatten(),
                        _ = &mut expiry => break None,
                        _ = changed.changed() => { if !current.load(Ordering::Acquire) {return Ok(())} },
                        _ = tokio::time::sleep(Duration::from_millis(500)) => {
                            if client.form_detail(&session, &id).await?["state"]["status"] != "pending" {
                                current.store(false, Ordering::Release);
                                let _ = client.inner.events.send(json!({"method":"bridge/form", "params":{"status":"expired"}}));
                                return Ok(());
                            }
                        }
                    }
                };
                let _gate = client.inner.admission.lock().await;
                if !current.load(Ordering::Acquire) || !client.accepts_followups() {return Ok(())}
                client.settle_form(&session, &id, answer.clone()).await?;
                let _ = client.inner.events.send(json!({"method":"bridge/form", "params":{"status":if answer.is_some() {"answered"} else {"cancelled"}}}));
                Ok::<(), anyhow::Error>(())
            }.await;
            if result.is_err() {
                client.inner.state.lock().await.collector_error = Some(
                    "Native question response was not confirmed; it will not be replayed".into(),
                );
            }
        });
        Ok(())
    }
    async fn settle_form(&self, session: &str, id: &str, answer: Option<Value>) -> Result<()> {
        let detail = self.form_detail(session, id).await?;
        if detail["state"]["status"] != "pending" {
            bail!("Native question is no longer pending");
        }
        // Never replay an uncertain write. A readback can prove a lost acknowledgement.
        let acknowledgement = if let Some(value) = &answer {
            self.inner
                .api
                .no_content(
                    reqwest::Method::POST,
                    &format!("/api/session/{session}/form/{id}/reply"),
                    Some(json!({"answer":value})),
                )
                .await
        } else {
            self.inner
                .api
                .no_content(
                    reqwest::Method::DELETE,
                    &format!("/api/session/{session}/form/{id}"),
                    None,
                )
                .await
        };
        let readback = self.form_detail(session, id).await?;
        let verified = if let Some(value) = &answer {
            readback["state"]["status"] == "answered" && readback["state"]["answer"] == *value
        } else {
            readback["state"]["status"] == "cancelled"
        };
        if !verified {
            acknowledgement?;
            bail!("Native question response was not confirmed");
        }
        Ok(())
    }
    async fn permission(&self, session: &str, request: Value) -> Result<()> {
        let _gate = self.inner.admission.lock().await;
        if request["sessionID"] != session {
            bail!("Foreign native permission request refused");
        }
        let id = request["id"]
            .as_str()
            .filter(|id| NativeQueue::valid_id(id, "per_"))
            .context("Invalid native permission identity")?
            .to_owned();
        let identity = match (
            request["source"]["messageID"].as_str(),
            request["source"]["id"].as_str(),
        ) {
            (Some(message), Some(call)) if request["source"]["type"] == "tool" => {
                format!("{message}:{call}")
            }
            _ => String::new(),
        };
        self.recover_tools(session).await?;
        let tool = self.inner.state.lock().await.tools.get(&identity).cloned();
        if self.inner.state.lock().await.cancelled {
            return self.permission_reply(session, &id, false).await;
        }
        let mut changed = self.inner.permission_changes.subscribe();
        let (current, handler, generation) = {
            let mut permissions = self.inner.permissions.lock().unwrap();
            if permissions.seen.contains(&id) {
                return Ok(());
            }
            if permissions.seen.len() >= 1024 {
                bail!("Native permission collector exceeded limit");
            }
            permissions.seen.insert(id.clone());
            let current = Arc::new(AtomicBool::new(true));
            (current, permissions.handler.clone(), permissions.generation)
        };
        self.inner.state.lock().await.output.had_server_request = true;
        let eligible = request["action"] == "bash"
            && tool
                .as_ref()
                .is_some_and(|tool| tool["kind"] == "execute" && tool["rawInput"].is_object());
        let (reply, outcome) = tokio::sync::oneshot::channel();
        let sent = if eligible && let (Some(handler), Some(tool)) = (handler, tool) {
            let params = json!({"sessionId":session,"toolCall":tool,"options":[{"optionId":"native-once","kind":"allow_once"},{"optionId":"native-reject","kind":"reject_once"}]});
            handler
                .try_send(crate::acp::PermissionRequest {
                    params,
                    current: current.clone(),
                    reply,
                })
                .is_ok()
        } else {
            false
        };
        if !sent {
            self.permission_reply(session, &id, false).await?;
            let _ = self.inner.events.send(json!({"sessionUpdate":"permission_rejected","message":"Permission denied: native approval unavailable or unsupported for this request","sessionId":session}));
            return Ok(());
        }
        {
            let mut permissions = self.inner.permissions.lock().unwrap();
            if permissions.pending.is_empty() {
                permissions.since = Some(tokio::time::Instant::now());
            }
            permissions.pending.insert(id.clone(), current.clone());
        }
        let client = self.clone();
        let session = session.to_owned();
        tokio::spawn(async move {
            let result = tokio::select! {
                result = outcome => result.ok(),
                _ = changed.changed() => None,
                _ = tokio::time::sleep(Duration::from_secs(45 * 60)) => None,
            };
            let _gate = client.inner.admission.lock().await;
            let valid = {
                let permissions = client.inner.permissions.lock().unwrap();
                permissions.generation == generation && current.load(Ordering::SeqCst)
            } && client.accepts_followups();
            let once = valid
                && result.as_ref().is_some_and(|result| {
                    result["outcome"] == "selected" && result["optionId"] == "native-once"
                });
            if let Err(error) = client.permission_reply(&session, &id, once).await {
                client.inner.state.lock().await.collector_error = Some(error.to_string());
            }
            current.store(false, Ordering::SeqCst);
            client.inner.permissions.lock().unwrap().finish_wait(&id);
        });
        Ok(())
    }
    async fn permission_reply(&self, session: &str, id: &str, once: bool) -> Result<()> {
        let pending = self
            .api(
                reqwest::Method::GET,
                &format!("/api/session/{session}/permission"),
                None,
            )
            .await?;
        if !pending["data"]
            .as_array()
            .context("Invalid native permission list")?
            .iter()
            .any(|request| request["id"] == id && request["sessionID"] == session)
        {
            return Ok(());
        }
        self.api(
            reqwest::Method::POST,
            &format!("/api/session/{session}/permission/{id}/reply"),
            Some(json!({"decision":if once { "once" } else { "reject" }})),
        )
        .await?;
        Ok(())
    }
    pub async fn cancel(&self) -> Result<()> {
        let _gate = self.inner.admission.lock().await;
        self.inner.permissions.lock().unwrap().clear();
        self.inner
            .permission_changes
            .send_modify(|version| *version += 1);
        let Some(session) = self.session_id().await else {
            return Ok(());
        };
        self.inner.state.lock().await.cancelled = true;
        let pending = self.cancel_pending(&session).await;
        let result = self
            .api(
                reqwest::Method::POST,
                &format!("/api/session/{session}/interrupt?resume=false"),
                None,
            )
            .await?;
        if !result["interrupted"].is_boolean() {
            bail!("Native interrupt was not confirmed");
        }
        pending
    }
    async fn cancel_pending(&self, session: &str) -> Result<()> {
        let permissions = self
            .api(
                reqwest::Method::GET,
                &format!("/api/session/{session}/permission"),
                None,
            )
            .await?;
        for request in permissions["data"]
            .as_array()
            .context("Invalid native permission list")?
        {
            if request["sessionID"] != session {
                bail!("Foreign native permission refused during cancellation");
            }
            let id = request["id"]
                .as_str()
                .filter(|id| NativeQueue::valid_id(id, "per_"))
                .context("Invalid native permission identity")?;
            self.permission_reply(session, id, false).await?;
        }
        let queued = self.inner.api.inbox(session).await?;
        for item in queued {
            let id = item["id"].as_str().context("Invalid native inbox item")?;
            if let Err(error) = self.inner.api.cancel(session, id).await
                && self
                    .inner
                    .api
                    .inbox(session)
                    .await?
                    .iter()
                    .any(|item| item["id"] == id)
            {
                return Err(error).context("Native queued cancellation not confirmed");
            }
        }
        Ok(())
    }
    pub async fn set_mode(&self, mode: &str) -> Result<()> {
        let _gate = self.inner.admission.lock().await;
        if self.accepts_followups() {
            bail!("Native session is busy");
        }
        let session = self.session().await?;
        self.refresh(&session).await?;
        if !self
            .inner
            .state
            .lock()
            .await
            .agents
            .iter()
            .any(|agent| agent["id"] == mode)
        {
            bail!("Agent was not advertised");
        }
        self.api(
            reqwest::Method::POST,
            &format!("/api/session/{session}/agent"),
            Some(json!({"agent":mode})),
        )
        .await?;
        self.refresh(&session).await?;
        if self.inner.state.lock().await.agent != mode {
            bail!("Native agent selection was not confirmed");
        }
        Ok(())
    }
    pub async fn set_option(&self, id: &str, value: &str) -> Result<Value> {
        if id == "mode" {
            self.set_mode(value).await?;
        } else {
            let _gate = self.inner.admission.lock().await;
            if self.accepts_followups() {
                bail!("Native session is busy");
            }
            let session = self.session().await?;
            if id == "model" {
                let requested = value.split_once('/').context("Expected provider/model")?;
                self.refresh_for_model(&session, Some(requested)).await?;
            } else {
                self.refresh(&session).await?;
            }
            let mut model = self.inner.state.lock().await.model.clone();
            match id {
                "model" => {
                    let (provider, name) =
                        value.split_once('/').context("Expected provider/model")?;
                    if !self
                        .inner
                        .state
                        .lock()
                        .await
                        .models
                        .iter()
                        .any(|item| item["providerID"] == provider && item["id"] == name)
                    {
                        bail!("Model was not advertised");
                    }
                    model = json!({"providerID":provider,"id":name});
                }
                "effort" => {
                    if value == "default" {
                        model
                            .as_object_mut()
                            .context("No native model selected")?
                            .remove("variant");
                    } else {
                        let state = self.inner.state.lock().await;
                        if !state
                            .models
                            .iter()
                            .find(|item| {
                                item["providerID"] == model["providerID"]
                                    && item["id"] == model["id"]
                            })
                            .and_then(|item| item["variants"].as_array())
                            .is_some_and(|variants| {
                                variants.iter().any(|variant| variant["id"] == value)
                            })
                        {
                            bail!("Model variant was not advertised");
                        }
                        model["variant"] = value.into();
                    }
                }
                _ => bail!(
                    "Native transport supports session model, agent, and model variant only; global configuration is not edited"
                ),
            }
            // OpenCode canonicalizes an omitted variant to "default" on readback.
            // Submit that identity explicitly so exact confirmation still fails closed.
            if model.get("variant").is_none() {
                model["variant"] = "default".into();
            }
            self.api(
                reqwest::Method::POST,
                &format!("/api/session/{session}/model"),
                Some(json!({"model":model})),
            )
            .await?;
            if self.info(&session).await?["model"] != model {
                bail!("Native model selection was not confirmed");
            }
            self.refresh(&session).await?;
        }
        self.config_options()
            .await
            .into_iter()
            .find(|option| option["id"] == id && option["currentValue"] == value)
            .context("Native configuration selection was not confirmed")
    }
    pub async fn request(&self, _method: &str, _params: Value) -> Result<Value> {
        bail!("Raw ACP requests are not supported by native transport")
    }
    pub(crate) fn form_handler(&self) -> (FormGuard, mpsc::Receiver<FormRequest>) {
        let (sender, receiver) = mpsc::channel(8);
        let mut permissions = self
            .inner
            .permissions
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        permissions.form_handler = Some(sender);
        let generation = permissions.generation;
        (
            PermissionGuard(Arc::downgrade(&self.inner), generation),
            receiver,
        )
    }
    pub(crate) fn permission_handler(
        &self,
    ) -> (
        mpsc::Receiver<crate::acp::PermissionRequest>,
        PermissionGuard,
    ) {
        let (sender, receiver) = mpsc::channel(32);
        let mut permissions = self.inner.permissions.lock().unwrap();
        permissions.clear();
        permissions.handler = Some(sender);
        (
            receiver,
            PermissionGuard(Arc::downgrade(&self.inner), permissions.generation),
        )
    }
    pub async fn shutdown(&self) -> Result<()> {
        let _shutdown = self.inner.shutdown_gate.lock().await;
        if self.inner.closed.load(Ordering::SeqCst) {
            return Ok(());
        }
        let cancellation = self.cancel().await;
        self.inner.closed.store(true, Ordering::SeqCst);
        if let Some(collector) = self.inner.collector.lock().await.take() {
            collector.abort();
            let _ = collector.await;
        }
        let reaped = if let Some(server) = self.inner.server.lock().await.take() {
            server.shutdown().await
        } else {
            Ok(())
        };
        self.finish(Err(anyhow::anyhow!("Native server closed")))
            .await;
        self.inner.state.lock().await.session = None;
        reaped?;
        cancellation
    }
    async fn require_idle(&self, session: &str) -> Result<()> {
        let active = self
            .api(reqwest::Method::GET, "/api/session/active", None)
            .await?;
        if active["data"]
            .as_object()
            .context("Invalid native active sessions")?
            .contains_key(session)
            || !self.inner.api.inbox(session).await?.is_empty()
        {
            bail!("Native session has unowned running or queued work; resume refused");
        }
        Ok(())
    }
    async fn wait_collector(&self) -> Result<()> {
        if let Some(mut collector) = self.inner.collector.lock().await.take() {
            match tokio::time::timeout(self.inner.timeout, &mut collector).await {
                Ok(result) => {
                    result.context("Native collector failed during close")?;
                }
                Err(_) => {
                    collector.abort();
                    let _ = collector.await;
                    self.finish(Err(anyhow::anyhow!(
                        "Native cancellation completion timed out"
                    )))
                    .await;
                    bail!("Native cancellation completion timed out");
                }
            }
        }
        Ok(())
    }
}
fn message_text(row: &Value) -> String {
    if row["type"] == "user" {
        return row["text"].as_str().unwrap_or("").into();
    }
    row["content"]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|part| part["type"] == "text")
        .filter_map(|part| part["text"].as_str())
        .collect()
}
fn model_name(model: &Value) -> String {
    match (model["providerID"].as_str(), model["id"].as_str()) {
        (Some(provider), Some(id)) => format!("{provider}/{id}"),
        _ => String::new(),
    }
}
fn image_files(images: Vec<Value>) -> Result<Vec<Value>> {
    use base64::Engine;
    if images.len() > 4 {
        bail!("Native input image count exceeds limit");
    }
    images
        .into_iter()
        .map(|content| {
            if content["type"] != "image" {
                bail!("Unsupported native input content; not submitted");
            }
            let data = content["data"]
                .as_str()
                .context("Missing native image data")?;
            if data.len() > 2 * 1024 * 1024 {
                bail!("Native image input exceeds limit");
            }
            let bytes = base64::engine::general_purpose::STANDARD
                .decode(data)
                .context("Invalid native image encoding")?;
            let format = image::guess_format(&bytes).context("Invalid native image")?;
            let mime = match format {
                image::ImageFormat::Png => "image/png",
                image::ImageFormat::Jpeg => "image/jpeg",
                image::ImageFormat::WebP => "image/webp",
                _ => bail!("Unsupported native image type"),
            };
            if content["mimeType"] != mime {
                bail!("Native image MIME does not match bytes");
            }
            let (width, height) =
                image::ImageReader::with_format(std::io::Cursor::new(bytes), format)
                    .into_dimensions()?;
            if width == 0
                || height == 0
                || width > 32768
                || height > 32768
                || u64::from(width) * u64::from(height) > 20_000_000
            {
                bail!("Unsafe native image dimensions");
            }
            Ok(json!({"uri":format!("data:{mime};base64,{data}")}))
        })
        .collect()
}
fn native_file(cwd: &Path, part: &Value) -> Result<Option<InlineImage>> {
    let uri = part["uri"].as_str().context("Missing native media URI")?;
    let mime = part["mime"].as_str().context("Missing native media MIME")?;
    if let Some(data) = uri.strip_prefix(&format!("data:{mime};base64,")) {
        if !mime.starts_with("image/") {
            bail!("Native inline non-image media is unsupported");
        }
        let path = crate::media::save_inline(cwd, &json!({"data":data,"mimeType":mime}))?;
        return Ok(Some(InlineImage {
            path,
            alt: part["name"].as_str().map(str::to_owned),
        }));
    }
    let url = url::Url::parse(uri)?;
    if url.scheme() != "file"
        || url
            .host_str()
            .is_some_and(|host| !host.is_empty() && host != "localhost")
    {
        bail!("Remote native media URI refused");
    }
    let path = url
        .to_file_path()
        .map_err(|_| anyhow::anyhow!("Invalid native media path"))?;
    let path = path.to_str().context("Invalid native media filename")?;
    if path.contains(['[', ']', '\n', '\r']) {
        bail!("Native media path cannot be represented safely");
    }
    let media = crate::media::read_supported(
        cwd,
        &std::env::temp_dir().join("doclibrary_cache"),
        path,
        20 * 1024 * 1024,
    )?;
    if media.mime != mime {
        bail!("Native media MIME does not match bytes");
    }
    if mime.starts_with("image/") && !Path::new(path).starts_with(cwd) {
        use base64::Engine;
        let saved = crate::media::save_inline(
            cwd,
            &json!({"data":base64::engine::general_purpose::STANDARD.encode(&media.bytes),"mimeType":mime}),
        )?;
        return Ok(Some(InlineImage {
            path: saved,
            alt: part["name"].as_str().map(str::to_owned),
        }));
    }
    Ok(mime.starts_with("image/").then(|| InlineImage {
        path: path.into(),
        alt: part["name"].as_str().map(str::to_owned),
    }))
}
fn mcp_servers(servers: Vec<Value>) -> Result<Vec<(String, Value)>> {
    if servers.len() > 32 {
        bail!("Too many native MCP servers");
    }
    let mut names = HashSet::new();
    servers
        .into_iter()
        .map(|server| {
            let name = server["name"]
                .as_str()
                .filter(|name| {
                    !name.is_empty()
                        && name.len() <= 128
                        && name
                            .bytes()
                            .all(|byte| byte.is_ascii_alphanumeric() || b"_-".contains(&byte))
                })
                .context("Invalid native MCP server name")?;
            if !names.insert(name.to_owned()) {
                bail!("Duplicate native MCP server name");
            }
            let pairs = |field: &str| -> Result<serde_json::Map<String, Value>> {
                let mut result = serde_json::Map::new();
                if server[field].is_null() {
                    return Ok(result);
                }
                for item in server[field]
                    .as_array()
                    .context("Invalid MCP environment or headers")?
                {
                    let key = item["name"]
                        .as_str()
                        .context("Invalid MCP environment or header name")?;
                    let value = item["value"]
                        .as_str()
                        .context("Invalid MCP environment or header value")?;
                    if key.is_empty() || result.insert(key.into(), value.into()).is_some() {
                        bail!("Invalid or duplicate MCP environment/header name");
                    }
                }
                Ok(result)
            };
            let config = match server["type"].as_str().unwrap_or("stdio") {
                "stdio" => {
                    let command = server["command"]
                        .as_str()
                        .filter(|command| !command.is_empty())
                        .context("Invalid MCP executable")?;
                    let mut args = vec![command.to_owned()];
                    for argument in server["args"].as_array().context("Invalid MCP arguments")? {
                        args.push(
                            argument
                                .as_str()
                                .context("Invalid MCP argument")?
                                .to_owned(),
                        );
                    }
                    json!({"type":"local","command":args,"environment":pairs("env")?})
                }
                "http" | "sse" => {
                    let url = server["url"].as_str().context("Missing MCP URL")?;
                    let parsed =
                        url::Url::parse(url).map_err(|_| anyhow::anyhow!("Invalid MCP URL"))?;
                    if !["http", "https"].contains(&parsed.scheme())
                        || !parsed.username().is_empty()
                        || parsed.password().is_some()
                    {
                        bail!("Unsafe MCP URL");
                    }
                    json!({"type":"remote","url":url,"headers":pairs("headers")?,"oauth":false})
                }
                _ => bail!("Unsupported native MCP transport"),
            };
            Ok((name.to_owned(), config))
        })
        .collect()
}
