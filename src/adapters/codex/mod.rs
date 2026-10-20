//! ACP agent implemented in Rust over the installed Codex app-server protocol.
mod process;
mod protocol;
use super::{AcpConnection, text, update};
use agent_client_protocol::{Responder, UntypedMessage, schema::v1};
use anyhow::{Context, Result, bail};
use process::Process;
use protocol::*;
use serde_json::{Value, json};
use std::{
    collections::{HashMap, HashSet},
    path::PathBuf,
    sync::{
        Arc, OnceLock,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
};
use tokio::sync::{Mutex, OnceCell, Semaphore, watch};

const SESSION_LIMIT: usize = 64;
const HISTORY_LIMIT: usize = 8 * 1024 * 1024;
const TEXT_LIMIT: usize = 1024 * 1024;
const TOOL_OUTPUT_LIMIT: usize = 128 * 1024;
#[derive(Clone, Debug)]
enum Terminal {
    Complete,
    Cancelled,
    Error(String),
}
struct Turn {
    live: Arc<AtomicBool>,
    started: watch::Sender<Option<String>>,
    done: watch::Sender<Option<Terminal>>,
    cancelled: watch::Sender<bool>,
    followups: AtomicUsize,
    texts: Mutex<HashMap<String, String>>,
    tools: Mutex<HashSet<String>>,
    compact: bool,
    text_bytes: AtomicUsize,
}
impl Turn {
    fn new(compact: bool) -> Arc<Self> {
        let (started, _) = watch::channel(None);
        let (done, _) = watch::channel(None);
        let (cancelled, _) = watch::channel(false);
        Arc::new(Self {
            live: Arc::new(AtomicBool::new(true)),
            started,
            done,
            cancelled,
            followups: AtomicUsize::new(0),
            texts: Mutex::new(HashMap::new()),
            tools: Mutex::new(HashSet::new()),
            compact,
            text_bytes: AtomicUsize::new(0),
        })
    }
    fn reserve_text(&self, count: usize) -> Result<()> {
        let mut used = self.text_bytes.load(Ordering::SeqCst);
        loop {
            let next = used
                .checked_add(count)
                .filter(|bytes| *bytes <= 2 * TEXT_LIMIT)
                .context("Codex text collector exceeded limit")?;
            match self.text_bytes.compare_exchange_weak(
                used,
                next,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => return Ok(()),
                Err(current) => used = current,
            }
        }
    }
    fn finish(&self, terminal: Terminal) {
        self.live.store(false, Ordering::SeqCst);
        self.done.send_if_modified(|value| {
            if value.is_some() {
                false
            } else {
                *value = Some(terminal);
                true
            }
        });
    }
    async fn id(&self, native: &Process) -> Result<String> {
        let mut started = self.started.subscribe();
        let mut done = self.done.subscribe();
        let mut closed = native.closed();
        loop {
            if let Some(id) = started.borrow().clone() {
                return Ok(id);
            }
            if done.borrow().is_some() {
                bail!("Codex turn ended before accepting the operation");
            }
            if *closed.borrow() {
                bail!("Codex app server closed");
            }
            tokio::select! { result = started.changed() => {result?;}, result = done.changed() => {result?;}, result = closed.changed() => {result?;} }
        }
    }
    async fn terminal(&self, native: &Process) -> Result<Terminal> {
        let mut done = self.done.subscribe();
        let mut closed = native.closed();
        loop {
            if let Some(value) = done.borrow().clone() {
                return Ok(value);
            }
            if *closed.borrow() {
                bail!("Codex app server closed during the turn");
            }
            tokio::select! { result = done.changed() => {result?;}, result = closed.changed() => {result?;} }
        }
    }
}
struct Settings {
    model: String,
    effort: Option<String>,
    mode: String,
}
struct Session {
    id: String,
    cwd: PathBuf,
    settings: Mutex<Settings>,
    active: Mutex<Option<Arc<Turn>>>,
    admission: Mutex<()>,
}
struct State {
    native: OnceCell<Arc<Process>>,
    acp: OnceLock<AcpConnection>,
    sessions: Mutex<HashMap<String, Arc<Session>>>,
    catalog: Mutex<Vec<Value>>,
    initializing: Mutex<()>,
    sessions_change: Mutex<()>,
    permissions: Arc<Semaphore>,
    initialized: AtomicBool,
    operations: Semaphore,
    stop: watch::Sender<bool>,
}
impl State {
    fn new() -> Arc<Self> {
        let (stop, _) = watch::channel(false);
        Arc::new(Self {
            native: OnceCell::new(),
            acp: OnceLock::new(),
            sessions: Mutex::new(HashMap::new()),
            catalog: Mutex::new(vec![]),
            initializing: Mutex::new(()),
            sessions_change: Mutex::new(()),
            permissions: Arc::new(Semaphore::new(32)),
            initialized: AtomicBool::new(false),
            operations: Semaphore::new(128),
            stop,
        })
    }
    fn native(&self) -> Result<&Arc<Process>> {
        self.native
            .get()
            .context("Initialize the Codex ACP adapter first")
    }
    fn client(&self) -> Result<&AcpConnection> {
        self.acp.get().context("ACP client connection unavailable")
    }
    async fn initialize(self: &Arc<Self>, params: Value) -> Result<Value> {
        let request: v1::InitializeRequest = serde_json::from_value(params)?;
        if serde_json::to_value(request.protocol_version)? != 1 {
            bail!("Only ACP protocol version 1 is supported");
        }
        let _guard = self.initializing.lock().await;
        if self.initialized.load(Ordering::SeqCst) {
            bail!("ACP is already initialized");
        }
        let state = Arc::downgrade(self);
        let callbacks = state.clone();
        let native = self
            .native
            .get_or_try_init(|| async move {
                let command = std::env::var("CODEX_PATH").unwrap_or_else(|_| "codex".into());
                Process::spawn(
                    &command,
                    move |message| {
                        let state = state.clone();
                        async move {
                            if let Some(state) = state.upgrade()
                                && let Err(error) = state.notification(message).await
                            {
                                state.stop.send_replace(true);
                                return Err(error);
                            }
                            Ok(())
                        }
                    },
                    move |message, responder| {
                        let state = callbacks.clone();
                        async move {
                            if let Some(state) = state.upgrade() {
                                // Approval waits cannot stall stdout dispatch or cancel notifications.
                                let Ok(permit) = state.permissions.clone().try_acquire_owned()
                                else {
                                    responder.respond(json!({"decision":"cancel"}))?;
                                    return Ok(());
                                };
                                tokio::spawn(async move {
                                    let _permit = permit;
                                    if let Err(error) = state.approval(message, responder).await {
                                        eprintln!("Codex approval failed: {error}");
                                    }
                                });
                            } else {
                                responder.respond_with_error(
                                    agent_client_protocol::Error::method_not_found(),
                                )?;
                            }
                            Ok(())
                        }
                    },
                )
                .await
            })
            .await?;
        native.request("initialize", json!({"clientInfo":{"name":"soulsteads-codex-acp-rust","title":"Soulsteads Codex ACP","version":env!("CARGO_PKG_VERSION")},"capabilities":{"experimentalApi":false}})).await?;
        native.notify("initialized", json!({}))?;
        let mut closed = native.closed();
        let weak = Arc::downgrade(self);
        tokio::spawn(async move {
            if !*closed.borrow() {
                let _ = closed.changed().await;
            }
            if let Some(state) = weak.upgrade() {
                state.stop.send_replace(true);
            }
        });
        self.initialized.store(true, Ordering::SeqCst);
        Ok(
            json!({"protocolVersion":1,"agentInfo":{"name":"soulsteads-codex-acp-rust","title":"Codex (Rust)","version":env!("CARGO_PKG_VERSION")},
            "agentCapabilities":{"loadSession":true,"promptCapabilities":{"image":true,"embeddedContext":true},"mcpCapabilities":{"http":true,"sse":false},
                "sessionCapabilities":{"list":{},"resume":{},"close":{},"delete":{}}},"authMethods":[]}),
        )
    }
    async fn session(&self, id: &str) -> Result<Arc<Session>> {
        self.sessions
            .lock()
            .await
            .get(id)
            .cloned()
            .context("Unknown ACP session")
    }
    async fn models(&self) -> Result<Vec<Value>> {
        let mut catalog = self.catalog.lock().await;
        if !catalog.is_empty() {
            return Ok(catalog.clone());
        }
        let mut models = Vec::new();
        let mut cursor = Value::Null;
        let mut seen = HashSet::new();
        for _ in 0..100 {
            let result = self
                .native()?
                .request("model/list", json!({"limit":100,"cursor":cursor}))
                .await?;
            let page = result["data"]
                .as_array()
                .context("Codex did not return a model catalog")?;
            models.extend(page.iter().filter(|model| model["hidden"] != true).cloned());
            if models.len() > 1024 {
                bail!("Codex model catalog exceeded limit");
            }
            cursor = result["nextCursor"].clone();
            if cursor.is_null() || cursor == "" {
                *catalog = models.clone();
                return Ok(models);
            }
            if !seen.insert(cursor.to_string()) {
                bail!("Codex returned a repeated model cursor");
            }
        }
        bail!("Codex model catalog exceeded pagination limit")
    }
    async fn create(&self, params: Value, method: &str) -> Result<Value> {
        let _change = self.sessions_change.lock().await;
        let cwd = canonical_directory(params["cwd"].as_str().context("Session cwd is required")?)?;
        {
            let sessions = self.sessions.lock().await;
            if sessions.len() >= SESSION_LIMIT
                && !params["sessionId"]
                    .as_str()
                    .is_some_and(|id| sessions.contains_key(id))
            {
                bail!("Codex session limit reached");
            }
        }
        let previous = if let Some(id) = params["sessionId"].as_str() {
            self.sessions.lock().await.get(id).cloned()
        } else {
            None
        };
        let _session_admission = if let Some(session) = &previous {
            Some(session.admission.lock().await)
        } else {
            None
        };
        if let Some(session) = &previous
            && session.active.lock().await.is_some()
        {
            bail!("Cannot replace an active session");
        }
        if method != "session/new" {
            let id = params["sessionId"]
                .as_str()
                .context("Session ID is required")?;
            let metadata = self
                .native()?
                .request("thread/read", json!({"threadId":id,"includeTurns":false}))
                .await?;
            let actual = metadata["thread"]["cwd"]
                .as_str()
                .context("Codex history omitted workspace")?;
            if metadata["thread"]["id"] != id || canonical_directory(actual)? != cwd {
                bail!("Codex session belongs to a different workspace");
            }
        }
        let catalog = self.models().await?;
        let config = mcp_config(&params["mcpServers"], &cwd)?;
        let mut request = json!({"cwd":cwd,"approvalPolicy":"on-request","sandbox":"workspace-write","config":config});
        if method != "session/new" {
            request["threadId"] = params["sessionId"].clone();
        }
        let result = self
            .native()?
            .request(
                if method == "session/new" {
                    "thread/start"
                } else {
                    "thread/resume"
                },
                request,
            )
            .await?;
        let id = result["thread"]["id"]
            .as_str()
            .context("Codex did not return a thread ID")?
            .to_owned();
        if method != "session/new" && params["sessionId"] != id {
            bail!("Codex resumed a different thread");
        }
        if let Some(actual) = result["thread"]["cwd"].as_str()
            && canonical_directory(actual)? != cwd
        {
            bail!("Codex session belongs to a different workspace");
        }
        let model = result["model"]
            .as_str()
            .filter(|model| !model.is_empty())
            .map(str::to_owned)
            .or_else(|| {
                catalog
                    .iter()
                    .find(|model| model["isDefault"] == true)
                    .and_then(|model| model["model"].as_str())
                    .map(str::to_owned)
            })
            .context("Codex did not advertise a current model")?;
        let effort = result["reasoningEffort"].as_str().map(str::to_owned);
        let session = Arc::new(Session {
            id: id.clone(),
            cwd,
            settings: Mutex::new(Settings {
                model,
                effort,
                mode: "workspace-write".into(),
            }),
            active: Mutex::new(None),
            admission: Mutex::new(()),
        });
        if method == "session/load" {
            self.history(&session).await?;
        }
        self.sessions
            .lock()
            .await
            .insert(id.clone(), session.clone());
        update(
            self.client()?,
            &id,
            json!({"sessionUpdate":"available_commands_update","availableCommands":[{"name":"compact","description":"Compact the Codex conversation"}]}),
        )?;
        let mut response = json!({"configOptions":self.config_options(&session).await?,"modes":modes("workspace-write")});
        if method == "session/new" {
            response["sessionId"] = id.into();
        }
        Ok(response)
    }
    async fn history(&self, session: &Session) -> Result<()> {
        let mut cursor = Value::Null;
        let mut seen = HashSet::new();
        let mut bytes = 0;
        for _ in 0..256 {
            let result = self.native()?.request("thread/items/list", json!({"threadId":session.id,"limit":100,"sortDirection":"asc","cursor":cursor})).await?;
            for entry in result["data"]
                .as_array()
                .context("Codex did not return history items")?
            {
                bytes += entry.to_string().len();
                if bytes > HISTORY_LIMIT {
                    bail!("Codex history exceeded limit");
                }
                let item = entry.get("item").unwrap_or(entry);
                match item["type"].as_str() {
                    Some("userMessage") => {
                        for content in item["content"].as_array().into_iter().flatten() {
                            if content["type"] == "text" {
                                text(
                                    self.client()?,
                                    &session.id,
                                    "user_message_chunk",
                                    content["text"].as_str().unwrap_or(""),
                                )?;
                            } else if content["type"] == "image"
                                && let Some(url) = content["url"].as_str()
                            {
                                update(
                                    self.client()?,
                                    &session.id,
                                    json!({"sessionUpdate":"user_message_chunk","content":{"type":"resource_link","uri":url,"name":"image"}}),
                                )?;
                            }
                        }
                    }
                    Some("agentMessage") => text(
                        self.client()?,
                        &session.id,
                        "agent_message_chunk",
                        item["text"].as_str().unwrap_or(""),
                    )?,
                    Some("reasoning") => {
                        for summary in item["summary"].as_array().into_iter().flatten() {
                            text(
                                self.client()?,
                                &session.id,
                                "agent_thought_chunk",
                                summary.as_str().unwrap_or(""),
                            )?;
                        }
                    }
                    _ => {
                        if let Some(event) = tool_event(item, true)? {
                            update(self.client()?, &session.id, event)?;
                            emit_tool_images(self.client()?, &session.id, item)?;
                        }
                    }
                }
            }
            cursor = result["nextCursor"].clone();
            if cursor.is_null() || cursor == "" {
                return Ok(());
            }
            if !seen.insert(cursor.to_string()) {
                bail!("Codex returned a repeated history cursor");
            }
        }
        bail!("Codex history exceeded pagination limit")
    }
    async fn list(&self, params: Value) -> Result<Value> {
        let cwd = params["cwd"]
            .as_str()
            .map(canonical_directory)
            .transpose()?;
        let result = self
            .native()?
            .request(
                "thread/list",
                json!({"limit":100,"cursor":params["cursor"],"cwd":cwd}),
            )
            .await?;
        let sessions = result["data"].as_array().context("Codex did not return a session list")?.iter().map(|thread| {
            let title = thread["name"].as_str().filter(|name|!name.is_empty()).or_else(||thread["preview"].as_str());
            json!({"sessionId":thread["id"],"cwd":thread["cwd"],"title":title,"updatedAt":timestamp(thread["updatedAt"].as_i64())})
        }).collect::<Vec<_>>();
        Ok(json!({"sessions":sessions,"nextCursor":result["nextCursor"]}))
    }
    async fn config_options(&self, session: &Session) -> Result<Vec<Value>> {
        let catalog = self.models().await?;
        let settings = session.settings.lock().await;
        let mut seen = HashSet::new();
        let mut options = catalog
            .iter()
            .filter_map(|model| {
                let id = model["model"].as_str()?;
                if !seen.insert(id.to_owned()) {
                    return None;
                }
                Some(json!({"value":id,"name":model["displayName"].as_str().unwrap_or(id)}))
            })
            .collect::<Vec<_>>();
        if seen.insert(settings.model.clone()) {
            options.push(json!({"value":settings.model,"name":settings.model}));
        }
        let mut config = vec![
            json!({"id":"model","name":"Model","category":"model","type":"select","currentValue":settings.model,"options":options}),
            json!({"id":"mode","name":"Mode","category":"mode","type":"select","currentValue":settings.mode,"options":modes(&settings.mode)["availableModes"].as_array().unwrap().iter().map(|mode|json!({"value":mode["id"],"name":mode["name"]})).collect::<Vec<_>>()}),
        ];
        if let Some(model) = catalog
            .iter()
            .find(|model| model["model"] == settings.model)
        {
            let efforts = model["supportedReasoningEfforts"]
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(|entry| {
                    entry["reasoningEffort"]
                        .as_str()
                        .map(|effort| json!({"value":effort,"name":effort}))
                })
                .collect::<Vec<_>>();
            if !efforts.is_empty() {
                let effort = settings
                    .effort
                    .as_deref()
                    .or_else(|| model["defaultReasoningEffort"].as_str())
                    .unwrap_or("medium");
                config.push(json!({"id":"reasoning_effort","name":"Reasoning effort","category":"thought_level","type":"select","currentValue":effort,"options":efforts}));
            }
        }
        Ok(config)
    }
    async fn set_config(&self, params: Value) -> Result<Value> {
        let session = self
            .session(
                params["sessionId"]
                    .as_str()
                    .context("Session ID is required")?,
            )
            .await?;
        let _admission = session.admission.lock().await;
        if !self
            .sessions
            .lock()
            .await
            .get(&session.id)
            .is_some_and(|current| Arc::ptr_eq(current, &session))
        {
            bail!("Codex session has been replaced");
        }
        if session.active.lock().await.is_some() {
            bail!("Cannot change Codex settings during an active turn");
        }
        let id = params["configId"]
            .as_str()
            .context("Configuration ID is required")?;
        let value = params["value"]
            .as_str()
            .context("Configuration value is required")?;
        let options = self.config_options(&session).await?;
        let option = options
            .iter()
            .find(|option| option["id"] == id)
            .context("Configuration option is not advertised")?;
        if !option["options"]
            .as_array()
            .unwrap()
            .iter()
            .any(|option| option["value"] == value)
        {
            bail!("Configuration value is not advertised");
        }
        {
            let mut settings = session.settings.lock().await;
            match id {
                "model" => {
                    settings.model = value.into();
                    settings.effort = None;
                }
                "reasoning_effort" => settings.effort = Some(value.into()),
                "mode" => settings.mode = value.into(),
                _ => bail!("Unknown configuration option"),
            }
        }
        let options = self.config_options(&session).await?;
        update(
            self.client()?,
            &session.id,
            json!({"sessionUpdate":"config_option_update","configOptions":options}),
        )?;
        Ok(json!({"configOptions":options}))
    }
    async fn prompt(self: &Arc<Self>, params: Value) -> Result<Value> {
        let id = params["sessionId"]
            .as_str()
            .context("Session ID is required")?;
        let session = self.session(id).await?;
        let input = prompt_input(&params["prompt"])?;
        let compact = input.len() == 1 && input[0]["text"] == "/compact";
        let admission = session.admission.lock().await;
        if !self
            .sessions
            .lock()
            .await
            .get(id)
            .is_some_and(|current| Arc::ptr_eq(current, &session))
        {
            bail!("Codex session has been replaced");
        }
        let (turn, original) = {
            let mut active = session.active.lock().await;
            if let Some(turn) = active.as_ref() {
                if compact || turn.compact {
                    bail!("Cannot steer a compaction operation");
                }
                if turn.followups.fetch_add(1, Ordering::SeqCst) >= 8 {
                    turn.followups.fetch_sub(1, Ordering::SeqCst);
                    bail!("Codex follow-up limit reached");
                }
                (turn.clone(), false)
            } else {
                let turn = Turn::new(compact);
                *active = Some(turn.clone());
                (turn, true)
            }
        };
        struct Followup(Arc<Turn>, bool);
        impl Drop for Followup {
            fn drop(&mut self) {
                if !self.1 {
                    self.0.followups.fetch_sub(1, Ordering::SeqCst);
                }
            }
        }
        let _followup = Followup(turn.clone(), original);
        // Reserve admission before releasing it; an overlapping ACP prompt must
        // steer this exact turn rather than starting a duplicate generation.
        drop(admission);
        let operation = async {
            if original {
                if compact {
                    self.native()?.request("thread/compact/start", json!({"threadId":id})).await?;
                } else {
                    let settings = session.settings.lock().await;
                    let result = self.native()?.request("turn/start", json!({"threadId":id,"input":input,"cwd":session.cwd,
                        "model":settings.model,"effort":settings.effort,"approvalPolicy":approval_policy(&settings.mode),"sandboxPolicy":sandbox(&settings.mode,&session.cwd)})).await?;
                    let native_id = result["turn"]["id"].as_str().context("Codex did not return a turn ID")?.to_owned();
                    let existing = turn.started.borrow().clone();
                    if existing.is_some_and(|existing| existing != native_id) { bail!("Codex returned an inconsistent turn ID"); }
                    turn.started.send_replace(Some(native_id));
                }
            } else {
                let native_id = turn.id(self.native()?).await?;
                if !turn.live.load(Ordering::SeqCst) || *turn.cancelled.borrow() { bail!("Codex turn ended before accepting the follow-up"); }
                self.native()?.request("turn/steer", json!({"threadId":id,"expectedTurnId":native_id,"input":input})).await?;
            }
            Ok::<_,anyhow::Error>(())
        }.await;
        if let Err(error) = operation {
            if original {
                turn.finish(Terminal::Error(error.to_string()));
                self.release(&session, &turn).await;
            }
            return Err(error);
        }
        let terminal = turn.terminal(self.native()?).await;
        if original {
            self.release(&session, &turn).await;
        }
        match terminal? {
            Terminal::Complete => Ok(json!({"stopReason":"end_turn"})),
            Terminal::Cancelled => Ok(json!({"stopReason":"cancelled"})),
            Terminal::Error(error) => bail!(error),
        }
    }
    async fn release(&self, session: &Session, turn: &Arc<Turn>) {
        let mut active = session.active.lock().await;
        if active
            .as_ref()
            .is_some_and(|active| Arc::ptr_eq(active, turn))
        {
            active.take();
        }
    }
    async fn cancel(&self, id: &str) -> Result<()> {
        let session = self.session(id).await?;
        let Some(turn) = session.active.lock().await.clone() else {
            return Ok(());
        };
        turn.cancelled.send_replace(true);
        turn.live.store(false, Ordering::SeqCst);
        let native_id = turn.id(self.native()?).await?;
        if turn.done.borrow().is_some() {
            return Ok(());
        }
        if let Err(error) = self
            .native()?
            .request("turn/interrupt", json!({"threadId":id,"turnId":native_id}))
            .await
            && turn.done.borrow().is_none()
        {
            turn.finish(Terminal::Error(format!(
                "Codex cancellation failed: {error}"
            )));
            self.stop.send_replace(true);
            return Err(error);
        }
        Ok(())
    }
    async fn close(&self, id: &str, delete: bool) -> Result<Value> {
        let _change = self.sessions_change.lock().await;
        let session = self.sessions.lock().await.get(id).cloned();
        let _admission = if let Some(session) = &session {
            Some(session.admission.lock().await)
        } else {
            None
        };
        if let Some(session) = &session
            && session.active.lock().await.is_some()
        {
            bail!("Cannot close an active Codex session");
        }
        self.native()?
            .request(
                if delete {
                    "thread/delete"
                } else {
                    "thread/unsubscribe"
                },
                json!({"threadId":id}),
            )
            .await?;
        self.sessions.lock().await.remove(id);
        Ok(json!({}))
    }
    async fn notification(&self, message: UntypedMessage) -> Result<()> {
        let params = message.params;
        let Some(id) = params["threadId"].as_str() else {
            return Ok(());
        };
        let Some(session) = self.sessions.lock().await.get(id).cloned() else {
            return Ok(());
        };
        let Some(turn) = session.active.lock().await.clone() else {
            return Ok(());
        };
        if let Some(native_id) = params["turnId"].as_str()
            && let Some(current) = turn.started.borrow().as_ref()
            && current != native_id
        {
            return Ok(());
        }
        match message.method.as_str() {
            "turn/started" => {
                if let Some(native_id) = params["turn"]["id"].as_str()
                    && turn
                        .started
                        .borrow()
                        .as_ref()
                        .is_none_or(|current| current == native_id)
                {
                    turn.started.send_replace(Some(native_id.into()));
                }
            }
            "item/agentMessage/delta" | "item/plan/delta" => {
                let delta = params["delta"].as_str().unwrap_or("");
                let item = params["itemId"]
                    .as_str()
                    .context("Codex text delta omitted item ID")?;
                let mut texts = turn.texts.lock().await;
                if item.len() > 1024 || (texts.len() >= 2048 && !texts.contains_key(item)) {
                    bail!("Codex text item limit exceeded");
                }
                turn.reserve_text(delta.len())?;
                let collected = texts.entry(item.into()).or_default();
                if collected.len() + delta.len() > 2 * TEXT_LIMIT {
                    bail!("Codex text collector exceeded limit");
                }
                collected.push_str(delta);
                text(self.client()?, id, "agent_message_chunk", delta)?;
            }
            "item/reasoning/summaryTextDelta" | "item/reasoning/textDelta" => text(
                self.client()?,
                id,
                "agent_thought_chunk",
                params["delta"].as_str().unwrap_or(""),
            )?,
            "item/started" | "item/completed" => {
                let item = &params["item"];
                let completed = message.method == "item/completed";
                if completed && matches!(item["type"].as_str(), Some("agentMessage" | "plan")) {
                    let value = item["text"].as_str().unwrap_or("");
                    let item_id = item["id"]
                        .as_str()
                        .context("Codex message omitted item ID")?;
                    let mut texts = turn.texts.lock().await;
                    if item_id.len() > 1024 || (texts.len() >= 2048 && !texts.contains_key(item_id))
                    {
                        bail!("Codex text item limit exceeded");
                    }
                    let collected = texts.entry(item_id.into()).or_default();
                    if value.starts_with(collected.as_str()) {
                        turn.reserve_text(value.len() - collected.len())?;
                        text(
                            self.client()?,
                            id,
                            "agent_message_chunk",
                            &value[collected.len()..],
                        )?;
                        *collected = value.into();
                    } else if !value.is_empty() {
                        bail!("Codex replaced streamed text with inconsistent final text");
                    }
                } else if let Some(event) = tool_event(item, completed)? {
                    let tool_id = item["id"].as_str().context("Codex tool omitted item ID")?;
                    let mut tools = turn.tools.lock().await;
                    if tools.len() >= 2048 && !tools.contains(tool_id) {
                        bail!("Codex tool collector exceeded limit");
                    }
                    if completed
                        && !tools.contains(tool_id)
                        && let Some(started) = tool_event(item, false)?
                    {
                        update(self.client()?, id, started)?;
                    }
                    tools.insert(tool_id.into());
                    update(self.client()?, id, event)?;
                    if completed {
                        emit_tool_images(self.client()?, id, item)?;
                    }
                    // A completed compaction item is followed by the native
                    // turn/completed notification. Retain this collector until
                    // that terminal event so it cannot settle the next prompt.
                }
            }
            "item/commandExecution/outputDelta" => {
                let mut delta = params["delta"].as_str().unwrap_or("");
                while !delta.is_empty() {
                    let mut end = delta.len().min(8192);
                    while !delta.is_char_boundary(end) {
                        end -= 1;
                    }
                    update(
                        self.client()?,
                        id,
                        json!({"sessionUpdate":"tool_call_update","toolCallId":params["itemId"],"content":[{"type":"content","content":{"type":"text","text":&delta[..end]}}]}),
                    )?;
                    delta = &delta[end..];
                }
            }
            "turn/plan/updated" => {
                let entries = params["plan"].as_array().into_iter().flatten().map(|entry|json!({"content":entry["step"],"priority":"medium","status":match entry["status"].as_str(){Some("completed")=>"completed",Some("inProgress")=>"in_progress",_=>"pending"}})).collect::<Vec<_>>();
                update(
                    self.client()?,
                    id,
                    json!({"sessionUpdate":"plan","entries":entries}),
                )?;
            }
            "turn/completed" => {
                let native_turn = &params["turn"];
                if let Some(current) = turn.started.borrow().as_ref()
                    && native_turn["id"] != current.as_str()
                {
                    return Ok(());
                }
                turn.finish(match native_turn["status"].as_str() {
                    Some("completed") => Terminal::Complete,
                    Some("interrupted") => Terminal::Cancelled,
                    Some("failed") => Terminal::Error(
                        native_turn["error"]["message"]
                            .as_str()
                            .unwrap_or("Codex turn failed")
                            .into(),
                    ),
                    _ => Terminal::Error("Codex returned an unknown terminal turn status".into()),
                });
            }
            _ => {}
        }
        Ok(())
    }
    async fn approval(&self, message: UntypedMessage, responder: Responder<Value>) -> Result<()> {
        if message.method == "item/permissions/requestApproval" {
            return responder
                .respond(json!({"permissions":{},"scope":"turn"}))
                .map_err(Into::into);
        }
        let supported = matches!(
            message.method.as_str(),
            "item/commandExecution/requestApproval" | "item/fileChange/requestApproval"
        );
        if !supported {
            return responder
                .respond_with_error(agent_client_protocol::Error::method_not_found())
                .map_err(Into::into);
        }
        let params = &message.params;
        let session = self
            .session(params["threadId"].as_str().unwrap_or(""))
            .await
            .ok();
        let turn = if let Some(session) = &session {
            session.active.lock().await.clone()
        } else {
            None
        };
        let Some(turn) = turn.filter(|turn| turn.live.load(Ordering::SeqCst)) else {
            return responder
                .respond(json!({"decision":"cancel"}))
                .map_err(Into::into);
        };
        if let Some(id) = turn.started.borrow().as_ref()
            && params["turnId"] != id.as_str()
        {
            return responder
                .respond(json!({"decision":"cancel"}))
                .map_err(Into::into);
        }
        let session = session.unwrap();
        let offered_accept = params["availableDecisions"]
            .as_array()
            .is_none_or(|options| options.iter().any(|option| option == "accept"));
        let mut options =
            vec![json!({"optionId":"reject-once","name":"Reject once","kind":"reject_once"})];
        if offered_accept {
            options.insert(
                0,
                json!({"optionId":"allow-once","name":"Allow once","kind":"allow_once"}),
            );
        }
        let tool_id = params["itemId"]
            .as_str()
            .context("Codex approval omitted item ID")?;
        let permission = json!({"sessionId":session.id,"toolCall":{"toolCallId":tool_id,"title":params["command"].as_str().or_else(||params["reason"].as_str()).unwrap_or("Codex tool approval"),
            "kind":if message.method.contains("commandExecution") {"execute"} else {"edit"},"status":"pending","rawInput":params},"options":options});
        let future = self
            .client()?
            .send_request(UntypedMessage::new(
                "session/request_permission",
                permission,
            )?)
            .block_task();
        let mut done = turn.done.subscribe();
        let mut closed = self.native()?.closed();
        let mut cancelled = turn.cancelled.subscribe();
        let answer = if done.borrow().is_some() || *closed.borrow() || *cancelled.borrow() {
            None
        } else {
            tokio::select! {
                result = future => result.ok(),
                _ = done.changed() => None,
                _ = closed.changed() => None,
                _ = cancelled.changed() => None,
            }
        };
        let decision = if turn.live.load(Ordering::SeqCst) {
            match answer.as_ref().map(|answer| &answer["outcome"]) {
                Some(outcome)
                    if outcome["outcome"] == "selected"
                        && outcome["optionId"] == "allow-once"
                        && offered_accept =>
                {
                    "accept"
                }
                Some(outcome)
                    if outcome["outcome"] == "selected" && outcome["optionId"] == "reject-once" =>
                {
                    "decline"
                }
                _ => "cancel",
            }
        } else {
            "cancel"
        };
        self.native()?
            .respond_approval(responder, json!({"decision":decision}), turn.live.clone())
    }
    async fn request(self: &Arc<Self>, message: UntypedMessage) -> Result<Value> {
        let _permit = self
            .operations
            .try_acquire()
            .context("ACP request queue full")?;
        macro_rules! params {
            ($request:ty) => {
                serde_json::to_value(serde_json::from_value::<$request>(message.params)?)?
            };
        }
        if message.method != "initialize" && !self.initialized.load(Ordering::SeqCst) {
            bail!("Initialize ACP before requesting a session");
        }
        match message.method.as_str() {
            "initialize" => self.initialize(message.params).await,
            "session/new" => {
                self.create(params!(v1::NewSessionRequest), "session/new")
                    .await
            }
            "session/load" => {
                self.create(params!(v1::LoadSessionRequest), "session/load")
                    .await
            }
            "session/resume" => {
                self.create(params!(v1::ResumeSessionRequest), "session/resume")
                    .await
            }
            "session/list" => self.list(params!(v1::ListSessionsRequest)).await,
            "session/prompt" => self.prompt(params!(v1::PromptRequest)).await,
            "session/set_config_option" => {
                self.set_config(params!(v1::SetSessionConfigOptionRequest))
                    .await
            }
            "session/set_mode" => {
                let params = params!(v1::SetSessionModeRequest);
                self.set_config(json!({"sessionId":params["sessionId"],"configId":"mode","value":params["modeId"]})).await?;
                update(
                    self.client()?,
                    params["sessionId"].as_str().unwrap(),
                    json!({"sessionUpdate":"current_mode_update","currentModeId":params["modeId"]}),
                )?;
                Ok(json!({}))
            }
            "session/close" => {
                let params = params!(v1::CloseSessionRequest);
                self.close(params["sessionId"].as_str().unwrap(), false)
                    .await
            }
            "session/delete" => {
                let params = params!(v1::DeleteSessionRequest);
                self.close(params["sessionId"].as_str().unwrap(), true)
                    .await
            }
            _ => bail!("Unsupported ACP method"),
        }
    }
    async fn shutdown(&self) -> Result<()> {
        for session in self.sessions.lock().await.values() {
            if let Some(turn) = session.active.lock().await.as_ref() {
                turn.finish(Terminal::Error("ACP client disconnected".into()));
            }
        }
        if let Some(native) = self.native.get() {
            native.shutdown().await?;
        }
        Ok(())
    }
}

impl super::server::Handler for State {
    fn connection(&self) -> &OnceLock<AcpConnection> {
        &self.acp
    }
    fn methods(&self) -> &'static [&'static str] {
        &[
            "initialize",
            "session/new",
            "session/load",
            "session/resume",
            "session/list",
            "session/prompt",
            "session/set_config_option",
            "session/set_mode",
            "session/close",
            "session/delete",
        ]
    }
    fn stopping(&self) -> watch::Receiver<bool> {
        self.stop.subscribe()
    }
    fn request(
        self: Arc<Self>,
        message: UntypedMessage,
    ) -> futures::future::BoxFuture<'static, Result<Value>> {
        Box::pin(async move { State::request(&self, message).await })
    }
    fn cancel(self: Arc<Self>, session: String) -> futures::future::BoxFuture<'static, Result<()>> {
        Box::pin(async move { State::cancel(&self, &session).await })
    }
    fn shutdown(self: Arc<Self>) -> futures::future::BoxFuture<'static, Result<()>> {
        Box::pin(async move { State::shutdown(&self).await })
    }
}
pub async fn run() -> Result<()> {
    super::server::serve(State::new()).await
}
