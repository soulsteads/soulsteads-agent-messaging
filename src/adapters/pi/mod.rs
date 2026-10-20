//! Rust ACP SDK Agent backed by the installed Pi native RPC CLI.
mod history;
mod mcp;
mod process;
mod protocol;
mod turn;
mod ui;
use super::{AcpConnection, bounded, text, update};
use agent_client_protocol::{UntypedMessage, schema::v1};
use anyhow::{Context, Result, bail};
use process::Process;
use protocol::{
    Tool, canonical_directory, commands, configuration, model_id, prompt_content, replay, tool,
    tool_output,
};
use serde_json::{Value, json};
use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::{
        Arc, OnceLock,
        atomic::{AtomicBool, Ordering},
    },
};
use tokio::sync::{Mutex, OnceCell, Semaphore, watch};
use turn::{Terminal, Turn};

struct Approval {
    live: Arc<AtomicBool>,
    cancel: watch::Sender<bool>,
}
#[derive(Default)]
struct Catalog {
    state: Value,
    models: Vec<Value>,
    levels: Vec<String>,
}
struct Session {
    id: OnceLock<String>,
    cwd: PathBuf,
    path: OnceLock<PathBuf>,
    native: OnceCell<Arc<Process>>,
    native_ready: watch::Sender<bool>,
    catalog: Mutex<Catalog>,
    active: Mutex<Option<Arc<Turn>>>,
    admission: Mutex<()>,
    approvals: std::sync::Mutex<HashMap<String, Approval>>,
    _mcp: Option<mcp::Registration>,
    mcp_servers: Value,
}
impl Session {
    fn id(&self) -> Result<&str> {
        self.id
            .get()
            .map(String::as_str)
            .context("Pi session not ready")
    }
    fn native(&self) -> Result<&Arc<Process>> {
        self.native.get().context("Pi native transport unavailable")
    }
    fn cancel_approvals(&self) {
        for approval in self.approvals.lock().unwrap().values() {
            approval.live.store(false, Ordering::SeqCst);
            approval.cancel.send_replace(true);
        }
    }
    fn native_path(&self, state: &Value) -> Result<PathBuf> {
        let path = Path::new(
            state["sessionFile"]
                .as_str()
                .context("Pi session file missing")?,
        );
        Ok(if path.is_absolute() {
            path.into()
        } else {
            self.cwd.join(path)
        })
    }
    fn check_state(&self, state: &Value) -> Result<()> {
        if history::id(&state["sessionId"])? != self.id()?
            || self.native_path(state)?
                != *self.path.get().context("Pi session file unavailable")?
        {
            self.native()?.abort();
            bail!("Pi native session identity changed unexpectedly");
        }
        Ok(())
    }
    fn check_idle(&self, state: &Value) -> Result<()> {
        let result = (|| {
            self.check_state(state)?;
            let streaming = state["isStreaming"]
                .as_bool()
                .context("Pi state omitted streaming status")?;
            let compacting = state["isCompacting"]
                .as_bool()
                .context("Pi state omitted compaction status")?;
            let queued = state["pendingMessageCount"]
                .as_u64()
                .context("Pi state omitted queue depth")?;
            if streaming || compacting || queued > 0 {
                bail!("Pi reported pending work after terminal acknowledgement");
            }
            Ok(())
        })();
        if result.is_err() {
            self.native()?.abort();
        }
        result
    }
}
struct State {
    acp: OnceLock<AcpConnection>,
    initialized: AtomicBool,
    extension_ui: AtomicBool,
    sessions: Mutex<HashMap<String, Arc<Session>>>,
    known: Mutex<HashMap<String, (PathBuf, PathBuf)>>,
    creating: Mutex<()>,
    operations: Semaphore,
    permissions: Arc<Semaphore>,
    stop: watch::Sender<bool>,
}
impl State {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            acp: OnceLock::new(),
            initialized: AtomicBool::new(false),
            extension_ui: AtomicBool::new(false),
            sessions: Default::default(),
            known: Default::default(),
            creating: Default::default(),
            operations: Semaphore::new(128),
            permissions: Arc::new(Semaphore::new(64)),
            stop: watch::channel(false).0,
        })
    }
    fn client(&self) -> Result<&AcpConnection> {
        self.acp.get().context("ACP client unavailable")
    }
    async fn session(&self, id: &str) -> Result<Arc<Session>> {
        self.sessions
            .lock()
            .await
            .get(id)
            .cloned()
            .context("Unknown Pi session")
    }
    fn initialize(&self, params: Value) -> Result<Value> {
        let extension_ui = params["_meta"]["pi/extensionUi"] == true;
        let request: v1::InitializeRequest = serde_json::from_value(params)?;
        if serde_json::to_value(request.protocol_version)? != 1 {
            bail!("Only ACP protocol version 1 is supported");
        }
        if self.initialized.swap(true, Ordering::SeqCst) {
            bail!("ACP already initialized");
        }
        self.extension_ui.store(extension_ui, Ordering::SeqCst);
        Ok(
            json!({"protocolVersion":1,"agentInfo":{"name":"soulsteads-pi-acp-rust","title":"Pi (Rust)","version":env!("CARGO_PKG_VERSION")},"agentCapabilities":{"loadSession":true,"promptCapabilities":{"image":true,"embeddedContext":true},"mcpCapabilities":{"http":true,"sse":true},"sessionCapabilities":{"list":{},"resume":{},"close":{},"delete":{}}},"authMethods":[]}),
        )
    }
    async fn refresh(&self, session: &Session) -> Result<()> {
        let native = session.native()?;
        let state = native.request(json!({"type":"get_state"})).await?;
        session.check_state(&state)?;
        let models = native
            .request(json!({"type":"get_available_models"}))
            .await?;
        let models = models["models"]
            .as_array()
            .context("Pi omitted model catalog")?;
        if models.len() > 512 {
            bail!("Pi model catalog exceeded limit");
        }
        let thinking = native
            .request(json!({"type":"get_available_thinking_levels"}))
            .await?;
        let levels = thinking["levels"]
            .as_array()
            .context("Pi omitted thinking levels")?;
        if levels.is_empty() || levels.len() > 32 {
            bail!("Invalid Pi thinking level catalog");
        }
        let levels = levels
            .iter()
            .map(|value| {
                value
                    .as_str()
                    .filter(|level| !level.is_empty() && level.len() <= 128)
                    .map(str::to_owned)
                    .context("Invalid Pi thinking level")
            })
            .collect::<Result<Vec<_>>>()?;
        if !levels.iter().any(|level| state["thinkingLevel"] == *level) {
            bail!("Pi current thinking level not in catalog");
        }
        *session.catalog.lock().await = Catalog {
            state,
            models: models.clone(),
            levels,
        };
        Ok(())
    }
    async fn info(&self, session: &Session) -> Result<Value> {
        let catalog = session.catalog.lock().await;
        let mut info = configuration(&catalog.state, &catalog.models, &catalog.levels);
        info["sessionId"] = session.id()?.into();
        Ok(info)
    }
    async fn create(
        self: &Arc<Self>,
        params: Value,
        load: bool,
        replay_history: bool,
    ) -> Result<Value> {
        let _creating = self.creating.lock().await;
        let cwd = canonical_directory(params["cwd"].as_str().context("Session cwd missing")?)?;
        let mcp = mcp::Registration::create(&params["mcpServers"], &cwd)?;
        let mcp_servers = if params["mcpServers"].is_null() {
            json!([])
        } else {
            params["mcpServers"].clone()
        };
        let mut restore_settings = None;
        let requested = if load {
            Some(history::id(&params["sessionId"])?)
        } else {
            None
        };
        let existing = if let Some(id) = &requested {
            self.sessions.lock().await.get(id).cloned()
        } else {
            None
        };
        if let Some(session) = existing {
            let _admission = session.admission.lock().await;
            if session.cwd != cwd {
                bail!("Pi session belongs to a different workspace");
            }
            if session.active.lock().await.is_some() {
                bail!("Cannot load an active Pi session");
            }
            self.refresh(&session).await?;
            if session.mcp_servers == mcp_servers {
                if replay_history {
                    self.history(&session).await?;
                }
                return self.info(&session).await;
            }
            // MCP belongs to a native runtime. Reopen only this idle runtime,
            // preserving its native history and current model/thinking settings.
            let state = session.catalog.lock().await.state.clone();
            session.check_idle(&state)?;
            restore_settings = Some(state);
            session.native()?.shutdown().await?;
            self.sessions.lock().await.remove(session.id()?);
        }
        if self.sessions.lock().await.len() >= 32 || self.known.lock().await.len() >= 4096 {
            bail!("Pi session limit exceeded");
        }
        let path = if let Some(id) = &requested {
            let cwd = cwd.clone();
            let id = id.clone();
            Some(
                tokio::task::spawn_blocking(move || {
                    let entry = history::find(&cwd, &id)?;
                    history::check_size(&entry.path)?;
                    Ok::<_, anyhow::Error>(entry.path)
                })
                .await??,
            )
        } else {
            None
        };
        let session = Arc::new(Session {
            id: OnceLock::new(),
            cwd,
            path: OnceLock::new(),
            native: OnceCell::new(),
            native_ready: watch::channel(false).0,
            catalog: Default::default(),
            active: Default::default(),
            admission: Default::default(),
            approvals: Default::default(),
            _mcp: mcp,
            mcp_servers,
        });
        let weak_state = Arc::downgrade(self);
        let weak_session = Arc::downgrade(&session);
        let command = std::env::var("PI_PATH")
            .or_else(|_| std::env::var("PI_ACP_PI_COMMAND"))
            .unwrap_or_else(|_| "pi".into());
        let mut args = vec!["--mode".into(), "rpc".into(), "--no-themes".into()];
        if let Some(registration) = &session._mcp {
            args.extend([
                "--extension".into(),
                registration.path().to_string_lossy().into_owned(),
            ]);
        }
        if let Some(path) = path {
            args.extend(["--session".into(), path.to_string_lossy().into_owned()]);
        }
        let native = Process::spawn(&command, &session.cwd, args, move |event| {
            let state = weak_state.clone();
            let session = weak_session.clone();
            async move {
                if let (Some(state), Some(session)) = (state.upgrade(), session.upgrade()) {
                    state.event(&session, event).await?;
                }
                Ok(())
            }
        })
        .await?;
        session.native.set(native.clone()).ok();
        session.native_ready.send_replace(true);
        let result = async {
            let state=native.request(json!({"type":"get_state"})).await?;
            let id=history::id(&state["sessionId"])?;
            if requested.as_ref().is_some_and(|requested|requested!=&id){bail!("Pi loaded a different native session");}
            let path=session.native_path(&state)?;
            if path.exists(){history::validate(&path,&session.cwd,&id)?;}
            if self.sessions.lock().await.contains_key(&id) || (requested.is_none()&&self.known.lock().await.contains_key(&id)){bail!("Pi returned a duplicate native session ID");}
            session.id.set(id.clone()).ok();session.path.set(path.clone()).ok();
            if let Some(settings)=restore_settings {
                native.request(json!({"type":"set_model","provider":settings["model"]["provider"],"modelId":settings["model"]["id"]})).await?;
                native.request(json!({"type":"set_thinking_level","level":settings["thinkingLevel"]})).await?;
            }
            self.refresh(&session).await?;
            if replay_history{self.history(&session).await?;}
            let catalog=native.request(json!({"type":"get_commands"})).await?;
            update(self.client()?,&id,json!({"sessionUpdate":"available_commands_update","availableCommands":commands(&catalog)}))?;
            self.publish_usage(&session).await?;
            let info=self.info(&session).await?;
            self.known.lock().await.insert(id.clone(),(session.cwd.clone(),path));
            self.sessions.lock().await.insert(id,session.clone());
            Ok::<_,anyhow::Error>(info)
        }.await;
        if result.is_err() {
            native.shutdown().await?;
        }
        result
    }
    async fn history(&self, session: &Session) -> Result<()> {
        let path = session
            .path
            .get()
            .context("Pi native session file missing")?
            .clone();
        let cwd = session.cwd.clone();
        let id = session.id()?.to_owned();
        tokio::task::spawn_blocking(move || {
            history::validate(&path, &cwd, &id)?;
            history::check_size(&path)
        })
        .await??;
        let messages = session
            .native()?
            .request(json!({"type":"get_messages"}))
            .await?;
        replay(self.client()?, session.id()?, &session.cwd, &messages)
    }
    async fn list(&self, params: Value) -> Result<Value> {
        if !params["cursor"].is_null() {
            bail!("Pi listing does not use cursors");
        }
        let cwd = if let Some(cwd) = params["cwd"].as_str() {
            canonical_directory(cwd)?
        } else {
            std::env::current_dir()?.canonicalize()?
        };
        let directory = cwd.clone();
        let entries = tokio::task::spawn_blocking(move || history::list(&directory)).await??;
        let mut sessions = entries.iter().map(history::as_info).collect::<Vec<_>>();
        for session in self
            .sessions
            .lock()
            .await
            .values()
            .filter(|session| session.cwd == cwd)
        {
            let id = session.id()?;
            if !sessions.iter().any(|entry| entry["sessionId"] == id) {
                sessions.push(json!({"sessionId":id,"cwd":cwd,"title":"Pi conversation"}));
            }
        }
        Ok(json!({"sessions":sessions}))
    }
    async fn publish_usage(&self, session: &Session) -> Result<()> {
        let stats = session
            .native()?
            .request(json!({"type":"get_session_stats"}))
            .await?;
        if let (Some(used), Some(size)) = (
            stats["contextUsage"]["tokens"].as_u64(),
            stats["contextUsage"]["contextWindow"].as_u64(),
        ) && size > 0
        {
            update(
                self.client()?,
                session.id()?,
                json!({"sessionUpdate":"usage_update","used":used,"size":size}),
            )?;
        }
        Ok(())
    }
    async fn set_config(&self, params: Value) -> Result<Value> {
        let session = self
            .session(params["sessionId"].as_str().unwrap_or(""))
            .await?;
        let _admission = session.admission.lock().await;
        if session.active.lock().await.is_some() {
            bail!("Cannot change Pi settings during an active turn");
        }
        let id = params["configId"].as_str().unwrap_or("");
        let value = params["value"]
            .as_str()
            .context("Pi config value must be text")?;
        let command = {
            let catalog = session.catalog.lock().await;
            match id {
                "model" => {
                    let model = catalog
                        .models
                        .iter()
                        .find(|model| model_id(model).as_deref() == Some(value))
                        .context("Unknown Pi model")?;
                    json!({"type":"set_model","provider":model["provider"],"modelId":model["id"]})
                }
                "thought_level" => {
                    if !catalog.levels.iter().any(|level| level == value) {
                        bail!("Unsupported Pi thinking level");
                    }
                    json!({"type":"set_thinking_level","level":value})
                }
                _ => bail!("Unknown Pi config option"),
            }
        };
        session.native()?.request(command).await?;
        if let Err(error) = self.refresh(&session).await {
            session.native()?.abort();
            return Err(error);
        }
        let info = self.info(&session).await?;
        update(
            self.client()?,
            session.id()?,
            json!({"sessionUpdate":"current_mode_update","currentModeId":info["modes"]["currentModeId"]}),
        )?;
        update(
            self.client()?,
            session.id()?,
            json!({"sessionUpdate":"config_option_update","configOptions":info["configOptions"]}),
        )?;
        self.publish_usage(&session).await?;
        Ok(json!({"configOptions":info["configOptions"]}))
    }
    async fn prompt(&self, params: Value) -> Result<Value> {
        let session = self
            .session(params["sessionId"].as_str().unwrap_or(""))
            .await?;
        let (message, images) = prompt_content(&params["prompt"])?;
        let admission = session.admission.lock().await;
        let active = session.active.lock().await.clone();
        if let Some(turn) = active {
            if message.starts_with('/') {
                bail!("Pi slash commands cannot be queued during an active turn");
            }
            let state = session
                .native()?
                .request(json!({"type":"get_state"}))
                .await?;
            session.check_state(&state)?;
            if state["pendingMessageCount"]
                .as_u64()
                .context("Pi state omitted queue depth")?
                >= 64
            {
                bail!("Pi follow-up queue is full");
            }
            turn.admit()?;
            let response=session.native()?.prompt(json!({"type":"prompt","message":message,"images":images,"streamingBehavior":"followUp"}),turn.live.clone()).await;
            turn.admitted();
            response?;
            return Ok(json!({"stopReason":"end_turn"}));
        }
        let turn = Turn::new();
        turn.admit()?;
        *session.active.lock().await = Some(turn.clone());
        let command = is_command(&message);
        let submitted = if command {
            self.command(&session, &turn, &message).await
        } else {
            let response = session
                .native()?
                .prompt(
                    json!({"type":"prompt","message":message,"images":images}),
                    turn.live.clone(),
                )
                .await;
            match response {
                Ok(response) => {
                    if response["disposition"] == "handled" && !turn.status.lock().unwrap().started
                    {
                        turn.settle();
                    }
                    Ok(())
                }
                Err(_) if *turn.cancelled.borrow() => Ok(()),
                Err(error) => {
                    turn.fail(error.to_string());
                    turn.settle();
                    Err(error)
                }
            }
        };
        turn.admitted();
        drop(admission);
        let terminal = if submitted.is_ok() {
            turn.terminal(session.native()?).await
        } else {
            submitted.map(|_| Terminal::Complete("end_turn".into()))
        };
        let _admission = session.admission.lock().await;
        session.cancel_approvals();
        turn.live.store(false, Ordering::SeqCst);
        let result = async {
            match terminal? {
                Terminal::Complete(reason) => {
                    let state = session
                        .native()?
                        .request(json!({"type":"get_state"}))
                        .await?;
                    session.check_idle(&state)?;
                    self.publish_usage(&session).await?;
                    Ok(json!({"stopReason":reason}))
                }
                Terminal::Error(error) => bail!("{error}"),
            }
        }
        .await;
        *session.active.lock().await = None;
        result
    }
    async fn command(&self, session: &Session, turn: &Turn, message: &str) -> Result<()> {
        let (command, args) = message
            .split_once(char::is_whitespace)
            .unwrap_or((message, ""));
        let args = args.trim();
        let native = session.native()?;
        let response = match command {
            "/compact" => native
                .execute(json!({"type":"compact","customInstructions":args}))
                .await
                .map(|_| "Context compacted".to_owned()),
            "/session" => native
                .request(json!({"type":"get_session_stats"}))
                .await
                .map(|stats| {
                    format!(
                        "Pi session: {}\nMessages: {}\nCost: {}",
                        session.id().unwrap_or(""),
                        stats["totalMessages"],
                        stats["cost"]
                    )
                }),
            "/name" => {
                if args.is_empty() || args.len() > 512 {
                    bail!("Session name must contain 1–512 bytes");
                }
                native
                    .request(json!({"type":"set_session_name","name":args}))
                    .await
                    .map(|_| format!("Session named {args}"))
            }
            "/export" => {
                if !args.is_empty() {
                    bail!("Export does not accept a path");
                }
                let path = session
                    .cwd
                    .join(format!("pi-session-{}.html", uuid::Uuid::new_v4()));
                native
                    .request(json!({"type":"export_html","outputPath":path}))
                    .await
                    .map(|response| {
                        format!(
                            "Session exported to {}",
                            response["path"].as_str().unwrap_or("workspace")
                        )
                    })
            }
            "/autocompact" => {
                let enabled = match args {
                    "on" => true,
                    "off" => false,
                    "toggle" | "" => !session.catalog.lock().await.state["autoCompactionEnabled"]
                        .as_bool()
                        .unwrap_or(true),
                    _ => bail!("Use /autocompact on|off|toggle"),
                };
                native
                    .request(json!({"type":"set_auto_compaction","enabled":enabled}))
                    .await
                    .map(|_| {
                        format!(
                            "Automatic compaction {}",
                            if enabled { "on" } else { "off" }
                        )
                    })
            }
            "/steering" | "/follow-up" => {
                let kind = if command == "/steering" {
                    "steering"
                } else {
                    "follow_up"
                };
                if !matches!(args, "all" | "one-at-a-time") {
                    bail!("Use {command} all|one-at-a-time");
                }
                native
                    .request(json!({"type":format!("set_{kind}_mode"),"mode":args}))
                    .await
                    .map(|_| format!("{} delivery: {args}", &command[1..]))
            }
            _ => unreachable!(),
        };
        match response {
            Ok(response) => {
                turn.reserve(response.len())?;
                text(
                    self.client()?,
                    session.id()?,
                    "agent_message_chunk",
                    &response,
                )?;
                turn.settle();
                Ok(())
            }
            Err(error) => {
                if *turn.cancelled.borrow() {
                    turn.settle();
                    Ok(())
                } else {
                    turn.fail(error.to_string());
                    turn.settle();
                    Err(error)
                }
            }
        }
    }
    async fn cancel(&self, id: &str) -> Result<()> {
        let session = self.session(id).await?;
        // Mark cancellation before acquiring admission: a compact RPC may hold it.
        let Some(turn) = session.active.lock().await.clone() else {
            return Ok(());
        };
        if !turn.cancel() {
            return Ok(());
        }
        session.cancel_approvals();
        let native = session.native()?;
        native.request(json!({"type":"clear_queue"})).await?;
        native.request(json!({"type":"abort"})).await?;
        let state = native.request(json!({"type":"get_state"})).await?;
        session.check_idle(&state)?;
        turn.settle();
        Ok(())
    }
    async fn close(&self, id: &str, delete: bool) -> Result<Value> {
        let session = self.sessions.lock().await.get(id).cloned();
        if let Some(session) = session {
            self.cancel(id).await?;
            let _admission = session.admission.lock().await;
            session.cancel_approvals();
            session.native()?.shutdown().await?;
            self.sessions.lock().await.remove(id);
        }
        if delete {
            let (cwd, path) = self
                .known
                .lock()
                .await
                .get(id)
                .cloned()
                .context("Unknown Pi session")?;
            let session = id.to_owned();
            tokio::task::spawn_blocking(move || history::delete(&path, &cwd, &session)).await??;
            self.known.lock().await.remove(id);
        }
        Ok(json!({}))
    }
    async fn event(self: &Arc<Self>, session: &Arc<Session>, event: Value) -> Result<()> {
        if session.native.get().is_none() {
            let mut ready = session.native_ready.subscribe();
            while !*ready.borrow() {
                ready.changed().await?;
            }
        }
        if session.id.get().is_none() {
            if event["type"] == "extension_ui_request"
                && matches!(
                    event["method"].as_str(),
                    Some("select" | "confirm" | "input" | "editor")
                )
            {
                session
                    .native()?
                    .write(
                        json!({"type":"extension_ui_response","id":event["id"],"cancelled":true}),
                        None,
                    )
                    .await?;
            }
            return Ok(());
        }
        if let Some(id) = event["sessionId"].as_str()
            && id != session.id()?
        {
            return Ok(());
        }
        if event["type"] == "session_info_changed" {
            if let Some(name) = event["name"].as_str() {
                update(
                    self.client()?,
                    session.id()?,
                    json!({"sessionUpdate":"session_info_update","title":bounded(name,512)}),
                )?;
            }
            return Ok(());
        }
        if event["type"] == "extension_ui_request" {
            return self.extension_ui(session, event).await;
        }
        let Some(turn) = session.active.lock().await.clone() else {
            return Ok(());
        };
        if turn.done.borrow().is_some() {
            return Ok(());
        }
        match event["type"].as_str() {
            Some("agent_start") => turn.started(),
            Some("message_start") if event["message"]["role"] == "assistant" => {
                turn.fragments.lock().await.clear();
            }
            Some("message_update") => {
                let delta = &event["assistantMessageEvent"];
                let index = delta["contentIndex"].as_u64().unwrap_or(0) as usize;
                match delta["type"].as_str() {
                    Some("text_delta" | "thinking_delta") => {
                        let value = delta["delta"].as_str().context("Pi delta omitted text")?;
                        turn.reserve(value.len())?;
                        let mut fragments = turn.fragments.lock().await;
                        if fragments.len() >= 2048 && !fragments.contains_key(&index) {
                            bail!("Pi content block limit exceeded");
                        }
                        fragments.entry(index).or_default().push_str(value);
                        text(
                            self.client()?,
                            session.id()?,
                            if delta["type"] == "text_delta" {
                                "agent_message_chunk"
                            } else {
                                "agent_thought_chunk"
                            },
                            value,
                        )?;
                    }
                    Some("toolcall_start" | "toolcall_delta" | "toolcall_end") => {
                        let block = delta
                            .get("toolCall")
                            .unwrap_or(&delta["partial"]["content"][index]);
                        if let Some(id) = block["id"].as_str() {
                            self.tool_start(
                                session,
                                &turn,
                                id,
                                block["name"].as_str().unwrap_or("Pi tool"),
                                &block["arguments"],
                                false,
                            )
                            .await?;
                        }
                    }
                    _ => {}
                }
            }
            Some("message_end") if event["message"]["role"] == "assistant" => {
                let message = &event["message"];
                if let Some(blocks) = message["content"].as_array() {
                    let fragments = turn.fragments.lock().await;
                    for (index, block) in blocks.iter().enumerate() {
                        match block["type"].as_str() {
                            Some("text" | "thinking") => {
                                let kind = block["type"].as_str().unwrap();
                                let value = block[if kind == "text" { "text" } else { "thinking" }]
                                    .as_str()
                                    .unwrap_or("");
                                let prefix =
                                    fragments.get(&index).map(String::as_str).unwrap_or("");
                                if !value.starts_with(prefix) {
                                    bail!("Pi final content differs from streamed content");
                                }
                                let suffix = &value[prefix.len()..];
                                turn.reserve(suffix.len())?;
                                text(
                                    self.client()?,
                                    session.id()?,
                                    if kind == "text" {
                                        "agent_message_chunk"
                                    } else {
                                        "agent_thought_chunk"
                                    },
                                    suffix,
                                )?;
                            }
                            Some("toolCall") => {
                                if let Some(id) = block["id"].as_str() {
                                    self.tool_start(
                                        session,
                                        &turn,
                                        id,
                                        block["name"].as_str().unwrap_or("Pi tool"),
                                        &block["arguments"],
                                        false,
                                    )
                                    .await?;
                                }
                            }
                            _ => {}
                        }
                    }
                }
                let mut status = turn.status.lock().unwrap();
                status.failure = if message["stopReason"] == "error" {
                    Some(bounded(
                        message["errorMessage"]
                            .as_str()
                            .unwrap_or("Pi provider failed"),
                        8192,
                    ))
                } else if message["stopReason"] == "aborted" && !status.cancelled {
                    Some("Pi run aborted unexpectedly".into())
                } else {
                    None
                };
            }
            Some("tool_execution_start") => {
                let id = event["toolCallId"]
                    .as_str()
                    .context("Pi tool event omitted ID")?;
                self.tool_start(
                    session,
                    &turn,
                    id,
                    event["toolName"].as_str().unwrap_or("Pi tool"),
                    &event["args"],
                    true,
                )
                .await?;
            }
            Some("tool_execution_update" | "tool_execution_end") => {
                let id = event["toolCallId"]
                    .as_str()
                    .context("Pi tool event omitted ID")?;
                if !turn.tools.lock().await.contains_key(id) {
                    self.tool_start(
                        session,
                        &turn,
                        id,
                        event["toolName"].as_str().unwrap_or("Pi tool"),
                        &json!({}),
                        true,
                    )
                    .await?;
                }
                let final_event = event["type"] == "tool_execution_end";
                let result = if final_event {
                    &event["result"]
                } else {
                    &event["partialResult"]
                };
                let status = if !final_event {
                    "in_progress"
                } else if event["isError"] == true {
                    "failed"
                } else {
                    "completed"
                };
                tool_output(self.client()?, session.id()?, id, result, status)?;
                if final_event {
                    turn.tools.lock().await.insert(id.into(), status.into());
                }
            }
            // These are substeps, not terminal evidence. Pi may continue itself.
            Some(
                "agent_end"
                | "turn_end"
                | "auto_retry_start"
                | "auto_retry_end"
                | "auto_compaction_start"
                | "auto_compaction_end",
            ) => {}
            Some("agent_settled") => {
                turn.settle();
                session.cancel_approvals();
            }
            Some("error") => turn.fail(bounded(
                event["message"].as_str().unwrap_or("Pi native failure"),
                8192,
            )),
            _ => {}
        }
        Ok(())
    }
    async fn tool_start(
        &self,
        session: &Session,
        turn: &Turn,
        id: &str,
        name: &str,
        args: &Value,
        executing: bool,
    ) -> Result<()> {
        let mut tools = turn.tools.lock().await;
        if tools.len() >= 4096 && !tools.contains_key(id) {
            bail!("Pi tool count exceeded limit");
        }
        let existing = tools.contains_key(id);
        let status = if executing {
            "in_progress"
        } else {
            tools.get(id).map(String::as_str).unwrap_or("pending")
        };
        tool(
            self.client()?,
            session.id()?,
            &session.cwd,
            Tool {
                id,
                name,
                args,
                existing,
                status,
            },
        )?;
        let status = status.to_owned();
        tools.insert(id.into(), status);
        Ok(())
    }
    async fn request(self: &Arc<Self>, message: UntypedMessage) -> Result<Value> {
        let _permit = self
            .operations
            .try_acquire()
            .context("ACP operation queue full")?;
        if message.method != "initialize" && !self.initialized.load(Ordering::SeqCst) {
            bail!("Initialize ACP before requesting a session");
        }
        macro_rules! params {
            ($ty:ty) => {
                serde_json::to_value(serde_json::from_value::<$ty>(message.params)?)?
            };
        }
        match message.method.as_str() {
            "initialize" => self.initialize(message.params),
            "session/new" => {
                self.create(params!(v1::NewSessionRequest), false, false)
                    .await
            }
            "session/load" => {
                self.create(params!(v1::LoadSessionRequest), true, true)
                    .await
            }
            "session/resume" => {
                self.create(params!(v1::ResumeSessionRequest), true, false)
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
                self.set_config(json!({"sessionId":params["sessionId"],"configId":"thought_level","value":params["modeId"]})).await?;
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
        let sessions = self
            .sessions
            .lock()
            .await
            .drain()
            .map(|(_, session)| session)
            .collect::<Vec<_>>();
        let results = futures::future::join_all(sessions.into_iter().map(async |session| {
            if let Some(turn) = session.active.lock().await.as_ref() {
                turn.cancel();
            }
            session.cancel_approvals();
            if let Some(native) = session.native.get() {
                if !*native.closed().borrow() {
                    // Native Bash can own a detached process group. Let Pi's
                    // abort signal terminate tools before reaping the CLI.
                    let _ = tokio::time::timeout(std::time::Duration::from_secs(3), async {
                        native.request(json!({"type":"clear_queue"})).await?;
                        native.request(json!({"type":"abort"})).await
                    })
                    .await;
                }
                native.shutdown().await?;
            }
            Ok::<_, anyhow::Error>(())
        }))
        .await;
        for result in results {
            result?;
        }

        Ok(())
    }
}
fn is_command(message: &str) -> bool {
    matches!(
        message.split_whitespace().next(),
        Some(
            "/compact"
                | "/session"
                | "/name"
                | "/export"
                | "/autocompact"
                | "/steering"
                | "/follow-up"
        )
    )
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
    fn cancel(self: Arc<Self>, id: String) -> futures::future::BoxFuture<'static, Result<()>> {
        Box::pin(async move { State::cancel(&self, &id).await })
    }
    fn shutdown(self: Arc<Self>) -> futures::future::BoxFuture<'static, Result<()>> {
        Box::pin(async move { State::shutdown(&self).await })
    }
}
pub async fn run() -> Result<()> {
    if super::mcp_sse::requested() {
        return super::mcp_sse::run().await;
    }
    super::server::serve(State::new()).await
}
