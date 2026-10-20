//! ACP Agent implemented in Rust over the installed Claude Code stream-JSON CLI.
use super::{AcpConnection, bounded, text, update};
mod history;
mod process;
mod protocol;
use agent_client_protocol::{UntypedMessage, schema::v1};
use anyhow::{Context, Result, bail};
use process::Process;
use protocol::*;
use serde_json::{Value, json};
use std::{
    collections::{HashMap, HashSet},
    path::PathBuf,
    sync::{
        Arc, OnceLock,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio::sync::{Mutex, OnceCell, Semaphore, watch};

const TEXT_LIMIT: usize = 16 * 1024 * 1024;
const TOOL_LIMIT: usize = 64 * 1024;
#[derive(Clone)]
enum Terminal {
    Complete(String),
    Error(String),
}
#[derive(Default)]
struct Block {
    kind: String,
    tool_id: String,
    name: String,
    value: String,
    finalized: bool,
}
struct Turn {
    uuid: String,
    live: Arc<AtomicBool>,
    cancelled: watch::Sender<bool>,
    done: watch::Sender<Option<Terminal>>,
    blocks: Mutex<HashMap<(String, u64), Block>>,
    message_id: Mutex<String>,
    seen: Mutex<HashSet<String>>,
    tools: Mutex<HashSet<String>>,
    text_size: std::sync::atomic::AtomicUsize,
    emitted_text: AtomicBool,
}
impl Turn {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            uuid: uuid::Uuid::new_v4().to_string(),
            live: Arc::new(AtomicBool::new(true)),
            cancelled: watch::channel(false).0,
            done: watch::channel(None).0,
            blocks: Default::default(),
            message_id: Default::default(),
            seen: Default::default(),
            tools: Default::default(),
            text_size: Default::default(),
            emitted_text: AtomicBool::new(false),
        })
    }
    fn reserve(&self, bytes: usize) -> Result<()> {
        if self
            .text_size
            .fetch_add(bytes, Ordering::SeqCst)
            .saturating_add(bytes)
            > TEXT_LIMIT
        {
            bail!("Claude turn output exceeded limit");
        }
        Ok(())
    }
    fn finish(&self, terminal: Terminal) {
        self.live.store(false, Ordering::SeqCst);
        self.done.send_if_modified(|done| {
            if done.is_none() {
                *done = Some(terminal);
                true
            } else {
                false
            }
        });
    }
    async fn terminal(&self, native: &Process) -> Result<Terminal> {
        let mut done = self.done.subscribe();
        let mut closed = native.closed();
        loop {
            if let Some(terminal) = done.borrow().clone() {
                return Ok(terminal);
            }
            if *closed.borrow() {
                bail!("Claude transport closed during the turn");
            }
            tokio::select! {r=done.changed()=>{r?;},r=closed.changed()=>{r?;}}
        }
    }
}
struct Approval {
    live: Arc<AtomicBool>,
    cancel: watch::Sender<bool>,
}
struct Settings {
    model: String,
    effort: String,
    mode: String,
}
struct Session {
    id: String,
    cwd: PathBuf,
    native: OnceCell<Arc<Process>>,
    settings: Mutex<Settings>,
    catalog: Mutex<Vec<Value>>,
    active: Mutex<Option<Arc<Turn>>>,
    admission: Mutex<()>,
    approvals: std::sync::Mutex<HashMap<String, Approval>>,
}
impl Session {
    fn native(&self) -> Result<&Arc<Process>> {
        self.native
            .get()
            .context("Claude session transport unavailable")
    }
    fn cancel_approvals(&self) {
        for ask in self.approvals.lock().unwrap().values() {
            ask.live.store(false, Ordering::SeqCst);
            ask.cancel.send_replace(true);
        }
    }
}
struct State {
    acp: OnceLock<AcpConnection>,
    initialized: AtomicBool,
    sessions: Mutex<HashMap<String, Arc<Session>>>,
    creating: Mutex<()>,
    known: Mutex<HashMap<String, PathBuf>>,
    operations: Semaphore,
    permissions: Arc<Semaphore>,
    stop: watch::Sender<bool>,
}
impl State {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            acp: OnceLock::new(),
            initialized: AtomicBool::new(false),
            sessions: Default::default(),
            creating: Default::default(),
            known: Default::default(),
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
            .context("Unknown Claude session")
    }
    fn initialize(&self, params: Value) -> Result<Value> {
        let params: v1::InitializeRequest = serde_json::from_value(params)?;
        if serde_json::to_value(params.protocol_version)? != 1 {
            bail!("Only ACP protocol version 1 is supported");
        }
        if self.initialized.swap(true, Ordering::SeqCst) {
            bail!("ACP is already initialized");
        }
        Ok(
            json!({"protocolVersion":1,"agentInfo":{"name":"soulsteads-claude-acp-rust","title":"Claude Code (Rust)","version":env!("CARGO_PKG_VERSION")},"agentCapabilities":{"loadSession":true,"promptCapabilities":{"image":true,"embeddedContext":true},"mcpCapabilities":{"http":true,"sse":true},"sessionCapabilities":{"list":{},"resume":{},"close":{},"delete":{}}},"authMethods":[]}),
        )
    }
    async fn create(self: &Arc<Self>, params: Value, load: bool, replay: bool) -> Result<Value> {
        let _creating = self.creating.lock().await;
        let cwd = canonical_directory(params["cwd"].as_str().context("Missing session cwd")?)?;
        let id = if load {
            history::session_id(
                params["sessionId"]
                    .as_str()
                    .context("Missing session UUID")?,
            )?
        } else {
            uuid::Uuid::new_v4().to_string()
        };
        let records = if load {
            let directory = cwd.clone();
            let session = id.clone();
            tokio::task::spawn_blocking(move || history::load(&directory, &session)).await??
        } else {
            vec![]
        };
        // Reloading the current session keeps its native process and settings.
        // Hold admission while replaying so prompt output cannot mix into history.
        let existing = self.sessions.lock().await.get(&id).cloned();
        if let Some(existing) = existing {
            let _admission = existing.admission.lock().await;
            if existing.cwd != cwd {
                bail!("Claude session belongs to a different workspace");
            }
            if existing.active.lock().await.is_some() {
                bail!("Cannot reload an active Claude turn");
            }
            if *existing.native()?.closed().borrow() {
                bail!("Close the failed Claude session before reloading");
            }
            if replay {
                for record in records {
                    self.replay(&existing, &record).await?;
                }
            }
            return Ok(session_info(&existing).await);
        }
        if self.known.lock().await.len() >= 4096 {
            bail!("Claude session registry limit exceeded");
        }
        if self.sessions.lock().await.len() >= 32 {
            bail!("Claude open session limit exceeded");
        }
        let mut args = vec![
            "--print".into(),
            "--input-format".into(),
            "stream-json".into(),
            "--output-format".into(),
            "stream-json".into(),
            "--verbose".into(),
            "--include-partial-messages".into(),
            "--replay-user-messages".into(),
            "--permission-prompt-tool".into(),
            "stdio".into(),
            "--permission-mode".into(),
            "manual".into(),
            if load {
                "--resume".into()
            } else {
                "--session-id".into()
            },
            id.clone(),
        ];
        let mcp = mcp_config(&params["mcpServers"])?;
        if mcp.as_object().is_some_and(|servers| !servers.is_empty()) {
            args.extend(["--mcp-config".into(), json!({"mcpServers":mcp}).to_string()]);
        }
        let session = Arc::new(Session {
            id: id.clone(),
            cwd,
            native: OnceCell::new(),
            settings: Mutex::new(Settings {
                model: "default".into(),
                effort: "default".into(),
                mode: "default".into(),
            }),
            catalog: Default::default(),
            active: Default::default(),
            admission: Default::default(),
            approvals: Default::default(),
        });
        let weak_session = Arc::downgrade(&session);
        let weak_state = Arc::downgrade(self);
        let command = std::env::var("CLAUDE_CODE_EXECUTABLE")
            .or_else(|_| std::env::var("CLAUDE_PATH"))
            .unwrap_or_else(|_| "claude".into());
        let native = Process::spawn(&command, &session.cwd, args, move |event| {
            let session = weak_session.clone();
            let state = weak_state.clone();
            async move {
                if let (Some(state), Some(session)) = (state.upgrade(), session.upgrade()) {
                    state.event(&session, event).await?;
                }
                Ok(())
            }
        })
        .await?;
        session.native.set(native.clone()).ok();
        let initialized = native
            .request(json!({"subtype":"initialize","hooks":{},"sdkMcpServers":[]}))
            .await;
        let initialized = match initialized {
            Ok(value) => value,
            Err(error) => {
                native.shutdown().await?;
                return Err(error);
            }
        };
        let models = initialized["models"]
            .as_array()
            .context("Claude initialization omitted models")?
            .iter()
            .take(256)
            .filter(|model| model["value"].as_str().is_some_and(|id| id.len() <= 1024))
            .cloned()
            .collect::<Vec<_>>();
        if models.is_empty() {
            native.shutdown().await?;
            bail!("Claude returned no supported models");
        }
        *session.catalog.lock().await = models;
        self.sessions
            .lock()
            .await
            .insert(id.clone(), session.clone());
        self.known
            .lock()
            .await
            .insert(id.clone(), session.cwd.clone());
        let result=async {
            if replay{for record in records {self.replay(&session,&record).await?;}}
            let commands=initialized["commands"].as_array().into_iter().flatten().filter_map(|command| {
                let name=command["name"].as_str()?;
                // Login and configuration commands are not bridge auth flows.
                if matches!(name,"login"|"logout"|"clear"){return None;}
                Some(json!({"name":bounded(name,128),"description":bounded(command["description"].as_str().unwrap_or("Claude command"),4096)}))
            }).take(256).collect::<Vec<_>>();
            update(self.client()?,&id,json!({"sessionUpdate":"available_commands_update","availableCommands":commands}))?;
            Ok::<_,anyhow::Error>(session_info(&session).await)
        }.await;
        if result.is_err() {
            self.sessions.lock().await.remove(&id);
            native.shutdown().await?;
        }
        result
    }
    async fn replay(&self, session: &Session, record: &Value) -> Result<()> {
        match record["type"].as_str() {
            Some("assistant") => {
                for block in content(&record["message"]["content"]) {
                    self.complete_block(session, None, &block, &record["message"]["id"])
                        .await?;
                }
            }
            Some("user") => {
                for block in content(&record["message"]["content"]) {
                    if block["type"] == "tool_result" {
                        self.tool_result(session, None, &block).await?;
                    } else if block["type"] == "text" {
                        text(
                            self.client()?,
                            &session.id,
                            "user_message_chunk",
                            block["text"].as_str().unwrap_or(""),
                        )?;
                    }
                }
            }
            _ => {}
        }
        Ok(())
    }
    async fn list(&self, params: Value) -> Result<Value> {
        if !params["cursor"].is_null() {
            bail!("Claude session list does not use cursors");
        }
        let cwd = canonical_directory(
            params["cwd"]
                .as_str()
                .unwrap_or(std::env::current_dir()?.to_str().context("Non-UTF8 cwd")?),
        )?;
        let directory = cwd.clone();
        let mut sessions = tokio::task::spawn_blocking(move || history::list(&directory)).await??;
        for session in self
            .sessions
            .lock()
            .await
            .values()
            .filter(|session| session.cwd == cwd)
        {
            if !sessions
                .iter()
                .any(|entry| entry["sessionId"] == session.id)
            {
                sessions.push(
                    json!({"sessionId":session.id,"cwd":session.cwd,"title":"Claude conversation"}),
                );
            }
        }
        Ok(json!({"sessions":sessions}))
    }
    async fn set_config(&self, params: Value) -> Result<Value> {
        let session = self
            .session(params["sessionId"].as_str().unwrap_or(""))
            .await?;
        let _admission = session.admission.lock().await;
        if session.active.lock().await.is_some() {
            bail!("Cannot change Claude settings during an active turn");
        }
        let mut settings = session.settings.lock().await;
        let catalog = session.catalog.lock().await;
        let config = params["configId"].as_str().unwrap_or("");
        let value = params["value"]
            .as_str()
            .context("Config value must be text")?;
        match config {
            "model" => {
                if !catalog.iter().any(|model| model["value"] == value) {
                    bail!("Unknown Claude model");
                }
                if settings.effort != "default" {
                    session.native()?.request(json!({"subtype":"apply_flag_settings","settings":{"effortLevel":null}})).await?;
                    settings.effort = "default".into();
                }
                session
                    .native()?
                    .request(json!({"subtype":"set_model","model":value}))
                    .await?;
                settings.model = value.into();
            }
            "reasoning_effort" => {
                let model = catalog
                    .iter()
                    .find(|model| model["value"] == settings.model)
                    .context("Unknown current model")?;
                if value != "default"
                    && !model["supportedEffortLevels"]
                        .as_array()
                        .is_some_and(|levels| levels.iter().any(|level| level == value))
                {
                    bail!("Unsupported Claude effort for this model");
                }
                session.native()?.request(json!({"subtype":"apply_flag_settings","settings":{"effortLevel":if value=="default"{Value::Null}else{value.into()}}})).await?;
                settings.effort = value.into();
            }
            "mode" => {
                let mode = match value {
                    "default" => "manual",
                    "plan" => "plan",
                    _ => bail!("Unknown Claude permission mode"),
                };
                session
                    .native()?
                    .request(json!({"subtype":"set_permission_mode","mode":mode}))
                    .await?;
                settings.mode = value.into();
            }
            _ => bail!("Unknown Claude configuration option"),
        }
        let options = config_options(&settings, &catalog);
        update(
            self.client()?,
            &session.id,
            json!({"sessionUpdate":"config_option_update","configOptions":options}),
        )?;
        Ok(json!({"configOptions":options}))
    }
    async fn prompt(&self, params: Value) -> Result<Value> {
        let session = self
            .session(params["sessionId"].as_str().unwrap_or(""))
            .await?;
        let blocks = prompt_content(&params["prompt"])?;
        let admission = session.admission.lock().await;
        if session.active.lock().await.is_some() {
            bail!("Claude session is busy; concurrent prompts are unsupported");
        }
        let turn = Turn::new();
        *session.active.lock().await = Some(turn.clone());
        let submitted=session.native()?.write(json!({"type":"user","uuid":turn.uuid,"session_id":session.id,"message":{"role":"user","content":blocks},"parent_tool_use_id":null}),None).await;
        drop(admission);
        let terminal = match submitted {
            Ok(()) => turn.terminal(session.native()?).await,
            Err(error) => {
                session.native()?.abort();
                Err(error)
            }
        };
        turn.live.store(false, Ordering::SeqCst);
        session.cancel_approvals();
        let mut active = session.active.lock().await;
        if active
            .as_ref()
            .is_some_and(|current| Arc::ptr_eq(current, &turn))
        {
            *active = None;
        }
        match terminal? {
            Terminal::Complete(reason) => Ok(json!({"stopReason":reason})),
            Terminal::Error(error) => bail!("{error}"),
        }
    }
    async fn cancel(&self, id: &str) -> Result<()> {
        let session = self.session(id).await?;
        let _admission = session.admission.lock().await;
        let Some(turn) = session.active.lock().await.clone() else {
            return Ok(());
        };
        if !turn.cancelled.send_if_modified(|cancelled| {
            if !*cancelled {
                *cancelled = true;
                true
            } else {
                false
            }
        }) {
            return Ok(());
        }
        turn.live.store(false, Ordering::SeqCst);
        session.cancel_approvals();
        let native = session.native()?;
        native
            .request(json!({"subtype":"interrupt","cancel_queued":true}))
            .await?;
        if tokio::time::timeout(Duration::from_secs(30), turn.terminal(native))
            .await
            .is_err()
        {
            native.abort();
            bail!("Claude cancellation terminal event timed out");
        }
        Ok(())
    }
    async fn close(&self, id: &str, delete: bool) -> Result<Value> {
        let session = match self.session(id).await {
            Ok(session) => session,
            Err(_) if delete => {
                let cwd = self
                    .known
                    .lock()
                    .await
                    .get(id)
                    .cloned()
                    .context("Unknown Claude session")?;
                let owned = id.to_owned();
                tokio::task::spawn_blocking(move || history::delete(&cwd, &owned)).await??;
                self.known.lock().await.remove(id);
                return Ok(json!({}));
            }
            Err(error) => return Err(error),
        };
        self.cancel(id).await?;
        let _admission = session.admission.lock().await;
        session.cancel_approvals();
        session.native()?.shutdown().await?;
        if delete {
            let cwd = session.cwd.clone();
            let id = id.to_owned();
            tokio::task::spawn_blocking(move || history::delete(&cwd, &id)).await??;
        }
        self.sessions.lock().await.remove(id);
        if delete {
            self.known.lock().await.remove(id);
        }
        Ok(json!({}))
    }
    async fn event(self: &Arc<Self>, session: &Arc<Session>, message: Value) -> Result<()> {
        if let Some(id) = message["session_id"].as_str()
            && id != session.id
        {
            return Ok(());
        }
        if message["type"] == "control_cancel_request" {
            if let Some(id) = message["request_id"].as_str()
                && let Some(ask) = session.approvals.lock().unwrap().get(id)
            {
                ask.live.store(false, Ordering::SeqCst);
                ask.cancel.send_replace(true);
            }
            return Ok(());
        }
        if message["type"] == "control_request" {
            let Some(turn) = session
                .active
                .lock()
                .await
                .clone()
                .filter(|turn| turn.live.load(Ordering::SeqCst))
            else {
                return deny_control(session, &message).await;
            };
            let request = &message["request"];
            if request["subtype"] != "can_use_tool"
                || request["requires_user_interaction"] == true
                || request["tool_name"] == "AskUserQuestion"
            {
                return deny_control(session, &message).await;
            }
            let id = message["request_id"]
                .as_str()
                .context("Claude control omitted request ID")?
                .to_owned();
            let Ok(permit) = self.permissions.clone().try_acquire_owned() else {
                return deny_control(session, &message).await;
            };
            let ask_live = Arc::new(AtomicBool::new(true));
            let (cancel, cancelled) = watch::channel(false);
            {
                let mut asks = session.approvals.lock().unwrap();
                if asks.len() >= 64 || asks.contains_key(&id) {
                    bail!("Claude pending approval limit or duplicate ID");
                }
                // Register before the next frame can cancel this native request.
                asks.insert(
                    id.clone(),
                    Approval {
                        live: ask_live.clone(),
                        cancel,
                    },
                );
            }
            let state = self.clone();
            let session = session.clone();
            tokio::spawn(async move {
                let _permit = permit;
                let result = state
                    .approval(&session, message, turn, ask_live, cancelled)
                    .await;
                session.approvals.lock().unwrap().remove(&id);
                if let Err(error) = result {
                    eprintln!("Claude permission failed: {error}");
                    if let Ok(native) = session.native() {
                        native.abort();
                    }
                }
            });
            return Ok(());
        }
        let Some(turn) = session.active.lock().await.clone() else {
            return Ok(());
        };
        if message["parent_tool_use_id"].as_str().is_some() {
            return Ok(());
        }
        if let Some(uuid) = message["user_message_uuid"].as_str()
            && uuid != turn.uuid
        {
            return Ok(());
        }
        match message["type"].as_str() {
            Some("stream_event") => self.stream(session, &turn, &message["event"]).await?,
            Some("assistant") => {
                if let Some(uuid) = message["uuid"].as_str() {
                    let mut seen = turn.seen.lock().await;
                    if seen.len() >= 2048 {
                        bail!("Claude message limit exceeded");
                    }
                    if !seen.insert(uuid.into()) {
                        return Ok(());
                    }
                }
                for block in content(&message["message"]["content"]) {
                    self.complete_block(session, Some(&turn), &block, &message["message"]["id"])
                        .await?;
                }
            }
            Some("user") => {
                for block in content(&message["message"]["content"]) {
                    if block["type"] == "tool_result" {
                        self.tool_result(session, Some(&turn), &block).await?;
                    }
                }
            }
            Some("result") => {
                if *turn.cancelled.borrow() {
                    turn.finish(Terminal::Complete("cancelled".into()));
                } else if message["is_error"] == true || message["subtype"] != "success" {
                    turn.finish(Terminal::Error(bounded(
                        message["result"].as_str().unwrap_or_else(|| {
                            message["errors"][0]
                                .as_str()
                                .unwrap_or("Claude turn failed")
                        }),
                        8192,
                    )));
                } else {
                    if !turn.emitted_text.load(Ordering::SeqCst)
                        && let Some(result) = message["result"].as_str()
                    {
                        turn.reserve(result.len())?;
                        text(self.client()?, &session.id, "agent_message_chunk", result)?;
                    }
                    turn.finish(Terminal::Complete("end_turn".into()));
                }
                session.cancel_approvals();
            }
            Some("system") if message["subtype"] == "local_command_output" => {
                if let Some(output) = message["content"].as_str() {
                    turn.reserve(output.len())?;
                    turn.emitted_text.store(true, Ordering::SeqCst);
                    text(self.client()?, &session.id, "agent_message_chunk", output)?;
                }
            }
            _ => {}
        }
        Ok(())
    }
    async fn stream(&self, session: &Session, turn: &Turn, event: &Value) -> Result<()> {
        match event["type"].as_str() {
            Some("message_start") => {
                *turn.message_id.lock().await = event["message"]["id"]
                    .as_str()
                    .context("Claude stream omitted message ID")?
                    .into();
            }
            Some("content_block_start") => {
                let id = turn.message_id.lock().await.clone();
                let index = event["index"]
                    .as_u64()
                    .context("Claude stream omitted block index")?;
                let value = &event["content_block"];
                let kind = value["type"].as_str().unwrap_or("other");
                let mut blocks = turn.blocks.lock().await;
                if blocks.len() >= 2048 || id.len() > 1024 {
                    bail!("Claude content block limit exceeded");
                }
                let block = Block {
                    kind: kind.into(),
                    tool_id: value["id"].as_str().unwrap_or("").into(),
                    name: value["name"].as_str().unwrap_or("Claude tool").into(),
                    value: match kind {
                        "text" => value["text"].as_str().unwrap_or(""),
                        "thinking" => value["thinking"].as_str().unwrap_or(""),
                        _ => "",
                    }
                    .into(),
                    finalized: false,
                };
                if kind == "tool_use" {
                    emit_tool(self.client()?, &session.id, value, false)?;
                    turn.tools.lock().await.insert(block.tool_id.clone());
                } else if !block.value.is_empty() {
                    turn.reserve(block.value.len())?;
                    if kind == "text" {
                        turn.emitted_text.store(true, Ordering::SeqCst);
                    }
                    text(
                        self.client()?,
                        &session.id,
                        if kind == "thinking" {
                            "agent_thought_chunk"
                        } else {
                            "agent_message_chunk"
                        },
                        &block.value,
                    )?;
                }
                blocks.insert((id, index), block);
            }
            Some("content_block_delta") => {
                let id = turn.message_id.lock().await.clone();
                let index = event["index"]
                    .as_u64()
                    .context("Claude delta omitted block index")?;
                let mut blocks = turn.blocks.lock().await;
                let block = blocks
                    .get_mut(&(id, index))
                    .context("Claude delta preceded block start")?;
                let delta = &event["delta"];
                let fragment = match delta["type"].as_str() {
                    Some("text_delta") => delta["text"].as_str(),
                    Some("thinking_delta") => delta["thinking"].as_str(),
                    Some("input_json_delta") => delta["partial_json"].as_str(),
                    _ => None,
                };
                if let Some(fragment) = fragment {
                    turn.reserve(fragment.len())?;
                    if block.value.len() + fragment.len() > TEXT_LIMIT {
                        bail!("Claude block exceeded limit");
                    }
                    block.value.push_str(fragment);
                    if block.kind == "text" || block.kind == "thinking" {
                        if block.kind == "text" {
                            turn.emitted_text.store(true, Ordering::SeqCst);
                        }
                        text(
                            self.client()?,
                            &session.id,
                            if block.kind == "thinking" {
                                "agent_thought_chunk"
                            } else {
                                "agent_message_chunk"
                            },
                            fragment,
                        )?;
                    }
                }
            }
            Some("content_block_stop") => {
                let id = turn.message_id.lock().await.clone();
                let index = event["index"]
                    .as_u64()
                    .context("Claude block stop omitted index")?;
                let blocks = turn.blocks.lock().await;
                if let Some(block) = blocks.get(&(id, index))
                    && block.kind == "tool_use"
                    && !block.value.is_empty()
                {
                    let input: Value = serde_json::from_str(&block.value)?;
                    emit_tool(
                        self.client()?,
                        &session.id,
                        &json!({"id":block.tool_id,"name":block.name,"input":input}),
                        true,
                    )?;
                }
            }
            _ => {}
        }
        Ok(())
    }
    async fn complete_block(
        &self,
        session: &Session,
        turn: Option<&Turn>,
        block: &Value,
        message_id: &Value,
    ) -> Result<()> {
        match block["type"].as_str() {
            Some("text" | "thinking") => {
                let kind = block["type"].as_str().unwrap();
                let value = block[if kind == "text" { "text" } else { "thinking" }]
                    .as_str()
                    .unwrap_or("");
                let mut suffix = value;
                if let Some(turn) = turn {
                    let mut blocks = turn.blocks.lock().await;
                    if let Some((_, streamed)) = blocks
                        .iter_mut()
                        .filter(|((id, _), streamed)| {
                            id == message_id.as_str().unwrap_or("")
                                && streamed.kind == kind
                                && !streamed.finalized
                        })
                        .min_by_key(|((_, index), _)| *index)
                    {
                        if !value.starts_with(&streamed.value) {
                            bail!("Claude final content differs from streamed content");
                        }
                        suffix = &value[streamed.value.len()..];
                        streamed.finalized = true;
                    }
                    turn.reserve(suffix.len())?;
                    if kind == "text" && !value.is_empty() {
                        turn.emitted_text.store(true, Ordering::SeqCst);
                    }
                }
                text(
                    self.client()?,
                    &session.id,
                    if kind == "text" {
                        "agent_message_chunk"
                    } else {
                        "agent_thought_chunk"
                    },
                    suffix,
                )?;
            }
            Some("tool_use") => {
                let id = block["id"].as_str().context("Claude tool omitted ID")?;
                let existing = if let Some(turn) = turn {
                    let mut tools = turn.tools.lock().await;
                    if tools.len() >= 2048 {
                        bail!("Claude tool count exceeded limit");
                    }
                    !tools.insert(id.into())
                } else {
                    false
                };
                emit_tool(self.client()?, &session.id, block, existing)?;
                if block["name"] == "TodoWrite" {
                    let entries=block["input"]["todos"].as_array().into_iter().flatten().take(128).map(|entry|json!({"content":bounded(entry["content"].as_str().unwrap_or(""),4096),"priority":"medium","status":match entry["status"].as_str(){Some("completed")=>"completed",Some("in_progress")=>"in_progress",_=>"pending"}})).collect::<Vec<_>>();
                    update(
                        self.client()?,
                        &session.id,
                        json!({"sessionUpdate":"plan","entries":entries}),
                    )?;
                }
            }
            _ => {}
        }
        Ok(())
    }
    async fn tool_result(
        &self,
        session: &Session,
        turn: Option<&Turn>,
        block: &Value,
    ) -> Result<()> {
        let id = block["tool_use_id"]
            .as_str()
            .context("Claude result omitted tool ID")?;
        if let Some(turn) = turn
            && !turn.tools.lock().await.contains(id)
        {
            emit_tool(
                self.client()?,
                &session.id,
                &json!({"id":id,"name":"Claude tool","input":{}}),
                false,
            )?;
            turn.tools.lock().await.insert(id.into());
        }
        let blocks = content(&block["content"]);
        let output = blocks
            .iter()
            .filter_map(|block| block["text"].as_str())
            .collect::<Vec<_>>()
            .join("\n");
        update(
            self.client()?,
            &session.id,
            json!({"sessionUpdate":"tool_call_update","toolCallId":id,"status":if block["is_error"]==true{"failed"}else{"completed"},"rawOutput":{"output":bounded(&output,TOOL_LIMIT)},"content":[{"type":"content","content":{"type":"text","text":bounded(&output,TOOL_LIMIT)}}]}),
        )?;
        for image in blocks
            .iter()
            .filter(|block| block["type"] == "image")
            .take(32)
        {
            let source = &image["source"];
            if source["type"] == "base64"
                && source["data"]
                    .as_str()
                    .is_some_and(|data| data.len() < 2 * 1024 * 1024 - 4096)
            {
                update(
                    self.client()?,
                    &session.id,
                    json!({"sessionUpdate":"agent_message_chunk","content":{"type":"image","mimeType":source["media_type"],"data":source["data"]}}),
                )?;
            }
        }
        Ok(())
    }
    async fn approval(
        &self,
        session: &Session,
        message: Value,
        turn: Arc<Turn>,
        ask_live: Arc<AtomicBool>,
        mut cancelled: watch::Receiver<bool>,
    ) -> Result<()> {
        let native = session.native()?;
        let request = &message["request"];
        let id = message["request_id"]
            .as_str()
            .context("Claude control omitted request ID")?;
        let tool_id = request["tool_use_id"]
            .as_str()
            .context("Claude permission omitted tool ID")?;
        let input = if request["input"].to_string().len() > 65536 {
            json!({"summary":bounded(&request["input"].to_string(),65536)})
        } else {
            request["input"].clone()
        };
        let future=self.client()?.send_request(UntypedMessage::new("session/request_permission",json!({"sessionId":session.id,"toolCall":{"toolCallId":tool_id,"kind":tool_kind(request["tool_name"].as_str().unwrap_or("")),"title":bounded(request["title"].as_str().or_else(||request["tool_name"].as_str()).unwrap_or("Claude permission"),4096),"status":"pending","rawInput":input},"options":[{"optionId":"allow-once","name":"Allow once","kind":"allow_once"},{"optionId":"reject-once","name":"Reject once","kind":"reject_once"}]}))?).block_task();
        let mut stopped = turn.cancelled.subscribe();
        let mut done = turn.done.subscribe();
        let mut closed = native.closed();
        let answer = if !turn.live.load(Ordering::SeqCst)
            || !ask_live.load(Ordering::SeqCst)
            || *closed.borrow()
        {
            None
        } else {
            tokio::select! {answer=future=>answer.ok(),_=cancelled.changed()=>None,_=stopped.changed()=>None,_=done.changed()=>None,_=closed.changed()=>None}
        };
        let granted = answer.is_some_and(|answer| {
            answer["outcome"]["outcome"] == "selected"
                && answer["outcome"]["optionId"] == "allow-once"
        });
        let response = if granted {
            json!({"behavior":"allow","updatedInput":request["input"]})
        } else {
            json!({"behavior":"deny","message":"ACP permission rejected or cancelled"})
        };
        // No permission_suggestions or updatedPermissions are ever persisted.
        let result=native.write(json!({"type":"control_response","response":{"subtype":"success","request_id":id,"response":response}}),Some((ask_live,turn.live.clone()))).await;
        session.approvals.lock().unwrap().remove(id);
        result
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
        let sessions = self
            .sessions
            .lock()
            .await
            .drain()
            .map(|(_, session)| session)
            .collect::<Vec<_>>();
        for session in sessions {
            if let Some(turn) = session.active.lock().await.as_ref() {
                turn.finish(Terminal::Error("ACP client disconnected".into()));
            }
            session.cancel_approvals();
            if let Some(native) = session.native.get() {
                native.shutdown().await?;
            }
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

async fn session_info(session: &Session) -> Value {
    let settings = session.settings.lock().await;
    let catalog = session.catalog.lock().await;
    let models=catalog.iter().map(|model|json!({"modelId":model["value"],"name":model["displayName"],"description":model["description"]})).collect::<Vec<_>>();
    json!({"sessionId":session.id,"modes":modes(&settings.mode),"models":{"currentModelId":settings.model,"availableModels":models},"configOptions":config_options(&settings,&catalog)})
}

async fn deny_control(session: &Session, message: &Value) -> Result<()> {
    let id = message["request_id"]
        .as_str()
        .context("Claude control omitted ID")?;
    if message["request"]["subtype"] == "can_use_tool" {
        session.native()?.write(json!({"type":"control_response","response":{"subtype":"success","request_id":id,"response":{"behavior":"deny","message":"Unsupported or inactive ACP permission","interrupt":true}}}),None).await
    } else {
        session.native()?.write(json!({"type":"control_response","response":{"subtype":"error","request_id":id,"error":"Unsupported control request"}}),None).await
    }
}
