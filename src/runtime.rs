use crate::connectors::matrix_restart::{
    MATRIX_RESTART_ACK, RestartDispatcher, SystemdUserRestart, has_restart_receipt,
    record_restart_receipt, resolve_restart_command,
};
use crate::{
    backend::{Client, Options, Transport},
    config::{boolean, string, strings},
    store::{Entry, Store},
};
use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, HashMap},
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::Mutex;

/// Startup-only minutes policy for the seven platform connectors.
/// Slack defaults to 30 minutes; the others disable expiry on invalid input.
#[derive(Clone, Debug)]
pub struct SessionRetentionPolicy {
    base_minutes: Option<f64>,
    slack_minutes: Option<f64>,
}

impl SessionRetentionPolicy {
    /// Derive policy once from the resolved project environment, without ambient reads.
    /// Uses decimal JavaScript `parseInt` prefixes and finite binary64 minutes.
    /// Large finite values are retained, not clamped to a shorter Rust duration.
    pub fn from_environment(environment: &BTreeMap<String, String>) -> Self {
        let minutes = environment
            .get("SESSION_RETENTION_MINS")
            .and_then(|raw| parse_retention_minutes(raw));
        Self {
            base_minutes: minutes,
            slack_minutes: Some(minutes.unwrap_or(30.0)),
        }
    }

    /// Whether this platform has a startup expiry policy (not whether it is enabled).
    pub fn is_enabled(&self, platform: &str) -> bool {
        self.minutes(platform).is_some()
    }

    fn minutes(&self, platform: &str) -> Option<f64> {
        match platform {
            "slack" => self.slack_minutes,
            "matrix" | "discord" | "mattermost" | "telegram" | "whatsapp" | "web" => {
                self.base_minutes
            }
            _ => None,
        }
    }
}

fn parse_retention_minutes(raw: &str) -> Option<f64> {
    let raw = raw.trim_start_matches(|character| {
        matches!(
            character,
            '\t' | '\u{000b}' | '\u{000c}' | ' ' | '\u{00a0}' | '\u{1680}' | '\u{2000}'
                ..='\u{200a}'
                    | '\u{202f}'
                    | '\u{205f}'
                    | '\u{3000}'
                    | '\u{feff}'
                    | '\n'
                    | '\r'
                    | '\u{2028}'
                    | '\u{2029}'
        )
    });
    let raw = raw.strip_prefix('+').unwrap_or(raw);
    let prefix = raw.bytes().take_while(|byte| byte.is_ascii_digit()).count();
    raw[..prefix]
        .parse::<f64>()
        .ok()
        .filter(|minutes| minutes.is_finite() && *minutes > 0.0)
}

fn project_name(cwd: &str) -> &str {
    Path::new(cwd)
        .file_name()
        .and_then(|name| name.to_str())
        .filter(|name| !name.is_empty())
        .unwrap_or(cwd)
}
fn history_fingerprint(history: &[Value]) -> String {
    let normalized = history
        .iter()
        .filter_map(|update| {
            update["content"]["text"].as_str().map(|text| {
                format!(
                    "{}:{}",
                    update["sessionUpdate"].as_str().unwrap_or(""),
                    text.trim()
                )
            })
        })
        .collect::<Vec<_>>()
        .join("\n---\n");
    format!("{:x}", Sha256::digest(normalized.as_bytes()))
}
fn picker_index(argument: &str) -> Option<usize> {
    argument
        .split_whitespace()
        .next()?
        .parse::<usize>()
        .ok()?
        .checked_sub(1)
}
fn history_message(history: &[Value], unavailable: bool) -> String {
    if unavailable {
        return "Recent history: unavailable for this session; attached without replay.".into();
    }
    let mut items = Vec::<(String, String)>::new();
    for update in history {
        let role = match update["sessionUpdate"].as_str() {
            Some("user_message_chunk") => "User",
            Some("agent_message_chunk") => "Assistant",
            _ => continue,
        };
        let Some(text) = update["content"]["text"].as_str() else {
            continue;
        };
        if let Some((previous, content)) = items.last_mut()
            && previous == role
        {
            content.push_str(text);
        } else {
            items.push((role.into(), text.into()));
        }
    }
    items.retain(|(_, text)| !text.trim().is_empty());
    if items.is_empty() {
        return "Recent history: no conversational messages found.".into();
    }
    format!(
        "Recent history:\n{}",
        items
            .iter()
            .rev()
            .take(6)
            .collect::<Vec<_>>()
            .into_iter()
            .rev()
            .map(|(role, text)| format!(
                "{role}: {}",
                text.trim().chars().take(500).collect::<String>()
            ))
            .collect::<Vec<_>>()
            .join("\n")
    )
}

#[derive(Clone, Debug)]
pub struct Event {
    pub platform: String,
    pub room: String,
    pub thread: String,
    pub id: String,
    pub reply_to: Option<String>,
    pub user: String,
    pub text: String,
    pub direct: bool,
    pub mentioned: bool,
    pub bot: bool,
    pub images: Vec<Value>,
}
impl Event {
    pub fn web(user: &str, text: &str) -> Self {
        Self {
            platform: "web".into(),
            room: user.into(),
            thread: "".into(),
            id: "".into(),
            reply_to: None,
            user: user.into(),
            text: text.into(),
            direct: true,
            mentioned: false,
            bot: false,
            images: vec![],
        }
    }
}
#[async_trait]
pub trait Sink: Send + Sync {
    async fn emit(&self, event: Value) -> Result<()>;
    fn matrix_user_id(&self) -> Option<String> {
        None
    }
    fn matrix_raw_thread(&self) -> Option<String> {
        None
    }
    fn owned(&self) -> Option<Arc<dyn Sink>> {
        None
    }
    async fn upload(&self, _name: &str, _mime: &str, _bytes: Vec<u8>) -> Result<()> {
        bail!("This connector does not support media uploads")
    }
}

#[derive(Default)]
struct InlineDelivery {
    attempted: std::collections::HashSet<String>,
    processed: usize,
    uploaded: usize,
    failed: usize,
}

async fn deliver_inline(
    client: &Client,
    sink: &dyn Sink,
    delivery: &mut InlineDelivery,
    image_count: usize,
) {
    for image in client
        .inline_images()
        .await
        .into_iter()
        .take(image_count)
        .skip(delivery.processed)
    {
        delivery.processed += 1;
        delivery.attempted.insert(image.path.clone());
        if let Ok(media) =
            crate::media::read(client.working_directory(), &image.path, 20 * 1024 * 1024)
            && sink
                .upload(
                    image.alt.as_deref().unwrap_or("Image"),
                    &media.mime,
                    media.bytes,
                )
                .await
                .is_ok()
        {
            delivery.uploaded += 1;
        } else {
            delivery.failed += 1;
        }
    }
}

async fn finish_response(
    sink: &dyn Sink,
    text: &str,
    mut observation: Value,
    uploaded: usize,
) -> Result<()> {
    let result = sink.emit(json!({"type":"response","text":text})).await;
    observation["deliveredChars"] = if result.is_ok() {
        text.encode_utf16().count().into()
    } else {
        0.into()
    };
    observation["uploadedImages"] = uploaded.into();
    if observation["uploadedMedia"].is_null() {
        observation["uploadedMedia"] = uploaded.into();
    }
    observation["deliveryFailed"] = result.is_err().into();
    observation["usableOutput"] = (observation["uploadedMedia"].as_u64().unwrap_or(0) > 0
        || (result.is_ok() && observation["cleanChars"].as_u64().unwrap_or(0) > 0))
        .into();
    crate::debug_log::observation(&observation);
    let _ = sink.emit(observation).await;
    result
}

async fn present_update(
    sink: &dyn Sink,
    update: &Value,
    captured: &mut String,
    presenter: &mut crate::tools::TurnPresenter,
) {
    if update["method"] == "bridge/form" {
        let text = match update["params"]["status"].as_str() {
            Some("answered") => "OpenCode confirmed the question answer.",
            Some("cancelled") => "OpenCode confirmed that the question was dismissed.",
            _ => "The question was resolved elsewhere or is no longer active.",
        };
        let _ = sink.emit(json!({"type":"notice", "text":text})).await;
        return;
    }
    match update["sessionUpdate"].as_str().unwrap_or("") {
        "agent_message_chunk" if update["content"]["type"] == "text" => {
            if let Some(text) = update["content"]["text"].as_str() {
                if captured.len().saturating_add(text.len()) <= 2 * 1024 * 1024 {
                    captured.push_str(text);
                }
                let _ = sink.emit(json!({"type":"chunk","text":text})).await;
            }
        }
        "permission_rejected" => {
            let _ = sink.emit(json!({"type":"notice","text":format!("> {}", update["message"].as_str().unwrap_or("Permission denied")),"permission":update["permission"],"path":update["path"],"sessionId":update["sessionId"],"toolCallId":update["toolCallId"]})).await;
        }
        "tool_call" | "tool_call_update" => {
            for activity in presenter.update(update) {
                let _ = sink.emit(activity).await;
            }
        }
        _ => {}
    }
}
async fn matrix_read(sink: &dyn Sink, id: &str) {
    let receipt = json!({"type":"read","eventId":id});
    if let Some(owned) = sink.owned() {
        tokio::spawn(async move {
            let _ = owned.emit(receipt).await;
        });
        tokio::task::yield_now().await;
    } else {
        let _ = sink.emit(receipt).await;
    }
}
struct Conversation {
    client: Option<Client>,
    backend: String,
    created_at: Instant,
    last_activity: Instant,
    message_count: usize,
    input_chars: usize,
    output_chars: usize,
    projects: Vec<String>,
    picker: Vec<Value>,
    selected_cwd: Option<String>,
    mirror: Option<Mirror>,
}
#[derive(Default)]
struct ConversationSlot {
    state: Mutex<Conversation>,
    matrix_admission: std::sync::Mutex<MatrixAdmission>,
    expiry_cleanup: Mutex<Option<Client>>,
}

async fn cleanup_expired_client(cleanup: &mut Option<Client>) -> bool {
    let Some(client) = cleanup.as_ref() else {
        return true;
    };
    match tokio::time::timeout(Duration::from_secs(2), client.shutdown()).await {
        Ok(Ok(())) => {
            *cleanup = None;
            true
        }
        Ok(Err(error)) => {
            eprintln!("Session expiry ACP cleanup failed; retained for retry: {error}");
            false
        }
        Err(_) => {
            eprintln!("Session expiry ACP cleanup timed out; retained for retry");
            false
        }
    }
}
#[derive(Default)]
struct MatrixAdmission {
    turn: Option<Arc<MatrixTurn>>,
    control_pending: bool,
    requests: usize,
}
struct RequestAdmissionGuard<'a>(&'a ConversationSlot);
impl Drop for RequestAdmissionGuard<'_> {
    fn drop(&mut self) {
        self.0.matrix_admission.lock().unwrap().requests -= 1;
    }
}
#[derive(Default)]
struct MatrixTurn {
    cancelled: std::sync::atomic::AtomicBool,
    forwarded: std::sync::atomic::AtomicBool,
    cancel_dispatch: Mutex<()>,
}
struct MatrixAdmissionGuard<'a>(&'a ConversationSlot, Option<Arc<MatrixTurn>>);
impl Drop for MatrixAdmissionGuard<'_> {
    fn drop(&mut self) {
        let mut admission = self.0.matrix_admission.lock().unwrap();
        if let Some(turn) = &self.1 {
            if admission
                .turn
                .as_ref()
                .is_some_and(|active| Arc::ptr_eq(active, turn))
            {
                admission.turn = None;
            }
        } else {
            admission.control_pending = false;
        }
    }
}
fn matrix_control(command: &str) -> bool {
    matches!(
        command,
        "/model"
            | "/models"
            | "/effort"
            | "/mode"
            | "/agent"
            | "/commands"
            | "/backend"
            | "/config"
    )
}
fn matrix_session_change(command: &str) -> bool {
    matches!(
        command,
        "/clear"
            | "/reset"
            | "/p"
            | "/projects"
            | "/s"
            | "/sessions"
            | "/m"
            | "/mirror"
            | "/r"
            | "/reload"
            | "/d"
            | "/detach"
    )
}
fn matrix_query(command: &str) -> bool {
    !matrix_control(command)
        && !matrix_session_change(command)
        && !matches!(command, "/status" | "/help" | "/h" | "/cancel" | "/restart")
}
const MATRIX_COMMAND_MENU: &str = "Matrix commands:
- /backend [name] - Show or switch this thread’s backend; histories remain separate
- /help - Show this quick reference
- /status - Show bridge and session status
- /model [provider/model] - Show or switch the session model
- /models - List advertised models
- /effort [status|list|value] - Show or select advertised reasoning effort
- /mode [status|list|value] - Show or select advertised session mode
- /cancel - Request cancellation in this thread
- /compact - Compact the current backend conversation
- /commands - Show this menu; add all [page] to browse backend commands
- /clear - Delete current session history; next message starts fresh
- /reset - Alias of clear; also deletes current session history
- /restart - Restart the configured service when idle (if enabled)";
fn matrix_config_choices(options: &Value) -> Vec<&Value> {
    fn collect<'a>(options: &'a Value, choices: &mut Vec<&'a Value>, depth: usize) {
        if depth > 32 {
            return;
        }
        if let Some(options) = options.as_array() {
            for option in options {
                if option["value"].is_string() {
                    choices.push(option);
                }
                collect(&option["options"], choices, depth + 1);
            }
        }
    }
    let mut choices = vec![];
    collect(options, &mut choices, 0);
    choices
}
async fn matrix_session_control(client: &Client, command: &str, argument: &str) -> Result<String> {
    let arguments: Vec<_> = argument.split_whitespace().collect();
    if command == "/commands" {
        if arguments.is_empty() {
            return Ok(MATRIX_COMMAND_MENU.into());
        }
        let arguments = if arguments[0] == "all" {
            &arguments[1..]
        } else {
            &arguments[..]
        };
        let page = if arguments.is_empty() {
            1.0
        } else {
            arguments[0].parse::<f64>().unwrap_or(f64::NAN)
        };
        if arguments.len() > 1 || !page.is_finite() || page < 1.0 || page.fract() != 0.0 {
            bail!("Usage: /commands [all [page-number] | page-number]");
        }
        let commands = client.commands().await;
        let pages = commands.len().div_ceil(30).max(1);
        if page > pages as f64 {
            bail!("Unknown page. /commands has {pages} page(s).");
        }
        let page = page as usize;
        let backend = commands
            .iter()
            .skip((page - 1) * 30)
            .take(30)
            .map(|command| {
                let description = string(command, "description")
                    .split_whitespace()
                    .collect::<Vec<_>>()
                    .join(" ");
                format!(
                    "- /{} - {}",
                    string(command, "name"),
                    description.chars().take(120).collect::<String>()
                )
            })
            .collect::<Vec<_>>()
            .join("\n");
        let backend = if backend.is_empty() {
            "(none received yet; ACP advertisement is asynchronous)"
        } else {
            &backend
        };
        let next = if page < pages {
            format!("\nNext: /commands all {}", page + 1)
        } else {
            String::new()
        };
        return Ok(format!(
            "Backend commands (page {page}/{pages}; {} advertised for this session):\n{backend}{next}",
            commands.len()
        ));
    }
    let options = client.config_options().await;
    let key = match command {
        "/effort" => "effort",
        "/mode" | "/agent" => "mode",
        _ => "model",
    };
    let option = options.iter().find(|option| {
        option["id"] == key
            || option["category"] == key
            || (key == "effort" && option["category"] == "thought_level")
    });
    if command == "/models" {
        if !arguments.is_empty() {
            bail!("Usage: /models");
        }
        let choices = option
            .map(|option| matrix_config_choices(&option["options"]))
            .unwrap_or_default();
        if choices.is_empty() {
            bail!("This ACP session advertises no model choices");
        }
        return Ok(format!(
            "Available models:\n{}",
            choices
                .iter()
                .map(|choice| {
                    let name = string(choice, "name");
                    format!(
                        "- {}{}",
                        string(choice, "value"),
                        if name.is_empty() {
                            String::new()
                        } else {
                            format!(" ({name})")
                        }
                    )
                })
                .collect::<Vec<_>>()
                .join("\n")
        ));
    }
    let option = option.with_context(|| format!("This ACP session does not advertise {key}"))?;
    if arguments.is_empty()
        || (arguments.len() == 1
            && (arguments[0] == "status" || (key != "model" && arguments[0] == "list")))
    {
        let current = string(option, "currentValue");
        let current = if current.is_empty() {
            "unknown"
        } else {
            current
        };
        if key == "model" {
            return Ok(format!("Current model: {current}"));
        }
        let choices = matrix_config_choices(&option["options"])
            .iter()
            .map(|choice| string(choice, "value"))
            .collect::<Vec<_>>()
            .join(", ");
        return Ok(format!(
            "{key}: {current}; choices: {}",
            if choices.is_empty() {
                "none advertised"
            } else {
                &choices
            }
        ));
    }
    if arguments.len() != 1 || arguments[0] == "status" {
        bail!("Usage: /{key} [status|list|value]");
    }
    let confirmed = client
        .set_option(string(option, "id"), arguments[0])
        .await?;
    Ok(format!(
        "ACP session {key} selected: {}",
        string(&confirmed, "currentValue")
    ))
}
impl std::ops::Deref for ConversationSlot {
    type Target = Mutex<Conversation>;
    fn deref(&self) -> &Self::Target {
        &self.state
    }
}
#[derive(Clone)]
struct Mirror {
    generation: uuid::Uuid,
    session: Value,
    backend: String,
    fingerprint: String,
    next_poll: Instant,
    sink: Arc<dyn Sink>,
}
struct Attachment<'a> {
    backend: &'a str,
    session: &'a Value,
    expected_identity: Option<&'a str>,
    reload: bool,
}
impl Default for Conversation {
    fn default() -> Self {
        Self {
            client: None,
            backend: String::new(),
            created_at: Instant::now(),
            last_activity: Instant::now(),
            message_count: 0,
            input_chars: 0,
            output_chars: 0,
            projects: vec![],
            picker: vec![],
            selected_cwd: None,
            mirror: None,
        }
    }
}
// Approval controls bypass ordinary addressing, never broker authorization.
pub(crate) fn telegram_approval_control(event: &Event) -> bool {
    event.platform == "telegram"
        && event
            .text
            .split_whitespace()
            .next()
            .is_some_and(|word| matches!(word.to_ascii_lowercase().as_str(), "/approve" | "/deny"))
}
pub struct Bridge {
    pub config: Value,
    pub env: BTreeMap<String, String>,
    workspace: PathBuf,
    store: Store,
    sessions: Mutex<HashMap<String, Arc<ConversationSlot>>>,
    dedup: Mutex<HashMap<String, Instant>>,
    rates: Mutex<HashMap<String, Instant>>,
    matrix_work: Arc<tokio::sync::RwLock<()>>,
    restart_pending: Arc<std::sync::atomic::AtomicBool>,
    restart_dispatcher: Arc<dyn RestartDispatcher>,
    permissions: crate::connectors::matrix_permissions::Broker,
    questions: crate::connectors::matrix_questions::Broker,
}
impl Bridge {
    fn permission_scope(
        &self,
        event: &Event,
        sink: &dyn Sink,
        cwd: &Path,
    ) -> crate::connectors::matrix_permissions::Scope {
        crate::connectors::matrix_permissions::Scope {
            sender: event.user.clone(),
            room: event.room.clone(),
            thread: sink
                .matrix_raw_thread()
                .unwrap_or_else(|| event.thread.clone()),
            context: format!("{}\0{}", event.platform, self.context(event)),
            cwd: cwd.into(),
        }
    }
    pub fn working_directory(&self, event: &Event) -> PathBuf {
        session_dir(&self.workspace, &event.platform, &self.context(event))
    }
    pub fn user_allowed(&self, event: &Event) -> bool {
        let config = &self.config[&event.platform];
        let allowed = self
            .env
            .get(&format!("{}_ALLOWED_USERS", event.platform.to_uppercase()))
            .map(|value| csv(value))
            .filter(|values| !values.is_empty())
            .unwrap_or_else(|| strings(config, "allowedUsers"));
        (allowed.is_empty() || allowed.contains(&event.user))
            && !strings(config, "ignoreUsers").contains(&event.user)
            && !["ignoreRooms", "ignoreChannels", "ignoreChats"]
                .iter()
                .any(|key| strings(config, key).contains(&event.room))
            && !event.bot
            && event.user.len() <= 256
            && event.room.len() <= 1024
    }
    pub async fn transport_admissible(&self, event: &Event) -> bool {
        if !self.user_allowed(event) {
            return false;
        }
        let id = format!("{}\0{}", event.platform, event.id);
        if !event.id.is_empty()
            && self
                .dedup
                .lock()
                .await
                .get(&id)
                .is_some_and(|time| time.elapsed() < Duration::from_secs(600))
        {
            return false;
        }
        if telegram_approval_control(event) {
            return true;
        }
        let rate = format!("{}\0{}", event.platform, event.user);
        let seconds = self.config["rateLimitSeconds"]
            .as_f64()
            .unwrap_or(5.0)
            .max(0.0);
        !self
            .rates
            .lock()
            .await
            .get(&rate)
            .is_some_and(|time| time.elapsed().as_secs_f64() < seconds)
    }
    pub fn new(config: Value, env: BTreeMap<String, String>, workspace: PathBuf) -> Result<Self> {
        Self::new_with_restart_dispatcher(config, env, workspace, Arc::new(SystemdUserRestart))
    }
    pub fn new_with_restart_dispatcher(
        config: Value,
        env: BTreeMap<String, String>,
        workspace: PathBuf,
        restart_dispatcher: Arc<dyn RestartDispatcher>,
    ) -> Result<Self> {
        let path = string(&config, "sessionStorePath");
        if path.is_empty() {
            bail!("sessionStorePath is required");
        }
        let store = Store::new(path);
        Ok(Self {
            config,
            env,
            workspace,
            store,
            sessions: Mutex::new(HashMap::new()),
            dedup: Mutex::new(HashMap::new()),
            rates: Mutex::new(HashMap::new()),
            matrix_work: Default::default(),
            restart_pending: Default::default(),
            restart_dispatcher,
            permissions: Default::default(),
            questions: Default::default(),
        })
    }
    pub fn context(&self, event: &Event) -> String {
        if boolean(&self.config[&event.platform], "threadIsolation") && !event.thread.is_empty() {
            format!("{}:{}", event.room, event.thread)
        } else {
            event.room.clone()
        }
    }
    pub fn selected(&self, platform: &str, context: &str) -> Result<String> {
        self.store.selected(
            platform,
            context,
            self.config["acp"]["defaultBackend"]
                .as_str()
                .unwrap_or("opencode"),
        )
    }
    pub fn has_persisted(&self, platform: &str, context: &str) -> Result<bool> {
        Ok(self
            .store
            .get(platform, context, &self.selected(platform, context)?)?
            .is_some())
    }
    pub async fn has_active_session(&self, platform: &str, context: &str) -> Result<bool> {
        if !self.has_persisted(platform, context)? {
            return Ok(false);
        }
        let slot = self
            .sessions
            .lock()
            .await
            .get(&format!("{platform}\0{context}"))
            .cloned();
        let Some(slot) = slot else {
            return Ok(false);
        };
        // A busy initialized session holds this lock throughout its coding turn.
        // Do not wait for completion merely to admit a follow-up.
        Ok(match slot.try_lock() {
            Ok(state) => state.client.is_some(),
            Err(_) => true,
        })
    }
    async fn conversation(&self, platform: &str, context: &str) -> Result<Arc<ConversationSlot>> {
        let key = format!("{platform}\0{context}");
        let mut sessions = self.sessions.lock().await;
        if !sessions.contains_key(&key) && sessions.len() >= 1024 {
            bail!("Session capacity reached");
        }
        Ok(sessions
            .entry(key)
            .or_insert_with(|| Arc::new(ConversationSlot::default()))
            .clone())
    }
    pub async fn handle(&self, event: Event, sink: &dyn Sink) -> Result<()> {
        self.handle_inner(event, sink, false, None).await
    }

    /// Resolve file contents only after the normal Matrix admission reservation.
    #[cfg(feature = "platform-sdks")]
    pub(crate) async fn handle_matrix_file(
        &self,
        event: Event,
        sink: &dyn Sink,
        loader: futures_util::future::BoxFuture<'_, Result<String>>,
    ) -> Result<()> {
        if event.platform != "matrix" {
            bail!("Matrix file ingress requires a Matrix event");
        }
        self.handle_inner(event, sink, false, Some(loader)).await
    }

    pub async fn handle_web(&self, event: Event, sink: &dyn Sink) -> Result<()> {
        if event.platform != "web" {
            bail!("Web ingress requires a Web event");
        }
        self.handle_inner(event, sink, true, None).await
    }

    async fn handle_inner(
        &self,
        mut event: Event,
        sink: &dyn Sink,
        source_web_commands: bool,
        file_loader: Option<futures_util::future::BoxFuture<'_, Result<String>>>,
    ) -> Result<()> {
        let file_input = file_loader.is_some();
        if ![
            "matrix",
            "mattermost",
            "discord",
            "slack",
            "telegram",
            "whatsapp",
            "web",
            "cli",
        ]
        .contains(&event.platform.as_str())
        {
            bail!("Unknown connector");
        }
        if event.bot
            || event.user.len() > 256
            || event.room.len() > 1024
            || event.thread.len() > 1024
        {
            return Ok(());
        }
        if event.text.encode_utf16().count() > 100_000 {
            return sink
                .emit(json!({"type":"error","message":"Message text is too long."}))
                .await;
        }
        let config = &self.config[&event.platform];
        let allow_env = self
            .env
            .get(&format!("{}_ALLOWED_USERS", event.platform.to_uppercase()));
        let allowed = allow_env
            .map(|value| csv(value))
            .filter(|list| !list.is_empty())
            .unwrap_or_else(|| strings(config, "allowedUsers"));
        if !allowed.is_empty() && !allowed.contains(&event.user) {
            return Ok(());
        }
        if strings(config, "ignoreUsers").contains(&event.user)
            || ["ignoreRooms", "ignoreChannels", "ignoreChats"]
                .iter()
                .any(|key| strings(config, key).contains(&event.room))
        {
            return Ok(());
        }
        if event.platform == "matrix" {
            let rooms = strings(config, "allowedRooms");
            if (boolean(config, "strictAllowedRooms") || !rooms.is_empty())
                && !rooms.contains(&event.room)
            {
                return Ok(());
            }
        }
        let context = self.context(&event);
        let trigger = self
            .env
            .get(&format!("{}_TRIGGER", event.platform.to_uppercase()))
            .map(String::as_str)
            .filter(|value| !value.is_empty())
            .unwrap_or_else(|| string(&self.config, "trigger"));
        let trimmed = event.text.trim();
        let matrix_user_id = sink
            .matrix_user_id()
            .unwrap_or_else(|| string(config, "userId").to_owned());
        // Question controls preserve JSON literally and still require scoped broker admission.
        let question_control = !file_input
            && event.platform == "matrix"
            && trimmed.split_whitespace().next().is_some_and(|word| {
                matches!(word.to_ascii_lowercase().as_str(), "/answer" | "/dismiss")
            });
        let query = if question_control || telegram_approval_control(&event) {
            Some(trimmed.to_owned())
        } else if !trigger.is_empty() && trimmed.starts_with(trigger) {
            Some(trimmed[trigger.len()..].trim().to_owned())
        } else if event.platform == "matrix"
            && !matrix_user_id.is_empty()
            && trimmed.contains(&matrix_user_id)
        {
            Some(trimmed.replacen(&matrix_user_id, "", 1).trim().to_owned())
        } else if event.platform == "matrix"
            && let Some(query) = crate::core::name_query(trimmed, string(&self.config, "botName"))
        {
            Some(query)
        } else if (event.mentioned && event.platform != "matrix")
            || (event.direct && event.platform != "discord")
            || event.platform == "web"
            || event.platform == "cli"
            || ((event.platform == "matrix" || event.platform == "mattermost")
                && boolean(config, "respondToAllMessages"))
            || (!event.thread.is_empty()
                && (event.platform != "matrix" || boolean(config, "threadIsolation"))
                && (event.platform != "matrix"
                    || (!trimmed.is_empty()
                        && !trimmed.to_lowercase().starts_with(&trigger.to_lowercase())
                        && !trimmed.contains(&matrix_user_id)))
                && self.has_persisted(&event.platform, &context)?
                && (boolean(config, "respondToThreadReplies")
                    || (event.platform != "matrix"
                        && boolean(config, "respondToImplicitTopicReplies"))))
        {
            Some(trimmed.to_owned())
        } else {
            None
        };
        let Some(query) = query else {
            return Ok(());
        };
        let query = if event.platform == "matrix" {
            query
                .trim_start_matches(|character: char| character == ':' || character.is_whitespace())
                .trim()
                .to_owned()
        } else {
            query
        };
        if query.is_empty() && event.images.is_empty() && !file_input {
            return Ok(());
        }
        // Files are ordinary queries, never slash controls, even when captioned /reset.
        event.text = if file_input {
            "Matrix attachment".into()
        } else {
            query
        };
        if event.platform == "matrix"
            && self
                .restart_pending
                .load(std::sync::atomic::Ordering::SeqCst)
        {
            return sink.emit(json!({"type":"notice","text":"A restart is being dispatched. Please continue after the service returns."})).await;
        }
        if !event.id.is_empty() {
            let mut dedup = self.dedup.lock().await;
            dedup.retain(|_, time| time.elapsed() < Duration::from_secs(600));
            let key = format!("{}\0{}", event.platform, event.id);
            if dedup.contains_key(&key) {
                return Ok(());
            }
            if dedup.len() >= 8192 {
                bail!("Event capacity reached");
            }
            dedup.insert(key, Instant::now());
        }
        if event.platform == "matrix"
            && event
                .text
                .split_whitespace()
                .next()
                .is_some_and(|command| command.eq_ignore_ascii_case("/restart"))
        {
            return self.restart_matrix(&event, &context, sink).await;
        }
        let _matrix_work = if event.platform == "matrix" {
            let work = self.matrix_work.clone().read_owned().await;
            if self
                .restart_pending
                .load(std::sync::atomic::Ordering::SeqCst)
            {
                return sink.emit(json!({"type":"notice","text":"A restart is being dispatched. Please continue after the service returns."})).await;
            }
            Some(work)
        } else {
            None
        };
        if event.platform != "matrix" && !self.take_rate(&event, sink).await? {
            return Ok(());
        }
        let conversation = self.conversation(&event.platform, &context).await?;
        let _request_guard = {
            conversation.matrix_admission.lock().unwrap().requests += 1;
            RequestAdmissionGuard(&conversation)
        };
        let command = event
            .text
            .split_whitespace()
            .next()
            .unwrap_or("")
            .to_ascii_lowercase();
        let (mut turn, mut _admission_guard, refusal) = if event.platform == "matrix" {
            let mut admission = conversation.matrix_admission.lock().unwrap();
            if (matrix_control(&command) || matrix_session_change(&command))
                && (admission.turn.is_some() || admission.control_pending)
            {
                (
                    None,
                    None,
                    Some(
                        "A request is already running. Please wait before using ACP session controls.",
                    ),
                )
            } else if matrix_query(&command) && admission.control_pending {
                (
                    None,
                    None,
                    Some(
                        "Session controls are being updated. Please wait for that change to finish.",
                    ),
                )
            } else if matrix_control(&command) {
                admission.control_pending = true;
                (None, Some(MatrixAdmissionGuard(&conversation, None)), None)
            } else if matrix_query(&command) && admission.turn.is_none() {
                let turn = Arc::new(MatrixTurn::default());
                admission.turn = Some(turn.clone());
                (
                    Some(turn.clone()),
                    Some(MatrixAdmissionGuard(&conversation, Some(turn))),
                    None,
                )
            } else {
                (admission.turn.clone(), None, None)
            }
        } else {
            (None, None, None)
        };
        if let Some(text) = refusal {
            return sink.emit(json!({"type":"notice","text":text})).await;
        }
        if event.platform == "matrix" && !self.take_rate(&event, sink).await? {
            return Ok(());
        }
        if event.platform == "matrix" && !event.id.is_empty() {
            matrix_read(sink, &event.id).await;
        }
        if let Some(loader) = file_loader {
            let loaded = loader.await;
            if turn
                .as_ref()
                .is_some_and(|turn| turn.cancelled.load(std::sync::atomic::Ordering::SeqCst))
            {
                return sink
                    .emit(json!({"type":"notice","text":"Request cancelled."}))
                    .await;
            }
            event.text = match loaded {
                Ok(text) if text.encode_utf16().count() <= 100_000 => text,
                Ok(_) => return sink.emit(json!({"type":"error","message":"Attachment text plus caption exceeds the message limit."})).await,
                Err(error) => return sink.emit(json!({"type":"error","message":format!("Attachment rejected: {error}")})).await,
            };
            // An inherited prompt can finish while media is being read. Renew
            // ownership before starting a new turn, or join the current owner.
            let (admitted, own_turn) = {
                let mut admission = conversation.matrix_admission.lock().unwrap();
                if admission.control_pending {
                    (None, false)
                } else if let Some(active) = &admission.turn {
                    (Some(active.clone()), false)
                } else {
                    let active = Arc::new(MatrixTurn::default());
                    admission.turn = Some(active.clone());
                    (Some(active), true)
                }
            };
            let Some(admitted) = admitted else {
                return sink.emit(json!({"type":"notice","text":"Session controls are being updated. Please wait for that change to finish."})).await;
            };
            if own_turn {
                _admission_guard =
                    Some(MatrixAdmissionGuard(&conversation, Some(admitted.clone())));
            } else if !turn
                .as_ref()
                .is_some_and(|previous| Arc::ptr_eq(previous, &admitted))
            {
                _admission_guard = None;
            }
            turn = Some(admitted);
        }
        if event.platform == "matrix"
            && self
                .questions
                .decide(
                    &self.permission_scope(&event, sink, Path::new("")),
                    &event.text,
                    sink,
                )
                .await?
        {
            return Ok(());
        }
        if matches!(event.platform.as_str(), "matrix" | "telegram")
            && matches!(command.as_str(), "/approve" | "/deny")
        {
            let words: Vec<_> = event.text.split_whitespace().collect();
            let decided = words.len() == 2
                && self.permissions.decide(
                    &self.permission_scope(&event, sink, Path::new("")),
                    words[1],
                    command == "/approve",
                    &event.id,
                );
            sink.emit(json!({"type":"notice", "text":if decided { "Approval decision recorded." } else { "No matching pending approval for this request." }})).await?;
            return Ok(());
        }
        let result = self
            .process(
                &event,
                &context,
                &conversation,
                turn,
                sink,
                source_web_commands,
            )
            .await;
        if let Err(error) = &result {
            if event.platform == "matrix" && matrix_control(&command) {
                let prefix = if command == "/backend" {
                    "Backend switch failed; previous selection and histories were preserved. Check backend configuration/authentication."
                } else {
                    "ACP control failed:"
                };
                sink.emit(json!({"type":"notice","text":format!("{prefix} {error}")}))
                    .await?;
            } else {
                let message = error
                    .downcast_ref::<crate::native_client::PromptFailure>()
                    .map_or(
                        "Sorry, something went wrong processing your request.",
                        |failure| failure.message.as_str(),
                    );
                sink.emit(json!({"type":"error","message":message})).await?;
            }
        }
        sink.emit(json!({"type":"done"})).await?;
        result
    }
    async fn take_rate(&self, event: &Event, sink: &dyn Sink) -> Result<bool> {
        if telegram_approval_control(event) {
            return Ok(true);
        }
        let mut rates = self.rates.lock().await;
        rates.retain(|_, time| time.elapsed() < Duration::from_secs(600));
        let rate_key = format!("{}\0{}", event.platform, event.user);
        let seconds = self.config["rateLimitSeconds"]
            .as_f64()
            .unwrap_or(5.0)
            .max(0.0);
        if rates
            .get(&rate_key)
            .is_some_and(|last| last.elapsed().as_secs_f64() < seconds)
        {
            sink.emit(
                json!({"type":"error","message":"Please wait a moment before sending again."}),
            )
            .await?;
            return Ok(false);
        }
        if rates.len() >= 8192 {
            bail!("Rate-limit capacity reached");
        }
        rates.insert(rate_key, Instant::now());
        Ok(true)
    }
    async fn restart_matrix(&self, event: &Event, context: &str, sink: &dyn Sink) -> Result<()> {
        if event.text.split_whitespace().count() != 1 {
            return sink
                .emit(json!({"type":"notice","text":"Usage: /restart"}))
                .await;
        }
        if event.id.is_empty() {
            return Ok(());
        }
        let receipt = PathBuf::from(format!("{}.restarts.json", self.store.0.display()));
        match has_restart_receipt(&receipt, &event.id) {
            Ok(true) => return Ok(()),
            Ok(false) => {},
            Err(_) => return sink.emit(json!({"type":"notice","text":"Restart unavailable: cannot verify the durable restart receipt."})).await,
        }
        let Some(command) = self
            .env
            .get("SOULSTEADS_MESSAGING_SYSTEMD_UNIT")
            .and_then(|unit| resolve_restart_command(unit))
        else {
            return sink.emit(json!({"type":"notice","text":"/restart is unavailable: SOULSTEADS_MESSAGING_SYSTEMD_UNIT must name an allowed .service unit."})).await;
        };
        let existing = self
            .sessions
            .lock()
            .await
            .get(&format!("matrix\0{context}"))
            .cloned();
        if let Some(existing) = existing
            && let Ok(mut state) = existing.try_lock()
            && state.mirror.take().is_some()
        {
            drop(state);
            sink.emit(json!({"type":"notice","text":"Mirror mode stopped."}))
                .await?;
        }
        if self
            .restart_pending
            .load(std::sync::atomic::Ordering::SeqCst)
        {
            return sink.emit(json!({"type":"notice","text":"A restart is being dispatched. Please continue after the service returns."})).await;
        }
        let Ok(_idle) = self.matrix_work.try_write() else {
            return sink.emit(json!({"type":"notice","text":"Cannot restart while a prompt, model command, or session initialization is active."})).await;
        };
        if !self.take_rate(event, sink).await? {
            return Ok(());
        }
        let Some(owned_sink) = sink.owned() else {
            return sink.emit(json!({"type":"notice","text":"Restart unavailable: this transport cannot retain its acknowledgement context."})).await;
        };
        self.restart_pending
            .store(true, std::sync::atomic::Ordering::SeqCst);
        let ack = sink
            .emit(json!({"type":"notice","text":MATRIX_RESTART_ACK}))
            .await;
        if ack.is_err() || record_restart_receipt(&receipt, &event.id).is_err() {
            self.restart_pending
                .store(false, std::sync::atomic::Ordering::SeqCst);
            return sink.emit(json!({"type":"notice","text":"Restart was not dispatched: acknowledgement or durable receipt failed."})).await;
        }
        matrix_read(sink, &event.id).await;
        let dispatcher = self.restart_dispatcher.clone();
        let pending = self.restart_pending.clone();
        tokio::spawn(async move {
            if let Err(error) = dispatcher.dispatch(command).await {
                pending.store(false, std::sync::atomic::Ordering::SeqCst);
                let _ = owned_sink.emit(json!({"type":"notice","text":format!("Restart dispatch failed: {error}. The service was not confirmed restarted.")})).await;
            }
        });
        Ok(())
    }
    async fn open(
        &self,
        platform: &str,
        context: &str,
        backend: &str,
        state: &mut Conversation,
    ) -> Result<Client> {
        if state.backend == backend
            && let Some(client) = &state.client
        {
            return Ok(client.clone());
        }
        let stored = self.store.get(platform, context, backend)?;
        let directory = stored
            .as_ref()
            .map(|entry| PathBuf::from(&entry.cwd))
            .unwrap_or_else(|| session_dir(&self.workspace, platform, context));
        let profile = if backend == "opencode" {
            self.backend_config(backend)?["profileDir"]
                .as_str()
                .filter(|profile| !profile.is_empty())
        } else {
            None
        };
        let applied = profile
            .map(|profile| crate::profile::apply(profile, &directory))
            .transpose()?;
        let directory = if let Some(applied) = &applied {
            applied.verify()?;
            applied.directory.clone()
        } else {
            std::fs::create_dir_all(&directory)?;
            std::fs::canonicalize(directory)?
        };
        let (client, identity) = self.launch(backend, &directory).await?;
        let publication = async {
            if let Some(applied) = &applied {
                applied.verify()?;
            }
            let session = if let Some(stored) = stored {
                if stored.cwd != directory.to_string_lossy() || stored.backend_id != identity {
                    bail!("Stored backend identity or workspace differs; history preserved");
                }
                client.load_session(&stored.session_id, true).await?;
                stored.session_id
            } else {
                client.new_session().await?
            };
            if let Some(applied) = &applied {
                applied.verify()?;
            }
            self.store.set(
                Entry {
                    connector: platform.into(),
                    thread_id: context.into(),
                    session_id: session,
                    cwd: directory.to_string_lossy().into(),
                    backend_id: identity,
                    updated_at: chrono::Utc::now().to_rfc3339(),
                    backend: backend.into(),
                },
                true,
            )?;
            Result::<()>::Ok(())
        }
        .await;
        if let Err(error) = publication {
            let _ = client.shutdown().await;
            return Err(error);
        }
        if let Some(old) = state.client.replace(client.clone()) {
            let _ = old.shutdown().await;
        }
        if state.backend != backend {
            state.projects.clear();
            state.picker.clear();
            state.selected_cwd = None;
            state.mirror = None;
        }
        state.backend = backend.into();
        state.created_at = Instant::now();
        if !matches!(platform, "matrix" | "cli") {
            state.last_activity = state.created_at;
            state.message_count = 0;
            state.input_chars = 0;
            state.output_chars = 0;
        }
        Ok(client)
    }

    fn picker_enabled(&self, platform: &str) -> bool {
        let picker = &self.config["sessionPicker"];
        let connectors = strings(picker, "connectors");
        boolean(picker, "enabled")
            && (connectors.is_empty() || connectors.iter().any(|name| name == platform))
    }

    async fn shared_help(&self, event: &Event, state: &Conversation) -> String {
        let trigger = self
            .env
            .get(&format!("{}_TRIGGER", event.platform.to_uppercase()))
            .map(String::as_str)
            .filter(|value| !value.is_empty())
            .unwrap_or_else(|| string(&self.config, "trigger"));
        let mut text = format!(
            "{} - Soulsteads Agent Messaging\n\nBridge commands:\n- /h or /help - Show this help\n- /status - Show current chat session info\n",
            string(&self.config, "botName")
        );
        if self.picker_enabled(&event.platform) {
            let seconds = self.config["sessionPicker"]["mirrorIntervalSeconds"]
                .as_f64()
                .filter(|value| *value != 0.0)
                .unwrap_or(60.0)
                .max(10.0);
            text.push_str(&format!("- /p - List saved projects/workdirs from the ACP backend\n- /p <n> - Select a project/workdir\n- /s - List saved sessions in the selected project\n- /s <n> - Switch to a saved session and show recent history\n- /m <n> - Mirror a saved session read-only, checking every {seconds}s\n- /m - Stop mirror mode\n- /r - Reload current session and show recent history\n- /d - Detach from the current session without deleting it\n"));
        }
        text.push_str("- /clear or /reset - Delete current session history\n");
        if matches!(event.platform.as_str(), "web" | "telegram" | "mattermost")
            && let Some(client) = &state.client
        {
            let commands = client.commands().await;
            if !commands.is_empty() {
                text.push_str("\nOpenCode commands:\n");
                for command in commands {
                    text.push_str(&format!(
                        "- /{} - {}\n",
                        command["name"].as_str().unwrap_or("undefined"),
                        command["description"].as_str().unwrap_or("undefined")
                    ));
                }
            }
        }
        text.push_str(&format!("\nUsage: {trigger} <your question>"));
        text
    }
    fn backend_identity(&self, backend: &str) -> Result<String> {
        let config = self.backend_config(backend)?;
        if !string(config, "backendId").is_empty() {
            return Ok(string(config, "backendId").to_owned());
        }
        let identity = std::iter::once(string(config, "command").to_owned())
            .chain(strings(config, "args"))
            .collect::<Vec<_>>();
        Ok(format!(
            "command-sha256:{:x}",
            Sha256::digest(serde_json::to_vec(&identity)?)
        ))
    }
    async fn clear_stored(&self, platform: &str, context: &str, backend: &str) -> Result<()> {
        let Some(entry) = self.store.get(platform, context, backend)? else {
            return Ok(());
        };
        // Never delete a session through a changed backend or a picked/foreign cwd.
        let expected = session_dir(&self.workspace, platform, context);
        std::fs::create_dir_all(&expected)?;
        let directory = std::fs::canonicalize(&expected)?;
        if entry.cwd != directory.to_string_lossy()
            || entry.backend_id != self.backend_identity(backend)?
        {
            return Ok(());
        }
        let config = self.backend_config(backend)?;
        let profile = if backend == "opencode" {
            config["profileDir"]
                .as_str()
                .filter(|value| !value.is_empty())
        } else {
            None
        };
        let applied = match profile
            .map(|profile| crate::profile::apply(profile, &directory))
            .transpose()
        {
            Ok(applied) => applied,
            // Unsafe or unavailable profile preparation must not authorize an
            // ACP launch, but must not prevent forgetting the local binding.
            Err(_) => return Ok(()),
        };
        if let Some(applied) = &applied
            && applied.verify().is_err()
        {
            return Ok(());
        }
        if let Ok((client, _)) = self.launch(backend, &directory).await {
            if client.load_session(&entry.session_id, true).await.is_ok() {
                let _ = client.delete_session().await;
            }
            let _ = client.shutdown().await;
        }
        Ok(())
    }
    async fn launch(&self, backend: &str, directory: &Path) -> Result<(Client, String)> {
        let config = self.backend_config(backend)?;
        let command = string(config, "command");
        if command.is_empty() {
            bail!("Backend executable not configured");
        }
        let (command, args) = crate::adapters::launch::resolve(
            backend,
            command,
            strings(config, "args"),
            self.env
                .get("OPENCODE_PATH")
                .filter(|path| Path::new(path).is_file())
                .map(String::as_str),
            Transport::select(backend, config)?,
        )?;
        let identity = self.backend_identity(backend)?;
        let mut env: BTreeMap<String, String> = if backend == "opencode" {
            self.env.clone()
        } else {
            BTreeMap::new()
        };
        env.extend(
            config["env"]
                .as_object()
                .into_iter()
                .flatten()
                .filter_map(|(key, value)| value.as_str().map(|value| (key.clone(), value.into()))),
        );
        let servers = config["mcpServers"].as_array().cloned().unwrap_or_default();
        let client = Client::connect(
            Transport::select(backend, config)?,
            Options {
                command,
                args,
                cwd: directory.to_owned(),
                env,
                inherit_env: backend == "opencode",
                timeout: Duration::from_secs(300),
            },
            servers,
        )
        .await?;
        Ok((client, identity))
    }
    async fn picker(
        &self,
        event: &Event,
        context: &str,
        command: &str,
        argument: &str,
        state: &mut Conversation,
        sink: &dyn Sink,
    ) -> Result<()> {
        if !boolean(&self.config["sessionPicker"], "enabled")
            || (!strings(&self.config["sessionPicker"], "connectors").is_empty()
                && !strings(&self.config["sessionPicker"], "connectors").contains(&event.platform))
        {
            return sink.emit(json!({"type":"response","text":format!("Unknown command: {}. Try /help",event.text)})).await;
        }
        let backend = self.selected(&event.platform, context)?;
        if matches!(command, "/m" | "/mirror") {
            if argument.is_empty() {
                let text = if state.mirror.take().is_some() {
                    "Mirror mode stopped."
                } else {
                    "Mirror mode is not running."
                };
                return sink.emit(json!({"type":"response","text":text})).await;
            }
            let Some(session) = picker_index(argument)
                .and_then(|index| state.picker.get(index))
                .cloned()
            else {
                return sink
                    .emit(json!({"type":"response","text":"Unknown session number. Run /s again."}))
                    .await;
            };
            let Some(owned) = sink.owned() else {
                bail!("Connector does not support persistent mirror output");
            };
            let history = self.mirror_snapshot(&backend, &session).await?;
            let text = format!(
                "Mirror mode on: {}\nProject: {}\nSession: {}\n\nChecking every {}s. Any message here stops mirroring.\n\n{}",
                session["title"].as_str().unwrap_or("(untitled session)"),
                session["cwd"].as_str().unwrap_or(""),
                session["sessionId"].as_str().unwrap_or(""),
                self.mirror_interval().as_secs(),
                history_message(&history, false).replacen("Recent history:", "Current tail:", 1)
            );
            state.mirror = Some(Mirror {
                generation: uuid::Uuid::new_v4(),
                session,
                backend,
                fingerprint: history_fingerprint(&history),
                next_poll: Instant::now() + self.mirror_interval(),
                sink: owned,
            });
            return sink.emit(json!({"type":"response","text":text})).await;
        }
        if matches!(command, "/r" | "/reload") {
            let Some(entry) = self.store.get(&event.platform, context, &backend)? else {
                return sink.emit(json!({"type":"response","text":"No saved session is bound to this thread. Use /p and /s <number> first."})).await;
            };
            let session =
                json!({"sessionId":entry.session_id,"cwd":entry.cwd,"title":"current session"});
            return self
                .attach(
                    event,
                    context,
                    Attachment {
                        backend: &backend,
                        session: &session,
                        expected_identity: Some(&entry.backend_id),
                        reload: true,
                    },
                    state,
                    sink,
                )
                .await;
        }
        if matches!(command, "/p" | "/projects") && argument.is_empty() {
            let (client, _) = self.launch(&backend, &std::env::current_dir()?).await?;
            let sessions = client.list_sessions().await;
            client.shutdown().await?;
            let mut counts = HashMap::<String, usize>::new();
            for session in sessions? {
                if let Some(cwd) = session["cwd"].as_str().filter(|cwd| !cwd.is_empty()) {
                    *counts.entry(cwd.into()).or_default() += 1;
                }
            }
            let mut projects: Vec<_> = counts.into_iter().collect();
            projects.sort_by(|left, right| right.1.cmp(&left.1).then(left.0.cmp(&right.0)));
            projects.truncate(20);
            state.projects = projects.iter().map(|(cwd, _)| cwd.clone()).collect();
            let text = if projects.is_empty() {
                "No saved ACP sessions found.".into()
            } else {
                format!(
                    "Projects:\n{}\nUse /p <number> to choose a project.",
                    projects
                        .iter()
                        .enumerate()
                        .map(|(index, (cwd, count))| format!(
                            "{}. {}\n   {cwd}\n   {count} session{}",
                            index + 1,
                            project_name(cwd),
                            if *count == 1 { "" } else { "s" }
                        ))
                        .collect::<Vec<_>>()
                        .join("\n")
                )
            };
            return sink.emit(json!({"type":"response","text":text})).await;
        }
        if matches!(command, "/p" | "/projects") {
            let Some(cwd) = picker_index(argument)
                .and_then(|index| state.projects.get(index))
                .cloned()
            else {
                return sink
                    .emit(json!({"type":"response","text":"Unknown project number. Run /p again."}))
                    .await;
            };
            state.selected_cwd = Some(cwd);
            state.picker.clear();
        }
        let Some(cwd) = state.selected_cwd.clone() else {
            return sink.emit(json!({"type":"response","text":"No project selected. Run /p first, then /p <number>."})).await;
        };
        if matches!(command, "/s" | "/sessions") && !argument.is_empty() {
            let Some(session) = picker_index(argument)
                .and_then(|index| state.picker.get(index))
                .cloned()
            else {
                return sink
                    .emit(json!({"type":"response","text":"Unknown session number. Run /s again."}))
                    .await;
            };
            return self
                .attach(
                    event,
                    context,
                    Attachment {
                        backend: &backend,
                        session: &session,
                        expected_identity: None,
                        reload: false,
                    },
                    state,
                    sink,
                )
                .await;
        }
        let (client, _) = self.launch(&backend, Path::new(&cwd)).await?;
        let sessions = client.list_sessions().await;
        client.shutdown().await?;
        state.picker = sessions?
            .into_iter()
            .filter(|session| session["cwd"] == cwd)
            .take(20)
            .collect();
        let text = if state.picker.is_empty() {
            "No saved sessions found for this project.".into()
        } else {
            format!(
                "Sessions in {}:\n{}\nUse /s <number> to switch interactively.\nUse /m <number> to mirror read-only.",
                project_name(&cwd),
                state
                    .picker
                    .iter()
                    .enumerate()
                    .map(|(index, session)| format!(
                        "{}. {}\n   {}",
                        index + 1,
                        session["title"]
                            .as_str()
                            .filter(|title| !title.is_empty())
                            .unwrap_or("(untitled session)"),
                        session["sessionId"]
                            .as_str()
                            .unwrap_or("")
                            .chars()
                            .take(8)
                            .collect::<String>()
                    ))
                    .collect::<Vec<_>>()
                    .join("\n")
            )
        };
        let text = if matches!(command, "/p" | "/projects") {
            format!("Selected project: {}\n{cwd}\n\n{text}", project_name(&cwd))
        } else {
            text
        };
        sink.emit(json!({"type":"response","text":text})).await
    }
    async fn attach(
        &self,
        event: &Event,
        context: &str,
        attachment: Attachment<'_>,
        state: &mut Conversation,
        sink: &dyn Sink,
    ) -> Result<()> {
        let Attachment {
            backend,
            session,
            expected_identity,
            reload,
        } = attachment;
        let cwd = session["cwd"].as_str().context("Invalid project path")?;
        let session_id = session["sessionId"]
            .as_str()
            .context("Invalid saved session")?;
        let (client, identity) = self.launch(backend, Path::new(cwd)).await?;
        let publication = async {
            if expected_identity.is_some_and(|expected| expected != identity) {
                bail!("Stored backend identity differs");
            }
            let mut unavailable = false;
            let history = match client.load_session(session_id, false).await {
                Ok(history) => history,
                Err(_) => {
                    unavailable = true;
                    client.load_session(session_id, true).await?;
                    vec![]
                }
            };
            self.store.set(
                Entry {
                    connector: event.platform.clone(),
                    thread_id: context.into(),
                    session_id: session_id.into(),
                    cwd: cwd.into(),
                    backend_id: identity,
                    updated_at: chrono::Utc::now().to_rfc3339(),
                    backend: backend.into(),
                },
                false,
            )?;
            Ok::<_, anyhow::Error>((history, unavailable))
        }
        .await;
        let (history, unavailable) = match publication {
            Ok(published) => published,
            Err(error) => {
                let _ = client.shutdown().await;
                return Err(error);
            }
        };
        if let Some(previous) = state.client.replace(client) {
            let _ = previous.close_session().await;
            let _ = previous.shutdown().await;
        }
        state.backend = backend.into();
        state.selected_cwd = Some(cwd.into());
        let header = if reload {
            "Reloaded session"
        } else {
            "Attached to session"
        };
        let text = format!(
            "{header}: {}\nProject: {cwd}\nSession: {session_id}\n\n{}",
            session["title"]
                .as_str()
                .filter(|title| !title.is_empty())
                .unwrap_or("(untitled session)"),
            history_message(&history, unavailable)
        );
        sink.emit(json!({"type":"response","text":text})).await
    }
    fn backend_config(&self, backend: &str) -> Result<&Value> {
        if !["opencode", "codex", "claude", "pi"].contains(&backend) {
            bail!("Unknown or unconfigured backend");
        }
        if self.config["acp"]["backends"][backend].is_object() {
            return Ok(&self.config["acp"]["backends"][backend]);
        }
        if backend == "opencode" {
            return Ok(&self.config["acp"]);
        }
        bail!("Unknown or unconfigured backend")
    }
    async fn process(
        &self,
        event: &Event,
        context: &str,
        conversation: &ConversationSlot,
        turn: Option<Arc<MatrixTurn>>,
        sink: &dyn Sink,
        source_web_commands: bool,
    ) -> Result<()> {
        let mut command = event
            .text
            .split_whitespace()
            .next()
            .unwrap_or("")
            .to_ascii_lowercase();
        let cancel_dispatch = if command == "/cancel" {
            if let Some(turn) = &turn {
                Some(turn.cancel_dispatch.lock().await)
            } else {
                None
            }
        } else {
            None
        };
        let mut state = conversation.lock().await;
        if matches!(event.platform.as_str(), "matrix" | "cli") {
            state.last_activity = Instant::now();
        }
        let argument = event
            .text
            .split_once(char::is_whitespace)
            .map(|(_, tail)| tail.trim())
            .unwrap_or("");
        if !matches!(command.as_str(), "/m" | "/mirror") && state.mirror.take().is_some() {
            sink.emit(json!({"type":"response","text":"Mirror mode stopped."}))
                .await?;
        }
        if source_web_commands && event.text.starts_with('/') {
            let normalized = event.text.to_lowercase();
            let name = normalized
                .strip_prefix('/')
                .unwrap_or("")
                .split(' ')
                .next()
                .unwrap_or("");
            if matches!(
                name,
                "p" | "projects"
                    | "s"
                    | "sessions"
                    | "m"
                    | "mirror"
                    | "r"
                    | "reload"
                    | "d"
                    | "detach"
            ) {
                if !self.picker_enabled(&event.platform) {
                    return sink.emit(json!({"type":"response","text":format!("Unknown command: {}. Try /help",event.text)})).await;
                }
                command = format!("/{name}");
            } else if !matches!(
                normalized.as_str(),
                "/status" | "/help" | "/h" | "/clear" | "/reset" | "/cancel"
            ) {
                let commands = if let Some(client) = &state.client {
                    client.commands().await
                } else {
                    vec![]
                };
                if commands.iter().any(|command| command["name"] == name) {
                    command.clear();
                } else {
                    return sink.emit(json!({"type":"response","text":format!("Unknown command: {}. Try /help",event.text)})).await;
                }
            }
        }
        if command == "/cancel" {
            if event.platform == "matrix" {
                let Some(turn) = &turn else {
                    return sink
                        .emit(json!({"type":"notice","text":"No active prompt in this thread."}))
                        .await;
                };
                if !conversation
                    .matrix_admission
                    .lock()
                    .unwrap()
                    .turn
                    .as_ref()
                    .is_some_and(|active| Arc::ptr_eq(active, turn))
                {
                    return sink
                        .emit(json!({"type":"notice","text":"No active prompt in this thread."}))
                        .await;
                }
                if turn.cancelled.load(std::sync::atomic::Ordering::SeqCst) {
                    return sink.emit(json!({"type":"notice","text":"Cancellation is already pending for this thread."})).await;
                }
            }
            if let Some(client) = &state.client
                && let Err(error) = client.cancel().await
            {
                if event.platform != "matrix" {
                    return Err(error);
                }
                return sink.emit(json!({"type":"notice","text":"Cancellation failed; the active prompt remains running."})).await;
            }
            if let Some(turn) = &turn {
                turn.cancelled
                    .store(true, std::sync::atomic::Ordering::SeqCst);
            }
            drop(cancel_dispatch);
            return sink
                .emit(json!({"type":"response","text":"Cancellation requested."}))
                .await;
        }
        if command == "/status" {
            let text = if matches!(event.platform.as_str(), "matrix" | "cli") {
                format!(
                    "Backend: {}\nMessages: {}\nInput/output chars: {}/{}",
                    self.selected(&event.platform, context)?,
                    state.message_count,
                    state.input_chars,
                    state.output_chars
                )
            } else if state.client.is_none() {
                "No active session.".to_owned()
            } else {
                crate::core::session_status(
                    state.message_count,
                    state.input_chars,
                    state.output_chars,
                    state.created_at.elapsed(),
                    state.last_activity.elapsed(),
                )
            };
            return sink.emit(json!({"type":"response","text":text})).await;
        }
        if event.platform == "matrix" && matches!(command.as_str(), "/help" | "/h") {
            return sink
                .emit(json!({"type":"notice","text":MATRIX_COMMAND_MENU}))
                .await;
        }
        if event.platform != "matrix"
            && matches!(command.as_str(), "/help" | "/h" | "/commands")
            && argument.is_empty()
        {
            let text = if event.platform == "cli" {
                "Commands: /help /status /backend /model /models /effort /mode /cancel /compact /commands /clear /reset /restart; picker: /p /s /r /d /m".to_owned()
            } else {
                self.shared_help(event, &state).await
            };
            return sink.emit(json!({"type":"response","text":text})).await;
        }
        if state.client.as_ref().is_some_and(Client::accepts_followups) {
            if event.platform == "matrix" && !event.text.starts_with('/') {
                if turn
                    .as_ref()
                    .is_some_and(|turn| turn.cancelled.load(std::sync::atomic::Ordering::SeqCst))
                {
                    return sink.emit(json!({"type":"response","text":"Sorry, the backend did not accept your additional message."})).await;
                }
                if let Some(turn) = &turn {
                    turn.forwarded
                        .store(true, std::sync::atomic::Ordering::SeqCst);
                }
                state.message_count += 1;
                state.input_chars += event.text.encode_utf16().count();
                let client = state.client.clone().unwrap();
                drop(state);
                if client
                    .forward_content(&event.text, event.images.clone())
                    .await
                    .is_err()
                {
                    sink.emit(json!({"type":"response","text":"Sorry, the backend did not accept your additional message."})).await?;
                }
                return Ok(());
            }
            return sink.emit(json!({"type":"response","text":"A request is already running in this thread."})).await;
        }
        if matches!(command.as_str(), "/clear" | "/reset" | "/d" | "/detach") {
            let backend = self.selected(&event.platform, context)?;
            let had_session = matches!(event.platform.as_str(), "matrix" | "cli")
                || state.client.is_some()
                || self
                    .store
                    .get(&event.platform, context, &backend)?
                    .is_some();
            if let Some(client) = state.client.take() {
                let deletion = async {
                    if matches!(command.as_str(), "/clear" | "/reset") {
                        client.delete_session().await?;
                    } else {
                        client.close_session().await?;
                    }
                    Result::<()>::Ok(())
                }
                .await;
                let _ = client.shutdown().await;
                // Source clear/reset discards the local binding even if backend
                // deletion fails. Detach retains its existing error behavior.
                if !matches!(command.as_str(), "/clear" | "/reset") {
                    deletion?;
                }
            } else if matches!(command.as_str(), "/clear" | "/reset") {
                self.clear_stored(&event.platform, context, &backend)
                    .await?;
            }
            self.store
                .remove(&event.platform, context, Some(&backend))?;
            if !matches!(event.platform.as_str(), "matrix" | "cli") {
                state.message_count = 0;
                state.input_chars = 0;
                state.output_chars = 0;
            }
            let text = if event.platform != "matrix" && !had_session {
                "No active session."
            } else {
                "Session cleared. Next message will start a fresh session."
            };
            if matches!(event.platform.as_str(), "matrix" | "cli") {
                sink.emit(json!({"type":"session_state","state":"cleared"}))
                    .await?;
                return sink.emit(json!({"type":"response","text":text})).await;
            }
            sink.emit(json!({"type":"response","text":text})).await?;
            sink.emit(json!({"type":"session_state","state":"cleared"}))
                .await?;
            return Ok(());
        }
        if event.platform == "matrix" && command == "/backend" {
            let arguments: Vec<_> = argument.split_whitespace().collect();
            if arguments.is_empty()
                || (arguments.len() == 1 && matches!(arguments[0], "status" | "list"))
            {
                let mut names = vec!["opencode".to_owned()];
                if let Some(backends) = self.config["acp"]["backends"].as_object() {
                    names.extend(
                        backends
                            .keys()
                            .filter(|name| name.as_str() != "opencode")
                            .cloned(),
                    );
                }
                return sink.emit(json!({"type":"notice","text":format!("Backend: {}; configured: {}", self.selected(&event.platform, context)?, names.join(", "))})).await;
            }
            if arguments.len() != 1 {
                bail!("Usage: /backend [name|status|list]");
            }
        }
        if command == "/backend" && argument.is_empty() {
            return sink.emit(json!({"type":"response","text":format!("Backend: {}",self.selected(&event.platform,context)?)})).await;
        }
        let backend = if command == "/backend" {
            argument.to_owned()
        } else {
            self.selected(&event.platform, context)?
        };
        if [
            "/p",
            "/projects",
            "/s",
            "/sessions",
            "/r",
            "/reload",
            "/m",
            "/mirror",
        ]
        .contains(&command.as_str())
        {
            return self
                .picker(event, context, &command, argument, &mut state, sink)
                .await;
        }
        if event.platform == "matrix"
            && ![
                "/backend", "/model", "/models", "/effort", "/mode", "/agent", "/config",
                "/restart",
            ]
            .contains(&command.as_str())
        {
            let _ = sink.emit(json!({"type":"thinking"})).await;
        }
        let mut client = self
            .open(&event.platform, context, &backend, &mut state)
            .await?;
        if event.platform == "matrix"
            && matches!(
                command.as_str(),
                "/model" | "/models" | "/effort" | "/mode" | "/agent" | "/commands"
            )
        {
            let text = matrix_session_control(&client, &command, argument).await?;
            return sink.emit(json!({"type":"notice","text":text})).await;
        }
        if command == "/backend" {
            return sink
                .emit(json!({"type":"response","text":format!("Backend selected: {backend}")}))
                .await;
        }
        if ["/model", "/models", "/effort", "/mode", "/agent", "/config"]
            .contains(&command.as_str())
        {
            let options = client.config_options().await;
            let kind = if command == "/effort" {
                "thought_level"
            } else {
                "model"
            };
            if matches!(command.as_str(), "/mode" | "/agent") {
                if !argument.is_empty() && !["list", "status"].contains(&argument) {
                    client.set_mode(argument).await?;
                }
                return sink
                    .emit(json!({"type":"response","text":client.modes().await.to_string()}))
                    .await;
            }
            if command == "/config" {
                let mut args = argument.split_whitespace();
                if let (Some(id), Some(value)) = (args.next(), args.next()) {
                    client.set_option(id, value).await?;
                }
            } else if !argument.is_empty() && !["list", "status"].contains(&argument) {
                let option = options
                    .iter()
                    .find(|item| {
                        item["id"] == kind
                            || item["category"] == kind
                            || (kind == "thought_level" && item["id"] == "effort")
                    })
                    .context("ACP does not advertise this option")?;
                client
                    .set_option(option["id"].as_str().unwrap_or(kind), argument)
                    .await?;
            }
            return sink.emit(json!({"type":"response","text":serde_json::to_string(&client.config_options().await)?})).await;
        }
        if command == "/restart" {
            return sink
                .emit(json!({"type":"response","text":"Restart is not enabled."}))
                .await;
        }
        state.input_chars += event.text.encode_utf16().count();
        if event.platform != "matrix" {
            sink.emit(json!({"type":"thinking"})).await?;
        }
        drop(state);
        let mut retried = false;
        let mut completion_guard = None;
        let (result, tool_results, observation, delivery) = loop {
            if let Some(turn) = &turn {
                let dispatch = turn.cancel_dispatch.lock().await;
                if turn.cancelled.load(std::sync::atomic::Ordering::SeqCst) {
                    return sink.emit(json!({"type":"response","text":if retried { "Request cancelled before retrying." } else { "Request cancelled before prompting." }})).await;
                }
                drop(dispatch);
            }
            let (result, tool_results, had_activity, observation, delivery) =
                self.prompt_attempt(&client, event, sink).await?;
            if event.platform == "web" {
                let empty = crate::core::sanitize(&crate::core::media(&result.text, false).1)
                    .trim()
                    .is_empty();
                let normal = result.stop_reason.as_deref() == Some("end_turn");
                let has_markers = [false, true].into_iter().any(|document| {
                    !crate::core::media(&result.text, document).0.is_empty()
                        || !crate::core::media(&tool_results, document).0.is_empty()
                });
                if empty
                    && !has_markers
                    && normal
                    && !had_activity
                    && !result.output.had_server_request
                    && !result.output.output_lost
                    && result.output.raw_chars == 0
                    && result.output.chunks == 0
                    && result.output.tool_calls == 0
                    && event.images.is_empty()
                    && delivery.uploaded == 0
                    && delivery.processed == 0
                {
                    if retried {
                        let mut state = conversation.lock().await;
                        self.store
                            .remove(&event.platform, context, Some(&backend))?;
                        if let Some(previous) = state.client.take() {
                            let _ = previous.shutdown().await;
                        }
                        *state = Conversation::default();
                        drop(state);
                        sink.emit(json!({"type":"session_state","state":"stale-invalidated"}))
                            .await?;
                        let delivered = sink.emit(json!({"type":"error","message":"The AI service completed without returning a usable response. Please try again."})).await;
                        let mut observation = observation;
                        observation["failed"] = true.into();
                        observation["usableOutput"] = false.into();
                        observation["deliveredChars"] = 0.into();
                        observation["uploadedImages"] = 0.into();
                        observation["deliveryFailed"] = delivered.is_err().into();
                        crate::debug_log::observation(&observation);
                        let _ = sink.emit(observation).await;
                        return delivered;
                    }
                    let mut observation = observation;
                    observation["retry"] = true.into();
                    observation["deliveredChars"] = 0.into();
                    observation["uploadedImages"] = 0.into();
                    crate::debug_log::observation(&observation);
                    let _ = sink.emit(observation).await;
                    let mut state = conversation.lock().await;
                    self.store
                        .remove(&event.platform, context, Some(&backend))?;
                    if let Some(previous) = state.client.take() {
                        let _ = previous.shutdown().await;
                    }
                    *state = Conversation::default();
                    client = self
                        .open(&event.platform, context, &backend, &mut state)
                        .await?;
                    state.input_chars += event.text.encode_utf16().count();
                    retried = true;
                    continue;
                }
                break (result, tool_results, observation, delivery);
            }
            if event.platform != "matrix" {
                break (result, tool_results, observation, delivery);
            }
            completion_guard = if let Some(turn) = &turn {
                Some(turn.cancel_dispatch.lock().await)
            } else {
                None
            };
            if turn
                .as_ref()
                .is_some_and(|turn| turn.cancelled.load(std::sync::atomic::Ordering::SeqCst))
                || result.stop_reason.as_deref() == Some("cancelled")
            {
                return finish_response(sink, "Request cancelled.", observation, delivery.uploaded)
                    .await;
            }
            if event.text.split_whitespace().next() == Some("/compact") {
                let transport = client.transport_name();
                let text = if result.stop_reason.as_deref() == Some("end_turn") {
                    format!("Compaction completed for this {transport} session.")
                } else {
                    format!(
                        "Compaction was not confirmed by {transport} (stop reason: {}).",
                        result
                            .stop_reason
                            .as_deref()
                            .filter(|reason| !reason.is_empty())
                            .unwrap_or("unknown")
                    )
                };
                return finish_response(sink, &text, observation, delivery.uploaded).await;
            }
            if let Some(reason) = result
                .stop_reason
                .as_deref()
                .filter(|reason| !reason.is_empty())
                && reason != "end_turn"
            {
                return finish_response(
                    sink,
                    &format!(
                        "{} stopped without normal completion ({reason}).",
                        client.transport_name()
                    ),
                    observation,
                    delivery.uploaded,
                )
                .await;
            }
            let clean = crate::core::sanitize(&crate::core::media(&result.text, false).1);
            let has_markers = [false, true].into_iter().any(|document| {
                !crate::core::media(&result.text, document).0.is_empty()
                    || !crate::core::media(&result.output.tool_results, document)
                        .0
                        .is_empty()
            });
            if !clean.is_empty() || has_markers || result.output.image_count > 0 {
                break (result, tool_results, observation, delivery);
            }
            if retried
                || had_activity
                || turn
                    .as_ref()
                    .is_some_and(|turn| turn.forwarded.load(std::sync::atomic::Ordering::SeqCst))
            {
                return finish_response(sink, "Sorry, the ACP backend completed without returning a usable response. Please try again.", observation, delivery.uploaded).await;
            }
            let mut observation = observation;
            observation["retry"] = true.into();
            observation["deliveredChars"] = 0.into();
            observation["uploadedImages"] = 0.into();
            crate::debug_log::observation(&observation);
            let _ = sink.emit(observation).await;
            drop(completion_guard.take());
            let mut state = conversation.lock().await;
            self.store
                .remove(&event.platform, context, Some(&backend))?;
            if let Some(previous) = state.client.take() {
                let _ = previous.shutdown().await;
            }
            *state = Conversation::default();
            client = self
                .open(&event.platform, context, &backend, &mut state)
                .await?;
            state.input_chars += event.text.encode_utf16().count();
            retried = true;
        };
        let _completion_guard = completion_guard;
        let mut state = conversation.lock().await;
        state.message_count += 1;
        if matches!(event.platform.as_str(), "matrix" | "cli") {
            state.output_chars += result.text.encode_utf16().count();
        }
        state.last_activity = Instant::now();
        let mut clean = result.text.clone();
        let mut uploaded = delivery.attempted;
        let mut successful_uploads = delivery.uploaded;
        let mut successful_images = delivery.uploaded;
        let mut failed_uploads = delivery.failed;
        let cwd = client.working_directory();
        for document in [false, true] {
            let (paths, remaining) = crate::core::media(&clean, document);
            clean = remaining;
            let (tool_paths, _) = crate::core::media(&tool_results, document);
            for path in tool_paths.into_iter().chain(paths) {
                if uploaded.len() >= 16 {
                    break;
                }
                if !uploaded.insert(path.clone()) {
                    continue;
                }
                if let Ok(media) = crate::media::read_supported(
                    cwd,
                    &std::env::temp_dir().join("doclibrary_cache"),
                    &path,
                    20 * 1024 * 1024,
                ) {
                    let is_image = media.mime.starts_with("image/");
                    if sink
                        .upload(&media.name, &media.mime, media.bytes)
                        .await
                        .is_ok()
                    {
                        successful_uploads += 1;
                        successful_images += usize::from(is_image);
                    } else {
                        failed_uploads += 1;
                    }
                } else {
                    failed_uploads += 1;
                }
            }
        }
        let clean = crate::core::sanitize(&clean);
        if !matches!(event.platform.as_str(), "matrix" | "cli") {
            state.output_chars += clean.encode_utf16().count();
        }
        let text = if (event.platform == "matrix" && clean.is_empty())
            || (event.platform != "matrix" && clean.trim().is_empty())
        {
            if result.stop_reason.as_deref() == Some("cancelled") {
                "Request cancelled."
            } else if successful_uploads > 0 {
                ""
            } else if !uploaded.is_empty()
                || result.output.image_count > 0
                || result.output.had_tool_activity
            {
                "Sorry, the ACP backend completed without returning a usable response. Please try again."
            } else {
                "The AI returned no text response."
            }
        } else {
            &clean
        };
        let mut observation = observation;
        observation["uploadedMedia"] = successful_uploads.into();
        observation["uploadFailures"] = failed_uploads.into();
        finish_response(sink, text, observation, successful_images).await
    }
    async fn prompt_attempt(
        &self,
        client: &Client,
        event: &Event,
        sink: &dyn Sink,
    ) -> Result<(crate::acp::Response, String, bool, Value, InlineDelivery)> {
        use futures_util::{StreamExt, stream::FuturesUnordered};
        let interactive = matches!(event.platform.as_str(), "matrix" | "telegram")
            && self.config[&event.platform]["interactivePermissions"] == true
            && !event.id.is_empty()
            && !event.user.is_empty()
            && !event.room.is_empty()
            && client
                .session_id()
                .await
                .is_some_and(|session| !session.is_empty());
        let (mut permission_requests, _permission_guard) = if interactive {
            let (requests, guard) = client.permission_handler();
            (Some(requests), Some(guard))
        } else {
            (None, None)
        };
        let question_interactive = event.platform == "matrix"
            && self.config["matrix"]["interactiveQuestions"] != false
            && !event.id.is_empty()
            && !event.user.is_empty()
            && !event.room.is_empty();
        let (mut form_requests, _form_guard) = if question_interactive {
            client
                .form_handler()
                .map(|(guard, receiver)| (Some(receiver), Some(guard)))
                .unwrap_or((None, None))
        } else {
            (None, None)
        };
        let mut questions = FuturesUnordered::new();
        let mut approvals = FuturesUnordered::new();
        let scope = self.permission_scope(event, sink, client.working_directory());
        let mut receiver = client.subscribe();
        let prompt = client.prompt(&event.text, event.images.clone());
        tokio::pin!(prompt);
        let mut captured = String::new();
        let mut presentation_lost = false;
        let mut delivery = InlineDelivery::default();
        let mut presenter = crate::tools::TurnPresenter::new(
            self.config["toolMessages"].clone(),
            &event.platform,
            self.config["matrix"]["terminalThread"].clone(),
        )
        .with_matrix_full_paths(self.config["matrix"]["showFullPaths"] == true);
        let result = loop {
            tokio::select! {
                result = &mut prompt => break result,
                request = async { match form_requests.as_mut() { Some(requests) => requests.recv().await, None => std::future::pending().await } } => {
                    if let Some(request) = request {
                        questions.push(self.questions.request(scope.clone(), request.form, request.current, request.reply, sink));
                    } else { form_requests = None; }
                }
                Some(()) = questions.next(), if !questions.is_empty() => {}
                request = async { match permission_requests.as_mut() { Some(requests) => requests.recv().await, None => std::future::pending().await } } => {
                    if let Some(request) = request {
                        approvals.push(self.permissions.request(scope.clone(), request, sink));
                    } else { permission_requests = None; }
                }
                _ = approvals.next(), if !approvals.is_empty() => {},
                update = receiver.recv() => {
                    match update {
                        Ok(update) => {
                            deliver_inline(client, sink, &mut delivery, update["collectorImageCount"].as_u64().unwrap_or(0) as usize).await;
                            present_update(sink, &update, &mut captured, &mut presenter).await;
                        },
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => { presentation_lost = true; let _ = sink.emit(json!({"type":"error","message":"Activity stream overflow; final response remains intact."})).await; },
                        Err(_) => break Err(anyhow::anyhow!("ACP update channel closed")),
                    }
                }
            }
        };
        drop(approvals);
        drop(_permission_guard);
        loop {
            match receiver.try_recv() {
                Ok(update) => {
                    deliver_inline(
                        client,
                        sink,
                        &mut delivery,
                        update["collectorImageCount"].as_u64().unwrap_or(0) as usize,
                    )
                    .await;
                    present_update(sink, &update, &mut captured, &mut presenter).await;
                }
                Err(tokio::sync::broadcast::error::TryRecvError::Lagged(_)) => {
                    presentation_lost = true
                }
                Err(_) => break,
            }
        }
        deliver_inline(client, sink, &mut delivery, usize::MAX).await;
        let (output, stop_reason, model) = client.prompt_observation().await;
        let partial = result
            .as_ref()
            .err()
            .and_then(|error| error.downcast_ref::<crate::native_client::PromptFailure>());
        let clean = result
            .as_ref()
            .map(|response| response.text.as_str())
            .ok()
            .or_else(|| partial.map(|failure| failure.text.as_str()))
            .map(|text| {
                crate::core::sanitize(
                    &crate::core::media(&crate::core::media(text, false).1, true).1,
                )
            })
            .unwrap_or_default();
        let captured_chars = captured.encode_utf16().count();
        let diagnostic = crate::core::diagnostic_counts(
            output.raw_chars,
            captured_chars,
            clean.encode_utf16().count(),
        );
        let observation = json!({"type":"prompt_end","rawChars":output.raw_chars,"capturedChars":captured_chars,"cleanChars":clean.encode_utf16().count(),"chunks":output.chunks,"toolCalls":output.tool_calls,"hadToolActivity":output.had_tool_activity,"hasImages":output.image_count>0,"imageCount":output.image_count,"outputLost":output.output_lost,"presentationLost":presentation_lost,"captureRecovered":captured_chars<output.raw_chars && !clean.is_empty(),"collectorClosed":output.collector_closed,"stopReason":stop_reason,"model":model,"diagnostic":diagnostic,"failed":result.is_err()});
        if result.is_err() {
            let mut observation = observation.clone();
            let delivered = if partial.is_some() && !clean.trim().is_empty() {
                sink.emit(json!({"type":"response","text":clean,"partial":true,"failed":true}))
                    .await
                    .is_ok()
            } else {
                false
            };
            observation["deliveredChars"] = if delivered {
                clean.encode_utf16().count()
            } else {
                0
            }
            .into();
            observation["deliveryFailed"] =
                (partial.is_some() && !clean.trim().is_empty() && !delivered).into();
            observation["uploadedImages"] = delivery.uploaded.into();
            observation["uploadedMedia"] = delivery.uploaded.into();
            observation["uploadFailures"] = delivery.failed.into();
            crate::debug_log::observation(&observation);
            let _ = sink.emit(observation).await;
        }
        let result = result?;
        let had_activity = output.had_tool_activity || output.image_count > 0 || output.output_lost;
        let mut observation = observation;
        observation["uploadFailures"] = delivery.failed.into();
        Ok((
            result,
            output.tool_results,
            had_activity,
            observation,
            delivery,
        ))
    }
    fn mirror_interval(&self) -> Duration {
        Duration::from_secs(
            self.config["sessionPicker"]["mirrorIntervalSeconds"]
                .as_u64()
                .unwrap_or(60)
                .max(10),
        )
    }
    async fn mirror_snapshot(&self, backend: &str, session: &Value) -> Result<Vec<Value>> {
        let (client, _) = self
            .launch(
                backend,
                Path::new(session["cwd"].as_str().context("Invalid mirror project")?),
            )
            .await?;
        let snapshot = client
            .load_session(
                session["sessionId"]
                    .as_str()
                    .context("Invalid mirror session")?,
                false,
            )
            .await;
        let _ = client.close_session().await;
        client.shutdown().await?;
        snapshot
    }
    pub async fn poll_mirrors(&self) -> Result<usize> {
        let conversations: Vec<_> = self.sessions.lock().await.values().cloned().collect();
        let mut updated = 0;
        for conversation in conversations {
            let snapshot = {
                let Ok(mut state) = conversation.try_lock() else {
                    continue;
                };
                let Some(mirror) = state.mirror.as_mut() else {
                    continue;
                };
                if mirror.next_poll > Instant::now() {
                    continue;
                }
                mirror.next_poll = Instant::now() + self.mirror_interval();
                mirror.clone()
            };
            let history = self
                .mirror_snapshot(&snapshot.backend, &snapshot.session)
                .await;
            let mut state = conversation.lock().await;
            let Some(mirror) = state
                .mirror
                .as_mut()
                .filter(|mirror| mirror.generation == snapshot.generation)
            else {
                continue;
            };
            match history {
                Ok(history) => {
                    let fingerprint = history_fingerprint(&history);
                    if mirror.fingerprint != fingerprint {
                        mirror.fingerprint = fingerprint;
                        snapshot.sink.emit(json!({"type":"response","text":history_message(&history,false).replacen("Recent history:","Mirror update:",1)})).await?;
                        updated += 1;
                    }
                }
                Err(_) => {
                    state.mirror = None;
                    snapshot.sink.emit(json!({"type":"response","text":"Mirror mode stopped: session history could not be loaded."})).await?;
                }
            }
        }
        Ok(updated)
    }
    /// Counts live-client invalidations committed after exact-backend store removal
    /// succeeds, including absent mappings and pending ACP cleanup. Cleanup retries
    /// do not increment the count. Zero disables the sweep.
    pub async fn expire(&self, retention: Duration) -> Result<usize> {
        if retention.is_zero() {
            return Ok(0);
        }
        self.expire_sessions(Instant::now(), |_, elapsed| elapsed >= retention)
            .await
    }

    /// Sweep the explicit startup policy using one monotonic time snapshot.
    /// Production supplies `Instant::now()`; deterministic callers can supply a
    /// synthetic snapshot without changing clocks, environment, or persisted state.
    /// Disabled platforms still retry retained owned cleanup, without invalidation.
    pub async fn expire_with_policy(
        &self,
        policy: &SessionRetentionPolicy,
        sweep_time: Instant,
    ) -> Result<usize> {
        self.expire_sessions(sweep_time, |platform, elapsed| {
            policy
                .minutes(platform)
                .is_some_and(|minutes| elapsed.as_secs_f64() / 60.0 >= minutes)
        })
        .await
    }

    async fn expire_sessions(
        &self,
        sweep_time: Instant,
        is_stale: impl Fn(&str, Duration) -> bool,
    ) -> Result<usize> {
        let sessions = self
            .sessions
            .lock()
            .await
            .iter()
            .map(|(key, slot)| (key.clone(), slot.clone()))
            .collect::<Vec<_>>();
        let mut expired = 0;
        for (key, session) in sessions {
            let Some((platform, context)) = key.split_once('\0') else {
                continue;
            };
            let Ok(mut state) = session.try_lock() else {
                continue;
            };
            let Ok(mut cleanup) = session.expiry_cleanup.try_lock() else {
                continue;
            };
            if !cleanup_expired_client(&mut cleanup).await
                || !is_stale(
                    platform,
                    sweep_time
                        .checked_duration_since(state.last_activity)
                        .unwrap_or_default(),
                )
                || state.client.as_ref().is_none_or(Client::accepts_followups)
            {
                continue;
            }
            {
                let Ok(admission) = session.matrix_admission.try_lock() else {
                    continue;
                };
                if admission.turn.is_some() || admission.control_pending || admission.requests != 0
                {
                    continue;
                }
                if let Err(error) = self.store.remove(platform, context, Some(&state.backend)) {
                    eprintln!("Session expiry mapping removal failed: {error}");
                    continue;
                }
                *cleanup = state.client.take();
                state.mirror = None;
                state.message_count = 0;
                state.input_chars = 0;
                state.output_chars = 0;
            }
            expired += 1;
            cleanup_expired_client(&mut cleanup).await;
        }
        Ok(expired)
    }
    pub async fn shutdown(&self) -> Result<()> {
        let sessions = self
            .sessions
            .lock()
            .await
            .values()
            .cloned()
            .collect::<Vec<_>>();
        for session in sessions {
            let mut state = session.lock().await;
            let mut cleanup = session.expiry_cleanup.lock().await;
            cleanup_expired_client(&mut cleanup).await;
            state.mirror = None;
            if let Some(client) = state.client.take() {
                client.shutdown().await?;
            }
        }
        Ok(())
    }
}

pub fn csv(value: &str) -> Vec<String> {
    value
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
        .collect()
}

#[cfg(test)]
#[path = "runtime_expiry_tests.rs"]
mod expiry_tests;
pub fn session_dir(base: &Path, platform: &str, identifier: &str) -> PathBuf {
    let sanitized: String = identifier
        .encode_utf16()
        .map(|unit| {
            if unit <= 127 && (unit as u8).is_ascii_alphanumeric() || unit == 45 || unit == 95 {
                char::from_u32(unit as u32).unwrap()
            } else {
                '_'
            }
        })
        .collect();
    let prefix: String = sanitized.chars().take(120).collect();
    let prefix = if prefix.is_empty() {
        "thread".into()
    } else {
        prefix
    };
    let leaf = if sanitized != identifier || sanitized.len() > prefix.len() {
        format!(
            "{prefix}-{}",
            &format!("{:x}", Sha256::digest(identifier.as_bytes()))[..12]
        )
    } else {
        prefix
    };
    base.join(platform).join(leaf)
}
