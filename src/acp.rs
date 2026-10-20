use agent_client_protocol::{
    Agent, ConnectionTo, JsonRpcMessage, Lines, Responder, UntypedMessage,
};
use anyhow::{Context, Result, bail};
use futures::StreamExt;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, HashMap, VecDeque},
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::process::ChildStdin;
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    process::{Child, Command},
    sync::{Mutex, broadcast, mpsc, oneshot, watch},
};

#[derive(Clone)]
pub struct Options {
    pub command: String,
    pub args: Vec<String>,
    pub cwd: PathBuf,
    pub env: BTreeMap<String, String>,
    pub inherit_env: bool,
    pub timeout: Duration,
}
pub(crate) struct PermissionRequest {
    pub params: Value,
    pub current: Arc<AtomicBool>,
    pub reply: oneshot::Sender<Value>,
}
const APPROVAL_WAIT_CAP: Duration = Duration::from_secs(45 * 60);
#[derive(Clone, Copy, Default)]
struct ApprovalTime {
    elapsed: Duration,
    since: Option<tokio::time::Instant>,
}
impl ApprovalTime {
    fn total(self, now: tokio::time::Instant) -> Duration {
        self.elapsed
            + self
                .since
                .map_or(Duration::ZERO, |since| now.saturating_duration_since(since))
    }
}
// Credit only the union of live decision waits, never permission reply writes.
struct RequestDeadline {
    started: tokio::time::Instant,
    budget: Duration,
    baseline: Duration,
    clock: Option<watch::Receiver<ApprovalTime>>,
}
impl RequestDeadline {
    fn new(budget: Duration, clock: Option<watch::Receiver<ApprovalTime>>) -> Self {
        let started = tokio::time::Instant::now();
        let baseline = clock
            .as_ref()
            .map_or(Duration::ZERO, |clock| clock.borrow().total(started));
        Self {
            started,
            budget,
            baseline,
            clock,
        }
    }
    async fn wait<F: std::future::Future>(mut self, future: F) -> Option<F::Output> {
        tokio::pin!(future);
        loop {
            let now = tokio::time::Instant::now();
            let credit = self.clock.as_mut().map_or(Duration::ZERO, |clock| {
                let time = *clock.borrow_and_update();
                if time.since.is_some() {
                    APPROVAL_WAIT_CAP
                } else {
                    time.total(now)
                        .saturating_sub(self.baseline)
                        .min(APPROVAL_WAIT_CAP)
                }
            });
            let deadline = self.started + self.budget + credit;
            tokio::select! {
                biased;
                result = &mut future => return Some(result),
                _ = tokio::time::sleep_until(deadline) => return None,
                _ = async { match self.clock.as_mut() {
                    Some(clock) => { let _ = clock.changed().await; },
                    None => std::future::pending::<()>().await,
                }} => {},
            }
        }
    }
}
struct Permissions {
    generation: u64,
    handler: Option<mpsc::Sender<PermissionRequest>>,
    seen: std::collections::HashSet<String>,
    pending: HashMap<String, (Arc<AtomicBool>, oneshot::Sender<()>)>,
    waiting: std::collections::HashSet<String>,
    clock: watch::Sender<ApprovalTime>,
}
impl Default for Permissions {
    fn default() -> Self {
        Self {
            generation: 0,
            handler: None,
            seen: Default::default(),
            pending: Default::default(),
            waiting: Default::default(),
            clock: watch::channel(ApprovalTime::default()).0,
        }
    }
}
impl Permissions {
    fn begin_wait(&mut self, key: &str) {
        if self.waiting.insert(key.to_owned()) && self.waiting.len() == 1 {
            let mut time = *self.clock.borrow();
            time.since = Some(tokio::time::Instant::now());
            self.clock.send_replace(time);
        }
    }
    fn end_wait(&mut self, key: &str) {
        if self.waiting.remove(key) && self.waiting.is_empty() {
            self.end_all_waits();
        }
    }
    fn end_all_waits(&mut self) {
        self.waiting.clear();
        let mut time = *self.clock.borrow();
        if time.since.is_some() {
            time.elapsed = time.total(tokio::time::Instant::now());
            time.since = None;
            self.clock.send_replace(time);
        }
    }
    fn clear(&mut self) {
        self.end_all_waits();
        self.generation += 1;
        self.handler = None;
        self.seen.clear();
        for (_, (current, cancel)) in self.pending.drain() {
            current.store(false, Ordering::SeqCst);
            let _ = cancel.send(());
        }
    }
}
pub(crate) struct PermissionGuard(std::sync::Weak<Inner>, u64);
impl Drop for PermissionGuard {
    fn drop(&mut self) {
        if let Some(inner) = self.0.upgrade() {
            let mut permissions = inner.permissions.lock().unwrap();
            if permissions.generation == self.1 {
                permissions.clear();
            }
        }
    }
}
struct PermissionWrite {
    generation: u64,
    current: Arc<AtomicBool>,
    done: oneshot::Sender<std::result::Result<(), String>>,
}
// Legacy ACP modes can be a view of a config select (permission modes or Pi
// thinking levels). Keep that view current when its existing choices match.
fn replace_config_options(state: &mut State, options: Vec<Value>) {
    let mode_ids = state.modes["availableModes"]
        .as_array()
        .map(|modes| {
            modes
                .iter()
                .filter_map(|mode| mode["id"].as_str())
                .collect::<std::collections::BTreeSet<_>>()
        })
        .unwrap_or_default();
    let mirrored = if mode_ids.is_empty() {
        None
    } else {
        state
            .config_options
            .iter()
            .find(|option| {
                option["options"].as_array().is_some_and(|choices| {
                    choices
                        .iter()
                        .filter_map(|choice| choice["value"].as_str())
                        .collect::<std::collections::BTreeSet<_>>()
                        == mode_ids
                })
            })
            .and_then(|option| option["id"].as_str())
            .map(str::to_owned)
    };
    if let Some(option) = mirrored.and_then(|id| options.iter().find(|option| option["id"] == id))
        && let Some(choices) = option["options"].as_array()
    {
        state.modes = serde_json::json!({"currentModeId":option["currentValue"],"availableModes":choices.iter().map(|choice|serde_json::json!({"id":choice["value"],"name":choice["name"],"description":choice["description"]})).collect::<Vec<_>>()});
    }
    state.config_options = options;
}

struct Inner {
    stdin: Arc<Mutex<Option<ChildStdin>>>,
    connection: std::sync::OnceLock<ConnectionTo<Agent>>,
    stop: watch::Sender<bool>,
    requests: tokio::sync::Semaphore,
    permission_writes: std::sync::Mutex<HashMap<String, PermissionWrite>>,
    cancel_writes: std::sync::Mutex<VecDeque<oneshot::Sender<std::result::Result<(), String>>>>,
    cancel_gate: Mutex<()>,
    child: Mutex<Child>,
    closed: AtomicBool,
    closed_reason: std::sync::Mutex<Option<String>>,
    active: AtomicBool,
    followups: AtomicUsize,
    state: Mutex<State>,
    events: broadcast::Sender<Value>,
    options: Options,
    mcp_servers: Vec<Value>,
    history: std::sync::Mutex<Option<History>>,
    permissions: std::sync::Mutex<Permissions>,
    #[cfg(test)]
    shutdown_failure: AtomicBool,
}
impl Drop for Inner {
    fn drop(&mut self) {
        self.stop.send_replace(true);
        let _ = terminate_owned_group(self.child.get_mut());
    }
}

fn terminate_owned_group(child: &mut Child) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        // Tokio only clears Child::id after wait/try_wait has reaped it. Do not
        // use a numeric group once that ownership proof has gone away.
        let pid = child.id().ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "owned ACP process identity is no longer available",
            )
        })?;
        if unsafe { libc::kill(-(pid as i32), libc::SIGKILL) } == -1 {
            let error = std::io::Error::last_os_error();
            if error.raw_os_error() != Some(libc::ESRCH) {
                return Err(error);
            }
        }
        Ok(())
    }
    #[cfg(not(unix))]
    child.start_kill()
}
// Claude's native process shutdown itself waits up to five seconds. Leave it a
// small outer margin so its cooperative reap completes before this fallback.
const OWNED_PROCESS_SHUTDOWN_GRACE: Duration = Duration::from_secs(6);
const OWNED_PROCESS_POST_FORCE_REAP: Duration = Duration::from_secs(1);
#[cfg(unix)]
const OWNED_PROCESS_EXIT_POLL: Duration = Duration::from_millis(5);

#[cfg(unix)]
fn owned_child_exited_without_reaping(child: &Child) -> std::io::Result<bool> {
    let pid = child.id().ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "owned ACP process identity is no longer available",
        )
    })?;
    // WNOWAIT preserves the leader PID/PGID until the forced group cleanup has
    // either happened or failed. Tokio's wait is cancellation-safe, but it
    // reaps a completed child when polled, which would lose that ownership.
    let mut info: libc::siginfo_t = unsafe { std::mem::zeroed() };
    if unsafe {
        libc::waitid(
            libc::P_PID,
            pid as libc::id_t,
            &mut info,
            libc::WEXITED | libc::WNOHANG | libc::WNOWAIT,
        )
    } == -1
    {
        return Err(std::io::Error::last_os_error());
    }
    Ok(info.si_signo != 0)
}

#[cfg(unix)]
async fn wait_for_owned_child_exit_without_reaping(child: &Child) -> std::io::Result<()> {
    loop {
        if owned_child_exited_without_reaping(child)? {
            return Ok(());
        }
        tokio::time::sleep(OWNED_PROCESS_EXIT_POLL).await;
    }
}
#[derive(Default)]
struct History {
    updates: Vec<Value>,
    bytes: usize,
    overflow: bool,
}
#[derive(Default)]
struct State {
    session: Option<String>,
    config_options: Vec<Value>,
    commands: Vec<Value>,
    early_commands: HashMap<String, Vec<Value>>,
    capabilities: Value,
    stop_reason: Option<String>,
    modes: Value,
    collected_text: String,
    text_boundary_pending: bool,
    collector_error: Option<String>,
    output: Output,
}
#[derive(Clone, Debug, Default)]
pub struct Output {
    pub raw_chars: usize,
    pub chunks: usize,
    pub tool_calls: usize,
    pub had_tool_activity: bool,
    pub had_server_request: bool,
    pub image_count: usize,
    pub images: Vec<InlineImage>,
    pub tool_results: String,
    pub output_lost: bool,
    pub collector_closed: bool,
    seen_tools: std::collections::HashSet<[u8; 32]>,
}
#[derive(Clone, Debug)]
pub struct InlineImage {
    pub path: String,
    pub alt: Option<String>,
}
#[derive(Clone)]
pub struct Client {
    inner: Arc<Inner>,
}
#[derive(Debug)]
pub struct Response {
    pub text: String,
    pub stop_reason: Option<String>,
    pub output: Output,
}
impl Client {
    pub(crate) fn permission_handler(
        &self,
    ) -> (mpsc::Receiver<PermissionRequest>, PermissionGuard) {
        let (sender, receiver) = mpsc::channel(32);
        let mut permissions = self.inner.permissions.lock().unwrap();
        permissions.clear();
        permissions.handler = Some(sender);
        (
            receiver,
            PermissionGuard(Arc::downgrade(&self.inner), permissions.generation),
        )
    }
    pub async fn inline_images(&self) -> Vec<InlineImage> {
        self.inner.state.lock().await.output.images.clone()
    }
    pub async fn prompt_observation(&self) -> (Output, Option<String>, Value) {
        let state = self.inner.state.lock().await;
        let mut output = state.output.clone();
        output.collector_closed = !self.inner.active.load(Ordering::SeqCst);
        let model = state
            .config_options
            .iter()
            .find(|option| option["id"] == "model" || option["category"] == "model")
            .map(|option| option["currentValue"].clone())
            .unwrap_or(Value::Null);
        (output, state.stop_reason.clone(), model)
    }
    pub async fn close_session(&self) -> Result<()> {
        self.inner.permissions.lock().unwrap().clear();
        let Some(session) = self.session_id().await else {
            return Ok(());
        };
        self.request("session/close", json!({"sessionId":session}))
            .await?;
        self.inner.state.lock().await.session = None;
        Ok(())
    }
    pub async fn delete_session(&self) -> Result<()> {
        self.inner.permissions.lock().unwrap().clear();
        let Some(session) = self.session_id().await else {
            return Ok(());
        };
        self.request("session/close", json!({"sessionId":session}))
            .await?;
        self.request("session/delete", json!({"sessionId":session}))
            .await?;
        self.inner.state.lock().await.session = None;
        Ok(())
    }
    pub async fn connect(options: Options) -> Result<Self> {
        Self::connect_with_servers(options, vec![]).await
    }
    pub async fn connect_with_servers(options: Options, mcp_servers: Vec<Value>) -> Result<Self> {
        if mcp_servers.len() > 32 {
            bail!("Too many MCP servers");
        }
        let mut command = Command::new(&options.command);
        #[cfg(unix)]
        command.process_group(0);
        command
            .args(&options.args)
            .current_dir(&options.cwd)
            .kill_on_drop(true)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped());
        if !options.inherit_env {
            command.env_clear();
            for (key, value) in std::env::vars() {
                if [
                    "PATH",
                    "HOME",
                    "CODEX_HOME",
                    "CLAUDE_CONFIG_DIR",
                    "PI_CODING_AGENT_DIR",
                    "PI_CODING_AGENT_SESSION_DIR",
                    "XDG_CONFIG_HOME",
                    "XDG_DATA_HOME",
                    "XDG_CACHE_HOME",
                    "TMPDIR",
                    "TMP",
                    "TEMP",
                    "LANG",
                    "TERM",
                    "SHELL",
                    "USER",
                    "LOGNAME",
                    "SystemRoot",
                    "WINDIR",
                    "PATHEXT",
                    "COMSPEC",
                    "USERPROFILE",
                    "APPDATA",
                    "LOCALAPPDATA",
                    "HTTP_PROXY",
                    "HTTPS_PROXY",
                    "ALL_PROXY",
                    "NO_PROXY",
                    "http_proxy",
                    "https_proxy",
                    "all_proxy",
                    "no_proxy",
                    "NODE_EXTRA_CA_CERTS",
                    "SSL_CERT_FILE",
                    "SSL_CERT_DIR",
                ]
                .contains(&key.as_str())
                    || key.starts_with("LC_")
                {
                    command.env(key, value);
                }
            }
        }
        command.envs(&options.env);
        let mut child = command
            .spawn()
            .context("starting configured ACP executable")?;
        let stdin = child.stdin.take().context("ACP stdin unavailable")?;
        let stdout = child.stdout.take().context("ACP stdout unavailable")?;
        let stderr = child.stderr.take().context("ACP stderr unavailable")?;
        let (events, _) = broadcast::channel(256);
        let (stop, mut stopped) = watch::channel(false);
        let stdin = Arc::new(Mutex::new(Some(stdin)));
        let inner = Arc::new(Inner {
            #[cfg(test)]
            shutdown_failure: AtomicBool::new(false),
            stdin: stdin.clone(),
            connection: Default::default(),
            stop,
            requests: tokio::sync::Semaphore::new(128),
            permission_writes: Default::default(),
            cancel_writes: Default::default(),
            cancel_gate: Mutex::new(()),
            child: Mutex::new(child),
            closed: AtomicBool::new(false),
            closed_reason: Default::default(),
            active: AtomicBool::new(false),
            followups: AtomicUsize::new(0),
            state: Mutex::new(State::default()),
            events,
            options,
            mcp_servers,
            history: std::sync::Mutex::new(None),
            permissions: std::sync::Mutex::new(Permissions::default()),
        });
        // The SDK owns JSON-RPC IDs, response routing, errors and dispatch ordering.
        // Lines lets us retain the bridge's bounded stdio frames and write gate.
        let read_inner = Arc::downgrade(&inner);
        let incoming = futures::stream::try_unfold(
            (BufReader::new(stdout), VecDeque::<String>::new()),
            move |(mut reader, mut responses)| {
                let weak = read_inner.clone();
                async move {
                    loop {
                        if !reader.buffer().contains(&b'\n')
                            && let Some(line) = responses.pop_front()
                        {
                            return Ok(Some((line, (reader, responses))));
                        }
                        let mut bytes = Vec::new();
                        let count = (&mut reader)
                            .take(2 * 1024 * 1024 + 1)
                            .read_until(b'\n', &mut bytes)
                            .await?;
                        if count == 0 {
                            return Ok(responses
                                .pop_front()
                                .map(|line| (line, (reader, responses))));
                        }
                        if bytes.len() > 2 * 1024 * 1024 {
                            if let Some(inner) = weak.upgrade() {
                                inner.close("ACP frame exceeded limit").await;
                            }
                            return Err(std::io::Error::other("ACP frame exceeded limit"));
                        }
                        let line = String::from_utf8(bytes).map_err(std::io::Error::other)?;
                        // Some agents flush the last chunk after their terminal reply.
                        // Drain complete buffered notifications before yielding replies;
                        // the SDK still owns all ID matching and response dispatch.
                        let response = serde_json::from_str::<Value>(&line).is_ok_and(|value| {
                            value.is_object()
                                && value.get("method").is_none()
                                && value.get("id").is_some()
                        });
                        if response {
                            if responses.len() >= 128 {
                                return Err(std::io::Error::other("ACP response queue full"));
                            }
                            responses.push_back(line);
                        } else {
                            return Ok(Some((line, (reader, responses))));
                        }
                    }
                }
            },
        )
        .boxed();
        let wire_inner = Arc::downgrade(&inner);
        let outgoing = Box::pin(futures::sink::unfold(
            stdin,
            move |stdin, mut line: String| {
                let weak = wire_inner.clone();
                async move {
                    if line.len() > 2 * 1024 * 1024 {
                        if let Some(inner) = weak.upgrade() {
                            inner.close("ACP frame exceeded limit").await;
                        }
                        return Err(std::io::Error::other("ACP frame exceeded limit"));
                    }
                    let mut input = stdin.lock().await;
                    let mut value: Value =
                        serde_json::from_str(&line).map_err(std::io::Error::other)?;
                    let inner = weak.upgrade();
                    let permission = inner.as_ref().and_then(|inner| {
                        inner
                            .permission_writes
                            .lock()
                            .unwrap()
                            .remove(&value["id"].to_string())
                    });
                    if let (Some(inner), Some(permission)) = (&inner, &permission) {
                        let valid = inner.permissions.lock().unwrap().generation
                            == permission.generation
                            && permission.current.load(Ordering::SeqCst)
                            && inner.active.load(Ordering::SeqCst)
                            && !inner.closed.load(Ordering::SeqCst);
                        if !valid {
                            value["result"]["outcome"] =
                                crate::connectors::matrix_permissions::cancelled();
                            line = value.to_string();
                        }
                    }
                    let cancel = value["method"] == "session/cancel" && value.get("id").is_none();
                    let result = {
                        let writer = input.as_mut().ok_or_else(|| {
                            std::io::Error::new(std::io::ErrorKind::BrokenPipe, "ACP stdin closed")
                        })?;
                        async {
                            writer.write_all(line.as_bytes()).await?;
                            writer.write_all(b"\n").await?;
                            writer.flush().await
                        }
                        .await
                    };
                    let receipt = result.as_ref().map(|_| ()).map_err(ToString::to_string);
                    if let Some(permission) = permission {
                        let _ = permission.done.send(receipt.clone());
                    }
                    if cancel
                        && let Some(inner) = inner
                        && let Some(done) = inner.cancel_writes.lock().unwrap().pop_front()
                    {
                        let _ = done.send(receipt);
                        // A failed cancel notification must not discard a response
                        // still arriving on the independent stdout direction.
                        return Ok(stdin.clone());
                    }
                    result?;
                    drop(input);
                    Ok::<_, std::io::Error>(stdin)
                }
            },
        ));
        let weak = Arc::downgrade(&inner);
        let updates = weak.clone();
        let requests = weak.clone();
        let (ready, connected) = oneshot::channel();
        tokio::spawn(async move {
            let result = agent_client_protocol::Client
                .builder()
                .on_receive_notification(
                    async move |message: UntypedMessage, _cx| {
                        if let Some(inner) = updates.upgrade() {
                            inner
                                .handle_update(
                                    json!({"method":message.method,"params":message.params}),
                                )
                                .await;
                        }
                        Ok(())
                    },
                    agent_client_protocol::on_receive_notification!(),
                )
                .on_receive_request(
                    async move |message: UntypedMessage, responder: Responder<Value>, _cx| {
                        if let Some(inner) = requests.upgrade() {
                            if inner.active.load(Ordering::SeqCst) {
                                inner.state.lock().await.output.had_server_request = true;
                            }
                            if message.method == "session/request_permission" {
                                inner.permission_request(message.params, responder).await;
                                return Ok(());
                            }
                        }
                        responder
                            .respond_with_error(agent_client_protocol::Error::method_not_found())
                    },
                    agent_client_protocol::on_receive_request!(),
                )
                .connect_with(Lines::new(outgoing, incoming), async move |connection| {
                    let _ = ready.send(connection.clone());
                    tokio::select! {
                        _ = stopped.changed() => {},
                        _ = connection.incoming_closed() => {},
                    }
                    Ok(())
                })
                .await;
            if let Some(inner) = weak.upgrade() {
                let message = result.err().map_or_else(
                    || "ACP process closed".into(),
                    |error| format!("ACP process closed: {error}"),
                );
                inner.close(&message).await;
                // Explicit shutdown holds this lock throughout graceful exit
                // and reap, so this cannot preempt the peer's child cleanup.
                // Other fatal close paths still need to stop their owned group.
                let _ = terminate_owned_group(&mut *inner.child.lock().await);
            }
        });
        inner
            .connection
            .set(connected.await.context("ACP SDK connection closed")?)
            .ok();
        tokio::spawn(async move {
            let mut stderr = stderr;
            let mut buffer = [0; 4096];
            while matches!(stderr.read(&mut buffer).await, Ok(count) if count > 0) {}
        });
        let client = Self { inner };
        let initialized = client.request("initialize", json!({"protocolVersion":1,"clientCapabilities":{"fs":{"readTextFile":false,"writeTextFile":false},"terminal":false}})).await?;
        if initialized["protocolVersion"] != 1 {
            client.shutdown().await?;
            bail!("ACP agent did not negotiate protocol version 1");
        }
        client.inner.state.lock().await.capabilities = initialized["agentCapabilities"].clone();
        Ok(client)
    }
    pub fn subscribe(&self) -> broadcast::Receiver<Value> {
        self.inner.events.subscribe()
    }
    pub fn working_directory(&self) -> &std::path::Path {
        &self.inner.options.cwd
    }
    pub fn accepts_followups(&self) -> bool {
        self.inner.active.load(Ordering::SeqCst)
    }
    pub async fn session_id(&self) -> Option<String> {
        self.inner.state.lock().await.session.clone()
    }
    pub async fn config_options(&self) -> Vec<Value> {
        self.inner.state.lock().await.config_options.clone()
    }
    pub async fn commands(&self) -> Vec<Value> {
        self.inner.state.lock().await.commands.clone()
    }
    pub async fn modes(&self) -> Value {
        self.inner.state.lock().await.modes.clone()
    }
    pub async fn new_session(&self) -> Result<String> {
        self.inner.permissions.lock().unwrap().clear();
        let result = self
            .request(
                "session/new",
                json!({"cwd":self.inner.options.cwd,"mcpServers":self.inner.mcp_servers}),
            )
            .await?;
        let session = result["sessionId"]
            .as_str()
            .context("ACP did not return a session ID")?
            .to_owned();
        let mut state = self.inner.state.lock().await;
        state.session = Some(session.clone());
        state.config_options = result["configOptions"]
            .as_array()
            .cloned()
            .unwrap_or_default();
        state.modes = result["modes"].clone();
        state.commands = state.early_commands.remove(&session).unwrap_or_default();
        state.early_commands.clear();
        Ok(session)
    }
    pub async fn load_session(&self, session: &str, resume: bool) -> Result<Vec<Value>> {
        self.inner.permissions.lock().unwrap().clear();
        {
            let mut history = self.inner.history.lock().unwrap();
            if history.is_some() || self.accepts_followups() {
                bail!("A session operation is already active");
            }
            *history = Some(History::default());
        }
        struct HistoryGuard(Arc<Inner>);
        impl Drop for HistoryGuard {
            fn drop(&mut self) {
                self.0.history.lock().unwrap().take();
            }
        }
        let _guard = HistoryGuard(self.inner.clone());
        let previous = self.session_id().await;
        self.inner.state.lock().await.session = Some(session.into());
        let capabilities = self.inner.state.lock().await.capabilities.clone();
        let method = if resume && !capabilities["sessionCapabilities"]["resume"].is_null() {
            "session/resume"
        } else {
            "session/load"
        };
        let result = self.request(method, json!({"sessionId":session,"cwd":self.inner.options.cwd,"mcpServers":self.inner.mcp_servers})).await;
        let result = match result {
            Ok(result) => result,
            Err(error) => {
                self.inner.state.lock().await.session = previous;
                return Err(error);
            }
        };
        let mut state = self.inner.state.lock().await;
        state.config_options = result["configOptions"]
            .as_array()
            .cloned()
            .unwrap_or_default();
        state.modes = result["modes"].clone();
        drop(state);
        let history = self.inner.history.lock().unwrap().take().unwrap();
        if history.overflow {
            bail!("ACP loaded history exceeded limit");
        }
        Ok(history.updates)
    }
    pub async fn list_sessions(&self) -> Result<Vec<Value>> {
        let mut sessions = vec![];
        let mut cursor = Value::Null;
        let mut seen = std::collections::HashSet::new();
        for _ in 0..100 {
            let mut params = json!({});
            if !cursor.is_null() {
                params["cursor"] = cursor.clone();
            }
            let result = self.request("session/list", params).await?;
            sessions.extend(result["sessions"].as_array().into_iter().flatten().cloned());
            cursor = result["nextCursor"].clone();
            if cursor.as_str().unwrap_or("").is_empty() {
                return Ok(sessions);
            }
            if !seen.insert(cursor.to_string()) {
                bail!("ACP list cursor repeated");
            }
        }
        bail!("ACP session list exceeded page limit")
    }
    pub async fn request(&self, method: &str, params: Value) -> Result<Value> {
        if self.inner.closed.load(Ordering::SeqCst) {
            bail!(
                self.inner
                    .closed_reason
                    .lock()
                    .unwrap()
                    .clone()
                    .unwrap_or_else(|| "ACP process closed".into())
            );
        }
        let eligible = method == "session/prompt"
            && self.inner.active.load(Ordering::SeqCst)
            && self.inner.state.lock().await.session.as_deref() == params["sessionId"].as_str();
        let clock = eligible.then(|| self.inner.permissions.lock().unwrap().clock.subscribe());
        let _permit = self
            .inner
            .requests
            .try_acquire()
            .context("ACP request queue full")?;
        let connection = self
            .inner
            .connection
            .get()
            .context("ACP SDK connection unavailable")?;
        let mut stopped = self.inner.stop.subscribe();
        let response = connection
            .send_request(protocol_request(method, params)?)
            .block_task();
        let operation = async {
            tokio::select! {
                result = response => result.map_err(|error| {
                    self.inner.closed_reason.lock().unwrap().clone()
                        .map_or_else(|| anyhow::Error::from(error), anyhow::Error::msg)
                }),
                _ = stopped.changed() => Err(anyhow::anyhow!(self.inner.closed_reason.lock().unwrap().clone().unwrap_or_else(|| "ACP process closed".into()))),
            }
        };
        if method == "session/prompt" {
            operation.await
        } else {
            RequestDeadline::new(self.inner.options.timeout, clock)
                .wait(operation)
                .await
                .context("ACP request timed out")?
        }
    }
    pub async fn prompt(&self, text: &str, images: Vec<Value>) -> Result<Response> {
        if self.inner.active.swap(true, Ordering::SeqCst) {
            bail!("An ACP response collector is already active");
        }
        struct ActiveGuard(Arc<Inner>);
        impl Drop for ActiveGuard {
            fn drop(&mut self) {
                self.0.active.store(false, Ordering::SeqCst);
                self.0.permissions.lock().unwrap().clear();
            }
        }
        let _guard = ActiveGuard(self.inner.clone());
        {
            let mut state = self.inner.state.lock().await;
            state.stop_reason = None;
            state.collected_text.clear();
            state.text_boundary_pending = false;
            state.collector_error = None;
            state.output = Output::default();
        }
        if self.session_id().await.is_none() {
            self.new_session().await?;
        }
        let session = self.session_id().await.unwrap();
        let mut content = vec![json!({"type":"text","text":text})];
        content.extend(images);
        let result = self
            .request(
                "session/prompt",
                json!({"sessionId":session,"prompt":content}),
            )
            .await;
        if self.inner.followups.load(Ordering::SeqCst) == 0 || result.is_err() {
            self.inner.permissions.lock().unwrap().clear();
        }
        if let Ok(result) = &result {
            let mut state = self.inner.state.lock().await;
            if state.stop_reason.is_none() {
                state.stop_reason = result["stopReason"].as_str().map(str::to_owned);
            }
        }
        if result.is_ok() {
            for delay in [10, 50, 200] {
                if !self.inner.state.lock().await.collected_text.is_empty() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(delay)).await;
            }
        }
        // Follow-ups are part of the same coding turn. Keep one collector and
        // busy owner until each request settles (including transport failure).
        loop {
            let state = self.inner.state.lock().await;
            if self.inner.followups.load(Ordering::SeqCst) == 0 {
                self.inner.active.store(false, Ordering::SeqCst);
                break;
            }
            drop(state);
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        result?;
        let mut state = self.inner.state.lock().await;
        state.output.collector_closed = true;
        if let Some(error) = state.collector_error.take() {
            bail!(error);
        }
        Ok(Response {
            text: std::mem::take(&mut state.collected_text),
            stop_reason: state.stop_reason.clone(),
            output: state.output.clone(),
        })
    }
    pub async fn forward(&self, text: &str) -> Result<()> {
        let state = self.inner.state.lock().await;
        if !self.accepts_followups() {
            bail!("No active ACP prompt");
        }
        if self.inner.followups.load(Ordering::SeqCst) >= 8 {
            bail!("ACP follow-up queue full");
        }
        self.inner.followups.fetch_add(1, Ordering::SeqCst);
        drop(state);
        struct FollowupGuard(Arc<Inner>);
        impl Drop for FollowupGuard {
            fn drop(&mut self) {
                self.0.followups.fetch_sub(1, Ordering::SeqCst);
            }
        }
        let _guard = FollowupGuard(self.inner.clone());
        let session = self.session_id().await.context("No active ACP session")?;
        let result = self
            .request(
                "session/prompt",
                json!({"sessionId":session,"prompt":[{"type":"text","text":text}]}),
            )
            .await;
        let result = result?;
        if let Some(reason) = result["stopReason"]
            .as_str()
            .filter(|reason| !reason.is_empty() && *reason != "end_turn")
        {
            self.inner.state.lock().await.stop_reason = Some(reason.into());
        }
        Ok(())
    }
    pub async fn cancel(&self) -> Result<()> {
        let _gate = self.inner.cancel_gate.lock().await;
        self.inner.permissions.lock().unwrap().clear();
        if self.inner.closed.load(Ordering::SeqCst) {
            bail!(
                self.inner
                    .closed_reason
                    .lock()
                    .unwrap()
                    .clone()
                    .unwrap_or_else(|| "ACP process closed".into())
            );
        }
        let (done, receipt) = oneshot::channel();
        {
            let mut writes = self.inner.cancel_writes.lock().unwrap();
            if writes.len() >= 128 {
                bail!("ACP cancellation queue full");
            }
            writes.push_back(done);
        }
        let sent = self
            .inner
            .connection
            .get()
            .context("ACP SDK connection unavailable")?
            .send_notification(UntypedMessage::new(
                "session/cancel",
                json!({"sessionId":self.session_id().await}),
            )?);
        if let Err(error) = sent {
            self.inner.cancel_writes.lock().unwrap().pop_back();
            return Err(error.into());
        }
        let result = tokio::time::timeout(self.inner.options.timeout, receipt).await;
        // Abandoned receipts stay in FIFO order until their notification is
        // written; subsequent cancellation waits for its own physical write.
        result
            .context("ACP cancellation write timed out")?
            .context("ACP cancellation write closed")?
            .map_err(anyhow::Error::msg)?;
        self.inner.state.lock().await.stop_reason = Some("cancelled".into());
        Ok(())
    }
    pub async fn set_mode(&self, mode: &str) -> Result<()> {
        let modes = self.modes().await;
        if !modes["availableModes"]
            .as_array()
            .into_iter()
            .flatten()
            .any(|item| item["id"] == mode)
        {
            bail!("Mode is not advertised");
        }
        self.request(
            "session/set_mode",
            json!({"sessionId":self.session_id().await,"modeId":mode}),
        )
        .await?;
        self.inner.state.lock().await.modes["currentModeId"] = mode.into();
        Ok(())
    }
    pub async fn set_option(&self, id: &str, value: &str) -> Result<Value> {
        let state = self.inner.state.lock().await;
        let option = state
            .config_options
            .iter()
            .find(|item| item["id"] == id)
            .context("ACP does not advertise this configuration option")?;
        if !choice_contains(&option["options"], value) {
            bail!("Value is not advertised for this option");
        }
        drop(state);
        let result = self
            .request(
                "session/set_config_option",
                json!({"sessionId":self.session_id().await,"configId":id,"value":value}),
            )
            .await?;
        let options = result["configOptions"]
            .as_array()
            .context("Configuration switch was not confirmed")?;
        let confirmed = options
            .iter()
            .find(|item| item["id"] == id && item["currentValue"] == value)
            .context("Configuration switch was not confirmed")?
            .clone();
        replace_config_options(&mut *self.inner.state.lock().await, options.clone());
        Ok(confirmed)
    }
    pub async fn shutdown(&self) -> Result<()> {
        #[cfg(test)]
        if self.inner.shutdown_failure.swap(false, Ordering::SeqCst) {
            bail!("Synthetic owned ACP shutdown failure");
        }
        let mut child = self.inner.child.lock().await;
        if child.id().is_none() {
            child.wait().await?;
            self.inner.close("ACP process closed").await;
            self.inner.state.lock().await.session = None;
            return Ok(());
        }
        // Mark the client closed before grace begins so a new prompt cannot
        // race shutdown. The connection driver observes this flag and leaves
        // cooperative EOF/reap to this serialized owner path.
        self.inner.close("ACP process closed").await;
        let stdin_closed =
            match tokio::time::timeout(OWNED_PROCESS_SHUTDOWN_GRACE, self.inner.stdin.lock()).await
            {
                Ok(mut stdin) => {
                    drop(stdin.take());
                    true
                }
                Err(_) => false,
            };
        #[cfg(unix)]
        if stdin_closed {
            // Do not call Tokio's wait during the grace period: it reaps an
            // early-exiting group leader before its still-running descendants
            // can be signalled. WNOWAIT observes EOF shutdown without giving
            // up the owned PID/PGID.
            match tokio::time::timeout(
                OWNED_PROCESS_SHUTDOWN_GRACE,
                wait_for_owned_child_exit_without_reaping(&child),
            )
            .await
            {
                Ok(Ok(())) | Err(_) => {}
                Ok(Err(error)) => {
                    self.inner.close("ACP process closed").await;
                    return Err(error)
                        .context("checking owned ACP process identity before shutdown");
                }
            }
        }
        #[cfg(not(unix))]
        if stdin_closed {
            match tokio::time::timeout(OWNED_PROCESS_SHUTDOWN_GRACE, child.wait()).await {
                Ok(Ok(_)) => {
                    self.inner.state.lock().await.session = None;
                    return Ok(());
                }
                Ok(Err(error)) => return Err(error).context("reaping owned ACP process"),
                Err(_) => {}
            }
        }
        terminate_owned_group(&mut child).context("forcing owned ACP process group shutdown")?;
        let reaped = match tokio::time::timeout(OWNED_PROCESS_POST_FORCE_REAP, child.wait()).await {
            Ok(status) => status.context("reaping forced owned ACP process"),
            Err(_) => Err(anyhow::anyhow!(
                "owned ACP process did not reap after forced shutdown"
            )),
        };
        reaped?;
        self.inner.state.lock().await.session = None;
        Ok(())
    }
    #[cfg(test)]
    pub(crate) fn fail_expiry_shutdown_once(&self) {
        self.inner.shutdown_failure.store(true, Ordering::SeqCst);
    }
    #[cfg(test)]
    pub(crate) async fn hold_expiry_shutdown(&self) -> tokio::sync::MutexGuard<'_, Child> {
        self.inner.child.lock().await
    }
    #[cfg(test)]
    pub(crate) fn same_expiry_client(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.inner, &other.inner)
    }
}

// Validate and serialize standard requests with the SDK schema. Responses stay
// in the bridge's JSON projection so advertised backend extensions survive.
fn protocol_request(method: &str, params: Value) -> Result<UntypedMessage> {
    use agent_client_protocol::schema::v1;
    macro_rules! typed {
        ($request:ty) => {{
            let request: $request = serde_json::from_value(params)?;
            request.to_untyped_message().map_err(Into::into)
        }};
    }
    match method {
        "initialize" => typed!(v1::InitializeRequest),
        "session/new" => typed!(v1::NewSessionRequest),
        "session/load" => typed!(v1::LoadSessionRequest),
        "session/resume" => typed!(v1::ResumeSessionRequest),
        "session/list" => typed!(v1::ListSessionsRequest),
        "session/prompt" => typed!(v1::PromptRequest),
        "session/set_mode" => typed!(v1::SetSessionModeRequest),
        "session/set_config_option" => typed!(v1::SetSessionConfigOptionRequest),
        "session/close" => typed!(v1::CloseSessionRequest),
        "session/delete" => typed!(v1::DeleteSessionRequest),
        _ => UntypedMessage::new(method, params).map_err(Into::into),
    }
}

fn choice_contains(options: &Value, value: &str) -> bool {
    options
        .as_array()
        .into_iter()
        .flatten()
        .any(|item| item["value"] == value || choice_contains(&item["options"], value))
}

impl Inner {
    async fn close(&self, message: &str) {
        if !self.closed.swap(true, Ordering::SeqCst) {
            *self.closed_reason.lock().unwrap() = Some(message.into());
        }
        self.stop.send_replace(true);
        self.permissions.lock().unwrap().clear();
        self.permission_writes.lock().unwrap().clear();
        self.cancel_writes.lock().unwrap().clear();
    }
    async fn handle_update(self: &Arc<Self>, message: Value) {
        if let Some(method) = message["method"].as_str()
            && method == "session/update"
        {
            let params = &message["params"];
            let session = params["sessionId"].as_str().unwrap_or("");
            let mut update = params["update"].clone();
            if let Some(part) = params.get("part").or_else(|| update.get("part")).cloned() {
                update = json!({"sessionUpdate":"agent_message_chunk","content":part});
            }
            let mut state = self.state.lock().await;
            if update["sessionUpdate"] == "available_commands_update" {
                let commands = update["availableCommands"]
                    .as_array()
                    .cloned()
                    .unwrap_or_default();
                if state.session.as_deref() == Some(session) {
                    state.commands = commands;
                } else if state.session.is_none() && state.early_commands.len() < 16 {
                    state.early_commands.insert(session.into(), commands);
                }
            }
            if state.session.as_deref() != Some(session) {
                return;
            }
            if let Some(history) = self.history.lock().unwrap().as_mut() {
                let bytes = update.to_string().len();
                if history.bytes + bytes > 8 * 1024 * 1024 || history.updates.len() >= 16_384 {
                    history.overflow = true;
                } else {
                    history.bytes += bytes;
                    history.updates.push(update.clone());
                }
            }
            if !self.active.load(Ordering::SeqCst)
                && matches!(
                    update["sessionUpdate"].as_str(),
                    Some(
                        "agent_message_chunk"
                            | "tool_call"
                            | "tool_call_update"
                            | "agent_thought_chunk"
                    )
                )
            {
                return;
            }
            if self.active.load(Ordering::SeqCst) {
                let tool_calls = state.output.tool_calls;
                collect_output(&mut state.output, &update, &self.options.cwd);
                // A new tool starts a separate agent text block. Late updates
                // for an existing tool must not split subsequent token chunks.
                if state.output.tool_calls > tool_calls && !state.collected_text.trim().is_empty() {
                    state.text_boundary_pending = true;
                }
            }
            update["collectorImageCount"] = state.output.images.len().into();
            if self.active.load(Ordering::SeqCst)
                && update["sessionUpdate"] == "agent_message_chunk"
                && update["content"]["type"] == "text"
                && let Some(chunk) = update["content"]["text"].as_str()
            {
                let resumed = state.text_boundary_pending
                    && chunk.chars().any(|character| !character.is_whitespace());
                let missing_breaks = if resumed {
                    let trailing = state
                        .collected_text
                        .chars()
                        .rev()
                        .take_while(|character| character.is_whitespace())
                        .filter(|character| *character == '\n')
                        .take(2)
                        .count();
                    let leading = chunk
                        .chars()
                        .take_while(|character| character.is_whitespace())
                        .filter(|character| *character == '\n')
                        .take(2)
                        .count();
                    2usize.saturating_sub(trailing + leading)
                } else {
                    0
                };
                if state.collected_text.len() + chunk.len() + missing_breaks > 2 * 1024 * 1024 {
                    state.collector_error = Some("ACP response exceeded limit".into());
                    state.output.output_lost = true;
                } else {
                    let presented = format!("{}{}", "\n".repeat(missing_breaks), chunk);
                    state.collected_text.push_str(&presented);
                    if resumed {
                        state.text_boundary_pending = false;
                    }
                    if missing_breaks > 0 {
                        // collect_output above counts only original source text.
                        // Subscribers receive the same presentation as the final reply.
                        update["content"]["text"] = Value::String(presented);
                    }
                }
            }
            if update["sessionUpdate"] == "config_option_update" {
                let options = update["configOptions"]
                    .as_array()
                    .cloned()
                    .unwrap_or_default();
                replace_config_options(&mut state, options);
            }
            if update["sessionUpdate"] == "current_mode_update" {
                state.modes["currentModeId"] = update["currentModeId"].clone();
            }
            drop(state);
            let _ = self.events.send(update);
        }
    }
    async fn permission_request(self: &Arc<Self>, params: Value, responder: Responder<Value>) {
        use crate::connectors::matrix_permissions::cancelled;
        let params = &params;
        let mut state = self.state.lock().await;
        let matching = self.active.load(Ordering::SeqCst)
            && !self.closed.load(Ordering::SeqCst)
            && state
                .session
                .as_deref()
                .is_some_and(|session| Some(session) == params["sessionId"].as_str());
        if matching {
            state.output.had_tool_activity = true;
        }
        drop(state);
        let key = serde_json::to_value(responder.id()).unwrap().to_string();
        let valid = crate::connectors::matrix_permissions::native_options(params).is_some()
            && params["toolCall"].is_object();
        let dispatch = {
            let mut permissions = self.permissions.lock().unwrap();
            if permissions.seen.contains(&key) {
                permissions.end_wait(&key);
                if let Some((current, cancel)) = permissions.pending.remove(&key) {
                    current.store(false, Ordering::SeqCst);
                    let _ = cancel.send(());
                }
                return;
            }
            let handler = permissions.handler.clone();
            if matching
                && valid
                && permissions.pending.len() < 32
                && permissions.seen.len() < 128
                && let Some(handler) = handler
            {
                permissions.seen.insert(key.clone());
                permissions.begin_wait(&key);
                let current = Arc::new(AtomicBool::new(true));
                let (cancel, cancelled) = oneshot::channel();
                permissions
                    .pending
                    .insert(key.clone(), (current.clone(), cancel));
                Some((handler, current, cancelled, permissions.generation))
            } else {
                None
            }
        };
        let Some((handler, current, cancel, generation)) = dispatch else {
            if matching {
                let _ = self.events.send(permission_notice(params));
            }
            let _ = responder.respond(json!({"outcome":cancelled()}));
            return;
        };
        let (reply, response) = oneshot::channel();
        let invocation = PermissionRequest {
            params: params.clone(),
            current: current.clone(),
            reply,
        };
        let delivered = handler.try_send(invocation).is_ok();
        let params = params.clone();
        let weak = Arc::downgrade(self);
        tokio::spawn(async move {
            let outcome = if delivered {
                tokio::select! { biased; _ = cancel => cancelled(), outcome = response => outcome.unwrap_or_else(|_| cancelled()) }
            } else {
                cancelled()
            };
            let Some(inner) = weak.upgrade() else {
                return;
            };
            {
                let mut permissions = inner.permissions.lock().unwrap();
                if permissions.generation == generation {
                    permissions.end_wait(&key);
                }
            }
            let write = inner
                .write_permission(&params, responder, outcome, current.clone(), generation)
                .await;
            {
                let mut permissions = inner.permissions.lock().unwrap();
                if permissions.generation == generation {
                    permissions.pending.remove(&key);
                }
                current.store(false, Ordering::SeqCst);
            }
            if write.is_err() {
                inner.close("ACP permission reply failed").await;
            }
        });
    }
    async fn write_permission(
        &self,
        params: &Value,
        responder: Responder<Value>,
        outcome: Value,
        current: Arc<AtomicBool>,
        generation: u64,
    ) -> Result<()> {
        use crate::connectors::matrix_permissions::cancelled;
        tokio::time::timeout(self.options.timeout, async {
            let valid = self.permissions.lock().unwrap().generation == generation
                && current.load(Ordering::SeqCst)
                && self.active.load(Ordering::SeqCst)
                && !self.closed.load(Ordering::SeqCst);
            let offered = outcome["outcome"] == "selected"
                && params["options"].as_array().is_some_and(|options| {
                    options.iter().any(|option| {
                        option["optionId"] == outcome["optionId"]
                            && (option["kind"] == "allow_once" || option["kind"] == "reject_once")
                    })
                });
            let outcome = if valid && offered {
                outcome
            } else {
                cancelled()
            };
            let key = serde_json::to_value(responder.id())?.to_string();
            let (done, receipt) = oneshot::channel();
            self.permission_writes.lock().unwrap().insert(
                key.clone(),
                PermissionWrite {
                    generation,
                    current,
                    done,
                },
            );
            let result = async {
                responder.respond(json!({"outcome":outcome}))?;
                receipt
                    .await
                    .context("ACP permission write closed")?
                    .map_err(anyhow::Error::msg)
            }
            .await;
            self.permission_writes.lock().unwrap().remove(&key);
            result
        })
        .await
        .context("ACP permission reply timed out")?
    }
}

fn permission_notice(params: &Value) -> Value {
    json!({"sessionUpdate":"permission_rejected","permission":"tool","path":null,"message":"Permission denied: approval unavailable for this request","sessionId":params["sessionId"]})
}

fn collect_output(output: &mut Output, update: &Value, root: &std::path::Path) {
    match update["sessionUpdate"].as_str().unwrap_or("") {
        "agent_message_chunk" => {
            if update["content"]["type"] == "text" {
                if let Some(text) = update["content"]["text"].as_str() {
                    output.raw_chars += text.encode_utf16().count();
                    output.chunks += 1;
                }
            } else if update["content"]["type"] == "image" {
                collect_image(output, &update["content"], root);
            }
        }
        "tool_call" | "tool_call_update" => {
            output.had_tool_activity = true;
            let identity = update["toolCallId"]
                .as_str()
                .filter(|id| !id.is_empty())
                .or_else(|| update["title"].as_str().filter(|title| !title.is_empty()))
                .or_else(|| update["name"].as_str().filter(|name| !name.is_empty()))
                .unwrap_or("unknown");
            if output.seen_tools.len() < 1024 {
                if output
                    .seen_tools
                    .insert(Sha256::digest(identity.as_bytes()).into())
                {
                    output.tool_calls += 1;
                }
            } else {
                output.output_lost = true;
            }
            if update["sessionUpdate"] != "tool_call_update" || update["status"] != "completed" {
                return;
            }
            let mut result = String::new();
            if let Some(contents) = update["content"].as_array() {
                for item in contents {
                    let content = &item["content"];
                    if content["type"] == "image" {
                        collect_image(output, content, root);
                    } else if content["type"] == "text"
                        && let Some(text) = content["text"].as_str()
                    {
                        if result.len().saturating_add(text.len()) <= 2 * 1024 * 1024 {
                            result.push_str(text);
                        } else {
                            output.output_lost = true;
                        }
                    }
                }
            } else if let Some(text) = update["rawOutput"]["output"].as_str() {
                result.push_str(text);
            }
            if let Ok(parsed) = serde_json::from_str::<Value>(&result) {
                if let Some(items) = parsed.as_array().or_else(|| parsed["content"].as_array()) {
                    for item in items {
                        if item["type"] == "image"
                            && item["data"].as_str().is_some_and(|data| !data.is_empty())
                        {
                            collect_image(output, item, root);
                        }
                    }
                } else if parsed["type"] == "image"
                    && parsed["data"].as_str().is_some_and(|data| !data.is_empty())
                {
                    collect_image(output, &parsed, root);
                }
            }
            if !result.is_empty() {
                if output
                    .tool_results
                    .len()
                    .saturating_add(result.len())
                    .saturating_add(1)
                    <= 2 * 1024 * 1024
                {
                    output.tool_results.push_str(&result);
                    output.tool_results.push('\n');
                } else {
                    output.output_lost = true;
                }
            }
        }
        _ => {}
    }
}

fn collect_image(output: &mut Output, content: &Value, root: &std::path::Path) {
    output.image_count += 1;
    if output.images.len() >= 16 {
        output.output_lost = true;
        return;
    }
    match crate::media::save_inline(root, content) {
        Ok(path) => output.images.push(InlineImage {
            path,
            alt: content["alt"]
                .as_str()
                .filter(|alt| !alt.is_empty())
                .map(str::to_owned),
        }),
        Err(_) => output.output_lost = true,
    }
}

#[cfg(test)]
mod permission_tests {
    use super::*;
    async fn client(root: &std::path::Path, mode: &str, timeout: Duration) -> Client {
        client_with_params(root, mode, timeout, json!({})).await
    }
    async fn client_with_params(
        root: &std::path::Path,
        mode: &str,
        timeout: Duration,
        params: Value,
    ) -> Client {
        Client::connect(Options {
            command: "/usr/bin/python3".into(),
            args: vec![
                concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/tests/fixtures/matrix-approval-peer.py"
                )
                .into(),
            ],
            cwd: root.into(),
            env: BTreeMap::from([
                (
                    "APPROVAL_AUDIT".into(),
                    root.join("rpc.jsonl").to_str().unwrap().into(),
                ),
                ("APPROVAL_MODE".into(), mode.into()),
                ("APPROVAL_REQUEST".into(), params.to_string()),
            ]),
            inherit_env: false,
            timeout,
        })
        .await
        .unwrap()
    }
    fn replies(root: &std::path::Path) -> Vec<Value> {
        std::fs::read_to_string(root.join("rpc.jsonl"))
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str::<Value>(line).unwrap())
            .filter(|value| value["id"] == 900 && value.get("method").is_none())
            .collect()
    }
    async fn wait_reply(root: &std::path::Path) -> Vec<Value> {
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let values = replies(root);
                if !values.is_empty() {
                    break values;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap()
    }
    #[tokio::test]
    async fn dispatched_permission_wait_does_not_consume_prompt_deadline() {
        let root = tempfile::tempdir().unwrap();
        let client = client(root.path(), "", Duration::from_millis(250)).await;
        client.new_session().await.unwrap();
        let (mut requests, _guard) = client.permission_handler();
        let task = {
            let client = client.clone();
            tokio::spawn(async move { client.prompt("work", vec![]).await })
        };
        let request = tokio::time::timeout(Duration::from_secs(2), requests.recv())
            .await
            .unwrap()
            .unwrap();
        tokio::time::sleep(Duration::from_millis(400)).await;
        let alive = request.current.load(Ordering::SeqCst) && !task.is_finished();
        let _ = request
            .reply
            .send(json!({"outcome":"selected", "optionId":"native-once"}));
        let result = task.await.unwrap();
        client.shutdown().await.unwrap();
        assert!(
            alive,
            "valid approval wait consumed the ordinary prompt deadline"
        );
        result.unwrap();
        assert_eq!(
            replies(root.path())[0]["result"]["outcome"]["optionId"],
            "native-once"
        );
    }
    #[tokio::test]
    async fn pending_followup_approval_keeps_original_collector_alive() {
        let root = tempfile::tempdir().unwrap();
        let client = client(root.path(), "followup", Duration::from_millis(250)).await;
        client.new_session().await.unwrap();
        let (mut requests, _guard) = client.permission_handler();
        let mut events = client.subscribe();
        let task = {
            let client = client.clone();
            tokio::spawn(async move { client.prompt("work", vec![]).await })
        };
        tokio::time::timeout(Duration::from_secs(2), events.recv())
            .await
            .unwrap()
            .unwrap();
        let forward = {
            let client = client.clone();
            tokio::spawn(async move { client.forward("followup").await })
        };
        let request = tokio::time::timeout(Duration::from_secs(2), requests.recv())
            .await
            .unwrap()
            .unwrap();
        tokio::time::sleep(Duration::from_millis(400)).await;
        let alive =
            request.current.load(Ordering::SeqCst) && !task.is_finished() && !forward.is_finished();
        let _ = request
            .reply
            .send(json!({"outcome":"selected", "optionId":"native-once"}));
        let result = task.await.unwrap();
        let forwarded = forward.await.unwrap();
        client.shutdown().await.unwrap();
        assert!(
            alive,
            "original collector timed out during pending followup approval"
        );
        result.unwrap();
        forwarded.unwrap();
        assert_eq!(
            replies(root.path())[0]["result"]["outcome"]["optionId"],
            "native-once"
        );
    }
    #[tokio::test(start_paused = true)]
    async fn approval_clock_credits_union_without_resetting_the_hard_cap() {
        let mut permissions = Permissions::default();
        let clock = permissions.clock.subscribe();
        let deadline = RequestDeadline::new(Duration::from_secs(300), Some(clock));
        permissions.begin_wait("a");
        tokio::time::advance(Duration::from_secs(200)).await;
        permissions.begin_wait("b");
        tokio::time::advance(Duration::from_secs(100)).await;
        permissions.end_wait("a");
        tokio::time::advance(Duration::from_secs(200)).await;
        permissions.end_wait("b");
        assert_eq!(
            permissions.clock.borrow().elapsed,
            Duration::from_secs(200 + 100 + 200)
        );
        assert!(permissions.clock.borrow().since.is_none());
        permissions.begin_wait("c");
        tokio::time::advance(APPROVAL_WAIT_CAP + Duration::from_secs(300)).await;
        assert!(deadline.wait(std::future::pending::<()>()).await.is_none());
        permissions.clear();
        assert!(permissions.waiting.is_empty());
        assert!(permissions.clock.borrow().since.is_none());
    }
    #[tokio::test]
    async fn silent_prompt_waits_for_transport_failure_not_control_timeout() {
        let root = tempfile::tempdir().unwrap();
        let client = client(root.path(), "quiet", Duration::from_millis(250)).await;
        client.new_session().await.unwrap();
        let (_requests, _guard) = client.permission_handler();
        let worker = client.clone();
        let task = tokio::spawn(async move { worker.prompt("work", vec![]).await });
        tokio::time::sleep(Duration::from_millis(600)).await;
        let alive = !task.is_finished() && client.accepts_followups();
        client.shutdown().await.unwrap();
        let result = tokio::time::timeout(Duration::from_secs(1), task)
            .await
            .unwrap()
            .unwrap();
        assert!(alive, "silent prompt expired at control deadline");
        assert!(result.is_err(), "shutdown must settle the pending prompt");
    }
    #[tokio::test]
    async fn approval_wait_does_not_extend_unrelated_control_rpc() {
        let root = tempfile::tempdir().unwrap();
        let client = client(root.path(), "", Duration::from_millis(250)).await;
        client.new_session().await.unwrap();
        let (mut requests, _guard) = client.permission_handler();
        let task = {
            let client = client.clone();
            tokio::spawn(async move { client.prompt("work", vec![]).await })
        };
        let request = requests.recv().await.unwrap();
        let result = tokio::time::timeout(
            Duration::from_secs(1),
            client.request("fixture/hang", json!({})),
        )
        .await
        .unwrap();
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("ACP request timed out")
        );
        assert!(request.current.load(Ordering::SeqCst) && !task.is_finished());
        request
            .reply
            .send(json!({"outcome":"selected", "optionId":"native-once"}))
            .unwrap();
        task.await.unwrap().unwrap();
        client.shutdown().await.unwrap();
    }
    #[tokio::test]
    async fn approval_decision_keeps_prompt_alive_until_transport_failure_without_replay() {
        let root = tempfile::tempdir().unwrap();
        let client = client(
            root.path(),
            "hold_after_decision",
            Duration::from_millis(250),
        )
        .await;
        client.new_session().await.unwrap();
        let (mut requests, _guard) = client.permission_handler();
        let task = {
            let client = client.clone();
            tokio::spawn(async move { client.prompt("work", vec![]).await })
        };
        let request = requests.recv().await.unwrap();
        tokio::time::sleep(Duration::from_millis(400)).await;
        assert!(!task.is_finished());
        request
            .reply
            .send(json!({"outcome":"selected", "optionId":"native-once"}))
            .unwrap();
        tokio::time::sleep(Duration::from_millis(600)).await;
        let alive = !task.is_finished() && client.accepts_followups();
        client.shutdown().await.unwrap();
        let result = tokio::time::timeout(Duration::from_secs(1), task)
            .await
            .unwrap()
            .unwrap();
        assert!(alive, "approval decision reintroduced a prompt deadline");
        assert!(result.is_err(), "shutdown must settle the pending prompt");
        assert_eq!(replies(root.path()).len(), 1);
        let audit = std::fs::read_to_string(root.path().join("rpc.jsonl")).unwrap();
        assert_eq!(
            audit
                .lines()
                .filter(
                    |line| serde_json::from_str::<Value>(line).unwrap()["method"]
                        == "session/prompt"
                )
                .count(),
            1
        );
    }
    #[tokio::test]
    async fn permission_stdout_reader_remains_live_and_fabricated_always_outcomes_cancel() {
        for outcome in [
            json!({"outcome":"selected", "optionId":"fabricated"}),
            json!({"outcome":"selected", "optionId":"native-always"}),
            json!({"outcome":"selected", "optionId":"native-once"}),
        ] {
            let root = tempfile::tempdir().unwrap();
            let client = client_with_params(root.path(), "", Duration::from_secs(3), json!({"options":[{"kind":"allow_once","optionId":"native-once"},{"kind":"allow_always","optionId":"native-always"}]})).await;
            client.new_session().await.unwrap();
            let (mut requests, _guard) = client.permission_handler();
            let task = {
                let client = client.clone();
                tokio::spawn(async move { client.prompt("work", vec![]).await })
            };
            let request = tokio::time::timeout(Duration::from_secs(2), requests.recv())
                .await
                .unwrap()
                .unwrap();
            assert!(request.current.load(Ordering::SeqCst));
            assert!(
                tokio::time::timeout(Duration::from_secs(1), client.list_sessions())
                    .await
                    .unwrap()
                    .unwrap()
                    .is_empty()
            );
            assert!(
                replies(root.path()).is_empty(),
                "no early permission response"
            );
            let expected = if outcome["optionId"] == "native-once" {
                outcome.clone()
            } else {
                json!({"outcome":"cancelled"})
            };
            request.reply.send(outcome).unwrap();
            task.await.unwrap().unwrap();
            assert_eq!(
                wait_reply(root.path()).await[0]["result"]["outcome"],
                expected
            );
            client.shutdown().await.unwrap();
        }
    }
    #[tokio::test]
    async fn permission_malformed_native_options_never_reach_the_handler() {
        for options in [
            json!([{"kind":"allow_once","optionId":"same"},{"kind":"reject_once","optionId":"same"}]),
            json!([{"kind":"allow_once","optionId":"once"},{"kind":"reject_once","optionId":12}]),
            json!([{"kind":"allow_always","optionId":"always"}]),
        ] {
            let root = tempfile::tempdir().unwrap();
            let client = client_with_params(
                root.path(),
                "",
                Duration::from_secs(3),
                json!({"options":options}),
            )
            .await;
            client.new_session().await.unwrap();
            let (mut requests, _guard) = client.permission_handler();
            let mut task = {
                let client = client.clone();
                tokio::spawn(async move { client.prompt("work", vec![]).await })
            };
            let received =
                tokio::select! { request = requests.recv() => request, _ = &mut task => None };
            let dispatched = received.is_some();
            if let Some(request) = received {
                let _ = request.reply.send(json!({"outcome":"cancelled"}));
                let _ = task.await;
            }
            client.shutdown().await.unwrap();
            assert!(
                !dispatched,
                "malformed or missing allow_once options reached handler: {options}"
            );
            assert_eq!(
                replies(root.path())[0]["result"]["outcome"],
                json!({"outcome":"cancelled"})
            );
        }
    }
    #[tokio::test]
    async fn permission_lifecycle_invalidates_current_operation_once() {
        for action in [
            "cancel",
            "new",
            "load",
            "close",
            "delete",
            "replace",
            "shutdown",
            "abort",
            "handler-drop",
        ] {
            let root = tempfile::tempdir().unwrap();
            let timeout = if action == "handler-drop" {
                Duration::from_millis(150)
            } else {
                Duration::from_secs(3)
            };
            let client = client(root.path(), "", timeout).await;
            client.new_session().await.unwrap();
            let (mut requests, _guard) = client.permission_handler();
            let task = {
                let client = client.clone();
                tokio::spawn(async move { client.prompt("work", vec![]).await })
            };
            let request = tokio::time::timeout(Duration::from_secs(2), requests.recv())
                .await
                .unwrap()
                .unwrap();
            match action {
                "cancel" => client.cancel().await.unwrap(),
                "new" => {
                    client.new_session().await.unwrap();
                }
                "load" => {
                    assert!(
                        client
                            .load_session("approval-session", false)
                            .await
                            .is_err()
                    );
                }
                "close" => client.close_session().await.unwrap(),
                "delete" => client.delete_session().await.unwrap(),
                "replace" => {
                    let (_requests, _new_guard) = client.permission_handler();
                }
                "shutdown" => client.shutdown().await.unwrap(),
                "abort" => {
                    task.abort();
                    tokio::task::yield_now().await;
                }
                "handler-drop" => drop(_guard),
                _ => unreachable!(),
            }
            assert!(
                !request.current.load(Ordering::SeqCst),
                "current after {action}"
            );
            let _ = request
                .reply
                .send(json!({"outcome":"selected", "optionId":"native-once"}));
            let _ = tokio::time::timeout(Duration::from_secs(2), task)
                .await
                .unwrap();
            if action != "shutdown" {
                let values = wait_reply(root.path()).await;
                assert_eq!(values.len(), 1, "replies after {action}");
                assert_eq!(
                    values[0]["result"]["outcome"],
                    json!({"outcome":"cancelled"}),
                    "outcome after {action}"
                );
            }
            client.shutdown().await.unwrap();
        }
    }
    #[tokio::test]
    async fn permission_duplicate_completion_and_process_loss_invalidate_without_replay() {
        for mode in ["duplicate", "complete", "exit"] {
            let root = tempfile::tempdir().unwrap();
            let client = client(root.path(), mode, Duration::from_secs(3)).await;
            client.new_session().await.unwrap();
            let (mut requests, _guard) = client.permission_handler();
            let task = {
                let client = client.clone();
                tokio::spawn(async move { client.prompt("work", vec![]).await })
            };
            let request = tokio::time::timeout(Duration::from_secs(2), requests.recv())
                .await
                .unwrap()
                .unwrap();
            tokio::time::timeout(Duration::from_secs(2), async {
                while request.current.load(Ordering::SeqCst) {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            let _ = request
                .reply
                .send(json!({"outcome":"selected", "optionId":"native-once"}));
            let _ = tokio::time::timeout(Duration::from_secs(2), task)
                .await
                .unwrap();
            if mode != "exit" {
                let values = wait_reply(root.path()).await;
                assert_eq!(values.len(), 1);
                assert_eq!(
                    values[0]["result"]["outcome"],
                    json!({"outcome":"cancelled"})
                );
            }
            client.shutdown().await.unwrap();
        }
    }
    #[tokio::test]
    async fn permission_dropping_last_client_has_no_retained_task_cycle() {
        let root = tempfile::tempdir().unwrap();
        let client = client(root.path(), "", Duration::from_secs(3)).await;
        client.new_session().await.unwrap();
        let (mut requests, guard) = client.permission_handler();
        let task = {
            let client = client.clone();
            tokio::spawn(async move { client.prompt("work", vec![]).await })
        };
        let request = tokio::time::timeout(Duration::from_secs(2), requests.recv())
            .await
            .unwrap()
            .unwrap();
        let weak = Arc::downgrade(&client.inner);
        task.abort();
        let _ = task.await;
        drop(guard);
        drop(client);
        drop(request);
        drop(requests);
        tokio::time::timeout(Duration::from_secs(2), async {
            while weak.upgrade().is_some() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }
    #[tokio::test]
    async fn permission_cancel_while_reply_write_is_blocked_cannot_deliver_a_stale_grant() {
        let root = tempfile::tempdir().unwrap();
        let client = client(root.path(), "", Duration::from_secs(3)).await;
        client.new_session().await.unwrap();
        let (mut requests, _guard) = client.permission_handler();
        let task = {
            let client = client.clone();
            tokio::spawn(async move { client.prompt("work", vec![]).await })
        };
        let request = tokio::time::timeout(Duration::from_secs(2), requests.recv())
            .await
            .unwrap()
            .unwrap();
        let stdin = client.inner.stdin.lock().await;
        let generation = client.inner.permissions.lock().unwrap().generation;
        request
            .reply
            .send(json!({"outcome":"selected", "optionId":"native-once"}))
            .unwrap();
        tokio::task::yield_now().await;
        let cancel = {
            let client = client.clone();
            tokio::spawn(async move { client.cancel().await })
        };
        tokio::time::timeout(Duration::from_secs(1), async {
            while client.inner.permissions.lock().unwrap().generation == generation {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        drop(stdin);
        cancel.await.unwrap().unwrap();
        task.await.unwrap().unwrap();
        let actual = wait_reply(root.path()).await;
        client.shutdown().await.unwrap();
        assert_eq!(actual.len(), 1);
        assert_eq!(
            actual[0]["result"]["outcome"],
            json!({"outcome":"cancelled"}),
            "cancellation must revalidate a decision after waiting for stdin"
        );
    }
    #[tokio::test]
    async fn abandoned_cancel_receipt_cannot_acknowledge_the_next_cancel() {
        for abort in [true, false] {
            let root = tempfile::tempdir().unwrap();
            let client = client(root.path(), "", Duration::from_millis(150)).await;
            client.new_session().await.unwrap();
            let stdin = client.inner.stdin.lock().await;
            let first = {
                let client = client.clone();
                tokio::spawn(async move { client.cancel().await })
            };
            tokio::time::timeout(Duration::from_secs(1), async {
                while client.inner.cancel_writes.lock().unwrap().is_empty() {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            if abort {
                first.abort();
                let _ = first.await;
            } else {
                assert!(
                    first
                        .await
                        .unwrap()
                        .unwrap_err()
                        .to_string()
                        .contains("timed out")
                );
            }
            let second = {
                let client = client.clone();
                tokio::spawn(async move { client.cancel().await })
            };
            tokio::time::timeout(Duration::from_secs(1), async {
                while client.inner.cancel_writes.lock().unwrap().len() != 2 {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            assert!(!second.is_finished());
            drop(stdin);
            second.await.unwrap().unwrap();
            assert!(client.inner.cancel_writes.lock().unwrap().is_empty());
            client.shutdown().await.unwrap();
        }
    }

    #[tokio::test]
    async fn permission_reply_write_timeout_invalidates_transport_without_replay() {
        let root = tempfile::tempdir().unwrap();
        let client = client(root.path(), "", Duration::from_millis(150)).await;
        client.new_session().await.unwrap();
        let (mut requests, _guard) = client.permission_handler();
        let task = {
            let client = client.clone();
            tokio::spawn(async move { client.prompt("work", vec![]).await })
        };
        let request = tokio::time::timeout(Duration::from_secs(2), requests.recv())
            .await
            .unwrap()
            .unwrap();
        let stdin = client.inner.stdin.lock().await;
        request
            .reply
            .send(json!({"outcome":"selected", "optionId":"native-once"}))
            .unwrap();
        tokio::time::timeout(Duration::from_secs(2), async {
            while !client.inner.closed.load(Ordering::SeqCst) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(client.inner.permissions.lock().unwrap().pending.is_empty());
        assert!(client.inner.permissions.lock().unwrap().handler.is_none());
        drop(stdin);
        assert!(task.await.unwrap().is_err());
        assert!(client.prompt("no replay", vec![]).await.is_err());
        assert!(
            replies(root.path()).is_empty(),
            "no stale grant after failed write"
        );
        client.shutdown().await.unwrap();
    }
}
