use anyhow::{Context, Result, ensure};
use axum::{
    Json, Router,
    extract::{
        Request, State, WebSocketUpgrade,
        ws::{Message, WebSocket},
    },
    response::{IntoResponse, Response},
    routing::{any, get},
};
use futures_util::StreamExt;
use serde_json::{Value, json};
use soulsteads_messaging::{config, connectors::http::Platform, runtime::Bridge};
use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    future::IntoFuture,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio::{
    sync::{Mutex, Notify, watch},
    task::JoinHandle,
    time::timeout,
};

const ROOM: &str = "ROOM";
const ROOT: &str = "ROOT";
const TELEGRAM_CHAT: &str = "-100";
const TELEGRAM_TOPIC: i64 = 77;
const HELD_TEXT: &str = "wire replay terminal";

#[derive(Clone, Debug)]
struct RecordedRequest {
    ordinal: usize,
    method: String,
    path: String,
    body: Value,
}

#[derive(Clone)]
struct WireReplayFixture {
    kind: &'static str,
    base: String,
    requests: Arc<Mutex<Vec<RecordedRequest>>>,
    socket_inputs: Arc<Mutex<Vec<Value>>>,
    telegram_updates: Arc<Mutex<VecDeque<Value>>>,
    socket_updates: Arc<Mutex<VecDeque<Value>>>,
    requests_changed: Arc<Notify>,
    socket_changed: Arc<Notify>,
    telegram_changed: Arc<Notify>,
    socket_ready: Arc<Notify>,
    socket_connected: Arc<AtomicBool>,
}

impl WireReplayFixture {
    fn new(kind: &'static str, base: String) -> Self {
        Self {
            kind,
            base,
            requests: Arc::default(),
            socket_inputs: Arc::default(),
            telegram_updates: Arc::default(),
            socket_updates: Arc::default(),
            requests_changed: Arc::new(Notify::new()),
            socket_changed: Arc::new(Notify::new()),
            telegram_changed: Arc::new(Notify::new()),
            socket_ready: Arc::new(Notify::new()),
            socket_connected: Arc::new(AtomicBool::new(false)),
        }
    }

    async fn record_socket(&self, value: Value) {
        self.socket_inputs.lock().await.push(value);
        self.socket_changed.notify_waiters();
    }

    async fn enqueue_telegram(&self, update: Value) {
        self.telegram_updates.lock().await.push_back(update);
        self.telegram_changed.notify_waiters();
    }

    async fn enqueue_socket(&self, event: Value) {
        self.socket_updates.lock().await.push_back(event);
        self.socket_changed.notify_waiters();
    }

    async fn next_telegram(&self) -> Vec<Value> {
        loop {
            let notified = self.telegram_changed.notified();
            if let Some(update) = self.telegram_updates.lock().await.pop_front() {
                return vec![update];
            }
            notified.await;
        }
    }

    async fn next_socket(&self) -> Value {
        loop {
            let notified = self.socket_changed.notified();
            if let Some(event) = self.socket_updates.lock().await.pop_front() {
                return event;
            }
            notified.await;
        }
    }

    async fn wait_for_socket(&self) -> Result<()> {
        let ready = timeout(Duration::from_secs(5), async {
            loop {
                let notified = self.socket_ready.notified();
                if self.socket_connected.load(Ordering::SeqCst) {
                    return;
                }
                notified.await;
            }
        })
        .await;
        if ready.is_err() {
            anyhow::bail!(
                "socket connection did not reach ingress readiness; HTTP replay: {:?}",
                self.requests().await
            );
        }
        Ok(())
    }

    async fn wait_for_request<F>(&self, name: &str, predicate: F) -> Result<RecordedRequest>
    where
        F: Fn(&RecordedRequest) -> bool,
    {
        let matched = timeout(Duration::from_secs(5), async {
            loop {
                let notified = self.requests_changed.notified();
                if let Some(request) = self
                    .requests
                    .lock()
                    .await
                    .iter()
                    .find(|request| predicate(request))
                    .cloned()
                {
                    return request;
                }
                notified.await;
            }
        })
        .await;
        match matched {
            Ok(request) => Ok(request),
            Err(_) => anyhow::bail!(
                "timed out waiting for {name}; HTTP replay: {:?}",
                self.requests().await
            ),
        }
    }

    async fn wait_for_socket_input<F>(&self, name: &str, predicate: F) -> Result<Value>
    where
        F: Fn(&Value) -> bool,
    {
        timeout(Duration::from_secs(5), async {
            loop {
                let notified = self.socket_changed.notified();
                if let Some(value) = self
                    .socket_inputs
                    .lock()
                    .await
                    .iter()
                    .find(|value| predicate(value))
                    .cloned()
                {
                    return value;
                }
                notified.await;
            }
        })
        .await
        .with_context(|| format!("timed out waiting for {name}"))
    }

    async fn requests(&self) -> Vec<RecordedRequest> {
        self.requests.lock().await.clone()
    }
}

async fn endpoint(State(fixture): State<WireReplayFixture>, request: Request) -> Json<Value> {
    let method = request.method().to_string();
    let path = request.uri().path().to_owned();
    let bytes = axum::body::to_bytes(request.into_body(), 1024 * 1024)
        .await
        .unwrap();
    let body = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
    {
        let mut requests = fixture.requests.lock().await;
        let ordinal = requests.len() + 1;
        requests.push(RecordedRequest {
            ordinal,
            method,
            path: path.clone(),
            body: body.clone(),
        });
    }
    fixture.requests_changed.notify_waiters();

    let response = if path.ends_with("getUpdates") {
        json!(fixture.next_telegram().await)
    } else if path.ends_with("getMe") {
        json!({"id": "BOT", "username": "bridge"})
    } else if path.ends_with("deleteWebhook") {
        json!(true)
    } else if path.ends_with("users/me") || path.ends_with("users/@me") {
        json!({"id": "BOT", "username": "bridge"})
    } else if path.ends_with("auth.test") {
        json!({"ok": true, "user_id": "BOT"})
    } else if path.ends_with("apps.connections.open") {
        json!({"ok": true, "url": fixture.base.replacen("http:", "ws:", 1) + "/socket"})
    } else if path.ends_with("gateway/bot") {
        json!({"url": fixture.base.replacen("http:", "ws:", 1) + "/socket"})
    } else if path.contains("chat.") {
        json!({"ok": true, "ts": "17"})
    } else {
        json!({"id": "17", "message_id": 17, "ok": true})
    };
    if fixture.kind == "telegram" && path.contains("/bot") {
        Json(json!({"ok": true, "result": response}))
    } else {
        Json(response)
    }
}

async fn upgrade(State(fixture): State<WireReplayFixture>, socket: WebSocketUpgrade) -> Response {
    socket
        .on_upgrade(move |socket| socket_conversation(socket, fixture))
        .into_response()
}

async fn socket_conversation(mut socket: WebSocket, fixture: WireReplayFixture) {
    let kind = fixture.kind;

    if kind == "discord" {
        socket
            .send(Message::Text(
                json!({"op": 10, "d": {"heartbeat_interval": 60_000}})
                    .to_string()
                    .into(),
            ))
            .await
            .unwrap();
        if let Some(Ok(message)) = socket.next().await
            && let Ok(text) = message.to_text()
            && let Ok(value) = serde_json::from_str(text)
        {
            fixture.record_socket(value).await;
        }
        socket
            .send(Message::Text(
                json!({"op": 0, "t": "READY", "s": 1, "d": {"session_id": "wire-replay"}})
                    .to_string()
                    .into(),
            ))
            .await
            .unwrap();
    } else if kind == "mattermost" {
        if let Some(Ok(message)) = socket.next().await
            && let Ok(text) = message.to_text()
            && let Ok(value) = serde_json::from_str(text)
        {
            fixture.record_socket(value).await;
        }
    }

    fixture.socket_connected.store(true, Ordering::SeqCst);
    fixture.socket_ready.notify_waiters();
    loop {
        tokio::select! {
            event = fixture.next_socket() => {
                if socket.send(Message::Text(event.to_string().into())).await.is_err() {
                    return;
                }
            }
            message = socket.next() => {
                let Some(Ok(message)) = message else { return; };
                if let Ok(text) = message.to_text()
                    && let Ok(value) = serde_json::from_str(text)
                {
                    fixture.record_socket(value).await;
                }
            }
        }
    }
}

struct ProtocolCase {
    kind: &'static str,
    wire: fn(&str, u64) -> Value,
    terminal_path: &'static str,
    activity_path: &'static str,
    typing_path: Option<&'static str>,
}

const PROTOCOLS: [ProtocolCase; 4] = [
    ProtocolCase {
        kind: "telegram",
        wire: telegram_wire,
        terminal_path: "sendMessage",
        activity_path: "sendMessage",
        typing_path: Some("sendChatAction"),
    },
    ProtocolCase {
        kind: "mattermost",
        wire: mattermost_wire,
        terminal_path: "/api/v4/posts",
        activity_path: "/api/v4/posts",
        typing_path: None,
    },
    ProtocolCase {
        kind: "slack",
        wire: slack_wire,
        terminal_path: "chat.postMessage",
        activity_path: "chat.postMessage",
        typing_path: None,
    },
    ProtocolCase {
        kind: "discord",
        wire: discord_wire,
        terminal_path: "/messages",
        activity_path: "/messages",
        typing_path: Some("/typing"),
    },
];

fn telegram_wire(command: &str, id: u64) -> Value {
    json!({
        "update_id": id,
        "message": {
            "message_id": id,
            "chat": {"id": -100, "type": "supergroup"},
            "from": {"id": "human"},
            "is_topic_message": true,
            "message_thread_id": TELEGRAM_TOPIC,
            "text": format!("!oc {command}"),
        }
    })
}

fn mattermost_wire(command: &str, id: u64) -> Value {
    json!({
        "event": "posted",
        "data": {
            "post": json!({
                "id": format!("mattermost-{id}"),
                "channel_id": ROOM,
                "user_id": "human",
                "root_id": ROOT,
                "message": format!("!oc {command}"),
            }).to_string(),
            "channel_type": "O",
        }
    })
}

fn slack_wire(command: &str, id: u64) -> Value {
    json!({
        "envelope_id": format!("slack-envelope-{id}"),
        "payload": {
            "team_id": "TEAM",
            "event": {
                "type": "app_mention",
                "channel": ROOM,
                "user": "human",
                "text": format!("<@BOT> {command}"),
                "ts": format!("{id}.1"),
                "thread_ts": ROOT,
            }
        }
    })
}

fn discord_wire(command: &str, id: u64) -> Value {
    json!({
        "op": 0,
        "t": "MESSAGE_CREATE",
        "s": id,
        "d": {
            "id": format!("discord-{id}"),
            "channel_id": ROOM,
            "guild_id": "GUILD",
            "author": {"id": "human", "bot": false},
            "content": format!("<@BOT> {command}"),
        }
    })
}

fn assert_protocol_closure() {
    let names = PROTOCOLS
        .iter()
        .map(|case| case.kind)
        .collect::<BTreeSet<_>>();
    assert_eq!(
        names,
        BTreeSet::from(["telegram", "mattermost", "slack", "discord"])
    );
    assert_eq!(
        PROTOCOLS.len(),
        names.len(),
        "each protocol must have one case"
    );
}

fn output_text<'a>(case: &ProtocolCase, request: &'a RecordedRequest) -> Option<&'a str> {
    match case.kind {
        "telegram" => request.body["text"].as_str(),
        "mattermost" => request.body["message"].as_str(),
        "slack" => request.body["text"].as_str(),
        "discord" => request.body["content"].as_str(),
        _ => None,
    }
}

fn is_output(case: &ProtocolCase, request: &RecordedRequest) -> bool {
    request.path.ends_with(case.terminal_path)
}

fn assert_routing(case: &ProtocolCase, request: &RecordedRequest, id: u64) -> Result<()> {
    ensure!(
        request.method == "POST",
        "{} output was not POST: {request:?}",
        case.kind
    );
    match case.kind {
        "telegram" => {
            ensure!(request.path.ends_with("sendMessage"), "{request:?}");
            ensure!(request.body["chat_id"] == TELEGRAM_CHAT, "{request:?}");
            ensure!(
                request.body["message_thread_id"] == TELEGRAM_TOPIC,
                "{request:?}"
            );
        }
        "mattermost" => {
            ensure!(request.path.ends_with("/api/v4/posts"), "{request:?}");
            ensure!(request.body["channel_id"] == ROOM, "{request:?}");
            ensure!(request.body["root_id"] == ROOT, "{request:?}");
        }
        "slack" => {
            ensure!(request.path.ends_with("chat.postMessage"), "{request:?}");
            ensure!(request.body["channel"] == ROOM, "{request:?}");
            ensure!(request.body["thread_ts"] == ROOT, "{request:?}");
        }
        "discord" => {
            ensure!(request.path.ends_with("/messages"), "{request:?}");
            ensure!(request.body["content"].is_string(), "{request:?}");
            ensure!(
                request.body["message_reference"]["message_id"] == format!("discord-{id}"),
                "{request:?}"
            );
        }
        _ => unreachable!(),
    }
    Ok(())
}

fn assert_activity_routing(case: &ProtocolCase, request: &RecordedRequest) -> Result<()> {
    ensure!(
        request.method == "POST",
        "{} activity was not POST: {request:?}",
        case.kind
    );
    match case.kind {
        "telegram" => {
            ensure!(request.path.ends_with("sendMessage"), "{request:?}");
            ensure!(request.body["chat_id"] == TELEGRAM_CHAT, "{request:?}");
            ensure!(
                request.body["message_thread_id"] == TELEGRAM_TOPIC,
                "{request:?}"
            );
        }
        "mattermost" => {
            ensure!(request.path.ends_with("/api/v4/posts"), "{request:?}");
            ensure!(
                request.body["channel_id"] == ROOM && request.body["root_id"] == ROOT,
                "{request:?}"
            );
        }
        "slack" => {
            ensure!(request.path.ends_with("chat.postMessage"), "{request:?}");
            ensure!(
                request.body["channel"] == ROOM && request.body["thread_ts"] == ROOT,
                "{request:?}"
            );
        }
        "discord" => {
            ensure!(request.path.ends_with("/messages"), "{request:?}");
            ensure!(request.body["content"].is_string(), "{request:?}");
            ensure!(
                request.body.get("message_reference").is_none(),
                "{request:?}"
            );
        }
        _ => unreachable!(),
    }
    Ok(())
}

fn rpc_audit(path: &Path, permit_absent: bool) -> Result<Vec<Value>> {
    let input = match std::fs::read_to_string(path) {
        Ok(input) => input,
        Err(error) if permit_absent && error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(vec![]);
        }
        Err(error) => {
            return Err(error).with_context(|| format!("read ACP audit {}", path.display()));
        }
    };
    input
        .lines()
        .enumerate()
        .map(|(index, line)| {
            serde_json::from_str(line)
                .with_context(|| format!("parse ACP audit {} line {}", path.display(), index + 1))
        })
        .collect()
}

async fn wait_for_file(path: &Path, name: &str) -> Result<()> {
    timeout(Duration::from_secs(5), async {
        while !path.is_file() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .with_context(|| format!("timed out waiting for {name}"))
}

struct RunningReplay {
    fixture: WireReplayFixture,
    bridge: Arc<Bridge>,
    shutdown: watch::Sender<bool>,
    platform: JoinHandle<anyhow::Result<()>>,
    server: JoinHandle<()>,
    release_file: Option<PathBuf>,
    cleaned: bool,
}

impl RunningReplay {
    async fn start(case: &ProtocolCase, held: bool) -> Result<(Self, tempfile::TempDir)> {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let base = format!("http://{}", listener.local_addr()?);
        let fixture = WireReplayFixture::new(case.kind, base.clone());
        let root = tempfile::tempdir()?;
        let audit = root.path().join("rpc-audit.jsonl");
        let started = root.path().join("prompt-started");
        let release = root.path().join("prompt-release");
        let mut acp_env = BTreeMap::from([(
            "ACP_FIXTURE_RPC_AUDIT".to_owned(),
            audit.to_string_lossy().to_string(),
        )]);
        if held {
            let script = root.path().join("held-script.json");
            std::fs::write(
                &script,
                json!([{
                    "updates": [
                        {"update": {"sessionUpdate": "tool_call", "toolCallId": "wire-read", "title": "read", "status": "in_progress", "rawInput": {"filePath": "wire.md"}}},
                        {"update": {"sessionUpdate": "agent_message_chunk", "content": {"type": "text", "text": HELD_TEXT}}},
                    ],
                    "startedFile": started,
                    "releaseFile": release,
                    "stopReason": "end_turn",
                }])
                .to_string(),
            )?;
            acp_env.insert(
                "ACP_FIXTURE_MATRIX_SCRIPT".to_owned(),
                script.to_string_lossy().to_string(),
            );
            acp_env.insert(
                "ACP_FIXTURE_MATRIX_COUNTER".to_owned(),
                root.path()
                    .join("script-counter")
                    .to_string_lossy()
                    .to_string(),
            );
        }
        let settings = json!({
            "rateLimitSeconds": 0,
            "sessionStorePath": root.path().join("store.json"),
            "mattermost": {"allowedChannels": [ROOM]},
            "acp": {
                "command": env!("CARGO_BIN_EXE_acp-fixture"),
                "args": [],
                "env": acp_env,
            },
        });
        let configuration = config::load(&settings.to_string(), &BTreeMap::new())?;
        let bridge = Arc::new(Bridge::new(
            configuration,
            BTreeMap::from([("SLACK_APP_TOKEN".to_owned(), "wire-app-token".to_owned())]),
            root.path().join("sessions"),
        )?);
        let server_fixture = fixture.clone();
        let server = tokio::spawn(async move {
            let _ = axum::serve(
                listener,
                Router::new()
                    .route("/socket", get(upgrade))
                    .route("/api/v4/websocket", get(upgrade))
                    .fallback(any(endpoint))
                    .with_state(server_fixture),
            )
            .into_future()
            .await;
        });
        let (shutdown, receiver) = watch::channel(false);
        let runtime_bridge = bridge.clone();
        let kind = case.kind;
        let platform = tokio::spawn(async move {
            Platform::new(kind, &base, "wire-token", "", "")
                .run(runtime_bridge, receiver)
                .await
        });
        let running = Self {
            fixture,
            bridge,
            shutdown,
            platform,
            server,
            release_file: held.then_some(release),
            cleaned: false,
        };
        running.wait_ready(case).await?;
        Ok((running, root))
    }

    async fn wait_ready(&self, case: &ProtocolCase) -> Result<()> {
        if case.kind == "telegram" {
            self.fixture
                .wait_for_request("Telegram getUpdates", |request| {
                    request.path.ends_with("getUpdates")
                })
                .await?;
        } else {
            self.fixture.wait_for_socket().await?;
        }
        Ok(())
    }

    async fn dispatch(&self, case: &ProtocolCase, command: &str, id: u64) {
        let wire = (case.wire)(command, id);
        if case.kind == "telegram" {
            self.fixture.enqueue_telegram(wire).await;
        } else {
            self.fixture.enqueue_socket(wire).await;
        }
    }

    async fn cleanup(&mut self) -> Result<Vec<RecordedRequest>> {
        if let Some(release) = self.release_file.take() {
            let _ = std::fs::write(release, "cleanup release");
        }
        let _ = self.shutdown.send(true);
        let platform = match timeout(Duration::from_secs(3), &mut self.platform).await {
            Ok(result) => result
                .context("platform task panicked")
                .and_then(|result| result),
            Err(_) => {
                self.platform.abort();
                Err(anyhow::anyhow!("platform task did not stop"))
            }
        };
        let bridge = self.bridge.shutdown().await;
        let requests = self.fixture.requests().await;
        self.server.abort();
        self.cleaned = true;
        platform.and(bridge).map(|_| requests)
    }
}

impl Drop for RunningReplay {
    fn drop(&mut self) {
        if self.cleaned {
            return;
        }
        if let Some(release) = self.release_file.take() {
            let _ = std::fs::write(release, "drop release");
        }
        let _ = self.shutdown.send(true);
        self.platform.abort();
        self.server.abort();
        let bridge = self.bridge.clone();
        tokio::spawn(async move {
            let _ = bridge.shutdown().await;
        });
    }
}

async fn common_local_commands(case: &ProtocolCase) -> Result<()> {
    let (mut running, root) = RunningReplay::start(case, false).await?;
    let result = async {
        for (id, command, expected) in [
            (1, "/help", "Bridge commands:"),
            (2, "/h", "Bridge commands:"),
            (3, "/status", "No active session."),
            (4, "/clear", "No active session."),
            (5, "/reset", "No active session."),
        ] {
            let prior = running
                .fixture
                .requests()
                .await
                .last()
                .map(|request| request.ordinal)
                .unwrap_or(0);
            running.dispatch(case, command, id).await;
            let response = running
                .fixture
                .wait_for_request(command, |request| {
                    request.ordinal > prior
                        && is_output(case, request)
                        && output_text(case, request).is_some_and(|text| text.contains(expected))
                })
                .await?;
            assert_routing(case, &response, id)?;
        }

        if case.kind == "slack" {
            for id in 1..=5 {
                running
                    .fixture
                    .wait_for_socket_input("Slack envelope acknowledgement", |value| {
                        value["envelope_id"] == format!("slack-envelope-{id}")
                    })
                    .await?;
            }
        }
        match case.kind {
            "mattermost" => {
                let auth = running
                    .fixture
                    .wait_for_socket_input("Mattermost authentication challenge", |value| {
                        value["action"] == "authentication_challenge"
                    })
                    .await?;
                ensure!(auth["data"]["token"] == "wire-token", "{auth:?}");
            }
            "discord" => {
                let identify = running
                    .fixture
                    .wait_for_socket_input("Discord IDENTIFY", |value| value["op"] == 2)
                    .await?;
                ensure!(identify["d"]["token"] == "wire-token", "{identify:?}");
            }
            _ => {}
        }
        Ok(())
    }
    .await;
    let requests = running.cleanup().await;
    result?;
    let requests = requests?;
    ensure!(
        requests
            .iter()
            .filter(|request| is_output(case, request))
            .count()
            == 5,
        "local commands produced an unexpected outbound response count for {}: {requests:?}",
        case.kind
    );
    let methods = rpc_audit(&root.path().join("rpc-audit.jsonl"), true)?;
    ensure!(
        !methods
            .iter()
            .any(|request| request["method"] == "session/prompt"),
        "local commands unexpectedly generated an ACP prompt: {methods:?}"
    );
    Ok(())
}

async fn held_tool_activity(case: &ProtocolCase) -> Result<()> {
    let (mut running, root) = RunningReplay::start(case, true).await?;
    let result = async {
        let id = 99;
        let prior = running
            .fixture
            .requests()
            .await
            .last()
            .map(|request| request.ordinal)
            .unwrap_or(0);
        running.dispatch(case, "question", id).await;
        wait_for_file(&root.path().join("prompt-started"), "held ACP prompt start").await?;
        let activity = running
            .fixture
            .wait_for_request("real tool activity", |request| {
                request.ordinal > prior
                    && request.path.ends_with(case.activity_path)
                    && output_text(case, request) == Some("> [read]")
            })
            .await?;
        assert_activity_routing(case, &activity)?;
        ensure!(
            running
                .fixture
                .requests()
                .await
                .iter()
                .filter(|request| {
                    request.ordinal >= activity.ordinal
                        && request.path.ends_with(case.activity_path)
                        && output_text(case, request) == Some("> [read]")
                })
                .count()
                == 1,
            "expected one activity before release for {}",
            case.kind
        );
        if let Some(typing_path) = case.typing_path {
            running
                .fixture
                .wait_for_request("platform typing", |request| {
                    request.ordinal > prior && request.path.ends_with(typing_path)
                })
                .await?;
        }
        let visible_before_release = running
            .fixture
            .requests()
            .await
            .into_iter()
            .filter(|request| is_output(case, request))
            .filter(|request| {
                output_text(case, request).is_some_and(|text| text.contains(HELD_TEXT))
            })
            .count();
        ensure!(
            visible_before_release == 0,
            "terminal text was sent before release for {}",
            case.kind
        );
        std::fs::write(root.path().join("prompt-release"), "release")?;
        let terminal = running
            .fixture
            .wait_for_request("terminal response", |request| {
                request.ordinal > activity.ordinal
                    && is_output(case, request)
                    && output_text(case, request).is_some_and(|text| text.contains(HELD_TEXT))
            })
            .await?;
        assert_routing(case, &terminal, id)?;
        Ok(())
    }
    .await;
    let requests = running.cleanup().await;
    result?;
    let requests = requests?;
    let terminal_count = requests
        .iter()
        .filter(|request| is_output(case, request))
        .filter(|request| output_text(case, request).is_some_and(|text| text.contains(HELD_TEXT)))
        .count();
    ensure!(
        terminal_count == 1,
        "duplicate terminal response after {} quiesced: {requests:?}",
        case.kind
    );
    ensure!(
        requests
            .iter()
            .filter(|request| {
                request.path.ends_with(case.activity_path)
                    && output_text(case, request) == Some("> [read]")
            })
            .count()
            == 1,
        "duplicate tool activity after {} quiesced: {requests:?}",
        case.kind
    );
    let prompts = rpc_audit(&root.path().join("rpc-audit.jsonl"), false)?
        .into_iter()
        .filter(|request| request["method"] == "session/prompt")
        .count();
    ensure!(
        prompts == 1,
        "expected one ACP prompt for {}, got {prompts}",
        case.kind
    );
    Ok(())
}

#[tokio::test]
async fn http_wire_replay_common_local_commands_use_real_ingress_and_outbound_api() {
    assert_protocol_closure();
    for case in &PROTOCOLS {
        common_local_commands(case).await.unwrap();
    }
}

#[tokio::test]
async fn http_wire_replay_holds_after_real_tool_activity_then_releases_terminal_response() {
    assert_protocol_closure();
    for case in &PROTOCOLS {
        held_tool_activity(case).await.unwrap();
    }
}

#[tokio::test]
async fn replay_cleanup_propagates_platform_task_failure() {
    let (mut running, _root) = RunningReplay::start(&PROTOCOLS[0], false).await.unwrap();
    running.platform.abort();
    let _ = (&mut running.platform).await;
    running.platform = tokio::spawn(async { anyhow::bail!("fixture platform failed") });

    let error = running.cleanup().await.unwrap_err();
    assert!(
        error.to_string().contains("fixture platform failed"),
        "{error:#}"
    );
}

#[tokio::test]
async fn replay_cleanup_stops_server_after_platform_task_failure() {
    let (mut running, _root) = RunningReplay::start(&PROTOCOLS[0], false).await.unwrap();
    running.platform.abort();

    assert!(running.cleanup().await.is_err());
    timeout(Duration::from_secs(1), async {
        while !running.server.is_finished() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("cleanup must stop the replay server after platform failure");
}
