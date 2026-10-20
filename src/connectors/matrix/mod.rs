use super::matrix_indicators::{TypingClient, TypingGuard, TypingManager};
use crate::{
    config::{boolean, string},
    runtime::{Bridge, Event, Sink},
};
use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use matrix_sdk::{
    Client, Room, RoomState, SessionMeta, SessionTokens,
    authentication::matrix::MatrixSession,
    config::SyncSettings,
    ruma::{
        OwnedEventId,
        api::{
            IncomingResponseExt, OutgoingRequestExt, auth_scheme::SendAccessToken,
            client::account::whoami, error::FromHttpResponseError,
        },
        events::room::message::{OriginalSyncRoomMessageEvent, Relation, RoomMessageEventContent},
    },
    store::RoomLoadSettings,
};
use serde_json::{Value, json};
use std::{
    collections::HashMap,
    hash::{Hash, Hasher},
    path::Path,
    sync::Arc,
    time::Duration,
};

fn normalize_matrix_message(
    raw: matrix_sdk::ruma::serde::Raw<OriginalSyncRoomMessageEvent>,
) -> Result<OriginalSyncRoomMessageEvent> {
    let mut event: Value = serde_json::from_str(raw.json().get())?;
    if let Some(user_ids) = event
        .get_mut("content")
        .and_then(|content| content.get_mut("m.mentions"))
        .and_then(|mentions| mentions.get_mut("user_ids"))
        .and_then(Value::as_array_mut)
    {
        user_ids.retain(|id| {
            id.as_str().is_some_and(|id| {
                <matrix_sdk::ruma::OwnedUserId as std::str::FromStr>::from_str(id).is_ok()
            })
        });
    }
    serde_json::from_value(event).context("invalid Matrix room message")
}

struct NativeTyping(Client);
#[async_trait]
impl TypingClient for NativeTyping {
    async fn set_typing(&self, room: &str, typing: bool, timeout_ms: u64) -> Result<()> {
        use matrix_sdk::ruma::api::client::typing::create_typing_event::{
            self,
            v3::{Typing, TypingInfo},
        };
        let request = create_typing_event::v3::Request::new(
            self.0
                .user_id()
                .context("Matrix user identity missing")?
                .to_owned(),
            room.try_into()?,
            if typing {
                Typing::Yes(TypingInfo::new(Duration::from_millis(timeout_ms)))
            } else {
                Typing::No
            },
        );
        self.0.send(request).await?;
        Ok(())
    }
}

async fn token_identity(client: &Client, token: &str) -> Result<Option<whoami::v3::Response>> {
    let versions = tokio::time::timeout(Duration::from_secs(10), client.supported_versions())
        .await
        .map_err(|_| anyhow::anyhow!("Matrix server version discovery timed out"))?
        .map_err(|_| anyhow::anyhow!("Matrix server version discovery failed"))?;
    let request = whoami::v3::Request::new()
        .try_into_http_request::<Vec<u8>>(
            client.homeserver().as_str(),
            SendAccessToken::Always(token),
            std::borrow::Cow::Borrowed(&versions),
        )
        .map_err(|_| anyhow::anyhow!("Matrix token identity request could not be constructed"))?;
    let http = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(Duration::from_secs(10))
        .build()?;
    let request = reqwest::Request::try_from(request)
        .map_err(|_| anyhow::anyhow!("Matrix token identity request is invalid"))?;
    let mut response = http.execute(request).await.map_err(|_| {
        anyhow::anyhow!("Matrix token identity lookup failed at the HTTP transport")
    })?;
    let status = response.status();
    if status.is_redirection() {
        bail!("Matrix token identity lookup refused a redirect");
    }
    let mut body = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|_| anyhow::anyhow!("Matrix token identity response could not be read"))?
    {
        if body.len().saturating_add(chunk.len()) > 64 * 1024 {
            bail!("Matrix token identity response exceeds the size limit");
        }
        body.extend_from_slice(&chunk);
    }
    let response = axum::http::Response::builder()
        .status(status)
        .body(body.as_slice())?;
    match whoami::v3::Response::try_from_http_response(response) {
        Ok(identity) => Ok(Some(identity)),
        Err(FromHttpResponseError::Server(error)) => {
            let invalid = error.error_kind().is_some_and(|kind| {
                matches!(
                    kind.errcode().as_str(),
                    "M_UNKNOWN_TOKEN" | "M_MISSING_TOKEN"
                )
            });
            if invalid {
                Ok(None)
            } else {
                bail!(
                    "Matrix token identity lookup failed (HTTP {})",
                    status.as_u16()
                );
            }
        }
        Err(_) => bail!("Matrix token identity response is invalid"),
    }
}

pub async fn connect(config: &Value, storage: &Path) -> Result<Client> {
    #[cfg(not(feature = "matrix-encryption"))]
    if config
        .pointer("/encryption/enabled")
        .and_then(Value::as_bool)
        .unwrap_or(false)
    {
        bail!("Matrix encryption requires a build with the matrix-encryption feature");
    }
    std::fs::create_dir_all(storage)?;
    let client = Client::builder()
        .homeserver_url(string(config, "homeserver"))
        .sqlite_store(storage, None)
        .build()
        .await?;
    let token_file = storage.join("access_token");
    let explicit = string(config, "accessToken");
    let saved = if explicit.is_empty() {
        match std::fs::read_to_string(&token_file) {
            Ok(token) => token,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => String::new(),
            Err(error) => return Err(error.into()),
        }
    } else {
        String::new()
    };
    let token = if explicit.is_empty() {
        saved.as_str()
    } else {
        explicit
    };
    if !token.is_empty() {
        let meta = match token_identity(&client, token).await? {
            Some(identity)
                if !explicit.is_empty()
                    || string(config, "userId").is_empty()
                    || identity.user_id.as_str() == string(config, "userId") =>
            {
                let device_id = identity
                    .device_id
                    .filter(|device| !device.as_str().is_empty())
                    .context("Matrix token identity response does not identify a device")?;
                Some(SessionMeta {
                    user_id: identity.user_id,
                    device_id,
                })
            }
            _ if !explicit.is_empty() => {
                bail!("Matrix explicit token identity could not be resolved");
            }
            _ => None,
        };
        if let Some(meta) = meta {
            client
                .matrix_auth()
                .restore_session(
                    MatrixSession {
                        meta,
                        tokens: SessionTokens {
                            access_token: token.into(),
                            refresh_token: None,
                        },
                    },
                    RoomLoadSettings::default(),
                )
                .await?;
            return Ok(client);
        }
    }
    if string(config, "password").is_empty() {
        bail!("Matrix authentication requires an access token or password");
    }
    let authentication = client.matrix_auth();
    let mut login =
        authentication.login_username(string(config, "userId"), string(config, "password"));
    if !string(config, "deviceId").is_empty() {
        login = login.device_id(string(config, "deviceId"));
    }
    login.send().await?;
    let token = client
        .access_token()
        .context("Matrix login did not return a token")?;
    crate::store::write_private(&token_file, token.as_bytes())?;
    Ok(client)
}

struct MatrixReplyContext {
    events: tokio::sync::Mutex<HashMap<String, OwnedEventId>>,
    sends: [tokio::sync::Mutex<()>; 64],
}
impl Default for MatrixReplyContext {
    fn default() -> Self {
        Self {
            events: Default::default(),
            sends: std::array::from_fn(|_| tokio::sync::Mutex::new(())),
        }
    }
}
impl MatrixReplyContext {
    fn send_lock(&self, key: &str) -> &tokio::sync::Mutex<()> {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        key.hash(&mut hasher);
        &self.sends[hasher.finish() as usize % self.sends.len()]
    }
}
impl std::ops::Deref for MatrixReplyContext {
    type Target = tokio::sync::Mutex<HashMap<String, OwnedEventId>>;
    fn deref(&self) -> &Self::Target {
        &self.events
    }
}
#[derive(Default)]
struct MatrixActivities {
    ids: HashMap<String, OwnedEventId>,
    destinations: HashMap<String, Option<OwnedEventId>>,
    terminal_calls: std::collections::HashSet<String>,
    terminal_destinations: HashMap<String, Option<OwnedEventId>>,
    terminal_root: Option<OwnedEventId>,
    terminal_unavailable: bool,
    terminal_failures: std::collections::HashSet<String>,
    next_anonymous: usize,
    last_anonymous: Option<String>,
}
impl MatrixActivities {
    fn correlation(&mut self, event: &Value) -> String {
        if let Some(id) = event["activityId"].as_str().filter(|id| !id.is_empty()) {
            return id.to_owned();
        }
        if event["type"] == "activity_update"
            && let Some(id) = &self.last_anonymous
        {
            return id.clone();
        }
        let id = loop {
            let id = format!("activity:{}", self.next_anonymous);
            self.next_anonymous += 1;
            if !self.ids.contains_key(&id) {
                break id;
            }
        };
        self.last_anonymous = Some(id.clone());
        id
    }
}
pub struct MatrixSink {
    pub room: Room,
    pub thread: Option<OwnedEventId>,
    raw_thread: Option<OwnedEventId>,
    pub html: bool,
    rich_tools: bool,
    activities: Arc<tokio::sync::Mutex<MatrixActivities>>,
    indicators: TypingManager,
    typing: Arc<tokio::sync::Mutex<Option<TypingGuard>>>,
    incoming: OwnedEventId,
    reply_context: Arc<MatrixReplyContext>,
}
fn raw_permission_thread(content: &RoomMessageEventContent) -> Option<OwnedEventId> {
    match &content.relates_to {
        Some(Relation::Thread(thread)) => Some(thread.event_id.clone()),
        _ => None,
    }
}
#[cfg(not(feature = "matrix-encryption"))]
async fn ensure_plaintext_room(room: &Room) -> Result<()> {
    // Fresh server state also covers a transition not yet observed by sync.
    let request = matrix_sdk::ruma::api::client::state::get_state_event_for_key::v3::Request::new(
        room.room_id().to_owned(),
        matrix_sdk::ruma::events::StateEventType::RoomEncryption,
        String::new(),
    );
    let absent = matches!(
        tokio::time::timeout(Duration::from_secs(5), room.client().send(request)).await,
        Ok(Err(error)) if error.client_api_error_kind() == Some(&matrix_sdk::ruma::api::error::ErrorKind::NotFound)
    );
    if !absent || room.encryption_state().is_encrypted() {
        bail!("plaintext Matrix operation refused: room is encrypted or state is unavailable");
    }
    Ok(())
}
impl MatrixSink {
    fn reply_key(&self, root: &OwnedEventId) -> String {
        format!("{}:{root}", self.room.room_id())
    }
    async fn send_message(&self, content: RoomMessageEventContent) -> Result<OwnedEventId> {
        self.send_message_to(content, self.thread.as_ref()).await
    }
    async fn send_message_to(
        &self,
        mut content: RoomMessageEventContent,
        thread: Option<&OwnedEventId>,
    ) -> Result<OwnedEventId> {
        let _send = if let Some(root) = thread {
            Some(
                self.reply_context
                    .send_lock(&self.reply_key(root))
                    .lock()
                    .await,
            )
        } else {
            None
        };
        if let Some(root) = thread {
            let last = self
                .reply_context
                .lock()
                .await
                .get(&self.reply_key(root))
                .cloned()
                .unwrap_or_else(|| root.clone());
            content.relates_to = Some(Relation::Thread(
                matrix_sdk::ruma::events::relation::Thread::plain(root.clone(), last),
            ));
        }
        #[cfg(not(feature = "matrix-encryption"))]
        ensure_plaintext_room(&self.room).await?;
        let id = self.room.send(content).await?.response.event_id;
        if let Some(root) = thread {
            self.reply_context
                .lock()
                .await
                .insert(self.reply_key(root), id.clone());
        }
        Ok(id)
    }
    async fn terminal_destination(
        &self,
        event: &Value,
        activities: &mut MatrixActivities,
    ) -> Option<OwnedEventId> {
        let Some(call) = event["terminalToolCallId"]
            .as_str()
            .filter(|id| !id.is_empty())
        else {
            return self.thread.clone();
        };
        let mode = event["terminalThread"]["mode"].as_str().unwrap_or("off");
        if !matches!(mode, "always" | "auto") {
            return self.thread.clone();
        }
        if let Some(destination) = activities.terminal_destinations.get(call) {
            return destination.clone();
        }
        if activities.terminal_calls.len() < 1024 {
            activities.terminal_calls.insert(call.to_owned());
        }
        let threshold = event["terminalThread"]["threshold"]
            .as_u64()
            .filter(|n| *n > 0)
            .unwrap_or(3);
        let eligible = mode == "always" || activities.terminal_calls.len() as u64 >= threshold;
        if eligible && activities.terminal_root.is_none() && !activities.terminal_unavailable {
            let source = format!(
                "https://matrix.to/#/{}/{}",
                self.room.room_id(),
                self.incoming
            );
            match self
                .send_message_to(
                    RoomMessageEventContent::notice_plain(format!(
                        "Terminal activity for {source}"
                    )),
                    None,
                )
                .await
            {
                Ok(root) => {
                    if self.thread.is_some() {
                        let link = format!(
                            "Terminal activity: https://matrix.to/#/{}/{}",
                            self.room.room_id(),
                            root
                        );
                        if self
                            .send_message(RoomMessageEventContent::notice_plain(link))
                            .await
                            .is_err()
                        {
                            // The root may exist, but it is not reachable from the
                            // original thread. Keep output there and don't replay
                            // either publication with a new transaction.
                            activities.terminal_unavailable = true;
                        } else {
                            activities.terminal_root = Some(root);
                        }
                    } else {
                        activities.terminal_root = Some(root);
                    }
                }
                Err(_) => {
                    activities.terminal_unavailable = true;
                }
            }
        }
        let destination = if eligible {
            activities
                .terminal_root
                .clone()
                .or_else(|| self.thread.clone())
        } else {
            self.thread.clone()
        };
        if activities.terminal_destinations.len() < 1024 {
            activities
                .terminal_destinations
                .insert(call.to_owned(), destination.clone());
        }
        destination
    }
}
#[async_trait]
impl Sink for MatrixSink {
    fn matrix_raw_thread(&self) -> Option<String> {
        Some(
            self.raw_thread
                .as_ref()
                .map(ToString::to_string)
                .unwrap_or_default(),
        )
    }
    fn matrix_user_id(&self) -> Option<String> {
        self.room.client().user_id().map(ToString::to_string)
    }
    fn owned(&self) -> Option<Arc<dyn Sink>> {
        Some(Arc::new(Self {
            room: self.room.clone(),
            thread: self.thread.clone(),
            raw_thread: self.raw_thread.clone(),
            html: self.html,
            rich_tools: self.rich_tools,
            activities: self.activities.clone(),
            indicators: self.indicators.clone(),
            typing: self.typing.clone(),
            incoming: self.incoming.clone(),
            reply_context: self.reply_context.clone(),
        }))
    }
    async fn upload(&self, name: &str, mime: &str, bytes: Vec<u8>) -> Result<()> {
        let mime = mime.parse()?;
        let _send = if let Some(root) = &self.thread {
            Some(
                self.reply_context
                    .send_lock(&self.reply_key(root))
                    .lock()
                    .await,
            )
        } else {
            None
        };
        let mut config = matrix_sdk::attachment::AttachmentConfig::new();
        if let Some(root) = &self.thread {
            let last = self
                .reply_context
                .lock()
                .await
                .get(&self.reply_key(root))
                .cloned()
                .unwrap_or_else(|| root.clone());
            config = config.extra_content(Some(json!({"m.relates_to":{"rel_type":"m.thread","event_id":root,"is_falling_back":true,"m.in_reply_to":{"event_id":last}}}).as_object().unwrap().clone()));
        }
        #[cfg(not(feature = "matrix-encryption"))]
        ensure_plaintext_room(&self.room).await?;
        let response = self
            .room
            .send_attachment(name, &mime, bytes, config)
            .await?;
        if let Some(root) = &self.thread {
            self.reply_context
                .lock()
                .await
                .insert(self.reply_key(root), response.event_id);
        }
        Ok(())
    }
    async fn emit(&self, event: Value) -> Result<()> {
        let kind = event["type"].as_str().unwrap_or("");
        if kind == "terminal_call" {
            if let Some(id) = event["toolCallId"].as_str().filter(|id| !id.is_empty()) {
                let mut activities = self.activities.lock().await;
                if activities.terminal_calls.len() < 1024 {
                    activities.terminal_calls.insert(id.to_owned());
                }
            }
            return Ok(());
        }
        if kind == "terminal_failure" {
            let Some(call) = event["terminalToolCallId"].as_str() else {
                return Ok(());
            };
            let mut activities = self.activities.lock().await;
            if let Some(Some(root)) = activities.terminal_destinations.get(call).cloned()
                && Some(&root) != self.thread.as_ref()
                && activities.terminal_failures.len() < 1024
                && activities.terminal_failures.insert(call.to_owned())
            {
                let text = format!(
                    "A terminal command failed. Details: https://matrix.to/#/{}/{}",
                    self.room.room_id(),
                    root
                );
                self.send_message(RoomMessageEventContent::notice_plain(text))
                    .await?;
            }
            return Ok(());
        }
        if kind == "read" {
            if let Some(id) = event["eventId"].as_str() {
                let _ = self.room.send_single_receipt(
                    matrix_sdk::ruma::api::client::receipt::create_receipt::v3::ReceiptType::Read,
                    matrix_sdk::ruma::events::receipt::ReceiptThread::Unthreaded,
                    id.try_into()?,
                ).await;
            }
            return Ok(());
        }
        if kind == "thinking" {
            if let Some(root) = &self.thread {
                let _send = self
                    .reply_context
                    .send_lock(&self.reply_key(root))
                    .lock()
                    .await;
                self.reply_context
                    .lock()
                    .await
                    .insert(self.reply_key(root), self.incoming.clone());
            }
            let mut typing = self.typing.lock().await;
            if typing.is_none() {
                *typing = Some(self.indicators.start(self.room.room_id().as_str()).await);
            }
        }
        if kind == "done"
            && let Some(typing) = self.typing.lock().await.take()
        {
            typing.stop().await;
        }
        if matches!(kind, "activity" | "activity_update") {
            let text = event["message"].as_str().unwrap_or("");
            if text.is_empty() {
                return Ok(());
            }
            let mut activities = self.activities.lock().await;
            let key = activities.correlation(&event);
            let destination = if let Some(destination) = activities.destinations.get(&key) {
                destination.clone()
            } else {
                self.terminal_destination(&event, &mut activities).await
            };
            let mut notice = crate::tools::matrix_notice(text, self.rich_tools);
            if event["terminalToolCallId"].is_string()
                && let Some(root) = &destination
            {
                notice["m.relates_to"] = json!({"rel_type":"m.thread","event_id":root,"is_falling_back":true,"m.in_reply_to":{"event_id":root}});
            }
            if let Some(id) = activities.ids.get(&key) {
                let mut edit = notice.clone();
                edit["body"] = format!("* {}", notice["body"].as_str().unwrap_or("")).into();
                edit["m.new_content"] = notice.clone();
                edit["m.relates_to"] = json!({"rel_type":"m.replace","event_id":id});
                #[cfg(not(feature = "matrix-encryption"))]
                ensure_plaintext_room(&self.room).await?;
                if self.room.send_raw("m.room.message", edit).await.is_ok() {
                    return Ok(());
                }
            }
            let content: RoomMessageEventContent = serde_json::from_value(notice)?;
            let id = self.send_message_to(content, destination.as_ref()).await?;
            activities.destinations.insert(key.clone(), destination);
            activities.ids.insert(key, id);
            return Ok(());
        }
        if matches!(kind, "response" | "error" | "permission_denied" | "notice") {
            let text = event["text"]
                .as_str()
                .or(event["message"].as_str())
                .unwrap_or("");
            if !text.is_empty() {
                let content = if kind == "notice" {
                    RoomMessageEventContent::notice_plain(text)
                } else if self.html {
                    RoomMessageEventContent::text_html(text, crate::tools::markdown(text))
                } else {
                    RoomMessageEventContent::text_plain(text)
                };
                if event["terminalToolCallId"].is_string() {
                    let mut activities = self.activities.lock().await;
                    let destination = self.terminal_destination(&event, &mut activities).await;
                    self.send_message_to(content, destination.as_ref()).await?;
                } else {
                    self.send_message(content).await?;
                }
            }
        }
        Ok(())
    }
}

pub async fn run(
    client: Client,
    bridge: Arc<Bridge>,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) -> Result<()> {
    let bot = client
        .user_id()
        .context("Matrix user identity missing")?
        .to_string();
    let policy = bridge.config["matrix"].clone();
    // A new SDK store has no compatible legacy transport cursor. Populate
    // room state before registering handlers; historical timeline contents
    // must not become new coding work or replay settled commands at cutover.
    let cursor = client
        .state_store()
        .get_kv_data(matrix_sdk::store::StateStoreDataKey::SyncToken)
        .await?
        .and_then(|value| value.into_sync_token());
    if cursor.is_none() {
        let response = tokio::select! {
            biased;
            _ = shutdown.changed() => return Ok(()),
            result = client.sync_once(SyncSettings::default()) => result?,
        };
        let mut joined = false;
        if boolean(&policy, "autoJoin") {
            for room_id in response.rooms.invited.keys() {
                if crate::core::room_allowed(&policy, room_id.as_str()) {
                    tokio::select! {
                        biased;
                        _ = shutdown.changed() => return Ok(()),
                        result = client.join_room_by_id(room_id) => { result?; }
                    }
                    joined = true;
                }
            }
        }
        if joined {
            // Joining adds another initial room timeline. Bootstrap it too,
            // before announcing readiness and admitting live work.
            tokio::select! {
                biased;
                _ = shutdown.changed() => return Ok(()),
                result = client.sync_once(SyncSettings::default()) => { result?; }
            }
        }
    }
    let permits = Arc::new(tokio::sync::Semaphore::new(32));
    let indicators = TypingManager::new(
        Arc::new(NativeTyping(client.clone())),
        Duration::from_secs(4),
    );
    let reply_context = Arc::new(MatrixReplyContext::default());
    let queries = Arc::new(tokio::sync::Mutex::new(tokio::task::JoinSet::new()));
    let accepting = Arc::new(std::sync::atomic::AtomicBool::new(true));
    let incoming_queries = queries.clone();
    let incoming_accepting = accepting.clone();
    let handler = client.add_event_handler(
        move |raw: matrix_sdk::ruma::serde::Raw<OriginalSyncRoomMessageEvent>, room: Room| {
            let bridge = bridge.clone();
            let bot = bot.clone();
            let permits = permits.clone();
            let indicators = indicators.clone();
            let reply_context = reply_context.clone();
            let queries = incoming_queries.clone();
            let accepting = incoming_accepting.clone();
            async move {
                let Ok(message) = normalize_matrix_message(raw) else {
                    return;
                };
                if !accepting.load(std::sync::atomic::Ordering::Acquire) {
                    return;
                }
                if room.state() != RoomState::Joined || message.sender.as_str() == bot {
                    return;
                }
                let file_content = match &message.content.msgtype {
                    matrix_sdk::ruma::events::room::message::MessageType::Text(_) => None,
                    matrix_sdk::ruma::events::room::message::MessageType::File(_) => {
                        let Ok(content) = serde_json::to_value(&message.content) else {
                            return;
                        };
                        Some(content)
                    }
                    _ => return,
                };
                let Ok(_permit) = permits.try_acquire_owned() else {
                    return;
                };
                let body = message.content.body().to_owned();
                let mentioned = message.content.mentions.as_ref().is_some_and(|mentions| {
                    mentions.user_ids.iter().any(|user| user.as_str() == bot)
                });
                let thread = match &message.content.relates_to {
                    Some(Relation::Thread(thread)) => Some(thread.event_id.clone()),
                    _ => Some(message.event_id.clone()),
                };
                let raw_thread = raw_permission_thread(&message.content);
                let sink = MatrixSink {
                    room: room.clone(),
                    raw_thread,
                    thread: if boolean(&bridge.config["matrix"], "threadIsolation") {
                        thread.clone()
                    } else {
                        None
                    },
                    html: boolean(&bridge.config["matrix"], "formatHtml"),
                    rich_tools: bridge.config["toolMessages"]["mode"] == "hermes",
                    activities: Default::default(),
                    indicators,
                    typing: Default::default(),
                    incoming: message.event_id.clone(),
                    reply_context,
                };
                let mut event = Event {
                    platform: "matrix".into(),
                    room: room.room_id().to_string(),
                    thread: thread.map(|id| id.to_string()).unwrap_or_default(),
                    id: message.event_id.to_string(),
                    user: message.sender.to_string(),
                    text: body,
                    direct: false,
                    mentioned,
                    bot: false,
                    images: vec![],
                    reply_to: None,
                };
                if !bridge.user_allowed(&event)
                    || !crate::core::room_allowed(&bridge.config["matrix"], &event.room)
                {
                    return;
                }
                #[cfg(not(feature = "matrix-encryption"))]
                if ensure_plaintext_room(&room).await.is_err() {
                    eprintln!(
                        "Matrix plaintext input refused: encrypted or unavailable room state"
                    );
                    return;
                }
                let request =
                    matrix_sdk::ruma::api::client::membership::joined_members::v3::Request::new(
                        room.room_id().to_owned(),
                    );
                let Ok(members) = room.client().send(request).await else {
                    return;
                };
                event.direct = members.joined.len() == 2;
                let mut queries = queries.lock().await;
                if !accepting.load(std::sync::atomic::Ordering::Acquire) {
                    return;
                }
                while queries.try_join_next().is_some() {}
                queries.spawn(async move {
                    // Keep the inherited admission bound for the whole query, not
                    // merely the now-short SDK callback. Sync must remain able to
                    // admit follow-ups, cancellation and one-use approval input.
                    let _permit = _permit;
                    if let Some(content) = file_content {
                        let client = sink.room.client();
                        let loader = Box::pin(super::matrix_attachment::prompt(&client, content));
                        let _ = bridge.handle_matrix_file(event, &sink, loader).await;
                    } else {
                        let _ = bridge.handle(event, &sink).await;
                    }
                });
            }
        },
    );
    eprintln!("Matrix listening");
    let result = async {
        loop {
            tokio::select! {
                _=shutdown.changed()=>return Ok(()),
                result=client.sync_once(SyncSettings::default())=>{
                    let response=result?;
                    if boolean(&policy,"autoJoin"){
                        for room_id in response.rooms.invited.keys(){
                            if crate::core::room_allowed(&policy,room_id.as_str()) {
                                tokio::select! {
                                    biased;
                                    _ = shutdown.changed() => return Ok(()),
                                    result = client.join_room_by_id(room_id) => { result?; }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    .await;
    accepting.store(false, std::sync::atomic::Ordering::Release);
    client.remove_event_handler(handler);
    let mut queries = queries.lock().await;
    queries.abort_all();
    while queries.join_next().await.is_some() {}
    result
}

#[cfg(all(test, not(feature = "matrix-encryption")))]
#[path = "../../../tests/connectors/matrix/plaintext.rs"]
mod plaintext_tests;

#[cfg(test)]
#[path = "../../../tests/connectors/matrix/attachment_sdk.rs"]
mod attachment_sdk_tests;

#[cfg(test)]
#[path = "../../../tests/connectors/matrix/approval.rs"]
mod native_approval_tests;
#[cfg(test)]
#[path = "../../../tests/connectors/matrix/question_sdk.rs"]
mod native_question_sdk_tests;
#[cfg(test)]
#[path = "../../../tests/connectors/matrix/terminal.rs"]
mod terminal_thread_tests;

#[cfg(test)]
#[path = "../../../tests/connectors/matrix/mod.rs"]
mod tests;
