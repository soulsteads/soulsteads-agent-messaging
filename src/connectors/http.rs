use crate::runtime::{Bridge, Event, Sink};
use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use std::{sync::Arc, time::Duration};
use tokio::sync::{Mutex, Semaphore, watch};

#[derive(Clone)]
pub struct Platform {
    pub kind: String,
    pub base: String,
    pub token: String,
    pub bot: String,
    pub username: String,
}
impl Platform {
    async fn admit(&self, bridge: &Bridge, message: &Value) -> Result<Option<Event>> {
        let Some(mut event) = self.map(message) else {
            return Ok(None);
        };
        if self.kind == "mattermost" {
            let config = &bridge.config["mattermost"];
            let channels = config.get("allowedChannels");
            let permitted = channels.is_some_and(|channels| {
                channels.as_array().is_some_and(|channels| {
                    !channels.is_empty()
                        && channels
                            .iter()
                            .all(|channel| channel.as_str().is_some_and(|id| !id.is_empty()))
                        && channels
                            .iter()
                            .any(|channel| channel.as_str() == Some(event.room.as_str()))
                })
            });
            if !permitted {
                return Ok(None);
            }
            let body: Value = match message["data"]["post"].as_str() {
                Some(text) => serde_json::from_str(text)?,
                None => message["data"]["post"].clone(),
            };
            if config["respondToMentions"] == false && event.mentioned {
                event.text = body["message"].as_str().unwrap_or("").trim().into();
                event.mentioned = false;
            }
            let trigger = bridge
                .env
                .get("MATTERMOST_TRIGGER")
                .map(String::as_str)
                .filter(|value| !value.is_empty())
                .unwrap_or_else(|| crate::config::string(&bridge.config, "trigger"));
            let implicit = config["threadIsolation"] == true
                && config["respondToThreadReplies"] == true
                && body["root_id"]
                    .as_str()
                    .is_some_and(|root| !root.is_empty())
                && bridge
                    .has_active_session("mattermost", &bridge.context(&event))
                    .await?;
            if event.text.trim().is_empty()
                || !(event.direct
                    || event.mentioned
                    || config["respondToAllMessages"] == true
                    || (!trigger.is_empty() && event.text.trim().starts_with(trigger))
                    || implicit)
            {
                return Ok(None);
            }
        }
        if !bridge.transport_admissible(&event).await {
            return Ok(None);
        }
        if self.kind != "telegram" {
            return Ok(Some(event));
        }
        let body = if message["message"].is_object() {
            &message["message"]
        } else {
            message
        };
        let config = &bridge.config["telegram"];
        let context = bridge.context(&event);
        let active = bridge.has_persisted("telegram", &context)?;
        let trigger = bridge
            .env
            .get("TELEGRAM_TRIGGER")
            .map(String::as_str)
            .unwrap_or_else(|| crate::config::string(&bridge.config, "trigger"));
        let replying = config["respondToReplies"] == true
            && body["reply_to_message"]["from"]["is_bot"] == true
            && scalar(&body["reply_to_message"]["from"]["id"]) == self.bot;
        let implicit = active
            && (replying
                || (config["respondToImplicitTopicReplies"] == true
                    && config["threadIsolation"] == true
                    && !event.thread.is_empty()));
        let addressed = crate::runtime::telegram_approval_control(&event)
            || event.direct
            || event.text.starts_with(trigger)
            || (event.mentioned && config["respondToMentions"] == true)
            || implicit;
        if !addressed {
            return Ok(None);
        }
        if implicit {
            event.mentioned = true;
        }
        Ok(Some(event))
    }
    pub async fn prepare(&self, bridge: &Bridge, message: &Value) -> Result<Option<Event>> {
        let Some(mut event) = self.admit(bridge, message).await? else {
            return Ok(None);
        };
        // Literal approval controls have already passed every transport and
        // Telegram admission gate; never make their broker delivery wait on media.
        if self.kind != "telegram" || crate::runtime::telegram_approval_control(&event) {
            return Ok(Some(event));
        }
        let body = if message["message"].is_object() {
            &message["message"]
        } else {
            message
        };
        let config = &bridge.config["telegram"];
        let trigger = bridge
            .env
            .get("TELEGRAM_TRIGGER")
            .map(String::as_str)
            .unwrap_or_else(|| crate::config::string(&bridge.config, "trigger"));
        if config["attachments"]["enabled"] == false {
            return Ok(Some(event));
        }
        let attachments = crate::core::attachments(body);
        let maximum = config["attachments"]["maxFileBytes"]
            .as_u64()
            .unwrap_or(20 * 1024 * 1024)
            .clamp(1, 20 * 1024 * 1024);
        let limit = config["attachments"]["maxFilesPerMessage"]
            .as_u64()
            .unwrap_or(4)
            .clamp(1, 16) as usize;
        let mut headers = Vec::new();
        for attachment in attachments.as_array().into_iter().flatten().take(limit) {
            if attachment["size"]
                .as_u64()
                .is_some_and(|size| size > maximum)
            {
                continue;
            }
            let downloaded = self
                .download_telegram(bridge, &event, attachment, maximum)
                .await;
            if let Ok(header) = downloaded {
                headers.push(header);
            }
        }
        if !headers.is_empty() {
            let query = event
                .text
                .strip_prefix(trigger)
                .map(str::trim)
                .unwrap_or(event.text.trim());
            if !query.starts_with('/') {
                let query = if query.is_empty() {
                    "(User sent a file attachment with no message text. Use the attached tool(s) to examine it.)"
                } else {
                    query
                };
                event.text = format!("{}\n\n{query}", headers.join("\n"));
                event.mentioned = true;
            }
        }
        Ok(Some(event))
    }
    async fn download_telegram(
        &self,
        bridge: &Bridge,
        event: &Event,
        attachment: &Value,
        maximum: u64,
    ) -> Result<String> {
        let file = self
            .api("POST", "getFile", json!({"file_id":attachment["fileId"]}))
            .await?;
        if file["file_size"]
            .as_u64()
            .is_some_and(|size| size > maximum)
        {
            bail!("Telegram attachment exceeds size limit");
        }
        let path = file["file_path"]
            .as_str()
            .context("Telegram file path missing")?;
        if path.is_empty()
            || path.len() > 2048
            || path.contains(['\\', '?', '#'])
            || path.contains('%')
            || path.split('/').any(|part| matches!(part, "" | "." | ".."))
        {
            bail!("Unsafe Telegram attachment path");
        }
        let mut url = url::Url::parse(&self.base)?;
        url.set_path(&format!(
            "{}/file/bot{}/{}",
            url.path().trim_end_matches('/'),
            self.token,
            path
        ));
        let response = self
            .client()?
            .get(url)
            .send()
            .await
            .map_err(|_| anyhow::anyhow!("Telegram file transport failed"))?;
        if !response.status().is_success() {
            bail!("Telegram file download rejected");
        }
        if response.content_length().is_some_and(|size| size > maximum) {
            bail!("Telegram file exceeds size limit");
        }
        let mut bytes = Vec::new();
        let mut stream = response.bytes_stream();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(|_| anyhow::anyhow!("Telegram file read failed"))?;
            if bytes.len() as u64 + chunk.len() as u64 > maximum {
                bail!("Telegram file exceeds size limit");
            }
            bytes.extend_from_slice(&chunk);
        }
        let original = attachment["fileName"].as_str().unwrap_or(path);
        let extension = crate::core::extension(
            &json!(original.rsplit_once('.').map(|(_, extension)| extension)),
            "bin",
        );
        let name = format!("{}.{extension}", uuid::Uuid::new_v4());
        let uploads = bridge.working_directory(event).join("uploads");
        let destination = uploads.join(&name);
        crate::store::write_private(&destination, &bytes)?;
        Ok(format!(
            "[Attached file: {} ({}, {} bytes, {}, filename=\"{name}\")]",
            destination.to_string_lossy(),
            attachment["mimeType"]
                .as_str()
                .unwrap_or("application/octet-stream"),
            bytes.len(),
            attachment["kind"].as_str().unwrap_or("document")
        ))
    }
    pub fn new(kind: &str, base: &str, token: &str, bot: &str, username: &str) -> Self {
        Self {
            kind: kind.into(),
            base: base.into(),
            token: token.into(),
            bot: bot.into(),
            username: username.into(),
        }
    }
    fn url(&self, endpoint: &str) -> Result<String> {
        let mut base = url::Url::parse(&self.base).context("Invalid connector base URL")?;
        if !["http", "https"].contains(&base.scheme())
            || !base.username().is_empty()
            || base.password().is_some()
        {
            bail!("Invalid connector base URL");
        }
        let prefix = match self.kind.as_str() {
            "mattermost" => "api/v4".into(),
            "discord" => "api/v10".into(),
            "slack" => "api".into(),
            "telegram" => format!("bot{}", self.token),
            _ => bail!("Unknown HTTP connector"),
        };
        base.set_path(&format!(
            "{}/{}/{}",
            base.path().trim_end_matches('/'),
            prefix,
            endpoint.trim_start_matches('/')
        ));
        Ok(base.to_string())
    }
    fn client(&self) -> Result<reqwest::Client> {
        Ok(reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(Duration::from_secs(40))
            .build()?)
    }
    fn authorize(&self, request: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        if self.kind == "discord" {
            request.header("Authorization", format!("Bot {}", self.token))
        } else if self.kind == "telegram" {
            request
        } else {
            request.bearer_auth(&self.token)
        }
    }
    pub async fn api(&self, method: &str, endpoint: &str, body: Value) -> Result<Value> {
        let client = self.client()?;
        let url = self.url(endpoint)?;
        for attempt in 0..6 {
            let request = client.request(reqwest::Method::from_bytes(method.as_bytes())?, &url);
            let request = if method == "GET" {
                if body.is_object() {
                    request.query(&body)
                } else {
                    request
                }
            } else {
                request.json(&body)
            };
            let response = self
                .authorize(request)
                .send()
                .await
                .map_err(|_| anyhow::anyhow!("{} transport request failed", self.kind))?;
            let status = response.status();
            let retry_header = response
                .headers()
                .get("retry-after")
                .and_then(|value| value.to_str().ok())
                .and_then(|value| value.parse::<f64>().ok());
            let value = read_json(response).await?;
            if status.as_u16() == 429 || value["error_code"] == 429 {
                if attempt == 5 {
                    bail!("{} rate-limit retries exhausted (429)", self.kind);
                }
                let delay = value["parameters"]["retry_after"].as_f64().or(retry_header);
                tokio::time::sleep(Duration::from_millis(retry_delay(delay))).await;
                continue;
            }
            if !status.is_success() || value["ok"] == false {
                bail!("{} API rejected request ({})", self.kind, status.as_u16());
            }
            return Ok(if self.kind == "telegram" {
                value["result"].clone()
            } else {
                value
            });
        }
        bail!("Connector retry exhausted")
    }
    pub fn map(&self, message: &Value) -> Option<Event> {
        let mut event = Event::web("", "");
        event.platform = self.kind.clone();
        event.direct = false;
        match self.kind.as_str() {
            "telegram" => {
                let body = if message["message"].is_object() {
                    &message["message"]
                } else {
                    message
                };
                event.user = scalar(&body["from"]["id"]);
                event.room = scalar(&body["chat"]["id"]);
                event.id = format!("{}:{}", event.room, scalar(&body["message_id"]));
                event.reply_to = body["reply_to_message"]["message_id"]
                    .as_i64()
                    .map(|id| id.to_string());
                event.bot = body["from"]["is_bot"] == true || event.user == self.bot;
                event.direct = body["chat"]["type"] == "private";
                if body["is_topic_message"] == true && body["message_thread_id"].is_number() {
                    event.thread = scalar(&body["message_thread_id"]);
                }
                event.text = body["text"]
                    .as_str()
                    .or_else(|| body["caption"].as_str())
                    .unwrap_or("")
                    .trim()
                    .into();
                if let Some(word) = event.text.split_whitespace().next()
                    && word.starts_with('/')
                    && let Some((command, target)) = word.split_once('@')
                    && (2..=33).contains(&command.len())
                    && command[1..]
                        .bytes()
                        .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
                {
                    if self.username.is_empty() || !target.eq_ignore_ascii_case(&self.username) {
                        return None;
                    }
                    let suffix = &event.text[word.len()..];
                    event.text = format!("{command}{suffix}");
                    event.mentioned = true;
                }
                let mention = format!("@{}", self.username);
                if !self.username.is_empty()
                    && event
                        .text
                        .to_lowercase()
                        .starts_with(&mention.to_lowercase())
                {
                    event.text = event.text[mention.len()..].trim().into();
                    event.mentioned = true;
                }
            }
            "mattermost" => {
                if message["event"] != "posted" {
                    return None;
                }
                let body: Value = if let Some(text) = message["data"]["post"].as_str() {
                    serde_json::from_str(text).ok()?
                } else {
                    message["data"]["post"].clone()
                };
                if body["type"].as_str().is_some_and(|kind| !kind.is_empty()) {
                    return None;
                }
                event.room = scalar(&body["channel_id"]);
                event.user = scalar(&body["user_id"]);
                event.id = format!("{}:{}", event.room, scalar(&body["id"]));
                event.thread = body["root_id"]
                    .as_str()
                    .filter(|value| !value.is_empty())
                    .unwrap_or_else(|| body["id"].as_str().unwrap_or(""))
                    .into();
                event.text = body["message"].as_str().unwrap_or("").trim().into();
                event.bot = event.user == self.bot;
                event.direct = message["data"]["channel_type"] == "D";
                let mention = format!("@{}", self.username);
                if !self.username.is_empty() && event.text.starts_with(&mention) {
                    event.text = event.text[mention.len()..].trim().into();
                    event.mentioned = true;
                }
            }
            "slack" => {
                let payload = if message["payload"].is_object() {
                    &message["payload"]
                } else {
                    message
                };
                let body = if payload["event"].is_object() {
                    &payload["event"]
                } else {
                    payload
                };
                if !["message", "app_mention"].contains(&body["type"].as_str().unwrap_or(""))
                    || !body["subtype"].is_null()
                    || !body["bot_id"].is_null()
                {
                    return None;
                }
                event.user = scalar(&body["user"]);
                event.room = scalar(&body["channel"]);
                event.id = format!(
                    "{}:{}:{}",
                    scalar(&payload["team_id"]),
                    event.room,
                    scalar(&body["ts"])
                );
                event.thread = body["thread_ts"]
                    .as_str()
                    .filter(|value| !value.is_empty())
                    .unwrap_or_else(|| body["ts"].as_str().unwrap_or(""))
                    .into();
                event.text = body["text"].as_str().unwrap_or("").into();
                event.bot = event.user == self.bot;
                event.mentioned = body["type"] == "app_mention"
                    || (!self.bot.is_empty()
                        && event.text.starts_with(&format!("<@{}>", self.bot)));
                if event.mentioned {
                    event.text = regex::Regex::new(r"<@[A-Z0-9]+>")
                        .ok()?
                        .replace_all(&event.text, "")
                        .trim()
                        .into();
                }
            }
            "discord" => {
                if message["t"] != "MESSAGE_CREATE" {
                    return None;
                }
                let body = &message["d"];
                event.room = scalar(&body["channel_id"]);
                event.user = scalar(&body["author"]["id"]);
                event.id = scalar(&body["id"]);
                event.bot = body["author"]["bot"] == true || event.user == self.bot;
                event.text = body["content"].as_str().unwrap_or("").trim().into();
                for mention in [format!("<@{}>", self.bot), format!("<@!{}>", self.bot)] {
                    if !self.bot.is_empty() && event.text.starts_with(&mention) {
                        event.text = event.text[mention.len()..].trim().into();
                        event.mentioned = true;
                        break;
                    }
                }
            }
            _ => return None,
        }
        if event.room.is_empty() || event.user.is_empty() || event.id.is_empty() {
            return None;
        }
        Some(event)
    }
    pub async fn send(&self, event: &Event, text: &str) -> Result<String> {
        let limit = match self.kind.as_str() {
            "discord" => 1900,
            "telegram" => 4096,
            _ => 3900,
        };
        let mut last = String::new();
        let chunks = if self.kind == "telegram" {
            split_lines(text, limit)
        } else {
            split(text, limit)
        };
        for (index, text) in chunks.into_iter().enumerate() {
            let (endpoint, body) = match self.kind.as_str() {
                "telegram" => ("sendMessage".into(), {
                    let mut body = json!({"chat_id":event.room,"text":text});
                    if let Ok(thread) = event.thread.parse::<i64>() {
                        body["message_thread_id"] = thread.into();
                    } else if index == 0
                        && !event.direct
                        && let Some(parent) = event
                            .reply_to
                            .as_ref()
                            .and_then(|id| id.parse::<i64>().ok())
                    {
                        body["reply_to_message_id"] = parent.into();
                    }
                    body
                }),
                "mattermost" => (
                    "posts".into(),
                    json!({"channel_id":event.room,"root_id":event.thread,"message":text}),
                ),
                "slack" => (
                    "chat.postMessage".into(),
                    json!({"channel":event.room,"thread_ts":event.thread,"text":text,"unfurl_links":false,"unfurl_media":false}),
                ),
                "discord" => (
                    format!("channels/{}/messages", segment(&event.room)),
                    json!({"content":text,"message_reference":{"message_id":event.id},"allowed_mentions":{"parse":[]}}),
                ),
                _ => bail!("Unknown connector"),
            };
            let response = match self.api("POST", &endpoint, body.clone()).await {
                Err(_) if self.kind == "telegram" && body.get("reply_to_message_id").is_some() => {
                    let mut retry = body;
                    retry.as_object_mut().unwrap().remove("reply_to_message_id");
                    self.api("POST", &endpoint, retry).await?
                }
                result => result?,
            };
            last = scalar(if self.kind == "telegram" {
                &response["message_id"]
            } else if self.kind == "slack" {
                &response["ts"]
            } else {
                &response["id"]
            });
        }
        Ok(last)
    }
    pub async fn edit(&self, event: &Event, id: &str, text: &str) -> Result<()> {
        let (method, endpoint, body) = match self.kind.as_str() {
            "telegram" => (
                "POST",
                "editMessageText".into(),
                json!({"chat_id":event.room,"message_id":id.parse::<i64>()?,"text":text}),
            ),
            "slack" => (
                "POST",
                "chat.update".into(),
                json!({"channel":event.room,"ts":id,"text":text}),
            ),
            "mattermost" => (
                "PUT",
                format!("posts/{}/patch", segment(id)),
                json!({"message":text}),
            ),
            "discord" => (
                "PATCH",
                format!("channels/{}/messages/{}", segment(&event.room), segment(id)),
                json!({"content":text,"allowed_mentions":{"parse":[]}}),
            ),
            _ => bail!("Unknown connector"),
        };
        self.api(method, &endpoint, body).await?;
        Ok(())
    }
    async fn send_activity(&self, event: &Event, text: &str, threaded: bool) -> Result<String> {
        let (endpoint, body) = match self.kind.as_str() {
            "slack" => {
                let mut body = json!({"channel":event.room,"text":text});
                if threaded {
                    body["thread_ts"] = event.thread.clone().into();
                }
                ("chat.postMessage".to_owned(), body)
            }
            "mattermost" => {
                let mut body = json!({"channel_id":event.room,"message":text});
                if threaded {
                    body["root_id"] = event.thread.clone().into();
                }
                ("posts".to_owned(), body)
            }
            "discord" => (
                format!("channels/{}/messages", segment(&event.room)),
                json!({"content":text,"allowed_mentions":{"parse":[]}}),
            ),
            "telegram" => {
                let mut body = json!({"chat_id":event.room,"text":text});
                if let Ok(thread) = event.thread.parse::<i64>() {
                    body["message_thread_id"] = thread.into();
                }
                ("sendMessage".to_owned(), body)
            }
            _ => bail!("Unknown connector"),
        };
        let result = self.api("POST", &endpoint, body).await?;
        Ok(scalar(if self.kind == "slack" {
            &result["ts"]
        } else if self.kind == "telegram" {
            &result["message_id"]
        } else {
            &result["id"]
        }))
    }
    pub async fn upload(&self, event: &Event, name: &str, bytes: Vec<u8>) -> Result<()> {
        if bytes.len() > 20 * 1024 * 1024 || name.len() > 256 || name.contains(['/', '\\']) {
            bail!("Invalid upload");
        }
        let client = self.client()?;
        if self.kind == "slack" {
            let slot = self
                .api(
                    "POST",
                    "files.getUploadURLExternal",
                    json!({"filename":name,"length":bytes.len()}),
                )
                .await?;
            let url = slot["upload_url"]
                .as_str()
                .context("Slack upload URL missing")?;
            let parsed = url::Url::parse(url)?;
            if parsed.scheme() != "https"
                && !(parsed.scheme() == "http"
                    && parsed
                        .host_str()
                        .is_some_and(|host| host == "127.0.0.1" || host == "localhost"))
            {
                bail!("Unsafe upload URL");
            }
            let response = client
                .post(url)
                .body(bytes)
                .send()
                .await
                .map_err(|_| anyhow::anyhow!("Slack upload transport failed"))?;
            if !response.status().is_success() {
                bail!("Slack upload rejected");
            }
            self.api("POST","files.completeUploadExternal",json!({"files":[{"id":slot["file_id"],"title":name}],"channel_id":event.room,"thread_ts":event.thread})).await?;
            return Ok(());
        }
        let endpoint = match self.kind.as_str() {
            "telegram" => "sendDocument".into(),
            "mattermost" => "files".into(),
            "discord" => format!("channels/{}/messages", segment(&event.room)),
            _ => bail!("Unknown upload connector"),
        };
        for attempt in 0..6 {
            let part = reqwest::multipart::Part::bytes(bytes.clone()).file_name(name.to_owned());
            let form=match self.kind.as_str() {
                "telegram"=>{ let form=reqwest::multipart::Form::new().text("chat_id",event.room.clone()).part("document",part); if !event.thread.is_empty(){form.text("message_thread_id",event.thread.clone())}else{form} },
                "mattermost"=>reqwest::multipart::Form::new().text("channel_id",event.room.clone()).part("files",part),
                _=>reqwest::multipart::Form::new().text("payload_json",json!({"allowed_mentions":{"parse":[]},"attachments":[{"id":0,"filename":name}]}).to_string()).part("files[0]",part),
            };
            let response = self
                .authorize(client.post(self.url(&endpoint)?).multipart(form))
                .send()
                .await
                .map_err(|_| anyhow::anyhow!("Upload transport failed"))?;
            let status = response.status();
            let response = read_json(response).await?;
            if status.as_u16() == 429 || response["error_code"] == 429 {
                if attempt == 5 {
                    bail!("Upload rate-limit retries exhausted");
                }
                tokio::time::sleep(Duration::from_millis(retry_delay(
                    response["parameters"]["retry_after"].as_f64(),
                )))
                .await;
                continue;
            }
            if !status.is_success() || response["ok"] == false {
                bail!("Upload rejected");
            }
            if self.kind == "mattermost" {
                self.api("POST","posts",json!({"channel_id":event.room,"root_id":event.thread,"message":name,"file_ids":response["file_infos"].as_array().into_iter().flatten().map(|file|file["id"].clone()).collect::<Vec<_>>()})).await?;
            }
            return Ok(());
        }
        bail!("Upload retry exhausted")
    }
    async fn refuse_telegram_overload(&self, event: Event) -> Result<()> {
        PlatformSink::new(self.clone(), event)
            .emit(json!({
                "type":"notice",
                "text":"Busy: this message was not accepted or run. Please retry shortly."
            }))
            .await
    }
    pub async fn run(
        mut self,
        bridge: Arc<Bridge>,
        mut shutdown: watch::Receiver<bool>,
    ) -> Result<()> {
        let identity = self
            .api(
                if matches!(self.kind.as_str(), "slack" | "telegram") {
                    "POST"
                } else {
                    "GET"
                },
                match self.kind.as_str() {
                    "telegram" => "getMe",
                    "slack" => "auth.test",
                    "mattermost" => "users/me",
                    _ => "users/@me",
                },
                json!({}),
            )
            .await?;
        self.bot = scalar(if self.kind == "slack" {
            &identity["user_id"]
        } else {
            &identity["id"]
        });
        self.username = scalar(&identity["username"]);
        let capacity = Arc::new(Semaphore::new(64));
        let mut tasks = tokio::task::JoinSet::new();
        if self.kind == "telegram" {
            // Keep one bounded lane for approvals so 64 permission-waiting
            // prompts cannot prevent their own approval or denial.
            let controls = Arc::new(Semaphore::new(1));
            let drop_pending = bridge
                .env
                .get("TELEGRAM_DROP_PENDING")
                .is_some_and(|value| value == "1");
            let _ = tokio::select! {
                _=shutdown.changed()=>return Ok(()),
                result=self.api("POST","deleteWebhook",json!({"drop_pending_updates":drop_pending}))=>result,
            };
            let mut offset = 0;
            if drop_pending {
                let pending = tokio::select! {
                    _=shutdown.changed()=>return Ok(()),
                    pending=self.api("POST","getUpdates",json!({"timeout":0,"allowed_updates":["message"]}))=>pending,
                };
                if let Ok(pending) = pending {
                    offset = pending
                        .as_array()
                        .into_iter()
                        .flatten()
                        .filter_map(|update| update["update_id"].as_i64())
                        .max()
                        .map(|id| id.saturating_add(1))
                        .unwrap_or(0);
                }
            }
            let mut failures = 0u32;
            'telegram: loop {
                if *shutdown.borrow() {
                    break;
                }
                let updates = tokio::select! { _=shutdown.changed()=>break, updates=self.api("POST","getUpdates",json!({"offset":offset,"timeout":30,"allowed_updates":["message"]}))=>updates };
                match updates {
                    Ok(updates) => {
                        failures = 0;
                        for update in updates.as_array().into_iter().flatten() {
                            let control = self.map(update).is_some_and(|event| {
                                crate::runtime::telegram_approval_control(&event)
                            });
                            let permit = if control {
                                tokio::select! {
                                    _ = shutdown.changed() => break 'telegram,
                                    permit = controls.clone().acquire_owned() => permit.expect("Telegram control semaphore remains open"),
                                }
                            } else {
                                match capacity.clone().try_acquire_owned() {
                                    Ok(permit) => permit,
                                    Err(tokio::sync::TryAcquireError::NoPermits) => {
                                        // Recheck all normal transport/address gates before refusal,
                                        // but never download media or start a native turn for work refused
                                        // at this bounded ordinary-capacity boundary.
                                        if let Some(event) = self.admit(&bridge, update).await? {
                                            tokio::select! {
                                                _ = shutdown.changed() => break 'telegram,
                                                result = self.refuse_telegram_overload(event) => result?,
                                            }
                                        }
                                        if let Some(id) = update["update_id"].as_i64() {
                                            offset = offset.max(id.saturating_add(1));
                                        }
                                        continue;
                                    }
                                    Err(tokio::sync::TryAcquireError::Closed) => {
                                        bail!("Telegram polling semaphore closed")
                                    }
                                }
                            };
                            if let Some(id) = update["update_id"].as_i64() {
                                offset = offset.max(id.saturating_add(1));
                            }
                            let platform = self.clone();
                            let bridge = bridge.clone();
                            let update = update.clone();
                            tasks.spawn(async move {
                                let _permit = permit;
                                if let Ok(Some(event)) = platform.prepare(&bridge, &update).await {
                                    let sink = PlatformSink::new(platform, event.clone())
                                        .with_thread_isolation(crate::config::boolean(
                                            &bridge.config[&event.platform],
                                            "threadIsolation",
                                        ));
                                    let _ = bridge.handle(event, &sink).await;
                                }
                            });
                        }
                    }
                    Err(_) => {
                        failures += 1;
                        if failures >= 10 {
                            bail!("Telegram polling retry limit reached");
                        }
                        let delay = Duration::from_secs((1u64 << failures.min(5)).min(30));
                        tokio::select! {_=shutdown.changed()=>break,_=tokio::time::sleep(delay)=>{}}
                    }
                }
                while tasks.try_join_next().is_some() {}
            }
        } else {
            let mut failures = 0u32;
            let mut session_id = String::new();
            let mut sequence = Value::Null;
            loop {
                if *shutdown.borrow() {
                    break;
                }
                let result = self
                    .socket_once(
                        bridge.clone(),
                        &mut shutdown,
                        &capacity,
                        &mut tasks,
                        &mut session_id,
                        &mut sequence,
                    )
                    .await;
                if *shutdown.borrow() {
                    break;
                }
                failures = if result.is_ok() {
                    0
                } else {
                    (failures + 1).min(6)
                };
                tokio::select! {_=shutdown.changed()=>break,_=tokio::time::sleep(Duration::from_secs(1u64<<failures))=>{}}
            }
        }
        tasks.abort_all();
        Ok(())
    }
    async fn socket_once(
        &self,
        bridge: Arc<Bridge>,
        shutdown: &mut watch::Receiver<bool>,
        capacity: &Arc<Semaphore>,
        tasks: &mut tokio::task::JoinSet<()>,
        session_id: &mut String,
        sequence: &mut Value,
    ) -> Result<()> {
        let address = match self.kind.as_str() {
            "mattermost" => {
                let mut url = url::Url::parse(&self.base)?;
                url.set_scheme(if url.scheme() == "https" { "wss" } else { "ws" })
                    .map_err(|_| anyhow::anyhow!("Invalid WebSocket URL"))?;
                url.set_path(&format!(
                    "{}/api/v4/websocket",
                    url.path().trim_end_matches('/')
                ));
                url.to_string()
            }
            "slack" => {
                let mut app = self.clone();
                app.token = bridge
                    .env
                    .get("SLACK_APP_TOKEN")
                    .context("SLACK_APP_TOKEN required")?
                    .clone();
                app.api("POST", "apps.connections.open", json!({})).await?["url"]
                    .as_str()
                    .context("Slack socket URL missing")?
                    .into()
            }
            "discord" => {
                let result = self.api("GET", "gateway/bot", json!({})).await?;
                format!(
                    "{}?v=10&encoding=json",
                    result["url"]
                        .as_str()
                        .context("Discord gateway URL missing")?
                )
            }
            _ => bail!("Unknown socket connector"),
        };
        let (mut socket, _) = tokio_tungstenite::connect_async(address)
            .await
            .map_err(|_| anyhow::anyhow!("Platform WebSocket connection failed"))?;
        if self.kind == "mattermost" {
            socket.send(json!({"seq":1,"action":"authentication_challenge","data":{"token":self.token}}).to_string().into()).await?;
        }
        let mut heartbeat = tokio::time::interval_at(
            tokio::time::Instant::now() + Duration::from_secs(30),
            Duration::from_secs(30),
        );
        let mut awaiting_ack = false;
        let mut hello_received = self.kind != "discord";
        loop {
            tokio::select! {
                _=shutdown.changed()=>{let _=socket.close(None).await;return Ok(());},
                _=heartbeat.tick(),if hello_received=>{
                    if self.kind=="discord" {
                        if awaiting_ack {bail!("Discord heartbeat not acknowledged");}
                        socket.send(json!({"op":1,"d":sequence}).to_string().into()).await?;awaiting_ack=true;
                    }else{socket.send(tokio_tungstenite::tungstenite::Message::Ping(vec![].into())).await?;}
                },
                message=socket.next()=>{
                    let Some(message)=message else{return Ok(());}; let message=message?;
                    if message.is_close(){return Ok(());}
                    if !message.is_text(){continue;}
                    if message.len()>2*1024*1024{bail!("Platform event too large");}
                    let value:Value=serde_json::from_str(message.to_text()?)?;
                    if self.kind=="slack"&&let Some(id)=value["envelope_id"].as_str(){socket.send(json!({"envelope_id":id}).to_string().into()).await?;}
                    if self.kind=="discord" {
                        match value["op"].as_u64().unwrap_or(0) {
                            10=>{
                                let interval=Duration::from_millis(value["d"]["heartbeat_interval"].as_u64().unwrap_or(30000).clamp(100,120000));heartbeat=tokio::time::interval_at(tokio::time::Instant::now()+interval,interval); awaiting_ack=false;hello_received=true;
                                let payload=if session_id.is_empty(){json!({"op":2,"d":{"token":self.token,"intents":37377,"properties":{"os":"linux","browser":"soulsteads-messaging-rust","device":"soulsteads-messaging-rust"}}})}else{json!({"op":6,"d":{"token":self.token,"session_id":session_id,"seq":sequence}})};
                                socket.send(payload.to_string().into()).await?;
                            },
                            11=>awaiting_ack=false,
                            1=>{socket.send(json!({"op":1,"d":sequence}).to_string().into()).await?;awaiting_ack=true;},
                            7=>return Ok(()),
                            9=>{session_id.clear();return Ok(());},
                            _=>{}
                        }
                        if !value["s"].is_null(){*sequence=value["s"].clone();}
                        if value["t"]=="READY"{*session_id=scalar(&value["d"]["session_id"]);}
                    }
                    let event = if self.kind == "mattermost" {
                        self.prepare(&bridge, &value).await?
                    } else {
                        self.map(&value)
                    };
                    if let Some(event)=event{spawn_event(self.clone(),bridge.clone(),event,capacity,tasks)?;}
                    while tasks.try_join_next().is_some(){}
                }
            }
        }
    }
}

pub struct PlatformSink {
    pub platform: Platform,
    pub event: Event,
    activities: Mutex<std::collections::HashMap<String, String>>,
    activity_threaded: bool,
}
impl PlatformSink {
    pub fn new(platform: Platform, event: Event) -> Self {
        Self {
            platform,
            event,
            activities: Mutex::new(std::collections::HashMap::new()),
            activity_threaded: true,
        }
    }
    pub fn with_thread_isolation(mut self, enabled: bool) -> Self {
        self.activity_threaded = enabled;
        self
    }
    fn reply_event(&self) -> Event {
        let mut event = self.event.clone();
        if self.platform.kind == "mattermost" && !self.activity_threaded {
            event.thread.clear();
        }
        event
    }
}
#[async_trait]
impl Sink for PlatformSink {
    fn owned(&self) -> Option<Arc<dyn Sink>> {
        Some(Arc::new(
            Self::new(self.platform.clone(), self.event.clone())
                .with_thread_isolation(self.activity_threaded),
        ))
    }
    async fn upload(&self, name: &str, _mime: &str, bytes: Vec<u8>) -> Result<()> {
        self.platform.upload(&self.reply_event(), name, bytes).await
    }
    async fn emit(&self, value: Value) -> Result<()> {
        match value["type"].as_str().unwrap_or("") {
            kind @ ("response" | "error" | "permission_denied" | "notice")
                if kind != "notice"
                    || matches!(self.platform.kind.as_str(), "mattermost" | "telegram") =>
            {
                let text = value["text"]
                    .as_str()
                    .or_else(|| value["message"].as_str())
                    .unwrap_or("");
                if !text.is_empty() {
                    self.platform.send(&self.reply_event(), text).await?;
                }
            }
            "activity" | "activity_update" => {
                let key = scalar(&value["activityId"]);
                let text = format!("> {}", value["message"].as_str().unwrap_or("tool"));
                let mut activities = self.activities.lock().await;
                if let Some(id) = activities.get(&key) {
                    if self.platform.edit(&self.event, id, &text).await.is_err() {
                        let id = self
                            .platform
                            .send_activity(&self.event, &text, self.activity_threaded)
                            .await?;
                        activities.insert(key, id);
                    }
                } else {
                    let id = self
                        .platform
                        .send_activity(&self.event, &text, self.activity_threaded)
                        .await?;
                    activities.insert(key, id);
                }
            }
            "thinking" => match self.platform.kind.as_str() {
                "telegram" => {
                    let _=self.platform.api("POST","sendChatAction",json!({"chat_id":self.event.room,"action":"typing","message_thread_id":self.event.thread.parse::<i64>().ok()})).await;
                }
                "discord" => {
                    let _ = self
                        .platform
                        .api(
                            "POST",
                            &format!("channels/{}/typing", segment(&self.event.room)),
                            json!({}),
                        )
                        .await;
                }
                _ => {}
            },
            _ => {}
        }
        Ok(())
    }
}
fn spawn_event(
    platform: Platform,
    bridge: Arc<Bridge>,
    event: Event,
    capacity: &Arc<Semaphore>,
    tasks: &mut tokio::task::JoinSet<()>,
) -> Result<()> {
    let permit = capacity
        .clone()
        .try_acquire_owned()
        .context("Platform admission queue full")?;
    tasks.spawn(async move {
        let _permit = permit;
        let sink = PlatformSink::new(platform, event.clone()).with_thread_isolation(
            crate::config::boolean(&bridge.config[&event.platform], "threadIsolation"),
        );
        let _ = bridge.handle(event, &sink).await;
    });
    Ok(())
}
pub fn scalar(value: &Value) -> String {
    value.as_str().map(str::to_owned).unwrap_or_else(|| {
        if value.is_number() {
            value.to_string()
        } else {
            String::new()
        }
    })
}
fn segment(value: &str) -> String {
    url::form_urlencoded::byte_serialize(value.as_bytes()).collect()
}
pub fn retry_delay(value: Option<f64>) -> u64 {
    value
        .filter(|value| value.is_finite())
        .map(|value| (value * 1000.0).clamp(0.0, 300000.0) as u64)
        .unwrap_or(5000)
}
fn split(text: &str, limit: usize) -> Vec<String> {
    let mut output = vec![];
    let mut current = String::new();
    let mut count = 0;
    for character in text.chars() {
        if count + character.len_utf16() > limit {
            output.push(std::mem::take(&mut current));
            count = 0;
        }
        current.push(character);
        count += character.len_utf16();
    }
    if !current.is_empty() {
        output.push(current);
    }
    output
}
fn split_lines(text: &str, limit: usize) -> Vec<String> {
    let mut output = Vec::new();
    let mut remaining = text;
    while remaining.encode_utf16().count() > limit {
        let mut units = 0;
        let boundary = remaining
            .char_indices()
            .find_map(|(offset, character)| {
                units += character.len_utf16();
                (units > limit).then_some(offset)
            })
            .unwrap_or(remaining.len());
        let split_at = remaining
            .char_indices()
            .take_while(|(offset, _)| *offset <= boundary)
            .filter(|(_, character)| *character == '\n')
            .map(|(offset, _)| offset)
            .last()
            .filter(|offset| *offset > 0)
            .unwrap_or(boundary);
        output.push(remaining[..split_at].to_owned());
        remaining = remaining[split_at..].trim_start_matches(|character| {
            matches!(character,
            '\u{0009}'..='\u{000d}' | '\u{0020}' | '\u{00a0}' | '\u{1680}' |
            '\u{2000}'..='\u{200a}' | '\u{2028}' | '\u{2029}' | '\u{202f}' |
            '\u{205f}' | '\u{3000}' | '\u{feff}')
        });
    }
    if !remaining.is_empty() {
        output.push(remaining.to_owned());
    }
    output
}
async fn read_json(response: reqwest::Response) -> Result<Value> {
    if response.status().as_u16() == 204 {
        return Ok(Value::Null);
    }
    let mut stream = response.bytes_stream();
    let mut bytes = Vec::new();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|_| anyhow::anyhow!("Platform response stream failed"))?;
        if bytes.len() + chunk.len() > 2 * 1024 * 1024 {
            bail!("Platform response too large");
        }
        bytes.extend_from_slice(&chunk);
    }
    if bytes.is_empty() {
        return Ok(Value::Null);
    }
    serde_json::from_slice(&bytes).context("Platform returned invalid JSON")
}
