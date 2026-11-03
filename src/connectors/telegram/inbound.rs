use crate::{
    connectors::http::{Platform, scalar},
    runtime::{Bridge, Event},
};
use anyhow::{Context, Result, bail};
use futures_util::StreamExt;
use serde_json::{Value, json};

pub(crate) fn map(platform: &Platform, message: &Value) -> Option<Event> {
    let body = if message["message"].is_object() {
        &message["message"]
    } else {
        message
    };
    let mut event = Event::web("", "");
    event.platform = platform.kind.clone();
    event.direct = false;
    event.user = scalar(&body["from"]["id"]);
    event.room = scalar(&body["chat"]["id"]);
    event.id = format!("{}:{}", event.room, scalar(&body["message_id"]));
    event.reply_to = body["reply_to_message"]["message_id"]
        .as_i64()
        .map(|id| id.to_string());
    event.bot = body["from"]["is_bot"] == true || event.user == platform.bot;
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
        if platform.username.is_empty() || !target.eq_ignore_ascii_case(&platform.username) {
            return None;
        }
        let suffix = &event.text[word.len()..];
        event.text = format!("{command}{suffix}");
        event.mentioned = true;
    }
    let mention = format!("@{}", platform.username);
    if !platform.username.is_empty()
        && event
            .text
            .to_lowercase()
            .starts_with(&mention.to_lowercase())
    {
        event.text = event.text[mention.len()..].trim().into();
        event.mentioned = true;
    }
    if event.room.is_empty() || event.user.is_empty() || event.id.is_empty() {
        return None;
    }
    Some(event)
}

pub(crate) async fn admit(
    platform: &Platform,
    bridge: &Bridge,
    message: &Value,
) -> Result<Option<Event>> {
    let Some(mut event) = map(platform, message) else {
        return Ok(None);
    };
    if !bridge.transport_admissible(&event).await {
        return Ok(None);
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
        && scalar(&body["reply_to_message"]["from"]["id"]) == platform.bot;
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

pub(crate) async fn prepare(
    platform: &Platform,
    bridge: &Bridge,
    message: &Value,
) -> Result<Option<Event>> {
    let Some(mut event) = admit(platform, bridge, message).await? else {
        return Ok(None);
    };
    // Literal approval controls have already passed every transport and
    // Telegram admission gate; never make their broker delivery wait on media.
    if crate::runtime::telegram_approval_control(&event) {
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
        if let Ok(header) = download(platform, bridge, &event, attachment, maximum).await {
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

async fn download(
    platform: &Platform,
    bridge: &Bridge,
    event: &Event,
    attachment: &Value,
    maximum: u64,
) -> Result<String> {
    let file = platform
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
    let mut url = url::Url::parse(&platform.base)?;
    url.set_path(&format!(
        "{}/file/bot{}/{}",
        url.path().trim_end_matches('/'),
        platform.token,
        path
    ));
    let response = platform
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
