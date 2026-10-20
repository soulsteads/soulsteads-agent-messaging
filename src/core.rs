use crate::{
    config::{boolean, string, strings},
    connectors::http::scalar,
};
use serde_json::{Value, json};
pub fn normalize(platform: &str, input: &Value, isolated: bool) -> Value {
    let context = |room: &str, thread: &str| {
        if isolated && !thread.is_empty() {
            format!("{room}:{thread}")
        } else {
            room.into()
        }
    };
    let fallback = |key: &str| {
        if string(input, key).is_empty() {
            "unknown"
        } else {
            string(input, key)
        }
    };
    match platform {
        "matrix" => {
            let room = string(input, "roomId");
            let id = string(input, "eventId");
            let thread = string(input, "threadRootEventId");
            let root = if thread.is_empty() { id } else { thread };
            json!({"roomId":room,"sender":fallback("sender"),"text":string(input,"text"),"eventId":id,"threadRootEventId":thread,"replyThreadRootId":root,"sessionId":context(room,root),"dedupeId":id})
        }
        "mattermost" => {
            let room = string(input, "channelId");
            let id = string(input, "postId");
            let thread = string(input, "rootId");
            let root = if thread.is_empty() { id } else { thread };
            json!({"channelId":room,"userId":fallback("userId"),"text":string(input,"text"),"postId":id,"rootId":thread,"replyRootId":root,"sessionId":context(room,root),"dedupeId":format!("{room}:{id}")})
        }
        "slack" => {
            let room = string(input, "channelId");
            let id = string(input, "eventTs");
            let thread = string(input, "threadTs");
            let root = if thread.is_empty() { id } else { thread };
            let team = if string(input, "teamId").is_empty() {
                format!("ch_{room}")
            } else {
                string(input, "teamId").into()
            };
            let mut result = json!({"teamId":team,"channelId":room,"userId":fallback("userId"),"text":string(input,"text"),"eventTs":id,"replyThreadTs":root,"contextId":format!("{room}:{root}"),"dedupeId":format!("{room}:{id}")});
            if let Some(thread) = input.get("threadTs") {
                result["threadTs"] = thread.clone();
            }
            result
        }
        "telegram" => {
            let room = scalar(&input["chat"]["id"]);
            let user = scalar(&input["from"]["id"]);
            let thread =
                if input["is_topic_message"] == true && input["message_thread_id"].is_number() {
                    input["message_thread_id"].clone()
                } else {
                    Value::Null
                };
            let reply = input["reply_to_message"]["message_id"].clone();
            let reply_user = if input["reply_to_message"]["from"].get("id").is_some() {
                json!(scalar(&input["reply_to_message"]["from"]["id"]))
            } else {
                Value::Null
            };
            let session = if isolated && !thread.is_null() {
                format!("{room}:{thread}")
            } else {
                room.clone()
            };
            json!({"chatId":room,"userId":user,"messageId":input["messageId"],"text":string(input,"text").trim(),"messageThreadId":thread,"replyToMessageId":reply,"replyToMessageFromId":reply_user,"replyToMessageIsBot":input["reply_to_message"]["from"]["is_bot"]==true,"chatType":input["chat"]["type"],"isForumTopic":input["chat"]["is_forum"]==true&&input["is_topic_message"]==true,"isPrivate":input["chat"]["type"]=="private","sessionId":session,"dedupeId":format!("{room}:{}",input["messageId"])})
        }
        _ => Value::Null,
    }
}
pub fn admission(kind: &str, input: &Value) -> bool {
    if kind == "telegramAttachmentBypass" {
        return boolean(input, "hasAttachments")
            && (boolean(input, "isPrivate")
                || (boolean(input, "hasActiveSession")
                    && (boolean(input, "isReplyToThisBot")
                        || (boolean(input, "implicitTopicRepliesEnabled")
                            && boolean(input, "threadIsolation")
                            && !input["messageThreadId"].is_null()))));
    }
    if kind == "telegramBotReply" {
        return boolean(input, "enabled")
            && !string(input, "text").is_empty()
            && boolean(input, "replyToMessageIsBot")
            && !input["replyToMessageFromId"].is_null()
            && scalar(&input["ourBotId"]) == scalar(&input["replyToMessageFromId"]);
    }
    if input["enabled"] == false {
        return false;
    }
    let text = string(input, "text").trim();
    let lower = text.to_lowercase();
    let trigger = string(input, "trigger").to_lowercase();
    if text.is_empty() {
        return false;
    }
    match kind {
        "slackAdmission" => {
            !string(input, "threadTs").is_empty()
                && !strings(
                    &json!({"values":["bot_message","message_changed","message_deleted"]}),
                    "values",
                )
                .contains(&string(input, "subtype").into())
                && string(input, "botId").is_empty()
                && !lower.starts_with(&format!("{trigger} "))
                && !regex::Regex::new(r"^<@[A-Z0-9]+>").unwrap().is_match(text)
        }
        "mattermostAdmission" => {
            !string(input, "rootId").is_empty()
                && !lower.starts_with(&trigger)
                && !text.starts_with(&format!("@{}", string(input, "botUsername")))
        }
        "telegramImplicit" => {
            !boolean(input, "isPrivate")
                && !input["messageThreadId"].is_null()
                && lower != trigger
                && !lower.starts_with(&format!("{trigger} "))
                && (string(input, "botUsername").is_empty() || {
                    let mention = format!("@{}", string(input, "botUsername").to_lowercase());
                    lower != mention && !lower.starts_with(&format!("{mention} "))
                })
        }
        _ => false,
    }
}
pub fn attachments(input: &Value) -> Value {
    let mut output = vec![];
    if let Some(photos) = input["photo"].as_array() {
        let valid: Vec<_> = photos
            .iter()
            .filter(|item| item.is_object() && !scalar(&item["file_id"]).is_empty())
            .collect();
        let sized = valid
            .iter()
            .filter(|item| item["file_size"].is_number())
            .max_by(|left, right| {
                left["file_size"]
                    .as_f64()
                    .unwrap()
                    .total_cmp(&right["file_size"].as_f64().unwrap())
            });
        if let Some(photo) = sized.copied().or_else(|| valid.last().copied()) {
            let mut item =
                json!({"kind":"photo","fileId":scalar(&photo["file_id"]),"mimeType":"image/jpeg"});
            if photo["file_size"].is_number() {
                item["size"] = photo["file_size"].clone();
            }
            output.push(item);
        }
    }
    for (kind, mime, filename) in [
        ("document", "application/octet-stream", true),
        ("video", "video/mp4", true),
        ("video_note", "video/mp4", false),
        ("audio", "audio/mpeg", true),
        ("voice", "audio/ogg", false),
        ("animation", "video/mp4", true),
        ("sticker", "image/webp", false),
    ] {
        let file = &input[kind];
        if !file.is_object() || scalar(&file["file_id"]).is_empty() {
            continue;
        }
        let mut item = json!({"kind":kind,"fileId":scalar(&file["file_id"]),"mimeType":file["mime_type"].as_str().unwrap_or(mime)});
        if file["file_size"].is_number() {
            item["size"] = file["file_size"].clone();
        }
        if filename && file["file_name"].is_string() {
            item["fileName"] = file["file_name"].clone();
        }
        output.push(item);
    }
    json!(output)
}
pub fn extension(value: &Value, fallback: &str) -> String {
    let sanitized: String = value
        .as_str()
        .unwrap_or("")
        .to_lowercase()
        .chars()
        .filter(|character| character.is_ascii_alphanumeric())
        .take(16)
        .collect();
    if sanitized.is_empty() {
        fallback.into()
    } else {
        sanitized
    }
}
pub fn media(text: &str, document: bool) -> (Vec<String>, String) {
    let name = if document { "DOC" } else { "IMAGE" };
    let marker = regex::Regex::new(&format!(
        r"(?i)\[DOCLIBRARY_{name}\]([^\[]+)\[/DOCLIBRARY_{name}\]"
    ))
    .unwrap();
    let mut paths = vec![];
    for captures in marker.captures_iter(text) {
        let path = captures[1].trim().to_owned();
        if !path.is_empty() && !paths.contains(&path) {
            paths.push(path);
        }
    }
    let mut clean = marker.replace_all(text, "").into_owned();
    if !document {
        let pattern =
            regex::Regex::new(r"(?i)Path:\s*(/[^\s\n]+\.(?:png|jpg|jpeg|gif|webp))").unwrap();
        for captures in pattern.captures_iter(text) {
            let path = captures[1].trim().to_owned();
            if !paths.contains(&path) {
                paths.push(path);
            }
        }
        clean = regex::Regex::new(r"(?i)Path:\s*/[^\s\n]+\.(?:png|jpg|jpeg|gif|webp)\n?")
            .unwrap()
            .replace_all(&clean, "")
            .into_owned();
    }
    (paths, clean.trim().into())
}
pub fn sanitize(text: &str) -> String {
    if regex::Regex::new(r"(?i)https?://").unwrap().is_match(text) {
        text.into()
    } else {
        regex::Regex::new(r"/(?:[\w.-]+/)+([^/\s]+\.[a-zA-Z0-9]+)")
            .unwrap()
            .replace_all(text, "$1")
            .into_owned()
    }
}
pub fn session_status(
    message_count: usize,
    input_chars: usize,
    output_chars: usize,
    age: std::time::Duration,
    idle: std::time::Duration,
) -> String {
    fn grouped(value: usize) -> String {
        let digits = value.to_string();
        let mut output = String::new();
        for (index, character) in digits.chars().enumerate() {
            if index > 0 && (digits.len() - index).is_multiple_of(3) {
                output.push(',');
            }
            output.push(character);
        }
        output
    }
    let input_tokens = input_chars / 4 + usize::from(input_chars % 4 >= 2);
    let output_tokens = output_chars / 4 + usize::from(output_chars % 4 >= 2);
    let total_tokens = input_tokens + output_tokens;
    let context_percent = total_tokens as f64 / 200_000.0 * 100.0;
    let fractional_percent = context_percent.fract();
    let context_percent = if fractional_percent == 0.125 || fractional_percent == 0.625 {
        format!(
            "{:.0}.{:02}",
            context_percent.trunc(),
            (fractional_percent * 100.0).ceil() as usize
        )
    } else {
        format!("{context_percent:.2}")
    };
    let age = (age.as_secs_f64() / 60.0).round();
    let idle = (idle.as_secs_f64() / 60.0).round();
    format!(
        "Session status:\n- Messages: {message_count}\n- Age: {age} min | Last active: {idle} min ago\n- Tokens (est): ~{} ({context_percent}% of 200k)\n  Input: ~{} | Output: ~{}\nNote: OpenCode auto-compacts when context fills",
        grouped(total_tokens),
        grouped(input_tokens),
        grouped(output_tokens)
    )
}

pub fn diagnostic(acp: &str, bridge: &str, clean: &str) -> Value {
    diagnostic_counts(
        acp.encode_utf16().count(),
        bridge.encode_utf16().count(),
        clean.encode_utf16().count(),
    )
}
pub fn diagnostic_counts(acp: usize, bridge: usize, clean: usize) -> Value {
    if clean > 0 {
        return Value::Null;
    }
    json!({"source":if acp==0&&bridge==0{"acp-no-text"}else if bridge==0{"bridge-capture-lost"}else{"bridge-processing-removed"},"acpChars":acp,"bridgeChars":bridge,"cleanChars":clean})
}
pub fn name_query(text: &str, name: &str) -> Option<String> {
    let name = name.trim();
    if name.is_empty() {
        return None;
    }
    for prefix in [format!("@{name}"), name.into()] {
        if let Some(start) = text.get(..prefix.len())
            && start.eq_ignore_ascii_case(&prefix)
            && let Some(separator) = text[prefix.len()..].chars().next()
            && (separator == ':' || separator.is_whitespace())
        {
            return Some(
                text[prefix.len()..]
                    .trim_start_matches(|character: char| {
                        character == ':' || character.is_whitespace()
                    })
                    .trim()
                    .into(),
            );
        }
    }
    None
}
pub fn room_allowed(input: &Value, room: &str) -> bool {
    let rooms = strings(input, "allowedRooms");
    if boolean(input, "strictAllowedRooms") || !rooms.is_empty() {
        rooms.iter().any(|allowed| allowed == room)
    } else {
        true
    }
}
