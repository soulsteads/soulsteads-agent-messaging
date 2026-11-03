use crate::{
    connectors::http::{Platform, scalar},
    runtime::Event,
};
use anyhow::Result;
use serde_json::json;

pub(crate) async fn send(platform: &Platform, event: &Event, text: &str) -> Result<String> {
    let mut last = String::new();
    for (index, text) in split_lines(text, 4096).into_iter().enumerate() {
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
        let response = match platform.api("POST", "sendMessage", body.clone()).await {
            Err(_) if body.get("reply_to_message_id").is_some() => {
                let mut retry = body;
                retry.as_object_mut().unwrap().remove("reply_to_message_id");
                platform.api("POST", "sendMessage", retry).await?
            }
            result => result?,
        };
        last = scalar(&response["message_id"]);
    }
    Ok(last)
}

pub(crate) async fn edit(platform: &Platform, event: &Event, id: &str, text: &str) -> Result<()> {
    platform
        .api(
            "POST",
            "editMessageText",
            json!({"chat_id":event.room,"message_id":id.parse::<i64>()?,"text":text}),
        )
        .await?;
    Ok(())
}

pub(crate) async fn send_activity(
    platform: &Platform,
    event: &Event,
    text: &str,
) -> Result<String> {
    let mut body = json!({"chat_id":event.room,"text":text});
    if let Ok(thread) = event.thread.parse::<i64>() {
        body["message_thread_id"] = thread.into();
    }
    let result = platform.api("POST", "sendMessage", body).await?;
    Ok(scalar(&result["message_id"]))
}

pub(crate) async fn typing(platform: &Platform, event: &Event) {
    let _ = platform.api("POST", "sendChatAction", json!({"chat_id":event.room,"action":"typing","message_thread_id":event.thread.parse::<i64>().ok()})).await;
}

pub(crate) async fn upload(
    platform: &Platform,
    event: &Event,
    name: &str,
    bytes: Vec<u8>,
) -> Result<()> {
    let client = platform.client()?;
    platform
        .upload_multipart(&client, "sendDocument", name, bytes, |part| {
            let form = reqwest::multipart::Form::new()
                .text("chat_id", event.room.clone())
                .part("document", part);
            if !event.thread.is_empty() {
                form.text("message_thread_id", event.thread.clone())
            } else {
                form
            }
        })
        .await?;
    Ok(())
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
