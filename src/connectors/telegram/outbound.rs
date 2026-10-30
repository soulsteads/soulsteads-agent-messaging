use crate::{
    connectors::http::{Platform, scalar},
    runtime::Event,
};
use anyhow::Result;
use serde_json::{Value, json};

#[derive(Clone, PartialEq, Eq)]
struct Entity {
    kind: &'static str,
    start: usize,
    end: usize,
    url: Option<String>,
    language: Option<String>,
}

impl Entity {
    fn json(&self, text: &str) -> Value {
        let offset = text[..self.start].encode_utf16().count();
        let length = text[self.start..self.end].encode_utf16().count();
        let mut value = json!({"type":self.kind,"offset":offset,"length":length});
        if let Some(url) = &self.url {
            value["url"] = url.clone().into();
        }
        if let Some(language) = &self.language
            && !language.is_empty()
        {
            value["language"] = language.clone().into();
        }
        value
    }
}

#[derive(Clone, PartialEq, Eq)]
struct Rendered {
    text: String,
    entities: Vec<Entity>,
}

pub(crate) struct ToolActivity {
    chunks: Vec<ToolChunk>,
}

impl ToolActivity {
    pub(crate) fn empty() -> Self {
        Self { chunks: Vec::new() }
    }
}

struct ToolChunk {
    id: String,
    rendered: Rendered,
}

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

async fn send_rendered(
    platform: &Platform,
    event: &Event,
    rendered: Rendered,
    reply_to_parent: bool,
) -> Result<String> {
    let mut last = String::new();
    for (index, chunk) in split_rendered(&rendered, 4096).into_iter().enumerate() {
        let mut body = json!({"chat_id":event.room,"text":chunk.text});
        if !chunk.entities.is_empty() {
            body["entities"] = chunk
                .entities
                .iter()
                .map(|entity| entity.json(&chunk.text))
                .collect();
        }
        if let Ok(thread) = event.thread.parse::<i64>() {
            body["message_thread_id"] = thread.into();
        } else if reply_to_parent
            && index == 0
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

pub(crate) async fn send_tool_output(
    platform: &Platform,
    event: &Event,
    presentation: &Value,
    output: &str,
) -> Result<String> {
    send_rendered(
        platform,
        event,
        rendered_tool_output(presentation, output).expect("validated tool presentation"),
        true,
    )
    .await
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

fn rendered_tool_activity(presentation: &Value) -> Option<Rendered> {
    match presentation["kind"].as_str()? {
        "tool_call" => Some(rendered_tool_literal(
            presentation["tool"].as_str().unwrap_or("unknown"),
            presentation["status"].as_str().unwrap_or("running"),
            presentation["arguments"]
                .as_str()
                .or_else(|| presentation["command"].as_str()),
        )),
        "tool_page" => Some(rendered_tool_page(presentation)),
        _ => None,
    }
}

fn append_tool_line(rendered: &mut Rendered, tool: &Value, include_literal: bool) {
    if !rendered.text.is_empty() {
        rendered.text.push('\n');
    }
    let header = crate::tools::rich_tool_header(
        tool["tool"].as_str().unwrap_or("unknown"),
        tool["status"].as_str().unwrap_or("running"),
    );
    let start = rendered.text.len();
    rendered.text.push_str(&header);
    rendered.entities.push(Entity {
        kind: "bold",
        start,
        end: rendered.text.len(),
        url: None,
        language: None,
    });
    if include_literal && let Some(literal) = tool["arguments"].as_str() {
        rendered.text.push('\n');
        let start = rendered.text.len();
        rendered.text.push_str(literal);
        rendered.entities.push(Entity {
            kind: "pre",
            start,
            end: rendered.text.len(),
            url: None,
            language: None,
        });
    }
}

fn rendered_tool_page(presentation: &Value) -> Rendered {
    let mode = presentation["mode"].as_str().unwrap_or("trace");
    let mut rendered = Rendered {
        text: String::new(),
        entities: Vec::new(),
    };
    if mode == "status" {
        if let Some(active) = presentation
            .get("active")
            .filter(|active| !active.is_null())
        {
            rendered.text.push_str("Working…\nCurrent:");
            let end = "Working…".len();
            rendered.entities.push(Entity {
                kind: "bold",
                start: 0,
                end,
                url: None,
                language: None,
            });
            rendered.text.push(' ');
            append_tool_line(&mut rendered, active, true);
            rendered.text.push_str(&format!(
                "\nCompleted: {} tools",
                presentation["completed"].as_u64().unwrap_or(0)
            ));
        } else {
            rendered.text = format!(
                "Completed: {} tools",
                presentation["completed"].as_u64().unwrap_or(0)
            );
            rendered.entities.push(Entity {
                kind: "bold",
                start: 0,
                end: rendered.text.len(),
                url: None,
                language: None,
            });
        }
    } else {
        let label = if mode == "hermes" {
            "Tool activity"
        } else {
            "Tool trace"
        };
        let page = presentation["page"].as_u64().unwrap_or(1);
        let pages = presentation["pages"].as_u64().unwrap_or(1);
        let working = presentation["entries"]
            .as_array()
            .into_iter()
            .flatten()
            .any(|entry| matches!(entry["status"].as_str(), Some("pending" | "running")));
        let phase = if working {
            "working"
        } else if page == pages && presentation["active"].is_null() {
            "completed"
        } else {
            "continued"
        };
        let title = if pages > 1 {
            format!("{label} (part {page}/{pages}, {phase})")
        } else {
            format!("{label} ({phase})")
        };
        rendered.text.push_str(&title);
        rendered.entities.push(Entity {
            kind: "bold",
            start: 0,
            end: title.len(),
            url: None,
            language: None,
        });
        for entry in presentation["entries"].as_array().into_iter().flatten() {
            append_tool_line(&mut rendered, entry, true);
        }
    }
    rendered
}

fn rendered_tool_output(presentation: &Value, output: &str) -> Option<Rendered> {
    (presentation["kind"] == "tool_output").then(|| {
        rendered_tool_literal(
            presentation["tool"].as_str().unwrap_or("unknown"),
            "output",
            Some(output),
        )
    })
}

fn rendered_tool_literal(tool: &str, status: &str, literal: Option<&str>) -> Rendered {
    let header = crate::tools::rich_tool_header(tool, status);
    let literal = literal.filter(|value| !value.is_empty());
    let text = literal
        .map(|value| format!("{header}\n{value}"))
        .unwrap_or_else(|| header.clone());
    let mut entities = vec![Entity {
        kind: "bold",
        start: 0,
        end: header.len(),
        url: None,
        language: None,
    }];
    if let Some(value) = literal {
        entities.push(Entity {
            kind: "pre",
            start: header.len() + 1,
            end: header.len() + 1 + value.len(),
            url: None,
            language: None,
        });
    }
    Rendered { text, entities }
}

pub(crate) async fn sync_tool_activity(
    platform: &Platform,
    event: &Event,
    activity: &mut ToolActivity,
    presentation: &Value,
) -> Result<()> {
    let wanted = split_rendered(
        &rendered_tool_activity(presentation).expect("validated tool presentation"),
        4096,
    );
    let shared = activity.chunks.len().min(wanted.len());
    for (existing, rendered) in activity.chunks[..shared].iter_mut().zip(&wanted[..shared]) {
        if existing.rendered != *rendered {
            edit_tool_chunk(platform, event, &existing.id, rendered).await?;
            existing.rendered = rendered.clone();
        }
    }
    for rendered in &wanted[shared..] {
        let id = send_tool_chunk(platform, event, rendered).await?;
        activity.chunks.push(ToolChunk {
            id,
            rendered: rendered.clone(),
        });
    }
    while activity.chunks.len() > wanted.len() {
        let obsolete = activity.chunks.last().unwrap();
        platform
            .api(
                "POST",
                "deleteMessage",
                json!({"chat_id":event.room,"message_id":obsolete.id.parse::<i64>()?}),
            )
            .await?;
        activity.chunks.pop();
    }
    Ok(())
}

async fn send_tool_chunk(platform: &Platform, event: &Event, chunk: &Rendered) -> Result<String> {
    let mut body = json!({"chat_id":event.room,"text":chunk.text});
    if !chunk.entities.is_empty() {
        body["entities"] = chunk
            .entities
            .iter()
            .map(|entity| entity.json(&chunk.text))
            .collect();
    }
    if let Ok(thread) = event.thread.parse::<i64>() {
        body["message_thread_id"] = thread.into();
    }
    let response = platform.api("POST", "sendMessage", body).await?;
    Ok(scalar(&response["message_id"]))
}

async fn edit_tool_chunk(
    platform: &Platform,
    event: &Event,
    id: &str,
    chunk: &Rendered,
) -> Result<()> {
    let mut body = json!({"chat_id":event.room,"message_id":id.parse::<i64>()?,"text":chunk.text});
    if !chunk.entities.is_empty() {
        body["entities"] = chunk
            .entities
            .iter()
            .map(|entity| entity.json(&chunk.text))
            .collect();
    }
    platform.api("POST", "editMessageText", body).await?;
    Ok(())
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

fn split_rendered(rendered: &Rendered, limit: usize) -> Vec<Rendered> {
    if rendered.text.is_empty() {
        return Vec::new();
    }
    let mut chunks = Vec::new();
    let mut start = 0;
    while start < rendered.text.len() {
        let remaining = &rendered.text[start..];
        let mut units = 0;
        let mut boundary = remaining.len();
        for (offset, character) in remaining.char_indices() {
            if units + character.len_utf16() > limit {
                boundary = offset;
                break;
            }
            units += character.len_utf16();
        }
        let mut end = start + boundary;
        if end < rendered.text.len()
            && let Some(newline) = rendered.text[start..end].rfind('\n')
            && newline > 0
        {
            end = start + newline + 1;
        }
        if end == start {
            end = start + boundary;
        }
        let text = rendered.text[start..end].to_owned();
        let entities = rendered
            .entities
            .iter()
            .filter_map(|entity| {
                let entity_start = entity.start.max(start);
                let entity_end = entity.end.min(end);
                (entity_start < entity_end).then(|| Entity {
                    kind: entity.kind,
                    start: entity_start - start,
                    end: entity_end - start,
                    url: entity.url.clone(),
                    language: entity.language.clone(),
                })
            })
            .collect();
        chunks.push(Rendered { text, entities });
        start = end;
    }
    chunks
}
