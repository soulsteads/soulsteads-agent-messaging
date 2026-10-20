use anyhow::{Result, bail};
use serde_json::Value;
use std::collections::HashSet;
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Update {
    TextDelta { message_id: String, text: String },
    Tool { kind: String, data: Value },
    Finished,
    Failed,
    Interrupted,
    Reconcile { expected: u64, received: u64 },
    ReplayWindowExceeded,
}
pub struct EventProjector {
    session: String,
    seen: HashSet<String>,
    exhausted: bool,
    durable_seq: Option<u64>,
}
impl EventProjector {
    pub fn new(session: impl Into<String>) -> Self {
        Self {
            session: session.into(),
            seen: HashSet::new(),
            exhausted: false,
            durable_seq: None,
        }
    }
    pub fn dedup_len(&self) -> usize {
        self.seen.len()
    }
    pub fn push(&mut self, event: Value) -> Vec<Update> {
        let data = &event["data"];
        if data.get("sessionID").and_then(Value::as_str) != Some(self.session.as_str()) {
            return vec![];
        }
        if self.exhausted {
            return vec![];
        }
        let Some(id) = event.get("id").and_then(Value::as_str) else {
            return vec![];
        };
        if id.len() > 160 {
            return vec![];
        }
        if self.seen.contains(id) {
            return vec![];
        }
        if self.seen.len() >= 4096 {
            self.exhausted = true;
            return vec![Update::ReplayWindowExceeded];
        }
        self.seen.insert(id.into());
        let mut out = vec![];
        if let Some(seq) = event["durable"]["seq"].as_u64() {
            if let Some(last) = self.durable_seq {
                if seq > last.saturating_add(1) {
                    out.push(Update::Reconcile {
                        expected: last.saturating_add(1),
                        received: seq,
                    })
                }
                if seq <= last {
                    return out;
                }
            }
            self.durable_seq = Some(seq)
        }
        match event["type"].as_str().unwrap_or("") {
            "session.text.delta" => {
                if let (Some(mid), Some(text)) =
                    (data["assistantMessageID"].as_str(), data["delta"].as_str())
                {
                    out.push(Update::TextDelta {
                        message_id: mid.into(),
                        text: text.into(),
                    })
                }
            }
            kind @ ("session.tool.input.started"
            | "session.tool.input.delta"
            | "session.tool.input.ended"
            | "session.tool.called"
            | "session.tool.progress"
            | "session.tool.success"
            | "session.tool.failed") => {
                if data["id"].is_string() && data["assistantMessageID"].is_string() {
                    out.push(Update::Tool {
                        kind: kind.into(),
                        data: data.clone(),
                    });
                }
            }
            "session.execution.succeeded" => out.push(Update::Finished),
            "session.execution.failed" => out.push(Update::Failed),
            "session.execution.interrupted" => out.push(Update::Interrupted),
            _ => {}
        }
        out
    }
}
pub struct SseDecoder {
    buf: Vec<u8>,
    max_frame: usize,
}
impl SseDecoder {
    pub fn new(max_frame: usize) -> Self {
        Self {
            buf: Vec::new(),
            max_frame: max_frame.max(1),
        }
    }
    pub fn feed(&mut self, bytes: &[u8]) -> Result<Vec<Value>> {
        self.feed_checked(bytes)
    }
    pub fn feed_checked(&mut self, bytes: &[u8]) -> Result<Vec<Value>> {
        let mut out = vec![];
        for &b in bytes {
            self.buf.push(b);
            if self.buf.len() > self.max_frame {
                self.buf.clear();
                bail!("Native SSE frame exceeded limit");
            }
            if self.buf.ends_with(b"\n\n") || self.buf.ends_with(b"\r\n\r\n") {
                let frame = std::mem::take(&mut self.buf);
                let text = std::str::from_utf8(&frame)
                    .map_err(|_| anyhow::anyhow!("Invalid native SSE UTF-8"))?;
                let payload = text
                    .lines()
                    .filter_map(|l| l.strip_prefix("data:"))
                    .map(str::trim_start)
                    .collect::<Vec<_>>()
                    .join("\n");
                if !payload.is_empty() {
                    let event: Value = serde_json::from_str(&payload)
                        .map_err(|_| anyhow::anyhow!("Invalid native SSE JSON"))?;
                    if !event["id"]
                        .as_str()
                        .is_some_and(|id| !id.is_empty() && id.len() <= 160)
                        || !event["type"]
                            .as_str()
                            .is_some_and(|kind| !kind.is_empty() && kind.len() <= 256)
                        || !event["data"].is_object()
                    {
                        bail!("Invalid native SSE envelope");
                    }
                    out.push(event);
                }
            }
        }
        Ok(out)
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DurableMessage {
    pub id: String,
    pub role: String,
    pub text: String,
}
pub fn durable_messages(snapshot: &Value, session: &str) -> Option<Vec<DurableMessage>> {
    let rows = snapshot.get("data")?.as_array()?;
    let mut result = rows
        .iter()
        .filter_map(|m| {
            if m.get("sessionID")
                .and_then(Value::as_str)
                .is_some_and(|s| s != session)
            {
                return None;
            }
            let role = m.get("type")?.as_str()?;
            if role != "user" && role != "assistant" {
                return None;
            }
            let id = m.get("id")?.as_str()?.into();
            let text = if role == "user" {
                m.get("text").and_then(Value::as_str).unwrap_or("").into()
            } else {
                m.get("content")?
                    .as_array()?
                    .iter()
                    .filter(|p| p["type"] == "text")
                    .filter_map(|p| p["text"].as_str())
                    .collect::<String>()
            };
            Some((
                m["time"]["created"].as_i64().unwrap_or(0),
                DurableMessage {
                    id,
                    role: role.into(),
                    text,
                },
            ))
        })
        .collect::<Vec<_>>();
    result.sort_by_key(|(t, _)| *t);
    Some(result.into_iter().map(|(_, m)| m).collect())
}
