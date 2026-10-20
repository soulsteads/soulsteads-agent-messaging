//! Rust ACP agents backed by the locally installed coding CLIs.
pub mod claude;
pub mod codex;
pub(crate) mod launch;
mod mcp_sse;
pub mod opencode;
pub mod pi;
mod server;

/// Internal MCP transport entry used by both standalone and builtin adapters.
pub async fn serve_mcp_transport_if_requested() -> Result<bool> {
    if !mcp_sse::requested() {
        return Ok(false);
    }
    mcp_sse::run().await?;
    Ok(true)
}

use agent_client_protocol::{Client, ConnectionTo, UntypedMessage};
use anyhow::Result;
use serde_json::{Value, json};

pub(crate) type AcpConnection = ConnectionTo<Client>;

pub(crate) fn update(connection: &AcpConnection, session: &str, update: Value) -> Result<()> {
    if update.to_string().len() > 2 * 1024 * 1024 - 1024 {
        anyhow::bail!("ACP update exceeded frame limit");
    }
    connection.send_notification(UntypedMessage::new(
        "session/update",
        json!({"sessionId":session,"update":update}),
    )?)?;
    Ok(())
}

pub(crate) fn text(
    connection: &AcpConnection,
    session: &str,
    kind: &str,
    text: &str,
) -> Result<()> {
    let mut remaining = text;
    while !remaining.is_empty() {
        let mut end = remaining.len().min(8192);
        while !remaining.is_char_boundary(end) {
            end -= 1;
        }
        update(
            connection,
            session,
            json!({"sessionUpdate":kind,"content":{"type":"text","text":&remaining[..end]}}),
        )?;
        remaining = &remaining[end..];
    }
    Ok(())
}

pub(crate) fn protocol_error(error: impl std::fmt::Display) -> agent_client_protocol::Error {
    let message = error.to_string();
    let mut end = message.len().min(8192);
    while !message.is_char_boundary(end) {
        end -= 1;
    }
    agent_client_protocol::Error::new(-32603, message[..end].to_owned())
}

pub(crate) fn bounded(value: &str, maximum: usize) -> String {
    if value.len() <= maximum {
        return value.into();
    }
    let mut end = maximum;
    while !value.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}\n[output truncated by ACP adapter]", &value[..end])
}

#[derive(Clone, Copy, Debug, clap::ValueEnum)]
pub enum Kind {
    Opencode,
    Codex,
    Claude,
    Pi,
}
impl Kind {
    pub fn name(self) -> &'static str {
        match self {
            Self::Opencode => "opencode",
            Self::Codex => "codex",
            Self::Claude => "claude",
            Self::Pi => "pi",
        }
    }
    pub async fn serve(self) -> Result<()> {
        match self {
            Self::Opencode => opencode::run().await,
            Self::Codex => codex::run().await,
            Self::Claude => claude::run().await,
            Self::Pi => pi::run().await,
        }
    }
}
