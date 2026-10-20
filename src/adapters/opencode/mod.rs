//! OpenCode's native ACP server, with session-scoped transport compatibility.
mod acp;
use anyhow::Result;

pub async fn run() -> Result<()> {
    if super::serve_mcp_transport_if_requested().await? {
        return Ok(());
    }
    acp::run().await
}

pub mod native_client;
pub mod native_events;
pub mod native_queue;
#[cfg(test)]
mod native_runtime_tests;
pub mod native_server;
