//! Stdio transport for the owned live MCP server.
use super::live_mcp::{append, response};
use serde_json::Value;
use std::path::PathBuf;
pub fn run() {
    use std::io::{BufRead, Write};
    let audit = PathBuf::from(std::env::var("ADAPTER_LIVE_MCP_AUDIT").unwrap());
    if let Ok(value) = std::env::var("ADAPTER_LIVE_LITERAL") {
        append(&audit, &serde_json::json!({"literalEnv":value})).unwrap();
    }
    for line in std::io::stdin().lock().lines() {
        let request: Value = serde_json::from_str(&line.unwrap()).unwrap();
        append(&audit, &request).unwrap();
        if let Some(response) = response(&request) {
            let mut stdout = std::io::stdout().lock();
            writeln!(stdout, "{response}").unwrap();
            stdout.flush().unwrap();
        }
    }
}
