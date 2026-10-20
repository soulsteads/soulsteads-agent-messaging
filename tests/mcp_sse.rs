//! Exercise the shared compatibility transport through its actual process entry points.
use anyhow::{Context, Result, ensure};
use serde_json::{Value, json};
use std::{path::Path, time::Duration};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
#[path = "support/live_mcp.rs"]
mod live_mcp;
#[path = "support/live_mcp_http.rs"]
mod live_mcp_http;

async fn round_trip(binary: &str) -> Result<()> {
    let root = tempfile::tempdir()?;
    let audit = root.path().join("sse-audit.jsonl");
    let http = live_mcp_http::Http::start(audit.clone()).await?;
    let config = json!({"url":format!("{}/sse",http.url),"headers":[{"name":"X-Adapter-Probe","value":"owned"}]});
    let mut child = tokio::process::Command::new(binary)
        .arg("--mcp-sse-proxy")
        .env("SOULSTEADS_MCP_SSE_CONFIG", config.to_string())
        .kill_on_drop(true)
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()?;
    let mut stdin = child.stdin.take().context("stdin")?;
    let mut stdout = BufReader::new(child.stdout.take().context("stdout")?);
    for (id, method, params) in [
        (
            1,
            "initialize",
            json!({"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"owned","version":"1"}}),
        ),
        (2, "tools/list", json!({})),
        (
            3,
            "tools/call",
            json!({"name":"probe_echo","arguments":{"value":"SSE_ROUNDTRIP_OK"}}),
        ),
        (
            4,
            "tools/call",
            json!({"name":"probe_image","arguments":{}}),
        ),
    ] {
        stdin
            .write_all(
                format!(
                    "{}\n",
                    json!({"jsonrpc":"2.0","id":id,"method":method,"params":params})
                )
                .as_bytes(),
            )
            .await?;
        stdin.flush().await?;
        let mut line = String::new();
        tokio::time::timeout(Duration::from_secs(5), stdout.read_line(&mut line)).await??;
        let response: Value = serde_json::from_str(&line)?;
        ensure!(
            response["id"] == id && response["error"].is_null(),
            "Unexpected response: {response}"
        );
        if id == 3 {
            ensure!(
                response.to_string().contains("SSE_ROUNDTRIP_OK"),
                "Echo bytes lost"
            );
        }
        if id == 4 {
            ensure!(
                response["result"]["content"]
                    .as_array()
                    .is_some_and(|blocks| blocks.iter().any(|block| block["type"] == "image")),
                "Image lost"
            );
        }
    }
    drop(stdin);
    ensure!(
        tokio::time::timeout(Duration::from_secs(5), child.wait())
            .await??
            .success(),
        "SSE proxy did not exit successfully on EOF"
    );
    let calls = std::fs::read_to_string(audit)?
        .lines()
        .map(|line| serde_json::from_str::<Value>(line).unwrap())
        .collect::<Vec<_>>();
    ensure!(
        calls
            .iter()
            .any(|call| call["transport"] == "sse_connect" && call["probeHeader"] == "owned"),
        "GET headers lost"
    );
    ensure!(
        calls
            .iter()
            .filter(|call| call["transport"] == "sse")
            .all(|call| call["probeHeader"] == "owned"),
        "POST headers lost"
    );
    Ok(())
}
#[tokio::test]
async fn standalone_and_builtin_sse_transport_forward_json_headers_and_exit_on_eof() {
    for binary in [
        env!("CARGO_BIN_EXE_pi-acp-rust"),
        env!("CARGO_BIN_EXE_opencode-acp-rust"),
        env!("CARGO_BIN_EXE_soulsteads-messaging"),
    ] {
        round_trip(binary).await.unwrap();
    }
}

async fn reject_stream(root: &Path, data: String, expected: &str) -> Result<()> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let url = format!("http://{}/sse", listener.local_addr()?);
    let task = tokio::spawn(async move {
        use axum::{Router, routing::get};
        let app = Router::new().route(
            "/sse",
            get(move || async move {
                (
                    [(axum::http::header::CONTENT_TYPE, "text/event-stream")],
                    data,
                )
            }),
        );
        let _ = axum::serve(listener, app).await;
    });
    let mut child = tokio::process::Command::new(env!("CARGO_BIN_EXE_pi-acp-rust"))
        .current_dir(root)
        .arg("--mcp-sse-proxy")
        .env(
            "SOULSTEADS_MCP_SSE_CONFIG",
            json!({"url":url,"headers":[]}).to_string(),
        )
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .kill_on_drop(true)
        .spawn()?;
    let _stdin = child.stdin.take();
    let result = tokio::time::timeout(Duration::from_secs(5), child.wait_with_output()).await;
    task.abort();
    let output = result??;
    ensure!(!output.status.success(), "Malformed server accepted");
    ensure!(
        String::from_utf8_lossy(&output.stderr).contains(expected),
        "Unexpected error: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    Ok(())
}
#[tokio::test]
async fn sse_transport_rejects_cross_origin_and_missing_endpoint() {
    let root = tempfile::tempdir().unwrap();
    reject_stream(
        root.path(),
        "event: endpoint\ndata: https://example.invalid/messages\n\n".into(),
        "changed origin",
    )
    .await
    .unwrap();
    reject_stream(
        root.path(),
        ": keepalive\n\n".into(),
        "closed before endpoint",
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn sse_transport_eof_stops_pending_connection() {
    use tokio::io::AsyncReadExt;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/sse", listener.local_addr().unwrap());
    let (accepted, ready) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut bytes = [0; 1024];
        assert!(socket.read(&mut bytes).await.unwrap() > 0);
        let _ = accepted.send(());
        std::future::pending::<()>().await;
    });
    let mut child = tokio::process::Command::new(env!("CARGO_BIN_EXE_pi-acp-rust"))
        .arg("--mcp-sse-proxy")
        .env(
            "SOULSTEADS_MCP_SSE_CONFIG",
            json!({"url":url,"headers":[]}).to_string(),
        )
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::inherit())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let mut stdin = child.stdin.take().unwrap();
    stdin
        .write_all(b"{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"initialize\"}\n")
        .await
        .unwrap();
    stdin.flush().await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), ready)
        .await
        .unwrap()
        .unwrap();
    drop(stdin);
    let status = tokio::time::timeout(Duration::from_secs(2), child.wait()).await;
    task.abort();
    assert!(status.unwrap().unwrap().success());
}
