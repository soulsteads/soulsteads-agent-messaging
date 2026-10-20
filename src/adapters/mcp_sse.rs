//! Owned stdio transport for legacy SSE MCP servers.
use anyhow::{Context, Result, bail, ensure};
use futures_util::StreamExt;
use serde_json::{Value, json};
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
const LIMIT: usize = 8 * 1024 * 1024;
const CONFIG: &str = "SOULSTEADS_MCP_SSE_CONFIG";

pub(crate) fn requested() -> bool {
    std::env::args().nth(1).as_deref() == Some("--mcp-sse-proxy")
}
pub(crate) fn adapt(server: &Value) -> Result<Value> {
    if server["type"] != "sse" {
        return Ok(server.clone());
    }
    let url = server["url"].as_str().context("MCP SSE URL missing")?;
    let parsed = url::Url::parse(url)?;
    ensure!(
        matches!(parsed.scheme(), "http" | "https"),
        "Unsupported MCP SSE URL scheme"
    );
    headers(&server["headers"])?;
    let config = json!({"url":url,"headers":server["headers"]});
    ensure!(
        config.to_string().len() <= 1024 * 1024,
        "MCP SSE configuration exceeded limit"
    );
    Ok(
        json!({"name":server["name"],"command":std::env::current_exe()?,"args":["--mcp-sse-proxy"],"env":[{"name":CONFIG,"value":config.to_string()}]}),
    )
}
fn headers(values: &Value) -> Result<reqwest::header::HeaderMap> {
    ensure!(
        values.is_null() || values.is_array(),
        "MCP SSE headers must be an array"
    );
    let mut headers = reqwest::header::HeaderMap::new();
    for pair in values.as_array().into_iter().flatten() {
        let name = reqwest::header::HeaderName::from_bytes(
            pair["name"]
                .as_str()
                .context("MCP header name missing")?
                .as_bytes(),
        )?;
        ensure!(!headers.contains_key(&name), "Duplicate MCP header");
        headers.insert(
            name,
            reqwest::header::HeaderValue::from_str(
                pair["value"].as_str().context("MCP header value missing")?,
            )?,
        );
    }
    Ok(headers)
}
#[derive(Default)]
struct Events {
    buffer: Vec<u8>,
    kind: String,
    data: String,
}
impl Events {
    fn feed(&mut self, bytes: &[u8]) -> Result<Vec<(String, String)>> {
        ensure!(
            self.buffer.len() + bytes.len() <= LIMIT,
            "MCP SSE frame exceeded limit"
        );
        self.buffer.extend(bytes);
        let mut events = Vec::new();
        while let Some(end) = self.buffer.iter().position(|byte| *byte == b'\n') {
            let line = self.buffer.drain(..=end).collect::<Vec<_>>();
            let line = std::str::from_utf8(&line)?.trim_end_matches(['\r', '\n']);
            if line.is_empty() {
                if !self.data.is_empty() {
                    events.push((
                        if self.kind.is_empty() {
                            "message".into()
                        } else {
                            std::mem::take(&mut self.kind)
                        },
                        std::mem::take(&mut self.data),
                    ));
                }
                self.kind.clear();
            } else if let Some(kind) = line.strip_prefix("event:") {
                self.kind = kind.trim_start().into();
            } else if let Some(data) = line.strip_prefix("data:") {
                ensure!(
                    self.data.len() + data.len() < LIMIT,
                    "MCP SSE event exceeded limit"
                );
                if !self.data.is_empty() {
                    self.data.push('\n');
                }
                self.data.push_str(data.strip_prefix(' ').unwrap_or(data));
            }
        }
        Ok(events)
    }
}
pub(crate) async fn run() -> Result<()> {
    let (sender, receiver) = tokio::sync::mpsc::channel(8);
    let input = async {
        let mut input = BufReader::new(tokio::io::stdin());
        loop {
            let mut bytes = Vec::new();
            if (&mut input)
                .take((LIMIT + 1) as u64)
                .read_until(b'\n', &mut bytes)
                .await?
                == 0
            {
                return Ok(());
            }
            ensure!(bytes.len() <= LIMIT, "MCP stdio frame exceeded limit");
            let message: Value = serde_json::from_slice(&bytes)?;
            sender
                .send(message)
                .await
                .context("MCP SSE transport closed")?;
        }
    };
    // Observe EOF during connection/endpoint discovery as well as active use.
    tokio::select! {result=input=>result,result=bridge(receiver)=>result}
}
async fn bridge(mut input: tokio::sync::mpsc::Receiver<Value>) -> Result<()> {
    let config: Value =
        serde_json::from_str(&std::env::var(CONFIG).context("MCP SSE configuration missing")?)?;
    let url = url::Url::parse(config["url"].as_str().context("MCP SSE URL missing")?)?;
    ensure!(
        matches!(url.scheme(), "http" | "https"),
        "Unsupported MCP SSE URL scheme"
    );
    let headers = headers(&config["headers"])?;
    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .connect_timeout(Duration::from_secs(10))
        .build()?;
    let response = tokio::time::timeout(
        Duration::from_secs(15),
        client
            .get(url.clone())
            .headers(headers.clone())
            .header("Accept", "text/event-stream")
            .send(),
    )
    .await??;
    ensure!(
        response.status().is_success(),
        "MCP SSE connection failed: {}",
        response.status()
    );
    let mut stream = response.bytes_stream();
    let mut parser = Events::default();
    let mut initial = Vec::new();
    let mut initial_bytes = 0;
    let endpoint = tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let bytes = stream
                .next()
                .await
                .context("MCP SSE closed before endpoint")??;
            let mut found = None;
            for (kind, data) in parser.feed(&bytes)? {
                if kind == "endpoint" {
                    let endpoint = url.join(data.trim())?;
                    ensure!(
                        endpoint.origin() == url.origin(),
                        "MCP SSE endpoint changed origin"
                    );
                    ensure!(
                        found.as_ref().is_none_or(|previous| previous == &endpoint),
                        "MCP SSE supplied conflicting endpoints"
                    );
                    found = Some(endpoint);
                }
                if kind == "message" {
                    initial_bytes += data.len();
                    ensure!(
                        initial_bytes <= LIMIT && initial.len() < 128,
                        "MCP SSE prelude exceeded limit"
                    );
                    initial.push(data);
                }
            }
            if let Some(endpoint) = found {
                return Ok::<_, anyhow::Error>(endpoint);
            }
        }
    })
    .await??;
    let send = async {
        while let Some(message) = input.recv().await {
            let response = client
                .post(endpoint.clone())
                .headers(headers.clone())
                .json(&message)
                .timeout(Duration::from_secs(30))
                .send()
                .await
                .map_err(reqwest::Error::without_url)?;
            ensure!(
                response.status().is_success(),
                "MCP SSE POST failed: {}",
                response.status()
            );
        }
        Ok::<_, anyhow::Error>(())
    };
    let receive = async {
        let mut output = tokio::io::stdout();
        for message in initial {
            let value: Value = serde_json::from_str(&message)?;
            output.write_all(format!("{value}\n").as_bytes()).await?;
        }
        output.flush().await?;
        while let Some(bytes) = stream.next().await {
            for (kind, data) in parser.feed(&bytes?)? {
                if kind == "endpoint" {
                    ensure!(
                        url.join(data.trim())? == endpoint,
                        "MCP SSE endpoint changed during the session"
                    );
                }
                if kind == "message" {
                    let value: Value = serde_json::from_str(&data)?;
                    output.write_all(format!("{value}\n").as_bytes()).await?;
                    output.flush().await?;
                }
            }
        }
        bail!("MCP SSE stream closed")
    };
    tokio::select! {result=send=>result,result=receive=>result}
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn sse_parser_preserves_chunked_utf8_multiline_and_ignores_keepalive() {
        let mut parser = Events::default();
        let bytes =
            "event: message\r\ndata: {\"text\":\"é\",\r\ndata: \"id\":1}\r\n\r\n:keepalive\n\n"
                .as_bytes();
        let mut events = vec![];
        for byte in bytes {
            events.extend(parser.feed(&[*byte]).unwrap());
        }
        assert_eq!(
            events,
            vec![("message".into(), "{\"text\":\"é\",\n\"id\":1}".into())]
        );
    }
    #[test]
    fn sse_parser_retains_default_messages_coalesced_with_endpoint_and_bounds_events() {
        let mut parser = Events::default();
        assert_eq!(
            parser
                .feed(b"event: endpoint\ndata: /messages\n\ndata: {\"id\":1}\n\n")
                .unwrap(),
            vec![
                ("endpoint".into(), "/messages".into()),
                ("message".into(), "{\"id\":1}".into())
            ]
        );
        assert!(parser.feed(&vec![b'x'; LIMIT + 1]).is_err());
    }
}
