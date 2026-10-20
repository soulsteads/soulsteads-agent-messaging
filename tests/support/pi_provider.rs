//! Owned OpenAI-compatible local provider used to exercise the real Pi CLI.
use anyhow::{Context, Result, bail};
use serde_json::{Value, json};
use std::path::Path;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    sync::mpsc,
};
pub struct Provider {
    pub url: String,
    pub requests: mpsc::UnboundedReceiver<Value>,
    task: tokio::task::JoinHandle<()>,
}
impl Drop for Provider {
    fn drop(&mut self) {
        self.task.abort();
    }
}
impl Provider {
    pub async fn start(bash: bool) -> Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let url = format!("http://{}/v1", listener.local_addr()?);
        let (sender, requests) = mpsc::unbounded_channel();
        let task = tokio::spawn(async move {
            while let Ok((mut socket, _)) = listener.accept().await {
                let sender = sender.clone();
                tokio::spawn(async move {
                    let result=async {
                        let mut bytes=Vec::new();let mut buffer=[0;8192];
                        let (header_end,content_len)=loop {
                            let read=socket.read(&mut buffer).await?;if read==0{if bytes.is_empty(){return Ok(());}bail!("Provider peer closed before headers");}bytes.extend_from_slice(&buffer[..read]);if bytes.len()>2*1024*1024{bail!("Provider input exceeded limit");}
                            if let Some(end)=bytes.windows(4).position(|bytes|bytes==b"\r\n\r\n") {
                                let headers=std::str::from_utf8(&bytes[..end])?;
                                let len=headers.lines().find_map(|line|line.split_once(':').filter(|(name,_)|name.eq_ignore_ascii_case("content-length")).and_then(|(_,value)|value.trim().parse::<usize>().ok())).context("Provider content length missing")?;
                                if !headers.lines().next().unwrap_or("").starts_with("POST /v1/chat/completions "){bail!("Unexpected provider endpoint");}
                                if len>2*1024*1024{bail!("Provider body exceeded limit");}break(end+4,len);
                            }
                        };
                        while bytes.len()<header_end+content_len {let read=socket.read(&mut buffer).await?;if read==0{bail!("Provider body truncated");}bytes.extend_from_slice(&buffer[..read]);}
                        let request:Value=serde_json::from_slice(&bytes[header_end..header_end+content_len])?;sender.send(request.clone()).ok();
                        let model=request["model"].as_str().unwrap_or("fixture");let messages=request["messages"].as_array().context("Provider messages missing")?;
                        let (delta,reason)=turn(&request, messages, bash)?;
                        let mut body=String::new();
                        for chunk in [json!({"id":"owned-local-provider","object":"chat.completion.chunk","created":1,"model":model,"choices":[{"index":0,"delta":delta,"finish_reason":null}]}),json!({"id":"owned-local-provider","object":"chat.completion.chunk","created":1,"model":model,"choices":[{"index":0,"delta":{},"finish_reason":reason}],"usage":{"prompt_tokens":123,"completion_tokens":12,"total_tokens":135}})]{body.push_str(&format!("data: {chunk}\n\n"));}
                        body.push_str("data: [DONE]\n\n");let response=format!("HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",body.len());socket.write_all(response.as_bytes()).await?;socket.shutdown().await?;Ok::<_,anyhow::Error>(())
                    }.await;
                    if let Err(error) = result {
                        eprintln!("Owned Pi provider failed: {error}");
                    }
                });
            }
        });
        Ok(Self {
            url,
            requests,
            task,
        })
    }
    pub fn configure(&self, agent: &Path) -> Result<()> {
        std::fs::create_dir_all(agent)?;
        std::fs::write(agent.join("models.json"),json!({"providers":{"owned-local":{"baseUrl":self.url,"api":"openai-completions","apiKey":"owned-local-fixture-only","models":[{"id":"fixture","name":"Owned local fixture","reasoning":false,"input":["text"],"cost":{"input":0,"output":0,"cacheRead":0,"cacheWrite":0},"contextWindow":10000,"maxTokens":2048}]}}}).to_string())?;
        std::fs::write(agent.join("settings.json"),json!({"defaultProvider":"owned-local","defaultModel":"fixture","defaultThinkingLevel":"off","packages":[],"autoUpdate":"off"}).to_string())?;
        Ok(())
    }
}

fn tool(name: &str, args: Value) -> (Value, &'static str) {
    (
        json!({"role":"assistant","tool_calls":[{"index":0,"id":format!("owned-{name}"),"type":"function","function":{"name":name,"arguments":args.to_string()}}]}),
        "tool_calls",
    )
}
fn text(value: &str) -> (Value, &'static str) {
    (json!({"role":"assistant","content":value}), "stop")
}
fn turn(request: &Value, messages: &[Value], bash: bool) -> Result<(Value, &'static str)> {
    // Pi converts tool images into synthetic user messages for OpenAI providers.
    // Keep the scenario anchored to the real user prompt across those images.
    let user = messages
        .iter()
        .rposition(|message| {
            message["role"] == "user"
                && message["content"][0]["text"] != "Attached image(s) from tool result:"
        })
        .unwrap_or(0);
    let prompt = messages[user]["content"].to_string();
    let results = messages[user..]
        .iter()
        .filter(|message| message["role"] == "tool")
        .count();
    if request["tools"].as_array().is_none_or(Vec::is_empty) {
        return Ok(text(
            "Owned adapter context summary: file copy, tool and recovery probes succeeded.",
        ));
    }
    if prompt.contains("PI_MATRIX_") {
        if prompt.contains("PI_MATRIX_MCP") {
            let actions = [
                ("read", json!({"path":"probe.txt"})),
                (
                    "write",
                    json!({"path":"result.txt","content":"all-adapters-live-probe\n"}),
                ),
                (
                    "mcp__live_stdio__probe_echo",
                    json!({"value":"STDIO_LIVE_OK"}),
                ),
                (
                    "mcp__live_http__probe_echo",
                    json!({"value":"HTTP_LIVE_OK"}),
                ),
                ("mcp__live_sse__probe_echo", json!({"value":"SSE_LIVE_OK"})),
                ("mcp__live_stdio__probe_image", json!({})),
            ];
            if let Some((name, args)) = actions.get(results) {
                let suffix = name.strip_prefix("mcp__");
                let actual = request["tools"]
                    .as_array()
                    .and_then(|tools| {
                        tools
                            .iter()
                            .filter_map(|entry| entry["function"]["name"].as_str())
                            .find(|actual| {
                                actual == name
                                    || suffix.is_some_and(|suffix| {
                                        actual.starts_with("mcp__acp_") && actual.ends_with(suffix)
                                    })
                            })
                    })
                    .with_context(|| format!("Native Pi did not expose {name}"))?;
                return Ok(tool(actual, args.clone()));
            }
            return Ok(text("ADAPTER_LIVE_OK"));
        }
        if prompt.contains("PI_MATRIX_OVERLAP") && results == 0 {
            return Ok(tool(
                "bash",
                json!({"command":"echo ready > overlap.ready; sleep 2"}),
            ));
        }
        if prompt.contains("PI_MATRIX_OVERLAP") {
            return Ok(text("ORIGINAL_LIVE_OK"));
        }
        if prompt.contains("PI_MATRIX_FOLLOWUP") {
            return Ok(text("FOLLOWUP_LIVE_OK"));
        }
        if prompt.contains("PI_MATRIX_CANCEL") && results == 0 {
            return Ok(tool(
                "bash",
                json!({"command":"echo $$ > cancel.pid; echo ready > cancel.ready; sleep 45; echo finished > cancel.done"}),
            ));
        }
        if ["CONFIRM", "SELECT", "INPUT", "EDITOR", "DENY", "LATE"]
            .iter()
            .any(|kind| prompt.contains(&format!("PI_MATRIX_{kind}")))
        {
            if results == 0 {
                return Ok(tool(
                    "owned_gate",
                    json!({"kind":if prompt.contains("SELECT"){"select"}else if prompt.contains("INPUT"){"input"}else if prompt.contains("EDITOR"){"editor"}else{"confirm"},"marker":if prompt.contains("DENY"){"denied.marker"}else if prompt.contains("LATE"){"late.marker"}else{"granted.marker"}}),
                ));
            }
            return Ok(text("DIALOG_LIVE_OK"));
        }
        return Ok(text("RECOVERY_LIVE_OK FRESH_PROCESS_OK IMAGE_INPUT_OK"));
    }
    Ok(match results {
        0 if bash => tool(
            "bash",
            json!({"command":"echo $$ > owned-bash.pid; exec sleep 120"}),
        ),
        0 => tool("read", json!({"path":"probe.txt"})),
        1 => tool(
            "write",
            json!({"path":"result.txt","content":"rust-pi-acp-probe\n"}),
        ),
        _ => text("PI_ADAPTER_OK"),
    })
}
