//! Native content, model and MCP mappings for this backend.
use super::TOOL_OUTPUT_LIMIT;
use crate::adapters::{AcpConnection, text, update};
use anyhow::{Context, Result, bail};
use serde_json::{Value, json};
use std::path::{Path, PathBuf};

pub(super) fn canonical_directory(path: &str) -> Result<PathBuf> {
    let path = Path::new(path);
    if !path.is_absolute() {
        bail!("Session cwd must be absolute");
    }
    let cwd = path
        .canonicalize()
        .context("Session workspace does not exist")?;
    if !cwd.is_dir() {
        bail!("Session cwd is not a directory");
    }
    Ok(cwd)
}
pub(super) fn mcp_config(servers: &Value, cwd: &Path) -> Result<Value> {
    let mut config = json!({"projects":{cwd.display().to_string():{"trust_level":"trusted"}}});
    let mut mcp = serde_json::Map::new();
    for server in servers.as_array().into_iter().flatten() {
        let name = server["name"]
            .as_str()
            .context("MCP server name is required")?;
        if name.is_empty() || name.len() > 128 || mcp.contains_key(name) {
            bail!("Invalid or duplicate MCP server name");
        }
        let pairs = |items: &Value| -> Result<Value> {
            let mut values = serde_json::Map::new();
            for item in items.as_array().into_iter().flatten() {
                values.insert(
                    item["name"]
                        .as_str()
                        .context("MCP field name is required")?
                        .into(),
                    item["value"]
                        .as_str()
                        .context("MCP field value is required")?
                        .into(),
                );
            }
            Ok(Value::Object(values))
        };
        let value = match server["type"].as_str() {
            None | Some("stdio") => {
                json!({"command":server["command"],"args":server["args"],"env":pairs(&server["env"])?})
            }
            Some("http") => json!({"url":server["url"],"http_headers":pairs(&server["headers"])?}),
            _ => bail!("Codex supports stdio and HTTP MCP servers, not SSE"),
        };
        #[cfg(feature = "agent-network")]
        let value = {
            let mut value = value;
            if name == "soulsteads_agents"
                && crate::delivery::agent_tools::runtime_grant_fingerprint(server).is_some_and(
                    |fingerprint| {
                        std::env::var("SOULSTEADS_LOCAL_AGENT_TOOLS_GRANT")
                            .ok()
                            .as_deref()
                            == Some(&fingerprint)
                    },
                )
            {
                value["enabled_tools"] = json!(["list_agents", "ask_agent"]);
                value["tools"] = json!({
                    "list_agents":{"approval_mode":"approve"},
                    "ask_agent":{"approval_mode":"approve"}
                });
                value["default_tools_approval_mode"] = json!("prompt");
                value["tool_timeout_sec"] = json!(310);
            }
            value
        };
        mcp.insert(name.into(), value);
    }
    if !mcp.is_empty() {
        config["mcp_servers"] = Value::Object(mcp);
    }
    Ok(config)
}
pub(super) fn modes(current: &str) -> Value {
    json!({"currentModeId":current,"availableModes":[
        {"id":"read-only","name":"Read-only","description":"Read the workspace; ask before changes."},
        {"id":"workspace-write","name":"Workspace access","description":"Work inside the workspace; ask before operations outside it."},
        {"id":"full-access","name":"Full access","description":"Allow unrestricted execution without tool approvals."}]})
}
pub(super) fn approval_policy(mode: &str) -> &'static str {
    if mode == "full-access" {
        "never"
    } else {
        "on-request"
    }
}
pub(super) fn sandbox(mode: &str, cwd: &Path) -> Value {
    match mode {
        "read-only" => json!({"type":"readOnly","networkAccess":false}),
        "full-access" => json!({"type":"dangerFullAccess"}),
        _ => {
            json!({"type":"workspaceWrite","writableRoots":[cwd],"networkAccess":false,"excludeTmpdirEnvVar":false,"excludeSlashTmp":false})
        }
    }
}
pub(super) fn prompt_input(prompt: &Value) -> Result<Vec<Value>> {
    let mut input = Vec::new();
    for content in prompt.as_array().context("Prompt content is required")? {
        let value = match content["type"].as_str() {
            Some("text") => json!({"type":"text","text":content["text"],"text_elements":[]}),
            Some("image") => {
                let mime = content["mimeType"]
                    .as_str()
                    .context("Image MIME type is required")?;
                if !matches!(
                    mime,
                    "image/png" | "image/jpeg" | "image/webp" | "image/gif"
                ) {
                    bail!("Codex cannot accept this image MIME type");
                }
                json!({"type":"image","url":format!("data:{mime};base64,{}",content["data"].as_str().context("Image data is required")?)})
            }
            Some("resource") => {
                json!({"type":"text","text":format!("{}\n{}",content["resource"]["uri"].as_str().unwrap_or(""),content["resource"]["text"].as_str().context("Only text embedded resources are supported")?),"text_elements":[]})
            }
            Some("resource_link") => {
                json!({"type":"text","text":content["uri"],"text_elements":[]})
            }
            _ => bail!("Codex prompt contains unsupported content"),
        };
        input.push(value);
    }
    if input.is_empty() {
        bail!("Prompt is empty");
    }
    Ok(input)
}
pub(super) fn bounded(value: &str, maximum: usize) -> String {
    if value.len() <= maximum {
        return value.into();
    }
    let mut end = maximum;
    while !value.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}\n[output truncated by ACP adapter]", &value[..end])
}
pub(super) fn tool_event(item: &Value, completed: bool) -> Result<Option<Value>> {
    let kind = match item["type"].as_str().unwrap_or("") {
        "commandExecution" => "execute",
        "fileChange" => "edit",
        "webSearch" => "search",
        "mcpToolCall" | "dynamicToolCall" | "collabAgentToolCall" | "contextCompaction" => "other",
        "imageView" => "read",
        "imageGeneration" => "other",
        "userMessage" | "agentMessage" | "plan" | "reasoning" => return Ok(None),
        // Preserve activity for future native tool variants. Otherwise a tool
        // followed by empty text could incorrectly qualify for an empty retry.
        _ => "other",
    };
    let tool_id = item["id"].as_str().context("Codex tool omitted item ID")?;
    if tool_id.len() > 1024 {
        bail!("Codex tool ID exceeded limit");
    }
    let title = item["command"]
        .as_str()
        .or_else(|| item["tool"].as_str())
        .or_else(|| item["query"].as_str())
        .unwrap_or(item["type"].as_str().unwrap_or("Codex tool"));
    let mut event = json!({"sessionUpdate":if completed {"tool_call_update"} else {"tool_call"},"toolCallId":tool_id,"kind":kind,"title":bounded(title,4096),"status":if completed {if matches!(item["status"].as_str(),Some("failed"|"declined")) {"failed"} else {"completed"}} else {"in_progress"}});
    if !completed {
        let encoded = item.to_string();
        event["rawInput"] = if encoded.len() <= 65536 {
            item.clone()
        } else {
            json!({"summary":bounded(&encoded,65536),"truncated":true})
        };
    } else {
        let output = item["aggregatedOutput"]
            .as_str()
            .map(str::to_owned)
            .or_else(|| item["error"]["message"].as_str().map(str::to_owned))
            .or_else(|| {
                item.get("result")
                    .filter(|result| !result.is_null())
                    .map(|result| image_safe_summary(result).to_string())
            })
            .or_else(|| {
                item["savedPath"]
                    .as_str()
                    .or_else(|| item["path"].as_str())
                    .map(str::to_owned)
            })
            .or_else(|| item.get("changes").map(Value::to_string))
            .unwrap_or_default();
        event["rawOutput"] =
            json!({"output":bounded(&output,TOOL_OUTPUT_LIMIT),"exitCode":item["exitCode"]});
        event["content"] = json!([{"type":"content","content":{"type":"text","text":bounded(&output,TOOL_OUTPUT_LIMIT)}}]);
    }
    let paths = item["changes"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|change| change["path"].as_str())
        .filter(|path| path.len() <= 4096)
        .take(64)
        .map(|path| json!({"path":path}))
        .collect::<Vec<_>>();
    if !paths.is_empty() {
        event["locations"] = paths.into();
    }
    Ok(Some(event))
}
pub(super) fn timestamp(seconds: Option<i64>) -> Value {
    seconds
        .and_then(|seconds| chrono::DateTime::<chrono::Utc>::from_timestamp(seconds, 0))
        .map(|time| Value::String(time.to_rfc3339_opts(chrono::SecondsFormat::Secs, true)))
        .unwrap_or(Value::Null)
}

// Keep binary image payloads out of tool summaries. Images use separate ACP
// content notifications so one large blob cannot duplicate or swamp a result.
pub(super) fn image_safe_summary(value: &Value) -> Value {
    match value {
        Value::Object(fields) if fields.get("type") == Some(&Value::String("image".into())) => {
            let mut fields = fields.clone();
            fields.remove("data");
            Value::Object(fields)
        }
        Value::Object(fields) => Value::Object(
            fields
                .iter()
                .map(|(key, value)| (key.clone(), image_safe_summary(value)))
                .collect(),
        ),
        Value::Array(items) => items.iter().map(image_safe_summary).collect(),
        _ => value.clone(),
    }
}
pub(super) fn emit_tool_images(
    connection: &AcpConnection,
    session: &str,
    item: &Value,
) -> Result<()> {
    use base64::Engine;
    let mut images = item["result"]["content"]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|block| block["type"] == "image")
        .cloned()
        .collect::<Vec<_>>();
    if item["type"] == "imageGeneration"
        && let Some(data) = item["result"]
            .as_str()
            .filter(|data| !data.is_empty() && data.len() < 2 * 1024 * 1024 - 4096)
        && let Ok(bytes) = base64::engine::general_purpose::STANDARD.decode(data)
    {
        let mime = match image::guess_format(&bytes) {
            Ok(image::ImageFormat::Png) => Some("image/png"),
            Ok(image::ImageFormat::Jpeg) => Some("image/jpeg"),
            Ok(image::ImageFormat::WebP) => Some("image/webp"),
            _ => None,
        };
        if let Some(mime) = mime {
            images.push(json!({"type":"image","mimeType":mime,"data":data}));
        }
    }
    for image in images.into_iter().take(32) {
        if image["data"]
            .as_str()
            .is_some_and(|data| data.len() < 2 * 1024 * 1024 - 4096)
        {
            update(
                connection,
                session,
                json!({"sessionUpdate":"agent_message_chunk","content":image}),
            )?;
        } else {
            text(
                connection,
                session,
                "agent_message_chunk",
                "[Image exceeds ACP frame limit]",
            )?;
        }
    }
    Ok(())
}
