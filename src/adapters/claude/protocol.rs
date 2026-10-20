//! Native content, model and MCP mappings for this backend.
use super::Settings;
use crate::adapters::{AcpConnection, bounded, update};
use anyhow::{Context, Result, bail};
use serde_json::{Value, json};
use std::path::{Path, PathBuf};

pub(super) fn canonical_directory(path: &str) -> Result<PathBuf> {
    let path = Path::new(path);
    if !path.is_absolute() {
        bail!("Session cwd must be absolute");
    }
    let path = path
        .canonicalize()
        .context("Session workspace does not exist")?;
    if !path.is_dir() {
        bail!("Session cwd is not a directory");
    }
    Ok(path)
}
pub(super) fn modes(current: &str) -> Value {
    json!({"currentModeId":current,"availableModes":[{"id":"default","name":"Default","description":"Claude's manual permission mode with approve-once prompts"},{"id":"plan","name":"Plan","description":"Read-only planning"}]})
}
pub(super) fn config_options(settings: &Settings, catalog: &[Value]) -> Value {
    let mut options = vec![
        json!({"id":"model","name":"Model","category":"model","type":"select","currentValue":settings.model,"options":catalog.iter().map(|model|json!({"value":model["value"],"name":model["displayName"],"description":model["description"]})).collect::<Vec<_>>()}),
        json!({"id":"mode","name":"Mode","category":"mode","type":"select","currentValue":settings.mode,"options":[{"value":"default","name":"Default"},{"value":"plan","name":"Plan"}]}),
    ];
    if let Some(levels) = catalog
        .iter()
        .find(|model| model["value"] == settings.model)
        .and_then(|model| model["supportedEffortLevels"].as_array())
        .filter(|levels| !levels.is_empty())
    {
        let mut levels = levels
            .iter()
            .map(|level| json!({"value":level,"name":level}))
            .collect::<Vec<_>>();
        levels.insert(0, json!({"value":"default","name":"Default"}));
        options.push(json!({"id":"reasoning_effort","name":"Effort","category":"thought_level","type":"select","currentValue":settings.effort,"options":levels}));
    }
    options.into()
}
pub(super) fn content(value: &Value) -> Vec<Value> {
    if let Some(text) = value.as_str() {
        vec![json!({"type":"text","text":text})]
    } else {
        value.as_array().cloned().unwrap_or_default()
    }
}
pub(super) fn tool_kind(name: &str) -> &'static str {
    match name {
        "Bash" | "PowerShell" => "execute",
        "Read" => "read",
        "Edit" | "Write" | "MultiEdit" | "NotebookEdit" => "edit",
        "Glob" | "Grep" | "WebSearch" => "search",
        "WebFetch" => "fetch",
        _ => "other",
    }
}
pub(super) fn emit_tool(
    client: &AcpConnection,
    session: &str,
    block: &Value,
    existing: bool,
) -> Result<()> {
    let id = block["id"].as_str().context("Claude tool omitted ID")?;
    if id.len() > 1024 {
        bail!("Claude tool ID exceeded limit");
    }
    let name = block["name"].as_str().unwrap_or("Claude tool");
    let input = &block["input"];
    let encoded = input.to_string();
    let input = if encoded.len() > 65536 {
        json!({"summary":bounded(&encoded,65536)})
    } else {
        input.clone()
    };
    let mut event = json!({"sessionUpdate":if existing{"tool_call_update"}else{"tool_call"},"toolCallId":id,"title":bounded(block["input"]["command"].as_str().unwrap_or(name),4096),"kind":tool_kind(name),"status":"in_progress","rawInput":input});
    if let Some(path) = block["input"]["file_path"]
        .as_str()
        .filter(|path| path.len() <= 4096)
    {
        event["locations"] = json!([{"path":path}]);
    }
    update(client, session, event)
}
pub(super) fn prompt_content(prompt: &Value) -> Result<Vec<Value>> {
    let blocks = prompt.as_array().context("Prompt must contain blocks")?;
    if blocks.is_empty() || blocks.len() > 128 {
        bail!("Invalid prompt block count");
    }
    let mut content = Vec::new();
    let mut size = 0;
    for block in blocks {
        let native = match block["type"].as_str() {
            Some("text") => {
                let value = block["text"].as_str().context("Missing prompt text")?;
                if value.starts_with("/login") || value.starts_with("/logout") {
                    bail!("Authenticate with the installed Claude CLI");
                }
                json!({"type":"text","text":value})
            }
            Some("image") => {
                let data = block["data"].as_str().context("Missing image data")?;
                let mime = block["mimeType"].as_str().context("Missing image type")?;
                if !["image/png", "image/jpeg", "image/webp", "image/gif"].contains(&mime) {
                    bail!("Unsupported Claude image type");
                }
                json!({"type":"image","source":{"type":"base64","media_type":mime,"data":data}})
            }
            Some("resource") => {
                let resource = &block["resource"];
                json!({"type":"text","text":resource["text"].as_str().context("Only text resources are supported")?})
            }
            Some("resource_link") => {
                json!({"type":"text","text":format!("{}: {}",block["name"].as_str().unwrap_or("Resource"),block["uri"].as_str().context("Missing resource URI")?)})
            }
            _ => bail!("Unsupported Claude prompt content"),
        };
        size += native.to_string().len();
        if size > 2 * 1024 * 1024 - 4096 {
            bail!("Claude prompt exceeded limit");
        }
        content.push(native);
    }
    Ok(content)
}
pub(super) fn mcp_config(servers: &Value) -> Result<Value> {
    // ACP resume carries no MCP list in the stable v1 schema.
    if servers.is_null() {
        return Ok(json!({}));
    }
    let servers = servers.as_array().context("MCP servers must be an array")?;
    if servers.len() > 64 {
        bail!("MCP server limit exceeded");
    }
    let mut result = serde_json::Map::new();
    for server in servers {
        let name = server["name"].as_str().context("MCP name missing")?;
        if name.is_empty() || name.len() > 256 || result.contains_key(name) {
            bail!("Invalid or duplicate MCP name");
        }
        let config = if let Some(kind) = server["type"].as_str() {
            if !["http", "sse"].contains(&kind) {
                bail!("Unsupported MCP transport");
            }
            let mut headers = serde_json::Map::new();
            for header in server["headers"].as_array().into_iter().flatten() {
                headers.insert(
                    header["name"]
                        .as_str()
                        .context("MCP header name missing")?
                        .into(),
                    header["value"].clone(),
                );
            }
            json!({"type":kind,"url":server["url"].as_str().context("MCP URL missing")?,"headers":headers})
        } else {
            let mut env = serde_json::Map::new();
            for variable in server["env"].as_array().into_iter().flatten() {
                env.insert(
                    variable["name"]
                        .as_str()
                        .context("MCP env name missing")?
                        .into(),
                    variable["value"].clone(),
                );
            }
            json!({"type":"stdio","command":server["command"].as_str().context("MCP command missing")?,"args":server["args"],"env":env})
        };
        result.insert(name.into(), config);
    }
    Ok(result.into())
}
