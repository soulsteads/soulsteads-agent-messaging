//! ACP content, model, thinking and tool projections for Pi.
use crate::adapters::{AcpConnection, bounded, text, update};
use anyhow::{Context, Result, bail};
use serde_json::{Value, json};
use std::path::{Component, Path, PathBuf};

pub(super) fn canonical_directory(path: &str) -> Result<PathBuf> {
    let path = Path::new(path);
    if !path.is_absolute() {
        bail!("Session cwd must be absolute");
    }
    let path = path.canonicalize()?;
    if !path.is_dir() {
        bail!("Session cwd is not a directory");
    }
    Ok(path)
}
pub(super) fn prompt_content(prompt: &Value) -> Result<(String, Vec<Value>)> {
    let blocks = prompt.as_array().context("Prompt must contain blocks")?;
    if blocks.is_empty() || blocks.len() > 128 {
        bail!("Invalid prompt block count");
    }
    let mut texts = vec![];
    let mut images = vec![];
    let mut bytes = 0;
    for block in blocks {
        bytes += block.to_string().len();
        if bytes > 2 * 1024 * 1024 - 4096 {
            bail!("Pi prompt exceeded limit");
        }
        match block["type"].as_str() {
            Some("text") => texts.push(
                block["text"]
                    .as_str()
                    .context("Prompt text missing")?
                    .to_owned(),
            ),
            Some("image") => {
                let data = block["data"].as_str().context("Image data missing")?;
                let mime = block["mimeType"]
                    .as_str()
                    .context("Image MIME type missing")?;
                if !["image/png", "image/jpeg", "image/webp", "image/gif"].contains(&mime) {
                    bail!("Unsupported Pi image MIME type");
                }
                images.push(json!({"type":"image","data":data,"mimeType":mime}));
            }
            Some("resource") => {
                let resource = &block["resource"];
                texts.push(format!(
                    "{}\n{}",
                    resource["uri"].as_str().unwrap_or("Context"),
                    resource["text"]
                        .as_str()
                        .context("Only text resources are supported")?
                ));
            }
            Some("resource_link") => texts.push(format!(
                "{}: {}",
                block["name"].as_str().unwrap_or("Resource"),
                block["uri"].as_str().context("Resource URI missing")?
            )),
            _ => bail!("Unsupported Pi prompt content"),
        }
    }
    let message = texts.join("\n");
    if matches!(
        message.split_whitespace().next(),
        Some("/login" | "/logout")
    ) {
        bail!("Authenticate with the installed Pi CLI");
    }
    Ok((message, images))
}
pub(super) fn model_id(model: &Value) -> Option<String> {
    let provider = model["provider"].as_str()?;
    let id = model["id"].as_str()?;
    if provider.is_empty()
        || id.is_empty()
        || provider == "unknown"
        || id == "unknown"
        || provider.len() + id.len() > 1024
    {
        return None;
    }
    Some(format!("{provider}/{id}"))
}
pub(super) fn configuration(state: &Value, models: &[Value], levels: &[String]) -> Value {
    let current = model_id(&state["model"]);
    let level = state["thinkingLevel"].as_str().unwrap_or("off");
    let modes = json!({"currentModeId":level,"availableModes":levels.iter().map(|level|json!({"id":level,"name":level,"description":format!("Pi thinking level: {level}")})).collect::<Vec<_>>()});
    let mut options = vec![
        json!({"id":"thought_level","name":"Thinking","category":"thought_level","type":"select","currentValue":level,"options":levels.iter().map(|level|json!({"value":level,"name":level})).collect::<Vec<_>>()}),
    ];
    let available=models.iter().filter_map(|model|Some(json!({"modelId":model_id(model)?,"name":format!("{}/{}",model["provider"].as_str()?,model["name"].as_str().or_else(||model["id"].as_str())?)}))).collect::<Vec<_>>();
    if let Some(current) = &current
        && !available.is_empty()
    {
        options.insert(0,json!({"id":"model","name":"Model","category":"model","type":"select","currentValue":current,"options":available.iter().map(|model|json!({"value":model["modelId"],"name":model["name"]})).collect::<Vec<_>>()}));
    }
    json!({"modes":modes,"models":current.map(|current|json!({"currentModelId":current,"availableModels":available})),"configOptions":options})
}
pub(super) fn commands(native: &Value) -> Vec<Value> {
    let mut commands = vec![
        json!({"name":"compact","description":"Compact the session context","input":{"hint":"optional instructions"}}),
        json!({"name":"session","description":"Show Pi session statistics"}),
        json!({"name":"name","description":"Set the session title","input":{"hint":"name"}}),
        json!({"name":"export","description":"Export this session to HTML"}),
        json!({"name":"autocompact","description":"Set automatic compaction","input":{"hint":"on|off|toggle"}}),
        json!({"name":"steering","description":"Set steering delivery mode","input":{"hint":"all|one-at-a-time"}}),
        json!({"name":"follow-up","description":"Set follow-up delivery mode","input":{"hint":"all|one-at-a-time"}}),
    ];
    for command in native["commands"]
        .as_array()
        .into_iter()
        .flatten()
        .take(256)
    {
        if let Some(name) = command["name"]
            .as_str()
            .filter(|name| !matches!(*name, "login" | "logout") && name.len() <= 256)
            && !commands.iter().any(|command| command["name"] == name)
        {
            commands.push(json!({"name":name,"description":bounded(command["description"].as_str().unwrap_or("Pi command"),4096)}));
        }
    }
    commands
}
fn tool_kind(name: &str) -> &'static str {
    match name {
        "bash" => "execute",
        "read" => "read",
        "write" | "edit" => "edit",
        "grep" | "find" | "ls" => "search",
        _ => "other",
    }
}
pub(super) struct Tool<'a> {
    pub id: &'a str,
    pub name: &'a str,
    pub args: &'a Value,
    pub existing: bool,
    pub status: &'a str,
}
pub(super) fn tool(
    client: &AcpConnection,
    session: &str,
    cwd: &Path,
    call: Tool<'_>,
) -> Result<()> {
    let Tool {
        id,
        name,
        args,
        existing,
        status,
    } = call;
    if id.is_empty() || id.len() > 1024 {
        bail!("Invalid Pi tool ID");
    }
    let encoded = args.to_string();
    let input = if encoded.len() > 65536 {
        json!({"summary":bounded(&encoded,65536)})
    } else {
        args.clone()
    };
    let mut event = json!({"sessionUpdate":if existing{"tool_call_update"}else{"tool_call"},"toolCallId":id,"title":bounded(args["command"].as_str().unwrap_or(name),4096),"kind":tool_kind(name),"status":status,"rawInput":input});
    if let Some(path) = args["path"]
        .as_str()
        .or_else(|| args["file_path"].as_str())
        .filter(|path| path.len() <= 4096)
    {
        let path = Path::new(path);
        let absolute = if path.is_absolute() {
            path.into()
        } else {
            cwd.join(path)
        };
        let mut path = PathBuf::new();
        for part in absolute.components() {
            match part {
                Component::ParentDir => {
                    path.pop();
                }
                Component::CurDir => {}
                part => path.push(part),
            }
        }
        event["locations"] = json!([{"path":path}]);
    }
    update(client, session, event)
}
pub(super) fn tool_output(
    client: &AcpConnection,
    session: &str,
    id: &str,
    result: &Value,
    status: &str,
) -> Result<()> {
    let output = result["details"]["diff"]
        .as_str()
        .map(str::to_owned)
        .or_else(|| {
            result["content"].as_array().map(|blocks| {
                blocks
                    .iter()
                    .filter_map(|block| block["text"].as_str())
                    .collect::<String>()
            })
        })
        .filter(|text| !text.is_empty())
        .or_else(|| {
            result["details"]["output"]
                .as_str()
                .or_else(|| result["details"]["stdout"].as_str())
                .map(str::to_owned)
        })
        .unwrap_or_default();
    let output = bounded(&output, 65536);
    update(
        client,
        session,
        json!({"sessionUpdate":"tool_call_update","toolCallId":id,"status":status,"rawOutput":{"output":output,"exitCode":result["details"]["exitCode"]},"content":[{"type":"content","content":{"type":"text","text":output}}]}),
    )?;
    if matches!(status, "completed" | "failed") {
        for image in result["content"]
            .as_array()
            .into_iter()
            .flatten()
            .filter(|block| block["type"] == "image")
            .take(32)
        {
            if image["data"]
                .as_str()
                .is_some_and(|data| data.len() < 2 * 1024 * 1024 - 4096)
            {
                update(
                    client,
                    session,
                    json!({"sessionUpdate":"agent_message_chunk","content":image}),
                )?;
            } else {
                text(
                    client,
                    session,
                    "agent_message_chunk",
                    "[Pi image exceeds ACP frame limit]",
                )?;
            }
        }
    }
    Ok(())
}
pub(super) fn replay(
    client: &AcpConnection,
    session: &str,
    cwd: &Path,
    messages: &Value,
) -> Result<()> {
    let messages = messages["messages"]
        .as_array()
        .context("Pi history omitted messages")?;
    if messages.len() > 16384
        || messages
            .iter()
            .map(|message| message.to_string().len())
            .sum::<usize>()
            > 16 * 1024 * 1024
    {
        bail!("Pi history exceeded limit");
    }
    for message in messages {
        let content = if let Some(value) = message["content"].as_str() {
            vec![json!({"type":"text","text":value})]
        } else {
            message["content"].as_array().cloned().unwrap_or_default()
        };
        match message["role"].as_str() {
            Some("user" | "assistant") => {
                for block in content {
                    match block["type"].as_str() {
                        Some("text") => text(
                            client,
                            session,
                            if message["role"] == "user" {
                                "user_message_chunk"
                            } else {
                                "agent_message_chunk"
                            },
                            block["text"].as_str().unwrap_or(""),
                        )?,
                        Some("thinking") => text(
                            client,
                            session,
                            "agent_thought_chunk",
                            block["thinking"].as_str().unwrap_or(""),
                        )?,
                        Some("toolCall") => tool(
                            client,
                            session,
                            cwd,
                            Tool {
                                id: block["id"]
                                    .as_str()
                                    .context("Pi historical tool ID missing")?,
                                name: block["name"].as_str().unwrap_or("Pi tool"),
                                args: &block["arguments"],
                                existing: false,
                                status: "in_progress",
                            },
                        )?,
                        _ => {}
                    }
                }
            }
            Some("toolResult") => tool_output(
                client,
                session,
                message["toolCallId"]
                    .as_str()
                    .context("Pi historical result ID missing")?,
                message,
                if message["isError"] == true {
                    "failed"
                } else {
                    "completed"
                },
            )?,
            _ => {}
        }
    }
    Ok(())
}
