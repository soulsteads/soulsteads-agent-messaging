//! Owned MCP server for installed adapter qualification (stdio, HTTP and SSE).
use anyhow::Result;
use base64::Engine;
use serde_json::{Value, json};
use std::path::Path;

pub fn png() -> String {
    use image::ImageEncoder;
    let pixels = [255_u8, 0, 0, 255].repeat(32 * 32);
    let mut data = Vec::new();
    image::codecs::png::PngEncoder::new(&mut data)
        .write_image(&pixels, 32, 32, image::ExtendedColorType::Rgba8)
        .unwrap();
    base64::engine::general_purpose::STANDARD.encode(data)
}
pub fn response(request: &Value) -> Option<Value> {
    let id = request.get("id")?;
    let result = match request["method"].as_str().unwrap_or("") {
        "initialize" => {
            json!({"protocolVersion":request["params"]["protocolVersion"].as_str().unwrap_or("2025-06-18"),"capabilities":{"tools":{}},"serverInfo":{"name":"adapter-live-probe","version":"1.0.0"},"instructions":"Owned tools for live adapter testing."})
        }
        "ping" => json!({}),
        "tools/list" => json!({"tools":[
            {"name":"probe_echo","description":"Echo a marker for adapter live testing. Returns ADAPTER_TOOL_OK and the supplied value.","inputSchema":{"type":"object","properties":{"value":{"type":"string"}},"required":["value"],"additionalProperties":false},"annotations":{"readOnlyHint":true,"destructiveHint":false,"openWorldHint":false}},
            {"name":"probe_image","description":"Return a 32px red PNG for adapter image testing.","inputSchema":{"type":"object","properties":{},"additionalProperties":false},"annotations":{"readOnlyHint":true,"destructiveHint":false,"openWorldHint":false}},
            {"name":"probe_write","description":"Create an owned marker for approval denial and cancellation tests.","inputSchema":{"type":"object","properties":{"marker":{"type":"string","enum":["denied.marker","late.marker"]}},"required":["marker"],"additionalProperties":false},"annotations":{"readOnlyHint":false,"destructiveHint":true,"openWorldHint":false}}
        ]}),
        "tools/call" => match request["params"]["name"].as_str().unwrap_or("") {
            "probe_echo" => {
                json!({"content":[{"type":"text","text":format!("ADAPTER_TOOL_OK {}",request["params"]["arguments"]["value"].as_str().unwrap_or(""))}],"isError":false})
            }
            "probe_image" => {
                json!({"content":[{"type":"image","mimeType":"image/png","data":png()}],"isError":false})
            }
            "probe_write" => {
                let marker = request["params"]["arguments"]["marker"]
                    .as_str()
                    .unwrap_or("");
                let written = std::env::var("ADAPTER_LIVE_MCP_AUDIT")
                    .ok()
                    .filter(|_| matches!(marker, "denied.marker" | "late.marker"))
                    .and_then(|audit| {
                        Path::new(&audit)
                            .parent()
                            .map(|root| std::fs::write(root.join(marker), "once").is_ok())
                    })
                    .unwrap_or(false);
                json!({"content":[{"type":"text","text":if written{"OWNED_WRITTEN"}else{"OWNED_WRITE_UNAVAILABLE"}}],"isError":!written})
            }
            _ => {
                return Some(
                    json!({"jsonrpc":"2.0","id":id,"error":{"code":-32601,"message":"Unknown owned MCP tool"}}),
                );
            }
        },
        "resources/list" | "resources/templates/list" => {
            json!({"resources":[],"resourceTemplates":[]})
        }
        "prompts/list" => json!({"prompts":[]}),
        _ => {
            return Some(
                json!({"jsonrpc":"2.0","id":id,"error":{"code":-32601,"message":"Unknown owned MCP method"}}),
            );
        }
    };
    Some(json!({"jsonrpc":"2.0","id":id,"result":result}))
}
pub fn append(path: &Path, value: &Value) -> Result<()> {
    use std::io::Write;
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)?;
    writeln!(file, "{value}")?;
    Ok(())
}
