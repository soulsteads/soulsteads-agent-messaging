//! Session-scoped native MCP registration, without editing Pi configuration.
use anyhow::{Context, Result, bail, ensure};
use serde_json::{Value, json};
use std::{collections::HashSet, io::Write, path::Path};

pub(super) struct Registration {
    directory: tempfile::TempDir,
}

impl Registration {
    pub fn create(servers: &Value, cwd: &Path) -> Result<Option<Self>> {
        let Some(servers) = servers.as_array() else {
            ensure!(servers.is_null(), "MCP servers must be an array");
            return Ok(None);
        };
        if servers.is_empty() {
            return Ok(None);
        }
        ensure!(servers.len() <= 64, "Pi MCP server limit exceeded");
        let prefix = format!("acp_{}", &uuid::Uuid::new_v4().simple().to_string()[..8]);
        let mut names = HashSet::new();
        let pairs = |values: &Value| -> Result<Value> {
            ensure!(
                values.is_null() || values.is_array(),
                "MCP fields must be an array"
            );
            let mut result = serde_json::Map::new();
            for pair in values.as_array().into_iter().flatten() {
                let name = pair["name"].as_str().context("MCP field name missing")?;
                let value = pair["value"].as_str().context("MCP field value missing")?;
                ensure!(
                    !name.is_empty()
                        && name.len() <= 256
                        && !name.contains(['\0', '='])
                        && value.len() <= 65536
                        && !value.contains('\0'),
                    "Invalid MCP field"
                );
                // Native Pi treats leading ! as a shell command and $ as an
                // environment reference. ACP values are literal strings.
                let escaped = value.replace('$', "$$");
                let escaped = if escaped.starts_with('!') {
                    format!("${escaped}")
                } else {
                    escaped
                };
                ensure!(
                    result.insert(name.into(), escaped.into()).is_none(),
                    "Duplicate MCP field"
                );
            }
            Ok(Value::Object(result))
        };
        let mut entries = Vec::new();
        for server in servers {
            let server = crate::adapters::mcp_sse::adapt(server)?;
            let name = server["name"].as_str().context("MCP server name missing")?;
            ensure!(
                !name.is_empty()
                    && name.len() <= 64
                    && name
                        .bytes()
                        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-')),
                "Invalid MCP server name"
            );
            ensure!(
                names.insert(name.replace('-', "_")),
                "Duplicate MCP namespace"
            );
            let config = match server["type"].as_str() {
                None | Some("stdio") => {
                    let command = server["command"]
                        .as_str()
                        .filter(|command| !command.is_empty())
                        .context("MCP command missing")?;
                    ensure!(
                        server["args"].is_null() || server["args"].is_array(),
                        "MCP arguments must be an array"
                    );
                    let args = server["args"].as_array().cloned().unwrap_or_default();
                    ensure!(
                        command.len() <= 65536 && !command.contains('\0'),
                        "Invalid MCP command"
                    );
                    ensure!(
                        args.len() <= 256
                            && args.iter().all(|arg| arg
                                .as_str()
                                .is_some_and(|arg| arg.len() <= 65536 && !arg.contains('\0'))),
                        "Invalid MCP arguments"
                    );
                    json!({"type":"stdio","command":command,"args":args,"env":pairs(&server["env"] )?,"cwd":cwd,"exposure":"direct","description":format!("ACP client MCP server {name}")})
                }
                Some("http") => {
                    let url = server["url"].as_str().context("MCP URL missing")?;
                    let parsed = url::Url::parse(url)?;
                    ensure!(
                        matches!(parsed.scheme(), "http" | "https"),
                        "Unsupported MCP URL scheme"
                    );
                    json!({"type":"http","url":url,"headers":pairs(&server["headers"] )?,"exposure":"direct","description":format!("ACP client MCP server {name}")})
                }
                _ => bail!("Pi native MCP supports stdio and HTTP, not legacy SSE"),
            };
            // Avoid native configuration taking precedence over a client server
            // with the same name. The description retains its supplied name.
            entries.push(json!({"name":format!("{prefix}_{name}"),"config":config}));
        }
        let directory = tempfile::Builder::new()
            .prefix("soulsteads-pi-mcp-")
            .tempdir()?;
        let path = directory.path().join("register.js");
        let mut options = std::fs::OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let mut file = options.open(path)?;
        // JSON data is serialized into a declarative native extension; no
        // client input is treated as executable source or evaluated.
        let data = serde_json::to_string(&entries)?;
        ensure!(
            data.len() <= 1024 * 1024,
            "Pi MCP configuration exceeds limit"
        );
        writeln!(
            file,
            "const servers={data};\nexport default function(pi){{for(const server of servers)pi.registerMcpServer(server.name,server.config);}}"
        )?;
        Ok(Some(Self { directory }))
    }
    pub fn path(&self) -> std::path::PathBuf {
        self.directory.path().join("register.js")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn malformed_fields_and_namespace_collisions_are_rejected() {
        let cwd = tempfile::tempdir().unwrap();
        for servers in [
            json!({}),
            json!([{"name":"server","command":"tool","args":{}}]),
            json!([{"name":"server","command":"tool","env":{}}]),
            json!([{"name":"server","type":"http","url":"http://localhost","headers":{}}]),
            json!([{"name":"a-b","command":"tool"},{"name":"a_b","command":"tool"}]),
            json!([{"name":"server","type":"http","url":"file:///tmp/invalid"}]),
        ] {
            assert!(
                Registration::create(&servers, cwd.path()).is_err(),
                "Accepted malformed declaration"
            );
        }
    }
}
