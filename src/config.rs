use anyhow::{Result, bail};
use serde_json::Value;
use std::collections::BTreeMap;

pub fn defaults() -> Value {
    serde_json::from_str(include_str!("defaults.json")).expect("embedded default configuration")
}

#[derive(Default)]
pub struct Loader {
    cached: Option<Value>,
}
impl Loader {
    pub fn clear(&mut self) {
        self.cached = None;
    }
    pub fn load(
        &mut self,
        cwd: &std::path::Path,
        path: Option<&std::path::Path>,
        env: &BTreeMap<String, String>,
    ) -> Result<Value> {
        if let Some(cached) = &self.cached {
            return Ok(cached.clone());
        }
        let explicit = path.is_some();
        let path = path
            .map(std::path::Path::to_owned)
            .unwrap_or_else(|| cwd.join("soulsteads-messaging.json"));
        let loaded = match std::fs::read_to_string(path) {
            Ok(input) if explicit => {
                // The compatibility normalizer tolerates malformed JSON; an
                // operator-selected file must be parsed before that fallback.
                let supplied: Value = serde_json::from_str(&input)
                    .map_err(|_| anyhow::anyhow!("Cannot parse explicit configuration"))?;
                if !supplied.is_object() {
                    bail!("Explicit configuration must be an object");
                }
                load(&input, env)
                    .map_err(|_| anyhow::anyhow!("Cannot parse explicit configuration"))?
            }
            Ok(input) => {
                let opts_into_agent_tools = serde_json::from_str::<Value>(&input)
                    .ok()
                    .and_then(|value| {
                        value
                            .get("agentMessaging")
                            .and_then(|value| value.get("tools"))
                            .cloned()
                    })
                    .is_some();
                if opts_into_agent_tools {
                    load(&input, env)?
                } else {
                    load(&input, env).unwrap_or_else(|_| defaults())
                }
            }
            Err(_) if explicit => bail!("Cannot read explicit configuration"),
            Err(_) => defaults(),
        };
        self.cached = Some(loaded.clone());
        Ok(loaded)
    }
}
pub fn project_environment(
    cwd: &std::path::Path,
    ambient: BTreeMap<String, String>,
) -> Result<BTreeMap<String, String>> {
    let mut result = ambient.clone();
    let mut files = vec![".env".to_owned()];
    let mode = ambient
        .get("NODE_ENV")
        .map(String::as_str)
        .unwrap_or("development");
    if ["development", "production", "test"].contains(&mode) {
        files.push(format!(".env.{mode}"));
    }
    if mode != "test" {
        files.push(".env.local".into());
    }
    for file in files {
        let path = cwd.join(file);
        let metadata = match std::fs::metadata(&path) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(_) => bail!("Cannot read environment file"),
        };
        if metadata.len() > 1024 * 1024 {
            bail!("Environment file exceeds size limit");
        }
        let entries = dotenvy::from_path_iter(path)
            .map_err(|_| anyhow::anyhow!("Cannot parse environment file"))?;
        for entry in entries {
            let (key, value) =
                entry.map_err(|_| anyhow::anyhow!("Cannot parse environment assignment"))?;
            if !ambient.contains_key(&key) {
                result.insert(key, value);
            }
        }
    }
    Ok(result)
}
pub fn apply_runtime_overrides(config: &mut Value, env: &BTreeMap<String, String>) -> Result<()> {
    for (platform, field, name, environment_first) in [
        ("matrix", "accessToken", "MATRIX_ACCESS_TOKEN", false),
        ("matrix", "password", "MATRIX_PASSWORD", false),
        ("matrix", "userId", "MATRIX_USER_ID", false),
        ("mattermost", "url", "MATTERMOST_URL", false),
        ("mattermost", "token", "MATTERMOST_TOKEN", false),
        ("mattermost", "teamName", "MATTERMOST_TEAM", false),
        ("telegram", "token", "TELEGRAM_BOT_TOKEN", true),
        ("web", "host", "WEB_HOST", true),
        ("web", "publicUrl", "WEB_PUBLIC_URL", true),
    ] {
        if let Some(value) = env.get(name).filter(|value| !value.is_empty())
            && (environment_first || string(&config[platform], field).is_empty())
        {
            if !config[platform].is_object() {
                bail!("Connector configuration must be an object");
            }
            config[platform][field] = value.clone().into();
        }
    }
    if let Some(port) = env.get("WEB_PORT").filter(|value| !value.is_empty()) {
        let port = port
            .parse::<u16>()
            .map_err(|_| anyhow::anyhow!("WEB_PORT is invalid"))?;
        config["web"]["port"] = port.into();
    }
    if let Some(origins) = env
        .get("WEB_ALLOWED_ORIGINS")
        .filter(|value| !value.is_empty())
    {
        config["web"]["allowedOrigins"] = crate::runtime::csv(origins).into();
    }
    if let Some(value) = env.get("WHATSAPP_RESPOND_TO_OTHERS") {
        config["whatsapp"]["respondToOthers"] =
            (!["0", "false", "no", "off"].contains(&value.to_lowercase().as_str())).into();
    }
    Ok(())
}

pub(crate) fn validate_agent_tools(config: &Value) -> Result<()> {
    let Some(tools) = config
        .get("agentMessaging")
        .and_then(|value| value.get("tools"))
    else {
        return Ok(());
    };
    let tools = tools
        .as_object()
        .ok_or_else(|| anyhow::anyhow!("agentMessaging.tools must be an object"))?;
    let enabled = tools
        .get("enabled")
        .and_then(Value::as_bool)
        .ok_or_else(|| anyhow::anyhow!("agentMessaging.tools.enabled must be a boolean"))?;
    if enabled {
        if config["agentMessaging"]["enabled"] != true {
            bail!("agentMessaging.tools requires agentMessaging.enabled=true");
        }
        if tools.get("harness").and_then(Value::as_str) != Some("codex")
            && tools.get("harness").and_then(Value::as_str) != Some("opencode")
        {
            bail!(
                "agentMessaging.tools.harness must be explicitly set to \"codex\" or \"opencode\" when tools are enabled"
            );
        }
        if tools.get("harness").and_then(Value::as_str) == Some("opencode") {
            let backend = if config["acp"]["backends"]["opencode"].is_object() {
                &config["acp"]["backends"]["opencode"]
            } else {
                &config["acp"]
            };
            if crate::backend::Transport::select("opencode", backend)?
                != crate::backend::Transport::Acp
            {
                bail!("OpenCode agent tools require the ACP transport");
            }
        }
    }
    if tools
        .keys()
        .any(|key| !["enabled", "harness"].contains(&key.as_str()))
    {
        bail!("agentMessaging.tools contains an unsupported setting");
    }
    Ok(())
}

pub fn load(input: &str, env: &BTreeMap<String, String>) -> Result<Value> {
    let mut supplied: Value = serde_json::from_str(input).unwrap_or_else(|_| serde_json::json!({}));
    if !supplied.is_object() {
        bail!("configuration must be an object");
    }
    if let Some(legacy) = supplied
        .get("streamTools")
        .filter(|item| item.is_array())
        .cloned()
    {
        if !supplied["toolMessages"].is_object() {
            supplied["toolMessages"] = serde_json::json!({});
        }
        if supplied["toolMessages"]["showOutputFor"].is_null() {
            supplied["toolMessages"]["showOutputFor"] = legacy;
        }
        supplied.as_object_mut().unwrap().remove("streamTools");
    }
    substitute(&mut supplied, env);
    let mut value = defaults();
    merge(&mut value, supplied);
    validate_agent_tools(&value)?;
    if value["matrix"].is_object() {
        if !value["matrix"]["showFullPaths"].is_boolean() {
            value["matrix"]["showFullPaths"] = false.into();
        }
        if !value["matrix"]["terminalThread"].is_object() {
            value["matrix"]["terminalThread"] = defaults()["matrix"]["terminalThread"].clone();
        }
        if !["off", "always", "auto"].contains(
            &value["matrix"]["terminalThread"]["mode"]
                .as_str()
                .unwrap_or(""),
        ) {
            value["matrix"]["terminalThread"]["mode"] = "off".into();
        }
        if value["matrix"]["terminalThread"]["threshold"]
            .as_u64()
            .unwrap_or(0)
            == 0
        {
            value["matrix"]["terminalThread"]["threshold"] = 3.into();
        }
    }
    if (!value["toolMessages"].is_object() && !value["toolMessages"].is_array())
        || (!value["web"].is_object() && !value["web"].is_array())
    {
        return Ok(defaults());
    }
    if value["toolMessages"].is_object() {
        if !["off", "events", "status", "trace", "hermes"]
            .contains(&value["toolMessages"]["mode"].as_str().unwrap_or(""))
        {
            value["toolMessages"]["mode"] = "events".into();
        }
        if value["toolMessages"]["maxTraceEntries"]
            .as_u64()
            .unwrap_or(0)
            == 0
        {
            value["toolMessages"]["maxTraceEntries"] = 20.into();
        }
    }
    if value["web"].is_array() {
        return Ok(value);
    }
    let original = defaults();
    if !value["web"]["attachments"].is_object() {
        value["web"]["attachments"] = original["web"]["attachments"].clone();
    }
    let images = &mut value["web"]["attachments"];
    if !images["enabled"].is_boolean() {
        images["enabled"] = false.into();
    }
    for key in [
        "maxFileBytes",
        "maxFilesPerMessage",
        "maxWidth",
        "maxHeight",
        "maxPixels",
        "resizeMaxDimension",
    ] {
        if images[key].as_u64().unwrap_or(0) == 0 {
            images[key] = original["web"]["attachments"][key].clone();
        }
    }
    let allowed: Vec<_> = images["allowedMimeTypes"]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|item| {
            ["image/jpeg", "image/png", "image/webp"].contains(&item.as_str().unwrap_or(""))
        })
        .cloned()
        .collect();
    images["allowedMimeTypes"] = if allowed.is_empty() {
        original["web"]["attachments"]["allowedMimeTypes"].clone()
    } else {
        allowed.into()
    };
    Ok(value)
}

fn substitute(value: &mut Value, env: &BTreeMap<String, String>) {
    match value {
        Value::String(text) => {
            let pattern = regex::Regex::new(r"\{env:([^}]+)\}").unwrap();
            *text = pattern
                .replace_all(text, |captures: &regex::Captures<'_>| {
                    env.get(&captures[1]).cloned().unwrap_or_default()
                })
                .into_owned();
        }
        Value::Array(items) => items.iter_mut().for_each(|item| substitute(item, env)),
        Value::Object(items) => items.values_mut().for_each(|item| substitute(item, env)),
        _ => {}
    }
}

fn merge(target: &mut Value, source: Value) {
    if let (Some(target), Some(source)) = (target.as_object_mut(), source.as_object()) {
        for (key, value) in source {
            match target.get_mut(key) {
                Some(old) if old.is_object() && value.is_object() => merge(old, value.clone()),
                _ => {
                    target.insert(key.clone(), value.clone());
                }
            }
        }
    } else {
        *target = source;
    }
}

pub fn string<'a>(value: &'a Value, key: &str) -> &'a str {
    value[key].as_str().unwrap_or("")
}
pub fn boolean(value: &Value, key: &str) -> bool {
    value[key].as_bool().unwrap_or(false)
}
pub fn strings(value: &Value, key: &str) -> Vec<String> {
    value[key]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|item| item.as_str().map(str::to_owned))
        .collect()
}

#[cfg(test)]
mod terminal_thread_tests {
    use super::*;
    #[test]
    fn malformed_matrix_connector_does_not_panic_in_terminal_normalizer() {
        for connector in [
            serde_json::json!(false),
            serde_json::json!([]),
            serde_json::json!("invalid"),
        ] {
            let value = load(
                &serde_json::json!({"matrix":connector}).to_string(),
                &Default::default(),
            )
            .unwrap();
            assert_eq!(value["matrix"], connector);
        }
    }
    #[test]
    fn matrix_terminal_thread_defaults_to_off_with_threshold_three() {
        let value = defaults();
        assert_eq!(value["matrix"]["terminalThread"]["mode"], "off");
        assert_eq!(value["matrix"]["terminalThread"]["threshold"], 3);
    }
}
