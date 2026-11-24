use anyhow::{Context, Result, bail};
use serde_json::Value;
use std::{
    collections::BTreeMap,
    fs::{self, OpenOptions},
    io::{self, Read},
    path::Path,
};

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
                let strict = serde_json::from_str::<Value>(&input).is_ok_and(|value| {
                    value.get("agents").is_some()
                        || value.pointer("/agentMessaging/tools").is_some()
                });
                if strict {
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

const SETTINGS_FILE_LIMIT: u64 = 1024 * 1024;

pub(crate) fn read_settings_file(path: &Path) -> io::Result<Vec<u8>> {
    let check_file = |metadata: fs::Metadata| {
        if !metadata.is_file() || metadata.len() > SETTINGS_FILE_LIMIT {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Invalid settings file type or size",
            ));
        }
        Ok(())
    };
    check_file(fs::metadata(path)?)?;
    let mut options = OpenOptions::new();
    options.read(true);
    // The path may have changed between metadata and open.
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NONBLOCK);
    }
    let file = options.open(path)?;
    check_file(file.metadata()?)?;
    let mut bytes = Vec::new();
    file.take(SETTINGS_FILE_LIMIT + 1).read_to_end(&mut bytes)?;
    if bytes.len() as u64 > SETTINGS_FILE_LIMIT {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Settings file exceeds size limit",
        ));
    }
    Ok(bytes)
}

pub fn project_environment(
    cwd: &Path,
    ambient: BTreeMap<String, String>,
) -> Result<BTreeMap<String, String>> {
    project_environment_with(cwd, ambient, |path| {
        // Preserve legacy startup behavior, including empty /dev/null links.
        let metadata = match fs::metadata(path) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(_) => bail!("Cannot read environment file"),
        };
        if metadata.len() > SETTINGS_FILE_LIMIT {
            bail!("Environment file exceeds size limit");
        }
        dotenvy::from_path_iter(path)
            .map(Some)
            .map_err(|_| anyhow::anyhow!("Cannot parse environment file"))
    })
}

pub(crate) fn settings_environment(
    cwd: &Path,
    ambient: BTreeMap<String, String>,
) -> Result<BTreeMap<String, String>> {
    project_environment_with(cwd, ambient, |path| match read_settings_file(path) {
        Ok(bytes) => Ok(Some(dotenvy::from_read_iter(io::Cursor::new(bytes)))),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(_) => bail!("Cannot read environment file"),
    })
}

fn project_environment_with<R: Read>(
    cwd: &Path,
    ambient: BTreeMap<String, String>,
    read: impl Fn(&Path) -> Result<Option<dotenvy::Iter<R>>>,
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
        let Some(entries) = read(&cwd.join(file))? else {
            continue;
        };
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

/// Returns the configured port for an enabled Web connector without binding.
pub fn enabled_web_port(config: &Value) -> Result<Option<u16>> {
    if !boolean(&config["web"], "enabled") {
        return Ok(None);
    }
    config["web"]["port"]
        .as_u64()
        .and_then(|port| u16::try_from(port).ok())
        .map(Some)
        .context("Invalid web port")
}

/// Checks whether this build supports the enabled SDK connectors.
pub fn validate_platform_sdk_support(config: &Value) -> Result<()> {
    if !cfg!(feature = "platform-sdks")
        && ["matrix", "whatsapp"]
            .iter()
            .any(|platform| boolean(&config[*platform], "enabled"))
    {
        bail!("Enabled connector requires platform-sdks build feature");
    }
    Ok(())
}
pub fn apply_runtime_overrides(config: &mut Value, env: &BTreeMap<String, String>) -> Result<()> {
    apply_runtime_overrides_with(config, env, |_, _| {})
}

pub(crate) fn apply_runtime_overrides_with(
    config: &mut Value,
    env: &BTreeMap<String, String>,
    mut applied: impl FnMut(&str, &str),
) -> Result<()> {
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
            applied(platform, field);
        }
    }
    if let Some(port) = env.get("WEB_PORT").filter(|value| !value.is_empty()) {
        if !config["web"].is_object() {
            bail!("Web environment overrides require a web configuration object");
        }
        let port = port
            .parse::<u16>()
            .map_err(|_| anyhow::anyhow!("WEB_PORT is invalid"))?;
        config["web"]["port"] = port.into();
        applied("web", "port");
    }
    if let Some(origins) = env
        .get("WEB_ALLOWED_ORIGINS")
        .filter(|value| !value.is_empty())
    {
        if !config["web"].is_object() {
            bail!("Web environment overrides require a web configuration object");
        }
        config["web"]["allowedOrigins"] = crate::runtime::csv(origins).into();
        applied("web", "allowedOrigins");
    }
    if let Some(value) = env.get("WHATSAPP_RESPOND_TO_OTHERS") {
        config["whatsapp"]["respondToOthers"] =
            (!["0", "false", "no", "off"].contains(&value.to_lowercase().as_str())).into();
        applied("whatsapp", "respondToOthers");
    }
    Ok(())
}

/// Refuse ambiguous remote access at the service boundary. An empty allowlist
/// must never accidentally expose a native harness to every chat participant.
pub fn validate_connector_access(config: &Value, env: &BTreeMap<String, String>) -> Result<()> {
    mattermost_receipt_dir(config)?;
    crate::connectors::telegram::skill_reviews::settings(config)?;
    for platform in [
        "matrix",
        "mattermost",
        "telegram",
        "slack",
        "discord",
        "whatsapp",
    ] {
        let connector = &config[platform];
        if connector["enabled"] != true {
            continue;
        }
        let allowed = env
            .get(&format!("{}_ALLOWED_USERS", platform.to_uppercase()))
            .map(|value| crate::runtime::csv(value))
            .filter(|values| !values.is_empty())
            .unwrap_or_else(|| strings(connector, "allowedUsers"));
        if allowed.is_empty() && connector["allowAllUsers"] != true {
            bail!("{platform}: set allowedUsers or explicitly opt in with allowAllUsers=true");
        }
    }
    Ok(())
}

pub(crate) fn mattermost_receipt_dir(config: &Value) -> Result<Option<&std::path::Path>> {
    let Some(dir) = config["mattermost"].get("turnReceiptDir") else {
        return Ok(None);
    };
    let path = dir
        .as_str()
        .map(std::path::Path::new)
        .filter(|path| path.is_absolute())
        .ok_or_else(|| anyhow::anyhow!("mattermost.turnReceiptDir must be an absolute path"))?;
    if config["mattermost"]["threadIsolation"] != true {
        bail!("mattermost.turnReceiptDir requires threadIsolation=true");
    }
    Ok(Some(path))
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
    if tools.get("reviewQueueDir").is_some_and(|path| {
        !enabled
            || !path
                .as_str()
                .is_some_and(|value| std::path::Path::new(value).is_absolute())
    }) {
        bail!("agentMessaging.tools.reviewQueueDir requires enabled tools and an absolute path");
    }
    if tools
        .keys()
        .any(|key| !["enabled", "harness", "reviewQueueDir"].contains(&key.as_str()))
    {
        bail!("agentMessaging.tools contains an unsupported setting");
    }
    Ok(())
}

pub fn load(input: &str, env: &BTreeMap<String, String>) -> Result<Value> {
    let supplied: Value = serde_json::from_str(input).unwrap_or_else(|_| serde_json::json!({}));
    if supplied.get("agents").is_some() {
        // Validate duplicate members before losing them to Value's map semantics.
        // Leave interpolation and defaults to each individual agent config.
        return Ok(crate::settings::parse_object(input)?);
    }
    normalize(supplied, env)
}

pub(crate) fn normalize(mut supplied: Value, env: &BTreeMap<String, String>) -> Result<Value> {
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
