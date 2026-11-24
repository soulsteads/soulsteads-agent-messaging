//! Read-only settings inspection shared by operator interfaces.

use crate::{config, delivery::AdmissionConfig, private_journal::Disk, runtime};
use serde::{
    Deserialize, Deserializer, Serialize,
    de::{self, MapAccess, SeqAccess, Visitor},
};
use serde_json::{Map, Value};
use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    io::ErrorKind,
    path::Path,
};

#[derive(Debug)]
pub struct Error {
    pub category: &'static str,
    guidance: &'static str,
}

impl fmt::Display for Error {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}: {}", self.category, self.guidance)
    }
}

impl std::error::Error for Error {}

fn error(category: &'static str, guidance: &'static str) -> Error {
    Error { category, guidance }
}

#[derive(Clone, Copy, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Source {
    Default,
    Configured,
    EnvOverride,
}

impl Source {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Default => "default",
            Self::Configured => "configured",
            Self::EnvOverride => "env_override",
        }
    }
}

#[derive(Serialize)]
pub struct Entry {
    pub path: String,
    pub source: Source,
}

pub struct Settings {
    supplied: Value,
    normalized: Value,
    environment: BTreeMap<String, String>,
    overrides: BTreeSet<String>,
}

impl Settings {
    pub fn load(
        cwd: &Path,
        explicit: Option<&Path>,
        ambient: BTreeMap<String, String>,
    ) -> Result<Self, Error> {
        let path = cwd.join(explicit.unwrap_or_else(|| Path::new("soulsteads-messaging.json")));
        let supplied = match config::read_settings_file(&path)
            .and_then(|bytes| String::from_utf8(bytes).map_err(std::io::Error::other))
        {
            Ok(input) => parse_object(&input)?,
            Err(failure) if explicit.is_none() && failure.kind() == ErrorKind::NotFound => {
                Value::Object(Map::new())
            }
            Err(_) => {
                return Err(error(
                    "config_read",
                    "Use a readable, regular UTF-8 configuration file of at most 1 MiB.",
                ));
            }
        };
        let defaults = config::defaults();
        for (key, value) in defaults.as_object().expect("object defaults") {
            if value.is_object()
                && let Some(value) = supplied.get(key)
                && !(value.is_object()
                    || matches!(key.as_str(), "toolMessages" | "web") && value.is_array())
            {
                return Err(error(
                    "invalid_shape",
                    "Use JSON objects for connector and settings sections. Legacy toolMessages and web arrays are also accepted.",
                ));
            }
        }
        if supplied
            .get("agentMessaging")
            .is_some_and(|value| !value.is_object())
        {
            return Err(error(
                "invalid_shape",
                "Use a JSON object for agentMessaging.",
            ));
        }
        if supplied
            .pointer("/acp/backends")
            .is_some_and(|value| !value.is_object())
        {
            return Err(error(
                "invalid_shape",
                "Use a JSON object for acp.backends.",
            ));
        }
        let environment = config::settings_environment(cwd, ambient).map_err(|_| {
            error(
                "environment",
                "Use readable, regular project dotenv files of at most 1 MiB and check their syntax.",
            )
        })?;
        let mut normalized = config::normalize(supplied.clone(), &environment).map_err(|_| {
            error(
                "agent_tools",
                "Check agentMessaging.tools, its harness, and the ACP transport settings.",
            )
        })?;
        let mut overrides = BTreeSet::new();
        config::apply_runtime_overrides_with(&mut normalized, &environment, |section, field| {
            overrides.insert(format!("/{section}/{field}"));
        })
        .map_err(|_| {
            error(
                "runtime_override",
                "Check connector environment overrides, including WEB_PORT (0–65535).",
            )
        })?;
        Ok(Self {
            supplied,
            normalized,
            environment,
            overrides,
        })
    }

    pub fn list(&self) -> Vec<Entry> {
        let mut paths = BTreeSet::new();
        default_paths(&config::defaults(), String::new(), &mut paths);
        for (key, path, prefix) in [
            ("toolMessages", "/toolMessages", "/toolMessages/"),
            ("web", "/web", "/web/"),
        ] {
            if self.normalized[key].is_array() {
                paths.retain(|path| !path.starts_with(prefix));
                paths.insert(path.to_owned());
            }
        }
        for path in [
            "/agentMessaging/enabled",
            "/agentMessaging/tools/enabled",
            "/agentMessaging/tools/harness",
            "/agentMessaging/tools/reviewQueueDir",
            "/mattermost/turnReceiptDir",
        ] {
            if self.supplied.pointer(path).is_some() {
                paths.insert(path.to_owned());
            }
        }
        paths
            .into_iter()
            .map(|path| Entry {
                source: if self.overrides.contains(&path) {
                    Source::EnvOverride
                } else if self.supplied.pointer(&path).is_some()
                    || path == "/toolMessages/showOutputFor"
                        && self.supplied["streamTools"].is_array()
                {
                    Source::Configured
                } else {
                    Source::Default
                },
                path,
            })
            .collect()
    }

    pub fn get(&self, path: &str) -> Result<Entry, Error> {
        self.list()
            .into_iter()
            .find(|entry| entry.path == path)
            .ok_or_else(|| {
                error(
                    "unknown_setting",
                    "Choose a JSON Pointer shown by settings list.",
                )
            })
    }

    /// Checks local startup configuration without constructing stores or a Bridge.
    pub fn validate(&self) -> Result<(), Error> {
        // Check receipt constraints separately for actionable diagnostics.
        config::mattermost_receipt_dir(&self.normalized)
            .and_then(|path| path.map_or(Ok(()), Disk::validate_directory))
            .map_err(|_| error(
                "receipt_directory",
                "On Unix, use an existing absolute, non-symlink mattermost.turnReceiptDir owned by the bridge user with mode 0700, and set mattermost.threadIsolation to true.",
            ))?;
        config::validate_connector_access(&self.normalized, &self.environment).map_err(|_| error(
            "connector_access", "For each enabled remote connector, configure allowedUsers or explicitly set allowAllUsers to true.",
        ))?;
        crate::connectors::telegram::skill_reviews::validate_receipt_directory(&self.normalized)
            .map_err(|_| error(
                "skillstore_receipt_directory",
                "On Unix, use an existing absolute directory (not a symlink) for telegram.skillReviews.receiptDir, owned by the bridge user with no group or other permissions.",
            ))?;
        config::enabled_web_port(&self.normalized)
            .map_err(|_| error("web_port", "Set web.port to an integer from 0 to 65535."))?;
        config::validate_platform_sdk_support(&self.normalized).map_err(|_| {
            error(
                "platform_sdks_feature",
                "Enabled Matrix and WhatsApp connectors require the platform-sdks build feature.",
            )
        })?;
        // Agent home normalization can inspect directory metadata. Keep the
        // normalized paths local to validation and out of all reports.
        let mut configuration = self.normalized.clone();
        runtime::normalize_agent_homes(&mut configuration, &self.environment).map_err(|_| error(
            "agent_home", "Check that configured ACP agent homes are existing absolute or ~/ directories and compatible with profileDir.",
        ))?;
        if configuration["agentMessaging"]["tools"]["enabled"] == true
            && !cfg!(feature = "agent-network")
        {
            return Err(error(
                "agent_tools_feature",
                "Enabled agentMessaging.tools requires the agent-network build feature.",
            ));
        }
        AdmissionConfig::from_value(&configuration)
            .map_err(|_| error("agent_admission", "Check agentMessaging.agents for valid owners, unique targets, and allowed senders."))?;
        if let Some(path) = configuration
            .pointer("/agentMessaging/tools/reviewQueueDir")
            .and_then(Value::as_str)
        {
            Disk::validate_directory(Path::new(path)).map_err(|_| error(
                "review_queue_directory",
                "On Unix, use an existing absolute, non-symlink agentMessaging.tools.reviewQueueDir owned by the bridge user with mode 0700.",
            ))?;
        }
        if config::string(&configuration, "sessionStorePath").is_empty() {
            return Err(error(
                "session_store",
                "Set sessionStorePath to a nonempty string.",
            ));
        }
        Ok(())
    }
}

fn default_paths(value: &Value, path: String, paths: &mut BTreeSet<String>) {
    match path.as_str() {
        "/matrix/accessToken" | "/matrix/password" | "/mattermost/token" | "/telegram/token" => {
            return;
        }
        _ => {}
    }
    if let Some(fields) = value.as_object() {
        for (key, value) in fields {
            let segment = key.replace('~', "~0").replace('/', "~1");
            default_paths(value, format!("{path}/{segment}"), paths);
        }
    } else {
        paths.insert(path);
    }
}

const DUPLICATE: &str = "duplicate settings object member";

fn parse_object(input: &str) -> Result<Value, Error> {
    let StrictValue(value) = serde_json::from_str(input).map_err(|failure| {
        if failure.to_string().starts_with(DUPLICATE) {
            error(
                "duplicate_key",
                "Remove repeated keys from the configuration objects.",
            )
        } else {
            error(
                "invalid_json",
                "Correct the JSON syntax in the configuration file.",
            )
        }
    })?;
    if !value.is_object() {
        return Err(error(
            "expected_object",
            "Use a JSON object at the configuration root.",
        ));
    }
    Ok(value)
}

// serde_json::Value normally keeps the last value for a duplicate member.
struct StrictValue(Value);

impl<'de> Deserialize<'de> for StrictValue {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_any(StrictVisitor).map(Self)
    }
}

struct StrictVisitor;

impl<'de> Visitor<'de> for StrictVisitor {
    type Value = Value;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a JSON value with unique object members")
    }

    fn visit_unit<E: de::Error>(self) -> Result<Value, E> {
        Ok(Value::Null)
    }

    fn visit_bool<E: de::Error>(self, value: bool) -> Result<Value, E> {
        Ok(value.into())
    }

    fn visit_i64<E: de::Error>(self, value: i64) -> Result<Value, E> {
        Ok(value.into())
    }

    fn visit_u64<E: de::Error>(self, value: u64) -> Result<Value, E> {
        Ok(value.into())
    }

    fn visit_f64<E: de::Error>(self, value: f64) -> Result<Value, E> {
        Ok(value.into())
    }

    fn visit_str<E: de::Error>(self, value: &str) -> Result<Value, E> {
        Ok(value.into())
    }

    fn visit_seq<A: SeqAccess<'de>>(self, mut sequence: A) -> Result<Value, A::Error> {
        let mut values = Vec::new();
        while let Some(StrictValue(value)) = sequence.next_element()? {
            values.push(value);
        }
        Ok(Value::Array(values))
    }

    fn visit_map<A: MapAccess<'de>>(self, mut object: A) -> Result<Value, A::Error> {
        let mut fields = Map::new();
        while let Some(key) = object.next_key::<String>()? {
            if fields.contains_key(&key) {
                return Err(de::Error::custom(DUPLICATE));
            }
            let StrictValue(value) = object.next_value()?;
            fields.insert(key, value);
        }
        Ok(Value::Object(fields))
    }
}
