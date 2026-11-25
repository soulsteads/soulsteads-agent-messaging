//! Local configuration for a set of independently bound agents.
use crate::{config, private_journal::Disk};
use anyhow::{Context, Result, bail, ensure};
use serde::Deserialize;
use serde_json::Value;
use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::{Path, PathBuf},
};

pub const PLATFORMS: [&str; 7] = [
    "telegram",
    "matrix",
    "mattermost",
    "web",
    "slack",
    "discord",
    "whatsapp",
];

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Manifest {
    state_dir: PathBuf,
    agents: Vec<AgentConfig>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct AgentConfig {
    id: String,
    config: Value,
}

pub struct Agent {
    pub id: String,
    pub config: Value,
    pub state: PathBuf,
}

pub struct Gateway {
    pub state: PathBuf,
    pub agents: Vec<Agent>,
}

impl Gateway {
    pub fn load(value: Value, environment: &BTreeMap<String, String>) -> Result<Self> {
        let Manifest { state_dir, agents } = serde_json::from_value(value).map_err(|_| {
            anyhow::anyhow!("Expected only stateDir and an agents list of {{id, config}} entries")
        })?;
        ensure!(!agents.is_empty(), "agents must not be empty");
        Disk::validate_directory(&state_dir)
            .context("stateDir must be an existing, owned, non-symlink directory with mode 0700")?;
        let mut ids = BTreeSet::new();
        let mut telegram_bots = BTreeSet::new();
        let mut matrix_tokens = BTreeSet::new();
        let mut mattermost_tokens = BTreeSet::new();
        let mut web_ports = BTreeSet::new();
        let mut result = Vec::with_capacity(agents.len());
        for AgentConfig { id, config } in agents {
            ensure!(
                valid_id(&id),
                "Invalid agent ID: use 1–64 lowercase ASCII letters, digits, or hyphens, starting with a letter or digit"
            );
            ensure!(ids.insert(id.clone()), "Duplicate agent ID: {id}");
            let state = state_dir.join(&id);
            let config = prepare_agent(config, &state, environment)?;
            if config["telegram"]["enabled"] == true {
                let token = config::string(&config["telegram"], "token");
                let Some((bot, suffix)) = token.split_once(':') else {
                    bail!("telegram.token must be a bot token");
                };
                let bot = bot.parse::<u64>().ok().filter(|id| *id > 0);
                ensure!(
                    bot.is_some() && !suffix.is_empty() && token.trim() == token,
                    "telegram.token must be a bot token"
                );
                ensure!(telegram_bots.insert(bot), "Duplicate Telegram bot");
            }
            if config["matrix"]["enabled"] == true {
                let token = config::string(&config["matrix"], "accessToken");
                ensure!(
                    !token.trim().is_empty(),
                    "matrix.accessToken is required in a gateway manifest"
                );
                ensure!(
                    matrix_tokens.insert(token.to_owned()),
                    "Duplicate Matrix access token"
                );
                ensure!(
                    cfg!(feature = "matrix-encryption")
                        || config["matrix"]["encryption"]["enabled"] != true,
                    "Matrix encryption requires the matrix-encryption build feature"
                );
            }
            if config["mattermost"]["enabled"] == true {
                let settings = &config["mattermost"];
                let token = config::string(settings, "token");
                ensure!(!token.trim().is_empty(), "mattermost.token is required");
                ensure!(
                    url::Url::parse(config::string(settings, "url")).is_ok_and(|url| {
                        matches!(url.scheme(), "http" | "https") && url.host().is_some()
                    }),
                    "mattermost.url must be an HTTP or HTTPS URL"
                );
                ensure!(
                    mattermost_tokens.insert(token.to_owned()),
                    "Duplicate Mattermost token"
                );
            }
            if let Some(port) = config::enabled_web_port(&config)? {
                ensure!(
                    port != 0 && !config::string(&config["web"], "host").is_empty(),
                    "Web requires a host and a nonzero port"
                );
                // Check the effective values after interpolation. The legacy
                // wildcard default is deliberately not valid in a gateway.
                ensure!(
                    config["web"]["allowedOrigins"]
                        .as_array()
                        .is_some_and(
                            |origins| !origins.is_empty() && origins.iter().all(exact_web_origin)
                        ),
                    "Enabled gateway Web bindings require web.allowedOrigins to list exact HTTP(S) origins (no wildcard or path)"
                );
                ensure!(web_ports.insert(port), "Duplicate Web port");
            }
            result.push(Agent { id, config, state });
        }
        Ok(Self {
            state: state_dir,
            agents: result,
        })
    }
}

fn exact_web_origin(value: &Value) -> bool {
    value.as_str().is_some_and(|origin| {
        !origin.contains('*')
            && url::Url::parse(origin).is_ok_and(|url| {
                matches!(url.scheme(), "http" | "https")
                    && url.origin().ascii_serialization() == origin
            })
    })
}

fn prepare_agent(
    mut input: Value,
    state: &Path,
    environment: &BTreeMap<String, String>,
) -> Result<Value> {
    Disk::validate_directory(state).context(
        "Each agent state directory must exist, be owned, non-symlink and have mode 0700",
    )?;
    let fields = input
        .as_object()
        .context("An agent config must be an object")?;
    for field in ["agents", "stateDir", "sessionStorePath", "agentMessaging"] {
        ensure!(
            !fields.contains_key(field),
            "{field} is not supported in a gateway agent config"
        );
    }
    ensure!(
        fields.get("acp").is_none_or(Value::is_object),
        "acp must be an object"
    );
    for &platform in &PLATFORMS {
        if let Some(section) = fields.get(platform) {
            ensure!(section.is_object(), "{platform} must be an object");
            ensure!(
                section.get("enabled").is_none_or(Value::is_boolean),
                "{platform}.enabled must be a boolean"
            );
        }
    }
    ensure!(
        input["sessionPicker"]["enabled"] != true,
        "sessionPicker is not supported in a gateway manifest"
    );
    ensure!(
        input.pointer("/mattermost/turnReceiptDir").is_none(),
        "mattermost.turnReceiptDir is not supported in a gateway manifest"
    );
    ensure!(
        input.pointer("/telegram/skillReviews").is_none(),
        "telegram.skillReviews is not supported in a gateway manifest"
    );
    if input["web"].is_object() && input["web"].get("host").is_none() {
        input["web"]["host"] = "127.0.0.1".into();
    }
    let mut config = config::normalize(input, environment)
        .map_err(|_| anyhow::anyhow!("Invalid agent settings"))?;
    ensure!(
        !["slack", "discord", "whatsapp"]
            .iter()
            .any(|p| config[*p]["enabled"] == true),
        "Gateway manifests support Telegram, Matrix, Mattermost and Web bindings"
    );
    ensure!(
        PLATFORMS.iter().any(|p| config[*p]["enabled"] == true),
        "No connectors enabled for agent"
    );
    config::validate_platform_sdk_support(&config)?;
    config::validate_connector_access(&config, &BTreeMap::new())?;
    crate::runtime::normalize_agent_homes(&mut config, environment)
        .map_err(|_| anyhow::anyhow!("Invalid ACP agent home"))?;
    validate_state_entry(&state.join("sessions.json"), false)?;
    validate_state_entry(&state.join("sessions"), true)?;
    if config["matrix"]["enabled"] == true {
        validate_state_entry(&state.join("matrix"), true)?;
    }
    config["sessionStorePath"] = serde_json::to_value(state.join("sessions.json"))?;
    Ok(config)
}

fn valid_id(id: &str) -> bool {
    let alphanumeric = |b: u8| b.is_ascii_lowercase() || b.is_ascii_digit();
    id.len() <= 64
        && id.bytes().next().is_some_and(alphanumeric)
        && id.bytes().all(|b| alphanumeric(b) || b == b'-')
}

fn validate_state_entry(path: &Path, directory: bool) -> Result<()> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(_) => bail!("Cannot inspect an agent state entry"),
    };
    if directory {
        ensure!(
            metadata.is_dir(),
            "Agent state directories must not be symlinks or files"
        );
    } else {
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            ensure!(
                metadata.nlink() == 1,
                "Agent session store must not be hardlinked"
            );
        }
        ensure!(
            metadata.is_file(),
            "Agent session store must be a regular non-symlink file"
        );
    }
    Ok(())
}

/// Connector credentials are resolved from explicit settings, not applied as
/// global runtime overrides or inherited by all children.
pub fn agent_environment(environment: &BTreeMap<String, String>) -> BTreeMap<String, String> {
    ["HOME", "PATH", "SESSION_RETENTION_MINS"]
        .into_iter()
        .filter_map(|key| Some((key.to_owned(), environment.get(key)?.clone())))
        .collect()
}
