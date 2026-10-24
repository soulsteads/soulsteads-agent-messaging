//! Private, inert A2A configuration and exact inbound allowlist policy.
use anyhow::{Result, bail};
use serde_json::Value;
use std::collections::BTreeSet;

const ID_MAX_BYTES: usize = 128;

pub(crate) fn validate(value: &Value) -> Result<()> {
    let Some(a2a) = value.get("a2a") else {
        return Ok(());
    };
    let object = a2a
        .as_object()
        .ok_or_else(|| anyhow::anyhow!("a2a must be an object"))?;
    if contains_substitution(a2a) {
        bail!("a2a does not support environment substitution");
    }
    if object
        .keys()
        .any(|key| !["version", "enabled", "peers", "agents"].contains(&key.as_str()))
    {
        bail!("a2a contains an unsupported setting");
    }
    let enabled = object
        .get("enabled")
        .and_then(Value::as_bool)
        .ok_or_else(|| anyhow::anyhow!("a2a.enabled must be a boolean"))?;
    if let Some(version) = object.get("version")
        && version.as_u64() != Some(1)
    {
        bail!("a2a.version must be integer 1");
    }
    let peers: &[Value] = match object.get("peers") {
        Some(v) => v
            .as_array()
            .map(Vec::as_slice)
            .ok_or_else(|| anyhow::anyhow!("a2a.peers must be an array"))?,
        None => &[],
    };
    let agents: &[Value] = match object.get("agents") {
        Some(v) => v
            .as_array()
            .map(Vec::as_slice)
            .ok_or_else(|| anyhow::anyhow!("a2a.agents must be an array"))?,
        None => &[],
    };
    if enabled && object.get("version").and_then(Value::as_u64) != Some(1) {
        bail!("enabled a2a requires version 1");
    }
    if enabled && peers.is_empty() {
        bail!("enabled a2a requires a nonempty peers array");
    }
    if enabled && agents.is_empty() {
        bail!("enabled a2a requires a nonempty agents array");
    }
    let mut peer_ids = BTreeSet::new();
    for peer in peers {
        let peer = peer
            .as_object()
            .ok_or_else(|| anyhow::anyhow!("a2a peer must be an object"))?;
        if peer
            .keys()
            .any(|key| !["id", "verifierPath"].contains(&key.as_str()))
        {
            bail!("a2a peer contains an unsupported setting");
        }
        if let Some(value) = peer.get("id") {
            let id = value
                .as_str()
                .filter(|id| valid_id(id))
                .ok_or_else(|| anyhow::anyhow!("a2a peer id is invalid"))?;
            if !peer_ids.insert(id.to_owned()) {
                bail!("a2a peer IDs must be unique");
            }
        } else if enabled {
            bail!("a2a peer id is required");
        }
        if let Some(value) = peer.get("verifierPath") {
            let path = value
                .as_str()
                .ok_or_else(|| anyhow::anyhow!("a2a peer verifierPath must be a string"))?;
            if path.is_empty() || !std::path::Path::new(path).is_absolute() {
                bail!("a2a peer verifierPath must be a nonempty absolute path");
            }
        } else if enabled {
            bail!("a2a peer verifierPath is required");
        }
    }
    let acp = value.get("acp").and_then(Value::as_object);
    let backends = acp
        .and_then(|a| a.get("backends"))
        .and_then(Value::as_object);
    let mut agent_ids = BTreeSet::new();

    for agent in agents {
        let agent = agent
            .as_object()
            .ok_or_else(|| anyhow::anyhow!("a2a agent must be an object"))?;
        if agent
            .keys()
            .any(|key| !["id", "backend", "allowedPeers"].contains(&key.as_str()))
        {
            bail!("a2a agent contains an unsupported setting");
        }
        let id = match agent.get("id") {
            Some(v) => v
                .as_str()
                .filter(|id| valid_id(id))
                .ok_or_else(|| anyhow::anyhow!("a2a agent id is invalid"))?,
            None if enabled => bail!("a2a agent id is required"),
            None => "",
        };
        if !id.is_empty() && !agent_ids.insert(id.to_owned()) {
            bail!("a2a agent IDs must be unique");
        }

        let backend = match agent.get("backend") {
            Some(v) => v
                .as_str()
                .filter(|s| valid_id(s))
                .ok_or_else(|| anyhow::anyhow!("a2a agent backend must be a valid string"))?,
            None if enabled => bail!("a2a agent backend is required"),
            None => "",
        };
        let supported_backend = ["opencode", "codex", "claude", "pi"].contains(&backend);
        let configured =
            backends.is_some_and(|items| items.get(backend).is_some_and(Value::is_object));
        if enabled && !supported_backend {
            bail!("a2a agent backend is not a supported backend");
        }
        if enabled && !configured && !(backend == "opencode" && acp.is_some()) {
            bail!("a2a agent backend is not configured");
        }
        let allowed = match agent.get("allowedPeers") {
            Some(v) => v
                .as_array()
                .ok_or_else(|| anyhow::anyhow!("a2a agent allowedPeers must be an array"))?,
            None if enabled => bail!("a2a agent allowedPeers is required"),
            None => continue,
        };
        let mut grants = BTreeSet::new();
        for peer in allowed {
            let peer = peer
                .as_str()
                .filter(|id| valid_id(id))
                .ok_or_else(|| anyhow::anyhow!("a2a allowed peer ID is invalid"))?;
            if !grants.insert(peer) {
                bail!("a2a allowedPeers contains a duplicate grant");
            }
            if !peer_ids.contains(peer) {
                bail!("a2a allowedPeers references an unknown peer");
            }
        }
    }
    if enabled {
        #[cfg(not(feature = "a2a"))]
        bail!("enabled a2a requires the a2a build feature");
    }
    Ok(())
}

fn contains_substitution(value: &Value) -> bool {
    match value {
        Value::String(text) => text.contains("{env:"),
        Value::Array(items) => items.iter().any(contains_substitution),
        Value::Object(items) => items.values().any(contains_substitution),
        _ => false,
    }
}

fn valid_id(id: &str) -> bool {
    !id.is_empty()
        && id.len() <= ID_MAX_BYTES
        && id.trim() == id
        && !id.chars().any(char::is_control)
        && !id.contains("{env:")
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub(crate) struct Allowlist {
    grants: BTreeSet<(String, String)>,
}
#[allow(dead_code)]
impl Allowlist {
    pub(crate) fn from_config(value: &Value) -> Result<Self> {
        validate(value)?;
        let mut grants = BTreeSet::new();
        if value
            .get("a2a")
            .and_then(|a| a.get("enabled"))
            .and_then(Value::as_bool)
            == Some(true)
            && let Some(agents) = value
                .get("a2a")
                .and_then(|a| a.get("agents"))
                .and_then(Value::as_array)
        {
            for agent in agents {
                let id = agent["id"].as_str().expect("validated A2A agent ID");
                for peer in agent["allowedPeers"]
                    .as_array()
                    .expect("validated A2A grants")
                    .iter()
                    .filter_map(Value::as_str)
                {
                    grants.insert((peer.to_owned(), id.to_owned()));
                }
            }
        }
        Ok(Self { grants })
    }
    pub(crate) fn permits(&self, peer_id: &str, agent_id: &str) -> bool {
        self.grants
            .contains(&(peer_id.to_owned(), agent_id.to_owned()))
    }
}

#[cfg(test)]
mod policy_tests {
    use super::*;
    #[cfg(feature = "a2a")]
    #[test]
    fn allowlist_is_exact_and_denies_by_default() {
        let mut value = serde_json::json!({"a2a":{"version":1,"enabled":true,"peers":[{"id":"review-peer","verifierPath":"/missing"}],"agents":[{"id":"reviewer","backend":"opencode","allowedPeers":["review-peer"]},{"id":"private-agent","backend":"opencode","allowedPeers":[]}]}});
        value["acp"] = crate::config::defaults()["acp"].clone();
        let policy = Allowlist::from_config(&value).unwrap();
        assert!(policy.permits("review-peer", "reviewer"));
        assert!(!policy.permits("Review-peer", "reviewer"));
        assert!(!policy.permits("review-peer", "private-agent"));
        assert!(!policy.permits("local-agent", "reviewer"));
        assert!(!policy.permits("unknown-peer", "unknown-agent"));
    }
    #[test]
    fn identity_is_opaque_bounded_and_never_normalized() {
        for invalid in [
            "",
            " reviewer",
            "reviewer ",
            "review\ner",
            "a".repeat(ID_MAX_BYTES + 1).as_str(),
            "{env:ID}",
        ] {
            assert!(!valid_id(invalid), "accepted {invalid:?}");
        }
        assert!(valid_id("reviewer"));
    }
    #[test]
    fn disabled_configuration_never_creates_policy_grants() {
        let policy = Allowlist::from_config(&serde_json::json!({"a2a":{"enabled":false,"peers":[{"id":"review-peer","verifierPath":"/missing"}],"agents":[{"id":"reviewer","allowedPeers":["review-peer"]}]}})).unwrap();
        assert!(!policy.permits("review-peer", "reviewer"));
    }
    #[test]
    fn policy_rejects_unvalidated_raw_grants() {
        let result = Allowlist::from_config(
            &serde_json::json!({"a2a":{"enabled":true,"agents":[{"id":"reviewer","allowedPeers":["unconfigured"]}]}}),
        );
        assert!(result.is_err());
    }
}
