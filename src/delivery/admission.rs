use super::AgentId;
use anyhow::{Result, bail};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ConfiguredOwner {
    pub agent: AgentId,
    pub platform: String,
    pub context: String,
    pub backend: String,
    pub allowed_senders: BTreeSet<AgentId>,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct AdmissionConfig {
    owners: BTreeMap<AgentId, ConfiguredOwner>,
    targets: BTreeMap<(String, String), AgentId>,
}

impl AdmissionConfig {
    pub(crate) fn from_value(config: &Value) -> Result<Option<Self>> {
        let Some(value) = config.get("agentMessaging") else {
            return Ok(None);
        };
        if value.get("enabled").and_then(Value::as_bool) != Some(true) {
            return Ok(None);
        }
        let owners = value
            .get("agents")
            .and_then(Value::as_array)
            .ok_or_else(|| {
                anyhow::anyhow!("agentMessaging.agents must be an array when enabled")
            })?;
        let mut result = Self::default();
        for owner in owners {
            let object = owner
                .as_object()
                .ok_or_else(|| anyhow::anyhow!("agentMessaging owner must be an object"))?;
            let agent = required_agent(object.get("id"), "agentMessaging agent id")?;
            if !["name", "label"].iter().any(|key| {
                object
                    .get(*key)
                    .and_then(Value::as_str)
                    .is_some_and(|value| !value.is_empty())
            }) {
                bail!("agentMessaging agent name or label is required");
            }
            let target = object
                .get("target")
                .and_then(Value::as_object)
                .ok_or_else(|| anyhow::anyhow!("agentMessaging owner target is required"))?;
            let platform =
                required_string(target.get("platform"), "agentMessaging target platform")?;
            let context = required_string(target.get("context"), "agentMessaging target context")?;
            let backend = required_string(target.get("backend"), "agentMessaging target backend")?;
            let mut allowed_senders = BTreeSet::new();
            let senders = object
                .get("allowedSenders")
                .and_then(Value::as_array)
                .ok_or_else(|| anyhow::anyhow!("agentMessaging allowedSenders must be an array"))?;
            for sender in senders {
                allowed_senders.insert(required_agent(
                    Some(sender),
                    "agentMessaging allowed sender",
                )?);
            }
            let configured = ConfiguredOwner {
                agent: agent.clone(),
                platform: platform.clone(),
                context: context.clone(),
                backend,
                allowed_senders,
            };
            if result.owners.insert(agent.clone(), configured).is_some() {
                bail!("agentMessaging owner is duplicated");
            }
            if result.targets.insert((platform, context), agent).is_some() {
                bail!("agentMessaging targets must have one owner");
            }
        }
        Ok(Some(result))
    }

    pub(crate) fn owner_at(
        &self,
        platform: &str,
        context: &str,
        backend: &str,
    ) -> Option<&ConfiguredOwner> {
        let agent = self
            .targets
            .get(&(platform.to_owned(), context.to_owned()))?;
        let owner = self.owners.get(agent)?;
        (owner.backend == backend).then_some(owner)
    }

    pub(crate) fn owner(&self, agent: &AgentId) -> Option<&ConfiguredOwner> {
        self.owners.get(agent)
    }
}

fn required_string(value: Option<&Value>, field: &'static str) -> Result<String> {
    let value = value
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty());
    value
        .map(str::to_owned)
        .ok_or_else(|| anyhow::anyhow!("{field} is required"))
}

fn required_agent(value: Option<&Value>, field: &'static str) -> Result<AgentId> {
    let value = required_string(value, field)?;
    AgentId::try_from(value.as_str()).map_err(|_| anyhow::anyhow!("{field} is invalid"))
}
