//! Review pending Skillstore proposals from an explicitly configured Telegram DM.

use super::inbound;
use crate::{
    connectors::http::{Platform, read_json},
    runtime::{Bridge, Event},
};
use anyhow::{Context, Result, bail, ensure};
use serde::Deserialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{collections::BTreeSet, fs::OpenOptions, io::Read, path::PathBuf, time::Duration};
use tokio::sync::watch;

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct Settings {
    enabled: bool,
    registry_url: Option<String>,
    reviewer_user_id: Option<i64>,
    reviewer_token_file: Option<PathBuf>,
    receipt_dir: Option<PathBuf>,
    #[serde(default)]
    skills: BTreeSet<String>,
    poll_seconds: Option<u64>,
    allow_decisions: Option<bool>,
    #[serde(default)]
    pub(super) reviews_only: bool,
}

pub(crate) fn settings(config: &Value) -> Result<Option<Settings>> {
    let Some(value) = config["telegram"].get("skillReviews") else {
        return Ok(None);
    };
    let settings: Settings = serde_json::from_value(value.clone())
        .map_err(|_| anyhow::anyhow!("invalid telegram.skillReviews configuration"))?;
    if !settings.enabled {
        return Ok(None);
    }
    let url = settings
        .registry_url
        .as_deref()
        .context("skillReviews.registryUrl is required")?;
    let url =
        url::Url::parse(url).map_err(|_| anyhow::anyhow!("invalid skillReviews.registryUrl"))?;
    let loopback = matches!(url.host(), Some(url::Host::Domain("localhost")))
        || url
            .host_str()
            .and_then(|host| {
                host.trim_matches(['[', ']'])
                    .parse::<std::net::IpAddr>()
                    .ok()
            })
            .is_some_and(|address| address.is_loopback());
    ensure!(
        config["telegram"]["enabled"] == true
            && url.host().is_some()
            && (url.scheme() == "https" || url.scheme() == "http" && loopback)
            && url.username().is_empty()
            && url.password().is_none()
            && url.query().is_none()
            && url.fragment().is_none()
            && settings.reviewer_user_id.is_some_and(|id| id > 0)
            && settings
                .reviewer_token_file
                .as_ref()
                .is_some_and(|path| path.is_absolute())
            && settings
                .receipt_dir
                .as_ref()
                .is_some_and(|path| path.is_absolute())
            && settings
                .poll_seconds
                .is_none_or(|seconds| (5..=3600).contains(&seconds))
            && settings.skills.iter().all(|name| valid_name(name))
            && (!settings.reviews_only
                || [
                    "matrix",
                    "whatsapp",
                    "discord",
                    "slack",
                    "mattermost",
                    "web"
                ]
                .iter()
                .all(|connector| config[*connector]["enabled"] != true)),
        "skillReviews requires Telegram, a registry URL, reviewer user ID, and absolute token and receipt paths; reviewsOnly requires other connectors disabled"
    );
    Ok(Some(settings))
}

fn valid_name(name: &str) -> bool {
    name.len() <= 64
        && name.split('-').all(|part| {
            !part.is_empty()
                && part
                    .bytes()
                    .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit())
        })
}

fn valid_id(id: &str) -> bool {
    !id.is_empty()
        && id.len() <= 64
        && id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
}

pub(super) struct SkillReviews {
    endpoint: url::Url,
    reviewer: i64,
    token: String,
    receipt_dir: PathBuf,
    skills: BTreeSet<String>,
    period: Duration,
    allow_decisions: bool,
    pub(super) reviews_only: bool,
    client: reqwest::Client,
}

#[cfg(unix)]
fn private_token(path: &std::path::Path) -> Result<String> {
    use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
    let file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open(path)
        .context("cannot open Skillstore reviewer token file")?;
    let metadata = file.metadata()?;
    ensure!(
        metadata.is_file()
            && metadata.uid() == unsafe { libc::geteuid() }
            && metadata.mode() & 0o077 == 0
            && metadata.nlink() == 1,
        "Skillstore reviewer token must be an owner-only regular file"
    );
    let mut token = String::new();
    file.take(1025).read_to_string(&mut token)?;
    let token = token.trim();
    ensure!(
        !token.is_empty()
            && token.len() <= 1024
            && token.bytes().all(|byte| byte.is_ascii_graphic()),
        "invalid Skillstore reviewer token"
    );
    Ok(token.to_owned())
}

#[cfg(unix)]
fn check_receipt_dir(path: &std::path::Path) -> Result<()> {
    use std::os::unix::fs::MetadataExt;
    let metadata = std::fs::symlink_metadata(path)
        .context("cannot read Skillstore notification receipt directory")?;
    ensure!(
        metadata.is_dir()
            && metadata.uid() == unsafe { libc::geteuid() }
            && metadata.mode() & 0o077 == 0,
        "Skillstore receipts require an owner-only directory"
    );
    Ok(())
}

impl SkillReviews {
    #[cfg(unix)]
    pub(super) fn new(settings: Settings) -> Result<Self> {
        let mut endpoint = url::Url::parse(&settings.registry_url.expect("validated"))?;
        endpoint.set_path(&format!("{}/v1/rpc", endpoint.path().trim_end_matches('/')));
        let reviewer = settings.reviewer_user_id.expect("validated");
        let receipt_dir = settings.receipt_dir.expect("validated");
        check_receipt_dir(&receipt_dir)?;
        Ok(Self {
            endpoint,
            reviewer,
            receipt_dir,
            skills: settings.skills,
            token: private_token(&settings.reviewer_token_file.expect("validated"))?,
            period: Duration::from_secs(settings.poll_seconds.unwrap_or(30)),
            allow_decisions: settings.allow_decisions.unwrap_or(true),
            reviews_only: settings.reviews_only,
            client: reqwest::Client::builder()
                .timeout(Duration::from_secs(15))
                .redirect(reqwest::redirect::Policy::none())
                .build()?,
        })
    }

    #[cfg(not(unix))]
    pub(super) fn new(_settings: Settings) -> Result<Self> {
        bail!("Telegram Skillstore review currently requires a Unix host");
    }

    async fn rpc(&self, method: &str, params: Value) -> Result<Value> {
        let response = self
            .client
            .post(self.endpoint.clone())
            .bearer_auth(&self.token)
            .json(&json!({"method":method,"params":params}))
            .send()
            .await
            .map_err(|_| anyhow::anyhow!("Skillstore request failed"))?;
        let status = response.status();
        let envelope = read_json(response).await?;
        ensure!(
            status.is_success() && envelope["ok"] == true,
            "Skillstore refused the request"
        );
        envelope
            .get("result")
            .cloned()
            .context("Skillstore returned no result")
    }

    fn covers(&self, name: &str) -> bool {
        valid_name(name) && (self.skills.is_empty() || self.skills.contains(name))
    }

    pub(super) fn permits(&self, bridge: &Bridge, event: &Event) -> bool {
        let id = self.reviewer.to_string();
        event.direct && event.user == id && event.room == id && bridge.user_allowed(event)
    }

    pub(super) async fn notices(
        &self,
        platform: &Platform,
        bridge: &Bridge,
        mut shutdown: watch::Receiver<bool>,
    ) {
        let mut ticks = tokio::time::interval(self.period);
        ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        while !*shutdown.borrow() {
            tokio::select! {
                _ = shutdown.changed() => break,
                _ = ticks.tick() => {},
            }
            let _ = tokio::select! {
                _ = shutdown.changed() => break,
                result = self.notify(platform, bridge) => result,
            }
            .inspect_err(|_| eprintln!("Telegram Skillstore review notification check failed"));
        }
    }

    async fn notify(&self, platform: &Platform, bridge: &Bridge) -> Result<()> {
        let event = inbound::map(platform, &json!({
            "from":{"id":self.reviewer},"chat":{"id":self.reviewer,"type":"private"},"message_id":0,
        })).context("could not construct the configured review destination")?;
        if !self.permits(bridge, &event) {
            return Ok(());
        }
        let info = self.rpc("info", json!({})).await?;
        let store_id = info["store_id"]
            .as_str()
            .filter(|id| valid_id(id))
            .context("invalid Skillstore ID")?;
        let proposals = self.rpc("list_proposals", json!({"status":"open"})).await?;
        for item in proposals
            .as_array()
            .context("invalid Skillstore proposal list")?
        {
            let (Some(id), Some(name), Some(digest)) = (
                item["id"].as_str(),
                item["skill"].as_str(),
                item["digest"].as_str(),
            ) else {
                continue;
            };
            if !valid_id(id) || !self.covers(name) || item["status"] != "open" {
                continue;
            }
            let hash = Sha256::digest(serde_json::to_vec(&(
                store_id,
                &platform.bot,
                self.reviewer,
                id,
                digest,
            ))?);
            let receipt = self.receipt_dir.join(format!("{hash:x}"));
            if receipt.exists() {
                continue;
            }
            self.post(
                platform,
                format!(
                    "Skillstore proposal {id} for {name} is awaiting review.\n/skillreview {id}"
                ),
            )
            .await?;
            let file = OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&receipt)?;
            file.sync_all()?;
            std::fs::File::open(&self.receipt_dir)?.sync_all()?;
        }
        Ok(())
    }

    async fn post(&self, platform: &Platform, text: String) -> Result<()> {
        platform
            .api(
                "POST",
                "sendMessage",
                json!({"chat_id": self.reviewer, "text": text}),
            )
            .await?;
        Ok(())
    }

    pub(super) async fn handle(&self, platform: &Platform, bridge: &Bridge, event: Event) {
        if !self.permits(bridge, &event) {
            return;
        }
        let response = self.control(&event.text).await.unwrap_or_else(|_| {
            "Skillstore could not confirm the request. Use /skillreview ID to check the current decision.".into()
        });
        let _ = self.post(platform, response).await;
    }

    async fn control(&self, text: &str) -> Result<String> {
        let parts: Vec<_> = text.split_whitespace().take(4).collect();
        let (command, id, version) = match parts.as_slice() {
            [command @ ("/skillreview" | "/skillreject"), id] if valid_id(id) => {
                (*command, *id, None)
            }
            ["/skillapprove", id, version] if valid_id(id) && valid_version(version) => {
                ("/skillapprove", *id, Some(*version))
            }
            _ => {
                return Ok(
                    "Use /skillreview ID, /skillapprove ID NEW_VERSION, or /skillreject ID.".into(),
                );
            }
        };
        if command != "/skillreview" && !self.allow_decisions {
            return Ok("This Telegram review inbox is read-only.".into());
        }
        let proposal = self.rpc("proposal", json!({"proposal_id":id})).await?;
        let name = proposal["skill"]
            .as_str()
            .filter(|name| self.covers(name))
            .context("proposal is outside the review scope")?;
        ensure!(proposal["id"] == id, "Skillstore returned another proposal");
        if proposal["status"] != "open" {
            return match proposal["status"].as_str() {
                Some("accepted") => {
                    let published = proposal["decision"]["released_version"]
                        .as_str()
                        .filter(|version| valid_version(version))
                        .context("invalid published version")?;
                    Ok(format!(
                        "Proposal {id} is already approved as {name}@{published}."
                    ))
                }
                Some("rejected") => Ok(format!("Proposal {id} is already rejected.")),
                _ => bail!("invalid Skillstore proposal status"),
            };
        }
        if command == "/skillreview" {
            let diff = self.rpc("proposal_diff", json!({"proposal_id":id})).await?;
            let text = diff["diff"].as_str().context("invalid Skillstore diff")?;
            if text.encode_utf16().take(3401).count() > 3400 {
                return Ok(format!(
                    "The diff for {id} is too long for Telegram. Review it with skillstore proposal-diff {id}."
                ));
            }
            let controls = if self.allow_decisions {
                format!("\n/skillapprove {id} NEW_VERSION\n/skillreject {id}")
            } else {
                String::new()
            };
            return Ok(format!("{name} · {id}\n\n{text}{controls}"));
        }
        let decision = if version.is_some() {
            "accept"
        } else {
            "reject"
        };
        self.rpc(
            "decide_proposal",
            json!({
                "proposal_id":id, "decision":decision, "version":version,
                "note":format!("Telegram review by configured user {}", self.reviewer),
            }),
        )
        .await?;
        Ok(match version {
            Some(version) => format!("Approved {id} as {name}@{version}."),
            None => format!("Rejected {id}."),
        })
    }
}

fn valid_version(version: &str) -> bool {
    !version.is_empty()
        && version.len() <= 128
        && version.as_bytes()[0].is_ascii_digit()
        && version
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b".+-".contains(&byte))
}

pub(super) fn is_control(event: &Event) -> bool {
    matches!(
        event.text.split_whitespace().next(),
        Some("/skillreview" | "/skillapprove" | "/skillreject")
    )
}
