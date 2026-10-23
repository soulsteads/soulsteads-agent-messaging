#![cfg(feature = "agent-network")]

use async_trait::async_trait;
use serde_json::{Value, json};
use soulsteads_messaging::{
    config,
    runtime::{Bridge, Event, Sink},
};
use std::{
    collections::BTreeMap,
    path::PathBuf,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::time::{sleep, timeout};

struct SinkRecorder(Mutex<Vec<Value>>);

#[async_trait]
impl Sink for SinkRecorder {
    async fn emit(&self, event: Value) -> anyhow::Result<()> {
        self.0.lock().unwrap().push(event);
        Ok(())
    }
}

struct ReleaseHeldPrompt(PathBuf);

impl Drop for ReleaseHeldPrompt {
    fn drop(&mut self) {
        let _ = std::fs::write(&self.0, "release");
    }
}

fn registrations(audit: &std::path::Path) -> Vec<(String, Value)> {
    std::fs::read_to_string(audit)
        .unwrap()
        .lines()
        .filter_map(|line| serde_json::from_str::<Value>(line).ok())
        .filter_map(|row| {
            let method = row["method"].as_str()?.to_owned();
            if !matches!(
                method.as_str(),
                "session/new" | "session/load" | "session/resume"
            ) {
                return None;
            }
            let servers = row["params"]["mcpServers"].as_array()?;
            servers
                .iter()
                .find(|server| server["name"] == "soulsteads_agents")
                .cloned()
                .map(|server| (method, server))
        })
        .collect()
}

fn audit_methods(audit: &std::path::Path) -> String {
    std::fs::read_to_string(audit)
        .unwrap()
        .lines()
        .filter_map(|line| serde_json::from_str::<Value>(line).ok())
        .map(|row| {
            let has_registration = row["params"]["mcpServers"]
                .as_array()
                .is_some_and(|servers| {
                    servers
                        .iter()
                        .any(|server| server["name"] == "soulsteads_agents")
                });
            format!(
                "{}(registration={has_registration})",
                row["method"].as_str().unwrap_or("?")
            )
        })
        .collect::<Vec<_>>()
        .join(", ")
}

fn authorization(registration: &Value) -> &str {
    registration["headers"][0]["value"].as_str().unwrap()
}

async fn wait_for_file(path: &std::path::Path) {
    timeout(Duration::from_secs(10), async {
        while !path.exists() {
            sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("fixture did not enter the held response");
}

#[tokio::test]
async fn reload_publishes_new_agent_tools_and_revokes_old_registration() {
    let root = tempfile::tempdir().unwrap();
    let audit = root.path().join("rpc.jsonl");
    let script = root.path().join("script.json");
    let held_started = root.path().join("held-started");
    let held_release = root.path().join("held-release");
    std::fs::write(
        &script,
        json!([
            {"text":"warm response","stopReason":"end_turn"},
            {"text":"held response","startedFile":held_started,"releaseFile":held_release,"stopReason":"end_turn"}
        ])
        .to_string(),
    )
    .unwrap();
    let config = config::load(
        &json!({
            "rateLimitSeconds":0,
            "sessionStorePath":root.path().join("store.json"),
            "sessionPicker":{"enabled":true,"connectors":["web"]},
            "acp":{"defaultBackend":"codex","backends":{"codex":{
                "command":env!("CARGO_BIN_EXE_acp-fixture"),"args":[],
                "env":{"ACP_FIXTURE_RPC_AUDIT":audit,"ACP_FIXTURE_MATRIX_SCRIPT":script,
                       "ACP_FIXTURE_MATRIX_COUNTER":root.path().join("counter")}
            }}},
            "agentMessaging":{"enabled":true,"tools":{"enabled":true,"harness":"codex"},"agents":[
                {"id":"owner","name":"Owner","target":{"platform":"web","context":"owner","backend":"codex"},"allowedSenders":[]},
                {"id":"peer","name":"Peer","target":{"platform":"web","context":"peer","backend":"codex"},"allowedSenders":["owner"]}
            ]},
            "web":{"enabled":true},
            "matrix":{"enabled":false},"telegram":{"enabled":false},"mattermost":{"enabled":false},
            "discord":{"enabled":false},"slack":{"enabled":false},"whatsapp":{"enabled":false}
        })
        .to_string(),
        &BTreeMap::new(),
    )
    .unwrap();
    let bridge =
        Arc::new(Bridge::new(config, BTreeMap::new(), root.path().join("sessions")).unwrap());
    bridge.prepare_agent_tools().await.unwrap();
    let sink = Arc::new(SinkRecorder(Mutex::new(Vec::new())));

    bridge
        .handle(Event::web("owner", "warm"), sink.as_ref())
        .await
        .unwrap();
    let initial_registrations = registrations(&audit);
    assert_eq!(
        initial_registrations.len(),
        1,
        "initial audited methods: {}",
        audit_methods(&audit)
    );
    let old = initial_registrations[0].1.clone();

    bridge
        .handle_web(Event::web("owner", "/reload"), sink.as_ref())
        .await
        .unwrap();
    let reloaded_registrations = registrations(&audit);
    assert!(
        reloaded_registrations.len() >= 2,
        "reload should emit a second audited tool registration; methods: {}",
        audit_methods(&audit)
    );
    let new = reloaded_registrations.last().unwrap().1.clone();
    assert_ne!(
        authorization(&old),
        authorization(&new),
        "reload must publish a fresh agent-tool registration"
    );

    let _release_guard = ReleaseHeldPrompt(held_release.clone());
    let held_bridge = bridge.clone();
    let held_sink = sink.clone();
    let held_prompt = tokio::spawn(async move {
        held_bridge
            .handle(Event::web("owner", "hold root active"), held_sink.as_ref())
            .await
    });
    wait_for_file(&held_started).await;

    let request = |registration: &Value, id: u64| {
        reqwest::Client::new()
            .post(registration["url"].as_str().unwrap())
            .header("Authorization", authorization(registration))
            .json(&json!({
                "jsonrpc":"2.0",
                "id":id,
                "method":"tools/call",
                "params":{"name":"list_agents","arguments":{}}
            }))
    };

    let current = request(&new, 1).send().await;
    let current = current.expect("reloaded endpoint should accept authenticated RPC");
    assert!(
        current.status().is_success(),
        "reloaded endpoint should serve the active human root, status {}",
        current.status()
    );
    let current: Value = current.json().await.unwrap();
    let agents = current["result"]["structuredContent"]["agents"]
        .as_array()
        .expect("list_agents should return structured agent data");
    assert!(agents.iter().any(|agent| agent["id"] == "peer"));
    assert!(agents.iter().all(|agent| agent["id"] != "owner"));

    let stale = request(&old, 2).send().await;
    let old_registration_denied = match stale {
        Err(_) => true,
        Ok(response) if !response.status().is_success() => true,
        Ok(response) => response
            .json::<Value>()
            .await
            .is_ok_and(|body| body["error"].is_object()),
    };
    assert!(
        old_registration_denied,
        "old endpoint must be closed or explicitly deny its RPC after reload"
    );

    std::fs::write(&held_release, "release").unwrap();
    held_prompt.await.unwrap().unwrap();
    bridge.shutdown().await.unwrap();
}
