#![cfg(feature = "agent-network")]

use async_trait::async_trait;
use serde_json::{Value, json};
use soulsteads_messaging::{
    config,
    runtime::{Bridge, Event, Sink},
};
use std::{collections::BTreeMap, sync::Arc, time::Duration};
use tokio::time::{sleep, timeout};

struct SinkRecorder;
#[async_trait]
impl Sink for SinkRecorder {
    async fn emit(&self, _: Value) -> anyhow::Result<()> {
        Ok(())
    }
}
struct ReleaseOnDrop(std::path::PathBuf);
impl Drop for ReleaseOnDrop {
    fn drop(&mut self) {
        let _ = std::fs::write(&self.0, "release");
    }
}

async fn wait_for(path: &std::path::Path) {
    timeout(Duration::from_secs(10), async {
        while !path.exists() {
            sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("fixture did not enter held prompt: {}", path.display()));
}

fn latest_registration(audit: &std::path::Path) -> Value {
    std::fs::read_to_string(audit)
        .unwrap()
        .lines()
        .filter_map(|line| serde_json::from_str::<Value>(line).ok())
        .filter_map(|row| {
            if !matches!(
                row["method"].as_str(),
                Some("session/new" | "session/load" | "session/resume")
            ) {
                return None;
            }
            row["params"]["mcpServers"]
                .as_array()?
                .iter()
                .find(|server| server["name"] == "soulsteads_agents")
                .cloned()
        })
        .next_back()
        .expect("owner session must register MCP tools")
}

async fn call(registration: &Value, id: u64, name: &str, args: Value) -> Value {
    reqwest::Client::new().post(registration["url"].as_str().unwrap())
        .header("Authorization", registration["headers"][0]["value"].as_str().unwrap())
        .json(&json!({"jsonrpc":"2.0","id":id,"method":"tools/call","params":{"name":name,"arguments":args}}))
        .send().await.unwrap().json().await.unwrap()
}

#[tokio::test]
async fn one_ask_budget_survives_transparent_empty_root_retry() {
    let root = tempfile::tempdir().unwrap();
    let audit = root.path().join("rpc.jsonl");
    let script = root.path().join("script.json");
    let counter = root.path().join("counter");
    let first_started = root.path().join("first-started");
    let first_release = root.path().join("first-release");
    let retry_started = root.path().join("retry-started");
    let retry_release = root.path().join("retry-release");
    let _first_guard = ReleaseOnDrop(first_release.clone());
    let _retry_guard = ReleaseOnDrop(retry_release.clone());
    std::fs::write(&script, json!([
        {"text":"peer warm","stopReason":"end_turn"},
        {"text":"","startedFile":first_started,"releaseFile":first_release,"stopReason":"end_turn"},
        {"text":"peer reply","stopReason":"end_turn"},
        {"text":"retry complete","startedFile":retry_started,"releaseFile":retry_release,"stopReason":"end_turn"},
        {"text":"second peer reply","stopReason":"end_turn"}
    ]).to_string()).unwrap();
    let settings = json!({
        "rateLimitSeconds":0,"sessionStorePath":root.path().join("store.json"),
        "acp":{"defaultBackend":"codex","backends":{"codex":{"command":env!("CARGO_BIN_EXE_acp-fixture"),"args":[],"env":{"ACP_FIXTURE_RPC_AUDIT":audit,"ACP_FIXTURE_MATRIX_SCRIPT":script,"ACP_FIXTURE_MATRIX_COUNTER":counter}}}},
        "agentMessaging":{"enabled":true,"tools":{"enabled":true,"harness":"codex"},"agents":[
            {"id":"owner","name":"Owner","target":{"platform":"web","context":"owner","backend":"codex"},"allowedSenders":[]},
            {"id":"peer","name":"Peer","target":{"platform":"web","context":"peer","backend":"codex"},"allowedSenders":["owner"]}
        ]},"web":{"enabled":true},"matrix":{"enabled":false},"telegram":{"enabled":false},"mattermost":{"enabled":false},"discord":{"enabled":false},"slack":{"enabled":false},"whatsapp":{"enabled":false}
    });
    let config = config::load(&settings.to_string(), &BTreeMap::new()).unwrap();
    let bridge =
        Arc::new(Bridge::new(config, BTreeMap::new(), root.path().join("sessions")).unwrap());
    bridge.prepare_agent_tools().await.unwrap();
    let sink = Arc::new(SinkRecorder);
    bridge
        .handle(Event::web("peer", "warm"), sink.as_ref())
        .await
        .unwrap();

    let first_bridge = bridge.clone();
    let first_sink = sink.clone();
    let first = tokio::spawn(async move {
        first_bridge
            .handle(Event::web("owner", "root"), first_sink.as_ref())
            .await
    });
    wait_for(&first_started).await;
    let initial = latest_registration(&audit);
    let ask = call(
        &initial,
        1,
        "ask_agent",
        json!({"recipient":"peer","text":"first ask"}),
    )
    .await;
    assert!(
        ask["result"].is_object(),
        "first ask should reach peer and succeed: {ask}"
    );
    std::fs::write(&first_release, "release").unwrap();
    wait_for(&retry_started).await;
    let retry_registration = latest_registration(&audit);

    let replay = call(
        &retry_registration,
        2,
        "ask_agent",
        json!({"recipient":"peer","text":"replay"}),
    )
    .await;
    assert_eq!(
        replay["error"]["code"], -32001,
        "same human root must not regain its ask budget: {replay}"
    );
    let rows: Vec<Value> = std::fs::read_to_string(&audit)
        .unwrap()
        .lines()
        .filter_map(|line| serde_json::from_str(line).ok())
        .collect();
    let prompt_count = rows
        .iter()
        .filter(|row| row["method"] == "session/prompt")
        .count();
    assert_eq!(
        prompt_count, 4,
        "only warm, first root, first peer ask, and retried root prompts should occur; replay must not prompt peer"
    );
    std::fs::write(&retry_release, "release").unwrap();
    first.await.unwrap().unwrap();
    bridge.shutdown().await.unwrap();
}
