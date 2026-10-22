use async_trait::async_trait;
use serde_json::{Value, json};
use soulsteads_messaging::{
    config,
    delivery::{AgentId, LocalClient, LocalRefusal, PeerSend},
    runtime::{Bridge, Event, Sink},
};
use std::{collections::BTreeMap, path::Path, sync::Mutex};

struct LocalRecorder {
    local: Mutex<Vec<LocalClient>>,
}

#[async_trait]
impl Sink for LocalRecorder {
    async fn emit(&self, _event: Value) -> anyhow::Result<()> {
        Ok(())
    }

    fn local_client(&self, client: LocalClient) {
        self.local.lock().unwrap().push(client);
    }
}

fn configuration(root: &Path) -> Value {
    config::load(
        &json!({
            "rateLimitSeconds": 0,
            "sessionStorePath": root.join("store.json"),
            "acp": {
                "command": env!("CARGO_BIN_EXE_acp-fixture"),
                "args": [],
                "env": {
                    "ACP_FIXTURE_MATRIX_SCRIPT": root.join("script.json"),
                    "ACP_FIXTURE_MATRIX_COUNTER": root.join("counter"),
                    "ACP_FIXTURE_RPC_AUDIT": root.join("rpc.jsonl")
                }
            },
            "agentMessaging": {
                "enabled": true,
                "agents": [
                    {"id":"alpha", "name":"Alpha", "target":{"platform":"web", "context":"alpha-room", "backend":"opencode"}, "allowedSenders":[]},
                    {"id":"beta", "label":"Beta", "target":{"platform":"web", "context":"beta-room", "backend":"opencode"}, "allowedSenders":["alpha"]}
                ]
            }
        })
        .to_string(),
        &BTreeMap::new(),
    )
    .unwrap()
}

fn rpc_methods(root: &Path) -> Vec<String> {
    std::fs::read_to_string(root.join("rpc.jsonl"))
        .unwrap_or_default()
        .lines()
        .map(|line| {
            serde_json::from_str::<Value>(line).unwrap()["method"]
                .as_str()
                .unwrap()
                .to_owned()
        })
        .collect()
}

#[tokio::test]
async fn web_backend_control_does_not_open_or_issue_until_a_real_prompt() {
    let root = tempfile::tempdir().unwrap();
    std::fs::write(
        root.path().join("script.json"),
        json!([{"text":"human answer","stopReason":"end_turn"}]).to_string(),
    )
    .unwrap();
    let bridge = Bridge::new(
        configuration(root.path()),
        BTreeMap::new(),
        root.path().join("sessions"),
    )
    .unwrap();
    let sink = LocalRecorder {
        local: Mutex::new(Vec::new()),
    };

    bridge
        .handle(Event::web("alpha-room", "/backend opencode"), &sink)
        .await
        .unwrap();
    assert!(sink.local.lock().unwrap().is_empty());
    assert!(
        !rpc_methods(root.path())
            .iter()
            .any(|method| method == "session/prompt"),
        "control must not prompt the ACP session"
    );

    bridge
        .handle_web(Event::web("alpha-room", "human prompt"), &sink)
        .await
        .unwrap();
    assert_eq!(sink.local.lock().unwrap().len(), 1);
    assert_eq!(
        rpc_methods(root.path())
            .into_iter()
            .filter(|method| matches!(method.as_str(), "session/new" | "session/prompt"))
            .collect::<Vec<_>>(),
        vec!["session/new", "session/prompt"]
    );
    bridge.shutdown().await.unwrap();
}

#[tokio::test]
async fn clear_revokes_an_old_context_before_reissuing_the_same_fixture_session_id() {
    let root = tempfile::tempdir().unwrap();
    std::fs::write(
        root.path().join("script.json"),
        json!([
            {"text":"recipient answer","stopReason":"end_turn"},
            {"text":"first answer","stopReason":"end_turn"},
            {"text":"second answer","stopReason":"end_turn"}
        ])
        .to_string(),
    )
    .unwrap();
    let bridge = Bridge::new(
        configuration(root.path()),
        BTreeMap::new(),
        root.path().join("sessions"),
    )
    .unwrap();
    let sink = LocalRecorder {
        local: Mutex::new(Vec::new()),
    };

    bridge
        .handle(Event::web("beta-room", "recipient prompt"), &sink)
        .await
        .unwrap();
    bridge
        .handle(Event::web("alpha-room", "first prompt"), &sink)
        .await
        .unwrap();
    let old = sink.local.lock().unwrap()[1].clone();
    bridge
        .handle(Event::web("alpha-room", "/clear"), &sink)
        .await
        .unwrap();
    bridge
        .handle(Event::web("alpha-room", "second prompt"), &sink)
        .await
        .unwrap();
    let current = sink.local.lock().unwrap()[2].clone();
    let send = PeerSend::new(AgentId::try_from("beta").unwrap(), "old-context", "data").unwrap();

    assert_eq!(old.send(send).await, Err(LocalRefusal::Denied));
    assert_eq!(current.list_visible().await.unwrap().len(), 1);
    bridge.shutdown().await.unwrap();
}

#[tokio::test]
async fn disabled_agent_messaging_never_issues_a_local_client() {
    let root = tempfile::tempdir().unwrap();
    std::fs::write(
        root.path().join("script.json"),
        json!([{"text":"human answer","stopReason":"end_turn"}]).to_string(),
    )
    .unwrap();
    let mut configuration = configuration(root.path());
    configuration["agentMessaging"]["enabled"] = false.into();
    let bridge = Bridge::new(configuration, BTreeMap::new(), root.path().join("sessions")).unwrap();
    let sink = LocalRecorder {
        local: Mutex::new(Vec::new()),
    };

    bridge
        .handle_web(Event::web("alpha-room", "human prompt"), &sink)
        .await
        .unwrap();

    assert!(sink.local.lock().unwrap().is_empty());
    bridge.shutdown().await.unwrap();
}

#[tokio::test]
async fn admitted_owner_receives_a_bound_local_client_and_send_is_not_durable_yet() {
    let root = tempfile::tempdir().unwrap();
    std::fs::write(
        root.path().join("script.json"),
        json!([
            {"text":"recipient answer","stopReason":"end_turn"},
            {"text":"owner answer","stopReason":"end_turn"}
        ])
        .to_string(),
    )
    .unwrap();
    let bridge = Bridge::new(
        configuration(root.path()),
        BTreeMap::new(),
        root.path().join("sessions"),
    )
    .unwrap();
    let sink = LocalRecorder {
        local: Mutex::new(Vec::new()),
    };

    bridge
        .handle_web(Event::web("beta-room", "human prompt"), &sink)
        .await
        .unwrap();
    sink.local
        .lock()
        .unwrap()
        .pop()
        .expect("recipient owner hook");
    bridge
        .handle_web(Event::web("alpha-room", "human prompt"), &sink)
        .await
        .unwrap();
    let client = sink.local.lock().unwrap().pop().expect("owner hook");
    let send = PeerSend::new(
        AgentId::try_from("beta").unwrap(),
        "p3-request",
        "/reset data",
    )
    .unwrap();

    assert_eq!(
        client.send(send).await,
        Err(LocalRefusal::DurableUnavailable)
    );
    bridge.shutdown().await.unwrap();
}

#[tokio::test]
async fn retry_revokes_the_old_context_and_issues_a_fresh_owner_context() {
    let root = tempfile::tempdir().unwrap();
    std::fs::write(
        root.path().join("script.json"),
        json!([
            {"text":"","stopReason":"end_turn"},
            {"text":"recovered","stopReason":"end_turn"}
        ])
        .to_string(),
    )
    .unwrap();
    let bridge = Bridge::new(
        configuration(root.path()),
        BTreeMap::new(),
        root.path().join("sessions"),
    )
    .unwrap();
    let sink = LocalRecorder {
        local: Mutex::new(Vec::new()),
    };

    bridge
        .handle_web(Event::web("alpha-room", "human prompt"), &sink)
        .await
        .unwrap();

    assert_eq!(
        sink.local.lock().unwrap().len(),
        2,
        "one context per binding generation"
    );
    bridge.shutdown().await.unwrap();
}

#[tokio::test]
async fn shutdown_revokes_a_retained_local_client_before_legacy_store_bytes_can_authorize_it() {
    let root = tempfile::tempdir().unwrap();
    std::fs::write(
        root.path().join("script.json"),
        json!([
            {"text":"recipient answer","stopReason":"end_turn"},
            {"text":"owner answer","stopReason":"end_turn"}
        ])
        .to_string(),
    )
    .unwrap();
    let bridge = Bridge::new(
        configuration(root.path()),
        BTreeMap::new(),
        root.path().join("sessions"),
    )
    .unwrap();
    let sink = LocalRecorder {
        local: Mutex::new(Vec::new()),
    };
    bridge
        .handle_web(Event::web("beta-room", "recipient prompt"), &sink)
        .await
        .unwrap();
    sink.local.lock().unwrap().pop().unwrap();
    bridge
        .handle_web(Event::web("alpha-room", "owner prompt"), &sink)
        .await
        .unwrap();
    let client = sink.local.lock().unwrap().pop().unwrap();
    let send = PeerSend::new(AgentId::try_from("beta").unwrap(), "shutdown", "data").unwrap();

    bridge.shutdown().await.unwrap();
    assert_eq!(client.send(send).await, Err(LocalRefusal::Denied));
}
