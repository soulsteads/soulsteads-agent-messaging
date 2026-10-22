#![cfg(feature = "agent-network")]

use async_trait::async_trait;
use serde_json::{Value, json};
use soulsteads_messaging::{
    config,
    delivery::{AgentId, DirectRequest, LocalClient},
    runtime::{Bridge, Event, Sink},
};
use std::{collections::BTreeMap, path::Path, sync::Mutex};

#[derive(Default)]
struct Recorder {
    clients: Mutex<Vec<LocalClient>>,
    events: Mutex<Vec<Value>>,
}

#[async_trait]
impl Sink for Recorder {
    async fn emit(&self, event: Value) -> anyhow::Result<()> {
        self.events.lock().unwrap().push(event);
        Ok(())
    }

    fn local_client(&self, client: LocalClient) {
        self.clients.lock().unwrap().push(client);
    }
}

fn configuration(root: &Path, backend: &str) -> Value {
    let fixture = env!("CARGO_BIN_EXE_acp-fixture");
    let audit = root.join("native.jsonl");
    let (command, environment) = match backend {
        "claude" => (
            env!("CARGO_BIN_EXE_claude-acp-rust"),
            json!({
                "CLAUDE_CODE_EXECUTABLE": fixture, "ACP_CLAUDE_FIXTURE": "1",
                "CLAUDE_CONFIG_DIR": root.join("claude"), "CLAUDE_FIXTURE_AUDIT": audit,
            }),
        ),
        "codex" => (
            env!("CARGO_BIN_EXE_codex-acp-rust"),
            json!({
                "CODEX_PATH": fixture, "ACP_CODEX_FIXTURE": "1",
                "CODEX_HOME": root.join("codex"), "CODEX_FIXTURE_AUDIT": audit,
            }),
        ),
        "pi" => (
            env!("CARGO_BIN_EXE_pi-acp-rust"),
            json!({
                "PI_PATH": fixture, "ACP_PI_FIXTURE": "1",
                "PI_CODING_AGENT_DIR": root.join("pi"), "PI_FIXTURE_AUDIT": audit,
            }),
        ),
        "opencode" => (
            env!("CARGO_BIN_EXE_opencode-acp-rust"),
            json!({
                "OPENCODE_PATH": fixture, "ACP_FIXTURE_RPC_AUDIT": audit,
            }),
        ),
        _ => unreachable!(),
    };
    let specification = json!({"command": command, "args": [], "env": environment});
    let mut settings = json!({
        "rateLimitSeconds": 0,
        "sessionStorePath": root.join("store.json"),
        "acp": {"defaultBackend": backend, "timeoutMs": 5000, "backends": {backend: specification}},
        "agentMessaging": {"enabled": true, "agents": [
            {"id":"alpha", "name":"Alpha", "target":{"platform":"web", "context":"alpha", "backend":backend}, "allowedSenders":[]},
            {"id":"beta", "name":"Beta", "target":{"platform":"web", "context":"beta", "backend":backend}, "allowedSenders":["alpha"]}
        ]}
    });
    if backend == "opencode" {
        settings["acp"]
            .as_object_mut()
            .unwrap()
            .extend(specification.as_object().unwrap().clone());
    }
    config::load(&settings.to_string(), &BTreeMap::new()).unwrap()
}

fn admitted_text<'a>(backend: &str, entry: &'a Value) -> Option<&'a str> {
    match backend {
        "claude" if entry["type"] == "user" => entry["message"]["content"][0]["text"].as_str(),
        "codex" if entry["method"] == "turn/start" => entry["params"]["input"][0]["text"].as_str(),
        "pi" if entry["type"] == "prompt" => entry["message"].as_str(),
        "opencode" if entry["method"] == "session/prompt" => {
            entry["params"]["prompt"][0]["text"].as_str()
        }
        _ => None,
    }
}

#[tokio::test]
async fn owner_bound_direct_requests_use_each_compiled_native_adapter_without_human_commands() {
    for backend in ["claude", "codex", "pi", "opencode"] {
        let root = tempfile::tempdir().unwrap();
        let bridge = std::sync::Arc::new(
            Bridge::new(
                configuration(root.path(), backend),
                BTreeMap::new(),
                root.path().join("sessions"),
            )
            .unwrap(),
        );
        let sink = Recorder::default();
        bridge
            .handle_web(Event::web("beta", "warm beta"), &sink)
            .await
            .unwrap();
        bridge
            .handle_web(Event::web("alpha", "warm alpha"), &sink)
            .await
            .unwrap();
        let caller = sink.clients.lock().unwrap().last().unwrap().clone();
        let before = sink.events.lock().unwrap().len();
        let result = bridge
            .direct_request(
                caller,
                DirectRequest::new(AgentId::try_from("beta").unwrap(), "/clear literal").unwrap(),
            )
            .await;
        // Always settle the owned backends before inspecting assertions.
        let shutdown = bridge.shutdown().await;
        let result = result.unwrap_or_else(|error| panic!("{backend}: {error:?}"));
        shutdown.unwrap();
        assert_eq!(
            result.text(),
            if backend == "opencode" {
                "hello world"
            } else {
                "native reply"
            },
            "{backend}"
        );
        assert_eq!(result.stop_reason(), Some("end_turn"), "{backend}");
        assert_eq!(
            sink.events.lock().unwrap().len(),
            before,
            "{backend}: direct work must not publish to the human sink"
        );
        let entries: Vec<Value> = std::fs::read_to_string(root.path().join("native.jsonl"))
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        let texts: Vec<_> = entries
            .iter()
            .filter_map(|entry| admitted_text(backend, entry))
            .collect();
        assert_eq!(
            texts,
            ["warm beta", "warm alpha", "/clear literal"],
            "{backend}: exactly one native submission for the direct literal, no command interpretation"
        );
        assert!(
            !root
                .path()
                .join("store.json.peer-delivery.v1.json")
                .exists()
        );
    }
}
