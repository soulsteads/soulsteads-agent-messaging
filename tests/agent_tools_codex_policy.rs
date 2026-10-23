#![cfg(feature = "agent-network")]

use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use soulsteads_messaging::acp::{Client, Options};
use std::{collections::BTreeMap, time::Duration};

fn fingerprint(server: &Value) -> String {
    let canonical = json!([
        server["name"],
        server["type"],
        server["url"],
        server["headers"]
    ]);
    format!("{:x}", Sha256::digest(canonical.to_string().as_bytes()))
}

#[tokio::test]
async fn native_codex_preapproves_only_the_runtime_granted_local_tools() {
    let directory = tempfile::tempdir().unwrap();
    let local = json!({"name":"soulsteads_agents","type":"http","url":"http://127.0.0.1:43219/mcp","headers":[{"name":"Authorization","value":"Bearer opaque"}]});
    let ordinary =
        json!({"name":"ordinary","type":"http","url":"http://127.0.0.1:43220/mcp","headers":[]});
    let mut env = BTreeMap::from([
        (
            "CODEX_PATH".into(),
            env!("CARGO_BIN_EXE_acp-fixture").into(),
        ),
        ("ACP_CODEX_FIXTURE".into(), "1".into()),
        (
            "CODEX_FIXTURE_AUDIT".into(),
            directory.path().join("native.jsonl").display().to_string(),
        ),
        (
            "CODEX_FIXTURE_PID".into(),
            directory.path().join("native.pid").display().to_string(),
        ),
        (
            "SOULSTEADS_LOCAL_AGENT_TOOLS_GRANT".into(),
            fingerprint(&local),
        ),
    ]);
    let client = Client::connect_with_servers(
        Options {
            command: env!("CARGO_BIN_EXE_codex-acp-rust").into(),
            args: vec![],
            cwd: directory.path().canonicalize().unwrap(),
            env: std::mem::take(&mut env),
            inherit_env: false,
            timeout: Duration::from_secs(3),
        },
        vec![ordinary, local],
    )
    .await
    .unwrap();
    client.new_session().await.unwrap();
    let audit: Vec<Value> = std::fs::read_to_string(directory.path().join("native.jsonl"))
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    let start = audit
        .iter()
        .find(|row| row["method"] == "thread/start")
        .unwrap();
    let config = &start["params"]["config"]["mcp_servers"];
    assert_eq!(
        config["soulsteads_agents"]["enabled_tools"],
        json!(["list_agents", "ask_agent"])
    );
    assert_eq!(
        config["soulsteads_agents"]["tools"]["list_agents"]["approval_mode"],
        "approve"
    );
    assert_eq!(
        config["soulsteads_agents"]["tools"]["ask_agent"]["approval_mode"],
        "approve"
    );
    assert_eq!(
        config["soulsteads_agents"]["default_tools_approval_mode"],
        "prompt"
    );
    assert_eq!(config["soulsteads_agents"]["tool_timeout_sec"], 310);
    assert!(config["ordinary"]["enabled_tools"].is_null());
    assert!(config["ordinary"]["default_tools_approval_mode"].is_null());
    client.shutdown().await.unwrap();
}

#[tokio::test]
async fn native_codex_does_not_grant_local_tools_for_a_mismatched_fingerprint() {
    let directory = tempfile::tempdir().unwrap();
    let local = json!({"name":"soulsteads_agents","type":"http","url":"http://127.0.0.1:43219/mcp","headers":[{"name":"Authorization","value":"Bearer opaque"}]});
    let mut env = BTreeMap::from([
        (
            "CODEX_PATH".into(),
            env!("CARGO_BIN_EXE_acp-fixture").into(),
        ),
        ("ACP_CODEX_FIXTURE".into(), "1".into()),
        (
            "CODEX_FIXTURE_AUDIT".into(),
            directory.path().join("native.jsonl").display().to_string(),
        ),
        (
            "CODEX_FIXTURE_PID".into(),
            directory.path().join("native.pid").display().to_string(),
        ),
        (
            "SOULSTEADS_LOCAL_AGENT_TOOLS_GRANT".into(),
            "wrong-grant".into(),
        ),
    ]);
    let client = Client::connect_with_servers(
        Options {
            command: env!("CARGO_BIN_EXE_codex-acp-rust").into(),
            args: vec![],
            cwd: directory.path().canonicalize().unwrap(),
            env: std::mem::take(&mut env),
            inherit_env: false,
            timeout: Duration::from_secs(3),
        },
        vec![local],
    )
    .await
    .unwrap();
    client.new_session().await.unwrap();
    let audit: Vec<Value> = std::fs::read_to_string(directory.path().join("native.jsonl"))
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    let start = audit
        .iter()
        .find(|row| row["method"] == "thread/start")
        .unwrap();
    assert!(
        start["params"]["config"]["mcp_servers"]["soulsteads_agents"]["enabled_tools"].is_null()
    );
    client.shutdown().await.unwrap();
}
