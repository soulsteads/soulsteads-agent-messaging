use soulsteads_messaging::acp::{Client, Options};
use serde_json::Value;
use std::{collections::BTreeMap, time::Duration};
#[path = "support/acp_peer.rs"]
mod acp_peer;
#[tokio::test]
async fn opencode_adapter_preserves_native_acp_and_installed_path_override() {
    let root = tempfile::tempdir().unwrap();
    let audit = root.path().join("rpc.jsonl");
    let client = Client::connect(Options {
        command: env!("CARGO_BIN_EXE_opencode-acp-rust").into(),
        args: vec![],
        cwd: root.path().into(),
        env: BTreeMap::from([
            (
                "OPENCODE_PATH".into(),
                env!("CARGO_BIN_EXE_acp-fixture").into(),
            ),
            (
                "ACP_FIXTURE_RPC_AUDIT".into(),
                audit.to_string_lossy().into_owned(),
            ),
        ]),
        inherit_env: false,
        timeout: Duration::from_secs(10),
    })
    .await
    .unwrap();
    client.new_session().await.unwrap();
    assert!(
        !client
            .prompt("hello", vec![])
            .await
            .unwrap()
            .text
            .is_empty()
    );
    client.shutdown().await.unwrap();
    let messages = std::fs::read_to_string(audit)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str::<Value>(line).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(
        messages
            .iter()
            .filter(|message| message["method"] == "initialize")
            .count(),
        1
    );
    assert_eq!(
        messages
            .iter()
            .filter(|message| message["method"] == "session/prompt")
            .count(),
        1
    );
}

#[tokio::test]
async fn opencode_proxy_adapts_session_transports_and_scopes_model_override() {
    use agent_client_protocol::UntypedMessage;
    use serde_json::json;
    let root = tempfile::tempdir().unwrap();
    let audit = root.path().join("rpc.jsonl");
    let mut peer = acp_peer::Peer::spawn(
        env!("CARGO_BIN_EXE_opencode-acp-rust"),
        root.path(),
        BTreeMap::from([
            (
                "OPENCODE_PATH".into(),
                env!("CARGO_BIN_EXE_acp-fixture").into(),
            ),
            (
                "ACP_FIXTURE_RPC_AUDIT".into(),
                audit.to_string_lossy().into_owned(),
            ),
            ("OPENCODE_ADAPTER_MODEL".into(), "provider/model".into()),
        ]),
    )
    .await
    .unwrap();
    let request = |method: &str, params: Value| {
        peer.connection
            .send_request(UntypedMessage::new(method, params).unwrap())
            .block_task()
    };
    let initialized = request(
        "initialize",
        json!({"protocolVersion":1,"clientCapabilities":{}}),
    )
    .await
    .unwrap();
    assert_eq!(initialized["agentInfo"]["name"], "offline-fixture");
    assert_eq!(
        initialized["agentCapabilities"]["mcpCapabilities"]["sse"],
        true
    );
    let direct = json!({"name":"stdio","command":"tool","args":["literal"],"env":[]});
    let http =
        json!({"type":"http","name":"http","url":"https://example.invalid/mcp","headers":[]});
    let sse = json!({"type":"sse","name":"legacy","url":"http://127.0.0.1:1/sse","headers":[{"name":"Authorization","value":"owned secret fixture"}]});
    for method in ["session/new", "session/load", "session/resume"] {
        let info = request(
            method,
            json!({"sessionId":"fixture","cwd":root.path(),"mcpServers":[direct,http,sse]}),
        )
        .await
        .unwrap();
        assert!(info["configOptions"].as_array().unwrap().iter().any(|option|option["id"]=="model" && option["currentValue"]=="provider/model"));
    }
    request("fixture/unknown_extension", json!({"kept":true}))
        .await
        .unwrap();
    let error = request("fixture/error", json!({})).await.unwrap_err();
    let error = serde_json::to_value(error).unwrap();
    assert_eq!(error["code"], -32042);
    assert_eq!(error["data"]["retained"], true);
    assert!(
        peer.updates.try_recv().is_ok(),
        "Native notifications must pass through"
    );
    assert!(peer.permissions.is_empty());
    peer.shutdown().await.unwrap();
    let entries = std::fs::read_to_string(audit)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str::<Value>(line).unwrap())
        .collect::<Vec<_>>();
    let setups = entries
        .iter()
        .filter(|entry| {
            matches!(
                entry["method"].as_str(),
                Some("session/new" | "session/load" | "session/resume")
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(setups.len(), 3);
    for setup in setups {
        assert_eq!(setup["params"]["mcpServers"][0], direct);
        assert_eq!(setup["params"]["mcpServers"][1], http);
        let proxy = &setup["params"]["mcpServers"][2];
        assert_eq!(proxy["args"], json!(["--mcp-sse-proxy"]));
        assert!(!proxy["args"].to_string().contains("owned secret fixture"));
        let config: Value =
            serde_json::from_str(proxy["env"][0]["value"].as_str().unwrap()).unwrap();
        assert_eq!(config["headers"], sse["headers"]);
    }
    assert_eq!(
        entries
            .iter()
            .filter(|entry| entry["method"] == "session/set_config_option")
            .count(),
        3
    );
}
#[tokio::test]
async fn builtin_opencode_runs_from_bridge_binary() {
    let root = tempfile::tempdir().unwrap();
    let config = root.path().join("bridge.json");
    std::fs::write(&config,serde_json::json!({"acp":{"command":"builtin:opencode","args":[],"env":{"OPENCODE_PATH":env!("CARGO_BIN_EXE_acp-fixture")}}}).to_string()).unwrap();
    let output = tokio::process::Command::new(env!("CARGO_BIN_EXE_soulsteads-messaging"))
        .current_dir(root.path())
        .args(["--config", config.to_str().unwrap(), "--no-images", "hello"])
        .output()
        .await
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(!output.stdout.is_empty());
}

#[tokio::test]
async fn opencode_invalid_model_cleans_new_sessions_and_retains_loaded_history() {
    use agent_client_protocol::UntypedMessage;
    use serde_json::json;
    let root = tempfile::tempdir().unwrap();
    let audit = root.path().join("rpc.jsonl");
    let peer = acp_peer::Peer::spawn(
        env!("CARGO_BIN_EXE_opencode-acp-rust"),
        root.path(),
        BTreeMap::from([
            (
                "OPENCODE_PATH".into(),
                env!("CARGO_BIN_EXE_acp-fixture").into(),
            ),
            (
                "ACP_FIXTURE_RPC_AUDIT".into(),
                audit.to_string_lossy().into_owned(),
            ),
            ("OPENCODE_ADAPTER_MODEL".into(), "provider/absent".into()),
            ("ACP_FIXTURE_REJECT_MODEL".into(), "provider/absent".into()),
        ]),
    )
    .await
    .unwrap();
    let request = |method: &str, params: Value| {
        peer.connection
            .send_request(UntypedMessage::new(method, params).unwrap())
            .block_task()
    };
    request(
        "initialize",
        json!({"protocolVersion":1,"clientCapabilities":{}}),
    )
    .await
    .unwrap();
    for method in ["session/new", "session/load"] {
        let error = request(
            method,
            json!({"sessionId":"fixture","cwd":root.path(),"mcpServers":[]}),
        )
        .await
        .unwrap_err();
        assert_eq!(serde_json::to_value(error).unwrap()["code"], -32602);
    }
    peer.shutdown().await.unwrap();
    let entries = std::fs::read_to_string(audit)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str::<Value>(line).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(
        entries
            .iter()
            .filter(|entry| entry["method"] == "session/delete")
            .count(),
        1
    );
    let deletion = entries
        .iter()
        .position(|entry| entry["method"] == "session/delete")
        .unwrap();
    let load = entries
        .iter()
        .position(|entry| entry["method"] == "session/load")
        .unwrap();
    assert!(deletion < load);
}
#[tokio::test]
#[ignore = "uses installed OpenCode credentials; creates and deletes an owned session without coding"]
async fn installed_opencode_adapter_initializes_and_creates_session() {
    let root = tempfile::tempdir().unwrap();
    let client = Client::connect(Options {
        command: env!("CARGO_BIN_EXE_opencode-acp-rust").into(),
        args: vec![],
        cwd: root.path().into(),
        env: BTreeMap::new(),
        inherit_env: true,
        timeout: Duration::from_secs(60),
    })
    .await
    .unwrap();
    client.new_session().await.unwrap();
    assert!(!client.config_options().await.is_empty());
    client.delete_session().await.unwrap();
    client.shutdown().await.unwrap();
}
