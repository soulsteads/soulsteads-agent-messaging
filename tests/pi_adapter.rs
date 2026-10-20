use soulsteads_messaging::acp::{Client, Options};
use serde_json::{Value, json};
use std::{collections::BTreeMap, path::Path, time::Duration};
#[path = "support/acp_peer.rs"]
mod acp_peer;
fn environment(root: &Path) -> BTreeMap<String, String> {
    BTreeMap::from([
        ("PI_PATH".into(), env!("CARGO_BIN_EXE_acp-fixture").into()),
        ("ACP_PI_FIXTURE".into(), "1".into()),
        (
            "PI_CODING_AGENT_DIR".into(),
            root.join("agent").to_string_lossy().into_owned(),
        ),
        (
            "PI_FIXTURE_AUDIT".into(),
            root.join("native.jsonl").to_string_lossy().into_owned(),
        ),
    ])
}
fn options(root: &Path) -> Options {
    Options {
        command: env!("CARGO_BIN_EXE_pi-acp-rust").into(),
        args: vec![],
        cwd: root.into(),
        env: environment(root),
        inherit_env: false,
        timeout: Duration::from_secs(10),
    }
}
fn audit(root: &Path) -> Vec<Value> {
    std::fs::read_to_string(root.join("native.jsonl"))
        .unwrap_or_default()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect()
}
async fn wait_command(root: &Path, kind: &str) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while !audit(root).iter().any(|value| value["type"] == kind) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
}
async fn peer(root: &Path) -> acp_peer::Peer {
    use agent_client_protocol::schema::{ProtocolVersion, v1};
    let peer = acp_peer::Peer::spawn(env!("CARGO_BIN_EXE_pi-acp-rust"), root, environment(root))
        .await
        .unwrap();
    let response = peer
        .connection
        .send_request(v1::InitializeRequest::new(ProtocolVersion::V1))
        .block_task()
        .await
        .unwrap();
    assert!(response.auth_methods.is_empty());
    peer
}
#[tokio::test]
async fn pi_streams_without_duplication_and_restores_native_history() {
    let root = tempfile::tempdir().unwrap();
    let client = Client::connect(options(root.path())).await.unwrap();
    let id = client.new_session().await.unwrap();
    uuid::Uuid::parse_str(&id).unwrap();
    assert_eq!(
        client.prompt("hello", vec![]).await.unwrap().text,
        "native reply"
    );
    assert!(
        client
            .list_sessions()
            .await
            .unwrap()
            .iter()
            .any(|entry| entry["sessionId"] == id)
    );
    assert!(
        client
            .commands()
            .await
            .iter()
            .any(|command| command["name"] == "fixture-extension")
    );
    assert!(
        !client
            .commands()
            .await
            .iter()
            .any(|command| command["name"] == "login")
    );
    client.close_session().await.unwrap();
    client.shutdown().await.unwrap();
    let client = Client::connect(options(root.path())).await.unwrap();
    let history = client.load_session(&id, false).await.unwrap();
    assert!(
        history
            .iter()
            .any(|entry| entry["content"]["text"] == "native reply")
    );
    assert_eq!(
        client.prompt("next", vec![]).await.unwrap().text,
        "native reply"
    );
    client.delete_session().await.unwrap();
    assert!(
        !client
            .list_sessions()
            .await
            .unwrap()
            .iter()
            .any(|entry| entry["sessionId"] == id)
    );
    client.shutdown().await.unwrap();
    assert!(
        audit(root.path())
            .iter()
            .filter(|entry| entry["type"] == "spawn")
            .any(|entry| entry["args"]
                .as_array()
                .unwrap()
                .iter()
                .any(|arg| arg == "--session"))
    );
}
#[tokio::test]
async fn pi_thinking_models_idle_reload_and_modes_use_native_controls() {
    let root = tempfile::tempdir().unwrap();
    let client = Client::connect(options(root.path())).await.unwrap();
    let id = client.new_session().await.unwrap();
    client.set_option("thought_level", "high").await.unwrap();
    client.prompt("hello", vec![]).await.unwrap();
    client.load_session(&id, false).await.unwrap();
    assert!(
        client
            .config_options()
            .await
            .iter()
            .any(|option| option["id"] == "thought_level" && option["currentValue"] == "high")
    );
    assert_eq!(
        audit(root.path())
            .iter()
            .filter(|entry| entry["type"] == "spawn")
            .count(),
        1
    );
    client.set_option("model", "fixture/plain").await.unwrap();
    assert_eq!(client.modes().await["currentModeId"], "off");
    assert_eq!(
        client.modes().await["availableModes"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    assert!(client.set_option("thought_level", "high").await.is_err());
    assert!(client.set_option("model", "fixture/absent").await.is_err());
    client.shutdown().await.unwrap();
}
#[tokio::test]
async fn pi_tool_media_large_and_unknown_tools_preserve_activity() {
    let root = tempfile::tempdir().unwrap();
    let client = Client::connect(options(root.path())).await.unwrap();
    client.new_session().await.unwrap();
    for prompt in ["tool", "media", "large", "unknown-tool"] {
        let reply = client.prompt(prompt, vec![]).await.unwrap();
        assert!(reply.output.had_tool_activity);
        assert!(!reply.output.output_lost);
        if prompt == "media" {
            assert_eq!(reply.output.images.len(), 1);
        }
        if prompt == "large" {
            assert!(reply.output.tool_results.contains("truncated"));
        }
        if prompt == "unknown-tool" {
            assert!(reply.text.is_empty());
        } else {
            assert_eq!(reply.text, "tool reply");
        }
    }
    client.shutdown().await.unwrap();
}
#[tokio::test]
async fn pi_waits_for_settlement_and_routes_followups_into_original_collector() {
    let root = tempfile::tempdir().unwrap();
    let client = Client::connect(options(root.path())).await.unwrap();
    client.new_session().await.unwrap();
    let mut prompt = tokio::spawn({
        let client = client.clone();
        async move { client.prompt("hold", vec![]).await }
    });
    wait_command(root.path(), "prompt").await;
    assert!(
        tokio::time::timeout(Duration::from_millis(80), &mut prompt)
            .await
            .is_err()
    );
    assert!(client.set_option("thought_level", "high").await.is_err());
    client.forward("followup").await.unwrap();
    let reply = prompt.await.unwrap().unwrap();
    assert_eq!(reply.text, "original replyfollowup reply");
    assert_eq!(reply.stop_reason.as_deref(), Some("end_turn"));
    assert!(
        audit(root.path())
            .iter()
            .any(|entry| entry["type"] == "prompt" && entry["streamingBehavior"] == "followUp")
    );
    client.shutdown().await.unwrap();
}
#[tokio::test]
async fn pi_settlement_before_admission_ack_does_not_lose_output() {
    let root = tempfile::tempdir().unwrap();
    let client = Client::connect(options(root.path())).await.unwrap();
    client.new_session().await.unwrap();
    assert_eq!(
        client
            .prompt("settled-before-ack", vec![])
            .await
            .unwrap()
            .text,
        "native reply"
    );
    assert_eq!(
        client
            .prompt("handled", vec![])
            .await
            .unwrap()
            .stop_reason
            .as_deref(),
        Some("end_turn")
    );
    client.shutdown().await.unwrap();
}
#[tokio::test]
async fn pi_native_failures_retry_steps_and_transport_loss_are_distinct() {
    let root = tempfile::tempdir().unwrap();
    let client = Client::connect(options(root.path())).await.unwrap();
    client.new_session().await.unwrap();
    assert!(
        client
            .prompt("fail", vec![])
            .await
            .unwrap_err()
            .to_string()
            .contains("Native generation failed")
    );
    assert!(
        client
            .prompt("fail-ack", vec![])
            .await
            .unwrap_err()
            .to_string()
            .contains("Native admission failed")
    );
    assert_eq!(
        client.prompt("retry", vec![]).await.unwrap().text,
        "retried reply"
    );
    assert!(client.prompt("bad-frame", vec![]).await.is_err());
    assert!(client.prompt("next", vec![]).await.is_err());
    client.shutdown().await.unwrap();
}
#[tokio::test]
async fn pi_cancellation_clears_queue_and_reaches_native_idle() {
    let root = tempfile::tempdir().unwrap();
    let client = Client::connect(options(root.path())).await.unwrap();
    client.new_session().await.unwrap();
    let prompt = tokio::spawn({
        let client = client.clone();
        async move { client.prompt("hold", vec![]).await }
    });
    wait_command(root.path(), "prompt").await;
    client.cancel().await.unwrap();
    assert_eq!(
        prompt.await.unwrap().unwrap().stop_reason.as_deref(),
        Some("cancelled")
    );
    let commands = audit(root.path());
    let clear = commands
        .iter()
        .position(|entry| entry["type"] == "clear_queue")
        .unwrap();
    let abort = commands
        .iter()
        .position(|entry| entry["type"] == "abort")
        .unwrap();
    assert!(clear < abort);
    assert_eq!(
        client.prompt("hello", vec![]).await.unwrap().text,
        "native reply"
    );
    client.shutdown().await.unwrap();
}
#[tokio::test]
async fn pi_manual_compaction_can_be_cancelled_while_native_request_is_pending() {
    let root = tempfile::tempdir().unwrap();
    let client = Client::connect(options(root.path())).await.unwrap();
    client.new_session().await.unwrap();
    let prompt = tokio::spawn({
        let client = client.clone();
        async move { client.prompt("/compact hold", vec![]).await }
    });
    wait_command(root.path(), "compact").await;
    client.cancel().await.unwrap();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(3), prompt)
            .await
            .unwrap()
            .unwrap()
            .unwrap()
            .stop_reason
            .as_deref(),
        Some("cancelled")
    );
    client.shutdown().await.unwrap();
}
#[tokio::test]
async fn pi_builtin_commands_update_native_state_and_export_to_workspace() {
    let root = tempfile::tempdir().unwrap();
    let client = Client::connect(options(root.path())).await.unwrap();
    client.new_session().await.unwrap();
    for command in [
        "/compact",
        "/name Pi fixture",
        "/session",
        "/autocompact off",
        "/steering all",
        "/follow-up one-at-a-time",
        "/export",
    ] {
        assert_eq!(
            client
                .prompt(command, vec![])
                .await
                .unwrap()
                .stop_reason
                .as_deref(),
            Some("end_turn")
        );
    }
    assert!(
        client
            .list_sessions()
            .await
            .unwrap()
            .iter()
            .any(|entry| entry["title"] == "Pi fixture")
    );
    assert!(std::fs::read_dir(root.path()).unwrap().any(|entry| {
        entry
            .unwrap()
            .path()
            .extension()
            .is_some_and(|extension| extension == "html")
    }));
    assert!(client.prompt("/login", vec![]).await.is_err());
    client.shutdown().await.unwrap();
}
#[tokio::test]
async fn pi_typed_sdk_confirm_and_select_dialogs_are_request_scoped() {
    use agent_client_protocol::schema::v1;
    let root = tempfile::tempdir().unwrap();
    let mut peer = peer(root.path()).await;
    let id = peer
        .connection
        .send_request(v1::NewSessionRequest::new(root.path().to_owned()))
        .block_task()
        .await
        .unwrap()
        .session_id;
    for (message, option) in [("confirm", "allow-once"), ("select", "choice-1")] {
        let prompt = tokio::spawn({
            let cx = peer.connection.clone();
            let id = id.clone();
            async move {
                cx.send_request(v1::PromptRequest::new(
                    id,
                    vec![v1::ContentBlock::Text(v1::TextContent::new(message))],
                ))
                .block_task()
                .await
            }
        });
        let (request, responder) =
            tokio::time::timeout(Duration::from_secs(3), peer.permissions.recv())
                .await
                .unwrap()
                .unwrap();
        assert_eq!(request.session_id, id);
        assert!(request.options.iter().all(|option| matches!(
            option.kind,
            v1::PermissionOptionKind::AllowOnce | v1::PermissionOptionKind::RejectOnce
        )));
        responder
            .respond(v1::RequestPermissionResponse::new(
                v1::RequestPermissionOutcome::Selected(v1::SelectedPermissionOutcome::new(option)),
            ))
            .unwrap();
        assert_eq!(
            prompt.await.unwrap().unwrap().stop_reason,
            v1::StopReason::EndTurn
        );
    }
    let mut events = vec![];
    while let Ok(event) = peer.updates.try_recv() {
        events.push(serde_json::to_value(event).unwrap());
    }
    assert!(
        events
            .iter()
            .any(|event| event["update"]["sessionUpdate"] == "usage_update")
    );
    let responses = audit(root.path());
    assert_eq!(
        responses
            .iter()
            .find(|entry| entry["type"] == "extension_ui_response"
                && entry.get("confirmed").is_some())
            .unwrap()["confirmed"],
        true
    );
    assert_eq!(
        responses
            .iter()
            .find(|entry| entry["type"] == "extension_ui_response" && entry.get("value").is_some())
            .unwrap()["value"],
        "Second"
    );
    peer.shutdown().await.unwrap();
}
#[tokio::test]
async fn pi_cancelled_dialog_never_accepts_a_late_permission() {
    use agent_client_protocol::schema::v1;
    let root = tempfile::tempdir().unwrap();
    let mut peer = peer(root.path()).await;
    let id = peer
        .connection
        .send_request(v1::NewSessionRequest::new(root.path().to_owned()))
        .block_task()
        .await
        .unwrap()
        .session_id;
    let prompt = tokio::spawn({
        let cx = peer.connection.clone();
        let id = id.clone();
        async move {
            cx.send_request(v1::PromptRequest::new(
                id,
                vec![v1::ContentBlock::Text(v1::TextContent::new("confirm"))],
            ))
            .block_task()
            .await
        }
    });
    let (_, responder) = tokio::time::timeout(Duration::from_secs(3), peer.permissions.recv())
        .await
        .unwrap()
        .unwrap();
    peer.connection
        .send_notification(v1::CancelNotification::new(id))
        .unwrap();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(3), prompt)
            .await
            .unwrap()
            .unwrap()
            .unwrap()
            .stop_reason,
        v1::StopReason::Cancelled
    );
    let _ = responder.respond(v1::RequestPermissionResponse::new(
        v1::RequestPermissionOutcome::Selected(v1::SelectedPermissionOutcome::new("allow-once")),
    ));
    wait_command(root.path(), "extension_ui_response").await;
    let responses = audit(root.path());
    assert_eq!(
        responses
            .iter()
            .find(|entry| entry["type"] == "extension_ui_response")
            .unwrap()["cancelled"],
        true
    );
    peer.shutdown().await.unwrap();
}
#[tokio::test]
async fn pi_expired_and_unsupported_input_dialogs_are_cancelled() {
    let root = tempfile::tempdir().unwrap();
    let client = Client::connect(options(root.path())).await.unwrap();
    client.new_session().await.unwrap();
    for message in ["expired", "input", "editor"] {
        assert_eq!(client.prompt(message, vec![]).await.unwrap().text, "denied");
    }
    assert!(
        audit(root.path())
            .iter()
            .filter(|entry| entry["type"] == "extension_ui_response")
            .all(|entry| entry["cancelled"] == true)
    );
    client.shutdown().await.unwrap();
}
#[tokio::test]
async fn pi_wrong_workspace_history_failure_and_duplicate_ids_are_not_accepted() {
    let root = tempfile::tempdir().unwrap();
    let client = Client::connect(options(root.path())).await.unwrap();
    let id = client.new_session().await.unwrap();
    client.prompt("hello", vec![]).await.unwrap();
    client.shutdown().await.unwrap();
    let path = Path::new(audit(root.path())[0]["path"].as_str().unwrap()).to_owned();
    let original = std::fs::read_to_string(&path).unwrap();
    let other = tempfile::tempdir().unwrap();
    let mut lines = original.lines();
    let mut header: Value = serde_json::from_str(lines.next().unwrap()).unwrap();
    header["cwd"] = json!(other.path());
    std::fs::write(
        &path,
        format!("{header}\n{}\n", lines.collect::<Vec<_>>().join("\n")),
    )
    .unwrap();
    let client = Client::connect(options(root.path())).await.unwrap();
    assert!(client.load_session(&id, false).await.is_err());
    assert!(client.load_session("unknown", false).await.is_err());
    std::fs::write(&path, &original).unwrap();
    let duplicate = path.with_file_name("duplicate.jsonl");
    std::fs::write(&duplicate, &original).unwrap();
    assert!(client.load_session(&id, false).await.is_err());
    std::fs::remove_file(duplicate).unwrap();
    client.new_session().await.unwrap();
    assert_eq!(
        client.prompt("hello", vec![]).await.unwrap().text,
        "native reply"
    );
    client.shutdown().await.unwrap();
}
#[tokio::test]
async fn pi_images_and_client_mcp_configuration_reach_native_runtime() {
    use agent_client_protocol::schema::v1;
    let root = tempfile::tempdir().unwrap();
    let peer = peer(root.path()).await;
    let id = peer
        .connection
        .send_request(
            v1::NewSessionRequest::new(root.path().to_owned()).mcp_servers(vec![
                v1::McpServer::Stdio(v1::McpServerStdio::new("client_tools", "tool")),
            ]),
        )
        .block_task()
        .await
        .unwrap()
        .session_id;
    let entries = audit(root.path());
    let args = entries[0]["args"].as_array().unwrap();
    let extension = Path::new(
        args.windows(2)
            .find(|args| args[0] == "--extension")
            .unwrap()[1]
            .as_str()
            .unwrap(),
    );
    let source = std::fs::read_to_string(extension).unwrap();
    assert!(source.contains("registerMcpServer"));
    assert!(source.contains("client_tools"));
    assert!(!root.path().join("agent/mcp.json").exists());
    peer.connection
        .send_request(v1::PromptRequest::new(
            id,
            vec![v1::ContentBlock::Image(v1::ImageContent::new(
                "AA==",
                "image/png",
            ))],
        ))
        .block_task()
        .await
        .unwrap();
    assert_eq!(
        audit(root.path())
            .iter()
            .find(|entry| entry["type"] == "prompt")
            .unwrap()["images"][0]["mimeType"],
        "image/png"
    );
    peer.shutdown().await.unwrap();
    assert!(
        !extension.exists(),
        "Owned MCP configuration must be removed after shutdown"
    );
}

#[tokio::test]
async fn pi_mcp_changes_reopen_only_idle_runtime_preserving_native_session_and_settings() {
    use agent_client_protocol::UntypedMessage;
    let root = tempfile::tempdir().unwrap();
    let peer = peer(root.path()).await;
    let request = |method: &str, params: Value| {
        peer.connection
            .send_request(UntypedMessage::new(method, params).unwrap())
            .block_task()
    };
    let servers = json!([{"name":"client","command":"tool","args":[],"env":[]}]);
    let created = request(
        "session/new",
        json!({"cwd":root.path(),"mcpServers":servers}),
    )
    .await
    .unwrap();
    let id = created["sessionId"].clone();
    request(
        "session/set_config_option",
        json!({"sessionId":id,"configId":"thought_level","value":"high"}),
    )
    .await
    .unwrap();
    request(
        "session/prompt",
        json!({"sessionId":id,"prompt":[{"type":"text","text":"hello"}]}),
    )
    .await
    .unwrap();
    request(
        "session/load",
        json!({"sessionId":id,"cwd":root.path(),"mcpServers":servers}),
    )
    .await
    .unwrap();
    assert_eq!(
        audit(root.path())
            .iter()
            .filter(|entry| entry["type"] == "spawn")
            .count(),
        1
    );
    assert!(request("session/load",json!({"sessionId":id,"cwd":root.path(),"mcpServers":[{"name":"bad/name","command":"tool"}]})).await.is_err());
    assert_eq!(
        audit(root.path())
            .iter()
            .filter(|entry| entry["type"] == "spawn")
            .count(),
        1
    );
    let changed = request(
        "session/load",
        json!({"sessionId":id,"cwd":root.path(),"mcpServers":[]}),
    )
    .await
    .unwrap();
    assert_eq!(changed["sessionId"], id);
    assert!(
        changed["configOptions"]
            .as_array()
            .unwrap()
            .iter()
            .any(|option| option["id"] == "thought_level" && option["currentValue"] == "high")
    );
    let entries = audit(root.path());
    let spawns = entries
        .iter()
        .filter(|entry| entry["type"] == "spawn")
        .collect::<Vec<_>>();
    assert_eq!(spawns.len(), 2);
    assert_eq!(spawns[0]["path"], spawns[1]["path"]);
    let extension = spawns[0]["args"]
        .as_array()
        .unwrap()
        .windows(2)
        .find(|pair| pair[0] == "--extension")
        .unwrap()[1]
        .as_str()
        .unwrap();
    assert!(!Path::new(extension).exists());
    assert!(
        !spawns[1]["args"]
            .as_array()
            .unwrap()
            .iter()
            .any(|arg| arg == "--extension")
    );
    request(
        "session/prompt",
        json!({"sessionId":id,"prompt":[{"type":"text","text":"next"}]}),
    )
    .await
    .unwrap();
    peer.shutdown().await.unwrap();
}

#[tokio::test]
async fn pi_opted_in_freeform_dialogs_return_text_and_cancel_late_answers() {
    use agent_client_protocol::{UntypedMessage, schema::v1};
    let root = tempfile::tempdir().unwrap();
    let mut peer = acp_peer::Peer::spawn(
        env!("CARGO_BIN_EXE_pi-acp-rust"),
        root.path(),
        environment(root.path()),
    )
    .await
    .unwrap();
    peer.connection.send_request(UntypedMessage::new("initialize",json!({"protocolVersion":1,"clientCapabilities":{},"_meta":{"pi/extensionUi":true}})).unwrap()).block_task().await.unwrap();
    let id = peer
        .connection
        .send_request(v1::NewSessionRequest::new(root.path().to_owned()))
        .block_task()
        .await
        .unwrap()
        .session_id;
    for kind in ["input", "editor"] {
        let pending = peer
            .connection
            .send_request(v1::PromptRequest::new(
                id.clone(),
                vec![v1::ContentBlock::Text(v1::TextContent::new(kind))],
            ))
            .block_task();
        let (dialog, responder) =
            tokio::time::timeout(Duration::from_secs(5), peer.extension_ui.recv())
                .await
                .unwrap()
                .unwrap();
        assert_eq!(dialog["method"], kind);
        assert_eq!(dialog["initialValue"], "Fixture initial text");
        responder
            .respond(json!({"value":"typed line\nsecond line"}))
            .unwrap();
        pending.await.unwrap();
        assert_eq!(
            audit(root.path())
                .iter()
                .rev()
                .find(|entry| entry["type"] == "extension_ui_response")
                .unwrap()["value"],
            "typed line\nsecond line"
        );
    }
    let pending = peer
        .connection
        .send_request(v1::PromptRequest::new(
            id.clone(),
            vec![v1::ContentBlock::Text(v1::TextContent::new("input"))],
        ))
        .block_task();
    let (_, responder) = tokio::time::timeout(Duration::from_secs(5), peer.extension_ui.recv())
        .await
        .unwrap()
        .unwrap();
    peer.connection
        .send_notification(v1::CancelNotification::new(id.clone()))
        .unwrap();
    assert_eq!(
        pending.await.unwrap().stop_reason,
        v1::StopReason::Cancelled
    );
    let _ = responder.respond(json!({"value":"late must not apply"}));
    peer.connection
        .send_request(v1::PromptRequest::new(
            id,
            vec![v1::ContentBlock::Text(v1::TextContent::new("hello"))],
        ))
        .block_task()
        .await
        .unwrap();
    assert!(
        !audit(root.path())
            .iter()
            .any(|entry| entry["type"] == "extension_ui_response"
                && entry["value"] == "late must not apply")
    );
    peer.shutdown().await.unwrap();
}
#[tokio::test]
async fn builtin_pi_runs_through_bridge_and_resumes_native_uuid() {
    let root = tempfile::tempdir().unwrap();
    let config = root.path().join("bridge.json");
    std::fs::write(
        &config,
        json!({"acp":{"defaultBackend":"pi","backends":{"pi":{"env":environment(root.path())}}}})
            .to_string(),
    )
    .unwrap();
    for _ in 0..2 {
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
        assert!(String::from_utf8_lossy(&output.stdout).contains("native reply"));
    }
    let entries = audit(root.path());
    let spawns = entries
        .iter()
        .filter(|entry| entry["type"] == "spawn")
        .collect::<Vec<_>>();
    assert_eq!(spawns.len(), 2);
    assert_eq!(spawns[0]["sessionId"], spawns[1]["sessionId"]);
    assert!(
        spawns[1]["args"]
            .as_array()
            .unwrap()
            .iter()
            .any(|arg| arg == "--session")
    );
}
#[cfg(unix)]
#[tokio::test]
async fn pi_child_exits_on_acp_eof() {
    let root = tempfile::tempdir().unwrap();
    let mut environment = environment(root.path());
    let pid = root.path().join("pid");
    environment.insert("PI_FIXTURE_PID".into(), pid.to_string_lossy().into_owned());
    let peer = acp_peer::Peer::spawn(env!("CARGO_BIN_EXE_pi-acp-rust"), root.path(), environment)
        .await
        .unwrap();
    use agent_client_protocol::schema::{ProtocolVersion, v1};
    peer.connection
        .send_request(v1::InitializeRequest::new(ProtocolVersion::V1))
        .block_task()
        .await
        .unwrap();
    peer.connection
        .send_request(v1::NewSessionRequest::new(root.path().to_owned()))
        .block_task()
        .await
        .unwrap();
    let pid = std::fs::read_to_string(pid)
        .unwrap()
        .parse::<libc::pid_t>()
        .unwrap();
    peer.shutdown().await.unwrap();
    assert_eq!(unsafe { libc::kill(pid, 0) }, -1);
    assert_eq!(
        std::io::Error::last_os_error().raw_os_error(),
        Some(libc::ESRCH)
    );
}
#[tokio::test]
async fn pi_custom_session_directory_is_respected_for_listing_and_resume() {
    let root = tempfile::tempdir().unwrap();
    let mut options = options(root.path());
    options.env.insert(
        "PI_CODING_AGENT_SESSION_DIR".into(),
        root.path()
            .join("custom/sessions")
            .to_string_lossy()
            .into_owned(),
    );
    let client = Client::connect(options.clone()).await.unwrap();
    let id = client.new_session().await.unwrap();
    client.prompt("hello", vec![]).await.unwrap();
    assert!(
        client
            .list_sessions()
            .await
            .unwrap()
            .iter()
            .any(|entry| entry["sessionId"] == id)
    );
    client.shutdown().await.unwrap();
    let client = Client::connect(options).await.unwrap();
    client.load_session(&id, true).await.unwrap();
    client.delete_session().await.unwrap();
    client.shutdown().await.unwrap();
}

#[path = "support/pi_provider.rs"]
mod pi_provider;
#[tokio::test]
#[ignore = "requires PI_ADAPTER_PROBE_PATH pointing to installed Pi; uses an owned local provider, no cloud credentials"]
async fn installed_pi_executes_native_tools_streams_and_restores_owned_session() {
    let command = std::env::var("PI_ADAPTER_PROBE_PATH")
        .expect("Set PI_ADAPTER_PROBE_PATH to a Pi executable");
    let root = tempfile::tempdir().unwrap();
    let agent = root.path().join("agent");
    let mut provider = pi_provider::Provider::start(false).await.unwrap();
    provider.configure(&agent).unwrap();
    std::fs::write(root.path().join("probe.txt"), "rust-pi-acp-probe\n").unwrap();
    std::fs::create_dir_all(agent.join("prompts")).unwrap();
    std::fs::write(agent.join("prompts/copy.md"),"---\ndescription: Copy owned files\n---\nCopy $1 verbatim into $2, then reply PI_ADAPTER_OK").unwrap();
    std::fs::create_dir_all(root.path().join(".pi")).unwrap();
    std::fs::write(
        root.path().join(".pi/settings.json"),
        json!({"sessionDir":".native-sessions"}).to_string(),
    )
    .unwrap();
    let options = Options {
        command: env!("CARGO_BIN_EXE_pi-acp-rust").into(),
        args: vec![],
        cwd: root.path().into(),
        env: BTreeMap::from([
            ("PI_PATH".into(), command),
            ("PI_CODING_AGENT_SESSION_DIR".into(), String::new()),
            (
                "PI_CODING_AGENT_DIR".into(),
                agent.to_string_lossy().into_owned(),
            ),
        ]),
        inherit_env: true,
        timeout: Duration::from_secs(60),
    };
    let client = Client::connect(options.clone()).await.unwrap();
    let id = client.new_session().await.unwrap();
    assert!(
        client.config_options().await.iter().any(
            |option| option["id"] == "model" && option["currentValue"] == "owned-local/fixture"
        )
    );
    let generated = client.prompt("/copy probe.txt result.txt", vec![]).await;
    client.shutdown().await.unwrap();
    let client = Client::connect(options).await.unwrap();
    assert!(root.path().join(".native-sessions").exists());
    let restored = client.load_session(&id, false).await;
    let listed = client.list_sessions().await.unwrap();
    let deleted = client.delete_session().await;
    client.shutdown().await.unwrap();
    let generated = generated.unwrap();
    assert_eq!(generated.text, "PI_ADAPTER_OK");
    assert!(generated.output.had_tool_activity);
    assert!(!generated.output.output_lost);
    assert_eq!(
        std::fs::read_to_string(root.path().join("result.txt")).unwrap(),
        "rust-pi-acp-probe\n"
    );
    assert!(
        restored
            .unwrap()
            .iter()
            .any(|entry| entry["content"]["text"] == "PI_ADAPTER_OK")
    );
    assert!(listed.iter().any(|entry| entry["sessionId"] == id));
    deleted.unwrap();
    let mut requests = vec![];
    while let Ok(request) = provider.requests.try_recv() {
        requests.push(request);
    }
    assert_eq!(requests.len(), 3);
    let user = requests[0]["messages"]
        .as_array()
        .unwrap()
        .iter()
        .rev()
        .find(|message| message["role"] == "user")
        .unwrap();
    assert!(
        user["content"]
            .to_string()
            .contains("Copy probe.txt verbatim into result.txt")
    );
    assert!(
        requests.last().unwrap()["messages"]
            .as_array()
            .unwrap()
            .iter()
            .any(|message| message["role"] == "tool")
    );
}

#[tokio::test]
async fn pi_uncertain_control_timeout_closes_transport_before_another_prompt() {
    let root = tempfile::tempdir().unwrap();
    let mut options = options(root.path());
    options.timeout = Duration::from_secs(40);
    options
        .env
        .insert("PI_FIXTURE_UNRESPONSIVE_SET_MODEL".into(), "1".into());
    let client = Client::connect(options).await.unwrap();
    client.new_session().await.unwrap();
    let error = client
        .set_option("model", "fixture/plain")
        .await
        .unwrap_err();
    assert!(error.to_string().contains("timed out"), "{error}");
    assert!(client.prompt("hello", vec![]).await.is_err());
    assert!(
        !audit(root.path())
            .iter()
            .any(|request| request["type"] == "prompt")
    );
    client.shutdown().await.unwrap();
}

#[cfg(unix)]
#[tokio::test]
#[ignore = "requires PI_ADAPTER_PROBE_PATH; checks native Bash cleanup on ACP disconnect"]
async fn installed_pi_acp_eof_aborts_detached_native_tool_process() {
    use agent_client_protocol::schema::{ProtocolVersion, v1};
    let executable = std::env::var("PI_ADAPTER_PROBE_PATH").expect("Set PI_ADAPTER_PROBE_PATH");
    let root = tempfile::tempdir().unwrap();
    let agent = root.path().join("agent");
    let _provider = pi_provider::Provider::start(true).await.unwrap();
    _provider.configure(&agent).unwrap();
    let peer = acp_peer::Peer::spawn(
        env!("CARGO_BIN_EXE_pi-acp-rust"),
        root.path(),
        BTreeMap::from([
            ("PI_PATH".into(), executable),
            (
                "PI_CODING_AGENT_DIR".into(),
                agent.to_string_lossy().into_owned(),
            ),
            ("PI_CODING_AGENT_SESSION_DIR".into(), String::new()),
        ]),
    )
    .await
    .unwrap();
    peer.connection
        .send_request(v1::InitializeRequest::new(ProtocolVersion::V1))
        .block_task()
        .await
        .unwrap();
    let id = peer
        .connection
        .send_request(v1::NewSessionRequest::new(root.path().to_owned()))
        .block_task()
        .await
        .unwrap()
        .session_id;
    let prompt = tokio::spawn({
        let cx = peer.connection.clone();
        async move {
            cx.send_request(v1::PromptRequest::new(
                id,
                vec![v1::ContentBlock::Text(v1::TextContent::new(
                    "Run the owned Bash probe",
                ))],
            ))
            .block_task()
            .await
        }
    });
    let pid_path = root.path().join("owned-bash.pid");
    let running = tokio::time::timeout(Duration::from_secs(10), async {
        while !pid_path.exists() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    let shutdown = peer.shutdown().await;
    let _ = prompt.await;
    running.unwrap();
    shutdown.unwrap();
    let pid = std::fs::read_to_string(pid_path)
        .unwrap()
        .trim()
        .parse::<libc::pid_t>()
        .unwrap();
    let stopped = tokio::time::timeout(Duration::from_secs(3), async {
        while unsafe { libc::kill(pid, 0) } == 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    if stopped.is_err() {
        unsafe { libc::kill(pid, libc::SIGKILL) };
    }
    assert!(
        stopped.is_ok(),
        "Owned native Bash process survived ACP EOF"
    );
    assert_eq!(unsafe { libc::kill(pid, 0) }, -1);
    assert_eq!(
        std::io::Error::last_os_error().raw_os_error(),
        Some(libc::ESRCH)
    );
}
