use soulsteads_messaging::acp::{Client, Options};
use serde_json::{Value, json};
use std::{collections::BTreeMap, path::Path, time::Duration};
#[path = "support/acp_peer.rs"]
mod acp_peer;
fn environment(root: &Path) -> BTreeMap<String, String> {
    BTreeMap::from([
        (
            "CLAUDE_CODE_EXECUTABLE".into(),
            env!("CARGO_BIN_EXE_acp-fixture").into(),
        ),
        (
            "CLAUDE_CONFIG_DIR".into(),
            root.join("config").to_string_lossy().into_owned(),
        ),
        ("ACP_CLAUDE_FIXTURE".into(), "1".into()),
        (
            "CLAUDE_FIXTURE_AUDIT".into(),
            root.join("native.jsonl").to_string_lossy().into_owned(),
        ),
    ])
}
fn options(root: &Path) -> Options {
    Options {
        command: env!("CARGO_BIN_EXE_claude-acp-rust").into(),
        args: vec![],
        cwd: root.into(),
        env: environment(root),
        inherit_env: false,
        timeout: Duration::from_secs(10),
    }
}
fn audit(root: &Path) -> Vec<Value> {
    std::fs::read_to_string(root.join("native.jsonl"))
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect()
}
async fn peer(root: &Path) -> acp_peer::Peer {
    use agent_client_protocol::schema::{ProtocolVersion, v1};
    let peer = acp_peer::Peer::spawn(
        env!("CARGO_BIN_EXE_claude-acp-rust"),
        root,
        environment(root),
    )
    .await
    .unwrap();
    let initialized = peer
        .connection
        .send_request(v1::InitializeRequest::new(ProtocolVersion::V1))
        .block_task()
        .await
        .unwrap();
    assert!(initialized.auth_methods.is_empty());
    peer
}
#[tokio::test]
async fn claude_streams_once_and_resumes_native_history() {
    let root = tempfile::tempdir().unwrap();
    let client = Client::connect(options(root.path())).await.unwrap();
    let session = client.new_session().await.unwrap();
    uuid::Uuid::parse_str(&session).unwrap();
    let result = client.prompt("hello", vec![]).await.unwrap();
    assert_eq!(result.text, "native reply");
    assert_eq!(client.commands().await[0]["name"], "compact");
    assert!(
        client
            .list_sessions()
            .await
            .unwrap()
            .iter()
            .any(|entry| entry["sessionId"] == session)
    );
    client.shutdown().await.unwrap();
    let client = Client::connect(options(root.path())).await.unwrap();
    let history = client.load_session(&session, false).await.unwrap();
    assert!(
        history
            .iter()
            .any(|entry| entry["content"]["text"] == "native reply")
    );
    assert_eq!(
        client.prompt("next", vec![]).await.unwrap().text,
        "native reply"
    );
    client.close_session().await.unwrap();
    client.shutdown().await.unwrap();
    assert!(
        audit(root.path())
            .iter()
            .filter(|event| event["type"] == "spawn")
            .any(|event| event["args"]
                .as_array()
                .unwrap()
                .iter()
                .any(|arg| arg == "--resume"))
    );
}
#[tokio::test]
async fn claude_tool_media_large_output_and_compaction() {
    let root = tempfile::tempdir().unwrap();
    let client = Client::connect(options(root.path())).await.unwrap();
    client.new_session().await.unwrap();
    for prompt in ["tool", "media", "large"] {
        let reply = client.prompt(prompt, vec![]).await.unwrap();
        assert_eq!(reply.text, "tool reply");
        assert!(reply.output.had_tool_activity);
        assert!(!reply.output.output_lost);
        if prompt == "media" {
            assert_eq!(reply.output.images.len(), 1);
        }
        if prompt == "large" {
            assert!(reply.output.tool_results.contains("truncated"));
        }
    }
    assert_eq!(
        client.prompt("/compact", vec![]).await.unwrap().text,
        "Context compacted"
    );
    client.shutdown().await.unwrap();
}
#[tokio::test]
async fn claude_native_failure_stale_result_and_eof_are_not_success() {
    let root = tempfile::tempdir().unwrap();
    let client = Client::connect(options(root.path())).await.unwrap();
    client.new_session().await.unwrap();
    assert!(
        client
            .prompt("fail", vec![])
            .await
            .unwrap_err()
            .to_string()
            .contains("Native fixture failure")
    );
    assert_eq!(client.prompt("stale", vec![]).await.unwrap().text, "fresh");
    assert!(client.prompt("crash", vec![]).await.is_err());
    assert!(client.prompt("hello", vec![]).await.is_err());
    client.shutdown().await.unwrap();
}
#[tokio::test]
async fn claude_settings_apply_native_controls_and_busy_turns_reject_mutations() {
    let root = tempfile::tempdir().unwrap();
    let client = Client::connect(options(root.path())).await.unwrap();
    client.new_session().await.unwrap();
    assert!(
        client
            .config_options()
            .await
            .iter()
            .any(|option| option["id"] == "reasoning_effort")
    );
    client.set_option("reasoning_effort", "high").await.unwrap();
    client.set_option("model", "haiku").await.unwrap();
    assert!(
        !client
            .config_options()
            .await
            .iter()
            .any(|option| option["id"] == "reasoning_effort")
    );
    assert!(client.set_option("reasoning_effort", "high").await.is_err());
    client.set_option("mode", "plan").await.unwrap();
    let prompt = tokio::spawn({
        let client = client.clone();
        async move { client.prompt("hold", vec![]).await }
    });
    tokio::time::timeout(Duration::from_secs(3), async {
        while !audit(root.path())
            .iter()
            .any(|event| event["type"] == "user")
        {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    assert!(client.set_option("model", "default").await.is_err());
    assert!(client.prompt("overlap", vec![]).await.is_err());
    client.cancel().await.unwrap();
    assert_eq!(
        prompt.await.unwrap().unwrap().stop_reason.as_deref(),
        Some("cancelled")
    );
    assert_eq!(
        audit(root.path())
            .iter()
            .filter(|event| event["type"] == "user")
            .count(),
        1
    );
    client.shutdown().await.unwrap();
}
#[tokio::test]
async fn claude_typed_sdk_approve_once_preserves_input_without_saving_permissions() {
    use agent_client_protocol::schema::v1;
    let root = tempfile::tempdir().unwrap();
    let mut peer = peer(root.path()).await;
    let session = peer
        .connection
        .send_request(v1::NewSessionRequest::new(root.path().to_owned()))
        .block_task()
        .await
        .unwrap();
    let id = session.session_id;
    let prompt = tokio::spawn({
        let cx = peer.connection.clone();
        let id = id.clone();
        async move {
            cx.send_request(v1::PromptRequest::new(
                id,
                vec![v1::ContentBlock::Text(v1::TextContent::new("approve"))],
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
    assert_eq!(request.options.len(), 2);
    assert!(request.options.iter().all(|option| matches!(
        option.kind,
        v1::PermissionOptionKind::AllowOnce | v1::PermissionOptionKind::RejectOnce
    )));
    responder
        .respond(v1::RequestPermissionResponse::new(
            v1::RequestPermissionOutcome::Selected(v1::SelectedPermissionOutcome::new(
                "allow-once",
            )),
        ))
        .unwrap();
    assert_eq!(
        prompt.await.unwrap().unwrap().stop_reason,
        v1::StopReason::EndTurn
    );
    let reply = audit(root.path())
        .into_iter()
        .find(|event| event["type"] == "control_response")
        .unwrap();
    assert_eq!(reply["response"]["response"]["behavior"], "allow");
    assert!(
        reply["response"]["response"]
            .get("updatedPermissions")
            .is_none()
    );
    let mut events = vec![];
    while let Ok(event) = peer.updates.try_recv() {
        events.push(serde_json::to_value(event).unwrap());
    }
    assert!(
        events
            .iter()
            .any(|event| event["update"]["sessionUpdate"] == "tool_call")
    );
    peer.shutdown().await.unwrap();
}
#[tokio::test]
async fn claude_cancelled_approval_cannot_grant_late_answer() {
    use agent_client_protocol::schema::v1;
    let root = tempfile::tempdir().unwrap();
    let mut peer = peer(root.path()).await;
    let session = peer
        .connection
        .send_request(v1::NewSessionRequest::new(root.path().to_owned()))
        .block_task()
        .await
        .unwrap();
    let id = session.session_id;
    let prompt = tokio::spawn({
        let cx = peer.connection.clone();
        let id = id.clone();
        async move {
            cx.send_request(v1::PromptRequest::new(
                id,
                vec![v1::ContentBlock::Text(v1::TextContent::new("approve"))],
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
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if audit(root.path())
                .iter()
                .any(|event| event["type"] == "control_response")
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        audit(root.path())
            .into_iter()
            .find(|event| event["type"] == "control_response")
            .unwrap()["response"]["response"]["behavior"],
        "deny"
    );
    peer.shutdown().await.unwrap();
}
#[tokio::test]
async fn claude_native_permission_cancellation_is_observed_before_task_scheduling() {
    let root = tempfile::tempdir().unwrap();
    let client = Client::connect(options(root.path())).await.unwrap();
    client.new_session().await.unwrap();
    assert_eq!(
        client.prompt("cancel-ask", vec![]).await.unwrap().text,
        "denied"
    );
    let replies = audit(root.path());
    assert_eq!(
        replies
            .iter()
            .find(|event| event["type"] == "control_response")
            .unwrap()["response"]["response"]["behavior"],
        "deny"
    );
    client.shutdown().await.unwrap();
}
#[tokio::test]
async fn claude_load_rejects_workspace_collision_and_recovers_from_missing_history() {
    let root = tempfile::tempdir().unwrap();
    let client = Client::connect(options(root.path())).await.unwrap();
    client.new_session().await.unwrap();
    client.prompt("hello", vec![]).await.unwrap();
    client.shutdown().await.unwrap();
    let config = root.path().join("config/projects");
    let transcript = std::fs::read_dir(&config)
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path();
    let transcript = std::fs::read_dir(transcript)
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path();
    let id = transcript.file_stem().unwrap().to_str().unwrap();
    let other = tempfile::tempdir().unwrap();
    let original = std::fs::read_to_string(&transcript).unwrap();
    let invalid = original
        .lines()
        .map(|line| {
            let mut value: Value = serde_json::from_str(line).unwrap();
            value["cwd"] = json!(other.path());
            value.to_string() + "\n"
        })
        .collect::<String>();
    std::fs::write(&transcript, invalid).unwrap();
    let client = Client::connect(options(root.path())).await.unwrap();
    assert!(
        client
            .load_session(id, true)
            .await
            .unwrap_err()
            .to_string()
            .contains("different workspace")
    );
    assert!(
        client
            .load_session(&uuid::Uuid::new_v4().to_string(), true)
            .await
            .is_err()
    );
    client.new_session().await.unwrap();
    assert_eq!(
        client.prompt("hello", vec![]).await.unwrap().text,
        "native reply"
    );
    client.shutdown().await.unwrap();
}
#[tokio::test]
async fn claude_mcp_transports_and_prompt_images_are_native_inputs() {
    use agent_client_protocol::schema::v1;
    let root = tempfile::tempdir().unwrap();
    let peer = peer(root.path()).await;
    let request = v1::NewSessionRequest::new(root.path().to_owned()).mcp_servers(vec![
        v1::McpServer::Stdio(v1::McpServerStdio::new("fixture", "tool").args(vec!["arg".into()])),
        v1::McpServer::Http(v1::McpServerHttp::new("remote", "https://example.test/mcp")),
        v1::McpServer::Sse(v1::McpServerSse::new("events", "https://example.test/sse")),
    ]);
    let session = peer
        .connection
        .send_request(request)
        .block_task()
        .await
        .unwrap();
    let response = peer
        .connection
        .send_request(v1::PromptRequest::new(
            session.session_id,
            vec![v1::ContentBlock::Image(v1::ImageContent::new(
                "AA==",
                "image/png",
            ))],
        ))
        .block_task()
        .await
        .unwrap();
    assert_eq!(response.stop_reason, v1::StopReason::EndTurn);
    let audit = audit(root.path());
    let spawn = &audit[0]["args"].as_array().unwrap();
    let config: Value = serde_json::from_str(
        spawn[spawn.iter().position(|arg| arg == "--mcp-config").unwrap() + 1]
            .as_str()
            .unwrap(),
    )
    .unwrap();
    assert_eq!(config["mcpServers"]["fixture"]["type"], "stdio");
    assert_eq!(config["mcpServers"]["remote"]["type"], "http");
    assert_eq!(config["mcpServers"]["events"]["type"], "sse");
    assert_eq!(
        audit.iter().find(|event| event["type"] == "user").unwrap()["message"]["content"][0]["source"]
            ["type"],
        "base64"
    );
    peer.shutdown().await.unwrap();
}
#[cfg(unix)]
#[tokio::test]
async fn claude_child_exits_when_acp_client_closes() {
    let root = tempfile::tempdir().unwrap();
    let mut options = options(root.path());
    let pid = root.path().join("pid");
    options.env.insert(
        "CLAUDE_FIXTURE_PID".into(),
        pid.to_string_lossy().into_owned(),
    );
    let client = Client::connect(options).await.unwrap();
    client.new_session().await.unwrap();
    let pid = std::fs::read_to_string(pid)
        .unwrap()
        .parse::<libc::pid_t>()
        .unwrap();
    client.shutdown().await.unwrap();
    assert_eq!(unsafe { libc::kill(pid, 0) }, -1);
    assert_eq!(
        std::io::Error::last_os_error().raw_os_error(),
        Some(libc::ESRCH)
    );
}
#[tokio::test]
#[ignore = "requires installed authenticated Claude Code; sends no coding prompt"]
async fn installed_claude_initializes_and_lists_models_without_login() {
    let root = tempfile::tempdir().unwrap();
    let client = Client::connect(Options {
        command: env!("CARGO_BIN_EXE_claude-acp-rust").into(),
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
    client.close_session().await.unwrap();
    client.shutdown().await.unwrap();
}

#[tokio::test]
async fn configured_builtin_claude_runs_from_bridge_and_resumes_native_uuid() {
    let root = tempfile::tempdir().unwrap();
    let config = root.path().join("bridge.json");
    std::fs::write(&config,json!({"acp":{"defaultBackend":"claude","backends":{"claude":{"env":environment(root.path())}}}}).to_string()).unwrap();
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
    let audit = audit(root.path());
    let args = audit
        .iter()
        .filter(|event| event["type"] == "spawn")
        .map(|event| event["args"].as_array().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(args.len(), 2);
    assert!(args[0].iter().any(|arg| arg == "--session-id"));
    assert!(args[1].iter().any(|arg| arg == "--resume"));
}
#[tokio::test]
async fn claude_delete_removes_only_verified_native_transcripts() {
    let root = tempfile::tempdir().unwrap();
    let client = Client::connect(options(root.path())).await.unwrap();
    let id = client.new_session().await.unwrap();
    client.prompt("hello", vec![]).await.unwrap();
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
}
#[tokio::test]
#[ignore = "submits an owned file-copy coding prompt using installed Claude credentials"]
async fn installed_claude_executes_prompt_approves_once_and_restores_history() {
    use agent_client_protocol::schema::{ProtocolVersion, v1};
    let root = tempfile::tempdir().unwrap();
    std::fs::write(root.path().join("probe.txt"), "rust-claude-acp-probe\n").unwrap();
    let mut peer = acp_peer::Peer::spawn(
        env!("CARGO_BIN_EXE_claude-acp-rust"),
        root.path(),
        BTreeMap::new(),
    )
    .await
    .unwrap();
    peer.connection
        .send_request(v1::InitializeRequest::new(ProtocolVersion::V1))
        .block_task()
        .await
        .unwrap();
    let session = peer
        .connection
        .send_request(v1::NewSessionRequest::new(root.path().to_owned()))
        .block_task()
        .await
        .unwrap();
    let id = session.session_id;
    let prompt=peer.connection.send_request(v1::PromptRequest::new(id.clone(),vec![v1::ContentBlock::Text(v1::TextContent::new("Use Read and Write to copy probe.txt verbatim into result.txt in this directory. Do not change other files. Reply exactly CLAUDE_ADAPTER_OK."))])).block_task();
    tokio::pin!(prompt);
    let generated=tokio::time::timeout(Duration::from_secs(180),async{loop{tokio::select!{result=&mut prompt=>break result,ask=peer.permissions.recv()=>{let (_,responder)=ask.unwrap();responder.respond(v1::RequestPermissionResponse::new(v1::RequestPermissionOutcome::Selected(v1::SelectedPermissionOutcome::new("allow-once")))).unwrap();}}}}).await;
    let file = std::fs::read_to_string(root.path().join("result.txt"));
    let mut events = vec![];
    while let Ok(event) = peer.updates.try_recv() {
        events.push(serde_json::to_value(event).unwrap());
    }
    peer.connection
        .send_request(v1::CloseSessionRequest::new(id.clone()))
        .block_task()
        .await
        .unwrap();
    peer.shutdown().await.unwrap();
    let mut peer = acp_peer::Peer::spawn(
        env!("CARGO_BIN_EXE_claude-acp-rust"),
        root.path(),
        BTreeMap::new(),
    )
    .await
    .unwrap();
    peer.connection
        .send_request(v1::InitializeRequest::new(ProtocolVersion::V1))
        .block_task()
        .await
        .unwrap();
    let restored = peer
        .connection
        .send_request(v1::LoadSessionRequest::new(
            id.clone(),
            root.path().to_owned(),
        ))
        .block_task()
        .await;
    let mut history = vec![];
    while let Ok(event) = peer.updates.try_recv() {
        history.push(serde_json::to_value(event).unwrap());
    }
    let deleted = peer
        .connection
        .send_request(v1::DeleteSessionRequest::new(id))
        .block_task()
        .await;
    peer.shutdown().await.unwrap();
    assert_eq!(
        generated.unwrap().unwrap().stop_reason,
        v1::StopReason::EndTurn
    );
    assert_eq!(file.unwrap(), "rust-claude-acp-probe\n");
    assert!(
        events
            .iter()
            .any(|event| event["update"]["sessionUpdate"] == "tool_call")
    );
    let text = events
        .iter()
        .filter(|event| event["update"]["sessionUpdate"] == "agent_message_chunk")
        .filter_map(|event| event["update"]["content"]["text"].as_str())
        .collect::<String>();
    assert!(text.contains("CLAUDE_ADAPTER_OK"), "{text}");
    restored.unwrap();
    assert!(history.iter().any(|event| {
        event["update"]["content"]["text"]
            .as_str()
            .is_some_and(|s| s.contains("CLAUDE_ADAPTER_OK"))
    }));
    deleted.unwrap();
}

#[tokio::test]
async fn claude_reload_idle_session_preserves_native_process_and_settings() {
    let root = tempfile::tempdir().unwrap();
    let client = Client::connect(options(root.path())).await.unwrap();
    let id = client.new_session().await.unwrap();
    client.set_option("reasoning_effort", "high").await.unwrap();
    client.prompt("hello", vec![]).await.unwrap();
    let history = client.load_session(&id, false).await.unwrap();
    assert!(
        history
            .iter()
            .any(|entry| entry["content"]["text"] == "native reply")
    );
    assert!(
        client
            .config_options()
            .await
            .iter()
            .any(|option| option["id"] == "reasoning_effort" && option["currentValue"] == "high")
    );
    assert_eq!(
        client.prompt("next", vec![]).await.unwrap().text,
        "native reply"
    );
    assert_eq!(
        audit(root.path())
            .iter()
            .filter(|event| event["type"] == "spawn")
            .count(),
        1
    );
    client.shutdown().await.unwrap();
}
