use soulsteads_messaging::acp::{Client, Options};
use serde_json::{Value, json};
use std::{collections::BTreeMap, path::Path, time::Duration};

fn options(directory: &Path) -> Options {
    Options {
        command: env!("CARGO_BIN_EXE_codex-acp-rust").into(),
        args: vec![],
        cwd: directory.canonicalize().unwrap(),
        env: BTreeMap::from([
            (
                "CODEX_PATH".into(),
                env!("CARGO_BIN_EXE_acp-fixture").into(),
            ),
            ("ACP_CODEX_FIXTURE".into(), "1".into()),
            (
                "CODEX_FIXTURE_AUDIT".into(),
                directory.join("native.jsonl").display().to_string(),
            ),
            (
                "CODEX_FIXTURE_PID".into(),
                directory.join("native.pid").display().to_string(),
            ),
        ]),
        inherit_env: false,
        timeout: Duration::from_secs(3),
    }
}
fn audit(directory: &Path) -> Vec<Value> {
    std::fs::read_to_string(directory.join("native.jsonl"))
        .unwrap_or_default()
        .split_inclusive('\n')
        .filter(|line| line.ends_with('\n'))
        .map(|line| serde_json::from_str(line).unwrap())
        .collect()
}
async fn wait_for(directory: &Path, method: &str) {
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if audit(directory)
                .iter()
                .any(|request| request["method"] == method)
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn codex_unknown_native_tools_preserve_activity_for_empty_response_retry() {
    let directory = tempfile::tempdir().unwrap();
    let client = Client::connect(options(directory.path())).await.unwrap();
    client.new_session().await.unwrap();
    let response = client.prompt("future-tool", vec![]).await.unwrap();
    assert!(response.text.is_empty());
    assert!(response.output.had_tool_activity);
    client.shutdown().await.unwrap();
}

#[tokio::test]
async fn codex_streaming_config_images_and_compaction_use_native_rpcs_without_login() {
    let directory = tempfile::tempdir().unwrap();
    let client = Client::connect(options(directory.path())).await.unwrap();
    let id = client.new_session().await.unwrap();
    assert!(id.starts_with("native-thread-"));
    assert_eq!(client.commands().await[0]["name"], "compact");
    client.set_option("reasoning_effort", "high").await.unwrap();
    client.set_mode("read-only").await.unwrap();
    let response = client
        .prompt(
            "hello",
            vec![json!({"type":"image","data":"eA==","mimeType":"image/png"})],
        )
        .await
        .unwrap();
    assert_eq!(response.text, "native reply");
    assert_eq!(response.stop_reason.as_deref(), Some("end_turn"));
    let request = audit(directory.path())
        .into_iter()
        .find(|request| request["method"] == "turn/start")
        .unwrap();
    assert_eq!(request["params"]["effort"], "high");
    assert_eq!(request["params"]["sandboxPolicy"]["type"], "readOnly");
    assert_eq!(
        request["params"]["input"][1]["url"],
        "data:image/png;base64,eA=="
    );
    let response = client.prompt("/compact", vec![]).await.unwrap();
    assert!(response.output.had_tool_activity);
    assert_eq!(
        audit(directory.path())
            .iter()
            .filter(|request| request["method"] == "turn/start")
            .count(),
        1
    );
    assert!(
        client
            .request("authenticate", json!({"methodId":"login"}))
            .await
            .is_err()
    );
    client.shutdown().await.unwrap();
}
#[tokio::test]
async fn codex_overlapping_prompt_steers_the_current_turn_and_retains_one_collector() {
    let directory = tempfile::tempdir().unwrap();
    let client = Client::connect(options(directory.path())).await.unwrap();
    client.new_session().await.unwrap();
    let prompt = {
        let client = client.clone();
        tokio::spawn(async move { client.prompt("hold", vec![]).await })
    };
    wait_for(directory.path(), "turn/start").await;
    client.forward("follow-up").await.unwrap();
    assert_eq!(prompt.await.unwrap().unwrap().text, "steered reply");
    let audit = audit(directory.path());
    assert_eq!(
        audit
            .iter()
            .filter(|request| request["method"] == "turn/start")
            .count(),
        1
    );
    assert_eq!(
        audit
            .iter()
            .filter(|request| request["method"] == "turn/steer")
            .count(),
        1
    );
    client.shutdown().await.unwrap();
}
#[tokio::test]
async fn codex_cancellation_uses_the_owned_turn_id_and_waits_for_native_completion() {
    let directory = tempfile::tempdir().unwrap();
    let client = Client::connect(options(directory.path())).await.unwrap();
    client.new_session().await.unwrap();
    let prompt = {
        let client = client.clone();
        tokio::spawn(async move { client.prompt("hold", vec![]).await })
    };
    wait_for(directory.path(), "turn/start").await;
    client.cancel().await.unwrap();
    let response = prompt.await.unwrap().unwrap();
    assert_eq!(response.stop_reason.as_deref(), Some("cancelled"));
    assert_eq!(
        audit(directory.path())
            .iter()
            .filter(|request| request["method"] == "turn/interrupt")
            .count(),
        1
    );
    client.shutdown().await.unwrap();
}
#[tokio::test]
async fn codex_history_is_paginated_and_controls_preserve_native_ids() {
    let directory = tempfile::tempdir().unwrap();
    let client = Client::connect(options(directory.path())).await.unwrap();
    let id = client.new_session().await.unwrap();
    assert_eq!(client.list_sessions().await.unwrap()[0]["sessionId"], id);
    client.close_session().await.unwrap();
    let history = client.load_session(&id, false).await.unwrap();
    assert!(
        history
            .iter()
            .any(|item| item["content"]["text"] == "old question")
    );
    assert!(
        history
            .iter()
            .any(|item| item["content"]["text"] == "old answer")
    );
    assert_eq!(
        audit(directory.path())
            .iter()
            .filter(|request| request["method"] == "thread/items/list")
            .count(),
        2
    );
    client.delete_session().await.unwrap();
    assert!(client.list_sessions().await.unwrap().is_empty());
    client.shutdown().await.unwrap();
}
#[tokio::test]
async fn codex_stale_turn_updates_and_native_failure_do_not_replay_prompts() {
    let directory = tempfile::tempdir().unwrap();
    let client = Client::connect(options(directory.path())).await.unwrap();
    client.new_session().await.unwrap();
    assert_eq!(client.prompt("stale", vec![]).await.unwrap().text, "fresh");
    assert!(
        client
            .prompt("fail", vec![])
            .await
            .unwrap_err()
            .to_string()
            .contains("Native fixture failed")
    );
    assert_eq!(
        audit(directory.path())
            .iter()
            .filter(|request| request["method"] == "turn/start")
            .count(),
        2
    );
    client.shutdown().await.unwrap();
}
#[tokio::test]
async fn codex_native_transport_loss_releases_the_active_collector() {
    let directory = tempfile::tempdir().unwrap();
    let client = Client::connect(options(directory.path())).await.unwrap();
    client.new_session().await.unwrap();
    let result = tokio::time::timeout(Duration::from_secs(3), client.prompt("crash", vec![]))
        .await
        .unwrap();
    assert!(result.is_err());
    client.shutdown().await.unwrap();
}
#[tokio::test]
async fn codex_repeated_history_cursor_is_an_error_and_restores_the_previous_session() {
    let directory = tempfile::tempdir().unwrap();
    let mut config = options(directory.path());
    config
        .env
        .insert("CODEX_FIXTURE_HISTORY".into(), "cycle".into());
    let client = Client::connect(config).await.unwrap();
    let id = client.new_session().await.unwrap();
    assert!(
        client
            .load_session(&id, false)
            .await
            .unwrap_err()
            .to_string()
            .contains("repeated history cursor")
    );
    assert_eq!(client.session_id().await.as_deref(), Some(id.as_str()));
    assert_eq!(
        client.prompt("hello", vec![]).await.unwrap().text,
        "native reply"
    );
    client.shutdown().await.unwrap();
}
#[tokio::test]
#[ignore = "Requires installed Codex credentials; creates and deletes an owned session without submitting a prompt"]
async fn installed_codex_initializes_lists_models_and_creates_a_session() {
    let directory = tempfile::tempdir().unwrap();
    let client = Client::connect(Options {
        command: env!("CARGO_BIN_EXE_codex-acp-rust").into(),
        args: vec![],
        cwd: directory.path().to_owned(),
        env: BTreeMap::new(),
        inherit_env: true,
        timeout: Duration::from_secs(40),
    })
    .await
    .unwrap();
    let id = client.new_session().await;
    if id.is_ok() {
        assert!(!client.config_options().await.is_empty());
        client.delete_session().await.unwrap();
    }
    client.shutdown().await.unwrap();
    assert!(!id.unwrap().is_empty());
}

#[path = "support/acp_peer.rs"]
mod acp_peer;
use agent_client_protocol::{
    UntypedMessage,
    schema::{ProtocolVersion, v1},
};

async fn typed_peer(directory: &Path) -> acp_peer::Peer {
    let peer = acp_peer::Peer::spawn(
        env!("CARGO_BIN_EXE_codex-acp-rust"),
        directory,
        options(directory).env,
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
async fn codex_sdk_typed_messages_and_approve_once_interoperate() {
    let directory = tempfile::tempdir().unwrap();
    let mut peer = typed_peer(directory.path()).await;
    let session = peer
        .connection
        .send_request(v1::NewSessionRequest::new(directory.path().to_owned()))
        .block_task()
        .await
        .unwrap();
    let id = session.session_id;
    let connection = peer.connection.clone();
    let request_id = id.clone();
    let prompt = tokio::spawn(async move {
        connection
            .send_request(v1::PromptRequest::new(
                request_id,
                vec![v1::ContentBlock::Text(v1::TextContent::new("approve"))],
            ))
            .block_task()
            .await
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
    let mut events = vec![];
    while let Ok(update) = peer.updates.try_recv() {
        events.push(serde_json::to_value(update).unwrap());
    }
    assert!(
        events
            .iter()
            .any(|event| event["update"]["sessionUpdate"] == "tool_call")
    );
    assert!(
        events
            .iter()
            .any(|event| event["update"]["sessionUpdate"] == "tool_call_update")
    );
    assert!(
        events
            .iter()
            .any(|event| event["update"]["content"]["text"] == "approved")
    );
    assert_eq!(
        audit(directory.path())
            .iter()
            .find(|request| request["id"] == 900)
            .unwrap()["result"]["decision"],
        "accept"
    );
    peer.shutdown().await.unwrap();
}
#[tokio::test]
async fn codex_cancelled_approval_never_grants_a_late_answer() {
    let directory = tempfile::tempdir().unwrap();
    let mut peer = typed_peer(directory.path()).await;
    let session = peer
        .connection
        .send_request(v1::NewSessionRequest::new(directory.path().to_owned()))
        .block_task()
        .await
        .unwrap();
    let id = session.session_id;
    let connection = peer.connection.clone();
    let request_id = id.clone();
    let prompt = tokio::spawn(async move {
        connection
            .send_request(v1::PromptRequest::new(
                request_id,
                vec![v1::ContentBlock::Text(v1::TextContent::new("approve"))],
            ))
            .block_task()
            .await
    });
    let (_, responder) = tokio::time::timeout(Duration::from_secs(3), peer.permissions.recv())
        .await
        .unwrap()
        .unwrap();
    peer.connection
        .send_notification(v1::CancelNotification::new(id))
        .unwrap();
    let outcome = tokio::time::timeout(Duration::from_secs(3), prompt)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(outcome.stop_reason, v1::StopReason::Cancelled);
    let _ = responder.respond(v1::RequestPermissionResponse::new(
        v1::RequestPermissionOutcome::Selected(v1::SelectedPermissionOutcome::new("allow-once")),
    ));
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if audit(directory.path())
                .iter()
                .any(|request| request["id"] == 900)
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        audit(directory.path())
            .iter()
            .find(|request| request["id"] == 900)
            .unwrap()["result"]["decision"],
        "cancel"
    );
    peer.shutdown().await.unwrap();
}
#[tokio::test]
async fn codex_invalid_input_and_settings_fail_before_native_submission() {
    let directory = tempfile::tempdir().unwrap();
    let peer = typed_peer(directory.path()).await;
    let session = peer
        .connection
        .send_request(v1::NewSessionRequest::new(directory.path().to_owned()))
        .block_task()
        .await
        .unwrap();
    let id = session.session_id.to_string();
    assert!(
        peer.connection
            .send_request(
                UntypedMessage::new(
                    "session/set_config_option",
                    json!({"sessionId":id,"configId":"model","value":"unadvertised-model"})
                )
                .unwrap()
            )
            .block_task()
            .await
            .is_err()
    );
    assert!(peer.connection.send_request(UntypedMessage::new("session/prompt",json!({"sessionId":id,"prompt":[{"type":"image","data":"eA==","mimeType":"application/unknown"}]})).unwrap()).block_task().await.is_err());
    assert_eq!(
        audit(directory.path())
            .iter()
            .filter(|request| request["method"] == "turn/start")
            .count(),
        0
    );
    peer.shutdown().await.unwrap();
}

#[tokio::test]
async fn configured_builtin_codex_runs_from_the_bridge_binary_and_resumes_stored_native_ids() {
    let directory = tempfile::tempdir().unwrap();
    let config = directory.path().join("config.json");
    std::fs::write(&config,json!({"acp":{"defaultBackend":"codex","backends":{"codex":{"env":options(directory.path()).env}}}}).to_string()).unwrap();
    for _ in 0..2 {
        let output = tokio::process::Command::new(env!("CARGO_BIN_EXE_soulsteads-messaging"))
            .current_dir(directory.path())
            .args(["--config", config.to_str().unwrap(), "--no-images", "hello"])
            .output()
            .await
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(
            String::from_utf8_lossy(&output.stdout).contains("native reply"),
            "{}",
            String::from_utf8_lossy(&output.stdout)
        );
    }
    let audit = audit(directory.path());
    assert_eq!(
        audit
            .iter()
            .filter(|request| request["method"] == "thread/start")
            .count(),
        1
    );
    assert_eq!(
        audit
            .iter()
            .filter(|request| request["method"] == "thread/resume")
            .count(),
        1
    );
    assert_eq!(
        audit
            .iter()
            .filter(|request| request["method"] == "turn/start")
            .count(),
        2
    );
}

#[tokio::test]
#[ignore = "Submits a small coding prompt with installed Codex credentials and deletes the owned native session"]
async fn installed_codex_executes_a_prompt_and_restores_native_history() {
    let directory = tempfile::tempdir().unwrap();
    std::fs::write(
        directory.path().join("probe.txt"),
        "rust-acp-probe-2026-10-13\n",
    )
    .unwrap();
    let client = Client::connect(Options {
        command: env!("CARGO_BIN_EXE_codex-acp-rust").into(),
        args: vec![],
        cwd: directory.path().to_owned(),
        env: BTreeMap::new(),
        inherit_env: true,
        timeout: Duration::from_secs(40),
    })
    .await
    .unwrap();
    let session = client.new_session().await.unwrap();
    let generated=tokio::time::timeout(Duration::from_secs(180),client.prompt("Read probe.txt, copy its contents verbatim into result.txt in this directory, and reply exactly CODEX_ADAPTER_OK. Do not change any other files.",vec![])).await;
    let result = std::fs::read_to_string(directory.path().join("result.txt"));
    let history = if matches!(generated, Ok(Ok(_))) {
        Some(client.load_session(&session, false).await)
    } else {
        None
    };
    let deleted = if matches!(generated, Ok(Ok(_))) {
        Some(client.delete_session().await)
    } else {
        None
    };
    client.shutdown().await.unwrap();
    let response = generated.unwrap().unwrap();
    assert!(
        response.text.contains("CODEX_ADAPTER_OK"),
        "{}",
        response.text
    );
    assert!(response.output.had_tool_activity);
    assert_eq!(result.unwrap(), "rust-acp-probe-2026-10-13\n");
    let history = history.unwrap().unwrap();
    assert!(history.iter().any(|item| {
        item["content"]["text"]
            .as_str()
            .is_some_and(|text| text.contains("CODEX_ADAPTER_OK"))
    }));
    deleted.unwrap().unwrap();
}

#[tokio::test]
async fn codex_mcp_configuration_and_inline_results_survive_translation() {
    let directory = tempfile::tempdir().unwrap();
    let client=Client::connect_with_servers(options(directory.path()),vec![json!({"name":"stdio-test","command":"fixture-mcp","args":["--stdio"],"env":[{"name":"TEST_VALUE","value":"fixture"}]}),json!({"name":"http-test","type":"http","url":"http://127.0.0.1:9999/mcp","headers":[{"name":"X-Test","value":"fixture"}]})]).await.unwrap();
    client.new_session().await.unwrap();
    let native = audit(directory.path())
        .into_iter()
        .find(|request| request["method"] == "thread/start")
        .unwrap();
    assert_eq!(
        native["params"]["config"]["mcp_servers"]["stdio-test"]["env"]["TEST_VALUE"],
        "fixture"
    );
    assert_eq!(
        native["params"]["config"]["mcp_servers"]["http-test"]["http_headers"]["X-Test"],
        "fixture"
    );
    let response = client.prompt("media", vec![]).await.unwrap();
    assert_eq!(response.text, "media result");
    let images = client.inline_images().await;
    assert_eq!(images.len(), 1);
    assert!(images[0].path.ends_with(".png"));
    assert!(directory.path().join(&images[0].path).exists());
    client.shutdown().await.unwrap();
}
#[tokio::test]
async fn codex_large_tool_output_is_bounded_without_losing_the_turn() {
    let directory = tempfile::tempdir().unwrap();
    let client = Client::connect(options(directory.path())).await.unwrap();
    client.new_session().await.unwrap();
    let response = client.prompt("stdout", vec![]).await.unwrap();
    assert_eq!(response.text, "large output");
    assert!(response.output.had_tool_activity);
    client.shutdown().await.unwrap();
}
#[tokio::test]
async fn codex_wrong_workspace_and_busy_settings_are_rejected_before_mutating_the_thread() {
    let directory = tempfile::tempdir().unwrap();
    let other = tempfile::tempdir().unwrap();
    let peer = typed_peer(directory.path()).await;
    let session = peer
        .connection
        .send_request(v1::NewSessionRequest::new(directory.path().to_owned()))
        .block_task()
        .await
        .unwrap();
    let id = session.session_id;
    assert!(
        peer.connection
            .send_request(v1::LoadSessionRequest::new(
                id.clone(),
                other.path().to_owned()
            ))
            .block_task()
            .await
            .is_err()
    );
    assert!(
        !audit(directory.path())
            .iter()
            .any(|request| request["method"] == "thread/resume")
    );
    let connection = peer.connection.clone();
    let request_id = id.clone();
    let prompt = tokio::spawn(async move {
        connection
            .send_request(v1::PromptRequest::new(
                request_id,
                vec![v1::ContentBlock::Text(v1::TextContent::new("hold"))],
            ))
            .block_task()
            .await
    });
    wait_for(directory.path(), "turn/start").await;
    assert!(
        peer.connection
            .send_request(v1::SetSessionConfigOptionRequest::new(
                id.clone(),
                "model",
                "other-model"
            ))
            .block_task()
            .await
            .is_err()
    );
    peer.connection
        .send_notification(v1::CancelNotification::new(id))
        .unwrap();
    assert_eq!(
        prompt.await.unwrap().unwrap().stop_reason,
        v1::StopReason::Cancelled
    );
    peer.shutdown().await.unwrap();
}
#[cfg(unix)]
#[tokio::test]
async fn codex_owned_native_process_exits_after_the_acp_client_disconnects() {
    let directory = tempfile::tempdir().unwrap();
    let peer = typed_peer(directory.path()).await;
    let native_pid: i32 = std::fs::read_to_string(directory.path().join("native.pid"))
        .unwrap()
        .parse()
        .unwrap();
    peer.shutdown().await.unwrap();
    assert_eq!(unsafe { libc::kill(native_pid, 0) }, -1);
    assert_eq!(
        std::io::Error::last_os_error().raw_os_error(),
        Some(libc::ESRCH)
    );
}

#[tokio::test]
async fn codex_uncertain_native_control_timeout_closes_transport_without_a_second_turn() {
    let directory = tempfile::tempdir().unwrap();
    let client = Client::connect(options(directory.path())).await.unwrap();
    client.new_session().await.unwrap();
    let result = tokio::time::timeout(Duration::from_secs(40), client.prompt("timeout", vec![]))
        .await
        .unwrap();
    assert!(result.is_err());
    assert!(client.prompt("second turn", vec![]).await.is_err());
    assert_eq!(
        audit(directory.path())
            .iter()
            .filter(|request| request["method"] == "turn/start")
            .count(),
        1
    );
    client.shutdown().await.unwrap();
}

#[tokio::test]
async fn codex_compaction_retains_collector_until_native_turn_terminal() {
    let directory = tempfile::tempdir().unwrap();
    let marker = directory.path().join("compact.pending");
    let mut settings = options(directory.path());
    settings.env.insert(
        "CODEX_FIXTURE_COMPACT_MARKER".into(),
        marker.display().to_string(),
    );
    let client = std::sync::Arc::new(Client::connect(settings).await.unwrap());
    client.new_session().await.unwrap();
    let compact = tokio::spawn({
        let client = client.clone();
        async move { client.prompt("/compact", vec![]).await }
    });
    tokio::time::timeout(Duration::from_secs(2), async {
        while !marker.exists() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_millis(30)).await;
    assert!(
        !compact.is_finished(),
        "An item completion must not release the compaction turn"
    );
    compact.await.unwrap().unwrap();
    assert_eq!(
        client
            .prompt("after compaction", vec![])
            .await
            .unwrap()
            .text,
        "native reply"
    );
    client.shutdown().await.unwrap();
}
