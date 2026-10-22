use soulsteads_messaging::acp::{Client, Options};
use serde_json::json;
use std::{collections::BTreeMap, sync::Arc, time::Duration};

fn followup_fixture(directory: &std::path::Path, actions: serde_json::Value) -> Options {
    let script = directory.join("script.json");
    std::fs::write(&script, actions.to_string()).unwrap();
    let mut config = options();
    config.cwd = directory.to_owned();
    for (name, path) in [
        ("ACP_FIXTURE_MATRIX_SCRIPT", script),
        ("ACP_FIXTURE_MATRIX_COUNTER", directory.join("counter")),
        ("ACP_FIXTURE_RPC_AUDIT", directory.join("rpc-audit.jsonl")),
    ] {
        config.env.insert(name.into(), path.display().to_string());
    }
    config
}

fn rpc_audit(directory: &std::path::Path) -> Vec<serde_json::Value> {
    let bytes = std::fs::read_to_string(directory.join("rpc-audit.jsonl")).unwrap_or_default();
    bytes
        .split_inclusive('\n')
        .filter(|line| line.ends_with('\n'))
        .map(|line| serde_json::from_str(line).unwrap())
        .collect()
}

async fn prompt_rpc(directory: &std::path::Path, text: &str) -> serde_json::Value {
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if let Some(request) = rpc_audit(directory).into_iter().find(|request| {
                request["method"] == "session/prompt"
                    && request["params"]["prompt"][0]["text"] == text
            }) {
                return request;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap()
}

async fn emit_barrier(client: &Client, messages: serde_json::Value) {
    client
        .request("fixture/emit", json!({"messages":messages}))
        .await
        .unwrap();
}

fn text_update(text: &str) -> serde_json::Value {
    json!({"method":"session/update","params":{"sessionId":"fixture","update":{"sessionUpdate":"agent_message_chunk","content":{"type":"text","text":text}}}})
}

fn completed_rpc(request: &serde_json::Value, reason: &str) -> serde_json::Value {
    json!({"jsonrpc":"2.0","id":request["id"],"result":{"stopReason":reason}})
}

pub fn options() -> Options {
    Options {
        command: env!("CARGO_BIN_EXE_acp-fixture").into(),
        args: vec![],
        cwd: std::env::current_dir().unwrap(),
        env: BTreeMap::new(),
        inherit_env: false,
        timeout: Duration::from_secs(3),
    }
}

#[tokio::test]
async fn nonempty_prompt_has_no_fixed_empty_drain_delay_and_ignores_idle_updates() {
    let directory = tempfile::tempdir().unwrap();
    let mut config = options();
    config.cwd = directory.path().to_owned();
    let client = Client::connect(config).await.unwrap();
    client.new_session().await.unwrap();
    let mut receiver = client.subscribe();
    client.request("fixture/emit", json!({"messages":[{"method":"session/update","params":{"sessionId":"fixture","update":{"sessionUpdate":"agent_message_chunk","content":{"type":"text","text":"stale"}}}}]})).await.unwrap();
    assert!(
        receiver.try_recv().is_err(),
        "inactive updates must not reach a prompt presentation pump"
    );
    let response =
        tokio::time::timeout(Duration::from_millis(220), client.prompt("hello", vec![])).await;
    client.shutdown().await.unwrap();
    assert!(
        response.is_ok(),
        "source drains only an empty response, not a nonempty completed prompt"
    );
    assert!(!response.unwrap().unwrap().text.contains("stale"));
}

#[tokio::test]
async fn source_nonempty_completion_does_not_wait_empty_response_backoff() {
    let client = Client::connect(options()).await.unwrap();
    client.new_session().await.unwrap();
    let response =
        tokio::time::timeout(Duration::from_millis(220), client.prompt("hello", vec![])).await;
    client.shutdown().await.unwrap();
    assert!(
        response.is_ok(),
        "nonempty completed prompt waited for the empty-response backoff"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn buffered_completion_and_trailing_chunk_close_one_collector_without_delay() {
    let client = Client::connect(options()).await.unwrap();
    client.new_session().await.unwrap();
    for _attempt in 0..64 {
        let response =
            tokio::time::timeout(Duration::from_millis(220), client.prompt("hello", vec![]))
                .await
                .unwrap()
                .unwrap();
        assert_eq!(response.text, "hello world");
        assert_eq!(response.output.chunks, 2);
        assert!(response.output.collector_closed);
    }
    client.shutdown().await.unwrap();
}

#[tokio::test]
async fn nonnormal_followup_result_wins_and_rejected_followup_keeps_one_collector() {
    for reason in ["refusal", "max_tokens", "cancelled", "error"] {
        let directory = tempfile::tempdir().unwrap();
        let client = Client::connect(followup_fixture(
            directory.path(),
            json!([
                {"hold":true},
                if reason == "error" { json!({"error":true}) } else { json!({"hold":true}) },
                {"hold":true}
            ]),
        ))
        .await
        .unwrap();
        client.new_session().await.unwrap();
        let original = {
            let client = client.clone();
            tokio::spawn(async move { client.prompt("hello", vec![]).await })
        };
        let original_rpc = prompt_rpc(directory.path(), "hello").await;
        let tool_start = json!({"method":"session/update","params":{"sessionId":"fixture","update":{"sessionUpdate":"tool_call","toolCallId":"ongoing-tool","title":"read"}}});
        let tool_delta = json!({"method":"session/update","params":{"sessionId":"fixture","update":{"sessionUpdate":"tool_call_update","toolCallId":"ongoing-tool","status":"completed","rawOutput":{"output":"original tool delta"}}}});
        emit_barrier(
            &client,
            json!([tool_start, tool_delta, text_update("original ")]),
        )
        .await;
        if reason == "error" {
            assert!(
                client
                    .forward("bad")
                    .await
                    .unwrap_err()
                    .to_string()
                    .contains("scripted failure")
            );
            assert!(client.accepts_followups());
            let (output, stop, _) = client.prompt_observation().await;
            assert_eq!(output.tool_calls, 1);
            assert_eq!(output.tool_results, "original tool delta\n");
            assert!(!output.collector_closed);
            assert!(stop.is_none());
        }
        let followup = {
            let client = client.clone();
            tokio::spawn(async move { client.forward("good").await })
        };
        let followup_rpc = prompt_rpc(directory.path(), "good").await;
        emit_barrier(
            &client,
            json!([completed_rpc(
                &followup_rpc,
                if reason == "error" {
                    "end_turn"
                } else {
                    reason
                }
            )]),
        )
        .await;
        followup.await.unwrap().unwrap();
        assert!(client.accepts_followups());
        assert!(!original.is_finished());
        let continued_delta = json!({"method":"session/update","params":{"sessionId":"fixture","update":{"sessionUpdate":"tool_call_update","toolCallId":"ongoing-tool","status":"completed","rawOutput":{"output":"continued tool delta"}}}});
        emit_barrier(
            &client,
            json!([
                tool_start,
                continued_delta,
                text_update("answer"),
                completed_rpc(&original_rpc, "end_turn")
            ]),
        )
        .await;
        let response = original.await.unwrap().unwrap();
        assert_eq!(response.text, "original answer");
        assert_eq!(response.output.tool_calls, 1);
        assert_eq!(
            response.output.tool_results,
            "original tool delta\ncontinued tool delta\n"
        );
        assert_eq!(response.output.chunks, 2);
        assert_eq!(
            response.stop_reason.as_deref(),
            Some(if reason == "error" {
                "end_turn"
            } else {
                reason
            })
        );
        assert!(response.output.collector_closed);
        assert!(!client.accepts_followups());
        assert_eq!(
            rpc_audit(directory.path())
                .iter()
                .filter(|request| request["method"] == "session/prompt")
                .count(),
            if reason == "error" { 3 } else { 2 }
        );
        client.shutdown().await.unwrap();
    }
}

#[cfg(unix)]
#[tokio::test]
async fn shutdown_terminates_only_its_owned_subprocess_group() {
    let directory = tempfile::tempdir().unwrap();
    let pid_path = directory.path().join("child.pid");
    let mut config = options();
    config.cwd = directory.path().to_owned();
    config.env.insert(
        "ACP_FIXTURE_CHILD_PID".into(),
        pid_path.display().to_string(),
    );
    let client = Client::connect(config).await.unwrap();
    let child: i32 = std::fs::read_to_string(pid_path).unwrap().parse().unwrap();
    let mut independent = tokio::process::Command::new(env!("CARGO_BIN_EXE_acp-fixture"))
        .arg("--owned-child")
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    client.shutdown().await.unwrap();
    let stopped = tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            let stat = std::fs::read_to_string(format!("/proc/{child}/stat"));
            if stat.is_err() || stat.unwrap().split_whitespace().nth(2) == Some("Z") {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .is_ok();
    let independent_alive = independent.try_wait().unwrap().is_none();
    independent.kill().await.unwrap();
    independent.wait().await.unwrap();
    if !stopped {
        unsafe {
            libc::kill(child, libc::SIGKILL);
        }
    }
    assert!(stopped, "owned ACP grandchild survived shutdown");
    assert!(independent_alive, "shutdown touched an independent process");
}

#[tokio::test]
async fn rejected_cancel_write_does_not_commit_stop_reason() {
    let directory = tempfile::tempdir().unwrap();
    let started = directory.path().join("started");
    let release = directory.path().join("release");
    let script = directory.path().join("script.json");
    std::fs::write(&script, json!([{"closeInput":true,"startedFile":started,"releaseFile":release,"text":"original answer","stopReason":"end_turn"}]).to_string()).unwrap();
    let mut config = options();
    config.env.insert(
        "ACP_FIXTURE_MATRIX_SCRIPT".into(),
        script.display().to_string(),
    );
    config.env.insert(
        "ACP_FIXTURE_MATRIX_COUNTER".into(),
        directory.path().join("counter").display().to_string(),
    );
    let client = Client::connect(config).await.unwrap();
    let original = {
        let client = client.clone();
        tokio::spawn(async move { client.prompt("hello", vec![]).await })
    };
    tokio::time::timeout(Duration::from_secs(2), async {
        while !started.exists() {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap();
    assert!(client.cancel().await.is_err());
    std::fs::write(release, "release").unwrap();
    let response = original.await.unwrap().unwrap();
    assert_eq!(response.stop_reason.as_deref(), Some("end_turn"));
    assert_eq!(response.text, "original answer");
    client.shutdown().await.unwrap();
}

#[tokio::test]
async fn load_retains_large_history_and_excludes_foreign_session_updates() {
    let client = Client::connect(options()).await.unwrap();
    let history = client.load_session("history-long", false).await.unwrap();
    assert_eq!(history.len(), 600);
    assert!(
        history
            .iter()
            .all(|update| update["content"]["text"] == "history")
    );
    client.shutdown().await.unwrap();
}

#[tokio::test]
async fn dropped_requests_do_not_exhaust_pending_capacity() {
    let client = Client::connect(options()).await.unwrap();
    for _ in 0..270 {
        assert!(
            tokio::time::timeout(
                Duration::from_millis(2),
                client.request("fixture/never", json!({}))
            )
            .await
            .is_err()
        );
    }
    assert_eq!(client.new_session().await.unwrap(), "fixture");
    client.shutdown().await.unwrap();
}
#[tokio::test]
async fn mcp_options_close_and_delete_controls_use_real_rpc_and_clear_session() {
    let servers = vec![json!({"name":"fixture-mcp","command":"fixture","args":[],"env":[]})];
    let client = Client::connect_with_servers(options(), servers.clone())
        .await
        .unwrap();
    client.new_session().await.unwrap();
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(
            &client.prompt("mcp", vec![]).await.unwrap().text
        )
        .unwrap(),
        json!(servers)
    );
    client.close_session().await.unwrap();
    assert!(client.session_id().await.is_none());
    client.new_session().await.unwrap();
    client.delete_session().await.unwrap();
    assert!(client.session_id().await.is_none());
    client.shutdown().await.unwrap();
}

#[tokio::test]
async fn real_subprocess_initializes_assembles_filters_and_controls() {
    let client = Client::connect(options()).await.unwrap();
    assert_eq!(client.new_session().await.unwrap(), "fixture");
    let response = client.prompt("hello", vec![]).await.unwrap();
    assert_eq!(response.text, "hello world");
    assert_eq!(response.stop_reason.as_deref(), Some("end_turn"));
    assert_eq!(
        client.request("session/list", json!({})).await.unwrap()["sessions"][0]["sessionId"],
        "fixture"
    );
    assert!(client.set_option("model", "invented").await.is_err());
    assert_eq!(
        client.set_option("model", "two").await.unwrap()["currentValue"],
        "two"
    );
    assert_eq!(
        client.prompt("permission", vec![]).await.unwrap().text,
        "denied safely"
    );
    assert!(client.prompt("error", vec![]).await.is_err());
    client.shutdown().await.unwrap();
}

#[tokio::test]
async fn active_followup_has_one_collector_and_cancellation_finishes() {
    let directory = tempfile::tempdir().unwrap();
    let mut config = options();
    config.cwd = directory.path().to_owned();
    config.env.insert(
        "ACP_FIXTURE_RPC_AUDIT".into(),
        directory
            .path()
            .join("rpc-audit.jsonl")
            .display()
            .to_string(),
    );
    let client = Arc::new(Client::connect(config).await.unwrap());
    let before_idle = rpc_audit(directory.path());
    assert!(client.session_id().await.is_none());
    assert_eq!(
        client
            .forward("idle-before-session")
            .await
            .unwrap_err()
            .to_string(),
        "No active ACP prompt"
    );
    assert!(!client.accepts_followups());
    assert!(client.session_id().await.is_none());
    emit_barrier(&client, json!([])).await;
    let after_idle = rpc_audit(directory.path());
    assert_eq!(&after_idle[..before_idle.len()], before_idle.as_slice());
    assert_eq!(after_idle.len(), before_idle.len() + 1);
    assert_eq!(after_idle.last().unwrap()["method"], "fixture/emit");
    assert!(
        after_idle
            .iter()
            .all(|request| request["method"] != "session/new"
                && request["method"] != "session/prompt")
    );
    client.new_session().await.unwrap();
    assert!(client.forward("idle").await.is_err());
    let task_client = client.clone();
    let task = tokio::spawn(async move { task_client.prompt("wait", vec![]).await });
    prompt_rpc(directory.path(), "wait").await;
    emit_barrier(&client, json!([])).await;
    client.forward("followup").await.unwrap();
    assert_eq!(task.await.unwrap().unwrap().text, "followed");
    let task_client = client.clone();
    let task = tokio::spawn(async move { task_client.prompt("wait", vec![]).await });
    tokio::time::timeout(Duration::from_secs(2), async {
        while rpc_audit(directory.path())
            .iter()
            .filter(|request| request["params"]["prompt"][0]["text"] == "wait")
            .count()
            != 2
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    emit_barrier(&client, json!([])).await;
    client.cancel().await.unwrap();
    assert_eq!(
        task.await.unwrap().unwrap().stop_reason.as_deref(),
        Some("cancelled")
    );
    client.shutdown().await.unwrap();
}

#[tokio::test]
async fn child_exit_rejects_pending_instead_of_hanging() {
    for pending_followup in [false, true] {
        let directory = tempfile::tempdir().unwrap();
        let actions = if pending_followup {
            json!([{"hold":true}, {"exit":true}])
        } else {
            json!([{"exit":true}])
        };
        let client = Client::connect(followup_fixture(directory.path(), actions))
            .await
            .unwrap();
        client.new_session().await.unwrap();
        let worker = client.clone();
        let original = tokio::spawn(async move { worker.prompt("exit", vec![]).await });
        prompt_rpc(directory.path(), "exit").await;
        if pending_followup {
            let error = client
                .forward("exit-followup")
                .await
                .unwrap_err()
                .to_string();
            assert!(
                error.contains("closed") || error.contains("exited"),
                "{error}"
            );
            prompt_rpc(directory.path(), "exit-followup").await;
        }
        let error = tokio::time::timeout(Duration::from_secs(2), original)
            .await
            .unwrap()
            .unwrap()
            .unwrap_err()
            .to_string();
        assert!(
            error.contains("closed") || error.contains("exited"),
            "{error}"
        );
        assert!(!client.accepts_followups());
        assert!(client.prompt_observation().await.0.collector_closed);
        let error = tokio::time::timeout(Duration::from_secs(2), client.prompt("next", vec![]))
            .await
            .unwrap()
            .unwrap_err()
            .to_string();
        assert!(
            error.contains("closed") || error.contains("exited"),
            "{error}"
        );
        let (output, stop, _) = client.prompt_observation().await;
        assert!(output.collector_closed);
        assert!(stop.is_none());
        assert!(!client.accepts_followups());
        assert!(client.forward("idle").await.is_err());
        let requests = rpc_audit(directory.path());
        assert_eq!(
            requests
                .iter()
                .filter(|request| request["method"] == "initialize")
                .count(),
            1
        );
        assert_eq!(
            requests
                .iter()
                .filter(|request| request["method"] == "session/new")
                .count(),
            1
        );
        assert_eq!(
            requests
                .iter()
                .filter(|request| request["method"] == "session/prompt")
                .count(),
            if pending_followup { 2 } else { 1 }
        );
        client.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn coding_turns_outlive_control_deadlines_without_replay() {
    for mode in ["original", "with-followup", "draining-followup"] {
        let directory = tempfile::tempdir().unwrap();
        let mut config = followup_fixture(directory.path(), json!([{"hold":true},{"hold":true}]));
        config.timeout = Duration::from_millis(250);
        let client = Client::connect(config).await.unwrap();
        let session = client.new_session().await.unwrap();
        let worker = client.clone();
        let original = tokio::spawn(async move { worker.prompt("long-original", vec![]).await });
        let rpc = prompt_rpc(directory.path(), "long-original").await;
        emit_barrier(&client, json!([text_update("partial")])).await;
        let followup = if mode != "original" {
            let worker = client.clone();
            Some(tokio::spawn(async move {
                worker.forward("long-followup").await
            }))
        } else {
            None
        };
        let extra = if followup.is_some() {
            Some(prompt_rpc(directory.path(), "long-followup").await)
        } else {
            None
        };
        if mode == "draining-followup" {
            emit_barrier(&client, json!([completed_rpc(&rpc, "end_turn")])).await;
        }
        tokio::time::sleep(Duration::from_millis(650)).await;
        let alive = !original.is_finished()
            && client.accepts_followups()
            && followup.as_ref().is_none_or(|task| !task.is_finished());
        let mut messages = vec![text_update(" final")];
        if mode != "draining-followup" {
            messages.push(completed_rpc(&rpc, "end_turn"));
        }
        if let Some(extra) = extra {
            messages.push(completed_rpc(&extra, "end_turn"));
        }
        emit_barrier(&client, json!(messages)).await;
        let result = tokio::time::timeout(Duration::from_secs(2), original)
            .await
            .unwrap()
            .unwrap();
        if let Some(task) = followup {
            let _ = tokio::time::timeout(Duration::from_secs(2), task)
                .await
                .unwrap()
                .unwrap();
        }
        let preserved_session = client.session_id().await;
        client.shutdown().await.unwrap();
        assert!(
            alive,
            "coding collector expired at control deadline: {mode}"
        );
        let response = result.unwrap();
        assert_eq!(response.text, "partial final");
        assert!(response.output.collector_closed);
        assert_eq!(preserved_session.as_deref(), Some(session.as_str()));
        assert_eq!(
            rpc_audit(directory.path())
                .iter()
                .filter(|r| r["method"] == "session/prompt")
                .count(),
            if mode == "original" { 1 } else { 2 }
        );
    }
}

#[tokio::test]
async fn dropped_followup_closes_collector_without_replay() {
    {
        let failure = "followup-drop";
        let directory = tempfile::tempdir().unwrap();
        let has_followup = true;
        let mut actions = vec![json!({"hold":true})];
        if has_followup {
            actions.push(json!({"hold":true}));
        }
        actions.extend((0..9).map(|_| json!({"hold":true})));
        let mut config = followup_fixture(directory.path(), json!(actions));
        config.timeout = Duration::from_millis(400);
        let client = Client::connect(config).await.unwrap();
        let session = client.new_session().await.unwrap();
        let mut events = client.subscribe();
        let worker = client.clone();
        let original = tokio::spawn(async move { worker.prompt("stalled-original", vec![]).await });
        let original_rpc = prompt_rpc(directory.path(), "stalled-original").await;
        emit_barrier(&client, json!([text_update("partial")])).await;
        assert_eq!(events.recv().await.unwrap()["content"]["text"], "partial");
        let followup = if has_followup {
            let worker = client.clone();
            Some(tokio::spawn(async move {
                worker.forward("stalled-followup").await
            }))
        } else {
            None
        };
        let followup_rpc = if has_followup {
            Some(prompt_rpc(directory.path(), "stalled-followup").await)
        } else {
            None
        };
        if failure.starts_with("followup-") {
            emit_barrier(&client, json!([completed_rpc(&original_rpc, "end_turn")])).await;
            tokio::time::timeout(Duration::from_secs(2), async {
                while client.prompt_observation().await.1.as_deref() != Some("end_turn") {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            assert!(!original.is_finished());
            assert!(!client.prompt_observation().await.0.collector_closed);
        }
        let followup = followup.unwrap();
        followup.abort();
        assert!(followup.await.unwrap_err().is_cancelled());
        let response = tokio::time::timeout(Duration::from_secs(2), original)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(response.text, "partial");
        assert_eq!(response.stop_reason.as_deref(), Some("end_turn"));
        assert!(response.output.collector_closed);
        let (output, stop, _) = client.prompt_observation().await;
        assert!(output.collector_closed, "{failure}");
        assert_eq!(output.chunks, 1);
        assert!(!client.accepts_followups());
        assert!(client.forward("idle").await.is_err());
        let mut late = vec![
            text_update("late idle"),
            completed_rpc(&original_rpc, "cancelled"),
        ];
        if let Some(request) = followup_rpc {
            late.push(completed_rpc(&request, "cancelled"));
        }
        emit_barrier(&client, json!(late)).await;
        assert!(events.try_recv().is_err());
        let (after_late, after_stop, _) = client.prompt_observation().await;
        assert_eq!(after_late.chunks, 1);
        assert_eq!(after_stop, stop);
        assert!(after_late.collector_closed);
        let worker = client.clone();
        let next = tokio::spawn(async move { worker.prompt("next", vec![]).await });
        let next_rpc = prompt_rpc(directory.path(), "next").await;
        let mut additional = Vec::new();
        let mut additional_rpcs = Vec::new();
        for index in 0..8 {
            let text = format!("next-followup-{index}");
            let worker = client.clone();
            let instruction = text.clone();
            additional.push(tokio::spawn(
                async move { worker.forward(&instruction).await },
            ));
            additional_rpcs.push(prompt_rpc(directory.path(), &text).await);
        }
        assert_eq!(
            client.forward("overflow").await.unwrap_err().to_string(),
            "ACP follow-up queue full"
        );
        let mut completion = vec![text_update("fresh"), completed_rpc(&next_rpc, "end_turn")];
        completion.extend(
            additional_rpcs
                .iter()
                .map(|request| completed_rpc(request, "end_turn")),
        );
        emit_barrier(&client, json!(completion)).await;
        for followup in additional {
            followup.await.unwrap().unwrap();
        }
        let response = next.await.unwrap().unwrap();
        assert_eq!(response.text, "fresh");
        assert_eq!(response.stop_reason.as_deref(), Some("end_turn"));
        assert_eq!(response.output.chunks, 1);
        assert!(response.output.collector_closed);
        assert!(!client.accepts_followups());
        assert_eq!(client.session_id().await.as_deref(), Some(session.as_str()));
        let requests = rpc_audit(directory.path());
        assert_eq!(
            requests
                .iter()
                .filter(|request| request["method"] == "session/new")
                .count(),
            1
        );
        let prompts: Vec<_> = requests
            .iter()
            .filter(|request| request["method"] == "session/prompt")
            .collect();
        assert_eq!(prompts.len(), if has_followup { 11 } else { 10 });
        assert!(
            prompts
                .iter()
                .all(|request| request["params"]["sessionId"] == session)
        );
        assert_eq!(
            prompts
                .iter()
                .filter(|request| request["params"]["prompt"][0]["text"] == "stalled-original")
                .count(),
            1
        );
        assert_eq!(
            prompts
                .iter()
                .filter(|request| request["params"]["prompt"][0]["text"] == "stalled-followup")
                .count(),
            usize::from(has_followup)
        );
        client.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn long_stream_collects_600_chunks_without_overflow() {
    let client = Client::connect(options()).await.unwrap();
    client.new_session().await.unwrap();
    assert_eq!(
        client.prompt("long", vec![]).await.unwrap().text,
        "x".repeat(600)
    );
    client.shutdown().await.unwrap();
}

#[tokio::test]
async fn collector_waits_for_delayed_followup_after_original_rpc() {
    let directory = tempfile::tempdir().unwrap();
    let client = Client::connect(followup_fixture(
        directory.path(),
        json!([
            {"hold":true}, {"hold":true}, {"text":"next","stopReason":"end_turn"}
        ]),
    ))
    .await
    .unwrap();
    let session = client.new_session().await.unwrap();
    let mut events = client.subscribe();
    let worker = client.clone();
    let original = tokio::spawn(async move { worker.prompt("original", vec![]).await });
    let original_rpc = prompt_rpc(directory.path(), "original").await;
    let worker = client.clone();
    let followup = tokio::spawn(async move { worker.forward("delayed-followup").await });
    let followup_rpc = prompt_rpc(directory.path(), "delayed-followup").await;
    assert_ne!(original_rpc["id"], followup_rpc["id"]);
    assert_eq!(original_rpc["params"]["sessionId"], session);
    assert_eq!(followup_rpc["params"]["sessionId"], session);
    assert!(!original.is_finished());
    emit_barrier(
        &client,
        json!([
            text_update("first "),
            completed_rpc(&original_rpc, "end_turn")
        ]),
    )
    .await;
    tokio::time::timeout(Duration::from_secs(2), async {
        while client.prompt_observation().await.1.as_deref() != Some("end_turn") {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(
        !original.is_finished(),
        "original completed while follow-up RPC remained pending"
    );
    let (output, _, _) = client.prompt_observation().await;
    assert!(!output.collector_closed);
    assert_eq!(output.chunks, 1);
    assert_eq!(events.recv().await.unwrap()["content"]["text"], "first ");
    emit_barrier(
        &client,
        json!([
            text_update("follow-up"),
            completed_rpc(&followup_rpc, "end_turn")
        ]),
    )
    .await;
    let response = original.await.unwrap().unwrap();
    followup.await.unwrap().unwrap();
    assert_eq!(response.text, "first follow-up");
    assert_eq!(response.output.chunks, 2);
    assert!(response.output.collector_closed);
    assert_eq!(events.recv().await.unwrap()["content"]["text"], "follow-up");
    assert!(events.try_recv().is_err());
    assert!(!client.accepts_followups());
    emit_barrier(&client, json!([text_update("late idle")])).await;
    assert!(events.try_recv().is_err());
    assert_eq!(client.prompt_observation().await.0.chunks, 2);
    let next = client.prompt("next", vec![]).await.unwrap();
    assert_eq!(next.text, "next");
    assert_eq!(next.output.chunks, 1);
    assert_eq!(next.stop_reason.as_deref(), Some("end_turn"));
    assert!(next.output.collector_closed);
    let requests = rpc_audit(directory.path());
    let prompts: Vec<_> = requests
        .iter()
        .filter(|request| request["method"] == "session/prompt")
        .collect();
    assert_eq!(
        prompts
            .iter()
            .map(|request| request["params"]["prompt"][0]["text"].as_str().unwrap())
            .collect::<Vec<_>>(),
        ["original", "delayed-followup", "next"]
    );
    assert!(
        prompts
            .iter()
            .all(|request| request["params"]["sessionId"] == session)
    );
    assert_eq!(
        requests
            .iter()
            .filter(|request| request["method"] == "session/new")
            .count(),
        1
    );
    client.shutdown().await.unwrap();
}

#[test]
fn isolated_backend_inherits_runtime_proxies_not_ambient_secrets() {
    if std::env::var_os("BRIDGE_TEST_ENV_WORKER").is_none() {
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "isolated_backend_inherits_runtime_proxies_not_ambient_secrets",
                "--nocapture",
            ])
            .env("BRIDGE_TEST_ENV_WORKER", "1")
            .env("HTTP_PROXY", "http://127.0.0.1:9")
            .env("NO_PROXY", "localhost,127.0.0.1")
            .env("BRIDGE_TEST_AMBIENT_SECRET", "fake-ambient-fixture")
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stdout)
        );
        return;
    }
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        let mut options = options();
        options.env.insert(
            "BRIDGE_TEST_EXPLICIT".into(),
            "fake-explicit-fixture".into(),
        );
        let client = Client::connect(options).await.unwrap();
        let response = client.prompt("environment", vec![]).await.unwrap();
        let seen: serde_json::Value = serde_json::from_str(&response.text).unwrap();
        assert!(seen["BRIDGE_TEST_AMBIENT_SECRET"].is_null());
        assert_eq!(seen["BRIDGE_TEST_EXPLICIT"], "fake-explicit-fixture");
        assert_eq!(seen["HTTP_PROXY"], "http://127.0.0.1:9");
        assert_eq!(seen["NO_PROXY"], "localhost,127.0.0.1");
        client.shutdown().await.unwrap();
    });
}
