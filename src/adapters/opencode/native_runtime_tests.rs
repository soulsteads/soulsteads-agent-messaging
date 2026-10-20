use crate::{
    acp::Options,
    backend::{Client, Transport},
    config,
    runtime::{Bridge, Event, Sink},
};
use async_trait::async_trait;
use serde_json::{Value, json};
use std::{collections::BTreeMap, sync::Arc, time::Duration};
use tokio::sync::Mutex;

fn options(root: &std::path::Path) -> Options {
    Options {
        command: "python3".into(),
        args: vec![
            "-u".into(),
            concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/native_api.py").into(),
        ],
        cwd: root.to_owned(),
        env: BTreeMap::from([
            (
                "NATIVE_FIXTURE_ROOT".into(),
                root.to_string_lossy().into_owned(),
            ),
            ("BRIDGE_TEST_NATIVE_STDIO".into(), "1".into()),
        ]),
        inherit_env: false,
        timeout: Duration::from_secs(5),
    }
}

#[tokio::test]
async fn native_form_request_is_dispatched_and_answered_with_readback() {
    let root = tempfile::tempdir().unwrap();
    std::fs::write(root.path().join("release"), "").unwrap();
    let client = Client::connect(Transport::OpenCodeNative, options(root.path()), vec![])
        .await
        .unwrap();
    client.new_session().await.unwrap();
    let (_guard, mut requests) = client.form_handler().expect("native form handler");
    let c = client.clone();
    let prompt = tokio::spawn(async move { c.prompt("form", vec![]).await });
    let request = tokio::time::timeout(Duration::from_secs(2), requests.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(request.form["title"], "fixture");
    request.reply.send(Some(json!({"answer":"typed"}))).unwrap();
    std::fs::write(root.path().join("release"), "").unwrap();
    assert_eq!(prompt.await.unwrap().unwrap().text, "reply:form");
    client.shutdown().await.unwrap();
}

#[tokio::test]
async fn native_question_lost_ack_is_reconciled_without_replay() {
    let root = tempfile::tempdir().unwrap();
    std::fs::write(root.path().join("release"), "").unwrap();
    let mut options = options(root.path());
    options
        .env
        .insert("NATIVE_FIXTURE_FORM_LOST_ACK".into(), "1".into());
    let client = Client::connect(Transport::OpenCodeNative, options, vec![])
        .await
        .unwrap();
    client.new_session().await.unwrap();
    let (_guard, mut requests) = client.form_handler().unwrap();
    let task = {
        let client = client.clone();
        tokio::spawn(async move { client.prompt("form", vec![]).await })
    };
    let request = tokio::time::timeout(Duration::from_secs(2), requests.recv())
        .await
        .unwrap()
        .unwrap();
    request.reply.send(Some(json!({"answer":"yes"}))).unwrap();
    tokio::time::timeout(Duration::from_secs(3), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    client.shutdown().await.unwrap();
    let state: Value =
        serde_json::from_slice(&std::fs::read(root.path().join("native-state.json")).unwrap())
            .unwrap();
    assert_eq!(
        state["sessions"]
            .as_object()
            .unwrap()
            .values()
            .next()
            .unwrap()["formReplyCount"],
        1
    );
}
#[tokio::test]
async fn native_question_wrong_readback_never_reports_success() {
    let root = tempfile::tempdir().unwrap();
    std::fs::write(root.path().join("release"), "").unwrap();
    let mut options = options(root.path());
    options
        .env
        .insert("NATIVE_FIXTURE_FORM_WRONG_READBACK".into(), "1".into());
    let client = Client::connect(Transport::OpenCodeNative, options, vec![])
        .await
        .unwrap();
    client.new_session().await.unwrap();
    let (_guard, mut requests) = client.form_handler().unwrap();
    let task = {
        let client = client.clone();
        tokio::spawn(async move { client.prompt("form", vec![]).await })
    };
    let request = tokio::time::timeout(Duration::from_secs(2), requests.recv())
        .await
        .unwrap()
        .unwrap();
    request.reply.send(Some(json!({"answer":"yes"}))).unwrap();
    let result = tokio::time::timeout(Duration::from_secs(3), task)
        .await
        .unwrap()
        .unwrap();
    assert!(result.is_err(), "a mismatched readback must fail closed");
    client.shutdown().await.unwrap();
}
#[tokio::test]
async fn native_question_cancel_invalidates_late_answer() {
    let root = tempfile::tempdir().unwrap();
    std::fs::write(root.path().join("release"), "").unwrap();
    let client = Client::connect(Transport::OpenCodeNative, options(root.path()), vec![])
        .await
        .unwrap();
    client.new_session().await.unwrap();
    let (_guard, mut requests) = client.form_handler().unwrap();
    let task = {
        let client = client.clone();
        tokio::spawn(async move { client.prompt("form", vec![]).await })
    };
    let request = tokio::time::timeout(Duration::from_secs(2), requests.recv())
        .await
        .unwrap()
        .unwrap();
    client.cancel().await.unwrap();
    assert!(!request.current.load(std::sync::atomic::Ordering::Acquire));
    let _ = request.reply.send(Some(json!({"answer":"late"})));
    tokio::time::timeout(Duration::from_secs(3), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    client.shutdown().await.unwrap();
    let state: Value =
        serde_json::from_slice(&std::fs::read(root.path().join("native-state.json")).unwrap())
            .unwrap();
    let session = state["sessions"]
        .as_object()
        .unwrap()
        .values()
        .next()
        .unwrap();
    assert!(session["formReplyCount"].is_null());
    assert_eq!(session["formCancelled"], true);
}
#[tokio::test]
async fn native_question_wait_does_not_spend_model_timeout() {
    let root = tempfile::tempdir().unwrap();
    std::fs::write(root.path().join("release"), "").unwrap();
    let mut options = options(root.path());
    options.timeout = Duration::from_millis(400);
    let client = Client::connect(Transport::OpenCodeNative, options, vec![])
        .await
        .unwrap();
    client.new_session().await.unwrap();
    let (_guard, mut requests) = client.form_handler().unwrap();
    let task = {
        let client = client.clone();
        tokio::spawn(async move { client.prompt("form", vec![]).await })
    };
    let request = tokio::time::timeout(Duration::from_secs(2), requests.recv())
        .await
        .unwrap()
        .unwrap();
    tokio::time::sleep(Duration::from_millis(700)).await;
    assert!(
        !task.is_finished(),
        "human answer wait must not time out model execution"
    );
    request.reply.send(Some(json!({"answer":"yes"}))).unwrap();
    tokio::time::timeout(Duration::from_secs(3), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    client.shutdown().await.unwrap();
}

#[tokio::test]
async fn native_model_selection_confirms_explicit_default_variant() {
    let root = tempfile::tempdir().unwrap();
    let mut config = options(root.path());
    config
        .env
        .insert("NATIVE_FIXTURE_NORMALIZE_MODEL_VARIANT".into(), "1".into());
    let client = Client::connect(Transport::OpenCodeNative, config, vec![])
        .await
        .unwrap();
    client.new_session().await.unwrap();
    client.set_option("model", "fixture/alt").await.unwrap();
    client.set_option("effort", "high").await.unwrap();
    client.set_option("effort", "default").await.unwrap();
    assert_eq!(
        client.config_options().await[0]["currentValue"],
        "fixture/alt"
    );
    client.shutdown().await.unwrap();
}

#[tokio::test]
async fn native_catalog_and_mcp_queries_use_deep_object_location_scope() {
    let root = tempfile::tempdir().unwrap();
    std::fs::write(root.path().join("release"), "").unwrap();
    let mut config = options(root.path());
    config
        .env
        .insert("NATIVE_FIXTURE_REQUIRE_SCOPE".into(), "1".into());
    config
        .env
        .insert("NATIVE_FIXTURE_DEFAULT_MODEL".into(), "1".into());
    let client = Client::connect(
        Transport::OpenCodeNative,
        config,
        vec![json!({"name":"fixture","command":"fixture-mcp","args":[]})],
    )
    .await
    .unwrap();
    client.new_session().await.unwrap();
    client.set_option("model", "fixture/alt").await.unwrap();
    client.set_mode("plan").await.unwrap();
    assert_eq!(
        client.prompt("scoped", vec![]).await.unwrap().text,
        "reply:scoped"
    );
    client.shutdown().await.unwrap();
}

#[tokio::test]
async fn native_catalog_retries_missing_advertised_model_not_only_missing_default() {
    let root = tempfile::tempdir().unwrap();
    std::fs::write(root.path().join("release"), "").unwrap();
    let mut config = options(root.path());
    config
        .env
        .insert("NATIVE_FIXTURE_LATE_MODEL_LIST".into(), "1".into());
    let client = Client::connect(Transport::OpenCodeNative, config, vec![])
        .await
        .unwrap();
    client.new_session().await.unwrap();
    client.set_option("model", "fixture/fake").await.unwrap();
    assert_eq!(
        client.prompt("settled", vec![]).await.unwrap().text,
        "reply:settled"
    );
    client.shutdown().await.unwrap();
}

#[tokio::test]
async fn native_catalog_missing_selection_times_out_without_changing_session() {
    let root = tempfile::tempdir().unwrap();
    let mut config = options(root.path());
    config.timeout = Duration::from_millis(300);
    config
        .env
        .insert("NATIVE_FIXTURE_NEVER_ALT_MODEL".into(), "1".into());
    let client = Client::connect(Transport::OpenCodeNative, config, vec![])
        .await
        .unwrap();
    client.new_session().await.unwrap();
    let started = tokio::time::Instant::now();
    assert!(client.set_option("model", "fixture/alt").await.is_err());
    assert!(
        started.elapsed() >= Duration::from_millis(100),
        "missing advertised selection must be refetched within a bounded budget"
    );
    assert!(started.elapsed() < Duration::from_secs(2));
    assert_eq!(
        client.config_options().await[0]["currentValue"],
        "fixture/fake"
    );
    client.shutdown().await.unwrap();
}

#[tokio::test]
async fn native_command_requires_verified_acknowledgement_and_new_durable_terminal() {
    for behavior in ["empty-ack", "wrong-ack", "rejected"] {
        let root = tempfile::tempdir().unwrap();
        let mut config = options(root.path());
        config.timeout = Duration::from_millis(300);
        config
            .env
            .insert("NATIVE_FIXTURE_COMMAND_BEHAVIOR".into(), behavior.into());
        let client = Client::connect(Transport::OpenCodeNative, config, vec![])
            .await
            .unwrap();
        client.new_session().await.unwrap();
        let result = client.prompt("/fixture arguments", vec![]).await;
        assert!(
            result.is_err(),
            "{behavior} must not fabricate command completion: {result:?}"
        );
        assert_ne!(
            client.prompt_observation().await.1.as_deref(),
            Some("end_turn")
        );
        client.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn native_failed_execution_delivers_durable_partial_reply_and_terminal_error() {
    let root = tempfile::tempdir().unwrap();
    std::fs::write(root.path().join("release"), "").unwrap();
    let mut options = options(root.path());
    options
        .env
        .insert("NATIVE_FIXTURE_FAIL_AFTER_REPLY".into(), "1".into());
    options
        .env
        .insert("NATIVE_FIXTURE_DISCONNECT".into(), "1".into());
    let cfg = config::load(&json!({"rateLimitSeconds":0,"sessionStorePath":root.path().join("store.json"),"matrix":{"respondToAllMessages":true},"acp":{"command":options.command,"args":options.args,"transport":"native","env":options.env}}).to_string(), &BTreeMap::new()).unwrap();
    let bridge = Bridge::new(cfg, BTreeMap::new(), root.path().join("sessions")).unwrap();
    let sink = Recorder::default();
    let result = bridge.handle(event("partial-failure"), &sink).await;
    assert!(
        result.is_err(),
        "a terminal failure must not become a successful response"
    );
    let events = sink.0.lock().await;
    assert!(
        events.iter().any(|item| item["type"] == "response"
            && item["text"]
                .as_str()
                .is_some_and(|text| text.contains("reply:partial-failure"))),
        "durable partial output must reach the runtime sink: {events:?}"
    );
    assert!(
        events.iter().any(|item| item["type"] == "error"
            && item["message"]
                .as_str()
                .is_some_and(|text| text.contains("failed"))),
        "terminal error must also be visible: {events:?}"
    );
    assert!(events.iter().any(|item| item["type"] == "prompt_end"
        && item["failed"] == true
        && item["stopReason"] == "failed"
        && item["deliveredChars"].as_u64().unwrap_or(0) > 0));
    drop(events);
    bridge.shutdown().await.unwrap();
}

#[tokio::test]
async fn native_catalog_refetches_requested_model_before_switching() {
    let root = tempfile::tempdir().unwrap();
    let mut config = options(root.path());
    config
        .env
        .insert("NATIVE_FIXTURE_LATE_ALT_MODEL".into(), "1".into());
    let client = Client::connect(Transport::OpenCodeNative, config, vec![])
        .await
        .unwrap();
    client.new_session().await.unwrap();
    client.set_option("model", "fixture/alt").await.unwrap();
    assert_eq!(
        client.config_options().await[0]["currentValue"],
        "fixture/alt"
    );
    client.shutdown().await.unwrap();
}

#[tokio::test]
async fn native_session_pagination_rejects_malformed_and_repeated_cursors() {
    for behavior in ["malformed", "repeated"] {
        let root = tempfile::tempdir().unwrap();
        let mut config = options(root.path());
        config
            .env
            .insert("NATIVE_FIXTURE_BAD_CURSOR".into(), behavior.into());
        let client = Client::connect(Transport::OpenCodeNative, config, vec![])
            .await
            .unwrap();
        client.new_session().await.unwrap();
        assert!(
            client.list_sessions().await.is_err(),
            "{behavior} cursor must not silently truncate the native session picker"
        );
        client.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn native_history_pagination_rejects_malformed_cursor_before_binding() {
    let root = tempfile::tempdir().unwrap();
    let mut config = options(root.path());
    config.env.insert(
        "NATIVE_FIXTURE_BAD_HISTORY_CURSOR".into(),
        "malformed".into(),
    );
    let client = Client::connect(Transport::OpenCodeNative, config, vec![])
        .await
        .unwrap();
    let session = client.new_session().await.unwrap();
    client.close_session().await.unwrap();
    assert!(
        tokio::time::timeout(Duration::from_secs(2), client.load_session(&session, true))
            .await
            .expect("history enumeration must bound request count, not just total bytes")
            .is_err()
    );
    assert!(client.session_id().await.is_none());
    client.shutdown().await.unwrap();
}

#[tokio::test]
async fn native_history_pagination_limit_is_failure_not_truncated_success() {
    let root = tempfile::tempdir().unwrap();
    let mut config = options(root.path());
    config
        .env
        .insert("NATIVE_FIXTURE_BAD_HISTORY_CURSOR".into(), "limit".into());
    let client = Client::connect(Transport::OpenCodeNative, config, vec![])
        .await
        .unwrap();
    let session = client.new_session().await.unwrap();
    client.close_session().await.unwrap();
    assert!(
        tokio::time::timeout(Duration::from_secs(2), client.load_session(&session, true))
            .await
            .expect("history enumeration must bound request count, not just total bytes")
            .is_err()
    );
    assert!(client.session_id().await.is_none());
    client.shutdown().await.unwrap();
}

#[tokio::test]
async fn native_replay_window_overflow_recovers_durably_without_reapplying_text() {
    let root = tempfile::tempdir().unwrap();
    let mut config = options(root.path());
    config
        .env
        .insert("NATIVE_FIXTURE_REPLAY_OVERFLOW".into(), "1".into());
    let client = Client::connect(Transport::OpenCodeNative, config, vec![])
        .await
        .unwrap();
    client.new_session().await.unwrap();
    let mut receiver = client.subscribe();
    let pending_client = client.clone();
    let pending = tokio::spawn(async move { pending_client.prompt("overflow", vec![]).await });
    tokio::time::timeout(Duration::from_secs(2), async {
        while !client.prompt_observation().await.0.output_lost {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("busy collector must signal the exhausted replay window before release");
    std::fs::write(root.path().join("release"), "").unwrap();
    let response = pending.await.unwrap().unwrap();
    assert_eq!(response.text, "reply:overflow");
    assert!(
        response.output.output_lost,
        "overflow must be visible even after durable recovery"
    );
    assert!(response.output.collector_closed);
    let mut replayed = 0;
    while let Ok(update) = receiver.try_recv() {
        if update["content"]["text"] == "window-text" {
            replayed += 1;
        }
    }
    assert!(
        replayed <= 1,
        "old non-durable text was reapplied after overflow"
    );
    client.shutdown().await.unwrap();
}

#[tokio::test]
async fn native_failed_prompt_error_exposes_partial_payload_without_success() {
    let root = tempfile::tempdir().unwrap();
    std::fs::write(root.path().join("release"), "").unwrap();
    let mut config = options(root.path());
    config
        .env
        .insert("NATIVE_FIXTURE_FAIL_AFTER_REPLY".into(), "1".into());
    config
        .env
        .insert("NATIVE_FIXTURE_DISCONNECT".into(), "1".into());
    let client = Client::connect(Transport::OpenCodeNative, config, vec![])
        .await
        .unwrap();
    client.new_session().await.unwrap();
    let error = client.prompt("partial-failure", vec![]).await.unwrap_err();
    let failure = error
        .downcast_ref::<crate::native_client::PromptFailure>()
        .expect("native failure must carry its partial result across the client API boundary");
    assert_eq!(failure.text, "reply:partial-failure");
    assert_eq!(failure.stop_reason.as_deref(), Some("failed"));
    assert!(failure.output.collector_closed);
    assert!(failure.message.contains("failed"));
    client.shutdown().await.unwrap();
}

#[tokio::test]
async fn native_creation_waits_for_initial_default_model_settlement() {
    let root = tempfile::tempdir().unwrap();
    let mut config = options(root.path());
    config
        .env
        .insert("NATIVE_FIXTURE_LATE_CATALOG".into(), "1".into());
    let client = Client::connect(Transport::OpenCodeNative, config, vec![])
        .await
        .unwrap();
    assert!(client.new_session().await.unwrap().starts_with("ses_"));
    assert_eq!(
        client.config_options().await[0]["currentValue"],
        "fixture/fake"
    );
    client.shutdown().await.unwrap();
}

#[tokio::test]
async fn native_session_picker_follows_durable_pagination() {
    let root = tempfile::tempdir().unwrap();
    let mut config = options(root.path());
    config
        .env
        .insert("NATIVE_FIXTURE_PAGED_LIST".into(), "1".into());
    let client = Client::connect(Transport::OpenCodeNative, config, vec![])
        .await
        .unwrap();
    client.new_session().await.unwrap();
    let sessions = client.list_sessions().await.unwrap();
    assert_eq!(sessions.len(), 2);
    assert_eq!(sessions[1]["sessionId"], "ses_saved");
    client.shutdown().await.unwrap();
}

#[tokio::test]
async fn native_compaction_uses_native_control_not_an_ordinary_prompt() {
    let root = tempfile::tempdir().unwrap();
    std::fs::write(root.path().join("release"), "").unwrap();
    let client = Client::connect(Transport::OpenCodeNative, options(root.path()), vec![])
        .await
        .unwrap();
    client.new_session().await.unwrap();
    let response = client.prompt("/compact", vec![]).await.unwrap();
    assert_eq!(response.stop_reason.as_deref(), Some("end_turn"));
    client.shutdown().await.unwrap();
    let snapshot: Value =
        serde_json::from_slice(&std::fs::read(root.path().join("native-state.json")).unwrap())
            .unwrap();
    let session = snapshot["sessions"]
        .as_object()
        .unwrap()
        .values()
        .next()
        .unwrap();
    assert_eq!(
        session["compact"], true,
        "must not falsely report model output as compaction"
    );
    assert_eq!(session["submissions"], 0);
}

#[tokio::test]
async fn native_terminal_snapshot_validates_tool_media_before_completion() {
    let root = tempfile::tempdir().unwrap();
    std::fs::write(root.path().join("release"), "").unwrap();
    let mut config = options(root.path());
    config
        .env
        .insert("NATIVE_FIXTURE_LATE_TERMINAL_MEDIA".into(), "1".into());
    config
        .env
        .insert("NATIVE_FIXTURE_DISCONNECT".into(), "1".into());
    let client = Client::connect(Transport::OpenCodeNative, config, vec![])
        .await
        .unwrap();
    client.new_session().await.unwrap();
    let response = client.prompt("late-media-output", vec![]).await;
    assert!(
        response.is_err(),
        "terminal snapshot media must not bypass collector safety validation"
    );
    let (output, _, _) = client.prompt_observation().await;
    assert!(output.output_lost);
    assert!(output.collector_closed);
    client.shutdown().await.unwrap();
}

#[tokio::test]
async fn native_tool_media_is_validated_collected_and_bounded() {
    use base64::Engine;
    let mut png = std::io::Cursor::new(Vec::new());
    image::DynamicImage::new_rgb8(1, 1)
        .write_to(&mut png, image::ImageFormat::Png)
        .unwrap();
    let part = json!({"type":"file","mime":"image/png","uri":format!("data:image/png;base64,{}",base64::engine::general_purpose::STANDARD.encode(png.into_inner()))});
    for (content, succeeds) in [
        (vec![part.clone()], true),
        (vec![part; 17], false),
        (
            vec![json!({"type":"file","mime":"image/png","uri":"https://example.invalid/private"})],
            false,
        ),
    ] {
        let root = tempfile::tempdir().unwrap();
        std::fs::write(root.path().join("release"), "").unwrap();
        let mut config = options(root.path());
        config.env.insert(
            "NATIVE_FIXTURE_MEDIA_CONTENT".into(),
            serde_json::to_string(&content).unwrap(),
        );
        let client = Client::connect(Transport::OpenCodeNative, config, vec![])
            .await
            .unwrap();
        client.new_session().await.unwrap();
        let response = client.prompt("media-output", vec![]).await;
        if succeeds {
            assert_eq!(response.unwrap().output.image_count, 1);
            assert_eq!(client.inline_images().await.len(), 1);
        } else {
            assert!(
                response.is_err(),
                "unsafe or excessive media must be visible failure"
            );
        }
        client.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn native_cancel_still_interrupts_when_permission_listing_fails() {
    let root = tempfile::tempdir().unwrap();
    let mut config = options(root.path());
    config
        .env
        .insert("NATIVE_FIXTURE_FAIL_CANCEL".into(), "1".into());
    let client = Client::connect(Transport::OpenCodeNative, config, vec![])
        .await
        .unwrap();
    client.new_session().await.unwrap();
    assert!(client.cancel().await.is_err());
    let snapshot: Value =
        serde_json::from_slice(&std::fs::read(root.path().join("native-state.json")).unwrap())
            .unwrap();
    assert_eq!(
        snapshot["sessions"]
            .as_object()
            .unwrap()
            .values()
            .next()
            .unwrap()["cancelled"],
        true
    );
    assert!(client.shutdown().await.is_err());
    assert!(root.path().join("exited").exists());
}

#[tokio::test]
async fn native_malformed_history_is_not_bound_or_reported_as_empty_success() {
    let root = tempfile::tempdir().unwrap();
    let mut config = options(root.path());
    config
        .env
        .insert("NATIVE_FIXTURE_BAD_HISTORY".into(), "1".into());
    let client = Client::connect(Transport::OpenCodeNative, config, vec![])
        .await
        .unwrap();
    let session = client.new_session().await.unwrap();
    client.close_session().await.unwrap();
    assert!(client.load_session(&session, true).await.is_err());
    assert!(client.session_id().await.is_none());
    client.shutdown().await.unwrap();
}

#[tokio::test]
async fn native_cancel_reconciles_queue_delete_race_then_interrupts() {
    let root = tempfile::tempdir().unwrap();
    let mut config = options(root.path());
    config
        .env
        .insert("NATIVE_FIXTURE_CANCEL_RACE".into(), "1".into());
    let client = Client::connect(Transport::OpenCodeNative, config, vec![])
        .await
        .unwrap();
    client.new_session().await.unwrap();
    let first = client.clone();
    let pending = tokio::spawn(async move { first.prompt("first", vec![]).await });
    tokio::time::timeout(Duration::from_secs(5), async {
        while !root.path().join("started").exists() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    client.forward("second").await.unwrap();
    client.cancel().await.unwrap();
    assert_eq!(
        pending.await.unwrap().unwrap().stop_reason.as_deref(),
        Some("cancelled")
    );
    client.shutdown().await.unwrap();
}

#[tokio::test]
async fn native_default_model_catalog_info_is_converted_to_session_ref() {
    let root = tempfile::tempdir().unwrap();
    let mut config = options(root.path());
    config
        .env
        .insert("NATIVE_FIXTURE_DEFAULT_MODEL".into(), "1".into());
    let client = Client::connect(Transport::OpenCodeNative, config, vec![])
        .await
        .unwrap();
    client.new_session().await.unwrap();
    client.set_option("effort", "high").await.unwrap();
    client.shutdown().await.unwrap();
}

#[tokio::test]
async fn native_shutdown_reaps_lease_even_when_cancellation_api_fails() {
    let root = tempfile::tempdir().unwrap();
    let mut config = options(root.path());
    config
        .env
        .insert("NATIVE_FIXTURE_FAIL_CANCEL".into(), "1".into());
    let client = Client::connect(Transport::OpenCodeNative, config, vec![])
        .await
        .unwrap();
    client.new_session().await.unwrap();
    assert!(
        client.shutdown().await.is_err(),
        "cancellation failure must stay visible"
    );
    assert!(
        root.path().join("exited").exists(),
        "owned lease must still close and reap"
    );
    assert!(client.session_id().await.is_none());
    assert!(client.new_session().await.is_err());
}

#[tokio::test]
async fn native_resume_refuses_foreign_running_execution_without_binding() {
    let root = tempfile::tempdir().unwrap();
    let client = Client::connect(Transport::OpenCodeNative, options(root.path()), vec![])
        .await
        .unwrap();
    let session = client.new_session().await.unwrap();
    let first = client.clone();
    let pending = tokio::spawn(async move { first.prompt("first", vec![]).await });
    tokio::time::timeout(Duration::from_secs(5), async {
        while !root.path().join("started").exists() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    let resumed = Client::connect(Transport::OpenCodeNative, options(root.path()), vec![])
        .await
        .unwrap();
    assert!(resumed.load_session(&session, true).await.is_err());
    assert!(resumed.session_id().await.is_none());
    resumed.shutdown().await.unwrap();
    client.cancel().await.unwrap();
    pending.await.unwrap().unwrap();
    client.shutdown().await.unwrap();
}

#[tokio::test]
async fn native_queue_keeps_one_collector_through_both_replies_and_reloads_history() {
    let root = tempfile::tempdir().unwrap();
    let client = Client::connect(Transport::OpenCodeNative, options(root.path()), vec![])
        .await
        .unwrap();
    let session = client.new_session().await.unwrap();
    let first = client.clone();
    let pending = tokio::spawn(async move { first.prompt("first", vec![]).await });
    tokio::time::timeout(Duration::from_secs(5), async {
        while !root.path().join("started").exists() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    assert!(client.accepts_followups());
    client.forward("second").await.unwrap();
    assert!(!pending.is_finished(), "admission is not completion");
    std::fs::write(root.path().join("release"), "").unwrap();
    let response = tokio::time::timeout(Duration::from_secs(5), pending)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(response.text, "reply:firstreply:second");
    assert!(response.output.collector_closed);
    assert!(!client.accepts_followups());
    client.shutdown().await.unwrap();
    let resumed = Client::connect(Transport::OpenCodeNative, options(root.path()), vec![])
        .await
        .unwrap();
    let history = resumed.load_session(&session, true).await.unwrap();
    assert_eq!(
        history
            .iter()
            .map(|item| item["content"]["text"].as_str().unwrap())
            .collect::<Vec<_>>(),
        ["first", "reply:first", "second", "reply:second"]
    );
    resumed.delete_session().await.unwrap();
    assert!(resumed.load_session(&session, true).await.is_err());
    resumed.shutdown().await.unwrap();
}

#[derive(Clone, Default)]
struct Recorder(Arc<Mutex<Vec<Value>>>);
#[async_trait]
impl Sink for Recorder {
    fn owned(&self) -> Option<Arc<dyn Sink>> {
        Some(Arc::new(self.clone()))
    }
    async fn emit(&self, value: Value) -> anyhow::Result<()> {
        self.0.lock().await.push(value);
        Ok(())
    }
}
fn event(text: &str) -> Event {
    let mut event = Event::web("native", text);
    event.platform = "matrix".into();
    event.room = "!fixture:localhost".into();
    event.thread = "$thread".into();
    event.user = "@fixture:localhost".into();
    event.id = format!("$fixture-{text}");
    event
}

#[tokio::test]
async fn native_bridge_expiry_reaps_owned_server_and_preserves_independent_history_and_workspace() {
    let root = tempfile::tempdir().unwrap();
    std::fs::write(root.path().join("release"), "").unwrap();
    let options = options(root.path());
    let cfg = config::load(&json!({
        "rateLimitSeconds":0,"sessionStorePath":root.path().join("store.json"),
        "acp":{"command":options.command,"args":options.args,"transport":"native","env":options.env}
    }).to_string(), &BTreeMap::new()).unwrap();
    let bridge = Bridge::new(cfg, BTreeMap::new(), root.path().join("sessions")).unwrap();
    let sink = Recorder::default();
    bridge
        .handle(Event::web("expiry", "first"), &sink)
        .await
        .unwrap();
    let store = crate::store::Store::new(root.path().join("store.json"));
    let original = store.get("web", "expiry", "opencode").unwrap().unwrap();
    let artifact = std::path::Path::new(&original.cwd).join("user-artifact.txt");
    std::fs::write(&artifact, "preserve me").unwrap();
    let mut independent = original.clone();
    independent.backend = "codex".into();
    independent.session_id = "independent-history".into();
    store.set(independent.clone(), false).unwrap();
    assert_eq!(bridge.expire(Duration::from_nanos(1)).await.unwrap(), 1);
    assert!(root.path().join("exited").exists());
    assert!(store.get("web", "expiry", "opencode").unwrap().is_none());
    assert_eq!(
        store.get("web", "expiry", "codex").unwrap(),
        Some(independent)
    );
    assert_eq!(std::fs::read_to_string(artifact).unwrap(), "preserve me");
    bridge
        .handle(Event::web("expiry", "next"), &sink)
        .await
        .unwrap();
    assert_ne!(
        store
            .get("web", "expiry", "opencode")
            .unwrap()
            .unwrap()
            .session_id,
        original.session_id
    );
    bridge.shutdown().await.unwrap();
}

#[tokio::test]
async fn native_matrix_question_answer_is_bound_to_requester_and_raw_thread() {
    fn room_event(text: &str) -> Event {
        let mut value = event(text);
        value.direct = false;
        value
    }
    let root = tempfile::tempdir().unwrap();
    std::fs::write(root.path().join("release"), "").unwrap();
    let options = options(root.path());
    let cfg = config::load(&json!({
        "rateLimitSeconds":0,"sessionStorePath":root.path().join("store.json"),
        "acp":{"command":options.command,"args":options.args,"transport":"native","env":options.env},
        "matrix":{"interactivePermissions":true,"respondToAllMessages":false,"threadIsolation":false,
            "allowedUsers":["@fixture:localhost","@other:localhost"],"allowedRooms":["!fixture:localhost"],"strictAllowedRooms":true}
    }).to_string(), &BTreeMap::new()).unwrap();
    let bridge = Arc::new(Bridge::new(cfg, BTreeMap::new(), root.path().join("sessions")).unwrap());
    let sink = Recorder::default();
    let task = {
        let bridge = bridge.clone();
        let sink = sink.clone();
        tokio::spawn(async move { bridge.handle(room_event("!oc form"), &sink).await })
    };
    let notice = tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if let Some(text) = sink
                .0
                .lock()
                .await
                .iter()
                .filter_map(|item| item["text"].as_str())
                .find(|t| t.contains("/answer "))
                .map(str::to_owned)
            {
                break text;
            }
            assert!(
                !task.is_finished(),
                "Matrix must display the native question before completing"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    let token = notice
        .split("/answer ")
        .nth(1)
        .unwrap()
        .split_whitespace()
        .next()
        .unwrap();
    let answer = format!("/answer {token} {{\"answer\":\"yes\"}}");
    let mut foreign = room_event(&answer);
    foreign.user = "@other:localhost".into();
    foreign.id = "$foreign-answer".into();
    bridge.handle(foreign, &sink).await.unwrap();
    let mut wrong_thread = room_event(&answer);
    wrong_thread.thread = "$other-thread".into();
    wrong_thread.id = "$wrong-thread-answer".into();
    bridge.handle(wrong_thread, &sink).await.unwrap();
    assert!(!task.is_finished());
    bridge
        .handle(
            room_event(&format!("/answer {token} {{\"wrong_key\":\"yes\"}}")),
            &sink,
        )
        .await
        .unwrap();
    assert!(!task.is_finished());
    bridge.handle(room_event(&answer), &sink).await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let mut duplicate = room_event(&answer);
    duplicate.id = "$duplicate-answer".into();
    bridge.handle(duplicate, &sink).await.unwrap();
    assert!(
        sink.0.lock().await.iter().any(|item| item["text"]
            .as_str()
            .is_some_and(|text| text.contains("OpenCode confirmed the question answer"))),
        "Question answers need a verified backend acknowledgement"
    );
    bridge.shutdown().await.unwrap();
    let state: Value =
        serde_json::from_slice(&std::fs::read(root.path().join("native-state.json")).unwrap())
            .unwrap();
    let session = state["sessions"]
        .as_object()
        .unwrap()
        .values()
        .next()
        .unwrap();
    assert_eq!(session["formAnswers"], json!({"answer":"yes"}));
    assert_eq!(session["formReplyCount"], 1);
    assert_eq!(
        session["submissions"], 1,
        "answers must not enter native prompt queue"
    );
}

#[tokio::test]
async fn native_matrix_broker_approves_once_only_from_originating_user() {
    let root = tempfile::tempdir().unwrap();
    std::fs::write(root.path().join("release"), "").unwrap();
    let config = options(root.path());
    let cfg = config::load(&json!({
        "rateLimitSeconds":0,"sessionStorePath":root.path().join("store.json"),
        "acp":{"command":config.command,"args":config.args,"transport":"native","env":config.env},
        "matrix":{"interactivePermissions":true,"respondToAllMessages":true,"threadIsolation":false,
            "allowedUsers":["@fixture:localhost","@other:localhost"],"allowedRooms":["!fixture:localhost"],"strictAllowedRooms":true}
    }).to_string(), &BTreeMap::new()).unwrap();
    let bridge = Arc::new(Bridge::new(cfg, BTreeMap::new(), root.path().join("sessions")).unwrap());
    let sink = Recorder::default();
    let first = bridge.clone();
    let recorder = sink.clone();
    let pending = tokio::spawn(async move { first.handle(event("permission"), &recorder).await });
    let notice = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let Some(notice) = sink
                .0
                .lock()
                .await
                .iter()
                .filter_map(|item| item["text"].as_str())
                .find(|text| text.contains("/approve "))
                .map(str::to_owned)
            {
                break notice;
            }
            assert!(
                !pending.is_finished(),
                "permission must wait for broker decision"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    assert!(notice.contains("printf fixture"));
    let id = notice
        .split("/approve ")
        .nth(1)
        .unwrap()
        .split_whitespace()
        .next()
        .unwrap();
    let mut foreign = event(&format!("/approve {id}"));
    foreign.id = "foreign-approval".into();
    foreign.user = "@other:localhost".into();
    bridge.handle(foreign, &sink).await.unwrap();
    assert!(!pending.is_finished());
    bridge
        .handle(event(&format!("/approve {id}")), &sink)
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), pending)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    bridge.shutdown().await.unwrap();
    let snapshot: Value =
        serde_json::from_slice(&std::fs::read(root.path().join("native-state.json")).unwrap())
            .unwrap();
    assert_eq!(
        snapshot["sessions"]
            .as_object()
            .unwrap()
            .values()
            .next()
            .unwrap()["decisions"],
        json!(["once"])
    );
}

#[tokio::test]
async fn explicit_native_backend_runs_matrix_busy_followup_without_acp() {
    let root = tempfile::tempdir().unwrap();
    let options = options(root.path());
    let cfg = config::load(&json!({
        "rateLimitSeconds":0, "sessionStorePath":root.path().join("store.json"),
        "matrix":{"respondToAllMessages":true},
        "acp":{"command":options.command, "args":options.args, "transport":"native", "env":options.env}
    }).to_string(), &BTreeMap::new()).unwrap();
    let bridge = Arc::new(Bridge::new(cfg, BTreeMap::new(), root.path().join("sessions")).unwrap());
    let sink = Recorder::default();
    let first_bridge = bridge.clone();
    let first_sink = sink.clone();
    let pending =
        tokio::spawn(async move { first_bridge.handle(event("first"), &first_sink).await });
    tokio::time::timeout(Duration::from_secs(5), async {
        while !root.path().join("started").exists() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    let mut second = event("second");
    use base64::Engine;
    let mut png = std::io::Cursor::new(Vec::new());
    image::DynamicImage::new_rgb8(1, 1)
        .write_to(&mut png, image::ImageFormat::Png)
        .unwrap();
    second.images.push(json!({"type":"image","mimeType":"image/png","data":base64::engine::general_purpose::STANDARD.encode(png.into_inner())}));
    bridge.handle(second, &sink).await.unwrap();
    assert!(!pending.is_finished());
    std::fs::write(root.path().join("release"), "").unwrap();
    tokio::time::timeout(Duration::from_secs(5), pending)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let events = sink.0.lock().await;
    assert!(
        events.iter().any(|item| item["type"] == "response"
            && item["text"]
                .as_str()
                .is_some_and(|text| text.contains("reply:first") && text.contains("reply:second"))),
        "{events:?}"
    );
    drop(events);
    bridge.shutdown().await.unwrap();
    let snapshot: Value =
        serde_json::from_slice(&std::fs::read(root.path().join("native-state.json")).unwrap())
            .unwrap();
    assert!(
        snapshot["sessions"]
            .as_object()
            .unwrap()
            .values()
            .any(|session| session["submittedFiles"]
                .as_array()
                .is_some_and(|files| !files.is_empty())),
        "busy image must not be silently stripped"
    );
}

#[test]
fn transport_selection_is_backend_scoped_not_model_scoped() {
    assert!(Transport::select("opencode", &json!({"transport":true})).is_err());
    assert!(Transport::select("opencode", &json!({"transport":null})).is_err());
    assert_eq!(
        Transport::select("opencode", &json!({"transport":"native"})).unwrap(),
        Transport::OpenCodeNative
    );
    assert_eq!(
        Transport::select("codex", &json!({"transport":"acp", "model":"opencode/foo"})).unwrap(),
        Transport::Acp
    );
    assert!(Transport::select("claude", &json!({"transport":"native"})).is_err());
    assert!(Transport::select("opencode", &json!({"transport":"typo"})).is_err());
}

#[tokio::test]
async fn native_controls_confirm_model_variant_and_agent_without_global_edits() {
    let root = tempfile::tempdir().unwrap();
    let client = Client::connect(Transport::OpenCodeNative, options(root.path()), vec![])
        .await
        .unwrap();
    client.new_session().await.unwrap();
    assert!(
        !client.config_options().await.is_empty(),
        "native catalogs must be advertised"
    );
    assert_eq!(
        client.set_option("model", "fixture/alt").await.unwrap()["currentValue"],
        "fixture/alt"
    );
    assert_eq!(
        client.set_option("effort", "high").await.unwrap()["currentValue"],
        "high"
    );
    client.set_mode("plan").await.unwrap();
    assert_eq!(client.modes().await["currentModeId"], "plan");
    assert!(client.set_option("model", "other/unknown").await.is_err());
    assert!(client.set_option("permission", "allow").await.is_err());
    assert!(client.set_mode("invented").await.is_err());
    client.shutdown().await.unwrap();
}

#[tokio::test]
async fn native_cancel_interrupts_running_and_removes_queued_input() {
    let root = tempfile::tempdir().unwrap();
    let client = Client::connect(Transport::OpenCodeNative, options(root.path()), vec![])
        .await
        .unwrap();
    client.new_session().await.unwrap();
    let first = client.clone();
    let pending = tokio::spawn(async move { first.prompt("first", vec![]).await });
    tokio::time::timeout(Duration::from_secs(5), async {
        while !root.path().join("started").exists() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    client.forward("second").await.unwrap();
    client.cancel().await.unwrap();
    let response = tokio::time::timeout(Duration::from_secs(5), pending)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(response.stop_reason.as_deref(), Some("cancelled"));
    let snapshot: Value =
        serde_json::from_slice(&std::fs::read(root.path().join("native-state.json")).unwrap())
            .unwrap();
    let session = snapshot["sessions"]
        .as_object()
        .unwrap()
        .values()
        .next()
        .unwrap();
    assert!(session["inbox"].as_array().unwrap().is_empty());
    assert_eq!(session["messages"].as_array().unwrap().len(), 1);
    client.shutdown().await.unwrap();
}

#[tokio::test]
async fn native_tools_and_approve_once_are_scoped_and_forms_fail_closed() {
    let root = tempfile::tempdir().unwrap();
    std::fs::write(root.path().join("release"), "").unwrap();
    let client = Client::connect(Transport::OpenCodeNative, options(root.path()), vec![])
        .await
        .unwrap();
    client.new_session().await.unwrap();
    let (mut requests, guard) = client.permission_handler();
    let first = client.clone();
    let pending = tokio::spawn(async move { first.prompt("permission", vec![]).await });
    let request = tokio::time::timeout(Duration::from_secs(3), requests.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        request.params["toolCall"]["rawInput"]["command"],
        "printf fixture"
    );
    request
        .reply
        .send(json!({"outcome":"selected","optionId":"native-once"}))
        .unwrap();
    let response = tokio::time::timeout(Duration::from_secs(5), pending)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(response.output.had_tool_activity);
    assert_eq!(response.output.tool_calls, 1);
    assert!(response.output.tool_results.contains("fixture tool output"));
    assert!(response.output.had_server_request);
    drop(guard);
    let form = client.prompt("form", vec![]).await.unwrap();
    assert!(form.output.had_server_request);
    client.shutdown().await.unwrap();
    let snapshot: Value =
        serde_json::from_slice(&std::fs::read(root.path().join("native-state.json")).unwrap())
            .unwrap();
    let session = snapshot["sessions"]
        .as_object()
        .unwrap()
        .values()
        .next()
        .unwrap();
    assert_eq!(session["decisions"][0], "once");
    assert_eq!(session["formCancelled"], true);
}

#[tokio::test]
async fn native_media_and_advertised_command_use_verified_payloads() {
    use base64::Engine;
    let root = tempfile::tempdir().unwrap();
    std::fs::write(root.path().join("release"), "").unwrap();
    let client = Client::connect(Transport::OpenCodeNative, options(root.path()), vec![])
        .await
        .unwrap();
    client.new_session().await.unwrap();
    let mut png = std::io::Cursor::new(Vec::new());
    image::DynamicImage::new_rgb8(1, 1)
        .write_to(&mut png, image::ImageFormat::Png)
        .unwrap();
    let content = json!({"type":"image","mimeType":"image/png","data":base64::engine::general_purpose::STANDARD.encode(png.into_inner())});
    assert_eq!(
        client.prompt("image", vec![content]).await.unwrap().text,
        "reply:image"
    );
    assert!(
        client
            .prompt(
                "invalid",
                vec![json!({"type":"resource","uri":"https://example.com/private"})]
            )
            .await
            .is_err()
    );
    assert_eq!(client.commands().await[0]["name"], "fixture");
    assert_eq!(
        client
            .prompt("/fixture argument", vec![])
            .await
            .unwrap()
            .text,
        "reply:argument"
    );
    client.shutdown().await.unwrap();
    let snapshot: Value =
        serde_json::from_slice(&std::fs::read(root.path().join("native-state.json")).unwrap())
            .unwrap();
    let session = snapshot["sessions"]
        .as_object()
        .unwrap()
        .values()
        .next()
        .unwrap();
    assert!(
        session["submittedFiles"][0]["uri"]
            .as_str()
            .unwrap()
            .starts_with("data:image/png;base64,")
    );
    assert_eq!(session["command"], "fixture");
}

#[tokio::test]
async fn native_disconnect_recovers_durable_reply_and_uncertain_ack_is_not_replayed() {
    let root = tempfile::tempdir().unwrap();
    let mut config = options(root.path());
    config
        .env
        .insert("NATIVE_FIXTURE_DISCONNECT".into(), "1".into());
    config
        .env
        .insert("NATIVE_FIXTURE_UNCERTAIN_ACK".into(), "1".into());
    let client = Client::connect(Transport::OpenCodeNative, config, vec![])
        .await
        .unwrap();
    client.new_session().await.unwrap();
    let first = client.clone();
    let pending = tokio::spawn(async move { first.prompt("first", vec![]).await });
    tokio::time::timeout(Duration::from_secs(3), async {
        while !client.prompt_observation().await.0.output_lost {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    std::fs::write(root.path().join("release"), "").unwrap();
    let response = tokio::time::timeout(Duration::from_secs(5), pending)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(response.text, "reply:first");
    assert!(response.output.output_lost);
    client.shutdown().await.unwrap();
    let snapshot: Value =
        serde_json::from_slice(&std::fs::read(root.path().join("native-state.json")).unwrap())
            .unwrap();
    let session = snapshot["sessions"]
        .as_object()
        .unwrap()
        .values()
        .next()
        .unwrap();
    assert_eq!(
        session["submissions"], 1,
        "an uncertain POST must not cause duplicate work"
    );
}

#[tokio::test]
async fn native_mcp_registration_is_owned_runtime_scoped_and_confirmed() {
    let root = tempfile::tempdir().unwrap();
    let servers = vec![
        json!({"name":"fixture","command":"fixture-command","args":["--stdio"],"env":[{"name":"FIXTURE","value":"synthetic"}]}),
    ];
    let client = Client::connect(Transport::OpenCodeNative, options(root.path()), servers)
        .await
        .unwrap();
    client.new_session().await.unwrap();
    client.shutdown().await.unwrap();
    let state: Value =
        serde_json::from_slice(&std::fs::read(root.path().join("native-state.json")).unwrap())
            .unwrap();
    assert_eq!(
        state["mcp"]["fixture"]["command"],
        json!(["fixture-command", "--stdio"])
    );
    assert_eq!(
        state["mcp"]["fixture"]["environment"]["FIXTURE"],
        "synthetic"
    );
}

#[tokio::test]
async fn native_cancel_invalidates_permission_and_rejects_late_approval_and_followup() {
    let root = tempfile::tempdir().unwrap();
    let client = Client::connect(Transport::OpenCodeNative, options(root.path()), vec![])
        .await
        .unwrap();
    client.new_session().await.unwrap();
    let (mut requests, _guard) = client.permission_handler();
    let first = client.clone();
    let pending = tokio::spawn(async move { first.prompt("stale-permission", vec![]).await });
    let request = tokio::time::timeout(Duration::from_secs(3), requests.recv())
        .await
        .unwrap()
        .unwrap();
    client.cancel().await.unwrap();
    assert!(!request.current.load(std::sync::atomic::Ordering::SeqCst));
    let _ = request
        .reply
        .send(json!({"outcome":"selected","optionId":"native-once"}));
    assert!(client.forward("late").await.is_err());
    tokio::time::timeout(Duration::from_secs(5), pending)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    client.shutdown().await.unwrap();
    let state: Value =
        serde_json::from_slice(&std::fs::read(root.path().join("native-state.json")).unwrap())
            .unwrap();
    let session = state["sessions"]
        .as_object()
        .unwrap()
        .values()
        .next()
        .unwrap();
    assert!(
        !session["decisions"]
            .as_array()
            .unwrap()
            .contains(&json!("once"))
    );
}
