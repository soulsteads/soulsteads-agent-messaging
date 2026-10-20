use async_trait::async_trait;
use soulsteads_messaging::{
    config,
    runtime::{Bridge, Event, Sink},
};
use serde_json::{Value, json};
use std::{collections::BTreeMap, path::Path, sync::Arc, time::Duration};
use tokio::sync::Mutex;

#[derive(Clone, Default)]
struct Recorder(Arc<Mutex<Vec<Value>>>);
#[async_trait]
impl Sink for Recorder {
    async fn emit(&self, event: Value) -> anyhow::Result<()> {
        self.0.lock().await.push(event);
        Ok(())
    }
}
fn event(text: &str) -> Event {
    Event {
        platform: "matrix".into(),
        room: "!room:local".into(),
        thread: "$root".into(),
        id: uuid::Uuid::new_v4().to_string(),
        user: "@user:local".into(),
        text: text.into(),
        direct: false,
        mentioned: false,
        bot: false,
        images: vec![],
        reply_to: None,
    }
}
fn configuration(directory: &Path, script: Value, extra: Value) -> Value {
    std::fs::write(directory.join("script.json"), script.to_string()).unwrap();
    let mut env = json!({"ACP_FIXTURE_MATRIX_SCRIPT":directory.join("script.json"),"ACP_FIXTURE_MATRIX_COUNTER":directory.join("counter"),"ACP_FIXTURE_MATRIX_AUDIT":directory.join("audit.jsonl")});
    env.as_object_mut()
        .unwrap()
        .extend(extra.as_object().unwrap().clone());
    config::load(&json!({"rateLimitSeconds":0,"sessionStorePath":directory.join("store.json"),"acp":{"command":env!("CARGO_BIN_EXE_acp-fixture"),"args":[],"env":env},"matrix":{"respondToAllMessages":true},"toolMessages":{"mode":"off"}}).to_string(), &BTreeMap::new()).unwrap()
}
fn audit(directory: &Path) -> Vec<Value> {
    std::fs::read_to_string(directory.join("audit.jsonl"))
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect()
}

#[tokio::test]
async fn failed_cancel_write_preserves_active_result_and_reports_failure() {
    let directory = tempfile::tempdir().unwrap();
    let started = directory.path().join("started");
    let release = directory.path().join("release");
    let config = configuration(
        directory.path(),
        json!([{"closeInput":true,"startedFile":started,"releaseFile":release,"text":"original answer","stopReason":"end_turn"}]),
        json!({}),
    );
    let bridge =
        Arc::new(Bridge::new(config, BTreeMap::new(), directory.path().join("sessions")).unwrap());
    let sink = Recorder::default();
    let original = {
        let bridge = bridge.clone();
        let sink = sink.clone();
        tokio::spawn(async move { bridge.handle(event("hello"), &sink).await })
    };
    tokio::time::timeout(Duration::from_secs(3), async {
        while !started.exists() {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap();
    let (first, second) = tokio::join!(
        bridge.handle(event("/cancel"), &sink),
        bridge.handle(event("/cancel"), &sink)
    );
    std::fs::write(release, "release").unwrap();
    original.await.unwrap().unwrap();
    let output = sink.0.lock().await;
    assert!(
        output
            .iter()
            .any(|event| event["type"] == "response" && event["text"] == "original answer"),
        "{output:?}; cancel: {first:?}, {second:?}"
    );
    assert_eq!(
        output
            .iter()
            .filter(|event| event["text"]
                .as_str()
                .is_some_and(|text| text.contains("Cancellation failed")))
            .count(),
        2,
        "{output:?}"
    );
    assert!(
        !output
            .iter()
            .any(|event| event["text"] == "Request cancelled."),
        "{output:?}"
    );
    drop(output);
    bridge.shutdown().await.unwrap();
}

#[tokio::test]
async fn overlapping_successful_cancels_dispatch_once_and_do_not_cancel_the_next_turn() {
    let directory = tempfile::tempdir().unwrap();
    let started = directory.path().join("started");
    let config = configuration(
        directory.path(),
        json!([{"waitForFollowup":true},{"text":"next answer","stopReason":"end_turn"}]),
        json!({"ACP_FIXTURE_PROMPT_STARTED_FILE":started}),
    );
    let bridge =
        Arc::new(Bridge::new(config, BTreeMap::new(), directory.path().join("sessions")).unwrap());
    let sink = Recorder::default();
    let original = {
        let bridge = bridge.clone();
        let sink = sink.clone();
        tokio::spawn(async move { bridge.handle(event("hello"), &sink).await })
    };
    tokio::time::timeout(Duration::from_secs(3), async {
        while !started.exists() {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap();
    let (first, second) = tokio::join!(
        bridge.handle(event("/cancel"), &sink),
        bridge.handle(event("/cancel"), &sink)
    );
    first.unwrap();
    second.unwrap();
    original.await.unwrap().unwrap();
    assert!(sink.0.lock().await.iter().any(|value| {
        value["text"]
            .as_str()
            .is_some_and(|text| text.contains("already pending"))
    }));
    assert_eq!(
        audit(directory.path())
            .iter()
            .filter(|value| value["method"] == "session/cancel")
            .count(),
        1
    );
    bridge.handle(event("next turn"), &sink).await.unwrap();
    assert!(
        sink.0
            .lock()
            .await
            .iter()
            .any(|value| value["type"] == "response" && value["text"] == "next answer")
    );
    bridge.handle(event("/cancel"), &sink).await.unwrap();
    assert_eq!(
        audit(directory.path())
            .iter()
            .filter(|value| value["method"] == "session/cancel")
            .count(),
        1
    );
    bridge.shutdown().await.unwrap();
}

#[tokio::test]
async fn matrix_source_controller_menu_options_aliases_and_command_pages() {
    let directory = tempfile::tempdir().unwrap();
    let options = json!([
        {"id":"backend-model","category":"model","currentValue":"one","options":[{"options":[{"value":"one","name":"First"},{"value":"two","name":"Second"}]}]},
        {"id":"reasoning","category":"thought_level","currentValue":"low","options":[{"value":"low"},{"value":"high"}]},
        {"id":"backend-mode","category":"mode","currentValue":"build","options":[{"value":"build"},{"value":"plan"},{"value":"danger-full-access"}]}
    ]);
    let commands: Vec<Value> = (0..65)
        .map(|index| json!({"name":format!("skill-{index}"),"description":"x".repeat(5000)}))
        .collect();
    let commands_file = directory.path().join("commands.json");
    std::fs::write(&commands_file, json!(commands).to_string()).unwrap();
    let config = configuration(
        directory.path(),
        json!([]),
        json!({"ACP_FIXTURE_CONFIG_OPTIONS":options.to_string(),"ACP_FIXTURE_COMMANDS_FILE":commands_file}),
    );
    let bridge = Bridge::new(config, BTreeMap::new(), directory.path().join("sessions")).unwrap();
    for command in ["/help", "/h", "/commands"] {
        let sink = Recorder::default();
        bridge.handle(event(command), &sink).await.unwrap();
        let output = sink.0.lock().await;
        let menu = output
            .iter()
            .find_map(|value| value["text"].as_str())
            .unwrap();
        assert!(
            menu.starts_with("Matrix commands:\n- /backend"),
            "{command}: {output:?}"
        );
        assert_eq!(
            menu.lines().filter(|line| line.starts_with("- /")).count(),
            13
        );
        assert!(!menu.contains("skill-"));
    }
    for (command, expected) in [
        ("/backend status", "Backend: opencode"),
        ("/backend list", "configured: opencode"),
        ("/model status", "Current model: one"),
        ("/models", "two (Second)"),
        ("/effort list", "choices: low, high"),
        ("/mode list", "choices: build, plan, danger-full-access"),
        ("/agent plan", "mode selected: plan"),
        (
            "/mode danger-full-access",
            "mode selected: danger-full-access",
        ),
        ("/effort high", "effort selected: high"),
        ("/model two", "model selected: two"),
        ("/commands all", "page 1/3"),
        ("/commands 2", "/skill-30 -"),
        ("/commands all 3", "/skill-64 -"),
    ] {
        let sink = Recorder::default();
        bridge.handle(event(command), &sink).await.unwrap();
        let output = sink.0.lock().await;
        assert!(
            output.iter().any(|value| value["text"]
                .as_str()
                .is_some_and(|text| text.contains(expected) && text.len() < 8000)),
            "{command}: {output:?}"
        );
    }
    for command in [
        "/models extra",
        "/model two extra",
        "/effort high extra",
        "/mode plan extra",
        "/commands 4",
        "/commands all 0",
        "/backend two extra",
    ] {
        let sink = Recorder::default();
        assert!(
            bridge.handle(event(command), &sink).await.is_err(),
            "{command}"
        );
        assert!(
            sink.0.lock().await.iter().any(|value| value["text"]
                .as_str()
                .is_some_and(|text| text.contains("ACP control failed")
                    || text.contains("Backend switch failed"))),
            "{command}"
        );
    }
    assert!(
        !audit(directory.path())
            .iter()
            .any(|value| value["method"] == "session/prompt")
    );
    bridge.shutdown().await.unwrap();
}

#[tokio::test]
async fn matrix_failed_control_initialization_releases_admission() {
    let directory = tempfile::tempdir().unwrap();
    let mut config = configuration(directory.path(), json!([]), json!({}));
    config["acp"]["command"] = directory
        .path()
        .join("missing-executable")
        .display()
        .to_string()
        .into();
    let bridge = Bridge::new(config, BTreeMap::new(), directory.path().join("sessions")).unwrap();
    for command in ["/model", "/effort", "/mode", "/agent", "/commands"] {
        let sink = Recorder::default();
        assert!(bridge.handle(event(command), &sink).await.is_err());
        assert!(sink.0.lock().await.iter().any(|value| {
            value["text"]
                .as_str()
                .is_some_and(|text| text.contains("ACP control failed"))
        }));
        let sink = Recorder::default();
        bridge
            .handle(event("/backend status"), &sink)
            .await
            .unwrap();
        assert!(sink.0.lock().await.iter().any(|value| {
            value["text"]
                .as_str()
                .is_some_and(|text| text.starts_with("Backend: opencode"))
        }));
    }
    bridge.shutdown().await.unwrap();
}

#[tokio::test]
async fn matrix_active_and_pending_controls_refuse_before_receipts_or_state_lock() {
    for initial in ["hello", "/model"] {
        let directory = tempfile::tempdir().unwrap();
        let started = directory.path().join("started");
        let release = directory.path().join("release");
        let config = configuration(
            directory.path(),
            json!([{"text":"original answer","stopReason":"end_turn"}]),
            json!({"ACP_FIXTURE_INIT_STARTED_FILE":started,"ACP_FIXTURE_INIT_RELEASE_FILE":release}),
        );
        let bridge = Arc::new(
            Bridge::new(config, BTreeMap::new(), directory.path().join("sessions")).unwrap(),
        );
        let initial_sink = Recorder::default();
        let original = {
            let bridge = bridge.clone();
            let sink = initial_sink.clone();
            tokio::spawn(async move { bridge.handle(event(initial), &sink).await })
        };
        tokio::time::timeout(Duration::from_secs(3), async {
            while !started.exists() {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .unwrap();
        let mut failures = Vec::new();
        for command in [
            "/model two",
            "/models",
            "/effort",
            "/mode plan",
            "/agent plan",
            "/commands",
            "/backend codex",
        ] {
            let sink = Recorder::default();
            if tokio::time::timeout(
                Duration::from_millis(100),
                bridge.handle(event(command), &sink),
            )
            .await
            .is_err()
            {
                failures.push(format!("{command} waited on active state"));
            }
            let output = sink.0.lock().await;
            if output.iter().any(|value| value["type"] == "read")
                || !output.iter().any(|value| {
                    value["text"]
                        .as_str()
                        .is_some_and(|text| text.contains("already running"))
                })
            {
                failures.push(format!("{command}: {output:?}"));
            }
        }
        if initial == "/model" {
            let sink = Recorder::default();
            if tokio::time::timeout(
                Duration::from_millis(100),
                bridge.handle(event("new prompt"), &sink),
            )
            .await
            .is_err()
            {
                failures.push("prompt waited for pending control".into());
            }
            if !sink.0.lock().await.iter().any(|value| {
                value["text"]
                    .as_str()
                    .is_some_and(|text| text.contains("controls are being updated"))
            }) {
                failures.push("pending control did not refuse prompt".into());
            }
        }
        std::fs::write(release, "release").unwrap();
        original.await.unwrap().unwrap();
        bridge
            .handle(event("/backend"), &Recorder::default())
            .await
            .unwrap();
        bridge.shutdown().await.unwrap();
        assert!(failures.is_empty(), "{initial}: {failures:?}");
    }
}

#[tokio::test]
async fn matrix_empty_response_retry_is_fresh_once_and_never_replays_errors_or_side_effects() {
    for (script, attempts, expected) in [
        (
            json!([{"text":"   ","stopReason":"end_turn"},{"text":"recovered from whitespace","stopReason":"end_turn"}]),
            2,
            "recovered from whitespace",
        ),
        (
            json!([{"text":"backend response","stopReason":""}]),
            1,
            "backend response",
        ),
        (
            json!([{"text":"","stopReason":"end_turn"},{"error":true}]),
            2,
            "something went wrong",
        ),
        (
            json!([{"text":"","stopReason":"end_turn"},{"text":"recovered answer","stopReason":"end_turn"}]),
            2,
            "recovered answer",
        ),
        (
            json!([{"text":"","stopReason":"end_turn"},{"text":"","stopReason":"end_turn"}]),
            2,
            "completed without returning a usable response",
        ),
        (json!([{"error":true}]), 1, "something went wrong"),
        (
            json!([{"tool":true,"stopReason":"end_turn"}]),
            1,
            "completed without returning a usable response",
        ),
        (
            json!([{"image":true,"stopReason":"end_turn"}]),
            1,
            "completed without returning a usable response",
        ),
        (
            json!([{"permission":true,"stopReason":"end_turn"}]),
            1,
            "completed without returning a usable response",
        ),
        (
            json!([{"text":"partial answer","stopReason":"cancelled"}]),
            1,
            "Request cancelled.",
        ),
        (
            json!([{"text":"partial answer","stopReason":"refusal"}]),
            1,
            "ACP stopped without normal completion (refusal).",
        ),
        (
            json!([{"text":"","stopReason":"end_turn"},{"text":"partial","stopReason":"max_tokens"}]),
            2,
            "ACP stopped without normal completion (max_tokens).",
        ),
    ] {
        let directory = tempfile::tempdir().unwrap();
        let config = configuration(directory.path(), script.clone(), json!({}));
        let bridge =
            Bridge::new(config, BTreeMap::new(), directory.path().join("sessions")).unwrap();
        let sink = Recorder::default();
        let result = bridge.handle(event("hello"), &sink).await;
        assert_eq!(
            result.is_err(),
            script
                .as_array()
                .unwrap()
                .iter()
                .any(|action| action["error"] == true)
        );
        let output = sink.0.lock().await;
        assert!(
            output.iter().any(|event| event["text"]
                .as_str()
                .or(event["message"].as_str())
                .is_some_and(|text| text.contains(expected))),
            "{script}: {output:?}"
        );
        assert_eq!(
            output
                .iter()
                .filter(|event| event["type"] == "thinking")
                .count(),
            1
        );
        assert_eq!(
            output
                .iter()
                .filter(|event| event["type"] == "done")
                .count(),
            1
        );
        drop(output);
        let audit = audit(directory.path());
        let prompts: Vec<_> = audit
            .iter()
            .filter(|event| event["method"] == "session/prompt")
            .collect();
        assert_eq!(prompts.len(), attempts, "{script}: {audit:?}");
        if attempts == 2 {
            assert_ne!(prompts[0]["pid"], prompts[1]["pid"]);
        }
        assert!(
            audit
                .iter()
                .all(|event| event["method"] != "session/load"
                    && event["method"] != "session/delete")
        );
        bridge.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn matrix_compact_requires_exact_lowercase_command_and_end_turn_without_retry() {
    for (query, reason, expected) in [
        ("/compact", Some("end_turn"), "Compaction completed"),
        (
            "/compact now",
            Some("refusal"),
            "Compaction was not confirmed",
        ),
        ("/compact", None, "stop reason: unknown"),
        ("/compact", Some(""), "stop reason: unknown"),
        ("/compact", Some("cancelled"), "Request cancelled."),
        ("/COMPACT", Some("end_turn"), "backend response"),
    ] {
        let directory = tempfile::tempdir().unwrap();
        let mut action = json!({"text":if query == "/COMPACT" { "backend response" } else { "" }});
        if let Some(reason) = reason {
            action["stopReason"] = reason.into();
        }
        let config = configuration(directory.path(), json!([action]), json!({}));
        let bridge =
            Bridge::new(config, BTreeMap::new(), directory.path().join("sessions")).unwrap();
        let sink = Recorder::default();
        bridge.handle(event(query), &sink).await.unwrap();
        assert!(
            sink.0.lock().await.iter().any(|event| event["text"]
                .as_str()
                .is_some_and(|text| text.contains(expected))),
            "{query} {reason:?}"
        );
        assert_eq!(
            audit(directory.path())
                .iter()
                .filter(|event| event["method"] == "session/prompt")
                .count(),
            1
        );
        bridge.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn matrix_cancellation_during_initialization_or_reconnect_never_starts_next_prompt() {
    for retry in [false, true] {
        let directory = tempfile::tempdir().unwrap();
        let started = directory.path().join("started");
        let release = directory.path().join("release");
        let extra = if retry {
            json!({"ACP_FIXTURE_RETRY_STARTED_FILE":started,"ACP_FIXTURE_RETRY_RELEASE_FILE":release})
        } else {
            json!({"ACP_FIXTURE_INIT_STARTED_FILE":started,"ACP_FIXTURE_INIT_RELEASE_FILE":release})
        };
        let config = configuration(
            directory.path(),
            json!([{"text":"","stopReason":"end_turn"},{"text":"must not prompt"}]),
            extra,
        );
        let bridge = Arc::new(
            Bridge::new(config, BTreeMap::new(), directory.path().join("sessions")).unwrap(),
        );
        let sink = Recorder::default();
        let original = {
            let bridge = bridge.clone();
            let sink = sink.clone();
            tokio::spawn(async move { bridge.handle(event("hello"), &sink).await })
        };
        tokio::time::timeout(Duration::from_secs(2), async {
            while !started.exists() {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .unwrap();
        let cancelled = {
            let bridge = bridge.clone();
            let sink = sink.clone();
            tokio::spawn(async move { bridge.handle(event("/cancel"), &sink).await })
        };
        tokio::time::sleep(Duration::from_millis(10)).await;
        std::fs::write(release, "release").unwrap();
        tokio::time::timeout(Duration::from_secs(2), original)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        tokio::time::timeout(Duration::from_secs(2), cancelled)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(
            audit(directory.path())
                .iter()
                .filter(|event| event["method"] == "session/prompt")
                .count(),
            usize::from(retry)
        );
        assert!(sink.0.lock().await.iter().any(|event| {
            event["text"]
                .as_str()
                .is_some_and(|text| text.contains("cancelled before"))
        }));
        bridge.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn matrix_empty_combined_followup_never_retries_or_replays_only_original_input() {
    let directory = tempfile::tempdir().unwrap();
    let config = configuration(
        directory.path(),
        json!([{"waitForFollowup":true},{"text":"","stopReason":"end_turn"}]),
        json!({}),
    );
    let bridge =
        Arc::new(Bridge::new(config, BTreeMap::new(), directory.path().join("sessions")).unwrap());
    let sink = Recorder::default();
    let original = {
        let bridge = bridge.clone();
        let sink = sink.clone();
        tokio::spawn(async move { bridge.handle(event("first"), &sink).await })
    };
    tokio::time::timeout(Duration::from_secs(2), async {
        while !directory.path().join("counter").exists() {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap();
    bridge.handle(event("additional"), &sink).await.unwrap();
    original.await.unwrap().unwrap();
    let audit = audit(directory.path());
    let prompts: Vec<_> = audit
        .iter()
        .filter(|event| event["method"] == "session/prompt")
        .collect();
    assert_eq!(prompts.len(), 2);
    assert_eq!(prompts[0]["pid"], prompts[1]["pid"]);
    assert_eq!(prompts[0]["text"], "first");
    assert_eq!(prompts[1]["text"], "additional");
    assert!(sink.0.lock().await.iter().any(|event| {
        event["text"]
            .as_str()
            .is_some_and(|text| text.contains("completed without returning a usable response"))
    }));
    bridge.shutdown().await.unwrap();
}
