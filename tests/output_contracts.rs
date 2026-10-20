use async_trait::async_trait;
use base64::{Engine, engine::general_purpose::STANDARD};
use soulsteads_messaging::{
    config,
    runtime::{Bridge, Event, Sink},
};
use serde_json::{Value, json};
use std::{collections::BTreeMap, path::Path};
use tokio::sync::Mutex;

#[derive(Default)]
struct Recorder {
    events: Mutex<Vec<Value>>,
    uploads: Mutex<Vec<Vec<u8>>>,
    fail_display: bool,
    fail_upload: bool,
    pause_first_chunk_ms: std::sync::atomic::AtomicU64,
}
#[async_trait]
impl Sink for Recorder {
    async fn emit(&self, event: Value) -> anyhow::Result<()> {
        if event["type"] == "chunk" {
            let delay = self
                .pause_first_chunk_ms
                .swap(0, std::sync::atomic::Ordering::SeqCst);
            if delay > 0 {
                tokio::time::sleep(std::time::Duration::from_millis(delay)).await;
            }
        }
        if self.fail_display
            && matches!(
                event["type"].as_str(),
                Some("tool" | "tool_activity" | "chunk" | "notice")
            )
        {
            anyhow::bail!("fixture display failed");
        }
        self.events.lock().await.push(event);
        Ok(())
    }
    async fn upload(&self, name: &str, mime: &str, bytes: Vec<u8>) -> anyhow::Result<()> {
        self.events
            .lock()
            .await
            .push(json!({"type":"uploaded","mime":mime,"name":name}));
        if self.fail_upload {
            anyhow::bail!("fixture upload failed");
        }
        self.uploads.lock().await.push(bytes);
        Ok(())
    }
}
fn png() -> Vec<u8> {
    let mut bytes = std::io::Cursor::new(Vec::new());
    image::DynamicImage::new_rgb8(1, 1)
        .write_to(&mut bytes, image::ImageFormat::Png)
        .unwrap();
    bytes.into_inner()
}
fn setup(root: &Path, script: Value) -> Bridge {
    std::fs::write(root.join("script.json"), script.to_string()).unwrap();
    let config = config::load(&json!({"rateLimitSeconds":0,"sessionStorePath":root.join("store.json"),"acp":{"command":env!("CARGO_BIN_EXE_acp-fixture"),"args":[],"env":{"ACP_FIXTURE_MATRIX_SCRIPT":root.join("script.json"),"ACP_FIXTURE_MATRIX_COUNTER":root.join("counter"),"ACP_FIXTURE_PERMISSION_RESPONSE":root.join("permission-response.json")}},"matrix":{"respondToAllMessages":true},"toolMessages":{"mode":"off"}}).to_string(), &BTreeMap::new()).unwrap();
    Bridge::new(config, BTreeMap::new(), root.join("sessions")).unwrap()
}
fn event(platform: &str) -> Event {
    let mut event = Event::web("client", "hello");
    event.platform = platform.into();
    if platform == "matrix" {
        event.room = "!room:local".into();
        event.user = "@user:local".into();
    }
    event
}
fn update(content: Value) -> Value {
    json!({"update":{"sessionUpdate":"agent_message_chunk","content":content}})
}

#[tokio::test]
async fn inline_only_and_mixed_upload_before_final_caption_on_web_generic_matrix() {
    for platform in ["web", "telegram", "matrix"] {
        for caption in ["", "caption"] {
            let root = tempfile::tempdir().unwrap();
            let bytes = png();
            let bridge = setup(
                root.path(),
                json!([{"updates":[update(json!({"type":"image","data":STANDARD.encode(&bytes),"mimeType":"image/png"}))],"text":caption,"stopReason":"end_turn"}]),
            );
            let sink = Recorder::default();
            bridge.handle(event(platform), &sink).await.unwrap();
            assert_eq!(
                *sink.uploads.lock().await,
                vec![bytes],
                "{platform}/{caption}"
            );
            let events = sink.events.lock().await;
            let upload = events
                .iter()
                .position(|item| item["type"] == "uploaded")
                .unwrap();
            let response = events
                .iter()
                .position(|item| item["type"] == "response")
                .unwrap();
            assert!(upload < response);
            assert_eq!(events[response]["text"], caption);
            drop(events);
            assert_eq!(
                std::fs::read_to_string(root.path().join("counter")).unwrap(),
                "1"
            );
            bridge.shutdown().await.unwrap();
        }
    }
}

#[tokio::test]
async fn unavailable_approval_notice_is_generic_cancelled_and_transport_visible() {
    let root = tempfile::tempdir().unwrap();
    let bridge = setup(
        root.path(),
        json!([{"expectedPermissionOutcome":{"outcome":"cancelled"},"request":{"jsonrpc":"2.0","id":900,"method":"session/request_permission","params":{"sessionId":"fixture","toolCall":{"toolCallId":"fixture-write","title":"write","rawInput":{"filePath":"notes.txt"}},"options":[{"optionId":"allow","kind":"allow_once"},{"optionId":"reject","kind":"reject_once"}]}}}]),
    );
    let sink = Recorder::default();
    bridge.handle(event("matrix"), &sink).await.unwrap();
    assert_eq!(
        serde_json::from_slice::<Value>(
            &std::fs::read(root.path().join("permission-response.json")).unwrap()
        )
        .unwrap(),
        json!({"jsonrpc":"2.0","id":900,"result":{"outcome":{"outcome":"cancelled"}}})
    );
    assert!(
        sink.events
            .lock()
            .await
            .iter()
            .any(|item| item["type"] == "notice"
                && item["text"] == "> Permission denied: approval unavailable for this request"
                && item["permission"] == "tool"
                && item["path"].is_null()
                && item["sessionId"] == "fixture"
                && item["toolCallId"].is_null())
    );
    assert!(
        sink.events
            .lock()
            .await
            .iter()
            .any(|item| item["type"] == "prompt_end" && item["stopReason"] == "end_turn")
    );
    assert_eq!(
        std::fs::read_to_string(root.path().join("counter")).unwrap(),
        "1"
    );
    bridge.shutdown().await.unwrap();
}

#[tokio::test]
async fn unavailable_approval_never_discloses_titles_or_paths() {
    for (params, _, _) in [
        (
            json!({"title":"","toolCall":{"title":""}}),
            "Permission denied: unknown",
            Value::Null,
        ),
        (
            json!({"path":"params.txt","toolCall":{"title":"read","locations":[{"path":""}]}}),
            "Permission denied: read (params.txt)",
            json!("params.txt"),
        ),
        (
            json!({"path":"","directory":"folder","toolCall":{"title":"read"}}),
            "Permission denied: read (folder)",
            json!("folder"),
        ),
    ] {
        let root = tempfile::tempdir().unwrap();
        let mut params = params;
        params["sessionId"] = "fixture".into();
        params["toolCall"]["toolCallId"] = "fixture-fallback".into();
        params["options"] = json!([]);
        let bridge = setup(
            root.path(),
            json!([{"expectedPermissionOutcome":{"outcome":"cancelled"},"request":{"jsonrpc":"2.0","id":900,"method":"session/request_permission","params":params}}]),
        );
        let sink = Recorder::default();
        bridge.handle(event("web"), &sink).await.unwrap();
        assert_eq!(
            serde_json::from_slice::<Value>(
                &std::fs::read(root.path().join("permission-response.json")).unwrap()
            )
            .unwrap(),
            json!({"jsonrpc":"2.0","id":900,"result":{"outcome":{"outcome":"cancelled"}}})
        );
        assert!(
            sink.events
                .lock()
                .await
                .iter()
                .any(|item| item["type"] == "prompt_end" && item["stopReason"] == "end_turn")
        );
        assert!(
            sink.events
                .lock()
                .await
                .iter()
                .any(|item| item["type"] == "notice"
                    && item["text"]
                        == "> Permission denied: approval unavailable for this request"
                    && item["path"].is_null()
                    && item["sessionId"] == "fixture")
        );
        bridge.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn whitespace_observations_are_per_prompt_and_collector_closed() {
    let root = tempfile::tempdir().unwrap();
    let bridge = setup(
        root.path(),
        json!([{"text":"   ","tool":true,"stopReason":"end_turn"},{"text":"answer","stopReason":"end_turn"}]),
    );
    let sink = Recorder::default();
    bridge.handle(event("matrix"), &sink).await.unwrap();
    bridge.handle(event("matrix"), &sink).await.unwrap();
    let events = sink.events.lock().await;
    let observations = events
        .iter()
        .filter(|item| item["type"] == "prompt_end")
        .collect::<Vec<_>>();
    assert_eq!(observations.len(), 2);
    assert_eq!(observations[0]["rawChars"], 3);
    assert_eq!(observations[0]["cleanChars"], 0);
    assert_eq!(observations[0]["hadToolActivity"], true);
    assert_eq!(
        observations[0]["diagnostic"]["source"],
        "bridge-processing-removed"
    );
    assert_eq!(observations[1]["rawChars"], 6);
    assert_eq!(observations[1]["hadToolActivity"], false);
    assert_eq!(observations[1]["collectorClosed"], true);
    assert_eq!(observations[1]["model"], "one");
    drop(events);
    bridge.shutdown().await.unwrap();
}

#[tokio::test]
async fn typed_tool_images_follow_source_event_count_and_content_precedence() {
    let root = tempfile::tempdir().unwrap();
    let image = json!({"type":"image","data":STANDARD.encode(png()),"alt":"caption"});
    let mut updates = vec![
        update(image.clone()),
        json!({"part":image}),
        json!({"update":{"part":image}}),
        json!({"update":{"sessionUpdate":"tool_call","toolCallId":"one","content":[{"content":image}]}}),
        json!({"update":{"sessionUpdate":"tool_call_update","toolCallId":"one","status":"completed","content":[{"content":image}]}}),
        json!({"update":{"sessionUpdate":"tool_call_update","toolCallId":"one","status":"completed","content":[],"rawOutput":{"output":image.to_string()}}}),
    ];
    for output in [
        image.clone(),
        json!([image]),
        json!({"content":[image]}),
        json!({"data":image["data"]}),
        json!({"type":"text","data":image["data"]}),
    ] {
        updates.push(json!({"update":{"sessionUpdate":"tool_call_update","toolCallId":"one","status":"completed","rawOutput":{"output":output.to_string()}}}));
    }
    let bridge = setup(
        root.path(),
        json!([{"updates":updates,"stopReason":"end_turn"}]),
    );
    let sink = Recorder::default();
    bridge.handle(event("matrix"), &sink).await.unwrap();
    assert_eq!(sink.uploads.lock().await.len(), 7);
    assert!(
        sink.events
            .lock()
            .await
            .iter()
            .filter(|item| item["type"] == "uploaded")
            .all(|item| item["name"] == "caption")
    );
    assert!(
        sink.events
            .lock()
            .await
            .iter()
            .any(|item| item["type"] == "prompt_end" && item["imageCount"] == 7)
    );
    let sessions = root.path().join("sessions");
    let session = soulsteads_messaging::runtime::session_dir(&sessions, "matrix", "!room:local");
    let images = session.join(".bridge-images");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(
            std::fs::metadata(&images).unwrap().permissions().mode() & 0o777,
            0o700
        );
        for file in std::fs::read_dir(images).unwrap() {
            assert_eq!(
                file.unwrap().metadata().unwrap().permissions().mode() & 0o777,
                0o600
            );
        }
    }
    bridge.shutdown().await.unwrap();
}

#[tokio::test]
async fn failed_image_upload_and_display_never_replay_side_effects() {
    for content in [
        json!({"type":"image","mimeType":"image/jpeg","data":STANDARD.encode(png())}),
        json!({"type":"image","mimeType":"image/png","data":STANDARD.encode(png())}),
    ] {
        let root = tempfile::tempdir().unwrap();
        let bridge = setup(
            root.path(),
            json!([{"updates":[update(content)],"tool":true,"stopReason":"end_turn"}]),
        );
        let sink = Recorder {
            fail_display: true,
            fail_upload: true,
            ..Default::default()
        };
        bridge.handle(event("matrix"), &sink).await.unwrap();
        assert_eq!(
            std::fs::read_to_string(root.path().join("counter")).unwrap(),
            "1"
        );
        assert!(sink.events.lock().await.iter().any(|item| {
            item["type"] == "response"
                && item["text"]
                    .as_str()
                    .unwrap_or("")
                    .contains("usable response")
        }));
        bridge.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn annotation_only_owned_media_delivers_without_retry_and_reports_delivery_length() {
    let root = tempfile::tempdir().unwrap();
    let session =
        soulsteads_messaging::runtime::session_dir(&root.path().join("sessions"), "matrix", "!room:local");
    std::fs::create_dir_all(&session).unwrap();
    let path = session.join("annotation.png");
    std::fs::write(&path, png()).unwrap();
    let bridge = setup(
        root.path(),
        json!([{"text":format!("Path: {}", path.display()),"stopReason":"end_turn"}]),
    );
    let sink = Recorder::default();
    bridge.handle(event("matrix"), &sink).await.unwrap();
    assert_eq!(sink.uploads.lock().await.len(), 1);
    assert_eq!(
        std::fs::read_to_string(root.path().join("counter")).unwrap(),
        "1"
    );
    assert!(
        sink.events
            .lock()
            .await
            .iter()
            .any(|item| item["type"] == "prompt_end"
                && item["deliveredChars"] == 0
                && item["uploadedImages"] == 1)
    );
    bridge.shutdown().await.unwrap();
}

#[tokio::test]
async fn foreign_permission_and_stale_text_never_disclose_or_contaminate_next_prompt() {
    let root = tempfile::tempdir().unwrap();
    let bridge = setup(
        root.path(),
        json!([{"updates":[{"sessionId":"foreign","update":{"sessionUpdate":"agent_message_chunk","content":{"type":"text","text":"foreign secret"}}}],"expectedPermissionOutcome":{"outcome":"cancelled"},"request":{"jsonrpc":"2.0","id":900,"method":"session/request_permission","params":{"sessionId":"foreign","toolCall":{"toolCallId":"foreign-write","title":"foreign secret","rawInput":{"filePath":"foreign-private.txt"}},"options":[{"optionId":"foreign-allow","kind":"allow_once"},{"optionId":"foreign-deny","kind":"reject_once"}]}}},{"text":"next answer","stopReason":"end_turn"}]),
    );
    let sink = Recorder::default();
    bridge.handle(event("web"), &sink).await.unwrap();
    bridge.handle(event("web"), &sink).await.unwrap();
    assert_eq!(
        serde_json::from_slice::<Value>(
            &std::fs::read(root.path().join("permission-response.json")).unwrap()
        )
        .unwrap(),
        json!({"jsonrpc":"2.0","id":900,"result":{"outcome":{"outcome":"cancelled"}}})
    );
    assert!(
        !sink
            .events
            .lock()
            .await
            .iter()
            .any(|item| item.to_string().contains("foreign secret")
                || item.to_string().contains("foreign-private.txt"))
    );
    let events = sink.events.lock().await;
    assert_eq!(
        events
            .iter()
            .filter(|item| item["type"] == "prompt_end" && item["stopReason"] == "end_turn")
            .count(),
        2
    );
    assert!(
        events
            .iter()
            .any(|item| item["type"] == "response" && item["text"] == "next answer")
    );
    drop(events);
    assert_eq!(
        std::fs::read_to_string(root.path().join("counter")).unwrap(),
        "2"
    );
    bridge.shutdown().await.unwrap();
}

#[tokio::test]
async fn dropped_presentation_activity_cannot_replay_authoritative_tool_side_effects() {
    let root = tempfile::tempdir().unwrap();
    let mut updates = vec![
        update(json!({"type":"text","text":" "})),
        json!({"update":{"sessionUpdate":"tool_call","toolCallId":"side-effect","title":"write"}}),
    ];
    for _ in 0..512 {
        updates.push(update(json!({"type":"text","text":" "})));
    }
    let bridge = setup(
        root.path(),
        json!([{"updates":updates,"stopReason":"end_turn"}]),
    );
    let sink = Recorder {
        pause_first_chunk_ms: std::sync::atomic::AtomicU64::new(100),
        ..Default::default()
    };
    bridge.handle(event("matrix"), &sink).await.unwrap();
    assert_eq!(
        std::fs::read_to_string(root.path().join("counter")).unwrap(),
        "1"
    );
    let events = sink.events.lock().await;
    let observation = events
        .iter()
        .find(|event| event["type"] == "prompt_end")
        .unwrap();
    assert_eq!(observation["hadToolActivity"], true);
    assert_eq!(observation["rawChars"], 513);
    assert_eq!(observation["presentationLost"], true);
    assert!(observation["capturedChars"].as_u64().unwrap() < 513);
    assert_eq!(observation["usableOutput"], false);
    drop(events);
    bridge.shutdown().await.unwrap();
}

#[tokio::test]
async fn denial_then_error_keep_model_clear_stop_reason_and_report_collector_raw_length() {
    let root = tempfile::tempdir().unwrap();
    let bridge = setup(
        root.path(),
        json!([{"permission":true},{"updates":[update(json!({"type":"text","text":"lost"}))],"error":true}]),
    );
    let sink = Recorder::default();
    bridge.handle(event("web"), &sink).await.unwrap();
    assert!(bridge.handle(event("web"), &sink).await.is_err());
    let events = sink.events.lock().await;
    let observations = events
        .iter()
        .filter(|item| item["type"] == "prompt_end")
        .collect::<Vec<_>>();
    assert_eq!(observations.len(), 2);
    assert!(
        observations
            .iter()
            .all(|item| item["model"] == "one" && item["collectorClosed"] == true)
    );
    assert_eq!(observations[1]["stopReason"], Value::Null);
    assert_eq!(observations[1]["rawChars"], 4);
    assert_eq!(observations[1]["diagnostic"]["acpChars"], 4);
    assert_eq!(observations[1]["failed"], true);
    drop(events);
    bridge.shutdown().await.unwrap();
}

#[tokio::test]
async fn unsupported_server_method_returns_protocol_error_without_reading_or_disclosing() {
    let root = tempfile::tempdir().unwrap();
    let bridge = setup(
        root.path(),
        json!([{"request":{"jsonrpc":"2.0","id":900,"method":"fs/read","params":{"sessionId":"fixture","path":"foreign secret"}}}]),
    );
    let sink = Recorder::default();
    bridge.handle(event("web"), &sink).await.unwrap();
    assert!(
        !sink
            .events
            .lock()
            .await
            .iter()
            .any(|item| item.to_string().contains("foreign secret"))
    );
    assert_eq!(
        std::fs::read_to_string(root.path().join("counter")).unwrap(),
        "1"
    );
    bridge.shutdown().await.unwrap();
}

#[tokio::test]
async fn inline_delivery_survives_nonnormal_stop_and_precedes_later_text_chunks() {
    for reason in ["end_turn", "cancelled", "refusal", "max_tokens"] {
        let root = tempfile::tempdir().unwrap();
        let bridge = setup(
            root.path(),
            json!([{"updates":[update(json!({"type":"image","data":STANDARD.encode(png()),"mimeType":"image/png"}))],"text":"caption","stopReason":reason}]),
        );
        let sink = Recorder::default();
        bridge.handle(event("matrix"), &sink).await.unwrap();
        assert_eq!(sink.uploads.lock().await.len(), 1, "{reason}");
        let events = sink.events.lock().await;
        let upload = events
            .iter()
            .position(|item| item["type"] == "uploaded")
            .unwrap();
        let chunk = events
            .iter()
            .position(|item| item["type"] == "chunk")
            .unwrap();
        assert!(
            upload < chunk,
            "source inline image event precedes the later text chunk: {reason}"
        );
        assert!(
            events
                .iter()
                .any(|item| item["type"] == "prompt_end" && item["uploadedImages"] == 1)
        );
        drop(events);
        assert_eq!(
            std::fs::read_to_string(root.path().join("counter")).unwrap(),
            "1"
        );
        bridge.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn inline_images_preserve_surrounding_text_event_order_without_future_uploads() {
    for _attempt in 0..16 {
        let root = tempfile::tempdir().unwrap();
        let image = json!({"type":"image","data":STANDARD.encode(png()),"mimeType":"image/png"});
        let bridge = setup(
            root.path(),
            json!([{"updates":[update(json!({"type":"text","text":"before"})),update(image.clone()),update(json!({"type":"text","text":"middle"})),update(image),update(json!({"type":"text","text":"after"}))],"stopReason":"end_turn"}]),
        );
        let sink = Recorder::default();
        bridge.handle(event("web"), &sink).await.unwrap();
        let events = sink.events.lock().await;
        let actual = events
            .iter()
            .filter_map(|item| match item["type"].as_str() {
                Some("chunk") => item["text"].as_str(),
                Some("uploaded") => Some("image"),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(actual, ["before", "image", "middle", "image", "after"]);
        drop(events);
        bridge.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn source_tool_identity_counts_once_even_with_display_disabled_and_duplicate_updates() {
    let root = tempfile::tempdir().unwrap();
    let tool = json!({"update":{"sessionUpdate":"tool_call","toolCallId":"same","title":"write","rawInput":{"path":"notes.txt"}}});
    let bridge = setup(
        root.path(),
        json!([{"updates":[tool.clone(),tool,{"update":{"sessionUpdate":"tool_call_update","toolCallId":"same","status":"completed"}}],"text":"answer","stopReason":"end_turn"}]),
    );
    let sink = Recorder::default();
    bridge.handle(event("matrix"), &sink).await.unwrap();
    let events = sink.events.lock().await;
    let output = events
        .iter()
        .find(|item| item["type"] == "prompt_end")
        .unwrap();
    assert_eq!(output["toolCalls"], 1);
    assert_eq!(output["hadToolActivity"], true);
    assert_eq!(output["hasImages"], false);
    assert_eq!(output["usableOutput"], true);
    drop(events);
    bridge.shutdown().await.unwrap();
}
