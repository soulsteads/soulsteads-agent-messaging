//! Actual Matrix SDK outbound routing, against an isolated HTTP peer.
use super::*;
use axum::{Json, Router, extract::Request, response::IntoResponse, routing::any};

struct Fixture {
    sent: Arc<tokio::sync::Mutex<Vec<Value>>>,
    base: String,
    fail: Arc<std::sync::atomic::AtomicBool>,
    fail_at: Arc<std::sync::atomic::AtomicUsize>,
    server: tokio::task::JoinHandle<()>,
}
impl Drop for Fixture {
    fn drop(&mut self) {
        self.server.abort();
    }
}
impl Fixture {
    async fn start() -> Self {
        let sent = Arc::new(tokio::sync::Mutex::new(Vec::<Value>::new()));
        let fail = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let fail_at = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let app=Router::new().fallback(any({ let sent=sent.clone(); let fail=fail.clone(); let fail_at=fail_at.clone(); move |request:Request| {
            let sent=sent.clone(); let fail=fail.clone(); let fail_at=fail_at.clone(); async move {
                let path=request.uri().path().to_owned();
                if path.trim_end_matches('/').ends_with("/state/m.room.encryption") {
                    return (axum::http::StatusCode::NOT_FOUND,Json(json!({"errcode":"M_NOT_FOUND","error":"plaintext"}))).into_response();
                }
                let value=if path.ends_with("versions") {json!({"versions":["v1.12"],"unstable_features":{}})}
                else if path.ends_with("whoami") {json!({"user_id":"@bot:local","device_id":"LOCAL"})}
                else if path.ends_with("sync") {json!({"next_batch":"one","rooms":{"join":{"!room:local":{"state":{"events":[
                    {"type":"m.room.create","event_id":"$create","origin_server_ts":1,"state_key":"","sender":"@bot:local","content":{"creator":"@bot:local","room_version":"10"}},
                    {"type":"m.room.member","event_id":"$member","origin_server_ts":1,"state_key":"@bot:local","sender":"@bot:local","content":{"membership":"join"}}
                ]},"timeline":{"events":[],"limited":false},"ephemeral":{"events":[]},"account_data":{"events":[]}}}},"device_lists":{"changed":[],"left":[]},"device_one_time_keys_count":{}})}
                else if path.contains("/send/") {
                    let bytes=axum::body::to_bytes(request.into_body(),1024*1024).await.unwrap();
                    let content:Value=serde_json::from_slice(&bytes).unwrap();
                    let mut messages=sent.lock().await; messages.push(content); let n=messages.len();
                    if fail.swap(false,std::sync::atomic::Ordering::SeqCst) || n == fail_at.load(std::sync::atomic::Ordering::SeqCst) {
                        return (axum::http::StatusCode::BAD_REQUEST,Json(json!({"errcode":"M_BAD_JSON","error":"synthetic rejection"}))).into_response();
                    }
                    json!({"event_id":format!("$sent-{n}")})
                } else {json!({})};
                Json(value).into_response()
            }
        }}));
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        Self {
            sent,
            base,
            fail,
            fail_at,
            server,
        }
    }
    async fn sink(&self, root: &Path, threaded: bool) -> MatrixSink {
        let client=connect(&json!({"homeserver":self.base,"userId":"@bot:local","accessToken":"fixture","encryption":{"enabled":false}}),root).await.unwrap();
        client.sync_once(SyncSettings::default()).await.unwrap();
        MatrixSink {
            room: client.get_room("!room:local".try_into().unwrap()).unwrap(),
            thread: threaded.then(|| "$original".try_into().unwrap()),
            raw_thread: Some("$original".try_into().unwrap()),
            html: false,
            rich_tools: false,
            activities: Default::default(),
            indicators: TypingManager::new(Arc::new(NativeTyping(client)), Duration::from_secs(4)),
            typing: Default::default(),
            incoming: "$input".try_into().unwrap(),
            reply_context: Default::default(),
        }
    }
}
async fn runtime_case(mode: &str, style: &str, show_calls: bool) -> Vec<Value> {
    let f = Fixture::start().await;
    let dir = tempfile::tempdir().unwrap();
    let sink = f.sink(&dir.path().join("sdk"), true).await;
    let updates = json!([
        {"sessionUpdate":"tool_call","toolCallId":"one","title":"bash","kind":"execute","rawInput":{"command":"first-command"}},
        {"sessionUpdate":"tool_call_update","toolCallId":"one","status":"in_progress"},
        {"sessionUpdate":"tool_call","toolCallId":"two","title":"bash","rawInput":{"command":"second-command"}},
        {"sessionUpdate":"tool_call","toolCallId":"read","title":"read","rawInput":{"filePath":"nonterminal-file"}},
        {"sessionUpdate":"tool_call","toolCallId":"three","title":"bash","rawInput":{"command":"third-command"}},
        {"sessionUpdate":"tool_call_update","toolCallId":"three","status":"completed","title":"bash","rawOutput":{"output":"third-output"}},
        {"sessionUpdate":"tool_call_update","toolCallId":"one","status":"completed","title":"bash","rawOutput":{"output":"first-output"}}
    ]);
    let config=crate::config::load(&json!({"rateLimitSeconds":0,"sessionStorePath":dir.path().join("sessions.json"),"toolMessages":{"mode":style,"showCalls":show_calls,"showArguments":true,"showOutputFor":["bash"]},
        "acp":{"command":"/usr/bin/python3","args":[concat!(env!("CARGO_MANIFEST_DIR"),"/tests/fixtures/matrix-terminal-peer.py")],"env":{"TERMINAL_UPDATES":updates.to_string()}},
        "matrix":{"respondToAllMessages":true,"allowedUsers":["@human:local"],"terminalThread":{"mode":mode,"threshold":3}}
    }).to_string(),&Default::default()).unwrap();
    let bridge = Bridge::new(config, Default::default(), dir.path().join("workspace")).unwrap();
    let event = Event {
        platform: "matrix".into(),
        room: "!room:local".into(),
        thread: "$original".into(),
        id: "$input".into(),
        user: "@human:local".into(),
        text: "run fixture".into(),
        direct: false,
        mentioned: false,
        bot: false,
        images: vec![],
        reply_to: None,
    };
    bridge.handle(event, &sink).await.unwrap();
    bridge.shutdown().await.unwrap();
    f.sent.lock().await.clone()
}
#[tokio::test]
async fn matrix_file_runtime_respects_full_path_option() {
    for full_paths in [false, true] {
        let f = Fixture::start().await;
        let dir = tempfile::tempdir().unwrap();
        let sink = f.sink(&dir.path().join("sdk"), true).await;
        let updates = json!([
            {"sessionUpdate":"tool_call","toolCallId":"read-file","title":"read","status":"in_progress","rawInput":{"filePath":"/private/tree/read.rs"}},
            {"sessionUpdate":"tool_call","toolCallId":"edit-file","title":"edit","status":"in_progress","rawInput":{"file_path":"C:\\private\\tree\\edit.rs","oldString":"secretold","newString":"secretnew"}},
            {"sessionUpdate":"tool_call","toolCallId":"write-file","title":"write","status":"in_progress","rawInput":{"path":"/private/tree/write.rs","content":"secretcontent"}}
        ]);
        let config=crate::config::load(&json!({"rateLimitSeconds":0,"sessionStorePath":dir.path().join("sessions.json"),"toolMessages":{"mode":"hermes","showArguments":false,"showOutputFor":[]},
            "acp":{"command":"/usr/bin/python3","args":[concat!(env!("CARGO_MANIFEST_DIR"),"/tests/fixtures/matrix-terminal-peer.py")],"env":{"TERMINAL_UPDATES":updates.to_string()}},
            "matrix":{"respondToAllMessages":true,"allowedUsers":["@human:local"],"showFullPaths":full_paths,"terminalThread":{"mode":"always"}}
        }).to_string(),&Default::default()).unwrap();
        let bridge = Bridge::new(config, Default::default(), dir.path().join("workspace")).unwrap();
        let event = Event {
            platform: "matrix".into(),
            room: "!room:local".into(),
            thread: "$original".into(),
            id: "$input".into(),
            user: "@human:local".into(),
            text: "files".into(),
            direct: false,
            mentioned: false,
            bot: false,
            images: vec![],
            reply_to: None,
        };
        bridge.handle(event, &sink).await.unwrap();
        bridge.shutdown().await.unwrap();
        let messages = f.sent.lock().await;
        let text = messages
            .iter()
            .filter_map(|m| m["body"].as_str())
            .collect::<Vec<_>>()
            .join("\n");
        for file in ["read.rs", "edit.rs", "write.rs"] {
            assert!(text.contains(file), "{text}");
        }
        assert_eq!(text.contains("/private/tree"), full_paths, "{text}");
        assert_eq!(text.contains("C:\\private\\tree"), full_paths, "{text}");
        assert!(!text.contains("secret"), "{text}");
        assert!(
            messages
                .iter()
                .all(|m| m["m.relates_to"]["event_id"] == "$original"
                    || m["m.relates_to"]["rel_type"] == "m.replace")
        );
    }
}

#[tokio::test]
async fn terminal_native_api_stream_routes_third_command_and_preserves_final_response() {
    let f = Fixture::start().await;
    let dir = tempfile::tempdir().unwrap();
    let sink = f.sink(&dir.path().join("sdk"), true).await;
    std::fs::write(dir.path().join("release"), "").unwrap();
    let configuration=crate::config::load(&json!({"rateLimitSeconds":0,"sessionStorePath":dir.path().join("store.json"),"toolMessages":{"mode":"hermes","showArguments":true,"showOutputFor":["bash"]},
        "acp":{"transport":"native","command":"/usr/bin/python3","args":[concat!(env!("CARGO_MANIFEST_DIR"),"/tests/fixtures/native_api.py")],"env":{"NATIVE_FIXTURE_ROOT":dir.path(),"BRIDGE_TEST_NATIVE_STDIO":"1"}},
        "matrix":{"respondToAllMessages":true,"allowedUsers":["@human:local"],"terminalThread":{"mode":"auto","threshold":3}}
    }).to_string(),&Default::default()).unwrap();
    let bridge = Bridge::new(
        configuration,
        Default::default(),
        dir.path().join("workspace"),
    )
    .unwrap();
    let event = Event {
        platform: "matrix".into(),
        room: "!room:local".into(),
        thread: "$original".into(),
        id: "$input".into(),
        user: "@human:local".into(),
        text: "terminal-output".into(),
        direct: false,
        mentioned: false,
        bot: false,
        images: vec![],
        reply_to: None,
    };
    bridge.handle(event, &sink).await.unwrap();
    bridge.shutdown().await.unwrap();
    let messages = f.sent.lock().await;
    let roots: Vec<_> = messages
        .iter()
        .enumerate()
        .filter(|(_, m)| {
            m["body"]
                .as_str()
                .is_some_and(|s| s.starts_with("Terminal activity for "))
        })
        .collect();
    assert_eq!(roots.len(), 1, "{messages:?}");
    let root = format!("$sent-{}", roots[0].0 + 1);
    let command = messages
        .iter()
        .find(|m| {
            m["body"]
                .as_str()
                .is_some_and(|s| s.contains("native-command-2"))
        })
        .unwrap();
    assert_eq!(command["m.relates_to"]["rel_type"], "m.replace");
    assert_eq!(command["m.new_content"]["m.relates_to"]["event_id"], root);
    let output = messages
        .iter()
        .find(|m| m["body"] == "native-output-2")
        .unwrap();
    assert_eq!(output["m.relates_to"]["event_id"], root);
    let response = messages
        .iter()
        .find(|m| m["body"] == "reply:terminal-output")
        .unwrap();
    assert_eq!(response["m.relates_to"]["event_id"], "$original");
}
#[tokio::test]
async fn terminal_runtime_auto_routes_third_command_and_output_without_moving_other_tools() {
    let messages = runtime_case("auto", "hermes", true).await;
    let roots: Vec<_> = messages
        .iter()
        .enumerate()
        .filter(|(_, m)| {
            m["body"]
                .as_str()
                .is_some_and(|s| s.starts_with("Terminal activity for "))
        })
        .collect();
    assert_eq!(
        roots.len(),
        1,
        "one activity root must be created: {messages:?}"
    );
    let root = format!("$sent-{}", roots[0].0 + 1);
    assert!(roots[0].1.get("m.relates_to").is_none());
    let command = messages
        .iter()
        .find(|m| {
            m["body"]
                .as_str()
                .is_some_and(|s| s.contains("third-command"))
        })
        .unwrap();
    assert_eq!(command["m.relates_to"]["event_id"], root);
    for label in ["nonterminal-file", "Final fixture answer", "first-output"] {
        let m = messages
            .iter()
            .find(|m| m["body"].as_str().is_some_and(|s| s.contains(label)))
            .unwrap();
        assert_eq!(m["m.relates_to"]["event_id"], "$original", "{label}: {m}");
    }
    let output = messages
        .iter()
        .find(|m| m["body"] == "third-output")
        .unwrap();
    assert_eq!(output["m.relates_to"]["event_id"], root);
}
#[tokio::test]
async fn terminal_auto_deduplicates_updates_and_keeps_existing_edits_in_place() {
    let f = Fixture::start().await;
    let dir = tempfile::tempdir().unwrap();
    let sink = f.sink(dir.path(), true).await;
    sink.emit(terminal("one", "auto", "first")).await.unwrap();
    sink.owned()
        .unwrap()
        .emit(terminal("one", "auto", "first update"))
        .await
        .unwrap();
    sink.emit(terminal("two", "auto", "second")).await.unwrap();
    assert_eq!(
        f.sent.lock().await.len(),
        3,
        "updates must not count as new commands"
    );
    sink.emit(terminal("three", "auto", "third")).await.unwrap();
    sink.emit(terminal("one", "auto", "first completed"))
        .await
        .unwrap();
    sink.emit(terminal("three", "auto", "third completed"))
        .await
        .unwrap();
    let messages = f.sent.lock().await;
    assert_eq!(messages.len(), 8);
    assert!(messages[3].get("m.relates_to").is_none());
    assert_eq!(messages[5]["m.relates_to"]["event_id"], "$sent-4");
    assert_eq!(messages[6]["m.relates_to"]["event_id"], "$sent-1");
    assert_eq!(
        messages[6]["m.new_content"]["m.relates_to"]["event_id"],
        "$original"
    );
    assert_eq!(messages[7]["m.relates_to"]["event_id"], "$sent-6");
    assert_eq!(
        messages[7]["m.new_content"]["m.relates_to"]["event_id"],
        "$sent-4"
    );
}
#[tokio::test]
async fn terminal_failed_navigation_falls_back_without_stranding_output() {
    let f = Fixture::start().await;
    let dir = tempfile::tempdir().unwrap();
    let sink = f.sink(dir.path(), true).await;
    f.fail_at.store(2, std::sync::atomic::Ordering::SeqCst);
    sink.emit(terminal("one", "always", "first")).await.unwrap();
    sink.emit(json!({"type":"response","terminalToolCallId":"one","terminalThread":{"mode":"always"},"text":"selected output"})).await.unwrap();
    sink.emit(terminal("two", "always", "second"))
        .await
        .unwrap();
    let messages = f.sent.lock().await;
    assert_eq!(
        messages.len(),
        5,
        "root, rejected navigation, two commands and output: {messages:?}"
    );
    for m in &messages[2..] {
        assert_eq!(
            m["m.relates_to"]["event_id"], "$original",
            "output must stay discoverable: {m}"
        );
    }
    assert_eq!(
        messages
            .iter()
            .filter(|m| m.get("m.relates_to").is_none())
            .count(),
        1,
        "must not recreate a root"
    );
}
#[tokio::test]
async fn terminal_failed_root_publication_falls_back_without_retrying_the_root() {
    let f = Fixture::start().await;
    let dir = tempfile::tempdir().unwrap();
    let sink = f.sink(dir.path(), true).await;
    f.fail.store(true, std::sync::atomic::Ordering::SeqCst);
    sink.emit(terminal("one", "always", "first")).await.unwrap();
    sink.emit(terminal("two", "always", "second"))
        .await
        .unwrap();
    let messages = f.sent.lock().await;
    assert_eq!(
        messages.len(),
        3,
        "failed root must not be recreated: {messages:?}"
    );
    assert_eq!(messages[1]["m.relates_to"]["event_id"], "$original");
    assert_eq!(messages[2]["m.relates_to"]["event_id"], "$original");
}
#[tokio::test]
async fn terminal_failure_alert_is_kept_in_original_conversation_once() {
    let f = Fixture::start().await;
    let dir = tempfile::tempdir().unwrap();
    let sink = f.sink(dir.path(), true).await;
    sink.emit(terminal("one", "always", "command"))
        .await
        .unwrap();
    let failure = json!({"type":"terminal_failure","terminalToolCallId":"one"});
    sink.emit(failure.clone()).await.unwrap();
    sink.emit(failure).await.unwrap();
    let messages = f.sent.lock().await;
    assert_eq!(
        messages.len(),
        4,
        "root, link, command, single alert: {messages:?}"
    );
    assert_eq!(messages[3]["m.relates_to"]["event_id"], "$original");
    assert!(messages[3]["body"].as_str().unwrap().contains("failed"));
}
#[tokio::test]
async fn terminal_failed_edit_fallback_remains_in_the_activity_thread() {
    let f = Fixture::start().await;
    let dir = tempfile::tempdir().unwrap();
    let sink = f.sink(dir.path(), false).await;
    sink.emit(terminal("one", "always", "first")).await.unwrap();
    f.fail.store(true, std::sync::atomic::Ordering::SeqCst);
    sink.emit(terminal("one", "always", "updated"))
        .await
        .unwrap();
    let messages = f.sent.lock().await;
    assert_eq!(messages.len(), 4);
    assert_eq!(messages[2]["m.relates_to"]["rel_type"], "m.replace");
    assert_eq!(messages[3]["m.relates_to"]["event_id"], "$sent-1");
}
#[tokio::test]
async fn terminal_owned_clones_share_one_root_but_separate_requests_do_not() {
    let f = Fixture::start().await;
    let dir = tempfile::tempdir().unwrap();
    let sink = f.sink(&dir.path().join("one"), false).await;
    let clone = sink.owned().unwrap();
    let (a, b) = tokio::join!(
        sink.emit(terminal("one", "always", "first")),
        clone.emit(terminal("two", "always", "second"))
    );
    a.unwrap();
    b.unwrap();
    let second = f.sink(&dir.path().join("two"), false).await;
    second
        .emit(terminal("one", "always", "separate request"))
        .await
        .unwrap();
    let messages = f.sent.lock().await;
    assert_eq!(
        messages
            .iter()
            .filter(|m| m.get("m.relates_to").is_none())
            .count(),
        2
    );
    assert_eq!(messages[1]["m.relates_to"]["event_id"], "$sent-1");
    assert_eq!(messages[2]["m.relates_to"]["event_id"], "$sent-1");
    assert_eq!(messages[4]["m.relates_to"]["event_id"], "$sent-4");
}
#[tokio::test]
async fn terminal_modes_preserve_nonterminal_tools_and_final_responses() {
    for style in ["events", "status", "trace", "hermes"] {
        let messages = runtime_case("always", style, true).await;
        let roots: Vec<_> = messages
            .iter()
            .enumerate()
            .filter(|(_, m)| {
                m["body"]
                    .as_str()
                    .is_some_and(|s| s.starts_with("Terminal activity for "))
            })
            .collect();
        assert_eq!(roots.len(), 1, "{style}: {messages:?}");
        let root = format!("$sent-{}", roots[0].0 + 1);
        for m in messages.iter().filter(|m| {
            m["body"].as_str().is_some_and(|s| {
                s.contains("nonterminal-file") || s.contains("Final fixture answer")
            })
        }) {
            assert_ne!(m["m.relates_to"]["event_id"], root, "{style}: {m}");
        }
        let third = messages
            .iter()
            .find(|m| m["body"] == "third-output")
            .unwrap();
        assert_eq!(third["m.relates_to"]["event_id"], root);
    }
    let messages = runtime_case("off", "hermes", true).await;
    assert!(messages.iter().all(|m| m.get("m.relates_to").is_some()));
    let messages = runtime_case("auto", "off", false).await;
    let third = messages
        .iter()
        .find(|m| m["body"] == "third-output")
        .unwrap();
    let first = messages
        .iter()
        .find(|m| m["body"] == "first-output")
        .unwrap();
    // Calls can be hidden without losing the count or exposing their arguments.
    assert!(
        messages
            .iter()
            .all(|m| !m["body"].as_str().unwrap().contains("third-command"))
    );
    assert_eq!(
        third["m.relates_to"]["event_id"],
        first["m.relates_to"]["event_id"]
    );
}
fn terminal(id: &str, mode: &str, text: &str) -> Value {
    json!({"type":"activity","activityId":id,"terminalToolCallId":id,"terminalThread":{"mode":mode,"threshold":3},"message":text})
}
#[tokio::test]
async fn terminal_always_uses_new_unthreaded_root_and_preserves_original_conversation() {
    let f = Fixture::start().await;
    let dir = tempfile::tempdir().unwrap();
    let sink = f.sink(dir.path(), true).await;
    sink.emit(terminal("one", "always", "printf one"))
        .await
        .unwrap();
    sink.emit(json!({"type":"response","text":"Final answer"}))
        .await
        .unwrap();
    let messages = f.sent.lock().await;
    assert!(
        messages[0].get("m.relates_to").is_none(),
        "terminal root must not be a nested thread: {messages:?}"
    );
    assert_eq!(
        messages.len(),
        4,
        "root, navigation, command, final: {messages:?}"
    );
    assert_eq!(messages[1]["m.relates_to"]["event_id"], "$original");
    assert!(messages[1]["body"].as_str().unwrap().contains("$sent-1"));
    assert_eq!(messages[2]["m.relates_to"]["event_id"], "$sent-1");
    assert_eq!(messages[3]["m.relates_to"]["event_id"], "$original");
    assert_eq!(sink.matrix_raw_thread().as_deref(), Some("$original"));
}
