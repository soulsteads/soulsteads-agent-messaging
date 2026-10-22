//! Public WebSocket coverage for the owned OpenCode native REST/SSE adapter.
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use soulsteads_messaging::{config, runtime::Bridge, web};
use std::{collections::BTreeMap, future::IntoFuture, path::Path, sync::Arc, time::Duration};

async fn collect_until_done(
    socket: &mut tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
) -> Vec<Value> {
    tokio::time::timeout(Duration::from_secs(5), async {
        let mut events = Vec::new();
        loop {
            let event: Value = serde_json::from_str(
                socket
                    .next()
                    .await
                    .expect("WebSocket must remain connected")
                    .expect("WebSocket frame must be valid")
                    .to_text()
                    .expect("server replies must be text"),
            )
            .expect("server reply must be JSON");
            let done = event["type"] == "done";
            events.push(event);
            if done {
                return events;
            }
        }
    })
    .await
    .expect("WebSocket turn must finish")
}

async fn wait_for_file(path: &Path) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while !path.is_file() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("native fixture did not reach its durable marker");
}

async fn send(
    socket: &mut tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
    text: &str,
) -> Vec<Value> {
    socket
        .send(json!({"type":"message","text":text}).to_string().into())
        .await
        .expect("WebSocket prompt write must succeed");
    collect_until_done(socket).await
}

fn event_text(events: &[Value]) -> String {
    events
        .iter()
        .map(Value::to_string)
        .collect::<Vec<_>>()
        .join("\n")
}

fn position(trace: &str, expected: &str) -> usize {
    trace
        .find(expected)
        .unwrap_or_else(|| panic!("missing {expected:?} from public WebSocket trace: {trace}"))
}

fn native_state(root: &Path) -> Value {
    serde_json::from_slice(
        &std::fs::read(root.join("native-state.json"))
            .expect("fixture must persist native durable state"),
    )
    .expect("fixture durable state must be JSON")
}

#[tokio::test]
async fn public_websocket_drives_owned_native_rest_sse_prompt_compaction_and_advertised_command() {
    let root = tempfile::tempdir().expect("temporary native root");
    for path in [
        "home",
        "tmp",
        "xdg-config",
        "xdg-cache",
        "xdg-data",
        "workspace",
    ] {
        std::fs::create_dir(root.path().join(path)).expect("isolated native directory");
    }
    let configuration = config::load(
        &json!({
            "rateLimitSeconds": 0,
            "sessionStorePath": root.path().join("store.json"),
            "toolMessages": {"mode":"hermes","showArguments":true,"showOutputFor":["bash"]},
            "acp": {
                "transport": "native",
                "command": "/usr/bin/python3",
                "args": [concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/native_api.py")],
                "env": {
                    "NATIVE_FIXTURE_ROOT": root.path(),
                    "HOME": root.path().join("home"),
                    "TMPDIR": root.path().join("tmp"),
                    "XDG_CONFIG_HOME": root.path().join("xdg-config"),
                    "XDG_CACHE_HOME": root.path().join("xdg-cache"),
                    "XDG_DATA_HOME": root.path().join("xdg-data"),
                    "PATH": "/usr/bin:/bin"
                }
            },
            "web": {"allowedOrigins":["http://127.0.0.1"]}
        })
        .to_string(),
        &BTreeMap::new(),
    )
    .expect("native Web configuration must load");
    let bridge = Arc::new(
        Bridge::new(
            configuration.clone(),
            BTreeMap::new(),
            root.path().join("workspace"),
        )
        .expect("bridge must start"),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("loopback listener");
    let address = listener.local_addr().expect("loopback address");
    let server = tokio::spawn(
        axum::serve(
            listener,
            web::router(bridge.clone(), configuration["web"].clone()),
        )
        .into_future(),
    );
    let (mut socket, _) =
        tokio_tungstenite::connect_async(format!("ws://{address}/ws?clientId=native-api-driver"))
            .await
            .expect("public WebSocket must accept loopback client");
    let connected: Value = serde_json::from_str(
        socket
            .next()
            .await
            .expect("connected frame")
            .expect("connected frame is valid")
            .to_text()
            .expect("connected frame is text"),
    )
    .expect("connected frame is JSON");
    assert_eq!(connected["type"], "connected");

    socket
        .send(
            json!({"type":"message","text":"terminal-output"})
                .to_string()
                .into(),
        )
        .await
        .expect("public prompt write must succeed");
    wait_for_file(&root.path().join("started")).await;
    assert!(
        !root.path().join("release").exists(),
        "fixture must still be held before the prompt release"
    );
    let mut prompt = tokio::time::timeout(Duration::from_secs(5), async {
        let mut events = Vec::new();
        loop {
            let message = socket.next().await.unwrap().unwrap();
            let event: Value = serde_json::from_str(message.to_text().unwrap()).unwrap();
            assert!(
                !matches!(
                    event["type"].as_str(),
                    Some("error" | "prompt_end" | "done")
                ),
                "native terminal is still held: {event:?}"
            );
            let last_tool_output = event.to_string().contains("native-output-2");
            events.push(event);
            if last_tool_output {
                return events;
            }
        }
    })
    .await
    .expect("native tool output must reach WebSocket before terminal release");
    std::fs::write(root.path().join("release"), "release")
        .expect("release the basic native prompt");
    prompt.extend(collect_until_done(&mut socket).await);
    let prompt_trace = event_text(&prompt);
    let mut prior = 0;
    for expected in [
        "native-command-0",
        "native-output-0",
        "native-command-1",
        "native-output-1",
        "native-command-2",
        "native-output-2",
        "reply:terminal-output",
    ] {
        let next = position(&prompt_trace, expected);
        assert!(
            next >= prior,
            "{expected} is out of terminal order: {prompt_trace}"
        );
        prior = next;
    }
    assert!(
        prompt
            .iter()
            .any(|event| event["type"] == "prompt_end" && event["failed"] == false),
        "native prompt must have a successful public terminal receipt: {prompt:?}"
    );

    let compact = send(&mut socket, "/compact").await;
    assert!(
        compact.iter().all(|event| event["type"] != "error"
            && !(event["type"] == "session_state" && event["state"] == "stale-invalidated")),
        "confirmed compaction must preserve the session: {compact:?}"
    );
    assert!(
        event_text(&compact).contains("Context compacted"),
        "confirmed compaction must acknowledge success: {compact:?}"
    );
    assert!(
        compact
            .iter()
            .any(|event| event["type"] == "prompt_end" && event["failed"] == false),
        "native compaction must finish on the public WebSocket: {compact:?}"
    );
    let command = send(&mut socket, "/fixture argument").await;
    assert!(
        event_text(&command).contains("reply:argument"),
        "advertised native command must return its fixture result: {command:?}"
    );
    assert!(
        command
            .iter()
            .any(|event| event["type"] == "prompt_end" && event["failed"] == false),
        "advertised command must have a successful public terminal receipt: {command:?}"
    );

    socket.close(None).await.expect("close public socket");
    bridge
        .shutdown()
        .await
        .expect("owned native bridge shutdown");
    server.abort();

    let state = native_state(root.path());
    let session = state["sessions"]
        .as_object()
        .expect("fixture sessions object")
        .values()
        .next()
        .expect("one fixture session");
    assert_eq!(
        session["submissions"], 1,
        "basic prompt must not be replayed"
    );
    assert_eq!(
        session["compact"], true,
        "/compact must use native compact endpoint"
    );
    assert_eq!(
        session["command"], "fixture",
        "only advertised commands may be submitted"
    );
    assert_eq!(
        session["messages"]
            .as_array()
            .expect("fixture messages")
            .iter()
            .filter(|message| message["type"] == "user" && message["text"] == "terminal-output")
            .count(),
        1,
        "durable fixture history must contain exactly one public prompt"
    );
}
