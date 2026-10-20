use async_trait::async_trait;
use soulsteads_messaging::{
    config,
    runtime::{Bridge, Event, Sink},
    web,
};
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use std::{collections::BTreeMap, future::IntoFuture, sync::Arc, time::Duration};
use tokio::sync::Mutex;

#[derive(Default)]
struct Recorder(Mutex<Vec<Value>>);
#[async_trait]
impl Sink for Recorder {
    async fn emit(&self, value: Value) -> anyhow::Result<()> {
        self.0.lock().await.push(value);
        Ok(())
    }
}
fn configuration(path: &std::path::Path) -> Value {
    config::load(&json!({"rateLimitSeconds":0,"sessionStorePath":path.join("store.json"),"acp":{"command":env!("CARGO_BIN_EXE_acp-fixture"),"args":[]},"web":{"allowedOrigins":["https://local.test"]}}).to_string(), &BTreeMap::new()).unwrap()
}

#[tokio::test]
async fn web_completed_stream_has_one_text_delivery_and_commands_do_not_inherit_turn_state() {
    use base64::Engine;
    let directory = tempfile::tempdir().unwrap();
    let mut bytes = std::io::Cursor::new(Vec::new());
    image::DynamicImage::new_rgb8(1, 1)
        .write_to(&mut bytes, image::ImageFormat::Png)
        .unwrap();
    let data = base64::engine::general_purpose::STANDARD.encode(bytes.into_inner());
    let image_update = json!({"update":{"sessionUpdate":"agent_message_chunk","content":{"type":"image","mimeType":"image/png","data":data}}});
    let script = directory.path().join("script.json");
    std::fs::write(&script, json!([
        {"updates":[
            {"update":{"sessionUpdate":"agent_message_chunk","content":{"type":"text","text":"first "}}},
            {"update":{"sessionUpdate":"agent_message_chunk","content":{"type":"text","text":"answer"}}}
        ],"stopReason":"end_turn"},
        {"text":"second answer","stopReason":"end_turn"},
        {"updates":[image_update.clone()],"text":"mixed answer","stopReason":"end_turn"},
        {"updates":[image_update],"stopReason":"end_turn"},
        // Empty Web completion now consumes exactly two bounded attempts.
        {"stopReason":"end_turn"},
        {"stopReason":"end_turn"},
        {"error":true},
        {"text":"recovered answer","stopReason":"end_turn"}
    ]).to_string()).unwrap();
    let mut config = configuration(directory.path());
    config["acp"]["env"] = json!({"ACP_FIXTURE_MATRIX_SCRIPT":script,"ACP_FIXTURE_MATRIX_COUNTER":directory.path().join("counter")});
    let bridge = Arc::new(
        Bridge::new(
            config.clone(),
            BTreeMap::new(),
            directory.path().join("sessions"),
        )
        .unwrap(),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(
        axum::serve(listener, web::router(bridge.clone(), config["web"].clone())).into_future(),
    );
    let (mut socket, _) =
        tokio_tungstenite::connect_async(format!("ws://{address}/ws?clientId=wire-contract"))
            .await
            .unwrap();
    socket.next().await.unwrap().unwrap();
    let mut failures = Vec::new();
    for (input, expected, images, command) in [
        ("first", "first answer", 0, false),
        ("/status", "", 0, true),
        ("second", "second answer", 0, false),
        ("mixed", "mixed answer", 1, false),
        ("image only", "", 1, false),
        ("empty", "The AI returned no text response.", 0, false),
        ("error", "", 0, false),
        ("recovery", "recovered answer", 0, false),
        ("/status", "", 0, true),
    ] {
        socket
            .send(json!({"type":"message","text":input}).to_string().into())
            .await
            .unwrap();
        let mut events = Vec::new();
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let item: Value =
                    serde_json::from_str(socket.next().await.unwrap().unwrap().to_text().unwrap())
                        .unwrap();
                let done = item["type"] == "done";
                events.push(item);
                if done {
                    break;
                }
            }
        })
        .await
        .unwrap();
        let responses = events
            .iter()
            .filter(|item| item["type"] == "response")
            .collect::<Vec<_>>();
        let chunks = events
            .iter()
            .filter(|item| item["type"] == "chunk")
            .filter_map(|item| item["text"].as_str())
            .collect::<String>();
        assert_eq!(
            events.iter().filter(|item| item["type"] == "image").count(),
            images,
            "{input}"
        );
        if command {
            assert_eq!(responses.len(), 1);
            assert!(chunks.is_empty());
            assert!(!responses[0]["text"].as_str().unwrap().is_empty());
        } else if input == "error" || input == "empty" {
            assert!(events.iter().any(|item| item["type"] == "error"));
            if input == "empty" {
                assert!(
                    events.iter().any(|item| item["type"] == "session_state"
                        && item["state"] == "stale-invalidated")
                );
                assert!(events.iter().any(|item| item["type"] == "error" && item["message"] == "The AI service completed without returning a usable response. Please try again."));
                assert!(responses.is_empty());
            }
            assert!(
                events
                    .iter()
                    .any(|item| item["type"] == "prompt_end" && item["failed"] == true)
            );
        } else if !chunks.is_empty() {
            assert_eq!(chunks, expected);
            if !responses.is_empty() {
                failures.push(format!(
                    "{input}: streamed text followed by {} full responses",
                    responses.len()
                ));
            }
        } else if images > 0 {
            if !responses.is_empty() {
                failures.push(format!("{input}: empty response after image"));
            }
        } else {
            assert_eq!(responses.len(), 1);
            assert_eq!(responses[0]["text"], expected);
        }
    }
    socket.close(None).await.unwrap();
    bridge.shutdown().await.unwrap();
    server.abort();
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

#[tokio::test]
async fn actual_web_sink_upload_preserves_inline_bytes_alt_and_prompt_end_model() {
    use base64::Engine;
    let directory = tempfile::tempdir().unwrap();
    let mut bytes = std::io::Cursor::new(Vec::new());
    image::DynamicImage::new_rgb8(1, 1)
        .write_to(&mut bytes, image::ImageFormat::Png)
        .unwrap();
    let data = base64::engine::general_purpose::STANDARD.encode(bytes.into_inner());
    let script = directory.path().join("script.json");
    std::fs::write(&script, json!([{"updates":[{"update":{"sessionUpdate":"agent_message_chunk","content":{"type":"image","mimeType":"image/png","data":data,"alt":"Source caption"}}}],"text":"after image","stopReason":"end_turn"}]).to_string()).unwrap();
    let mut config = configuration(directory.path());
    config["acp"]["env"] = json!({"ACP_FIXTURE_MATRIX_SCRIPT":script,"ACP_FIXTURE_MATRIX_COUNTER":directory.path().join("counter")});
    let bridge = Arc::new(
        Bridge::new(
            config.clone(),
            BTreeMap::new(),
            directory.path().join("sessions"),
        )
        .unwrap(),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(
        axum::serve(listener, web::router(bridge.clone(), config["web"].clone())).into_future(),
    );
    let (mut socket, _) =
        tokio_tungstenite::connect_async(format!("ws://{address}/ws?clientId=inline-image"))
            .await
            .unwrap();
    socket.next().await.unwrap().unwrap();
    socket
        .send(json!({"type":"message","text":"hello"}).to_string().into())
        .await
        .unwrap();
    let mut events = vec![];
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let item: Value =
                serde_json::from_str(socket.next().await.unwrap().unwrap().to_text().unwrap())
                    .unwrap();
            let done = item["type"] == "done";
            events.push(item);
            if done {
                break;
            }
        }
    })
    .await
    .unwrap();
    let images = events
        .iter()
        .filter(|item| item["type"] == "image")
        .collect::<Vec<_>>();
    assert_eq!(images.len(), 1);
    assert_eq!(images[0]["data"], data);
    assert_eq!(images[0]["alt"], "Source caption");
    let uploaded = events
        .iter()
        .position(|item| item["type"] == "image")
        .unwrap();
    let chunk = events
        .iter()
        .position(|item| item["type"] == "chunk")
        .unwrap();
    assert!(uploaded < chunk);
    assert!(events.iter().any(|item| item["type"] == "prompt_end"
        && item["model"] == "one"
        && item["uploadedImages"] == 1
        && item["uploadedMedia"] == 1
        && item["uploadFailures"] == 0
        && item["usableOutput"] == true
        && item["deliveryFailed"] == false));
    socket.close(None).await.unwrap();
    bridge.shutdown().await.unwrap();
    server.abort();
}
#[tokio::test]
async fn shared_runtime_uses_real_acp_and_authorization_dedup() {
    let directory = tempfile::tempdir().unwrap();
    let mut config = configuration(directory.path());
    config["matrix"]["allowedUsers"] = json!(["allowed"]);
    let bridge = Bridge::new(config, BTreeMap::new(), directory.path().join("sessions")).unwrap();
    let recorder = Recorder::default();
    let mut event = Event::web("allowed", "!oc hello");
    event.platform = "matrix".into();
    event.id = "first".into();
    event.direct = false;
    bridge.handle(event.clone(), &recorder).await.unwrap();
    assert!(
        recorder
            .0
            .lock()
            .await
            .iter()
            .any(|item| item["type"] == "response" && item["text"] == "hello world")
    );
    let count = recorder.0.lock().await.len();
    bridge.handle(event.clone(), &recorder).await.unwrap();
    assert_eq!(recorder.0.lock().await.len(), count);
    event.user = "denied".into();
    event.id = "other".into();
    bridge.handle(event, &recorder).await.unwrap();
    assert_eq!(recorder.0.lock().await.len(), count);
    bridge.shutdown().await.unwrap();
}
#[tokio::test]
async fn web_replacement_socket_survives_old_disconnect_and_receives_active_result() {
    let directory = tempfile::tempdir().unwrap();
    let script = directory.path().join("script.json");
    std::fs::write(&script, json!([
        {"updates":[{"update":{"sessionUpdate":"agent_message_chunk","content":{"type":"text","text":"old partial"}}}],"waitForFollowup":true},
        {"text":"fresh turn","stopReason":"end_turn"}
    ]).to_string()).unwrap();
    let mut config = configuration(directory.path());
    config["acp"]["env"] = json!({"ACP_FIXTURE_MATRIX_SCRIPT":script,"ACP_FIXTURE_MATRIX_COUNTER":directory.path().join("counter")});
    let bridge = Arc::new(
        Bridge::new(
            config.clone(),
            BTreeMap::new(),
            directory.path().join("sessions"),
        )
        .unwrap(),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(
        axum::serve(listener, web::router(bridge.clone(), config["web"].clone())).into_future(),
    );
    let (mut old, _) =
        tokio_tungstenite::connect_async(format!("ws://{address}/ws?clientId=replacement"))
            .await
            .unwrap();
    old.next().await.unwrap().unwrap();
    old.send(json!({"type":"message","text":"wait"}).to_string().into())
        .await
        .unwrap();
    loop {
        let event: Value =
            serde_json::from_str(old.next().await.unwrap().unwrap().to_text().unwrap()).unwrap();
        if event["type"] == "chunk" {
            assert_eq!(event["text"], "old partial");
            break;
        }
    }
    old.send(
        json!({"type":"message","text":"/status"})
            .to_string()
            .into(),
    )
    .await
    .unwrap();
    let mut status_response = false;
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let event: Value =
                serde_json::from_str(old.next().await.unwrap().unwrap().to_text().unwrap())
                    .unwrap();
            if event["type"] == "response" {
                status_response = true;
            }
            if event["type"] == "done" {
                break;
            }
        }
    })
    .await
    .unwrap();
    assert!(
        status_response,
        "concurrent command must not inherit active streaming state"
    );
    let (mut new, _) =
        tokio_tungstenite::connect_async(format!("ws://{address}/ws?clientId=replacement"))
            .await
            .unwrap();
    new.next().await.unwrap().unwrap();
    old.close(None).await.unwrap();
    new.send(
        json!({"type":"message","text":"/cancel"})
            .to_string()
            .into(),
    )
    .await
    .unwrap();
    let mut received_cancelled = false;
    let mut completed = 0;
    let mut cancel_ack = false;
    tokio::time::timeout(Duration::from_secs(3), async {
        while let Some(Ok(message)) = new.next().await {
            let event: Value = serde_json::from_str(message.to_text().unwrap()).unwrap();
            assert_ne!(
                event["text"], "old partial",
                "replacement must not replay old chunks"
            );
            if event["type"] == "response" && event["text"] == "Cancellation requested." {
                cancel_ack = true;
            }
            // A cancelled turn with usable partial text preserves that text;
            // it does not fabricate a second cancellation answer or replay it
            // to the replacement socket. Completion is the terminal receipt.
            if event["type"] == "prompt_end" && event["stopReason"] == "cancelled" {
                received_cancelled = true;
            }
            if event["type"] == "done" {
                completed += 1;
            }
            if completed == 2 {
                break;
            }
        }
    })
    .await
    .unwrap();
    assert!(received_cancelled);
    assert!(cancel_ack);
    assert_eq!(completed, 2);
    new.send(
        json!({"type":"message","text":"next prompt"})
            .to_string()
            .into(),
    )
    .await
    .unwrap();
    let mut next_text = String::new();
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let event: Value =
                serde_json::from_str(new.next().await.unwrap().unwrap().to_text().unwrap())
                    .unwrap();
            assert_ne!(
                event["type"], "response",
                "new turn must not replay full text"
            );
            if event["type"] == "chunk" {
                next_text.push_str(event["text"].as_str().unwrap());
            }
            if event["type"] == "done" {
                break;
            }
        }
    })
    .await
    .unwrap();
    assert_eq!(next_text, "fresh turn");
    new.close(None).await.unwrap();
    bridge.shutdown().await.unwrap();
    server.abort();
}

#[tokio::test]
async fn web_http_origin_widget_websocket_acp_and_reconnect() {
    let directory = tempfile::tempdir().unwrap();
    let config = configuration(directory.path());
    let bridge = Arc::new(
        Bridge::new(
            config.clone(),
            BTreeMap::new(),
            directory.path().join("sessions"),
        )
        .unwrap(),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let task = tokio::spawn(
        axum::serve(listener, web::router(bridge.clone(), config["web"].clone())).into_future(),
    );
    let http = reqwest::Client::new();
    let preflight = http
        .request(
            reqwest::Method::OPTIONS,
            format!("http://{address}/widget.js"),
        )
        .header("Origin", "https://local.test")
        .send()
        .await
        .unwrap();
    assert_eq!(preflight.status(), 204);
    assert_eq!(
        preflight.headers()["access-control-allow-origin"],
        "https://local.test"
    );
    let denied_cors = http
        .get(format!("http://{address}/widget.js"))
        .header("Origin", "https://evil.test")
        .send()
        .await
        .unwrap();
    assert!(
        denied_cors
            .headers()
            .get("access-control-allow-origin")
            .is_none()
    );
    assert_eq!(
        http.get(format!("http://{address}/health"))
            .send()
            .await
            .unwrap()
            .json::<Value>()
            .await
            .unwrap()["status"],
        "ok"
    );
    let widget = http
        .get(format!("http://{address}/widget.js"))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(widget.contains("WebSocket"));
    assert!(widget.contains("window.OpenCodeWidgetServerConfig ="));
    let page = http
        .get(format!("http://{address}/chat"))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(page.contains("window.OpenCodeWidget ="));
    assert!(!page.contains("OpenCodeChat.init"));
    use tokio_tungstenite::tungstenite::client::IntoClientRequest;
    let mut denied = format!("ws://{address}/ws").into_client_request().unwrap();
    denied
        .headers_mut()
        .insert("Origin", "https://evil.test".parse().unwrap());
    assert!(tokio_tungstenite::connect_async(denied).await.is_err());
    for _ in 0..2 {
        let mut request = format!("ws://{address}/ws?clientId=test-context")
            .into_client_request()
            .unwrap();
        request
            .headers_mut()
            .insert("Origin", "https://local.test".parse().unwrap());
        let (mut socket, _) = tokio_tungstenite::connect_async(request).await.unwrap();
        let hello: Value =
            serde_json::from_str(socket.next().await.unwrap().unwrap().to_text().unwrap()).unwrap();
        assert_eq!(hello["type"], "connected");
        socket
            .send(json!({"type":"message","text":"hello"}).to_string().into())
            .await
            .unwrap();
        let mut response = false;
        loop {
            let next = tokio::time::timeout(Duration::from_secs(5), socket.next())
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            let value: Value = serde_json::from_str(next.to_text().unwrap()).unwrap();
            if value["type"] == "chunk" {
                response = true;
            }
            if value["type"] == "response" {
                panic!("normal Web stream must not also send a full response: {value}");
            }
            if value["type"] == "done" {
                break;
            }
        }
        assert!(response);
        socket.close(None).await.unwrap();
    }
    bridge.shutdown().await.unwrap();
    task.abort();
}
