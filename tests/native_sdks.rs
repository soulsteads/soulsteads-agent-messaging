#![cfg(feature = "platform-sdks")]
use axum::{Json, Router, extract::Request, routing::any};
use soulsteads_messaging::{
    config,
    connectors::{matrix, whatsapp},
    runtime::Bridge,
};
use futures_util::StreamExt;

#[test]
fn whatsapp_pairing_qr_is_local_private_svg_without_token_text() {
    let directory = tempfile::tempdir().unwrap();
    whatsapp::write_pairing_qr(directory.path(), "offline-pairing-fixture").unwrap();
    let path = directory.path().join("pairing-qr.svg");
    let svg = std::fs::read_to_string(&path).unwrap();
    assert!(svg.contains("<svg"));
    assert!(!svg.contains("offline-pairing-fixture"));
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(
            std::fs::metadata(path).unwrap().permissions().mode() & 0o077,
            0
        );
    }
}

#[tokio::test]
async fn matrix_saved_non_auth_error_preserves_token_and_never_password_relogs() {
    use axum::response::IntoResponse;
    let logins = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let counter = logins.clone();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let router = Router::new().fallback(any(move |request: Request| {
        let counter = counter.clone();
        async move {
            let path = request.uri().path();
            if path.ends_with("whoami") {
                return (
                    axum::http::StatusCode::FORBIDDEN,
                    Json(json!({"errcode":"M_FORBIDDEN","error":"Fixture non-auth failure"})),
                )
                    .into_response();
            }
            if path.ends_with("login") {
                counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            }
            Json(json!({"versions":["v1.12"],"unstable_features":{}})).into_response()
        }
    }));
    let server = tokio::spawn(axum::serve(listener, router).into_future());
    let directory = tempfile::tempdir().unwrap();
    std::fs::write(directory.path().join("access_token"), "saved-fixture").unwrap();
    let configuration = json!({"homeserver":base,"userId":"@bot:local","deviceId":"LOCAL","password":"fixture-password"});
    assert!(
        matrix::connect(&configuration, directory.path())
            .await
            .is_err()
    );
    assert_eq!(logins.load(std::sync::atomic::Ordering::SeqCst), 0);
    assert_eq!(
        std::fs::read_to_string(directory.path().join("access_token")).unwrap(),
        "saved-fixture"
    );
    server.abort();
}
use serde_json::{Value, json};
use std::{collections::BTreeMap, future::IntoFuture, sync::Arc};
async fn homeserver(request: Request) -> Json<Value> {
    let path = request.uri().path();
    Json(if path.ends_with("versions") {
        json!({"versions":["v1.12"],"unstable_features":{}})
    } else if path.ends_with("sync") {
        json!({"next_batch":"batch-1","rooms":{},"device_lists":{"changed":[],"left":[]},"device_one_time_keys_count":{}})
    } else if path.ends_with("whoami") {
        json!({"user_id":"@bot:local","device_id":"LOCAL"})
    } else if path.ends_with("upload") {
        json!({"one_time_key_counts":{}})
    } else {
        json!({})
    })
}
#[tokio::test]
async fn matrix_maintained_sdk_restores_syncs_and_reopens_real_sqlite_store() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let server =
        tokio::spawn(axum::serve(listener, Router::new().fallback(any(homeserver))).into_future());
    let directory = tempfile::tempdir().unwrap();
    let config = json!({"homeserver":base,"userId":"@bot:local","deviceId":"LOCAL","accessToken":"fake-token","encryption":{"enabled":cfg!(feature = "matrix-encryption")}});
    for _ in 0..2 {
        let client = matrix::connect(&config, directory.path()).await.unwrap();
        assert_eq!(client.user_id().unwrap().as_str(), "@bot:local");
        let sync = client
            .sync_once(matrix_sdk::config::SyncSettings::default())
            .await
            .unwrap();
        assert_eq!(sync.next_batch, "batch-1");
        drop(client);
    }
    assert!(std::fs::read_dir(directory.path()).unwrap().count() > 0);
    server.abort();
}
#[tokio::test]
async fn whatsapp_native_sdk_builds_with_persistent_sqlite_without_live_connection() {
    let directory = tempfile::tempdir().unwrap();
    let config=config::load(&json!({"sessionStorePath":directory.path().join("store.json"),"whatsapp":{"authFolder":directory.path().join("auth")}}).to_string(),&BTreeMap::new()).unwrap();
    let bridge = Arc::new(
        Bridge::new(
            config.clone(),
            BTreeMap::new(),
            directory.path().join("sessions"),
        )
        .unwrap(),
    );
    let bot = whatsapp::build(&config["whatsapp"], bridge.clone())
        .await
        .unwrap();
    drop(bot);
    assert!(directory.path().join("auth/whatsapp.db").exists());
}

#[tokio::test]
async fn whatsapp_native_noise_handshake_uses_local_websocket_and_stops() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (received, saw) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut socket = tokio_tungstenite::accept_async(stream).await.unwrap();
        let message = tokio::time::timeout(std::time::Duration::from_secs(5), socket.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert!(message.is_binary());
        let data = message.into_data();
        assert!(
            data.len() > 32,
            "Native Noise handshake must contain an ephemeral public key"
        );
        let _ = received.send(());
        while socket.next().await.is_some() {}
    });
    let directory = tempfile::tempdir().unwrap();
    let config=config::load(&json!({"sessionStorePath":directory.path().join("store.json"),"whatsapp":{"authFolder":directory.path().join("auth")}}).to_string(),&BTreeMap::new()).unwrap();
    let bridge = Arc::new(
        Bridge::new(
            config.clone(),
            BTreeMap::new(),
            directory.path().join("sessions"),
        )
        .unwrap(),
    );
    let transport = whatsapp_rust::transport::TokioWebSocketTransportFactory::new()
        .with_url(format!("ws://{address}/ws"));
    let bot = whatsapp::build_with_transport(&config["whatsapp"], bridge, transport)
        .await
        .unwrap();
    let (sender, receiver) = tokio::sync::watch::channel(false);
    let task = tokio::spawn(whatsapp::run(bot, receiver));
    tokio::time::timeout(std::time::Duration::from_secs(5), saw)
        .await
        .unwrap()
        .unwrap();
    sender.send(true).unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(5), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    server.abort();
}

#[derive(Clone, Default)]
struct MatrixFixture {
    syncs: Arc<std::sync::atomic::AtomicUsize>,
    sent: Arc<tokio::sync::Mutex<Vec<Value>>>,
    requests: Arc<tokio::sync::Mutex<Vec<String>>>,
    keys: Arc<tokio::sync::Mutex<Value>>,
    encrypted: bool,
    body: String,
    content: Value,
    sender: String,
    members: usize,
    receipt_failure: bool,
    uploads: Arc<tokio::sync::Mutex<Vec<Vec<u8>>>>,
}
async fn matrix_transport(
    axum::extract::State(state): axum::extract::State<MatrixFixture>,
    request: Request,
) -> axum::response::Response {
    use axum::response::IntoResponse;
    let path = request.uri().path().to_owned();
    let body = axum::body::to_bytes(request.into_body(), 1024 * 1024)
        .await
        .unwrap();
    if !path.ends_with("sync") {
        state.requests.lock().await.push(path.clone());
    }
    if path.contains("/media/") && path.ends_with("upload") {
        state.uploads.lock().await.push(body.to_vec());
        return Json(json!({"content_uri":"mxc://local/fixture-image"})).into_response();
    }
    if path.contains("/media/") && path.ends_with("config") {
        return Json(json!({"m.upload.size":20971520})).into_response();
    }
    if path
        .trim_end_matches('/')
        .ends_with("/state/m.room.encryption")
    {
        return if state.encrypted {
            Json(json!({"algorithm":"m.megolm.v1.aes-sha2"})).into_response()
        } else {
            (
                axum::http::StatusCode::NOT_FOUND,
                Json(json!({"errcode":"M_NOT_FOUND","error":"Room is not encrypted"})),
            )
                .into_response()
        };
    }
    let result = if path.ends_with("versions") {
        json!({"versions":["v1.12"],"unstable_features":{}})
    } else if path.ends_with("sync") {
        tokio::time::sleep(std::time::Duration::from_millis(30)).await;
        // The first sync establishes state without admitting historical input.
        // Deliver this fixture's new user message on the next incremental sync.
        let first = state
            .syncs
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
            == 1;
        let mut events = vec![
            json!({"type":"m.room.create","event_id":"$create","origin_server_ts":1,"state_key":"","sender":"@bot:local","content":{"creator":"@bot:local","room_version":"10"}}),
            json!({"type":"m.room.member","event_id":"$member","origin_server_ts":1,"state_key":"@bot:local","sender":"@bot:local","content":{"membership":"join"}}),
        ];
        if state.encrypted {
            events.push(json!({"type":"m.room.encryption","event_id":"$encryption","origin_server_ts":1,"state_key":"","sender":"@bot:local","content":{"algorithm":"m.megolm.v1.aes-sha2"}}));
        }
        let body = if state.body.is_empty() {
            "!oc hello"
        } else {
            &state.body
        };
        let mut content = json!({"msgtype":"m.text","body":body});
        if let Some(extra) = state.content.as_object() {
            content.as_object_mut().unwrap().extend(extra.clone());
        }
        let sender = if state.sender.is_empty() {
            "@human:local"
        } else {
            &state.sender
        };
        json!({"next_batch":format!("batch-{}",state.syncs.load(std::sync::atomic::Ordering::SeqCst)),"rooms":{"join":{"!room:local":{"state":{"events":events},"timeline":{"events":if first{vec![json!({"type":"m.room.message","event_id":"$initial","sender":sender,"origin_server_ts":chrono::Utc::now().timestamp_millis(),"content":content})]}else{vec![]},"limited":false,"prev_batch":"previous"},"ephemeral":{"events":[]},"account_data":{"events":[]}}}},"device_lists":{"changed":[],"left":[]},"device_one_time_keys_count":{"signed_curve25519":50}})
    } else if path.contains("/send/") {
        let mut sent = state.sent.lock().await;
        sent.push(serde_json::from_slice(&body).unwrap());
        json!({"event_id":format!("$outbound-{}", sent.len())})
    } else if path.ends_with("joined_members") {
        let mut joined = serde_json::Map::new();
        joined.insert("@bot:local".into(), json!({"display_name":"bot"}));
        for index in 1..state.members {
            joined.insert(
                format!("@member{index}:local"),
                json!({"display_name":format!("member{index}")}),
            );
        }
        json!({"joined":joined})
    } else if path.ends_with("members") {
        let mut members = vec![
            json!({"type":"m.room.member","state_key":"@bot:local","sender":"@bot:local","event_id":"$member","content":{"membership":"join"},"origin_server_ts":1}),
        ];
        for index in 1..state.members {
            members.push(json!({"type":"m.room.member","state_key":format!("@member{index}:local"),"sender":"@bot:local","event_id":format!("$member{index}"),"content":{"membership":"join"},"origin_server_ts":1}));
        }
        json!({"chunk":members})
    } else if path.contains("/receipt/") && state.receipt_failure {
        return (
            axum::http::StatusCode::FORBIDDEN,
            Json(json!({"errcode":"M_FORBIDDEN","error":"receipt refused"})),
        )
            .into_response();
    } else if path.ends_with("query") {
        let request: Value = serde_json::from_slice(&body).unwrap();
        let mut keys = serde_json::Map::new();
        let own_keys = state.keys.lock().await.clone();
        for user in request["device_keys"]
            .as_object()
            .into_iter()
            .flatten()
            .map(|(user, _)| user)
        {
            keys.insert(
                user.clone(),
                if user == "@bot:local" && !own_keys.is_null() {
                    json!({"LOCAL":own_keys})
                } else {
                    json!({})
                },
            );
        }
        json!({"device_keys":keys,"failures":{}})
    } else if path.ends_with("claim") {
        json!({"one_time_keys":{},"failures":{}})
    } else if path.ends_with("whoami") {
        json!({"user_id":"@bot:local","device_id":"LOCAL"})
    } else if path.ends_with("upload") {
        let request: Value = serde_json::from_slice(&body).unwrap();
        if request["device_keys"].is_object() {
            *state.keys.lock().await = request["device_keys"].clone();
        }
        json!({"one_time_key_counts":{"signed_curve25519":50}})
    } else {
        json!({})
    };
    Json(result).into_response()
}

#[tokio::test]
async fn matrix_sdk_inline_image_upload_is_real_threaded_and_advances_caption_reply() {
    use base64::Engine;
    let directory = tempfile::tempdir().unwrap();
    let mut bytes = std::io::Cursor::new(Vec::new());
    image::DynamicImage::new_rgb8(1, 1)
        .write_to(&mut bytes, image::ImageFormat::Png)
        .unwrap();
    let bytes = bytes.into_inner();
    let script = directory.path().join("script.json");
    std::fs::write(&script, json!([{"updates":[{"update":{"sessionUpdate":"agent_message_chunk","content":{"type":"image","mimeType":"image/png","data":base64::engine::general_purpose::STANDARD.encode(&bytes)}}}],"text":"image caption","stopReason":"end_turn"}]).to_string()).unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let state = MatrixFixture::default();
    let server = tokio::spawn(
        axum::serve(
            listener,
            Router::new()
                .fallback(any(matrix_transport))
                .with_state(state.clone()),
        )
        .into_future(),
    );
    let configuration = config::load(&json!({"rateLimitSeconds":0,"sessionStorePath":directory.path().join("store.json"),"acp":{"command":env!("CARGO_BIN_EXE_acp-fixture"),"args":[],"env":{"ACP_FIXTURE_MATRIX_SCRIPT":script,"ACP_FIXTURE_MATRIX_COUNTER":directory.path().join("counter")}},"toolMessages":{"mode":"off"},"matrix":{"homeserver":base,"userId":"@bot:local","deviceId":"LOCAL","accessToken":"fake-token"}}).to_string(), &BTreeMap::new()).unwrap();
    let bridge = Arc::new(
        Bridge::new(
            configuration.clone(),
            BTreeMap::new(),
            directory.path().join("sessions"),
        )
        .unwrap(),
    );
    let client = matrix::connect(&configuration["matrix"], &directory.path().join("matrix"))
        .await
        .unwrap();
    let (shutdown, receiver) = tokio::sync::watch::channel(false);
    let task = tokio::spawn(matrix::run(client, bridge.clone(), receiver));
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while !state
            .sent
            .lock()
            .await
            .iter()
            .any(|content| content["body"] == "image caption")
        {
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    shutdown.send(true).unwrap();
    task.await.unwrap().unwrap();
    assert_eq!(
        *state.uploads.lock().await,
        vec![bytes],
        "requests={:?}; sent={:?}",
        state.requests.lock().await,
        state.sent.lock().await
    );
    let sent = state.sent.lock().await;
    let image_index = sent
        .iter()
        .position(|content| content["msgtype"] == "m.image")
        .unwrap();
    let image = &sent[image_index];
    assert_eq!(image["url"], "mxc://local/fixture-image");
    assert_eq!(image["body"], "Image");
    assert_eq!(image["m.relates_to"]["event_id"], "$initial");
    assert_eq!(
        image["m.relates_to"]["m.in_reply_to"]["event_id"],
        "$initial"
    );
    let caption = sent
        .iter()
        .find(|content| content["body"] == "image caption")
        .unwrap();
    assert_eq!(
        caption["m.relates_to"]["m.in_reply_to"]["event_id"],
        format!("$outbound-{}", image_index + 1)
    );
    bridge.shutdown().await.unwrap();
    server.abort();
}

#[tokio::test]
async fn matrix_sdk_routes_real_sync_to_acp_threaded_send_and_encrypts_outbound() {
    let encryption_modes: &[bool] = if cfg!(feature = "matrix-encryption") {
        &[false, true]
    } else {
        &[false]
    };
    for &encrypted in encryption_modes {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let state = MatrixFixture {
            encrypted,
            ..Default::default()
        };
        let server = tokio::spawn(
            axum::serve(
                listener,
                Router::new()
                    .fallback(any(matrix_transport))
                    .with_state(state.clone()),
            )
            .into_future(),
        );
        let directory = tempfile::tempdir().unwrap();
        let configuration=config::load(&json!({"rateLimitSeconds":0,"sessionStorePath":directory.path().join("store.json"),"acp":{"command":env!("CARGO_BIN_EXE_acp-fixture"),"args":[]},"matrix":{"homeserver":base,"userId":"@bot:local","deviceId":"LOCAL","accessToken":"fake-token","encryption":{"enabled":encrypted}}}).to_string(),&BTreeMap::new()).unwrap();
        let bridge = Arc::new(
            Bridge::new(
                configuration.clone(),
                BTreeMap::new(),
                directory.path().join("sessions"),
            )
            .unwrap(),
        );
        let client = matrix::connect(&configuration["matrix"], &directory.path().join("matrix"))
            .await
            .unwrap();
        let observed_messages = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let observer = observed_messages.clone();
        client.add_event_handler(
            move |event: matrix_sdk::ruma::events::room::message::OriginalSyncRoomMessageEvent,
                  room: matrix_sdk::Room| {
                let observer = observer.clone();
                async move {
                    assert_eq!(event.content.body(), "!oc hello");
                    assert_eq!(room.state(), matrix_sdk::RoomState::Joined);
                    observer.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                }
            },
        );
        let (sender, receiver) = tokio::sync::watch::channel(false);
        let task = tokio::spawn(matrix::run(client, bridge.clone(), receiver));
        let observed = tokio::time::timeout(std::time::Duration::from_secs(15), async {
            while state.sent.lock().await.is_empty() {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await;
        if observed.is_err() {
            if task.is_finished() {
                panic!("Matrix runtime exited: {:?}", task.await);
            }
            panic!(
                "Matrix sent no response after {} syncs; {} messages; stored={}; encrypted={encrypted}; requests={:?}",
                state.syncs.load(std::sync::atomic::Ordering::SeqCst),
                observed_messages.load(std::sync::atomic::Ordering::SeqCst),
                directory.path().join("store.json").exists(),
                state.requests.lock().await
            );
        }
        sender.send(true).unwrap();
        task.await.unwrap().unwrap();
        let sent = state.sent.lock().await;
        if encrypted {
            assert!(sent[0]["ciphertext"].is_string());
            assert_eq!(sent[0]["algorithm"], "m.megolm.v1.aes-sha2");
            assert!(sent[0].get("body").is_none());
        } else {
            assert_eq!(sent[0]["body"], "hello world");
            assert_eq!(sent[0]["m.relates_to"]["rel_type"], "m.thread");
            assert_eq!(sent[0]["m.relates_to"]["event_id"], "$initial");
        }
        bridge.shutdown().await.unwrap();
        server.abort();
    }
}

#[tokio::test]
async fn matrix_name_address_and_tool_trace_use_real_sync_and_correlated_notice_edits() {
    for body in [
        "oc: hello",
        "@OC hello",
        "oc hello",
        "oc: !oc tools",
        "oc: tools",
        "!oc tools",
    ] {
        let sent =
            matrix_runtime_messages(body, json!({"mode":"trace","showArguments":true})).await;
        assert_eq!(sent.last().unwrap()["m.relates_to"]["event_id"], "$initial");
        if body == "!oc tools" {
            assert_matrix_tool_edits(&sent, 1, false);
            assert!(sent.iter().any(|message| {
                message["m.relates_to"]["rel_type"] == "m.replace"
                    && message["m.new_content"]["msgtype"] == "m.notice"
                    && message["m.new_content"]["body"]
                        .as_str()
                        .is_some_and(|text| {
                            text.contains("Tool trace (completed)")
                                && text.contains("[read]")
                                && text.contains("[grep]")
                        })
            }));
        }
    }
}

#[tokio::test]
async fn matrix_tool_progress_modes_preserve_notices_and_edit_ids() {
    for (options, pages, rich) in [
        (json!({"mode":"trace","showArguments":true}), 1, false),
        (
            json!({"mode":"trace","showArguments":true,"maxTraceEntries":1}),
            2,
            false,
        ),
        (json!({"mode":"status"}), 1, false),
        (json!({"mode":"hermes","showArguments":true}), 1, true),
    ] {
        let sent = matrix_runtime_messages("!oc tools", options).await;
        assert_matrix_tool_edits(&sent, pages, rich);
    }
    let sent = matrix_runtime_messages("!oc tools", json!({"mode":"events"})).await;
    assert_eq!(
        sent.iter()
            .filter(|message| message["msgtype"] == "m.notice")
            .count(),
        2
    );
    assert!(
        sent.iter()
            .all(|message| message["m.relates_to"]["rel_type"] == "m.thread")
    );
    let sent = matrix_runtime_messages("!oc tools", json!({"mode":"off"})).await;
    assert!(sent.iter().all(|message| message["msgtype"] != "m.notice"));
}

fn assert_matrix_tool_edits(sent: &[Value], pages: usize, rich: bool) {
    let notices: Vec<_> = sent
        .iter()
        .enumerate()
        .filter(|(_, message)| {
            message["msgtype"] == "m.notice" && message["m.relates_to"]["rel_type"] == "m.thread"
        })
        .collect();
    assert_eq!(
        notices.len(),
        pages,
        "missing or duplicate initial tool notices: {sent:?}"
    );
    for (index, notice) in notices {
        assert_eq!(notice["m.relates_to"]["event_id"], "$initial");
        let original_id = format!("$outbound-{}", index + 1);
        let edits: Vec<_> = sent
            .iter()
            .filter(|message| {
                message["m.relates_to"]["rel_type"] == "m.replace"
                    && message["m.relates_to"]["event_id"] == original_id
            })
            .collect();
        assert!(!edits.is_empty(), "no updates correlated to {original_id}");
        for content in
            std::iter::once(notice).chain(edits.iter().map(|message| &message["m.new_content"]))
        {
            assert_eq!(content["msgtype"], "m.notice");
            if rich {
                assert_eq!(content["format"], "org.matrix.custom.html");
                assert!(content["formatted_body"].is_string());
            } else {
                assert!(content["body"].as_str().unwrap().starts_with("> "));
                assert!(content.get("format").is_none());
            }
        }
        for edit in edits {
            assert_eq!(edit["msgtype"], "m.notice");
            assert_eq!(
                edit["body"],
                format!("* {}", edit["m.new_content"]["body"].as_str().unwrap())
            );
            assert!(edit["m.new_content"].get("m.relates_to").is_none());
        }
    }
    for edit in sent
        .iter()
        .filter(|message| message["m.relates_to"]["rel_type"] == "m.replace")
    {
        let original_id = edit["m.relates_to"]["event_id"].as_str().unwrap();
        let index = original_id
            .strip_prefix("$outbound-")
            .unwrap()
            .parse::<usize>()
            .unwrap()
            - 1;
        assert_eq!(sent[index]["msgtype"], "m.notice");
        assert_eq!(sent[index]["m.relates_to"]["rel_type"], "m.thread");
    }
}

async fn matrix_runtime_messages(body: &str, options: Value) -> Vec<Value> {
    let expected = if matches!(body, "!oc tools" | "oc: tools") {
        "done"
    } else {
        "hello world"
    };
    let state = matrix_runtime_case(
        MatrixFixture {
            body: body.into(),
            ..Default::default()
        },
        options,
        json!({}),
        Some(expected),
    )
    .await;
    state.sent.lock().await.clone()
}

async fn matrix_runtime_case(
    state: MatrixFixture,
    options: Value,
    matrix_config: Value,
    expected: Option<&str>,
) -> MatrixFixture {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(
        axum::serve(
            listener,
            Router::new()
                .fallback(any(matrix_transport))
                .with_state(state.clone()),
        )
        .into_future(),
    );
    let directory = tempfile::tempdir().unwrap();
    let mut matrix = json!({"homeserver":base,"userId":"@bot:local","deviceId":"LOCAL","accessToken":"fake-token"});
    matrix
        .as_object_mut()
        .unwrap()
        .extend(matrix_config.as_object().unwrap().clone());
    let configuration=config::load(&json!({"rateLimitSeconds":0,"sessionStorePath":directory.path().join("store.json"),"acp":{"command":env!("CARGO_BIN_EXE_acp-fixture"),"args":[]},"toolMessages":options,"matrix":matrix}).to_string(),&BTreeMap::new()).unwrap();
    let bridge = Arc::new(
        Bridge::new(
            configuration.clone(),
            BTreeMap::new(),
            directory.path().join("sessions"),
        )
        .unwrap(),
    );
    let client = matrix::connect(&configuration["matrix"], &directory.path().join("matrix"))
        .await
        .unwrap();
    let (shutdown, receiver) = tokio::sync::watch::channel(false);
    let running = tokio::spawn(matrix::run(client, bridge.clone(), receiver));
    let completed = tokio::time::timeout(std::time::Duration::from_secs(3), async {
        if expected.is_none() {
            while state.syncs.load(std::sync::atomic::Ordering::SeqCst) < 2 {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
            tokio::time::sleep(std::time::Duration::from_millis(150)).await;
            return;
        }
        loop {
            if state.sent.lock().await.iter().any(|message| {
                message["body"]
                    .as_str()
                    .is_some_and(|body| body.starts_with(expected.unwrap()))
            }) {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await;
    shutdown.send(true).unwrap();
    running.await.unwrap().unwrap();
    bridge.shutdown().await.unwrap();
    assert!(
        completed.is_ok(),
        "Matrix did not answer {}; requests={:?}; sent={:?}",
        state.body,
        state.requests.lock().await,
        state.sent.lock().await
    );
    server.abort();
    state
}

#[tokio::test]
async fn matrix_source_thread_isolation_off_and_reply_fallback_context() {
    for (isolation, relation) in [
        (false, json!({})),
        (
            true,
            json!({"m.relates_to":{"rel_type":"m.thread","event_id":"$root","m.in_reply_to":{"event_id":"$last"}}}),
        ),
    ] {
        let state = matrix_runtime_case(
            MatrixFixture {
                body: "!oc hello".into(),
                content: relation,
                ..Default::default()
            },
            json!({"mode":"off"}),
            json!({"threadIsolation":isolation}),
            Some("hello world"),
        )
        .await;
        let sent = state.sent.lock().await;
        if isolation {
            assert_eq!(sent[0]["m.relates_to"]["event_id"], "$root");
            assert_eq!(
                sent[0]["m.relates_to"]["m.in_reply_to"]["event_id"],
                "$initial"
            );
            assert_eq!(sent[0]["m.relates_to"]["is_falling_back"], true);
        } else {
            assert!(
                sent[0].get("m.relates_to").is_none(),
                "isolation off must not create threads: {sent:?}"
            );
        }
    }
}

#[tokio::test]
async fn matrix_source_addressing_and_refusals_through_real_sync() {
    for (body, content, config, members) in [
        ("@bot:local /status", json!({}), json!({}), 3),
        (
            "@bot:local : /status",
            json!({"m.mentions":{"user_ids":["@bot:local"]}}),
            json!({}),
            3,
        ),
        ("!oc : /status", json!({}), json!({}), 3),
        (
            "@OC /status",
            json!({"m.mentions":{"user_ids":["@bot:local"]}}),
            json!({}),
            3,
        ),
        ("/status", json!({}), json!({}), 2),
        (
            "/status",
            json!({}),
            json!({"respondToAllMessages":true}),
            3,
        ),
    ] {
        let state = matrix_runtime_case(
            MatrixFixture {
                body: body.into(),
                content,
                members,
                receipt_failure: true,
                ..Default::default()
            },
            json!({}),
            config,
            Some("Backend:"),
        )
        .await;
        assert!(
            state
                .requests
                .lock()
                .await
                .iter()
                .any(|path| path.contains("/receipt/m.read/")),
            "no read receipt for {body}"
        );
    }
    for (body, sender, content, config) in [
        ("hello", "@human:local", json!({}), json!({})),
        (
            "!oc hello",
            "@human:local",
            json!({}),
            json!({"allowedUsers":["@allowed:local"]}),
        ),
        (
            "!oc hello",
            "@human:local",
            json!({}),
            json!({"ignoreUsers":["@human:local"]}),
        ),
        (
            "!oc hello",
            "@human:local",
            json!({}),
            json!({"ignoreRooms":["!room:local"]}),
        ),
        (
            "!oc hello",
            "@human:local",
            json!({}),
            json!({"allowedRooms":["!else:local"]}),
        ),
        ("!oc hello", "@bot:local", json!({}), json!({})),
        (
            "!oc hello",
            "@human:local",
            json!({"msgtype":"m.notice"}),
            json!({}),
        ),
    ] {
        let state = matrix_runtime_case(
            MatrixFixture {
                body: body.into(),
                sender: sender.into(),
                content,
                members: 3,
                ..Default::default()
            },
            json!({}),
            config,
            None,
        )
        .await;
        assert!(
            state.sent.lock().await.is_empty(),
            "denied event answered: {body} {sender}"
        );
        assert!(
            state
                .requests
                .lock()
                .await
                .iter()
                .all(|path| !path.contains("/receipt/") && !path.contains("/typing/"))
        );
    }
}
