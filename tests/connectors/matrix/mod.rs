use super::*;
use axum::{Json, Router, extract::Request, routing::any};
#[test]
fn malformed_optional_mention_metadata_does_not_reject_valid_message() {
    let raw = matrix_sdk::ruma::serde::Raw::<OriginalSyncRoomMessageEvent>::from_json_string(
            r#"{"type":"m.room.message","event_id":"$event","sender":"@alice:local","origin_server_ts":1,"room_id":"!room:local","content":{"msgtype":"m.text","body":"exact @host:~/path$ text","m.mentions":{"user_ids":["@host:~/path$","@bot:local"]},"m.relates_to":{"rel_type":"m.thread","event_id":"$root"}}}"#.to_owned(),
        ).unwrap();
    let message = normalize_matrix_message(raw)
        .expect("message should survive malformed optional mention metadata");
    assert_eq!(message.sender.as_str(), "@alice:local");
    assert_eq!(message.event_id.as_str(), "$event");
    assert_eq!(message.content.body(), "exact @host:~/path$ text");
    assert!(matches!(
        message.content.relates_to,
        Some(Relation::Thread(_))
    ));
    let mentions = message.content.mentions.unwrap();
    assert_eq!(mentions.user_ids.len(), 1);
    assert_eq!(
        mentions.user_ids.iter().next().unwrap().as_str(),
        "@bot:local"
    );
}

#[test]
fn mention_normalization_preserves_authority_validation() {
    let base = json!({"type":"m.room.message","event_id":"$event","sender":"@alice:local","origin_server_ts":1,"content":{"msgtype":"m.text","body":"unchanged","m.mentions":{"user_ids":["@host:~/path$"]}}});
    for field in ["sender", "event_id"] {
        let mut event = base.clone();
        event[field] = json!("invalid");
        let raw = matrix_sdk::ruma::serde::Raw::from_json_string(event.to_string()).unwrap();
        assert!(
            normalize_matrix_message(raw).is_err(),
            "invalid {field} must remain rejected"
        );
    }
    let mut event = base.clone();
    event["content"]["m.relates_to"] = json!({"rel_type":"m.thread","event_id":"invalid"});
    let raw = matrix_sdk::ruma::serde::Raw::from_json_string(event.to_string()).unwrap();
    let actual = normalize_matrix_message(raw).unwrap();
    event["content"]
        .as_object_mut()
        .unwrap()
        .remove("m.mentions");
    let expected: OriginalSyncRoomMessageEvent = serde_json::from_value(event).unwrap();
    // Ruma falls back to no relation for malformed thread metadata. Keep
    // that existing behavior; mention repair must not reinterpret threads.
    assert_eq!(
        serde_json::to_value(actual.content.relates_to).unwrap(),
        serde_json::to_value(expected.content.relates_to).unwrap()
    );
    let mut plain = base;
    plain["content"]
        .as_object_mut()
        .unwrap()
        .remove("m.mentions");
    let raw = matrix_sdk::ruma::serde::Raw::from_json_string(plain.to_string()).unwrap();
    let message = normalize_matrix_message(raw).unwrap();
    assert_eq!(message.content.body(), "unchanged");
    assert!(message.content.mentions.is_none());
}

#[test]
fn native_permission_raw_thread_never_uses_incoming_event_or_reply_fallback() {
    for (relation, expected) in [
        (Value::Null, None),
        (json!({"m.in_reply_to":{"event_id":"$reply"}}), None),
        (
            json!({"rel_type":"m.thread", "event_id":"$raw-root", "is_falling_back":true, "m.in_reply_to":{"event_id":"$latest"}}),
            Some("$raw-root"),
        ),
    ] {
        let mut content = json!({"msgtype":"m.text", "body":"work"});
        if !relation.is_null() {
            content["m.relates_to"] = relation;
        }
        let content: RoomMessageEventContent = serde_json::from_value(content).unwrap();
        assert_eq!(
            raw_permission_thread(&content)
                .as_ref()
                .map(|id| id.as_str()),
            expected
        );
    }
}

#[tokio::test]
async fn concurrent_native_thread_sends_advance_after_success_only() {
    for fail_first in [false, true] {
        let sent = Arc::new(tokio::sync::Mutex::new(Vec::<Value>::new()));
        let started = Arc::new(tokio::sync::Semaphore::new(0));
        let release = Arc::new(tokio::sync::Semaphore::new(0));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let router = Router::new().fallback(any({
                let sent = sent.clone();
                let started = started.clone();
                let release = release.clone();
                move |request: Request| {
                    let sent = sent.clone();
                    let started = started.clone();
                    let release = release.clone();
                    async move {
                        use axum::response::IntoResponse;
                        let path = request.uri().path().to_owned();
                        if path.trim_end_matches('/').ends_with("/state/m.room.encryption") {
                            return (axum::http::StatusCode::NOT_FOUND, Json(json!({"errcode":"M_NOT_FOUND","error":"Fixture room is not encrypted"}))).into_response();
                        }
                        let result = if path.ends_with("versions") {
                            json!({"versions":["v1.12"],"unstable_features":{}})
                        } else if path.ends_with("sync") {
                            json!({"next_batch":"one","rooms":{"join":{"!room:local":{"state":{"events":[
                                {"type":"m.room.create","event_id":"$create","origin_server_ts":1,"state_key":"","sender":"@bot:local","content":{"creator":"@bot:local","room_version":"10"}},
                                {"type":"m.room.member","event_id":"$member","origin_server_ts":1,"state_key":"@bot:local","sender":"@bot:local","content":{"membership":"join"}}
                            ]},"timeline":{"events":[],"limited":false},"ephemeral":{"events":[]},"account_data":{"events":[]},"unread_notifications":{}}}},"device_lists":{"changed":[],"left":[]},"device_one_time_keys_count":{"signed_curve25519":50}})
                        } else if path.contains("/send/") {
                            let bytes = axum::body::to_bytes(request.into_body(), 1024 * 1024).await.unwrap();
                            let count = {
                                let mut sent = sent.lock().await;
                                sent.push(serde_json::from_slice(&bytes).unwrap());
                                sent.len()
                            };
                            if count == 1 {
                                started.add_permits(1);
                                release.acquire().await.unwrap().forget();
                                if fail_first {
                                    return (axum::http::StatusCode::BAD_REQUEST, Json(json!({"errcode":"M_BAD_JSON","error":"fixture send failure"}))).into_response();
                                }
                            }
                            json!({"event_id":format!("$sent-{count}")})
                        } else if path.ends_with("whoami") {
                            json!({"user_id":"@bot:local","device_id":"LOCAL"})
                        } else if path.ends_with("upload") {
                            json!({"one_time_key_counts":{"signed_curve25519":50}})
                        } else if path.ends_with("query") {
                            json!({"device_keys":{},"failures":{}})
                        } else if path.ends_with("members") {
                            json!({"chunk":[]})
                        } else { json!({}) };
                        Json(result).into_response()
                    }
                }
            }));
        let server = tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });
        let directory = tempfile::tempdir().unwrap();
        let client = connect(&json!({"homeserver":base,"userId":"@bot:local","accessToken":"fixture","encryption":{"enabled":false}}), directory.path()).await.unwrap();
        client.sync_once(SyncSettings::default()).await.unwrap();
        let room = client.get_room("!room:local".try_into().unwrap()).unwrap();
        let sink = Arc::new(MatrixSink {
            room,
            thread: Some("$root".try_into().unwrap()),
            raw_thread: Some("$root".try_into().unwrap()),
            html: false,
            rich_tools: false,
            activities: Default::default(),
            indicators: TypingManager::new(Arc::new(NativeTyping(client)), Duration::from_secs(4)),
            typing: Default::default(),
            incoming: "$incoming".try_into().unwrap(),
            reply_context: Default::default(),
        });
        assert_eq!(sink.matrix_raw_thread().as_deref(), Some("$root"));
        assert_eq!(
            sink.owned().unwrap().matrix_raw_thread().as_deref(),
            Some("$root")
        );
        let first = {
            let sink = sink.owned().unwrap();
            tokio::spawn(async move { sink.emit(json!({"type":"notice","text":"first"})).await })
        };
        tokio::time::timeout(Duration::from_secs(5), started.acquire())
            .await
            .unwrap()
            .unwrap()
            .forget();
        let second = {
            let sink = sink.owned().unwrap();
            tokio::spawn(async move { sink.emit(json!({"type":"response","text":"second"})).await })
        };
        tokio::time::sleep(Duration::from_millis(80)).await;
        release.add_permits(1);
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(5), first)
                .await
                .unwrap()
                .unwrap()
                .is_err(),
            fail_first
        );
        tokio::time::timeout(Duration::from_secs(5), second)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let messages = sent.lock().await;
        assert_eq!(messages.len(), 2);
        assert_eq!(
            messages[0]["m.relates_to"]["m.in_reply_to"]["event_id"],
            "$root"
        );
        assert_eq!(
            messages[1]["m.relates_to"]["m.in_reply_to"]["event_id"],
            if fail_first { "$root" } else { "$sent-1" },
            "{messages:?}"
        );
        assert_eq!(
            sink.reply_context
                .lock()
                .await
                .get(&sink.reply_key(sink.thread.as_ref().unwrap()))
                .unwrap()
                .as_str(),
            "$sent-2"
        );
        drop(messages);
        sink.emit(json!({"type":"activity","activityId":"","message":"anonymous first"}))
            .await
            .unwrap();
        sink.owned()
            .unwrap()
            .emit(json!({"type":"activity_update","message":"anonymous update"}))
            .await
            .unwrap();
        sink.emit(json!({"type":"activity","message":"anonymous second"}))
            .await
            .unwrap();
        let messages = sent.lock().await;
        assert_eq!(messages[3]["m.relates_to"]["rel_type"], "m.replace");
        assert_eq!(messages[3]["m.relates_to"]["event_id"], "$sent-3");
        assert_eq!(
            messages[4]["m.relates_to"]["rel_type"], "m.thread",
            "new anonymous activities must not edit an unrelated notice: {messages:?}"
        );
        assert_eq!(
            messages[4]["m.relates_to"]["m.in_reply_to"]["event_id"],
            "$sent-3"
        );
        server.abort();
    }
}
