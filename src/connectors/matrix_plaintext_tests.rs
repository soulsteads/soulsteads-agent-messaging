//! Plaintext-only build: real SDK HTTP ingress/egress must respect room crypto.
use super::*;
use axum::{Json, Router, extract::Request, response::IntoResponse, routing::any};
use std::sync::atomic::{AtomicU8, AtomicUsize, Ordering};

struct Fixture {
    base: String,
    mode: Arc<AtomicU8>,
    sends: Arc<AtomicUsize>,
    uploads: Arc<AtomicUsize>,
    syncs: Arc<AtomicUsize>,
    server: tokio::task::JoinHandle<()>,
}
impl Drop for Fixture {
    fn drop(&mut self) {
        self.server.abort();
    }
}
impl Fixture {
    async fn start(mode: u8, input: bool) -> Self {
        let mode = Arc::new(AtomicU8::new(mode));
        let sends = Arc::new(AtomicUsize::new(0));
        let uploads = Arc::new(AtomicUsize::new(0));
        let syncs = Arc::new(AtomicUsize::new(0));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let router = Router::new().fallback(any({
            let mode=mode.clone(); let sends=sends.clone(); let uploads=uploads.clone(); let syncs=syncs.clone();
            move |request: Request| {
                let mode=mode.clone(); let sends=sends.clone(); let uploads=uploads.clone(); let syncs=syncs.clone();
                async move {
                    let path=request.uri().path();
                    let current=mode.load(Ordering::SeqCst);
                    if path.trim_end_matches('/').ends_with("/state/m.room.encryption") {
                        return match current {
                            0 => (axum::http::StatusCode::NOT_FOUND,Json(json!({"errcode":"M_NOT_FOUND","error":"plaintext fixture"}))).into_response(),
                            1 => Json(json!({"algorithm":"m.megolm.v1.aes-sha2"})).into_response(),
                            _ => (axum::http::StatusCode::BAD_REQUEST,Json(json!({"errcode":"M_BAD_JSON","error":"state unavailable"}))).into_response(),
                        };
                    }
                    let value=if path.ends_with("versions") { json!({"versions":["v1.12"],"unstable_features":{}}) }
                    else if path.ends_with("whoami") { json!({"user_id":"@bot:local","device_id":"PLAIN"}) }
                    else if path.ends_with("joined_members") { json!({"joined":{"@bot:local":{},"@human:local":{}}}) }
                    else if path.ends_with("sync") {
                        let n=syncs.fetch_add(1,Ordering::SeqCst);
                        tokio::time::sleep(Duration::from_millis(25)).await;
                        let mut state=vec![json!({"type":"m.room.create","event_id":"$create","origin_server_ts":1,"state_key":"","sender":"@bot:local","content":{"creator":"@bot:local","room_version":"10"}}),json!({"type":"m.room.member","event_id":"$member","origin_server_ts":1,"state_key":"@bot:local","sender":"@bot:local","content":{"membership":"join"}})];
                        if current==1 {state.push(json!({"type":"m.room.encryption","event_id":"$crypto","origin_server_ts":2,"state_key":"","sender":"@human:local","content":{"algorithm":"m.megolm.v1.aes-sha2"}}));}
                        let events=if input && n==1 {vec![json!({"type":"m.room.message","event_id":"$input","origin_server_ts":3,"sender":"@human:local","content":{"msgtype":"m.text","body":"fixture input must not reach ACP"}})]} else {vec![]};
                        json!({"next_batch":format!("batch-{n}"),"rooms":{"join":{"!room:local":{"state":{"events":state},"timeline":{"events":events,"limited":false},"ephemeral":{"events":[]},"account_data":{"events":[]}}}}})
                    } else if path.contains("/send/") { sends.fetch_add(1,Ordering::SeqCst); json!({"event_id":"$sent"}) }
                    else if path.contains("/upload") { uploads.fetch_add(1,Ordering::SeqCst); json!({"content_uri":"mxc://local/file"}) }
                    else {json!({})};
                    Json(value).into_response()
                }
            }
        }));
        let server = tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });
        Self {
            base,
            mode,
            sends,
            uploads,
            syncs,
            server,
        }
    }
    fn config(&self, root: &Path) -> Value {
        crate::config::load(&json!({"sessionStorePath":root.join("sessions.json"),"rateLimitSeconds":0,"toolMessages":{"mode":"off"},"acp":{"command":"/usr/bin/python3","args":[concat!(env!("CARGO_MANIFEST_DIR"),"/tests/fixtures/matrix-approval-peer.py")],"env":{"APPROVAL_AUDIT":root.join("audit.jsonl")}},"matrix":{"homeserver":self.base,"userId":"@bot:local","accessToken":"fixture","encryption":{"enabled":false},"respondToAllMessages":true,"allowedRooms":["!room:local"],"allowedUsers":["@human:local"],"strictAllowedRooms":true}}).to_string(),&Default::default()).unwrap()
    }
    async fn sink(&self, root: &Path, thread: bool) -> MatrixSink {
        let client = connect(&self.config(root)["matrix"], &root.join("matrix"))
            .await
            .unwrap();
        client.sync_once(SyncSettings::default()).await.unwrap();
        let room = client.get_room("!room:local".try_into().unwrap()).unwrap();
        MatrixSink {
            room,
            thread: thread.then(|| "$root".try_into().unwrap()),
            raw_thread: None,
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
#[tokio::test]
async fn encrypted_and_failed_unknown_room_state_refuse_messages_and_files() {
    for mode in [1, 2] {
        let fixture = Fixture::start(mode, false).await;
        let root = tempfile::tempdir().unwrap();
        let sink = fixture.sink(root.path(), false).await;
        if mode == 2 {
            assert!(sink.room.encryption_state().is_unknown());
        }
        assert!(
            sink.emit(json!({"type":"notice","text":"private response fixture"}))
                .await
                .is_err(),
            "plaintext message was published"
        );
        assert!(
            sink.upload("private.txt", "text/plain", b"private fixture".to_vec())
                .await
                .is_err(),
            "plaintext media was uploaded"
        );
        assert_eq!(fixture.sends.load(Ordering::SeqCst), 0);
        assert_eq!(fixture.uploads.load(Ordering::SeqCst), 0);
    }
}
async fn run_ingress_case(mode: u8) -> (bool, usize) {
    let fixture = Fixture::start(mode, true).await;
    let root = tempfile::tempdir().unwrap();
    let bridge = Arc::new(
        Bridge::new(
            fixture.config(root.path()),
            Default::default(),
            root.path().join("cwd"),
        )
        .unwrap(),
    );
    let (shutdown, receive) = tokio::sync::watch::channel(false);
    let client = connect(&bridge.config["matrix"], &root.path().join("platform"))
        .await
        .unwrap();
    let task = tokio::spawn(super::run(client, bridge.clone(), receive));
    tokio::time::timeout(Duration::from_secs(5), async {
        while fixture.syncs.load(Ordering::SeqCst) < 4 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    shutdown.send(true).unwrap();
    tokio::time::timeout(Duration::from_secs(5), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    bridge.shutdown().await.unwrap();
    (
        root.path().join("audit.jsonl").exists(),
        fixture.sends.load(Ordering::SeqCst),
    )
}
#[tokio::test]
async fn room_admission_refuses_encrypted_and_unknown_with_plaintext_positive_control() {
    for (mode, allowed) in [(0, true), (1, false), (2, false)] {
        let (launched, sends) = run_ingress_case(mode).await;
        assert_eq!(
            launched, allowed,
            "ACP launch violated room policy in mode {mode}"
        );
        if !allowed {
            assert_eq!(sends, 0);
        }
    }
}
#[tokio::test]
async fn encryption_change_while_thread_send_lock_pending_refuses_publication() {
    let fixture = Fixture::start(0, false).await;
    let root = tempfile::tempdir().unwrap();
    let sink = Arc::new(fixture.sink(root.path(), true).await);
    // Establish a supported plaintext room before the transition.
    sink.room.request_encryption_state().await.unwrap();
    assert!(matches!(
        sink.room.encryption_state(),
        matrix_sdk::EncryptionState::NotEncrypted
    ));
    let context = sink.reply_context.clone();
    let key = sink.reply_key(sink.thread.as_ref().unwrap());
    let hold = context.send_lock(&key).lock().await;
    let send = {
        let sink = sink.clone();
        tokio::spawn(async move {
            sink.send_message(RoomMessageEventContent::text_plain(
                "private queued response fixture",
            ))
            .await
        })
    };
    tokio::task::yield_now().await;
    assert!(!send.is_finished());
    fixture.mode.store(1, Ordering::SeqCst);
    sink.room
        .client()
        .sync_once(SyncSettings::default())
        .await
        .unwrap();
    assert!(sink.room.encryption_state().is_encrypted());
    drop(hold);
    assert!(
        tokio::time::timeout(Duration::from_secs(5), send)
            .await
            .unwrap()
            .unwrap()
            .is_err(),
        "queued plaintext response bypassed new encrypted state"
    );
    assert_eq!(fixture.sends.load(Ordering::SeqCst), 0);
}
