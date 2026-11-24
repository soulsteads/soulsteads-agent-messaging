#![cfg(all(unix, feature = "platform-sdks"))]

use axum::{
    Json, Router,
    extract::{Path, Request, State, WebSocketUpgrade},
    response::{IntoResponse, Response},
    routing::{any, get},
};
use futures_util::StreamExt;
use serde_json::{Value, json};
use soulsteads_messaging::store::Store;
use std::{
    future::IntoFuture,
    process::{Child, Command, ExitStatus, Stdio},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::{
    sync::{Mutex, Notify},
    time::timeout,
};

const MATRIX_ROOM: &str = "!matrix:local";
const MATRIX_EVENT: &str = "$matrix-inbound";
const MATTERMOST_CHANNEL: &str = "MM_ROOM";
const MATTERMOST_ROOT: &str = "MM_ROOT";
const MATRIX_PROMPT: &str = "matrix inbound";
const MATTERMOST_PROMPT: &str = "mattermost inbound";

#[derive(Clone, Default)]
struct Fixture {
    matrix_syncs: Arc<AtomicUsize>,
    matrix_ready: Arc<AtomicBool>,
    mattermost_ready: Arc<AtomicBool>,
    released: Arc<AtomicBool>,
    changed: Arc<Notify>,
    matrix_outbound: Arc<Mutex<Vec<(String, Value)>>>,
    mattermost_outbound: Arc<Mutex<Vec<Value>>>,
}

impl Fixture {
    fn mark_matrix_ready(&self) {
        self.matrix_ready.store(true, Ordering::SeqCst);
        self.changed.notify_waiters();
    }

    fn mark_mattermost_ready(&self) {
        self.mattermost_ready.store(true, Ordering::SeqCst);
        self.changed.notify_waiters();
    }

    async fn wait_for_pair_readiness(&self) {
        timeout(Duration::from_secs(10), async {
            loop {
                let notified = self.changed.notified();
                if self.matrix_ready.load(Ordering::SeqCst)
                    && self.mattermost_ready.load(Ordering::SeqCst)
                {
                    return;
                }
                notified.await;
            }
        })
        .await
        .expect("Matrix incremental sync and Mattermost authentication did not both become ready");
    }

    async fn wait_for_release(&self) {
        loop {
            let notified = self.changed.notified();
            if self.released.load(Ordering::SeqCst) {
                return;
            }
            notified.await;
        }
    }

    fn release_both_inbound_events(&self) {
        self.released.store(true, Ordering::SeqCst);
        self.changed.notify_waiters();
    }

    async fn wait_for_outbound(&self) {
        timeout(Duration::from_secs(10), async {
            loop {
                let notified = self.changed.notified();
                let matrix = self.matrix_outbound.lock().await;
                let mattermost = self.mattermost_outbound.lock().await;
                let complete = matrix.iter().any(|(_, message)| {
                    message["body"]
                        .as_str()
                        .is_some_and(|s| s.starts_with(MATRIX_PROMPT))
                }) && mattermost.iter().any(|post| {
                    post["message"]
                        .as_str()
                        .is_some_and(|s| s.starts_with(MATTERMOST_PROMPT))
                });
                drop(mattermost);
                drop(matrix);
                if complete {
                    return;
                }
                notified.await;
            }
        })
        .await
        .expect("the running service did not produce both connector replies");
    }
}

fn matrix_sync(ordinal: usize, messages: Vec<Value>) -> Value {
    json!({
        "next_batch": format!("matrix-batch-{ordinal}"),
        "rooms": {"join": {MATRIX_ROOM: {
            "state": {"events": [
                {"type":"m.room.create","event_id":"$matrix-create","origin_server_ts":1,"state_key":"","sender":"@bot:local","content":{"creator":"@bot:local","room_version":"10"}},
                {"type":"m.room.member","event_id":"$matrix-member","origin_server_ts":1,"state_key":"@bot:local","sender":"@bot:local","content":{"membership":"join"}}
            ]},
            "timeline": {"events":messages,"limited":false,"prev_batch":"matrix-previous"},
            "ephemeral":{"events":[]},
            "account_data":{"events":[]}
        }}},
        "device_lists":{"changed":[],"left":[]},
        "device_one_time_keys_count":{"signed_curve25519":50}
    })
}

async fn matrix_http(State(fixture): State<Fixture>, request: Request) -> Response {
    let path = request.uri().path().to_owned();
    if path.ends_with("/sync") {
        let ordinal = fixture.matrix_syncs.fetch_add(1, Ordering::SeqCst);
        let events = if ordinal == 1 {
            fixture.mark_matrix_ready();
            fixture.wait_for_release().await;
            vec![json!({
                "type":"m.room.message",
                "event_id":MATRIX_EVENT,
                "sender":"@human:local",
                "origin_server_ts":chrono::Utc::now().timestamp_millis(),
                "content":{"msgtype":"m.text","body":format!("!oc {MATRIX_PROMPT}")}
            })]
        } else if ordinal > 1 {
            // Model the next long poll without flooding the client with empty syncs.
            return std::future::pending().await;
        } else {
            vec![]
        };
        return Json(matrix_sync(ordinal, events)).into_response();
    }

    if path.ends_with("/versions") {
        return Json(json!({"versions":["v1.12"],"unstable_features":{}})).into_response();
    }
    if path.ends_with("/whoami") {
        return Json(json!({"user_id":"@bot:local","device_id":"LOCAL"})).into_response();
    }
    if path.ends_with("/joined_members") {
        return Json(json!({"joined":{
            "@bot:local":{"display_name":"bot"},
            "@human:local":{"display_name":"human"}
        }}))
        .into_response();
    }
    if path
        .trim_end_matches('/')
        .ends_with("/state/m.room.encryption")
    {
        return (
            axum::http::StatusCode::NOT_FOUND,
            Json(json!({"errcode":"M_NOT_FOUND","error":"fixture plaintext room"})),
        )
            .into_response();
    }
    Json(json!({})).into_response()
}

async fn matrix_send(
    State(fixture): State<Fixture>,
    Path((_, room, _)): Path<(String, String, String)>,
    Json(message): Json<Value>,
) -> Json<Value> {
    fixture.matrix_outbound.lock().await.push((room, message));
    fixture.changed.notify_waiters();
    Json(json!({"event_id":"$matrix-outbound"}))
}

async fn mattermost_http(State(fixture): State<Fixture>, request: Request) -> Response {
    let path = request.uri().path().to_owned();
    let body = axum::body::to_bytes(request.into_body(), 1024 * 1024)
        .await
        .unwrap();
    if path.ends_with("/users/me") {
        return Json(json!({"id":"MM_BOT","username":"bridge"})).into_response();
    }
    if path.ends_with("/posts") {
        let post: Value = serde_json::from_slice(&body).unwrap();
        fixture.mattermost_outbound.lock().await.push(post.clone());
        fixture.changed.notify_waiters();
        return Json(json!({
            "id":"mm-outbound",
            "channel_id":post["channel_id"],
            "root_id":post["root_id"],
            "user_id":"MM_BOT",
            "message":post["message"]
        }))
        .into_response();
    }
    Json(json!({"id":"fixture"})).into_response()
}

async fn mattermost_socket(State(fixture): State<Fixture>, socket: WebSocketUpgrade) -> Response {
    socket
        .on_upgrade(move |mut socket| async move {
            let Some(Ok(authentication)) = socket.next().await else {
                return;
            };
            let Ok(authentication) = authentication.to_text() else {
                return;
            };
            let Ok(authentication): Result<Value, _> = serde_json::from_str(authentication) else {
                return;
            };
            if authentication["action"] != "authentication_challenge"
                || authentication["data"]["token"] != "fixture-mattermost-token"
            {
                return;
            }
            fixture.mark_mattermost_ready();
            fixture.wait_for_release().await;
            let event = json!({"event":"posted","data":{"post":json!({
                "id":"mm-inbound",
                "channel_id":MATTERMOST_CHANNEL,
                "user_id":"MM_HUMAN",
                "root_id":MATTERMOST_ROOT,
                "message":format!("!oc {MATTERMOST_PROMPT}")
            }).to_string(),"channel_type":"O"}});
            if socket
                .send(axum::extract::ws::Message::Text(event.to_string().into()))
                .await
                .is_err()
            {
                return;
            }
            while socket.next().await.is_some() {}
        })
        .into_response()
}

struct Service(Child);

impl Service {
    async fn stop(&mut self) -> ExitStatus {
        assert_eq!(
            unsafe { libc::kill(self.0.id() as i32, libc::SIGTERM) },
            0,
            "could not signal the owned soulsteads-messaging service"
        );
        let deadline = Instant::now() + Duration::from_secs(8);
        loop {
            if let Some(status) = self.0.try_wait().unwrap() {
                return status;
            }
            assert!(
                Instant::now() < deadline,
                "owned soulsteads-messaging service did not stop after SIGTERM"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }
}

impl Drop for Service {
    fn drop(&mut self) {
        if self.0.try_wait().ok().flatten().is_none() {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
}

async fn wait_for_sessions(
    matrix_store: &std::path::Path,
    mattermost_store: &std::path::Path,
    mattermost_backend: &str,
) {
    timeout(Duration::from_secs(10), async {
        loop {
            if Store::new(matrix_store)
                .get(
                    "matrix",
                    &format!("{MATRIX_ROOM}:{MATRIX_EVENT}"),
                    "opencode",
                )
                .unwrap()
                .is_some()
                && Store::new(mattermost_store)
                    .get(
                        "mattermost",
                        &format!("{MATTERMOST_CHANNEL}:{MATTERMOST_ROOT}"),
                        mattermost_backend,
                    )
                    .unwrap()
                    .is_some()
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("both connector conversations were not persisted");
}

#[tokio::test]
async fn serve_runs_matrix_and_mattermost_inbound_outbound_with_separate_persisted_threads() {
    run_pair(false).await;
}

#[tokio::test]
async fn serve_routes_two_chat_bindings_to_distinct_agent_homes_and_stores() {
    run_pair(true).await;
}

async fn run_pair(multi_agent: bool) {
    let root = tempfile::tempdir().unwrap();
    let fixture = Fixture::default();

    let matrix_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let matrix_base = format!("http://{}", matrix_listener.local_addr().unwrap());
    let matrix_server = tokio::spawn(
        axum::serve(
            matrix_listener,
            Router::new()
                .route(
                    "/_matrix/client/{version}/rooms/{room}/send/m.room.message/{txn}",
                    axum::routing::put(matrix_send),
                )
                .fallback(any(matrix_http))
                .with_state(fixture.clone()),
        )
        .into_future(),
    );

    let mattermost_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mattermost_base = format!("http://{}", mattermost_listener.local_addr().unwrap());
    let mattermost_server = tokio::spawn(
        axum::serve(
            mattermost_listener,
            Router::new()
                .route("/api/v4/websocket", get(mattermost_socket))
                .fallback(any(mattermost_http))
                .with_state(fixture.clone()),
        )
        .into_future(),
    );

    let legacy_store = root.path().join("sessions.json");
    let config = root.path().join("bridge.json");
    let legacy = json!({
        "rateLimitSeconds":0,
        "sessionStorePath":legacy_store,
        "toolMessages":{"mode":"off"},
        "acp":{"command":env!("CARGO_BIN_EXE_acp-fixture"),"args":["--echo-prompt"]},
        "matrix":{
            "enabled":true,
            "homeserver":matrix_base,
            "userId":"@bot:local",
            "deviceId":"LOCAL",
            "accessToken":"fixture-matrix-token",
            "encryption":{"enabled":false},
            "allowedRooms":[MATRIX_ROOM],
            "allowedUsers":["@human:local"],
            "threadIsolation":true
        },
        "mattermost":{
            "enabled":true,
            "url":mattermost_base,
            "token":"fixture-mattermost-token",
            "allowedChannels":[MATTERMOST_CHANNEL],
            "allowedUsers":["MM_HUMAN"],
            "threadIsolation":true
        }
    });
    let mut homes = Vec::new();
    let (supplied, matrix_store, mattermost_store) = if multi_agent {
        use std::os::unix::fs::DirBuilderExt;
        let make_private = |path: &std::path::Path| {
            std::fs::DirBuilder::new().mode(0o700).create(path).unwrap();
        };
        let state = root.path().join("state");
        make_private(&state);
        let mut agents = Vec::new();
        for platform in ["matrix", "mattermost"] {
            make_private(&state.join(platform));
            let home = root.path().join(format!("home-{platform}"));
            make_private(&home);
            let mut agent = legacy.clone();
            let fields = agent.as_object_mut().unwrap();
            fields.remove("sessionStorePath");
            fields.remove(if platform == "matrix" {
                "mattermost"
            } else {
                "matrix"
            });
            agent["acp"]["agentHome"] = json!(home);
            if platform == "mattermost" {
                agent["acp"]["defaultBackend"] = json!("pi");
                agent["acp"]["backends"]["pi"] = json!({
                    "command":env!("CARGO_BIN_EXE_acp-fixture"),"args":["--echo-prompt"]
                });
            }
            homes.push(home);
            agents.push(json!({"id":platform,"config":agent}));
        }
        (
            json!({"stateDir":state,"agents":agents}),
            state.join("matrix/sessions.json"),
            state.join("mattermost/sessions.json"),
        )
    } else {
        (legacy, legacy_store.clone(), legacy_store.clone())
    };
    std::fs::write(&config, supplied.to_string()).unwrap();
    if multi_agent {
        let check = Command::new(env!("CARGO_BIN_EXE_soulsteads-messaging"))
            .args(["--check-config", "--config", config.to_str().unwrap()])
            .current_dir(root.path())
            .env_clear()
            .output()
            .unwrap();
        assert!(check.status.success());
        assert!(!matrix_store.exists() && !mattermost_store.exists());
    }

    let mut service = Service(
        Command::new(env!("CARGO_BIN_EXE_soulsteads-messaging"))
            .args(["--serve", "--config", config.to_str().unwrap()])
            .current_dir(root.path())
            .env_clear()
            .env("HOME", root.path().join("home"))
            .env("SESSION_BASE_DIR", root.path().join("acp-sessions"))
            .env("MATRIX_STORAGE_PATH", root.path().join("matrix-store"))
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap(),
    );

    fixture.wait_for_pair_readiness().await;
    fixture.release_both_inbound_events();
    fixture.wait_for_outbound().await;
    let mattermost_backend = if multi_agent { "pi" } else { "opencode" };
    wait_for_sessions(&matrix_store, &mattermost_store, mattermost_backend).await;

    let matrix = Store::new(&matrix_store)
        .get(
            "matrix",
            &format!("{MATRIX_ROOM}:{MATRIX_EVENT}"),
            "opencode",
        )
        .unwrap()
        .expect("Matrix conversation missing from persisted store");
    let mattermost = Store::new(&mattermost_store)
        .get(
            "mattermost",
            &format!("{MATTERMOST_CHANNEL}:{MATTERMOST_ROOT}"),
            mattermost_backend,
        )
        .unwrap()
        .expect("Mattermost conversation missing from persisted store");
    assert_ne!(
        matrix.session_id, mattermost.session_id,
        "Matrix and Mattermost must bind different ACP sessions"
    );
    if multi_agent {
        assert_eq!(std::path::Path::new(&matrix.cwd), homes[0]);
        assert_eq!(std::path::Path::new(&mattermost.cwd), homes[1]);
        assert!(
            Store::new(&matrix_store)
                .get(
                    "mattermost",
                    &format!("{MATTERMOST_CHANNEL}:{MATTERMOST_ROOT}"),
                    mattermost_backend
                )
                .unwrap()
                .is_none()
        );
        assert!(
            Store::new(&mattermost_store)
                .get(
                    "matrix",
                    &format!("{MATRIX_ROOM}:{MATRIX_EVENT}"),
                    "opencode"
                )
                .unwrap()
                .is_none()
        );
        assert!(!legacy_store.exists());
    }
    let matrix_reply = format!("{MATRIX_PROMPT} via {}", matrix.session_id);
    let mattermost_reply = format!("{MATTERMOST_PROMPT} via {}", mattermost.session_id);
    assert!(
        fixture
            .matrix_outbound
            .lock()
            .await
            .iter()
            .any(|(room, message)| {
                room == MATRIX_ROOM
                    && message["body"] == matrix_reply
                    && message["m.relates_to"]["rel_type"] == "m.thread"
                    && message["m.relates_to"]["event_id"] == MATRIX_EVENT
            }),
        "Matrix did not receive its own session's reply in the expected room and thread"
    );
    assert!(
        fixture.mattermost_outbound.lock().await.iter().any(|post| {
            post["message"] == mattermost_reply
                && post["channel_id"] == MATTERMOST_CHANNEL
                && post["root_id"] == MATTERMOST_ROOT
        }),
        "Mattermost inbound event did not produce a threaded Mattermost outbound post"
    );

    let status = service.stop().await;
    assert_eq!(status.code(), Some(0), "--serve did not stop cleanly");
    matrix_server.abort();
    mattermost_server.abort();
}
