use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use std::{
    net::SocketAddr,
    path::{Path, PathBuf},
    time::Duration,
};
use tokio::{
    net::TcpListener,
    process::{Child, Command},
    time::{sleep, timeout},
};

struct Server {
    child: Child,
    address: SocketAddr,
    compact_release: PathBuf,
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.child.start_kill();
    }
}

impl Server {
    async fn start(root: &Path, compact_failure: bool) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        drop(listener);
        let audit = root.join("native.jsonl");
        let marker = root.join("compact.marker");
        let compact_release = root.join("compact.release");
        let config = root.join("config.json");
        std::fs::write(
            &config,
            json!({
                "rateLimitSeconds": 0,
                "sessionStorePath": root.join("store.json"),
                "acp": {
                    "defaultBackend": "codex",
                    "timeoutMs": 3000,
                    "backends": {"codex": {
                        "command": env!("CARGO_BIN_EXE_codex-acp-rust"),
                        "args": [],
                        "env": {
                            "CODEX_PATH": env!("CARGO_BIN_EXE_acp-fixture"),
                            "ACP_CODEX_FIXTURE": "1",
                            "CODEX_FIXTURE_AUDIT": audit,
                            "CODEX_FIXTURE_COMPACT_MARKER": marker,
                            "CODEX_FIXTURE_COMPACT_RELEASE": compact_release,
                            "CODEX_FIXTURE_COMPACT_FAILURE": compact_failure.to_string(),
                        }
                    }}
                },
                "web": {"enabled": true, "host": "127.0.0.1", "port": address.port(), "allowedOrigins": ["http://127.0.0.1"]},
                "telegram": {"enabled": false}, "matrix": {"enabled": false}, "mattermost": {"enabled": false},
                "slack": {"enabled": false}, "discord": {"enabled": false}, "whatsapp": {"enabled": false}
            })
            .to_string(),
        )
        .unwrap();
        let home = root.join("home");
        let tmp = root.join("tmp");
        let sessions = root.join("sessions");
        std::fs::create_dir(&home).unwrap();
        std::fs::create_dir(&tmp).unwrap();
        let mut command = Command::new(env!("CARGO_BIN_EXE_soulsteads-messaging"));
        command
            .current_dir(root)
            .args(["--serve", "--config", config.to_str().unwrap()])
            .env_clear()
            .env("PATH", "/usr/bin:/bin")
            .env("HOME", home)
            .env("TMPDIR", tmp)
            .env("SESSION_BASE_DIR", sessions)
            .env("LANG", "C.UTF-8")
            .env("NO_COLOR", "1")
            .kill_on_drop(true)
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null());
        let child = command.spawn().unwrap();
        let server = Self {
            child,
            address,
            compact_release,
        };
        let health = reqwest::Client::new();
        timeout(Duration::from_secs(5), async {
            loop {
                if let Ok(response) = health
                    .get(format!("http://{}/health", server.address))
                    .send()
                    .await
                    && response.status().is_success()
                {
                    break;
                }
                sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        server
    }

    async fn stop(mut self) {
        let _ = self.child.kill().await;
        let _ = self.child.wait().await;
    }
}

async fn connect(
    server: &Server,
) -> tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>> {
    let (mut socket, _) = timeout(
        Duration::from_secs(5),
        tokio_tungstenite::connect_async(format!(
            "ws://{}/ws?clientId=compact-contract",
            server.address
        )),
    )
    .await
    .unwrap()
    .unwrap();
    let message = timeout(Duration::from_secs(5), socket.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let connected: Value = serde_json::from_str(message.to_text().unwrap()).unwrap();
    assert_eq!(connected["type"], "connected");
    socket
}

async fn prompt(
    socket: &mut tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
    text: &str,
) -> Vec<Value> {
    socket
        .send(json!({"type":"message","text":text}).to_string().into())
        .await
        .unwrap();
    collect_until_done(socket).await
}

async fn collect_until_done(
    socket: &mut tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
) -> Vec<Value> {
    timeout(Duration::from_secs(5), async {
        let mut events = vec![];
        loop {
            let event: Value =
                serde_json::from_str(socket.next().await.unwrap().unwrap().to_text().unwrap())
                    .unwrap();
            let done = event["type"] == "done";
            events.push(event);
            if done {
                return events;
            }
        }
    })
    .await
    .unwrap()
}

async fn wait_for_file(path: &Path) {
    timeout(Duration::from_secs(5), async {
        while !path.is_file() {
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
}

fn audit(root: &Path) -> Vec<Value> {
    std::fs::read_to_string(root.join("native.jsonl"))
        .unwrap_or_default()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect()
}

fn visible(events: &[Value]) -> String {
    events
        .iter()
        .filter(|event| {
            matches!(
                event["type"].as_str(),
                Some("chunk" | "response" | "notice")
            )
        })
        .filter_map(|event| event["text"].as_str())
        .collect()
}

#[tokio::test]
async fn compiled_web_api_codex_compact_acknowledges_only_after_native_terminal_and_recovers() {
    let root = tempfile::tempdir().unwrap();
    let server = Server::start(root.path(), false).await;
    let mut socket = connect(&server).await;
    let setup = prompt(&mut socket, "hello").await;
    assert_eq!(visible(&setup), "native reply", "{setup:?}");
    socket
        .send(
            json!({"type":"message","text":"/compact"})
                .to_string()
                .into(),
        )
        .await
        .unwrap();
    wait_for_file(&root.path().join("compact.marker")).await;
    let mut before_release = vec![];
    while let Ok(Some(Ok(message))) = timeout(Duration::from_millis(100), socket.next()).await {
        before_release.push(serde_json::from_str(message.to_text().unwrap()).unwrap());
    }
    assert!(
        before_release.iter().all(|event: &Value| {
            !matches!(event["type"].as_str(), Some("prompt_end" | "done"))
                && !event["text"]
                    .as_str()
                    .is_some_and(|text| text.contains("Context compacted"))
        }),
        "native terminal is held; acknowledgement must not arrive early: {before_release:?}"
    );
    std::fs::write(&server.compact_release, "release").unwrap();
    let mut compact = before_release;
    compact.extend(collect_until_done(&mut socket).await);
    assert_eq!(visible(&compact), "Context compacted", "{compact:?}");
    assert_eq!(
        compact
            .iter()
            .filter(|event| event["type"] == "prompt_end")
            .count(),
        1,
        "{compact:?}"
    );
    assert!(
        compact.iter().all(|event| event["type"] != "error"),
        "{compact:?}"
    );
    assert!(
        compact
            .iter()
            .all(|event| event["type"] != "session_state" || event["state"] != "stale-invalidated"),
        "{compact:?}"
    );
    assert!(!visible(&compact).contains("Sorry,"), "{compact:?}");
    assert_eq!(
        std::fs::read_to_string(root.path().join("compact.marker")).unwrap(),
        "item completed; turn still active"
    );
    let native = audit(root.path());
    assert_eq!(
        native
            .iter()
            .filter(|request| request["method"] == "thread/compact/start")
            .count(),
        1,
        "{native:?}"
    );
    assert_eq!(
        native
            .iter()
            .filter(|request| request["method"] == "turn/start")
            .count(),
        1,
        "{native:?}"
    );

    let followup = prompt(&mut socket, "hello").await;
    assert_eq!(visible(&followup), "native reply", "{followup:?}");
    assert!(
        followup.iter().all(|event| event["type"] != "error"),
        "{followup:?}"
    );
    let native = audit(root.path());
    assert_eq!(
        native
            .iter()
            .filter(|request| request["method"] == "turn/start")
            .count(),
        2,
        "{native:?}"
    );
    socket.close(None).await.unwrap();
    server.stop().await;
}

#[tokio::test]
async fn compiled_web_api_codex_compact_failure_has_no_ack_and_ordinary_empty_prompt_still_errors()
{
    let root = tempfile::tempdir().unwrap();
    let server = Server::start(root.path(), true).await;
    let mut socket = connect(&server).await;
    let setup = prompt(&mut socket, "hello").await;
    assert_eq!(visible(&setup), "native reply", "{setup:?}");
    let compact = prompt(&mut socket, "/compact").await;
    assert!(
        compact.iter().any(|event| event["type"] == "error"),
        "{compact:?}"
    );
    assert!(
        !visible(&compact).contains("Context compacted"),
        "{compact:?}"
    );
    assert!(!visible(&compact).contains("Sorry,"), "{compact:?}");

    let empty = prompt(&mut socket, "empty").await;
    assert!(
        empty.iter().any(|event| event["type"] == "error"),
        "{empty:?}"
    );
    assert!(
        empty
            .iter()
            .any(|event| event["type"] == "session_state" && event["state"] == "stale-invalidated"),
        "{empty:?}"
    );
    let native = audit(root.path());
    assert_eq!(
        native
            .iter()
            .filter(|request| request["method"] == "thread/compact/start")
            .count(),
        1,
        "{native:?}"
    );
    assert_eq!(
        native
            .iter()
            .filter(|request| request["method"] == "turn/start")
            .count(),
        3,
        "{native:?}"
    );
    assert!(
        native
            .iter()
            .all(|request| request["method"] != "turn/steer"),
        "{native:?}"
    );
    socket.close(None).await.unwrap();
    server.stop().await;
}
