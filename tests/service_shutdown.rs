use futures_util::{SinkExt, StreamExt};
use serde_json::json;
use std::{
    process::{Child, Command, Stdio},
    time::{Duration, Instant},
};
use tokio_tungstenite::tungstenite::Message;

struct Server(Child);
impl Drop for Server {
    fn drop(&mut self) {
        let _ = unsafe { libc::kill(self.0.id() as i32, libc::SIGKILL) };
        let _ = self.0.wait();
    }
}

#[tokio::test]
async fn sigterm_gracefully_stops_serve_and_reaps_owned_acp_child() {
    let directory = tempfile::tempdir().unwrap();
    let child_pid = directory.path().join("owned-child.pid");
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    let config = directory.path().join("bridge.json");
    std::fs::write(&config, json!({
        "rateLimitSeconds":0,
        "sessionStorePath":directory.path().join("store.json"),
        "acp":{"command":env!("CARGO_BIN_EXE_acp-fixture"),"args":[],"env":{"ACP_FIXTURE_CHILD_PID":child_pid}},
        "web":{"enabled":true,"host":"127.0.0.1","port":port,"allowedOrigins":["*"]}
    }).to_string()).unwrap();
    let mut server = Server(
        Command::new(env!("CARGO_BIN_EXE_soulsteads-messaging"))
            .args(["--serve", "--config", config.to_str().unwrap()])
            .current_dir(directory.path())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap(),
    );
    let address = format!("ws://127.0.0.1:{port}/ws?clientId=sigterm-test");
    let deadline = Instant::now() + Duration::from_secs(5);
    let (mut socket, _) = loop {
        match tokio_tungstenite::connect_async(&address).await {
            Ok(connection) => break connection,
            Err(error) if Instant::now() < deadline => {
                let _ = error;
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            Err(error) => panic!("serve did not accept loopback WebSocket: {error}"),
        }
    };
    let _ = socket.next().await;
    socket
        .send(Message::Text(
            json!({"type":"message","text":"shutdown probe"})
                .to_string()
                .into(),
        ))
        .await
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    while !child_pid.exists() && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(
        child_pid.exists(),
        "real serve request must start the configured ACP process"
    );
    let owned_pid: i32 = std::fs::read_to_string(&child_pid)
        .unwrap()
        .parse()
        .unwrap();
    assert_ne!(owned_pid, std::process::id() as i32);
    assert_eq!(
        unsafe { libc::kill(server.0.id() as i32, libc::SIGTERM) },
        0
    );
    let deadline = Instant::now() + Duration::from_secs(7);
    let status = loop {
        if let Some(status) = server.0.try_wait().unwrap() {
            break status;
        }
        if Instant::now() >= deadline {
            panic!("SIGTERM did not complete bounded graceful shutdown");
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    assert_eq!(
        status.code(),
        Some(0),
        "SIGTERM should be a successful service stop"
    );
    drop(socket);
    let deadline = Instant::now() + Duration::from_secs(2);
    while unsafe { libc::kill(owned_pid, 0) } == 0 && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_ne!(
        unsafe { libc::kill(owned_pid, 0) },
        0,
        "Bridge shutdown must clean up its owned ACP child"
    );
}
