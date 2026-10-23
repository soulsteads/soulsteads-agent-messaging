#![cfg(feature = "agent-network")]

use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use std::{collections::BTreeMap, time::Duration};
use tokio::{
    net::TcpListener,
    process::Command,
    time::{sleep, timeout},
};

#[tokio::test]
async fn opencode_registration_is_exactly_owned_and_active_root_can_call_it() {
    let root = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    let audit = root.path().join("rpc.jsonl");
    let script = root.path().join("script.json");
    let held_started = root.path().join("held-started");
    let held_release = root.path().join("held-release");
    std::fs::write(&script, json!([{"text":"builder held","startedFile":held_started,"releaseFile":held_release,"stopReason":"end_turn"}]).to_string()).unwrap();
    let config = root.path().join("config.json");
    let settings = json!({
        "rateLimitSeconds": 0,
        "sessionStorePath": root.path().join("store.json"),
        "acp": {"defaultBackend":"opencode", "backends": {
            "opencode":{"command":env!("CARGO_BIN_EXE_acp-fixture"),"args":[],"env":{"ACP_FIXTURE_RPC_AUDIT":audit,"ACP_FIXTURE_MATRIX_SCRIPT":script,"ACP_FIXTURE_MATRIX_COUNTER":root.path().join("counter")}},
            "codex":{"command":env!("CARGO_BIN_EXE_acp-fixture"),"args":[],"env":{"ACP_FIXTURE_RPC_AUDIT":audit}}
        }},
        "agentMessaging":{"enabled":true,"tools":{"enabled":true,"harness":"opencode"},"agents":[
            {"id":"builder","name":"Builder","target":{"platform":"web","context":"builder","backend":"opencode"},"allowedSenders":[]},
            {"id":"wrong","name":"Wrong backend","target":{"platform":"web","context":"wrong-backend","backend":"codex"},"allowedSenders":[]}
        ]},
        "web":{"enabled":true,"host":"127.0.0.1","port":port,"allowedOrigins":["http://127.0.0.1"]},
        "matrix":{"enabled":false},"telegram":{"enabled":false},"mattermost":{"enabled":false},
        "discord":{"enabled":false},"slack":{"enabled":false},"whatsapp":{"enabled":false}
    });
    std::fs::write(&config, settings.to_string()).unwrap();
    let home = root.path().join("home");
    std::fs::create_dir(&home).unwrap();
    let mut child = Command::new(env!("CARGO_BIN_EXE_soulsteads-messaging"))
        .current_dir(root.path())
        .args(["--serve", "--config", config.to_str().unwrap()])
        .env_clear()
        .envs(BTreeMap::from([
            ("PATH", "/usr/bin:/bin"),
            ("HOME", home.to_str().unwrap()),
            ("SESSION_BASE_DIR", root.path().to_str().unwrap()),
        ]))
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let client = reqwest::Client::new();
    timeout(Duration::from_secs(10), async {
        loop {
            if client
                .get(format!("http://127.0.0.1:{port}/health"))
                .send()
                .await
                .is_ok()
            {
                break;
            }
            assert!(
                child.try_wait().unwrap().is_none(),
                "bridge exited before readiness"
            );
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    let (mut socket, _) =
        tokio_tungstenite::connect_async(format!("ws://127.0.0.1:{port}/ws?clientId=builder"))
            .await
            .unwrap();
    let _ = socket.next().await.unwrap().unwrap();
    socket
        .send(json!({"type":"message","text":"hello"}).to_string().into())
        .await
        .unwrap();
    timeout(Duration::from_secs(10), async {
        while !held_started.exists() {
            sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    let requests: Vec<Value> = std::fs::read_to_string(&audit)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    let opened: Vec<_> = requests
        .iter()
        .filter(|r| r["method"] == "session/new")
        .collect();
    assert_eq!(opened.len(), 1);
    let registration = &opened[0]["params"]["mcpServers"][0];
    assert_eq!(
        registration["name"], "soulsteads_agents",
        "exact configured OpenCode owner receives registration"
    );
    let response = client.post(registration["url"].as_str().unwrap())
        .header("Authorization", registration["headers"][0]["value"].as_str().unwrap())
        .json(&json!({"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"list_agents","arguments":{}}}))
        .send().await.unwrap().json::<Value>().await.unwrap();
    assert_eq!(
        response["result"]["structuredContent"]["agents"],
        json!([]),
        "active registered endpoint returns a permission-filtered roster, never self: {response}"
    );
    std::fs::write(&held_release, "release").unwrap();
    timeout(Duration::from_secs(10), async {
        loop {
            let event: Value =
                serde_json::from_str(socket.next().await.unwrap().unwrap().to_text().unwrap())
                    .unwrap();
            if event["type"] == "done" {
                break;
            }
        }
    })
    .await
    .unwrap();
    let revoked = client.post(registration["url"].as_str().unwrap())
        .header("Authorization", registration["headers"][0]["value"].as_str().unwrap())
        .json(&json!({"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"list_agents","arguments":{}}}))
        .send().await.unwrap().json::<Value>().await.unwrap();
    assert_eq!(
        revoked["error"]["code"], -32003,
        "completed root must revoke endpoint scope"
    );
    socket.close(None).await.unwrap();
    for owner in ["wrong-backend", "unregistered"] {
        let (mut socket, _) =
            tokio_tungstenite::connect_async(format!("ws://127.0.0.1:{port}/ws?clientId={owner}"))
                .await
                .unwrap();
        let _ = socket.next().await.unwrap().unwrap();
        socket
            .send(json!({"type":"message","text":"hello"}).to_string().into())
            .await
            .unwrap();
        timeout(Duration::from_secs(10), async {
            loop {
                let event: Value =
                    serde_json::from_str(socket.next().await.unwrap().unwrap().to_text().unwrap())
                        .unwrap();
                if event["type"] == "done" {
                    break;
                }
            }
        })
        .await
        .unwrap();
        socket.close(None).await.unwrap();
    }
    let requests: Vec<Value> = std::fs::read_to_string(&audit)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    child.kill().await.unwrap();
    child.wait().await.unwrap();
    let opened: Vec<_> = requests
        .iter()
        .filter(|r| r["method"] == "session/new")
        .collect();
    assert_eq!(opened.len(), 3);
    assert_eq!(
        opened[1]["params"]["mcpServers"],
        json!([]),
        "wrong backend target must not receive tools"
    );
    assert_eq!(
        opened[2]["params"]["mcpServers"],
        json!([]),
        "unregistered context must not receive tools"
    );
}
