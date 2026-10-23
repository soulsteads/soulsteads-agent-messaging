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
async fn ask_agent_keeps_sender_scope_but_not_recipient_or_completed_scope() {
    let root = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    let audit = root.path().join("rpc.jsonl");
    let config = root.path().join("config.json");
    let script = root.path().join("script.json");
    let counter = root.path().join("counter");
    let held_started = root.path().join("held-started");
    let held_release = root.path().join("held-release");
    std::fs::write(
        &script,
        json!([
            {"text":"reviewer warm","stopReason":"end_turn"},
            {"text":"builder held","startedFile":held_started,"releaseFile":held_release,"stopReason":"end_turn"},
            {"text":"recipient reply","stopReason":"end_turn"}
        ]).to_string(),
    ).unwrap();
    let settings = json!({
        "rateLimitSeconds": 0,
        "sessionStorePath": root.path().join("store.json"),
        "acp": {"defaultBackend":"codex", "backends":{"codex":{
            "command": env!("CARGO_BIN_EXE_acp-fixture"), "args":[],
            "env":{"ACP_FIXTURE_RPC_AUDIT":audit,"ACP_FIXTURE_MATRIX_SCRIPT":script,
                   "ACP_FIXTURE_MATRIX_COUNTER":counter}
        }}},
        "agentMessaging":{"enabled":true,"tools":{"enabled":true,"harness":"codex"},"agents":[
            {"id":"builder","name":"Builder","target":{"platform":"web","context":"builder","backend":"codex"},"allowedSenders":["reviewer"]},
            {"id":"reviewer","name":"Reviewer","target":{"platform":"web","context":"reviewer","backend":"codex"},"allowedSenders":["builder"]}
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
    let (mut reviewer, _) =
        tokio_tungstenite::connect_async(format!("ws://127.0.0.1:{port}/ws?clientId=reviewer"))
            .await
            .unwrap();
    let _ = reviewer.next().await.unwrap().unwrap();
    reviewer
        .send(
            json!({"type":"message","text":"warm reviewer"})
                .to_string()
                .into(),
        )
        .await
        .unwrap();
    wait_done(&mut reviewer).await;

    let (mut builder, _) =
        tokio_tungstenite::connect_async(format!("ws://127.0.0.1:{port}/ws?clientId=builder"))
            .await
            .unwrap();
    let _ = builder.next().await.unwrap().unwrap();
    builder
        .send(
            json!({"type":"message","text":"hold builder"})
                .to_string()
                .into(),
        )
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
    let reviewer_registration = opened[0]["params"]["mcpServers"]
        .as_array()
        .unwrap()
        .iter()
        .find(|s| s["name"] == "soulsteads_agents")
        .unwrap()
        .clone();
    let builder_registration = opened[1]["params"]["mcpServers"]
        .as_array()
        .unwrap()
        .iter()
        .find(|s| s["name"] == "soulsteads_agents")
        .unwrap()
        .clone();
    let post_tool = |registration: Value, id: u64, name: &'static str, arguments: Value| {
        let client = client.clone();
        async move {
            client.post(registration["url"].as_str().unwrap())
                .header("Authorization", registration["headers"][0]["value"].as_str().unwrap())
                .json(&json!({"jsonrpc":"2.0","id":id,"method":"tools/call","params":{"name":name,"arguments":arguments}}))
                .send().await.unwrap().json::<Value>().await.unwrap()
        }
    };
    let denied = post_tool(reviewer_registration, 1, "list_agents", json!({})).await;
    assert_eq!(
        denied["error"]["code"], -32003,
        "inactive recipient endpoint must be denied"
    );
    let ask = post_tool(
        builder_registration.clone(),
        2,
        "ask_agent",
        json!({"recipient":"reviewer","text":"return the scripted reply"}),
    )
    .await;
    assert!(
        ask["result"]["content"][0]["text"]
            .as_str()
            .unwrap_or("")
            .contains("recipient reply"),
        "sender scope must survive the recipient turn: {ask}"
    );
    std::fs::write(&held_release, "release").unwrap();
    wait_done(&mut builder).await;
    let inactive = post_tool(builder_registration, 3, "list_agents", json!({})).await;
    assert_eq!(
        inactive["error"]["code"], -32003,
        "completed human root must revoke its endpoint scope"
    );
    builder.close(None).await.unwrap();
    reviewer.close(None).await.unwrap();
    child.kill().await.unwrap();
    child.wait().await.unwrap();
}

async fn wait_done(
    socket: &mut tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
) {
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
}

#[tokio::test]
async fn compiled_bridge_registers_tools_only_for_the_configured_codex_owner() {
    let root = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    let audit = root.path().join("rpc.jsonl");
    let config = root.path().join("config.json");
    let settings = json!({
        "rateLimitSeconds": 0,
        "sessionStorePath": root.path().join("store.json"),
        "acp": {"defaultBackend":"codex", "backends":{"codex":{
            "command": env!("CARGO_BIN_EXE_acp-fixture"), "args":[],
            "env":{"ACP_FIXTURE_RPC_AUDIT":audit}
        }}},
        "agentMessaging":{"enabled":true,"tools":{"enabled":true,"harness":"codex"},"agents":[
            {"id":"builder","name":"Builder","target":{"platform":"web","context":"builder","backend":"codex"},"allowedSenders":[]}
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
    for owner in ["builder", "unregistered"] {
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
    assert_eq!(opened.len(), 2);
    assert_eq!(
        opened[0]["params"]["mcpServers"][0]["name"], "soulsteads_agents",
        "configured owner must receive its staged tool registration"
    );
    assert_eq!(
        opened[1]["params"]["mcpServers"],
        json!([]),
        "an unregistered conversation must not receive tools"
    );
}
