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

#[cfg(unix)]
#[tokio::test]
async fn codex_queued_review_answers_after_restart() {
    queued_review_answers_after_restart("codex").await;
}

#[cfg(unix)]
#[tokio::test]
async fn opencode_queued_review_answers_after_restart() {
    queued_review_answers_after_restart("opencode").await;
}

#[cfg(unix)]
async fn queued_review_answers_after_restart(harness: &str) {
    use std::os::unix::fs::PermissionsExt;

    const REVISION: &str = "1234567890abcdef1234567890abcdef12345678";
    let root = tempfile::tempdir().unwrap();
    let path = root.path();
    let queue = path.join("review-queue");
    std::fs::create_dir(&queue).unwrap();
    std::fs::set_permissions(&queue, std::fs::Permissions::from_mode(0o700)).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    let audit = path.join("rpc.jsonl");
    let config = path.join("config.json");
    let script = path.join("script.json");
    let (reviewer_started, builder_started) =
        (path.join("reviewer-first"), path.join("builder-first"));
    let first_release = path.join("release-first");
    let (review_started, review_release) =
        (path.join("review-started"), path.join("review-release"));
    let (sender_started, sender_release) = (
        path.join("builder-reconnected"),
        path.join("release-sender"),
    );
    std::fs::write(&script, json!([
        {"text":"human reviewer turn","startedFile":reviewer_started,"releaseFile":first_release,"stopReason":"end_turn"},
        {"text":"human builder turn","startedFile":builder_started,"releaseFile":first_release,"stopReason":"end_turn"},
        {"text":"reviewer reattached","stopReason":"end_turn"},
        {"text":"Synthetic review verdict","startedFile":review_started,"releaseFile":review_release,"stopReason":"end_turn"},
        {"text":"builder checks review","startedFile":sender_started,"releaseFile":sender_release,"stopReason":"end_turn"}
    ]).to_string()).unwrap();
    let settings = json!({
        "rateLimitSeconds":0, "sessionStorePath":path.join("store.json"),
        "acp":{"defaultBackend":harness,"backends":{(harness):{
            "command":env!("CARGO_BIN_EXE_acp-fixture"),"args":[],
            "env":{"ACP_FIXTURE_RPC_AUDIT":audit,"ACP_FIXTURE_MATRIX_SCRIPT":script,
                   "ACP_FIXTURE_MATRIX_COUNTER":path.join("counter")}
        }}},
        "agentMessaging":{"enabled":true,"tools":{"enabled":true,"harness":harness,
            "reviewQueueDir":queue},"agents":[
            {"id":"builder","name":"Builder","target":{"platform":"web","context":"builder","backend":harness},"allowedSenders":[]},
            {"id":"reviewer","name":"Reviewer","target":{"platform":"web","context":"reviewer","backend":harness},
                "allowedSenders":["builder"],"acceptQueuedReviews":true}
        ]},
        "web":{"enabled":true,"host":"127.0.0.1","port":port,"allowedOrigins":["http://127.0.0.1"]},
        "matrix":{"enabled":false},"telegram":{"enabled":false},"mattermost":{"enabled":false},
        "discord":{"enabled":false},"slack":{"enabled":false},"whatsapp":{"enabled":false}
    });
    std::fs::write(&config, settings.to_string()).unwrap();
    std::fs::create_dir(path.join("home")).unwrap();
    let http = reqwest::Client::new();
    let start = || async {
        let mut child = Command::new(env!("CARGO_BIN_EXE_soulsteads-messaging"))
            .current_dir(path)
            .args(["--serve", "--config", config.to_str().unwrap()])
            .env_clear()
            .env("PATH", "/usr/bin:/bin")
            .env("HOME", path.join("home"))
            .env("SESSION_BASE_DIR", path)
            .stdout(std::process::Stdio::null())
            .kill_on_drop(true)
            .spawn()
            .unwrap();
        timeout(Duration::from_secs(10), async {
            while http
                .get(format!("http://127.0.0.1:{port}/health"))
                .send()
                .await
                .is_err()
            {
                assert!(child.try_wait().unwrap().is_none(), "bridge exited");
                sleep(Duration::from_millis(15)).await;
            }
        })
        .await
        .unwrap();
        child
    };
    let message = |owner: &'static str, text: &'static str| async move {
        let (mut socket, _) =
            tokio_tungstenite::connect_async(format!("ws://127.0.0.1:{port}/ws?clientId={owner}"))
                .await
                .unwrap();
        let _ = socket.next().await.unwrap().unwrap();
        socket
            .send(json!({"type":"message","text":text}).to_string().into())
            .await
            .unwrap();
        socket
    };
    let registration = |owner: &str| {
        std::fs::read_to_string(&audit)
            .unwrap()
            .lines()
            .rev()
            .filter_map(|line| serde_json::from_str::<Value>(line).ok())
            .find_map(|entry| {
                let cwd = entry["params"]["cwd"].as_str()?;
                if !cwd.ends_with(&format!("/{owner}")) {
                    return None;
                }
                entry["params"]["mcpServers"]
                    .as_array()?
                    .iter()
                    .find(|server| server["name"] == "soulsteads_agents")
                    .cloned()
            })
            .unwrap()
    };
    let call = |registration: Value, tool: &'static str, arguments: Value| {
        let http = http.clone();
        async move {
            http.post(registration["url"].as_str().unwrap())
                .header("Authorization", registration["headers"][0]["value"].as_str().unwrap())
                .json(&json!({"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":tool,"arguments":arguments}}))
                .send().await.unwrap().json::<Value>().await.unwrap()
        }
    };

    let mut child = start().await;
    let _reviewer = message("reviewer", "hold reviewer").await;
    await_marker(&reviewer_started).await;
    let _builder = message("builder", "hold builder").await;
    await_marker(&builder_started).await;
    let sender = registration("builder");
    let agents = call(sender.clone(), "list_agents", json!({})).await;
    assert_eq!(
        agents["result"]["structuredContent"]["agents"][0]["acceptsQueuedReviews"],
        true
    );
    let request = json!({"recipient":"reviewer","revision":REVISION,
        "idempotencyKey":"review-change-1","text":"Check the synthetic revision"});
    let accepted = call(sender.clone(), "send_agent", request.clone()).await;
    assert_eq!(
        accepted["result"]["structuredContent"]["status"], "queued",
        "{accepted}"
    );
    let id = accepted["result"]["structuredContent"]["requestId"].clone();
    assert!(id.is_string());
    let duplicate = call(sender.clone(), "send_agent", request.clone()).await;
    assert_eq!(duplicate["result"]["structuredContent"]["requestId"], id);
    let mut changed = request.clone();
    changed["text"] = json!("different review");
    assert_eq!(
        call(sender.clone(), "send_agent", changed).await["error"]["code"],
        -32602
    );
    let mut extra = request;
    extra["idempotencyKey"] = json!("another-review");
    assert_eq!(
        call(sender.clone(), "send_agent", extra).await["error"]["code"],
        -32001
    );
    let read = json!({"requestId":id});
    assert_eq!(
        call(
            registration("reviewer"),
            "agent_request_status",
            read.clone()
        )
        .await["error"]["code"],
        -32003
    );
    sleep(Duration::from_millis(1200)).await;
    let still_queued = call(sender, "agent_request_status", read.clone()).await;
    assert_eq!(
        still_queued["result"]["structuredContent"]["status"],
        "queued"
    );
    child.kill().await.unwrap();
    child.wait().await.unwrap();
    std::fs::write(&first_release, "").unwrap();

    let mut child = start().await;
    let mut reviewer = message("reviewer", "reattach reviewer").await;
    wait_done(&mut reviewer).await;
    await_marker(&review_started).await;
    let mut builder = message("builder", "retrieve review").await;
    await_marker(&sender_started).await;
    let sender = registration("builder");
    let running = call(sender.clone(), "agent_request_status", read.clone()).await;
    assert_eq!(
        running["result"]["structuredContent"]["status"], "running",
        "{running}"
    );
    std::fs::write(&review_release, "").unwrap();
    timeout(Duration::from_secs(6), async {
        loop {
            let answer = call(sender.clone(), "agent_request_status", read.clone()).await;
            let data = &answer["result"]["structuredContent"];
            if data["status"] == "answered" {
                assert_eq!(data["reply"], "Synthetic review verdict");
                assert_eq!(data["revision"], REVISION);
                break;
            }
            assert_eq!(data["status"], "running", "{answer}");
            sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    std::fs::write(&sender_release, "").unwrap();
    wait_done(&mut builder).await;
    let requests: Vec<Value> = std::fs::read_to_string(&audit)
        .unwrap()
        .lines()
        .filter_map(|line| serde_json::from_str(line).ok())
        .collect();
    let prompts: Vec<Value> = requests
        .iter()
        .filter(|entry| entry["method"] == "session/prompt")
        .flat_map(|entry| entry["params"]["prompt"].as_array().into_iter().flatten())
        .filter_map(|segment| serde_json::from_str(segment["text"].as_str()?).ok())
        .filter(|prompt: &Value| prompt["kind"] == "review")
        .collect();
    assert_eq!(prompts.len(), 1);
    assert_eq!(prompts[0]["sender"], "builder");
    assert_eq!(prompts[0]["requestId"], id);
    assert_eq!(prompts[0]["revision"], REVISION);
    child.kill().await.unwrap();
    child.wait().await.unwrap();
}

#[cfg(unix)]
async fn await_marker(path: &std::path::Path) {
    timeout(Duration::from_secs(10), async {
        while !path.exists() {
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("fixture marker missing at {}", path.display()));
}
