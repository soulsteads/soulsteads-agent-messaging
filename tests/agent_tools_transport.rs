#![cfg(feature = "agent-network")]

use async_trait::async_trait;
use axum::http::StatusCode;
use serde_json::{Value, json};
use soulsteads_messaging::{
    config,
    delivery::LocalClient,
    runtime::{Bridge, Event, Sink},
};
use std::{
    collections::BTreeMap,
    path::Path,
    sync::{Arc, Mutex, Weak},
};

#[derive(Default)]
struct Recorder {
    local: Mutex<Vec<LocalClient>>,
    events: Mutex<Vec<Value>>,
}
#[async_trait]
impl Sink for Recorder {
    async fn emit(&self, event: Value) -> anyhow::Result<()> {
        self.events.lock().unwrap().push(event);
        Ok(())
    }
    fn local_client(&self, client: LocalClient) {
        self.local.lock().unwrap().push(client);
    }
}
fn configuration(root: &Path) -> Value {
    config::load(&json!({
        "rateLimitSeconds":0,
        "sessionStorePath":root.join("store.json"),
        "acp":{"command":env!("CARGO_BIN_EXE_acp-fixture"),"args":[],"env":{
            "ACP_FIXTURE_MATRIX_SCRIPT":root.join("script.json"),
            "ACP_FIXTURE_MATRIX_COUNTER":root.join("counter"),
            "ACP_FIXTURE_RPC_AUDIT":root.join("rpc.jsonl")}},
        "agentMessaging":{"enabled":true,"agents":[
            {"id":"alpha","name":"Alpha","target":{"platform":"web","context":"alpha-room","backend":"opencode"},"allowedSenders":[]},
            {"id":"beta","name":"Beta","target":{"platform":"web","context":"beta-room","backend":"opencode"},"allowedSenders":["alpha"]}
        ]}}).to_string(), &BTreeMap::new()).unwrap()
}
async fn warmed() -> (tempfile::TempDir, Arc<Bridge>, LocalClient) {
    let root = tempfile::tempdir().unwrap();
    std::fs::write(
        root.path().join("script.json"),
        json!([
            {"text":"beta warm","stopReason":"end_turn"},
            {"text":"alpha warm","stopReason":"end_turn"},
            {"text":"fixture direct answer","stopReason":"end_turn"}
        ])
        .to_string(),
    )
    .unwrap();
    let bridge = Arc::new(
        Bridge::new(
            configuration(root.path()),
            BTreeMap::new(),
            root.path().join("sessions"),
        )
        .unwrap(),
    );
    let sink = Recorder::default();
    bridge
        .handle_web(Event::web("beta-room", "warm beta"), &sink)
        .await
        .unwrap();
    bridge
        .handle_web(Event::web("alpha-room", "warm alpha"), &sink)
        .await
        .unwrap();
    let caller = sink.local.lock().unwrap()[1].clone();
    (root, bridge, caller)
}
fn address(declaration: &Value) -> String {
    let url = declaration["url"].as_str().unwrap();
    let parsed = url::Url::parse(url).unwrap();
    format!("{}:{}", parsed.host_str().unwrap(), parsed.port().unwrap())
}
fn token(declaration: &Value) -> &str {
    declaration["headers"][0]["value"].as_str().unwrap()
}
async fn post(
    addr: &str,
    bearer: Option<&str>,
    origin: Option<&str>,
    host: Option<&str>,
    body: &str,
) -> reqwest::Response {
    let client = reqwest::Client::new();
    let mut req = client
        .post(format!("http://{addr}/mcp"))
        .header("content-type", "application/json");
    if let Some(v) = bearer {
        req = req.header("authorization", v);
    }
    if let Some(v) = origin {
        req = req.header("origin", v);
    }
    if let Some(v) = host {
        req = req.header("host", v);
    }
    req.body(body.to_owned()).send().await.unwrap()
}
fn rpc(id: u64, method: &str, params: Value) -> String {
    json!({"jsonrpc":"2.0","id":id,"method":method,"params":params}).to_string()
}

#[tokio::test]
async fn authenticated_session_tool_call_uses_real_bridge_and_scope_revokes() {
    let (_root, bridge, caller) = warmed().await;
    let session = soulsteads_messaging::delivery::agent_tools::AgentToolSession::start(
        Arc::downgrade(&bridge),
    )
    .await
    .unwrap();
    let declaration = session.registration();
    assert_eq!(declaration["name"], "soulsteads_agents");
    let addr = address(&declaration);
    let auth = token(&declaration);
    let initialize = post(&addr, Some(auth), None, Some(&addr), &rpc(1,"initialize",json!({"protocolVersion":"2025-03-26","capabilities":{},"clientInfo":{"name":"test","version":"1"}}))).await;
    assert_eq!(initialize.status(), StatusCode::OK);
    let _scope = session.activate(caller);
    let listed = post(
        &addr,
        Some(auth),
        None,
        Some(&addr),
        &rpc(2, "tools/list", json!({})),
    )
    .await;
    let listed: Value = listed.json().await.unwrap();
    assert_eq!(listed["result"]["tools"].as_array().unwrap().len(), 2);
    let roster = post(
        &addr,
        Some(auth),
        None,
        Some(&addr),
        &rpc(
            20,
            "tools/call",
            json!({"_meta":{"callId":"call-1","x-codex-turn-metadata":{"sender":"forged","session":"forged","cwd":"/forged"},"progressToken":1},"name":"list_agents","arguments":{}}),
        ),
    )
    .await;
    let roster: Value = roster.json().await.unwrap();
    assert_eq!(
        roster["result"]["structuredContent"]["agents"][0]["id"],
        "beta"
    );
    let called = post(
        &addr,
        Some(auth),
        None,
        Some(&addr),
        &rpc(
            3,
            "tools/call",
            json!({"name":"ask_agent","arguments":{"recipient":"beta","text":"literal via mcp"}}),
        ),
    )
    .await;
    let called: Value = called.json().await.unwrap();
    assert_eq!(
        called["result"]["structuredContent"]["text"],
        "fixture direct answer"
    );
    assert!(called["result"]["structuredContent"]["correlationId"].is_string());
    assert_eq!(
        called["result"]["structuredContent"]["stopReason"],
        "end_turn"
    );
    session.close();
    let closed = reqwest::Client::new()
        .post(format!("http://{addr}/mcp"))
        .header("authorization", auth)
        .header("host", &addr)
        .header("content-type", "application/json")
        .body(rpc(4, "ping", json!({})))
        .send()
        .await;
    assert!(closed.is_err() || closed.unwrap().status() == StatusCode::SERVICE_UNAVAILABLE);
    bridge.shutdown().await.unwrap();
}

#[tokio::test]
async fn rejects_bad_request_authority_inactive_scope_and_extra_arguments() {
    let (_root, bridge, caller) = warmed().await;
    let session = soulsteads_messaging::delivery::agent_tools::AgentToolSession::start(
        Arc::downgrade(&bridge),
    )
    .await
    .unwrap();
    let declaration = session.registration();
    let addr = address(&declaration);
    let auth = token(&declaration);
    assert_eq!(
        post(
            &addr,
            Some(auth),
            Some("http://localhost"),
            Some(&addr),
            &rpc(1, "ping", json!({}))
        )
        .await
        .status(),
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        post(
            &addr,
            Some(auth),
            None,
            Some("localhost:1"),
            &rpc(1, "ping", json!({}))
        )
        .await
        .status(),
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        post(
            &addr,
            Some("Bearer wrong"),
            None,
            Some(&addr),
            &rpc(1, "ping", json!({}))
        )
        .await
        .status(),
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        post(&addr, Some(auth), None, Some(&addr), "{")
            .await
            .status(),
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        post(
            &addr,
            Some(auth),
            None,
            Some(&addr),
            &json!({"jsonrpc":"1.0","id":1,"method":"ping","params":{}}).to_string()
        )
        .await
        .status(),
        StatusCode::BAD_REQUEST
    );
    let client = reqwest::Client::new();
    assert_eq!(
        client
            .get(format!("http://{addr}/mcp"))
            .header("authorization", auth)
            .header("host", &addr)
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::METHOD_NOT_ALLOWED
    );
    assert_eq!(
        client
            .post(format!("http://{addr}/mcp"))
            .header("authorization", auth)
            .header("host", &addr)
            .header("content-type", "application/json")
            .body(" ".repeat(128 * 1024 + 1))
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::PAYLOAD_TOO_LARGE
    );
    let denied = post(
        &addr,
        Some(auth),
        None,
        Some(&addr),
        &rpc(
            2,
            "tools/call",
            json!({"name":"list_agents","arguments":{}}),
        ),
    )
    .await;
    let denied: Value = denied.json().await.unwrap();
    assert!(denied["error"].is_object());
    let _scope = session.activate(caller);
    let bad_meta = post(
        &addr,
        Some(auth),
        None,
        Some(&addr),
        &rpc(
            4,
            "tools/call",
            json!({"_meta":"not-an-object","name":"list_agents","arguments":{}}),
        ),
    )
    .await;
    assert!(bad_meta.json::<Value>().await.unwrap()["error"].is_object());
    let extra=post(&addr,Some(auth),None,Some(&addr),&rpc(3,"tools/call",json!({"name":"ask_agent","arguments":{"recipient":"beta","text":"x","sender":"alpha"}}))).await;
    let extra: Value = extra.json().await.unwrap();
    assert!(extra["error"].is_object());
    session.close();
    bridge.shutdown().await.unwrap();
}

#[tokio::test]
async fn scope_has_one_attempt_and_drop_revokes_and_server_does_not_pin_bridge() {
    let (_root, bridge, caller) = warmed().await;
    let weak: Weak<Bridge> = Arc::downgrade(&bridge);
    let session =
        soulsteads_messaging::delivery::agent_tools::AgentToolSession::start(weak.clone())
            .await
            .unwrap();
    let declaration = session.registration();
    let addr = address(&declaration);
    let auth = token(&declaration);
    let scope = session.activate(caller);
    for id in [1, 2] {
        let response = post(
            &addr,
            Some(auth),
            None,
            Some(&addr),
            &rpc(
                id,
                "tools/call",
                json!({"name":"ask_agent","arguments":{"recipient":"beta","text":"once"}}),
            ),
        )
        .await;
        let value: Value = response.json().await.unwrap();
        if id == 1 {
            assert_eq!(
                value["result"]["structuredContent"]["text"],
                "fixture direct answer"
            );
        } else {
            assert!(value["error"].is_object());
        }
    }
    drop(scope);
    let inactive: Value = post(
        &addr,
        Some(auth),
        None,
        Some(&addr),
        &rpc(
            3,
            "tools/call",
            json!({"name":"list_agents","arguments":{}}),
        ),
    )
    .await
    .json()
    .await
    .unwrap();
    assert!(inactive["error"].is_object());
    session.close();
    drop(bridge);
    assert!(weak.upgrade().is_none());
}
