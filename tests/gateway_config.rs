#![cfg(unix)]

use serde_json::{Value, json};
use soulsteads_messaging::{config, gateway::Gateway, runtime::Bridge, web};
use std::{
    collections::BTreeMap, fs, future::IntoFuture, os::unix::fs::DirBuilderExt, path::Path,
    process::Command, sync::Arc, time::Duration,
};
use tokio_tungstenite::{
    connect_async,
    tungstenite::{Error, client::IntoClientRequest},
};

fn private_directory(path: &Path) {
    fs::DirBuilder::new().mode(0o700).create(path).unwrap();
}

#[test]
fn check_config_validates_agent_bindings_without_opening_state_or_connecting() {
    let root = tempfile::tempdir().unwrap();
    let state = root.path().canonicalize().unwrap().join("state");
    private_directory(&state);
    private_directory(&state.join("research"));
    private_directory(&state.join("build"));
    let path = root.path().join("gateway.json");
    let check = |supplied: &str| {
        fs::write(&path, supplied).unwrap();
        Command::new(env!("CARGO_BIN_EXE_soulsteads-messaging"))
            .current_dir(root.path())
            .env_clear()
            .env("RESEARCH_BOT", "101:fixture-research")
            .env("BUILD_BOT", "102:fixture-build")
            .env("TELEGRAM_BOT_TOKEN", "103:must-not-override")
            .env("TELEGRAM_ALLOWED_USERS", "999")
            .args(["--check-config", "--config"])
            .arg(&path)
            .output()
            .unwrap()
    };
    let base = json!({"stateDir":state,"agents":[
        {"id":"research","config":{"telegram":{"enabled":true,"token":"{env:RESEARCH_BOT}","allowedUsers":["1"]}}},
        {"id":"build","config":{"telegram":{"enabled":true,"token":"{env:BUILD_BOT}","allowedUsers":["2"]}}}
    ]});
    let result = check(&base.to_string());
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    let invalid: &[(&str, Value, &str)] = &[
        ("/agents", json!([]), "agents"),
        (
            "/agents/1/config",
            json!({"acp": [], "web": {"enabled": true}}),
            "acp must be an object",
        ),
        ("/agents/1/id", json!("research"), "Duplicate agent ID"),
        ("/agents/1/id", json!("../build"), "agent ID"),
        (
            "/agents/1/config/telegram/token",
            json!("101:fixture-rotated"),
            "Duplicate Telegram bot",
        ),
        (
            "/agents/1/config/telegram/token",
            json!("{env:UNSET_BOT}"),
            "telegram.token",
        ),
        (
            "/agents/1/config/telegram/allowedUsers",
            json!([]),
            "allowedUsers",
        ),
        (
            "/agents/1/config/telegram/enabled",
            json!(false),
            "No connectors enabled",
        ),
        (
            "/agents/1/config/telegram/enabled",
            json!("true"),
            "enabled must be a boolean",
        ),
    ];
    for (pointer, replacement, diagnostic) in invalid {
        let mut supplied = base.clone();
        *supplied.pointer_mut(pointer).unwrap() = replacement.clone();
        let result = check(&supplied.to_string());
        let stderr = String::from_utf8_lossy(&result.stderr);
        assert!(!result.status.success(), "invalid {pointer} was accepted");
        assert!(
            stderr.contains(diagnostic),
            "unexpected diagnostic: {stderr}"
        );
        assert!(!stderr.contains("fixture-") && result.stdout.is_empty());
    }
    let duplicate = base.to_string().replacen(
        "\"id\":\"build\"",
        "\"id\":\"research\",\"id\":\"build\"",
        1,
    );
    assert!(!check(&duplicate).status.success());
    assert_eq!(fs::read_dir(state.join("research")).unwrap().count(), 0);
    assert_eq!(fs::read_dir(state.join("build")).unwrap().count(), 0);
}

#[test]
fn check_config_requires_exact_origins_for_enabled_gateway_web() {
    let root = tempfile::tempdir().unwrap();
    let state = root.path().canonicalize().unwrap().join("state");
    private_directory(&state);
    private_directory(&state.join("build"));
    let path = root.path().join("gateway.json");
    let check = |supplied: Value| {
        fs::write(&path, supplied.to_string()).unwrap();
        Command::new(env!("CARGO_BIN_EXE_soulsteads-messaging"))
            .current_dir(root.path())
            .env_clear()
            .env("WEB_ALLOWED_ORIGINS", "*")
            .env("BROWSER_ORIGIN", "*")
            .args(["--check-config", "--config"])
            .arg(&path)
            .output()
            .unwrap()
    };
    let base = json!({"stateDir":state,"agents":[
        {"id":"build","config":{"web":{"enabled":true}}}
    ]});
    let same_origin = json!("http://127.0.0.1:3420");
    for (case, origins) in [
        ("omitted", None),
        ("wildcard", Some(json!([same_origin, "*"]))),
        ("wildcard host", Some(json!(["https://*.example.org"]))),
        (
            "interpolated wildcard",
            Some(json!(["{env:BROWSER_ORIGIN}"])),
        ),
        ("empty", Some(json!([]))),
        ("null", Some(Value::Null)),
        ("scalar", Some(same_origin.clone())),
        ("mixed types", Some(json!([same_origin, 42]))),
        ("opaque origin", Some(json!(["null"]))),
        ("page URL", Some(json!(["http://127.0.0.1:3420/chat"]))),
    ] {
        let mut supplied = base.clone();
        if let Some(origins) = origins {
            supplied["agents"][0]["config"]["web"]["allowedOrigins"] = origins;
        }
        let result = check(supplied);
        assert!(!result.status.success(), "{case} origins were accepted");
        let stderr = String::from_utf8_lossy(&result.stderr);
        assert!(stderr.contains("web.allowedOrigins"), "{case}: {stderr}");
    }
    let mut supplied = base;
    supplied["agents"][0]["config"]["web"]["allowedOrigins"] =
        json!([same_origin, "https://chat.example.org"]);
    let result = check(supplied);
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    assert_eq!(fs::read_dir(state.join("build")).unwrap().count(), 0);
}

#[tokio::test]
async fn gateway_web_upgrades_only_listed_browser_origins() {
    let root = tempfile::tempdir().unwrap();
    let state = root.path().canonicalize().unwrap().join("state");
    private_directory(&state);
    let agent_state = state.join("build");
    private_directory(&agent_state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let same_origin = format!("http://{address}");
    let env = BTreeMap::from([("BROWSER_ORIGIN".to_owned(), same_origin.clone())]);
    let gateway = Gateway::load(
        json!({"stateDir":state,"agents":[{"id":"build","config":{"web":{
            "enabled":true,"port":address.port(),"allowedOrigins":["{env:BROWSER_ORIGIN}"]
        }}}]}),
        &env,
    )
    .unwrap();
    let config = &gateway.agents[0].config;
    assert_eq!(config["web"]["host"], "127.0.0.1");
    let bridge = Arc::new(
        Bridge::new(
            config.clone(),
            BTreeMap::new(),
            agent_state.join("sessions"),
        )
        .unwrap(),
    );
    let server = tokio::spawn(
        axum::serve(listener, web::router(bridge.clone(), config["web"].clone())).into_future(),
    );
    let request = |origin: &str| {
        let mut request = format!("ws://{address}/ws").into_client_request().unwrap();
        request
            .headers_mut()
            .insert("origin", origin.parse().unwrap());
        request
    };
    let (mut socket, response) =
        tokio::time::timeout(Duration::from_secs(5), connect_async(request(&same_origin)))
            .await
            .unwrap()
            .unwrap();
    assert_eq!(response.status(), 101);
    assert_eq!(
        response.headers()["access-control-allow-origin"],
        same_origin
    );
    socket.close(None).await.unwrap();
    let denied = tokio::time::timeout(
        Duration::from_secs(5),
        connect_async(request("https://unlisted.example.org")),
    )
    .await
    .unwrap();
    let Err(Error::Http(response)) = denied else {
        panic!("unlisted origin did not receive an HTTP rejection: {denied:?}");
    };
    assert_eq!(response.status(), 403);
    assert!(
        !response
            .headers()
            .contains_key("access-control-allow-origin")
    );
    server.abort();
    assert!(server.await.unwrap_err().is_cancelled());
    bridge.shutdown().await.unwrap();
}

#[test]
fn single_agent_web_keeps_legacy_origin_default() {
    let legacy = config::load(r#"{"web":{"enabled":true}}"#, &BTreeMap::new()).unwrap();
    assert_eq!(legacy["web"]["allowedOrigins"], json!(["*"]));
}

#[test]
fn check_config_rejects_aliased_agent_state() {
    let root = tempfile::tempdir().unwrap();
    let state = root.path().canonicalize().unwrap().join("state");
    private_directory(&state);
    private_directory(&state.join("first"));
    std::os::unix::fs::symlink(state.join("first"), state.join("second")).unwrap();
    let config = root.path().join("gateway.json");
    fs::write(
        &config,
        json!({"stateDir":state,"agents":[
            {"id":"first","config":{"web":{"enabled":true,"port":4341,"allowedOrigins":["http://127.0.0.1:4341"]}}},
            {"id":"second","config":{"web":{"enabled":true,"port":4342,"allowedOrigins":["http://127.0.0.1:4342"]}}}
        ]})
        .to_string(),
    )
    .unwrap();
    let result = Command::new(env!("CARGO_BIN_EXE_soulsteads-messaging"))
        .current_dir(root.path())
        .env_clear()
        .args(["--check-config", "--config"])
        .arg(&config)
        .output()
        .unwrap();
    assert!(
        !result.status.success(),
        "two agents were allowed to share one state directory"
    );
    assert!(String::from_utf8_lossy(&result.stderr).contains("agent state directory"));
    assert_eq!(fs::read_dir(state.join("first")).unwrap().count(), 0);
}
