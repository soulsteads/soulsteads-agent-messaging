#![cfg(unix)]

use serde_json::{Value, json};
use std::{fs, os::unix::fs::DirBuilderExt, path::Path, process::Command};

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
            {"id":"first","config":{"web":{"enabled":true,"port":4341}}},
            {"id":"second","config":{"web":{"enabled":true,"port":4342}}}
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
