use soulsteads_messaging::config;
use serde_json::json;
use std::{collections::BTreeMap, process::Command};

#[test]
fn project_dotenv_precedence_config_cache_and_connector_overrides() {
    let directory = tempfile::tempdir().unwrap();
    std::fs::write(directory.path().join(".env"),"WEB_HOST=127.0.0.1\nWEB_PORT=4000\nTELEGRAM_BOT_TOKEN=fixture-token\nexport BOT_LABEL=\"from fixture\"\n").unwrap();
    let ambient = BTreeMap::from([("WEB_PORT".into(), "4001".into())]);
    let env = config::project_environment(directory.path(), ambient).unwrap();
    assert_eq!(env["WEB_PORT"], "4001");
    assert_eq!(env["BOT_LABEL"], "from fixture");
    let path = directory.path().join("soulsteads-messaging.json");
    std::fs::write(
        &path,
        json!({"botName":"{env:BOT_LABEL}","telegram":{"token":"config-token"}}).to_string(),
    )
    .unwrap();
    let mut loader = config::Loader::default();
    let mut loaded = loader.load(directory.path(), None, &env).unwrap();
    std::fs::write(&path, "{}").unwrap();
    assert_eq!(loader.load(directory.path(), None, &env).unwrap(), loaded);
    config::apply_runtime_overrides(&mut loaded, &env).unwrap();
    assert_eq!(loaded["telegram"]["token"], "fixture-token");
    assert_eq!(loaded["web"]["port"], 4001);
    assert_eq!(loaded["web"]["host"], "127.0.0.1");
    loader.clear();
    assert_eq!(
        loader.load(directory.path(), None, &env).unwrap()["botName"],
        "oc"
    );
}

#[test]
fn loader_reads_explicit_custom_config_path() {
    let directory = tempfile::tempdir().unwrap();
    let custom = directory.path().join("elsewhere/custom.json");
    std::fs::create_dir_all(custom.parent().unwrap()).unwrap();
    std::fs::write(&custom, r#"{"botName":"custom-path-bot"}"#).unwrap();

    let loaded = config::Loader::default()
        .load(directory.path(), Some(&custom), &BTreeMap::new())
        .unwrap();

    assert_eq!(loaded["botName"], "custom-path-bot");
}

#[test]
fn executable_help_version_and_check_do_not_start_services() {
    let directory = tempfile::tempdir().unwrap();
    for arguments in [vec!["--help"], vec!["--version"], vec!["--check-config"]] {
        let output = Command::new(env!("CARGO_BIN_EXE_soulsteads-messaging"))
            .args(&arguments)
            .current_dir(directory.path())
            .output()
            .unwrap();
        assert!(output.status.success());
        assert!(
            !output.stdout.is_empty(),
            "CLI must implement {arguments:?}"
        );
    }
}

#[test]
fn executable_check_alias_validates_isolated_config_without_starting_backend() {
    let directory = tempfile::tempdir().unwrap();
    std::fs::write(
        directory.path().join("soulsteads-messaging.json"),
        json!({"acp":{"command":"absent-backend"}}).to_string(),
    )
    .unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_soulsteads-messaging"))
        .arg("--check")
        .current_dir(directory.path())
        .env_clear()
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(String::from_utf8_lossy(&output.stdout).contains("no connectors started"));
    assert!(!directory.path().join("acp-sessions.json").exists());
}
