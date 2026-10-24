use serde_json::{Value, json};
use soulsteads_messaging::config::{self, Loader};
use std::{collections::BTreeMap, path::Path};

fn valid(verifier: &Path) -> Value {
    json!({"a2a":{"version":1,"enabled":true,"peers":[{"id":"review-peer","verifierPath":verifier}],"agents":[{"id":"reviewer","backend":"opencode","allowedPeers":["review-peer"]}]}})
}

fn load(value: Value) -> anyhow::Result<Value> {
    config::load(&value.to_string(), &BTreeMap::new())
}

#[cfg(feature = "a2a")]
#[test]
fn accepts_enabled_config_without_touching_verifier_reference() {
    let dir = tempfile::tempdir().unwrap();
    let verifier = dir.path().join("absent.verifier");
    let result = load(valid(&verifier)).expect("valid inert configuration");
    assert_eq!(result["a2a"]["enabled"], true);
    assert!(!verifier.exists());
}

#[test]
fn minimal_disabled_section_is_valid_and_absent_section_keeps_legacy_shape() {
    assert_eq!(
        load(json!({"a2a":{"enabled":false}})).unwrap()["a2a"]["enabled"],
        false
    );
    assert!(load(json!({})).unwrap().get("a2a").is_none());
}

#[test]
fn rejects_unknown_fields_even_when_disabled() {
    let error = load(json!({"a2a":{"enabled":false,"listen":"127.0.0.1"}})).unwrap_err();
    assert!(error.to_string().contains("unsupported"), "{error}");
}

#[test]
fn rejects_nested_unknown_fields_even_when_disabled() {
    let error =
        load(json!({"a2a":{"enabled":false,"peers":[{"id":"p","token":"secret"}]}})).unwrap_err();
    assert!(error.to_string().contains("a2a peer"), "{error}");
}

#[test]
fn rejects_substitution_syntax_in_opaque_identifiers() {
    let dir = tempfile::tempdir().unwrap();
    let mut value = valid(&dir.path().join("absent.verifier"));
    value["a2a"]["peers"][0]["id"] = "{env:PEER}".into();
    let error = config::load(
        &value.to_string(),
        &BTreeMap::from([("PEER".into(), "review-peer".into())]),
    )
    .unwrap_err();
    assert!(error.to_string().contains("substitution"), "{error}");
}

#[test]
fn rejects_substitution_syntax_in_verifier_reference() {
    let value = json!({"a2a":{"version":1,"enabled":true,"peers":[{"id":"peer","verifierPath":"/abs/{env:HOME}/peer"}],"agents":[{"id":"agent","backend":"opencode","allowedPeers":["peer"]}]}});
    let error = load(value).unwrap_err();
    assert!(error.to_string().contains("substitution"), "{error}");
}

#[test]
fn loader_propagates_supplied_a2a_validation_errors() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(
        dir.path().join("soulsteads-messaging.json"),
        r#"{"a2a":{"enabled":true,"version":2}}"#,
    )
    .unwrap();
    let mut loader = Loader::default();
    assert!(loader.load(dir.path(), None, &BTreeMap::new()).is_err());
}

#[test]
fn explicit_loader_propagates_supplied_a2a_validation_errors() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("chosen.json");
    std::fs::write(&path, r#"{"a2a":{"enabled":true,"version":2}}"#).unwrap();
    let mut loader = Loader::default();
    assert!(
        loader
            .load(dir.path(), Some(&path), &BTreeMap::new())
            .is_err()
    );
}

#[test]
fn raw_bridge_rejects_invalid_a2a_config() {
    let mut value = config::defaults();
    value["sessionStorePath"] = "/tmp/a2a-unused-store.json".into();
    value["a2a"] = json!({"enabled":true,"version":2});
    let error =
        soulsteads_messaging::runtime::Bridge::new(value, BTreeMap::new(), std::env::temp_dir())
            .err()
            .expect("invalid A2A must fail");
    assert!(error.to_string().contains("version"), "{error}");
}

#[cfg(feature = "a2a")]
#[test]
fn rejects_duplicate_agent_ids() {
    let dir = tempfile::tempdir().unwrap();
    let mut value = valid(&dir.path().join("absent.verifier"));
    let duplicate = value["a2a"]["agents"][0].clone();
    value["a2a"]["agents"]
        .as_array_mut()
        .unwrap()
        .push(duplicate);
    let error = load(value).unwrap_err();
    assert!(
        error.to_string().contains("agent IDs must be unique"),
        "{error}"
    );
}

#[test]
fn rejects_bad_nested_peer_types_even_when_disabled() {
    let error = load(json!({"a2a":{"enabled":false,"peers":[{"id":17,"verifierPath":false}]}}))
        .unwrap_err();
    assert!(error.to_string().contains("peer id"), "{error}");
}

#[cfg(not(feature = "a2a"))]
#[test]
fn raw_bridge_rejects_enabled_a2a_when_feature_is_off() {
    let mut value = config::defaults();
    value["sessionStorePath"] = "/tmp/a2a-unused-store.json".into();
    value["a2a"] = json!({"version":1,"enabled":true,"peers":[{"id":"p","verifierPath":"/absent"}],"agents":[{"id":"a","backend":"codex","allowedPeers":["p"]}]});
    let error =
        soulsteads_messaging::runtime::Bridge::new(value, BTreeMap::new(), std::env::temp_dir())
            .err()
            .expect("feature-off Bridge must reject enabled A2A");
    assert!(error.to_string().contains("a2a build feature"), "{error}");
}

#[cfg(feature = "a2a")]
#[test]
fn backend_resolution_matches_runtime_supported_names_and_defaults() {
    let mut value = config::defaults();
    value["a2a"] = json!({"version":1,"enabled":true,"peers":[{"id":"p","verifierPath":"/absent"}],"agents":[{"id":"a","backend":"codex","allowedPeers":["p"]}]});
    assert!(
        load(value.clone()).is_ok(),
        "embedded codex backend must resolve"
    );
    value["a2a"]["agents"][0]["backend"] = "custom".into();
    value["acp"]["backends"]["custom"] = json!({"command":"fake"});
    assert!(
        load(value).is_err(),
        "unrecognized runtime backend names must not be granted"
    );
}

#[cfg(feature = "a2a")]
#[test]
fn legacy_normalization_error_does_not_panic_or_drop_supplied_a2a() {
    let mut value = config::defaults();
    value["a2a"] = json!({"version":1,"enabled":true,"peers":[{"id":"p","verifierPath":"/absent"}],"agents":[{"id":"a","backend":"codex","allowedPeers":["p"]}]});
    value["web"] = false.into();
    let result = std::panic::catch_unwind(|| load(value));
    assert!(
        result.is_ok(),
        "malformed legacy fields must return an error, not panic"
    );
    assert!(
        result.unwrap().is_err(),
        "A2A must not be erased by legacy normalization fallback"
    );
}

#[cfg(not(feature = "a2a"))]
#[test]
fn feature_off_rejects_enabled_configuration() {
    let dir = tempfile::tempdir().unwrap();
    let error = load(valid(&dir.path().join("absent.verifier"))).unwrap_err();
    assert!(error.to_string().contains("a2a build feature"), "{error}");
}

#[cfg(feature = "a2a")]
#[test]
fn feature_on_accepts_enabled_inert_configuration() {
    let dir = tempfile::tempdir().unwrap();
    assert!(load(valid(&dir.path().join("absent.verifier"))).is_ok());
}

#[cfg(all(feature = "a2a", unix))]
#[test]
fn cli_check_config_succeeds_but_ordinary_startup_refuses_before_side_effects() {
    let root = tempfile::tempdir().unwrap();
    let home = root.path().join("home");
    let cwd = root.path().join("cwd");
    std::fs::create_dir_all(&home).unwrap();
    std::fs::create_dir_all(&cwd).unwrap();
    let verifier = root.path().join("missing.verifier");
    let config_path = cwd.join("config.json");
    let mut value = config::defaults();
    value["sessionStorePath"] = root
        .path()
        .join("missing/session.json")
        .to_string_lossy()
        .to_string()
        .into();
    value["web"]["enabled"] = true.into();
    value["web"]["host"] = "127.0.0.1".into();
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    value["web"]["port"] = listener.local_addr().unwrap().port().into();
    let marker = root.path().join("backend.marker");
    let backend = root.path().join("fake-backend");
    std::fs::write(
        &backend,
        r##"#!/bin/sh
printf started > "$BACKEND_MARKER"
exit 0
"##,
    )
    .unwrap();
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(&backend, std::fs::Permissions::from_mode(0o700)).unwrap();
    value["acp"]["command"] = backend.to_string_lossy().to_string().into();
    value["acp"]["args"] = json!([]);
    std::fs::write(&config_path, {
        value["a2a"] = valid(&verifier)["a2a"].clone();
        value.to_string()
    })
    .unwrap();
    let binary = env!("CARGO_BIN_EXE_soulsteads-messaging");
    let run = |args: &[&str]| {
        let mut child = std::process::Command::new(binary)
            .env_clear()
            .env("PATH", "/usr/bin:/bin")
            .env("HOME", &home)
            .env("XDG_CONFIG_HOME", home.join("xdg"))
            .env("NODE_ENV", "test")
            .env("SESSION_BASE_DIR", root.path().join("sessions"))
            .env("BACKEND_MARKER", &marker)
            .current_dir(&cwd)
            .arg("--config")
            .arg(&config_path)
            .args(args)
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .unwrap();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        loop {
            if let Some(status) = child.try_wait().unwrap() {
                let output = child.wait_with_output().unwrap();
                return (status, output);
            }
            if std::time::Instant::now() >= deadline {
                let _ = child.kill();
                let _ = child.wait();
                panic!("A2A subprocess timed out");
            }
            std::thread::sleep(std::time::Duration::from_millis(20));
        }
    };
    let (check, _) = run(&["--check-config"]);
    assert!(check.success());
    let (ordinary, _) = run(&["--no-images", "probe"]);
    assert!(!ordinary.success());
    assert!(
        !marker.exists(),
        "enabled A2A must refuse before starting ACP"
    );
    assert!(!root.path().join("missing/session.json").exists());
    assert!(!verifier.exists());
    assert!(listener.local_addr().is_ok());
    let mut disabled: Value =
        serde_json::from_slice(&std::fs::read(&config_path).unwrap()).unwrap();
    disabled.as_object_mut().unwrap().remove("a2a");
    std::fs::write(&config_path, disabled.to_string()).unwrap();
    let _ = run(&["--no-images", "probe"]);
    assert!(
        marker.exists(),
        "disabled control should reach the fake backend"
    );
}
