use serde_json::json;
use soulsteads_messaging::config::{defaults, validate_connector_access};
use std::collections::BTreeMap;

#[test]
fn enabled_remote_connectors_require_explicit_access() {
    for platform in [
        "matrix",
        "mattermost",
        "telegram",
        "slack",
        "discord",
        "whatsapp",
    ] {
        let mut config = defaults();
        config[platform]["enabled"] = json!(true);
        assert!(validate_connector_access(&config, &BTreeMap::new()).is_err());
        config[platform]["allowAllUsers"] = json!("true");
        assert!(validate_connector_access(&config, &BTreeMap::new()).is_err());
        config[platform]["allowedUsers"] = json!(["operator"]);
        assert!(validate_connector_access(&config, &BTreeMap::new()).is_ok());
        config[platform]["allowedUsers"] = json!([]);
        let environment = BTreeMap::from([(
            format!("{}_ALLOWED_USERS", platform.to_uppercase()),
            "operator".into(),
        )]);
        assert!(validate_connector_access(&config, &environment).is_ok());
        config[platform]["allowAllUsers"] = json!(true);
        assert!(validate_connector_access(&config, &BTreeMap::new()).is_ok());
    }
    assert!(validate_connector_access(&defaults(), &BTreeMap::new()).is_ok());
}

#[test]
fn embedded_bridge_enforces_the_same_access_boundary() {
    let mut config = defaults();
    config["matrix"]["enabled"] = json!(true);
    assert!(
        soulsteads_messaging::runtime::Bridge::new(
            config.clone(),
            BTreeMap::new(),
            std::path::PathBuf::from("unused")
        )
        .is_err()
    );
    config["matrix"]["allowedUsers"] = json!(["@operator:example.org"]);
    assert!(
        soulsteads_messaging::runtime::Bridge::new(
            config,
            BTreeMap::new(),
            std::path::PathBuf::from("unused")
        )
        .is_ok()
    );
}
