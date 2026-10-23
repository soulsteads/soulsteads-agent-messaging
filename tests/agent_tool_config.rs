use serde_json::json;
use soulsteads_messaging::config;
use std::collections::BTreeMap;

fn load(value: serde_json::Value) -> anyhow::Result<serde_json::Value> {
    config::load(&value.to_string(), &BTreeMap::new())
}

#[test]
fn rejects_malformed_agent_tools_configuration_instead_of_silently_disabling_it() {
    let result = load(json!({"agentMessaging":{"tools":{"enabled":"yes"}}}));
    assert!(
        result.is_err(),
        "malformed opt-in must be visible to the operator"
    );
}

#[test]
fn rejects_tools_without_agent_messaging_enabled() {
    let error = load(json!({"agentMessaging":{"tools":{"enabled":true,"harness":"codex"}}}))
        .unwrap_err()
        .to_string();
    assert!(error.contains("agentMessaging.tools requires agentMessaging.enabled=true"));
}

#[test]
fn enabled_tools_require_explicit_supported_harness() {
    for harness in [None, Some("other"), Some("")] {
        let mut tools = json!({"enabled":true});
        if let Some(harness) = harness {
            tools["harness"] = harness.into();
        }
        let error = load(json!({"agentMessaging":{"enabled":true,"tools":tools}}))
            .unwrap_err()
            .to_string();
        assert!(error.contains("agentMessaging.tools.harness"), "{error}");
    }
}

#[test]
fn implicit_loader_propagates_parseable_tools_errors_but_preserves_legacy_malformed_fallback() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("soulsteads-messaging.json");
    std::fs::write(&path, r#"{"agentMessaging":{"tools":null}}"#).unwrap();
    let mut loader = config::Loader::default();
    assert!(loader.load(root.path(), None, &BTreeMap::new()).is_err());
    loader.clear();
    std::fs::write(&path, r#"{"agentMessaging":{"tools":{"enabled":true}"#).unwrap();
    assert!(loader.load(root.path(), None, &BTreeMap::new()).is_ok());
}

#[test]
fn raw_bridge_constructor_rejects_invalid_tools_harness() {
    let config = json!({
        "sessionStorePath":"/tmp/agent-tools-invalid-store.json",
        "agentMessaging":{"enabled":true,"tools":{"enabled":true,"harness":"other"},"agents":[]}
    });
    let error =
        soulsteads_messaging::runtime::Bridge::new(config, BTreeMap::new(), std::env::temp_dir())
            .err()
            .expect("invalid harness should be rejected")
            .to_string();
    assert!(error.contains("agentMessaging.tools.harness"));
}

#[test]
fn open_code_tools_require_acp_without_restricting_disabled_native_backends() {
    for acp in [
        json!({"command":"opencode","transport":"native"}),
        json!({"backends":{"opencode":{"command":"opencode","transport":"native"}}}),
    ] {
        let mut value = json!({"acp":acp,"agentMessaging":{"enabled":true,"tools":{"enabled":true,"harness":"opencode"}}});
        let error =
            load(value.clone()).expect_err("unqualified native MCP isolation must fail closed");
        assert!(
            error
                .to_string()
                .contains("OpenCode agent tools require the ACP transport"),
            "{error}"
        );
        value["agentMessaging"]["tools"]["enabled"] = false.into();
        assert!(
            load(value).is_ok(),
            "ordinary native backends are unaffected"
        );
    }
}

#[test]
fn accepts_explicit_opencode_tools_opt_in() {
    let enabled = load(json!({
        "agentMessaging":{"enabled":true,"tools":{"enabled":true,"harness":"opencode"}}
    }))
    .unwrap();
    assert_eq!(enabled["agentMessaging"]["tools"]["harness"], "opencode");
}

#[cfg(not(feature = "agent-network"))]
#[tokio::test]
async fn feature_off_rejects_enabled_tools_and_disabled_tools_prepare_without_listener() {
    let enabled = load(json!({
        "sessionStorePath":"/tmp/agent-tools-feature-off.json",
        "agentMessaging":{"enabled":true,"tools":{"enabled":true,"harness":"codex"},"agents":[]}
    }))
    .unwrap();
    let error =
        soulsteads_messaging::runtime::Bridge::new(enabled, BTreeMap::new(), std::env::temp_dir())
            .err()
            .expect("feature-off build must reject enabled tools")
            .to_string();
    assert!(error.contains("requires the agent-network build feature"));

    let disabled = load(json!({
        "sessionStorePath":"/tmp/agent-tools-disabled.json",
        "agentMessaging":{"tools":{"enabled":false}}
    }))
    .unwrap();
    let bridge = std::sync::Arc::new(
        soulsteads_messaging::runtime::Bridge::new(disabled, BTreeMap::new(), std::env::temp_dir())
            .unwrap(),
    );
    bridge.prepare_agent_tools().await.unwrap();
}

#[test]
fn accepts_explicit_codex_tools_opt_in_and_preserves_absent_legacy_behavior() {
    let enabled =
        load(json!({"agentMessaging":{"enabled":true,"tools":{"enabled":true,"harness":"codex"}}}))
            .unwrap();
    assert_eq!(enabled["agentMessaging"]["tools"]["enabled"], true);
    assert!(load(json!({})).is_ok());
}
