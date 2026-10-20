use soulsteads_messaging::acp::{Client, Options};
use std::{collections::BTreeMap, time::Duration};

#[tokio::test]
async fn standard_sdk_agent_interoperates_without_authentication() {
    let client = Client::connect(Options {
        command: env!("CARGO_BIN_EXE_acp-fixture").into(),
        args: vec!["--sdk-agent".into()],
        cwd: std::env::current_dir().unwrap(),
        env: BTreeMap::new(),
        inherit_env: false,
        timeout: Duration::from_secs(3),
    })
    .await
    .unwrap();
    assert_eq!(client.new_session().await.unwrap(), "sdk-fixture");
    let response = client.prompt("hello", vec![]).await.unwrap();
    assert_eq!(response.text, "SDK reply");
    assert_eq!(response.stop_reason.as_deref(), Some("end_turn"));
    client.shutdown().await.unwrap();
}

#[tokio::test]
async fn oversized_outbound_frame_closes_sdk_transport() {
    let client = Client::connect(Options {
        command: env!("CARGO_BIN_EXE_acp-fixture").into(),
        args: vec![],
        cwd: std::env::current_dir().unwrap(),
        env: BTreeMap::new(),
        inherit_env: false,
        timeout: Duration::from_secs(3),
    })
    .await
    .unwrap();
    let error = client
        .prompt(&"x".repeat(2 * 1024 * 1024), vec![])
        .await
        .unwrap_err();
    assert!(
        error.to_string().contains("frame exceeded limit"),
        "{error}"
    );
    assert!(client.new_session().await.is_err());
    client.shutdown().await.unwrap();
}

#[tokio::test]
#[ignore = "requires installed OpenCode; creates a session using existing CLI credentials"]
async fn installed_opencode_initializes_sdk_session() {
    let directory = tempfile::tempdir().unwrap();
    let client = Client::connect(Options {
        command: "opencode".into(),
        args: vec!["acp".into()],
        cwd: directory.path().to_owned(),
        env: BTreeMap::new(),
        inherit_env: true,
        timeout: Duration::from_secs(40),
    })
    .await
    .unwrap();
    let session = client.new_session().await;
    client.shutdown().await.unwrap();
    assert!(!session.unwrap().is_empty());
}

#[tokio::test]
async fn oversized_inbound_frame_closes_sdk_transport() {
    let directory = tempfile::tempdir().unwrap();
    let script = directory.path().join("script.json");
    std::fs::write(
        &script,
        serde_json::json!([{"text":"x".repeat(2 * 1024 * 1024)}]).to_string(),
    )
    .unwrap();
    let client = Client::connect(Options {
        command: env!("CARGO_BIN_EXE_acp-fixture").into(),
        args: vec![],
        cwd: directory.path().to_owned(),
        env: BTreeMap::from([
            (
                "ACP_FIXTURE_MATRIX_SCRIPT".into(),
                script.display().to_string(),
            ),
            (
                "ACP_FIXTURE_MATRIX_COUNTER".into(),
                directory.path().join("counter").display().to_string(),
            ),
        ]),
        inherit_env: false,
        timeout: Duration::from_secs(3),
    })
    .await
    .unwrap();
    let error = tokio::time::timeout(Duration::from_secs(3), client.prompt("frame", vec![]))
        .await
        .unwrap()
        .unwrap_err();
    assert!(
        error.to_string().contains("frame exceeded limit"),
        "{error}"
    );
    assert!(client.new_session().await.is_err());
    client.shutdown().await.unwrap();
}
