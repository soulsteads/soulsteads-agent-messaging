#![cfg(feature = "platform-sdks")]

use axum::{Json, Router, extract::Request, response::IntoResponse, routing::any};
use soulsteads_messaging::{config, connectors::matrix};
use serde_json::{Value, json};
use std::{future::IntoFuture, sync::Arc, time::Duration};
use tokio::sync::Mutex;

#[derive(Default)]
struct Observed {
    identity_requests: usize,
    login_requests: usize,
    authenticated_identity_requests: usize,
    uploaded_keys: Vec<Value>,
    login_bodies: Vec<Value>,
}

struct Fixture {
    base: String,
    observed: Arc<Mutex<Observed>>,
    server: tokio::task::JoinHandle<()>,
}

impl Fixture {
    async fn start(status: axum::http::StatusCode, identity: Value) -> Self {
        let observed = Arc::new(Mutex::new(Observed::default()));
        let state = observed.clone();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let router = Router::new().fallback(any(move |request: Request| {
            let state = state.clone();
            let identity = identity.clone();
            async move {
                let path = request.uri().path().to_owned();
                if path.ends_with("whoami") {
                    let mut observed = state.lock().await;
                    observed.identity_requests += 1;
                    if request.headers().get("authorization").is_some_and(|header| {
                        header == "Bearer explicit-fixture" || header == "Bearer saved-fixture"
                    }) {
                        observed.authenticated_identity_requests += 1;
                    }
                    return (status, Json(identity)).into_response();
                }
                if path.ends_with("login") {
                    let body = axum::body::to_bytes(request.into_body(), 1024 * 1024)
                        .await
                        .unwrap();
                    let mut observed = state.lock().await;
                    observed.login_requests += 1;
                    observed.login_bodies.push(serde_json::from_slice(&body).unwrap());
                    return Json(json!({"user_id":"@bot:local","device_id":"LOGIN-DEVICE","access_token":"replacement-fixture"})).into_response();
                }
                if path.ends_with("/keys/upload") {
                    let body = axum::body::to_bytes(request.into_body(), 1024 * 1024)
                        .await
                        .unwrap();
                    let upload: Value = serde_json::from_slice(&body).unwrap();
                    state.lock().await.uploaded_keys.push(upload.clone());
                    if let Some(keys) = upload.get("device_keys")
                        && (keys["device_id"] != identity["device_id"]
                            || keys["user_id"] != identity["user_id"])
                    {
                        return (axum::http::StatusCode::BAD_REQUEST, Json(json!({"errcode":"M_INVALID_PARAM","error":"Device keys do not match token identity"}))).into_response();
                    }
                    return Json(json!({"one_time_key_counts":{"signed_curve25519":50}})).into_response();
                }
                Json(if path.ends_with("versions") {
                    json!({"versions":["v1.12"],"unstable_features":{}})
                } else if path.ends_with("sync") {
                    json!({"next_batch":"identity-batch","rooms":{},"device_lists":{"changed":[],"left":[]},"device_one_time_keys_count":{"signed_curve25519":50}})
                } else {
                    json!({})
                }).into_response()
            }
        }));
        let server = tokio::spawn(async move {
            axum::serve(listener, router).into_future().await.unwrap();
        });
        Self {
            base,
            observed,
            server,
        }
    }

    fn configuration(&self, explicit: bool) -> Value {
        let mut config = json!({"homeserver":self.base,"userId":"@bot:local","password":"offline-fixture-password","encryption":{"enabled":cfg!(feature = "matrix-encryption")}});
        if explicit {
            config["accessToken"] = json!("explicit-fixture");
        }
        config
    }

    fn loaded_configuration(&self, explicit: bool) -> Value {
        config::load(
            &json!({"matrix":self.configuration(explicit)}).to_string(),
            &Default::default(),
        )
        .unwrap()["matrix"]
            .clone()
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        self.server.abort();
    }
}

async fn connect(config: &Value, storage: &std::path::Path) -> anyhow::Result<matrix_sdk::Client> {
    tokio::time::timeout(Duration::from_secs(15), matrix::connect(config, storage))
        .await
        .expect("Matrix identity restoration must complete within the fixture deadline")
}

#[tokio::test]
#[cfg(feature = "matrix-encryption")]
async fn explicit_token_without_device_id_discovers_identity_and_reopens_crypto_store() {
    let fixture = Fixture::start(
        axum::http::StatusCode::OK,
        json!({"user_id":"@bot:local","device_id":"TOKEN-DEVICE"}),
    )
    .await;
    let directory = tempfile::tempdir().unwrap();
    let token_file = directory.path().join("access_token");
    std::fs::write(&token_file, "saved-fixture").unwrap();
    let config = fixture.configuration(true);
    let mut original_key = None;
    for _ in 0..2 {
        let client = connect(&config, directory.path()).await.unwrap();
        assert_eq!(client.device_id().unwrap().as_str(), "TOKEN-DEVICE");
        assert_eq!(client.user_id().unwrap().as_str(), "@bot:local");
        assert_eq!(client.access_token().as_deref(), Some("explicit-fixture"));
        let identity_key = client.encryption().ed25519_key().await.unwrap();
        if let Some(previous) = &original_key {
            assert_eq!(
                &identity_key, previous,
                "SQLite reopen must reuse the encryption identity"
            );
        } else {
            original_key = Some(identity_key);
        }
        tokio::time::timeout(
            Duration::from_secs(15),
            client.sync_once(matrix_sdk::config::SyncSettings::default()),
        )
        .await
        .unwrap()
        .unwrap();
        drop(client);
    }
    let observed = fixture.observed.lock().await;
    assert_eq!(observed.identity_requests, 2);
    assert_eq!(observed.authenticated_identity_requests, 2);
    assert_eq!(observed.login_requests, 0);
    let keys: Vec<_> = observed
        .uploaded_keys
        .iter()
        .filter_map(|upload| upload.get("device_keys"))
        .collect();
    assert!(
        !keys.is_empty(),
        "The native SDK must upload real device keys"
    );
    for keys in keys {
        assert_eq!(keys["device_id"], "TOKEN-DEVICE");
        assert_eq!(
            keys["keys"]["ed25519:TOKEN-DEVICE"],
            original_key.as_ref().unwrap().as_str()
        );
    }
    assert_eq!(
        std::fs::read_to_string(token_file).unwrap(),
        "saved-fixture"
    );
}

#[tokio::test]
#[cfg(feature = "matrix-encryption")]
async fn loaded_config_token_without_device_id_restores_actual_identity_and_reopens_crypto_store() {
    let fixture = Fixture::start(
        axum::http::StatusCode::OK,
        json!({"user_id":"@bot:local","device_id":"TOKEN-DEVICE"}),
    )
    .await;
    let supplied = fixture.configuration(true);
    assert!(supplied.get("deviceId").is_none());
    let loaded =
        config::load(&json!({"matrix":supplied}).to_string(), &Default::default()).unwrap();
    assert_eq!(loaded["matrix"]["deviceId"], "OPENCODE_BRIDGE");
    let directory = tempfile::tempdir().unwrap();
    let token_file = directory.path().join("access_token");
    std::fs::write(&token_file, "saved-fixture").unwrap();
    let mut original_key = None;
    for _ in 0..2 {
        let client = connect(&loaded["matrix"], directory.path()).await.unwrap();
        assert_eq!(client.user_id().unwrap().as_str(), "@bot:local");
        assert_eq!(client.device_id().unwrap().as_str(), "TOKEN-DEVICE");
        assert_eq!(client.access_token().as_deref(), Some("explicit-fixture"));
        let identity_key = client.encryption().ed25519_key().await.unwrap();
        if let Some(previous) = &original_key {
            assert_eq!(&identity_key, previous);
        } else {
            original_key = Some(identity_key);
        }
        tokio::time::timeout(
            Duration::from_secs(15),
            client.sync_once(matrix_sdk::config::SyncSettings::default()),
        )
        .await
        .unwrap()
        .unwrap();
        drop(client);
    }
    let observed = fixture.observed.lock().await;
    assert_eq!(observed.identity_requests, 2);
    assert_eq!(observed.authenticated_identity_requests, 2);
    assert_eq!(observed.login_requests, 0);
    let keys: Vec<_> = observed
        .uploaded_keys
        .iter()
        .filter_map(|upload| upload.get("device_keys"))
        .collect();
    assert!(
        !keys.is_empty(),
        "The native SDK must upload real device keys"
    );
    for keys in keys {
        assert_eq!(keys["user_id"], "@bot:local");
        assert_eq!(keys["device_id"], "TOKEN-DEVICE");
        assert_eq!(
            keys["keys"]["ed25519:TOKEN-DEVICE"],
            original_key.as_ref().unwrap().as_str()
        );
    }
    assert_eq!(
        std::fs::read_to_string(token_file).unwrap(),
        "saved-fixture"
    );
}

#[tokio::test]
async fn explicit_configured_user_and_device_cannot_override_token_identity() {
    let fixture = Fixture::start(
        axum::http::StatusCode::OK,
        json!({"user_id":"@actual:local","device_id":"TOKEN-DEVICE"}),
    )
    .await;
    let directory = tempfile::tempdir().unwrap();
    let mut config = fixture.configuration(true);
    config["userId"] = json!("@configured:local");
    config["deviceId"] = json!("CONFIGURED");
    let client = connect(&config, directory.path()).await.unwrap();
    assert_eq!(client.user_id().unwrap().as_str(), "@actual:local");
    assert_eq!(client.device_id().unwrap().as_str(), "TOKEN-DEVICE");
    assert_eq!(client.access_token().as_deref(), Some("explicit-fixture"));
    tokio::time::timeout(
        Duration::from_secs(15),
        client.sync_once(matrix_sdk::config::SyncSettings::default()),
    )
    .await
    .unwrap()
    .unwrap();
    let observed = fixture.observed.lock().await;
    assert_eq!(observed.identity_requests, 1);
    assert_eq!(observed.authenticated_identity_requests, 1);
    assert_eq!(observed.login_requests, 0);
    #[cfg(feature = "matrix-encryption")]
    assert!(observed.uploaded_keys.iter().any(|upload| {
        upload["device_keys"]["user_id"] == "@actual:local"
            && upload["device_keys"]["device_id"] == "TOKEN-DEVICE"
    }));
    #[cfg(not(feature = "matrix-encryption"))]
    assert!(observed.uploaded_keys.is_empty());
    assert!(!directory.path().join("access_token").exists());
}

#[tokio::test]
async fn explicit_identity_failures_never_fall_back_to_password_or_replace_saved_token() {
    for (status, body) in [
        (
            axum::http::StatusCode::UNAUTHORIZED,
            json!({"errcode":"M_UNKNOWN_TOKEN","error":"explicit-fixture"}),
        ),
        (
            axum::http::StatusCode::FORBIDDEN,
            json!({"errcode":"M_FORBIDDEN","error":"explicit-fixture"}),
        ),
        (
            axum::http::StatusCode::BAD_GATEWAY,
            json!({"error":"explicit-fixture"}),
        ),
        (axum::http::StatusCode::OK, json!({"user_id":"@bot:local"})),
        (
            axum::http::StatusCode::OK,
            json!({"user_id":"@bot:local","device_id":""}),
        ),
        (
            axum::http::StatusCode::OK,
            json!({"user_id":123,"device_id":"TOKEN-DEVICE"}),
        ),
    ] {
        for configured_device in ["OPENCODE_BRIDGE", "CONFIGURED"] {
            let fixture = Fixture::start(status, body.clone()).await;
            let directory = tempfile::tempdir().unwrap();
            let token_file = directory.path().join("access_token");
            std::fs::write(&token_file, "saved-fixture").unwrap();
            let mut config = fixture.loaded_configuration(true);
            config["deviceId"] = json!(configured_device);
            let error = connect(&config, directory.path())
                .await
                .expect_err("Unresolved explicit token identity must fail closed");
            assert!(!format!("{error:#}").contains("explicit-fixture"));
            let observed = fixture.observed.lock().await;
            assert_eq!(observed.identity_requests, 1);
            assert_eq!(observed.login_requests, 0);
            assert!(observed.uploaded_keys.is_empty());
            assert_eq!(
                std::fs::read_to_string(token_file).unwrap(),
                "saved-fixture"
            );
        }
    }
}

#[tokio::test]
async fn loaded_saved_token_ignores_default_device_before_crypto_restoration() {
    let fixture = Fixture::start(
        axum::http::StatusCode::OK,
        json!({"user_id":"@bot:local","device_id":"SAVED-DEVICE"}),
    )
    .await;
    let directory = tempfile::tempdir().unwrap();
    std::fs::write(directory.path().join("access_token"), "saved-fixture").unwrap();
    let client = connect(&fixture.loaded_configuration(false), directory.path())
        .await
        .unwrap();
    assert_eq!(client.device_id().unwrap().as_str(), "SAVED-DEVICE");
    assert_eq!(client.access_token().as_deref(), Some("saved-fixture"));
    let observed = fixture.observed.lock().await;
    assert_eq!(observed.identity_requests, 1);
    assert_eq!(observed.authenticated_identity_requests, 1);
    assert_eq!(observed.login_requests, 0);
}

#[tokio::test]
async fn saved_token_auth_errors_or_wrong_user_allow_password_login_with_server_device() {
    for (status, identity) in [
        (
            axum::http::StatusCode::UNAUTHORIZED,
            json!({"errcode":"M_UNKNOWN_TOKEN","error":"offline rejection"}),
        ),
        (
            axum::http::StatusCode::UNAUTHORIZED,
            json!({"errcode":"M_MISSING_TOKEN","error":"offline rejection"}),
        ),
        (
            axum::http::StatusCode::OK,
            json!({"user_id":"@other:local","device_id":"OTHER-DEVICE"}),
        ),
    ] {
        let fixture = Fixture::start(status, identity).await;
        let directory = tempfile::tempdir().unwrap();
        let token_file = directory.path().join("access_token");
        std::fs::write(&token_file, "saved-fixture").unwrap();
        let client = connect(&fixture.configuration(false), directory.path())
            .await
            .unwrap();
        assert_eq!(client.device_id().unwrap().as_str(), "LOGIN-DEVICE");
        assert_eq!(
            client.access_token().as_deref(),
            Some("replacement-fixture")
        );
        let observed = fixture.observed.lock().await;
        assert_eq!(observed.login_requests, 1);
        assert!(observed.login_bodies[0].get("device_id").is_none());
        assert_eq!(
            std::fs::read_to_string(token_file).unwrap(),
            "replacement-fixture"
        );
    }
}

#[tokio::test]
async fn saved_non_auth_errors_fail_closed_without_restoring_wrong_crypto_identity() {
    for (status, identity) in [
        (
            axum::http::StatusCode::FORBIDDEN,
            json!({"errcode":"M_FORBIDDEN","error":"offline failure"}),
        ),
        (
            axum::http::StatusCode::BAD_GATEWAY,
            json!({"error":"offline failure"}),
        ),
        (axum::http::StatusCode::OK, json!({"user_id":"@bot:local"})),
        (
            axum::http::StatusCode::OK,
            json!({"user_id":"@bot:local","device_id":""}),
        ),
        (
            axum::http::StatusCode::OK,
            json!({"user_id":123,"device_id":"SAVED-DEVICE"}),
        ),
    ] {
        let fixture = Fixture::start(status, identity).await;
        let directory = tempfile::tempdir().unwrap();
        let token_file = directory.path().join("access_token");
        std::fs::write(&token_file, "saved-fixture").unwrap();
        assert!(
            connect(&fixture.loaded_configuration(false), directory.path())
                .await
                .is_err()
        );
        let observed = fixture.observed.lock().await;
        assert_eq!(observed.login_requests, 0);
        assert!(observed.uploaded_keys.is_empty());
        assert_eq!(
            std::fs::read_to_string(token_file).unwrap(),
            "saved-fixture"
        );
    }
}

#[tokio::test]
#[cfg(not(feature = "matrix-encryption"))]
async fn encryption_request_on_plaintext_build_fails_before_io() {
    let fixture = Fixture::start(
        axum::http::StatusCode::OK,
        json!({"user_id":"@bot:local","device_id":"TOKEN-DEVICE"}),
    )
    .await;
    let directory = tempfile::tempdir().unwrap();
    let absent = directory.path().join("must-stay-absent");
    let mut config = fixture.configuration(true);
    config["encryption"]["enabled"] = json!(true);
    let result = connect(&config, &absent).await;
    assert!(
        result.is_err(),
        "encrypted configuration was accepted without crypto support"
    );
    assert!(
        !absent.exists(),
        "failed build capability validation created platform state"
    );
    let observed = fixture.observed.lock().await;
    assert_eq!(observed.identity_requests, 0);
    assert_eq!(observed.login_requests, 0);
    assert!(observed.uploaded_keys.is_empty());
}

#[tokio::test]
#[cfg(not(feature = "matrix-encryption"))]
async fn disabled_encryption_does_not_upload_device_keys_during_sync() {
    let fixture = Fixture::start(
        axum::http::StatusCode::OK,
        json!({"user_id":"@bot:local","device_id":"TOKEN-DEVICE"}),
    )
    .await;
    let directory = tempfile::tempdir().unwrap();
    let mut config = fixture.configuration(true);
    config["encryption"]["enabled"] = json!(false);
    let client = connect(&config, directory.path()).await.unwrap();
    tokio::time::timeout(
        Duration::from_secs(15),
        client.sync_once(matrix_sdk::config::SyncSettings::default()),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(fixture.observed.lock().await.uploaded_keys.is_empty());
}
