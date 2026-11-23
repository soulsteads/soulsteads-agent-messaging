#![cfg(unix)]

use anyhow::{Result, ensure};
use axum::{
    Json, Router,
    extract::State,
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    routing::post,
};
use serde_json::{Value, json};
use soulsteads_messaging::{config, connectors::http::Platform, runtime::Bridge};
use std::{
    collections::{BTreeMap, VecDeque},
    os::unix::fs::PermissionsExt,
    sync::Arc,
    time::Duration,
};
use tokio::{
    net::TcpListener,
    sync::{Mutex, Notify, watch},
    time::timeout,
};

#[derive(Default)]
struct Telegram {
    updates: Mutex<VecDeque<Value>>,
    posts: Mutex<Vec<Value>>,
    changed: Notify,
}

async fn telegram(
    State(state): State<Arc<Telegram>>,
    axum::extract::Path(method): axum::extract::Path<String>,
    Json(body): Json<Value>,
) -> impl IntoResponse {
    let result = match method.as_str() {
        "getMe" => json!({"id":999,"username":"fixture_bot"}),
        "deleteWebhook" => Value::Bool(true),
        "getUpdates" => {
            while state.updates.lock().await.is_empty() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            Value::Array(state.updates.lock().await.drain(..).collect())
        }
        "sendMessage" => {
            state.posts.lock().await.push(body);
            state.changed.notify_one();
            json!({"message_id":5})
        }
        _ => return StatusCode::BAD_REQUEST.into_response(),
    };
    Json(json!({"ok":true,"result":result})).into_response()
}

#[derive(Default)]
struct Registry {
    decisions: Mutex<Vec<Value>>,
}

async fn registry(
    State(state): State<Arc<Registry>>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> impl IntoResponse {
    if headers
        .get("authorization")
        .and_then(|value| value.to_str().ok())
        != Some("Bearer fixture-reviewer-token")
    {
        return StatusCode::FORBIDDEN.into_response();
    }
    let mut decisions = state.decisions.lock().await;
    let status = if decisions.is_empty() {
        "open"
    } else {
        "accepted"
    };
    let proposal = json!({
        "id":"p0001-abcd","skill":"shipping","status":status,"digest":"sha256:fixture",
        "decision":{"released_version":"1.1.0"}
    });
    let result = match body["method"].as_str() {
        Some("info") => json!({"store_id":"fixture-store"}),
        Some("list_proposals") => {
            if decisions.is_empty() {
                json!([proposal])
            } else {
                json!([])
            }
        }
        Some("proposal") => proposal,
        Some("proposal_diff") => {
            json!({"diff":"-Record parcel weight.\n+Record parcel weight in grams.\n"})
        }
        Some("decide_proposal") => {
            decisions.push(body["params"].clone());
            json!({"version":"1.1.0"})
        }
        _ => return StatusCode::BAD_REQUEST.into_response(),
    };
    Json(json!({"ok":true,"result":result})).into_response()
}

fn update(id: u64, sender: u64, chat: u64, chat_type: &str, text: &str) -> Value {
    json!({
        "update_id":id,
        "message":{"message_id":id, "from":{"id":sender},"chat":{"id":chat,"type":chat_type},"text":text}
    })
}

async fn received(state: &Telegram, needle: &str) -> Result<()> {
    timeout(Duration::from_secs(10), async {
        loop {
            if state.posts.lock().await.iter().any(|post| {
                post["chat_id"] == 123
                    && post["text"]
                        .as_str()
                        .is_some_and(|text| text.contains(needle))
            }) {
                return;
            }
            state.changed.notified().await;
        }
    })
    .await?;
    Ok(())
}

async fn exercise(
    registry_url: &str,
    token_file: &std::path::Path,
    proposal_id: &str,
) -> Result<()> {
    let state = Arc::new(Telegram::default());
    let api = Router::new()
        .route("/botfixture/{method}", post(telegram))
        .with_state(state.clone());
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let telegram_url = format!("http://{}", listener.local_addr()?);
    let server = tokio::spawn(async move { axum::serve(listener, api).await });
    let scratch = tempfile::tempdir()?;
    let receipts = scratch.path().join("receipts");
    std::fs::create_dir(&receipts)?;
    std::fs::set_permissions(&receipts, std::fs::Permissions::from_mode(0o700))?;
    let mut config = config::defaults();
    config["telegram"]["enabled"] = json!(true);
    config["web"]["enabled"] = json!(false);
    config["telegram"]["allowedUsers"] = json!(["123", "456"]);
    config["telegram"]["skillReviews"] = json!({
        "enabled":true, "registryUrl":registry_url, "reviewerUserId":123,
        "reviewerTokenFile":token_file, "receiptDir":receipts, "skills":["shipping"],
        "pollSeconds":5, "reviewsOnly":true,
    });
    let bridge = Arc::new(Bridge::new(
        config,
        BTreeMap::new(),
        scratch.path().join("work"),
    )?);
    let platform = Platform::new("telegram", &telegram_url, "fixture", "", "");
    let (shutdown, receiver) = watch::channel(false);
    let mut transport = tokio::spawn(platform.clone().run(bridge.clone(), receiver));
    received(&state, "awaiting review").await?;
    timeout(Duration::from_secs(2), async {
        while std::fs::read_dir(&receipts).unwrap().next().is_none() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await?;
    shutdown.send(true)?;
    timeout(Duration::from_secs(5), &mut transport).await???;

    let (shutdown, receiver) = watch::channel(false);
    let transport = tokio::spawn(platform.run(bridge.clone(), receiver));
    let wrong = format!("/skillapprove {proposal_id} 9.9.9");
    state.updates.lock().await.extend([
        update(1, 456, 456, "private", &wrong),
        update(2, 123, 123, "group", &wrong),
        update(
            3,
            123,
            123,
            "private",
            &format!("/skillreview {proposal_id}"),
        ),
    ]);
    received(&state, "Record parcel weight in grams.").await?;
    state.updates.lock().await.push_back(update(
        4,
        123,
        123,
        "private",
        &format!("/skillapprove {proposal_id} 1.1.0"),
    ));
    received(&state, "as shipping@1.1.0").await?;
    state.updates.lock().await.push_back(update(
        5,
        123,
        123,
        "private",
        &format!("/skillapprove {proposal_id} 1.1.0"),
    ));
    received(&state, "already approved as shipping@1.1.0").await?;
    shutdown.send(true)?;
    timeout(Duration::from_secs(5), transport).await???;
    ensure!(
        state
            .posts
            .lock()
            .await
            .iter()
            .filter(|p| p["text"]
                .as_str()
                .is_some_and(|t| t.contains("awaiting review")))
            .count()
            == 1,
        "a delivered proposal was announced again after restart"
    );
    bridge.shutdown().await?;
    server.abort();
    Ok(())
}

#[tokio::test]
async fn skillstore_notice_review_approval_and_restart() -> Result<()> {
    let state = Arc::new(Registry::default());
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let url = format!("http://{}", listener.local_addr()?);
    let api = Router::new()
        .route("/v1/rpc", post(registry))
        .with_state(state.clone());
    let server = tokio::spawn(async move { axum::serve(listener, api).await });
    let token = tempfile::NamedTempFile::new()?;
    std::fs::set_permissions(token.path(), std::fs::Permissions::from_mode(0o600))?;
    std::fs::write(token.path(), "fixture-reviewer-token\n")?;
    exercise(&url, token.path(), "p0001-abcd").await?;
    let decisions = state.decisions.lock().await;
    ensure!(decisions.len() == 1);
    ensure!(decisions[0]["version"] == "1.1.0" && decisions[0]["decision"] == "accept");
    server.abort();
    Ok(())
}

#[tokio::test]
#[ignore = "needs a disposable Skillstore server with a pending synthetic shipping proposal"]
async fn telegram_review_with_installed_skillstore() -> Result<()> {
    let registry = std::env::var("SOULSTEADS_TEST_SKILLSTORE_URL")?;
    let token_file =
        std::path::PathBuf::from(std::env::var_os("SOULSTEADS_TEST_REVIEWER_TOKEN_FILE").unwrap());
    let proposal_id = std::env::var("SOULSTEADS_TEST_SKILLSTORE_PROPOSAL")?;
    exercise(&registry, &token_file, &proposal_id).await
}
