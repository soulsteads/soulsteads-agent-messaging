#![cfg(feature = "platform-sdks")]
use axum::{Json, Router, extract::Request, response::IntoResponse, routing::any};
use soulsteads_messaging::{config, connectors::matrix, runtime::Bridge};
use serde_json::json;
use std::{collections::BTreeMap, future::IntoFuture, sync::Arc, time::Duration};

async fn stalled_transport_shutdown(invite: bool) {
    let entered = Arc::new(tokio::sync::Semaphore::new(0));
    let blocked = Arc::new(tokio::sync::Semaphore::new(0));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let router = Router::new().fallback(any({
        let entered = entered.clone();
        let blocked = blocked.clone();
        move |request: Request| {
            let entered = entered.clone();
            let blocked = blocked.clone();
            async move {
                let path = request.uri().path();
                let result = if path.ends_with("versions") {
                    json!({"versions":["v1.12"],"unstable_features":{}})
                } else if path.ends_with("whoami") {
                    json!({"user_id":"@bot:local","device_id":"LOCAL"})
                } else if path.ends_with("sync") {
                    if !invite {
                        entered.add_permits(1);
                        let _ = blocked.acquire().await;
                    }
                    json!({"next_batch":"batch","rooms":{"invite":{"!invited:local":{"invite_state":{"events":[{"type":"m.room.member","state_key":"@bot:local","sender":"@human:local","content":{"membership":"invite"}}]}}}},"device_lists":{"changed":[],"left":[]},"device_one_time_keys_count":{}})
                } else if path.contains("/join/") || path.ends_with("/join") {
                    entered.add_permits(1);
                    let _ = blocked.acquire().await;
                    json!({"room_id":"!invited:local"})
                } else {
                    json!({})
                };
                Json(result).into_response()
            }
        }
    }));
    let server = tokio::spawn(axum::serve(listener, router).into_future());
    let directory = tempfile::tempdir().unwrap();
    let configuration = config::load(&json!({"sessionStorePath":directory.path().join("store.json"),"matrix":{"homeserver":base,"userId":"@bot:local","accessToken":"fixture","autoJoin":true,"allowedRooms":["!invited:local"],"strictAllowedRooms":true,"encryption":{"enabled":false}}}).to_string(), &BTreeMap::new()).unwrap();
    let bridge = Arc::new(
        Bridge::new(
            configuration.clone(),
            BTreeMap::new(),
            directory.path().join("sessions"),
        )
        .unwrap(),
    );
    let client = matrix::connect(&configuration["matrix"], &directory.path().join("matrix"))
        .await
        .unwrap();
    let (shutdown, receiver) = tokio::sync::watch::channel(false);
    let mut task = tokio::spawn(matrix::run(client, bridge.clone(), receiver));
    tokio::time::timeout(Duration::from_secs(5), entered.acquire())
        .await
        .expect("native transport reached stall")
        .unwrap()
        .forget();
    shutdown.send(true).unwrap();
    let outcome = tokio::time::timeout(Duration::from_millis(500), &mut task).await;
    if outcome.is_err() {
        task.abort();
        let _ = task.await;
    }
    server.abort();
    bridge.shutdown().await.unwrap();
    assert!(
        outcome.is_ok(),
        "shutdown ignored while native {} was stalled",
        if invite { "invite join" } else { "sync" }
    );
    outcome.unwrap().unwrap().unwrap();
    assert!(
        !directory.path().join("store.json").exists(),
        "transport-only shutdown must not create an ACP session"
    );
}

#[tokio::test]
async fn native_matrix_shutdown_cancels_stalled_invite_join() {
    stalled_transport_shutdown(true).await;
}
#[tokio::test]
async fn native_matrix_shutdown_cancels_stalled_sync() {
    stalled_transport_shutdown(false).await;
}
