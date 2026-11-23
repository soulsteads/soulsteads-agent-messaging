#![cfg(unix)]

use async_trait::async_trait;
use axum::{
    Json, Router,
    extract::State,
    response::{IntoResponse, Response},
    routing::post,
};
use serde_json::{Value, json};
use soulsteads_messaging::{
    config,
    connectors::http::{Platform, PlatformSink},
    runtime::{Bridge, Sink},
};
use std::{collections::BTreeMap, future::IntoFuture, os::unix::fs::DirBuilderExt, sync::Arc};
use tokio::sync::Mutex;

#[derive(Default)]
struct Posts(Mutex<Vec<Value>>, std::sync::atomic::AtomicBool);

async fn create_post(State(posts): State<Arc<Posts>>, Json(mut body): Json<Value>) -> Response {
    let mut captured = posts.0.lock().await;
    body["id"] = format!("post-{}", captured.len() + 1).into();
    body["user_id"] = "BOT".into();
    captured.push(body.clone());
    if body["message"] == "hello world" && posts.1.load(std::sync::atomic::Ordering::SeqCst) {
        // The server recorded the post, but no usable acknowledgement arrived.
        return (
            axum::http::StatusCode::CREATED,
            "incomplete acknowledgement",
        )
            .into_response();
    }
    Json(body).into_response()
}

struct Fixture {
    root: tempfile::TempDir,
    config: Value,
    platform: Platform,
    posts: Arc<Posts>,
    server: tokio::task::JoinHandle<std::io::Result<()>>,
}

impl Fixture {
    async fn new() -> Self {
        let root = tempfile::tempdir().unwrap();
        let receipts = root.path().join("receipts");
        std::fs::DirBuilder::new()
            .mode(0o700)
            .create(&receipts)
            .unwrap();
        let config = config::load(
            &json!({
                "rateLimitSeconds":0, "toolMessages":{"mode":"off"},
                "sessionStorePath":root.path().join("sessions.json"),
                "acp":{"command":env!("CARGO_BIN_EXE_acp-fixture"),"args":[],
                    "env":{"ACP_FIXTURE_RPC_AUDIT":root.path().join("rpc.jsonl")}},
                "mattermost":{"allowedChannels":["channel"], "allowedUsers":["human","other"],
                    "threadIsolation":true,"turnReceiptDir":receipts}
            })
            .to_string(),
            &BTreeMap::new(),
        )
        .unwrap();
        let posts = Arc::new(Posts::default());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(
            axum::serve(
                listener,
                Router::new()
                    .route("/api/v4/posts", post(create_post))
                    .with_state(posts.clone()),
            )
            .into_future(),
        );
        Self {
            root,
            config,
            platform: Platform::new("mattermost", &base, "fixture", "BOT", "bridge"),
            posts,
            server,
        }
    }

    fn bridge(&self) -> Bridge {
        Bridge::new(
            self.config.clone(),
            BTreeMap::new(),
            self.root.path().join("workspace"),
        )
        .unwrap()
    }

    async fn handle(
        &self,
        bridge: &Bridge,
        id: &str,
        root: &str,
        user: &str,
        text: &str,
        skip_delivery: bool,
    ) -> anyhow::Result<()> {
        let post = json!({"event":"posted","data":{"post":json!({
            "id":id,"root_id":root,"channel_id":"channel","user_id":user,"message":text
        }).to_string(),"channel_type":"P"}});
        let event = self
            .platform
            .prepare(bridge, &post)
            .await?
            .expect("valid addressed post");
        let sink = PlatformSink::new(self.platform.clone(), event.clone());
        if skip_delivery {
            bridge.handle(event, &FailBeforeDelivery(sink)).await
        } else {
            bridge.handle(event, &sink).await
        }
    }

    fn prompt_count(&self) -> usize {
        std::fs::read_to_string(self.root.path().join("rpc.jsonl"))
            .unwrap()
            .lines()
            .filter(|line| {
                serde_json::from_str::<Value>(line).unwrap()["method"] == "session/prompt"
            })
            .count()
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        self.server.abort();
    }
}

struct FailBeforeDelivery(PlatformSink);

#[async_trait]
impl Sink for FailBeforeDelivery {
    fn mattermost_receipt_identity(
        &self,
        event: &soulsteads_messaging::runtime::Event,
    ) -> Option<String> {
        self.0.mattermost_receipt_identity(event)
    }
    async fn emit(&self, value: Value) -> anyhow::Result<()> {
        self.0.emit(value).await
    }
    async fn deliver_mattermost_reply(
        &self,
        _text: &str,
        _before_first_post: &mut (dyn FnMut() -> anyhow::Result<()> + Send),
    ) -> anyhow::Result<Vec<String>> {
        anyhow::bail!("fixture stopped before the first post")
    }
}

#[tokio::test]
async fn saved_reply_recovers_after_restart_without_another_prompt() {
    let f = Fixture::new().await;
    let first = f.bridge();
    assert!(
        f.handle(&first, "root", "", "human", "!oc hello", true)
            .await
            .is_err()
    );
    first.shutdown().await.unwrap();
    drop(first);
    assert_eq!(f.prompt_count(), 1);
    assert!(
        !f.posts
            .0
            .lock()
            .await
            .iter()
            .any(|p| p["message"] == "hello world")
    );
    let reopened = f.bridge();
    f.handle(&reopened, "root", "", "human", "!oc hello", false)
        .await
        .unwrap();
    f.handle(
        &reopened,
        "foreign-user",
        "root",
        "other",
        "!oc /recover",
        false,
    )
    .await
    .unwrap();
    f.handle(
        &reopened,
        "foreign-thread",
        "different",
        "human",
        "!oc /recover",
        false,
    )
    .await
    .unwrap();
    assert!(
        !f.posts
            .0
            .lock()
            .await
            .iter()
            .any(|p| p["message"] == "hello world")
    );
    f.handle(&reopened, "status", "root", "human", "!oc /status", false)
        .await
        .unwrap();
    f.handle(&reopened, "recover", "root", "human", "!oc /recover", false)
        .await
        .unwrap();
    f.handle(
        &reopened,
        "recover-again",
        "root",
        "human",
        "!oc /recover",
        false,
    )
    .await
    .unwrap();
    reopened.shutdown().await.unwrap();
    let posts = f.posts.0.lock().await;
    assert!(
        posts.iter().any(|p| p["message"]
            .as_str()
            .unwrap()
            .contains("Reply saved; not yet sent.")),
        "{posts:?}"
    );
    let replies: Vec<_> = posts
        .iter()
        .filter(|p| p["message"] == "hello world")
        .collect();
    assert_eq!(replies.len(), 1, "{posts:?}");
    assert_eq!(replies[0]["channel_id"], "channel");
    assert_eq!(replies[0]["root_id"], "root");
    assert_eq!(f.prompt_count(), 1);
}

#[tokio::test]
async fn accepted_post_with_unusable_ack_is_not_reposted_after_restart() {
    let f = Fixture::new().await;
    f.posts.1.store(true, std::sync::atomic::Ordering::SeqCst);
    let first = f.bridge();
    assert!(
        f.handle(&first, "root", "", "human", "!oc lost-ack", false)
            .await
            .is_err()
    );
    first.shutdown().await.unwrap();
    drop(first);
    assert_eq!(f.prompt_count(), 1);
    assert_eq!(
        f.posts
            .0
            .lock()
            .await
            .iter()
            .filter(|p| p["message"] == "hello world")
            .count(),
        1
    );

    let reopened = f.bridge();
    f.handle(&reopened, "root", "", "human", "!oc lost-ack", false)
        .await
        .unwrap();
    f.handle(&reopened, "status", "root", "human", "!oc /status", false)
        .await
        .unwrap();
    f.handle(&reopened, "recover", "root", "human", "!oc /recover", false)
        .await
        .unwrap();
    reopened.shutdown().await.unwrap();
    let posts = f.posts.0.lock().await;
    assert_eq!(
        posts
            .iter()
            .filter(|p| p["message"] == "hello world")
            .count(),
        1,
        "{posts:?}"
    );
    assert!(
        posts.last().unwrap()["message"]
            .as_str()
            .unwrap()
            .contains("will not repost"),
        "{posts:?}"
    );
    assert_eq!(f.prompt_count(), 1);
}

#[tokio::test]
async fn replaying_a_recovery_event_does_not_release_another_reply() {
    let f = Fixture::new().await;
    let first = f.bridge();
    for (id, root) in [("root", ""), ("follow-up", "root")] {
        assert!(
            f.handle(&first, id, root, "human", "!oc hello", true)
                .await
                .is_err()
        );
    }
    f.handle(&first, "recover-1", "root", "human", "!oc /recover", false)
        .await
        .unwrap();
    first.shutdown().await.unwrap();
    drop(first);
    let reopened = f.bridge();
    f.handle(
        &reopened,
        "recover-1",
        "root",
        "human",
        "!oc /recover",
        false,
    )
    .await
    .unwrap();
    let replies = || async {
        f.posts
            .0
            .lock()
            .await
            .iter()
            .filter(|p| p["message"] == "hello world")
            .count()
    };
    assert_eq!(replies().await, 1);
    f.handle(
        &reopened,
        "recover-2",
        "root",
        "human",
        "!oc /recover",
        false,
    )
    .await
    .unwrap();
    assert_eq!(replies().await, 2);
    assert_eq!(f.prompt_count(), 2);
    reopened.shutdown().await.unwrap();
}

#[tokio::test]
async fn rejects_bad_receipt_store_and_concurrent_writer() {
    let f = Fixture::new().await;
    let build =
        |config: Value| Bridge::new(config, BTreeMap::new(), f.root.path().join("workspace"));
    let mut not_isolated = f.config.clone();
    not_isolated["mattermost"]["threadIsolation"] = false.into();
    assert!(build(not_isolated).is_err());
    let mut invalid = f.config.clone();
    invalid["mattermost"]["turnReceiptDir"] = "relative/path".into();
    assert!(build(invalid).is_err());
    let first = f.bridge();
    assert!(build(f.config.clone()).is_err());
    drop(first);
    let journal = f.root.path().join("receipts/turns.json");
    std::fs::write(&journal, b"{malformed").unwrap();
    assert!(build(f.config.clone()).is_err());
    assert_eq!(std::fs::read(journal).unwrap(), b"{malformed");
}
