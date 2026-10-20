use async_trait::async_trait;
use soulsteads_messaging::{
    config,
    runtime::{Bridge, Event, Sink},
    store::Store,
    tools::{Presenter, matrix_notice},
};
use serde_json::{Value, json};
use std::{collections::BTreeMap, path::Path, sync::Arc, time::Duration};
use tokio::sync::Mutex;

#[derive(Clone, Default)]
struct Recorder(Arc<Mutex<Vec<Value>>>);
#[async_trait]
impl Sink for Recorder {
    fn owned(&self) -> Option<Arc<dyn Sink>> {
        Some(Arc::new(self.clone()))
    }
    async fn emit(&self, event: Value) -> anyhow::Result<()> {
        self.0.lock().await.push(event);
        Ok(())
    }
}

fn configuration(directory: &Path) -> Value {
    let executable = env!("CARGO_BIN_EXE_acp-fixture");
    config::load(
        &json!({"rateLimitSeconds":0,"sessionStorePath":directory.join("store.json"),
        "matrix":{"respondToAllMessages":true},
        "acp":{"command":executable,"args":[],"backendId":"oc","env":{},"backends":{
            "codex":{"command":executable,"args":[],"backendId":"cx","env":{}},
            "claude":{"command":executable,"args":[],"backendId":"cl","env":{}}
        }}})
        .to_string(),
        &BTreeMap::new(),
    )
    .unwrap()
}
fn event(text: &str) -> Event {
    Event {
        platform: "matrix".into(),
        room: "!fixture:localhost".into(),
        thread: "$thread".into(),
        id: String::new(),
        reply_to: None,
        user: "@user:localhost".into(),
        text: text.into(),
        direct: false,
        mentioned: false,
        bot: false,
        images: vec![],
    }
}
async fn wait_file(path: &Path) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while !path.exists() {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap();
}
fn audit(directory: &Path) -> Vec<Value> {
    std::fs::read_to_string(directory.join("audit"))
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect()
}

#[tokio::test]
async fn backend_histories_switch_restart_clear_and_detach_independently() {
    let directory = tempfile::tempdir().unwrap();
    let configuration = configuration(directory.path());
    let workspace = directory.path().join("sessions");
    let bridge = Bridge::new(configuration.clone(), BTreeMap::new(), workspace.clone()).unwrap();
    let sink = Recorder::default();
    let context = bridge.context(&event("hello"));
    let store = Store::new(directory.path().join("store.json"));
    bridge.handle(event("hello"), &sink).await.unwrap();
    let original = store.get("matrix", &context, "opencode").unwrap().unwrap();
    for backend in ["codex", "claude", "opencode"] {
        bridge
            .handle(event(&format!("/backend {backend}")), &sink)
            .await
            .unwrap();
        assert_eq!(
            store.selected("matrix", &context, "unused").unwrap(),
            backend
        );
        assert_eq!(
            store.get("matrix", &context, backend).unwrap().unwrap().cwd,
            original.cwd
        );
    }
    assert_eq!(
        store
            .get("matrix", &context, "opencode")
            .unwrap()
            .unwrap()
            .session_id,
        original.session_id
    );
    bridge.handle(event("/backend codex"), &sink).await.unwrap();
    bridge.shutdown().await.unwrap();
    let restarted = Bridge::new(configuration, BTreeMap::new(), workspace).unwrap();
    restarted.handle(event("hello again"), &sink).await.unwrap();
    assert_eq!(restarted.selected("matrix", &context).unwrap(), "codex");
    restarted.handle(event("/clear"), &sink).await.unwrap();
    assert!(store.get("matrix", &context, "codex").unwrap().is_none());
    assert!(store.get("matrix", &context, "opencode").unwrap().is_some());
    assert!(store.get("matrix", &context, "claude").unwrap().is_some());
    assert_eq!(restarted.selected("matrix", &context).unwrap(), "codex");
    restarted.handle(event("hello"), &sink).await.unwrap();
    restarted.handle(event("/d"), &sink).await.unwrap();
    assert!(store.get("matrix", &context, "codex").unwrap().is_none());
    assert!(store.get("matrix", &context, "opencode").unwrap().is_some());
    restarted.shutdown().await.unwrap();
    assert!(Path::new(&original.cwd).is_dir());
}

#[tokio::test]
async fn failed_backend_candidate_and_store_publication_keep_original_usable() {
    let directory = tempfile::tempdir().unwrap();
    let mut configuration = configuration(directory.path());
    configuration["acp"]["backends"]["claude"]["command"] =
        directory.path().join("absent").to_string_lossy().into();
    let bridge = Bridge::new(
        configuration,
        BTreeMap::new(),
        directory.path().join("sessions"),
    )
    .unwrap();
    let sink = Recorder::default();
    let context = bridge.context(&event("hello"));
    bridge.handle(event("hello"), &sink).await.unwrap();
    assert!(
        bridge
            .handle(event("/backend claude"), &sink)
            .await
            .is_err()
    );
    let file = directory.path().join("store.json");
    let saved = directory.path().join("saved.json");
    std::fs::rename(&file, &saved).unwrap();
    std::fs::create_dir(&file).unwrap();
    assert!(bridge.handle(event("/backend codex"), &sink).await.is_err());
    std::fs::remove_dir(&file).unwrap();
    std::fs::rename(saved, file).unwrap();
    assert_eq!(bridge.selected("matrix", &context).unwrap(), "opencode");
    bridge.handle(event("still usable"), &sink).await.unwrap();
    assert_eq!(
        sink.0
            .lock()
            .await
            .iter()
            .filter(|output| output["type"] == "response" && output["text"] == "hello world")
            .count(),
        2
    );
    bridge.shutdown().await.unwrap();
}

#[tokio::test]
async fn matrix_followup_during_initialization_uses_one_prompt_collector() {
    let directory = tempfile::tempdir().unwrap();
    let mut configuration = configuration(directory.path());
    let started = directory.path().join("started");
    let release = directory.path().join("release");
    let prompt_started = directory.path().join("prompt-started");
    let script = directory.path().join("script.json");
    std::fs::write(
        &script,
        json!([{"waitForFollowup":true,"text":"first "},{"text":"second","stopReason":"end_turn"}])
            .to_string(),
    )
    .unwrap();
    configuration["acp"]["env"] = json!({"ACP_FIXTURE_INIT_STARTED_FILE":started,"ACP_FIXTURE_INIT_RELEASE_FILE":release,
        "ACP_FIXTURE_PROMPT_STARTED_FILE":prompt_started,"ACP_FIXTURE_MATRIX_SCRIPT":script,
        "ACP_FIXTURE_MATRIX_COUNTER":directory.path().join("counter"),"ACP_FIXTURE_RPC_AUDIT":directory.path().join("audit")});
    let bridge = Arc::new(
        Bridge::new(
            configuration,
            BTreeMap::new(),
            directory.path().join("sessions"),
        )
        .unwrap(),
    );
    let sink = Recorder::default();
    let original = {
        let bridge = bridge.clone();
        let sink = sink.clone();
        tokio::spawn(async move { bridge.handle(event("first"), &sink).await })
    };
    wait_file(&started).await;
    let followup_sink = Recorder::default();
    let followup = {
        let bridge = bridge.clone();
        let sink = followup_sink.clone();
        tokio::spawn(async move { bridge.handle(event("second"), &sink).await })
    };
    std::fs::write(release, "release").unwrap();
    wait_file(&prompt_started).await;
    tokio::time::timeout(Duration::from_secs(5), async {
        followup.await.unwrap().unwrap();
        original.await.unwrap().unwrap();
    })
    .await
    .unwrap();
    let requests = audit(directory.path());
    assert_eq!(
        requests
            .iter()
            .filter(|request| request["method"] == "session/new")
            .count(),
        1
    );
    let prompts: Vec<_> = requests
        .iter()
        .filter(|request| request["method"] == "session/prompt")
        .collect();
    assert_eq!(prompts.len(), 2);
    assert_eq!(
        prompts[0]["params"]["sessionId"],
        prompts[1]["params"]["sessionId"]
    );
    assert!(
        followup_sink
            .0
            .lock()
            .await
            .iter()
            .all(|output| output["type"] != "response" && output["type"] != "chunk")
    );
    assert_eq!(
        sink.0
            .lock()
            .await
            .iter()
            .filter(|output| output["type"] == "response")
            .count(),
        1
    );
    bridge.shutdown().await.unwrap();
}

#[test]
fn grouped_tools_keep_updating_without_heading_or_extra_spacing() {
    let mut presenter =
        Presenter::new(json!({"mode":"hermes","showCalls":true,"showArguments":true}));
    let pending = json!({"sessionUpdate":"tool_call","toolCallId":"read1","title":"read","status":"pending","rawInput":{"filePath":"/scratch/README.md"}});
    let initial = presenter.update(&pending);
    assert_eq!(initial[0]["message"], "📖 Reading /scratch/README.md");
    let mut running = pending;
    running["sessionUpdate"] = "tool_call_update".into();
    running["status"] = "in_progress".into();
    assert!(presenter.update(&running).is_empty());
    running["status"] = "completed".into();
    assert!(presenter.update(&running).is_empty());
    let output = presenter.update(&json!({"sessionUpdate":"tool_call","toolCallId":"bash1","title":"bash","status":"completed","rawInput":{"command":"git status --short"}}));
    let text = "📖 Reading /scratch/README.md\n💻 terminal\n```\ngit status --short\n```";
    assert_eq!(output[0]["message"], text);
    assert_eq!(
        matrix_notice(text, true)["formatted_body"],
        "📖 Reading /scratch/README.md<br>💻 terminal<pre><code>git status --short</code></pre>"
    );
}
