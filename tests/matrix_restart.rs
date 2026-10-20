use async_trait::async_trait;
use soulsteads_messaging::{
    config,
    connectors::matrix_restart::{
        MATRIX_RESTART_ACK, RestartCommand, RestartDispatcher, resolve_restart_command,
    },
    runtime::{Bridge, Event, Sink},
};
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio::sync::{Mutex, Semaphore};

#[derive(Clone)]
struct Recorder {
    events: Arc<Mutex<Vec<Value>>>,
    fail_ack: Arc<AtomicBool>,
    fail_receipt: Arc<AtomicBool>,
    receipt: PathBuf,
    block_mirror: Arc<AtomicBool>,
    mirror_started: Arc<Semaphore>,
    mirror_release: Arc<Semaphore>,
    block_ack: Arc<AtomicBool>,
    ack_started: Arc<Semaphore>,
    ack_release: Arc<Semaphore>,
}
#[async_trait]
impl Sink for Recorder {
    fn owned(&self) -> Option<Arc<dyn Sink>> {
        Some(Arc::new(self.clone()))
    }
    async fn emit(&self, value: Value) -> anyhow::Result<()> {
        let ack = value["text"] == MATRIX_RESTART_ACK;
        let mirror = value["text"] == "Mirror mode stopped.";
        self.events.lock().await.push(value);
        if ack && self.block_ack.load(Ordering::SeqCst) {
            self.ack_started.add_permits(1);
            self.ack_release.acquire().await.unwrap().forget();
        }
        if mirror && self.block_mirror.load(Ordering::SeqCst) {
            self.mirror_started.add_permits(1);
            self.mirror_release.acquire().await.unwrap().forget();
        }
        if ack && self.fail_ack.load(Ordering::SeqCst) {
            anyhow::bail!("ack failed");
        }
        if ack && self.fail_receipt.load(Ordering::SeqCst) {
            std::fs::create_dir(&self.receipt)?;
        }
        Ok(())
    }
}
struct Dispatcher {
    calls: Mutex<Vec<RestartCommand>>,
    fail: AtomicBool,
    block: AtomicBool,
    release: Semaphore,
    sink: Recorder,
}
#[async_trait]
impl RestartDispatcher for Dispatcher {
    async fn dispatch(&self, command: RestartCommand) -> anyhow::Result<()> {
        assert_eq!(command.program(), "/usr/bin/systemctl");
        assert_eq!(
            command.args(),
            [
                "--user",
                "--no-block",
                "restart",
                "bridge@matrix:local.service"
            ]
        );
        let events = self.sink.events.lock().await;
        let ack = events
            .iter()
            .position(|event| event["text"] == MATRIX_RESTART_ACK)
            .unwrap();
        assert!(
            events[ack + 1..]
                .iter()
                .any(|event| event["type"] == "read")
        );
        let receipts: Value =
            serde_json::from_slice(&std::fs::read(&self.sink.receipt).unwrap()).unwrap();
        assert_eq!(receipts["version"], 1);
        assert!(!receipts["events"].as_array().unwrap().is_empty());
        drop(events);
        self.calls.lock().await.push(command);
        if self.block.load(Ordering::SeqCst) {
            self.release.acquire().await.unwrap().forget();
        }
        if self.fail.load(Ordering::SeqCst) {
            anyhow::bail!("hermetic dispatch rejected");
        }
        Ok(())
    }
}
fn configuration(directory: &Path) -> Value {
    config::load(&json!({"rateLimitSeconds":0,"sessionStorePath":directory.join("store.json"),"acp":{"command":env!("CARGO_BIN_EXE_acp-fixture"),"args":[]},"matrix":{"respondToAllMessages":true,"allowedUsers":["@user:local"],"allowedRooms":["!room:local"]}}).to_string(), &BTreeMap::new()).unwrap()
}
fn fixture(
    directory: &Path,
    config: Value,
    unit: &str,
) -> (Arc<Bridge>, Recorder, Arc<Dispatcher>) {
    let sink = Recorder {
        events: Default::default(),
        fail_ack: Default::default(),
        fail_receipt: Default::default(),
        receipt: directory.join("store.json.restarts.json"),
        block_mirror: Default::default(),
        mirror_started: Arc::new(Semaphore::new(0)),
        mirror_release: Arc::new(Semaphore::new(0)),
        block_ack: Default::default(),
        ack_started: Arc::new(Semaphore::new(0)),
        ack_release: Arc::new(Semaphore::new(0)),
    };
    let dispatcher = Arc::new(Dispatcher {
        calls: Default::default(),
        fail: AtomicBool::new(false),
        block: AtomicBool::new(false),
        release: Semaphore::new(0),
        sink: sink.clone(),
    });
    let env = BTreeMap::from([("SOULSTEADS_MESSAGING_SYSTEMD_UNIT".into(), unit.into())]);
    let bridge = Arc::new(
        Bridge::new_with_restart_dispatcher(
            config,
            env,
            directory.join("sessions"),
            dispatcher.clone(),
        )
        .unwrap(),
    );
    (bridge, sink, dispatcher)
}
fn event(text: &str) -> Event {
    Event {
        platform: "matrix".into(),
        room: "!room:local".into(),
        thread: "$root".into(),
        id: uuid::Uuid::new_v4().to_string(),
        user: "@user:local".into(),
        text: text.into(),
        direct: false,
        mentioned: false,
        bot: false,
        images: vec![],
        reply_to: None,
    }
}
async fn dispatched(dispatcher: &Dispatcher, count: usize) {
    tokio::time::timeout(Duration::from_secs(1), async {
        while dispatcher.calls.lock().await.len() < count {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap();
}

#[test]
fn restart_command_restricts_unit_and_fixed_argv_without_execution() {
    for unit in [
        "bridge.service",
        "bridge@matrix:local.service",
        "a.service",
        "0.service",
        "bridge_worker-1.service",
    ] {
        let command = resolve_restart_command(unit).unwrap();
        assert_eq!(command.program(), "/usr/bin/systemctl");
        assert_eq!(command.args(), ["--user", "--no-block", "restart", unit]);
    }
    for unit in [
        "",
        "foo",
        "--user.service",
        "/tmp/foo.service",
        "foo.service;id",
        "foo.service extra",
        "foo.service\n",
    ] {
        assert!(resolve_restart_command(unit).is_none(), "{unit}");
    }
}

#[tokio::test]
async fn restart_ack_receipt_dispatch_gate_and_process_lifetime_replay_preserve_sessions() {
    let directory = tempfile::tempdir().unwrap();
    let config = configuration(directory.path());
    let (bridge, sink, dispatcher) = fixture(
        directory.path(),
        config.clone(),
        "bridge@matrix:local.service",
    );
    bridge.handle(event("hello"), &sink).await.unwrap();
    let before = std::fs::read(directory.path().join("store.json")).unwrap();
    dispatcher.block.store(true, Ordering::SeqCst);
    let restart = event("/restart");
    bridge.handle(restart.clone(), &sink).await.unwrap();
    dispatched(&dispatcher, 1).await;
    bridge.handle(event("/restart"), &sink).await.unwrap();
    bridge
        .handle(event("hello while pending"), &sink)
        .await
        .unwrap();
    assert_eq!(dispatcher.calls.lock().await.len(), 1);
    assert!(sink.events.lock().await.iter().any(|event| {
        event["text"]
            .as_str()
            .is_some_and(|text| text.contains("restart is being dispatched"))
    }));
    assert_eq!(
        std::fs::read(directory.path().join("store.json")).unwrap(),
        before
    );
    dispatcher.release.add_permits(1);
    bridge.shutdown().await.unwrap();
    let (reopened, reopened_sink, reopened_dispatcher) =
        fixture(directory.path(), config, "bridge@matrix:local.service");
    reopened.handle(restart, &reopened_sink).await.unwrap();
    assert!(reopened_sink.events.lock().await.is_empty());
    assert!(reopened_dispatcher.calls.lock().await.is_empty());
    assert_eq!(
        std::fs::read(directory.path().join("store.json")).unwrap(),
        before
    );
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(
            std::fs::metadata(directory.path().join("store.json.restarts.json"))
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
    }
}

#[tokio::test]
async fn restart_refuses_arguments_missing_ids_authorization_units_and_corrupt_receipts() {
    for refusal in ["args", "id", "user", "room", "unit", "corrupt", "schema"] {
        let directory = tempfile::tempdir().unwrap();
        let (bridge, sink, dispatcher) = fixture(
            directory.path(),
            configuration(directory.path()),
            if refusal == "unit" {
                "x.service;id"
            } else {
                "bridge@matrix:local.service"
            },
        );
        let mut input = event("/restart");
        match refusal {
            "args" => input.text = "/restart other.service".into(),
            "id" => input.id.clear(),
            "user" => input.user = "@other:local".into(),
            "room" => input.room = "!other:local".into(),
            "corrupt" => std::fs::write(&sink.receipt, "invalid").unwrap(),
            "schema" => std::fs::write(&sink.receipt, r#"{"version":2,"events":[1]}"#).unwrap(),
            _ => {}
        }
        bridge.handle(input, &sink).await.unwrap();
        assert!(dispatcher.calls.lock().await.is_empty(), "{refusal}");
        assert!(
            sink.events
                .lock()
                .await
                .iter()
                .all(|value| value["text"] != MATRIX_RESTART_ACK && value["type"] != "read"),
            "{refusal}"
        );
        assert!(
            !directory.path().join("store.json").exists(),
            "restart must not initialize ACP"
        );
    }
}

#[tokio::test]
async fn restart_ack_and_receipt_failures_never_dispatch_or_mark_read_and_clear_gate() {
    for receipt_failure in [false, true] {
        let directory = tempfile::tempdir().unwrap();
        let (bridge, sink, dispatcher) = fixture(
            directory.path(),
            configuration(directory.path()),
            "bridge@matrix:local.service",
        );
        if receipt_failure {
            sink.fail_receipt.store(true, Ordering::SeqCst);
        } else {
            sink.fail_ack.store(true, Ordering::SeqCst);
        }
        bridge.handle(event("/restart"), &sink).await.unwrap();
        assert!(dispatcher.calls.lock().await.is_empty());
        assert!(
            sink.events
                .lock()
                .await
                .iter()
                .all(|value| value["type"] != "read")
        );
        assert!(sink.events.lock().await.iter().any(|value| {
            value["text"]
                .as_str()
                .is_some_and(|text| text.contains("Restart was not dispatched"))
        }));
        bridge.handle(event("/status"), &sink).await.unwrap();
        assert!(sink.events.lock().await.iter().any(|value| {
            value["text"]
                .as_str()
                .is_some_and(|text| text.starts_with("Backend:"))
        }));
    }
}

#[tokio::test]
async fn restart_dispatch_failure_clears_gate_but_keeps_durable_replay_receipt() {
    let directory = tempfile::tempdir().unwrap();
    let (bridge, sink, dispatcher) = fixture(
        directory.path(),
        configuration(directory.path()),
        "bridge@matrix:local.service",
    );
    dispatcher.fail.store(true, Ordering::SeqCst);
    let restart = event("/restart");
    bridge.handle(restart.clone(), &sink).await.unwrap();
    dispatched(&dispatcher, 1).await;
    tokio::time::timeout(Duration::from_secs(1), async {
        while !sink.events.lock().await.iter().any(|value| {
            value["text"]
                .as_str()
                .is_some_and(|text| text.contains("dispatch failed"))
        }) {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap();
    bridge.handle(restart, &sink).await.unwrap();
    bridge.handle(event("/status"), &sink).await.unwrap();
    assert_eq!(dispatcher.calls.lock().await.len(), 1);
    assert!(sink.events.lock().await.iter().any(|value| {
        value["text"]
            .as_str()
            .is_some_and(|text| text.starts_with("Backend:"))
    }));
}

#[tokio::test]
async fn restart_idle_guard_covers_other_threads_prompts_and_initializing_controls() {
    for control in [false, true] {
        let directory = tempfile::tempdir().unwrap();
        let started = directory.path().join("init-started");
        let release = directory.path().join("init-release");
        let mut config = configuration(directory.path());
        if control {
            config["acp"]["env"] = json!({"ACP_FIXTURE_INIT_STARTED_FILE":started,"ACP_FIXTURE_INIT_RELEASE_FILE":release});
        } else {
            config["acp"]["env"] = json!({"ACP_FIXTURE_PROMPT_STARTED_FILE":started});
        }
        let (bridge, sink, dispatcher) =
            fixture(directory.path(), config, "bridge@matrix:local.service");
        let active = {
            let bridge = bridge.clone();
            let sink = sink.clone();
            tokio::spawn(async move {
                bridge
                    .handle(event(if control { "/model" } else { "wait" }), &sink)
                    .await
            })
        };
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if started.exists() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .unwrap();
        let mut restart = event("/restart");
        restart.thread = "$other-thread".into();
        bridge.handle(restart, &sink).await.unwrap();
        assert!(dispatcher.calls.lock().await.is_empty());
        assert!(sink.events.lock().await.iter().any(|value| {
            value["text"]
                .as_str()
                .is_some_and(|text| text.contains("Cannot restart while"))
        }));
        if control {
            std::fs::write(release, "ready").unwrap();
        } else {
            bridge.handle(event("/cancel"), &sink).await.unwrap();
        }
        active.await.unwrap().unwrap();
        bridge.handle(event("/restart"), &sink).await.unwrap();
        dispatched(&dispatcher, 1).await;
        bridge.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn restart_rechecks_gate_after_asynchronous_mirror_stop() {
    let directory = tempfile::tempdir().unwrap();
    let mut config = configuration(directory.path());
    config["sessionPicker"]["enabled"] = json!(true);
    config["sessionPicker"]["connectors"] = json!(["matrix"]);
    config["acp"]["env"] = json!({"ACP_FIXTURE_PROJECT_DIR":directory.path()});
    let (bridge, sink, dispatcher) =
        fixture(directory.path(), config, "bridge@matrix:local.service");
    for command in ["/p", "/p 1", "/m 1"] {
        bridge.handle(event(command), &sink).await.unwrap();
    }
    assert!(sink.events.lock().await.iter().any(|value| {
        value["text"]
            .as_str()
            .is_some_and(|text| text.contains("Mirror mode on:"))
    }));
    sink.block_mirror.store(true, Ordering::SeqCst);
    let first = {
        let bridge = bridge.clone();
        let sink = sink.clone();
        tokio::spawn(async move { bridge.handle(event("/restart"), &sink).await })
    };
    tokio::time::timeout(Duration::from_secs(1), sink.mirror_started.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
    let mut other = event("/restart");
    other.thread = "$other".into();
    bridge.handle(other, &sink).await.unwrap();
    dispatched(&dispatcher, 1).await;
    sink.mirror_release.add_permits(1);
    first.await.unwrap().unwrap();
    assert_eq!(dispatcher.calls.lock().await.len(), 1);
    assert_eq!(
        sink.events
            .lock()
            .await
            .iter()
            .filter(|value| value["text"] == MATRIX_RESTART_ACK)
            .count(),
        1
    );
    bridge.shutdown().await.unwrap();
}

#[tokio::test]
async fn restart_gate_blocks_competing_work_while_ack_is_in_flight() {
    let directory = tempfile::tempdir().unwrap();
    let (bridge, sink, dispatcher) = fixture(
        directory.path(),
        configuration(directory.path()),
        "bridge@matrix:local.service",
    );
    sink.block_ack.store(true, Ordering::SeqCst);
    let first = {
        let bridge = bridge.clone();
        let sink = sink.clone();
        tokio::spawn(async move { bridge.handle(event("/restart"), &sink).await })
    };
    tokio::time::timeout(Duration::from_secs(1), sink.ack_started.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
    bridge.handle(event("/restart"), &sink).await.unwrap();
    bridge.handle(event("hello"), &sink).await.unwrap();
    assert!(dispatcher.calls.lock().await.is_empty());
    assert!(!directory.path().join("store.json").exists());
    sink.ack_release.add_permits(1);
    first.await.unwrap().unwrap();
    dispatched(&dispatcher, 1).await;
    assert_eq!(
        sink.events
            .lock()
            .await
            .iter()
            .filter(|value| value["text"] == MATRIX_RESTART_ACK)
            .count(),
        1
    );
}

#[tokio::test]
async fn rate_limited_restart_never_acknowledges_or_dispatches() {
    let directory = tempfile::tempdir().unwrap();
    let mut config = configuration(directory.path());
    config["rateLimitSeconds"] = json!(60);
    let (bridge, sink, dispatcher) =
        fixture(directory.path(), config, "bridge@matrix:local.service");
    bridge.handle(event("/status"), &sink).await.unwrap();
    let reads = sink
        .events
        .lock()
        .await
        .iter()
        .filter(|value| value["type"] == "read")
        .count();
    bridge.handle(event("/restart"), &sink).await.unwrap();
    assert!(dispatcher.calls.lock().await.is_empty());
    assert!(
        sink.events
            .lock()
            .await
            .iter()
            .all(|value| value["text"] != MATRIX_RESTART_ACK)
    );
    assert_eq!(
        sink.events
            .lock()
            .await
            .iter()
            .filter(|value| value["type"] == "read")
            .count(),
        reads
    );
}
