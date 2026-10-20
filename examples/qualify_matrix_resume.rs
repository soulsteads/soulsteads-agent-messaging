//! Offline transport qualification: load private copied mappings through Bridge.
//! No Matrix client, prompt, generation, or message publication is performed.
use anyhow::{Context, Result, ensure};
use async_trait::async_trait;
use soulsteads_messaging::{
    config,
    runtime::{Bridge, Event, Sink},
    store::{Entry, Store},
};
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    path::PathBuf,
    sync::atomic::{AtomicUsize, Ordering},
    time::Duration,
};

#[derive(Default)]
struct MetadataSink {
    notices: AtomicUsize,
}
#[async_trait]
impl Sink for MetadataSink {
    async fn emit(&self, event: Value) -> Result<()> {
        if event["type"] == "notice" && event["text"].as_str().is_some_and(|s| !s.is_empty()) {
            self.notices.fetch_add(1, Ordering::SeqCst);
        }
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let root = PathBuf::from(
        std::env::args()
            .nth(1)
            .context("private qualification root required")?,
    );
    let environment: BTreeMap<String, String> = std::env::vars().collect();
    let configuration =
        config::Loader::default().load(&root, Some(&root.join("bridge.json")), &environment)?;
    ensure!(
        PathBuf::from(std::env::var("OPENCODE_DB")?).starts_with(&root),
        "private copied database required"
    );
    ensure!(
        std::env::var("HINDSIGHT_DISABLE_HOOKS").as_deref() == Ok("1"),
        "memory hooks must be disabled for qualification"
    );
    ensure!(
        configuration["acp"]["profileDir"]
            .as_str()
            .unwrap_or("")
            .is_empty(),
        "native environment must own this dependency-bearing profile"
    );
    let before: Value = serde_json::from_slice(&std::fs::read(root.join("sessions.json"))?)?;
    let entries: Vec<Entry> = serde_json::from_value(before["sessions"].clone())?;
    ensure!(
        !entries.is_empty(),
        "qualification requires actual mappings"
    );
    let sender = configuration["matrix"]["allowedUsers"]
        .as_array()
        .and_then(|a| a.first())
        .and_then(Value::as_str)
        .context("authorized sender required")?
        .to_owned();
    let bridge = Bridge::new(
        configuration,
        environment,
        root.join("unused-new-workspaces"),
    )?;
    let sink = MetadataSink::default();
    let mut loaded = 0;
    for entry in &entries {
        ensure!(entry.connector == "matrix", "Matrix mappings only");
        let mut event = Event::web(&sender, "/model");
        event.platform = "matrix".into();
        event.room = entry.thread_id.clone();
        // Preserved thread-key entries are exercised as exact contexts in main-room mode.
        event.id = format!("isolated-native-resume-{loaded}");
        let previous = sink.notices.load(Ordering::SeqCst);
        let result =
            tokio::time::timeout(Duration::from_secs(180), bridge.handle(event, &sink)).await;
        if !matches!(result, Ok(Ok(()))) {
            bridge.shutdown().await?;
            anyhow::bail!("native mapping load/control failed at index {loaded}");
        }
        ensure!(
            sink.notices.load(Ordering::SeqCst) > previous,
            "missing native control response"
        );
        let retained = Store::new(root.join("sessions.json"))
            .get("matrix", &entry.thread_id, &entry.backend)?
            .context("binding lost")?;
        ensure!(
            retained.session_id == entry.session_id
                && retained.cwd == entry.cwd
                && retained.backend_id == entry.backend_id,
            "binding identity changed"
        );
        loaded += 1;
    }
    bridge.shutdown().await?;
    let after: Value = serde_json::from_slice(&std::fs::read(root.join("sessions.json"))?)?;
    let selections = after["selectedBackends"]
        .as_array()
        .context("missing selections")?;
    for original in before["selectedBackends"]
        .as_array()
        .context("missing original selections")?
    {
        ensure!(
            selections.contains(original),
            "explicit backend choice changed"
        );
    }
    for selected in selections {
        ensure!(
            before["selectedBackends"]
                .as_array()
                .unwrap()
                .contains(selected)
                || entries
                    .iter()
                    .any(|entry| selected["connector"] == entry.connector
                        && selected["threadId"] == entry.thread_id
                        && selected["backend"] == entry.backend),
            "effective backend choice changed"
        );
    }
    println!(
        "{}",
        json!({"loaded_mappings":loaded,"nonempty_control_notices":sink.notices.load(Ordering::SeqCst),"selected_backends_preserved":true,"matrix_consumer_started":false,"generation_requested":false})
    );
    Ok(())
}
