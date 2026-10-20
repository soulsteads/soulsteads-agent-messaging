use async_trait::async_trait;
use soulsteads_messaging::{
    config,
    runtime::{Bridge, Event, Sink},
    store::Store,
};
use serde_json::{Value, json};
use std::{collections::BTreeMap, sync::Arc};
use tokio::sync::Mutex;

#[derive(Clone, Default)]
struct Recorder(Arc<Mutex<Vec<Value>>>);
#[async_trait]
impl Sink for Recorder {
    fn owned(&self) -> Option<Arc<dyn Sink>> {
        Some(Arc::new(self.clone()))
    }
    async fn emit(&self, value: Value) -> anyhow::Result<()> {
        self.0.lock().await.push(value);
        Ok(())
    }
}
fn event(text: &str) -> Event {
    let mut event = Event::web("picker", text);
    event.platform = "matrix".into();
    event.room = "!picker:localhost".into();
    event.thread = "$thread".into();
    event
}
fn rpc(path: &std::path::Path) -> Vec<Value> {
    std::fs::read_to_string(path)
        .unwrap_or_default()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect()
}

#[tokio::test]
async fn nondefault_picker_uses_selected_backend_and_switch_invalidates_cached_selection() {
    for backend in ["codex", "claude"] {
        let root = tempfile::tempdir().unwrap();
        let project = root.path().join(backend);
        std::fs::create_dir(&project).unwrap();
        let history = root.path().join("history");
        std::fs::write(&history, format!("{backend}-private-history")).unwrap();
        let audit = root.path().join("selected-rpc");
        let default_audit = root.path().join("default-rpc");
        let executable = env!("CARGO_BIN_EXE_acp-fixture");
        let cfg = config::load(&json!({
            "rateLimitSeconds":0,"sessionStorePath":root.path().join("store.json"),
            "matrix":{"respondToAllMessages":true},"sessionPicker":{"enabled":true},
            "acp":{"command":executable,"args":[],"backendId":"default-identity",
                "env":{"ACP_FIXTURE_RPC_AUDIT":default_audit},
                "backends":{backend:{"command":executable,"args":[],"backendId":"selected-identity",
                    "env":{"ACP_FIXTURE_PROJECT_DIR":project,"ACP_FIXTURE_HISTORY_FILE":history,
                        "ACP_FIXTURE_RPC_AUDIT":audit}}}}
        }).to_string(), &BTreeMap::new()).unwrap();
        let workspace = root.path().join("sessions");
        let bridge = Bridge::new(cfg.clone(), BTreeMap::new(), workspace.clone()).unwrap();
        let sink = Recorder::default();
        let context = bridge.context(&event(""));
        let store = Store::new(root.path().join("store.json"));
        bridge
            .handle(event("/backend opencode"), &sink)
            .await
            .unwrap();
        let original = store.get("matrix", &context, "opencode").unwrap().unwrap();
        bridge
            .handle(event(&format!("/backend {backend}")), &sink)
            .await
            .unwrap();
        let original_selected = store.get("matrix", &context, backend).unwrap().unwrap();
        let default_before = rpc(&default_audit).len();
        for command in ["/p", "/p 1", "/s 1", "/r", "/m 1"] {
            bridge.handle(event(command), &sink).await.unwrap();
        }
        assert_eq!(
            rpc(&default_audit).len(),
            default_before,
            "picker must never contact the default backend"
        );
        let attached = store.get("matrix", &context, backend).unwrap().unwrap();
        assert_eq!(attached.cwd, project.to_string_lossy());
        assert_eq!(attached.backend_id, "selected-identity");
        assert_eq!(
            store
                .get("matrix", &context, "opencode")
                .unwrap()
                .unwrap()
                .session_id,
            original.session_id
        );
        let rows = rpc(&audit);
        assert!(rows.iter().any(|r| r["method"] == "session/list"));
        let loaded: Vec<_> = rows
            .iter()
            .filter(|r| r["method"] == "session/load")
            .collect();
        assert!(
            loaded.len() >= 3,
            "attach/reload/mirror must each load history"
        );
        for row in loaded {
            assert_eq!(row["params"]["cwd"], project.to_string_lossy().as_ref());
            assert_eq!(row["params"]["sessionId"], "fixture");
        }
        assert!(!rows.iter().any(|r| r["method"] == "session/prompt"));
        assert!(sink.0.lock().await.iter().any(|r| {
            r["text"].as_str().is_some_and(|t| {
                t.contains("Mirror mode on:") && t.contains(&format!("{backend}-private-history"))
            })
        }));
        let selected_before_switch = rows.len();
        bridge
            .handle(event("/backend opencode"), &sink)
            .await
            .unwrap();
        let selected_after_switch = rpc(&audit).len();
        assert!(selected_after_switch >= selected_before_switch);
        let replies_before = sink.0.lock().await.len();
        bridge.handle(event("/s 1"), &sink).await.unwrap();
        let after_switch = sink.0.lock().await[replies_before..].to_vec();
        assert!(
            after_switch.iter().any(|r| {
                r["text"]
                    .as_str()
                    .is_some_and(|t| t.contains("No project selected"))
            }),
            "backend switch reused stale picker: {after_switch:?}"
        );
        assert_eq!(bridge.poll_mirrors().await.unwrap(), 0);
        assert_eq!(
            rpc(&audit).len(),
            selected_after_switch,
            "invalidated picker and mirror cannot reopen the former backend"
        );
        assert_eq!(
            store
                .get("matrix", &context, backend)
                .unwrap()
                .unwrap()
                .session_id,
            original_selected.session_id
        );
        bridge.shutdown().await.unwrap();
        let restarted = Bridge::new(cfg, BTreeMap::new(), workspace).unwrap();
        restarted
            .handle(event(&format!("/backend {backend}")), &sink)
            .await
            .unwrap();
        restarted.handle(event("/r"), &sink).await.unwrap();
        assert_eq!(
            store.get("matrix", &context, backend).unwrap().unwrap().cwd,
            project.to_string_lossy()
        );
        restarted.shutdown().await.unwrap();
    }
}
