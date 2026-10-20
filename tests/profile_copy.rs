use async_trait::async_trait;
use soulsteads_messaging::{
    config,
    runtime::{Bridge, Event, Sink},
    store::Store,
};
use serde_json::{Value, json};
use std::{collections::BTreeMap, path::Path, sync::Arc};
use tokio::sync::Mutex;

#[derive(Default, Clone)]
struct Recorder(Arc<Mutex<Vec<Value>>>);
#[async_trait]
impl Sink for Recorder {
    async fn emit(&self, event: Value) -> anyhow::Result<()> {
        self.0.lock().await.push(event);
        Ok(())
    }
}

fn configuration(root: &Path, profile: &Path) -> Value {
    config::load(&json!({"rateLimitSeconds":0,"sessionStorePath":root.join("store.json"),"acp":{"command":env!("CARGO_BIN_EXE_acp-fixture"),"args":[],"profileDir":profile,"env":{"ACP_FIXTURE_MATRIX_AUDIT":root.join("audit"),"ACP_FIXTURE_PROFILE_EXPECT":json!({"AGENTS.md":"synthetic agents",".ferrum/config.toml":"synthetic config",".opencode/skills/demo/SKILL.md":"synthetic skill"}).to_string()}}}).to_string(), &BTreeMap::new()).unwrap()
}

fn profile(root: &Path) -> std::path::PathBuf {
    let source = root.join("profile");
    for (name, bytes) in [
        ("AGENTS.md", "synthetic agents"),
        (".ferrum/config.toml", "synthetic config"),
        (".opencode/skills/demo/SKILL.md", "synthetic skill"),
    ] {
        let file = source.join(name);
        std::fs::create_dir_all(file.parent().unwrap()).unwrap();
        std::fs::write(file, bytes).unwrap();
    }
    source
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn profile_bytes_are_present_before_actual_session_new() {
    let root = tempfile::tempdir().unwrap();
    let source = profile(root.path());
    let bridge = Bridge::new(
        configuration(root.path(), &source),
        BTreeMap::new(),
        root.path().join("sessions"),
    )
    .unwrap();
    let event = Event::web("profile", "/model");
    let sink = Recorder::default();
    let opened = bridge.handle(event.clone(), &sink).await;
    assert!(
        opened.is_ok(),
        "fixture must observe applied profile before session/new: {opened:?}"
    );
    assert!(
        Store::new(root.path().join("store.json"))
            .get("web", "profile", "opencode")
            .unwrap()
            .is_some(),
        "fixture must admit the session after verifying profile bytes"
    );
    assert_eq!(
        std::fs::read(bridge.working_directory(&event).join("AGENTS.md")).unwrap(),
        b"synthetic agents"
    );
    bridge.shutdown().await.unwrap();
}

fn admitted(root: &Path, context: &str, backend: &str) -> bool {
    Store::new(root.join("store.json"))
        .get("web", context, backend)
        .unwrap()
        .is_some()
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn profile_effective_backend_override_and_non_opencode_gate() {
    for backend in ["opencode", "codex"] {
        let root = tempfile::tempdir().unwrap();
        let source = profile(root.path());
        let mut settings = configuration(root.path(), &root.path().join("invalid-top-level"));
        let mut effective = settings["acp"].clone();
        effective["profileDir"] = source.to_string_lossy().into();
        if backend == "codex" {
            effective["profileDir"] = root.path().join("missing").to_string_lossy().into();
            effective["env"]["ACP_FIXTURE_PROFILE_EXPECT"] = "{}".into();
        }
        settings["acp"]["defaultBackend"] = backend.into();
        settings["acp"]["backends"][backend] = effective;
        let bridge = Bridge::new(settings, BTreeMap::new(), root.path().join("sessions")).unwrap();
        let event = Event::web("effective", "/model");
        bridge
            .handle(event.clone(), &Recorder::default())
            .await
            .unwrap();
        assert!(admitted(root.path(), "effective", backend));
        assert_eq!(
            bridge.working_directory(&event).join("AGENTS.md").exists(),
            backend == "opencode"
        );
        bridge.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn profile_failed_backend_switch_preserves_selection_and_previous_active_session() {
    let root = tempfile::tempdir().unwrap();
    let source = profile(root.path());
    let mut settings = configuration(root.path(), &root.path().join("invalid-opencode"));
    let mut alternate = settings["acp"].clone();
    alternate["profileDir"] = source.to_string_lossy().into();
    alternate["env"]["ACP_FIXTURE_PROFILE_EXPECT"] = "{}".into();
    settings["acp"]["defaultBackend"] = "codex".into();
    settings["acp"]["backends"]["codex"] = alternate;
    let bridge = Bridge::new(settings, BTreeMap::new(), root.path().join("sessions")).unwrap();
    let sink = Recorder::default();
    bridge
        .handle(Event::web("switch", "/model"), &sink)
        .await
        .unwrap();
    let store = root.path().join("store.json");
    let before = std::fs::read(&store).unwrap();
    let audit = std::fs::read(root.path().join("audit")).unwrap();
    assert!(
        bridge
            .handle(Event::web("switch", "/backend opencode"), &sink)
            .await
            .is_err()
    );
    assert_eq!(std::fs::read(&store).unwrap(), before);
    assert_eq!(std::fs::read(root.path().join("audit")).unwrap(), audit);
    bridge
        .handle(Event::web("switch", "/backend"), &sink)
        .await
        .unwrap();
    assert!(
        sink.0
            .lock()
            .await
            .iter()
            .any(|event| event["text"] == "Backend: codex")
    );
    bridge
        .handle(Event::web("switch", "/model"), &sink)
        .await
        .unwrap();
    assert_eq!(std::fs::read(root.path().join("audit")).unwrap(), audit);
    bridge.shutdown().await.unwrap();
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn profile_reconnect_load_overwrites_but_active_reuse_and_leftovers_are_retained() {
    let root = tempfile::tempdir().unwrap();
    let source = profile(root.path());
    let mut settings = configuration(root.path(), &source);
    let bridge = Bridge::new(
        settings.clone(),
        BTreeMap::new(),
        root.path().join("sessions"),
    )
    .unwrap();
    let event = Event::web("reload", "/model");
    bridge
        .handle(event.clone(), &Recorder::default())
        .await
        .unwrap();
    assert!(admitted(root.path(), "reload", "opencode"));
    let workspace = bridge.working_directory(&event);
    std::fs::write(workspace.join("unrelated"), b"retain unrelated").unwrap();
    std::fs::write(source.join("AGENTS.md"), b"replacement").unwrap();
    std::fs::remove_file(source.join(".opencode/skills/demo/SKILL.md")).unwrap();
    bridge
        .handle(event.clone(), &Recorder::default())
        .await
        .unwrap();
    assert_eq!(
        std::fs::read(workspace.join("AGENTS.md")).unwrap(),
        b"synthetic agents"
    );
    bridge.shutdown().await.unwrap();
    settings["acp"]["env"]["ACP_FIXTURE_PROFILE_EXPECT"] = json!({"AGENTS.md":"replacement",".ferrum/config.toml":"synthetic config",".opencode/skills/demo/SKILL.md":"synthetic skill","unrelated":"retain unrelated"}).to_string().into();
    for _ in 0..2 {
        let bridge = Bridge::new(
            settings.clone(),
            BTreeMap::new(),
            root.path().join("sessions"),
        )
        .unwrap();
        bridge
            .handle(event.clone(), &Recorder::default())
            .await
            .unwrap();
        assert_eq!(
            std::fs::read(workspace.join("AGENTS.md")).unwrap(),
            b"replacement"
        );
        assert_eq!(
            std::fs::read(workspace.join("unrelated")).unwrap(),
            b"retain unrelated"
        );
        assert!(admitted(root.path(), "reload", "opencode"));
        bridge.shutdown().await.unwrap();
    }
    let audit = std::fs::read_to_string(root.path().join("audit")).unwrap();
    assert_eq!(
        audit
            .lines()
            .filter(|line| line.contains("session/new"))
            .count(),
        1
    );
    assert_eq!(
        audit
            .lines()
            .filter(|line| line.contains("session/load"))
            .count(),
        2
    );
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn profile_relative_path_is_process_cwd_relative() {
    let root = tempfile::tempdir().unwrap();
    let source = profile(root.path());
    let relative = source
        .strip_prefix(std::env::current_dir().unwrap())
        .unwrap();
    let bridge = Bridge::new(
        configuration(root.path(), relative),
        BTreeMap::new(),
        root.path().join("sessions"),
    )
    .unwrap();
    bridge
        .handle(Event::web("relative", "/model"), &Recorder::default())
        .await
        .unwrap();
    assert!(admitted(root.path(), "relative", "opencode"));
    bridge.shutdown().await.unwrap();
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn profile_absent_empty_and_empty_tree_are_valid_but_invalid_paths_fail_before_launch() {
    for case in ["absent", "empty-option", "empty-tree", "missing", "file"] {
        let root = tempfile::tempdir().unwrap();
        let source = root.path().join("input");
        if case == "empty-tree" {
            std::fs::create_dir(&source).unwrap();
        }
        if case == "file" {
            std::fs::write(&source, b"synthetic").unwrap();
        }
        let mut settings = configuration(root.path(), &source);
        settings["acp"]["env"]["ACP_FIXTURE_PROFILE_EXPECT"] = "{}".into();
        if case == "absent" {
            settings["acp"]
                .as_object_mut()
                .unwrap()
                .remove("profileDir");
        }
        if case == "empty-option" {
            settings["acp"]["profileDir"] = "".into();
        }
        let bridge = Bridge::new(settings, BTreeMap::new(), root.path().join("sessions")).unwrap();
        let valid = matches!(case, "absent" | "empty-option" | "empty-tree");
        let result = bridge
            .handle(Event::web("validity", "/model"), &Recorder::default())
            .await;
        assert_eq!(result.is_ok(), valid, "{case}: {result:?}");
        assert_eq!(
            admitted(root.path(), "validity", "opencode"),
            valid,
            "{case}"
        );
        assert_eq!(root.path().join("audit").exists(), valid, "{case}");
        bridge.shutdown().await.unwrap();
    }
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn profile_invalid_load_preserves_store_and_persisted_only_clear_does_not_open() {
    let root = tempfile::tempdir().unwrap();
    let source = profile(root.path());
    let mut settings = configuration(root.path(), &source);
    let bridge = Bridge::new(
        settings.clone(),
        BTreeMap::new(),
        root.path().join("sessions"),
    )
    .unwrap();
    bridge
        .handle(Event::web("persist", "/model"), &Recorder::default())
        .await
        .unwrap();
    bridge.shutdown().await.unwrap();
    let before = std::fs::read(root.path().join("store.json")).unwrap();
    let audit = std::fs::read(root.path().join("audit")).unwrap();
    settings["acp"]["profileDir"] = root.path().join("missing").to_string_lossy().into();
    let bridge = Bridge::new(settings, BTreeMap::new(), root.path().join("sessions")).unwrap();
    assert!(
        bridge
            .handle(Event::web("persist", "/model"), &Recorder::default())
            .await
            .is_err()
    );
    assert_eq!(
        std::fs::read(root.path().join("store.json")).unwrap(),
        before
    );
    assert_eq!(std::fs::read(root.path().join("audit")).unwrap(), audit);
    bridge
        .handle(Event::web("persist", "/clear"), &Recorder::default())
        .await
        .unwrap();
    assert!(!admitted(root.path(), "persist", "opencode"));
    assert_eq!(std::fs::read(root.path().join("audit")).unwrap(), audit);
    bridge.shutdown().await.unwrap();
}

#[cfg(unix)]
#[tokio::test]
async fn profile_unsupported_source_and_destination_entries_fail_before_launch_without_external_writes()
 {
    use std::os::unix::fs::symlink;
    for case in [
        "source-root",
        "source-ancestor",
        "source-leaf",
        "source-hardlink",
        "source-fifo",
        "source-nested",
        "destination-root",
        "destination-ancestor",
        "destination-leaf",
        "destination-hardlink",
        "destination-fifo",
        "destination-nested",
    ] {
        let root = tempfile::tempdir().unwrap();
        let mut source = profile(root.path());
        let external = root.path().join("external");
        std::fs::create_dir(&external).unwrap();
        let sentinel = external.join("AGENTS.md");
        std::fs::write(&sentinel, b"external sentinel").unwrap();
        if case == "source-root" {
            let alias = root.path().join("alias");
            symlink(&source, &alias).unwrap();
            source = alias;
        }
        if case == "source-ancestor" {
            let alias = root.path().join("alias");
            symlink(root.path(), &alias).unwrap();
            source = alias.join("profile");
        }
        if case == "source-nested" {
            symlink(&external, source.join(".ferrum/escape")).unwrap();
        }
        if matches!(case, "source-leaf" | "source-hardlink" | "source-fifo") {
            let target = source.join("AGENTS.md");
            std::fs::remove_file(&target).unwrap();
            if case == "source-leaf" {
                symlink(&sentinel, &target).unwrap();
            }
            if case == "source-hardlink" {
                std::fs::hard_link(&sentinel, &target).unwrap();
            }
            if case == "source-fifo" {
                fifo(&target);
            }
        }
        let bridge = Bridge::new(
            configuration(root.path(), &source),
            BTreeMap::new(),
            root.path().join("sessions"),
        )
        .unwrap();
        let event = Event::web("escapes", "/model");
        let workspace = bridge.working_directory(&event);
        if case.starts_with("destination") {
            std::fs::create_dir_all(workspace.parent().unwrap()).unwrap();
            if case == "destination-root" {
                symlink(&external, &workspace).unwrap();
            } else if case == "destination-ancestor" {
                std::fs::remove_dir(workspace.parent().unwrap()).unwrap();
                symlink(&external, workspace.parent().unwrap()).unwrap();
            } else {
                std::fs::create_dir(&workspace).unwrap();
                let target = workspace.join("AGENTS.md");
                if case == "destination-leaf" {
                    symlink(&sentinel, &target).unwrap();
                }
                if case == "destination-hardlink" {
                    std::fs::hard_link(&sentinel, &target).unwrap();
                }
                if case == "destination-fifo" {
                    fifo(&target);
                }
                if case == "destination-nested" {
                    symlink(&external, workspace.join(".ferrum")).unwrap();
                }
            }
        }
        assert!(
            bridge.handle(event, &Recorder::default()).await.is_err(),
            "{case}"
        );
        assert!(!admitted(root.path(), "escapes", "opencode"), "{case}");
        assert!(!root.path().join("audit").exists(), "{case}");
        assert_eq!(
            std::fs::read(&sentinel).unwrap(),
            b"external sentinel",
            "{case}"
        );
        assert_eq!(std::fs::read_dir(&external).unwrap().count(), 1, "{case}");
        if workspace.is_dir() && !matches!(case, "destination-root" | "destination-ancestor") {
            assert!(
                !workspace.join(".opencode").exists(),
                "preflight must reject before copying any tree contents: {case}"
            );
        }
        bridge.shutdown().await.unwrap();
    }
}

#[cfg(unix)]
fn fifo(path: &Path) {
    use std::os::unix::ffi::OsStrExt;
    let name = std::ffi::CString::new(path.as_os_str().as_bytes()).unwrap();
    assert_eq!(unsafe { libc::mkfifo(name.as_ptr(), 0o600) }, 0);
}

#[cfg(not(target_os = "linux"))]
#[tokio::test]
async fn profile_publication_is_rejected_before_native_launch_on_unsupported_platforms() {
    let root = tempfile::tempdir().unwrap();
    let source = profile(root.path());
    let bridge = Bridge::new(
        configuration(root.path(), &source),
        BTreeMap::new(),
        root.path().join("sessions"),
    )
    .unwrap();
    let event = Event::web("unsupported", "/model");
    let error = bridge
        .handle(event.clone(), &Recorder::default())
        .await
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("requires Linux descriptor publication"),
        "{error}"
    );
    assert!(!root.path().join("audit").exists());
    assert!(!admitted(root.path(), "unsupported", "opencode"));
    assert!(!bridge.working_directory(&event).join("AGENTS.md").exists());
    bridge.shutdown().await.unwrap();
}
