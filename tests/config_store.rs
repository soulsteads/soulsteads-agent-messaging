#[cfg(feature = "reference-conformance")]
#[path = "support/reference_source.rs"]
mod reference_source;
use serde_json::json;
use soulsteads_messaging::{
    config,
    store::{Entry, Store},
};

#[cfg(feature = "reference-conformance")]
#[test]
fn mixed_actual_typescript_and_rust_writers_share_lock_and_preserve_all_entries() {
    let source = reference_source::root().join("src/session-store.ts");
    assert!(
        source.is_file(),
        "Pinned TypeScript source checkout is required"
    );
    assert!(
        std::process::Command::new("bun")
            .arg("--version")
            .output()
            .expect("Cached Bun is required for mixed-store qualification")
            .status
            .success()
    );
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("store.json");
    let ready = directory.path().join("ready");
    let mut child = std::process::Command::new("bun")
        .arg(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/source-store.ts"
        ))
        .arg(source)
        .arg(&path)
        .arg(&ready)
        .current_dir(directory.path())
        .env("BUN_RUNTIME_TRANSPILER_CACHE_PATH", "0")
        .stdout(std::process::Stdio::null())
        .spawn()
        .unwrap();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    while !ready.exists() {
        if std::time::Instant::now() > deadline {
            let _ = child.kill();
            panic!("Source writer did not start");
        }
        std::thread::sleep(std::time::Duration::from_millis(5));
    }
    let store = soulsteads_messaging::store::Store::new(&path);
    for index in 0..40 {
        store
            .set(
                soulsteads_messaging::store::Entry {
                    connector: "matrix".into(),
                    thread_id: format!("rust-{index}"),
                    session_id: format!("rust-{index}"),
                    cwd: "/fixture".into(),
                    backend_id: "fixture".into(),
                    updated_at: "2026-10-04T00:00:00Z".into(),
                    backend: "claude".into(),
                },
                true,
            )
            .unwrap();
    }
    loop {
        if let Some(status) = child.try_wait().unwrap() {
            assert!(status.success());
            break;
        }
        if std::time::Instant::now() > deadline {
            let _ = child.kill();
            panic!("Source writer timed out");
        }
        std::thread::sleep(std::time::Duration::from_millis(5));
    }
    for index in 0..40 {
        assert!(
            store
                .get("matrix", &format!("source-{index}"), "codex")
                .unwrap()
                .is_some()
        );
        assert!(
            store
                .get("matrix", &format!("rust-{index}"), "claude")
                .unwrap()
                .is_some()
        );
        assert_eq!(
            store
                .selected("matrix", &format!("source-{index}"), "opencode")
                .unwrap(),
            "codex"
        );
    }
}

#[test]
fn corrupt_store_mutations_preserve_original_bytes_and_release_lock() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("store.json");
    let original = b"{ corrupt fixture";
    std::fs::write(&path, original).unwrap();
    assert!(
        soulsteads_messaging::store::Store::new(&path)
            .set_selected("web", "context", "codex")
            .is_err()
    );
    assert_eq!(std::fs::read(&path).unwrap(), original);
    assert!(!directory.path().join("store.json.lock").exists());
}
use std::{collections::BTreeMap, fs, sync::Arc};

#[test]
fn source_defaults_merge_and_environment_substitution() {
    let env = BTreeMap::from([("ROOM".into(), "!local:test".into())]);
    let value = config::load(r#"{"matrix":{"allowedRooms":["{env:ROOM}"]},"streamTools":["read"],"toolMessages":{"mode":"future","maxTraceEntries":0},"web":{"attachments":{"allowedMimeTypes":["image/png","image/svg+xml"],"maxFilesPerMessage":0}}}"#, &env).unwrap();
    assert_eq!(value["botName"], "oc");
    assert_eq!(value["matrix"]["allowedRooms"], json!(["!local:test"]));
    assert_eq!(value["toolMessages"]["showOutputFor"], json!(["read"]));
    assert_eq!(value["toolMessages"]["mode"], "events");
    assert_eq!(value["toolMessages"]["maxTraceEntries"], 20);
    assert_eq!(value["web"]["attachments"]["maxFilesPerMessage"], 1);
    assert_eq!(
        value["web"]["attachments"]["allowedMimeTypes"],
        json!(["image/png"])
    );
    assert_eq!(
        value["telegram"]["attachments"]["maxFileBytes"],
        20 * 1024 * 1024
    );
}

#[test]
fn migrates_v1_without_overriding_legacy_backend() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("sessions.json");
    fs::write(&path, json!({"version":1,"sessions":[{"connector":"matrix","threadId":"room","sessionId":"old","cwd":"/local","backendId":"","updatedAt":"2026-01-01T00:00:00Z"}]}).to_string()).unwrap();
    let store = Store::new(&path);
    assert_eq!(
        store.selected("matrix", "room", "codex").unwrap(),
        "opencode"
    );
    assert_eq!(
        store
            .get("matrix", "room", "opencode")
            .unwrap()
            .unwrap()
            .session_id,
        "old"
    );
    store.set(entry("room", "codex"), true).unwrap();
    assert_eq!(
        store.selected("matrix", "room", "opencode").unwrap(),
        "codex"
    );
    assert!(store.get("matrix", "room", "opencode").unwrap().is_some());
    let data: serde_json::Value = serde_json::from_slice(&fs::read(path).unwrap()).unwrap();
    assert_eq!(data["version"], 2);
    assert_eq!(data["sessions"].as_array().unwrap().len(), 2);
}

#[test]
fn concurrent_atomic_writes_preserve_other_contexts() {
    let directory = tempfile::tempdir().unwrap();
    let store = Arc::new(Store::new(directory.path().join("sessions.json")));
    let writers: Vec<_> = (0..24)
        .map(|index| {
            let store = store.clone();
            std::thread::spawn(move || {
                store
                    .set(entry(&index.to_string(), "opencode"), true)
                    .unwrap()
            })
        })
        .collect();
    for writer in writers {
        writer.join().unwrap();
    }
    for index in 0..24 {
        assert!(
            store
                .get("matrix", &index.to_string(), "opencode")
                .unwrap()
                .is_some()
        );
    }
}

fn entry(thread: &str, backend: &str) -> Entry {
    Entry {
        connector: "matrix".into(),
        thread_id: thread.into(),
        session_id: "session".into(),
        cwd: "/local".into(),
        backend_id: "".into(),
        updated_at: "2026-01-01T00:00:00Z".into(),
        backend: backend.into(),
    }
}

#[test]
fn all_source_configuration_oracles_match_without_panics() {
    let oracle: serde_json::Value =
        serde_json::from_str(include_str!("oracles/config.json")).unwrap();
    let env = oracle["env"]
        .as_object()
        .unwrap()
        .iter()
        .map(|(key, value)| (key.clone(), value.as_str().unwrap().into()))
        .collect();
    for (index, case) in oracle["cases"].as_array().unwrap().iter().enumerate() {
        let mut actual = config::load(&case["input"].to_string(), &env).unwrap();
        // Rust-only presentation extensions: validate disabled defaults, then compare
        // every legacy field to the unchanged source-generated oracle.
        if let Some(matrix) = actual["matrix"].as_object_mut() {
            assert_eq!(
                matrix.remove("showFullPaths"),
                Some(serde_json::json!(false))
            );
            assert_eq!(
                matrix.remove("terminalThread"),
                Some(serde_json::json!({"mode":"off","threshold":3}))
            );
        }
        if let Some(mattermost) = actual["mattermost"].as_object_mut() {
            assert_eq!(
                mattermost.remove("respondToAllMessages"),
                Some(serde_json::json!(false))
            );
        }
        let mut expected = case["expected"].clone();
        // The built-in Rust backend is an intentional migration extension.
        // Preserve explicit legacy overrides and compare all other fields.
        if let Some(backends) = expected
            .get_mut("acp")
            .and_then(|acp| acp.get_mut("backends"))
            .and_then(serde_json::Value::as_object_mut)
        {
            for adapter in ["codex", "claude", "pi"] {
                backends
                    .entry(adapter)
                    .or_insert_with(|| json!({"command":format!("builtin:{adapter}"),"args":[]}));
            }
        }
        assert_eq!(
            actual, expected,
            "source config case {index}: {}",
            case["input"]
        );
    }
}

#[test]
fn whole_thread_removal_clears_selection_but_backend_removal_keeps_it() {
    let directory = tempfile::tempdir().unwrap();
    let store = Store::new(directory.path().join("store.json"));
    store.set(entry("thread", "codex"), true).unwrap();
    store.remove("matrix", "thread", Some("codex")).unwrap();
    assert_eq!(store.selected("matrix", "thread", "pi").unwrap(), "codex");
    store.remove("matrix", "thread", None).unwrap();
    assert_eq!(store.selected("matrix", "thread", "pi").unwrap(), "pi");
}

#[test]
fn interoperates_with_source_exclusive_create_lock() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("store.json");
    let lock = directory.path().join("store.json.lock");
    fs::write(&lock, "").unwrap();
    let other_path = path.clone();
    let writer =
        std::thread::spawn(move || Store::new(other_path).set(entry("thread", "opencode"), true));
    std::thread::sleep(std::time::Duration::from_millis(100));
    assert!(
        !path.exists(),
        "Rust must wait while the TypeScript lock is held"
    );
    fs::remove_file(lock).unwrap();
    writer.join().unwrap().unwrap();
}

#[cfg(unix)]
#[test]
fn store_and_lock_symlinks_are_rejected_without_touching_targets() {
    use std::os::unix::fs::symlink;
    let directory = tempfile::tempdir().unwrap();
    let target = directory.path().join("target");
    fs::write(&target, "sentinel").unwrap();
    let path = directory.path().join("store.json");
    symlink(&target, &path).unwrap();
    assert!(
        Store::new(&path)
            .set(entry("thread", "opencode"), true)
            .is_err()
    );
    assert_eq!(fs::read_to_string(&target).unwrap(), "sentinel");
    fs::remove_file(&path).unwrap();
    symlink(&target, directory.path().join("store.json.lock")).unwrap();
    assert!(
        Store::new(&path)
            .set(entry("thread", "opencode"), true)
            .is_err()
    );
    assert_eq!(fs::read_to_string(&target).unwrap(), "sentinel");
}
