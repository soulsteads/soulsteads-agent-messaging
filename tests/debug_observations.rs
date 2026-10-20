use soulsteads_messaging::debug_log::DebugLogger;
use serde_json::json;

#[test]
fn source_optional_debug_append_is_private_bounded_and_never_logs_payloads() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("logs/bridge-debug.log");
    let disabled = DebugLogger::new(false, path.clone());
    disabled.observation(&json!({"rawChars":4}));
    assert!(!path.exists());
    let logger = DebugLogger::new(true, path.clone());
    for _ in 0..2 {
        logger.observation(&json!({"type":"prompt_end","rawChars":4,"cleanChars":0,"imageCount":1,"stopReason":"end_turn","prompt":"fixture-secret","headers":{"authorization":"fixture-secret"},"env":{"TOKEN":"fixture-secret"},"model":"fixture-secret","text":"fixture-secret"}));
    }
    let log = std::fs::read_to_string(&path).unwrap();
    assert_eq!(log.lines().count(), 2);
    assert!(log.contains("rawChars"));
    assert!(log.contains("end_turn"));
    assert!(!log.contains("fixture-secret"));
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(
            std::fs::metadata(&path).unwrap().permissions().mode() & 0o777,
            0o600
        );
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();
        logger.observation(&json!({"rawChars":3}));
        assert_eq!(
            std::fs::metadata(&path).unwrap().permissions().mode() & 0o777,
            0o600
        );
    }
}

#[test]
fn failed_debug_path_is_best_effort_and_symlink_is_never_followed() {
    let root = tempfile::tempdir().unwrap();
    std::fs::write(root.path().join("blocker"), "fixture").unwrap();
    let logger = DebugLogger::new(true, root.path().join("blocker/bridge-debug.log"));
    logger.observation(&json!({"rawChars":1}));
    logger.observation(&json!({"rawChars":2}));
    assert_eq!(
        std::fs::read_to_string(root.path().join("blocker")).unwrap(),
        "fixture"
    );
    #[cfg(unix)]
    {
        std::fs::write(root.path().join("outside"), "unchanged").unwrap();
        std::os::unix::fs::symlink(
            root.path().join("outside"),
            root.path().join("bridge-debug.log"),
        )
        .unwrap();
        let logger = DebugLogger::new(true, root.path().join("bridge-debug.log"));
        logger.observation(&json!({"rawChars":1}));
        assert_eq!(
            std::fs::read_to_string(root.path().join("outside")).unwrap(),
            "unchanged"
        );
    }
}
