use serde_json::Value;
use std::{
    fs,
    path::Path,
    process::{Command, Output},
};

fn run(root: &Path, args: &[&str], env: &[(&str, &str)]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_soulsteads-messaging"))
        .current_dir(root)
        .env_clear()
        .envs(env.iter().copied())
        .args(args)
        .output()
        .unwrap()
}

fn success(output: Output) -> Value {
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(output.stderr.is_empty());
    serde_json::from_slice(&output.stdout).unwrap()
}

fn failure(output: Output, category: &str) {
    assert!(!output.status.success());
    assert!(output.stdout.is_empty());
    let error = String::from_utf8(output.stderr).unwrap();
    assert!(error.contains(category), "{error}");
    assert!(!error.contains("synthetic_private"), "{error}");
}

#[test]
fn list_reports_only_known_paths_and_proven_sources() {
    let temp = tempfile::tempdir().unwrap();
    fs::write(
        temp.path().join("custom.json"),
        r#"{"botName":"{env:LABEL}","web":{"port":3420},"matrix":{"accessToken":"synthetic_private","encryption":{"synthetic_private":"secret"}},"agentMessaging":{"enabled":false,"synthetic_private":{"enabled":true}},"acp":{"backends":{"synthetic_private":{"command":"secret"}}}}"#,
    )
    .unwrap();
    let list = success(run(
        temp.path(),
        &["--settings", "list", "--config", "custom.json", "--json"],
        &[("LABEL", "synthetic_private"), ("WEB_PORT", "3420")],
    ));
    let entries = list["settings"].as_array().unwrap();
    for (path, source) in [
        ("/web/port", "env_override"),
        ("/web/host", "default"),
        ("/botName", "configured"),
        ("/agentMessaging/enabled", "configured"),
    ] {
        assert!(
            entries
                .iter()
                .any(|entry| entry["path"] == path && entry["source"] == source)
        );
    }
    assert!(
        entries
            .iter()
            .all(|entry| entry.as_object().unwrap().len() == 2)
    );
    let rendered = list.to_string();
    assert!(!rendered.contains("synthetic_private"));
    assert!(!rendered.contains("accessToken"));
    assert_eq!(fs::read_dir(temp.path()).unwrap().count(), 1);
}

#[test]
fn validate_is_strict_and_read_only() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("soulsteads-messaging.json");
    let args = ["--settings", "validate", "--json"];
    assert_eq!(success(run(temp.path(), &args, &[]))["valid"], true);
    for (fixture, category) in [
        (r#"{"synthetic_private":"#, "invalid_json"),
        ("[]", "expected_object"),
        (
            r#"{"web":{"synthetic_private":1,"synthetic_privat\u0065":2}}"#,
            "duplicate_key",
        ),
        (
            r#"{"ignored":[{"synthetic_private":1,"synthetic_private":2}]}"#,
            "duplicate_key",
        ),
        (
            r#"{"telegram":{"enabled":true,"token":"synthetic_private"}}"#,
            "connector_access",
        ),
        (r#"{"web":false}"#, "invalid_shape"),
        (r#"{"sessionStorePath":""}"#, "session_store"),
        (r#"{"acp":{"agentHome":"synthetic_private"}}"#, "agent_home"),
        (r#"{"agentMessaging":{"enabled":true}}"#, "agent_admission"),
    ] {
        fs::write(&path, fixture).unwrap();
        failure(run(temp.path(), &args, &[]), category);
    }
    let fixture = r#"{"acp":{"command":"synthetic_private"}}"#;
    fs::write(&path, fixture).unwrap();
    failure(
        run(temp.path(), &args, &[("WEB_PORT", "synthetic_private")]),
        "runtime_override",
    );
    assert_eq!(success(run(temp.path(), &args, &[]))["valid"], true);

    assert_eq!(fs::read_to_string(&path).unwrap(), fixture);
    assert_eq!(fs::read_dir(temp.path()).unwrap().count(), 1);
    failure(
        run(
            temp.path(),
            &["--settings", "validate", "--config", "synthetic_private"],
            &[],
        ),
        "config_read",
    );
}

#[cfg(unix)]
#[test]
fn validates_mattermost_receipt_directory_without_initializing_it() {
    use std::os::unix::fs::PermissionsExt;

    let temp = tempfile::tempdir().unwrap();
    let receipts = temp
        .path()
        .canonicalize()
        .unwrap()
        .join("synthetic_private");
    let path = temp.path().join("soulsteads-messaging.json");
    let fixture = serde_json::json!({"mattermost": {"turnReceiptDir": receipts}}).to_string();
    fs::write(&path, &fixture).unwrap();
    let args = ["--settings", "validate", "--json"];
    failure(run(temp.path(), &args, &[]), "receipt_directory");
    assert!(!receipts.exists());

    fs::create_dir(&receipts).unwrap();
    fs::set_permissions(&receipts, fs::Permissions::from_mode(0o755)).unwrap();
    failure(run(temp.path(), &args, &[]), "receipt_directory");
    fs::set_permissions(&receipts, fs::Permissions::from_mode(0o700)).unwrap();
    assert_eq!(success(run(temp.path(), &args, &[]))["valid"], true);
    assert_eq!(fs::read_dir(&receipts).unwrap().count(), 0);
    assert_eq!(fs::read_to_string(&path).unwrap(), fixture);
}

#[cfg(all(unix, feature = "agent-network"))]
#[test]
fn validates_agent_review_queue_without_initializing_it() {
    use std::os::unix::fs::PermissionsExt;

    let temp = tempfile::tempdir().unwrap();
    let queue = temp
        .path()
        .canonicalize()
        .unwrap()
        .join("synthetic_private");
    let path = temp.path().join("soulsteads-messaging.json");
    let fixture = serde_json::json!({"agentMessaging": {
        "enabled": true,
        "tools": {"enabled": true, "harness": "codex", "reviewQueueDir": queue},
        "agents": [{"id": "builder", "name": "Builder", "allowedSenders": [],
            "target": {"platform": "web", "context": "builder", "backend": "codex"}}]
    }})
    .to_string();
    fs::write(&path, &fixture).unwrap();
    let args = ["--settings", "validate", "--json"];
    failure(run(temp.path(), &args, &[]), "review_queue_directory");
    assert!(!queue.exists());
    fs::create_dir(&queue).unwrap();
    fs::set_permissions(&queue, fs::Permissions::from_mode(0o700)).unwrap();
    assert_eq!(success(run(temp.path(), &args, &[]))["valid"], true);
    assert_eq!(fs::read_dir(&queue).unwrap().count(), 0);
    assert_eq!(fs::read_to_string(&path).unwrap(), fixture);
    let entry = success(run(
        temp.path(),
        &[
            "--settings",
            "get",
            "/agentMessaging/tools/reviewQueueDir",
            "--json",
        ],
        &[],
    ));
    assert_eq!(
        entry,
        serde_json::json!({
            "path":"/agentMessaging/tools/reviewQueueDir", "source":"configured"
        })
    );
}

#[cfg(unix)]
#[test]
fn legacy_check_preserves_empty_dotenv_device_links() {
    let temp = tempfile::tempdir().unwrap();
    std::os::unix::fs::symlink("/dev/null", temp.path().join(".env.local")).unwrap();
    let legacy = run(temp.path(), &["--check-config"], &[]);
    assert!(
        legacy.status.success(),
        "{}",
        String::from_utf8_lossy(&legacy.stderr)
    );
    assert!(String::from_utf8_lossy(&legacy.stdout).contains("no connectors started"));
    failure(
        run(temp.path(), &["--settings", "validate", "--json"], &[]),
        "environment",
    );
}

#[test]
fn legacy_tool_messages_array_replaces_the_default_child_paths() {
    let temp = tempfile::tempdir().unwrap();
    fs::write(
        temp.path().join("soulsteads-messaging.json"),
        r#"{"toolMessages":[]}"#,
    )
    .unwrap();
    let list = success(run(temp.path(), &["--settings", "list", "--json"], &[]));
    let entry = serde_json::json!({"path":"/toolMessages","source":"configured"});
    let entries: Vec<_> = list["settings"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|entry| entry["path"].as_str().unwrap().starts_with("/toolMessages"))
        .collect();
    assert_eq!(entries, vec![&entry]);
    assert_eq!(
        success(run(
            temp.path(),
            &["--settings", "get", "/toolMessages", "--json"],
            &[]
        )),
        entry
    );
    failure(
        run(
            temp.path(),
            &["--settings", "get", "/toolMessages/mode"],
            &[],
        ),
        "unknown_setting",
    );
}

#[test]
fn legacy_web_array_is_reported_as_configured_without_web_child_paths() {
    let temp = tempfile::tempdir().unwrap();
    fs::write(
        temp.path().join("soulsteads-messaging.json"),
        r#"{"web":[]}"#,
    )
    .unwrap();
    let list = success(run(temp.path(), &["--settings", "list", "--json"], &[]));
    let expected = serde_json::json!({"path":"/web","source":"configured"});
    let entries: Vec<_> = list["settings"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|entry| {
            let path = entry["path"].as_str().unwrap();
            path == "/web" || path.starts_with("/web/")
        })
        .collect();
    assert_eq!(entries, vec![&expected]);
    assert_eq!(
        success(run(
            temp.path(),
            &["--settings", "get", "/web", "--json"],
            &[]
        )),
        expected
    );
    let validate = ["--settings", "validate", "--json"];
    assert_eq!(success(run(temp.path(), &validate, &[]))["valid"], true);
    for (key, value) in [
        ("WEB_PORT", "1234"),
        ("WEB_ALLOWED_ORIGINS", "http://example.test"),
    ] {
        let output = run(temp.path(), &validate, &[(key, value)]);
        assert!(!String::from_utf8_lossy(&output.stderr).contains("panicked"));
        failure(output, "runtime_override");
    }
}

#[test]
fn get_returns_metadata_and_sanitizes_unknown_or_hidden_paths() {
    let temp = tempfile::tempdir().unwrap();
    fs::write(temp.path().join(".env"), "WEB_HOST=synthetic_private\n").unwrap();
    let entry = success(run(
        temp.path(),
        &["--settings", "get", "/web/host", "--json"],
        &[],
    ));
    assert_eq!(
        entry,
        serde_json::json!({"path":"/web/host","source":"env_override"})
    );
    for path in ["/matrix/accessToken", "/synthetic_private"] {
        failure(
            run(temp.path(), &["--settings", "get", path, "--json"], &[]),
            "unknown_setting",
        );
    }
}

#[test]
fn legacy_prompt_keeps_literal_settings_words() {
    let temp = tempfile::tempdir().unwrap();
    for prompt in [["prompt", "--settings"], ["--", "--settings"]] {
        failure(
            run(
                temp.path(),
                &["--config", "absent.json", prompt[0], prompt[1]],
                &[],
            ),
            "Chat bridge failed",
        );
    }
}

#[test]
fn settings_dispatch_wins_over_mcp_and_rejects_mixed_modes() {
    let temp = tempfile::tempdir().unwrap();
    for args in [
        &["--mcp-sse-proxy", "--settings", "validate"][..],
        &["--settings", "validate", "--serve"],
        &["--settings", "list", "--check-config"],
        &["--settings", "list", "--acp-adapter", "codex"],
        &["--settings=synthetic_private", "--json"],
        &["--settings", "synthetic_private"],
    ] {
        failure(run(temp.path(), args, &[]), "settings_usage");
    }
}

#[test]
fn malformed_acp_backends_get_a_sanitized_error() {
    let temp = tempfile::tempdir().unwrap();
    for backends in [
        serde_json::json!("synthetic_private"),
        serde_json::json!([]),
    ] {
        fs::write(
            temp.path().join("soulsteads-messaging.json"),
            serde_json::json!({"acp": {"backends": backends}}).to_string(),
        )
        .unwrap();
        failure(
            run(temp.path(), &["--settings", "validate"], &[]),
            "invalid_shape",
        );
    }
}

#[test]
fn settings_help_skips_config_and_dotenv() {
    let temp = tempfile::tempdir().unwrap();
    fs::write(temp.path().join("soulsteads-messaging.json"), "[").unwrap();
    fs::write(temp.path().join(".env"), "invalid assignment").unwrap();
    let output = run(temp.path(), &["--settings", "--help"], &[]);
    assert!(output.status.success());
    assert!(output.stderr.is_empty());
    let help = String::from_utf8(output.stdout).unwrap();
    for option in [
        "--settings",
        "list",
        "validate",
        "get",
        "--config",
        "--json",
    ] {
        assert!(help.contains(option), "{help}");
    }
}

#[test]
fn settings_accepts_dot_relative_config_and_preserves_dotenv_precedence() {
    let temp = tempfile::tempdir().unwrap();
    fs::write(temp.path().join("soulsteads-messaging.json"), "[").unwrap();
    fs::write(temp.path().join("custom.json"), "{}").unwrap();
    fs::write(temp.path().join(".env"), "WEB_PORT=invalid\n").unwrap();
    fs::write(temp.path().join(".env.development"), "WEB_PORT=1234\n").unwrap();
    fs::write(temp.path().join(".env.local"), "WEB_PORT=invalid\n").unwrap();
    let args = [
        "--settings",
        "validate",
        "--config",
        "./custom.json",
        "--json",
    ];
    failure(run(temp.path(), &args, &[]), "runtime_override");
    assert_eq!(
        success(run(temp.path(), &args, &[("WEB_PORT", "0")]))["valid"],
        true
    );
    fs::remove_file(temp.path().join(".env.local")).unwrap();
    assert_eq!(success(run(temp.path(), &args, &[]))["valid"], true);

    #[cfg(unix)]
    {
        std::os::unix::fs::symlink("custom.json", temp.path().join("linked.json")).unwrap();
        let mut args = args;
        args[3] = "./linked.json";
        assert_eq!(success(run(temp.path(), &args, &[]))["valid"], true);
    }
}

#[test]
fn validation_checks_enabled_web_port_without_binding() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("soulsteads-messaging.json");
    let args = ["--settings", "validate", "--json"];
    for port in [
        serde_json::json!("8080"),
        serde_json::json!(-1),
        serde_json::json!(65536),
    ] {
        fs::write(
            &path,
            serde_json::json!({"web": {"enabled": true, "port": port}}).to_string(),
        )
        .unwrap();
        failure(run(temp.path(), &args, &[]), "web_port");
    }
    fs::write(&path, r#"{"web":{"enabled":false,"port":65536}}"#).unwrap();
    assert_eq!(success(run(temp.path(), &args, &[]))["valid"], true);
    fs::write(
        &path,
        r#"{"web":{"enabled":true,"port":0,"host":"invalid host"}}"#,
    )
    .unwrap();
    assert_eq!(success(run(temp.path(), &args, &[]))["valid"], true);
}

#[test]
fn validation_checks_platform_sdk_support_without_connecting() {
    let temp = tempfile::tempdir().unwrap();
    let args = ["--settings", "validate", "--json"];
    for platform in ["matrix", "whatsapp"] {
        fs::write(
            temp.path().join("soulsteads-messaging.json"),
            serde_json::json!({(platform): {"enabled": true, "allowAllUsers": true}}).to_string(),
        )
        .unwrap();
        let output = run(temp.path(), &args, &[]);
        if cfg!(feature = "platform-sdks") {
            assert_eq!(success(output)["valid"], true);
        } else {
            failure(output, "platform_sdks_feature");
        }
    }
}

#[cfg(unix)]
fn check_fifo_is_rejected(filename: &str, category: &str) {
    use std::{
        ffi::CString,
        os::unix::ffi::OsStrExt,
        process::Stdio,
        time::{Duration, Instant},
    };

    let temp = tempfile::tempdir().unwrap();
    let fifo = CString::new(temp.path().join(filename).as_os_str().as_bytes()).unwrap();
    assert_eq!(unsafe { libc::mkfifo(fifo.as_ptr(), 0o600) }, 0);
    let stdout = temp.path().join("stdout");
    let stderr = temp.path().join("stderr");
    let mut child = Command::new(env!("CARGO_BIN_EXE_soulsteads-messaging"))
        .current_dir(temp.path())
        .env_clear()
        .args(["--settings", "validate"])
        .stdin(Stdio::null())
        .stdout(fs::File::create(&stdout).unwrap())
        .stderr(fs::File::create(&stderr).unwrap())
        .spawn()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(2);
    let status = loop {
        match child.try_wait() {
            Ok(Some(status)) => break status,
            Ok(None) if Instant::now() < deadline => std::thread::sleep(Duration::from_millis(10)),
            result => {
                let _ = child.kill();
                child.wait().unwrap();
                panic!("settings did not exit before the deadline for {filename}: {result:?}");
            }
        }
    };
    failure(
        Output {
            status,
            stdout: fs::read(stdout).unwrap(),
            stderr: fs::read(stderr).unwrap(),
        },
        category,
    );
}

#[cfg(unix)]
#[test]
fn settings_rejects_config_fifo() {
    check_fifo_is_rejected("soulsteads-messaging.json", "config_read");
}

#[cfg(unix)]
#[test]
fn settings_rejects_dotenv_fifo() {
    check_fifo_is_rejected(".env", "environment");
}

#[test]
fn settings_rejects_oversized_files() {
    let temp = tempfile::tempdir().unwrap();
    for (filename, category) in [
        ("soulsteads-messaging.json", "config_read"),
        (".env", "environment"),
    ] {
        let path = temp.path().join(filename);
        fs::File::create(&path)
            .unwrap()
            .set_len(1024 * 1024 + 1)
            .unwrap();
        failure(run(temp.path(), &["--settings", "validate"], &[]), category);
        fs::remove_file(path).unwrap();
    }
}
