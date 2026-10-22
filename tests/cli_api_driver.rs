use serde_json::{Value, json};
use std::{collections::BTreeMap, path::Path, process::Stdio, time::Duration};
use tokio::{io::AsyncWriteExt, process::Command, time::timeout};

struct Case {
    command: &'static str,
    expected_output: &'static str,
    required_rpc: Option<&'static str>,
    prompts: usize,
}

// Characterization inventory for the CLI routes in src/runtime.rs.
const COMMAND_CASES: &[Case] = &[
    Case {
        command: "/help",
        expected_output: "Commands:",
        required_rpc: None,
        prompts: 1,
    },
    Case {
        command: "/h",
        expected_output: "Commands:",
        required_rpc: None,
        prompts: 1,
    },
    Case {
        command: "/commands",
        expected_output: "Commands:",
        required_rpc: None,
        prompts: 1,
    },
    Case {
        command: "/status",
        expected_output: "Messages: 1",
        required_rpc: None,
        prompts: 1,
    },
    Case {
        command: "/backend",
        expected_output: "Backend: opencode",
        required_rpc: None,
        prompts: 1,
    },
    Case {
        command: "/models",
        expected_output: "currentValue",
        required_rpc: None,
        prompts: 1,
    },
    Case {
        command: "/model two",
        expected_output: "two",
        required_rpc: Some("session/set_config_option"),
        prompts: 1,
    },
    Case {
        command: "/config model two",
        expected_output: "two",
        required_rpc: Some("session/set_config_option"),
        prompts: 1,
    },
    Case {
        command: "/effort high",
        expected_output: "high",
        required_rpc: Some("session/set_config_option"),
        prompts: 1,
    },
    Case {
        command: "/mode",
        expected_output: "build",
        required_rpc: None,
        prompts: 1,
    },
    Case {
        command: "/mode plan",
        expected_output: "build",
        required_rpc: Some("session/set_mode"),
        prompts: 1,
    },
    Case {
        command: "/agent plan",
        expected_output: "build",
        required_rpc: Some("session/set_mode"),
        prompts: 1,
    },
    Case {
        command: "/cancel",
        expected_output: "Cancellation requested.",
        required_rpc: Some("session/cancel"),
        prompts: 1,
    },
    Case {
        command: "/clear",
        expected_output: "Session cleared.",
        required_rpc: Some("session/delete"),
        prompts: 1,
    },
    Case {
        command: "/reset",
        expected_output: "Session cleared.",
        required_rpc: Some("session/delete"),
        prompts: 1,
    },
    Case {
        command: "/detach",
        expected_output: "Session cleared.",
        required_rpc: Some("session/close"),
        prompts: 1,
    },
    Case {
        command: "/d",
        expected_output: "Session cleared.",
        required_rpc: Some("session/close"),
        prompts: 1,
    },
    Case {
        command: "/restart",
        expected_output: "Restart is not enabled.",
        required_rpc: None,
        prompts: 1,
    },
    Case {
        command: "/compact",
        expected_output: "hello world",
        required_rpc: None,
        prompts: 2,
    },
];

// runtime.rs makes these aliases explicitly unknown when sessionPicker.enabled is false.
const PICKER_DISABLED_CASES: &[Case] = &[
    Case {
        command: "/p",
        expected_output: "Unknown command: /p. Try /help",
        required_rpc: None,
        prompts: 1,
    },
    Case {
        command: "/projects",
        expected_output: "Unknown command: /projects. Try /help",
        required_rpc: None,
        prompts: 1,
    },
    Case {
        command: "/s",
        expected_output: "Unknown command: /s. Try /help",
        required_rpc: None,
        prompts: 1,
    },
    Case {
        command: "/sessions",
        expected_output: "Unknown command: /sessions. Try /help",
        required_rpc: None,
        prompts: 1,
    },
    Case {
        command: "/r",
        expected_output: "Unknown command: /r. Try /help",
        required_rpc: None,
        prompts: 1,
    },
    Case {
        command: "/reload",
        expected_output: "Unknown command: /reload. Try /help",
        required_rpc: None,
        prompts: 1,
    },
    Case {
        command: "/m",
        expected_output: "Unknown command: /m. Try /help",
        required_rpc: None,
        prompts: 1,
    },
    Case {
        command: "/mirror",
        expected_output: "Unknown command: /mirror. Try /help",
        required_rpc: None,
        prompts: 1,
    },
];

fn counts(path: &Path) -> BTreeMap<String, usize> {
    std::fs::read_to_string(path)
        .unwrap_or_default()
        .lines()
        .map(|line| serde_json::from_str::<Value>(line).unwrap())
        .fold(BTreeMap::new(), |mut counts, request| {
            let method = request["method"].as_str().unwrap_or("response").to_owned();
            *counts.entry(method).or_default() += 1;
            counts
        })
}

async fn replay(case: &Case) {
    let root = tempfile::Builder::new()
        .prefix("cli-api-driver-")
        .tempdir()
        .unwrap();
    let home = root.path().join("home");
    let tmp = root.path().join("tmp");
    let sessions = root.path().join("sessions");
    let audit = root.path().join("rpc.jsonl");
    std::fs::create_dir(&home).unwrap();
    std::fs::create_dir(&tmp).unwrap();

    let mut fixture_env = json!({"ACP_FIXTURE_RPC_AUDIT": audit});
    if case.command.starts_with("/effort") {
        fixture_env["ACP_FIXTURE_CONFIG_OPTIONS"] = json!([
            {"id":"model","category":"model","currentValue":"one","options":[{"value":"one"},{"value":"two"}]},
            {"id":"thought_level","category":"thought_level","currentValue":"low","options":[{"value":"low"},{"value":"high"}]}
        ])
        .to_string()
        .into();
    }
    let config = root.path().join("config.json");
    std::fs::write(
        &config,
        json!({
            "rateLimitSeconds": 0,
            "sessionStorePath": root.path().join("store.json"),
            "sessionPicker": {"enabled": false},
            "acp": {
                "command": env!("CARGO_BIN_EXE_acp-fixture"),
                "args": [],
                "timeoutMs": 2000,
                "env": fixture_env,
            },
        })
        .to_string(),
    )
    .unwrap();

    let mut command = Command::new(env!("CARGO_BIN_EXE_soulsteads-messaging"));
    command
        .current_dir(root.path())
        .args(["--no-images", "--config", config.to_str().unwrap()])
        .env_clear()
        .env("PATH", "/usr/bin:/bin")
        .env("HOME", &home)
        .env("TMPDIR", &tmp)
        .env("SESSION_BASE_DIR", &sessions)
        .env("LANG", "C.UTF-8")
        .env("NO_COLOR", "1")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    let mut child = command.spawn().unwrap();
    let stdin = format!("hello\n{}\n/quit\n", case.command);
    child
        .stdin
        .take()
        .unwrap()
        .write_all(stdin.as_bytes())
        .await
        .unwrap();

    let output = timeout(Duration::from_secs(15), child.wait_with_output())
        .await
        .unwrap_or_else(|_| panic!("CLI timed out for {}", case.command))
        .unwrap();
    let stdout = String::from_utf8(output.stdout).unwrap();
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(
        output.status.success(),
        "{} exited {:?}; stdout={stdout:?}; stderr={stderr:?}",
        case.command,
        output.status.code()
    );
    assert!(
        stdout.contains(case.expected_output),
        "{} did not contain {:?}; stdout={stdout:?}; stderr={stderr:?}",
        case.command,
        case.expected_output
    );
    assert!(
        !stdout.contains("Sorry, something went wrong"),
        "{} emitted a generic error; stdout={stdout:?}; stderr={stderr:?}",
        case.command
    );

    let rpc_counts = counts(&audit);
    assert_eq!(
        rpc_counts
            .get("session/prompt")
            .copied()
            .unwrap_or_default(),
        case.prompts,
        "{} RPC counts: {rpc_counts:?}",
        case.command
    );
    if let Some(required_rpc) = case.required_rpc {
        assert_eq!(
            rpc_counts.get(required_rpc).copied().unwrap_or_default(),
            1,
            "{} RPC counts: {rpc_counts:?}",
            case.command
        );
    }
}

#[tokio::test]
async fn compiled_cli_stdin_command_inventory_replays_source_diagnostic() {
    for case in COMMAND_CASES {
        replay(case).await;
    }
}

#[tokio::test]
async fn compiled_cli_stdin_picker_disabled_aliases_are_explicit() {
    for case in PICKER_DISABLED_CASES {
        replay(case).await;
    }
}
