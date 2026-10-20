#![cfg(feature = "reference-conformance")]

#[path = "support/reference_source.rs"]
mod reference_source;
use async_trait::async_trait;
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use soulsteads_messaging::{
    config,
    runtime::{Bridge, Event, Sink},
    web,
};
use std::{collections::BTreeMap, future::IntoFuture, path::Path, sync::Arc, time::Duration};
use tokio::sync::Mutex;

#[derive(Default)]
struct Recorder(Mutex<Vec<Value>>);
#[async_trait]
impl Sink for Recorder {
    async fn emit(&self, event: Value) -> anyhow::Result<()> {
        self.0.lock().await.push(event);
        Ok(())
    }
}

fn configuration(root: &Path, picker: &Value) -> Value {
    config::load(&json!({"botName":"Command Bot","trigger":"!bridge","rateLimitSeconds":0,
        "sessionPicker":picker, "sessionStorePath":root.join("store.json"),
        "acp":{"command":env!("CARGO_BIN_EXE_acp-fixture"),"args":[],"env":{
            "ACP_FIXTURE_RPC_AUDIT":root.join("audit.jsonl"),"ACP_FIXTURE_COMMANDS_FILE":root.join("commands.json"),
            "ACP_FIXTURE_MATRIX_SCRIPT":root.join("script.json"),"ACP_FIXTURE_MATRIX_COUNTER":root.join("counter")}}
    }).to_string(), &BTreeMap::new()).unwrap()
}

fn prepare(root: &Path, steps: &Value, picker: &Value) {
    std::fs::create_dir_all(root).unwrap();
    std::fs::write(
        root.join("commands.json"),
        json!([
            {"name":"compact","description":"Compact context"},
            {"name":"summary","description":"Summarize this session"},
            {"name":"model","description":"Backend model command"},
            {"name":"help","description":"Backend help command"},
            {"name":"MiXeD","description":"Case-sensitive advertised name"},
            {"name":"p","description":"Native command shadowed by disabled picker"},
            {"name":"status","description":"Native status with arguments"},
            {"name":"clear","description":"Native clear with arguments"}
        ])
        .to_string(),
    )
    .unwrap();
    let mut actions = vec![json!({"text":"hello world","stopReason":"end_turn"}); 16];
    for (index, reply) in steps
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|step| step["reply"].as_str())
        .enumerate()
    {
        actions[index]["text"] = reply.into();
    }
    std::fs::write(root.join("script.json"), json!(actions).to_string()).unwrap();
    std::fs::write(
        root.join("soulsteads-messaging.json"),
        configuration(root, picker).to_string(),
    )
    .unwrap();
    std::fs::write(
        root.join("specification.json"),
        json!({"steps":steps}).to_string(),
    )
    .unwrap();
}

fn source_cases(root: &Path, steps: &Value, picker: &Value) -> Vec<Value> {
    prepare(root, steps, picker);
    let source = reference_source::root();
    let oracle = std::process::Command::new("bun")
        .arg("--no-env-file")
        .arg(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/source-commands.ts"
        ))
        .arg(source)
        .arg(root.join("specification.json"))
        .current_dir(root)
        .env_clear()
        .env("PATH", std::env::var("PATH").unwrap())
        .env("HOME", root)
        .env("SESSION_BASE_DIR", root.join("sessions"))
        .env("TMPDIR", std::env::temp_dir())
        .env("TMP", std::env::temp_dir())
        .env("TEMP", std::env::temp_dir())
        .env("BUN_RUNTIME_TRANSPILER_CACHE_PATH", "0")
        .output()
        .unwrap();
    assert!(
        oracle.status.success(),
        "Source ingress failed: {}\n{}",
        String::from_utf8_lossy(&oracle.stdout),
        String::from_utf8_lossy(&oracle.stderr)
    );
    let stdout = String::from_utf8(oracle.stdout).unwrap();
    let mut cases: Vec<Value> = serde_json::from_str(stdout.lines().last().unwrap()).unwrap();
    // The product rename is intentional; normalize only the historical help
    // header. All command content, state and ACP audit comparisons stay exact.
    for case in &mut cases {
        for field in ["output", "libraryOutput"] {
            for value in case[field].as_array_mut().into_iter().flatten() {
                if let Some(tail) = value.as_str().and_then(|text| {
                    text.strip_prefix("Command Bot - OpenCode Chat Bridge\n\nBridge commands:")
                }) {
                    *value = json!(format!(
                        "Command Bot - Soulsteads Agent Messaging\n\nBridge commands:{tail}"
                    ));
                }
            }
        }
    }
    assert_eq!(cases.len(), steps.as_array().unwrap().len());
    cases
}

fn presentation(events: &[Value]) -> (Vec<Value>, Vec<Value>) {
    let responses: Vec<_> = events
        .iter()
        .filter(|event| event["type"] == "response")
        .map(|event| event["text"].clone())
        .collect();
    let chunks: String = events
        .iter()
        .filter(|event| event["type"] == "chunk")
        .filter_map(|event| event["text"].as_str())
        .collect();
    let output = if !responses.is_empty() {
        responses
    } else if !chunks.is_empty() {
        vec![json!(chunks)]
    } else {
        vec![]
    };
    let states = events
        .iter()
        .filter(|event| event["type"] == "session_state")
        .map(|event| event["state"].clone())
        .collect();
    (output, states)
}

async fn compare_web_and_library(steps: Value) {
    compare_web_and_library_with_picker(steps, json!({"enabled":false})).await;
}

async fn compare_web_and_library_with_picker(steps: Value, picker: Value) {
    let root = tempfile::tempdir().unwrap();
    let cases = source_cases(&root.path().join("source"), &steps, &picker);
    let mut mismatches = Vec::new();
    for wire in [false, true] {
        let destination = root.path().join(if wire { "web" } else { "library" });
        prepare(&destination, &steps, &picker);
        let configuration = configuration(&destination, &picker);
        let bridge = Arc::new(
            Bridge::new(
                configuration.clone(),
                BTreeMap::new(),
                destination.join("sessions"),
            )
            .unwrap(),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(
            axum::serve(
                listener,
                web::router(bridge.clone(), configuration["web"].clone()),
            )
            .into_future(),
        );
        let (mut socket, _) =
            tokio_tungstenite::connect_async(format!("ws://{address}/ws?clientId=shared"))
                .await
                .unwrap();
        socket.next().await.unwrap().unwrap();
        for case in &cases {
            let text = case["text"].as_str().unwrap();
            let label = text.chars().take(60).collect::<String>();
            let recorder = Recorder::default();
            let mut events = Vec::new();
            if wire {
                socket
                    .send(json!({"type":"message","text":text}).to_string().into())
                    .await
                    .unwrap();
                tokio::time::timeout(Duration::from_secs(8), async {
                    loop {
                        let event: Value = serde_json::from_str(
                            socket.next().await.unwrap().unwrap().to_text().unwrap(),
                        )
                        .unwrap();
                        let done = event["type"] == "done";
                        events.push(event);
                        if done {
                            break;
                        }
                    }
                })
                .await
                .unwrap();
            } else {
                let result = bridge
                    .handle_web(Event::web("shared", text), &recorder)
                    .await;
                if let Err(error) = result {
                    mismatches.push(format!("library {label:?}: {error}"));
                }
                events = recorder.0.into_inner();
            }
            let (output, states) = presentation(&events);
            let expected = if wire {
                &case["output"]
            } else {
                &case["libraryOutput"]
            };
            if json!(output) != *expected || json!(states) != case["states"] {
                mismatches.push(format!(
                    "{} {label:?}: Source={} Rust={}",
                    if wire { "WebSocket" } else { "library" },
                    json!({"output":expected,"states":case["states"]}),
                    json!({"output":output,"states":states})
                ));
            }
            let requests: Vec<Value> = std::fs::read_to_string(destination.join("audit.jsonl"))
                .unwrap_or_default()
                .lines()
                .map(|line| serde_json::from_str(line).unwrap())
                .collect();
            let prompts: Vec<_> = requests
                .iter()
                .filter(|request| request["method"] == "session/prompt")
                .map(|request| request["params"]["prompt"][0]["text"].clone())
                .collect();
            let opens = requests
                .iter()
                .filter(|request| request["method"] == "session/new")
                .count();
            let deletes = requests
                .iter()
                .filter(|request| request["method"] == "session/delete")
                .count();
            if json!(prompts) != case["prompts"]
                || json!(opens) != case["sessionOpens"]
                || json!(deletes) != case["sessionDeletes"]
            {
                mismatches.push(format!(
                    "{} {label:?}: ACP prompt/open/delete audit differs from Source",
                    if wire { "WebSocket" } else { "library" }
                ));
            }
        }
        socket.close(None).await.unwrap();
        bridge.shutdown().await.unwrap();
        server.abort();
    }
    assert!(mismatches.is_empty(), "{}", mismatches.join("\n"));
}

#[tokio::test]
async fn shared_status_uses_actual_source_clean_output_character_counts() {
    compare_web_and_library(json!([
        {"id":"shared","text":"hello","reply":"Report: /fixture/report.md"},
        {"id":"shared","text":"/status"}
    ]))
    .await;
}

#[test]
fn shared_status_rounding_matches_source_ingress_using_actual_completed_prompt_statistics() {
    let root = tempfile::tempdir().unwrap();
    let steps = json!([
        {"id":"shared","text":"hello😀"},
        {"id":"shared","text":"/status","ageMs":29999,"idleMs":1},
        {"id":"shared","text":"/status","ageMs":30000,"idleMs":29999},
        {"id":"shared","text":"/status","ageMs":89999,"idleMs":30000},
        {"id":"shared","text":"/status","ageMs":90000,"idleMs":90000}
    ]);
    let cases = source_cases(
        &root.path().join("source"),
        &steps,
        &json!({"enabled":false}),
    );
    for case in cases.iter().skip(1) {
        let stats = &case["stats"];
        let actual = soulsteads_messaging::core::session_status(
            stats["messageCount"].as_u64().unwrap() as usize,
            stats["inputChars"].as_u64().unwrap() as usize,
            stats["outputChars"].as_u64().unwrap() as usize,
            Duration::from_millis(case["ageMs"].as_u64().unwrap()),
            Duration::from_millis(case["idleMs"].as_u64().unwrap()),
        );
        assert_eq!(json!([actual]), case["output"]);
    }
}

#[tokio::test]
async fn shared_status_context_percentage_matches_source_decimal_rounding() {
    compare_web_and_library(json!([
        {"id":"shared","text":"x".repeat(988)},
        {"id":"shared","text":"/status"},
        {"id":"shared","text":"/clear"},
        {"id":"shared","text":"x".repeat(1148)},
        {"id":"shared","text":"/status"},
        {"id":"shared","text":"/clear"},
        {"id":"shared","text":"x".repeat(2988)},
        {"id":"shared","text":"/status"},
        {"id":"shared","text":"/clear"},
        {"id":"shared","text":"x".repeat(4988)},
        {"id":"shared","text":"/status"},
        {"id":"shared","text":"/clear"},
        {"id":"shared","text":"x".repeat(8028)},
        {"id":"shared","text":"/status"}
    ]))
    .await;
}

#[tokio::test]
async fn shared_help_picker_options_match_source_without_opening_a_backend() {
    for picker in [
        json!({"enabled":true,"connectors":["web"],"mirrorIntervalSeconds":3}),
        json!({"enabled":true,"connectors":["web"],"mirrorIntervalSeconds":17}),
        json!({"enabled":true,"connectors":["matrix"],"mirrorIntervalSeconds":17}),
    ] {
        compare_web_and_library_with_picker(
            json!([
                {"id":"shared","text":"/help"},
                {"id":"shared","text":"/h"},
                {"id":"shared","text":"/status"}
            ]),
            picker,
        )
        .await;
    }
}

#[tokio::test]
async fn web_unknown_malformed_and_picker_disabled_commands_do_not_forward_but_advertised_commands_do()
 {
    compare_web_and_library(json!([
        {"id":"shared","text":"/summary before-session"},
        {"id":"shared","text":"/help extra"},
        {"id":"shared","text":"/status extra"},
        {"id":"shared","text":"/clear extra"},
        {"id":"shared","text":"/reset now"},
        {"id":"shared","text":"/"},
        {"id":"shared","text":"//summary"},
        {"id":"shared","text":"/unknown"},
        {"id":"shared","text":"/p 1"},
        {"id":"shared","text":"/projects"},
        {"id":"shared","text":"/s"},
        {"id":"shared","text":"/sessions"},
        {"id":"shared","text":"/m 1"},
        {"id":"shared","text":"/mirror 1"},
        {"id":"shared","text":"/r now"},
        {"id":"shared","text":"/reload now"},
        {"id":"shared","text":"/d"},
        {"id":"shared","text":"/detach"},
        {"id":"shared","text":"/h"},
        {"id":"shared","text":"hello"},
        {"id":"shared","text":"/unknown value"},
        {"id":"shared","text":"/status extra"},
        {"id":"shared","text":"/summary value"},
        {"id":"shared","text":"/clear extra"},
        {"id":"shared","text":"/SuMmArY arg"},
        {"id":"shared","text":"/summary\targ"},
        {"id":"shared","text":"/model two"},
        {"id":"shared","text":"/help extra"},
        {"id":"shared","text":"/compact"},
        {"id":"shared","text":"/commands"},
        {"id":"shared","text":"/MiXeD"},
        {"id":"shared","text":"/status"},
        {"id":"shared","text":"/reset"},
        {"id":"shared","text":"/summary"},
        {"id":"shared","text":"/status"}
    ]))
    .await;
}

#[tokio::test]
async fn shared_local_commands_match_actual_source_web_ingress_and_completed_prompt_accounting() {
    compare_web_and_library(json!([
        {"id":"shared","text":"/status"},
        {"id":"shared","text":"/help"},
        {"id":"shared","text":" \t/H \n"},
        {"id":"shared","text":"/clear"},
        {"id":"shared","text":"/RESET"},
        {"id":"shared","text":"/p"},
        {"id":"shared","text":"  hello😀  "},
        {"id":"shared","text":"/status"},
        {"id":"shared","text":"/help"},
        {"id":"shared","text":"/h"},
        {"id":"shared","text":"x".repeat(5001)+"😀"},
        {"id":"shared","text":"/status"},
        {"id":"shared","text":"/clear"},
        {"id":"shared","text":"/status"},
        {"id":"shared","text":"hello"},
        {"id":"shared","text":"/reset"},
        {"id":"shared","text":"/status"}
    ]))
    .await;
}
