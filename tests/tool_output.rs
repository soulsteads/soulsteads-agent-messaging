#[path = "support/reference_source.rs"]
mod reference_source;
use async_trait::async_trait;
use soulsteads_messaging::{
    config,
    runtime::{Bridge, Event, Sink},
};
use serde_json::{Value, json};
use std::collections::BTreeMap;
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

#[tokio::test]
async fn configured_output_selectors_forward_cumulative_deltas_completed_and_failed_results() {
    let root = tempfile::tempdir().unwrap();
    let updates = json!([
        {"sessionUpdate":"tool_call","toolCallId":"one","title":"bash"},
        {"sessionUpdate":"tool_call_update","toolCallId":"one","title":"bash","status":"in_progress","rawOutput":{"output":" first "}},
        {"sessionUpdate":"tool_call_update","toolCallId":"one","title":"bash","status":"in_progress","rawOutput":{"output":" first "}},
        {"sessionUpdate":"tool_call_update","toolCallId":"one","title":"bash","status":"in_progress","rawOutput":{"output":" first second "}},
        {"sessionUpdate":"tool_call_update","toolCallId":"one","title":"bash","status":"completed","content":[{"content":{"type":"text","text":"first second"}}]},
        {"sessionUpdate":"tool_call_update","toolCallId":"two","title":"read","status":"completed","rawOutput":{"output":"r".repeat(2100)}},
        {"sessionUpdate":"tool_call_update","toolCallId":"three","title":"bash","status":"failed","rawOutput":{"error":"denied"}}
    ]);
    let selectors = [
        json!([]),
        json!([" "]),
        json!(["bash"]),
        json!([" * "]),
        json!(["ash"]),
        json!(["BASH"]),
    ];
    let input = root.path().join("input.json");
    std::fs::write(
        &input,
        json!(
            selectors
                .iter()
                .map(|selectors| json!({"selectors":selectors,"updates":updates}))
                .collect::<Vec<_>>()
        )
        .to_string(),
    )
    .unwrap();
    let source = reference_source::root();
    let oracle = std::process::Command::new("bun")
        .arg("--no-env-file")
        .arg(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/source-tool-output.ts"
        ))
        .arg(source)
        .arg(input)
        .current_dir(root.path())
        .env("BUN_RUNTIME_TRANSPILER_CACHE_PATH", "0")
        .output()
        .unwrap();
    assert!(
        oracle.status.success(),
        "{}",
        String::from_utf8_lossy(&oracle.stderr)
    );
    let stdout = String::from_utf8(oracle.stdout).unwrap();
    let expected: Vec<Vec<Value>> = serde_json::from_str(stdout.lines().last().unwrap()).unwrap();
    let mut mismatches = Vec::new();
    for (index, selectors) in selectors.iter().enumerate() {
        let directory = root.path().join(index.to_string());
        std::fs::create_dir(&directory).unwrap();
        let script = directory.join("script.json");
        std::fs::write(&script,json!([{"updates":updates.as_array().unwrap().iter().map(|update|json!({"update":update})).collect::<Vec<_>>(),"text":"answer","stopReason":"end_turn"}]).to_string()).unwrap();
        let configuration=config::load(&json!({"rateLimitSeconds":0,"sessionStorePath":directory.join("store.json"),"toolMessages":{"showCalls":false,"showOutputFor":selectors},
            "acp":{"command":env!("CARGO_BIN_EXE_acp-fixture"),"args":[],"env":{"ACP_FIXTURE_MATRIX_SCRIPT":script,"ACP_FIXTURE_MATRIX_COUNTER":directory.join("counter")}}}).to_string(),&BTreeMap::new()).unwrap();
        let bridge =
            Bridge::new(configuration, BTreeMap::new(), directory.join("sessions")).unwrap();
        let sink = Recorder::default();
        bridge
            .handle(Event::web("output", "query"), &sink)
            .await
            .unwrap();
        let actual: Vec<_> = sink
            .0
            .lock()
            .await
            .iter()
            .filter(|event| event["type"] == "tool_result" || event["type"] == "tool_output")
            .cloned()
            .collect();
        if actual != expected[index] {
            mismatches.push(format!(
                "selectors={selectors}: source={} output events, Rust={}",
                expected[index].len(),
                actual.len()
            ));
        }
        bridge.shutdown().await.unwrap();
    }
    assert!(mismatches.is_empty(), "{}", mismatches.join("\n"));
}
