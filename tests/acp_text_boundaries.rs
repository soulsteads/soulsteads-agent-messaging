use soulsteads_messaging::acp::{Client, Options};
use serde_json::{Value, json};
use std::{collections::BTreeMap, time::Duration};

fn text(text: &str) -> Value {
    json!({"update":{"sessionUpdate":"agent_message_chunk","content":{"type":"text","text":text}}})
}
fn tool(kind: &str, id: &str) -> Value {
    json!({"update":{"sessionUpdate":kind,"toolCallId":id,"title":"read","status":"completed"}})
}
async fn client(root: &std::path::Path, actions: Value) -> Client {
    let script = root.join("script.json");
    std::fs::write(&script, actions.to_string()).unwrap();
    let mut env = BTreeMap::new();
    env.insert(
        "ACP_FIXTURE_MATRIX_SCRIPT".into(),
        script.display().to_string(),
    );
    env.insert(
        "ACP_FIXTURE_MATRIX_COUNTER".into(),
        root.join("counter").display().to_string(),
    );
    let client = Client::connect(Options {
        command: env!("CARGO_BIN_EXE_acp-fixture").into(),
        args: vec![],
        cwd: root.into(),
        env,
        inherit_env: false,
        timeout: Duration::from_secs(3),
    })
    .await
    .unwrap();
    client.new_session().await.unwrap();
    client
}

#[tokio::test]
async fn tool_boundary_separates_paragraphs_in_final_and_streamed_text() {
    let root = tempfile::tempdir().unwrap();
    let client = client(
        root.path(),
        json!([{"updates":[
            text("safe."), tool("tool_call", "one"), tool("tool_call_update", "one"),
            text("The "), text("continuation.")
        ]}]),
    )
    .await;
    let mut events = client.subscribe();
    let response = client.prompt("probe", vec![]).await.unwrap();
    let mut shown = String::new();
    while let Ok(update) = events.try_recv() {
        if update["sessionUpdate"] == "agent_message_chunk" {
            shown.push_str(update["content"]["text"].as_str().unwrap());
        }
    }
    client.shutdown().await.unwrap();
    assert_eq!(response.text, "safe.\n\nThe continuation.");
    assert_eq!(shown, response.text);
    assert_eq!(
        soulsteads_messaging::tools::markdown(&response.text),
        "<p>safe.</p>\n<p>The continuation.</p>\n"
    );
    assert_eq!(
        response.output.raw_chars,
        "safe.The continuation.".encode_utf16().count()
    );
    assert_eq!(response.output.chunks, 3);
    assert_eq!(response.output.tool_calls, 1);
}

#[tokio::test]
async fn normal_token_chunks_and_late_tool_updates_do_not_split_words_or_code() {
    let root = tempfile::tempdir().unwrap();
    let client = client(
        root.path(),
        json!([{"updates":[
            text("before"), tool("tool_call", "one"), text("```rust\nlet con"),
            tool("tool_call_update", "one"), text("tinuation = true;\n```"),
            tool("tool_call", "one"), text("\nend")
        ]}]),
    )
    .await;
    let response = client.prompt("probe", vec![]).await.unwrap();
    client.shutdown().await.unwrap();
    assert_eq!(
        response.text,
        "before\n\n```rust\nlet continuation = true;\n```\nend"
    );
}

#[tokio::test]
async fn existing_whitespace_is_preserved_without_duplicate_paragraph_breaks() {
    for (before, whitespace, after, expected) in [
        ("safe.", "", "\n\nThe", "safe.\n\nThe"),
        ("safe.\n", "", "\nThe", "safe.\n\nThe"),
        ("safe.", "\n\n", "The", "safe.\n\nThe"),
        ("safe.", "\n", "The", "safe.\n\nThe"),
        ("safe.", " ", "The", "safe. \n\nThe"),
        ("safe.", "", "The", "safe.\n\nThe"),
    ] {
        let root = tempfile::tempdir().unwrap();
        let client = client(
            root.path(),
            json!([{"updates":[
                text(before), tool("tool_call", "one"), text(""), text(whitespace), text(after)
            ]}]),
        )
        .await;
        let response = client.prompt("probe", vec![]).await.unwrap();
        client.shutdown().await.unwrap();
        assert_eq!(
            response.text, expected,
            "before={before:?} whitespace={whitespace:?} after={after:?}"
        );
    }
}

#[tokio::test]
async fn pending_boundary_resets_per_prompt_and_tools_before_text_add_no_prefix() {
    let root = tempfile::tempdir().unwrap();
    let client = client(
        root.path(),
        json!([
            {"updates":[text("first"), tool("tool_call", "one")]},
            {"updates":[tool("tool_call", "two"), text("sec"), text("ond")]}
        ]),
    )
    .await;
    let first = client.prompt("one", vec![]).await.unwrap();
    let second = client.prompt("two", vec![]).await.unwrap();
    client.shutdown().await.unwrap();
    assert_eq!(first.text, "first");
    assert_eq!(second.text, "second");
}
