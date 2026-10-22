mod claude_cli;
mod codex_app_server;
#[path = "../support/live_mcp.rs"]
mod live_mcp;
mod live_mcp_stdio;
mod pi_rpc;
use serde_json::{Value, json};
use std::io::{self, BufRead, Write};

fn send(mut message: Value) {
    message["jsonrpc"] = "2.0".into();
    let mut output = io::stdout().lock();
    writeln!(output, "{message}").unwrap();
    output.flush().unwrap();
}
fn update(session: &str, kind: &str, content: Value) {
    send(
        json!({"jsonrpc":"2.0","method":"session/update","params":{"sessionId":session,"update":{"sessionUpdate":kind,"content":content}}}),
    );
}
fn main() {
    if std::env::args().any(|arg| arg == "--adapter-live-mcp") {
        live_mcp_stdio::run();
        return;
    }
    if std::env::var_os("ACP_PI_FIXTURE").is_some() {
        pi_rpc::run();
        return;
    }
    if std::env::var_os("ACP_CLAUDE_FIXTURE").is_some() {
        claude_cli::run();
        return;
    }
    if std::env::var_os("ACP_CODEX_FIXTURE").is_some() {
        codex_app_server::run();
        return;
    }
    if std::env::args().any(|argument| argument == "--sdk-agent") {
        tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(sdk_agent())
            .unwrap();
        return;
    }
    if std::env::args().any(|argument| argument == "--owned-child") {
        let stop = std::env::var_os("ACP_FIXTURE_CHILD_STOP_FILE");
        loop {
            if stop
                .as_ref()
                .is_some_and(|path| std::path::Path::new(path).exists())
            {
                return;
            }
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
    }
    let owned_child = if let Ok(path) = std::env::var("ACP_FIXTURE_CHILD_PID") {
        let stop = format!("{path}.stop");
        let mut command = std::process::Command::new(std::env::current_exe().unwrap());
        command.arg("--owned-child");
        if std::env::var_os("ACP_FIXTURE_COOPERATIVE_CHILD_REAP_AUDIT").is_some() {
            command.env("ACP_FIXTURE_CHILD_STOP_FILE", &stop);
        }
        let child = command.spawn().unwrap();
        std::fs::write(path, child.id().to_string()).unwrap();
        Some((child, stop))
    } else {
        None
    };
    let mut waiting: Option<Value> = None;
    let mut permission: Option<Value> = None;
    let mut expected_permission_outcome: Option<Value> = None;
    let mut servers = Value::Null;
    let mut config_options = Value::Null;
    for line in io::stdin().lock().lines() {
        let request: Value = serde_json::from_str(&line.unwrap()).unwrap();
        let id = request["id"].clone();
        let params = &request["params"];
        if let Ok(path) = std::env::var("ACP_FIXTURE_RPC_AUDIT") {
            let mut file = std::fs::OpenOptions::new()
                .append(true)
                .create(true)
                .open(path)
                .unwrap();
            writeln!(file, "{request}").unwrap();
        }
        if matches!(
            request["method"].as_str(),
            Some("session/new" | "session/load" | "session/resume")
        ) && let Ok(expected) = std::env::var("ACP_FIXTURE_PROFILE_EXPECT")
        {
            let expected: std::collections::BTreeMap<String, String> =
                serde_json::from_str(&expected).unwrap();
            if expected
                .iter()
                .any(|(name, bytes)| std::fs::read(name).ok().as_deref() != Some(bytes.as_bytes()))
            {
                send(
                    json!({"jsonrpc":"2.0","id":id,"error":{"code":-32000,"message":"Synthetic profile was not applied before session open"}}),
                );
                continue;
            }
        }
        if let Ok(path) = std::env::var("ACP_FIXTURE_MATRIX_AUDIT") {
            let mut file = std::fs::OpenOptions::new()
                .append(true)
                .create(true)
                .open(path)
                .unwrap();
            let record = format!(
                "{}\n",
                json!({"pid":std::process::id(),"method":request["method"],"text":params["prompt"][0]["text"]})
            );
            file.write_all(record.as_bytes()).unwrap();
        }
        let result = match request["method"].as_str().unwrap_or("") {
            "fixture/error" => {
                send(
                    json!({"jsonrpc":"2.0","id":id,"error":{"code":-32042,"message":"Native fixture error","data":{"retained":true}}}),
                );
                continue;
            }
            "fixture/never" => continue,
            "fixture/oversized-frame" => {
                let mut output = io::stdout().lock();
                output.write_all(&vec![b'x'; 2 * 1024 * 1024 + 1]).unwrap();
                output.write_all(b"\n").unwrap();
                output.flush().unwrap();
                continue;
            }
            "fixture/emit" => {
                for message in params["messages"].as_array().into_iter().flatten() {
                    send(message.clone());
                }
                json!({})
            }
            "initialize" => {
                if let Ok(counter) = std::env::var("ACP_FIXTURE_MATRIX_COUNTER")
                    && std::fs::read_to_string(counter).is_ok_and(|value| value == "1")
                    && let Ok(started) = std::env::var("ACP_FIXTURE_RETRY_STARTED_FILE")
                {
                    std::fs::write(started, "started").unwrap();
                    let release = std::env::var("ACP_FIXTURE_RETRY_RELEASE_FILE").unwrap();
                    while !std::path::Path::new(&release).exists() {
                        std::thread::sleep(std::time::Duration::from_millis(1));
                    }
                }
                if let Ok(started) = std::env::var("ACP_FIXTURE_INIT_STARTED_FILE") {
                    std::fs::write(started, "started").unwrap();
                    let release = std::env::var("ACP_FIXTURE_INIT_RELEASE_FILE").unwrap();
                    while !std::path::Path::new(&release).exists() {
                        std::thread::sleep(std::time::Duration::from_millis(1));
                    }
                }
                json!({"protocolVersion":1,"agentCapabilities":{"loadSession":true},"agentInfo":{"name":"offline-fixture"}})
            }
            "session/new" => {
                servers = params["mcpServers"].clone();
                let commands = std::env::var("ACP_FIXTURE_COMMANDS_FILE")
                    .ok()
                    .map(|path| {
                        serde_json::from_str::<Value>(&std::fs::read_to_string(path).unwrap())
                            .unwrap()
                    })
                    .unwrap_or_else(|| json!([{"name":"compact"}]));
                send(
                    json!({"method":"session/update","params":{"sessionId":"fixture","update":{"sessionUpdate":"available_commands_update","availableCommands":commands}}}),
                );
                if let Ok(options) = std::env::var("ACP_FIXTURE_CONFIG_OPTIONS") {
                    config_options = serde_json::from_str::<Value>(&options).unwrap();
                    json!({"sessionId":"fixture","configOptions":config_options})
                } else {
                    json!({"sessionId":"fixture","configOptions":[{"id":"model","category":"model","currentValue":"one","options":[{"options":[{"value":"one"},{"value":"two"}]}]}],"modes":{"currentModeId":"build","availableModes":[{"id":"build"},{"id":"plan"}]}})
                }
            }
            "session/list" => {
                let cwd =
                    std::env::var("ACP_FIXTURE_PROJECT_DIR").unwrap_or_else(|_| "/offline".into());
                json!({"sessions":[{"sessionId":"fixture","cwd":cwd,"title":"Fixture"}]})
            }
            "session/load" | "session/resume" => {
                let history = std::env::var("ACP_FIXTURE_HISTORY_FILE")
                    .map(|path| std::fs::read_to_string(path).unwrap())
                    .unwrap_or_else(|_| "history".into());
                let session = params["sessionId"].as_str().unwrap();
                update(
                    "foreign",
                    "user_message_chunk",
                    json!({"type":"text","text":"foreign"}),
                );
                for _ in 0..if session == "history-long" { 600 } else { 1 } {
                    update(
                        session,
                        "user_message_chunk",
                        json!({"type":"text","text":history}),
                    );
                }
                json!({})
            }
            "session/set_config_option" => {
                if std::env::var("ACP_FIXTURE_REJECT_MODEL")
                    .is_ok_and(|value| params["configId"] == "model" && params["value"] == value)
                {
                    send(
                        json!({"jsonrpc":"2.0","id":id,"error":{"code":-32602,"message":"Unknown native model","data":{"retained":true}}}),
                    );
                    continue;
                }
                if let Some(options) = config_options.as_array_mut() {
                    for option in options {
                        if option["id"] == params["configId"] {
                            option["currentValue"] = params["value"].clone();
                        }
                    }
                    json!({"configOptions":config_options})
                } else {
                    json!({"configOptions":[{"id":params["configId"],"currentValue":params["value"],"options":[{"value":"one"},{"value":"two"}]}]})
                }
            }
            "session/set_mode" => json!({}),
            "session/prompt" => {
                if let Ok(started) = std::env::var("ACP_FIXTURE_PROMPT_STARTED_FILE") {
                    std::fs::write(started, "started").unwrap();
                }
                if let Ok(script) = std::env::var("ACP_FIXTURE_MATRIX_SCRIPT") {
                    let script: Value =
                        serde_json::from_str(&std::fs::read_to_string(script).unwrap()).unwrap();
                    let counter = std::env::var("ACP_FIXTURE_MATRIX_COUNTER").unwrap();
                    let index: usize = std::fs::read_to_string(&counter)
                        .unwrap_or_else(|_| "0".into())
                        .parse()
                        .unwrap();
                    std::fs::write(counter, (index + 1).to_string()).unwrap();
                    let action = &script[index];
                    for entry in action["updates"].as_array().into_iter().flatten() {
                        let mut params = entry.clone();
                        params["sessionId"] =
                            entry["sessionId"].as_str().unwrap_or("fixture").into();
                        send(json!({"method":"session/update","params":params}));
                    }
                    if action["exit"] == true {
                        std::process::exit(0);
                    }
                    if action["hold"] == true {
                        continue;
                    }
                    if action["closeInput"] != true {
                        if let Some(release) = action["releaseFile"].as_str() {
                            std::fs::write(action["startedFile"].as_str().unwrap(), "started")
                                .unwrap();
                            while !std::path::Path::new(release).exists() {
                                std::thread::sleep(std::time::Duration::from_millis(1));
                            }
                        }
                    }
                    if let Some(request) = action.get("request") {
                        if request["method"] == "session/request_permission" {
                            assert!(
                                request["params"]["toolCall"]["toolCallId"]
                                    .as_str()
                                    .is_some_and(|identity| !identity.is_empty())
                            );
                            expected_permission_outcome =
                                action.get("expectedPermissionOutcome").cloned();
                        }
                        permission = Some(id);
                        send(request.clone());
                        continue;
                    }
                    if action["closeInput"] == true {
                        unsafe { libc::close(0) };
                        std::fs::write(action["startedFile"].as_str().unwrap(), "started").unwrap();
                        while !std::path::Path::new(action["releaseFile"].as_str().unwrap())
                            .exists()
                        {
                            std::thread::sleep(std::time::Duration::from_millis(1));
                        }
                    }
                    if action["waitForFollowup"] == true {
                        waiting = Some(id);
                        continue;
                    }
                    if action["error"] == true {
                        send(
                            json!({"jsonrpc":"2.0","id":id,"error":{"code":-32000,"message":"scripted failure"}}),
                        );
                        continue;
                    }
                    if action["tool"] == true {
                        send(
                            json!({"method":"session/update","params":{"sessionId":"fixture","update":{"sessionUpdate":"tool_call","toolCallId":"script-tool","title":"read"}}}),
                        );
                    }
                    if action["image"] == true {
                        update(
                            "fixture",
                            "agent_message_chunk",
                            json!({"type":"image","data":"fixture-image","mimeType":"image/png"}),
                        );
                    }
                    if action["permission"] == true {
                        permission = Some(id);
                        expected_permission_outcome = Some(json!({"outcome":"cancelled"}));
                        send(
                            json!({"jsonrpc":"2.0","id":900,"method":"session/request_permission","params":{"sessionId":"fixture","toolCall":{"toolCallId":"fixture-script-shell","title":"shell"},"options":[{"optionId":"deny-id","kind":"reject_once"}]}}),
                        );
                        continue;
                    }
                    if let Some(text) = action["text"].as_str().filter(|text| !text.is_empty()) {
                        update(
                            "fixture",
                            "agent_message_chunk",
                            json!({"type":"text","text":text}),
                        );
                    }
                    let result = action
                        .get("stopReason")
                        .map(|reason| json!({"stopReason":reason}))
                        .unwrap_or_else(|| json!({}));
                    send(json!({"jsonrpc":"2.0","id":id,"result":result}));
                    if let Some(original) = waiting.take() {
                        send(
                            json!({"jsonrpc":"2.0","id":original,"result":{"stopReason":"end_turn"}}),
                        );
                    }
                    continue;
                }
                let text = params["prompt"][0]["text"].as_str().unwrap_or("");
                match text {
                    "tools" => {
                        for update in [
                            json!({"sessionUpdate":"tool_call","toolCallId":"read-1","title":"read","rawInput":{"filePath":"notes.txt"}}),
                            json!({"sessionUpdate":"tool_call_update","toolCallId":"read-1","title":"notes.txt","status":"completed"}),
                            json!({"sessionUpdate":"tool_call","toolCallId":"grep-1","title":"grep","rawInput":{"pattern":"test"}}),
                            json!({"sessionUpdate":"tool_call_update","toolCallId":"grep-1","title":"Search directory","status":"completed"}),
                        ] {
                            send(
                                json!({"method":"session/update","params":{"sessionId":"fixture","update":update}}),
                            );
                            std::thread::sleep(std::time::Duration::from_millis(80));
                        }
                        update(
                            "fixture",
                            "agent_message_chunk",
                            json!({"type":"text","text":"done"}),
                        );
                    }
                    "mcp" => update(
                        "fixture",
                        "agent_message_chunk",
                        json!({"type":"text","text":servers.to_string()}),
                    ),
                    "media" => {
                        let path = std::env::var("ACP_FIXTURE_MEDIA_PATH").unwrap();
                        update(
                            "fixture",
                            "agent_message_chunk",
                            json!({"type":"text","text":format!("[DOCLIBRARY_IMAGE]{path}[/DOCLIBRARY_IMAGE] [DOCLIBRARY_IMAGE]{path}[/DOCLIBRARY_IMAGE]\nHere is the image.")}),
                        );
                    }
                    "long" => {
                        for _ in 0..600 {
                            update(
                                "fixture",
                                "agent_message_chunk",
                                json!({"type":"text","text":"x"}),
                            );
                        }
                    }
                    "original" => {
                        update(
                            "fixture",
                            "agent_message_chunk",
                            json!({"type":"text","text":"initial-"}),
                        );
                        let id = id.clone();
                        std::thread::spawn(move || {
                            std::thread::sleep(std::time::Duration::from_millis(100));
                            update(
                                "fixture",
                                "agent_message_chunk",
                                json!({"type":"text","text":"original-"}),
                            );
                            send(
                                json!({"jsonrpc":"2.0","id":id,"result":{"stopReason":"end_turn"}}),
                            );
                        });
                        continue;
                    }
                    "delayed-followup" => {
                        let id = id.clone();
                        std::thread::spawn(move || {
                            std::thread::sleep(std::time::Duration::from_millis(800));
                            update(
                                "fixture",
                                "agent_message_chunk",
                                json!({"type":"text","text":"followup"}),
                            );
                            send(
                                json!({"jsonrpc":"2.0","id":id,"result":{"stopReason":"end_turn"}}),
                            );
                        });
                        continue;
                    }
                    "environment" => {
                        let environment: serde_json::Map<String, Value> = [
                            "BRIDGE_TEST_AMBIENT_SECRET",
                            "BRIDGE_TEST_EXPLICIT",
                            "HTTP_PROXY",
                            "NO_PROXY",
                        ]
                        .into_iter()
                        .map(|key| {
                            (
                                key.into(),
                                std::env::var(key)
                                    .ok()
                                    .map(Value::String)
                                    .unwrap_or(Value::Null),
                            )
                        })
                        .collect();
                        update(
                            "fixture",
                            "agent_message_chunk",
                            json!({"type":"text","text":Value::Object(environment).to_string()}),
                        );
                    }
                    "exit" => std::process::exit(7),
                    "error" => {
                        send(
                            json!({"jsonrpc":"2.0","id":id,"error":{"code":-32000,"message":"fixture failure"}}),
                        );
                        continue;
                    }
                    "wait" => {
                        waiting = Some(id);
                        continue;
                    }
                    "permission" => {
                        permission = Some(id);
                        expected_permission_outcome = Some(json!({"outcome":"cancelled"}));
                        send(
                            json!({"jsonrpc":"2.0","id":900,"method":"session/request_permission","params":{"sessionId":"fixture","toolCall":{"toolCallId":"fixture-shell","title":"shell","rawInput":{"command":"must never run"}},"options":[{"optionId":"deny-id","kind":"reject_once"}]}}),
                        );
                        continue;
                    }
                    "followup" => {
                        update(
                            "fixture",
                            "agent_message_chunk",
                            json!({"type":"text","text":"followed"}),
                        );
                        if let Some(first) = waiting.take() {
                            send(
                                json!({"jsonrpc":"2.0","id":first,"result":{"stopReason":"end_turn"}}),
                            );
                        }
                    }
                    _ => {
                        update(
                            "foreign",
                            "agent_message_chunk",
                            json!({"type":"text","text":"not ours"}),
                        );
                        update(
                            "fixture",
                            "agent_thought_chunk",
                            json!({"type":"text","text":"private thought"}),
                        );
                        update(
                            "fixture",
                            "agent_message_chunk",
                            json!({"type":"text","text":"hello "}),
                        );
                        let completion =
                            json!({"jsonrpc":"2.0","id":id,"result":{"stopReason":"end_turn"}});
                        let trailing = json!({"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"fixture","update":{"sessionUpdate":"agent_message_chunk","content":{"type":"text","text":"world"}}}});
                        let mut output = io::stdout().lock();
                        output
                            .write_all(format!("{completion}\n{trailing}\n").as_bytes())
                            .unwrap();
                        output.flush().unwrap();
                        continue;
                    }
                }
                json!({"stopReason":"end_turn"})
            }
            "session/cancel" => {
                if let Some(first) = waiting.take() {
                    send(json!({"jsonrpc":"2.0","id":first,"result":{"stopReason":"cancelled"}}));
                }
                continue;
            }
            "" if id == 900 => {
                if !request["error"].is_null() {
                    assert_eq!(request["error"]["code"], -32601);
                    if let Some(first) = permission.take() {
                        send(
                            json!({"jsonrpc":"2.0","id":first,"result":{"stopReason":"end_turn"}}),
                        );
                    }
                    continue;
                }
                if let Some(expected) = expected_permission_outcome.take() {
                    assert_eq!(
                        request["result"]["outcome"], expected,
                        "exact permission outcome"
                    );
                    if let Ok(path) = std::env::var("ACP_FIXTURE_PERMISSION_RESPONSE") {
                        std::fs::write(path, request.to_string()).unwrap();
                    }
                }
                let rejected = matches!(
                    request["result"]["outcome"]["optionId"].as_str(),
                    Some("deny-id" | "reject" | "foreign-deny")
                ) || request["result"]["outcome"]["outcome"] == "cancelled";
                if std::env::var("ACP_FIXTURE_MATRIX_SCRIPT").is_ok() {
                    assert!(rejected, "permission must be auto-denied");
                } else {
                    update(
                        "fixture",
                        "agent_message_chunk",
                        json!({"type":"text","text":if rejected { "denied safely" } else { "UNSAFE" }}),
                    );
                }
                if let Some(first) = permission.take() {
                    send(json!({"jsonrpc":"2.0","id":first,"result":{"stopReason":"end_turn"}}));
                }
                continue;
            }
            _ => json!({}),
        };
        send(json!({"jsonrpc":"2.0","id":id,"result":result}));
    }
    if std::env::var_os("ACP_FIXTURE_IGNORE_STDIN_EOF").is_some() {
        loop {
            std::thread::sleep(std::time::Duration::from_secs(1));
        }
    }
    if let Some((mut child, stop)) = owned_child
        && let Some(audit) = std::env::var_os("ACP_FIXTURE_COOPERATIVE_CHILD_REAP_AUDIT")
    {
        std::fs::write(stop, "stop").unwrap();
        child.wait().unwrap();
        std::fs::write(audit, "reaped").unwrap();
    }
}

async fn sdk_agent() -> agent_client_protocol::Result<()> {
    use agent_client_protocol::schema::ProtocolVersion;
    use agent_client_protocol::schema::v1::{
        ContentBlock, ContentChunk, InitializeRequest, InitializeResponse, NewSessionRequest,
        NewSessionResponse, PromptRequest, PromptResponse, SessionId, SessionNotification,
        SessionUpdate, StopReason, TextContent,
    };
    use agent_client_protocol::{Agent, Stdio};
    Agent
        .builder()
        .on_receive_request(
            async |_: InitializeRequest, responder, _cx| {
                responder.respond(InitializeResponse::new(ProtocolVersion::V1))
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_request(
            async |_: NewSessionRequest, responder, _cx| {
                responder.respond(NewSessionResponse::new(SessionId::new("sdk-fixture")))
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_request(
            async |request: PromptRequest, responder, cx| {
                cx.send_notification(SessionNotification::new(
                    request.session_id,
                    SessionUpdate::AgentMessageChunk(ContentChunk::new(ContentBlock::Text(
                        TextContent::new("SDK reply"),
                    ))),
                ))?;
                responder.respond(PromptResponse::new(StopReason::EndTurn))
            },
            agent_client_protocol::on_receive_request!(),
        )
        .connect_to(Stdio::new())
        .await
}
