use serde_json::{Value, json};
use std::{
    io::{self, BufRead, Write},
    path::PathBuf,
};
fn send(value: Value) {
    let mut out = io::stdout().lock();
    writeln!(out, "{value}").unwrap();
    out.flush().unwrap();
}
fn control(id: &Value, response: Value) {
    send(
        json!({"type":"control_response","response":{"subtype":"success","request_id":id,"response":response}}),
    );
}
fn record(session: &str, value: Value) {
    let cwd = std::env::current_dir().unwrap();
    let encoded = cwd
        .to_string_lossy()
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '-' })
        .collect::<String>();
    let path = PathBuf::from(std::env::var("CLAUDE_CONFIG_DIR").unwrap())
        .join("projects")
        .join(encoded);
    std::fs::create_dir_all(&path).unwrap();
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path.join(format!("{session}.jsonl")))
        .unwrap();
    let mut value = value;
    value["cwd"] = json!(cwd);
    value["sessionId"] = session.into();
    value["timestamp"] = "2026-10-13T20:00:00Z".into();
    writeln!(file, "{value}").unwrap();
}
fn result(session: &str, uuid: &str, text: &str, error: bool) {
    send(
        json!({"type":"result","subtype":"success","is_error":error,"result":text,"session_id":session,"user_message_uuid":uuid}),
    );
}
fn reply(session: &str, uuid: &str, text: &str) {
    for event in [
        json!({"type":"message_start","message":{"id":uuid}}),
        json!({"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}),
        json!({"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":text}}),
        json!({"type":"content_block_stop","index":0}),
    ] {
        send(
            json!({"type":"stream_event","event":event,"session_id":session,"user_message_uuid":uuid}),
        );
    }
    let message = json!({"type":"assistant","uuid":format!("assistant-{uuid}"),"message":{"id":uuid,"content":[{"type":"text","text":text}]},"parent_tool_use_id":null,"session_id":session,"user_message_uuid":uuid});
    send(message.clone());
    record(session, message);
    result(session, uuid, text, false);
}
pub fn run() {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    assert!(args.iter().any(|arg| arg == "--print"));
    assert!(args.iter().any(|arg| arg == "stdio"));
    assert!(!args.iter().any(|arg| arg.contains("skip-permissions")));
    assert!(
        args.windows(2)
            .any(|pair| pair == ["--permission-mode", "manual"])
    );
    let session = args
        .windows(2)
        .find(|pair| matches!(pair[0].as_str(), "--session-id" | "--resume"))
        .unwrap()[1]
        .clone();
    uuid::Uuid::parse_str(&session).unwrap();
    let audit = std::env::var("CLAUDE_FIXTURE_AUDIT").unwrap();
    let mut audit = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(audit)
        .unwrap();
    writeln!(audit, "{}", json!({"type":"spawn","args":args})).unwrap();
    if let Ok(path) = std::env::var("CLAUDE_FIXTURE_PID") {
        std::fs::write(path, std::process::id().to_string()).unwrap();
    }
    let mut active = String::new();
    let mut pending = String::new();
    for line in io::stdin().lock().lines() {
        let message: Value = serde_json::from_str(&line.unwrap()).unwrap();
        writeln!(audit, "{message}").unwrap();
        audit.flush().unwrap();
        match message["type"].as_str().unwrap() {
            "control_request" => {
                let id = &message["request_id"];
                match message["request"]["subtype"].as_str().unwrap() {
                    "initialize" => control(
                        id,
                        json!({"models":[{"value":"default","displayName":"Default","description":"Fixture","supportedEffortLevels":["low","high"]},{"value":"haiku","displayName":"Haiku","description":"Fixture"}],"commands":[{"name":"compact","description":"Compact context"}],"current_permission_mode":"manual"}),
                    ),
                    "set_model" | "set_permission_mode" | "apply_flag_settings" => {
                        control(id, json!({}))
                    }
                    "interrupt" => {
                        assert_eq!(message["request"]["cancel_queued"], true);
                        control(id, json!({"cancelled":[],"still_queued":[]}));
                        if !active.is_empty() {
                            result(&session, &active, "", false);
                            active.clear();
                        }
                    }
                    other => panic!("Unsupported/auth control {other}"),
                }
            }
            "user" => {
                active = message["uuid"].as_str().unwrap().into();
                record(&session, message.clone());
                let prompt = message["message"]["content"][0]["text"]
                    .as_str()
                    .unwrap_or("image");
                match prompt {
                    "hold" => {}
                    "crash" => return,
                    "fail" => {
                        result(&session, &active, "Native fixture failure", true);
                        active.clear();
                    }
                    "approve" | "cancel-ask" => {
                        let tool = json!({"type":"tool_use","id":"tool","name":"Bash","input":{"command":"echo approved"}});
                        send(
                            json!({"type":"assistant","uuid":"tool-message","session_id":session,"message":{"id":"tool-message","content":[tool]}}),
                        );
                        pending = active.clone();
                        send(
                            json!({"type":"control_request","request_id":"permission","request":{"subtype":"can_use_tool","tool_name":"Bash","tool_use_id":"tool","input":{"command":"echo approved"},"permission_suggestions":[{"type":"addRules","destination":"userSettings"}]}}),
                        );
                        if prompt == "cancel-ask" {
                            send(
                                json!({"type":"control_cancel_request","request_id":"permission"}),
                            );
                        }
                    }
                    "tool" | "media" | "large" => {
                        let tool = json!({"type":"tool_use","id":"tool","name":"Write","input":{"file_path":"result.txt","content":"fixture"}});
                        send(
                            json!({"type":"stream_event","session_id":session,"event":{"type":"message_start","message":{"id":"tool-stream"}}}),
                        );
                        send(
                            json!({"type":"stream_event","session_id":session,"event":{"type":"content_block_start","index":0,"content_block":{"type":"tool_use","id":"tool","name":"Write","input":{}}}}),
                        );
                        send(
                            json!({"type":"stream_event","session_id":session,"event":{"type":"content_block_delta","index":0,"delta":{"type":"input_json_delta","partial_json":tool["input"].to_string()}}}),
                        );
                        send(
                            json!({"type":"stream_event","session_id":session,"event":{"type":"content_block_stop","index":0}}),
                        );
                        send(
                            json!({"type":"assistant","uuid":"tool-message","session_id":session,"message":{"id":"tool-stream","content":[tool]}}),
                        );
                        let mut blocks = vec![
                            json!({"type":"text","text":if prompt=="large"{"\u{0001}".repeat(300*1024)}else{"native tool output".into()}}),
                        ];
                        if prompt == "media" {
                            blocks.push(json!({"type":"image","source":{"type":"base64","media_type":"image/png","data":"iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAusB9Y9Z8AAAAABJRU5ErkJggg=="}}));
                        }
                        send(
                            json!({"type":"user","session_id":session,"message":{"content":[{"type":"tool_result","tool_use_id":"tool","content":blocks}]}}),
                        );
                        reply(&session, &active, "tool reply");
                        active.clear();
                    }
                    "stale" => {
                        send(
                            json!({"type":"result","session_id":session,"subtype":"success","is_error":false,"result":"stale","user_message_uuid":"old"}),
                        );
                        reply(&session, &active, "fresh");
                        active.clear();
                    }
                    "/compact" => {
                        send(
                            json!({"type":"system","subtype":"local_command_output","content":"Context compacted","session_id":session}),
                        );
                        result(&session, &active, "Context compacted", false);
                        active.clear();
                    }
                    _ => {
                        reply(&session, &active, "native reply");
                        active.clear();
                    }
                }
            }
            "control_response" => {
                assert!(!pending.is_empty());
                let response = &message["response"]["response"];
                assert!(response.get("updatedPermissions").is_none());
                let granted = response["behavior"] == "allow";
                if granted {
                    assert_eq!(response["updatedInput"]["command"], "echo approved");
                }
                send(
                    json!({"type":"user","session_id":session,"message":{"content":[{"type":"tool_result","tool_use_id":"tool","content":if granted{"approved"}else{"denied"}}]}}),
                );
                reply(
                    &session,
                    &pending,
                    if granted { "approved" } else { "denied" },
                );
                pending.clear();
                active.clear();
            }
            other => panic!("Unexpected {other}"),
        }
    }
}
