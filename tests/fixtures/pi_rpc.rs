use serde_json::{Value, json};
use std::{
    io::{self, BufRead, Write},
    path::{Path, PathBuf},
};
fn send(value: Value) {
    let mut out = io::stdout().lock();
    writeln!(out, "{value}").unwrap();
    out.flush().unwrap();
}
fn response(request: &Value, data: Value) {
    send(
        json!({"type":"response","id":request["id"],"command":request["type"],"success":true,"data":data}),
    );
}
fn failure(request: &Value, message: &str) {
    send(
        json!({"type":"response","id":request["id"],"command":request["type"],"success":false,"error":message}),
    );
}
fn record(path: &Path, message: Value) {
    let mut file = std::fs::OpenOptions::new().append(true).open(path).unwrap();
    writeln!(file,"{}",json!({"type":"message","id":uuid::Uuid::new_v4(),"parentId":null,"timestamp":"2026-10-13T20:00:00Z","message":message})).unwrap();
}
fn reply(path: &Path, text: &str, settle: bool) {
    let message = json!({"role":"assistant","content":[{"type":"thinking","thinking":"native thought"},{"type":"text","text":text}],"stopReason":"stop"});
    send(json!({"type":"message_start","message":{"role":"assistant"}}));
    send(
        json!({"type":"message_update","assistantMessageEvent":{"type":"thinking_delta","contentIndex":0,"delta":"native thought"}}),
    );
    send(
        json!({"type":"message_update","assistantMessageEvent":{"type":"text_delta","contentIndex":1,"delta":text}}),
    );
    send(json!({"type":"message_end","message":message}));
    record(path, message);
    send(json!({"type":"turn_end"}));
    send(json!({"type":"agent_end"}));
    if settle {
        send(json!({"type":"agent_settled"}));
    }
}
pub fn run() {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    assert!(args.windows(2).any(|args| args == ["--mode", "rpc"]));
    let cwd = std::env::current_dir().unwrap();
    let path = if let Some(pair) = args.windows(2).find(|args| args[0] == "--session") {
        PathBuf::from(&pair[1])
    } else {
        let root = std::env::var_os("PI_CODING_AGENT_SESSION_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|| {
                PathBuf::from(std::env::var("PI_CODING_AGENT_DIR").unwrap()).join("sessions")
            });
        let directory = root.join("fixture-project");
        std::fs::create_dir_all(&directory).unwrap();
        let id = uuid::Uuid::new_v4();
        let path = directory.join(format!("2026-10-13_{id}.jsonl"));
        std::fs::write(&path,format!("{}\n",json!({"type":"session","id":id,"cwd":cwd,"version":3,"timestamp":"2026-10-13T20:00:00Z"}))).unwrap();
        path
    };
    let id: Value = serde_json::from_str(
        std::fs::read_to_string(&path)
            .unwrap()
            .lines()
            .next()
            .unwrap(),
    )
    .unwrap();
    let id = id["id"].clone();
    let mut audit = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(std::env::var("PI_FIXTURE_AUDIT").unwrap())
        .unwrap();
    writeln!(
        audit,
        "{}",
        json!({"type":"spawn","args":args,"sessionId":id,"path":path,"cwd":cwd})
    )
    .unwrap();
    audit.flush().unwrap();
    if let Ok(path) = std::env::var("PI_FIXTURE_PID") {
        std::fs::write(path, std::process::id().to_string()).unwrap();
    }
    let mut streaming = false;
    let mut thinking = "off".to_owned();
    let mut model = "reasoning".to_owned();
    let mut pending_dialog = false;
    let mut compact = None::<Value>;
    for line in io::stdin().lock().lines() {
        let request: Value = serde_json::from_str(&line.unwrap()).unwrap();
        writeln!(audit, "{request}").unwrap();
        audit.flush().unwrap();
        match request["type"].as_str().unwrap() {
            "get_state" => response(
                &request,
                json!({"sessionId":id,"sessionFile":path,"isStreaming":streaming,"isCompacting":compact.is_some(),"pendingMessageCount":0,"thinkingLevel":thinking,"autoCompactionEnabled":true,"model":{"provider":"fixture","id":model,"name":model,"contextWindow":10000}}),
            ),
            "get_available_models" => response(
                &request,
                json!({"models":[{"provider":"fixture","id":"reasoning","name":"Reasoning","reasoning":true,"contextWindow":10000},{"provider":"fixture","id":"plain","name":"Plain","reasoning":false,"contextWindow":10000}]}),
            ),
            "get_available_thinking_levels" => response(
                &request,
                json!({"levels":if model=="reasoning"{vec!["off","low","high"]}else{vec!["off"]}}),
            ),
            "get_session_stats" => response(
                &request,
                json!({"totalMessages":3,"cost":0.1,"contextUsage":{"tokens":123,"contextWindow":10000}}),
            ),
            "get_commands" => response(
                &request,
                json!({"commands":[{"name":"fixture-extension","description":"Extension"},{"name":"login","description":"Login"}]}),
            ),
            "get_messages" => {
                let messages = std::fs::read_to_string(&path)
                    .unwrap()
                    .lines()
                    .filter_map(|line| {
                        let value: Value = serde_json::from_str(line).ok()?;
                        if value["type"] == "message" {
                            Some(value["message"].clone())
                        } else {
                            None
                        }
                    })
                    .collect::<Vec<_>>();
                response(&request, json!({"messages":messages}));
            }
            "set_model" => {
                if std::env::var_os("PI_FIXTURE_UNRESPONSIVE_SET_MODEL").is_some() {
                    continue;
                }
                assert_eq!(request["provider"], "fixture");
                model = request["modelId"].as_str().unwrap().into();
                thinking = "off".into();
                response(&request, json!({}));
            }
            "set_thinking_level" => {
                thinking = request["level"].as_str().unwrap().into();
                response(&request, json!({}));
            }
            "prompt" => {
                let prompt = request["message"].as_str().unwrap();
                record(&path, json!({"role":"user","content":prompt}));
                if streaming {
                    assert_eq!(request["streamingBehavior"], "followUp");
                    response(&request, json!({"disposition":"queued"}));
                    reply(&path, "original reply", false);
                    send(json!({"type":"agent_start"}));
                    reply(&path, "followup reply", true);
                    streaming = false;
                    continue;
                }
                if prompt == "handled" {
                    response(&request, json!({"disposition":"handled"}));
                    continue;
                }
                if prompt == "fail-ack" {
                    failure(&request, "Native admission failed");
                    continue;
                }
                send(json!({"type":"agent_start"}));
                streaming = true;
                if prompt != "settled-before-ack" {
                    response(&request, json!({"disposition":"started"}));
                }
                match prompt {
                    "hold" => {
                        send(json!({"type":"agent_end"}));
                    }
                    "crash" => return,
                    "bad-frame" => {
                        println!("malformed after protocol startup");
                        return;
                    }
                    "confirm" | "select" | "input" | "editor" | "expired" => {
                        pending_dialog = true;
                        send(
                            json!({"type":"extension_ui_request","method":if matches!(prompt,"select"|"input"|"editor"){prompt}else{"confirm"},"id":"dialog","title":"Fixture permission","message":"Proceed?","placeholder":"Fixture placeholder","prefill":"Fixture initial text","options":["First","Second"],"timeout":if prompt=="expired"{0}else{10000}}),
                        );
                    }
                    "fail" => {
                        send(
                            json!({"type":"message_end","message":{"role":"assistant","content":[],"stopReason":"error","errorMessage":"Native generation failed"}}),
                        );
                        send(json!({"type":"agent_end"}));
                        send(json!({"type":"agent_settled"}));
                        streaming = false;
                    }
                    "retry" => {
                        send(
                            json!({"type":"message_end","message":{"role":"assistant","content":[],"stopReason":"error","errorMessage":"retrying"}}),
                        );
                        send(json!({"type":"agent_end"}));
                        send(json!({"type":"auto_retry_start"}));
                        send(json!({"type":"agent_start"}));
                        reply(&path, "retried reply", true);
                        streaming = false;
                    }
                    "tool" | "media" | "large" | "unknown-tool" => {
                        let name = if prompt == "unknown-tool" {
                            "future_extension_tool"
                        } else {
                            "write"
                        };
                        let args = json!({"path":"result.txt","content":"fixture"});
                        send(
                            json!({"type":"message_update","assistantMessageEvent":{"type":"toolcall_end","toolCall":{"id":"tool","name":name,"arguments":args}}}),
                        );
                        send(
                            json!({"type":"tool_execution_start","toolCallId":"tool","toolName":name,"args":args}),
                        );
                        send(
                            json!({"type":"tool_execution_update","toolCallId":"tool","partialResult":{"content":[{"type":"text","text":"partial"}]}}),
                        );
                        let mut content = vec![
                            json!({"type":"text","text":if prompt=="large"{"\u{0001}".repeat(300*1024)}else{"tool output".into()}}),
                        ];
                        if prompt == "media" {
                            content.push(json!({"type":"image","mimeType":"image/png","data":"iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAusB9Y9Z8AAAAABJRU5ErkJggg=="}));
                        }
                        send(
                            json!({"type":"tool_execution_end","toolCallId":"tool","toolName":name,"result":{"content":content},"isError":false}),
                        );
                        reply(
                            &path,
                            if prompt == "unknown-tool" {
                                ""
                            } else {
                                "tool reply"
                            },
                            true,
                        );
                        streaming = false;
                    }
                    _ => {
                        reply(&path, "native reply", true);
                        streaming = false;
                        if prompt == "settled-before-ack" {
                            response(&request, json!({"disposition":"started"}));
                        }
                    }
                }
            }
            "clear_queue" => response(&request, json!({})),
            "abort" => {
                streaming = false;
                if let Some(compact) = compact.take() {
                    failure(&compact, "Compaction aborted");
                }
                response(&request, json!({}));
                send(json!({"type":"agent_settled"}));
            }
            "extension_ui_response" => {
                assert!(pending_dialog);
                pending_dialog = false;
                let accepted = request["confirmed"] == true || request["value"] == "Second";
                if streaming {
                    reply(&path, if accepted { "approved" } else { "denied" }, true);
                }
                streaming = false;
            }
            "compact" => {
                if request["customInstructions"] == "hold" {
                    compact = Some(request);
                } else {
                    response(&request, json!({}));
                }
            }
            "set_session_name" => {
                let mut file = std::fs::OpenOptions::new()
                    .append(true)
                    .open(&path)
                    .unwrap();
                writeln!(
                    file,
                    "{}",
                    json!({"type":"session_info","name":request["name"]})
                )
                .unwrap();
                response(&request, json!({}));
            }
            "set_auto_compaction" | "set_steering_mode" | "set_follow_up_mode" => {
                response(&request, json!({}))
            }
            "export_html" => {
                std::fs::write(
                    request["outputPath"].as_str().unwrap(),
                    "<html>Pi fixture</html>",
                )
                .unwrap();
                response(&request, json!({"path":request["outputPath"]}));
            }
            other => panic!("Unsupported/auth native command {other}"),
        }
    }
}
