use serde_json::{Value, json};
use std::{
    collections::HashMap,
    io::{self, BufRead, Write},
};

fn send(message: Value) {
    // This fixture follows Codex's versionless JSON-RPC wire format.
    let mut output = io::stdout().lock();
    writeln!(output, "{message}").unwrap();
    output.flush().unwrap();
}
fn notification(method: &str, params: Value) {
    send(json!({"method":method,"params":params}));
}
fn finish(thread: &str, turn: &str, message: &str, status: &str) {
    notification(
        "item/agentMessage/delta",
        json!({"threadId":thread,"turnId":turn,"itemId":"message","delta":message}),
    );
    notification(
        "item/completed",
        json!({"threadId":thread,"turnId":turn,"item":{"type":"agentMessage","id":"message","text":message}}),
    );
    notification(
        "turn/completed",
        json!({"threadId":thread,"turn":{"id":turn,"status":status,"items":[],"error":null}}),
    );
}
pub fn run() {
    assert_eq!(
        std::env::args().skip(1).collect::<Vec<_>>(),
        ["app-server", "--listen", "stdio://"]
    );
    if let Ok(path) = std::env::var("CODEX_FIXTURE_PID") {
        std::fs::write(path, std::process::id().to_string()).unwrap();
    }
    let mut threads = HashMap::<String, Value>::new();
    let mut current = HashMap::<String, String>::new();
    let mut approvals = HashMap::<String, (String, String)>::new();
    let mut serial = 0;
    for line in io::stdin().lock().lines() {
        let request: Value = serde_json::from_str(&line.unwrap()).unwrap();
        assert!(
            request.get("jsonrpc").is_none(),
            "Codex does not require the version field"
        );
        if let Ok(path) = std::env::var("CODEX_FIXTURE_AUDIT") {
            let mut file = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(path)
                .unwrap();
            writeln!(file, "{request}").unwrap();
        }
        let params = &request["params"];
        let id = request["id"].clone();
        if let Some((thread, turn)) = approvals.remove(&id.to_string()) {
            assert!(
                matches!(
                    request["result"]["decision"].as_str(),
                    Some("accept" | "decline" | "cancel")
                ),
                "Never grant session-wide approval"
            );
            notification(
                "item/completed",
                json!({"threadId":thread,"turnId":turn,"item":{"type":"commandExecution","id":"tool","command":"echo approved","status":"completed","exitCode":0,"aggregatedOutput":request["result"]["decision"]}}),
            );
            finish(
                &thread,
                &turn,
                if request["result"]["decision"] == "accept" {
                    "approved"
                } else {
                    "denied"
                },
                "completed",
            );
            current.remove(&thread);
            continue;
        }
        let method = request["method"].as_str().unwrap_or("");
        assert!(
            !method.starts_with("account/"),
            "Adapter must not invoke an authentication flow"
        );
        let result = match method {
            "initialize" => {
                json!({"userAgent":"fake-codex","platformFamily":"unix","platformOs":"fixture"})
            }
            "initialized" | "$/cancel_request" => continue,
            "model/list" => {
                json!({"data":[{"id":"model-id","model":"fake-model","displayName":"Fake model","description":"fixture","hidden":false,"isDefault":true,"supportedReasoningEfforts":[{"reasoningEffort":"low","description":"low"},{"reasoningEffort":"high","description":"high"}],"defaultReasoningEffort":"low"},{"id":"other-id","model":"other-model","displayName":"Other model","isDefault":false,"hidden":false,"supportedReasoningEfforts":[],"defaultReasoningEffort":"low"}],"nextCursor":null})
            }
            "thread/start" => {
                serial += 1;
                let thread_id = format!("native-thread-{serial}");
                let thread = json!({"id":thread_id,"cwd":params["cwd"],"preview":"Fixture conversation","name":null,"updatedAt":1791933600,"status":{"type":"idle"},"turns":[]});
                threads.insert(thread_id, thread.clone());
                assert_eq!(params["approvalPolicy"], "on-request");
                assert_eq!(params["sandbox"], "workspace-write");
                json!({"thread":thread,"model":"fake-model","modelProvider":"fixture","cwd":params["cwd"],"reasoningEffort":"low","approvalPolicy":"on-request","sandbox":{"type":"workspaceWrite"}})
            }
            "thread/read" => {
                json!({"thread":threads.get(params["threadId"].as_str().unwrap()).cloned().unwrap_or_else(||json!({"id":params["threadId"],"cwd":std::env::current_dir().unwrap(),"preview":"Fixture history","updatedAt":1791933600,"turns":[]}))})
            }
            "thread/resume" => {
                let thread=threads.get(params["threadId"].as_str().unwrap()).cloned().unwrap_or_else(||json!({"id":params["threadId"],"cwd":params["cwd"],"preview":"Fixture history","updatedAt":1791933600,"turns":[]}));
                json!({"thread":thread,"model":"fake-model","modelProvider":"fixture","cwd":params["cwd"],"reasoningEffort":"low"})
            }
            "thread/list" => {
                json!({"data":threads.values().cloned().collect::<Vec<_>>(),"nextCursor":null})
            }
            "thread/items/list" => {
                if std::env::var("CODEX_FIXTURE_HISTORY").as_deref() == Ok("cycle") {
                    json!({"data":[],"nextCursor":"repeat"})
                } else if params["cursor"].is_null() {
                    json!({"data":[{"turnId":"old","item":{"type":"userMessage","id":"user","content":[{"type":"text","text":"old question"}]}},{"turnId":"old","item":{"type":"agentMessage","id":"answer","text":"old answer"}}],"nextCursor":"second"})
                } else {
                    json!({"data":[{"turnId":"old","item":{"type":"commandExecution","id":"old-tool","command":"ls","status":"completed","exitCode":0,"aggregatedOutput":"old output"}}],"nextCursor":null})
                }
            }
            "turn/start" => {
                let thread = params["threadId"].as_str().unwrap();
                serial += 1;
                let turn = format!("turn-{serial}");
                current.insert(thread.into(), turn.clone());
                assert!(params["input"].as_array().is_some());
                if params["input"][0]["text"] == "timeout" {
                    notification(
                        "turn/started",
                        json!({"threadId":thread,"turn":{"id":turn,"status":"inProgress"}}),
                    );
                    continue;
                }
                send(
                    json!({"id":id,"result":{"turn":{"id":turn,"status":"inProgress","items":[],"error":null}}}),
                );
                notification(
                    "turn/started",
                    json!({"threadId":thread,"turn":{"id":turn,"status":"inProgress"}}),
                );
                match params["input"][0]["text"].as_str().unwrap_or("image") {
                    "future-tool" => {
                        notification(
                            "item/completed",
                            json!({"threadId":thread,"turnId":turn,"item":{"type":"futureNativeTool","id":"future-tool","status":"completed"}}),
                        );
                        finish(thread, &turn, "", "completed");
                        current.remove(thread);
                    }
                    "hold" => {}
                    "fail" => {
                        notification(
                            "turn/completed",
                            json!({"threadId":thread,"turn":{"id":turn,"status":"failed","error":{"message":"Native fixture failed"}}}),
                        );
                        current.remove(thread);
                    }
                    "crash" => return,
                    "approve" => {
                        notification(
                            "item/started",
                            json!({"threadId":thread,"turnId":turn,"item":{"type":"commandExecution","id":"tool","command":"echo approved","status":"inProgress"}}),
                        );
                        approvals.insert("900".into(), (thread.into(), turn.clone()));
                        send(
                            json!({"method":"item/commandExecution/requestApproval","id":900,"params":{"threadId":thread,"turnId":turn,"itemId":"tool","command":"echo approved","cwd":params["cwd"],"reason":"Fixture approval","availableDecisions":["accept","acceptForSession","decline","cancel"]}}),
                        );
                    }
                    "media" => {
                        let item = json!({"type":"mcpToolCall","id":"media-tool","server":"fixture","tool":"capture","arguments":{},"status":"inProgress"});
                        notification(
                            "item/started",
                            json!({"threadId":thread,"turnId":turn,"item":item}),
                        );
                        notification(
                            "item/completed",
                            json!({"threadId":thread,"turnId":turn,"item":{"type":"mcpToolCall","id":"media-tool","server":"fixture","tool":"capture","status":"completed","result":{"content":[{"type":"image","mimeType":"image/png","data":"iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAusB9Y9Z8AAAAABJRU5ErkJggg=="},{"type":"text","text":"native image"}]}}}),
                        );
                        finish(thread, &turn, "media result", "completed");
                        current.remove(thread);
                    }
                    "stdout" => {
                        let output = "\u{0001}".repeat(300 * 1024);
                        notification(
                            "item/started",
                            json!({"threadId":thread,"turnId":turn,"item":{"type":"commandExecution","id":"stdout","command":"printf","status":"inProgress"}}),
                        );
                        notification(
                            "item/commandExecution/outputDelta",
                            json!({"threadId":thread,"turnId":turn,"itemId":"stdout","delta":output}),
                        );
                        notification(
                            "item/completed",
                            json!({"threadId":thread,"turnId":turn,"item":{"type":"commandExecution","id":"stdout","command":"printf","status":"completed","exitCode":0,"aggregatedOutput":output}}),
                        );
                        finish(thread, &turn, "large output", "completed");
                        current.remove(thread);
                    }
                    "stale" => {
                        notification(
                            "item/agentMessage/delta",
                            json!({"threadId":thread,"turnId":"old-turn","itemId":"stale","delta":"stale"}),
                        );
                        notification(
                            "turn/completed",
                            json!({"threadId":thread,"turn":{"id":"old-turn","status":"completed"}}),
                        );
                        finish(thread, &turn, "fresh", "completed");
                        current.remove(thread);
                    }
                    _ => {
                        finish(thread, &turn, "native reply", "completed");
                        current.remove(thread);
                    }
                }
                continue;
            }
            "turn/steer" => {
                let thread = params["threadId"].as_str().unwrap();
                let turn = current.get(thread).unwrap().clone();
                assert_eq!(params["expectedTurnId"], turn);
                finish(thread, &turn, "steered reply", "completed");
                current.remove(thread);
                json!({"turnId":turn})
            }
            "turn/interrupt" => {
                let thread = params["threadId"].as_str().unwrap();
                let turn = current.remove(thread).unwrap();
                assert_eq!(params["turnId"], turn);
                send(json!({"id":id,"result":{}}));
                notification(
                    "turn/completed",
                    json!({"threadId":thread,"turn":{"id":turn,"status":"interrupted","items":[],"error":null}}),
                );
                continue;
            }
            "thread/compact/start" => {
                send(json!({"id":id,"result":{}}));
                let turn = "native-compact";
                notification(
                    "turn/started",
                    json!({"threadId":params["threadId"],"turn":{"id":turn,"status":"inProgress"}}),
                );
                notification(
                    "item/started",
                    json!({"threadId":params["threadId"],"turnId":turn,"item":{"type":"contextCompaction","id":"compact"}}),
                );
                notification(
                    "item/completed",
                    json!({"threadId":params["threadId"],"turnId":turn,"item":{"type":"contextCompaction","id":"compact","status":"completed"}}),
                );
                // Exercise the gap that exists in the real app-server: the
                // completed item does not release the native turn yet.
                if let Ok(path) = std::env::var("CODEX_FIXTURE_COMPACT_MARKER") {
                    std::fs::write(path, "item completed; turn still active").unwrap();
                }
                std::thread::sleep(std::time::Duration::from_millis(250));
                notification(
                    "turn/completed",
                    json!({"threadId":params["threadId"],"turn":{"id":turn,"status":"completed","items":[],"error":null}}),
                );
                continue;
            }
            "thread/unsubscribe" => json!({"status":"unsubscribed"}),
            "thread/delete" => {
                threads.remove(params["threadId"].as_str().unwrap());
                json!({})
            }
            _ => {
                send(
                    json!({"id":id,"error":{"code":-32601,"message":format!("Unknown native method {method}")}}),
                );
                continue;
            }
        };
        send(json!({"id":id,"result":result}));
    }
}
