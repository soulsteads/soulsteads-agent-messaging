//! Opt-in qualification against the actual installed native CLI protocols.
use agent_client_protocol::schema::v1;
use anyhow::{Context, Result, ensure};
use serde_json::{Value, json};
use std::{collections::BTreeMap, path::Path, time::Duration};
#[path = "support/acp_peer.rs"]
mod acp_peer;
#[path = "support/live_agent.rs"]
mod live_agent;
use live_agent::{Kind, Live};
#[path = "support/live_mcp.rs"]
mod live_mcp;
#[path = "support/live_mcp_http.rs"]
mod live_mcp_http;
#[path = "support/pi_provider.rs"]
mod pi_provider;

fn calls(path: &Path) -> Vec<Value> {
    std::fs::read_to_string(path)
        .unwrap_or_default()
        .lines()
        .filter_map(|line| serde_json::from_str(line).ok())
        .collect()
}
fn contains_image(value: &Value) -> bool {
    match value {
        Value::Object(fields) => {
            fields.get("type").is_some_and(|kind| kind == "image")
                || fields.values().any(contains_image)
        }
        Value::Array(values) => values.iter().any(contains_image),
        _ => false,
    }
}
async fn coding(kind: Kind) -> Result<()> {
    let root = tempfile::tempdir()?;
    let audit = root.path().join("mcp-stdio.jsonl");
    let http_audit = root.path().join("mcp-http.jsonl");
    let http = live_mcp_http::Http::start(http_audit.clone()).await?;
    std::fs::write(root.path().join("probe.txt"), "all-adapters-live-probe\n")?;
    let mut servers = vec![
        json!({"name":"live_stdio","command":env!("CARGO_BIN_EXE_acp-fixture"),"args":["--adapter-live-mcp"],"env":[{"name":"ADAPTER_LIVE_MCP_AUDIT","value":audit},{"name":"ADAPTER_LIVE_LITERAL","value":"!literal-${HOME}"}]}),
        json!({"type":"http","name":"live_http","url":format!("{}/mcp",http.url),"headers":[{"name":"X-Adapter-Probe","value":"owned"},{"name":"X-Adapter-Literal","value":"!literal-${HOME}"}]}),
    ];
    if matches!(kind, Kind::Claude | Kind::OpenCode | Kind::Pi) {
        servers.push(
            json!({"type":"sse","name":"live_sse","url":format!("{}/sse",http.url),"headers":[]}),
        );
    }
    let credentials = if matches!(kind, Kind::Pi) {
        Some(pi_cloud_environment(root.path())?)
    } else {
        None
    };
    let mut live = Live::start_env(
        kind,
        root.path(),
        servers,
        credentials
            .as_ref()
            .map(|copy| copy.env.clone())
            .unwrap_or_default(),
    )
    .await?;
    let result=async {
        let start=live.events.len();
        let prompt=format!("This is an owned adapter integration test. Read probe.txt and copy its exact bytes to result.txt in this directory. Preserve the trailing line feed: the JSON-escaped expected content is \"all-adapters-live-probe\\n\". Call probe_echo from the MCP server live_stdio with value STDIO_LIVE_OK, call probe_echo from live_http with value HTTP_LIVE_OK, {}and call probe_image from live_stdio once. Use the supplied MCP tools directly, not shell substitutes. Do not modify other files. Finally reply exactly ADAPTER_LIVE_OK.",if matches!(kind,Kind::Claude|Kind::OpenCode|Kind::Pi){"call probe_echo from live_sse with value SSE_LIVE_OK, "}else{""});
        let response=live.prompt(&prompt).await?;live.drain();
        ensure!(response["stopReason"]=="end_turn","Unexpected stop: {response}");
        let copied=std::fs::read_to_string(root.path().join("result.txt"))?;
        ensure!(copied=="all-adapters-live-probe\n","Wrong copied bytes: {copied:?}");
        ensure!(live.text(start).contains("ADAPTER_LIVE_OK"),"Missing final reply: {}",live.text(start));
        let stdio=calls(&audit);let remote=calls(&http_audit);
        ensure!(stdio.iter().any(|call|call["method"]=="tools/call"&&call["params"]["name"]=="probe_echo"),"stdio MCP not exercised");
        ensure!(stdio.iter().any(|call|call["method"]=="tools/call"&&call["params"]["name"]=="probe_image"),"image MCP not exercised");
        ensure!(remote.iter().any(|call|call["transport"]=="http"&&call["method"]=="tools/call"&&call["probeHeader"]=="owned"),"HTTP MCP/header not exercised");
        ensure!(stdio.iter().any(|call|call["literalEnv"]=="!literal-${HOME}"),"ACP environment value changed");
        ensure!(remote.iter().any(|call|call["literalHeader"]=="!literal-${HOME}"),"ACP header value changed");
        if matches!(kind,Kind::Claude|Kind::OpenCode|Kind::Pi){ensure!(remote.iter().any(|call|call["transport"]=="sse"&&call["method"]=="tools/call"),"SSE MCP not exercised");}
        ensure!(live.events[start..].iter().any(|event|event["update"]["sessionUpdate"]=="tool_call"),"No streamed tool activity");
        ensure!(live.events[start..].iter().any(contains_image),"No projected MCP image");
        println!("LIVE {} file/MCP/tool image complete; checking prompt image",kind.name());
        let vision_start=live.events.len();
        live.request("session/prompt",json!({"sessionId":live.id,"prompt":[{"type":"text","text":"Name the dominant color in this attached image and include IMAGE_INPUT_OK. Do not call tools."},{"type":"image","mimeType":"image/png","data":live_mcp::png()}]})).await?;live.drain();
        ensure!(live.text(vision_start).to_lowercase().contains("red")&&live.text(vision_start).contains("IMAGE_INPUT_OK"),"Image input not understood: {}",live.text(vision_start));
        let listed=live.request("session/list",json!({"cwd":root.path()})).await?;ensure!(listed["sessions"].as_array().context("Missing session list")?.iter().any(|entry|entry["sessionId"]==live.id),"Owned session not listed");
        let history_start=live.events.len();live.request("session/load",json!({"sessionId":live.id,"cwd":root.path(),"mcpServers":[]})).await?;live.drain();
        ensure!(live.text(history_start).contains("ADAPTER_LIVE_OK"),"Owned history did not replay");
        println!("LIVE {} coding PASS (text/tool/image, stdio/HTTP{} MCP, model/mode/effort, list/history; {} scoped approvals)",kind.name(),if matches!(kind,Kind::Claude|Kind::OpenCode|Kind::Pi){"/SSE"}else{""},live.permissions.len());Ok::<_,anyhow::Error>(())
    }.await;
    let cleanup = live.cleanup().await;
    if let Some(credentials) = credentials {
        ensure!(
            std::fs::read(credentials.source)? == credentials.original,
            "Source native credentials changed during owned-copy qualification"
        );
    }
    result?;
    cleanup
}
#[tokio::test]
#[ignore = "real authenticated OpenCode provider generation and owned MCP tools"]
async fn live_opencode_coding_media_mcp_controls_and_history() {
    coding(Kind::OpenCode).await.unwrap();
}

struct CloudCopy {
    env: BTreeMap<String, String>,
    source: std::path::PathBuf,
    original: Vec<u8>,
}
fn pi_cloud_environment(root: &Path) -> Result<CloudCopy> {
    use std::io::Write;
    let command = std::env::var("PI_ADAPTER_PROBE_PATH")
        .context("Set PI_ADAPTER_PROBE_PATH to the installed Pi CLI")?;
    let source = std::env::var_os("PI_ADAPTER_COPILOT_AUTH_SOURCE")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| {
            std::path::PathBuf::from(std::env::var_os("HOME").unwrap())
                .join(".local/share/opencode/auth.json")
        });
    let original = std::fs::read(&source)?;
    let auth: Value = serde_json::from_slice(&original)?;
    let credential = &auth["github-copilot"];
    ensure!(
        credential["type"] == "oauth"
            && credential["access"].is_string()
            && credential["refresh"].is_string()
            && credential["expires"].is_number(),
        "OpenCode Copilot credentials unavailable"
    );
    let agent = root.join("owned-pi-agent");
    std::fs::create_dir(&agent)?;
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    // OpenCode stores the GitHub token in both access and refresh. Pi's native
    // API-key path accepts that token at the public inference endpoint; its
    // OAuth path expects a separate, expiring Copilot token instead.
    let mode = std::env::var("PI_ADAPTER_COPILOT_AUTH_MODE").unwrap_or_else(|_| "api_key".into());
    ensure!(
        matches!(mode.as_str(), "api_key" | "oauth"),
        "Unsupported Pi cloud probe authentication mode"
    );
    let direct = mode == "api_key";
    let native_credential = if direct {
        json!({"type":"api_key","key":credential["refresh"]})
    } else {
        credential.clone()
    };
    options.open(agent.join("auth.json"))?.write_all(
        serde_json::to_string(&json!({"github-copilot":native_credential}))?.as_bytes(),
    )?;
    if direct {
        std::fs::write(
            agent.join("models.json"),
            json!({"providers":{"github-copilot":{"baseUrl":"https://api.githubcopilot.com"}}})
                .to_string(),
        )?;
    }
    std::fs::write(agent.join("settings.json"),json!({"defaultProvider":"github-copilot","defaultModel":"gpt-5-mini","defaultThinkingLevel":"off","packages":[],"autoUpdate":"off"}).to_string())?;
    Ok(CloudCopy {
        env: BTreeMap::from([
            ("PI_PATH".into(), command),
            (
                "PI_CODING_AGENT_DIR".into(),
                agent.to_string_lossy().into_owned(),
            ),
            ("PI_CODING_AGENT_SESSION_DIR".into(), String::new()),
        ]),
        source,
        original,
    })
}
#[tokio::test]
#[ignore = "real Pi cloud generation using a private owned copy of native Copilot credentials"]
async fn live_pi_cloud_coding_media_mcp_controls_and_history() {
    coding(Kind::Pi).await.unwrap();
}
#[tokio::test]
#[ignore = "real authenticated Codex provider generation and owned MCP tools"]
async fn live_codex_coding_media_mcp_controls_and_history() {
    coding(Kind::Codex).await.unwrap();
}
#[tokio::test]
#[ignore = "real authenticated Claude provider generation and owned MCP tools"]
async fn live_claude_coding_media_mcp_controls_and_history() {
    coding(Kind::Claude).await.unwrap();
}

async fn lifecycle(kind: Kind) -> Result<()> {
    let root = tempfile::tempdir()?;
    let mut live = Live::start(kind, root.path(), vec![]).await?;
    let result = async {
        let first = live.events.len();
        let original = live.background("Run exactly this owned Bash test in the current directory: echo ready > overlap.ready; sleep 6. Then reply ORIGINAL_LIVE_OK. Do not modify other files.");
        live.wait_file(&root.path().join("overlap.ready")).await?;
        let followup = live.prompt("Additional instruction: after the running command completes, include FOLLOWUP_LIVE_OK in your reply. Do not run more tools.").await;
        if matches!(kind, Kind::Claude | Kind::OpenCode) {
            ensure!(followup.as_ref().is_err_and(|error|error.to_string().contains("busy") || error.to_string().contains("active ACP prompt")), "Native adapter did not reject overlap: {followup:?}");
        } else {
            ensure!(followup?["stopReason"] == "end_turn", "Follow-up not accepted");
        }
        ensure!(live.finish(original).await?["stopReason"] == "end_turn", "Original failed after overlap");
        ensure!(live.text(first).contains("ORIGINAL_LIVE_OK"), "Original collector lost output: {}", live.text(first));
        if matches!(kind, Kind::Codex) { ensure!(live.text(first).contains("FOLLOWUP_LIVE_OK"), "Follow-up output lost: {}", live.text(first)); }
        let instructions=if matches!(kind,Kind::Codex){"Use exec_command with yield_time_ms=30000 and no background execution. "}else{""};
        let pending = live.background(&format!("{instructions}Run exactly this owned Bash test: echo $$ > cancel.pid; echo ready > cancel.ready; sleep 45; echo finished > cancel.done. Do not start it in the background. Wait for completion before replying CANCEL_SHOULD_NOT_COMPLETE."));
        live.wait_file(&root.path().join("cancel.ready")).await?;
        live.peer.connection.send_notification(v1::CancelNotification::new(live.id.clone()))?;
        ensure!(live.finish(pending).await?["stopReason"] == "cancelled", "Cancellation was not reported");
        ensure!(!root.path().join("cancel.done").exists(), "Cancelled command ran to completion");
        #[cfg(unix)] live_agent::wait_shell_exit(root.path()).await?;
        let recovery = live.events.len();
        ensure!(live.prompt("Reply exactly RECOVERY_LIVE_OK without using tools.").await?["stopReason"] == "end_turn", "Post-cancel prompt failed");
        ensure!(live.text(recovery).contains("RECOVERY_LIVE_OK"), "No post-cancel output");
        {
            ensure!(live.prompt("/compact").await?["stopReason"] == "end_turn", "Compaction failed");
            let after = live.events.len();
            live.prompt("Reply exactly COMPACTION_RECOVERY_OK without using tools.").await?;
            live.drain();
            ensure!(live.text(after).contains("COMPACTION_RECOVERY_OK"), "No post-compaction output: {}", live.text(after));
        }
        println!("LIVE {} lifecycle PASS (overlap, cancellation, recovery, compaction)", kind.name());
        Ok::<_,anyhow::Error>(())
    }.await;
    if let Err(error) = result {
        let _ = live.cleanup().await;
        return Err(error);
    }
    let id = live.id.clone();
    live.peer.shutdown().await?;
    let mut resumed = Live::connect(kind, root.path(), BTreeMap::new()).await?;
    resumed.id = id;
    let result = async {
        resumed
            .request(
                "session/load",
                json!({"sessionId":resumed.id,"cwd":root.path(),"mcpServers":[]}),
            )
            .await?;
        resumed.drain();
        ensure!(
            resumed.text(0).contains("RECOVERY_LIVE_OK"),
            "History did not survive fresh process"
        );
        if matches!(kind, Kind::OpenCode)
            && let Ok(model) = std::env::var("OPENCODE_ADAPTER_LIVE_MODEL")
        {
            resumed
                .request(
                    "session/set_config_option",
                    json!({"sessionId":resumed.id,"configId":"model","value":model}),
                )
                .await?;
        }
        let start = resumed.events.len();
        resumed
            .prompt("Reply exactly FRESH_PROCESS_OK without using tools.")
            .await?;
        resumed.drain();
        ensure!(
            resumed.text(start).contains("FRESH_PROCESS_OK"),
            "Resumed native turn failed"
        );
        println!("LIVE {} fresh-process resume PASS", kind.name());
        Ok::<_, anyhow::Error>(())
    }
    .await;
    let cleanup = resumed.cleanup().await;
    result?;
    cleanup
}
#[tokio::test]
#[ignore = "real authenticated Codex lifecycle: owned tools, steering, cancel, compact, fresh-process load"]
async fn live_codex_lifecycle() {
    lifecycle(Kind::Codex).await.unwrap();
}
#[tokio::test]
#[ignore = "real authenticated Claude lifecycle: overlap rejection, cancel, compact, fresh-process load"]
async fn live_claude_lifecycle() {
    lifecycle(Kind::Claude).await.unwrap();
}
#[tokio::test]
#[ignore = "real authenticated OpenCode lifecycle: overlap rejection, cancel, compact, fresh-process load"]
async fn live_opencode_lifecycle() {
    lifecycle(Kind::OpenCode).await.unwrap();
}

#[tokio::test]
#[ignore = "PI_ADAPTER_PROBE_PATH: installed Pi with owned local provider, MCP, extension approvals and lifecycle"]
async fn live_pi_full_matrix() {
    pi_matrix().await.unwrap();
}
async fn pi_matrix() -> Result<()> {
    let root = tempfile::tempdir()?;
    let agent = root.path().join("agent");
    let mut provider = pi_provider::Provider::start(false).await?;
    provider.configure(&agent)?;
    let mut settings: Value =
        serde_json::from_str(&std::fs::read_to_string(agent.join("settings.json"))?)?;
    settings["compaction"] = json!({"enabled":false,"keepRecentTokens":128,"reserveTokens":256});
    std::fs::write(agent.join("settings.json"), settings.to_string())?;
    let command = std::env::var("PI_ADAPTER_PROBE_PATH")
        .context("Set PI_ADAPTER_PROBE_PATH to the installed Pi CLI")?;
    let env = BTreeMap::from([
        ("PI_PATH".into(), command.clone()),
        (
            "PI_CODING_AGENT_DIR".into(),
            agent.to_string_lossy().into_owned(),
        ),
        ("PI_CODING_AGENT_SESSION_DIR".into(), String::new()),
    ]);
    let audit = root.path().join("mcp-stdio.jsonl");
    let http_audit = root.path().join("mcp-http.jsonl");
    let http = live_mcp_http::Http::start(http_audit.clone()).await?;
    std::fs::write(root.path().join("probe.txt"), "all-adapters-live-probe\n")?;
    let servers = vec![
        json!({"name":"live_stdio","command":env!("CARGO_BIN_EXE_acp-fixture"),"args":["--adapter-live-mcp"],"env":[{"name":"ADAPTER_LIVE_MCP_AUDIT","value":audit},{"name":"ADAPTER_LIVE_LITERAL","value":"!literal-${HOME}"}]}),
        json!({"type":"http","name":"live_http","url":format!("{}/mcp",http.url),"headers":[{"name":"X-Adapter-Probe","value":"owned"},{"name":"X-Adapter-Literal","value":"!literal-${HOME}"}]}),
        json!({"type":"sse","name":"live_sse","url":format!("{}/sse",http.url),"headers":[{"name":"X-Adapter-Probe","value":"owned"}]}),
    ];
    let native = std::fs::canonicalize(&command)?;
    let ai = native
        .ancestors()
        .map(|parent| parent.join("pi-ai/dist/index.js"))
        .find(|path| path.exists())
        .context("Cannot locate the installed Pi TypeBox export")?;
    std::fs::create_dir_all(agent.join("extensions"))?;
    let extension = r#"
import { Type } from __PI_AI__;
import fs from "node:fs";
import path from "node:path";
export default function(pi) {
  pi.registerCommand("owned-command",{description:"Owned native extension command",handler:async(args,ctx)=>ctx.ui.notify("OWNED_COMMAND_OK "+args,"info")});
  pi.registerTool({name:"owned_gate",label:"Owned approval probe",description:"Owned live adapter dialog probe",
    parameters:Type.Object({kind:Type.String(),marker:Type.String()}),
    async execute(_id,params,signal,_update,ctx) {
      let allowed;
      if(params.kind==="input" || params.kind==="editor") {
        const value=params.kind==="input" ? await ctx.ui.input("Owned input probe","Enter owned text") : await ctx.ui.editor("Owned editor probe","Initial owned text");
        allowed=value==="owned input\nsecond line";
      } else allowed=params.kind==="select" ? await ctx.ui.select("Owned select probe",["Grant once","Deny"])==="Grant once" : await ctx.ui.confirm("Owned confirm probe","Allow this owned marker write once?");
      if(!allowed || signal?.aborted) return {content:[{type:"text",text:"OWNED_DENIED"}],details:{allowed:false}};
      fs.writeFileSync(path.join(ctx.cwd,params.marker),"once");
      return {content:[{type:"text",text:"OWNED_GRANTED"}],details:{allowed:true}};
    }
  });
}
"#.replace("__PI_AI__", &serde_json::to_string(&ai)?);
    std::fs::write(agent.join("extensions/owned-gate.ts"), extension)?;
    let mut models: Value =
        serde_json::from_str(&std::fs::read_to_string(agent.join("models.json"))?)?;
    let mut vision = models["providers"]["owned-local"]["models"][0].clone();
    vision["id"] = json!("fixture-vision");
    vision["input"] = json!(["text", "image"]);
    vision["reasoning"] = json!(true);
    models["providers"]["owned-local"]["models"]
        .as_array_mut()
        .unwrap()
        .push(vision);
    std::fs::write(agent.join("models.json"), models.to_string())?;
    let mut live = Live::start_env(Kind::Pi, root.path(), servers, env.clone()).await?;
    let result = async {
        live.request("session/set_config_option",json!({"sessionId":live.id,"configId":"model","value":"owned-local/fixture-vision"})).await?;
        live.request("session/set_mode",json!({"sessionId":live.id,"modeId":"low"})).await?;
        live.prompt("PI_MATRIX_MCP").await?; live.drain();
        let copied=std::fs::read_to_string(root.path().join("result.txt"))?;
        ensure!(copied=="all-adapters-live-probe\n", "Pi file copy failed: {copied:?}; output: {}", live.text(0));
        ensure!(live.text(0).contains("ADAPTER_LIVE_OK"),"Pi reply missing");
        ensure!(calls(&audit).iter().any(|call|call["params"]["name"]=="probe_echo"),"Pi stdio MCP not called");
        ensure!(calls(&audit).iter().any(|call|call["params"]["name"]=="probe_image"),"Pi image tool not called");
        ensure!(calls(&http_audit).iter().any(|call|call["method"]=="tools/call"&&call["probeHeader"]=="owned"),"Pi HTTP MCP/header not called");
        ensure!(calls(&http_audit).iter().any(|call|call["transport"]=="sse"&&call["method"]=="tools/call"),"Pi ACP SSE MCP not called");
        ensure!(calls(&audit).iter().any(|call|call["literalEnv"]=="!literal-${HOME}"),"Pi changed literal MCP env");
        ensure!(calls(&http_audit).iter().any(|call|call["literalHeader"]=="!literal-${HOME}"),"Pi changed literal MCP header");
        ensure!(live.events.iter().any(contains_image),"Pi tool image not projected");
        let owned_id=live.id.clone();
        for (command,expected) in [("/name owned-live-qualification","Session named owned-live-qualification"),("/session",owned_id.as_str()),("/autocompact off","Automatic compaction off"),("/steering one-at-a-time","steering delivery: one-at-a-time"),("/follow-up one-at-a-time","follow-up delivery: one-at-a-time"),("/owned-command marker","OWNED_COMMAND_OK marker")] {
            let expected=expected.to_owned();let start=live.events.len();live.prompt(command).await?;live.drain();
            ensure!(live.text(start).contains(&expected),"Pi command {command} failed: {}",live.text(start));
        }
        live.prompt("/export").await?;
        let exported=std::fs::read_dir(root.path())?.filter_map(Result::ok).map(|entry|entry.path()).find(|path|path.file_name().is_some_and(|name|name.to_string_lossy().starts_with("pi-session-"))&&path.extension().is_some_and(|extension|extension=="html")).context("Pi native export did not create workspace HTML")?;
        let html=std::fs::read_to_string(exported)?;
        let encoded=html.split_once("<script id=\"session-data\" type=\"application/json\">").and_then(|(_,tail)|tail.split_once("</script>").map(|(data,_)|data.trim())).context("Pi export omitted its session-data payload")?;
        use base64::Engine;
        let data=base64::engine::general_purpose::STANDARD.decode(encoded)?;
        ensure!(std::str::from_utf8(&data)?.contains("ADAPTER_LIVE_OK"),"Pi export lost native history");
        live.request("session/prompt",json!({"sessionId":live.id,"prompt":[{"type":"text","text":"PI_MATRIX_IMAGE"},{"type":"image","mimeType":"image/png","data":live_mcp::png()}]})).await?;
        let mut requests=vec![]; while let Ok(request)=provider.requests.try_recv(){requests.push(request);}
        ensure!(requests.iter().any(|request|request["model"]=="fixture-vision"),"Pi model setter did not reach provider");
        ensure!(requests.iter().any(|request|request["reasoning_effort"]=="low"),"Pi thinking setter did not reach provider");
        ensure!(requests.iter().any(|request|request["messages"].to_string().contains("image_url")),"Pi image prompt did not reach provider");
        for kind in ["CONFIRM","SELECT"] {
            let before=live.permissions.len(); live.prompt(&format!("PI_MATRIX_{kind}")).await?;
            ensure!(live.permissions.len()>before,"Pi {kind} approval missing");
            ensure!(root.path().join("granted.marker").exists(),"Pi {kind} approval not applied");
            std::fs::remove_file(root.path().join("granted.marker"))?;
        }
        for kind in ["INPUT","EDITOR"] {
            let before=live.dialogs.len();live.prompt(&format!("PI_MATRIX_{kind}")).await?;
            ensure!(live.dialogs.len()>before,"Pi {kind} dialog missing");
            ensure!(root.path().join("granted.marker").exists(),"Pi {kind} value not returned to native extension");
            std::fs::remove_file(root.path().join("granted.marker"))?;
        }
        live.reject=true; let before=live.permissions.len(); live.prompt("PI_MATRIX_DENY").await?; live.reject=false;
        ensure!(live.permissions.len()>before,"Pi denial did not ask");
        ensure!(!root.path().join("denied.marker").exists(),"Pi rejected permission granted");
        let pending=live.background("PI_MATRIX_LATE");
        let (permission,responder)=tokio::time::timeout(Duration::from_secs(15),live.peer.permissions.recv()).await?.context("No Pi pending dialog")?;
        live.peer.connection.send_notification(v1::CancelNotification::new(live.id.clone()))?;
        ensure!(live.finish(pending).await?["stopReason"]=="cancelled","Pi approval cancellation failed");
        let allow=permission.options.iter().find(|option|option.kind==v1::PermissionOptionKind::AllowOnce).context("No scoped Pi approval")?;
        let _=responder.respond(v1::RequestPermissionResponse::new(v1::RequestPermissionOutcome::Selected(v1::SelectedPermissionOutcome::new(allow.option_id.clone()))));
        live.prompt("PI_MATRIX_RECOVERY").await?;
        ensure!(!root.path().join("late.marker").exists(),"Late Pi approval granted");
        let first=live.events.len(); let original=live.background("PI_MATRIX_OVERLAP");
        live.wait_file(&root.path().join("overlap.ready")).await?;
        live.prompt("PI_MATRIX_FOLLOWUP").await?; live.finish(original).await?;
        ensure!(live.text(first).contains("ORIGINAL_LIVE_OK")&&live.text(first).contains("FOLLOWUP_LIVE_OK"),"Pi follow-up collector lost output: {}",live.text(first));
        let pending=live.background("PI_MATRIX_CANCEL"); live.wait_file(&root.path().join("cancel.ready")).await?;
        live.peer.connection.send_notification(v1::CancelNotification::new(live.id.clone()))?;
        ensure!(live.finish(pending).await?["stopReason"]=="cancelled","Pi Bash cancellation failed");
        ensure!(!root.path().join("cancel.done").exists(),"Pi cancelled Bash completed");
        #[cfg(unix)] live_agent::wait_shell_exit(root.path()).await?;
        live.prompt("PI_MATRIX_RECOVERY").await?;
        live.prompt("/compact").await?;
        live.prompt("PI_MATRIX_RECOVERY").await?;
        println!("LIVE pi matrix PASS (native Read/Write, ACP stdio/HTTP/SSE MCP, tool image/input image, model/thinking, confirm/select/input/editor/deny/late approval, follow-up, Bash cancel, recovery, compact)");
        Ok::<_,anyhow::Error>(())
    }.await;
    if let Err(error) = result {
        let _ = live.cleanup().await;
        return Err(error);
    }
    let id = live.id.clone();
    live.peer.shutdown().await?;
    let mut resumed = Live::connect(Kind::Pi, root.path(), env).await?;
    resumed.id = id;
    let result = async {
        let listed = resumed
            .request("session/list", json!({"cwd":root.path()}))
            .await?;
        ensure!(
            listed["sessions"]
                .as_array()
                .context("Pi session list missing")?
                .iter()
                .any(|entry| entry["sessionId"] == resumed.id),
            "Pi owned history not listed"
        );
        resumed
            .request(
                "session/load",
                json!({"sessionId":resumed.id,"cwd":root.path(),"mcpServers":[]}),
            )
            .await?;
        resumed.drain();
        ensure!(
            resumed.text(0).contains("RECOVERY_LIVE_OK"),
            "Pi native history lost"
        );
        let start = resumed.events.len();
        resumed.prompt("PI_MATRIX_FRESH").await?;
        resumed.drain();
        ensure!(
            resumed.text(start).contains("FRESH_PROCESS_OK"),
            "Pi resumed turn failed"
        );
        println!("LIVE pi fresh-process resume/list PASS");
        Ok::<_, anyhow::Error>(())
    }
    .await;
    let cleanup = resumed.cleanup().await;
    result?;
    cleanup
}

async fn approvals(kind: Kind) -> Result<()> {
    let root = tempfile::tempdir()?;
    if matches!(kind, Kind::OpenCode) {
        std::fs::write(
            root.path().join("opencode.json"),
            json!({"permission":{"*":"ask"}}).to_string(),
        )?;
    }
    let servers = if matches!(kind, Kind::Claude | Kind::OpenCode) {
        vec![
            json!({"name":"live_gate","command":env!("CARGO_BIN_EXE_acp-fixture"),"args":["--adapter-live-mcp"],"env":[{"name":"ADAPTER_LIVE_MCP_AUDIT","value":root.path().join("gate-audit.jsonl")} ]}),
        ]
    } else {
        vec![]
    };
    let mut live = Live::start(kind, root.path(), servers).await?;
    let result=async {
        if matches!(kind,Kind::Codex) {
            live.request("session/set_mode",json!({"sessionId":live.id,"modeId":"read-only"})).await?;
        }
        let instruction=if matches!(kind,Kind::Codex) {
            "Use the native shell execution tool with sandbox_permissions=require_escalated and a justification asking for approval to write this owned test file. Run exactly printf once > denied.marker. Do not use apply_patch, alternatives or retries. If rejected, reply DENIAL_HANDLED."
        } else {
            "Call probe_write from live_gate with marker denied.marker exactly once. This is an owned approval test. Do not use alternatives or retry. If rejected, reply DENIAL_HANDLED."
        };
        let before=live.permissions.len();live.reject=true;live.prompt(instruction).await?;live.reject=false;
        ensure!(live.permissions.len()>before,"Native {} never requested denial approval; output: {}",kind.name(),live.text(0));
        ensure!(!root.path().join("denied.marker").exists(),"Rejected native operation executed");
        let instruction=if matches!(kind,Kind::Codex) {
            "Use the native shell execution tool with sandbox_permissions=require_escalated and a justification asking for approval to write this owned test file. Run exactly printf once > late.marker. Do not use apply_patch, alternatives or retries."
        } else {
            "Call probe_write from live_gate with marker late.marker exactly once. This is an owned approval test. Do not use alternatives or retry."
        };
        let pending=live.background(instruction);
        let (permission,responder)=tokio::time::timeout(Duration::from_secs(90),async {
            loop { tokio::select! {
                event=live.peer.updates.recv()=>live.events.push(serde_json::to_value(event.context("ACP notifications closed")?)?),
                permission=live.peer.permissions.recv()=>{
                    let (permission,responder)=permission.context("ACP permissions closed")?;
                    if serde_json::to_string(&permission)?.contains("late.marker") { break Ok::<_,anyhow::Error>((permission,responder)); }
                    responder.respond(v1::RequestPermissionResponse::new(v1::RequestPermissionOutcome::Cancelled))?;
                }
            }}
        }).await.context("No pending approval for the owned late marker")??;
        live.peer.connection.send_notification(v1::CancelNotification::new(live.id.clone()))?;
        ensure!(live.finish(pending).await?["stopReason"]=="cancelled","Native pending approval cancellation failed");
        let option=permission.options.iter().find(|option|option.kind==v1::PermissionOptionKind::AllowOnce).context("No scoped approval option")?;
        let _=responder.respond(v1::RequestPermissionResponse::new(v1::RequestPermissionOutcome::Selected(v1::SelectedPermissionOutcome::new(option.option_id.clone()))));
        let start=live.events.len();live.prompt("Reply exactly APPROVAL_RECOVERY_OK without tools.").await?;live.drain();
        ensure!(live.text(start).contains("APPROVAL_RECOVERY_OK"),"No prompt recovery after approval cancellation");
        ensure!(!root.path().join("late.marker").exists(),"Late native approval executed after cancellation");
        println!("LIVE {} approval denial / cancellation / late answer / recovery PASS",kind.name());
        Ok::<_,anyhow::Error>(())
    }.await;
    let cleanup = live.cleanup().await;
    result?;
    cleanup
}
#[tokio::test]
#[ignore = "real authenticated Codex read-only escalation approval denial and cancellation"]
async fn live_codex_approval_denial_and_late_answer() {
    approvals(Kind::Codex).await.unwrap();
}
#[tokio::test]
#[ignore = "real authenticated Claude MCP mutation approval denial and cancellation"]
async fn live_claude_approval_denial_and_late_answer() {
    approvals(Kind::Claude).await.unwrap();
}

#[tokio::test]
#[ignore = "real authenticated OpenCode with owned project ask permissions and MCP mutation"]
async fn live_opencode_approval_denial_and_late_answer() {
    approvals(Kind::OpenCode).await.unwrap();
}
