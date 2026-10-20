//! Installed adapter peer, event collection and scoped permission handling.
use super::acp_peer;
use agent_client_protocol::{
    UntypedMessage,
    schema::{ProtocolVersion, v1},
};
use anyhow::{Context, Result, ensure};
use serde_json::{Value, json};
use std::{collections::BTreeMap, path::Path, time::Duration};

#[derive(Clone, Copy, Debug)]
pub(super) enum Kind {
    OpenCode,
    Codex,
    Claude,
    Pi,
}
impl Kind {
    pub(super) fn name(self) -> &'static str {
        match self {
            Self::OpenCode => "opencode",
            Self::Codex => "codex",
            Self::Claude => "claude",
            Self::Pi => "pi",
        }
    }
    fn binary(self) -> &'static str {
        match self {
            Self::OpenCode => env!("CARGO_BIN_EXE_opencode-acp-rust"),
            Self::Codex => env!("CARGO_BIN_EXE_codex-acp-rust"),
            Self::Claude => env!("CARGO_BIN_EXE_claude-acp-rust"),
            Self::Pi => env!("CARGO_BIN_EXE_pi-acp-rust"),
        }
    }
}
pub(super) struct Live {
    pub(super) peer: acp_peer::Peer,
    pub(super) id: String,
    pub(super) events: Vec<Value>,
    pub(super) permissions: Vec<Value>,
    pub(super) reject: bool,
    pub(super) dialogs: Vec<Value>,
}
impl Live {
    pub(super) async fn start(kind: Kind, root: &Path, mcp: Vec<Value>) -> Result<Self> {
        Self::start_env(kind, root, mcp, BTreeMap::new()).await
    }
    pub(super) async fn connect(
        kind: Kind,
        root: &Path,
        env: BTreeMap<String, String>,
    ) -> Result<Self> {
        let peer = acp_peer::Peer::spawn(kind.binary(), root, env).await?;
        let mut initialize = serde_json::to_value(v1::InitializeRequest::new(ProtocolVersion::V1))?;
        if matches!(kind, Kind::Pi) {
            initialize["_meta"] = json!({"pi/extensionUi":true});
        }
        let initialized = peer
            .connection
            .send_request(UntypedMessage::new("initialize", initialize)?)
            .block_task()
            .await?;
        ensure!(
            initialized["protocolVersion"] == 1,
            "Unexpected ACP version"
        );
        Ok(Self {
            peer,
            id: String::new(),
            events: vec![],
            permissions: vec![],
            reject: false,
            dialogs: vec![],
        })
    }
    pub(super) async fn start_env(
        kind: Kind,
        root: &Path,
        mcp: Vec<Value>,
        env: BTreeMap<String, String>,
    ) -> Result<Self> {
        let mut live = Self::connect(kind, root, env).await?;
        let created = live
            .request("session/new", json!({"cwd":root,"mcpServers":mcp}))
            .await?;
        live.id = created["sessionId"]
            .as_str()
            .context("Missing native session ID")?
            .into();
        let options = created["configOptions"]
            .as_array()
            .cloned()
            .unwrap_or_default();
        println!(
            "LIVE {} catalog {}",
            kind.name(),
            json!({"modes":created["modes"],"options":options.iter().map(|option|json!({"id":option["id"],"currentValue":option["currentValue"],"choiceCount":option["options"].as_array().map(Vec::len)})).collect::<Vec<_>>()})
        );
        // Exercise model setter using the active model; preserve the selected provider.
        if let Some(model) = options.iter().find(|option| option["id"] == "model")
            && let Some(current) = model["currentValue"].as_str()
        {
            let selected = if matches!(kind, Kind::OpenCode) {
                std::env::var("OPENCODE_ADAPTER_LIVE_MODEL").unwrap_or_else(|_| current.into())
            } else {
                current.into()
            };
            live.request(
                "session/set_config_option",
                json!({"sessionId":live.id,"configId":"model","value":selected}),
            )
            .await?;
        }
        // Exercise both directions of a safe mode transition before coding.
        if let Some(modes) = created["modes"]["availableModes"].as_array()
            && let Some(current) = created["modes"]["currentModeId"].as_str()
        {
            if let Some(alternate) = modes
                .iter()
                .filter_map(|mode| mode["id"].as_str())
                .find(|id| *id != current && matches!(*id, "plan" | "read-only"))
            {
                live.request(
                    "session/set_mode",
                    json!({"sessionId":live.id,"modeId":alternate}),
                )
                .await?;
            }
            live.request(
                "session/set_mode",
                json!({"sessionId":live.id,"modeId":current}),
            )
            .await?;
        } else if let Some(mode) = options.iter().find(|option| option["id"] == "mode")
            && let Some(current) = mode["currentValue"].as_str()
        {
            if mode["options"]
                .as_array()
                .is_some_and(|options| options.iter().any(|option| option["value"] == "plan"))
            {
                live.request(
                    "session/set_config_option",
                    json!({"sessionId":live.id,"configId":"mode","value":"plan"}),
                )
                .await?;
            }
            live.request(
                "session/set_config_option",
                json!({"sessionId":live.id,"configId":"mode","value":current}),
            )
            .await?;
        }
        if let Some(effort) = options
            .iter()
            .find(|option| option["category"] == "thought_level")
            && let (Some(id), Some(current)) =
                (effort["id"].as_str(), effort["currentValue"].as_str())
        {
            let selected = effort["options"]
                .as_array()
                .and_then(|options| options.iter().find(|option| option["value"] == "low"))
                .and_then(|option| option["value"].as_str())
                .unwrap_or(current);
            live.request(
                "session/set_config_option",
                json!({"sessionId":live.id,"configId":id,"value":selected}),
            )
            .await?;
        }
        Ok(live)
    }
    async fn observe(&mut self) -> Result<()> {
        tokio::select! {
            dialog=self.peer.extension_ui.recv()=> {
                let (request,responder)=dialog.context("Pi dialog channel closed")?;
                self.dialogs.push(request);
                responder.respond(if self.reject {json!({"cancelled":true})}else{json!({"value":"owned input\nsecond line"})})?;
            },
            event = self.peer.updates.recv() => self.events.push(serde_json::to_value(event.context("ACP notification channel closed")?)?),
            permission = self.peer.permissions.recv() => {
                let (permission,responder) = permission.context("ACP permission channel closed")?;
                self.permissions.push(serde_json::to_value(&permission)?);
                let preferred = if self.reject { v1::PermissionOptionKind::RejectOnce } else { v1::PermissionOptionKind::AllowOnce };
                let outcome = if let Some(option) = permission.options.iter().find(|option|option.kind == preferred) {
                    v1::RequestPermissionOutcome::Selected(v1::SelectedPermissionOutcome::new(option.option_id.clone()))
                } else { v1::RequestPermissionOutcome::Cancelled };
                responder.respond(v1::RequestPermissionResponse::new(outcome))?;
            },
            _ = tokio::time::sleep(Duration::from_millis(20)) => {},
        }
        Ok(())
    }
    pub(super) async fn request(&mut self, method: &str, params: Value) -> Result<Value> {
        let connection = self.peer.connection.clone();
        let future = connection
            .send_request(UntypedMessage::new(method, params)?)
            .block_task();
        tokio::pin!(future);
        tokio::time::timeout(Duration::from_secs(180), async {
            loop {
                tokio::select! {
                    result = &mut future => break result.map_err(Into::into),
                    observed = self.observe() => observed?,
                }
            }
        })
        .await
        .context("Live request exceeded 180 seconds")?
    }
    pub(super) fn background(&self, text: &str) -> tokio::task::JoinHandle<Result<Value>> {
        let connection = self.peer.connection.clone();
        let params = json!({"sessionId":self.id,"prompt":[{"type":"text","text":text}]});
        tokio::spawn(async move {
            Ok(connection
                .send_request(UntypedMessage::new("session/prompt", params)?)
                .block_task()
                .await?)
        })
    }
    pub(super) async fn finish(
        &mut self,
        mut task: tokio::task::JoinHandle<Result<Value>>,
    ) -> Result<Value> {
        let result = tokio::time::timeout(Duration::from_secs(60), async {
            loop {
                tokio::select! {
                    result = &mut task => break result?,
                    observed = self.observe() => observed?,
                }
            }
        })
        .await
        .context("Native turn did not settle within 60 seconds")?;
        self.drain();
        result
    }
    pub(super) async fn wait_file(&mut self, path: &Path) -> Result<()> {
        tokio::time::timeout(Duration::from_secs(90), async {
            while !path.exists() {
                self.observe().await?;
            }
            Ok::<_, anyhow::Error>(())
        })
        .await
        .context("Native tool did not create its ready marker")?
    }
    pub(super) async fn prompt(&mut self, text: &str) -> Result<Value> {
        self.request(
            "session/prompt",
            json!({"sessionId":self.id,"prompt":[{"type":"text","text":text}]}),
        )
        .await
    }
    pub(super) fn drain(&mut self) {
        while let Ok(event) = self.peer.updates.try_recv() {
            self.events.push(serde_json::to_value(event).unwrap());
        }
    }
    pub(super) fn text(&self, start: usize) -> String {
        self.events[start..]
            .iter()
            .filter(|event| event["update"]["sessionUpdate"] == "agent_message_chunk")
            .filter_map(|event| event["update"]["content"]["text"].as_str())
            .collect()
    }
    pub(super) async fn cleanup(mut self) -> Result<()> {
        let closed = self
            .request("session/close", json!({"sessionId":self.id}))
            .await;
        let deleted = self
            .request("session/delete", json!({"sessionId":self.id}))
            .await;
        let shutdown = self.peer.shutdown().await;
        closed?;
        deleted?;
        shutdown
    }
}

#[cfg(unix)]
pub(super) async fn wait_shell_exit(root: &Path) -> Result<()> {
    let pid: libc::pid_t = std::fs::read_to_string(root.join("cancel.pid"))?
        .trim()
        .parse()?;
    ensure!(pid > 1, "Invalid owned shell PID");
    let stopped = tokio::time::timeout(Duration::from_secs(3), async {
        while unsafe { libc::kill(pid, 0) } == 0 {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await;
    if stopped.is_err() {
        let status = std::process::Command::new("ps")
            .args(["-o", "stat=", "-p", &pid.to_string()])
            .output()?;
        // The probe wrote this PID in its fresh temporary workspace. Clean up
        // only its process group and retain the failed qualification result.
        unsafe {
            let target = if libc::getpgid(pid) == pid { -pid } else { pid };
            libc::kill(target, libc::SIGTERM);
        }
        anyhow::bail!(
            "Cancelled native shell {pid} remained alive: {} (owned probe terminated for cleanup)",
            String::from_utf8_lossy(&status.stdout).trim()
        );
    }
    ensure!(
        !root.join("cancel.done").exists(),
        "Cancelled native shell completed its work"
    );
    Ok(())
}
