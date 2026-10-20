//! Native extension dialogs projected to request-scoped ACP choices.
use super::{Approval, Session, State, process::Lease, turn::Turn};
use crate::adapters::{bounded, text};
use agent_client_protocol::UntypedMessage;
use anyhow::{Context, Result, bail};
use serde_json::{Value, json};
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio::sync::watch;
impl State {
    pub(super) async fn extension_ui(
        self: &Arc<Self>,
        session: &Arc<Session>,
        event: Value,
    ) -> Result<()> {
        let method = event["method"].as_str().unwrap_or("");
        let native = session.native()?;
        if method == "notify" {
            text(
                self.client()?,
                session.id()?,
                "agent_message_chunk",
                &bounded(event["message"].as_str().unwrap_or("Pi notification"), 8192),
            )?;
            return Ok(());
        }
        if !matches!(method, "select" | "confirm" | "input" | "editor") {
            return Ok(());
        }
        let id = event["id"]
            .as_str()
            .filter(|id| !id.is_empty() && id.len() <= 1024)
            .context("Invalid Pi UI request ID")?
            .to_owned();
        let turn = session.active.lock().await.clone();
        let turn = turn.filter(|turn| turn.live.load(Ordering::SeqCst));
        let permit = self.permissions.clone().try_acquire_owned();
        if turn.is_none()
            || permit.is_err()
            || (matches!(method, "input" | "editor") && !self.extension_ui.load(Ordering::SeqCst))
        {
            native
                .write(
                    json!({"type":"extension_ui_response","id":id,"cancelled":true}),
                    None,
                )
                .await?;
            return Ok(());
        }
        let turn = turn.unwrap();
        let permit = permit.unwrap();
        let ask = Arc::new(AtomicBool::new(true));
        let (cancel, cancelled) = watch::channel(false);
        {
            let mut approvals = session.approvals.lock().unwrap();
            if approvals.contains_key(&id) || approvals.len() >= 64 {
                bail!("Duplicate or excessive Pi UI request");
            }
            approvals.insert(
                id.clone(),
                Approval {
                    live: ask.clone(),
                    cancel,
                },
            );
        }
        let deadline = event["timeout"].as_u64().map(|timeout| {
            tokio::time::Instant::now() + Duration::from_millis(timeout.min(45 * 60 * 1000))
        });
        let state = self.clone();
        let session = session.clone();
        tokio::spawn(async move {
            let _permit = permit;
            let result = state
                .approval(&session, &turn, &event, ask, cancelled, deadline)
                .await;
            session.approvals.lock().unwrap().remove(&id);
            if let Err(error) = result {
                eprintln!("Pi extension dialog failed: {error}");
                if let Ok(native) = session.native() {
                    native.abort();
                }
            }
        });
        Ok(())
    }
    async fn approval(
        &self,
        session: &Session,
        turn: &Turn,
        event: &Value,
        ask: Arc<AtomicBool>,
        mut cancelled: watch::Receiver<bool>,
        deadline: Option<tokio::time::Instant>,
    ) -> Result<()> {
        let id = event["id"].as_str().unwrap();
        let method = event["method"].as_str().unwrap();
        let choices = if method == "select" {
            let choices = event["options"]
                .as_array()
                .context("Pi select omitted choices")?;
            if choices.is_empty() || choices.len() > 64 {
                bail!("Invalid Pi select choice count");
            }
            choices
                .iter()
                .map(|value| {
                    value
                        .as_str()
                        .filter(|value| value.len() <= 4096)
                        .map(str::to_owned)
                        .context("Invalid Pi dialog choice")
                })
                .collect::<Result<Vec<_>>>()?
        } else {
            vec![]
        };
        let options = if method == "confirm" {
            json!([{"optionId":"allow-once","name":"Yes","kind":"allow_once"},{"optionId":"reject-once","name":"No","kind":"reject_once"}])
        } else {
            choices.iter().enumerate().map(|(index,name)|json!({"optionId":format!("choice-{index}"),"name":name,"kind":"allow_once"})).collect::<Vec<_>>().into()
        };
        let title = bounded(
            event["title"].as_str().unwrap_or("Pi extension dialog"),
            4096,
        );
        let raw = json!({"method":method,"message":bounded(event["message"].as_str().unwrap_or(""),8192)});
        let freeform = matches!(method, "input" | "editor");
        let request = if freeform {
            UntypedMessage::new(
                "_pi/extension_ui",
                json!({"sessionId":session.id()?,"requestId":id,"method":method,"title":title,"placeholder":bounded(event["placeholder"].as_str().unwrap_or(""),8192),"initialValue":bounded(event["prefill"].as_str().or_else(||event["initialValue"].as_str()).unwrap_or(""),65536)}),
            )?
        } else {
            UntypedMessage::new(
                "session/request_permission",
                json!({"sessionId":session.id()?,"toolCall":{"toolCallId":format!("pi-ui-{id}"),"title":title,"kind":"other","status":"pending","rawInput":raw},"options":options}),
            )?
        };
        let future = self.client()?.send_request(request).block_task();
        let mut stopping = turn.cancelled.subscribe();
        let mut done = turn.done.subscribe();
        let mut closed = session.native()?.closed();
        let expiry = async {
            if let Some(deadline) = deadline {
                tokio::time::sleep_until(deadline).await
            } else {
                std::future::pending::<()>().await
            }
        };
        let answer = if !ask.load(Ordering::SeqCst) || !turn.live.load(Ordering::SeqCst) {
            None
        } else {
            tokio::select! {answer=future=>answer.ok(),_=cancelled.changed()=>None,_=stopping.changed()=>None,_=done.changed()=>None,_=closed.changed()=>None,_=expiry=>None}
        };
        let selected = answer
            .as_ref()
            .filter(|answer| answer["outcome"]["outcome"] == "selected")
            .and_then(|answer| answer["outcome"]["optionId"].as_str());
        let response = if freeform {
            if let Some(value) = answer
                .as_ref()
                .filter(|answer| answer["cancelled"] != true)
                .and_then(|answer| answer["value"].as_str())
                .filter(|value| value.len() <= 65536)
            {
                json!({"type":"extension_ui_response","id":id,"value":value})
            } else {
                json!({"type":"extension_ui_response","id":id,"cancelled":true})
            }
        } else if method == "confirm" {
            match selected {
                Some("allow-once") => {
                    json!({"type":"extension_ui_response","id":id,"confirmed":true})
                }
                Some("reject-once") => {
                    json!({"type":"extension_ui_response","id":id,"confirmed":false})
                }
                _ => json!({"type":"extension_ui_response","id":id,"cancelled":true}),
            }
        } else if let Some(choice) = selected
            .and_then(|id| id.strip_prefix("choice-"))
            .and_then(|index| index.parse::<usize>().ok())
            .and_then(|index| choices.get(index))
        {
            json!({"type":"extension_ui_response","id":id,"value":choice})
        } else {
            json!({"type":"extension_ui_response","id":id,"cancelled":true})
        };
        session
            .native()?
            .write(
                response,
                Some(Lease {
                    ask,
                    turn: turn.live.clone(),
                    deadline,
                }),
            )
            .await
    }
}
