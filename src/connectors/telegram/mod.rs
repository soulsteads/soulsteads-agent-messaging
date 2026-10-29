//! Telegram transport behind the existing HTTP connector API.
//!
//! `connectors::http::Platform` remains the public connector surface; these
//! modules hold its Telegram-specific mapping, media, outbound, and polling behavior.

pub(crate) mod inbound;
pub(crate) mod outbound;

use crate::{
    connectors::http::{Platform, PlatformSink},
    runtime::{Bridge, Sink},
};
use anyhow::{Result, bail};
use serde_json::json;
use std::sync::Arc;
use tokio::sync::{Semaphore, watch};

pub(crate) async fn poll(
    platform: Platform,
    bridge: Arc<Bridge>,
    shutdown: &mut watch::Receiver<bool>,
    capacity: &Arc<Semaphore>,
    tasks: &mut tokio::task::JoinSet<()>,
) -> Result<()> {
    // Keep one bounded lane for approvals so 64 permission-waiting
    // prompts cannot prevent their own approval or denial.
    let controls = Arc::new(Semaphore::new(1));
    let drop_pending = bridge
        .env
        .get("TELEGRAM_DROP_PENDING")
        .is_some_and(|value| value == "1");
    let _ = tokio::select! {
        _=shutdown.changed()=>return Ok(()),
        result=platform.api("POST","deleteWebhook",json!({"drop_pending_updates":drop_pending}))=>result,
    };
    let mut offset = 0;
    if drop_pending {
        let pending = tokio::select! {
            _=shutdown.changed()=>return Ok(()),
            pending=platform.api("POST","getUpdates",json!({"timeout":0,"allowed_updates":["message"]}))=>pending,
        };
        if let Ok(pending) = pending {
            offset = pending
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(|update| update["update_id"].as_i64())
                .max()
                .map(|id| id.saturating_add(1))
                .unwrap_or(0);
        }
    }
    let mut failures = 0u32;
    'telegram: loop {
        if *shutdown.borrow() {
            break;
        }
        let updates = tokio::select! { _=shutdown.changed()=>break, updates=platform.api("POST","getUpdates",json!({"offset":offset,"timeout":30,"allowed_updates":["message"]}))=>updates };
        match updates {
            Ok(updates) => {
                failures = 0;
                for update in updates.as_array().into_iter().flatten() {
                    let control = inbound::map(&platform, update)
                        .is_some_and(|event| crate::runtime::telegram_approval_control(&event));
                    let permit = if control {
                        tokio::select! {
                            _ = shutdown.changed() => break 'telegram,
                            permit = controls.clone().acquire_owned() => permit.expect("Telegram control semaphore remains open"),
                        }
                    } else {
                        match capacity.clone().try_acquire_owned() {
                            Ok(permit) => permit,
                            Err(tokio::sync::TryAcquireError::NoPermits) => {
                                // Recheck all normal transport/address gates before refusal,
                                // but never download media or start a native turn for work refused
                                // at this bounded ordinary-capacity boundary.
                                if let Some(event) =
                                    inbound::admit(&platform, &bridge, update).await?
                                {
                                    let sink = PlatformSink::new(platform.clone(), event);
                                    tokio::select! {
                                        _ = shutdown.changed() => break 'telegram,
                                        result = sink.emit(json!({
                                            "type":"notice",
                                            "text":"Busy: this message was not accepted or run. Please retry shortly."
                                        })) => result?,
                                    }
                                }
                                if let Some(id) = update["update_id"].as_i64() {
                                    offset = offset.max(id.saturating_add(1));
                                }
                                continue;
                            }
                            Err(tokio::sync::TryAcquireError::Closed) => {
                                bail!("Telegram polling semaphore closed")
                            }
                        }
                    };
                    if let Some(id) = update["update_id"].as_i64() {
                        offset = offset.max(id.saturating_add(1));
                    }
                    let platform = platform.clone();
                    let bridge = bridge.clone();
                    let update = update.clone();
                    tasks.spawn(async move {
                        let _permit = permit;
                        if let Ok(Some(event)) = platform.prepare(&bridge, &update).await {
                            let sink = PlatformSink::new(platform, event.clone())
                                .with_thread_isolation(crate::config::boolean(
                                    &bridge.config[&event.platform],
                                    "threadIsolation",
                                ));
                            let _ = bridge.handle(event, &sink).await;
                        }
                    });
                }
            }
            Err(_) => {
                failures += 1;
                if failures >= 10 {
                    bail!("Telegram polling retry limit reached");
                }
                let delay = std::time::Duration::from_secs((1u64 << failures.min(5)).min(30));
                tokio::select! { _=shutdown.changed()=>break, _=tokio::time::sleep(delay)=>{} }
            }
        }
        while tasks.try_join_next().is_some() {}
    }
    Ok(())
}
