use crate::{acp::PermissionRequest, runtime::Sink};
use futures_util::FutureExt;
use regex::Regex;
use serde_json::{Value, json};
use std::{
    collections::HashMap,
    path::{Component, Path, PathBuf},
    sync::{
        Arc, Mutex, OnceLock,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio::sync::oneshot;

#[derive(Clone, PartialEq, Eq)]
pub(crate) struct Scope {
    pub sender: String,
    pub room: String,
    pub thread: String,
    pub context: String,
    pub cwd: PathBuf,
}
struct Pending {
    scope: Scope,
    ready: bool,
    expires: tokio::time::Instant,
    current: Arc<AtomicBool>,
    decision: oneshot::Sender<bool>,
}
#[derive(Default)]
pub(crate) struct Broker(Mutex<HashMap<String, Pending>>);
struct Guard<'a>(&'a Broker, String);
impl Drop for Guard<'_> {
    fn drop(&mut self) {
        self.0.0.lock().unwrap().remove(&self.1);
    }
}
pub(crate) fn cancelled() -> Value {
    json!({"outcome":"cancelled"})
}

fn preview(scope: &Scope, params: &Value) -> Option<(String, String, Option<String>)> {
    let tool = &params["toolCall"];
    if tool["kind"] != "execute" || tool["toolCallId"].as_str()?.trim().is_empty() {
        return None;
    }
    let input = tool["rawInput"].as_object()?;
    if (input.contains_key("command") && input.contains_key("cmd"))
        || (input.contains_key("cwd") && input.contains_key("workdir"))
        || input.keys().any(|key| {
            !["command", "cmd", "cwd", "workdir", "description", "timeout"].contains(&key.as_str())
        })
        || input
            .get("description")
            .is_some_and(|value| !value.is_string())
        || input
            .get("timeout")
            .is_some_and(|value| value.as_f64().is_none_or(|number| number < 0.0))
    {
        return None;
    }
    let command = input
        .get("command")
        .or_else(|| input.get("cmd"))?
        .as_str()?;
    if command.trim().is_empty() {
        return None;
    }
    let directory = input.get("workdir").or_else(|| input.get("cwd"));
    let directory = match directory {
        Some(value) => Path::new(value.as_str()?),
        None => &scope.cwd,
    };
    let mut cwd = PathBuf::new();
    for component in scope.cwd.join(directory).components() {
        match component {
            Component::ParentDir => {
                cwd.pop();
            }
            Component::CurDir => {}
            other => cwd.push(other.as_os_str()),
        }
    }
    if !cwd.is_absolute() {
        return None;
    }
    let (once, deny) = native_options(params)?;
    let display = format!(
        "Working directory: {}\nOperation (literal shell command):\n{command}",
        cwd.to_str()?
    );
    static SENSITIVE: OnceLock<Regex> = OnceLock::new();
    let sensitive = SENSITIVE.get_or_init(|| Regex::new(r"(?i)(?:password|passwd|secret|api[_-]?key|token)\s*[:=]\s*\S|bearer\s+\S|-----BEGIN[^\n]*PRIVATE KEY|://[^/\s:@]+:[^/\s@]+@|(?-u:\b)(?:sk-|ghp_|github_pat_)[A-Za-z0-9]").unwrap());
    if display.encode_utf16().count() > 8000 || sensitive.is_match(&display)
        || display.chars().any(|character| (character.is_control() && !matches!(character, '\n' | '\t')) || matches!(character, '\u{061c}' | '\u{200e}' | '\u{200f}' | '\u{202a}'..='\u{202e}' | '\u{2066}'..='\u{2069}' | '\u{feff}')) { return None; }
    Some((display, once, deny))
}
pub(crate) fn native_options(params: &Value) -> Option<(String, Option<String>)> {
    let options = params["options"].as_array()?;
    if options.len() > 128 {
        return None;
    }
    let mut ids = std::collections::HashSet::new();
    let mut once = None;
    let mut deny = None;
    for option in options {
        let id = option["optionId"].as_str()?;
        let kind = option["kind"].as_str()?;
        if id.trim().is_empty()
            || !ids.insert(id)
            || !["allow_once", "allow_always", "reject_once", "reject_always"].contains(&kind)
        {
            return None;
        }
        if kind == "allow_once" {
            if once.is_some() {
                return None;
            }
            once = Some(id.to_owned());
        }
        if kind == "reject_once" {
            if deny.is_some() {
                return None;
            }
            deny = Some(id.to_owned());
        }
    }
    Some((once?, deny))
}
impl Broker {
    pub(crate) fn decide(&self, scope: &Scope, id: &str, approve: bool, event_id: &str) -> bool {
        let mut pending = self.0.lock().unwrap();
        let Some(entry) = pending.get(id) else {
            return false;
        };
        if !entry.current.load(Ordering::SeqCst) || tokio::time::Instant::now() >= entry.expires {
            pending.remove(id);
            return false;
        }
        if event_id.is_empty()
            || !entry.ready
            || entry.scope.sender != scope.sender
            || entry.scope.room != scope.room
            || entry.scope.thread != scope.thread
            || entry.scope.context != scope.context
        {
            return false;
        }
        let entry = pending.remove(id).unwrap();
        entry.decision.send(approve).is_ok()
    }
    pub(crate) async fn request(&self, scope: Scope, request: PermissionRequest, sink: &dyn Sink) {
        self.request_until(
            scope,
            request,
            sink,
            tokio::time::Instant::now() + Duration::from_secs(45 * 60),
        )
        .await;
    }
    async fn request_until(
        &self,
        scope: Scope,
        mut request: PermissionRequest,
        sink: &dyn Sink,
        expires: tokio::time::Instant,
    ) {
        let Some((display, once, deny)) = preview(&scope, &request.params) else {
            let _ = request.reply.send(cancelled());
            return;
        };
        if !request.current.load(Ordering::SeqCst) {
            let _ = request.reply.send(cancelled());
            return;
        }
        let id = uuid::Uuid::new_v4().to_string();
        let (decision, outcome) = oneshot::channel();
        {
            let mut pending = self.0.lock().unwrap();
            if pending.len() >= 128 {
                let _ = request.reply.send(cancelled());
                return;
            }
            pending.insert(
                id.clone(),
                Pending {
                    scope,
                    ready: false,
                    expires,
                    current: request.current.clone(),
                    decision,
                },
            );
        }
        let _guard = Guard(self, id.clone());
        let body = format!(
            "Approval required — one operation only\n{display}\n\n/approve {id}\n/deny {id}\nExpires in 45 minutes. Reply in the same room/thread from the requesting account."
        );
        let result = async {
            sink.emit(json!({"type":"notice", "text":body}))
                .await
                .ok()?;
            if !request.current.load(Ordering::SeqCst) {
                return None;
            }
            self.0.lock().unwrap().get_mut(&id)?.ready = true;
            outcome.await.ok()
        }
        .boxed();
        let decision = tokio::select! {
            biased;
            _ = request.reply.closed() => None,
            _ = tokio::time::sleep_until(expires) => None,
            decision = result => decision,
        };
        let outcome = if request.current.load(Ordering::SeqCst) {
            match decision {
                Some(true) => json!({"outcome":"selected", "optionId":once}),
                Some(false) => deny
                    .map(|id| json!({"outcome":"selected", "optionId":id}))
                    .unwrap_or_else(cancelled),
                None => cancelled(),
            }
        } else {
            cancelled()
        };
        let _ = request.reply.send(outcome);
    }
}

#[cfg(test)]
#[path = "../../../tests/connectors/matrix/permissions.rs"]
mod tests;
