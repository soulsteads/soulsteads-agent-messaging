//! Durable, local review requests sent by an admitted human-root turn.

use crate::private_journal::Disk;
use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashSet,
    path::Path,
    sync::{
        Mutex, MutexGuard,
        atomic::{AtomicBool, Ordering},
    },
};
use uuid::Uuid;

const JOURNAL_BYTES: usize = 8 * 1024 * 1024;
const MAX_RECORDS: usize = 192;
const MAX_PENDING: usize = 64;
const MAX_TEXT: usize = 8192;
const MAX_ENCODED_TEXT: usize = 16 * 1024;
const DISPATCH_TTL: i64 = 3600;
const RETENTION: i64 = 7 * 24 * 3600;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Participant {
    pub(crate) id: String,
    pub(crate) scope: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Route {
    pub(crate) sender: Participant,
    pub(crate) recipient: Participant,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Request {
    pub(crate) id: Uuid,
    pub(crate) route: Route,
    key: String,
    pub(crate) revision: String,
    pub(crate) text: String,
    created_at: i64,
    pub(crate) state: State,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub(crate) enum State {
    Queued,
    Running,
    Answered { reply: String },
    Uncertain,
    Expired,
    Denied,
}

impl State {
    fn pending(&self) -> bool {
        matches!(self, Self::Queued | Self::Running)
    }
}

impl Request {
    pub(crate) fn new(route: Route, key: &str, revision: &str, text: &str) -> Option<Self> {
        if !valid_revision(revision) || !valid_key(key) || !valid_text(text) {
            return None;
        }
        Some(Self {
            id: Uuid::new_v4(),
            route,
            key: key.into(),
            revision: revision.into(),
            text: text.into(),
            created_at: now(),
            state: State::Queued,
        })
    }

    pub(crate) fn receipt(&self) -> serde_json::Value {
        let mut receipt = serde_json::to_value(&self.state).expect("review state is serializable");
        receipt["requestId"] = serde_json::json!(self.id);
        receipt["recipient"] = serde_json::json!(self.route.recipient.id);
        receipt["revision"] = serde_json::json!(self.revision);
        receipt["expiresAt"] = serde_json::json!(self.created_at.saturating_add(DISPATCH_TTL));
        receipt
    }

    pub(crate) fn expired(&self) -> bool {
        now().saturating_sub(self.created_at) >= DISPATCH_TTL
    }
}

fn now() -> i64 {
    chrono::Utc::now().timestamp()
}

fn valid_hex(value: &str) -> bool {
    value
        .bytes()
        .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
}

fn valid_revision(revision: &str) -> bool {
    matches!(revision.len(), 40 | 64) && valid_hex(revision)
}

fn valid_key(key: &str) -> bool {
    !key.is_empty()
        && key.len() <= super::MAX_IDEMPOTENCY_KEY_BYTES
        && !key.contains(char::is_control)
}

fn valid_participant(participant: &Participant) -> bool {
    super::AgentId::try_from(participant.id.as_str()).is_ok()
        && participant.scope.len() == 64
        && valid_hex(&participant.scope)
}

pub(crate) fn valid_text(text: &str) -> bool {
    !text.trim().is_empty()
        && text.len() <= MAX_TEXT
        && serde_json::to_vec(text).is_ok_and(|data| data.len() <= MAX_ENCODED_TEXT)
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Journal {
    version: u8,
    records: Vec<Request>,
}

pub(crate) enum SubmitError {
    Conflict,
    Busy,
    Storage,
}

pub(crate) struct Store {
    disk: Disk,
    records: Mutex<Option<Journal>>,
}

impl Store {
    pub(crate) fn open(path: &Path) -> Result<Self> {
        let disk = Disk::open(path, "reviews.json", JOURNAL_BYTES)?;
        let bytes = disk.read()?;
        let mut journal = match &bytes {
            Some(bytes) => {
                serde_json::from_slice::<Journal>(bytes).context("Invalid review journal")?
            }
            None => Journal {
                version: 1,
                records: vec![],
            },
        };
        let mut ids = HashSet::new();
        let mut keys = HashSet::new();
        ensure!(
            journal.version == 1
                && journal.records.len() <= MAX_RECORDS
                && journal.records.iter().all(|record| {
                    ids.insert(record.id)
                        && keys.insert((
                            &record.route.sender.id,
                            &record.route.sender.scope,
                            &record.key,
                        ))
                        && valid_participant(&record.route.sender)
                        && valid_participant(&record.route.recipient)
                        && record.route.sender.id != record.route.recipient.id
                        && record.created_at > 0
                        && valid_key(&record.key)
                        && valid_revision(&record.revision)
                        && valid_text(&record.text)
                        && match &record.state {
                            State::Answered { reply } => valid_text(reply),
                            _ => true,
                        }
                }),
            "Invalid review journal",
        );
        let mut interrupted = false;
        for record in &mut journal.records {
            if matches!(record.state, State::Running) {
                record.state = State::Uncertain;
                interrupted = true;
            }
        }
        if bytes.is_none() || interrupted {
            disk.write(&serde_json::to_vec(&journal)?, bytes.is_none())?;
        }
        Ok(Self {
            disk,
            records: Mutex::new(Some(journal)),
        })
    }

    fn records(&self) -> Result<MutexGuard<'_, Option<Journal>>> {
        let guard = self
            .records
            .lock()
            .map_err(|_| anyhow::anyhow!("Review journal unavailable"))?;
        ensure!(guard.is_some(), "Review journal unavailable until restart");
        Ok(guard)
    }

    fn commit(&self, current: &mut Option<Journal>, candidate: Journal) -> Result<()> {
        let bytes = serde_json::to_vec(&candidate)?;
        if let Err(error) = self.disk.write(&bytes, false) {
            // A sync error can follow publication. Fail closed until disk is reopened.
            *current = None;
            return Err(error);
        }
        *current = Some(candidate);
        Ok(())
    }

    pub(crate) fn submit(
        &self,
        request: Request,
        budget: &AtomicBool,
    ) -> Result<Request, SubmitError> {
        let mut current = self.records().map_err(|_| SubmitError::Storage)?;
        let mut next = current.as_ref().expect("checked").clone();
        next.records
            .retain(|old| old.state.pending() || now().saturating_sub(old.created_at) < RETENTION);
        if let Some(old) = next
            .records
            .iter()
            .find(|old| old.route.sender == request.route.sender && old.key == request.key)
        {
            return if old.route == request.route
                && old.revision == request.revision
                && old.text == request.text
            {
                Ok(old.clone())
            } else {
                Err(SubmitError::Conflict)
            };
        }
        if next.records.len() >= MAX_RECORDS
            || next.records.iter().filter(|r| r.state.pending()).count() >= MAX_PENDING
            || !budget.swap(false, Ordering::AcqRel)
        {
            return Err(SubmitError::Busy);
        }
        next.records.push(request.clone());
        self.commit(&mut current, next)
            .map_err(|_| SubmitError::Storage)?;
        Ok(request)
    }

    pub(crate) fn status(&self, caller: &Participant, id: Uuid) -> Result<Option<Request>> {
        Ok(self
            .records()?
            .as_ref()
            .expect("checked")
            .records
            .iter()
            .find(|r| r.id == id && &r.route.sender == caller)
            .cloned())
    }

    pub(crate) fn queued(&self) -> Result<Vec<Request>> {
        Ok(self
            .records()?
            .as_ref()
            .expect("checked")
            .records
            .iter()
            .filter(|r| matches!(r.state, State::Queued))
            .cloned()
            .collect())
    }

    pub(crate) fn start(&self, id: Uuid) -> Result<bool> {
        self.transition(id, |previous| {
            if matches!(previous.state, State::Queued) {
                Some(if previous.expired() {
                    State::Expired
                } else {
                    State::Running
                })
            } else {
                None
            }
        })
        .map(|state| matches!(state, Some(State::Running)))
    }

    pub(crate) fn settle_queued(&self, id: Uuid, state: State) -> Result<()> {
        self.transition(id, |r| matches!(r.state, State::Queued).then_some(state))?;
        Ok(())
    }

    pub(crate) fn finish(&self, id: Uuid, state: State) -> Result<()> {
        self.transition(id, |r| matches!(r.state, State::Running).then_some(state))?;
        Ok(())
    }

    fn transition(
        &self,
        id: Uuid,
        change: impl FnOnce(&Request) -> Option<State>,
    ) -> Result<Option<State>> {
        let mut current = self.records()?;
        let mut next = current.as_ref().expect("checked").clone();
        let Some(record) = next.records.iter_mut().find(|r| r.id == id) else {
            return Ok(None);
        };
        let Some(state) = change(record) else {
            return Ok(None);
        };
        record.state = state.clone();
        self.commit(&mut current, next)?;
        Ok(Some(state))
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;

    fn participant(id: &str) -> Participant {
        Participant {
            id: id.into(),
            scope: "a".repeat(64),
        }
    }

    #[test]
    fn interrupted_attempt_is_never_returned_as_queued() -> Result<()> {
        let temporary = tempfile::tempdir()?;
        std::fs::set_permissions(temporary.path(), std::fs::Permissions::from_mode(0o700))?;
        let sender = participant("builder");
        let route = Route {
            sender: sender.clone(),
            recipient: participant("reviewer"),
        };
        let accepted = Request::new(route, "unique-key", &"a".repeat(40), "Review").unwrap();
        let id = accepted.id;
        {
            let queue = Store::open(temporary.path())?;
            assert!(queue.submit(accepted, &AtomicBool::new(true)).is_ok());
            assert!(queue.start(id)?);
            assert!(
                Store::open(temporary.path()).is_err(),
                "one bridge owns the queue"
            );
        }
        let queue = Store::open(temporary.path())?;
        assert!(queue.queued()?.is_empty());
        assert!(!queue.start(id)?);
        let record = queue.status(&sender, id)?.unwrap();
        assert!(matches!(record.state, State::Uncertain));
        assert!(queue.status(&participant("reviewer"), id)?.is_none());
        Ok(())
    }
}
