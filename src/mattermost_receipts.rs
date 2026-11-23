//! A local outbox for the final text of an admitted Mattermost turn.
//! `Attempted` is durable *before* the first POST. It is never retried.
use crate::runtime::{Event, Sink};
use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{collections::HashSet, path::Path, sync::Mutex};

const MAX_JOURNAL: usize = 8 * 1024 * 1024;
const MAX_TURNS: usize = 8192;
const MAX_TEXT: usize = 256 * 1024;
const MAX_ENCODED_TEXT: usize = 384 * 1024;
const MAX_POSTS: usize = 68;
// Each running turn reserves its maximum encoded reply plus state overhead.
const RESERVATION: usize = MAX_ENCODED_TEXT + 512;

pub(crate) fn valid_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
}

#[derive(Clone)]
pub(crate) struct Scope {
    owner: String,
    turn: String,
}

impl Scope {
    pub(crate) fn new(event: &Event, sink: &dyn Sink) -> Result<Self> {
        let server_bot = sink
            .mattermost_receipt_identity(event)
            .context("Mattermost turn receipts require the original thread destination")?;
        let (channel, post) = event
            .id
            .split_once(':')
            .context("Missing Mattermost post ID")?;
        ensure!(
            channel == event.room
                && [channel, post, event.thread.as_str(), event.user.as_str()]
                    .into_iter()
                    .all(valid_id),
            "Invalid Mattermost post or thread identity"
        );
        fn key(parts: &[&str]) -> String {
            // Inputs are encoded as a tuple; separators in URLs cannot alias another scope.
            format!(
                "{:x}",
                Sha256::digest(serde_json::to_vec(parts).expect("string tuple"))
            )
        }
        Ok(Self {
            owner: key(&[&server_bot, channel, &event.thread, &event.user]),
            turn: key(&[&server_bot, post]),
        })
    }
}

#[derive(Clone, Copy, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Outcome {
    Completed,
    Cancelled,
    Stopped,
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(tag = "state", rename_all = "snake_case", deny_unknown_fields)]
enum State {
    Running,
    Interrupted,
    NoReply {
        outcome: Outcome,
    },
    ReplyTooLarge {
        outcome: Outcome,
    },
    Pending {
        outcome: Outcome,
        text: String,
    },
    Attempted {
        outcome: Outcome,
        text: String,
    },
    Delivered {
        outcome: Outcome,
        posts: Vec<String>,
    },
}

impl State {
    fn status(&self, active: bool) -> &'static str {
        match self {
            Self::Running if active => "Turn accepted; waiting for the backend to finish.",
            Self::Running | Self::Interrupted => {
                "Turn interrupted or outcome unknown; it will not be run again automatically."
            }
            Self::NoReply { .. } => "Turn ended without a final text reply to recover.",
            Self::ReplyTooLarge { .. } => {
                "The final reply exceeded the receipt size limit and was not sent."
            }
            Self::Pending { .. } => "Reply saved; not yet sent. Use /recover in this thread.",
            Self::Attempted { .. } => {
                "Reply delivery is in progress or uncertain. Check this thread; the bridge will not repost automatically."
            }
            Self::Delivered { .. } => "Reply delivery acknowledged by Mattermost.",
        }
    }

    fn unresolved(&self) -> bool {
        matches!(self, Self::Pending { .. } | Self::Attempted { .. })
    }
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Receipt {
    owner: String,
    turn: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    recovery: Option<String>,
    state: State,
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Journal {
    version: u8,
    receipts: Vec<Receipt>,
}

impl Journal {
    fn encode(&self) -> Result<Vec<u8>> {
        let bytes = serde_json::to_vec(self)?;
        let reserved: usize = self
            .receipts
            .iter()
            .map(|r| match r.state {
                State::Running => RESERVATION,
                State::Pending { .. } | State::Attempted { .. } => 256,
                _ => 0,
            })
            .sum();
        ensure!(
            self.receipts.len() <= MAX_TURNS && bytes.len() + reserved <= MAX_JOURNAL,
            "Mattermost turn receipt journal is full"
        );
        Ok(bytes)
    }

    fn seen(&self, key: &str) -> bool {
        self.receipts
            .iter()
            .any(|r| r.turn == key || r.recovery.as_deref() == Some(key))
    }

    fn validate(&self) -> Result<()> {
        fn digest(key: &str) -> bool {
            key.len() == 64
                && key
                    .bytes()
                    .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        }
        let mut turns = HashSet::new();
        ensure!(self.version == 1, "Unsupported Mattermost receipt version");
        for receipt in &self.receipts {
            ensure!(
                digest(&receipt.owner) && digest(&receipt.turn) && turns.insert(&receipt.turn),
                "Invalid or duplicate Mattermost turn receipt"
            );
            if let Some(recovery) = &receipt.recovery {
                ensure!(
                    digest(recovery)
                        && turns.insert(recovery)
                        && matches!(
                            receipt.state,
                            State::Attempted { .. } | State::Delivered { .. }
                        ),
                    "Invalid Mattermost recovery claim"
                );
            }
            match &receipt.state {
                State::Pending { text, .. } | State::Attempted { text, .. } => {
                    ensure!(valid_text(text)?, "Invalid Mattermost receipt reply");
                }
                State::Delivered { posts, .. } => {
                    ensure!(
                        valid_posts(posts),
                        "Invalid Mattermost post acknowledgement"
                    );
                }
                _ => {}
            }
        }
        self.encode()?;
        Ok(())
    }
}

fn valid_text(text: &str) -> Result<bool> {
    Ok(!text.trim().is_empty()
        && text.len() <= MAX_TEXT
        && serde_json::to_vec(text)?.len() <= MAX_ENCODED_TEXT)
}

fn valid_posts(posts: &[String]) -> bool {
    !posts.is_empty()
        && posts.len() <= MAX_POSTS
        && posts.iter().all(|id| valid_id(id))
        && posts.iter().collect::<HashSet<_>>().len() == posts.len()
}

struct Memory {
    journal: Journal,
    live: HashSet<String>,
    unavailable: bool,
}

pub(crate) struct Store {
    disk: crate::private_journal::Disk,
    memory: Mutex<Memory>,
}

impl Store {
    pub(crate) fn open(path: &Path) -> Result<Self> {
        let disk = crate::private_journal::Disk::open(path, "turns.json", MAX_JOURNAL)?;
        let journal = if let Some(bytes) = disk.read()? {
            let journal: Journal = serde_json::from_slice(&bytes)
                .map_err(|_| anyhow::anyhow!("Malformed Mattermost turn receipts"))?;
            journal.validate()?;
            journal
        } else {
            let journal = Journal {
                version: 1,
                receipts: vec![],
            };
            disk.write(&journal.encode()?, true)?;
            journal
        };
        // Old Running entries are no longer live; compact them into unknown
        // outcomes so crash history does not permanently reserve reply space.
        let mut journal = journal;
        let mut changed = false;
        for receipt in &mut journal.receipts {
            if matches!(receipt.state, State::Running) {
                receipt.state = State::Interrupted;
                changed = true;
            }
        }
        if changed {
            disk.write(&journal.encode()?, false)?;
        }
        Ok(Self {
            disk,
            memory: Mutex::new(Memory {
                journal,
                live: HashSet::new(),
                unavailable: false,
            }),
        })
    }

    fn memory(&self) -> Result<std::sync::MutexGuard<'_, Memory>> {
        let memory = self
            .memory
            .lock()
            .map_err(|_| anyhow::anyhow!("Mattermost turn receipts unavailable"))?;
        ensure!(
            !memory.unavailable,
            "Mattermost turn receipts unavailable; restart required"
        );
        Ok(memory)
    }

    fn commit(&self, memory: &mut Memory, next: Journal) -> Result<()> {
        let bytes = next.encode()?;
        if let Err(error) = self.disk.write(&bytes, false) {
            // An unsuccessful directory sync can follow a successful rename.
            // A stale in-memory copy must not authorize another transition.
            memory.unavailable = true;
            return Err(error);
        }
        memory.journal = next;
        Ok(())
    }

    pub(crate) fn seen(&self, scope: &Scope) -> Result<bool> {
        Ok(self.memory()?.journal.seen(&scope.turn))
    }

    pub(crate) fn begin(&self, scope: &Scope) -> Result<Turn<'_>> {
        let mut memory = self.memory()?;
        let mut next = memory.journal.clone();
        ensure!(!next.seen(&scope.turn), "Mattermost turn already accepted");
        next.receipts.push(Receipt {
            owner: scope.owner.clone(),
            turn: scope.turn.clone(),
            recovery: None,
            state: State::Running,
        });
        self.commit(&mut memory, next)?;
        memory.live.insert(scope.turn.clone());
        Ok(Turn {
            store: self,
            scope: scope.clone(),
        })
    }

    fn transition(
        &self,
        scope: &Scope,
        recovery: Option<&str>,
        update: impl FnOnce(&State) -> Result<State>,
    ) -> Result<()> {
        let mut memory = self.memory()?;
        if let Some(recovery) = recovery {
            ensure!(
                !memory.journal.seen(recovery),
                "Mattermost recovery already attempted"
            );
        }
        let mut next = memory.journal.clone();
        let receipt = next
            .receipts
            .iter_mut()
            .find(|r| r.owner == scope.owner && r.turn == scope.turn)
            .context("Missing Mattermost turn receipt")?;
        receipt.state = update(&receipt.state)?;
        if let Some(recovery) = recovery {
            receipt.recovery = Some(recovery.to_owned());
        }
        self.commit(&mut memory, next)
    }

    pub(crate) fn status(&self, scope: &Scope) -> Result<&'static str> {
        let memory = self.memory()?;
        // Prioritize unresolved delivery, then the latest turn for this sender.
        let own = || {
            memory
                .journal
                .receipts
                .iter()
                .rev()
                .filter(|r| r.owner == scope.owner)
        };
        let receipt = own()
            .find(|r| matches!(r.state, State::Pending { .. }))
            .or_else(|| own().find(|r| r.state.unresolved()))
            .or_else(|| own().next());
        Ok(
            receipt.map_or("No turn receipt for you in this thread.", |r| {
                r.state.status(memory.live.contains(&r.turn))
            }),
        )
    }

    pub(crate) async fn recover(&self, scope: &Scope, sink: &dyn Sink) -> Result<()> {
        let pending = {
            let memory = self.memory()?;
            memory.journal.receipts.iter().find_map(|r| match &r.state {
                State::Pending { text, .. } if r.owner == scope.owner => {
                    Some((r.turn.clone(), text.clone()))
                }
                _ => None,
            })
        };
        if let Some((turn, text)) = pending {
            self.deliver(
                &Scope {
                    turn,
                    ..scope.clone()
                },
                &text,
                sink,
                Some(&scope.turn),
            )
            .await
        } else {
            sink.emit(serde_json::json!({"type":"notice","text":self.status(scope)?}))
                .await
        }
    }

    async fn deliver(
        &self,
        scope: &Scope,
        text: &str,
        sink: &dyn Sink,
        recovery: Option<&str>,
    ) -> Result<()> {
        let send = async {
            let posts = sink
                .deliver_mattermost_reply(text, &mut || {
                    self.transition(scope, recovery, |state| match state {
                        State::Pending {
                            outcome,
                            text: saved,
                        } if saved == text => Ok(State::Attempted {
                            outcome: *outcome,
                            text: saved.clone(),
                        }),
                        _ => anyhow::bail!("Mattermost reply is no longer pending"),
                    })
                })
                .await?;
            ensure!(
                valid_posts(&posts),
                "Invalid Mattermost post acknowledgement"
            );
            self.transition(scope, None, |state| match state {
                State::Attempted { outcome, .. } => Ok(State::Delivered {
                    outcome: *outcome,
                    posts,
                }),
                _ => anyhow::bail!("Missing Mattermost send intent"),
            })
        };
        send.await.context(ReplyDeliveryError)
    }
}

#[derive(Debug)]
pub(crate) struct ReplyDeliveryError;
impl std::fmt::Display for ReplyDeliveryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Mattermost final reply could not be confirmed")
    }
}
impl std::error::Error for ReplyDeliveryError {}

pub(crate) struct Turn<'a> {
    store: &'a Store,
    scope: Scope,
}
impl Turn<'_> {
    pub(crate) async fn finish(self, outcome: Outcome, text: &str, sink: &dyn Sink) -> Result<()> {
        let state = if text.is_empty() {
            State::NoReply { outcome }
        } else if valid_text(text)? {
            State::Pending {
                outcome,
                text: text.to_owned(),
            }
        } else {
            State::ReplyTooLarge { outcome }
        };
        let pending = matches!(state, State::Pending { .. });
        let too_large = matches!(state, State::ReplyTooLarge { .. });
        self.store.transition(&self.scope, None, |old| {
            ensure!(
                matches!(old, State::Running),
                "Mattermost turn is not running"
            );
            Ok(state)
        })?;
        if pending {
            self.store.deliver(&self.scope, text, sink, None).await
        } else {
            ensure!(!too_large, "Mattermost final reply exceeds receipt limit");
            Ok(())
        }
    }
}
impl Drop for Turn<'_> {
    fn drop(&mut self) {
        if let Ok(mut memory) = self.store.memory() {
            memory.live.remove(&self.scope.turn);
            if memory
                .journal
                .receipts
                .iter()
                .any(|r| r.turn == self.scope.turn && matches!(r.state, State::Running))
            {
                let mut next = memory.journal.clone();
                if let Some(receipt) = next.receipts.iter_mut().find(|r| r.turn == self.scope.turn)
                {
                    receipt.state = State::Interrupted;
                }
                // A failed write leaves the durable Running receipt, which is
                // treated as unknown and never re-executed on the next open.
                let _ = self.store.commit(&mut memory, next);
            }
        }
    }
}
