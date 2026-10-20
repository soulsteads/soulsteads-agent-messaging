//! One ACP collector across native steps, retries and queued admissions.
use super::process::Process;
use anyhow::{Result, bail};
use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
};
use tokio::sync::{Mutex, watch};
#[derive(Clone)]
pub(super) enum Terminal {
    Complete(String),
    Error(String),
}
#[derive(Default)]
pub(super) struct Completion {
    pub(super) admissions: usize,
    pub(super) settled: bool,
    pub(super) started: bool,
    pub(super) finished: bool,
    pub(super) cancelled: bool,
    pub(super) failure: Option<String>,
}
pub(super) struct Turn {
    pub(super) live: Arc<AtomicBool>,
    pub(super) status: std::sync::Mutex<Completion>,
    pub(super) done: watch::Sender<Option<Terminal>>,
    pub(super) cancelled: watch::Sender<bool>,
    pub(super) fragments: Mutex<HashMap<usize, String>>,
    pub(super) tools: Mutex<HashMap<String, String>>,
    pub(super) bytes: AtomicUsize,
}
impl Turn {
    pub(super) fn new() -> Arc<Self> {
        Arc::new(Self {
            live: Arc::new(AtomicBool::new(true)),
            status: Default::default(),
            done: watch::channel(None).0,
            cancelled: watch::channel(false).0,
            fragments: Default::default(),
            tools: Default::default(),
            bytes: Default::default(),
        })
    }
    pub(super) fn reserve(&self, count: usize) -> Result<()> {
        if self
            .bytes
            .fetch_add(count, Ordering::SeqCst)
            .saturating_add(count)
            > 16 * 1024 * 1024
        {
            bail!("Pi turn output exceeded limit");
        }
        Ok(())
    }
    pub(super) fn admit(&self) -> Result<()> {
        let mut status = self.status.lock().unwrap();
        if status.finished || status.cancelled {
            bail!("Pi turn is finishing or cancelled");
        }
        if status.admissions >= 64 {
            bail!("Pi prompt admission limit exceeded");
        }
        status.admissions += 1;
        Ok(())
    }
    pub(super) fn complete_if_ready(&self, status: &mut Completion) {
        if status.finished || !status.settled || status.admissions > 0 {
            return;
        }
        status.finished = true;
        self.live.store(false, Ordering::SeqCst);
        self.done.send_replace(Some(if status.cancelled {
            Terminal::Complete("cancelled".into())
        } else if let Some(error) = &status.failure {
            Terminal::Error(error.clone())
        } else {
            Terminal::Complete("end_turn".into())
        }));
    }
    pub(super) fn admitted(&self) {
        let mut status = self.status.lock().unwrap();
        status.admissions = status.admissions.saturating_sub(1);
        self.complete_if_ready(&mut status);
    }
    pub(super) fn started(&self) {
        let mut status = self.status.lock().unwrap();
        status.started = true;
        status.settled = false;
    }
    pub(super) fn settle(&self) {
        let mut status = self.status.lock().unwrap();
        status.settled = true;
        self.complete_if_ready(&mut status);
    }
    pub(super) fn fail(&self, error: String) {
        let mut status = self.status.lock().unwrap();
        status.failure = Some(error);
    }
    pub(super) fn cancel(&self) -> bool {
        let mut status = self.status.lock().unwrap();
        if status.cancelled || status.finished {
            return false;
        }
        status.cancelled = true;
        self.live.store(false, Ordering::SeqCst);
        self.cancelled.send_replace(true);
        true
    }
    pub(super) async fn terminal(&self, native: &Process) -> Result<Terminal> {
        let mut done = self.done.subscribe();
        let mut closed = native.closed();
        loop {
            if let Some(terminal) = done.borrow().clone() {
                return Ok(terminal);
            }
            if *closed.borrow() {
                bail!("Pi transport closed during the turn");
            }
            tokio::select! {result=done.changed()=>{result?;},result=closed.changed()=>{result?;}}
        }
    }
}
