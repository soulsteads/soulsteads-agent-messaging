use anyhow::Result;
use async_trait::async_trait;
use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio::sync::{Mutex, Notify};

#[async_trait]
pub trait TypingClient: Send + Sync {
    async fn set_typing(&self, room: &str, typing: bool, timeout_ms: u64) -> Result<()>;
}
#[derive(Default)]
struct State {
    users: usize,
    revision: u64,
    completed: u64,
}
#[derive(Default)]
struct Entry {
    state: Mutex<State>,
    dirty: Notify,
    settled: Notify,
}
#[derive(Clone)]
pub struct TypingManager {
    client: Arc<dyn TypingClient>,
    rooms: Arc<Mutex<HashMap<String, Arc<Entry>>>>,
    refresh: Duration,
}
pub struct TypingGuard {
    entry: Arc<Entry>,
    stopped: AtomicBool,
}
impl TypingManager {
    pub fn new(client: Arc<dyn TypingClient>, refresh: Duration) -> Self {
        Self {
            client,
            rooms: Default::default(),
            refresh: refresh.max(Duration::from_millis(1)),
        }
    }
    pub async fn room_count(&self) -> usize {
        self.rooms.lock().await.len()
    }
    pub async fn start(&self, room: &str) -> TypingGuard {
        let mut rooms = self.rooms.lock().await;
        let fresh = !rooms.contains_key(room);
        let entry = rooms.entry(room.to_owned()).or_default().clone();
        {
            let mut state = entry.state.lock().await;
            state.users += 1;
            state.revision += 1;
        }
        entry.dirty.notify_one();
        if fresh {
            let manager = self.clone();
            let room = room.to_owned();
            let entry = entry.clone();
            tokio::spawn(async move {
                manager.run(room, entry).await;
            });
        }
        drop(rooms);
        tokio::task::yield_now().await;
        TypingGuard {
            entry,
            stopped: AtomicBool::new(false),
        }
    }
    async fn run(&self, room: String, entry: Arc<Entry>) {
        loop {
            let (typing, revision) = {
                let state = entry.state.lock().await;
                (state.users > 0, state.revision)
            };
            let timeout = if typing {
                (self
                    .refresh
                    .as_millis()
                    .saturating_mul(2)
                    .min(u64::MAX as u128) as u64)
                    .max(5000)
            } else {
                0
            };
            let _ = self.client.set_typing(&room, typing, timeout).await;
            let mut rooms = self.rooms.lock().await;
            let mut state = entry.state.lock().await;
            state.completed = revision;
            let dirty = revision != state.revision;
            if !dirty && state.users == 0 {
                rooms.remove(&room);
                entry.settled.notify_waiters();
                return;
            }
            entry.settled.notify_waiters();
            drop(state);
            drop(rooms);
            if !dirty {
                tokio::select! {
                    _ = entry.dirty.notified() => {},
                    _ = tokio::time::sleep(self.refresh) => {},
                }
            }
        }
    }
}
async fn stop(entry: &Entry) {
    let revision = {
        let mut state = entry.state.lock().await;
        state.users -= 1;
        if state.users > 0 {
            return;
        }
        state.revision += 1;
        state.revision
    };
    entry.dirty.notify_one();
    loop {
        let settled = entry.settled.notified();
        if entry.state.lock().await.completed >= revision {
            return;
        }
        settled.await;
    }
}
impl TypingGuard {
    pub async fn stop(&self) {
        if !self.stopped.swap(true, Ordering::SeqCst) {
            stop(&self.entry).await;
        }
    }
}
impl Drop for TypingGuard {
    fn drop(&mut self) {
        if !self.stopped.swap(true, Ordering::SeqCst) {
            let entry = self.entry.clone();
            if let Ok(runtime) = tokio::runtime::Handle::try_current() {
                runtime.spawn(async move {
                    stop(&entry).await;
                });
            }
        }
    }
}
