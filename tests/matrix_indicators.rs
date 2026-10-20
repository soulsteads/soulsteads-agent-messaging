use async_trait::async_trait;
use soulsteads_messaging::connectors::matrix_indicators::{TypingClient, TypingManager};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::time::Duration;
use tokio::sync::{Mutex, Semaphore};

struct Client {
    calls: Mutex<Vec<(String, bool, u64)>>,
    block: AtomicUsize,
    release: Semaphore,
    active: AtomicUsize,
    maximum: AtomicUsize,
}
impl Default for Client {
    fn default() -> Self {
        Self {
            calls: Mutex::new(vec![]),
            block: AtomicUsize::new(0),
            release: Semaphore::new(0),
            active: AtomicUsize::new(0),
            maximum: AtomicUsize::new(0),
        }
    }
}
#[async_trait]
impl TypingClient for Client {
    async fn set_typing(&self, room: &str, typing: bool, timeout: u64) -> anyhow::Result<()> {
        let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
        self.maximum.fetch_max(active, Ordering::SeqCst);
        let index = {
            let mut calls = self.calls.lock().await;
            calls.push((room.into(), typing, timeout));
            calls.len()
        };
        if self.block.load(Ordering::SeqCst) == index {
            self.release.acquire().await.unwrap().forget();
        }
        self.active.fetch_sub(1, Ordering::SeqCst);
        anyhow::bail!("transient typing failure")
    }
}
async fn calls(client: &Client, count: usize) {
    tokio::time::timeout(Duration::from_secs(1), async {
        while client.calls.lock().await.len() < count {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn room_typing_refresh_is_shared_refcounted_idempotent_and_best_effort() {
    let client = Arc::new(Client::default());
    let manager = TypingManager::new(client.clone(), Duration::from_millis(5));
    let first = manager.start("!room").await;
    let second = manager.start("!room").await;
    calls(&client, 3).await;
    first.stop().await;
    first.stop().await;
    assert!(
        client
            .calls
            .lock()
            .await
            .iter()
            .all(|(_, typing, timeout)| *typing && *timeout == 5000)
    );
    second.stop().await;
    let count = client.calls.lock().await.len();
    assert!(!client.calls.lock().await.last().unwrap().1);
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert_eq!(client.calls.lock().await.len(), count);
    assert_eq!(client.maximum.load(Ordering::SeqCst), 1);
    assert_eq!(manager.room_count().await, 0);
}

#[tokio::test]
async fn start_during_pending_true_never_sends_a_stale_false() {
    let client = Arc::new(Client::default());
    client.block.store(1, Ordering::SeqCst);
    let manager = TypingManager::new(client.clone(), Duration::from_millis(5));
    let first = Arc::new(manager.start("!room").await);
    calls(&client, 1).await;
    let stopping = {
        let first = first.clone();
        tokio::spawn(async move { first.stop().await })
    };
    tokio::time::sleep(Duration::from_millis(2)).await;
    let second = manager.start("!room").await;
    client.release.add_permits(1);
    stopping.await.unwrap();
    calls(&client, 3).await;
    assert!(
        client
            .calls
            .lock()
            .await
            .iter()
            .all(|(_, typing, _)| *typing)
    );
    second.stop().await;
    assert_eq!(client.maximum.load(Ordering::SeqCst), 1);
    assert_eq!(manager.room_count().await, 0);
}

#[tokio::test]
async fn start_during_pending_false_restores_refresh_and_drop_cleans_up() {
    let client = Arc::new(Client::default());
    let manager = TypingManager::new(client.clone(), Duration::from_millis(5));
    let first = Arc::new(manager.start("!room").await);
    calls(&client, 1).await;
    client
        .block
        .store(client.calls.lock().await.len() + 1, Ordering::SeqCst);
    let stopping = {
        let first = first.clone();
        tokio::spawn(async move { first.stop().await })
    };
    let blocked = client.block.load(Ordering::SeqCst);
    calls(&client, blocked).await;
    let second = manager.start("!room").await;
    client.release.add_permits(1);
    stopping.await.unwrap();
    calls(&client, blocked + 2).await;
    assert!(
        client.calls.lock().await[blocked..]
            .iter()
            .all(|(_, typing, _)| *typing)
    );
    drop(second);
    tokio::time::timeout(Duration::from_secs(1), async {
        while manager.room_count().await > 0 {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap();
    assert!(!client.calls.lock().await.last().unwrap().1);
    assert_eq!(client.maximum.load(Ordering::SeqCst), 1);
}
