//! Claude's stream-JSON control channel is a native CLI protocol, not JSON-RPC.
use anyhow::{Context, Result, bail};
use serde_json::{Value, json};
use std::{
    collections::HashMap,
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    process::{Child, Command},
    sync::{Mutex, mpsc, oneshot, watch},
};

const FRAME_LIMIT: u64 = 8 * 1024 * 1024;
struct Write {
    message: Value,
    lease: Option<(Arc<AtomicBool>, Arc<AtomicBool>)>,
    receipt: oneshot::Sender<Result<()>>,
}
pub(super) struct Process {
    child: Mutex<Child>,
    outgoing: mpsc::Sender<Write>,
    pending: std::sync::Mutex<HashMap<String, oneshot::Sender<Result<Value>>>>,
    stop: watch::Sender<bool>,
    closed: watch::Sender<bool>,
    task: Mutex<Option<tokio::task::JoinHandle<()>>>,
}
impl Drop for Process {
    fn drop(&mut self) {
        self.stop.send_replace(true);
        let _ = self.child.get_mut().start_kill();
    }
}
impl Process {
    pub async fn spawn<F, Fut>(
        command: &str,
        cwd: &Path,
        args: Vec<String>,
        event: F,
    ) -> Result<Arc<Self>>
    where
        F: Fn(Value) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = Result<()>> + Send,
    {
        let mut child = Command::new(command)
            .current_dir(cwd)
            .args(args)
            .kill_on_drop(true)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .context("Starting installed Claude Code")?;
        let mut stdin = child.stdin.take().context("Missing Claude stdin")?;
        let stdout = child.stdout.take().context("Missing Claude stdout")?;
        let mut stderr = child.stderr.take().context("Missing Claude stderr")?;
        let (outgoing, mut writes) = mpsc::channel::<Write>(128);
        let (stop, mut stopping) = watch::channel(false);
        let (closed, _) = watch::channel(false);
        let process = Arc::new(Self {
            child: Mutex::new(child),
            outgoing,
            pending: Default::default(),
            stop,
            closed,
            task: Mutex::new(None),
        });
        tokio::spawn(async move {
            let mut buffer = [0; 4096];
            while matches!(stderr.read(&mut buffer).await, Ok(n) if n > 0) {}
        });
        let weak = Arc::downgrade(&process);
        let task = tokio::spawn(async move {
            let writer = async {
                while let Some(mut write) = writes.recv().await {
                    // Recheck at the physical native write, after any queue delay.
                    if write.lease.as_ref().is_some_and(|(ask, turn)| {
                        !ask.load(Ordering::SeqCst) || !turn.load(Ordering::SeqCst)
                    }) {
                        write.message["response"]["response"] = json!({"behavior":"deny","message":"ACP permission cancelled","interrupt":true});
                    }
                    let mut bytes = serde_json::to_vec(&write.message)?;
                    if bytes.len() as u64 > FRAME_LIMIT {
                        bail!("Claude outbound frame exceeded limit");
                    }
                    bytes.push(b'\n');
                    let result = async {
                        stdin.write_all(&bytes).await?;
                        stdin.flush().await
                    }
                    .await;
                    let failed = result.is_err();
                    let _ = write.receipt.send(result.map_err(Into::into));
                    if failed {
                        bail!("Claude stdin closed");
                    }
                }
                Ok::<_, anyhow::Error>(())
            };
            let reader = async {
                let mut reader = BufReader::new(stdout);
                loop {
                    let mut bytes = Vec::new();
                    let size = (&mut reader)
                        .take(FRAME_LIMIT + 1)
                        .read_until(b'\n', &mut bytes)
                        .await?;
                    if size == 0 {
                        break;
                    }
                    if bytes.len() as u64 > FRAME_LIMIT {
                        bail!("Claude inbound frame exceeded limit");
                    }
                    let message: Value = serde_json::from_slice(&bytes)?;
                    if message["type"] == "control_response" {
                        let response = &message["response"];
                        if let Some(process) = weak.upgrade()
                            && let Some(id) = response["request_id"].as_str()
                            && let Some(sender) = process.pending.lock().unwrap().remove(id)
                        {
                            let result = if response["subtype"] == "success" {
                                Ok(response["response"].clone())
                            } else {
                                Err(anyhow::anyhow!(
                                    "Claude control failed: {}",
                                    response["error"].as_str().unwrap_or("unknown error")
                                ))
                            };
                            let _ = sender.send(result);
                        }
                    } else {
                        event(message).await?;
                    }
                }
                Ok::<_, anyhow::Error>(())
            };
            tokio::select! { _ = writer => {}, _ = reader => {}, _ = stopping.changed() => {} }
            if let Some(process) = weak.upgrade() {
                process.closed.send_replace(true);
                process.pending.lock().unwrap().clear();
                let _ = process.child.lock().await.start_kill();
            }
        });
        *process.task.lock().await = Some(task);
        Ok(process)
    }
    pub fn closed(&self) -> watch::Receiver<bool> {
        self.closed.subscribe()
    }
    pub fn abort(&self) {
        self.closed.send_replace(true);
        self.stop.send_replace(true);
    }
    pub async fn write(
        &self,
        message: Value,
        lease: Option<(Arc<AtomicBool>, Arc<AtomicBool>)>,
    ) -> Result<()> {
        if *self.closed.borrow() {
            bail!("Claude native transport closed");
        }
        let (receipt, written) = oneshot::channel();
        self.outgoing
            .try_send(Write {
                message,
                lease,
                receipt,
            })
            .context("Claude write queue unavailable")?;
        match tokio::time::timeout(Duration::from_secs(30), written).await {
            Ok(receipt) => receipt.context("Claude write failed")?,
            Err(_) => {
                self.abort();
                bail!("Claude write timed out; transport closed");
            }
        }
    }
    pub async fn request(&self, request: Value) -> Result<Value> {
        let id = uuid::Uuid::new_v4().to_string();
        let (sender, response) = oneshot::channel();
        {
            let mut pending = self.pending.lock().unwrap();
            if pending.len() >= 128 {
                bail!("Claude control queue full");
            }
            pending.insert(id.clone(), sender);
        }
        struct Pending<'a>(&'a Process, String);
        impl Drop for Pending<'_> {
            fn drop(&mut self) {
                self.0.pending.lock().unwrap().remove(&self.1);
            }
        }
        let _pending = Pending(self, id.clone());
        self.write(
            json!({"type":"control_request","request_id":id,"request":request}),
            None,
        )
        .await?;
        match tokio::time::timeout(Duration::from_secs(30), response).await {
            Ok(response) => response.context("Claude control transport closed")?,
            Err(_) => {
                self.abort();
                bail!("Claude control timed out; transport closed");
            }
        }
    }
    pub async fn shutdown(&self) -> Result<()> {
        self.abort();
        if let Some(mut task) = self.task.lock().await.take()
            && tokio::time::timeout(Duration::from_secs(5), &mut task)
                .await
                .is_err()
        {
            task.abort();
            let _ = task.await;
        }
        let mut child = self.child.lock().await;
        if tokio::time::timeout(Duration::from_secs(5), child.wait())
            .await
            .is_err()
        {
            child.start_kill()?;
            child.wait().await?;
        }
        Ok(())
    }
}
