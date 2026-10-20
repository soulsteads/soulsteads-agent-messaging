//! Pi's bounded NDJSON command/event protocol (not JSON-RPC).
use anyhow::{Context, Result, bail};
use serde_json::Value;
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
pub(super) struct Lease {
    pub ask: Arc<AtomicBool>,
    pub turn: Arc<AtomicBool>,
    pub deadline: Option<tokio::time::Instant>,
}
struct Write {
    message: Value,
    lease: Option<Lease>,
    admission: Option<Arc<AtomicBool>>,
    receipt: oneshot::Sender<Result<()>>,
}
struct Pending {
    command: String,
    reply: oneshot::Sender<Result<Value>>,
}
pub(super) struct Process {
    child: Mutex<Child>,
    writes: mpsc::Sender<Write>,
    pending: std::sync::Mutex<HashMap<String, Pending>>,
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
            .args(args)
            .current_dir(cwd)
            .kill_on_drop(true)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .context(
                "Starting installed Pi; install @earendil-works/pi-coding-agent or set PI_PATH",
            )?;
        let mut stdin = child.stdin.take().context("Missing Pi stdin")?;
        let stdout = child.stdout.take().context("Missing Pi stdout")?;
        let mut stderr = child.stderr.take().context("Missing Pi stderr")?;
        let (writes, mut queue) = mpsc::channel::<Write>(128);
        let (stop, mut stopping) = watch::channel(false);
        let (closed, _) = watch::channel(false);
        let process = Arc::new(Self {
            child: Mutex::new(child),
            writes,
            pending: Default::default(),
            stop,
            closed,
            task: Mutex::new(None),
        });
        tokio::spawn(async move {
            let mut buffer = [0; 4096];
            while matches!(stderr.read(&mut buffer).await,Ok(n) if n>0) {}
        });
        let weak = Arc::downgrade(&process);
        let task = tokio::spawn(async move {
            let writer = async {
                while let Some(write) = queue.recv().await {
                    let mut bytes = serde_json::to_vec(&write.message)?;
                    if bytes.len() as u64 > FRAME_LIMIT {
                        bail!("Pi outbound frame exceeded limit");
                    }
                    if write
                        .admission
                        .as_ref()
                        .is_some_and(|live| !live.load(Ordering::SeqCst))
                    {
                        let _ = write.receipt.send(Err(anyhow::anyhow!(
                            "Pi prompt cancelled before native write"
                        )));
                        continue;
                    }
                    if write.lease.as_ref().is_some_and(|lease| {
                        !lease.ask.load(Ordering::SeqCst)
                            || !lease.turn.load(Ordering::SeqCst)
                            || lease
                                .deadline
                                .is_some_and(|deadline| tokio::time::Instant::now() >= deadline)
                    }) {
                        let cancelled = serde_json::json!({"type":"extension_ui_response","id":write.message["id"],"cancelled":true});
                        bytes = serde_json::to_vec(&cancelled)?;
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
                        bail!("Pi stdin closed");
                    }
                }
                Ok::<_, anyhow::Error>(())
            };
            let reader = async {
                let mut reader = BufReader::new(stdout);
                let mut prelude_bytes = 0;
                let mut protocol_started = false;
                loop {
                    let mut bytes = Vec::new();
                    if (&mut reader)
                        .take(FRAME_LIMIT + 1)
                        .read_until(b'\n', &mut bytes)
                        .await?
                        == 0
                    {
                        break;
                    }
                    if bytes.len() as u64 > FRAME_LIMIT {
                        bail!("Pi inbound frame exceeded limit");
                    }
                    if bytes.iter().all(u8::is_ascii_whitespace) {
                        continue;
                    }
                    let message: Value = match serde_json::from_slice(&bytes) {
                        Ok(value) => value,
                        Err(_) if !protocol_started && bytes.first() != Some(&b'{') => {
                            prelude_bytes += bytes.len();
                            if prelude_bytes > 65536 {
                                bail!("Pi startup prelude exceeded limit");
                            }
                            continue;
                        }
                        Err(error) => return Err(error.into()),
                    };
                    protocol_started = true;
                    if message["type"] == "response" {
                        if let Some(process) = weak.upgrade()
                            && let Some(id) = message["id"].as_str()
                            && let Some(pending) = process.pending.lock().unwrap().remove(id)
                        {
                            if message["command"] != pending.command {
                                bail!("Pi command response did not match its request");
                            }
                            let result = if message["success"] == true {
                                Ok(message["data"].clone())
                            } else {
                                Err(anyhow::anyhow!(
                                    "Pi {} failed: {}",
                                    pending.command,
                                    message["error"].as_str().unwrap_or("unknown native error")
                                ))
                            };
                            let _ = pending.reply.send(result);
                        }
                    } else {
                        event(message).await?;
                    }
                }
                Ok::<_, anyhow::Error>(())
            };
            tokio::select! {_=writer=>{},_=reader=>{},_=stopping.changed()=>{}}
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
    pub async fn write(&self, message: Value, lease: Option<Lease>) -> Result<()> {
        self.write_guarded(message, lease, None).await
    }
    async fn write_guarded(
        &self,
        message: Value,
        lease: Option<Lease>,
        admission: Option<Arc<AtomicBool>>,
    ) -> Result<()> {
        if *self.closed.borrow() {
            bail!("Pi transport closed");
        }
        let (receipt, written) = oneshot::channel();
        self.writes
            .try_send(Write {
                message,
                lease,
                admission,
                receipt,
            })
            .context("Pi write queue unavailable")?;
        match tokio::time::timeout(Duration::from_secs(30), written).await {
            Ok(receipt) => receipt.context("Pi write failed")?,
            Err(_) => {
                self.abort();
                bail!("Pi write timed out; transport closed");
            }
        }
    }
    pub async fn request(&self, message: Value) -> Result<Value> {
        self.request_with_timeout(message, Some(Duration::from_secs(30)), None)
            .await
    }
    pub async fn prompt(&self, message: Value, live: Arc<AtomicBool>) -> Result<Value> {
        self.request_with_timeout(message, Some(Duration::from_secs(30)), Some(live))
            .await
    }
    pub async fn execute(&self, message: Value) -> Result<Value> {
        self.request_with_timeout(message, None, None).await
    }
    async fn request_with_timeout(
        &self,
        mut message: Value,
        timeout: Option<Duration>,
        admission: Option<Arc<AtomicBool>>,
    ) -> Result<Value> {
        let command = message["type"]
            .as_str()
            .context("Pi command type missing")?
            .to_owned();
        let id = uuid::Uuid::new_v4().to_string();
        message["id"] = id.clone().into();
        let (reply, response) = oneshot::channel();
        {
            let mut pending = self.pending.lock().unwrap();
            if pending.len() >= 128 {
                bail!("Pi pending command limit exceeded");
            }
            pending.insert(id.clone(), Pending { command, reply });
        }
        struct Guard<'a>(&'a Process, String);
        impl Drop for Guard<'_> {
            fn drop(&mut self) {
                self.0.pending.lock().unwrap().remove(&self.1);
            }
        }
        let _guard = Guard(self, id);
        self.write_guarded(message, None, admission).await?;
        if let Some(timeout) = timeout {
            match tokio::time::timeout(timeout, response).await {
                Ok(response) => response.context("Pi response transport closed")?,
                Err(_) => {
                    self.abort();
                    bail!("Pi command timed out; transport closed");
                }
            }
        } else {
            response.await.context("Pi response transport closed")?
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
