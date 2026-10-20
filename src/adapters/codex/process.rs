use agent_client_protocol::{ConnectionTo, Lines, Responder, UntypedMessage, UntypedRole};
use anyhow::{Context, Result, bail};
use futures::StreamExt;
use serde_json::{Value, json};
use std::{
    collections::HashMap,
    sync::{
        Arc, OnceLock,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    process::{Child, Command},
    sync::{Mutex, Semaphore, oneshot, watch},
};

const FRAME_LIMIT: u64 = 8 * 1024 * 1024;
pub(super) struct Process {
    child: Mutex<Child>,
    connection: OnceLock<ConnectionTo<UntypedRole>>,
    stop: watch::Sender<bool>,
    closed: watch::Sender<bool>,
    task: Mutex<Option<tokio::task::JoinHandle<()>>>,
    requests: Semaphore,
    approval_writes: std::sync::Mutex<HashMap<String, Arc<AtomicBool>>>,
}
impl Drop for Process {
    fn drop(&mut self) {
        self.stop.send_replace(true);
        let _ = self.child.get_mut().start_kill();
    }
}
impl Process {
    pub async fn spawn<N, NF, R, RF>(
        command: &str,
        notification: N,
        request: R,
    ) -> Result<Arc<Self>>
    where
        N: Fn(UntypedMessage) -> NF + Send + Sync + 'static,
        NF: std::future::Future<Output = Result<()>> + Send,
        R: Fn(UntypedMessage, Responder<Value>) -> RF + Send + Sync + 'static,
        RF: std::future::Future<Output = Result<()>> + Send,
    {
        // Inherit the adapter's process group so the bridge can reap the entire
        // adapter/CLI tree even if the adapter is forcibly terminated.
        let mut child = Command::new(command)
            .args(["app-server", "--listen", "stdio://"])
            .kill_on_drop(true)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .context("Starting installed Codex app server")?;
        let stdin = child.stdin.take().context("Missing Codex stdin")?;
        let stdout = child.stdout.take().context("Missing Codex stdout")?;
        let mut stderr = child.stderr.take().context("Missing Codex stderr")?;
        let (stop, mut stopping) = watch::channel(false);
        let (closed, _) = watch::channel(false);
        let process = Arc::new(Self {
            child: Mutex::new(child),
            connection: OnceLock::new(),
            stop,
            closed,
            task: Mutex::new(None),
            requests: Semaphore::new(128),
            approval_writes: Default::default(),
        });
        tokio::spawn(async move {
            let mut buffer = [0; 4096];
            while matches!(stderr.read(&mut buffer).await, Ok(n) if n > 0) {}
        });
        let incoming =
            futures::stream::try_unfold(BufReader::new(stdout), |mut reader| async move {
                let mut bytes = Vec::new();
                let size = (&mut reader)
                    .take(FRAME_LIMIT + 1)
                    .read_until(b'\n', &mut bytes)
                    .await?;
                if size == 0 {
                    return Ok(None);
                }
                if bytes.len() as u64 > FRAME_LIMIT {
                    return Err(std::io::Error::other("Codex frame exceeded limit"));
                }
                let mut message: Value =
                    serde_json::from_slice(&bytes).map_err(std::io::Error::other)?;
                // Codex omits JSON-RPC's version field; let the SDK own the rest of
                // framing, IDs, dispatch, request cancellation, and response routing.
                if !message.is_object() {
                    return Err(std::io::Error::other(
                        "Codex sent an invalid JSON-RPC frame",
                    ));
                }
                message
                    .as_object_mut()
                    .unwrap()
                    .entry("jsonrpc")
                    .or_insert("2.0".into());
                Ok(Some((message.to_string(), reader)))
            })
            .boxed();
        let weak = Arc::downgrade(&process);
        let wire = weak.clone();
        let outgoing = Box::pin(futures::sink::unfold(
            stdin,
            move |mut stdin, line: String| {
                let weak = wire.clone();
                async move {
                    if line.len() as u64 > FRAME_LIMIT {
                        return Err(std::io::Error::other("Codex frame exceeded limit"));
                    }
                    let mut message: Value =
                        serde_json::from_str(&line).map_err(std::io::Error::other)?;
                    if let Some(process) = weak.upgrade()
                        && let Some(current) = process
                            .approval_writes
                            .lock()
                            .unwrap()
                            .remove(&message["id"].to_string())
                        && !current.load(Ordering::SeqCst)
                    {
                        message["result"] = json!({"decision":"cancel"});
                    }
                    message
                        .as_object_mut()
                        .ok_or_else(|| std::io::Error::other("Invalid Codex frame"))?
                        .remove("jsonrpc");
                    let bytes = serde_json::to_vec(&message).map_err(std::io::Error::other)?;
                    stdin.write_all(&bytes).await?;
                    stdin.write_all(b"\n").await?;
                    stdin.flush().await?;
                    Ok::<_, std::io::Error>(stdin)
                }
            },
        ));
        let (ready, connected) = oneshot::channel();
        let task =
            tokio::spawn(async move {
                let result = UntypedRole
                .builder()
                .on_receive_notification(
                    async move |message: UntypedMessage, _cx| {
                        notification(message).await.map_err(crate::adapters::protocol_error)
                    },
                    agent_client_protocol::on_receive_notification!(),
                )
                .on_receive_request(
                    async move |message: UntypedMessage, responder, _cx| {
                        request(message, responder)
                            .await
                            .map_err(crate::adapters::protocol_error)
                    },
                    agent_client_protocol::on_receive_request!(),
                )
                .connect_with(Lines::new(outgoing, incoming), async move |cx| {
                    let _ = ready.send(cx.clone());
                    tokio::select! { _ = stopping.changed() => {}, _ = cx.incoming_closed() => {} }
                    Ok(())
                })
                .await;
                if let Some(process) = weak.upgrade() {
                    process.closed.send_replace(true);
                    process.approval_writes.lock().unwrap().clear();
                    if result.is_err() {
                        let _ = process.child.lock().await.start_kill();
                    }
                }
            });
        process
            .connection
            .set(connected.await.context("Codex SDK transport closed")?)
            .ok();
        *process.task.lock().await = Some(task);
        Ok(process)
    }
    pub fn closed(&self) -> watch::Receiver<bool> {
        self.closed.subscribe()
    }
    pub async fn request(&self, method: &str, params: Value) -> Result<Value> {
        if *self.closed.borrow() {
            bail!("Codex app server closed");
        }
        let _permit = self
            .requests
            .try_acquire()
            .context("Codex request queue full")?;
        let result = tokio::time::timeout(
            Duration::from_secs(30),
            self.connection
                .get()
                .context("Codex connection unavailable")?
                .send_request(UntypedMessage::new(method, params)?)
                .block_task(),
        )
        .await;
        match result {
            Ok(result) => result.map_err(Into::into),
            Err(_) => {
                // The native operation may already have started. Closing its
                // transport prevents a second coding turn from being submitted
                // while the result of this operation is unknown.
                self.closed.send_replace(true);
                self.stop.send_replace(true);
                bail!("Codex control request timed out");
            }
        }
    }
    pub fn notify(&self, method: &str, params: Value) -> Result<()> {
        self.connection
            .get()
            .context("Codex connection unavailable")?
            .send_notification(UntypedMessage::new(method, params)?)?;
        Ok(())
    }
    pub fn respond_approval(
        &self,
        responder: Responder<Value>,
        result: Value,
        current: Arc<AtomicBool>,
    ) -> Result<()> {
        let key = serde_json::to_value(responder.id())?.to_string();
        self.approval_writes
            .lock()
            .unwrap()
            .insert(key.clone(), current);
        if let Err(error) = responder.respond(result) {
            self.approval_writes.lock().unwrap().remove(&key);
            return Err(error.into());
        }
        Ok(())
    }
    pub async fn shutdown(&self) -> Result<()> {
        self.stop.send_replace(true);
        if let Some(mut task) = self.task.lock().await.take()
            && tokio::time::timeout(Duration::from_secs(5), &mut task)
                .await
                .is_err()
        {
            task.abort();
            let _ = task.await;
            let _ = self.child.lock().await.start_kill();
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
