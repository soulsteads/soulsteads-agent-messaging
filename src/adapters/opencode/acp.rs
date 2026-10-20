//! Bidirectional SDK forwarding. Native OpenCode owns auth, tools and sessions.
use crate::adapters::{AcpConnection, mcp_sse, protocol_error};
use agent_client_protocol::{Agent, Client, ConnectionTo, Lines, Responder, UntypedMessage};
use anyhow::{Context, Result, ensure};
use futures::StreamExt;
use serde_json::{Value, json};
use std::{
    collections::HashMap,
    pin::Pin,
    sync::{Arc, Mutex, OnceLock},
    time::Duration,
};
use tokio::{
    io::{AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader},
    sync::{Semaphore, oneshot, watch},
};

const LIMIT: usize = 8 * 1024 * 1024;
type Transport = Lines<
    Pin<Box<dyn futures::Sink<String, Error = std::io::Error> + Send>>,
    Pin<Box<dyn futures::Stream<Item = std::io::Result<String>> + Send>>,
>;

fn transport<R, W>(reader: R, writer: W) -> Transport
where
    R: AsyncRead + Send + Unpin + 'static,
    W: AsyncWrite + Send + Unpin + 'static,
{
    let incoming = futures::stream::try_unfold(BufReader::new(reader), |mut reader| async move {
        let mut bytes = Vec::new();
        if (&mut reader)
            .take((LIMIT + 1) as u64)
            .read_until(b'\n', &mut bytes)
            .await?
            == 0
        {
            return Ok(None);
        }
        if bytes.len() > LIMIT {
            return Err(std::io::Error::other("OpenCode ACP frame exceeded limit"));
        }
        let line = String::from_utf8(bytes).map_err(std::io::Error::other)?;
        Ok(Some((line, reader)))
    })
    .boxed();
    let outgoing: Pin<Box<dyn futures::Sink<String, Error = std::io::Error> + Send>> = Box::pin(
        futures::sink::unfold(writer, |mut writer, line: String| async move {
            if line.len() > LIMIT {
                return Err(std::io::Error::other("OpenCode ACP frame exceeded limit"));
            }
            writer.write_all(line.as_bytes()).await?;
            writer.write_all(b"\n").await?;
            writer.flush().await?;
            Ok::<_, std::io::Error>(writer)
        }),
    );
    Lines::new(outgoing, incoming)
}

struct State {
    client: OnceLock<AcpConnection>,
    native: OnceLock<ConnectionTo<Agent>>,
    operations: Arc<Semaphore>,
    callbacks: Arc<Semaphore>,
    // The first native prompt retains its lease when an overlapping request fails.
    prompts: Mutex<HashMap<String, watch::Sender<bool>>>,
    model: Option<String>,
}
impl State {
    fn client(&self) -> agent_client_protocol::Result<&AcpConnection> {
        self.client
            .get()
            .ok_or_else(|| protocol_error("ACP client unavailable"))
    }
    fn native(&self) -> agent_client_protocol::Result<&ConnectionTo<Agent>> {
        self.native
            .get()
            .ok_or_else(|| protocol_error("OpenCode ACP unavailable"))
    }
    fn request(
        self: &Arc<Self>,
        mut message: UntypedMessage,
        responder: Responder<Value>,
    ) -> agent_client_protocol::Result<()> {
        let permit = match self.operations.clone().try_acquire_owned() {
            Ok(permit) => permit,
            Err(_) => {
                return responder
                    .respond_with_error(protocol_error("OpenCode ACP request limit exceeded"));
            }
        };
        if matches!(
            message.method.as_str(),
            "session/new" | "session/load" | "session/resume"
        ) {
            let result = (|| {
                if let Some(servers) = message.params.get_mut("mcpServers") {
                    let servers = servers
                        .as_array_mut()
                        .context("MCP servers must be an array")?;
                    ensure!(servers.len() <= 64, "MCP server limit exceeded");
                    for server in servers {
                        *server = mcp_sse::adapt(server)?;
                    }
                }
                Ok::<_, anyhow::Error>(())
            })();
            if let Err(error) = result {
                return responder.respond_with_error(protocol_error(error));
            }
        }
        let method = message.method.clone();
        let session = message.params["sessionId"].as_str().map(str::to_owned);
        let lease = if method == "session/prompt" {
            let mut prompts = self.prompts.lock().unwrap();
            session
                .as_ref()
                .filter(|id| !prompts.contains_key(*id))
                .map(|id| {
                    let lease = watch::channel(false).0;
                    prompts.insert(id.clone(), lease.clone());
                    lease
                })
        } else {
            None
        };
        // Send in dispatch order; only the response wait is moved to a task.
        let future = self.native()?.send_request(message).block_task();
        let state = self.clone();
        tokio::spawn(async move {
            let _permit = permit;
            let mut result = future.await;
            if method == "initialize" {
                if let Ok(value) = &mut result {
                    value["agentCapabilities"]["mcpCapabilities"]["sse"] = true.into();
                }
            } else if matches!(
                method.as_str(),
                "session/new" | "session/load" | "session/resume"
            ) && let (Some(model), Ok(info)) = (&state.model, &mut result)
            {
                let id = info["sessionId"]
                    .as_str()
                    .map(str::to_owned)
                    .or(session.clone());
                let configured = match id.as_ref() {
                    Some(id) => {
                        state
                            .native()
                            .unwrap()
                            .send_request(
                                UntypedMessage::new(
                                    "session/set_config_option",
                                    json!({"sessionId":id,"configId":"model","value":model}),
                                )
                                .unwrap(),
                            )
                            .block_task()
                            .await
                    }
                    None => Err(protocol_error("OpenCode omitted native session ID")),
                };
                match configured {
                    Ok(options) => {
                        info["configOptions"] = options["configOptions"].clone();
                    }
                    Err(error) => {
                        // Delete only a session this request created. Loaded history remains owned by the user.
                        if method == "session/new"
                            && let Some(id) = id
                        {
                            let _ = state
                                .native()
                                .unwrap()
                                .send_request(
                                    UntypedMessage::new("session/delete", json!({"sessionId":id}))
                                        .unwrap(),
                                )
                                .block_task()
                                .await;
                        }
                        result = Err(error);
                    }
                }
            }
            if let (Some(session), Some(lease)) = (session, lease) {
                let mut prompts = state.prompts.lock().unwrap();
                lease.send_replace(true);
                prompts.remove(&session);
            }
            let _ = responder.respond_with_result(result);
        });
        Ok(())
    }
    fn callback(
        self: &Arc<Self>,
        message: UntypedMessage,
        responder: Responder<Value>,
    ) -> agent_client_protocol::Result<()> {
        let permit = match self.callbacks.clone().try_acquire_owned() {
            Ok(permit) => permit,
            Err(_) => {
                return responder
                    .respond_with_error(protocol_error("OpenCode ACP callback limit exceeded"));
            }
        };
        let permission = message.method == "session/request_permission";
        let lease = permission
            .then(|| {
                self.prompts
                    .lock()
                    .unwrap()
                    .get(message.params["sessionId"].as_str().unwrap_or(""))
                    .map(watch::Sender::subscribe)
            })
            .flatten();
        if permission && lease.as_ref().is_none_or(|lease| *lease.borrow()) {
            return responder.respond(json!({"outcome":{"outcome":"cancelled"}}));
        }
        let future = self.client()?.send_request(message).block_task();
        let state = self.clone();
        tokio::spawn(async move {
            let _permit = permit;
            let cancelled = || Ok(json!({"outcome":{"outcome":"cancelled"}}));
            let mut lease = lease;
            let result = if let Some(lease) = lease.as_mut() {
                let result = tokio::select! {answer=future=>answer, _=lease.changed()=>cancelled()};
                if *lease.borrow() { cancelled() } else { result }
            } else {
                future.await
            };
            // Serialize the final lease check with cancellation dispatch.
            let _guard = state.prompts.lock().unwrap();
            let result = if permission && lease.as_ref().is_none_or(|lease| *lease.borrow()) {
                cancelled()
            } else {
                result
            };
            let _ = responder.respond_with_result(result);
        });
        Ok(())
    }
    fn notification(&self, message: UntypedMessage) -> agent_client_protocol::Result<()> {
        if matches!(message.method.as_str(), "session/cancel")
            && let Some(lease) = self
                .prompts
                .lock()
                .unwrap()
                .get(message.params["sessionId"].as_str().unwrap_or(""))
        {
            lease.send_replace(true);
        }
        self.native()?.send_notification(message)
    }
}

pub(super) async fn run() -> Result<()> {
    let model = std::env::var("OPENCODE_ADAPTER_MODEL").ok();
    if let Some(model) = &model {
        ensure!(
            !model.is_empty()
                && model.len() <= 1024
                && model
                    .split_once('/')
                    .is_some_and(|(provider, id)| !provider.is_empty() && !id.is_empty()),
            "OPENCODE_ADAPTER_MODEL must be provider/model"
        );
    }
    let state = Arc::new(State {
        client: OnceLock::new(),
        native: OnceLock::new(),
        operations: Arc::new(Semaphore::new(128)),
        callbacks: Arc::new(Semaphore::new(64)),
        prompts: Default::default(),
        model,
    });
    let command = std::env::var("OPENCODE_PATH").unwrap_or_else(|_| "opencode".into());
    let mut child = tokio::process::Command::new(command)
        .arg("acp")
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::inherit())
        .kill_on_drop(true)
        .spawn()
        .context("Starting installed OpenCode ACP")?;
    let transport = transport(
        child.stdout.take().context("OpenCode stdout missing")?,
        child.stdin.take().context("OpenCode stdin missing")?,
    );
    let (ready, connected) = oneshot::channel();
    let (stop, mut stopping) = watch::channel(false);
    let (closed, mut closing) = watch::channel(false);
    let requests = state.clone();
    let notifications = state.clone();
    let native = state.clone();
    let driver = tokio::spawn(async move {
        let result = Client
            .builder()
            .on_receive_request(
                async move |message: UntypedMessage, responder: Responder<Value>, _cx| {
                    requests.callback(message, responder)
                },
                agent_client_protocol::on_receive_request!(),
            )
            .on_receive_notification(
                async move |message: UntypedMessage, _cx| {
                    notifications.client()?.send_notification(message)
                },
                agent_client_protocol::on_receive_notification!(),
            )
            .connect_with(transport, async move |cx| {
                native.native.set(cx.clone()).ok();
                let _ = ready.send(());
                tokio::select! {_=cx.incoming_closed()=>{},_=stopping.changed()=>{}}
                Ok(())
            })
            .await;
        closed.send_replace(true);
        result
    });
    connected.await.context("OpenCode ACP connection failed")?;
    let requests = state.clone();
    let notifications = state.clone();
    let connected = state.clone();
    let result = Agent
        .builder()
        .on_receive_request(
            async move |message: UntypedMessage, responder: Responder<Value>, cx| {
                requests.client.set(cx).ok();
                requests.request(message, responder)
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_notification(
            async move |message: UntypedMessage, _cx| notifications.notification(message),
            agent_client_protocol::on_receive_notification!(),
        )
        .connect_with(transport_stdio(), async move |cx| {
            connected.client.set(cx.clone()).ok();
            if !*closing.borrow() {
                tokio::select! {_=cx.incoming_closed()=>{},_=closing.changed()=>{}}
            }
            Ok(())
        })
        .await;
    for lease in state.prompts.lock().unwrap().values() {
        lease.send_replace(true);
    }
    stop.send_replace(true);
    let native_result = driver.await?;
    let status = match tokio::time::timeout(Duration::from_secs(5), child.wait()).await {
        Ok(status) => status?,
        Err(_) => {
            child.start_kill()?;
            child.wait().await?;
            anyhow::bail!("OpenCode ACP did not stop after stdin EOF");
        }
    };
    result?;
    native_result?;
    ensure!(status.success(), "OpenCode ACP exited with {status}");
    Ok(())
}

fn transport_stdio() -> Transport {
    transport(tokio::io::stdin(), tokio::io::stdout())
}
