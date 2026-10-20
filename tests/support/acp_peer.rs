use agent_client_protocol::{
    Agent, Client, ConnectionTo, Lines, Responder, UntypedMessage, schema::v1,
};
use anyhow::{Context, Result};
use futures::StreamExt;
use std::{collections::BTreeMap, path::Path, time::Duration};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    process::{Child, Command},
    sync::{mpsc, oneshot, watch},
};

pub struct Peer {
    pub connection: ConnectionTo<Agent>,
    pub updates: mpsc::UnboundedReceiver<v1::SessionNotification>,
    pub permissions: mpsc::UnboundedReceiver<(
        v1::RequestPermissionRequest,
        Responder<v1::RequestPermissionResponse>,
    )>,
    #[allow(dead_code)] // The shared peer also serves adapters without custom dialogs.
    pub extension_ui: mpsc::UnboundedReceiver<(serde_json::Value, Responder<serde_json::Value>)>,
    child: Child,
    stop: watch::Sender<bool>,
    driver: tokio::task::JoinHandle<()>,
}
impl Peer {
    pub async fn spawn(command: &str, cwd: &Path, env: BTreeMap<String, String>) -> Result<Self> {
        let mut child = Command::new(command)
            .current_dir(cwd)
            .envs(env)
            .kill_on_drop(true)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::inherit())
            .spawn()?;
        let stdin = child.stdin.take().context("stdin")?;
        let stdout = child.stdout.take().context("stdout")?;
        let incoming =
            futures::stream::try_unfold(BufReader::new(stdout), |mut reader| async move {
                let mut line = String::new();
                if reader.read_line(&mut line).await? == 0 {
                    return Ok(None);
                }
                Ok::<_, std::io::Error>(Some((line, reader)))
            })
            .boxed();
        let outgoing = Box::pin(futures::sink::unfold(
            stdin,
            |mut stdin, line: String| async move {
                stdin.write_all(line.as_bytes()).await?;
                stdin.write_all(b"\n").await?;
                stdin.flush().await?;
                Ok::<_, std::io::Error>(stdin)
            },
        ));
        let (ready, connection) = oneshot::channel();
        let (stop, mut stopping) = watch::channel(false);
        let (update, updates) = mpsc::unbounded_channel();
        let (permission, permissions) = mpsc::unbounded_channel();
        let (ui, extension_ui) = mpsc::unbounded_channel();
        let driver = tokio::spawn(async move {
            let _ = Client
                .builder()
                .on_receive_notification(
                    async move |message: v1::SessionNotification, _cx| {
                        let _ = update.send(message);
                        Ok(())
                    },
                    agent_client_protocol::on_receive_notification!(),
                )
                .on_receive_request(
                    async move |request: v1::RequestPermissionRequest, responder, _cx| {
                        let _ = permission.send((request, responder));
                        Ok(())
                    },
                    agent_client_protocol::on_receive_request!(),
                )
                .on_receive_request(
                    async move |request: UntypedMessage,
                                responder: Responder<serde_json::Value>,
                                _cx| {
                        if request.method == "_pi/extension_ui" {
                            let _ = ui.send((request.params, responder));
                            Ok(())
                        } else {
                            responder.respond_with_error(
                                    agent_client_protocol::Error::method_not_found(),
                                )
                        }
                    },
                    agent_client_protocol::on_receive_request!(),
                )
                .connect_with(Lines::new(outgoing, incoming), async move |cx| {
                    let _ = ready.send(cx.clone());
                    tokio::select! {_=cx.incoming_closed()=>{},_=stopping.changed()=>{}}
                    Ok(())
                })
                .await;
        });
        Ok(Self {
            connection: connection.await?,
            updates,
            permissions,
            extension_ui,
            child,
            stop,
            driver,
        })
    }
    pub async fn shutdown(mut self) -> Result<()> {
        self.stop.send_replace(true);
        let driver = tokio::time::timeout(Duration::from_secs(5), &mut self.driver).await;
        if driver.is_err() {
            self.driver.abort();
            let _ = self.driver.await;
        }
        if tokio::time::timeout(Duration::from_secs(8), self.child.wait())
            .await
            .is_err()
        {
            self.child.start_kill()?;
            self.child.wait().await?;
            anyhow::bail!("Adapter failed to stop after stdin EOF");
        }
        Ok(())
    }
}
