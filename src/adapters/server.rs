//! Shared SDK serving lifecycle. Native backend protocols stay in each adapter.
use super::{AcpConnection, protocol_error};
use agent_client_protocol::{Agent, Responder, Stdio, UntypedMessage, schema::v1};
use anyhow::Result;
use futures::future::BoxFuture;
use serde_json::Value;
use std::sync::{Arc, OnceLock};
use tokio::sync::watch;

pub(super) trait Handler: Send + Sync + 'static {
    fn connection(&self) -> &OnceLock<AcpConnection>;
    fn methods(&self) -> &'static [&'static str];
    fn stopping(&self) -> watch::Receiver<bool>;
    fn request(self: Arc<Self>, message: UntypedMessage) -> BoxFuture<'static, Result<Value>>;
    fn cancel(self: Arc<Self>, session: String) -> BoxFuture<'static, Result<()>>;
    fn shutdown(self: Arc<Self>) -> BoxFuture<'static, Result<()>>;
}

pub(super) async fn serve<S: Handler>(state: Arc<S>) -> Result<()> {
    let requests = state.clone();
    let notifications = state.clone();
    let connected = state.clone();
    let mut stopping = state.stopping();
    let result = Agent
        .builder()
        .on_receive_request(
            async move |message: UntypedMessage, responder: Responder<Value>, cx| {
                requests.connection().set(cx).ok();
                if !requests.methods().contains(&message.method.as_str()) {
                    return responder
                        .respond_with_error(agent_client_protocol::Error::method_not_found());
                }
                let state = requests.clone();
                tokio::spawn(async move {
                    let result = state.request(message).await.map_err(protocol_error);
                    let _ = responder.respond_with_result(result);
                });
                Ok(())
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_notification(
            async move |message: UntypedMessage, _cx| {
                if message.method == "session/cancel" {
                    let params: v1::CancelNotification =
                        serde_json::from_value(message.params).map_err(protocol_error)?;
                    let state = notifications.clone();
                    tokio::spawn(async move {
                        if let Err(error) = state.cancel(params.session_id.to_string()).await {
                            eprintln!("ACP cancellation failed: {error}");
                        }
                    });
                }
                Ok(())
            },
            agent_client_protocol::on_receive_notification!(),
        )
        .connect_with(Stdio::new(), async move |cx| {
            connected.connection().set(cx.clone()).ok();
            tokio::select! {_=cx.incoming_closed()=>{},_=stopping.changed()=>{}}
            Ok(())
        })
        .await;
    let cleanup = state.shutdown().await;
    result?;
    cleanup
}
