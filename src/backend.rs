pub use crate::acp::{InlineImage, Options, Output, Response};
use anyhow::{Result, bail};
use serde_json::Value;
use tokio::sync::{broadcast, mpsc};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Transport {
    Acp,
    OpenCodeNative,
}
impl Transport {
    pub fn select(backend: &str, config: &Value) -> Result<Self> {
        let transport = match config.get("transport") {
            None => "acp",
            Some(value) => value
                .as_str()
                .ok_or_else(|| anyhow::anyhow!("Backend transport must be an explicit string"))?,
        };
        match transport {
            "acp" => Ok(Self::Acp),
            "native" if backend == "opencode" => Ok(Self::OpenCodeNative),
            _ => bail!("Unsupported transport for selected backend"),
        }
    }
}

#[derive(Clone)]
pub enum Client {
    Acp(crate::acp::Client),
    Native(crate::native_client::Client),
}
pub(crate) enum PermissionGuard {
    Acp {
        _guard: crate::acp::PermissionGuard,
    },
    Native {
        _guard: crate::native_client::PermissionGuard,
    },
}
macro_rules! dispatch {
    ($self:expr, $method:ident $(, $argument:expr)*) => {
        match $self { Client::Acp(client) => client.$method($($argument),*), Client::Native(client) => client.$method($($argument),*) }
    };
}
macro_rules! dispatch_async {
    ($self:expr, $method:ident $(, $argument:expr)*) => {
        match $self { Client::Acp(client) => client.$method($($argument),*).await, Client::Native(client) => client.$method($($argument),*).await }
    };
}
impl Client {
    pub fn transport_name(&self) -> &'static str {
        match self {
            Self::Acp(_) => "ACP",
            Self::Native(_) => "OpenCode native",
        }
    }
    pub async fn connect(
        transport: Transport,
        mut options: Options,
        servers: Vec<Value>,
    ) -> Result<Self> {
        match transport {
            Transport::Acp => Ok(Self::Acp(
                crate::acp::Client::connect_with_servers(options, servers).await?,
            )),
            Transport::OpenCodeNative => {
                if options.args == ["acp"] {
                    options.args.clear();
                }
                if options.args.iter().any(|argument| argument == "acp") {
                    bail!("ACP arguments cannot be used for native serve");
                }
                Ok(Self::Native(
                    crate::native_client::Client::connect(options, servers).await?,
                ))
            }
        }
    }
    pub fn subscribe(&self) -> broadcast::Receiver<Value> {
        dispatch!(self, subscribe)
    }
    pub fn working_directory(&self) -> &std::path::Path {
        dispatch!(self, working_directory)
    }
    pub fn accepts_followups(&self) -> bool {
        dispatch!(self, accepts_followups)
    }
    pub async fn session_id(&self) -> Option<String> {
        dispatch_async!(self, session_id)
    }
    pub async fn inline_images(&self) -> Vec<InlineImage> {
        dispatch_async!(self, inline_images)
    }
    pub async fn prompt_observation(&self) -> (Output, Option<String>, Value) {
        dispatch_async!(self, prompt_observation)
    }
    pub async fn config_options(&self) -> Vec<Value> {
        dispatch_async!(self, config_options)
    }
    pub async fn modes(&self) -> Value {
        dispatch_async!(self, modes)
    }
    pub async fn commands(&self) -> Vec<Value> {
        dispatch_async!(self, commands)
    }
    pub async fn new_session(&self) -> Result<String> {
        dispatch_async!(self, new_session)
    }
    pub async fn load_session(&self, session: &str, resume: bool) -> Result<Vec<Value>> {
        dispatch_async!(self, load_session, session, resume)
    }
    pub async fn list_sessions(&self) -> Result<Vec<Value>> {
        dispatch_async!(self, list_sessions)
    }
    pub async fn close_session(&self) -> Result<()> {
        dispatch_async!(self, close_session)
    }
    pub async fn delete_session(&self) -> Result<()> {
        dispatch_async!(self, delete_session)
    }
    pub async fn prompt(&self, text: &str, images: Vec<Value>) -> Result<Response> {
        dispatch_async!(self, prompt, text, images)
    }
    pub async fn forward(&self, text: &str) -> Result<()> {
        dispatch_async!(self, forward, text)
    }
    pub async fn forward_content(&self, text: &str, images: Vec<Value>) -> Result<()> {
        match self {
            Self::Native(client) => client.forward_content(text, images).await,
            Self::Acp(client) if images.is_empty() => client.forward(text).await,
            Self::Acp(_) => {
                bail!("ACP backend cannot queue image follow-ups; nothing was submitted")
            }
        }
    }
    pub async fn cancel(&self) -> Result<()> {
        dispatch_async!(self, cancel)
    }
    pub async fn set_mode(&self, mode: &str) -> Result<()> {
        dispatch_async!(self, set_mode, mode)
    }
    pub async fn set_option(&self, id: &str, value: &str) -> Result<Value> {
        dispatch_async!(self, set_option, id, value)
    }
    pub async fn request(&self, method: &str, params: Value) -> Result<Value> {
        dispatch_async!(self, request, method, params)
    }
    pub async fn shutdown(&self) -> Result<()> {
        dispatch_async!(self, shutdown)
    }
    pub(crate) fn form_handler(
        &self,
    ) -> Option<(
        crate::native_client::FormGuard,
        mpsc::Receiver<crate::native_client::FormRequest>,
    )> {
        match self {
            Self::Native(client) => Some(client.form_handler()),
            Self::Acp(_) => None,
        }
    }
    pub(crate) fn permission_handler(
        &self,
    ) -> (
        mpsc::Receiver<crate::acp::PermissionRequest>,
        PermissionGuard,
    ) {
        match self {
            Self::Acp(client) => {
                let (receiver, guard) = client.permission_handler();
                (receiver, PermissionGuard::Acp { _guard: guard })
            }
            Self::Native(client) => {
                let (receiver, guard) = client.permission_handler();
                (receiver, PermissionGuard::Native { _guard: guard })
            }
        }
    }
    #[cfg(test)]
    pub(crate) fn fail_expiry_shutdown_once(&self) {
        if let Self::Acp(client) = self {
            client.fail_expiry_shutdown_once();
        }
    }
    #[cfg(test)]
    pub(crate) async fn hold_expiry_shutdown(
        &self,
    ) -> tokio::sync::MutexGuard<'_, tokio::process::Child> {
        match self {
            Self::Acp(client) => client.hold_expiry_shutdown().await,
            Self::Native(_) => panic!("ACP-only test hook"),
        }
    }
    #[cfg(test)]
    pub(crate) fn same_expiry_client(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Acp(first), Self::Acp(second)) => first.same_expiry_client(second),
            _ => false,
        }
    }
}
