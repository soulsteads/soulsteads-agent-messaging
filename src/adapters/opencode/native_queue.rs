use anyhow::{Context, Result, bail};
use serde_json::{Value, json};
use std::net::IpAddr;
#[cfg(test)]
type TestIo = std::sync::Arc<
    tokio::sync::Mutex<
        Option<(
            tokio::process::ChildStdin,
            tokio::io::BufReader<tokio::process::ChildStdout>,
        )>,
    >,
>;

/// Authenticated OpenCode native queue transport. A runtime must keep one
/// collector alive for the whole session and reconcile the durable inbox.
#[derive(Clone)]
pub struct NativeQueue {
    base: reqwest::Url,
    password: String,
    http: reqwest::Client,
    #[cfg(test)]
    io: Option<TestIo>,
}

impl NativeQueue {
    #[cfg(test)]
    pub(crate) fn with_test_stdio(
        mut self,
        stdin: tokio::process::ChildStdin,
        stdout: tokio::io::BufReader<tokio::process::ChildStdout>,
    ) -> Self {
        self.io = Some(std::sync::Arc::new(tokio::sync::Mutex::new(Some((
            stdin, stdout,
        )))));
        self
    }
    #[cfg(test)]
    pub(crate) async fn close_test_stdio(&self) {
        if let Some(io) = &self.io {
            io.lock().await.take();
        }
    }
    pub fn new(url: &str, password: String) -> Result<Self> {
        let base = reqwest::Url::parse(url).context("Invalid OpenCode native API URL")?;
        if base.scheme() != "http"
            || !base.username().is_empty()
            || base.password().is_some()
            || base.query().is_some()
            || base.fragment().is_some()
            || base.path() != "/"
            || base.host_str().and_then(|host| host.parse::<IpAddr>().ok())
                != Some(IpAddr::V4(std::net::Ipv4Addr::LOCALHOST))
            || password.is_empty()
        {
            bail!("OpenCode native API requires an authenticated IPv4 loopback URL");
        }
        let http = reqwest::Client::builder()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .build()?;
        Ok(Self {
            base,
            password,
            http,
            #[cfg(test)]
            io: None,
        })
    }

    pub(crate) async fn request(
        &self,
        method: reqwest::Method,
        path: &str,
        body: Option<Value>,
    ) -> Result<Value> {
        self.request_status(method, path, body, None).await
    }
    pub(crate) async fn no_content(
        &self,
        method: reqwest::Method,
        path: &str,
        body: Option<Value>,
    ) -> Result<()> {
        self.request_status(method, path, body, Some(204)).await?;
        Ok(())
    }
    async fn request_status(
        &self,
        method: reqwest::Method,
        path: &str,
        body: Option<Value>,
        expected_status: Option<u16>,
    ) -> Result<Value> {
        #[cfg(test)]
        if let Some(io) = &self.io {
            use tokio::io::{AsyncBufReadExt, AsyncWriteExt};
            let io = io.clone();
            let request = json!({"method":method.as_str(),"path":path,"body":body});
            return tokio::spawn(async move {
                let mut io = io.lock().await;
                let (stdin, stdout) = io.as_mut().context("Fixture stdio is closed")?;
                stdin.write_all(format!("{request}\n").as_bytes()).await?;
                let mut line = String::new();
                stdout.read_line(&mut line).await?;
                let result: Value = serde_json::from_str(&line)?;
                if !result["status"]
                    .as_u64()
                    .is_some_and(|status| (200..300).contains(&status))
                {
                    bail!("Native fixture returned HTTP {}", result["status"]);
                }
                if expected_status.is_some_and(|status| result["status"] != status) {
                    bail!("Native acknowledgement did not match required HTTP status");
                }
                Ok(result["body"].clone())
            })
            .await
            .context("Native test exchange failed")?;
        }
        let url = self.base.join(path.trim_start_matches('/'))?;
        let mut request = self
            .http
            .request(method, url)
            .timeout(std::time::Duration::from_secs(30))
            .basic_auth("opencode", Some(&self.password));
        if let Some(body) = body {
            request = request.json(&body);
        }
        let response = request
            .send()
            .await
            .context("OpenCode native API request failed")?;
        if !response.status().is_success() {
            bail!("OpenCode native API returned HTTP {}", response.status());
        }
        if expected_status.is_some_and(|status| response.status().as_u16() != status) {
            bail!("Native acknowledgement did not match required HTTP status");
        }
        if response.status() == reqwest::StatusCode::NO_CONTENT {
            return Ok(Value::Null);
        }
        use futures_util::StreamExt;
        let mut stream = response.bytes_stream();
        let mut bytes = Vec::new();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.context("OpenCode native response stream failed")?;
            if bytes.len() + chunk.len() > response_limit(path) {
                bail!("OpenCode native API response exceeded limit");
            }
            bytes.extend_from_slice(&chunk);
        }
        serde_json::from_slice(&bytes).context("Invalid OpenCode native API response")
    }

    pub(crate) async fn events(
        &self,
    ) -> Result<futures_util::stream::BoxStream<'static, Result<Vec<u8>>>> {
        use futures_util::StreamExt;
        #[cfg(test)]
        if self.io.is_some() {
            let api = self.clone();
            return Ok(Box::pin(futures_util::stream::unfold(api, |api| async {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                let result = api
                    .request(reqwest::Method::GET, "/__fixture/events", None)
                    .await
                    .map(|events| {
                        events
                            .as_array()
                            .into_iter()
                            .flatten()
                            .map(|event| format!("data: {event}\n\n"))
                            .collect::<String>()
                            .into_bytes()
                    });
                let result = result.and_then(|bytes| {
                    if bytes
                        .windows(19)
                        .any(|window| window == b"_fixture_disconnect")
                    {
                        Err(anyhow::anyhow!("Synthetic native disconnect"))
                    } else {
                        Ok(bytes)
                    }
                });
                Some((result, api))
            })));
        }
        let response = self
            .http
            .get(self.base.join("api/event")?)
            .basic_auth("opencode", Some(&self.password))
            .send()
            .await
            .context("OpenCode native event connection failed")?;
        if !response.status().is_success()
            || !response
                .headers()
                .get("content-type")
                .and_then(|value| value.to_str().ok())
                .is_some_and(|value| value.starts_with("text/event-stream"))
        {
            bail!("OpenCode native event stream refused");
        }
        Ok(Box::pin(response.bytes_stream().map(|chunk| {
            chunk
                .map(|bytes| bytes.to_vec())
                .map_err(|_| anyhow::anyhow!("Native event stream failed"))
        })))
    }

    pub(crate) fn valid_id(id: &str, prefix: &str) -> bool {
        id.starts_with(prefix)
            && id.len() > prefix.len()
            && id.len() <= 160
            && id
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
    }

    pub async fn create_session(
        &self,
        directory: &str,
        agent: Option<&str>,
        model: Option<Value>,
    ) -> Result<String> {
        if !std::path::Path::new(directory).is_absolute() {
            bail!("OpenCode session directory must be absolute");
        }
        let mut body = json!({"location":{"directory":directory}});
        if let Some(agent) = agent {
            body["agent"] = json!(agent);
        }
        if let Some(model) = model {
            body["model"] = model;
        }
        let value = self
            .request(reqwest::Method::POST, "/api/session", Some(body))
            .await?;
        value["data"]["id"]
            .as_str()
            .filter(|id| Self::valid_id(id, "ses_"))
            .map(str::to_owned)
            .context("OpenCode did not return a valid session id")
    }

    /// Returns an ACK only after native API confirms its durable inbox item.
    pub async fn enqueue(&self, session: &str, message_id: &str, text: &str) -> Result<String> {
        self.enqueue_files(session, message_id, text, vec![]).await
    }
    pub(crate) async fn enqueue_files(
        &self,
        session: &str,
        message_id: &str,
        text: &str,
        files: Vec<Value>,
    ) -> Result<String> {
        if !Self::valid_id(session, "ses_")
            || !Self::valid_id(message_id, "msg_")
            || (text.is_empty() && files.is_empty())
        {
            bail!("Invalid OpenCode native queue submission");
        }
        let value = self
            .request(
                reqwest::Method::POST,
                &format!("/api/session/{session}/prompt"),
                Some(json!({"id":message_id,"text":text,"files":files,"delivery":"queue","resume":true})),
            )
            .await?;
        value["data"]["id"]
            .as_str()
            .filter(|id| *id == message_id)
            .map(str::to_owned)
            .context("OpenCode did not confirm the queued message id")
    }

    pub async fn inbox(&self, session: &str) -> Result<Vec<Value>> {
        if !Self::valid_id(session, "ses_") {
            bail!("Invalid OpenCode session id");
        }
        let value = self
            .request(
                reqwest::Method::GET,
                &format!("/api/session/{session}/inbox"),
                None,
            )
            .await?;
        value["data"]
            .as_array()
            .cloned()
            .context("OpenCode inbox response is not an array")
    }

    pub async fn cancel(&self, session: &str, message_id: &str) -> Result<()> {
        if !Self::valid_id(session, "ses_") || !Self::valid_id(message_id, "msg_") {
            bail!("Invalid OpenCode queue cancellation");
        }
        self.request(
            reqwest::Method::DELETE,
            &format!("/api/session/{session}/inbox/{message_id}"),
            None,
        )
        .await?;
        Ok(())
    }
}

fn response_limit(path: &str) -> usize {
    if path.split('?').next() == Some("/api/model") {
        8 * 1024 * 1024
    } else {
        1024 * 1024
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn model_catalog_budget_is_large_but_bounded_separately_from_history() {
        assert_eq!(response_limit("/api/model?directory=/tmp"), 8 * 1024 * 1024);
        assert_eq!(
            response_limit("/api/session/ses_fixture/message"),
            1024 * 1024
        );
    }
    #[test]
    fn native_api_refuses_non_loopback_or_uncredentialed_endpoints() {
        assert!(NativeQueue::new("http://127.0.0.1:4096", "secret".into()).is_ok());
        assert!(NativeQueue::new("http://0.0.0.0:4096", "secret".into()).is_err());
        assert!(NativeQueue::new("https://127.0.0.1:4096", "secret".into()).is_err());
        assert!(NativeQueue::new("http://127.0.0.1:4096", String::new()).is_err());
    }
}
