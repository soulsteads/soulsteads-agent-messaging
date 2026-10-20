use crate::native_queue::NativeQueue;
use anyhow::{Context, Result, bail};
use serde_json::Value;
use std::{collections::BTreeMap, path::PathBuf, process::Stdio, time::Duration};
use tokio::{
    io::{AsyncReadExt, BufReader},
    process::{Child, ChildStdin, Command},
    task::JoinHandle,
};

pub struct Options {
    pub command: String,
    pub args: Vec<String>,
    pub cwd: PathBuf,
    pub env: BTreeMap<String, String>,
    pub inherit_env: bool,
    pub timeout: Duration,
}
impl From<crate::acp::Options> for Options {
    fn from(value: crate::acp::Options) -> Self {
        Self {
            command: value.command,
            args: value.args,
            cwd: value.cwd,
            env: value.env,
            inherit_env: value.inherit_env,
            timeout: value.timeout,
        }
    }
}
/// Owns the native server and its stdin lease. No credentials are logged or
/// exposed via Debug. Dropping an unfinished owner kills its child.
pub struct NativeServer {
    child: Child,
    lease: Option<ChildStdin>,
    client: NativeQueue,
    drain: JoinHandle<()>,
    timeout: Duration,
}
impl NativeServer {
    pub async fn start(options: impl Into<Options>) -> Result<Self> {
        let options = options.into();
        if options.args.iter().any(|argument| argument == "acp") {
            bail!("Legacy ACP arguments cannot be passed to native serve");
        }
        #[cfg(test)]
        let test_stdio = options.env.contains_key("BRIDGE_TEST_NATIVE_STDIO");
        let password = format!(
            "{}{}",
            uuid::Uuid::new_v4().simple(),
            uuid::Uuid::new_v4().simple()
        );
        let mut command = Command::new(&options.command);
        command
            .args(&options.args)
            .args(["serve", "--stdio", "--hostname", "127.0.0.1", "--port", "0"])
            .current_dir(options.cwd)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .kill_on_drop(true);
        if !options.inherit_env {
            command.env_clear();
        }
        command
            .envs(options.env)
            .env("OPENCODE_PASSWORD", &password);
        #[cfg(unix)]
        command.process_group(0);
        let mut child = command
            .spawn()
            .context("Could not start owned OpenCode native server")?;
        let lease = child.stdin.take().context("Missing OpenCode stdin lease")?;
        let stdout = child
            .stdout
            .take()
            .context("Missing OpenCode readiness stream")?;
        let mut stdout = BufReader::new(stdout);
        let readiness = tokio::time::timeout(options.timeout, async {
            let mut line = Vec::new();
            loop {
                let byte = stdout
                    .read_u8()
                    .await
                    .context("OpenCode exited before readiness")?;
                if byte == b'\n' {
                    break;
                }
                if line.len() >= 8192 {
                    bail!("OpenCode readiness exceeded limit");
                }
                line.push(byte);
            }
            let info: Value =
                serde_json::from_slice(&line).context("Invalid OpenCode readiness")?;
            let url = info["url"].as_str().context("Missing native server URL")?;
            let client = NativeQueue::new(url, password)?;
            Ok::<_, anyhow::Error>(client)
        })
        .await;
        let client = match readiness {
            Ok(Ok(client)) => client,
            failure => {
                drop(lease);
                terminate_owned_group(&mut child);
                let _ = child.kill().await;
                let _ = child.wait().await;
                match failure {
                    Ok(Err(error)) => return Err(error),
                    Err(_) => bail!("OpenCode native readiness timed out"),
                    Ok(Ok(_)) => unreachable!(),
                }
            }
        };
        #[cfg(test)]
        if test_stdio {
            let client = client.with_test_stdio(lease, stdout);
            return Ok(Self {
                child,
                lease: None,
                client,
                drain: tokio::spawn(async {}),
                timeout: options.timeout,
            });
        }
        let drain = tokio::spawn(async move {
            let mut buf = [0u8; 4096];
            while matches!(stdout.read(&mut buf).await, Ok(n) if n>0) {}
        });
        Ok(Self {
            child,
            lease: Some(lease),
            client,
            drain,
            timeout: options.timeout,
        })
    }
    pub fn client(&self) -> NativeQueue {
        self.client.clone()
    }
    pub async fn shutdown(mut self) -> Result<()> {
        self.lease.take();
        #[cfg(test)]
        self.client.close_test_stdio().await;
        let result = tokio::time::timeout(self.timeout, self.child.wait()).await;
        self.drain.abort();
        match result {
            Ok(Ok(status)) if status.success() => Ok(()),
            Ok(Ok(_)) => bail!("OpenCode native server exited unsuccessfully"),
            Ok(Err(error)) => Err(error).context("Could not reap OpenCode native server"),
            Err(_) => {
                terminate_owned_group(&mut self.child);
                let _ = self.child.kill().await;
                let _ = self.child.wait().await;
                bail!("OpenCode native shutdown timed out")
            }
        }
    }
}
impl Drop for NativeServer {
    fn drop(&mut self) {
        self.drain.abort();
        terminate_owned_group(&mut self.child);
    }
}
fn terminate_owned_group(child: &mut Child) {
    #[cfg(unix)]
    if let Some(pid) = child.id() {
        unsafe {
            libc::kill(-(pid as i32), libc::SIGKILL);
        }
    }
    #[cfg(not(unix))]
    let _ = child.start_kill();
}
