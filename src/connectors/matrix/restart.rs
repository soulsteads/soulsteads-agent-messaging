use anyhow::{Result, bail};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::path::Path;

pub const MATRIX_RESTART_ACK: &str = "Restart requested; continue in this same thread when back. Completed history is preserved; in-flight work may be lost.";
#[derive(Clone, Debug)]
pub struct RestartCommand {
    unit: String,
}
impl RestartCommand {
    pub fn program(&self) -> &'static str {
        "/usr/bin/systemctl"
    }
    pub fn args(&self) -> [&str; 4] {
        ["--user", "--no-block", "restart", &self.unit]
    }
}
pub fn resolve_restart_command(unit: &str) -> Option<RestartCommand> {
    static UNIT: std::sync::LazyLock<regex::Regex> = std::sync::LazyLock::new(|| {
        regex::Regex::new(r"^[A-Za-z0-9][A-Za-z0-9_.@:-]*\.service$").unwrap()
    });
    UNIT.is_match(unit).then(|| RestartCommand {
        unit: unit.to_owned(),
    })
}
#[async_trait]
pub trait RestartDispatcher: Send + Sync {
    async fn dispatch(&self, command: RestartCommand) -> Result<()>;
}
pub struct SystemdUserRestart;
#[async_trait]
impl RestartDispatcher for SystemdUserRestart {
    async fn dispatch(&self, command: RestartCommand) -> Result<()> {
        let status = tokio::process::Command::new(command.program())
            .args(command.args())
            .status()
            .await?;
        if !status.success() {
            bail!("systemctl returned {status}");
        }
        Ok(())
    }
}
#[derive(Serialize, Deserialize)]
struct Receipts {
    version: u8,
    events: Vec<String>,
}
fn receipts(path: &Path) -> Result<Receipts> {
    let data = match std::fs::read(path) {
        Ok(data) => serde_json::from_slice::<Receipts>(&data)?,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Receipts {
            version: 1,
            events: vec![],
        },
        Err(error) => return Err(error.into()),
    };
    if data.version != 1 {
        bail!("Invalid Matrix restart receipts");
    }
    Ok(data)
}
pub fn has_restart_receipt(path: &Path, id: &str) -> Result<bool> {
    Ok(receipts(path)?.events.iter().any(|event| event == id))
}
pub fn record_restart_receipt(path: &Path, id: &str) -> Result<()> {
    let mut data = receipts(path)?;
    if !data.events.iter().any(|event| event == id) {
        data.events.push(id.to_owned());
        crate::store::write_private(path, &serde_json::to_vec(&data)?)?;
    }
    Ok(())
}
