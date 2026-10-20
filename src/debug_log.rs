use serde_json::{Value, json};
use std::{
    path::PathBuf,
    sync::{Mutex, OnceLock},
};

pub struct DebugLogger {
    disabled: Mutex<bool>,
    file_path: PathBuf,
}
impl DebugLogger {
    pub fn new(enabled: bool, file_path: PathBuf) -> Self {
        Self {
            disabled: Mutex::new(!enabled),
            file_path,
        }
    }
    pub fn observation(&self, observation: &Value) {
        let Ok(mut disabled) = self.disabled.lock() else {
            return;
        };
        if *disabled {
            return;
        }
        if let Err(error) = self.append(observation) {
            *disabled = true;
            eprintln!("[ACP] Debug logging disabled: {error}");
        }
    }
    fn append(&self, observation: &Value) -> anyhow::Result<()> {
        use std::io::Write;
        let parent = self
            .file_path
            .parent()
            .ok_or_else(|| anyhow::anyhow!("Invalid debug log path"))?;
        let mut builder = std::fs::DirBuilder::new();
        builder.recursive(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::DirBuilderExt;
            builder.mode(0o700);
        }
        builder.create(parent)?;
        let mut file = crate::media::secure_open(
            &self.file_path,
            libc::O_WRONLY | libc::O_CREAT | libc::O_APPEND | libc::O_NONBLOCK,
            0o600,
        )?;
        let metadata = file.metadata()?;
        if !metadata.is_file() || metadata.len() > 8 * 1024 * 1024 {
            anyhow::bail!("Debug log boundary exceeded");
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::{MetadataExt, PermissionsExt};
            if metadata.uid() != unsafe { libc::geteuid() } || metadata.nlink() != 1 {
                anyhow::bail!("Debug log is not safely owned");
            }
            file.set_permissions(std::fs::Permissions::from_mode(0o600))?;
        }
        let mut record = json!({"event":"prompt_end"});
        for key in [
            "rawChars",
            "capturedChars",
            "cleanChars",
            "deliveredChars",
            "chunks",
            "toolCalls",
            "imageCount",
            "uploadedImages",
            "uploadedMedia",
            "uploadFailures",
        ] {
            if let Some(value) = observation[key].as_u64() {
                record[key] = value.into();
            }
        }
        for key in [
            "hadToolActivity",
            "hasImages",
            "outputLost",
            "presentationLost",
            "collectorClosed",
            "captureRecovered",
            "failed",
            "deliveryFailed",
            "usableOutput",
            "retry",
        ] {
            if let Some(value) = observation[key].as_bool() {
                record[key] = value.into();
            }
        }
        record["stopReason"] = match observation["stopReason"].as_str() {
            Some(reason @ ("end_turn" | "cancelled" | "max_tokens" | "refusal")) => reason.into(),
            _ => "unknown".into(),
        };
        let line = format!("[{}] {record}\n", chrono::Utc::now().to_rfc3339());
        if metadata.len().saturating_add(line.len() as u64) > 8 * 1024 * 1024 {
            anyhow::bail!("Debug log boundary exceeded");
        }
        file.write_all(line.as_bytes())?;
        Ok(())
    }
}

pub fn observation(value: &Value) {
    static LOGGER: OnceLock<DebugLogger> = OnceLock::new();
    LOGGER
        .get_or_init(|| {
            DebugLogger::new(
                std::env::var("BRIDGE_DEBUG").is_ok_and(|flag| flag == "1"),
                std::env::current_dir()
                    .unwrap_or_default()
                    .join("logs/bridge-debug.log"),
            )
        })
        .observation(value);
}
