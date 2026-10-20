//! Bounded, read-only access to Claude's native persisted transcripts.
use anyhow::{Context, Result, bail};
use serde_json::{Value, json};
use std::{
    io::{BufRead, BufReader, Read},
    path::{Path, PathBuf},
};
const HISTORY_LIMIT: u64 = 16 * 1024 * 1024;
const FRAME_LIMIT: u64 = 8 * 1024 * 1024;

pub(super) fn session_id(id: &str) -> Result<String> {
    let parsed = uuid::Uuid::parse_str(id).context("Invalid Claude session UUID")?;
    if parsed.to_string() != id {
        bail!("Claude session UUID must be canonical");
    }
    Ok(id.into())
}
fn projects() -> Result<PathBuf> {
    let config = std::env::var_os("CLAUDE_CONFIG_DIR")
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("HOME").map(|home| PathBuf::from(home).join(".claude")))
        .context("Claude configuration directory unavailable")?;
    Ok(config.join("projects"))
}
fn files(cwd: &Path) -> Result<Vec<PathBuf>> {
    let root = projects()?;
    let dirs = match std::fs::read_dir(&root) {
        Ok(dirs) => dirs,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(vec![]),
        Err(e) => return Err(e.into()),
    };
    let encoded = cwd
        .to_string_lossy()
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '-' })
        .collect::<String>();
    let mut files = Vec::new();
    for dir in dirs.take(4097) {
        let dir = dir?;
        let name = dir.file_name().to_string_lossy().into_owned();
        if !(name == encoded
            || (encoded.len() > 200 && name.starts_with(&format!("{}-", &encoded[..200]))))
        {
            continue;
        }
        if !dir.file_type()?.is_dir() {
            continue;
        }
        for file in std::fs::read_dir(dir.path())?.take(4097) {
            let file = file?;
            if file.file_type()?.is_file()
                && file.path().extension().is_some_and(|ext| ext == "jsonl")
                && file
                    .path()
                    .file_stem()
                    .and_then(|s| s.to_str())
                    .is_some_and(|id| session_id(id).is_ok())
            {
                files.push(file.path());
                if files.len() > 4096 {
                    bail!("Claude transcript listing exceeded limit");
                }
            }
        }
    }
    Ok(files)
}
fn read(path: &Path) -> Result<Vec<Value>> {
    if std::fs::symlink_metadata(path)?.file_type().is_symlink() {
        bail!("Claude transcript is a symlink");
    }
    let file = std::fs::File::open(path)?;
    if file.metadata()?.len() > HISTORY_LIMIT {
        bail!("Claude transcript exceeded history limit");
    }
    let mut reader = BufReader::new(file.take(HISTORY_LIMIT + 1));
    let mut frames = Vec::new();
    let mut total = 0;
    loop {
        let mut bytes = Vec::new();
        if (&mut reader)
            .take(FRAME_LIMIT + 1)
            .read_until(b'\n', &mut bytes)?
            == 0
        {
            break;
        }
        total += bytes.len() as u64;
        if bytes.len() as u64 > FRAME_LIMIT || total > HISTORY_LIMIT {
            bail!("Claude history frame exceeded limit");
        }
        // A partially written final record is not committed history.
        if !bytes.ends_with(b"\n") {
            break;
        }
        frames.push(serde_json::from_slice(&bytes).context("Malformed Claude transcript")?);
        if frames.len() > 16384 {
            bail!("Claude history record limit exceeded");
        }
    }
    Ok(frames)
}
fn belongs(records: &[Value], cwd: &Path) -> bool {
    // Project directory encoding is lossy. Check recorded physical workspace.
    let workspaces = records
        .iter()
        .filter_map(|record| record["cwd"].as_str())
        .collect::<Vec<_>>();
    !workspaces.is_empty()
        && workspaces
            .iter()
            .all(|path| Path::new(path).canonicalize().is_ok_and(|path| path == cwd))
}
pub(super) fn load(cwd: &Path, id: &str) -> Result<Vec<Value>> {
    session_id(id)?;
    for file in files(cwd)? {
        if file.file_stem().and_then(|s| s.to_str()) != Some(id) {
            continue;
        }
        let records = read(&file)?;
        if !belongs(&records, cwd) {
            bail!("Claude session belongs to a different workspace");
        }
        return Ok(records);
    }
    bail!("Claude session transcript not found in this workspace")
}
pub(super) fn list(cwd: &Path) -> Result<Vec<Value>> {
    let mut sessions = Vec::new();
    for file in files(cwd)? {
        let records = read(&file)?;
        if !belongs(&records, cwd) {
            continue;
        }
        let title = records
            .iter()
            .rev()
            .find_map(|record| record["customTitle"].as_str())
            .or_else(|| {
                records
                    .iter()
                    .find_map(|record| record["message"]["content"].as_str())
            })
            .unwrap_or("Claude conversation");
        let title = crate::adapters::bounded(title, 512);
        let updated = records
            .iter()
            .rev()
            .find_map(|record| record["timestamp"].as_str());
        sessions.push(json!({"sessionId":file.file_stem().unwrap().to_string_lossy(),"cwd":cwd,"title":title,"updatedAt":updated}));
    }
    Ok(sessions)
}

/// Match the native SDK's local delete semantics, scoped to verified workspace
/// and UUID. Nothing outside the transcript and its subagent directory is removed.
pub(super) fn delete(cwd: &Path, id: &str) -> Result<()> {
    session_id(id)?;
    for file in files(cwd)? {
        if file.file_stem().and_then(|s| s.to_str()) != Some(id) {
            continue;
        }
        if !belongs(&read(&file)?, cwd) {
            bail!("Claude session belongs to a different workspace");
        }
        let subagents = file.parent().context("Missing project directory")?.join(id);
        if let Ok(metadata) = std::fs::symlink_metadata(&subagents) {
            if metadata.file_type().is_symlink() || !metadata.is_dir() {
                bail!("Unsafe Claude subagent directory");
            }
            std::fs::remove_dir_all(&subagents)?;
        }
        std::fs::remove_file(file)?;
    }
    Ok(())
}
