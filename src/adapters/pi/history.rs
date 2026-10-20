//! Native Pi sessions: discover by verified headers, never by an ACP-supplied path.
use anyhow::{Context, Result, bail};
use serde_json::{Value, json};
use std::{
    io::{BufRead, BufReader, Read, Seek, SeekFrom},
    path::{Path, PathBuf},
};
const FILE_LIMIT: u64 = 16 * 1024 * 1024;
#[derive(Clone)]
pub(super) struct Entry {
    pub id: String,
    pub cwd: PathBuf,
    pub path: PathBuf,
    pub title: String,
    pub updated: Value,
}
pub(super) fn agent_dir() -> Result<PathBuf> {
    std::env::var_os("PI_CODING_AGENT_DIR")
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("HOME").map(|home| PathBuf::from(home).join(".pi/agent")))
        .context("Pi configuration directory unavailable")
}
fn root(cwd: &Path) -> Result<PathBuf> {
    let agent = agent_dir()?;
    let agent = if agent.is_absolute() {
        agent
    } else {
        cwd.join(agent)
    };
    let mut configured = None;
    for settings in [agent.join("settings.json"), cwd.join(".pi/settings.json")] {
        if !settings.exists() {
            continue;
        }
        let file = std::fs::File::open(settings)?;
        if file.metadata()?.len() > 1024 * 1024 {
            bail!("Pi settings exceeded limit");
        }
        let settings: Value = serde_json::from_reader(file)?;
        if let Some(path) = settings["sessionDir"]
            .as_str()
            .filter(|path| !path.is_empty())
        {
            configured = Some(PathBuf::from(path));
        }
    }
    let path = std::env::var_os("PI_CODING_AGENT_SESSION_DIR")
        .filter(|path| !path.is_empty())
        .map(PathBuf::from)
        .or(configured)
        .unwrap_or_else(|| agent.join("sessions"));
    Ok(if path.is_absolute() {
        path
    } else {
        cwd.join(path)
    })
}

fn files(cwd: &Path) -> Result<Vec<PathBuf>> {
    let root = root(cwd)?;
    if !root.exists() {
        return Ok(vec![]);
    }
    let mut directories = vec![(root, 0)];
    let mut files = vec![];
    let mut count = 0;
    while let Some((directory, depth)) = directories.pop() {
        for entry in std::fs::read_dir(directory)? {
            let entry = entry?;
            count += 1;
            if count > 16384 {
                bail!("Pi session scan exceeded limit");
            }
            let kind = entry.file_type()?;
            if kind.is_dir() {
                if depth >= 8 {
                    bail!("Pi session directory depth exceeded limit");
                }
                directories.push((entry.path(), depth + 1));
            } else if kind.is_file() && entry.path().extension().is_some_and(|ext| ext == "jsonl") {
                files.push(entry.path());
            }
        }
    }
    Ok(files)
}
pub(super) fn id(value: &Value) -> Result<String> {
    let id = value.as_str().context("Pi session ID missing")?;
    if id.is_empty() || id.len() > 256 || id.chars().any(char::is_control) {
        bail!("Invalid Pi session ID");
    }
    Ok(id.into())
}
fn header(path: &Path) -> Result<(String, PathBuf)> {
    if std::fs::symlink_metadata(path)?.file_type().is_symlink() {
        bail!("Pi session file is a symlink");
    }
    let mut reader = BufReader::new(std::fs::File::open(path)?.take(65537));
    let mut bytes = Vec::new();
    reader.read_until(b'\n', &mut bytes)?;
    if bytes.len() > 65536 {
        bail!("Pi session header exceeded limit");
    }
    let header: Value = serde_json::from_slice(&bytes)?;
    if header["type"] != "session" {
        bail!("Invalid Pi session header");
    }
    let cwd = PathBuf::from(
        header["cwd"]
            .as_str()
            .context("Pi session workspace missing")?,
    );
    if !cwd.is_absolute() {
        bail!("Pi session workspace must be absolute");
    }
    Ok((id(&header["id"])?, cwd))
}
pub(super) fn validate(path: &Path, cwd: &Path, session: &str) -> Result<()> {
    let (id, workspace) = header(path)?;
    if id != session || workspace.canonicalize()? != cwd {
        bail!("Pi session ID or workspace mismatch");
    }
    Ok(())
}
fn describe(path: PathBuf, session: String, cwd: PathBuf) -> Result<Entry> {
    let mut file = std::fs::File::open(&path)?;
    let len = file.metadata()?.len();
    let start = len.saturating_sub(256 * 1024);
    file.seek(SeekFrom::Start(start))?;
    let mut reader = BufReader::new(file.take(256 * 1024));
    let mut bytes = Vec::new();
    reader.read_to_end(&mut bytes)?;
    let tail = String::from_utf8_lossy(&bytes);
    let mut title = None;
    let mut updated = Value::Null;
    for line in tail
        .lines()
        .skip(usize::from(start > 0))
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
    {
        let Ok(record) = serde_json::from_str::<Value>(line) else {
            continue;
        };
        if record["type"] == "session_info" && title.is_none() {
            title = record["name"]
                .as_str()
                .map(|name| crate::adapters::bounded(name, 512));
        }
        if record["type"] == "message" && updated.is_null() {
            updated = record["timestamp"].clone();
        }
    }
    Ok(Entry {
        id: session,
        cwd,
        path,
        title: title.unwrap_or("Pi conversation".into()),
        updated,
    })
}
pub(super) fn list(cwd: &Path) -> Result<Vec<Entry>> {
    let mut entries = vec![];
    let mut ids = std::collections::HashSet::new();
    for file in files(cwd)? {
        let (session, workspace) = header(&file)?;
        if workspace.canonicalize().ok().as_deref() != Some(cwd) {
            continue;
        }
        if !ids.insert(session.clone()) {
            bail!("Ambiguous duplicate Pi session ID");
        }
        entries.push(describe(file, session, cwd.to_owned())?);
    }
    Ok(entries)
}
pub(super) fn find(cwd: &Path, id: &str) -> Result<Entry> {
    list(cwd)?
        .into_iter()
        .find(|entry| entry.id == id)
        .context("Pi session not found in this workspace")
}
pub(super) fn check_size(path: &Path) -> Result<()> {
    if std::fs::metadata(path)?.len() > FILE_LIMIT {
        bail!("Pi session exceeded history limit");
    }
    Ok(())
}
pub(super) fn delete(path: &Path, cwd: &Path, id: &str) -> Result<()> {
    if path.exists() {
        validate(path, cwd, id)?;
        std::fs::remove_file(path)?;
    }
    Ok(())
}
pub(super) fn as_info(entry: &Entry) -> Value {
    let mut info = json!({"sessionId":entry.id,"cwd":entry.cwd,"title":entry.title});
    if !entry.updated.is_null() {
        info["updatedAt"] = entry.updated.clone();
    }
    info
}
