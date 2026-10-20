use anyhow::{Result, bail};
use serde::{Deserialize, Serialize};
use std::{
    fs::{self, OpenOptions},
    io::{Read, Write},
    path::{Path, PathBuf},
    time::{Duration, SystemTime},
};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Entry {
    pub connector: String,
    pub thread_id: String,
    pub session_id: String,
    pub cwd: String,
    pub backend_id: String,
    pub updated_at: String,
    #[serde(default = "default_backend")]
    pub backend: String,
}

fn default_backend() -> String {
    "opencode".into()
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Selection {
    connector: String,
    thread_id: String,
    backend: String,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Document {
    version: u8,
    sessions: Vec<Entry>,
    #[serde(default)]
    selected_backends: Vec<Selection>,
}

pub struct Store(pub PathBuf);
impl Store {
    pub fn new(path: impl AsRef<Path>) -> Self {
        Self(path.as_ref().to_owned())
    }
    fn read(&self) -> Result<Document> {
        reject_symlinks(&self.0)?;
        let mut options = OpenOptions::new();
        options.read(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.custom_flags(libc::O_NOFOLLOW);
        }
        let file = match options.open(&self.0) {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Ok(Document {
                    version: 2,
                    sessions: vec![],
                    selected_backends: vec![],
                });
            }
            Err(error) => return Err(error.into()),
        };
        let mut data = Vec::new();
        file.take(16 * 1024 * 1024 + 1).read_to_end(&mut data)?;
        if data.len() > 16 * 1024 * 1024 {
            bail!("session store exceeds size limit");
        }
        let parsed: serde_json::Value = serde_json::from_slice(&data)?;
        if parsed["version"] == 2 && !parsed["selectedBackends"].is_array() {
            bail!("invalid backend selections");
        }
        let mut document: Document = serde_json::from_value(parsed)?;
        if ![1, 2].contains(&document.version) {
            bail!("invalid session store version");
        }
        if document.version == 1 {
            document.selected_backends.clear();
        }
        for entry in &mut document.sessions {
            if entry.backend.is_empty() {
                entry.backend = default_backend();
            }
        }
        document.version = 2;
        Ok(document)
    }
    pub fn get(&self, connector: &str, thread: &str, backend: &str) -> Result<Option<Entry>> {
        Ok(self.read()?.sessions.into_iter().find(|entry| {
            entry.connector == connector && entry.thread_id == thread && entry.backend == backend
        }))
    }
    pub fn selected(&self, connector: &str, thread: &str, fallback: &str) -> Result<String> {
        let data = self.read()?;
        if let Some(entry) = data
            .selected_backends
            .iter()
            .find(|entry| entry.connector == connector && entry.thread_id == thread)
        {
            return Ok(entry.backend.clone());
        }
        if data.sessions.iter().any(|entry| {
            entry.connector == connector && entry.thread_id == thread && entry.backend == "opencode"
        }) {
            return Ok(default_backend());
        }
        Ok(fallback.into())
    }
    pub fn set(&self, entry: Entry, select: bool) -> Result<()> {
        self.update(|data| {
            data.sessions.retain(|old| {
                !(old.connector == entry.connector
                    && old.thread_id == entry.thread_id
                    && old.backend == entry.backend)
            });
            if select {
                Self::select(data, &entry.connector, &entry.thread_id, &entry.backend);
            }
            data.sessions.push(entry);
        })
    }
    pub fn set_selected(&self, connector: &str, thread: &str, backend: &str) -> Result<()> {
        self.update(|data| Self::select(data, connector, thread, backend))
    }
    fn select(data: &mut Document, connector: &str, thread: &str, backend: &str) {
        data.selected_backends
            .retain(|old| old.connector != connector || old.thread_id != thread);
        data.selected_backends.push(Selection {
            connector: connector.into(),
            thread_id: thread.into(),
            backend: backend.into(),
        });
    }
    pub fn remove(&self, connector: &str, thread: &str, backend: Option<&str>) -> Result<()> {
        self.update(|data| {
            data.sessions.retain(|entry| {
                entry.connector != connector
                    || entry.thread_id != thread
                    || backend.is_some_and(|name| name != entry.backend)
            });
            if backend.is_none() {
                data.selected_backends
                    .retain(|entry| entry.connector != connector || entry.thread_id != thread);
            }
        })
    }
    fn update(&self, action: impl FnOnce(&mut Document)) -> Result<()> {
        let parent = self
            .0
            .parent()
            .filter(|path| !path.as_os_str().is_empty())
            .unwrap_or(Path::new("."));
        reject_symlinks(&self.0)?;
        fs::create_dir_all(parent)?;
        let lock_path = self.0.with_file_name(format!(
            "{}.lock",
            self.0.file_name().unwrap_or_default().to_string_lossy()
        ));
        let mut lock = None;
        for _ in 0..200 {
            reject_symlinks(&lock_path)?;
            let mut options = OpenOptions::new();
            options.create_new(true).write(true);
            #[cfg(unix)]
            {
                use std::os::unix::fs::OpenOptionsExt;
                options.mode(0o600).custom_flags(libc::O_NOFOLLOW);
            }
            match options.open(&lock_path) {
                Ok(file) => {
                    lock = Some(file);
                    break;
                }
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                    if let Ok(metadata) = fs::symlink_metadata(&lock_path) {
                        if metadata.file_type().is_symlink() {
                            bail!("Symlink lock rejected");
                        }
                        if metadata.modified().is_ok_and(|time| {
                            SystemTime::now()
                                .duration_since(time)
                                .is_ok_and(|age| age > Duration::from_secs(30))
                        }) {
                            let _ = fs::remove_file(&lock_path);
                        }
                    }
                    std::thread::sleep(Duration::from_millis(25));
                }
                Err(error) => return Err(error.into()),
            }
        }
        let Some(lock) = lock else {
            bail!("timed out locking session store");
        };
        struct LockGuard {
            file: fs::File,
            path: PathBuf,
        }
        impl Drop for LockGuard {
            fn drop(&mut self) {
                #[cfg(unix)]
                {
                    use std::os::unix::fs::MetadataExt;
                    if let (Ok(owned), Ok(current)) =
                        (self.file.metadata(), fs::symlink_metadata(&self.path))
                        && owned.ino() == current.ino()
                        && owned.dev() == current.dev()
                    {
                        let _ = fs::remove_file(&self.path);
                    }
                }
                #[cfg(not(unix))]
                {
                    let _ = fs::remove_file(&self.path);
                }
            }
        }
        let _lock = LockGuard {
            file: lock,
            path: lock_path,
        };
        let mut document = self.read()?;
        action(&mut document);
        let temporary = parent.join(format!(".sessions-{}.tmp", uuid::Uuid::new_v4()));
        let result = (|| -> Result<()> {
            let mut options = OpenOptions::new();
            options.create_new(true).write(true);
            #[cfg(unix)]
            {
                use std::os::unix::fs::OpenOptionsExt;
                options.mode(0o600);
            }
            let mut file = options.open(&temporary)?;
            file.write_all(&serde_json::to_vec_pretty(&document)?)?;
            file.write_all(b"\n")?;
            file.sync_all()?;
            fs::rename(&temporary, &self.0)?;
            fs::File::open(parent)?.sync_all().map_err(|error| {
                anyhow::anyhow!(
                    "Session store published; directory durability sync failed: {error}"
                )
            })?;
            Ok(())
        })();
        if temporary.exists() {
            let _ = fs::remove_file(temporary);
        }
        result
    }
}

pub fn write_private(path: &Path, bytes: &[u8]) -> Result<()> {
    reject_symlinks(path)?;
    let parent = path
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    fs::create_dir_all(parent)?;
    let temporary = parent.join(format!(".private-{}.tmp", uuid::Uuid::new_v4()));
    let result = (|| -> Result<()> {
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let mut file = options.open(&temporary)?;
        file.write_all(bytes)?;
        file.sync_all()?;
        fs::rename(&temporary, path)?;
        fs::File::open(parent)?.sync_all()?;
        Ok(())
    })();
    if temporary.exists() {
        let _ = fs::remove_file(temporary);
    }
    result
}

fn reject_symlinks(path: &Path) -> Result<()> {
    let mut current = PathBuf::new();
    for component in path.components() {
        current.push(component);
        match fs::symlink_metadata(&current) {
            Ok(metadata) if metadata.file_type().is_symlink() => {
                bail!("Symlink in session store path rejected")
            }
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
    }
    Ok(())
}
