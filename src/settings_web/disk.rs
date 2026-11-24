use crate::{
    config::SETTINGS_FILE_LIMIT,
    settings::{Error, error, parse_object},
};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::{
    fs::{self, File, Metadata, OpenOptions},
    io::{self, Read, Write},
    os::{
        fd::AsRawFd,
        unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt},
    },
    path::{Path, PathBuf},
};
use uuid::Uuid;

pub(super) struct ConfigFile {
    parent: PathBuf,
    identity: (u64, u64),
    path: PathBuf,
    uid: u32,
}

pub(super) struct Snapshot {
    bytes: Option<Vec<u8>>,
    pub(super) revision: String,
}

impl Snapshot {
    pub(super) fn input(&self) -> Result<&str, Error> {
        std::str::from_utf8(self.bytes.as_deref().unwrap_or(b"{}"))
            .map_err(|_| error("config_read", "Use a UTF-8 configuration file."))
    }

    pub(super) fn document(&self) -> Result<Value, Error> {
        parse_object(self.input()?)
    }
}

pub(super) enum PublishError {
    Before,
    After,
}

fn invalid_path() -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, "Unsafe settings path")
}

fn identity(metadata: &Metadata) -> (u64, u64) {
    (metadata.dev(), metadata.ino())
}

fn stamp(metadata: &Metadata) -> [u8; 56] {
    let mut result = [0; 56];
    for (chunk, value) in result.as_chunks_mut::<8>().0.iter_mut().zip([
        metadata.dev(),
        metadata.ino(),
        metadata.mode().into(),
        metadata.mtime() as u64,
        metadata.mtime_nsec() as u64,
        metadata.ctime() as u64,
        metadata.ctime_nsec() as u64,
    ]) {
        chunk.copy_from_slice(&value.to_le_bytes());
    }
    result
}

fn open_parent(path: &Path, uid: u32) -> io::Result<(File, (u64, u64))> {
    let directory = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW)
        .open(path)?;
    let metadata = directory.metadata()?;
    if !metadata.is_dir() || metadata.uid() != uid || metadata.mode() & 0o022 != 0 {
        return Err(invalid_path());
    }
    Ok((directory, identity(&metadata)))
}

impl ConfigFile {
    pub(super) fn open(path: &Path) -> io::Result<Self> {
        let leaf = path.file_name().ok_or_else(invalid_path)?;
        let parent = path.parent().ok_or_else(invalid_path)?.canonicalize()?;
        // SAFETY: geteuid takes no arguments and has no memory preconditions.
        let uid = unsafe { libc::geteuid() };
        let (_, identity) = open_parent(&parent, uid)?;
        let path = parent.join(leaf);
        Ok(Self {
            parent,
            identity,
            path,
            uid,
        })
    }

    fn directory(&self) -> io::Result<File> {
        let (directory, found) = open_parent(&self.parent, self.uid)?;
        if found != self.identity || self.parent.canonicalize()? != self.parent {
            return Err(invalid_path());
        }
        Ok(directory)
    }

    // Lock the directory inode so every cooperating writer uses the same
    // lock even when the config leaf is absent or is atomically replaced.
    pub(super) fn lock(&self) -> io::Result<Option<File>> {
        let directory = self.directory()?;
        // SAFETY: the owned descriptor stays open until the lock is dropped.
        if unsafe { libc::flock(directory.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } == 0 {
            return Ok(Some(directory));
        }
        let error = io::Error::last_os_error();
        if error.kind() == io::ErrorKind::WouldBlock {
            return Ok(None);
        }
        Err(error)
    }

    fn check_leaf(&self, metadata: &Metadata) -> io::Result<()> {
        if !metadata.is_file()
            || metadata.nlink() != 1
            || metadata.uid() != self.uid
            || metadata.mode() & 0o022 != 0
            || metadata.len() > SETTINGS_FILE_LIMIT
        {
            return Err(invalid_path());
        }
        Ok(())
    }

    pub(super) fn read(&self, key: &Uuid) -> io::Result<Snapshot> {
        self.directory()?;
        let mut hash = Sha256::new();
        let bytes = match fs::symlink_metadata(&self.path) {
            Err(error) if error.kind() == io::ErrorKind::NotFound => None,
            Err(error) => return Err(error),
            Ok(metadata) => {
                self.check_leaf(&metadata)?;
                let file = OpenOptions::new()
                    .read(true)
                    .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
                    .open(&self.path)?;
                let actual = file.metadata()?;
                self.check_leaf(&actual)?;
                if stamp(&actual) != stamp(&metadata) {
                    return Err(invalid_path());
                }
                let mut bytes = Vec::new();
                (&file)
                    .take(SETTINGS_FILE_LIMIT + 1)
                    .read_to_end(&mut bytes)?;
                let after = file.metadata()?;
                self.check_leaf(&after)?;
                if bytes.len() as u64 > SETTINGS_FILE_LIMIT || stamp(&after) != stamp(&actual) {
                    return Err(invalid_path());
                }
                hash.update(stamp(&actual));
                hash.update(&bytes);
                Some(bytes)
            }
        };
        let revision = format!(
            "{:x}",
            Sha256::new()
                .chain_update(key.as_bytes())
                .chain_update(hash.finalize())
                .finalize()
        );
        Ok(Snapshot { bytes, revision })
    }

    pub(super) fn publish(
        &self,
        original: &Snapshot,
        key: &Uuid,
        bytes: &[u8],
    ) -> Result<bool, PublishError> {
        if bytes.len() as u64 >= SETTINGS_FILE_LIMIT {
            return Err(PublishError::Before);
        }
        let directory = self.directory().map_err(|_| PublishError::Before)?;
        if self.read(key).map_err(|_| PublishError::Before)?.revision != original.revision {
            return Ok(false);
        }

        let mut temporary =
            tempfile::NamedTempFile::new_in(&self.parent).map_err(|_| PublishError::Before)?;
        temporary
            .as_file()
            .set_permissions(fs::Permissions::from_mode(0o600))
            .map_err(|_| PublishError::Before)?;
        temporary
            .write_all(bytes)
            .and_then(|()| temporary.write_all(b"\n"))
            .map_err(|_| PublishError::Before)?;
        temporary
            .as_file()
            .sync_all()
            .map_err(|_| PublishError::Before)?;
        if self.read(key).map_err(|_| PublishError::Before)?.revision != original.revision {
            return Ok(false);
        }

        let published = if original.bytes.is_some() {
            temporary.persist(&self.path)
        } else {
            temporary.persist_noclobber(&self.path)
        };
        match published {
            Err(error) if error.error.kind() == io::ErrorKind::AlreadyExists => return Ok(false),
            Err(_) => return Err(PublishError::Before),
            Ok(_) => {}
        }
        directory.sync_all().map_err(|_| PublishError::After)?;
        Ok(true)
    }
}
