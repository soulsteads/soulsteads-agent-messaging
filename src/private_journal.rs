//! Owner-only, atomically replaced JSON journal storage.

use anyhow::{Context, Result, ensure};
use std::path::Path;

pub(crate) use platform::Disk;

#[cfg(unix)]
mod platform {
    use super::*;
    use std::{
        fs::{self, File, OpenOptions},
        io::{Read, Write},
        os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt},
        path::PathBuf,
    };

    pub(crate) struct Disk {
        path: PathBuf,
        directory: File,
        filename: &'static str,
        limit: usize,
    }
    impl Disk {
        pub(crate) fn open(path: &Path, filename: &'static str, limit: usize) -> Result<Self> {
            let directory = Self::open_directory(path)?;
            directory
                .try_lock()
                .context("Journal directory is already in use")?;
            let disk = Self {
                path: path.into(),
                directory,
                filename,
                limit,
            };
            disk.rebind()?;
            Ok(disk)
        }

        /// Check directory admission without taking a lock or opening the journal.
        pub(crate) fn validate_directory(path: &Path) -> Result<()> {
            let directory = Self::open_directory(path)?;
            Self::check_binding(path, &directory)
        }

        fn open_directory(path: &Path) -> Result<File> {
            ensure!(
                path.is_absolute() && path != Path::new("/") && path.canonicalize()? == path,
                "Journal directory must be an existing absolute, non-symlink directory"
            );
            let directory = OpenOptions::new()
                .read(true)
                .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW)
                .open(path)?;
            let metadata = directory.metadata()?;
            // SAFETY: geteuid has no preconditions and does not change process state.
            ensure!(
                metadata.uid() == unsafe { libc::geteuid() } && metadata.mode() & 0o777 == 0o700,
                "Journal directory must be owned by the bridge and have mode 0700"
            );
            Ok(directory)
        }

        fn rebind(&self) -> Result<()> {
            Self::check_binding(&self.path, &self.directory)
        }

        fn check_binding(path: &Path, directory: &File) -> Result<()> {
            ensure!(path.canonicalize()? == path, "Journal directory changed");
            let named = fs::symlink_metadata(path)?;
            let opened = directory.metadata()?;
            ensure!(
                named.is_dir()
                    && named.dev() == opened.dev()
                    && named.ino() == opened.ino()
                    && named.mode() & 0o777 == 0o700,
                "Journal directory changed"
            );
            Ok(())
        }

        pub(crate) fn read(&self) -> Result<Option<Vec<u8>>> {
            self.rebind()?;
            let path = self.path.join(self.filename);
            let file = match OpenOptions::new()
                .read(true)
                .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
                .open(&path)
            {
                Ok(file) => file,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
                Err(error) => return Err(error.into()),
            };
            let before = file.metadata()?;
            ensure!(
                before.is_file()
                    && before.nlink() == 1
                    && before.mode() & 0o777 == 0o600
                    && before.uid() == self.directory.metadata()?.uid()
                    && before.len() <= self.limit as u64,
                "Unsafe or oversized journal"
            );
            let mut bytes = Vec::new();
            (&file)
                .take(self.limit as u64 + 1)
                .read_to_end(&mut bytes)?;
            let identity = |m: fs::Metadata| {
                (
                    m.dev(),
                    m.ino(),
                    m.nlink(),
                    m.mode(),
                    m.uid(),
                    m.len(),
                    m.mtime(),
                    m.mtime_nsec(),
                    m.ctime(),
                    m.ctime_nsec(),
                )
            };
            ensure!(
                bytes.len() as u64 == before.len()
                    && identity(before.clone()) == identity(file.metadata()?)
                    && identity(before) == identity(fs::symlink_metadata(path)?),
                "journal changed during read"
            );
            self.rebind()?;
            Ok(Some(bytes))
        }

        pub(crate) fn write(&self, bytes: &[u8], initialize: bool) -> Result<()> {
            ensure!(bytes.len() <= self.limit, "Private journal is too large");
            self.rebind()?;
            let mut tmp = tempfile::NamedTempFile::new_in(&self.path)?;
            tmp.as_file()
                .set_permissions(fs::Permissions::from_mode(0o600))?;
            tmp.write_all(bytes)?;
            tmp.as_file().sync_all()?;
            self.rebind()?;
            let path = self.path.join(self.filename);
            let file = if initialize {
                tmp.persist_noclobber(&path)
            } else {
                tmp.persist(&path)
            }
            .map_err(|e| e.error)?;
            self.directory.sync_all()?;
            self.rebind()?;
            let named = fs::symlink_metadata(self.path.join(self.filename))?;
            let opened = file.metadata()?;
            ensure!(
                named.is_file()
                    && named.nlink() == 1
                    && named.dev() == opened.dev()
                    && named.ino() == opened.ino()
                    && named.len() == bytes.len() as u64,
                "Journal publication changed"
            );
            Ok(())
        }
    }
}

#[cfg(not(unix))]
mod platform {
    use super::*;
    pub(crate) struct Disk;
    impl Disk {
        pub(crate) fn open(_: &Path, _: &'static str, _: usize) -> Result<Self> {
            anyhow::bail!("Private journals require Unix")
        }
        pub(crate) fn validate_directory(_: &Path) -> Result<()> {
            anyhow::bail!("Private journals require Unix")
        }
        pub(crate) fn read(&self) -> Result<Option<Vec<u8>>> {
            unreachable!()
        }
        pub(crate) fn write(&self, _: &[u8], _: bool) -> Result<()> {
            unreachable!()
        }
    }
}
