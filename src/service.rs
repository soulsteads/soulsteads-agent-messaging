use anyhow::{Context, Result, bail};
use std::{
    env, fs,
    io::Write,
    path::{Path, PathBuf},
};

pub const UNIT_NAME: &str = "soulsteads-messaging.service";
const SYSTEM_UNIT_DIRECTORY: &str = "/etc/systemd/system";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Scope {
    User,
    System,
}

pub struct InstallRequest {
    pub scope: Scope,
    pub run_as: Option<String>,
    pub dry_run: bool,
    pub config: PathBuf,
}

pub fn install(request: InstallRequest) -> Result<String> {
    let current_directory = env::current_dir().context("could not determine current directory")?;
    let config = absolute_from(&current_directory, &request.config)?;
    let executable = env::current_exe().context("could not determine executable path")?;
    let run_as = match request.scope {
        Scope::User => {
            if request.run_as.is_some() {
                bail!("--run-as is only valid with --system");
            }
            None
        }
        Scope::System => Some(validate_run_as(
            request
                .run_as
                .as_deref()
                .context("--system requires --run-as USER")?,
        )?),
    };
    let unit = unit_text(
        request.scope,
        run_as.as_deref(),
        &current_directory,
        &executable,
        &config,
    )?;
    if request.dry_run {
        return Ok(unit);
    }

    let destination = destination(request.scope)?;
    if request.scope == Scope::User {
        let parent = destination
            .parent()
            .context("unit destination has no parent")?;
        fs::create_dir_all(parent).context("could not create user systemd unit directory")?;
    }
    let parent = destination
        .parent()
        .context("unit destination has no parent")?;
    if !parent.is_dir() {
        bail!("systemd unit directory is unavailable");
    }
    write_new(&destination, unit.as_bytes())?;
    Ok(format!(
        "Installed {UNIT_NAME} at {}. It has not been reloaded, enabled, or started.",
        destination.display()
    ))
}

fn destination(scope: Scope) -> Result<PathBuf> {
    match scope {
        Scope::User => {
            let config_home = match env::var_os("XDG_CONFIG_HOME") {
                Some(value) => PathBuf::from(value),
                None => env::var_os("HOME")
                    .map(PathBuf::from)
                    .context("HOME is required when XDG_CONFIG_HOME is unset")?
                    .join(".config"),
            };
            let config_home = absolute_from(&env::current_dir()?, &config_home)?;
            Ok(config_home.join("systemd/user").join(UNIT_NAME))
        }
        Scope::System => Ok(Path::new(SYSTEM_UNIT_DIRECTORY).join(UNIT_NAME)),
    }
}

fn write_new(destination: &Path, content: &[u8]) -> Result<()> {
    write_new_with(destination, |output| output.write_all(content))
}

#[cfg(target_os = "linux")]
fn write_new_with(
    destination: &Path,
    write: impl FnOnce(&mut fs::File) -> std::io::Result<()>,
) -> Result<()> {
    use std::{
        ffi::CString,
        fs::OpenOptions,
        os::{
            fd::{AsRawFd, FromRawFd},
            unix::{
                ffi::OsStrExt,
                fs::{MetadataExt, OpenOptionsExt},
            },
        },
    };
    let parent = destination
        .parent()
        .context("unit destination has no parent")?;
    let name = CString::new(
        destination
            .file_name()
            .context("unit destination has no filename")?
            .as_bytes(),
    )?;
    let directory = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW)
        .open(parent)
        .context("could not open systemd unit directory")?;
    let directory_metadata = directory.metadata()?;
    let same_inode = |a: &fs::Metadata, b: &fs::Metadata| a.dev() == b.dev() && a.ino() == b.ino();
    // Keep the staging inode unnamed until the complete unit is durable.
    let fd = unsafe {
        libc::openat(
            directory.as_raw_fd(),
            c".".as_ptr(),
            libc::O_TMPFILE | libc::O_WRONLY | libc::O_CLOEXEC,
            0o600,
        )
    };
    if fd < 0 {
        return Err(std::io::Error::last_os_error())
            .context("could not stage an unnamed systemd unit");
    }
    let mut output = unsafe { fs::File::from_raw_fd(fd) };
    write(&mut output)
        .and_then(|()| output.sync_all())
        .context("could not write staged systemd unit")?;
    if !same_inode(&fs::symlink_metadata(parent)?, &directory_metadata) {
        bail!("systemd unit directory changed before installation");
    }
    // Following the procfs descriptor link publishes the exact inode and fails
    // if the destination already exists, including when it is a symlink.
    let source = CString::new(format!("/proc/self/fd/{}", output.as_raw_fd()))?;
    if unsafe {
        libc::linkat(
            libc::AT_FDCWD,
            source.as_ptr(),
            directory.as_raw_fd(),
            name.as_ptr(),
            libc::AT_SYMLINK_FOLLOW,
        )
    } != 0
    {
        let error = std::io::Error::last_os_error();
        if error.kind() == std::io::ErrorKind::AlreadyExists {
            bail!(
                "refusing to overwrite existing unit at {}",
                destination.display()
            );
        }
        return Err(error).context("could not publish systemd unit");
    }
    directory
        .sync_all()
        .context("unit was published but directory sync failed; inspect it before retrying")?;
    let installed = fs::symlink_metadata(destination)
        .context("unit was published but could not be verified")?;
    if !same_inode(&installed, &output.metadata()?) || installed.nlink() != 1 {
        bail!("published systemd unit changed during installation");
    }
    Ok(())
}

#[cfg(not(target_os = "linux"))]
fn write_new_with(
    _destination: &Path,
    _write: impl FnOnce(&mut fs::File) -> std::io::Result<()>,
) -> Result<()> {
    bail!("systemd unit installation is supported only on Linux")
}

fn absolute_from(base: &Path, path: &Path) -> Result<PathBuf> {
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        base.join(path)
    };
    let _ = absolute
        .to_str()
        .context("service paths must be valid UTF-8")?;
    Ok(absolute)
}

fn validate_run_as(value: &str) -> Result<String> {
    if value == "root" || value == "0" {
        bail!("--run-as must name a non-root Unix user");
    }
    let mut characters = value.chars();
    let Some(first) = characters.next() else {
        bail!("--run-as must name a Unix user");
    };
    if !(first.is_ascii_alphabetic() || first == '_')
        || !characters.all(|character| {
            character.is_ascii_alphanumeric() || matches!(character, '_' | '-' | '.')
        })
    {
        bail!("--run-as must be a plain Unix user name");
    }
    Ok(value.to_owned())
}

fn unit_text(
    scope: Scope,
    run_as: Option<&str>,
    working_directory: &Path,
    executable: &Path,
    config: &Path,
) -> Result<String> {
    let working_directory = systemd_directory(working_directory)?;
    let executable = systemd_command_word(executable, "executable")?;
    let config = systemd_command_word(config, "configuration path")?;
    let mut lines = vec![
        "[Unit]".to_owned(),
        "Description=Soulsteads Agent Messaging".to_owned(),
        "After=network-online.target".to_owned(),
        "Wants=network-online.target".to_owned(),
        String::new(),
        "[Service]".to_owned(),
        "Type=simple".to_owned(),
    ];
    if let Some(run_as) = run_as {
        lines.push(format!("User={run_as}"));
    }
    lines.extend([
        format!("WorkingDirectory={working_directory}"),
        format!("ExecStart={executable} --serve --config {config}"),
        "Restart=on-failure".to_owned(),
        "RestartSec=5".to_owned(),
        "UMask=0077".to_owned(),
        String::new(),
        "[Install]".to_owned(),
        match scope {
            Scope::User => "WantedBy=default.target".to_owned(),
            Scope::System => "WantedBy=multi-user.target".to_owned(),
        },
        String::new(),
    ]);
    Ok(lines.join("\n"))
}

fn systemd_directory(path: &Path) -> Result<String> {
    let value = path
        .to_str()
        .context("working directory must be valid UTF-8")?;
    if !path.is_absolute()
        || value.chars().any(char::is_control)
        || value.trim_end() != value
        || value.ends_with('\\')
    {
        bail!("working directory cannot be represented as a systemd path");
    }
    // WorkingDirectory= is a scalar path, not an ExecStart= argument list;
    // spaces are literal and enclosing quotes would become part of the path.
    Ok(value.replace('%', "%%"))
}

fn systemd_command_word(path: &Path, label: &str) -> Result<String> {
    let value = path
        .to_str()
        .with_context(|| format!("{label} must be valid UTF-8"))?;
    if value.is_empty() || value.chars().any(char::is_control) {
        bail!("{label} contains unsupported control characters");
    }
    Ok(format!(
        "\"{}\"",
        value
            .replace('\\', "\\\\")
            .replace('"', "\\\"")
            .replace('%', "%%")
            .replace('$', "$$")
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    #[test]
    #[cfg(target_os = "linux")]
    fn failed_stage_never_exposes_a_partial_unit() {
        let directory = tempfile::tempdir().unwrap();
        let unit = directory.path().join(UNIT_NAME);
        let mut visible_during_write = false;
        let result = write_new_with(&unit, |file| {
            file.write_all(b"[Unit]\n")?;
            visible_during_write = unit.symlink_metadata().is_ok();
            Err(std::io::Error::other("injected write failure"))
        });
        assert!(result.is_err());
        assert!(
            !visible_during_write,
            "the final unit was visible before publication"
        );
        assert!(!unit.exists());
        assert_eq!(directory.path().read_dir().unwrap().count(), 0);
    }

    #[test]
    #[cfg(target_os = "linux")]
    fn complete_unit_is_published_without_replacing_a_conflict() {
        let directory = tempfile::tempdir().unwrap();
        let unit = directory.path().join(UNIT_NAME);
        let initial = b"[Unit]\nDescription=Fixture\n";
        write_new(&unit, initial).unwrap();
        assert_eq!(fs::read(&unit).unwrap(), initial);
        assert!(write_new(&unit, b"replacement").is_err());
        assert_eq!(fs::read(&unit).unwrap(), initial);
        assert_eq!(directory.path().read_dir().unwrap().count(), 1);
    }

    #[test]
    fn unit_quotes_command_arguments_and_preserves_literal_specifiers() {
        let unit = unit_text(
            Scope::User,
            None,
            Path::new("/work dir/%home"),
            Path::new("/bin/soulsteads messaging"),
            Path::new("/config/$token file.json"),
        )
        .unwrap();

        assert!(unit.contains("WorkingDirectory=/work dir/%%home\n"));
        assert!(unit.contains(
            "ExecStart=\"/bin/soulsteads messaging\" --serve --config \"/config/$$token file.json\""
        ));
    }
}
