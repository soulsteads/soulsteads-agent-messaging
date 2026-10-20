//! Resolve configured backends before the bridge spawns its ACP client process.
use anyhow::{Result, bail};

pub(crate) fn resolve(
    backend: &str,
    command: &str,
    args: Vec<String>,
    opencode_path: Option<&str>,
    transport: crate::backend::Transport,
) -> Result<(String, Vec<String>)> {
    if transport == crate::backend::Transport::OpenCodeNative {
        if command == "builtin:opencode" && args.is_empty() {
            return Ok((opencode_path.unwrap_or("opencode").into(), vec![]));
        }
        if command.starts_with("builtin:") {
            bail!("Built-in native transport requires OpenCode");
        }
        return Ok((
            if command == "opencode" {
                opencode_path.unwrap_or(command)
            } else {
                command
            }
            .into(),
            args,
        ));
    }
    if let Some(adapter) = command.strip_prefix("builtin:") {
        if !["opencode", "codex", "claude", "pi"].contains(&adapter)
            || backend != adapter
            || !args.is_empty()
        {
            bail!("Built-in adapter requires its matching backend and no custom arguments");
        }
        return Ok((
            std::env::current_exe()?.to_string_lossy().into_owned(),
            vec!["--acp-adapter".into(), adapter.into()],
        ));
    }
    if backend == "opencode" && command == "opencode" && args == ["acp"] && opencode_path.is_none()
    {
        return Ok((
            std::env::current_exe()?.to_string_lossy().into_owned(),
            vec!["--acp-adapter".into(), "opencode".into()],
        ));
    }
    let executable = if backend == "opencode" && command == "opencode" {
        opencode_path.unwrap_or(command)
    } else {
        command
    };
    Ok((executable.into(), args))
}
