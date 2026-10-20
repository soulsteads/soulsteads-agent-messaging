#!/usr/bin/env python3
"""Prepare one isolated Linux systemd-user Telegram gateway instance."""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import stat
import subprocess
import sys
from pathlib import Path
from shlex import quote

SERVICE_NAME = "soulsteads-telegram-gateway.service"
TOKEN_NAME = "TELEGRAM_BOT_TOKEN"
USER_ID = re.compile(r"[1-9][0-9]{0,19}\Z")
SAFE_SYSTEMD_PATH = re.compile(r"/[A-Za-z0-9_./+\-]*\Z")


class SetupError(Exception):
    """An expected, secret-free setup refusal."""


def private_mode(path: Path, mode: int) -> None:
    os.chmod(path, mode)


def absolute_path(value: str, label: str) -> Path:
    if "\x00" in value:
        raise SetupError(f"{label} is invalid")
    return Path(value).expanduser().absolute()


def systemd_safe_path(path: Path, label: str) -> None:
    text = str(path)
    if not SAFE_SYSTEMD_PATH.fullmatch(text):
        raise SetupError(f"{label} contains unsupported systemd characters")


def require_executable(value: str, label: str) -> Path:
    path = absolute_path(value, label)
    if not path.is_file() or not os.access(path, os.X_OK):
        raise SetupError(f"{label} must be an executable file")
    return path.resolve()


def read_token(value: str) -> str:
    path = absolute_path(value, "environment file")
    try:
        flags = os.O_RDONLY | os.O_NONBLOCK | os.O_NOFOLLOW
        descriptor = os.open(path, flags)
    except OSError:
        if path.is_symlink():
            raise SetupError("environment file must not be a symlink") from None
        raise SetupError("environment file cannot be read") from None
    try:
        metadata = os.fstat(descriptor)
        if not stat.S_ISREG(metadata.st_mode):
            raise SetupError("environment file must be a regular file")
        if metadata.st_mode & 0o077:
            raise SetupError("environment file must not be readable by group or others")
        if metadata.st_size > 1024 * 1024:
            raise SetupError("environment file exceeds size limit")
        chunks: list[bytes] = []
        while True:
            chunk = os.read(descriptor, 65536)
            if not chunk:
                break
            chunks.append(chunk)
            if sum(map(len, chunks)) > 1024 * 1024:
                raise SetupError("environment file exceeds size limit")
        lines = b"".join(chunks).decode("utf-8").splitlines()
    except (OSError, UnicodeError):
        raise SetupError("environment file cannot be read") from None
    finally:
        os.close(descriptor)
    values: list[str] = []
    for line in lines:
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        if "=" not in line:
            raise SetupError("environment file must be token-only KEY=value data")
        key, value = line.split("=", 1)
        if key != TOKEN_NAME:
            raise SetupError("environment file must be token-only Telegram credentials")
        values.append(value)
    if len(values) != 1 or not values[0] or "\r" in values[0] or "\n" in values[0]:
        raise SetupError("environment file must contain one non-empty Telegram token")
    return values[0]


def write_private(path: Path, content: str, mode: int) -> None:
    path.write_text(content, encoding="utf-8")
    private_mode(path, mode)


def runtime_config(root: Path, backend: str, telegram_user: str) -> dict[str, object]:
    runtime = root / "runtime"
    return {
        "sessionStorePath": str((runtime / "chat-sessions.json").resolve()),
        "acp": {"defaultBackend": backend, "backends": {backend: {"command": f"builtin:{backend}", "args": []}}},
        "matrix": {"enabled": False},
        "mattermost": {"enabled": False},
        "whatsapp": {"enabled": False},
        "slack": {"enabled": False},
        "discord": {"enabled": False},
        "web": {"enabled": False},
        "telegram": {"enabled": True, "token": "", "allowedUsers": [telegram_user], "interactivePermissions": True},
    }


def runner_text(root: Path, binary: Path, backend: str, harness: Path | None) -> str:
    runtime = root / "runtime"
    config = root / "config" / "soulsteads-messaging.json"
    assignments = [
        f"export SESSION_BASE_DIR={quote(str(runtime / 'sessions'))}",
        f"export PATH={quote(os.environ.get('PATH', os.defpath))}",
    ]
    if harness is not None:
        name = "CLAUDE_CODE_EXECUTABLE" if backend == "claude" else "CODEX_PATH"
        assignments.append(f"export {name}={quote(str(harness))}")
    return "\n".join(
        [
            "#!/bin/sh",
            "set -eu",
            "# Keeps the existing login HOME; gateway sessions are under SESSION_BASE_DIR.",
            "unset TELEGRAM_BOT_TOKEN",
            "unset TELEGRAM_ALLOWED_USERS",
            *assignments,
            f"cd {quote(str(runtime))}",
            f"exec {quote(str(binary))} --serve --config {quote(str(config))}",
            "",
        ]
    )


def unit_text(root: Path) -> str:
    runtime = root / "runtime"
    return "\n".join(
        [
            "[Unit]",
            "Description=Soulsteads isolated Telegram gateway",
            "After=default.target",
            "",
            "[Service]",
            "Type=simple",
            f"WorkingDirectory={runtime}",
            f"ExecStart={root / 'run-gateway'}",
            "Restart=on-failure",
            "RestartSec=5",
            "",
            "[Install]",
            "WantedBy=default.target",
            "",
        ]
    )


def checked_environment(runtime: Path, harness: Path | None, backend: str) -> dict[str, str]:
    environment = os.environ.copy()
    environment.pop(TOKEN_NAME, None)
    environment.pop("TELEGRAM_ALLOWED_USERS", None)
    environment["SESSION_BASE_DIR"] = str(runtime / "sessions")
    if harness is not None:
        environment["CLAUDE_CODE_EXECUTABLE" if backend == "claude" else "CODEX_PATH"] = str(harness)
    return environment


def check_config(binary: Path, root: Path, harness: Path | None, backend: str) -> None:
    runtime = root / "runtime"
    command = [str(binary), "--config", str(root / "config" / "soulsteads-messaging.json"), "--check-config"]
    try:
        result = subprocess.run(
            command,
            cwd=runtime,
            env=checked_environment(runtime, harness, backend),
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            timeout=30,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired):
        raise SetupError("supported binary could not check configuration") from None
    if result.returncode:
        raise SetupError("supported binary rejected configuration")


def prepare(arguments: argparse.Namespace) -> None:
    if not USER_ID.fullmatch(arguments.telegram_user):
        raise SetupError("telegram user must be a non-empty numeric ID")
    root = absolute_path(arguments.root, "root")
    systemd_safe_path(root, "root")
    if os.path.lexists(root):
        raise SetupError("root already exists; refusing to overwrite existing evidence")
    binary = require_executable(arguments.binary, "binary")
    harness_value = arguments.harness_binary or shutil.which(arguments.backend)
    if not harness_value:
        raise SetupError("selected harness was not found; provide --harness-binary")
    harness = require_executable(harness_value, "harness binary")
    token = read_token(arguments.env_file)

    root.mkdir(mode=0o700, parents=True)
    private_mode(root, 0o700)
    config_dir = root / "config"
    runtime = root / "runtime"
    unit_dir = root / "systemd" / "user"
    for directory in (config_dir, runtime, runtime / "sessions", root / "systemd", unit_dir):
        directory.mkdir(mode=0o700)
        private_mode(directory, 0o700)
    config = config_dir / "soulsteads-messaging.json"
    write_private(config, json.dumps(runtime_config(root, arguments.backend, arguments.telegram_user), separators=(",", ":")) + "\n", 0o600)
    write_private(runtime / ".env", f"{TOKEN_NAME}={token}\n", 0o600)
    write_private(root / "run-gateway", runner_text(root, binary, arguments.backend, harness), 0o700)
    try:
        check_config(binary, root, harness, arguments.backend)
    except SetupError as error:
        raise SetupError(f"{error}; partial private root retained; correct inputs and use a fresh --root") from None
    write_private(unit_dir / SERVICE_NAME, unit_text(root), 0o600)
    print(f"Prepared isolated gateway at {root}; service is staged but not installed or started.")


def systemctl(command: list[str]) -> subprocess.CompletedProcess[bytes]:
    executable = shutil.which("systemctl")
    if executable is None:
        raise SetupError("systemctl is unavailable")
    try:
        return subprocess.run(
            [executable, "--user", *command],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            timeout=30,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired):
        raise SetupError("systemd user manager is unavailable") from None


def install_service(arguments: argparse.Namespace) -> None:
    root = absolute_path(arguments.root, "root")
    systemd_safe_path(root, "root")
    source = root / "systemd" / "user" / SERVICE_NAME
    if not source.is_file() or source.is_symlink():
        raise SetupError("prepared managed service unit is missing")
    config_home = Path(os.environ.get("XDG_CONFIG_HOME", str(Path.home() / ".config"))).expanduser().absolute()
    destination_dir = config_home / "systemd" / "user"
    destination = destination_dir / SERVICE_NAME
    if os.path.lexists(destination):
        try:
            identical_managed_unit = (
                destination.is_file()
                and not destination.is_symlink()
                and destination.read_bytes() == source.read_bytes()
            )
        except OSError:
            identical_managed_unit = False
        if not identical_managed_unit:
            raise SetupError("managed service destination already exists; refusing to clobber it")
    else:
        destination_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
        try:
            descriptor = os.open(destination, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
        except FileExistsError:
            raise SetupError("managed service destination appeared; refusing to clobber it") from None
        with os.fdopen(descriptor, "wb") as output:
            output.write(source.read_bytes())
    if not arguments.start:
        print(f"Installed {SERVICE_NAME}; it is not enabled or started.")
        return
    if systemctl(["show-environment"]).returncode:
        raise SetupError("systemd user manager is unavailable; service was not started")
    if systemctl(["daemon-reload"]).returncode:
        raise SetupError("systemd user manager refused service reload; service was not started")
    if systemctl(["enable", "--now", SERVICE_NAME]).returncode:
        raise SetupError("systemd could not enable and start the managed service")
    if systemctl(["is-active", "--quiet", SERVICE_NAME]).returncode:
        raise SetupError("managed service did not become active")
    print(f"Installed and started {SERVICE_NAME}.")


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description="Prepare an isolated Linux Telegram gateway; no Home integration.")
    commands = result.add_subparsers(dest="command", required=True, title="commands")
    prepare_parser = commands.add_parser("prepare", help="stage private config, runner, and user unit without installing it")
    prepare_parser.add_argument("--backend", choices=("claude", "codex"), required=True)
    prepare_parser.add_argument("--telegram-user", required=True, help="one numeric Telegram user ID")
    prepare_parser.add_argument("--env-file", required=True, help="private caller .env containing TELEGRAM_BOT_TOKEN")
    prepare_parser.add_argument("--binary", required=True, help="absolute or relative soulsteads-messaging executable")
    prepare_parser.add_argument("--harness-binary", help="optional executable for the selected Claude or Codex backend")
    prepare_parser.add_argument("--root", default=os.path.join(os.environ.get("XDG_DATA_HOME", str(Path.home() / ".local" / "share")), "soulsteads-agent-messaging"))
    install_parser = commands.add_parser("install-service", help="copy only this staged unit into the user systemd directory")
    install_parser.add_argument("--root", default=os.path.join(os.environ.get("XDG_DATA_HOME", str(Path.home() / ".local" / "share")), "soulsteads-agent-messaging"))
    install_parser.add_argument("--start", action="store_true", help="after installing, enable --now and verify the service is active")
    return result


def main(argv: list[str] | None = None) -> int:
    arguments = parser().parse_args(argv)
    try:
        if arguments.command == "prepare":
            prepare(arguments)
        else:
            install_service(arguments)
    except SetupError as error:
        print(f"error: {error}", file=sys.stderr)
        return 1
    except OSError:
        print("error: filesystem operation failed", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
