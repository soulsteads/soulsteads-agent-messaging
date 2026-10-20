#!/usr/bin/env python3
"""Black-box tests for the bounded Telegram gateway setup helper."""

from __future__ import annotations

import argparse
import importlib.util
import json
import os
import shlex
import stat
import subprocess
import sys
import tempfile
import textwrap
import unittest
from pathlib import Path
from unittest import mock


REPO = Path(__file__).resolve().parents[1]
HELPER = REPO / "scripts" / "setup_gateway.py"


class GatewaySetupTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.work = Path(self.temporary.name)
        self.root = self.work / "gateway"
        self.source_env = self.work / "caller.env"
        self.source_env.write_text("TELEGRAM_BOT_TOKEN=fixture-secret-token\n")
        self.source_env.chmod(0o600)
        self.binary = self.work / "soulsteads-messaging-fixture"
        self.binary.write_text(
            "#!/usr/bin/env python3\n"
            "import json, os, pathlib, sys\n"
            "if sys.argv[1:] != ['--config', sys.argv[2], '--check-config']:\n"
            "    raise SystemExit(12)\n"
            "config = json.loads(pathlib.Path(sys.argv[2]).read_text())\n"
            "if pathlib.Path.cwd() != pathlib.Path(os.environ['EXPECTED_RUNTIME']):\n"
            "    raise SystemExit(13)\n"
            "if pathlib.Path('.env').read_text() != 'TELEGRAM_BOT_TOKEN=fixture-secret-token\\n':\n"
            "    raise SystemExit(14)\n"
            "if os.environ.get('TELEGRAM_ALLOWED_USERS'):\n"
            "    raise SystemExit(15)\n"
            "if not pathlib.Path(os.environ['SESSION_BASE_DIR']).is_absolute():\n"
            "    raise SystemExit(16)\n"
            "if (os.environ.get('CLAUDE_CODE_EXECUTABLE') or os.environ.get('CODEX_PATH')) != os.environ['EXPECTED_HARNESS']:\n"
            "    raise SystemExit(18)\n"
            "print('checked')\n"
        )
        self.binary.chmod(0o700)

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def run_helper(self, *arguments: str, environment: dict[str, str] | None = None) -> subprocess.CompletedProcess[str]:
        env = os.environ.copy()
        env["EXPECTED_RUNTIME"] = str(self.root / "runtime")
        env["EXPECTED_HARNESS"] = str(self.binary.resolve())
        if environment:
            env.update(environment)
        return subprocess.run(
            [sys.executable, str(HELPER), *arguments],
            text=True,
            capture_output=True,
            env=env,
            check=False,
        )

    def prepare(self, *extra: str) -> subprocess.CompletedProcess[str]:
        return self.run_helper(
            "prepare",
            "--backend", "claude",
            "--telegram-user", "12345",
            "--env-file", str(self.source_env),
            "--binary", str(self.binary),
            "--harness-binary", str(self.binary),
            "--root", str(self.root),
            *extra,
        )

    def test_prepare_creates_isolated_telegram_only_gateway_and_checks_config(self) -> None:
        result = self.prepare()

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertNotIn("fixture-secret-token", result.stdout + result.stderr)
        runtime = self.root / "runtime"
        config = json.loads((self.root / "config" / "soulsteads-messaging.json").read_text())
        self.assertEqual(config["acp"]["defaultBackend"], "claude")
        self.assertEqual(config["telegram"], {"enabled": True, "token": "", "allowedUsers": ["12345"], "interactivePermissions": True})
        self.assertEqual(config["sessionStorePath"], str((runtime / "chat-sessions.json").resolve()))
        for platform in ("matrix", "mattermost", "whatsapp", "slack", "discord", "web"):
            self.assertEqual(config[platform], {"enabled": False})
        self.assertEqual((runtime / ".env").read_text(), "TELEGRAM_BOT_TOKEN=fixture-secret-token\n")
        runner = (self.root / "run-gateway").read_text()
        self.assertIn(str(self.binary.resolve()), runner)
        self.assertIn(str((self.root / "config" / "soulsteads-messaging.json").resolve()), runner)
        self.assertIn("unset TELEGRAM_BOT_TOKEN", runner)
        self.assertIn("unset TELEGRAM_ALLOWED_USERS", runner)
        self.assertIn("CLAUDE_CODE_EXECUTABLE", runner)
        self.assertIn("--serve", runner)
        self.assertFalse((self.root / "systemd" / "user" / "soulsteads-telegram-gateway.service").is_symlink())
        for path in (self.root, self.root / "config", runtime, runtime / "sessions", runtime / ".env", self.root / "run-gateway"):
            self.assertEqual(stat.S_IMODE(path.stat().st_mode) & 0o077, 0, path)

    def test_prepare_creates_missing_xdg_data_parent(self) -> None:
        self.root = self.work / "new-xdg" / "data" / "soulsteads-agent-messaging"
        result = self.run_helper(
            "prepare", "--backend", "claude", "--telegram-user", "12345",
            "--env-file", str(self.source_env), "--binary", str(self.binary),
            "--harness-binary", str(self.binary),
            environment={"XDG_DATA_HOME": str(self.root.parent)},
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertTrue((self.root / "config" / "soulsteads-messaging.json").is_file())
        self.assertEqual(stat.S_IMODE(self.root.stat().st_mode), 0o700)

    def test_prepare_resolves_default_harness_for_the_service_environment(self) -> None:
        tools = self.work / "tools"
        tools.mkdir()
        (tools / "claude").symlink_to(self.binary)
        environment = {
            "PATH": f"{tools}:{os.environ['PATH']}",
            "CLAUDE_CODE_EXECUTABLE": "ambient-override-not-the-selected-cli",
        }
        result = self.run_helper(
            "prepare", "--backend", "claude", "--telegram-user", "12345",
            "--env-file", str(self.source_env), "--binary", str(self.binary),
            "--root", str(self.root), environment=environment,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        runner = (self.root / "run-gateway").read_text()
        self.assertIn(f"export CLAUDE_CODE_EXECUTABLE={self.binary.resolve()}", runner)
        self.assertIn(f"export PATH={shlex.quote(environment['PATH'])}", runner)

    def test_prepare_codex_uses_codex_harness_environment(self) -> None:
        result = self.run_helper(
            "prepare", "--backend", "codex", "--telegram-user", "12345",
            "--env-file", str(self.source_env), "--binary", str(self.binary),
            "--harness-binary", str(self.binary), "--root", str(self.root),
        )

        self.assertEqual(result.returncode, 0, result.stderr)
        config = json.loads((self.root / "config" / "soulsteads-messaging.json").read_text())
        self.assertEqual(config["acp"]["defaultBackend"], "codex")
        runner = (self.root / "run-gateway").read_text()
        self.assertIn("CODEX_PATH", runner)
        self.assertNotIn("CLAUDE_CODE_EXECUTABLE", runner)

    def test_help_exposes_only_the_two_supported_commands(self) -> None:
        result = self.run_helper("--help")

        self.assertEqual(result.returncode, 0)
        self.assertIn("prepare", result.stdout)
        self.assertIn("install-service", result.stdout)
        self.assertIn("no Home integration", result.stdout)

    def test_failed_config_check_preserves_private_root_but_no_installable_unit(self) -> None:
        self.binary.write_text("#!/bin/sh\necho fixture-secret-token >&2\nexit 1\n")
        rejected = self.prepare()
        self.assertNotEqual(rejected.returncode, 0)
        self.assertNotIn("fixture-secret-token", rejected.stdout + rejected.stderr)
        self.assertIn("partial private root retained", rejected.stderr)
        self.assertTrue((self.root / "runtime" / ".env").is_file())
        self.assertEqual(stat.S_IMODE(self.root.stat().st_mode), 0o700)
        self.assertFalse((self.root / "systemd" / "user" / "soulsteads-telegram-gateway.service").exists())
        install = self.run_helper(
            "install-service", "--root", str(self.root),
            environment={"XDG_CONFIG_HOME": str(self.work / "isolated-config")},
        )
        self.assertNotEqual(install.returncode, 0)
        self.assertFalse((self.work / "isolated-config").exists())
        retry = self.prepare()
        self.assertNotEqual(retry.returncode, 0)
        self.assertIn("already exists", retry.stderr)
        self.binary.write_text("#!/bin/sh\nexit 0\n")
        self.root = self.work / "corrected-new-root"
        self.assertEqual(self.prepare().returncode, 0)

    def test_prepare_rejects_fifo_credentials_without_waiting_for_a_writer(self) -> None:
        self.source_env.unlink()
        os.mkfifo(self.source_env, 0o600)
        command = [
            sys.executable, str(HELPER), "prepare", "--backend", "claude",
            "--telegram-user", "12345", "--env-file", str(self.source_env),
            "--binary", str(self.binary), "--harness-binary", str(self.binary),
            "--root", str(self.root),
        ]
        try:
            result = subprocess.run(command, capture_output=True, text=True, timeout=2)
        except subprocess.TimeoutExpired:
            self.fail("non-regular credential input blocked instead of being refused")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("regular file", result.stderr)
        self.assertFalse(self.root.exists())

    def test_prepare_rejects_credential_files_with_unrelated_assignments(self) -> None:
        self.source_env.write_text("TELEGRAM_BOT_TOKEN=fixture-secret-token\nOTHER_SECRET=do-not-copy\n")
        self.source_env.chmod(0o600)

        result = self.prepare()

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("token-only", result.stderr)
        self.assertNotIn("fixture-secret-token", result.stdout + result.stderr)
        self.assertFalse(self.root.exists())

    def test_prepare_refuses_existing_or_unsafe_inputs_without_exposing_token(self) -> None:

        def existing_root() -> tuple[str, ...]:
            self.root.mkdir()
            marker = self.root / "keep"
            marker.write_text("evidence")
            return ()

        def symlinked_env() -> tuple[str, ...]:
            target = self.work / "actual.env"
            target.write_text("TELEGRAM_BOT_TOKEN=fixture-secret-token\n")
            target.chmod(0o600)
            self.source_env.unlink()
            self.source_env.symlink_to(target)
            return ()

        def public_env() -> tuple[str, ...]:
            self.source_env.chmod(0o644)
            return ()

        cases = [
            ("existing root", (), existing_root),
            ("symlinked credentials", (), symlinked_env),
            ("public credentials", (), public_env),
            ("missing token", (), lambda: (self.source_env.write_text("# no token\n"), self.source_env.chmod(0o600), ())[-1]),
            ("invalid user", ("--telegram-user", "12%3"), lambda: ()),
            ("missing binary", ("--binary", str(self.work / "missing-binary")), lambda: ()),
            ("systemd injection", ("--root", str(self.work / "gateway%n")), lambda: ()),
        ]
        for label, replacement, arrange in cases:
            with self.subTest(label):
                self.tearDown()
                self.setUp()
                arrange()
                arguments = list(replacement)
                if "--telegram-user" not in arguments:
                    arguments.extend(["--telegram-user", "12345"])
                if "--binary" not in arguments:
                    arguments.extend(["--binary", str(self.binary)])
                if "--root" not in arguments:
                    arguments.extend(["--root", str(self.root)])
                result = self.run_helper(
                    "prepare", "--backend", "claude", "--env-file", str(self.source_env),
                    "--harness-binary", str(self.binary), *arguments,
                )
                self.assertNotEqual(result.returncode, 0)
                self.assertNotIn("fixture-secret-token", result.stdout + result.stderr)
                if label == "existing root":
                    self.assertEqual((self.root / "keep").read_text(), "evidence")
                else:
                    self.assertFalse(self.root.exists())

    def test_install_refuses_unrelated_unit_without_starting_anything(self) -> None:
        self.assertEqual(self.prepare().returncode, 0)
        config_home = self.work / "xdg-config"
        destination = config_home / "systemd" / "user" / "soulsteads-telegram-gateway.service"
        destination.parent.mkdir(parents=True)
        destination.write_text("unrelated unit\n")

        result = self.run_helper("install-service", "--root", str(self.root), environment={"XDG_CONFIG_HOME": str(config_home)})

        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(destination.read_text(), "unrelated unit\n")

    def test_install_refuses_unit_created_after_absence_check(self) -> None:
        self.assertEqual(self.prepare().returncode, 0)
        config_home = self.work / "xdg-config"
        destination = config_home / "systemd" / "user" / "soulsteads-telegram-gateway.service"
        destination.parent.mkdir(parents=True)
        specification = importlib.util.spec_from_file_location("setup_gateway_race", HELPER)
        module = importlib.util.module_from_spec(specification)
        specification.loader.exec_module(module)
        original = module.os.path.lexists

        def appear_after_check(path: Path) -> bool:
            if Path(path) == destination:
                destination.write_text("unrelated concurrently created unit\n")
                return False
            return original(path)

        with mock.patch.dict(os.environ, {"XDG_CONFIG_HOME": str(config_home)}):
            with mock.patch.object(module.os.path, "lexists", side_effect=appear_after_check):
                with self.assertRaises(module.SetupError):
                    module.install_service(argparse.Namespace(root=str(self.root), start=False))
        self.assertEqual(destination.read_text(), "unrelated concurrently created unit\n")

    def test_start_requires_user_manager_before_activation(self) -> None:
        self.assertEqual(self.prepare().returncode, 0)
        config_home = self.work / "xdg-config"
        tools = self.work / "tools"
        tools.mkdir()
        log = self.work / "systemctl.log"
        systemctl = tools / "systemctl"
        systemctl.write_text(
            "#!/usr/bin/env python3\n"
            "import os, pathlib, sys\n"
            "pathlib.Path(os.environ['SYSTEMCTL_LOG']).open('a').write(' '.join(sys.argv[1:]) + '\\n')\n"
            "raise SystemExit(1)\n"
        )
        systemctl.chmod(0o700)

        result = self.run_helper(
            "install-service", "--root", str(self.root), "--start",
            environment={"XDG_CONFIG_HOME": str(config_home), "PATH": f"{tools}:{os.environ['PATH']}", "SYSTEMCTL_LOG": str(log)},
        )

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("was not started", result.stderr)
        self.assertEqual(log.read_text().splitlines(), ["--user show-environment"])

    def test_install_then_start_reuses_only_identical_managed_unit(self) -> None:
        self.assertEqual(self.prepare().returncode, 0)
        config_home = self.work / "xdg-config"
        tools = self.work / "tools"
        tools.mkdir()
        systemctl_log = self.work / "systemctl.log"
        systemctl = tools / "systemctl"
        systemctl.write_text(
            "#!/usr/bin/env python3\n"
            "import os, pathlib, sys\n"
            "pathlib.Path(os.environ['SYSTEMCTL_LOG']).open('a').write(' '.join(sys.argv[1:]) + '\\n')\n"
        )
        systemctl.chmod(0o700)
        environment = {
            "XDG_CONFIG_HOME": str(config_home),
            "PATH": f"{tools}:{os.environ['PATH']}",
            "SYSTEMCTL_LOG": str(systemctl_log),
        }

        first = self.run_helper("install-service", "--root", str(self.root), environment=environment)
        second = self.run_helper("install-service", "--root", str(self.root), "--start", environment=environment)

        self.assertEqual(first.returncode, 0, first.stderr)
        self.assertEqual(second.returncode, 0, second.stderr)
        installed = config_home / "systemd" / "user" / "soulsteads-telegram-gateway.service"
        self.assertEqual(installed.read_text(), (self.root / "systemd" / "user" / installed.name).read_text())
        self.assertEqual(
            systemctl_log.read_text().splitlines(),
            [
                "--user show-environment",
                "--user daemon-reload",
                "--user enable --now soulsteads-telegram-gateway.service",
                "--user is-active --quiet soulsteads-telegram-gateway.service",
            ],
        )


if __name__ == "__main__":
    unittest.main()
