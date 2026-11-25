"""Regression tests for qualification-runner command-line handling."""

import json
import os
from pathlib import Path
import runpy
import subprocess
import sys
import tempfile
import unittest
import uuid
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[1]
RUNNER = REPO_ROOT / "scripts" / "qualify_matrix_opencode.py"


class QualificationRunnerCliTests(unittest.TestCase):
    def run_with_fake_cargo(self, *args: str) -> tuple[subprocess.CompletedProcess[str], bool]:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            fake_bin = temp_path / "bin"
            fake_bin.mkdir()
            marker = temp_path / "cargo-was-called"
            fake_cargo = fake_bin / "cargo"
            fake_cargo.write_text(
                "#!/bin/sh\n"
                "printf called > \"$CARGO_MARKER\"\n"
                "exit 99\n",
                encoding="utf-8",
            )
            fake_cargo.chmod(0o755)

            env = os.environ.copy()
            env["PATH"] = os.pathsep.join(
                part for part in (str(fake_bin), env.get("PATH", "")) if part
            )
            env["CARGO_MARKER"] = str(marker)
            result = subprocess.run(
                [sys.executable, str(RUNNER), *args],
                cwd=REPO_ROOT,
                env=env,
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                timeout=10,
                check=False,
            )
            return result, marker.exists()

    def run_with_successful_fake_cargo(
        self,
        *,
        output_root: Path | None = None,
        temp_root: Path | None = None,
        opencode_passed: int = 1,
        opencode_ignored: int = 0,
    ) -> tuple[subprocess.CompletedProcess[str], dict[str, Any] | None, list[str], bool]:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            fake_bin = temp_path / "bin"
            fake_bin.mkdir()
            cargo_test_marker = temp_path / "cargo-test-was-called"
            cargo_args_marker = temp_path / "cargo-args"
            fake_cargo = fake_bin / "cargo"
            fake_cargo.write_text(
                """#!/bin/sh
if [ "$1" = "--version" ]; then
  echo "cargo fake 1.0"
  exit 0
fi
if [ "$1" = "test" ]; then
  : > "$CARGO_TEST_MARKER"
  echo "$@" > "$CARGO_ARGS_MARKER"
  for target in matrix_restart matrix_approvals matrix_identity opencode_adapter; do
    echo "Running tests/${target}.rs"
    if [ "$target" = opencode_adapter ]; then
      echo "test result: ok. $OPENCODE_PASSED passed; 0 failed; $OPENCODE_IGNORED ignored; 0 measured; 0 filtered out"
    else
      echo "test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out"
    fi
  done
  exit 0
fi
exit 2
""",
                encoding="utf-8",
            )
            fake_cargo.chmod(0o755)
            fake_rustc = fake_bin / "rustc"
            fake_rustc.write_text(
                """#!/bin/sh
echo "rustc fake 1.0"
""",
                encoding="utf-8",
            )
            fake_rustc.chmod(0o755)

            env = os.environ.copy()
            env["PATH"] = os.pathsep.join(
                part for part in (str(fake_bin), env.get("PATH", "")) if part
            )
            env["CARGO_TEST_MARKER"] = str(cargo_test_marker)
            env["CARGO_ARGS_MARKER"] = str(cargo_args_marker)
            env["OPENCODE_PASSED"] = str(opencode_passed)
            env["OPENCODE_IGNORED"] = str(opencode_ignored)
            env["CARGO_TARGET_DIR"] = str(temp_path / "cargo-target")
            env["TMPDIR"] = str(temp_root or temp_path / "safe-temp")
            if output_root is None:
                env.pop("C2_QUALIFICATION_OUT_DIR", None)
            else:
                env["C2_QUALIFICATION_OUT_DIR"] = str(output_root)

            result = subprocess.run(
                [sys.executable, str(RUNNER)],
                cwd=REPO_ROOT,
                env=env,
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                timeout=10,
                check=False,
            )
            report = None
            report_dir = next(
                (
                    line.partition(": ")[2]
                    for line in result.stdout.splitlines()
                    if line.startswith("Report directory: ")
                ),
                None,
            )
            if report_dir:
                report_path = Path(report_dir) / "qualification-report.json"
                if report_path.is_file():
                    report = json.loads(report_path.read_text(encoding="utf-8"))
            args = (
                cargo_args_marker.read_text(encoding="utf-8").split()
                if cargo_args_marker.exists()
                else []
            )
            return result, report, args, cargo_test_marker.exists()

    def test_help_exits_before_cargo_is_invoked(self) -> None:
        result, cargo_was_called = self.run_with_fake_cargo("--help")

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("usage:", result.stdout.lower())
        self.assertFalse(cargo_was_called)

    def test_unknown_arguments_are_rejected_before_cargo_is_invoked(self) -> None:
        result, cargo_was_called = self.run_with_fake_cargo("--unexpected-option")

        self.assertEqual(result.returncode, 2, result.stderr)
        self.assertIn("unrecognized arguments", result.stderr)
        self.assertFalse(cargo_was_called)


    def test_report_binds_full_source_identity_and_keeps_paths_private(self) -> None:
        result, report, cargo_args, cargo_test_called = (
            self.run_with_successful_fake_cargo()
        )

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertTrue(cargo_test_called)
        self.assertIsNotNone(report)
        assert report is not None
        self.assertEqual(report["result"], "pass")
        self.assertEqual(report["qualification_kind"], "source-fixture")

        source = report["source"]
        self.assertIsInstance(source, dict)
        assert isinstance(source, dict)
        head = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=REPO_ROOT,
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
        tree = subprocess.run(
            ["git", "rev-parse", "HEAD^{tree}"],
            cwd=REPO_ROOT,
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
        self.assertEqual(source["commit"], head)
        self.assertEqual(source["commit_tree"], tree)
        self.assertNotIn("input_files", source)
        self.assertIn("all tracked files", source["snapshot_scope"])
        self.assertEqual(
            source["tracked_snapshot_sha256_before"],
            source["tracked_snapshot_sha256_after"],
        )
        self.assertEqual(len(source["tracked_snapshot_sha256_before"]), 64)
        self.assertIsInstance(source["working_tree_clean_before"], bool)
        self.assertIsInstance(source["working_tree_clean_after"], bool)
        self.assertTrue(source["unchanged_during_test"])
        self.assertTrue(source["gitlinks_verified"])
        self.assertEqual(source["gitlinks_before"], source["gitlinks_after"])
        self.assertEqual(
            cargo_args[:5],
            ["test", "--locked", "--no-default-features", "--features", "matrix-encryption"],
        )
        self.assertEqual(
            [cargo_args[index + 1] for index, value in enumerate(cargo_args) if value == "--test"],
            ["matrix_restart", "matrix_approvals", "matrix_identity", "opencode_adapter"],
        )
        targets = [item["target"] for item in report["test_targets"]]
        self.assertEqual(
            targets,
            [
                "matrix_restart.rs",
                "matrix_approvals.rs",
                "matrix_identity.rs",
                "opencode_adapter.rs",
            ],
        )

        self.assertNotIn("report_directory", report)
        self.assertNotIn("cargo_target_dir", report["toolchain"])
        self.assertEqual(report["cargo_test_log"]["path"], "cargo-test.log")

        def assert_relative_strings(value: object) -> None:
            if isinstance(value, dict):
                for nested in value.values():
                    assert_relative_strings(nested)
            elif isinstance(value, list):
                for nested in value:
                    assert_relative_strings(nested)
            elif isinstance(value, str):
                self.assertFalse(Path(value).is_absolute(), value)

        assert_relative_strings(report)

    def test_each_target_must_have_a_passing_test(self) -> None:
        for passed, ignored, expected_result in (
            (0, 0, "fail"),
            (0, 1, "fail"),
            (1, 1, "pass"),
        ):
            with self.subTest(opencode_passed=passed, opencode_ignored=ignored):
                result, report, _, cargo_test_called = self.run_with_successful_fake_cargo(
                    opencode_passed=passed, opencode_ignored=ignored
                )

                self.assertTrue(cargo_test_called)
                self.assertIsNotNone(report, result.stderr)
                assert report is not None
                self.assertEqual(report["test_exit_code"], 0)
                self.assertTrue(report["source"]["unchanged_during_test"])
                self.assertTrue(report["source"]["gitlinks_verified"])
                self.assertEqual(
                    [
                        (row["target"], row["passed"], row["failed"], row["ignored"])
                        for row in report["test_targets"]
                    ],
                    [
                        ("matrix_restart.rs", 1, 0, 0),
                        ("matrix_approvals.rs", 1, 0, 0),
                        ("matrix_identity.rs", 1, 0, 0),
                        ("opencode_adapter.rs", passed, 0, ignored),
                    ],
                )
                self.assertEqual(report["result"], expected_result)
                self.assertEqual(result.returncode, 0 if expected_result == "pass" else 1)

    def test_override_inside_source_is_rejected_before_directory_creation(self) -> None:
        output_root = REPO_ROOT / f".c2-output-refusal-{uuid.uuid4().hex}"
        result, report, _, cargo_test_called = self.run_with_successful_fake_cargo(
            output_root=output_root
        )

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("outside the source tree", result.stderr)
        self.assertIsNone(report)
        self.assertFalse(output_root.exists())
        self.assertFalse(cargo_test_called)

    def test_default_tmpdir_inside_source_is_rejected_before_directory_creation(self) -> None:
        temp_root = REPO_ROOT / f".c2-default-temp-refusal-{uuid.uuid4().hex}"
        result, report, _, cargo_test_called = self.run_with_successful_fake_cargo(
            temp_root=temp_root
        )

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("outside the source tree", result.stderr)
        self.assertIsNone(report)
        self.assertFalse(temp_root.exists())
        self.assertFalse(cargo_test_called)


    def test_tracked_source_snapshot_handles_tracked_directory_symlink(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            temporary_root = Path(temporary_directory)
            repo = temporary_root / "repo"
            repo.mkdir()
            linked_directory = temporary_root / "linked-directory"
            linked_directory.mkdir()
            payload = b"target contents, not link payload\n"
            (linked_directory / "payload").write_bytes(payload)
            other_directory = temporary_root / "other-directory"
            other_directory.mkdir()
            (other_directory / "payload").write_bytes(payload)
            link = repo / "tracked-directory-link"
            link.symlink_to("../linked-directory", target_is_directory=True)

            env = os.environ.copy()
            for name in ("GIT_INDEX_FILE", "GIT_DIR", "GIT_WORK_TREE", "GIT_COMMON_DIR"):
                env.pop(name, None)

            def git(*args: str) -> subprocess.CompletedProcess[bytes]:
                return subprocess.run(
                    ["git", *args],
                    cwd=repo,
                    env=env,
                    check=True,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                )

            git("init", "--quiet")
            git("config", "user.name", "Qualification test")
            git("config", "user.email", "qualification-test@example.invalid")
            git("add", "--", "tracked-directory-link")
            git("commit", "--quiet", "-m", "track directory symlink")

            snapshot_globals = runpy.run_path(str(RUNNER))
            snapshot_globals["tracked_source_snapshot"].__globals__["REPO_ROOT"] = repo
            for helper_name in ("output_of", "git_bytes"):
                snapshot_globals[helper_name].__kwdefaults__["cwd"] = repo
            snapshot = snapshot_globals["tracked_source_snapshot"]

            clean = snapshot()
            self.assertTrue(clean["working_tree_clean"])
            self.assertEqual(
                git("cat-file", "-p", "HEAD:tracked-directory-link").stdout,
                b"../linked-directory",
            )

            link.unlink()
            link.symlink_to("../other-directory", target_is_directory=True)
            changed = snapshot()
            self.assertFalse(changed["working_tree_clean"])
            self.assertEqual(clean["commit_tree"], changed["commit_tree"])
            self.assertNotEqual(
                clean["tracked_snapshot_sha256"], changed["tracked_snapshot_sha256"]
            )
            self.assertNotEqual(
                clean["working_tree_status_sha256"], changed["working_tree_status_sha256"]
            )


if __name__ == "__main__":
    unittest.main()
