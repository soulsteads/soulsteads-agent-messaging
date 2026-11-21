#!/usr/bin/env python3
"""Run the Matrix/OpenCode source-fixture qualification gate.

This runner never joins a Matrix room, starts a service, or contacts an OpenCode
model endpoint. It only runs the named Cargo integration-test targets.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import subprocess
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path

HARNESS_VERSION = "1.0.0"
FEATURES = "matrix-encryption"
TEST_TARGETS = (
    "matrix_restart",
    "matrix_approvals",
    "matrix_identity",
    "opencode_adapter",
)
REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = Path(__file__).resolve()


def output_of(command: list[str], *, cwd: Path = REPO_ROOT) -> str | None:
    try:
        result = subprocess.run(
            command,
            cwd=cwd,
            check=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )
    except OSError:
        return None
    return result.stdout.strip() if result.returncode == 0 else None


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def git_bytes(*args: str, cwd: Path = REPO_ROOT) -> bytes:
    try:
        result = subprocess.run(
            ["git", *args],
            cwd=cwd,
            check=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
    except OSError as error:
        raise SystemExit(f"cannot read Git source identity ({args[0]}): {error}") from error
    if result.returncode != 0:
        raise SystemExit(f"cannot read Git source identity ({args[0]})")
    return result.stdout


def tracked_source_snapshot() -> dict[str, object]:
    commit = output_of(["git", "rev-parse", "HEAD"])
    commit_tree = output_of(["git", "rev-parse", "HEAD^{tree}"])
    if commit is None or commit_tree is None:
        raise SystemExit("qualification requires a Git checkout with a commit")

    head_entries: dict[bytes, tuple[bytes, bytes]] = {}
    for entry in git_bytes("ls-tree", "-r", "--full-tree", "-z", "HEAD").split(b"\0"):
        if not entry:
            continue
        metadata, raw_path = entry.split(b"\t", 1)
        mode, _kind, object_id = metadata.split()
        head_entries[raw_path] = (mode, object_id)

    index_entries: dict[bytes, list[tuple[bytes, bytes, bytes]]] = {}
    for entry in git_bytes("ls-files", "--stage", "--sparse", "-z").split(b"\0"):
        if not entry:
            continue
        metadata, raw_path = entry.split(b"\t", 1)
        mode, object_id, stage = metadata.split()
        index_entries.setdefault(raw_path, []).append((mode, object_id, stage))

    untracked = [
        item
        for item in git_bytes("ls-files", "--others", "--exclude-standard", "-z").split(b"\0")
        if item
    ]
    digest = hashlib.sha256()
    status_digest = hashlib.sha256()
    gitlinks: list[dict[str, object]] = []
    working_tree_clean = not untracked
    for raw_path in sorted(set(head_entries) | set(index_entries)):
        head_entry = head_entries.get(raw_path)
        staged_entries = sorted(index_entries.get(raw_path, []))
        path = REPO_ROOT / os.fsdecode(raw_path)
        digest.update(raw_path + b"\0")
        digest.update(b"head\0" + (b" ".join(head_entry) if head_entry else b"absent"))
        digest.update(b"\0index\0")
        for mode, object_id, stage in staged_entries:
            digest.update(mode + b" " + object_id + b" " + stage + b"\0")
        if not head_entry or len(staged_entries) != 1 or staged_entries[0][2] != b"0":
            working_tree_clean = False
            status_digest.update(b"index\0" + raw_path + b"\0")
        elif (head_entry[0], head_entry[1]) != staged_entries[0][:2]:
            working_tree_clean = False
            status_digest.update(b"index\0" + raw_path + b"\0")

        head_mode, head_object = head_entry or (b"", b"")
        worktree_mode = b"gitlink"
        if head_mode == b"160000" or any(item[0] == b"160000" for item in staged_entries):
            checkout_commit = output_of(["git", "rev-parse", "HEAD"], cwd=path)
            link_status = b"unavailable"
            link_clean = False
            if checkout_commit is not None:
                try:
                    diff = git_bytes("diff-files", "--name-only", "-z", cwd=path)
                    staged_link = git_bytes(
                        "diff-index", "--name-only", "-z", "HEAD", "--", cwd=path
                    )
                    untracked_link = git_bytes(
                        "ls-files", "--others", "--exclude-standard", "-z", cwd=path
                    )
                    link_status = diff + staged_link + untracked_link
                    index_commit = staged_entries[0][1].decode("ascii") if staged_entries else None
                    link_clean = (
                        checkout_commit == head_object.decode("ascii")
                        and index_commit == head_object.decode("ascii")
                        and not link_status
                    )
                except SystemExit:
                    link_clean = False
            gitlinks.append(
                {
                    "path": os.fsdecode(raw_path),
                    "referenced_commit": head_object.decode("ascii") if head_object else None,
                    "index_commit": (
                        staged_entries[0][1].decode("ascii") if staged_entries else None
                    ),
                    "checkout_commit": checkout_commit,
                    "working_tree_clean": link_clean,
                }
            )
            digest.update(b"gitlink\0" + (checkout_commit or "unavailable").encode("utf-8"))
            digest.update(hashlib.sha256(link_status).digest())
            if not link_clean:
                working_tree_clean = False
                status_digest.update(b"gitlink\0" + raw_path + b"\0")
        elif path.is_symlink():
            worktree_mode = b"120000"
            content = os.fsencode(os.readlink(path))
            digest.update(b"symlink\0" + content)
        elif path.is_file():
            worktree_mode = b"100755" if path.stat().st_mode & 0o111 else b"100644"
            content = path.read_bytes()
            digest.update(b"file\0" + content)
        elif path.exists():
            worktree_mode = b"other"
            content = b""
            digest.update(b"other\0")
        else:
            worktree_mode = b"missing"
            content = b""
            digest.update(b"missing\0")

        if head_entry and head_mode not in (b"160000", b"040000"):
            relative_path = os.fsdecode(raw_path)
            if head_mode == b"120000" and path.is_symlink():
                result = subprocess.run(
                    ["git", "hash-object", "--stdin"],
                    cwd=REPO_ROOT,
                    input=os.fsencode(os.readlink(path)),
                    check=False,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.DEVNULL,
                )
                actual_object = (
                    result.stdout.decode("ascii").strip() if result.returncode == 0 else None
                )
            else:
                actual_object = output_of(
                    ["git", "hash-object", f"--path={relative_path}", "--", relative_path]
                ) if path.exists() else None
            if worktree_mode != head_mode or actual_object != head_object.decode("ascii"):
                working_tree_clean = False
                status_digest.update(b"worktree\0" + raw_path + b"\0")
        digest.update(b"\0")

    for raw_path in sorted(untracked):
        status_digest.update(b"untracked\0" + raw_path + b"\0")

    return {
        "commit": commit,
        "commit_tree": commit_tree,
        "tracked_snapshot_sha256": digest.hexdigest(),
        "working_tree_clean": working_tree_clean,
        "working_tree_status_sha256": status_digest.hexdigest(),
        "gitlinks": gitlinks,
        "gitlinks_verified": all(link["working_tree_clean"] for link in gitlinks),
    }


def matrix_sdk_versions() -> list[str]:
    lockfile = REPO_ROOT / "Cargo.lock"
    if not lockfile.is_file():
        return []
    versions: set[str] = set()
    for block in re.split(r"(?m)^\[\[package\]\]\s*$", lockfile.read_text()):
        if re.search(r'(?m)^name = "matrix-sdk"$', block):
            match = re.search(r'(?m)^version = "([^"]+)"$', block)
            if match:
                versions.add(match.group(1))
    return sorted(versions)


def connector_package_version() -> str | None:
    manifest = REPO_ROOT / "Cargo.toml"
    if not manifest.is_file():
        return None
    package = re.search(
        r"(?ms)^\[package\]\s*(.*?)(?=^\[|\Z)", manifest.read_text()
    )
    if package is None:
        return None
    match = re.search(r'(?m)^version = "([^"]+)"$', package.group(1))
    return match.group(1) if match else None


def parse_test_counts(log_path: Path) -> list[dict[str, int | str]]:
    current_target = "unknown"
    results: list[dict[str, int | str]] = []
    target_re = re.compile(r"Running tests/([^\s]+)")
    result_re = re.compile(
        r"test result: (?:ok|FAILED)\. (\d+) passed; (\d+) failed; "
        r"(\d+) ignored; (\d+) measured; (\d+) filtered out"
    )
    for line in log_path.read_text(errors="replace").splitlines():
        target_match = target_re.search(line)
        if target_match:
            current_target = target_match.group(1)
        result_match = result_re.search(line)
        if result_match:
            passed, failed, ignored, measured, filtered = map(
                int, result_match.groups()
            )
            results.append(
                {
                    "target": current_target,
                    "passed": passed,
                    "failed": failed,
                    "ignored": ignored,
                    "measured": measured,
                    "filtered_out": filtered,
                }
            )
    return results


def validated_output_root(path: Path) -> Path:
    root = path.expanduser().resolve()
    try:
        root.relative_to(REPO_ROOT)
    except ValueError:
        return root
    raise SystemExit("qualification output root must be outside the source tree")


def make_report_dir() -> Path:
    requested = os.environ.get("C2_QUALIFICATION_OUT_DIR")
    if requested:
        parent = validated_output_root(Path(requested))
        parent.mkdir(parents=True, exist_ok=True)
        return Path(tempfile.mkdtemp(prefix="matrix-opencode-c2-", dir=parent))

    temp_root = validated_output_root(
        Path(os.environ.get("TMPDIR") or tempfile.gettempdir())
    )
    temp_root.mkdir(parents=True, exist_ok=True)
    return Path(tempfile.mkdtemp(prefix="matrix-opencode-c2-", dir=temp_root))


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run the Matrix/OpenCode source-fixture qualification gate."
    )
    parser.parse_args()
    cargo = output_of(["cargo", "--version"])
    rustc = output_of(["rustc", "--version"])
    if cargo is None or rustc is None:
        print("qualification requires working cargo and rustc on PATH", file=sys.stderr)
        return 2

    target_dir = Path(
        os.environ.get("CARGO_TARGET_DIR", str(REPO_ROOT.parent / "target"))
    ).expanduser().resolve()
    report_dir = make_report_dir()
    log_path = report_dir / "cargo-test.log"
    report_path = report_dir / "qualification-report.json"

    source_before = tracked_source_snapshot()

    command = [
        "cargo",
        "test",
        "--locked",
        "--no-default-features",
        "--features",
        FEATURES,
    ]
    for target in TEST_TARGETS:
        command.extend(("--test", target))

    environment = os.environ.copy()
    environment["CARGO_BUILD_JOBS"] = "1"
    environment["CARGO_TARGET_DIR"] = str(target_dir)
    print(f"Report directory: {report_dir}", flush=True)
    print("Running the source-fixture gate; no live Matrix account or OpenCode model access.", flush=True)
    with log_path.open("wb") as log_file:
        result = subprocess.run(
            command,
            cwd=REPO_ROOT,
            env=environment,
            stdout=log_file,
            stderr=subprocess.STDOUT,
            check=False,
        )

    counts = parse_test_counts(log_path)
    expected_targets = {f"{name}.rs" for name in TEST_TARGETS}
    observed_targets = {str(item["target"]) for item in counts}
    passed = (
        result.returncode == 0
        and len(counts) == len(TEST_TARGETS)
        and observed_targets == expected_targets
        and all(int(item["failed"]) == 0 for item in counts)
        and all(int(item["passed"]) > 0 for item in counts)
    )
    source_after = tracked_source_snapshot()
    source_files_unchanged = source_before == source_after
    source_gitlinks_verified = bool(source_before["gitlinks_verified"]) and bool(
        source_after["gitlinks_verified"]
    )
    passed = passed and source_files_unchanged and source_gitlinks_verified
    report = {
        "schema": "matrix-opencode-c2-source-qualification/v1",
        "harness": {
            "version": HARNESS_VERSION,
            "sha256": file_sha256(SCRIPT_PATH),
        },
        "result": "pass" if passed else "fail",
        "qualification_kind": "source-fixture",
        "source": {
            "commit": source_before["commit"],
            "commit_tree": source_before["commit_tree"],
            "connector_package_version": connector_package_version(),
            "tracked_snapshot_sha256_before": source_before[
                "tracked_snapshot_sha256"
            ],
            "tracked_snapshot_sha256_after": source_after[
                "tracked_snapshot_sha256"
            ],
            "working_tree_clean_before": source_before["working_tree_clean"],
            "working_tree_clean_after": source_after["working_tree_clean"],
            "working_tree_status_sha256_before": source_before[
                "working_tree_status_sha256"
            ],
            "working_tree_status_sha256_after": source_after[
                "working_tree_status_sha256"
            ],
            "gitlinks_before": source_before["gitlinks"],
            "gitlinks_after": source_after["gitlinks"],
            "gitlinks_verified": source_gitlinks_verified,
            "snapshot_scope": (
                "all tracked files in this checkout plus gitlink identities; "
                "untracked and ignored file contents are not hashed"
            ),
            "unchanged_during_test": source_files_unchanged,
        },
        "toolchain": {
            "cargo": cargo,
            "rustc": rustc,
            "python": sys.version.split()[0],
            "matrix_sdk_versions_from_lockfile": matrix_sdk_versions(),
            "feature_bundle": "matrix-encryption (enables platform-sdks and Matrix SDK E2EE)",
            "cargo_build_jobs": 1,
        },
        "command": command,
        "test_targets": counts,
        "test_exit_code": result.returncode,
        "workspace": "fixture-managed; no native/user workspace was qualified",
        "sender_policy": "synthetic source fixtures only; no live sender or room policy was checked",
        "encryption": "feature/build coverage only; no live E2EE room, device, or key verification",
        "sanitized_ids": "no live Matrix IDs collected; keep the local test log private before sharing",
        "native_opencode": {
            "status": "not exercised as an installed loopback",
            "version": None,
        },
        "cargo_test_log": {
            "path": log_path.name,
            "sha256": file_sha256(log_path),
        },
        "recorded_at_utc": datetime.now(timezone.utc).isoformat(),
    }
    report_path.write_text(json.dumps(report, indent=2) + "\n")

    if not passed:
        print(f"Qualification failed; full Cargo output is in {log_path}.", file=sys.stderr)
    print(f"Qualification report: {report_path}")
    print(f"Test log SHA-256: {report['cargo_test_log']['sha256']}")
    return 0 if passed else result.returncode or 1


if __name__ == "__main__":
    raise SystemExit(main())
