"""Exercise the release workflow's publisher without contacting GitHub."""

import hashlib
import os
from pathlib import Path
import re
import subprocess
import tempfile
import textwrap
import unittest


ARCHIVE = "soulsteads-agent-messaging-linux-x86_64.tar.gz"
MANIFEST = "SHA256SUMS"
WORKFLOW = Path(__file__).resolve().parents[1] / ".github/workflows/release.yml"


def publisher_script():
    # The publisher is an inline YAML literal block. Run that block, not a copy.
    step = WORKFLOW.read_text(encoding="utf-8").split(
        "      - name: Verify and upload assets without replacing existing files\n", 1
    )[1]
    block = re.search(r"^        run: \|\n((?: {10}[^\n]*\n|\n)+)", step, re.MULTILINE)
    if block is None:
        raise AssertionError("Couldn't find the publisher's inline run block")
    return textwrap.dedent(block[1])


@unittest.skipUnless(WORKFLOW.is_file(), "requires the source checkout's release workflow")
class ReleaseTriggerTests(unittest.TestCase):
    def test_pull_request_paths_cover_release_inputs(self):
        workflow = WORKFLOW.read_text(encoding="utf-8")
        paths = re.search(r"^  pull_request:\n    paths:\n((?:      - [^\n]+\n)+)",
                          workflow, re.MULTILINE)
        self.assertIsNotNone(paths, "missing release packaging pull request path filters")
        patterns = [line.removeprefix("      - ").strip("'\"") for line in paths[1].splitlines()]
        self.assertCountEqual(patterns, (
            ".github/workflows/release.yml", ".cargo/**", "Cargo.lock", "Cargo.toml", "build.rs",
            "src/**", "assets/**", "LICENSE", "NOTICE", "scripts/release-notices/**",
            "tests/test_release_assets.py",
        ))


@unittest.skipUnless(WORKFLOW.is_file(), "requires the source checkout's release workflow")
class ReleaseAssetTests(unittest.TestCase):
    def setUp(self):
        root = Path(self.enterContext(tempfile.TemporaryDirectory()))
        self.dist = root / "dist"
        self.dist.mkdir()
        self.archive = self.dist / ARCHIVE
        self.manifest = self.dist / MANIFEST
        self.archive.write_bytes(b"synthetic archive contents")
        self.entry = self.checksum_entry(self.archive)
        self.manifest.write_bytes(self.entry)

        self.uploads = root / "uploads"
        self.uploads.touch()
        stubs = root / "bin"
        stubs.mkdir()
        self.env = {
            "PATH": f"{stubs}{os.pathsep}{os.defpath}",
            "HOME": str(root),
            "LC_ALL": "C",
            "RELEASE_ID": "1",
            "RELEASE_TAG": "fixture",
            "GITHUB_REPOSITORY": "example/messaging",
            "GITHUB_SHA": "a" * 40,
            "UPLOAD_LOG": str(self.uploads),
        }
        self.stub(stubs / "gh", """
            case "$*" in
              'api repos/example/messaging/releases/1')
                printf '%s\\n' '{"tag_name":"fixture","draft":false}' ;;
              'api --paginate --slurp repos/example/messaging/releases/1/assets?per_page=100')
                printf '%s\\n' '[[]]' ;;
              *) exit 2 ;;
            esac
        """)
        self.stub(stubs / "git", """
            [[ "$1" == ls-remote ]]
            printf '%s\\trefs/tags/%s\\n' "$GITHUB_SHA" "$RELEASE_TAG"
        """)
        self.stub(stubs / "curl", """
            name=${!#}
            name=${name##*?name=}
            name=${name%%&*}
            printf '%s\\n' "$name" >> "$UPLOAD_LOG"
            digest=$(sha256sum -- "$name" | cut -d ' ' -f 1)
            jq -n --arg name "$name" --arg digest "sha256:$digest" \\
                '{name:$name,digest:$digest,state:"uploaded"}'
        """)

    @staticmethod
    def checksum_entry(path):
        return f"{hashlib.sha256(path.read_bytes()).hexdigest()}  {path.name}\n".encode()

    @staticmethod
    def stub(path, script):
        path.write_text("#!/bin/bash\nset -euo pipefail\n" + textwrap.dedent(script))
        path.chmod(0o700)

    def publish(self):
        result = subprocess.run(
            ["bash", "--noprofile", "--norc", "-e", "-o", "pipefail", "-c", publisher_script()],
            cwd=self.dist,
            env=self.env,
            capture_output=True,
            text=True,
            timeout=10,
        )
        return result, self.uploads.read_text().splitlines()

    def assert_no_uploads(self):
        result, uploads = self.publish()
        self.assertEqual(uploads, [], result.stderr)
        self.assertNotEqual(result.returncode, 0)

    def test_uploads_only_the_archive_and_checksum_manifest(self):
        result, uploads = self.publish()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertCountEqual(uploads, [ARCHIVE, MANIFEST])

    def test_unlisted_file_prevents_every_upload(self):
        (self.dist / "unexpected.txt").write_bytes(b"not a release asset")
        self.assert_no_uploads()

    def test_hidden_file_prevents_every_upload(self):
        (self.dist / ".unexpected").touch()
        self.assert_no_uploads()

    def test_missing_archive_prevents_every_upload(self):
        self.archive.unlink()
        self.assert_no_uploads()

    def test_symlink_manifest_prevents_every_upload(self):
        original = self.manifest.rename(self.dist.parent / MANIFEST)
        self.manifest.symlink_to(original)
        self.assert_no_uploads()

    def test_duplicate_manifest_entry_prevents_every_upload(self):
        self.manifest.write_bytes(self.entry * 2)
        self.assert_no_uploads()

    def test_manifest_must_name_the_archive(self):
        self.manifest.write_bytes(self.entry.replace(ARCHIVE.encode(), b"./" + ARCHIVE.encode()))
        self.assert_no_uploads()

    def test_checksum_mismatch_prevents_every_upload(self):
        self.archive.write_bytes(b"different contents")
        self.assert_no_uploads()


if __name__ == "__main__":
    unittest.main()
