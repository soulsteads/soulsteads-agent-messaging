# Matrix/OpenCode C2 qualification gate

This gate separates source-fixture evidence from installed loopback evidence and
user-facing verification. Passing the source gate is not evidence that a live
Matrix room, an installed OpenCode runtime, or a production workspace works.

## 1. Reproducible source-fixture gate

From the root of a clean, disposable checkout of the reviewed revision, run:

```sh
CARGO_TARGET_DIR="$(mktemp -d)" \
  python3 scripts/qualify_matrix_opencode.py
```

The CLI smoke test below uses a fake `cargo` executable and does not build Rust
code:

```sh
python3 -m unittest discover -s tests -p 'test_qualification_runner_cli.py'
```

The runner forces `CARGO_BUILD_JOBS=1` and runs these locked integration-test
targets with `--no-default-features --features matrix-encryption`:

- `matrix_restart`
- `matrix_approvals`
- `matrix_identity`
- `opencode_adapter`

`matrix-encryption` enables `platform-sdks` and the Matrix SDK E2EE features.
Each target must report at least one passing test and no failures. A target
with zero tests or only ignored tests does not satisfy the gate, even if Cargo
exits successfully. The report records each target's counts, the Git source
revision, and before/after observations of a trusted disposable checkout's
tracked-source snapshot. The snapshot binds the HEAD commit/tree and all tracked
files, plus dirty-state indicators and any submodule commit references and
checkout IDs. It excludes untracked and ignored file contents. Compare both
snapshots; a matching pair does not prove that the build was isolated or that
the files never changed in between. The report also records the toolchain and
Matrix SDK versions, the command, and the `cargo-test.log` SHA-256. These are
source and log digests, not an installed connector artifact digest.

This gate does not access a live Matrix account or OpenCode model, and it does
not qualify a native workspace, live sender policy, live room encryption, or
installed OpenCode artifact. Cargo may download uncached dependencies. The
report and `cargo-test.log` are written to a new directory under `TMPDIR` by
default. Set `C2_QUALIFICATION_OUT_DIR` to an approved directory outside the
source tree to retain them elsewhere.

The test modules are regression checks around the connector's restart,
permission/identity, and OpenCode adapter seams. They do not prove a completed
Matrix-to-native-OpenCode conversation or process restart. In particular,
`tests/opencode_adapter.rs` exercises the adapter/test executable; it does not
identify or hash an installed OpenCode distribution. Its
`installed_opencode_adapter_initializes_and_creates_session` test is explicitly
ignored because it uses installed OpenCode credentials and creates/deletes a
session. Do not unignore it with real credentials for this gate. The existing
`examples/qualify_matrix_resume.rs` is a separate opt-in path: it requires a
private qualification root and copied OpenCode database, and rejects enabled
memory hooks. This source runner does not invoke it. Do not point either runner
at an existing user's database, room, account, or workspace.

## 2. Installed, isolated loopback acceptance

Run this only in a separately provisioned, disposable loopback environment
with synthetic Matrix users/rooms and a disposable workspace. Do not use a
shared service, an existing room/account, real user data, production credentials,
a user model endpoint, or a downloaded model. Do not auto-start shared
infrastructure. If a headless installed OpenCode loopback is unavailable, leave
this gate `NOT RUN`; source fixtures are not a substitute.

Use a distinct evidence bundle for this gate. For each case, record the input,
expected result, observed result, and supporting sanitized event/session/log
references:

1. **Authorized reply and follow-up.** Send one harmless authorized request
   through the supported Matrix transport. Verify one substantive reply and
   record the inbound event ID and the actual reply relation/thread target.
   Send a second meaningful follow-up after the idle interval; verify its
   response is tied to the intended conversation, not merely that a process
   started or emitted an acknowledgement.
2. **Idle restart identity.** With no active turn, stop and restart only the
   isolated connector/OpenCode pair. Read back the native OpenCode session ID
   and exact synthetic workspace path before and after. Require both to match
   the expected values. Verify the follow-up reply's Matrix relation points to
   the intended inbound event/thread. A startup message or a path that merely
   looks similar is not proof.
3. **Denied sender and room.** Inject a message from a synthetic sender outside
   the allowlist and another from a denied room. For each, verify the dispatch
   counter and OpenCode request log remain at zero. Record the configured
   allowlist/room scope and the observed zero-dispatch evidence.
4. **Tool approval.** Request one harmless, bounded read-only operation inside
   the synthetic workspace and verify the expected approval/allow path. Then
   request a denied side-effecting operation and verify that it is rejected
   before invocation; confirm the fixture shows no side effect. Never test a
   destructive action against real data.
5. **Mid-turn cancellation.** Hold a deterministic turn open, request
   cancellation, and record the request separately from its outcome. Mark
   `stopped` only after observing a terminal stop/cancel acknowledgement or
   verified worker termination. If that evidence is missing or ambiguous, record
   `unknown`; a cancel request alone is not proof that work stopped.

Follow-up inputs held only in memory are not durable. Do not claim they were
replayed after restart, or that a workspace/session path persisted, unless the
specific value was read back and the corresponding event/session relationship
was verified. Record absent persistence as `not verified`, not as a pass.

## 3. Evidence record and separation

Keep the three evidence classes separate; a result in one class cannot promote
another:

- `source-fixture`: output of `scripts/qualify_matrix_opencode.py` only.
- `installed-loopback`: synthetic, isolated OpenCode/Matrix run with direct
  read-back evidence.
- `user-facing`: a separately authorized user-facing verification. It is not
  part of this source PR and must never be inferred from the first two classes.

The source-fixture JSON binds the HEAD commit/tree and a before/after digest of
all tracked files in a trusted disposable checkout, plus dirty-state indicators
and any submodule commit references and checkout IDs. It does not hash untracked
or ignored file contents. Use a report with both clean-state indicators set to
`true` for review of a committed revision. The JSON contains no absolute
report-output or Cargo target paths.

Treat the raw `cargo-test.log` as private: it can contain local paths or fixture
output. Keep it access-controlled and review it before sharing. The JSON records
only the relative log filename and its SHA-256.

For an installed-loopback record, include at least:

```text
qualification_kind: installed-loopback
connector_version_or_commit:
connector_source_sha256:
harness_name_version_sha256:
OpenCode_version:
OpenCode_artifact_sha256:
Matrix client/SDK version:
workspace_exact_synthetic_path:
sender_allowlist_and_room_scope:
encryption: enabled/disabled; observed device/key verification state
sanitized_sender_room_event_and_session_ids: stable pseudonyms; no credentials
authorized_reply_and_followup: event IDs and observed relation/thread targets
idle_restart: native session ID + exact workspace read-back, before/after
negative_cases: denied sender dispatch count; denied room dispatch count
allow_tool: requested action, approval, observed result
deny_tool: requested action, proof invocation did not occur
cancel: requested / stopped / unknown, with verification evidence
artifact_paths_and_sha256:
result: pass / fail / not verified
```

Keep the unredacted local record access-controlled. Sanitize identifiers
consistently before sharing while preserving the relationships between events,
senders, rooms, and native sessions. Never include access tokens, encryption
keys, private database contents, or real-user message bodies in a shared report.

## Out of scope

This is not an unattended B3 report gate, Discord qualification, production
setup, or permission to attach a real Matrix account/room. No installed
headless OpenCode loopback or user-facing run is claimed by this source PR.
