# Local agent tools — first usable exchange

## Scope

Branch `feat/local-agent-tools` starts from GitHub main `847551894d3b0548bd2c5e33297ef717b73b611f`.
Implement an authorized roster and one supported harness integration for `list_agents` and `ask_agent`, using the existing owner-bound direct-request path. Demonstrate Builder asking Reviewer and incorporating the answer into Builder's original conversation.

No remote peer transport, durable queue, asynchronous continuation, new service, profiles/roles, nested delegation, or production activation. Busy/cold targets refuse; uncertain outcomes are not retried. A tool response is not verification of the requested business outcome.

## Implementation packets

1. Read-only roster from configured permissions and current owner bindings. Return stable ID, short name, and truthful availability; no paths, sessions, conversation content, or unauthorized identities.
2. One explicitly enabled, session-scoped harness tool integration. Reuse existing dependencies and MCP registration if suitable. Derive sender authority from the runtime, not tool arguments. Peer turns must not inherit retained human-root tool authority. Preserve disabled behavior and human commands.
3. Deterministic end-to-end regression through the actual harness/tool boundary, plus a live two-agent check with an already-authenticated supported harness and informational prompts. Include denied, stale, busy and literal slash-text cases.
4. Freeze candidate; parent-run checks; independent specification then quality/security review; focused fixes and renewed checks/review; commit/publish the branch and verify exact remote SHA. No main merge or deployment.

## Verification and evidence

Use strict vertical RED/GREEN TDD. Preserve failed and passing command evidence outside the checkout, with exact commands, source fingerprints and executable hashes in the delivery receipt.

Run focused `--locked --no-default-features --features agent-network` tests during development, then the self-contained affected feature/disabled matrices. Qualify other build variants as resources allow; do not count fixture or source evidence as live inference.

## Build ownership

Task-owned regenerable outputs: `/var/tmp/soulsteads-local-agent-tools-target` and `/var/tmp/soulsteads-local-agent-tools-tmp`. Free space checked on `/var/tmp` before starting (94 GiB available). Serialize builds/tests sharing these directories. Preserve source and unique evidence. After delivery and reviews, check active process/executable/cwd/open-file/mapped references, then remove these exact inactive task outputs and measure space reclaimed. Never delete shared caches or the existing Forge-host artifacts.
