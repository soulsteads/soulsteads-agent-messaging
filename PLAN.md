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

---

# A2A integration plan

## Goal and boundaries

Add optional inbound and outbound Agent2Agent support using the official
[a2aproject Rust SDK](https://github.com/a2aproject/a2a-rs). Keep Bridge as the
single owner of backend sessions and execution. Do not introduce an agent loop,
central registry, message broker, external database server, or mandatory service.

A2A is separate from ACP: A2A connects agents; ACP remains a supported way for
Messaging to control a selected harness. Native backend transport selection must
not change merely because A2A is enabled.

## Checkpoint 0: SDK qualification

**Status: complete (SDK qualification); production A2A integration remains planned.**

Scope:
- Optional `a2a` Cargo feature, independent of `agent-network` and off by default.
- Exact published SDK versions, a committed lockfile, and explicit TLS choices.
- Deterministic loopback tests using real SDK client/server code: Agent Card,
  text exchange, task reads, SSE ordering, and active cancellation. Stream loss
  and disconnect/resubscribe are documented limits, not directly tested.
- Documentation of observed behavior and gaps; no production listener, new
  runtime configuration, backend session integration, or agent tool exposure.

See `docs/A2A.md` for qualification commands and evidence once verified. The
feature name is not a claim that Messaging already exposes an A2A endpoint.

## Checkpoint 1a: inert configuration and private policy

**Status: implemented; release qualification tracked in the exact-candidate delivery receipt.**

Source implementation: strict inert configuration and private exact allowlist policy. Final review and broader feature-matrix validation are not claimed here.

The staged configuration and private exact allowlist are not a production
admission mechanism. This packet accepts only strict inert configuration and
has no listener, authentication, execution, or task-storage path. Enabled A2A is
available for `--check-config` in builds with `a2a`; ordinary startup refuses it
until a separately reviewed endpoint exists. The policy does not authenticate
callers or confer execution authority. See `docs/A2A.md` for the exact example,
limits, vertical RED/GREEN evidence, and commands.

- [x] 0: official SDK-only qualification.
- [x] 1a: strict inert configuration and private exact allowlist policy.
- [ ] Final review and broader feature-matrix validation of the 1a candidate.
- [ ] Subsequent authentication, session, persistence, server, and outbound
  packets remain planned and require separate design/review.

## Next packets, in dependency order

Checkpoint 1a does not freeze a full A2A schema or authorize runtime support.
Continue the remaining plan only after candidate review and validation. Do not
freeze a public listener design or durable storage schema until those gates pass.
The full configuration and external-authority contract remains planned. Checkpoint
1a covers only the strict inert subset described above; future packets still
need to define named outbound peers, per-agent grants, limits, and admission
behavior before any runtime integration.

### 1. Configuration and external authority

Define a strict versioned configuration contract with named exposed agents,
peer credential-verifier references, inbound allowlists, named outbound peers,
per-agent grants, and explicit limits. Reject unsafe exposure and invalid or
unsupported settings. Configuration checks must not start listeners, create
sessions, modify task storage, or disclose secrets.

Authenticate before admission. Derive peer identity from private credentials,
not request text, metadata, claimed agent names, or context IDs. Separate remote
peer authority from the existing human-issued local caller capability.

### 2. Bridge-owned dedicated peer sessions

Introduce a sealed external admission path. Do not manufacture a human Event or
LocalClient to call the existing local direct-request API. Bind sessions to the
exact authenticated peer, configured target agent, and context identity; never
attach peer requests to human chat history.

Reuse Bridge's exclusive execution and reservation primitives where appropriate.
The peer cannot choose a native session, executable, cwd, permission mode, or
approval route. Peer text remains data, including slash commands. Initial peer
turns cannot chain another peer call or acquire human approval/form privileges.
Native tools retain their independently configured permissions.

### 3. Durable protocol task identity and recovery

Choose the smallest suitable local store after qualifying the SDK contract.
Persist ownership and duplicate identity before backend submission. Scope task
read/list/subscribe/cancel and continuation to the authenticated peer and target.
Keep one task-state writer; Forge workflow records remain a separate concern.

Suppress repeated exact submissions and reject changed-content identity reuse.
Do not claim exactly-once execution across a crash or automatically resubmit an
unknown backend outcome. Preserve completed reads and authorized context
continuity after restart; bound retention without recreating expired tasks.

A new storage dependency or durable schema needs an explicit design review before
implementation. An in-memory SDK fixture is not the production persistence layer.

### 4. Optional authenticated server

Integrate SDK routes with the existing process startup/shutdown lifecycle. Support
A2A-only operation without a chat connector. Start with text, JSON-RPC over HTTP,
and SSE. Cards must advertise only qualified capabilities and approved metadata.

Require task-method authentication, default to loopback, and require explicit
private/encrypted transport policy for remote exposure. Apply body/output/task
limits and bounded stream buffering. Connection loss must not imply cancellation
or cause replay. No attachments, URL fetching, push callbacks, gRPC, or SLIMRPC
in the initial release.

### 5. Bounded outbound calls

Use the official client against configured named peers. Agent tools cannot choose
arbitrary URLs or credentials. Validate both configured destinations and
card-advertised endpoints; prevent redirects or DNS/endpoint changes from leaking
credentials. Explicitly approved LAN peers must remain possible.

Register model-facing tools only for a qualified harness and admitted owner turn.
Keep remote peer operations distinct from local `list_agents`/`ask_agent` behavior.
Return bounded untrusted tool data and task references, with no blind submission
retry after uncertainty and no remote-turn chaining.

### 6. Independent interoperability and delivery

Test both directions with a pinned independent official SDK, not only the Rust
client against the Rust server. Cover credentials, isolation, duplicates, crash
windows, slow/lost streams, cancellation, limits, and existing connector behavior.
Obtain independent specification and quality/security reviews of the frozen
candidate before publishing an opt-in release.

Source delivery does not authorize deployment, credential creation, network
exposure, firewall changes, or service restarts. Qualify an isolated useful live
exchange separately before claiming live support.

## Critical qualification gates

- Stock SDK authorization is not Soulsteads authorization. Reads, listings,
  subscriptions, continuations, and cancellation need peer/target ownership.
- Stock SDK cancellation state is not proof of backend or subprocess termination.
  Qualify the handler ordering and use a policy that reports outcomes truthfully.
- Do not advertise token streaming merely because status/final events use SSE.
- SDK-only tests do not qualify any harness, platform account, deployment, or
  cross-machine path.
- Preserve the existing optional local-agent API and native harness permissions.
- Keep Agent Home configuration generation and Forge scheduled use as separate
  follow-on packets after standalone Messaging works; Model Router is unchanged.

## Verification and build hygiene

Use an explicit task-owned external `CARGO_TARGET_DIR`, check free space on its
filesystem, and serialize builds sharing that output. Keep unique logs/review
receipts outside disposable build output. Run feature-off, `a2a`, and combined
`a2a,agent-network` checks; test default platform features before release.

Remove only inactive regenerable task-owned outputs after final verification and
fresh process/open-file/reference checks. Preserve source, credentials, unique
evidence, deployed binaries, and rollback artifacts.

## Focused follow-up: peer request context

For model-facing direct requests, add a bounded JSON peer envelope at the existing
execution boundary. Sender and recipient IDs must come only from trusted
`DirectAdmission`; preserve the existing source class, authorization and interaction
isolation. Identify the recipient as answering for the caller and explicitly mark
request text as peer data, not operator/system authority. Keep content structurally
escaped, accept the existing maximum request payload within backend prompt budget,
and preserve human prompt bytes, reply routing, session ownership, busy/stale and
uncertain behavior. Do not add roles, services, permissions or configuration.

Regress actual backend prompt capture with spoofed identity/delimiter text, slash and
Unicode, maximum-size control-heavy text, and unchanged human prompts. Document the
public behavior and limits. Fixture qualification is not live model or production
acceptance. No private incident evidence belongs in the public tree.
