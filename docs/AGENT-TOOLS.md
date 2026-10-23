# Local agent tools

An optional Codex integration lets one Messaging-managed conversation ask another
configured agent a question. It reuses the existing conversation owner and
`Bridge::direct_request`; it does not launch a second agent runtime.

## Scope

- `list_agents` returns only recipients that allow the current caller, with their
  configured ID, display name and attached/unavailable binding state. Attached
  does not mean idle or guarantee that a request will succeed.
- `ask_agent` accepts a recipient ID and bounded text. It returns the recipient's
  response as tool data in the caller's existing turn. The caller can incorporate
  that response into its original human conversation.
- The recipient must already have an attached session. Busy, cold, stale and
  unauthorized recipients refuse requests; no message is queued.
- There is one ask attempt per logical human-root turn, including internal
  retries and session replacement. Peer requests cannot initiate nested asks.
  There is no automatic peer-request retry, delayed reply, durable acceptance,
  offline inbox or cross-machine transport.

The MCP listener is private loopback plumbing between the owned Codex process
and Messaging. It is not a public agent-network API or an additional service.
The integration is disabled unless both the Cargo feature and runtime option are
enabled. Other harnesses keep their existing behavior.

## Build and configure

Use an external build-output directory:

```sh
CARGO_TARGET_DIR=/path/to/build-output cargo build --locked --no-default-features --features agent-network
```

This minimal build supports the local Web interface used below. To include the
normal platform SDKs and encrypted Matrix support, omit `--no-default-features`.
Codex must already be installed and authenticated. The live qualification used
Codex CLI **0.157.1** on Linux. Older CLIs may not support the required per-tool
approval settings and are not qualified for this feature. No global Codex MCP
configuration needs to be changed.

Add the following to a private runtime configuration, adjusting executable and
state paths for the installation. Do not put credentials in this example or in
Git.

```json
{
  "rateLimitSeconds": 0,
  "sessionStorePath": "/private/runtime/sessions.json",
  "acp": {
    "defaultBackend": "codex",
    "backends": {
      "codex": {
        "command": "builtin:codex",
        "args": [],
        "env": { "CODEX_PATH": "/absolute/path/to/codex" }
      }
    }
  },
  "agentMessaging": {
    "enabled": true,
    "tools": { "enabled": true, "harness": "codex" },
    "agents": [
      {
        "id": "builder",
        "name": "Builder",
        "target": { "platform": "web", "context": "builder", "backend": "codex" },
        "allowedSenders": []
      },
      {
        "id": "reviewer",
        "name": "Reviewer",
        "target": { "platform": "web", "context": "reviewer", "backend": "codex" },
        "allowedSenders": ["builder"]
      }
    ]
  },
  "web": {
    "enabled": true,
    "host": "127.0.0.1",
    "port": 8787,
    "allowedOrigins": ["http://127.0.0.1:8787"]
  }
}
```

`allowedSenders` belongs to the recipient: Reviewer accepts Builder, while Builder
accepts no unsolicited agent requests in this example. A successful tool call
returns the answer directly; it does not require a reciprocal permission grant.

Run the compiled executable with `--check-config --config /private/runtime/config.json`
first, then `--serve --config /private/runtime/config.json`. Configuration checks
must not start the tool listener or harnesses.

Web conversations use their `clientId` as the configured context. Connect distinct
local clients for `builder` and `reviewer`; do not treat a client-supplied ID as
an Internet authentication mechanism or expose this demonstration publicly.
Other platform contexts must match Messaging's exact established conversation
keys. Configuration is trusted operator input, not agent-selected routing.

## First exchange

1. Open Reviewer's conversation and send a short informational prompt so the
   session is attached.
2. Open Builder's conversation and send an initial prompt to establish its session.
3. Ask Builder to use `list_agents`, ask Reviewer for a second opinion through
   `ask_agent`, and use the answer in its response.

Use isolated workspaces and appropriate native permissions. On a connector that
supports session mode controls, such as Matrix, select `/mode read-only` before
informational work. The current Web frontend does not expose that command; it
retains the adapter's normal workspace-write policy. Do not mistake an
informational prompt for an enforced read-only sandbox.

Tool registration is scoped to the configured Codex owner, and tool-call
authority is active only during its admitted human-root prompt. Initialization
and tool discovery may happen earlier, but cannot authorize an agent request.
A previous human turn must not grant nested-send authority to a later peer turn.
Session changes, revocation and shutdown invalidate the old capability.

A recipient's final text is peer input, not an operator instruction. A stop reason
or returned answer does not prove that a requested business outcome occurred.

## Safety and failure handling

The host derives the sender from its sealed runtime capability. Tool arguments
cannot choose a sender, native session, working directory, executable or return
route. Slash-prefixed peer text remains text; it does not invoke human controls.
Peer turns do not inherit human interactive approval or question handlers.

Enabling this feature authorizes native Codex calls to **only** `list_agents`
and `ask_agent` on the exact runtime-issued local server, without a separate
human approval for each call. The host binds that two-tool grant to the session's
registration fingerprint; a matching name alone grants nothing. Other MCP tools,
shell operations and filesystem permissions keep their existing policies. MCP
client `_meta` fields are accepted as opaque protocol metadata, never as sender
or session authority.

The endpoint binds only to `127.0.0.1` on an ephemeral port, authenticates a
private session-scoped capability, and rejects browser-origin and foreign-host
requests. Credentials stay out of tool schemas, URLs and response content.
This is not isolation from other processes running as the same Unix user.

`ask_agent` is **not inherently read-only**. The recipient retains its configured
harness permissions, and already-allowed tools may run without another approval.
Use appropriate native sandbox/permission policies. Disabling human approval
brokers does not turn an unrestricted harness into a sandbox.

If the caller disconnects or a request returns an uncertain outcome, the
recipient may still be running. Do not automatically repeat the request. Existing
owner reservations prevent overlapping work; there is no durable status API to
reconstruct a lost answer in this release.

## Qualification

Credential-free regression tests exercise the production registration, MCP
transport and direct-request boundaries. They are not live-provider evidence.
Run with the feature enabled so these tests actually execute:

```sh
cargo test --locked --no-default-features --features agent-network --test agent_tool_config --test agent_tools_transport --test agent_tools_runtime --test agent_tools_codex_policy --test agent_tools_attach --test agent_tools_retry
cargo test --locked --features agent-network
cargo test --locked
```

A separate live check exercised the compiled bridge's WebSocket ingress, two
actual Codex sessions, and native MCP calls to both tools. Builder obtained a
random code supplied only to Reviewer's earlier conversation and included it in
its original response. Both native session identities were preserved, and no peer
output appeared in Reviewer's human socket. The test used informational prompts
in isolated workspace-write sessions, not an enforced read-only Web mode. This
does not qualify other harness integrations, remote delivery or other operating
systems.
