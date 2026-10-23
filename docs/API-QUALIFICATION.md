# API qualification boundaries

The execution owner remains `Bridge` and its conversation slots. Human connector
admission and owner-issued agent capabilities are different sources of authority;
an agent request must not be converted into a human event to obtain commands or
interactive approval permissions.

## Core and optional agent requests

The `agent-network` Cargo feature enables the bounded, in-process
`Bridge::direct_request` interface. Configuration must also enable
`agentMessaging` and explicitly register targets and allowed senders. A trusted
host obtains a `LocalClient` from an admitted owner's `Sink::local_client`
callback; request text cannot manufacture that capability. Targets must already
be attached to their existing owner. Busy, cold, revoked or unauthorized targets
are refused, not queued. Results carry a correlation ID and are not automatically
posted into a human conversation.

The direct-request core is an in-process API, not a remote networking endpoint.
The optional [local Codex tools](AGENT-TOOLS.md) integration adds a session-scoped,
authenticated loopback MCP endpoint for `list_agents` and `ask_agent`. It requires
an additional explicit runtime opt-in; ordinary direct API use starts no listener.
The roster exposes only permitted identities and current attached/unavailable
bindings, not task details or a promise of idle capacity. There is no durable
queue, replay or delayed reply. Durable delivery through `LocalClient::send` and
`reply` remains unavailable. Caller disconnection does not prove execution
stopped; requests with an uncertain outcome must not be blindly retried.

## Repeatable, credential-free checks

These tests use production ingress/execution code and deterministic protocol
fixtures. They do not contact provider accounts or establish live model inference.
Use an external `CARGO_TARGET_DIR` and temporary directory. Build variants serially
when they share an artifact directory; do not overwrite adapter binaries while a
test is executing them.

- `wire_api_replay`: Telegram polling and Mattermost/Slack/Discord socket ingress
  through `Platform::run`, the normal bridge, and actual outbound HTTP sink calls
  captured on loopback. A held terminal proves tool activity arrives before final
  output; only platforms whose sink supports typing are required to send it.
- `harness_api`: compiled messaging executable and real WebSocket API through
  compiled Claude, Codex, Pi and OpenCode ACP adapters to their respective native
  fixture protocols. Codex compaction additionally holds the native terminal to
  prevent an early success acknowledgement.
- `native_api_driver`: public WebSocket ingress through the existing OpenCode
  native REST/SSE transport and its owned fixture process. A terminal barrier
  proves native tool output arrives before completion. Native `/compact` is
  advertised as an adapter-provided control and acknowledges success only after
  durable completion records match the admitted compaction IDs. An unrelated
  completion must fail, not acknowledge success or cause automatic replay.
- `native_direct_requests` (feature enabled): trusted owner-bound requests through
  each compiled native adapter; slash-prefixed text remains data and no human
  presentation is emitted.
- `native_sdks`: retained Matrix SDK `/sync` ingress, SDK outbound sends and thread
  correlations. SDK encryption-fixture checks are not a live interoperability
  certificate. WhatsApp checks include SDK construction and Noise-handshake
  shutdown, not an authenticated application-message round trip.
- WhatsApp connector unit tests exercise the exact SDK-message normalization
  helper called by the registered callback. The locked WhatsApp SDK has no public
  callback dispatcher for injecting a decrypted message. Normalization evidence
  is deliberately not labelled callback-to-sink replay. Full replay would require
  upstream support or a separately reviewed authenticated protocol fixture; no SDK
  fork, live pairing or credential access is implied.
- `cli_api_driver`: actual compiled CLI stdin, isolated generic ACP fixture,
  command aliases and native-control RPC counts, including explicit unsupported
  picker outcomes. This is generic ACP coverage, not native-provider inference.
- Existing Web, approval, question, session, shutdown, cancellation and
  adapter collections remain part of the full self-contained suite. Direct
  `Event`/`Sink` tests establish their own execution contracts, not wire-parser
  coverage.

For example:

```sh
cargo test --locked --features agent-network --test wire_api_replay --test harness_api
cargo test --locked --features agent-network --test native_api_driver --test native_direct_requests
cargo test --locked --features agent-network --test native_sdks
cargo test --locked --features agent-network
cargo test --locked
cargo test --locked --no-default-features
cargo test --locked --no-default-features --features platform-sdks
```

The optional `reference-conformance` suite requires a separately supplied
historical reference and Bun. Ignored live-provider tests remain unqualified
unless independently run with explicit authorization. A passing Linux fixture
suite is not macOS/Windows, live-account, installed-service or cross-machine
qualification. Source delivery does not authorize activation.
