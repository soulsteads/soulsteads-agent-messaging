# A2A SDK qualification (milestone 0)

**This is SDK-only qualification, not production A2A runtime support.** No production client/server, lifecycle, identity, authorization policy, persistence, TLS configuration, or service integration is implemented. `a2a` is opt-in and independent of `agent-network`; it is not enabled by default.

## SDKs pinned

- Protocol types: `a2a-lf = 0.4.2` (Rust library name `a2a`; A2A protocol v1).
- Client: `a2a-client-lf = 0.2.9` (library `a2a_client`).
- Server: `a2a-server-lf = 0.5.3` (library `a2a_server`).
- Upstream: [a2aproject/a2a-rs](https://github.com/a2aproject/a2a-rs); dependencies are exact published crates, not moving Git refs.
- The published SDK crates declare Rust 1.85 as their minimum; qualification used Rust 1.98.1.

The test exercises published APIs: client `A2AClientFactory::builder().build().create_from_card`, then `send_message`, `send_streaming_message`, `get_task`, and `cancel_task`; server `agent_card::agent_card_router`, `jsonrpc::jsonrpc_router`, `DefaultRequestHandler`, and `AgentExecutor`. The JSON-RPC v1 wire method constants are `SendMessage`, `SendStreamingMessage`, `GetTask`, and `CancelTask`. A deterministic executor supplies status/artifact events only; no external service or provider is contacted. The Agent Card is generated with the actual ephemeral listener address, fetched from `/.well-known/agent-card.json`, and passed unchanged to SDK discovery. Test server tasks are aborted on fixture drop and each network wait is bounded.

## Qualification coverage and limits

`tests/a2a_sdk_smoke.rs` has three feature-gated loopback tests covering published-card discovery plus SendMessage/text result/GetTask; a held oneshot terminal barrier with event sequence Working → artifact → Completed and GetTask while Working; and `CancelTask` of an active task, expecting SDK state `Canceled`. A strict-sequence regression verifies that the drain rejects a duplicate Working event; the drain requires exactly ArtifactUpdate then Completed and EOF, with bounded waits.

The server SDK exposes a `TaskStore` with `create`, `update`, `get`, `list`, and `begin_cancel` operations. Its `RequestAuthorizer::authorize(ServiceParams, Option<task_id>)` hook can be installed with `DefaultRequestHandler::with_authorizer`. The default handler has no authorization policy unless one is installed. The store API does not automatically enforce per-peer ownership for reads, lists, or contexts; a production adapter must provide those rules. Stock handler construction supplies `ExecutorContext.user = None`; this is what the handler does, not an intrinsic limit of the context type. Request auth headers remain in service parameters.

The SDK `DefaultRequestHandler::cancel_task` (published source 0.5.3, `src/handler.rs:973–1004`) persists its `TaskStore::begin_cancel` transition before invoking `AgentExecutor::cancel`. Thus the observed canceled task state does **not** establish that a backend/tool stopped. The active-task test uses an empty cancellation callback and observes protocol state only. Production cancellation and authorization semantics require an explicit adapter/handler policy decision; this milestone does not qualify them. Stream disconnect is not cancellation: the server SDK documents preserving execution for resubscription, but this milestone does not directly test disconnect/resubscribe.

## TLS, dependencies, and operational notes

The SDK client and server dependencies disable their default TLS features here. In the `--no-default-features --features a2a` qualification graph, reqwest 0.13 is HTTP-only, with no TLS features selected; the project retains its existing reqwest 0.12 declaration/features unchanged. Other feature combinations can unify additional transitive features. No existing package versions were removed or upgraded; the lockfile adds 33 packages, including platform-specific `protoc` packages. The published build sources show vendored protoc, so a system `protoc` executable is not required. The SDK graph also includes mandatory `a2a-pb 0.3.2` and its `tonic`/`prost` dependencies despite this qualification using no gRPC endpoint. The server and project both use Axum 0.8; Tokio is 1.x. No native TLS or Rustls crypto provider is selected for A2A, avoiding provider-feature conflicts and unnecessary TLS/native build dependencies. Production HTTPS configuration is unqualified.

## Commands and results

Run from the repository root:

```sh
CARGO_TARGET_DIR=/var/tmp/soulsteads-a2a-build-20261024-sdk/target \
CARGO_BUILD_JOBS=2 CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 \
CARGO_PROFILE_TEST_DEBUG=0 cargo test --locked --no-default-features --features a2a --test a2a_sdk_smoke
cargo fmt --all -- --check
CARGO_TARGET_DIR=/var/tmp/soulsteads-a2a-build-20261024-sdk/target \
CARGO_BUILD_JOBS=2 CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 \
CARGO_PROFILE_TEST_DEBUG=0 cargo test --locked --no-default-features --test a2a_sdk_smoke
```

Executed with the selected output directory and build limits above: `cargo test --locked --no-default-features --features a2a --test a2a_sdk_smoke`: **4 passed, 0 failed** (three loopback tests and one strict-sequence regression); feature-off `cargo test --locked --no-default-features --test a2a_sdk_smoke`: **0 tests, 0 failed** (expected cfg exclusion, not behavior coverage); `rustfmt --edition 2024 --check tests/a2a_sdk_smoke.rs`: passed. `cargo fmt --all -- --check` is **not clean**: it reported formatting diffs in many existing unrelated test files; no unrelated files were formatted or edited by this task. `cargo tree --locked --no-default-features --features a2a -e features -i reqwest@0.13.5` confirmed that the 0.13 dependency is reached through the optional SDK client/server and has no TLS feature selected. Initial compilation exposed published API mismatches; they were corrected before the passing locked run. These focused results do not establish live-provider, cross-language, disconnect/resubscribe, or deployment qualification. Broader feature-matrix results belong in the exact-candidate delivery receipt.
