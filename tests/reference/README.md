# TypeScript conformance reference

`opencode-chat-bridge` is an external test reference, not included in this
repository. The comparison baseline is commit
`506fbb3bbe72d4e710c15373bea3f73f10c34b2c`. Supply an authorized copy at
`tests/reference/opencode-chat-bridge`, preserving its upstream MIT license.
These tests execute its real store, commands,
connector callbacks and output logic; they do not reproduce the implementation
in mocks.

The default public test suite is self-contained:

```sh
cargo test --locked
```

Tests that execute the external TypeScript reference are explicitly opt-in via
`reference-conformance`, which is not a default feature. Independent Rust tests
in mixed test files still run by default, including clear/restart recovery and
Telegram Unicode splitting.

After supplying the reference, install Bun and run:

```sh
scripts/bootstrap-test-reference.sh
cargo test --locked --features reference-conformance
```

The helper installs the supplied checkout's frozen Bun dependencies without
lifecycle scripts; it does not fetch source. Enabling the feature without the
reference or Bun fails rather than silently skipping comparisons. Consequently,
`--all-features` also requires the reference and Bun. The reference has no role
in the Rust application's runtime or release artifacts.

`typescript-manifest.json` records SHA-256 digests of every source/connector file,
the package manifest, lockfile and license. Tests verify those bytes before
executing the reference, including in source archives without Git metadata.
Update the documented baseline and this manifest together when deliberately changing
the comparison baseline.
