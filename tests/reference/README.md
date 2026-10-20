# TypeScript conformance reference

`opencode-chat-bridge` is an external test reference, not included in this
repository. The comparison baseline is commit
`506fbb3bbe72d4e710c15373bea3f73f10c34b2c`. Supply an authorized copy at
`tests/reference/opencode-chat-bridge`, preserving its upstream MIT license.
These tests execute its real store, commands,
connector callbacks and output logic; they do not reproduce the implementation
in mocks.

After supplying the reference, run `scripts/bootstrap-test-reference.sh` before
the full suite. It installs the supplied checkout's frozen Bun dependencies
without lifecycle scripts; it does not fetch source. Bun is required. Without
the reference, source-conformance tests cannot run; they are not silently skipped. The
reference has no role in the Rust application's runtime or release artifacts.

`typescript-manifest.json` records SHA-256 digests of every source/connector file,
the package manifest, lockfile and license. Tests verify those bytes before
executing the reference, including in source archives without Git metadata.
Update the documented baseline and this manifest together when deliberately changing
the comparison baseline.
