# Changelog

## 0.4.0 (proposed)

Changes since [`v0.2.0`](https://github.com/soulsteads/soulsteads-agent-messaging/releases/tag/v0.2.0), peeled to [`baa57df`](https://github.com/soulsteads/soulsteads-agent-messaging/commit/baa57df107b3d4062f6e78817b7aa51a9a3ace77).

### Highlights

- Add `service install --user|--system [--run-as USER] [--dry-run]` for ordinary,
  manually controlled Linux systemd registration. It previews safely, uses an
  explicit gateway configuration and working directory, writes a fixed unit
  target atomically without clobbering, and never enables or starts a service.
- Require a named non-root identity for system units. Generated units have no
  credentials or `EnvironmentFile`; credentials remain in the gateway working
  directory and are read only when the gateway itself starts.
- Queue durable review requests, recover Mattermost threads after ACP disconnects,
  preserve Matrix tool approvals during active turns, and fix concurrent connector TLS.
- Add Docker and Podman deployment guidance plus release-asset and package
  qualification in CI.
- Add read-only local settings inspection and validation, including Telegram
  Skillstore receipt-directory validation.

### Included public commits

All 15 commits already on the public default branch in this release range are
listed below. Unmerged branches are not included.

- [`27657bd`](https://github.com/soulsteads/soulsteads-agent-messaging/commit/27657bd2f59a3b943fd7b9fcb6860a82ec6946d0) Harden connector access and native ACP messaging
- [`e9b2ccf`](https://github.com/soulsteads/soulsteads-agent-messaging/commit/e9b2ccfd13f6fc3ec7b71e8b66f13cb8de31ca26) chore(release): prepare v0.3.0
- [`ad3267c`](https://github.com/soulsteads/soulsteads-agent-messaging/commit/ad3267ce020054bb768f6f58b3cc885c091844a5) fix(matrix): keep tool approvals reachable during active turns
- [`4fbbbad`](https://github.com/soulsteads/soulsteads-agent-messaging/commit/4fbbbad1cdd5ec487018800b2fc692f34d9e9e48) test: use synthetic identifiers in public fixtures
- [`5581762`](https://github.com/soulsteads/soulsteads-agent-messaging/commit/55817627d30e2eb4509cca18e47d9f7d8bb44cd6) feat(mattermost): make threaded conversations recoverable
- [`bb44479`](https://github.com/soulsteads/soulsteads-agent-messaging/commit/bb44479c216e4f55966e39d381fcd1552355d5b4) fix(mattermost): reopen saved sessions after ACP disconnects
- [`0fa9d31`](https://github.com/soulsteads/soulsteads-agent-messaging/commit/0fa9d31ef4868a68536d4aa2f49408c26923259c) feat: review Skillstore proposals in Telegram
- [`857529f`](https://github.com/soulsteads/soulsteads-agent-messaging/commit/857529f84dba4fe2f8f83119bb2163858aff9b40) feat: add Docker and Podman deployment
- [`25a6393`](https://github.com/soulsteads/soulsteads-agent-messaging/commit/25a6393cf1e116f9223416e06ab30bba3cdea0a3) feat: queue durable review requests between agents
- [`8b04f14`](https://github.com/soulsteads/soulsteads-agent-messaging/commit/8b04f14feecc8f099d85146b7767d1ec17d46374) [verified] Fix TLS for concurrent messaging connectors
- [`6a5f3c0`](https://github.com/soulsteads/soulsteads-agent-messaging/commit/6a5f3c0e2b9199e63cb4e81dbcab83df4638c3c6) ci: build licensed Agent Messaging release assets
- [`a657c43`](https://github.com/soulsteads/soulsteads-agent-messaging/commit/a657c43c36037a8c49c5ecf3cef65effc42346fc) fix(ci): validate the complete release asset handoff
- [`85cdbd1`](https://github.com/soulsteads/soulsteads-agent-messaging/commit/85cdbd1fb9f119c965cfb1980b99045c6f56de8d) fix(ci): qualify packaging when release inputs change
- [`8ef19df`](https://github.com/soulsteads/soulsteads-agent-messaging/commit/8ef19df2ad57e8cc0271d751beb3367f48f5f3f2) feat(cli): inspect and validate local messaging settings
- [`02ef08a`](https://github.com/soulsteads/soulsteads-agent-messaging/commit/02ef08ac00f76f7a7822cb814cc3060e87df5d73) fix(settings): validate Telegram Skillstore receipt directory
