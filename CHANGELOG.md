# Changelog

## 0.3.0 (proposed)

Changes since [`v0.2.0`](https://github.com/soulsteads/soulsteads-agent-messaging/releases/tag/v0.2.0).

### Highlights

- **Upgrade:** configure a nonempty `allowedUsers` list before enabling remote connectors; sender admission is explicit.
- Normalize supported Codex approval events, add opt-in native Soulsteads ACP support, and queue Matrix text follow-ups until the active turn ends.
- Refresh setup and upgrade guidance, remove maintainer planning from the source tree, and update the offline ACP fixture for current Cargo layouts.
- Standardize `LICENSE` and `NOTICE` with author Scott Glover and `Copyright (c) 2026 Soulsteads`. The Apache-2.0 terms are unchanged.

### Included commits

All commits already on the public default branch in this release range, before this version and attribution update. Unmerged branches are not included.

- [`27657bd`](https://github.com/soulsteads/soulsteads-agent-messaging/commit/27657bd2f59a3b943fd7b9fcb6860a82ec6946d0) Harden connector access and native ACP messaging
