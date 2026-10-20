---
name: setup-soulsteads-messaging
description: Set up a Telegram gateway for a local agent harness.
version: 0.2.0
author: Scott Glover (scottgl), with agent assistance
license: MIT
platforms: [linux]
---

# Set up Soulsteads Agent Messaging

Use when a user asks to connect their installed Claude Code or Codex to Telegram
as a user service. Use your harness's approved terminal/file tools for the commands
below. Read the repository README and docs/SETUP.md first. This skill requires no particular
skill-manager product.

Do not use this path to migrate a live bot, publish the repository, update an
existing installation, or configure an unsupported platform. Those need a
separately scoped procedure.

## Prerequisites

- Linux, Python 3.11+, Git, a systemd user manager, and Rust/Cargo for a source build.
- An installed Claude Code or Codex CLI authenticated as the service's Unix user.
- Explicit user authorization to install the gateway; separate consent before
  starting it or replacing an existing bot connection.
- A dedicated Telegram bot token in an owned, private regular `.env` file, outside
  the repository. Content: `TELEGRAM_BOT_TOKEN=...`. Never ask the user to paste
  the token into chat, put it in command arguments, or print the file.
- The user's numeric Telegram **user ID**, not their username or chat ID.

## Procedure

1. **Inspect before changing anything.** Confirm the repository URL/revision,
   working-tree state, supported OS, native CLI path, and user service manager.
   Read `python3 scripts/setup_gateway.py --help` and subcommand help. Preserve
   existing files and services. Never bypass missing CLI login or permissions.
   Ask only for missing choices: backend, user ID, private credential-file path,
   and whether the dedicated bot is already connected elsewhere.

2. **Explain the session boundary.** The gateway creates harness-managed sessions
   for messaging conversations using the native CLI's authentication. It does
   not attach to the terminal session that installed it or transfer that session's
   history, identity, MCP configuration, or project context. Native sessions and
   files remain governed by the selected harness and Unix account.

3. **Build from the inspected source.** In the checkout, run:

   ```sh
   mkdir -p .build-tmp
   cargo build --release --locked --no-default-features --bin soulsteads-messaging
   ./target/release/soulsteads-messaging --help
   ```

   Use an explicit external `CARGO_TARGET_DIR`/`TMPDIR` if appropriate, then supply
   the actual resulting binary path. Do not guess a public release download,
   silently install system packages, use sudo, or change global harness settings.
   A first source build is not instant. If Rust is missing, explain the prerequisite
   and use the user's approved toolchain-installation process.

4. **Prepare a private standalone gateway.** Use the confirmed values:

   ```sh
   python3 scripts/setup_gateway.py prepare \
     --backend claude \
     --telegram-user NUMERIC_USER_ID \
     --env-file /absolute/path/to/private-telegram.env \
     --binary /absolute/path/to/soulsteads-messaging
   ```

   Use `codex` instead of `claude` when selected, and `--harness-binary` for an
   explicit native CLI path. Setup otherwise resolves the CLI on the installing
   PATH and stores that PATH in the private runner. Inspect it for stable, intended
   tool directories; the user manager may otherwise have a different PATH.
   `--root` selects a non-default private installation;
   repeat it for subsequent commands. Do not print credential contents. Success
   means only the local files and bridge configuration check passed. The helper
   does not test platform authentication or a provider response.

5. **Review activation with the user.** Report the backend, allowed sender ID,
   installation/state paths, and intended service name, without secrets. Explain
   that native tools can act under the service user, Telegram does not provide an
   end-to-end-encrypted secret channel. New preparations enable scoped shell-command
   approvals: review the literal command/cwd and use `/approve ID` or `/deny ID`
   from the requesting account in the same chat/topic. Approvals are one operation
   only, expire after 45 minutes, and do not cover arbitrary tools or question forms.
   Existing installations need an explicitly reviewed config change to enable
   `telegram.interactivePermissions`; this helper does not update them. Never grant
   blanket tool access to bypass unsupported previews. Codex has a documented native shell-survival issue
   after cancellation; do not call `/cancel` a verified execution stop.

   Activation selects Telegram long polling and calls `deleteWebhook`; do not
   activate a bot owned by another gateway or webhook consumer without an explicit
   handoff plan. Do not drop pending updates or enable linger as a convenience.

6. **Install; start only when authorized.** To install the staged unit without
   activation:

   ```sh
   python3 scripts/setup_gateway.py install-service
   ```

   With explicit start authorization:

   ```sh
   python3 scripts/setup_gateway.py install-service --start
   ```

   Use the same `--root` if customized. Refuse unit conflicts rather than overwrite
   them. Read back the exact managed unit's state using the reported name. A user
   service's current activity does not prove logout/reboot persistence.

7. **Verify a real conversation.** Ask the authorized user to open the dedicated
   bot and send `/status`, then a harmless prompt requesting a unique short reply
   without tools. Confirm the response came from the intended harness, then a
   follow-up. With a consented second test account, verify unauthorized messages
   do not reach the harness. Never borrow an account or send unsolicited tests.
   Do not submit private documents or use live destructive commands as probes.

8. **Report precisely.** Separate prepared, installed, running, platform-connected,
   and message/reply-verified outcomes. List missing checks and the exact service
   control commands. When credentials, a user manager, native login, or authorized
   platform testing are unavailable, report the blocker and stop at the last
   verified step. Do not fabricate chat evidence or repeatedly retry activation
   whose effects are unknown.

## Pitfalls

- Empty sender allowlists can mean public access in manually configured gateways;
  the setup helper requires one numeric sender. Do not remove that requirement.
- Existing ambient `TELEGRAM_ALLOWED_USERS` can alter native configuration;
  use the managed runner and do not inject unreviewed override variables.
- Permission settings and workspace paths are not OS sandboxing. Do not promise
  that this gateway isolates the harness from files owned by the Unix user.
- Shared credentials must not have simultaneous pollers. Do not reuse the user's
  existing bot merely because its token is accessible.
- Restarting the gateway is not equivalent to resuming an arbitrary terminal chat.
- Do not dump the environment, raw config, or journals into a model conversation.
  Inspect local diagnostics with appropriate redaction.
- A failed prepare can retain a private partial root and credential copy. A
  rejected config check stages no installable unit. Preserve the partial root,
  correct inputs, and retry with a fresh `--root`; do not silently remove evidence.
- The first-install helper preserves existing roots; it is not an updater. Stop
  and disable the exact managed unit before any separately authorized removal,
  preserving credentials and recovery/session data.

## Verification

The required local check is a successful helper preparation with its native
`--check-config` invocation. The required live acceptance is an authorized
Telegram message, a native harness-generated reply, and a follow-up—not merely
`systemctl --user is-active` or a copied historical test log.

For helper development, run:

```sh
python3 -m unittest discover -s tests -p 'test_setup_gateway.py' -v
```

See the README for current setup scope and known limits. Do not enable
opt-in live/provider tests or additional connectors as part of routine setup.
