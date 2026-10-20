# Run Matrix or Mattermost as a Linux user service

This guide installs a **new, manually configured** bridge. It does not use the
Telegram-only `setup_gateway.py` helper. Follow the [Matrix](MATRIX.md) or
[Mattermost](MATTERMOST.md) guide alongside this page.

These instructions target Linux with a systemd user manager. Other operating
systems need an equivalent process manager. No administrator privileges or public
inbound bridge endpoint are required by this procedure. Your host must be able to
reach the messaging server and the services used by your harness.

## 1. Check prerequisites and ownership

- Install Git, Rust/Cargo, and the harness you intend to use. Authenticate that
  harness as the same Unix user who will run the service. Check its own documented
  login/status command locally; do not publish its output or credentials.
- Inspect the checkout and select a source revision. Check available disk space
  on the build filesystem before building. A source build downloads dependencies.
- Use a dedicated bot account. Check your existing services and applications for
  another bridge using that account. Do not activate a competing consumer.
- Choose exactly which users and rooms/channels may reach the agent. An empty
  sender allowlist can allow everyone. Do not rely on a trigger as authorization.
- Review the harness's tools and permissions. The bridge is not a sandbox, and a
  conversation working directory does not restrict access to other files.

For an existing installation, stop here and plan a backed-up handoff. Do not
reuse this fresh-install procedure over its state or configuration.

## 2. Choose a harness and understand sessions

The platform guides use OpenCode, the bridge's default, with `opencode acp` on
`PATH`. Install and authenticate OpenCode before continuing. For another supported
harness, replace the example's top-level `acp` section using one of these fragments:

```json
{"acp": {"defaultBackend": "claude"}}
```

```json
{"acp": {"defaultBackend": "codex"}}
```

```json
{"acp": {"defaultBackend": "pi"}}
```

Each choice needs its corresponding installed and authenticated native CLI. The
built-in adapters run from the bridge binary; do not replace their `builtin:`
commands with guessed ACP commands. A custom ACP server uses `acp.command` and
`acp.args`; consult the [defaults](../src/defaults.json) and the server's own
instructions.

Messaging opens harness-managed conversations. It does not attach to the terminal
conversation that installed it. New conversation directories are created beneath
`SESSION_BASE_DIR`; saved bindings retain their recorded working directories.
Bridge configuration, conversation bindings, platform state, and native harness
history are separate things. Preserve all relevant stores for continuity.

### Optional Agent Home

**Agent Home is not required for this setup.** Home defines an agent, the harness
runs it, and Messaging connects people to it. Use the selected harness's supported
configuration mechanisms for identity, skills, tools, and model settings.

These instructions do not provide a generic Home-directory loader. Do not assume
that pointing `acp.profileDir` at an arbitrary Agent Home prepares every harness:
that option is an OpenCode-specific profile-copy mechanism, not a universal Home
contract. Only use Home preparation instructions that match your installed Home
and harness versions. Do not substitute an extra chat prompt for actual harness
configuration, or silently fall back to another identity when preparation fails.

## 3. Create private directories

Use a separate installation name for each connector. Choose **one**:

```sh
INSTANCE=matrix
# For Mattermost, use INSTANCE=mattermost instead.
ROOT="$HOME/.local/share/soulsteads-agent-messaging/$INSTANCE"
CONFIG_DIR="$HOME/.config/soulsteads-agent-messaging/$INSTANCE"
UNIT="soulsteads-messaging-$INSTANCE.service"

umask 077
# Refuse an existing installation rather than overwrite it.
if [ -e "$ROOT" ] || [ -L "$ROOT" ] || \
   [ -e "$CONFIG_DIR" ] || [ -L "$CONFIG_DIR" ] || \
   [ -e "$HOME/.config/systemd/user/$UNIT" ] || \
   [ -L "$HOME/.config/systemd/user/$UNIT" ]; then
  printf 'Installation paths already exist; inspect them before continuing.\n' >&2
  exit 1
fi
mkdir -p "$ROOT/bin" "$ROOT/work" "$ROOT/state/sessions" "$CONFIG_DIR"
```

Run shell blocks in the same shell so these variables remain defined. Do not
execute this block in a shell containing unsaved work if you need to retain it
after a failed prerequisite check.

In a local editor, create `$CONFIG_DIR/credentials.env` using the chosen platform
guide. Never paste real tokens into chat, command arguments, screenshots, or Git.
Use simple `NAME=value` assignments, not `export` commands or shell substitution.
Create `$CONFIG_DIR/bridge.json` from that guide's example, replacing every
placeholder. Then protect both files:

```sh
chmod 700 "$ROOT" "$CONFIG_DIR"
chmod 600 "$CONFIG_DIR/credentials.env" "$CONFIG_DIR/bridge.json"
```

The bridge loads `.env` files from its working directory, **not** from the
`--config` file's parent. This procedure instead loads the private credential file
through systemd. Do not add competing `.env` files under `$ROOT/work`; real
process environment variables take precedence, and nonempty platform
`*_ALLOWED_USERS` variables can override the JSON sender list.

## 4. Build and install a dedicated binary

From the source checkout, select the command for your connector:

```sh
# Matrix without encryption: includes the platform SDKs.
cargo build --release --locked --no-default-features --features platform-sdks --bin soulsteads-messaging
```

```sh
# Mattermost: excludes the optional Matrix/WhatsApp SDKs.
cargo build --release --locked --no-default-features --bin soulsteads-messaging
```

Matrix encryption is outside this walkthrough. The default build includes the
`matrix-encryption` feature, but that does not establish encrypted-room
interoperability. Keep the guide's `encryption.enabled` set to `false` and use an
unencrypted test room with nonsensitive messages.

Copy the selected build before building another feature combination:

```sh
install -m 700 target/release/soulsteads-messaging "$ROOT/bin/soulsteads-messaging"
"$ROOT/bin/soulsteads-messaging" --help
```

If you use `CARGO_TARGET_DIR`, copy from that directory instead. Do not run a
service directly from a disposable Cargo output directory. Separate binaries and
state let you operate Matrix and Mattermost independently.

## 5. Install the unit without starting it

Create `~/.config/systemd/user/soulsteads-messaging-matrix.service`, or use the
`mattermost` name for that connector. Create the parent directory if absent.
The following is a template: replace **every** `INSTANCE` with your chosen name,
and replace `HARNESS_BIN_DIR` with the absolute directory containing your native
CLI. Include other tool directories only if needed and trusted.

```ini
[Unit]
Description=Soulsteads Agent Messaging (INSTANCE)
After=network.target

[Service]
Type=simple
WorkingDirectory=%h/.local/share/soulsteads-agent-messaging/INSTANCE/work
EnvironmentFile=%h/.config/soulsteads-agent-messaging/INSTANCE/credentials.env
Environment="PATH=HARNESS_BIN_DIR:/usr/local/bin:/usr/bin:/bin"
Environment="SESSION_BASE_DIR=%h/.local/share/soulsteads-agent-messaging/INSTANCE/state/sessions"
Environment="MATRIX_STORAGE_PATH=%h/.local/share/soulsteads-agent-messaging/INSTANCE/state/matrix"
ExecStartPre=%h/.local/share/soulsteads-agent-messaging/INSTANCE/bin/soulsteads-messaging --config %h/.config/soulsteads-agent-messaging/INSTANCE/bridge.json --check-config
ExecStart=%h/.local/share/soulsteads-agent-messaging/INSTANCE/bin/soulsteads-messaging --config %h/.config/soulsteads-agent-messaging/INSTANCE/bridge.json --serve
Restart=on-failure
RestartSec=5
UMask=0077

[Install]
WantedBy=default.target
```

`MATRIX_STORAGE_PATH` is unused by Mattermost and may be omitted from its unit.
The explicit `sessionStorePath` in each platform example is resolved under
`WorkingDirectory`. Do not share these paths between connector instances.

Check the unit before activation:

```sh
systemd-analyze --user verify "$HOME/.config/systemd/user/$UNIT"
systemctl --user daemon-reload
systemctl --user cat "$UNIT"
```

Inspect the unit locally; do not publish private installation details. The
`ExecStartPre` check loads the explicit JSON and environment without starting
connectors. **It does not validate tokens, allowlists, build-feature availability,
server access, or harness authentication.** Those require the next steps.

## 6. Activate and verify

Start only after the account owner authorizes activation and you have ruled out
another consumer. Starting uses the credentials to connect to the platform.

```sh
systemctl --user start "$UNIT"
systemctl --user status "$UNIT" --no-pager
systemctl --user show "$UNIT" -p ActiveState -p SubState -p MainPID -p NRestarts -p ExecStart
journalctl --user -u "$UNIT" -n 50 --no-pager
```

Keep diagnostics private and redact them before sharing. Check that the selected
binary is running, the connector has authenticated as the expected bot, and the
service is not repeatedly restarting. Then run the platform guide's real-message
checks. A process can be active without a working platform/harness connection.
If startup fails repeatedly, stop the exact unit and investigate before retrying.

After successful acceptance, enable startup with the user manager if desired:

```sh
systemctl --user enable "$UNIT"
```

This does not guarantee startup after reboot before login, or continued operation
after logout. Those depend on the host's user-manager policy. Verify them
separately if required; do not enable lingering or change host policy silently.

## Operate, upgrade, and remove

Use the same instance and unit variables when returning in a new shell.

```sh
systemctl --user stop "$UNIT"
systemctl --user restart "$UNIT"
journalctl --user -u "$UNIT" -n 50 --no-pager
```

Stopping a service or sending `/cancel` is not proof every harness-started process
has stopped. Codex has a known case where a shell process survives cancellation.
Check continuing work before assuming an operation is cancelled.

For an upgrade:

1. Record the source revision and build features. Build the replacement separately.
2. Stop the exact connector service and check for continuing harness work.
3. Take a private, consistent backup of its unit, binary, configuration,
   credentials, bindings, session directories, platform store, and relevant native
   harness state. Preserve ownership and permissions. Do not copy a live SQLite
   store as if it were a consistent backup.
4. Install the replacement binary, preserve existing paths and configuration,
   inspect changes, and repeat startup and real-message acceptance checks.
5. If acceptance fails, stop the replacement. Restore the previous binary/unit/
   configuration as appropriate. Review any state-format changes before restoring
   state; do not blindly rewind Matrix sync or conversation state after new work.

Do not use `/clear` or `/reset` to troubleshoot continuity: they delete current
session history. A changed backend identity or working directory can prevent
resume; investigate rather than deleting the saved binding.

To uninstall, stop and disable only this unit, check remaining processes, remove
its exact unit file, and run `systemctl --user daemon-reload`. Keep the private
installation and credential directories until you deliberately decide what to
retain. Do not delete shared harness credentials, other Homes, native history, or
another connector's data. Revoke the dedicated bot token when retiring the account.

## Troubleshooting

- **Service cannot find the harness:** check the unit's explicit `PATH` and the
  native CLI under the same Unix account. An interactive shell's PATH is not the
  service manager's PATH. Never copy someone else's login to bypass authentication.
- **Configuration check passes but startup fails:** the check only loads config.
  Check the selected feature bundle, credentials, server access, and private logs.
- **No reply:** check bot membership, exact sender and room/channel IDs, environment
  overrides, triggers, and harness/provider access. Do not empty allowlists as a fix.
- **WebSocket or TLS failure:** verify certificates and proxy/WebSocket support.
  Do not disable certificate verification. For Mattermost, use the documented
  no-default-features binary rather than an unnecessary SDK-enabled bundle.
- **No resume after restart:** preserve bindings and native state, keep their paths
  stable, and follow the platform's explicit-addressing instructions.

Report preparation, installation, running state, platform connection, and verified
conversation separately. This manual procedure is not a claim that every
platform/harness combination has passed live testing.
