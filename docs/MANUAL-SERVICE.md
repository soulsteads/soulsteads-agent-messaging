# Run Matrix or Mattermost as a Linux user service

This guide installs a **new, manually configured** bridge. It does not use the
Telegram-only `setup_gateway.py` helper. Follow the [Matrix](MATRIX.md) or
[Mattermost](MATTERMOST.md) guide alongside this page.

These instructions target Linux with a systemd user manager. Other operating
systems need an equivalent process manager. No administrator privileges or public
inbound bridge endpoint are required by this procedure. Your host must be able to
reach the messaging server and the services used by your harness.

## Multiple connectors, one agent

Use one config file, one `soulsteads-messaging --serve` process, and one unit.
The process starts every connector whose top-level `enabled` is `true`. Use the
SDK-enabled build in step 4 if any enabled connector needs Matrix or WhatsApp;
Telegram and Mattermost can run in the same build. A Mattermost-only setup can
still use the smaller `--no-default-features` build.

For a fresh instance, choose a name such as `helper` in step 3. This example
uses an unencrypted Matrix room. Replace all sample IDs and URLs with yours:

```json
{
  "acp": { "defaultBackend": "codex" },
  "trigger": "!agent",
  "sessionStorePath": "../state/chat-sessions.json",
  "telegram": {
    "enabled": true,
    "token": "{env:TELEGRAM_BOT_TOKEN}",
    "allowedUsers": ["123456789"]
  },
  "mattermost": {
    "enabled": true,
    "url": "https://chat.example.org",
    "token": "{env:MATTERMOST_TOKEN}",
    "allowedUsers": ["mattermost-user-id"],
    "allowedChannels": ["mattermost-channel-id"]
  },
  "matrix": {
    "enabled": true,
    "homeserver": "https://matrix.example.org",
    "userId": "@bot:example.org",
    "accessToken": "{env:MATRIX_ACCESS_TOKEN}",
    "triggerPatterns": ["!agent "],
    "allowedUsers": ["@person:example.org"],
    "allowedRooms": ["!room:example.org"],
    "strictAllowedRooms": true,
    "encryption": { "enabled": false }
  }
}
```

Supply distinct platform credentials in the unit's private environment file.
Keep one active consumer per bot/account: don't start a second Telegram poller
or point an already running Matrix client at this instance's SDK store. For an
existing installation, review how to retain each connector's session bindings
and state before consolidating units; copying one store over the others does
not merge them.

The enabled connectors share the configured agent home, backend defaults, and
process lifetime. Users, rooms, replies, and conversation sessions remain
platform-scoped. This is not a cross-platform shared transcript, and a service
restart affects all its connectors. A connector that fails fatally can bring
this whole process down; keep separate units if that failure isolation matters.

### Multiple agents in one service

To bind different chat accounts to different agents in one process, use a
[gateway manifest](SETUP.md#run-multiple-agents-in-one-gateway) instead of the
single-agent example above. Each named agent has its own backend settings,
optional agent home, and state directory. Pass the manifest as `--config` to
the same unit installer in step 5. Validate it with `--check-config` before
starting the service. The manifest does not import legacy sessions or route
one shared bot to multiple agents. A service restart still affects every
binding, and the agents run as the same Unix user, not in separate sandboxes.

## 1. Check prerequisites and ownership

- Install Git, Rust/Cargo, and the harness you intend to use. Authenticate that
  harness as the same Unix user who will run the service. Check its own documented
  login/status command locally; do not publish its output or credentials.
- Inspect the checkout and select a source revision. Check available disk space
  on the build filesystem before building. A source build downloads dependencies.
- Use a dedicated bot account. Check your existing services and applications for
  another bridge using that account. Do not activate a competing consumer.
- Set `allowedUsers` to the platform user IDs that may reach the agent and
  restrict rooms/channels as appropriate. An enabled connector with an empty
  sender allowlist is rejected unless you explicitly opt in with `allowAllUsers`.
  Do not rely on a trigger as authorization.
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

Choose one instance name for the private state and configuration directories. Use
`matrix` or `mattermost` for a standalone connector, or a distinct name such as
`helper` for a combined service. The built-in installer owns the fixed
`soulsteads-messaging.service` name, so it supports one installed service per
user or system scope at a time:

```sh
INSTANCE=matrix
# For Mattermost, use INSTANCE=mattermost instead.
ROOT="$HOME/.local/share/soulsteads-agent-messaging/$INSTANCE"
CONFIG_DIR="$HOME/.config/soulsteads-agent-messaging/$INSTANCE"
UNIT="soulsteads-messaging.service"

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

In a local editor, create `$ROOT/work/.env` using the chosen platform guide.
Never paste real tokens into chat, command arguments, screenshots, or Git. Use
simple `NAME=value` assignments, not `export` commands or shell substitution.
Create `$CONFIG_DIR/bridge.json` from that guide's example, replacing every
placeholder. Then protect both files:

```sh
chmod 700 "$ROOT" "$CONFIG_DIR" "$ROOT/work"
chmod 600 "$ROOT/work/.env" "$CONFIG_DIR/bridge.json"
```

The bridge loads `.env` files from its working directory, **not** from the
`--config` file's parent. The installer records that working directory but never
copies credentials into the unit. Do not add other `.env` files under
`$ROOT/work`; real process environment variables take precedence, and nonempty
platform `*_ALLOWED_USERS` variables can override the JSON sender list.

## 4. Build and install a dedicated binary

From the source checkout, select the command for your connector:

```sh
# SDK-enabled: required when Matrix or WhatsApp is enabled, including combined services.
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

The built-in installer creates a unit for the exact binary, explicit config, and
current working directory. It does not read the config or `.env` file while
installing, so run it from the intended `$ROOT/work` directory. Preview the unit
first; preview does not create a unit or call systemd:

```sh
(
  cd "$ROOT/work"
  "$ROOT/bin/soulsteads-messaging" --config "$CONFIG_DIR/bridge.json" \
    service install --user --dry-run
)
```

After inspecting the preview, install it. This is an atomic no-clobber operation:
if `soulsteads-messaging.service` already exists, the command refuses it rather
than replacing an existing service. Installation requires Linux procfs and a unit
directory filesystem that supports anonymous temporary files. The installer
never enables, starts, or reloads a unit.

```sh
(
  cd "$ROOT/work"
  "$ROOT/bin/soulsteads-messaging" --config "$CONFIG_DIR/bridge.json" \
    service install --user
)
```

The generated unit has an explicit `WorkingDirectory`, starts the gateway with
`--serve --config`, and contains no credentials or `EnvironmentFile`.
`$ROOT/work` is the walkthrough's choice, not a bridge requirement. For a
custom layout, `$ROOT/.env` works only if `$ROOT` is the service's
`WorkingDirectory`. Run the installer preview from the intended directory and
check `WorkingDirectory` in its output. Use that same directory for the config
check, keep `.env` at mode `0600`, and review any relative paths in your config
before changing an existing service's working directory.

Validate the configuration as the service user before activation, then check
and reload the unit manually:

```sh
(
  cd "$ROOT/work"
  "$ROOT/bin/soulsteads-messaging" --config "$CONFIG_DIR/bridge.json" --check-config
)
systemd-analyze --user verify "$HOME/.config/systemd/user/$UNIT"
systemctl --user daemon-reload
systemctl --user cat "$UNIT"
```

For a system unit, an administrator must make the installation decision and name
the non-root account that will run the gateway. The installer always writes the
fixed `/etc/systemd/system/soulsteads-messaging.service` target; it does not read
the config as root. Ensure the selected account can read the private config and
working directory before manually reloading the system manager:

```sh
(
  cd "$ROOT/work"
  sudo "$ROOT/bin/soulsteads-messaging" --config "$CONFIG_DIR/bridge.json" \
    service install --system --run-as SERVICE_USER --dry-run
)
```

Omit `--dry-run` only after reviewing the preview, then use `systemctl daemon-reload`
and manually choose whether to start or enable `soulsteads-messaging.service`. Do
not add credentials to the unit to make a system installation convenient.

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
binary is running, each enabled connector has authenticated as the expected bot,
and the service is not repeatedly restarting. Then run the real-message checks
for each connector. Use the [Telegram guide](SETUP.md#telegram) if enabled. A
process can be active without a working platform/harness connection.
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
  Do not disable certificate verification. Use an SDK-enabled build for a service
  that includes Matrix or WhatsApp.
- **No resume after restart:** preserve bindings and native state, keep their paths
  stable, and follow the platform's explicit-addressing instructions.

Report preparation, installation, running state, platform connection, and verified
conversation separately. This manual procedure is not a claim that every
platform/harness combination has passed live testing.
