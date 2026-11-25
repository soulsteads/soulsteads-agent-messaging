# Setup

Everything you need to connect a chat app to a coding agent lives here. Pick your
platform:

| Platform | Setup | Needs |
|---|---|---|
| [Telegram](#telegram) | **Guided helper** (Linux) | Claude Code or Codex |
| [Matrix](#matrix) | Manual | Default-feature build |
| [Mattermost](#mattermost) | Manual | |
| [Slack](#slack) | Manual | Bot and app tokens |
| [Discord](#discord) | Manual | Bot token, Message Content intent |
| [WhatsApp](#whatsapp) | Manual | Default-feature build, QR pairing |
| [Web interface](#web-interface) | Manual | |

Only Telegram has a setup helper. The other platforms are configured by hand, as
described below. Install your chosen agent and sign in first; nothing here does
that for you.

**Prefer to let your agent do it?** Paste the
[setup prompt from the README](../README.md#the-easy-way-ask-your-agent) into
Claude Code, Codex, OpenCode or Pi, and it follows this guide for you.

## Before you start

You need Rust/Cargo and Git to build the bridge from source. The Telegram helper
also needs Python 3.11+ and a systemd user session.

**Build it** from this repository's directory:

```sh
mkdir -p .build-tmp

# Telegram, Mattermost, Slack, Discord and the web interface:
cargo build --release --locked --no-default-features --bin soulsteads-messaging

# Everything, including Matrix and WhatsApp (downloads their SDKs):
cargo build --release --locked --bin soulsteads-messaging
```

The first build downloads dependencies and can take a while.

### How manual configuration works

- **Configuration file.** The bridge reads `soulsteads-messaging.json` from the
  directory it runs in, or the file you pass with `--config`. Anything you omit
  falls back to the [configuration defaults](../src/defaults.json), which list
  every option.
- **Secrets.** Keep tokens out of the configuration file. Put them in an
  environment file and refer to them as `"{env:NAME}"`. The bridge reads `.env`
  and `.env.local` from its working directory, and real environment variables
  take precedence. [`.env.example`](../.env.example) lists every variable the
  bridge understands. Never commit real values.
- **Choose an agent.** `acp.defaultBackend` selects the agent. Built-in backends
  are `claude`, `codex`, `pi` and `copilot`; the default is `opencode`. For example,
  `{"acp": {"defaultBackend": "claude"}}`.
- **Use an existing agent home (optional).** Set `acp.agentHome` to an absolute
  directory or `~/…` path to run new ACP and native sessions in that exact
  directory, so the agent reads its normal `AGENTS.md`, `CLAUDE.md`, OpenCode
  configuration, and other runtime files. The directory must already exist; it
  is canonicalized at startup and is never created or copied into. A backend can
  override it with `acp.backends.<backend>.agentHome`; an explicit empty string
  there disables the inherited home for that backend. Do not combine this with
  OpenCode's legacy `acp.profileDir` copy mode.

  ```json
  {
    "acp": {
      "defaultBackend": "claude",
      "agentHome": "~/agents/shared",
      "backends": {
        "codex": { "agentHome": "/srv/agents/codex" },
        "pi": { "agentHome": "" }
      }
    }
  }
  ```

  Changing, enabling, or disabling an agent home intentionally prevents an old
  chat binding from resuming in a different identity. Use `/clear` to forget a
  retained binding; it will not delete a session outside the configured home.
- **Check, then run.**

  ```sh
  ./target/release/soulsteads-messaging --check-config   # loads config, starts nothing
  ./target/release/soulsteads-messaging --serve          # starts enabled connectors
  ```

  For all command-line options, run `./target/release/soulsteads-messaging --help`.

### Run multiple agents in one gateway

On Unix, use a gateway manifest when different bots, accounts, or web listeners should
reach different agents. Each connector belongs to the agent entry that enables
it. One agent can enable several connector types; two agents can enable the same
type with different bot accounts or, for Web, different ports. The gateway does
not choose an agent from message text. A regular configuration without `agents`
still runs one agent and keeps its existing state paths.

For example, create `gateway.json` with a Telegram bot for one agent and a Matrix
account for another (replace the example IDs and paths):

```json
{
  "stateDir": "/srv/messaging/state",
  "agents": [
    {
      "id": "build",
      "config": {
        "acp": { "defaultBackend": "codex", "agentHome": "/srv/agents/build" },
        "telegram": {
          "enabled": true,
          "token": "{env:BUILD_TELEGRAM_TOKEN}",
          "allowedUsers": ["123456789"]
        }
      }
    },
    {
      "id": "research",
      "config": {
        "acp": { "defaultBackend": "opencode", "agentHome": "/srv/agents/research" },
        "matrix": {
          "enabled": true,
          "homeserver": "https://matrix.example.org",
          "accessToken": "{env:RESEARCH_MATRIX_TOKEN}",
          "allowedUsers": ["@operator:example.org"],
          "allowedRooms": ["!research:example.org"],
          "strictAllowedRooms": true
        }
      }
    }
  ]
}
```

As the service user, create the state directories with mode `0700` (the parent
must already exist) and ensure the configured ACP homes exist. Use the canonical
absolute state path, without symlinks. Put the named credentials in a private
`.env` in the service's working directory or in its environment. Then run:

```sh
mkdir -m 700 /srv/messaging/state
mkdir -m 700 /srv/messaging/state/build /srv/messaging/state/research
soulsteads-messaging --check-config --config gateway.json
soulsteads-messaging --serve --config gateway.json
```

`--check-config` checks local configuration without starting connectors. The
same manifest can be passed to the [systemd service installer](MANUAL-SERVICE.md).
Use `--check-config`, not `--settings`, for a gateway manifest. CLI prompts and
the interactive console still require a single-agent configuration.

- **Bindings:** This mode supports Telegram, Matrix, Mattermost, and Web. It
  requires explicit Matrix access tokens. Use distinct bot accounts and avoid
  overlapping room/trigger rules that would let two agents answer the same
  message. Web defaults to loopback in this mode. Each enabled Web listener
  needs a distinct, nonzero port and an explicit `web.allowedOrigins` list,
  for example `["http://127.0.0.1:3420"]`. List the exact HTTP(S) origins
  browsers use, without wildcards, paths, or trailing slashes. This is a
  browser origin check, not authentication. Configure allowlists inside each
  agent entry; legacy connector environment overrides are not applied.
  `{env:NAME}` still works in each agent's settings. Pass provider variables
  through the agent's `acp.env` or `acp.backends.<backend>.env` rather than
  relying on full process environment inheritance.
- **State:** Each ID has its own `sessions.json`, generated `sessions/`
  workspaces, and Matrix SDK store under `stateDir/<id>/`. Keep IDs stable, do
  not reuse an ID for a different agent/account, and do not run another gateway
  against the same state or bot while this one is active. No legacy history is
  imported automatically. This keeps gateway conversation state separate; it
  does not sandbox native agents or isolate their Unix user, login state, or
  filesystem permissions.
- **Limits:** The native session picker, chat-triggered service restart,
  Mattermost turn receipts, Telegram Skillstore review bindings, and local
  `agentMessaging` are not available in this mode. Keep using a single-agent
  configuration if you need one of those features, or Slack, Discord, or
  WhatsApp. Stop/restart the shared process to apply configuration changes.

### Inspect and validate settings locally

```sh
./target/release/soulsteads-messaging --settings --help
./target/release/soulsteads-messaging --settings list
./target/release/soulsteads-messaging --settings get /web/port --json
./target/release/soulsteads-messaging --settings validate --config ./custom.json --json
```

Settings reads `soulsteads-messaging.json` in the current directory by default.
An absent default file uses the embedded defaults. Settings accepts readable,
regular config and project dotenv files of up to 1 MiB each, including symbolic
links to those files. Settings uses the usual project dotenv and process
environment precedence.

`list` reports canonical default paths and selected, documented optional
settings present in the file. `get` accepts a JSON Pointer from that list.
The output contains paths and source labels only. Arrays are listed as a
single setting. Legacy `toolMessages` and `web` arrays replace their default
child paths with `/toolMessages` and `/web`, respectively. An array-valued
`web` section does not enable the Web connector; use an object to configure it
or apply Web environment overrides. Credential fields and arbitrary input keys
are omitted.
`configured` means the file declares the setting, including declarations
that use an environment placeholder or a supported legacy alias. `env_override`
means the startup override logic applied an environment value to that setting.
`default` identifies an embedded default path with neither of those sources.
The labels describe declarations and overrides, so normalization can still
repair a configured value. Environment settings consumed separately by a
connector or the access policy are outside this inventory.

`validate` checks strict JSON, normalization, connector access policy, ACP agent
homes, agent messaging configuration, the session store path requirement, enabled
Web port range, and build support for enabled Matrix and WhatsApp connectors.
It also checks existence, ownership, and permissions for configured Mattermost
turn receipts, enabled Telegram Skillstore review receipts, and agent review
queue directories, without locking or reading the journals. Successful JSON
output is `{"valid":true}`.
Failures exit nonzero with a sanitized category and guidance on stderr and empty
stdout. Validation performs local checks only. Connector connectivity,
credentials, and service status are outside the scope of validation.

> [!WARNING]
> **Enabled remote connectors require an explicit access policy.** Set
> `allowedUsers` to your own user ID. An empty list is rejected at startup and by
> `--check-config`, unless you deliberately set `allowAllUsers: true`. That opt-in
> admits everyone who can reach the bot; it does not disable ignore lists. The
> environment variable `<PLATFORM>_ALLOWED_USERS` (for example
> `SLACK_ALLOWED_USERS`, a comma-separated list) overrides the configuration file
> when it is not empty. For an existing installation, update the allowlist and
> run `--check-config` before restarting.

Requests start with the `trigger` prefix (`!oc` by default; set `"trigger"` in the
configuration to change it). Each connector's section says when it answers without one.

---

## Soulsteads native harness (opt-in ACP)

The optional Rust harness can run as the explicit `soulsteads` backend. It is a
separately installed executable, not a bundled adapter or an alias for OpenCode.
The default backend remains `opencode`; selecting `soulsteads` requires an
explicit command and arguments:

```json
{
  "acp": {
    "defaultBackend": "soulsteads",
    "backends": {
      "soulsteads": {
        "command": "/opt/soulsteads/bin/soulsteads-harness",
        "args": ["--config", "/srv/agent/harness.toml", "--home", "/srv/agent/state", "acp"],
        "agentHome": "/srv/agent/workspace",
        "env": {"HARNESS_BASE_URL": "{env:HARNESS_BASE_URL}"}
      }
    }
  }
}
```

Prepare the harness configuration separately: select its model, instructions,
skills, MCP servers and tool grants there. The existing `agentHome` directory
must match the harness's granted workspace; the harness refuses a different ACP
cwd. This directory setting does not generate harness configuration or imply
Agent Home's automatic configuration translation supports the native harness.
Without a matching workspace configuration, per-conversation workspace creation
alone is not sufficient for this harness.

The bridge uses ordinary ACP negotiation, permission requests and persisted
session bindings. Native execution and history remain owned by the harness.
Configured environment entries are explicit; the child otherwise receives the
same restricted ambient environment as the other non-OpenCode backends, not the
whole bridge environment. If the provider needs a credential, name it in the
harness configuration and pass only that required variable through the backend's
`env` using `{env:NAME}`. The bridge does not log in, copy credentials or silently
switch models. Never put credential values in tracked configuration.

For the built-in `soulsteads` backend, text sent in an active Matrix thread is
acknowledged as queued, not live steering. Up to eight pending messages are kept
in memory and submitted in FIFO order on the same native session, each only
after the previous prompt returns `end_turn`. The existing thread collector
retains their output. Cancellation, a non-successful prompt result, disconnect,
or collector shutdown discards unsubmitted messages; restarting does not restore
them. Already submitted prompts are never retried automatically. Image follow-ups
are rejected without submission. Other ACP backends keep their existing steering
behavior.

Native capabilities and limits apply: unsupported controls fail rather than
being emulated by another harness. Separate conversation bindings do not provide
filesystem isolation when they share one configured workspace. This integration
is not an OS sandbox or general ACP/editor/platform certification.

## GitHub Copilot CLI (native ACP)

Copilot uses its own [ACP server](https://docs.github.com/en/copilot/reference/acp-server),
not one of this project's bundled native-protocol adapters. Install Copilot CLI
on the bridge host, make `copilot` available on the service user's `PATH`, and run
`copilot login` as that user with an account entitled to Copilot. GitHub CLI
authentication alone does not establish Copilot entitlement.

Select the backend in your configuration:

```json
{
  "acp": {
    "defaultBackend": "copilot",
    "agentHome": "~/agents/helper"
  }
}
```

`agentHome` is optional and must already exist. Without it, the bridge uses its
normal per-conversation workspace. The default launch command is
`copilot --acp --stdio`. An existing conversation can switch with
`/backend copilot`; its saved Copilot binding is independent of other backends
and resumes after a bridge restart once Copilot has persisted the conversation.
The tested Copilot CLI does not retain a session created only by control queries
before its first prompt. Before using separate one-shot CLI invocations for model
controls, send a text prompt once; if an empty session was lost after exit, use
`/clear` to forget its stale bridge binding. Long-lived conversations can query
and switch models within the same process normally.
Command, argument, environment and home
overrides use `acp.backends.copilot`, for example:

```json
{
  "acp": {
    "defaultBackend": "copilot",
    "backends": {
      "copilot": {
        "command": "/opt/copilot/bin/copilot",
        "args": ["--acp", "--stdio"],
        "env": {
          "COPILOT_HOME": "/srv/copilot",
          "COPILOT_GITHUB_TOKEN": "{env:COPILOT_GITHUB_TOKEN}"
        }
      }
    }
  }
}
```

The environment override is optional: normal `copilot login` uses the native
credential store. For headless authentication, keep the token in a private
environment file, not JSON or chat. Copilot backend processes receive the same
restricted ambient environment as the other non-OpenCode backends; custom
`COPILOT_HOME`, token, model or provider variables must be explicitly passed in
`acp.backends.copilot.env`. The bridge does not perform a login flow or add
`--allow-all`, `--allow-all-tools`, or other permission-bypass flags.
The existing connector approval rules apply; without a supported interactive
approval route, ACP permission requests are cancelled, not implicitly granted.

Copilot's ACP support is a public preview. Use `/models` to inspect the session's
advertised choices and `/model <model-id>` to select one, or use
`/config model <model-id>`. The bridge uses ACP `session/set_config_option` and requires the
returned configuration to confirm the selected model. `/model` queries the
current selection. Model, mode, session and MCP features depend on the installed
CLI and what it advertises; not every native slash
command is available through ACP. Guided Telegram setup and the optional local
agent-tools harness remain limited to their documented backends. Adding Copilot
does not qualify unattended shell execution, cancellation cleanup, or every live
connector combination.

For a real inference and restart/resume check through the compiled messaging
CLI, explicitly opt into a saved native login:

```sh
COPILOT_LIVE_HOME="$HOME/.copilot" cargo test --locked --test live_copilot -- --ignored --nocapture
```

Alternatively, supply `COPILOT_GITHUB_TOKEN` securely in the environment; the
test then uses a fresh Copilot home unless `COPILOT_LIVE_HOME` is also set.
The test sends only synthetic text, checks an exact reply, then restarts the
bridge and verifies the same session recalls a random test word. It starts no
chat connectors and grants no blanket tool permissions. A supplied native home
retains these synthetic test sessions; failures retain private diagnostic state
outside the checkout. Live inference needs network access and may consume
Copilot credits. Self-contained fixture tests run without credentials as part
of `cargo test --locked`.

## Telegram

For optional Skillstore approval messages, see the [Telegram review inbox](SKILL-REVIEWS.md).

The guided setup builds a new bridge on Linux using Claude Code or Codex and runs
it as a systemd user service.

**You need:**

- Python 3.11+, Git, Rust/Cargo, and a systemd user session
- Claude Code or Codex installed and signed in as the Linux user running the bridge
- A **dedicated Telegram bot** and its token. Do not share the bot with another app.
- Your numeric Telegram user ID. Only the account you allow can use this setup.
- A private file containing `TELEGRAM_BOT_TOKEN`, stored outside this repository with permissions `0600`

The token lets the bridge control the bot. Keep it out of Git, screenshots, chat,
and command arguments. Starting the bridge removes the bot's existing webhook
and uses long polling instead; another app using that bot will conflict.

Run these commands from this repository's directory.

### 1. Build the bridge

```sh
mkdir -p .build-tmp
cargo build --release --locked --no-default-features --bin soulsteads-messaging
```

This builds what the Telegram setup needs. It does not include the Matrix or
WhatsApp SDKs.

### 2. Save your bot token privately

Create a file **outside this checkout**, readable only by you (`chmod 600`), with:

```dotenv
TELEGRAM_BOT_TOKEN=your_bot_token
```

> [!WARNING]
> Do not put the real token in chat, Git, or command arguments.

### 3. Set up and start

Replace the user ID and credential-file path below. Use `--backend codex` instead
of `--backend claude` if you want Codex.

```sh
python3 scripts/setup_gateway.py prepare \
  --backend claude \
  --telegram-user YOUR_NUMERIC_USER_ID \
  --env-file /absolute/path/to/private-telegram.env \
  --binary "$PWD/target/release/soulsteads-messaging"

python3 scripts/setup_gateway.py install-service --start
```

1. **`prepare`** creates the private files and checks the local configuration. It
   does not start the bridge or verify your Telegram token or agent login. Review
   its output before continuing.
2. **`install-service --start`** installs a systemd user service, enables it,
   starts it, and checks that it is active. Omit `--start` to install it without
   starting.

> [!IMPORTANT]
> Starting switches the bot to long polling and removes its webhook, so **do not
> use a bot already serving another app**. The helper sets up a new installation;
> it does not overwrite or upgrade an existing one.

For all setup options:

```sh
python3 scripts/setup_gateway.py prepare --help
python3 scripts/setup_gateway.py install-service --help
```

### 4. Send a message

From the Telegram account you allowed, send `/status`, then a harmless prompt.
Check that your chosen agent replies and that you can send a follow-up. A running
service alone does not confirm the bot or agent is connected.

### Where are my files?

By default, setup stores configuration, sessions, and the launcher in:

```text
~/.local/share/soulsteads-agent-messaging
```

If you set `XDG_DATA_HOME`, setup uses `$XDG_DATA_HOME/soulsteads-agent-messaging`
instead. Treat this directory as private: it contains configuration and may
contain credentials and conversation data.

Want a different location? Add `--root /absolute/path` to **both** setup commands.
If the agent executable is not on `PATH`, add
`--harness-binary /absolute/path/to/executable` to `prepare`.

### Check, stop, or restart the service

Run these as the Linux user who installed the bridge:

```sh
# Check whether the bridge is running.
systemctl --user status soulsteads-telegram-gateway.service

# Read recent service logs. Keep any sensitive output private.
journalctl --user -u soulsteads-telegram-gateway.service -n 50 --no-pager

# Stop the bridge.
systemctl --user stop soulsteads-telegram-gateway.service

# Start it again, or restart it after a configuration change.
systemctl --user restart soulsteads-telegram-gateway.service
```

Stopping the bridge or sending `/cancel` is not proof every agent action has
stopped. Codex cancellation has a known case where an agent-started shell keeps
running. Check for continuing work before assuming an operation is cancelled.

### Agent-assisted setup

The [setup skill](../.agents/skills/setup-soulsteads-messaging/SKILL.md) walks a
coding agent through this same Telegram setup.

---

## Matrix

For a complete account-to-conversation walkthrough, follow the
[Matrix guide](MATRIX.md) and [manual user-service guide](MANUAL-SERVICE.md).
The settings below remain a connector reference.

Matrix needs manual configuration and a build with `platform-sdks`. For an
unencrypted setup, use `--no-default-features --features platform-sdks`; the
default-feature build also includes encryption support, whose interoperability
is not yet qualified.

You need a Matrix account for the bot and its credentials: an access token (preferred) or a
password. Add the following to `soulsteads-messaging.json`, replacing the placeholders:

```json
{
  "matrix": {
    "enabled": true,
    "homeserver": "https://matrix.example.org",
    "userId": "@your-bot:example.org",
    "allowedUsers": ["@you:example.org"],
    "allowedRooms": ["!roomid:example.org"]
  }
}
```

Put the secret in your environment file as `MATRIX_ACCESS_TOKEN` (or
`MATRIX_PASSWORD`); `MATRIX_USER_ID` can also come from there.

- **Rooms.** With `allowedRooms` set, the bot ignores every other room. Set
  `strictAllowedRooms` to `true` to ignore all rooms when the list is empty.
- **Invites.** `autoJoin` is on by default and joins rooms the bot is invited to.
  Turn it off, or keep `allowedRooms` set, if strangers could invite the bot.
- **Triggers.** Start a message with a pattern from `triggerPatterns` (`"!oc "` by
  default) or mention the bot's user ID.
- **Storage.** State and encryption keys live in `MATRIX_STORAGE_PATH`
  (default `~/.local/share/opencode-matrix-bot`). Treat it as private.
- **Approvals.** `interactivePermissions` lets you approve supported commands from Matrix.

Encrypted replies are not yet qualified for interoperability. Check a real reply
from your own account before relying on an encrypted room.

### Matrix display options

These settings change how tool activity looks in Matrix. None of them change what
the agent does, its permissions, or its final replies.

**Tool notices.** `toolMessages.mode` is `off`, `events` (the default), `status`,
`trace` or `hermes`. `showCalls: false` or `mode: "off"` hides tool-call notices.
`showArguments` and `showOutputFor` (default `["bash"]`) choose what else is shown.

**File labels.** Read, edit and write notices show only the file name by default,
for example `🔧 Editing config.rs`, even when `showArguments` is `false`. File
contents and replacements are not shown. To include directories:

```json
{ "matrix": { "showFullPaths": true } }
```

Only the boolean `true` turns this on. It relabels notices only; it is not a way
to hide paths from the agent or from other output. If the agent supplies no file
name, the notice stays generic. Read notices also show the requested line range
when the agent gives a numeric `offset` and `limit`, such as
`📖 Reading config.rs (requested lines 20–29)`. That is the range asked for, not
proof those lines were returned.

**Terminal activity threads.** Command-heavy requests can fill a room with
notices. This optional setting moves terminal commands into a separate thread:

```json
{ "matrix": { "terminalThread": { "mode": "auto", "threshold": 3 } } }
```

- `off` (default): keep the usual combined tool notices.
- `always`: use the activity thread from the first terminal call.
- `auto`: switch after `threshold` distinct terminal calls in one request (default
  `3`). Later updates to the same call do not count again, and other tools in
  between do not reset the count. Invalid values fall back to the defaults.

Only terminal call notices and output you already selected with
`toolMessages.showOutputFor` move there, limited to 2,000 characters per excerpt.
Replies, other tool notices, permission requests, questions and uploads stay in
the original conversation. If a command fails, a brief alert in the original
conversation links to the details. A terminal call is a `bash`, `terminal`,
`shell` or `exec` tool, or an ACP `execute` tool with a `command`.

Matrix has no nested threads, so the activity thread is a room-level notice that
links back to your message. Notices already sent are not moved, so calls before
`auto` switches stay where they were. Restarting the bridge does not resume an
old request's activity thread.

Tests cover this against a synthetic Matrix server, not a live rollout or an
encrypted room.

---

## Mattermost

Follow the [Mattermost guide](MATTERMOST.md) for bot setup, thread commands,
and a first conversation. Install it using the
[manual user-service guide](MANUAL-SERVICE.md), not the Telegram helper.
For restart and reply recovery, see
[optional durable turn receipts](MATTERMOST.md#optional-durable-turn-receipts).

---

## Slack

The Slack connector uses Socket Mode, so it needs two tokens and no public URL.

1. Create a Slack app with a bot user and turn on **Socket Mode**.
2. Install the app to your workspace and copy the **bot token** and an **app-level token**.
3. Put both in your environment file:

   ```dotenv
   SLACK_BOT_TOKEN=your_bot_token
   SLACK_APP_TOKEN=your_app_level_token
   ```

4. Enable the connector with your own Slack member ID:

   ```json
   {
     "slack": {
       "enabled": true,
       "allowedUsers": ["REPLACE_WITH_YOUR_MEMBER_ID"],
       "threadIsolation": true,
       "respondToThreadReplies": true
     }
   }
   ```

The bridge refuses to start with Slack enabled if either token is missing. Invite
the bot to a channel, start a message with the trigger, and check that a reply
arrives in the thread.

---

## Discord

1. Create a Discord application with a bot and copy its token.
2. In the developer portal, enable the **Message Content** intent. The bridge
   needs it to read what people send.
3. Invite the bot to your server with permission to read and send messages.
4. Put the token in your environment file:

   ```dotenv
   DISCORD_BOT_TOKEN=your_bot_token
   ```

5. Enable the connector with your own Discord user ID:

   ```json
   {
     "discord": {
       "enabled": true,
       "allowedUsers": ["REPLACE_WITH_YOUR_USER_ID"]
     }
   }
   ```

The bridge refuses to start with Discord enabled if the token is missing. Start a
message with the trigger in a channel the bot can see, or send it a direct
message, and check that a reply arrives.

---

## WhatsApp

WhatsApp needs the default-feature build and links to your account like a
WhatsApp Web session.

```json
{
  "whatsapp": {
    "enabled": true,
    "authFolder": "./.whatsapp-auth",
    "allowedUsers": ["REPLACE_WITH_YOUR_NUMBER"],
    "respondToOthers": false
  }
}
```

On first start, the bridge writes a pairing QR code to `pairing-qr.svg` inside
`authFolder`. Open it and scan it from WhatsApp under **Linked devices**. The
folder holds your WhatsApp session, so keep it private and out of Git.

`respondToOthers` defaults to `true`. Set it to `false` (or
`WHATSAPP_RESPOND_TO_OTHERS=false`) to ignore messages from other people.

---

## Web interface

The built-in web chat page and widget are off by default.

```json
{
  "web": {
    "enabled": true,
    "host": "127.0.0.1",
    "port": 3420,
    "allowedOrigins": ["http://127.0.0.1:3420", "http://localhost:3420"]
  }
}
```

Then open `http://127.0.0.1:3420/chat`. The page address must match an entry in `allowedOrigins`. `WEB_HOST`, `WEB_PORT`,
`WEB_PUBLIC_URL` and `WEB_ALLOWED_ORIGINS` can set the same options from your
environment file.

> [!CAUTION]
> The web interface has no login of its own, and the defaults listen on all
> network interfaces and allow any origin. Keep `host` on `127.0.0.1` and list
> specific `allowedOrigins` unless you put your own authentication in front of it.

---

## Something went wrong?

### Setup cannot find Claude Code or Codex

Check that the selected CLI works in the same Linux account. If it is installed
outside `PATH`, pass its full path with `--harness-binary`.

### Setup says the install directory already exists

The helper is for a new installation, not an updater. It refuses to overwrite
existing files. Inspect the directory before doing anything else; do not delete
it blindly or start a second bridge using the same bot.

### Setup failed partway through

It may have left private files, including a copied token. Protect those files
and inspect the error before retrying. Do not upload the installation directory
as a bug report.

### The bridge exits at start-up

Run `--check-config` first. A connector that is enabled but missing its
credentials stops the bridge, and so does enabling Matrix or WhatsApp in a build
made with `--no-default-features`.

### The service is running, but the bot does not reply

Check the service logs, token file, allowed user ID, and agent login. Make sure
no other app is polling the bot or using its webhook. Preparing files and
starting a service do not prove the chat app or the agent is connected.

---

## What can the agent do?

The agent runs with its Linux account's permissions. Choosing a working folder
does **not** sandbox it.

Telegram's `telegram.interactivePermissions` option can ask you to approve
supported shell commands. It is not an approval screen for every tool. Unsafe
or unsupported command previews are denied; tools the agent already permits
may run without another prompt.
