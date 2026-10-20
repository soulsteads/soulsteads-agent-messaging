# Give your agent long-term memory with Hindsight

[Hindsight](https://hindsight.vectorize.io) is a separate, optional memory
service for coding agents. Once it is set up, your agent can remember what it
learned in earlier sessions and recall it later. This guide covers Claude Code,
Codex, OpenCode and Pi.

Hindsight is not part of Soulsteads Agent Messaging, and the bridge does not
require it. You install it into the agent itself. Hindsight is a third-party
project, so check its [documentation](https://hindsight.vectorize.io/sdks/integrations/coding-agents)
if a command here has changed.

> [!NOTE]
> Chat sessions are separate from your terminal session. After setup, check that
> memory also works for messages that arrive through your chat app. See
> [Check that it works](#check-that-it-works).

## Before you start

You need:

- Node.js, with `npx` on your `PATH`.
- One of the following, to decide where memory lives:
  - **Hindsight Cloud:** an API token.
  - **Self-hosted:** a server URL. See [Run your own server](#run-your-own-server).
  - **Local daemon:** `uv` on your `PATH` and an LLM API key.
- For Codex, `codex_hooks = true` in its configuration.

> [!WARNING]
> Memory holds what you and your agent discuss, including anything you send
> through chat. With Hindsight Cloud, that content is stored by a third party.
> Prefer a server you run yourself for sensitive work.

## Install

Run one command per agent. Re-running a command updates the installation.

| Agent | Command |
|---|---|
| Claude Code | `npx @vectorize-io/hindsight-coding-agents install claude-code` |
| Codex | `npx @vectorize-io/hindsight-coding-agents install codex` |
| OpenCode | `npx @vectorize-io/hindsight-coding-agents install opencode` |
| Pi | `npx @vectorize-io/hindsight-coding-agents install pi` |
| Every agent it finds | `npx @vectorize-io/hindsight-coding-agents install all` |

A bare `install` with no agent changes nothing.

On a terminal, the installer asks where memory should live. To skip the question,
for example in a script, choose one:

```sh
# Hindsight Cloud
npx @vectorize-io/hindsight-coding-agents install claude-code --server cloud --api-token YOUR_TOKEN

# A server you run
npx @vectorize-io/hindsight-coding-agents install claude-code --server self-hosted --api-url http://localhost:8888

# A local daemon
npx @vectorize-io/hindsight-coding-agents install claude-code --server daemon
```

Replace `claude-code` with `codex`, `opencode` or `pi` as needed.

> [!WARNING]
> Do not paste an API token into a chat or a shared terminal log. Prefer the
> installer's prompt or an environment variable, and keep the token out of Git.

## What gets changed

The installer keeps a runtime copy in `~/.hindsight/coding-agents` and backs up
files it edits as `<file>.hindsight-backup`.

| Agent | What it adds |
|---|---|
| Claude Code | Three hooks in `~/.claude/settings.json`, an MCP server added with `claude mcp add` at user scope, and a skill in `~/.claude/skills/hindsight-coding-agent` |
| Codex | Three hooks in `~/.codex/hooks.json` and an `[mcp_servers]` entry in `config.toml` |
| OpenCode | A plugin entry in `~/.config/opencode/opencode.json`, with native tools and no MCP |
| Pi | An extension entry in `~/.pi/agent/settings.json` and a skill in `~/.pi/agent/skills`, with no MCP |

## Run your own server

Hindsight needs a server and an LLM provider key. One Docker command starts a
server with an embedded database:

```sh
export OPENAI_API_KEY=your_key
docker run -it --pull always --name hindsight --restart unless-stopped \
  -p 8888:8888 -p 9999:9999 \
  -e HINDSIGHT_API_LLM_API_KEY=$OPENAI_API_KEY \
  -v hindsight-data:/home/hindsight/.pg0 \
  ghcr.io/vectorize-io/hindsight:latest
```

Then install with `--server self-hosted --api-url http://localhost:8888`.
Hindsight's documentation also covers local models and a Kubernetes chart.

## Configuration

Settings live in `~/.hindsight/coding-agent.json`. Move it with the
`HINDSIGHT_CONFIG` environment variable. All agents share one file.

| Setting | Environment variable | Purpose |
|---|---|---|
| `apiUrl` | `HINDSIGHT_API_URL` | Server address. The default is Hindsight Cloud. |
| `apiToken` | `HINDSIGHT_API_TOKEN` | Bearer token for the server. |
| | `HINDSIGHT_SERVER_MODE` | `cloud`, `self-hosted` or `daemon`. |

When both are set, the file wins over the environment. You can give one folder
its own server and token under `paths.<prefix>`.

### One memory bank per project

By default every agent working in the same Git repository shares a bank named
`coding-agent::{gitProject}`, and linked worktrees use the main repository's
name. To keep each agent's memory separate, set:

```json
{ "bankIdTemplate": "{harness}::{gitProject}" }
```

## Check that it works

1. Restart the agent. Claude Code and Codex apply changes on the next prompt;
   OpenCode and Pi need a restart.
2. Have a short conversation that teaches the agent something specific, such as
   a project convention.
3. Start a new session in the same repository and ask about it.
4. Look at usage and logs:

   ```sh
   npx @vectorize-io/hindsight-coding-agents stats
   ```

   Logs are in `~/.hindsight/coding-agents-logs/` (`plugin.log`, `diag.jsonl`
   and `usage.jsonl`).

Then repeat step 2 and 3 through your chat app. Agents started by the bridge may
not load the same hooks or plugins as a terminal session, so confirm it for the
agent and chat app you actually use.

## Remove it

```sh
npx @vectorize-io/hindsight-coding-agents uninstall claude-code   # or codex, opencode, pi
npx @vectorize-io/hindsight-coding-agents uninstall all
```

This removes exactly what the installer added. To erase a project's memory, delete
its bank on the Hindsight server.

## Sources

- [Hindsight coding agents](https://hindsight.vectorize.io/sdks/integrations/coding-agents)
- [Hindsight integrations hub](https://hindsight.vectorize.io/integrations)
