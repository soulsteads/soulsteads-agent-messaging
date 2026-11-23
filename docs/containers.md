# Run Agent Messaging in a container

Build a Linux image from this repository with Docker or Podman. Run these
commands from the root of a Git checkout that contains `Dockerfile`. You need
a running container engine and network access for the public build dependencies.
No registry account or host language toolchain is required; the build pulls
public base images. Use `engine=podman` in place of `engine=docker` for all
commands below.

```sh
engine=docker                   # or: engine=podman
image=localhost/soulsteads-agent-messaging:local
"$engine" build --tag "$image" .
"$engine" volume create soulsteads-agent-messaging-data
```

The image runs as UID/GID `10001`, without root. A fresh engine-managed volume
at `/data` inherits the image's private directory and avoids bind-mount UID and
SELinux problems. If using a bind mount instead, provision a dedicated directory
writable by the mapped container user, mode `0700`, with suitable SELinux labeling
(e.g. a private `:Z` label on Podman); do not make it world-writable. Don't mount
a host home or the container-engine socket.

## Configure and check the bridge

The image includes Messaging's default connectors and ACP adapters, **not** the
native agent CLI or its login. An ACP backend is launched as a child process:
a host executable is not available just because the host runs it. For chat
replies, use a derived image that installs a supported Linux harness (including
its runtime dependencies) or a compatible ACP server, then set the backend
command to that executable **inside the container**. Authenticate it at
runtime under `/data/home` or mount only its documented dedicated credential
store. Do not copy credentials, another user's profile, or an entire host home
into an image. Follow the manual connector and ACP steps in [Setup](SETUP.md);
the host systemd installer is not needed here.

Create a private `bridge.json` outside the source checkout. Start with this
layout and add one connector, its narrow sender allowlist, and the in-container
backend from the setup guide. Keep the example's paths, or adjust all paths
consistently. The example alone enables no connector.

```json
{
  "sessionStorePath": "/data/chat-sessions.json",
  "acp": { "agentHome": "/data/workspace" }
}
```

For example, a Telegram config uses `"token": "{env:TELEGRAM_BOT_TOKEN}"`.
For connectors that need secrets, create `gateway.env` alongside `bridge.json`
with mode `0600` and the required `NAME=value` lines. Neither file belongs in Git
or the build context. The engine's administrators can inspect runtime environment
variables. Skip `--env-file` only if the selected config needs no secrets.

```sh
# Set BRIDGE_CONFIG and GATEWAY_ENV to the absolute paths to the private files.
"$engine" run --rm -i --network none -v soulsteads-agent-messaging-data:/data \
  --entrypoint sh "$image" -ec 'umask 077; set -C; cat > /data/bridge.json' \
  < "$BRIDGE_CONFIG"
"$engine" run --rm --network none --env-file "$GATEWAY_ENV" \
  -v soulsteads-agent-messaging-data:/data "$image" \
  --config /data/bridge.json --check-config
```

The check starts no connectors. If it succeeds and the harness is ready, start
the gateway. The polling/socket connectors need outbound network access but no
published host ports:

```sh
"$engine" run -d --name soulsteads-agent-messaging --init \
  --env-file "$GATEWAY_ENV" -v soulsteads-agent-messaging-data:/data "$image" \
  --config /data/bridge.json --serve
"$engine" logs soulsteads-agent-messaging
```

Use one bridge per bot/account. Do not reuse a bot still connected to a host
service. Validate the selected chat/harness separately with a dedicated test
conversation. The optional web chat has no login; it is deliberately not
published here. If you need local web chat on native Linux, follow the Web
interface section of [Setup](SETUP.md#web-interface) to bind explicitly to
`127.0.0.1` and set specific origins, then add `--network host` and no `-p` to
`run`. This shares the host's network namespace; do not use it for connectors
that only need outbound access. Never publish unauthenticated web chat.

## Retain state

The volume contains the working directory, sessions, connector state (including
relative Matrix/WhatsApp paths), and any harness login stored under `/data/home`.
Treat the volume and its backups as private. Before updating config or replacing
the image, stop the old container. Back up the volume and any separately mounted
state; check the replacement image with `--check-config`. Keep the previous image
and a compatible backup for rollback. Do not remove the volume to replace the
container.

To stop and remove just the container (the named volume remains):

```sh
"$engine" stop --time 30 soulsteads-agent-messaging
"$engine" rm soulsteads-agent-messaging
```

Then build the desired source revision and rerun the start command with the same
volume. Reuse a stable image tag or digest and the same deployment settings when
supervising it with your container engine or service manager. First test on a
fresh disposable volume. Run the offline image smoke locally with:

```sh
sh scripts/smoke-container.sh "$engine" "$image"
```
