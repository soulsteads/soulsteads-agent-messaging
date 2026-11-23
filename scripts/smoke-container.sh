#!/bin/sh
# Exercise the installed CLI and its state in disposable, network-disabled containers.
set -eu
engine=${1:-docker}
image=${2:-localhost/soulsteads-agent-messaging:smoke}
# Use an explicit name: Podman removes anonymous volumes with --rm.
suffix=$(od -An -N 8 -tx1 /dev/urandom | tr -d " \n")
volume=$("$engine" volume create "soulsteads-smoke-$suffix")
trap '"$engine" volume rm "$volume" >/dev/null' EXIT
run() {
  "$engine" run --rm --network none --mount "type=volume,source=$volume,target=/data" "$@"
}
run --entrypoint sh "$image" -ec 'test "$(id -u)" -eq 10001 && test -w /data'
run "$image" --version
run --entrypoint sh -i "$image" -c 'umask 077; cat > /data/bridge.json' <<'JSON'
{"sessionStorePath":"/data/chat-sessions.json","acp":{"agentHome":"/data/workspace"}}
JSON
run "$image" --config /data/bridge.json --check-config | grep -F 'no connectors started'
