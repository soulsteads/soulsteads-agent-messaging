#!/usr/bin/env bash
set -euo pipefail
task_root=$(cd "$(dirname "$0")/.." && pwd -P)
cd "$task_root"
if [[ ! -f tests/reference/opencode-chat-bridge/package.json ]]; then
  printf '%s\n' 'The TypeScript conformance reference is not included in this repository.' 'Supply the reference checkout described in tests/reference/README.md before running this helper.' >&2
  exit 1
fi
cd tests/reference/opencode-chat-bridge
bun install --frozen-lockfile --ignore-scripts
