#!/usr/bin/env bash
set -euo pipefail
if (( $# > 1 )); then
  echo "Usage: $0 [OUTPUT_DIRECTORY]" >&2
  exit 2
fi
command -v dpkg-deb >/dev/null || { echo 'dpkg-deb is required' >&2; exit 1; }
output=${1:-dist/debian}
source_root=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
scratch=$(mktemp -d)
trap 'rm -rf -- "$scratch"' EXIT
"$source_root/scripts/build-linux-bundle.sh" "$scratch"
python3 -B "$source_root/scripts/package-deb.py" "$scratch/soulsteads-agent-messaging-linux-x86_64" "$output"
