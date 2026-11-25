#!/usr/bin/env bash
# Build the production GNU/Linux executable and its release notices in scratch.
set -euo pipefail
if (( $# != 1 )); then
  echo "Usage: $0 EXISTING_SCRATCH_DIRECTORY" >&2
  exit 2
fi
RUNNER_TEMP=$(realpath -- "$1")
cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.."
bundle='soulsteads-agent-messaging-linux-x86_64'
mkdir "$RUNNER_TEMP/$bundle"
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$RUNNER_TEMP/release-target}"
export CARGO_PROFILE_RELEASE_DEBUG=0 CARGO_PROFILE_RELEASE_STRIP=debuginfo
# Ubuntu 24.04 GNU/Linux build; do not include dev/test executables.
cargo +1.100.0 build --release --locked --target x86_64-unknown-linux-gnu --bin soulsteads-messaging
install -m 0755 "$CARGO_TARGET_DIR/x86_64-unknown-linux-gnu/release/soulsteads-messaging" "$RUNNER_TEMP/$bundle/soulsteads-messaging"
install -m 0644 LICENSE NOTICE "$RUNNER_TEMP/$bundle/"
"$RUNNER_TEMP/$bundle/soulsteads-messaging" --help > /dev/null
cargo +1.100.0 metadata --format-version 1 --locked --filter-platform x86_64-unknown-linux-gnu > "$RUNNER_TEMP/cargo-metadata.json"
# Metadata's unified features can retain other-platform packages. Select
# notices from the same target's non-dev Cargo tree, not that superset.
cargo +1.100.0 tree --locked --target x86_64-unknown-linux-gnu --edges normal,build --prefix none --format '{p}' > "$RUNNER_TEMP/cargo-target-tree.txt"
python3 - "$RUNNER_TEMP/cargo-metadata.json" "$RUNNER_TEMP/cargo-target-tree.txt" "$RUNNER_TEMP/$bundle" <<'PY'
import hashlib
import json
from pathlib import Path, PurePosixPath
import re
import sys
import tarfile
import tomllib

metadata = json.loads(Path(sys.argv[1]).read_text())
target_tree = Path(sys.argv[2]).read_text().splitlines()
bundle = Path(sys.argv[3])
notices = bundle / 'dependency-notices'
notices.mkdir()
fallback_dir = Path('scripts/release-notices')
fallback_manifest = (fallback_dir / 'upstream.json').read_bytes()
fallbacks = {}
for group in json.loads(fallback_manifest):
    for crate, provenance in group['crates'].items():
        if crate in fallbacks:
            raise SystemExit(f'Duplicate upstream notice entry for {crate}')
        fallbacks[crate] = (group, provenance)

lock_bytes = Path('Cargo.lock').read_bytes()
locked = {(entry['name'], entry['version'], entry.get('source')): entry.get('checksum')
          for entry in tomllib.loads(lock_bytes.decode('utf-8'))['package']}
registry = 'registry+https://github.com/rust-lang/crates.io-index'
selected = set()
for line in target_tree:
    match = re.fullmatch(r'([a-zA-Z0-9_-]+) v([a-zA-Z0-9_.+-]+)(?: .*)?', line)
    if not match:
        raise SystemExit('Invalid package row in target dependency tree')
    selected.add(match.groups())
root, = [pkg for pkg in metadata['packages'] if pkg['id'] == metadata['resolve']['root']]
root_key = (root['name'], root['version'])
if root_key not in selected or len(selected) < 2:
    raise SystemExit('Missing root or dependencies in target dependency tree')
selected.remove(root_key)
packages = {}
for pkg in metadata['packages']:
    key = (pkg['name'], pkg['version'])
    if key in selected:
        if key in packages:
            raise SystemExit(f'Ambiguous target package for {key}')
        packages[key] = pkg
if packages.keys() != selected:
    raise SystemExit('Missing metadata for selected target dependencies')
if not fallbacks.keys() <= {f'{name}-{version}' for name, version in selected}:
    raise SystemExit('Upstream notice manifest includes packages not in the target dependency tree')

legal = re.compile(r'(license|licence|copying|copyright|notice|patents|unlicense)(?:$|[._-])', re.I)
sha256 = re.compile(r'[a-f0-9]{64}')
mpl = re.compile(r'\bMPL-2\.0(?:\+|-only|-or-later)?\b')
rows = []
sources = []
for name, version in sorted(selected):
    pkg = packages[name, version]
    crate = f'{name}-{version}'
    source = Path(pkg['manifest_path']).parent.resolve()
    target = notices / crate
    checksum = locked.get((name, version, pkg['source']))
    if (pkg['source'] != registry or not checksum or not sha256.fullmatch(checksum)
            or not re.fullmatch(r'[a-zA-Z0-9_.+-]+', crate)
            or source.name != crate or source.parent.parent.name != 'src'):
        raise SystemExit(f'Unsupported or unlocked crate source for {crate}')
    archive = source.parents[2] / 'cache' / source.parent.name / (crate + '.crate')
    if not archive.is_file():
        raise SystemExit(f'Missing Cargo archive for {crate}')
    with archive.open('rb') as stream:
        if hashlib.file_digest(stream, 'sha256').hexdigest() != checksum:
            raise SystemExit(f'Cargo archive checksum mismatch for {crate}')
        stream.seek(0)
        packaged = {}
        vcs_bytes = None
        with tarfile.open(fileobj=stream, mode='r|gz') as members:
            for member in members:
                path = PurePosixPath(member.name)
                if path.parts[:1] != (crate,) or '..' in path.parts:
                    raise SystemExit(f'Invalid Cargo archive path for {crate}: {member.name}')
                relative = PurePosixPath(*path.parts[1:])
                if relative.as_posix() == '.cargo_vcs_info.json' and member.isfile():
                    vcs_bytes = members.extractfile(member).read()
                if (relative.as_posix() == pkg['license_file']
                        or (legal.match(relative.name) and relative.suffix != '.rs'
                            and not member.isdir())):
                    if not member.isfile() or relative in packaged:
                        raise SystemExit(f'Invalid packaged notice for {crate}: {relative}')
                    packaged[relative] = members.extractfile(member).read()

    disk_files = {PurePosixPath(path.relative_to(source).as_posix())
                  for path in source.rglob('*')
                  if path.is_file() and legal.match(path.name) and path.suffix != '.rs'}
    if pkg['license_file']:
        disk_files.add(PurePosixPath(pkg['license_file']))
    if disk_files != packaged.keys():
        raise SystemExit(f'Missing or unexpected packaged notices for {crate}')
    for relative, data in packaged.items():
        path = source / relative
        if not path.resolve().is_relative_to(source) or not path.is_file() or path.read_bytes() != data:
            raise SystemExit(f'Packaged notice mismatch for {crate}: {relative}')

    if not packaged:
        if crate not in fallbacks:
            raise SystemExit(f'No notice files for {crate}')
        group, provenance = fallbacks[crate]
        vcs_path = source / '.cargo_vcs_info.json'
        if vcs_bytes is None or not vcs_path.is_file() or vcs_path.read_bytes() != vcs_bytes:
            raise SystemExit(f'Missing or invalid archived VCS metadata for {crate}')
        vcs = json.loads(vcs_bytes)
        origin, revision = group['origin'], group['revision']
        if (pkg['repository'] != provenance.get('repository', origin)
                or vcs['git'] != {'sha1': revision}
                or vcs['path_in_vcs'] != provenance['path_in_vcs']
                or not re.fullmatch(r'[a-f0-9]{40}', revision)):
            raise SystemExit(f'Upstream notice provenance mismatch for {crate}')
        if origin.startswith('https://github.com/'):
            permalink = f'{origin}/blob/{revision}'
        elif origin.startswith('https://codeberg.org/'):
            permalink = f'{origin}/src/commit/{revision}'
        else:
            raise SystemExit(f'Unsupported upstream notice origin for {crate}')
        for member, reference in group['notices'].items():
            relative = PurePosixPath(member)
            digest = reference['sha256']
            if (relative.name != member or not legal.match(member)
                    or not sha256.fullmatch(digest) or reference['url'] != f'{permalink}/{member}'):
                raise SystemExit(f'Invalid upstream notice reference for {crate}: {member}')
            path = fallback_dir / 'texts' / digest
            if not path.is_file():
                raise SystemExit(f'Missing upstream notice for {crate}: {member}')
            data = path.read_bytes()
            if hashlib.sha256(data).hexdigest() != digest:
                raise SystemExit(f'Upstream notice checksum mismatch for {crate}: {member}')
            packaged[relative] = data

    if not packaged:
        raise SystemExit(f'No notice files for {crate}')
    for relative, data in sorted(packaged.items()):
        try:
            content = data.decode('utf-8-sig')
        except UnicodeDecodeError:
            raise SystemExit(f'Invalid notice encoding for {crate}: {relative}') from None
        if not content.strip() or '\0' in content:
            raise SystemExit(f'Empty or invalid notice for {crate}: {relative}')
        dest = target / relative
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(data)

    files = ', '.join(path.as_posix() for path in sorted(packaged))
    license_name = pkg['license'] or 'See packaged license file'
    rows.append(f'{crate}\t{license_name}\t{files}\n')
    if mpl.search(license_name):
        url = f'https://static.crates.io/crates/{name}/{crate}.crate'
        sources.append(f'| {name} | {version} | {license_name} | <{url}> | `{checksum}` |\n')

(notices / 'INDEX.tsv').write_text('Crate\tDeclared license\tBundled notice files\n' + ''.join(rows))
(notices / 'UPSTREAM.json').write_bytes(fallback_manifest)
(bundle / 'README.md').write_text(
    '# Soulsteads Agent Messaging licensed default Linux binary\n\n'
    'This x86_64 GNU/Linux build includes the default features, including Matrix '
    'with end-to-end encryption and WhatsApp. See LICENSE, NOTICE, '
    'dependency-notices/INDEX.tsv, and MPL-SOURCE.md for licensing and source access.\n'
)
(bundle / 'MPL-SOURCE.md').write_text(
    '# Mozilla Public License source access\n\n'
    'The following dependencies contain source covered by the Mozilla Public License '
    '2.0. Their corresponding source is available at no charge in the exact crates.io '
    'archives below. This build uses these dependency sources unchanged. '
    'See the corresponding license texts in dependency-notices/.\n\n'
    f'Cargo.lock SHA-256: `{hashlib.sha256(lock_bytes).hexdigest()}`\n\n'
    '| Crate | Version | Declared license | Source archive | Archive SHA-256 |\n'
    '| --- | --- | --- | --- | --- |\n' + ''.join(sources)
)
PY
