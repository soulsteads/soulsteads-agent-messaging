#!/usr/bin/env python3
"""Turn the locally built Linux release bundle into a Debian binary package."""
import argparse
import os
from pathlib import Path
import re
import shutil
import subprocess
import tempfile
import tomllib

PACKAGE = 'soulsteads-agent-messaging'
BINARY = 'soulsteads-messaging'
EXTRA = ['MPL-SOURCE.md']
DESCRIPTION = 'Messaging gateway for local agent harnesses'
ROOT = Path(__file__).resolve().parents[1]

# Debian 13/Ubuntu 24.04 package names for the native GNU/Linux ABI. Stop
# rather than guessing if the release binary gains a new library dependency.
SONAMES = {
    'ld-linux-x86-64.so.2': 'libc6',
    'libc.so.6': 'libc6',
    'libdl.so.2': 'libc6',
    'libm.so.6': 'libc6',
    'libpthread.so.0': 'libc6',
    'libresolv.so.2': 'libc6',
    'librt.so.1': 'libc6',
    'libatomic.so.1': 'libatomic1',
    'libgcc_s.so.1': 'libgcc-s1',
    'libsqlite3.so.0': 'libsqlite3-0',
    'libssl.so.3': 'libssl3t64',
    'libcrypto.so.3': 'libssl3t64',
    'libstdc++.so.6': 'libstdc++6',
    'libz.so.1': 'zlib1g',
    'libzstd.so.1': 'libzstd1',
}


def elf_dependencies(binary):
    elf = subprocess.check_output(
        ['readelf', '-WhdV', binary], text=True, env={**os.environ, 'LC_ALL': 'C'},
    )
    if not re.search(r'^\s*Machine:\s+Advanced Micro Devices X86-64\s*$', elf, re.M):
        raise SystemExit('Expected the amd64 GNU/Linux release executable')
    needed = set(re.findall(r'\(NEEDED\).*?\[(.*?)\]', elf))
    if unknown := needed - SONAMES.keys():
        raise SystemExit('Unknown ELF dependencies: ' + ', '.join(sorted(unknown)))
    packages = {SONAMES[soname] for soname in needed}
    for prefix, package in [('GLIBC', 'libc6'), ('OPENSSL', 'libssl3t64')]:
        versions = re.findall(rf'\b{prefix}_([0-9]+(?:\.[0-9]+)+)\b', elf)
        if prefix == 'GLIBC' and 'GLIBC_ABI_DT_RELR' in elf:
            versions.append('2.36')
        if versions and package in packages:
            floor = max(versions, key=lambda v: tuple(map(int, v.split('.'))))
            packages.remove(package)
            packages.add(f'{package} (>= {floor})')
    return ', '.join(sorted(packages | {'ca-certificates'}))


def regular(path):
    if path.is_symlink() or not path.is_file() or not path.stat().st_size:
        raise SystemExit(f'Missing, empty, or invalid release file: {path.name}')
    return path


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('bundle', type=Path, help='staged release bundle directory')
    parser.add_argument('output', type=Path, help='directory for the .deb')
    args = parser.parse_args()
    bundle = args.bundle
    binary = regular(bundle / BINARY)
    depends = elf_dependencies(binary)
    version = tomllib.loads((ROOT / 'Cargo.toml').read_text())['package']['version']
    # Cargo SemVer prereleases must sort before the final Debian version.
    upstream, plus, build = version.partition('+')
    version = upstream.replace('-', '~', 1) + plus + build + '-1'
    notices = bundle / 'dependency-notices'
    if notices.is_symlink():
        raise SystemExit('The release notices directory must not be a symlink')
    regular(notices / 'INDEX.tsv')
    for path in notices.rglob('*'):
        if path.is_symlink() or not (path.is_dir() or path.is_file()):
            raise SystemExit('Unexpected entry in the release dependency notices')

    args.output.mkdir(parents=True, exist_ok=True)
    output = args.output.resolve() / f'{PACKAGE}_{version}_amd64.deb'
    with tempfile.TemporaryDirectory(dir=output.parent, prefix='.deb-build-') as temp:
        work = Path(temp)
        stage = work / 'pkg'
        control = stage / 'DEBIAN'
        bindir = stage / 'usr/bin'
        docs = stage / 'usr/share/doc' / PACKAGE
        for directory in [control, bindir, docs]:
            directory.mkdir(parents=True)
        shutil.copyfile(binary, bindir / BINARY)
        (bindir / BINARY).chmod(0o755)
        for name, dest in [('LICENSE', 'copyright'), ('NOTICE', 'NOTICE'), *[(n, n) for n in EXTRA]]:
            shutil.copyfile(regular(bundle / name), docs / dest)
        shutil.copytree(notices, docs / notices.name, copy_function=shutil.copyfile)
        (control / 'control').write_text(
            f'Package: {PACKAGE}\nVersion: {version}\nArchitecture: amd64\n'
            f'Section: utils\nPriority: optional\nMaintainer: Soulsteads\n'
            f'Depends: {depends}\nHomepage: https://github.com/soulsteads/{PACKAGE}\n'
            f'Description: {DESCRIPTION}\n',
        )
        for path in [stage, *stage.rglob('*')]:
            path.chmod(0o755 if path.is_dir() or path == bindir / BINARY else 0o644)
        candidate = work / output.name
        subprocess.run(['dpkg-deb', '--root-owner-group', '-Zxz', '--build', stage, candidate], check=True)
        candidate.replace(output)
    print(output)


if __name__ == '__main__':
    main()
