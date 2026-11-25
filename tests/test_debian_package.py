"""Exercise the Debian packager against a minimal native release bundle."""
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]
PACKAGE = 'soulsteads-agent-messaging'
BINARY = 'soulsteads-messaging'
EXTRA = ['MPL-SOURCE.md']


@unittest.skipUnless(
    all(shutil.which(tool) for tool in ('cc', 'dpkg', 'dpkg-deb', 'readelf')),
    'requires cc, dpkg, dpkg-deb and readelf',
)
class DebianPackageTests(unittest.TestCase):
    def setUp(self):
        self.root = Path(self.enterContext(tempfile.TemporaryDirectory()))
        self.bundle = self.root / 'bundle'
        self.bundle.mkdir()
        source = self.root / 'main.c'
        source.write_text('int main(void) { return 0; }\n')
        subprocess.run(['cc', str(source), '-o', str(self.bundle / BINARY)], check=True)
        (self.bundle / 'not-for-installation').write_text('fixture\n')
        for name in ['LICENSE', 'NOTICE', *EXTRA]:
            (self.bundle / name).write_text(f'{name} fixture\n')
        self.notices = self.bundle / 'dependency-notices'
        self.notices.mkdir()
        (self.notices / 'INDEX.tsv').write_text('fixture notice index\n')

    def pack_command(self, script, output):
        return ['python3', '-B', str(script), str(self.bundle), str(output)]

    def build_package(self, command):
        result = subprocess.run(command, cwd=self.root, check=True, text=True, stdout=subprocess.PIPE)
        return Path(result.stdout.strip().splitlines()[-1])

    def test_packages_only_the_production_elf_and_release_notices(self):
        root, bundle, notices = self.root, self.bundle, self.notices
        script = ROOT / 'scripts/package-deb.py'
        package = self.build_package(self.pack_command(script, root / 'output'))
        version = subprocess.check_output(['dpkg-deb', '-f', package, 'Version'], text=True).strip()
        self.assertEqual(package.name, f'{PACKAGE}_{version}_amd64.deb')
        self.assertEqual(subprocess.check_output(['dpkg-deb', '-f', package, 'Architecture'], text=True).strip(), 'amd64')
        self.assertIn('libc6 (>= ', subprocess.check_output(['dpkg-deb', '-f', package, 'Depends'], text=True))
        extracted = root / 'extracted'
        subprocess.run(['dpkg-deb', '-x', package, extracted], check=True)
        self.assertEqual(sorted(p.name for p in (extracted / 'usr/bin').iterdir()), [BINARY])
        subprocess.run([extracted / 'usr/bin' / BINARY], cwd=root, env={}, check=True)
        docs = extracted / 'usr/share/doc' / PACKAGE
        self.assertEqual((docs / 'copyright').read_bytes(), (bundle / 'LICENSE').read_bytes())
        self.assertEqual((docs / 'NOTICE').read_bytes(), (bundle / 'NOTICE').read_bytes())
        self.assertEqual((docs / 'dependency-notices/INDEX.tsv').read_bytes(), (notices / 'INDEX.tsv').read_bytes())
        for name in EXTRA:
            self.assertEqual((docs / name).read_bytes(), (bundle / name).read_bytes())
        control = root / 'control'
        subprocess.run(['dpkg-deb', '-e', package, control], check=True)
        self.assertEqual(os.listdir(control), ['control'])
        (notices / 'INDEX.tsv').unlink()
        blocked = subprocess.run(self.pack_command(script, root / 'bad-output'), cwd=root, capture_output=True)
        self.assertNotEqual(blocked.returncode, 0)
        self.assertFalse(list((root / 'bad-output').glob('*.deb')))

    def test_prerelease_packages_sort_before_the_final_release(self):
        # Run the real script with fixture manifests without editing the checkout.
        script = self.root / 'scripts/package-deb.py'
        script.parent.mkdir()
        shutil.copyfile(ROOT / 'scripts/package-deb.py', script)
        manifest = self.root / 'Cargo.toml'
        output = self.root / 'output'
        final = '1.2.3-1'
        for cargo, expected in (
            ('1.2.3', final),
            ('1.2.3-rc.1', '1.2.3~rc.1-1'),
            ('1.2.3-rc.1+build-7', '1.2.3~rc.1+build-7-1'),
        ):
            with self.subTest(cargo=cargo):
                manifest.write_text(f'[package]\nversion = "{cargo}"\n')
                package = self.build_package(self.pack_command(script, output))
                version = subprocess.check_output(['dpkg-deb', '-f', package, 'Version'], text=True).strip()
                self.assertEqual(version, expected)
                self.assertEqual(package.name, f'{PACKAGE}_{expected}_amd64.deb')
                if expected != final:
                    subprocess.run(['dpkg', '--compare-versions', version, 'lt', final], check=True)


if __name__ == '__main__':
    unittest.main()
