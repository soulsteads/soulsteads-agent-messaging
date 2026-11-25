"""Exercise the Debian packager against a minimal native release bundle."""
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import tomllib
import unittest

ROOT = Path(__file__).resolve().parents[1]
PACKAGE = 'soulsteads-agent-messaging'
BINARY = 'soulsteads-messaging'
EXTRA = ['MPL-SOURCE.md']


@unittest.skipUnless(shutil.which('dpkg-deb') and shutil.which('cc'), 'requires dpkg-deb and a native compiler')
class DebianPackageTests(unittest.TestCase):
    def test_packages_only_the_production_elf_and_release_notices(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            bundle, output = root / 'bundle', root / 'output'
            bundle.mkdir()
            source = root / 'main.c'
            source.write_text('int main(void) { return 0; }\n')
            subprocess.run(['cc', str(source), '-o', str(bundle / BINARY)], check=True)
            (bundle / 'not-for-installation').write_text('fixture\n')
            for name in ['LICENSE', 'NOTICE', *EXTRA]:
                (bundle / name).write_text(f'{name} fixture\n')
            notices = bundle / 'dependency-notices'
            notices.mkdir()
            (notices / 'INDEX.tsv').write_text('fixture notice index\n')
            pack = ['python3', '-B', str(ROOT / 'scripts/package-deb.py'), str(bundle), str(output)]
            self.assertTrue((ROOT / 'scripts/package-deb.py').is_file(), 'Debian packager is missing')
            result = subprocess.run(pack, cwd=root, check=True, text=True, capture_output=True)
            package = Path(result.stdout.strip().splitlines()[-1])
            version = tomllib.loads((ROOT / 'Cargo.toml').read_text())['package']['version'] + '-1'
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
            self.assertFalse(set(os.listdir(control)) & {'preinst', 'postinst', 'prerm', 'postrm', 'triggers'})
            (notices / 'INDEX.tsv').unlink()
            blocked = subprocess.run(pack[:-1] + [str(root / 'bad-output')], cwd=root, capture_output=True)
            self.assertNotEqual(blocked.returncode, 0)
            self.assertFalse(list((root / 'bad-output').glob('*.deb')))


if __name__ == '__main__':
    unittest.main()
