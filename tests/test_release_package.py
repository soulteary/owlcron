"""Release archives must be self-contained and preserve required notices."""
import importlib.util
import json
from pathlib import Path
import tarfile
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location('package_release', ROOT / 'tools/package_release.py')
PACKAGE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(PACKAGE)


class ReleasePackageTests(unittest.TestCase):
    def test_archive_materializes_assets_and_preserves_metadata_and_notices(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / 'source'
            root.mkdir()
            for name in ('owlcron-engine', 'owlcron-script', 'LICENSE', 'THIRD_PARTY_NOTICES.md',
                         'THIRD_PARTY_NOTICES.zh-CN.md', 'config.json', 'bin/echo-plugin/echo-plugin',
                         'tools/release/README.md', 'tools/release/README.zh-CN.md', 'licenses/upstream/LICENSE'):
                path = root / name
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text(name)
            (root / 'htdocs').mkdir()
            (root / 'dependency.js').write_text('external asset')
            (root / 'htdocs/asset.js').symlink_to(root / 'dependency.js')
            archive, checksum = PACKAGE.archive_distribution(root, Path(directory) / 'out',
                'v0.1.0', 'a' * 40, 'linux-amd64', 'go1.27.3')
            self.assertEqual(checksum.read_text(), f'{PACKAGE.sha256(archive)}  {archive.name}\n')
            with tarfile.open(archive) as bundle:
                self.assertFalse(any(member.issym() or member.islnk() for member in bundle))
                prefix = 'owlcron-v0.1.0-linux-amd64/'
                self.assertEqual(bundle.extractfile(prefix + 'htdocs/asset.js').read(), b'external asset')
                self.assertEqual(bundle.extractfile(prefix + 'licenses/upstream/LICENSE').read(), b'licenses/upstream/LICENSE')
                metadata = json.load(bundle.extractfile(prefix + 'BUILD.json'))
                self.assertEqual(metadata, dict(version='v0.1.0', commit='a' * 40, target='linux-amd64', go='go1.27.3'))
                for file in ('owlcron-engine', 'owlcron-script', 'bin/echo-plugin/echo-plugin', 'config.json'):
                    self.assertIsNotNone(bundle.getmember(prefix + file))
            with self.assertRaises(FileExistsError):
                PACKAGE.archive_distribution(root, archive.parent, 'v0.1.0', 'b' * 40, 'linux-amd64', 'go1.27.3')


if __name__ == '__main__':
    unittest.main()
