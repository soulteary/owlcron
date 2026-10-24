"""Release archives must be self-contained and preserve required notices."""
import importlib.util
import io
import os
import subprocess
import sys
import json
from pathlib import Path
import tarfile
import tempfile
import unittest
from unittest import mock

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
                         'tools/release/README.md', 'tools/release/README.zh-CN.md',
                         'docs/release-notes.md', 'docs/zh-CN/release-notes.md', 'licenses/upstream/LICENSE'):
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
                for file in ('docs/release-notes.md', 'docs/zh-CN/release-notes.md'):
                    self.assertEqual(bundle.extractfile(prefix + file).read(), file.encode())
            with self.assertRaises(FileExistsError):
                PACKAGE.archive_distribution(root, archive.parent, 'v0.1.0', 'b' * 40, 'linux-amd64', 'go1.27.3')

    def test_export_uses_committed_source_and_excludes_ignored_secrets(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / 'repo'
            root.mkdir()
            subprocess.run(['git', 'init', '-q', str(root)], check=True)
            (root / '.gitignore').write_text('htdocs/.env\n')
            (root / 'config.json').write_text('committed example')
            (root / 'htdocs').mkdir()
            (root / 'htdocs/index.html').write_text('committed frontend')
            for name in ('owlcron-engine', 'owlcron-script', 'LICENSE', 'THIRD_PARTY_NOTICES.md',
                         'THIRD_PARTY_NOTICES.zh-CN.md', 'bin/echo-plugin/echo-plugin',
                         'tools/release/README.md', 'tools/release/README.zh-CN.md',
                         'docs/release-notes.md', 'docs/zh-CN/release-notes.md', 'licenses/LICENSE'):
                file = root / name
                file.parent.mkdir(parents=True, exist_ok=True)
                file.write_text(name)
            subprocess.run(['git', 'add', '.'], cwd=root, check=True)
            subprocess.run(['git', '-c', 'user.name=Release Test', '-c', 'user.email=release@example.invalid',
                            '-c', 'commit.gpgsign=false', 'commit', '-qm', 'fixture'], cwd=root, check=True)
            (root / 'config.json').write_text('LOCAL_CONFIG_SECRET')
            (root / 'htdocs/.env').write_text('IGNORED_SECRET')
            (root / 'htdocs/private-key.pem').write_text('UNTRACKED_SECRET')
            destination = Path(directory) / 'export'
            destination.mkdir()
            source = PACKAGE.export_source(root, destination, 'HEAD')
            self.assertEqual((source / 'config.json').read_text(), 'committed example')
            self.assertEqual((source / 'htdocs/index.html').read_text(), 'committed frontend')
            self.assertFalse((source / 'htdocs/.env').exists())
            self.assertFalse((source / 'htdocs/private-key.pem').exists())
            archive, _ = PACKAGE.archive_distribution(source, Path(directory) / 'output',
                'v0.1.0', 'a' * 40, 'linux-amd64', 'go1.27.3')
            with tarfile.open(archive) as bundle:
                content = b'\n'.join(bundle.extractfile(member).read() for member in bundle if member.isfile())
            for secret in (b'LOCAL_CONFIG_SECRET', b'IGNORED_SECRET', b'UNTRACKED_SECRET'):
                self.assertNotIn(secret, content)

    def test_failed_archive_creation_leaves_no_final_package(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / 'source'
            root.mkdir()
            for name in ('owlcron-engine', 'owlcron-script', 'LICENSE', 'THIRD_PARTY_NOTICES.md',
                         'THIRD_PARTY_NOTICES.zh-CN.md', 'config.json', 'bin/echo-plugin/echo-plugin',
                         'tools/release/README.md', 'tools/release/README.zh-CN.md',
                         'docs/release-notes.md', 'docs/zh-CN/release-notes.md', 'licenses/LICENSE',
                         'htdocs/index.html'):
                file = root / name
                file.parent.mkdir(parents=True, exist_ok=True)
                file.write_text(name)
            output = Path(directory) / 'out'
            with mock.patch.object(tarfile.TarFile, 'add', side_effect=OSError('injected failure')):
                with self.assertRaisesRegex(OSError, 'injected failure'):
                    PACKAGE.archive_distribution(root, output, 'v0.1.0', 'a' * 40, 'linux-amd64', 'go1.27.3')
            self.assertEqual(list(output.iterdir()), [])

    def test_verifier_rejects_bad_checksums_and_unsafe_tar_even_when_optimized(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            marker = root / 'should-not-execute'
            archive = root / 'owlcron-test.tar.gz'
            cases = [('checksum', None), ('../' + root.name + '/escaped', tarfile.REGTYPE),
                     (str(root / 'absolute'), tarfile.REGTYPE), ('owlcron-test/../../' + root.name + '/escaped', tarfile.REGTYPE),
                     ('owlcron-test/link', tarfile.SYMTYPE), ('owlcron-test/hardlink', tarfile.LNKTYPE),
                     ('owlcron-test/device', tarfile.CHRTYPE)]
            for name, kind in cases:
                with self.subTest(name=name):
                    with tarfile.open(archive, 'w:gz') as bundle:
                        payload = ('#!/bin/sh\ntouch ' + str(marker) + '\n').encode()
                        entry = tarfile.TarInfo('owlcron-test/owlcron-engine')
                        entry.size, entry.mode = len(payload), 0o755
                        bundle.addfile(entry, io.BytesIO(payload))
                        if kind is not None:
                            entry = tarfile.TarInfo(name)
                            entry.type = kind
                            entry.linkname = str(marker)
                            bundle.addfile(entry)
                    digest = '0' * 64 if kind is None else PACKAGE.sha256(archive)
                    archive.with_suffix('.gz.sha256').write_text(f'{digest}  {archive.name}\n')
                    for optimized in (False, True):
                        command = [sys.executable] + (['-O'] if optimized else [])
                        result = subprocess.run(command + [str(ROOT / 'tools/verify_release.py'), str(archive)],
                            env=dict(os.environ, PYTHONOPTIMIZE='1'), capture_output=True, text=True)
                        self.assertNotEqual(result.returncode, 0)
                        self.assertIn('ValueError', result.stderr)
                        self.assertFalse(marker.exists(), 'Untrusted archive executable ran')
                        self.assertFalse((root / 'escaped').exists())
                        self.assertFalse((root / 'absolute').exists())

    def test_make_does_not_interpolate_version_as_shell_code(self):
        with tempfile.TemporaryDirectory() as directory:
            marker = Path(directory) / 'injected'
            for version in ('v0.1.0`touch ' + str(marker) + '`',
                            'v0.1.0$(shell touch ' + str(marker) + ')'):
                result = subprocess.run(['make', 'release', 'VERSION=' + version], cwd=ROOT,
                                        capture_output=True, text=True)
                self.assertNotEqual(result.returncode, 0)
                self.assertIn('version must use', result.stderr)
                self.assertFalse(marker.exists())

    def test_published_identity_is_checked_before_executing_the_archive(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            archive = root / 'owlcron-test.tar.gz'
            marker = root / 'must-not-execute'
            metadata = json.dumps(dict(version='v0.1.0', commit='a' * 40, target='linux-amd64')).encode()
            with tarfile.open(archive, 'w:gz') as bundle:
                for name, data, mode in [('BUILD.json', metadata, 0o644),
                                         ('owlcron-engine', ('#!/bin/sh\ntouch ' + str(marker) + '\n').encode(), 0o755)]:
                    entry = tarfile.TarInfo('owlcron-test/' + name)
                    entry.size, entry.mode = len(data), mode
                    bundle.addfile(entry, io.BytesIO(data))
            archive.with_suffix('.gz.sha256').write_text(f'{PACKAGE.sha256(archive)}  {archive.name}\n')
            for flag, value, expected in [('--expected-version', 'v0.2.0', 'Unexpected published version'),
                                           ('--expected-commit', 'b' * 40, 'Unexpected published source commit')]:
                result = subprocess.run([sys.executable, str(ROOT / 'tools/verify_release.py'), str(archive), flag, value],
                                        capture_output=True, text=True)
                self.assertNotEqual(result.returncode, 0)
                self.assertIn(expected, result.stderr)
                self.assertFalse(marker.exists())


if __name__ == '__main__':
    unittest.main()
