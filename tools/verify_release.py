#!/usr/bin/env python3
"""Verify a locally built archive and smoke-test its extracted distribution."""
import argparse
import importlib
import json
import os
from pathlib import Path
import subprocess
import sys
import tarfile
import tempfile
from package_release import sha256

ROOT = Path(__file__).resolve().parents[1]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('archive', type=Path)
    args = parser.parse_args()
    archive = args.archive.resolve()
    expected = archive.with_suffix(archive.suffix + '.sha256').read_text().strip()
    assert expected == f'{sha256(archive)}  {archive.name}', 'SHA256 mismatch'
    with tempfile.TemporaryDirectory(prefix='owlcron-verify-') as directory:
        root = Path(directory)
        with tarfile.open(archive) as bundle:
            # Release archives contain regular files/directories, never links.
            for member in bundle.getmembers():
                assert member.isfile() or member.isdir(), 'Unexpected archive entry type'
                assert Path(member.name).parts[0] == archive.name.removesuffix('.tar.gz'), 'Unexpected archive root'
                assert not Path(member.name).is_absolute() and '..' not in Path(member.name).parts, 'Unsafe archive path'
            bundle.extractall(root)
        distribution = root / archive.name.removesuffix('.tar.gz')
        metadata = json.loads((distribution / 'BUILD.json').read_text())
        env = {key:value for key,value in os.environ.items() if not key.startswith('OWLCRON_')}
        before = set(distribution.iterdir())
        version = subprocess.check_output([str(distribution / 'owlcron-engine'), 'version'], cwd=distribution, env=env, text=True).strip()
        assert version == f'OwlCron {metadata["version"]} (commit {metadata["commit"]})', 'Version mismatch'
        assert before == set(distribution.iterdir()), 'Version command created runtime files'
        adapter = subprocess.run([str(distribution / 'owlcron-script'), '--', '/bin/echo', 'RELEASE_ADAPTER_OK'], input='{}\n', text=True, capture_output=True, check=True)
        assert json.loads(adapter.stdout) == {'complete':True, 'code':0}
        assert 'RELEASE_ADAPTER_OK' in adapter.stderr
        sys.path.insert(0, str(ROOT / 'tests'))
        smoke = importlib.import_module('smoke_install')
        smoke.ROOT = distribution
        smoke.main()
        print(f'Release verified: checksum, metadata, adapter and installed application ({metadata["target"]})')


if __name__ == '__main__':
    main()
