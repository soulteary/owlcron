#!/usr/bin/env python3
"""Verify a locally built archive and smoke-test its extracted distribution."""
import argparse
import json
import os
from pathlib import Path
import subprocess
import sys
import tarfile
import tempfile
from package_release import require, safe_extract, sha256

ROOT = Path(__file__).resolve().parents[1]



def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('archive', type=Path)
    args = parser.parse_args()
    archive = args.archive.resolve()
    require(archive.name.endswith('.tar.gz'), 'Expected a .tar.gz archive')
    expected_root = archive.name.removesuffix('.tar.gz')
    expected = archive.with_suffix(archive.suffix + '.sha256').read_text().strip()
    require(expected == f'{sha256(archive)}  {archive.name}', 'SHA256 mismatch')
    with tempfile.TemporaryDirectory(prefix='owlcron-verify-') as directory:
        root = Path(directory)
        with tarfile.open(archive) as bundle:
            safe_extract(bundle, root, expected_root)
        distribution = root / expected_root
        metadata = json.loads((distribution / 'BUILD.json').read_text())
        env = {key:value for key,value in os.environ.items() if not key.startswith('OWLCRON_')}
        before = set(distribution.iterdir())
        version = subprocess.check_output([str(distribution / 'owlcron-engine'), 'version'], cwd=distribution, env=env, text=True).strip()
        require(version == f'OwlCron {metadata["version"]} (commit {metadata["commit"]})', 'Version mismatch')
        require(before == set(distribution.iterdir()), 'Version command created runtime files')
        adapter = subprocess.run([str(distribution / 'owlcron-script'), '--', '/bin/echo', 'RELEASE_ADAPTER_OK'], input='{}\n', text=True, capture_output=True, check=True)
        require(json.loads(adapter.stdout) == {'complete':True, 'code':0}, 'Adapter completion mismatch')
        require('RELEASE_ADAPTER_OK' in adapter.stderr, 'Adapter output missing')
        # The harness itself needs Python; the extracted application runs with
        # an empty PATH and cannot fall back to developer toolchains.
        env.pop('PYTHONOPTIMIZE', None)
        subprocess.run([sys.executable, str(ROOT / 'tests/release_acceptance.py'), str(distribution)],
                       env=env, check=True, timeout=150)
        print(f'Release verified: checksum, metadata, adapter and installed application ({metadata["target"]})')


if __name__ == '__main__':
    main()
