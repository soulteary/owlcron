#!/usr/bin/env python3
"""Build and verify a native release archive without publishing or creating tags."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import tarfile
import tempfile

ROOT = Path(__file__).resolve().parents[1]


def sha256(path):
    digest = hashlib.sha256()
    with path.open('rb') as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b''):
            digest.update(block)
    return digest.hexdigest()


def archive_distribution(root, output, version, commit, target, toolchain):
    name = f'owlcron-{version}-{target}'
    output.mkdir(parents=True, exist_ok=True)
    archive = output / f'{name}.tar.gz'
    if archive.exists():
        raise FileExistsError(f'Release archive already exists: {archive}')
    with tempfile.TemporaryDirectory(prefix='owlcron-release-') as directory:
        package = Path(directory) / name
        package.mkdir()
        for file in ('owlcron-engine', 'owlcron-script', 'LICENSE', 'THIRD_PARTY_NOTICES.md',
                     'THIRD_PARTY_NOTICES.zh-CN.md', 'config.json'):
            shutil.copy2(root / file, package / file)
        for file in ('README.md', 'README.zh-CN.md'):
            shutil.copy2(root / 'tools/release' / file, package / file)
        (package / 'bin/echo-plugin').mkdir(parents=True)
        shutil.copy2(root / 'bin/echo-plugin/echo-plugin', package / 'bin/echo-plugin/echo-plugin')
        # Frontend builds use symlinks into node_modules; archives must stand alone.
        for folder in ('htdocs', 'licenses'):
            shutil.copytree(root / folder, package / folder, symlinks=False)
        metadata = dict(version=version, commit=commit, target=target, go=toolchain)
        (package / 'BUILD.json').write_text(json.dumps(metadata, indent=2) + '\n')
        with tarfile.open(archive, 'w:gz', dereference=True) as bundle:
            bundle.add(package, arcname=name)
    checksum = sha256(archive)
    manifest = archive.with_suffix(archive.suffix + '.sha256')
    manifest.write_text(f'{checksum}  {archive.name}\n')
    return archive, manifest


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--version', required=True)
    parser.add_argument('--output', type=Path, default=ROOT / 'dist')
    args = parser.parse_args()
    if not re.fullmatch(r'v\d+\.\d+\.\d+(?:-[0-9A-Za-z.-]+)?', args.version):
        parser.error('version must use vMAJOR.MINOR.PATCH with an optional prerelease suffix')
    commit = subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=ROOT, text=True).strip()
    if subprocess.check_output(['git', 'status', '--porcelain'], cwd=ROOT, text=True).strip():
        commit += '-dirty'
    toolchain, target_os, target_arch, host_os, host_arch = subprocess.check_output(
        ['go', 'env', 'GOVERSION', 'GOOS', 'GOARCH', 'GOHOSTOS', 'GOHOSTARCH'], text=True).split()
    if not toolchain.startswith('go1.27.'):
        parser.error('release builds require the supported Go 1.27 toolchain')
    if (target_os, target_arch) != (host_os, host_arch):
        parser.error('release packaging validates native binaries; cross-compilation is not supported')
    env = dict(os.environ, CGO_ENABLED='0')
    flags = (f'-X github.com/soulteary/owlcron/cli.Version={args.version} '
             f'-X github.com/soulteary/owlcron/cli.Commit={commit}')
    subprocess.run(['make', 'build', 'frontend', 'GO_BUILD_FLAGS=-trimpath', f'LDFLAGS={flags}'], cwd=ROOT, env=env, check=True)
    actual = subprocess.check_output([str(ROOT / 'owlcron-engine'), 'version'], cwd=ROOT, text=True).strip()
    expected = f'OwlCron {args.version} (commit {commit})'
    if actual != expected:
        raise RuntimeError(f'Build metadata mismatch: {actual!r}')
    archive, manifest = archive_distribution(ROOT, args.output.resolve(), args.version,
                                              commit, f'{target_os}-{target_arch}', toolchain)
    print(f'Built {archive}\nSHA256 manifest: {manifest}')


if __name__ == '__main__':
    main()
