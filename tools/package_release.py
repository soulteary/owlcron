#!/usr/bin/env python3
"""Build and verify a native release archive without publishing or creating tags."""
import argparse
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import shutil
import subprocess
import tarfile
import tempfile

ROOT = Path(__file__).resolve().parents[1]


def require(condition, message):
    if not condition:
        raise ValueError(message)


def safe_extract(bundle, root, expected_root):
    # Validate every entry before writing any file. Do not rely on version-
    # dependent tarfile extraction filters or optimizable assert statements.
    members = bundle.getmembers()
    seen = set()
    for member in members:
        parts = member.name.split('/')
        require(member.isfile() or member.isdir(), 'Unexpected archive entry type')
        require(not PurePosixPath(member.name).is_absolute() and
                '\\' not in member.name and all(part not in ('', '.', '..') for part in parts),
                'Unsafe archive path')
        require(parts[0] == expected_root, 'Unexpected archive root')
        require(member.name not in seen, 'Duplicate archive path')
        require(len(parts) > 1 or member.isdir(), 'Archive root must be a directory')
        seen.add(member.name)
    for member in members:
        destination = root.joinpath(*member.name.split('/'))
        if member.isdir():
            destination.mkdir(parents=True, exist_ok=True)
        else:
            destination.parent.mkdir(parents=True, exist_ok=True)
            with bundle.extractfile(member) as source, destination.open('xb') as output:
                shutil.copyfileobj(source, output)
            # Preserve executable bits, without owner/group or special modes.
            destination.chmod(member.mode & 0o755)


def sha256(path):
    digest = hashlib.sha256()
    with path.open('rb') as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b''):
            digest.update(block)
    return digest.hexdigest()


def export_source(repository, destination, commit):
    archive = destination / 'source.tar'
    subprocess.run(['git', 'archive', '--format=tar', '--prefix=source/',
                    '--output', str(archive), commit], cwd=repository, check=True)
    with tarfile.open(archive) as bundle:
        safe_extract(bundle, destination, 'source')
    archive.unlink()
    return destination / 'source'


def archive_distribution(root, output, version, commit, target, toolchain):
    name = f'owlcron-{version}-{target}'
    output.mkdir(parents=True, exist_ok=True)
    archive = output / f'{name}.tar.gz'
    manifest = archive.with_suffix(archive.suffix + '.sha256')
    if archive.exists() or manifest.exists():
        raise FileExistsError(f'Release output already exists: {archive}')
    with tempfile.TemporaryDirectory(prefix='.owlcron-release-', dir=output) as directory:
        package = Path(directory) / name
        package.mkdir()
        for file in ('owlcron-engine', 'owlcron-script', 'LICENSE', 'THIRD_PARTY_NOTICES.md',
                     'THIRD_PARTY_NOTICES.zh-CN.md', 'config.json'):
            shutil.copy2(root / file, package / file)
        for file in ('README.md', 'README.zh-CN.md'):
            shutil.copy2(root / 'tools/release' / file, package / file)
        for file in ('docs/release-notes.md', 'docs/zh-CN/release-notes.md'):
            destination = package / file
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(root / file, destination)
        (package / 'bin/echo-plugin').mkdir(parents=True)
        shutil.copy2(root / 'bin/echo-plugin/echo-plugin', package / 'bin/echo-plugin/echo-plugin')
        # Frontend builds use symlinks into node_modules; archives must stand alone.
        for folder in ('htdocs', 'licenses'):
            shutil.copytree(root / folder, package / folder, symlinks=False)
        metadata = dict(version=version, commit=commit, target=target, go=toolchain)
        (package / 'BUILD.json').write_text(json.dumps(metadata, indent=2) + '\n')
        staged_archive = Path(directory) / archive.name
        staged_manifest = Path(directory) / manifest.name
        with tarfile.open(staged_archive, 'w:gz', dereference=True) as bundle:
            bundle.add(package, arcname=name)
        staged_manifest.write_text(f'{sha256(staged_archive)}  {archive.name}\n')
        # Publish complete files without overwriting existing release outputs.
        os.link(staged_archive, archive)
        try:
            os.link(staged_manifest, manifest)
        except BaseException:
            archive.unlink()
            raise
    return archive, manifest


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--version', default=os.environ.get('OWLCRON_RELEASE_VERSION', ''))
    parser.add_argument('--output', type=Path, default=Path(os.environ.get('OWLCRON_RELEASE_OUTPUT', str(ROOT / 'dist'))))
    args = parser.parse_args()
    if not re.fullmatch(r'v\d+\.\d+\.\d+(?:-[0-9A-Za-z.-]+)?', args.version):
        parser.error('version must use vMAJOR.MINOR.PATCH with an optional prerelease suffix')
    commit = subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=ROOT, text=True).strip()
    # Build only committed files; ignored files and local config edits must never
    # enter published assets or be mislabeled as this source commit.
    with tempfile.TemporaryDirectory(prefix='owlcron-source-') as directory:
        source = export_source(ROOT, Path(directory), commit)
        toolchain, target_os, target_arch, host_os, host_arch = subprocess.check_output(
            ['go', 'env', 'GOVERSION', 'GOOS', 'GOARCH', 'GOHOSTOS', 'GOHOSTARCH'], cwd=source, text=True).split()
        if not toolchain.startswith('go1.27.'):
            parser.error('release builds require the supported Go 1.27 toolchain')
        if (target_os, target_arch) != (host_os, host_arch):
            parser.error('release packaging validates native binaries; cross-compilation is not supported')
        env = dict(os.environ, CGO_ENABLED='0')
        flags = (f'-X github.com/soulteary/owlcron/cli.Version={args.version} '
                 f'-X github.com/soulteary/owlcron/cli.Commit={commit}')
        subprocess.run(['bun', 'install', '--cwd', 'htdocs-deps', '--frozen-lockfile'], cwd=source, env=env, check=True)
        subprocess.run(['make', 'build', 'frontend', 'GO_BUILD_FLAGS=-trimpath', f'LDFLAGS={flags}'], cwd=source, env=env, check=True)
        actual = subprocess.check_output([str(source / 'owlcron-engine'), 'version'], cwd=source, text=True).strip()
        expected = f'OwlCron {args.version} (commit {commit})'
        if actual != expected:
            raise RuntimeError(f'Build metadata mismatch: {actual!r}')
        archive, manifest = archive_distribution(source, args.output.resolve(), args.version,
                                                  commit, f'{target_os}-{target_arch}', toolchain)
    print(f'Built {archive}\nSHA256 manifest: {manifest}')


if __name__ == '__main__':
    main()
