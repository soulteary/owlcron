#!/usr/bin/env python3
"""Refresh (--write) or verify license inventory against locked dependencies."""
import argparse
import json
from pathlib import Path
import re
import shutil

ROOT = Path(__file__).resolve().parent.parent


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--write', action='store_true')
    args = parser.parse_args()
    lock = json.loads((ROOT / 'htdocs-deps/package-lock.json').read_text())
    rows = []
    copies = {}
    for package, entry in sorted(lock['packages'].items()):
        if not package:
            continue
        source = ROOT / 'htdocs-deps' / package
        metadata = json.loads((source / 'package.json').read_text())
        assert metadata['version'] == entry['version'], package
        destination = Path('licenses/npm') / (metadata['name'].replace('/', '__') + '@' + metadata['version'])
        files = []
        for file in sorted(source.iterdir()):
            if file.is_file() and re.match(r'(?i)^(licen[sc]e|copying|notice|copyright|readme)', file.name):
                files.append(file.name)
                copies[destination / file.name] = file.read_bytes()
        if not files:
            raise RuntimeError(f'No upstream license or notice material for {package}')
        rows.append(dict(name=metadata['name'], version=metadata['version'],
                         license=metadata.get('license', metadata.get('licenses', 'See bundled notices')),
                         notices=str(destination), files=files))
    manifest = ROOT / 'licenses/npm-inventory.json'
    if args.write:
        # Replace only the generated npm notice tree; preserve manually sourced notices.
        shutil.rmtree(ROOT / 'licenses/npm', ignore_errors=True)
        for path, content in copies.items():
            target = ROOT / path
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(content)
        manifest.write_text(json.dumps(rows, indent=2) + '\n')
    else:
        assert json.loads(manifest.read_text()) == rows, 'npm inventory differs; run make licenses-update'
        for path, content in copies.items():
            assert (ROOT / path).read_bytes() == content, f'Upstream notice changed: {path}'
        actual = {p.relative_to(ROOT) for p in (ROOT / 'licenses/npm').rglob('*') if p.is_file()}
        assert actual == set(copies), 'Missing or obsolete npm notices'
    modules = dict(re.findall(r'^\s*(\S+) (v\S+)', (ROOT / 'go.mod').read_text(), re.M))
    inventory = json.loads((ROOT / 'licenses/go-inventory.json').read_text())
    assert {row['module']: row['version'] for row in inventory} == modules, 'Go license inventory differs from go.mod'
    for row in inventory:
        assert row['files'], f'Missing Go license: {row["module"]}'
        for file in row['files']:
            assert (ROOT / row['notices'] / file).is_file(), f'Missing notice: {file}'
    print(f'License inventory verified: {len(rows)} npm packages, {len(inventory)} Go modules')


if __name__ == '__main__':
    main()
