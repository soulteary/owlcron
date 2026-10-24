#!/usr/bin/env python3
"""Check documentation links, language pairs and configuration examples offline."""
import json
import os
from pathlib import Path
import re
from urllib.parse import unquote, urlsplit

ROOT = Path(__file__).resolve().parent.parent


def heading_ids(text):
    ids = set()
    counts = {}
    for title in re.findall(r'^#{1,6}\s+(.+?)\s*#*$', text, re.M):
        slug = re.sub(r'[^\w\- ]', '', title.lower()).replace(' ', '-')
        occurrence = counts.get(slug, 0)
        counts[slug] = occurrence + 1
        ids.add(slug if not occurrence else f'{slug}-{occurrence}')
    return ids


def check_document(path, root, config_keys=None):
    text = path.read_text()
    errors = []
    for target in re.findall(r'!?\[[^\]\n]*\]\(([^)\n]+)\)', text):
        target = target.strip().strip('<>')
        url = urlsplit(target)
        if url.scheme or url.netloc:
            continue
        destination = (path.parent / unquote(url.path)).resolve() if url.path else path.resolve()
        if not destination.is_relative_to(root.resolve()):
            errors.append(f'Link leaves repository: {target}')
        elif not destination.exists():
            errors.append(f'Missing link target: {target}')
        elif url.fragment and destination.suffix == '.md':
            if unquote(url.fragment) not in heading_ids(destination.read_text()):
                errors.append(f'Missing heading: {target}')
    if re.search(r'/Users/|/private/(?:tmp|var)/|Cronicle-master|\.\./example', text):
        errors.append('Local workspace reference in published documentation')
    if config_keys is not None:
        for snippet in re.findall(r'```json\s*\n(.*?)```', text, re.S):
            try:
                config = json.loads(snippet)
            except json.JSONDecodeError as error:
                errors.append(f'Invalid configuration JSON: {error.msg}')
                continue
            if not isinstance(config, dict):
                errors.append('Configuration example must be an object')
                continue
            unknown = set(config) - config_keys
            if unknown:
                errors.append('Unknown configuration keys: ' + ', '.join(sorted(unknown)))
    return errors


def example_blocks(text):
    """Compare executable examples, allowing translated comments and JSON layout."""
    blocks = []
    for language, content in re.findall(r'```([^\n]*)\n(.*?)```', text, re.S):
        if language.strip() == 'json':
            content = json.dumps(json.loads(content), sort_keys=True)
        else:
            content = '\n'.join(line.strip() for line in content.splitlines()
                                if line.strip() and not line.lstrip().startswith('#'))
        blocks.append((language.strip(), content))
    return blocks


def check_translations(root):
    english = [p for p in sorted(root.glob('*.md')) if not p.name.endswith('.zh-CN.md')]
    english += sorted((root / 'docs').glob('*.md'))
    pairs = [(p, p.with_name(p.stem + '.zh-CN.md')) if p.parent == root
             else (p, root / 'docs/zh-CN' / p.name) for p in english]
    expected = {translated for _, translated in pairs}
    actual = set(root.glob('*.zh-CN.md')) | set((root / 'docs/zh-CN').glob('*.md'))
    errors = [f'Orphan translation: {p.relative_to(root)}' for p in sorted(actual - expected)]
    for original, translated in pairs:
        if not translated.is_file():
            errors.append(f'Missing translation: {translated.relative_to(root)}')
            continue
        for source, destination in [(original, translated), (translated, original)]:
            target = os.path.relpath(destination, source.parent).replace(os.sep, '/')
            if f']({target})' not in '\n'.join(source.read_text().splitlines()[:6]):
                errors.append(f'Missing language switch: {source.relative_to(root)}')
        try:
            if example_blocks(original.read_text()) != example_blocks(translated.read_text()):
                errors.append(f'Translated examples differ: {translated.relative_to(root)}')
        except json.JSONDecodeError:
            errors.append(f'Invalid JSON example in language pair: {original.relative_to(root)}')
    return errors


def main():
    documents = sorted(ROOT.glob('*.md')) + sorted((ROOT / 'docs').rglob('*.md'))
    source = (ROOT / 'types/types.go').read_text()
    config_struct = re.search(r'^type Config struct \{(.*?)^\}', source, re.M | re.S)
    if config_struct is None:
        raise SystemExit('Cannot locate types.Config')
    config_keys = set(re.findall(r'json:"([^",]+)', config_struct.group(1)))
    failures = check_translations(ROOT)
    for path in documents:
        keys = config_keys if path.name in ('configuration.md', 'deployment.md') else None
        failures.extend(f'{path.relative_to(ROOT)}: {error}' for error in check_document(path, ROOT, keys))
    if failures:
        raise SystemExit('\n'.join(failures))
    print(f'Documentation verified: {len(documents)} files, links, language pairs and examples')


if __name__ == '__main__':
    main()
