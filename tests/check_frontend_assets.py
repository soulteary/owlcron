#!/usr/bin/env python3
"""Check browser and worker assets through the same HTTP surface as users."""
import argparse
from html.parser import HTMLParser
import re
from urllib.parse import urldefrag, urljoin, urlsplit
from urllib.request import ProxyHandler, build_opener


class Assets(HTMLParser):
    def __init__(self):
        super().__init__()
        self.urls = []

    def handle_starttag(self, tag, attrs):
        attrs = dict(attrs)
        if tag == 'script' and attrs.get('src'):
            self.urls.append(attrs['src'])
        if tag == 'link' and 'stylesheet' in attrs.get('rel', '').split() and attrs.get('href'):
            self.urls.append(attrs['href'])


def check_frontend_assets(base_url, opener=None):
    base_url = base_url.rstrip('/') + '/'
    origin = urlsplit(base_url)
    pending = [base_url, urljoin(base_url, 'js/home-worker.js')]
    checked = set()
    opener = opener or build_opener(ProxyHandler({}))
    while pending:
        url = urldefrag(pending.pop())[0]
        parsed = urlsplit(url)
        if parsed.scheme not in ('http', 'https') or (parsed.scheme, parsed.netloc) != (origin.scheme, origin.netloc):
            continue
        if url in checked:
            continue
        if len(checked) >= 200:
            raise ValueError('Unexpectedly large frontend asset graph')
        with opener.open(url, timeout=10) as response:
            content_type = response.headers.get_content_type()
            body = response.read()
        if not body:
            raise ValueError(f'Empty frontend asset: {url}')
        checked.add(url)
        references = []
        if parsed.path.endswith('.css'):
            if content_type != 'text/css':
                raise ValueError(f'Expected CSS asset, got {content_type}: {url}')
            references = re.findall(r'url\(\s*[\'"]?([^\s\)\'\"]+)', body.decode('utf-8'))
        elif parsed.path.endswith('.js'):
            if content_type not in ('text/javascript', 'application/javascript', 'application/x-javascript'):
                raise ValueError(f'Expected JavaScript asset, got {content_type}: {url}')
            for imports in re.findall(r'\bimportScripts\s*\((.*?)\)', body.decode('utf-8'), re.S):
                references.extend(re.findall(r'[\'"]([^\'"]+)[\'"]', imports))
        elif url == base_url:
            if content_type != 'text/html':
                raise ValueError(f'Expected frontend HTML, got {content_type}')
            parser = Assets()
            parser.feed(body.decode('utf-8'))
            references = parser.urls
        pending.extend(urljoin(url, reference) for reference in references if not reference.startswith(('data:', '#')))
    return len(checked)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('base_url')
    args = parser.parse_args()
    print(f'Frontend assets verified: {check_frontend_assets(args.base_url)} HTTP resources')
