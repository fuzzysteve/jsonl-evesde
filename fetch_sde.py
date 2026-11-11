#!/usr/bin/env python3
"""Download every SDE table as JSONL from the sde.evetech.net REST API.

Replaces the old zip download + unzip. The release index (`/latest/`) returns
pinned per-release URLs, so a release published mid-download can't produce a
mixed set of files.

usage:
    fetch_sde.py build                 print the latest build number
    fetch_sde.py fetch <dest_dir>      download all tables into dest_dir

`fetch` writes into a temp dir next to dest_dir, checks that every line of
every file parses as JSON (catches truncation, since the API publishes no
checksums), then swaps the *.jsonl files into dest_dir. Other contents of
dest_dir (e.g. its .git) are left alone. Prints the build number on success.
"""
import gzip
import json
import os
import shutil
import sys
import tempfile
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

INDEX_URL = os.environ.get('SDE_API_URL', 'https://sde.evetech.net/tranquility/latest/')
USER_AGENT = 'jsonl-evesde-loader'
WORKERS = 8


def _log(msg):
    print(f"[{datetime.now():%H:%M:%S}] {msg}", file=sys.stderr)


def _open(url):
    """GET url, returning a file-like body with any gzip transfer encoding removed.

    The CDN may gzip even when not asked, and urllib doesn't decode it, so ask
    for gzip explicitly and decode it here."""
    req = urllib.request.Request(url, headers={'User-Agent': USER_AGENT, 'Accept-Encoding': 'gzip'})
    resp = urllib.request.urlopen(req, timeout=120)
    enc = resp.headers.get('Content-Encoding', 'identity')
    if enc == 'gzip':
        return gzip.GzipFile(fileobj=resp)
    if enc != 'identity':
        resp.close()
        raise RuntimeError(f"{url}: unsupported Content-Encoding {enc}")
    return resp


def release_index():
    with _open(INDEX_URL) as resp:
        return json.load(resp)


def _download(name, url, dest_dir):
    path = os.path.join(dest_dir, f'{name}.jsonl')
    with _open(url) as resp, open(path, 'wb') as fh:
        shutil.copyfileobj(resp, fh, 1 << 20)
    lines = 0
    with open(path, encoding='utf-8') as fh:
        for n, line in enumerate(fh, 1):
            if line.strip():
                json.loads(line)
                lines += 1
    return name, lines


def fetch(dest_dir):
    index = release_index()
    tables = {name: t['jsonl']['url'] for name, t in index['tables'].items() if t.get('jsonl')}
    _log(f"Release {index['release']} (build {index['buildNumber']}): {len(tables)} tables")

    parent = os.path.dirname(os.path.abspath(dest_dir))
    tmp = tempfile.mkdtemp(prefix='.sde-fetch-', dir=parent)
    try:
        with ThreadPoolExecutor(WORKERS) as pool:
            for name, lines in pool.map(lambda kv: _download(*kv, tmp), tables.items()):
                _log(f"  {name}: {lines} records")

        os.makedirs(dest_dir, exist_ok=True)
        for f in os.listdir(dest_dir):
            if f.endswith('.jsonl'):
                os.remove(os.path.join(dest_dir, f))
        for f in os.listdir(tmp):
            os.replace(os.path.join(tmp, f), os.path.join(dest_dir, f))
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
    return index['buildNumber']


def main():
    if len(sys.argv) == 2 and sys.argv[1] == 'build':
        print(release_index()['buildNumber'])
    elif len(sys.argv) == 3 and sys.argv[1] == 'fetch':
        print(fetch(sys.argv[2]))
    else:
        sys.exit(__doc__)


if __name__ == '__main__':
    main()
