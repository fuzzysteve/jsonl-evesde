#!/usr/bin/env python3
"""Download every SDE table as JSONL from the sde.evetech.net REST API.

Replaces the old zip download + unzip. The release index (`/latest/`) returns
pinned per-release URLs, so a release published mid-download can't produce a
mixed set of files.

usage:
    fetch_sde.py build                 print the latest build number
    fetch_sde.py fetch <dest_dir>      download all tables into dest_dir
    fetch_sde.py changed <dest_dir>    print the JSONL files the last fetch changed

`fetch` keeps a manifest, dest_dir/.sde-versions.json, recording each table's
API `version` and the SHA-256 of the file written. A table is downloaded only
when its version changed or its local file no longer matches the recorded
hash; otherwise the existing file is kept. (The JSONL version is an opaque
blob id, not a hash of the file, so the hash is what proves a local file is
intact.) Downloads go to a temp dir next to dest_dir and every line must parse
as JSON before files are swapped in. Tables no longer in the release are
removed. Other contents of dest_dir (e.g. its .git) are left alone. Prints the
build number on success.

The manifest also records `changed`: the tables whose version differs from the
previous manifest, including tables added or removed. It is null when there was
no previous manifest to compare with. `changed` prints those as <table>.jsonl,
one per line, for SDE_CHANGED_FILES; it prints nothing when the set is empty or
unknown, which the pipeline treats as a full load.
"""
import gzip
import hashlib
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
MANIFEST = '.sde-versions.json'


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


def _sha256(path):
    h = hashlib.sha256()
    with open(path, 'rb') as fh:
        while chunk := fh.read(1 << 20):
            h.update(chunk)
    return h.hexdigest()


def _download(name, url, dest_dir):
    path = os.path.join(dest_dir, f'{name}.jsonl')
    with _open(url) as resp, open(path, 'wb') as fh:
        shutil.copyfileobj(resp, fh, 1 << 20)
    lines = 0
    with open(path, encoding='utf-8') as fh:
        for line in fh:
            if line.strip():
                json.loads(line)
                lines += 1
    return name, lines, _sha256(path)


def _load_manifest(dest_dir):
    """The whole manifest dict, or None if there isn't a readable one."""
    try:
        with open(os.path.join(dest_dir, MANIFEST), encoding='utf-8') as fh:
            m = json.load(fh)
        return m if isinstance(m, dict) and isinstance(m.get('tables'), dict) else None
    except (OSError, ValueError):
        return None


def _write_manifest(dest_dir, index, tables, changed):
    path = os.path.join(dest_dir, MANIFEST)
    tmp = path + '.tmp'
    with open(tmp, 'w', encoding='utf-8') as fh:
        json.dump({'release': index['release'], 'buildNumber': index['buildNumber'],
                   'changed': changed,
                   'tables': dict(sorted(tables.items()))}, fh, indent=1)
        fh.write('\n')
    os.replace(tmp, path)


def _changed_tables(old, tables):
    """Tables whose version differs from the previous manifest, plus added and
    removed ones; None if there was no previous manifest."""
    if old is None:
        return None
    prev = old['tables']
    changed = {n for n, t in tables.items() if (prev.get(n) or {}).get('version') != t['version']}
    changed |= {n for n in prev if n not in tables}
    return sorted(changed)


def _unchanged(dest_dir, name, version, old):
    """True if the local file is the one the manifest recorded for this version."""
    entry = old.get(name)
    path = os.path.join(dest_dir, f'{name}.jsonl')
    return (entry is not None and entry.get('version') == version
            and os.path.isfile(path) and _sha256(path) == entry.get('sha256'))


def fetch(dest_dir):
    index = release_index()
    tables = {name: t for name, t in index['tables'].items() if t.get('jsonl')}
    _log(f"Release {index['release']} (build {index['buildNumber']}): {len(tables)} tables")

    os.makedirs(dest_dir, exist_ok=True)
    manifest = _load_manifest(dest_dir)
    old = manifest['tables'] if manifest else {}
    changed = _changed_tables(manifest, tables)
    new = {}
    todo = {}
    for name, t in tables.items():
        if _unchanged(dest_dir, name, t['version'], old):
            new[name] = old[name]
        else:
            todo[name] = t
    _log(f"  {len(todo)} to download, {len(new)} unchanged")
    if changed is None:
        _log("  no previous manifest: changed set unknown")
    else:
        _log(f"  {len(changed)} table(s) changed since previous fetch")

    parent = os.path.dirname(os.path.abspath(dest_dir))
    tmp = tempfile.mkdtemp(prefix='.sde-fetch-', dir=parent)
    try:
        with ThreadPoolExecutor(WORKERS) as pool:
            for name, lines, sha in pool.map(lambda kv: _download(kv[0], kv[1]['jsonl']['url'], tmp),
                                             todo.items()):
                new[name] = {'version': todo[name]['version'], 'sha256': sha}
                _log(f"  {name}: {lines} records")

        removed = []
        for f in os.listdir(dest_dir):
            if f.endswith('.jsonl') and f[:-len('.jsonl')] not in tables:
                os.remove(os.path.join(dest_dir, f))
                removed.append(f)
        if removed:
            _log(f"  removed {len(removed)} table(s) no longer in the release: {', '.join(sorted(removed))}")
        for f in os.listdir(tmp):
            os.replace(os.path.join(tmp, f), os.path.join(dest_dir, f))
        _write_manifest(dest_dir, index, new, changed)
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
    return index['buildNumber']


def main():
    if len(sys.argv) == 2 and sys.argv[1] == 'build':
        print(release_index()['buildNumber'])
    elif len(sys.argv) == 3 and sys.argv[1] == 'fetch':
        print(fetch(sys.argv[2]))
    elif len(sys.argv) == 3 and sys.argv[1] == 'changed':
        manifest = _load_manifest(sys.argv[2])
        for name in (manifest or {}).get('changed') or []:
            print(f'{name}.jsonl')
    else:
        sys.exit(__doc__)


if __name__ == '__main__':
    main()
