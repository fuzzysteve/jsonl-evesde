"""A local stand-in for sde.evetech.net: a release index plus per-release JSONL files.

Each publish() makes a new pinned release. Old releases stay served, as the
real API's per-release URLs do, so a fetch that read the index just before a
publish still downloads a consistent set.
"""
import gzip
import itertools
import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

_versions = itertools.count(1)


def jsonl(*records):
    return b''.join(json.dumps(r).encode() + b'\n' for r in records)


class FakeSDE:
    def __init__(self, build=1000, tables=None):
        self.gzip = False            # gzip every response, as the CDN sometimes does
        self.requests = []           # paths requested, in order
        self.releases = {}           # release id -> {'build', 'tables': {name: (version, bytes)}}
        self._seq = itertools.count(1)
        self.current = None
        self._server = ThreadingHTTPServer(('127.0.0.1', 0), self._handler())
        threading.Thread(target=self._server.serve_forever, kwargs={"poll_interval": 0.02}, daemon=True).start()
        self.base = f'http://127.0.0.1:{self._server.server_port}'
        self.index_url = f'{self.base}/tranquility/latest/'
        self.publish(build=build, set=tables or {})

    def close(self):
        self._server.shutdown()
        self._server.server_close()

    # ---- state ------------------------------------------------------------
    @property
    def build(self):
        return self.releases[self.current]['build']

    def tables(self):
        return dict(self.releases[self.current]['tables'])

    def publish(self, build=None, set=None, bump=(), remove=(), corrupt=()):
        """New release. `set` gives tables new content (and a new version) or
        adds them; `bump` gives tables a new version with the same bytes;
        `remove` drops tables; `corrupt` serves invalid JSON for those tables
        (with a new version). Returns the release id."""
        tables = dict(self.releases[self.current]['tables']) if self.current else {}
        for name, body in (set or {}).items():
            tables[name] = (f'v{next(_versions)}', body)
        for name in bump:
            tables[name] = (f'v{next(_versions)}', tables[name][1])
        for name in remove:
            del tables[name]
        for name in corrupt:
            tables[name] = (f'v{next(_versions)}', b'{"not json\n')
        release = f'20261116T{next(self._seq):06d}Z'
        self.releases[release] = {'build': self.build if build is None and self.current else build,
                                  'tables': tables}
        self.current = release
        return release

    def index(self):
        rel = self.releases[self.current]
        tables = {name: {'version': v,
                         'jsonl': {'url': f'{self.base}/tranquility/{self.current}/{name}.jsonl'},
                         'record': {}}
                  for name, (v, _) in rel['tables'].items()}
        # The real index also lists entries without a JSONL form; fetch must skip them.
        tables['_noJsonl'] = {'version': 'x', 'flatbuffers': {}}
        return {'release': self.current, 'buildNumber': rel['build'],
                'releaseDate': '2026-11-16T00:00:00Z', 'tables': tables}

    # ---- HTTP -------------------------------------------------------------
    def _handler(self):
        fake = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *a):
                pass

            def do_GET(self):
                fake.requests.append(self.path)
                if self.headers.get('User-Agent') != 'jsonl-evesde-loader':
                    return self._send(403, b'missing user agent')
                parts = self.path.strip('/').split('/')
                if parts == ['tranquility', 'latest']:
                    return self._send(200, json.dumps(fake.index()).encode())
                if len(parts) == 3 and parts[0] == 'tranquility' and parts[2].endswith('.jsonl'):
                    rel = fake.releases.get(parts[1])
                    entry = rel and rel['tables'].get(parts[2][:-len('.jsonl')])
                    if entry:
                        return self._send(200, entry[1])
                self._send(404, b'not found')

            def _send(self, code, body):
                if fake.gzip:
                    body = gzip.compress(body)
                self.send_response(code)
                if fake.gzip:
                    self.send_header('Content-Encoding', 'gzip')
                self.send_header('Content-Length', str(len(body)))
                self.end_headers()
                self.wfile.write(body)

        return Handler
