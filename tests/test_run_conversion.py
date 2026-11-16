"""run-conversion.sh end to end, in a sandbox.

The real script and fetch_sde.py are copied into a temp dir with a generated
run-conversion.cfg pointing everything (SDE dir, output, web root, venv) inside
it and at a local fake of the SDE API. Everything with side effects is a stub
that records how it was called: Load.py, export_csv.py, cleanup_builds.py and
sde_changes.py next to the script, and mysql, mysqldump, pg_dump, docker and
curl on PATH. Nothing touches a real database, the real web root or Discord.
"""
import json
import os
import shutil
import subprocess
import sys

import pytest

from conftest import REPO
from fakesde import jsonl

TARGETS = ['sqlite', 'mysql', 'postgres', 'postgresschema', 'mssql']

STUB = r'''#!{python}
import json, os, sys
name = os.path.basename(sys.argv[0])
args = sys.argv[1:]
with open({log!r}, 'a') as fh:
    fh.write(json.dumps({{'cmd': name, 'args': args,
                         'changed': os.environ.get('SDE_CHANGED_FILES')}}) + '\n')
if name == 'Load.py':
    if os.path.exists({fail!r}):
        sys.exit('Load.py: simulated failure')
    if args == ['sqlite']:
        open({sqlite!r}, 'w').write('db')
elif name == 'sde_changes.py':
    print('Build changes summary')
elif name == 'mysql':
    if '-N' in args:
        print('invTypes'); print('invGroups')
    elif '-e' not in args:
        sys.stdin.read()
elif name == 'mysqldump':
    print('-- dump')
elif name == 'pg_dump':
    open(args[args.index('-f') + 1], 'w').write('pgdump')
elif name == 'docker':
    if args[0] == 'ps':
        print('sqlserver')
    elif args[0] == 'cp':
        open(args[-1], 'w').write('bak')
elif name == 'curl':
    with open({payloads!r}, 'a') as fh:
        fh.write(json.dumps(json.loads(args[args.index('-d') + 1])) + '\n')
    if '-o' in args:
        open(args[args.index('-o') + 1], 'w').close()
    print('204', end='')
'''


class Sandbox:
    def __init__(self, root, api):
        self.root, self.api = root, api
        self.app = root / 'app'
        self.bin = root / 'bin'
        self.files = root / 'files'
        self.web = root / 'web'
        self.log = root / 'calls.jsonl'
        self.payloads = root / 'discord.jsonl'
        self.fail_flag = root / 'FAIL_LOAD'
        for d in (self.app, self.bin, self.files, self.web, root / 'out', root / 'venv' / 'bin'):
            d.mkdir(parents=True)
        for f in ('run-conversion.sh', 'fetch_sde.py', 'discord-notification.json'):
            shutil.copy(os.path.join(REPO, f), self.app / f)
        stub = STUB.format(python=sys.executable, log=str(self.log), fail=str(self.fail_flag),
                           sqlite=str(root / 'eve.db'), payloads=str(self.payloads))
        for name in ('Load.py', 'export_csv.py', 'cleanup_builds.py', 'sde_changes.py'):
            (self.app / name).write_text(stub)
        for name in ('mysql', 'mysqldump', 'pg_dump', 'docker', 'curl'):
            (self.bin / name).write_text(stub)
            (self.bin / name).chmod(0o755)
        (root / 'venv' / 'bin' / 'activate').write_text('')
        (self.app / 'run-conversion.cfg').write_text(f'''\
VENV_DIR={root}/venv
OUTPUT_DIR={root}/out
SDE_DIR={self.files}
SQLITE_FILE={root}/eve.db
WEB_ROOT={self.web}
SDE_API_URL={api.index_url}
MYSQL_USER=test
MYSQL_PASS=test
MYSQL_HOST=localhost
MYSQL_DB=sdeyaml
PG_USER=test
PG_PASS=test
PG_HOST=localhost
PG_DB=sdeyaml
PG_SCHEMA_DB=sdeyamlschema
MSSQL_SA_PASS=test
MSSQL_DB=evesde
MSSQL_CONTAINER=sqlserver
DISCORD_WEBHOOK_URL=http://discord.invalid/webhook
WEB_ROOT_URL=https://example.invalid/dump
''')
        git = ['git', '-C', str(self.files)]
        subprocess.run([*git, 'init', '-q'], check=True)
        subprocess.run([*git, 'config', 'user.email', 'test@example.invalid'], check=True)
        subprocess.run([*git, 'config', 'user.name', 'test'], check=True)

    def run(self, *args, ok=True):
        """Run the pipeline; returns (result, calls made, Discord payloads sent)."""
        for f in (self.log, self.payloads):
            f.unlink(missing_ok=True)
        env = dict(os.environ, PATH=f'{self.bin}:{os.environ["PATH"]}')
        env.pop('SDE_CHANGED_FILES', None)
        r = subprocess.run(['bash', str(self.app / 'run-conversion.sh'), *args],
                           env=env, capture_output=True, text=True, timeout=60)
        if ok and r.returncode != 0:
            pytest.fail(f'run-conversion.sh exited {r.returncode}\n{r.stdout}\n{r.stderr}')
        read = lambda f: [json.loads(l) for l in f.read_text().splitlines()] if f.exists() else []
        return r, read(self.log), read(self.payloads)

    def manifest(self):
        return json.loads((self.files / '.sde-versions.json').read_text())

    def last_build(self):
        p = self.app / '.last_build'
        return p.read_text().strip() if p.exists() else None

    def published_dir(self):
        """The build directory the most recent publish wrote (where `latest` points)."""
        from pathlib import Path
        return Path(os.readlink(self.web / 'latest'))

    def diff_files(self):
        d = self.published_dir()
        txt = d / 'sde-changes.txt'
        gz = d / 'sde-changes.diff.gz'
        import gzip
        return (txt.read_text() if txt.exists() else None,
                gzip.decompress(gz.read_bytes()).decode() if gz.exists() else None)

    def git_log(self):
        return subprocess.run(['git', '-C', str(self.files), 'log', '--format=%s'],
                              capture_output=True, text=True).stdout.split('\n')[:-1]


def fields(payload):
    return {f['name']: f['value'] for f in payload['embeds'][0]['fields']}


def loads(calls):
    """{target: SDE_CHANGED_FILES as a sorted list, or None if unset} for each Load.py call."""
    return {c['args'][0]: (sorted(c['changed'].split()) if c['changed'] is not None else None)
            for c in calls if c['cmd'] == 'Load.py'}


def called(calls, cmd):
    return [c for c in calls if c['cmd'] == cmd]


def sde_tables(build, **extra):
    """A new release's bookkeeping files, as every real release changes them."""
    return {'_sde': jsonl({'key': 'sde', 'buildNumber': build, 'releaseDate': extra.pop('date', 'x')}),
            '_changes': jsonl({'comparedWith': extra.pop('compared', 'y'), 'tables': []}), **extra}


@pytest.fixture
def box(tmp_path, sde):
    return Sandbox(tmp_path, sde)


@pytest.fixture
def published(box):
    """A sandbox whose first (full) run has completed."""
    box.run()
    return box


# ---------------------------------------------------------------------------

def test_first_run_is_a_full_load_and_publishes(box):
    r, calls, discord = box.run()
    assert loads(calls) == {t: None for t in TARGETS}
    assert box.last_build() == '1000'
    assert box.manifest()['processed'] is True
    latest = box.web / 'latest-sqlite.db.gz'
    assert latest.is_symlink() and latest.exists()
    for f in ('latest-mysql.sql.gz', 'latest-postgres.pgdump', 'latest-postgresschema.pgdump', 'latest-mssql.bak'):
        assert (box.web / f).exists() and (box.web / f'{f}.md5sum').exists()
    assert len(called(calls, 'cleanup_builds.py')) == 1
    assert called(calls, 'sde_changes.py')[0]['args'] == ['1000', '']
    [payload] = discord
    embed = payload['embeds'][0]
    assert embed['title'] == 'SDE build 1000 published'
    assert fields(payload)['Changed files'] == 'Build changes summary'
    assert 'File diff' not in fields(payload)                       # no base yet
    assert box.git_log() == [f'1000 ({box.api.current})']


def test_nothing_new_does_nothing(published):
    r, calls, discord = published.run()
    assert 'nothing to do' in r.stdout
    assert calls == [] and discord == []


def test_rerelease_with_data_change_is_an_update_and_announced_as_such(published):
    release = published.api.publish(set={**sde_tables(1000), 'types': jsonl({'typeID': 34})})
    r, calls, discord = published.run()
    assert loads(calls) == {t: ['_changes.jsonl', '_sde.jsonl', 'types.jsonl'] for t in TARGETS}
    assert called(calls, 'sde_changes.py') == []
    [payload] = discord
    assert fields(payload)['Changed files'] == \
        f'Re-release {release} of build 1000\nChanged tables: types'
    assert published.manifest()['processed'] is True
    assert published.git_log()[0] == f'1000 ({release})'


def test_rerelease_with_only_version_bumps_is_marked_done_without_publishing(published):
    release = published.api.publish(set=sde_tables(1000), bump=['types', 'mapMoons'])
    r, calls, discord = published.run()
    assert 'changes no loaded data' in r.stdout
    assert calls == [] and discord == []
    m = published.manifest()
    assert m['release'] == release and m['processed'] is True
    assert published.git_log()[0] == f'1000 ({release})'          # SDE history still recorded
    r, calls, _ = published.run()
    assert 'nothing to do' in r.stdout


def test_new_build_is_published_even_without_data_changes(published):
    published.api.publish(build=1001, set=sde_tables(1001))
    r, calls, discord = published.run()
    assert loads(calls) == {t: ['_changes.jsonl', '_sde.jsonl'] for t in TARGETS}
    assert called(calls, 'sde_changes.py')[0]['args'] == ['1001', '1000']
    assert discord[0]['embeds'][0]['title'] == 'SDE build 1001 published'
    assert published.last_build() == '1001'


def test_failed_run_is_retried_with_the_same_changes(published):
    published.api.publish(build=1001, set={**sde_tables(1001), 'groups': jsonl({'groupID': 19})})
    published.fail_flag.touch()
    r, calls, discord = published.run(ok=False)
    assert r.returncode != 0
    assert discord == [] and called(calls, 'cleanup_builds.py') == []
    assert published.last_build() == '1000'
    assert published.manifest()['processed'] is False

    published.fail_flag.unlink()
    r, calls, discord = published.run()                 # API unchanged since the failure
    assert loads(calls) == {t: ['_changes.jsonl', '_sde.jsonl', 'groups.jsonl'] for t in TARGETS}
    assert published.last_build() == '1001'
    assert published.manifest()['processed'] is True
    assert called(calls, 'sde_changes.py')[0]['args'] == ['1001', '1000']


def test_failed_run_overtaken_by_a_newer_release_loads_both(published):
    published.api.publish(set={**sde_tables(1000), 'groups': jsonl({'groupID': 19})})
    published.fail_flag.touch()
    published.run(ok=False)
    published.fail_flag.unlink()
    published.api.publish(set={**sde_tables(1000, date='z'), 'mapMoons': jsonl({'moonID': 1})})
    r, calls, _ = published.run()
    assert loads(calls)['mysql'] == ['_changes.jsonl', '_sde.jsonl', 'groups.jsonl', 'mapMoons.jsonl']


def test_failed_bookkeeping_release_does_not_hide_an_earlier_failure(published):
    # A failed data release, then a bookkeeping-only release on top: the data
    # change must still be loaded, not swallowed by the "no loaded data" skip.
    published.api.publish(set={**sde_tables(1000), 'groups': jsonl({'groupID': 19})})
    published.fail_flag.touch()
    published.run(ok=False)
    published.fail_flag.unlink()
    published.api.publish(set=sde_tables(1000, date='z'), bump=['types'])
    r, calls, _ = published.run()
    assert 'groups.jsonl' in loads(calls)['sqlite']


def test_force_with_nothing_new_is_a_full_load(published):
    r, calls, discord = published.run('--force')
    assert 'continuing' in r.stdout
    assert loads(calls) == {t: None for t in TARGETS}
    assert len(discord) == 1


def test_force_publishes_a_bookkeeping_only_rerelease(published):
    published.api.publish(set=sde_tables(1000), bump=['types'])
    r, calls, discord = published.run('--force')
    assert loads(calls) == {t: ['_changes.jsonl', '_sde.jsonl'] for t in TARGETS}
    assert len(discord) == 1


def test_preview_dirty_forces_a_full_load_even_for_a_bookkeeping_release(published):
    (published.app / '.preview_dirty').touch()
    published.api.publish(set=sde_tables(1000), bump=['types'])
    r, calls, _ = published.run()
    assert loads(calls) == {t: None for t in TARGETS}
    assert not (published.app / '.preview_dirty').exists()


def test_reprocess_after_failure_marks_done(published):
    published.api.publish(set={**sde_tables(1000), 'groups': jsonl({'groupID': 19})})
    published.fail_flag.touch()
    published.run(ok=False)
    published.fail_flag.unlink()
    published.api.requests.clear()
    r, calls, _ = published.run('--reprocess')
    assert not [p for p in published.api.requests if p.endswith('.jsonl')]   # no download
    assert loads(calls)['postgres'] == ['_changes.jsonl', '_sde.jsonl', 'groups.jsonl']
    assert published.manifest()['processed'] is True
    r, calls, _ = published.run()
    assert 'nothing to do' in r.stdout


def test_api_unreachable_fails_cleanly(published):
    published.api.close()
    r, calls, discord = published.run(ok=False)
    assert r.returncode == 1
    assert 'Could not check the SDE release index' in r.stdout
    assert calls == [] and discord == []


# ---- published file diff -----------------------------------------------------

def test_first_run_has_no_file_diff_but_records_a_base(box):
    r, _, _ = box.run()
    assert 'No previously published SDE commit recorded' in r.stdout
    assert box.diff_files() == (None, None)
    head = subprocess.run(['git', '-C', str(box.files), 'rev-parse', 'HEAD'],
                          capture_output=True, text=True).stdout.strip()
    assert (box.app / '.last_sde_commit').read_text().strip() == head


def test_file_diff_shows_the_changed_records(published):
    old_release = published.api.current
    release = published.api.publish(set={**sde_tables(1000),
                                         'types': jsonl({'typeID': 34, 'name': {'en': 'Tritanium'}},
                                                        {'typeID': 36, 'name': {'en': 'Mexallon'}}),
                                         'skins': jsonl({'skinID': 1})},
                                     remove=['groups'])
    published.run()
    txt, diff = published.diff_files()
    assert f'From: 1000 ({old_release})' in txt and f'To:   1000 ({release})' in txt
    for f in ('types.jsonl', 'skins.jsonl (new)', 'groups.jsonl (gone)'):
        assert f in txt
    assert 'mapMoons.jsonl' not in txt and '.sde-versions.json' not in diff
    assert '-{"typeID": 35, "name": {"en": "Pyerite"}}' in diff
    assert '+{"typeID": 36, "name": {"en": "Mexallon"}}' in diff
    assert ' {"typeID": 34, "name": {"en": "Tritanium"}}' in diff      # context line


def test_file_diff_spans_skipped_and_failed_releases(published):
    base = published.api.current
    published.api.publish(set={**sde_tables(1000), 'groups': jsonl({'groupID': 19})})
    published.fail_flag.touch()
    published.run(ok=False)                                            # failed: not published
    published.fail_flag.unlink()
    published.run()
    txt, diff = published.diff_files()
    assert f'From: 1000 ({base})' in txt and '+{"groupID": 19}' in diff

    base = published.api.current
    published.api.publish(set=sde_tables(1000, date='z'), bump=['types'])
    published.run()                                                    # bookkeeping only: skipped
    release = published.api.publish(set={**sde_tables(1000, date='w'), 'mapMoons': jsonl({'moonID': 1})})
    published.run()
    txt, diff = published.diff_files()
    assert f'From: 1000 ({base})' in txt and f'To:   1000 ({release})' in txt
    assert 'mapMoons.jsonl' in txt and '_sde.jsonl' in txt


def test_force_with_nothing_new_says_no_changes(published):
    published.run('--force')
    txt, diff = published.diff_files()
    assert 'No source file changes.' in txt and diff is None


def test_missing_base_commit_skips_the_diff(published):
    (published.app / '.last_sde_commit').write_text('0' * 40 + '\n')
    published.api.publish(set={**sde_tables(1000), 'groups': jsonl({'groupID': 19})})
    r, _, _ = published.run()
    assert 'not in' in r.stdout and published.diff_files() == (None, None)


def test_discord_links_this_builds_file_diff(published):
    published.api.publish(set={**sde_tables(1000), 'groups': jsonl({'groupID': 19})})
    _, _, [payload] = published.run()
    build_dir = published.published_dir().name
    base = f'https://example.invalid/dump/{build_dir}'
    assert fields(payload)['File diff'] == \
        f'[sde-changes.txt]({base}/sde-changes.txt) · [sde-changes.diff.gz]({base}/sde-changes.diff.gz)'


def test_discord_diff_link_without_gz_when_nothing_changed(published):
    _, _, [payload] = published.run('--force')
    build_dir = published.published_dir().name
    assert fields(payload)['File diff'] == \
        f'[sde-changes.txt](https://example.invalid/dump/{build_dir}/sde-changes.txt)'


def test_discord_comment_still_comes_after_the_diff_link(published):
    published.api.publish(set={**sde_tables(1000), 'groups': jsonl({'groupID': 19})})
    _, _, [payload] = published.run('--comment', 'hello')
    names = [f['name'] for f in payload['embeds'][0]['fields']]
    assert names[-2:] == ['File diff', 'Note'] and fields(payload)['Note'] == 'hello'
