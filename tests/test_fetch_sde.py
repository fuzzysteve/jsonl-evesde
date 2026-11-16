"""fetch_sde.py against a local fake of the SDE API: downloading, the version
manifest, `check`, the content-based `changed` set and the `done` marker."""
import json
import os
import subprocess
import sys

import pytest

import fetch_sde
from conftest import BASE_TABLES, REPO
from fakesde import jsonl

NEW_TYPES = jsonl({'typeID': 34, 'name': {'en': 'Tritanium'}}, {'typeID': 36, 'name': {'en': 'Mexallon'}})


@pytest.fixture
def api(sde, monkeypatch):
    monkeypatch.setattr(fetch_sde, 'INDEX_URL', sde.index_url)
    return sde


@pytest.fixture
def dest(tmp_path):
    return str(tmp_path / 'files')


def manifest(dest):
    with open(os.path.join(dest, '.sde-versions.json')) as fh:
        return json.load(fh)


def downloads(api):
    """Table names downloaded since the last call."""
    names = sorted(p.rsplit('/', 1)[1][:-len('.jsonl')] for p in api.requests if p.endswith('.jsonl'))
    api.requests.clear()
    return names


def fetched_and_done(dest):
    fetch_sde.fetch(dest)
    fetch_sde.mark_done(dest)


# ---- fetch ----------------------------------------------------------------

def test_first_fetch_downloads_everything_and_changed_is_unknown(api, dest):
    assert fetch_sde.fetch(dest) == 1000
    assert downloads(api) == sorted(BASE_TABLES)
    for name, body in BASE_TABLES.items():
        with open(os.path.join(dest, f'{name}.jsonl'), 'rb') as fh:
            assert fh.read() == body
    m = manifest(dest)
    assert m['release'] == api.current and m['buildNumber'] == 1000
    assert m['changed'] is None and m['processed'] is False
    assert set(m['tables']) == set(BASE_TABLES)          # _noJsonl entry skipped


def test_unchanged_tables_are_not_downloaded_again(api, dest):
    fetched_and_done(dest)
    downloads(api)
    api.publish(set={'types': NEW_TYPES})
    fetch_sde.fetch(dest)
    assert downloads(api) == ['types']


def test_changed_is_content_based(api, dest):
    fetched_and_done(dest)
    downloads(api)
    api.publish(set={'types': NEW_TYPES}, bump=['groups', 'mapMoons'])
    fetch_sde.fetch(dest)
    assert downloads(api) == ['groups', 'mapMoons', 'types']   # new versions are downloaded...
    assert manifest(dest)['changed'] == ['types']               # ...but identical bytes aren't changes


def test_version_bump_with_identical_bytes_changes_nothing(api, dest):
    fetched_and_done(dest)
    api.publish(bump=['types', 'groups'])
    fetch_sde.fetch(dest)
    assert manifest(dest)['changed'] == []


def test_added_and_removed_tables_are_changed(api, dest):
    fetched_and_done(dest)
    (open(os.path.join(dest, 'notes.txt'), 'w')).write('keep me')
    api.publish(set={'skins': jsonl({'skinID': 1})}, remove=['groups'])
    fetch_sde.fetch(dest)
    assert manifest(dest)['changed'] == ['groups', 'skins']
    assert not os.path.exists(os.path.join(dest, 'groups.jsonl'))
    assert os.path.exists(os.path.join(dest, 'skins.jsonl'))
    assert os.path.exists(os.path.join(dest, 'notes.txt'))     # non-JSONL files left alone


def test_damaged_local_file_is_redownloaded_but_not_a_change(api, dest):
    fetched_and_done(dest)
    with open(os.path.join(dest, 'types.jsonl'), 'ab') as fh:
        fh.write(b'{"typeID": 999}\n')
    downloads(api)
    fetch_sde.fetch(dest)
    assert downloads(api) == ['types']
    with open(os.path.join(dest, 'types.jsonl'), 'rb') as fh:
        assert fh.read() == BASE_TABLES['types']
    assert manifest(dest)['changed'] == []


def test_invalid_json_aborts_without_touching_dest(api, dest):
    fetched_and_done(dest)
    before = manifest(dest)
    api.publish(set={'types': NEW_TYPES}, corrupt=['groups'])
    with pytest.raises(json.JSONDecodeError):
        fetch_sde.fetch(dest)
    assert manifest(dest) == before
    with open(os.path.join(dest, 'types.jsonl'), 'rb') as fh:
        assert fh.read() == BASE_TABLES['types']
    parent = os.path.dirname(dest)
    assert not [d for d in os.listdir(parent) if d.startswith('.sde-fetch-')]   # temp dir cleaned up


def test_gzip_responses_are_decoded(api, dest):
    api.gzip = True
    fetch_sde.fetch(dest)
    with open(os.path.join(dest, 'types.jsonl'), 'rb') as fh:
        assert fh.read() == BASE_TABLES['types']


# ---- carry-over of unprocessed fetches --------------------------------------

def test_unprocessed_changed_set_carries_into_next_fetch(api, dest):
    fetched_and_done(dest)
    api.publish(set={'types': NEW_TYPES})
    fetch_sde.fetch(dest)                     # run "fails": never marked done
    api.publish(set={'groups': jsonl({'groupID': 19})})
    fetch_sde.fetch(dest)
    assert manifest(dest)['changed'] == ['groups', 'types']
    fetch_sde.mark_done(dest)
    api.publish(set={'mapMoons': jsonl({'moonID': 1})})
    fetch_sde.fetch(dest)
    assert manifest(dest)['changed'] == ['mapMoons']   # carry-over stops once done


def test_refetching_same_release_keeps_unprocessed_changes(api, dest):
    fetched_and_done(dest)
    api.publish(set={'types': NEW_TYPES})
    fetch_sde.fetch(dest)
    fetch_sde.fetch(dest)                     # retry of a failed run, nothing new
    assert manifest(dest)['changed'] == ['types']


def test_unknown_changed_set_carries_over_as_unknown(api, dest):
    fetch_sde.fetch(dest)                     # first fetch (changed None), never done
    api.publish(set={'types': NEW_TYPES})
    fetch_sde.fetch(dest)
    assert manifest(dest)['changed'] is None  # still a full load


def test_manifest_without_processed_flag_counts_as_unprocessed(api, dest):
    fetch_sde.fetch(dest)
    fetch_sde.mark_done(dest)
    api.publish(set={'types': NEW_TYPES})
    fetch_sde.fetch(dest)
    m = manifest(dest)
    del m['processed']                        # manifests written before the flag existed
    with open(os.path.join(dest, '.sde-versions.json'), 'w') as fh:
        json.dump(m, fh)
    assert fetch_sde.check(dest) is not None
    api.publish(set={'groups': jsonl({'groupID': 19})})
    fetch_sde.fetch(dest)
    assert manifest(dest)['changed'] == ['groups', 'types']


# ---- check / done ----------------------------------------------------------

def test_check_without_manifest_is_pending(api, dest):
    assert fetch_sde.check(dest) == (1000, api.current)


def test_check_fetched_but_not_done_is_pending(api, dest):
    fetch_sde.fetch(dest)
    assert fetch_sde.check(dest) == (1000, api.current)


def test_check_up_to_date(api, dest):
    fetched_and_done(dest)
    assert fetch_sde.check(dest) is None


@pytest.mark.parametrize('change', [
    dict(bump=['types']),                                  # same build, version bump only
    dict(set={'types': NEW_TYPES}),                        # same build, new content
    dict(set={'skins': jsonl({'skinID': 1})}),             # table added
    dict(remove=['groups']),                               # table removed
    dict(build=1001),                                      # new build, no table changes
], ids=['bump', 'content', 'added', 'removed', 'new-build'])
def test_check_detects_version_changes(api, dest, change):
    fetched_and_done(dest)
    release = api.publish(**change)
    assert fetch_sde.check(dest) == (api.build, release)


def test_check_does_not_download(api, dest):
    fetched_and_done(dest)
    api.publish(set={'types': NEW_TYPES})
    downloads(api)
    fetch_sde.check(dest)
    assert downloads(api) == []


def test_done_without_manifest_is_harmless(dest):
    fetch_sde.mark_done(dest)
    assert not os.path.exists(os.path.join(dest, '.sde-versions.json'))


def test_done_keeps_tables_and_changed(api, dest):
    fetched_and_done(dest)
    api.publish(set={'types': NEW_TYPES})
    fetch_sde.fetch(dest)
    before = manifest(dest)
    fetch_sde.mark_done(dest)
    after = manifest(dest)
    assert after.pop('processed') is True and before.pop('processed') is False
    assert after == before


# ---- command line ------------------------------------------------------------

def cli(api, *args):
    env = dict(os.environ, SDE_API_URL=api.index_url)
    return subprocess.run([sys.executable, os.path.join(REPO, 'fetch_sde.py'), *args],
                          env=env, capture_output=True, text=True, check=True).stdout


def test_cli(api, dest):
    assert cli(api, 'build') == '1000\n'
    assert cli(api, 'check', dest) == f'1000 {api.current}\n'
    assert cli(api, 'fetch', dest) == '1000\n'
    assert cli(api, 'changed', dest) == ''                 # unknown -> nothing (full load)
    cli(api, 'done', dest)
    assert cli(api, 'check', dest) == ''
    api.publish(set={'types': NEW_TYPES, 'groups': jsonl({'groupID': 19})})
    cli(api, 'fetch', dest)
    assert cli(api, 'changed', dest) == 'groups.jsonl\ntypes.jsonl\n'


def test_cli_usage_error():
    r = subprocess.run([sys.executable, os.path.join(REPO, 'fetch_sde.py'), 'bogus'],
                       capture_output=True, text=True)
    assert r.returncode != 0 and 'usage:' in r.stderr
