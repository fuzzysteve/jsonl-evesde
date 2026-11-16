import os
import sys

import pytest

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO)
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from fakesde import FakeSDE, jsonl  # noqa: E402

BASE_TABLES = {
    '_sde': jsonl({'key': 'sde', 'buildNumber': 1000}),
    '_changes': jsonl({'comparedWith': 'x', 'tables': []}),
    'types': jsonl({'typeID': 34, 'name': {'en': 'Tritanium'}}, {'typeID': 35, 'name': {'en': 'Pyerite'}}),
    'groups': jsonl({'groupID': 18, 'name': {'en': 'Mineral'}}),
    'mapMoons': jsonl({'moonID': 40000002, 'solarSystemID': 30000001}),
}


@pytest.fixture
def sde():
    fake = FakeSDE(build=1000, tables=BASE_TABLES)
    yield fake
    fake.close()
