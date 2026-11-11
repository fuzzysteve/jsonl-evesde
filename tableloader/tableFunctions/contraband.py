# -*- coding: utf-8 -*-

import json
import os
from sqlalchemy import Table, insert


def _num(s):
    """parse_float for json.loads: whole numbers >= 2**53 become exact ints.

    The API writes them as floats (e.g. 1.0249999925043528e+18). SQL Server
    mis-rounds that text form by 1 ULP but converts the integer exactly."""
    f = float(s)
    return int(f) if f.is_integer() and abs(f) >= 2**53 else f


def _jsonl(sourcePath, filename):
    filepath = os.path.join(sourcePath, filename)
    with open(filepath, 'r', encoding='utf-8') as fh:
        for line in fh:
            line = line.strip()
            if line:
                yield json.loads(line, parse_float=_num)


def _log(msg):
    from datetime import datetime
    print(f"[{datetime.now():%H:%M:%S}] {msg}")

def import_contraband_types(connection, metadata, sourcePath, language='en'):
    """contrabandTypes.jsonl -> invContrabandTypes"""
    _log("Importing contrabandTypes")
    tbl = Table('invContrabandTypes', metadata)
    trans = connection.begin()
    count = 0
    for r in _jsonl(sourcePath, 'contrabandTypes.jsonl'):
        type_id = r['typeID']
        for faction in r.get('factions', []):
            connection.execute(insert(tbl).values(
                factionID        = faction['factionID'],
                typeID           = type_id,
                standingLoss     = faction.get('standingLoss'),
                confiscateMinSec = faction.get('confiscateMinSec'),
                fineByValue      = faction.get('fineByValue'),
                attackMinSec     = faction.get('attackMinSec'),
            ))
            count += 1
    trans.commit()
    _log("    {} rows".format(count))
