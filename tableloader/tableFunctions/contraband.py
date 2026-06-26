# -*- coding: utf-8 -*-

import json
import os
from sqlalchemy import Table, insert


def _jsonl(sourcePath, filename):
    filepath = os.path.join(sourcePath, filename)
    with open(filepath, 'r', encoding='utf-8') as fh:
        for line in fh:
            line = line.strip()
            if line:
                yield json.loads(line)


def import_contraband_types(connection, metadata, sourcePath, language='en'):
    """contrabandTypes.jsonl -> invContrabandTypes"""
    print("Importing contrabandTypes")
    tbl = Table('invContrabandTypes', metadata)
    trans = connection.begin()
    count = 0
    for r in _jsonl(sourcePath, 'contrabandTypes.jsonl'):
        type_id = r['_key']
        for faction in r.get('factions', []):
            connection.execute(insert(tbl).values(
                factionID        = faction['_key'],
                typeID           = type_id,
                standingLoss     = faction.get('standingLoss'),
                confiscateMinSec = faction.get('confiscateMinSec'),
                fineByValue      = faction.get('fineByValue'),
                attackMinSec     = faction.get('attackMinSec'),
            ))
            count += 1
    trans.commit()
    print("    {} rows".format(count))
