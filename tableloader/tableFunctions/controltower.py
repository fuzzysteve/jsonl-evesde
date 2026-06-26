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


def import_control_tower_resources(connection, metadata, sourcePath, language='en'):
    """controlTowerResources.jsonl -> invControlTowerResources"""
    print("Importing controlTowerResources")
    tbl = Table('invControlTowerResources', metadata)
    trans = connection.begin()
    count = 0
    for r in _jsonl(sourcePath, 'controlTowerResources.jsonl'):
        tower_id = r['_key']
        for res in r.get('resources', []):
            connection.execute(insert(tbl).values(
                controlTowerTypeID = tower_id,
                resourceTypeID     = res['resourceTypeID'],
                purpose            = res.get('purpose'),
                quantity           = res.get('quantity'),
                minSecurityLevel   = res.get('minSecurityLevel'),
                factionID          = res.get('factionID'),
            ))
            count += 1
    trans.commit()
    print("    {} rows".format(count))
