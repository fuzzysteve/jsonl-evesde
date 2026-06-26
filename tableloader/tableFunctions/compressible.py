# -*- coding: utf-8 -*-

import os
import json
from sqlalchemy import Table, insert


def _jsonl(sourcePath, filename):
    filepath = os.path.join(sourcePath, filename)
    with open(filepath, 'r', encoding='utf-8') as fh:
        for line in fh:
            line = line.strip()
            if line:
                yield json.loads(line)


def import_compressible_types(connection, metadata, sourcePath, language='en'):
    """compressibleTypes.jsonl -> compressibleTypes"""
    print("Importing compressibleTypes")
    tbl = Table('compressibleTypes', metadata)
    trans = connection.begin()
    count = 0
    for r in _jsonl(sourcePath, 'compressibleTypes.jsonl'):
        connection.execute(insert(tbl).values(
            typeID           = r['_key'],
            compressedTypeID = r['compressedTypeID'],
        ))
        count += 1
    trans.commit()
    print("    {} rows".format(count))
