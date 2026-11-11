# -*- coding: utf-8 -*-

import os
import json
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

def import_compressible_types(connection, metadata, sourcePath, language='en'):
    """compressibleTypes.jsonl -> compressibleTypes"""
    _log("Importing compressibleTypes")
    tbl = Table('compressibleTypes', metadata)
    trans = connection.begin()
    count = 0
    for r in _jsonl(sourcePath, 'compressibleTypes.jsonl'):
        connection.execute(insert(tbl).values(
            typeID           = r['typeID'],
            compressedTypeID = r['compressedTypeID'],
        ))
        count += 1
    trans.commit()
    _log("    {} rows".format(count))
