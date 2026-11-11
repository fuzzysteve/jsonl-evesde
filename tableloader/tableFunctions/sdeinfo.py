# -*- coding: utf-8 -*-

import json
import os
from datetime import datetime
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
    print(f"[{datetime.now():%H:%M:%S}] {msg}")


def import_sde_info(connection, metadata, sourcePath, language='en'):
    """_sde.jsonl -> sdeInfo"""
    _log("Importing _sde")
    tbl = Table('sdeInfo', metadata)
    trans = connection.begin()
    for r in _jsonl(sourcePath, '_sde.jsonl'):
        released = r.get('releaseDate')
        connection.execute(insert(tbl).values(
            buildNumber = r['buildNumber'],
            # Stored naive (UTC); the source is always 'Z'-suffixed.
            releaseDate = datetime.fromisoformat(released).replace(tzinfo=None) if released else None,
        ))
        _log("    build {} released {}".format(r['buildNumber'], released))
    trans.commit()
