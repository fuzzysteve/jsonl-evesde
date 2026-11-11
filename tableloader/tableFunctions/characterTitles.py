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


def _en(d, language='en'):
    if isinstance(d, dict):
        return d.get(language) or d.get('en')
    return d


def _log(msg):
    from datetime import datetime
    print(f"[{datetime.now():%H:%M:%S}] {msg}")

def import_character_titles(connection, metadata, sourcePath, language='en'):
    """characterTitles.jsonl -> chrTitles + trnTranslations"""
    _log("Importing characterTitles")
    tbl             = Table('chrTitles', metadata)
    trnTranslations = Table('trnTranslations', metadata)
    trans = connection.begin()
    count = 0
    for r in _jsonl(sourcePath, 'characterTitles.jsonl'):
        connection.execute(insert(tbl).values(
            titleID   = r['titleID'],
            titleName = _en(r.get('name', {}), language),
        ))
        count += 1
    trans.commit()
    _log("    {} rows".format(count))
