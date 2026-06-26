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


def _en(d, language='en'):
    if isinstance(d, dict):
        return d.get(language) or d.get('en')
    return d


def import_character_titles(connection, metadata, sourcePath, language='en'):
    """characterTitles.jsonl -> chrTitles + trnTranslations"""
    print("Importing characterTitles")
    tbl             = Table('chrTitles', metadata)
    trnTranslations = Table('trnTranslations', metadata)
    trans = connection.begin()
    count = 0
    for r in _jsonl(sourcePath, 'characterTitles.jsonl'):
        connection.execute(insert(tbl).values(
            titleID   = r['_key'],
            titleName = _en(r.get('name', {}), language),
        ))
        count += 1
    trans.commit()
    print("    {} rows".format(count))
