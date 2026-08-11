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


def import_notification_types(connection, metadata, sourcePath, language='en'):
    """notificationTypes.jsonl -> ntfTypes + trnTranslations"""
    print("Importing notificationTypes")
    tbl      = Table('ntfTypes', metadata)
    trn      = Table('trnTranslations', metadata)
    trn_cols = Table('trnTranslationColumns', metadata)
    trans = connection.begin()
    connection.execute(insert(trn_cols).values(tcGroupID=None, tcID=48, tableName='ntfTypes', columnName='displayName', masterID='typeID'))
    count = 0
    for r in _jsonl(sourcePath, 'notificationTypes.jsonl'):
        tid   = r['_key']
        dname = r.get('displayName') or {}
        connection.execute(insert(tbl).values(
            typeID       = tid,
            internalName = r.get('internalName'),
            displayName  = _en(dname, language),
        ))
        count += 1
        if isinstance(dname, dict):
            for lang, text in dname.items():
                connection.execute(insert(trn).values(tcID=48, keyID=tid, languageID=lang, text=text))
    trans.commit()
    print("    {} rows".format(count))
