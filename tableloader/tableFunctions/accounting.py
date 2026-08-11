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


def _log(msg):
    from datetime import datetime
    print(f"[{datetime.now():%H:%M:%S}] {msg}")

def import_accounting_entry_types(connection, metadata, sourcePath, language='en'):
    """accountingEntryTypes.jsonl -> acctEntryTypes + trnTranslations"""
    _log("Importing accountingEntryTypes")
    tbl      = Table('acctEntryTypes', metadata)
    trn      = Table('trnTranslations', metadata)
    trn_cols = Table('trnTranslationColumns', metadata)
    trans = connection.begin()
    connection.execute(insert(trn_cols).values(tcGroupID=None, tcID=35, tableName='acctEntryTypes', columnName='name',           masterID='entryTypeID'))
    connection.execute(insert(trn_cols).values(tcGroupID=None, tcID=36, tableName='acctEntryTypes', columnName='journalMessage', masterID='entryTypeID'))
    count = 0
    for r in _jsonl(sourcePath, 'accountingEntryTypes.jsonl'):
        eid  = r['_key']
        name = r.get('name') or {}
        msg  = r.get('journalMessage') or {}
        connection.execute(insert(tbl).values(
            entryTypeID    = eid,
            internalName   = r.get('internalName'),
            name           = _en(name, language),
            journalMessage = _en(msg, language),
        ))
        count += 1
        if isinstance(name, dict):
            for lang, text in name.items():
                connection.execute(insert(trn).values(tcID=35, keyID=eid, languageID=lang, text=text))
        if isinstance(msg, dict):
            for lang, text in msg.items():
                connection.execute(insert(trn).values(tcID=36, keyID=eid, languageID=lang, text=text))
    trans.commit()
    _log("    {} rows".format(count))
