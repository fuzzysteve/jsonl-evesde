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

def import_fighter_abilities(connection, metadata, sourcePath, language='en'):
    """fighterAbilities.jsonl -> fighterAbilities + trnTranslations"""
    _log("Importing fighterAbilities")
    tbl      = Table('fighterAbilities', metadata)
    trn      = Table('trnTranslations', metadata)
    trn_cols = Table('trnTranslationColumns', metadata)
    trans = connection.begin()
    connection.execute(insert(trn_cols).values(tcGroupID=None, tcID=40, tableName='fighterAbilities', columnName='displayName', masterID='abilityID'))
    connection.execute(insert(trn_cols).values(tcGroupID=None, tcID=41, tableName='fighterAbilities', columnName='tooltipText', masterID='abilityID'))
    count = 0
    for r in _jsonl(sourcePath, 'fighterAbilities.jsonl'):
        aid     = r['_key']
        dname   = r.get('displayName') or {}
        tooltip = r.get('tooltipText') or {}
        connection.execute(insert(tbl).values(
            abilityID        = aid,
            disallowInHighSec= r.get('disallowInHighSec'),
            disallowInLowSec = r.get('disallowInLowSec'),
            displayName      = _en(dname, language),
            iconID           = r.get('iconID'),
            targetMode       = r.get('targetMode'),
            tooltipText      = _en(tooltip, language),
        ))
        count += 1
        if isinstance(dname, dict):
            for lang, text in dname.items():
                connection.execute(insert(trn).values(tcID=40, keyID=aid, languageID=lang, text=text))
        if isinstance(tooltip, dict):
            for lang, text in tooltip.items():
                connection.execute(insert(trn).values(tcID=41, keyID=aid, languageID=lang, text=text))
    trans.commit()
    _log("    {} rows".format(count))


def import_fighter_abilities_by_type(connection, metadata, sourcePath, language='en'):
    """fighterAbilitiesByType.jsonl -> fighterAbilitiesByType"""
    _log("Importing fighterAbilitiesByType")
    tbl = Table('fighterAbilitiesByType', metadata)
    trans = connection.begin()
    count = 0
    for r in _jsonl(sourcePath, 'fighterAbilitiesByType.jsonl'):
        type_id = r['_key']
        for i, slot_key in enumerate(['abilitySlot0', 'abilitySlot1', 'abilitySlot2']):
            slot = r.get(slot_key)
            if slot is None:
                continue
            charges = slot.get('charges') or {}
            connection.execute(insert(tbl).values(
                typeID          = type_id,
                slotNumber      = i,
                abilityID       = slot['abilityID'],
                cooldownSeconds = slot.get('cooldownSeconds'),
                chargeCount     = charges.get('chargeCount'),
                rearmTimeSeconds= charges.get('rearmTimeSeconds'),
            ))
            count += 1
    trans.commit()
    _log("    {} rows".format(count))
