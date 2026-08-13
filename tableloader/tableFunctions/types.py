# -*- coding: utf-8 -*-

import json
import os
from sqlalchemy import Table, insert

_BATCH = 1000


def _trunc(s, length=100):
    if s is None:
        return None
    if len(s) > length:
        _log("  WARNING: truncating name to {}: '{}'".format(length, s))
        return s[:length]
    return s


def _en(d, language='en'):
    if isinstance(d, dict):
        return d.get(language) or d.get('en')
    return d


def _jsonl(sourcePath, filename):
    filepath = os.path.join(sourcePath, filename)
    with open(filepath, 'r', encoding='utf-8') as fh:
        for line in fh:
            line = line.strip()
            if line:
                yield json.loads(line)


def _log(msg):
    from datetime import datetime
    print(f"[{datetime.now():%H:%M:%S}] {msg}")


def _bulk(connection, stmt, rows):
    for i in range(0, len(rows), _BATCH):
        connection.execute(stmt, rows[i:i + _BATCH])


def import_types(connection, metadata, sourcePath, language='en'):
    invTypes              = Table('invTypes', metadata)
    trnTranslations       = Table('trnTranslations', metadata)
    trnTranslationColumns = Table('trnTranslationColumns', metadata)
    invMetaTypes          = Table('invMetaTypes', metadata)
    _log("Importing Types")
    trans = connection.begin()
    connection.execute(insert(trnTranslationColumns).values(
        tcGroupID=None, tcID=8,  tableName='invTypes', columnName='typeName',    masterID='typeID'))
    connection.execute(insert(trnTranslationColumns).values(
        tcGroupID=None, tcID=33, tableName='invTypes', columnName='description', masterID='typeID'))

    type_rows = []
    meta_rows = []
    trn_rows  = []

    for typedata in _jsonl(sourcePath, 'types.jsonl'):
        type_rows.append(dict(
            typeID          = typedata['_key'],
            groupID         = typedata.get('groupID', 0),
            typeName        = _trunc((typedata.get('name') or {}).get(language, '')),
            description     = (typedata.get('description') or {}).get(language, ''),
            mass            = typedata.get('mass', 0),
            volume          = typedata.get('volume', 0),
            capacity        = typedata.get('capacity', 0),
            portionSize     = typedata.get('portionSize'),
            raceID          = typedata.get('raceID'),
            basePrice       = typedata.get('basePrice'),
            published       = typedata.get('published', 0),
            marketGroupID   = typedata.get('marketGroupID'),
            graphicID       = typedata.get('graphicID', 0),
            iconID          = typedata.get('iconID'),
            soundID         = typedata.get('soundID'),
            factionID       = typedata.get('factionID'),
            metaLevel       = typedata.get('metaLevel'),
            techLevel       = typedata.get('techLevel'),
            shipTreeGroupID = typedata.get('shipTreeGroupID'),
            packagedVolume  = typedata.get('packagedVolume'),
            isDynamicType   = typedata.get('isDynamicType'),
            isRepackable    = typedata.get('isRepackable'),
        ))
        if 'metaGroupID' in typedata or 'variationParentTypeID' in typedata:
            meta_rows.append(dict(
                typeID       = typedata['_key'],
                metaGroupID  = typedata.get('metaGroupID'),
                parentTypeID = typedata.get('variationParentTypeID'),
            ))
        if isinstance(typedata.get('name'), dict):
            for lang, text in typedata['name'].items():
                trn_rows.append(dict(tcID=8, keyID=typedata['_key'], languageID=lang, text=text))
        if isinstance(typedata.get('description'), dict):
            for lang, text in typedata['description'].items():
                trn_rows.append(dict(tcID=33, keyID=typedata['_key'], languageID=lang, text=text))

    _bulk(connection, insert(invTypes), type_rows)
    _bulk(connection, insert(invMetaTypes), meta_rows)
    _bulk(connection, insert(trnTranslations), trn_rows)
    trans.commit()


def import_bonus(connection, metadata, sourcePath, language='en'):
    invTraits = Table('invTraits', metadata)
    _log("Importing bonuses")
    trans = connection.begin()
    rows = []
    for typedata in _jsonl(sourcePath, 'typeBonus.jsonl'):
        key = typedata['_key']
        for rolebonus in typedata.get('roleBonuses', []):
            rows.append(dict(
                typeID    = key,
                skillID   = -1,
                bonus     = rolebonus.get('bonus'),
                bonusText = (rolebonus.get('bonusText') or {}).get(language, ''),
                unitID    = rolebonus.get('unitID'),
            ))
        for skillbonus in typedata.get('types', []):
            for skill in skillbonus['_value']:
                rows.append(dict(
                    typeID    = key,
                    skillID   = skillbonus.get('_key'),
                    bonus     = skill.get('bonus'),
                    bonusText = (skill.get('bonusText') or {}).get(language, ''),
                    unitID    = skill.get('unitID'),
                ))
    _bulk(connection, insert(invTraits), rows)
    trans.commit()


def import_materials(connection, metadata, sourcePath, language='en'):
    invTypeMaterials = Table('invTypeMaterials', metadata)
    _log("Importing materials")
    trans = connection.begin()
    rows = []
    for typedata in _jsonl(sourcePath, 'typeMaterials.jsonl'):
        key = typedata['_key']
        for material in typedata.get('materials', []):
            rows.append(dict(
                typeID         = key,
                materialTypeID = material['materialTypeID'],
                quantity       = material['quantity'],
            ))
    _bulk(connection, insert(invTypeMaterials), rows)
    trans.commit()


def import_dogma(connection, metadata, sourcePath, language='en'):
    dgmEffects    = Table('dgmTypeEffects', metadata)
    dgmAttributes = Table('dgmTypeAttributes', metadata)
    _log("Importing type dogma")
    trans = connection.begin()
    attr_rows   = []
    effect_rows = []
    for typedata in _jsonl(sourcePath, 'typeDogma.jsonl'):
        key = typedata['_key']
        for attribute in typedata.get('dogmaAttributes', []):
            attr_rows.append(dict(
                typeID      = key,
                attributeID = attribute.get('attributeID'),
                valueFloat  = attribute.get('value'),
            ))
        for effect in typedata.get('dogmaEffects', []):
            effect_rows.append(dict(
                typeID    = key,
                effectID  = effect.get('effectID'),
                isDefault = effect.get('isDefault'),
            ))
    _bulk(connection, insert(dgmAttributes), attr_rows)
    _bulk(connection, insert(dgmEffects), effect_rows)
    trans.commit()


def import_groups(connection, metadata, sourcePath, language='en'):
    invGroups             = Table('invGroups', metadata)
    trnTranslations       = Table('trnTranslations', metadata)
    trnTranslationColumns = Table('trnTranslationColumns', metadata)
    _log("Importing Groups")
    trans = connection.begin()
    connection.execute(insert(trnTranslationColumns).values(
        tcGroupID=None, tcID=7, tableName='invGroups', columnName='groupName', masterID='groupID'))
    group_rows = []
    trn_rows   = []
    for groupdata in _jsonl(sourcePath, 'groups.jsonl'):
        group_rows.append(dict(
            groupID              = groupdata['_key'],
            anchorable           = groupdata['anchorable'],
            anchored             = groupdata['anchored'],
            categoryID           = groupdata['categoryID'],
            fittableNonSingleton = groupdata['fittableNonSingleton'],
            groupName            = groupdata['name'].get(language, ''),
            published            = groupdata['published'],
            useBasePrice         = groupdata['useBasePrice'],
            iconID               = groupdata.get('iconID'),
        ))
        for lang, text in groupdata.get('name', {}).items():
            trn_rows.append(dict(tcID=7, keyID=groupdata['_key'], languageID=lang, text=text))
    _bulk(connection, insert(invGroups), group_rows)
    _bulk(connection, insert(trnTranslations), trn_rows)
    trans.commit()


def import_categories(connection, metadata, sourcePath, language='en'):
    invCategories         = Table('invCategories', metadata)
    trnTranslations       = Table('trnTranslations', metadata)
    trnTranslationColumns = Table('trnTranslationColumns', metadata)
    _log("Importing Categories")
    trans = connection.begin()
    connection.execute(insert(trnTranslationColumns).values(
        tcGroupID=None, tcID=6, tableName='invCategories', columnName='categoryName', masterID='categoryID'))
    cat_rows = []
    trn_rows = []
    for categorydata in _jsonl(sourcePath, 'categories.jsonl'):
        cat_rows.append(dict(
            categoryID   = categorydata['_key'],
            categoryName = categorydata['name'].get(language, ''),
            published    = categorydata['published'],
            iconID       = categorydata.get('iconID'),
        ))
        for lang, text in categorydata.get('name', {}).items():
            trn_rows.append(dict(tcID=6, keyID=categorydata['_key'], languageID=lang, text=text))
    _bulk(connection, insert(invCategories), cat_rows)
    _bulk(connection, insert(trnTranslations), trn_rows)
    trans.commit()


def import_meta_groups(connection, metadata, sourcePath, language='en'):
    """metaGroups.jsonl -> invMetaGroups"""
    _log("Importing metaGroups")
    tbl                   = Table('invMetaGroups', metadata)
    trnTranslations       = Table('trnTranslations', metadata)
    trnTranslationColumns = Table('trnTranslationColumns', metadata)
    trans = connection.begin()
    connection.execute(insert(trnTranslationColumns).values(
        tcGroupID=None, tcID=15, tableName='invMetaGroups', columnName='metaGroupName', masterID='metaGroupID'))
    meta_rows = []
    trn_rows  = []
    for r in _jsonl(sourcePath, 'metaGroups.jsonl'):
        meta_rows.append(dict(
            metaGroupID   = r['_key'],
            metaGroupName = _en(r.get('name', {}), language),
            description   = _en(r.get('description', {}), language),
            iconID        = r.get('iconID'),
        ))
        for lang, text in r.get('name', {}).items():
            trn_rows.append(dict(tcID=15, keyID=r['_key'], languageID=lang, text=text))
    _bulk(connection, insert(tbl), meta_rows)
    _bulk(connection, insert(trnTranslations), trn_rows)
    trans.commit()


def import_market_groups(connection, metadata, sourcePath, language='en'):
    """marketGroups.jsonl -> invMarketGroups + trnTranslations"""
    _log("Importing marketGroups")
    tbl                   = Table('invMarketGroups', metadata)
    trnTranslations       = Table('trnTranslations', metadata)
    trnTranslationColumns = Table('trnTranslationColumns', metadata)
    trans = connection.begin()
    connection.execute(insert(trnTranslationColumns).values(
        tcGroupID=None, tcID=14, tableName='invMarketGroups', columnName='marketGroupName', masterID='marketGroupID'))
    mkt_rows = []
    trn_rows = []
    for r in _jsonl(sourcePath, 'marketGroups.jsonl'):
        mkt_rows.append(dict(
            marketGroupID   = r['_key'],
            parentGroupID   = r.get('parentGroupID'),
            marketGroupName = _en(r.get('name', {}), language),
            description     = _en(r.get('description', {}), language),
            iconID          = r.get('iconID'),
            hasTypes        = r.get('hasTypes'),
        ))
        for lang, text in r.get('name', {}).items():
            trn_rows.append(dict(tcID=14, keyID=r['_key'], languageID=lang, text=text))
    _bulk(connection, insert(tbl), mkt_rows)
    _bulk(connection, insert(trnTranslations), trn_rows)
    trans.commit()
    _log("    {} rows".format(len(mkt_rows)))
