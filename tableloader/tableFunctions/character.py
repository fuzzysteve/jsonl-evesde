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

def import_ancestries(connection, metadata, sourcePath, language='en'):
    """ancestries.jsonl -> chrAncestries + trnTranslations"""
    _log("Importing ancestries")
    tbl = Table('chrAncestries', metadata)
    trnTranslations = Table('trnTranslations', metadata)
    trnTranslationColumns = Table('trnTranslationColumns', metadata)
    trans = connection.begin()
    connection.execute(insert(trnTranslationColumns).values(tcGroupID=None, tcID=12, tableName='chrAncestries', columnName='ancestryName', masterID='ancestryID'))
    count = 0
    for r in _jsonl(sourcePath, 'ancestries.jsonl'):
        connection.execute(insert(tbl).values(
            ancestryID       = r['ancestryID'],
            ancestryName     = _en(r.get('name', {}), language),
            bloodlineID      = r.get('bloodlineID'),
            description      = _en(r.get('description', {}), language),
            perception       = r.get('perception'),
            willpower        = r.get('willpower'),
            charisma         = r.get('charisma'),
            memory           = r.get('memory'),
            intelligence     = r.get('intelligence'),
            iconID           = r.get('iconID'),
            shortDescription = r.get('shortDescription'),
        ))
        for lang, text in r.get('name', {}).items():
            connection.execute(insert(trnTranslations).values(
                tcID=12, keyID=r['ancestryID'], languageID=lang, text=text))
        count += 1
    trans.commit()
    _log("    {} rows".format(count))


def import_bloodlines(connection, metadata, sourcePath, language='en'):
    """bloodlines.jsonl -> chrBloodlines + trnTranslations"""
    _log("Importing bloodlines")
    tbl = Table('chrBloodlines', metadata)
    trnTranslations = Table('trnTranslations', metadata)
    trnTranslationColumns = Table('trnTranslationColumns', metadata)
    trans = connection.begin()
    connection.execute(insert(trnTranslationColumns).values(tcGroupID=None, tcID=11, tableName='chrBloodlines', columnName='bloodlineName', masterID='bloodlineID'))
    count = 0
    for r in _jsonl(sourcePath, 'bloodlines.jsonl'):
        connection.execute(insert(tbl).values(
            bloodlineID            = r['bloodlineID'],
            bloodlineName          = _en(r.get('name', {}), language),
            raceID                 = r.get('raceID'),
            description            = _en(r.get('description', {}), language),
            maleDescription        = None,
            femaleDescription      = None,
            shipTypeID             = r.get('shipTypeID'),
            corporationID          = r.get('corporationID'),
            perception             = r.get('perception'),
            willpower              = r.get('willpower'),
            charisma               = r.get('charisma'),
            memory                 = r.get('memory'),
            intelligence           = r.get('intelligence'),
            iconID                 = r.get('iconID'),
            shortDescription       = None,
            shortMaleDescription   = None,
            shortFemaleDescription = None,
        ))
        for lang, text in r.get('name', {}).items():
            connection.execute(insert(trnTranslations).values(
                tcID=11, keyID=r['bloodlineID'], languageID=lang, text=text))
        count += 1
    trans.commit()
    _log("    {} rows".format(count))


def import_character_attributes(connection, metadata, sourcePath, language='en'):
    """characterAttributes.jsonl -> chrAttributes"""
    _log("Importing characterAttributes")
    tbl = Table('chrAttributes', metadata)
    trans = connection.begin()
    count = 0
    for r in _jsonl(sourcePath, 'characterAttributes.jsonl'):
        connection.execute(insert(tbl).values(
            attributeID      = r['attributeID'],
            attributeName    = _en(r.get('name', {}), language),
            description      = r.get('description'),
            iconID           = r.get('iconID'),
            shortDescription = r.get('shortDescription'),
            notes            = r.get('notes'),
        ))
        count += 1
    trans.commit()
    _log("    {} rows".format(count))


def import_factions(connection, metadata, sourcePath, language='en'):
    """factions.jsonl -> chrFactions + trnTranslations"""
    _log("Importing factions")
    tbl = Table('chrFactions', metadata)
    trnTranslations = Table('trnTranslations', metadata)
    trnTranslationColumns = Table('trnTranslationColumns', metadata)
    trans = connection.begin()
    connection.execute(insert(trnTranslationColumns).values(tcGroupID=None, tcID=19, tableName='chrFactions', columnName='factionName', masterID='factionID'))
    count = 0
    for r in _jsonl(sourcePath, 'factions.jsonl'):
        # raceIDs is a bitmask sum of memberRaces list, matching classic SDE
        raceIDs = sum(r.get('memberRaces', []))
        connection.execute(insert(tbl).values(
            factionID            = r['factionID'],
            factionName          = _en(r.get('name', {}), language),
            description          = _en(r.get('description', {}), language),
            raceIDs              = raceIDs,
            solarSystemID        = r.get('solarSystemID'),
            corporationID        = r.get('corporationID'),
            sizeFactor           = r.get('sizeFactor'),
            stationCount         = None,
            stationSystemCount   = None,
            militiaCorporationID = r.get('militiaCorporationID'),
            iconID               = r.get('iconID'),
            flatLogo             = r.get('flatLogo'),
            flatLogoWithName     = r.get('flatLogoWithName'),
        ))
        for lang, text in r.get('name', {}).items():
            connection.execute(insert(trnTranslations).values(
                tcID=19, keyID=r['factionID'], languageID=lang, text=text))
        count += 1
    trans.commit()
    _log("    {} rows".format(count))


def import_races(connection, metadata, sourcePath, language='en'):
    """races.jsonl -> chrRaces + trnTranslations"""
    _log("Importing races")
    tbl = Table('chrRaces', metadata)
    trnTranslations = Table('trnTranslations', metadata)
    trnTranslationColumns = Table('trnTranslationColumns', metadata)
    trans = connection.begin()
    connection.execute(insert(trnTranslationColumns).values(tcGroupID=None, tcID=16, tableName='chrRaces', columnName='raceName', masterID='raceID'))
    count = 0
    for r in _jsonl(sourcePath, 'races.jsonl'):
        connection.execute(insert(tbl).values(
            raceID           = r['raceID'],
            raceName         = _en(r.get('name', {}), language),
            description      = _en(r.get('description', {}), language),
            iconID           = r.get('iconID'),
            shortDescription = None,
        ))
        for lang, text in r.get('name', {}).items():
            connection.execute(insert(trnTranslations).values(
                tcID=16, keyID=r['raceID'], languageID=lang, text=text))
        count += 1
    trans.commit()
    _log("    {} rows".format(count))
