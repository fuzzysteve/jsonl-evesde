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


def import_ancestries(connection, metadata, sourcePath, language='en'):
    """ancestries.jsonl -> chrAncestries + trnTranslations"""
    print("Importing ancestries")
    tbl = Table('chrAncestries', metadata)
    trnTranslations = Table('trnTranslations', metadata)
    trans = connection.begin()
    count = 0
    for r in _jsonl(sourcePath, 'ancestries.jsonl'):
        connection.execute(insert(tbl).values(
            ancestryID       = r['_key'],
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
                tcID=12, keyID=r['_key'], languageID=lang, text=text))
        count += 1
    trans.commit()
    print("    {} rows".format(count))


def import_bloodlines(connection, metadata, sourcePath, language='en'):
    """bloodlines.jsonl -> chrBloodlines + trnTranslations"""
    print("Importing bloodlines")
    tbl = Table('chrBloodlines', metadata)
    trnTranslations = Table('trnTranslations', metadata)
    trans = connection.begin()
    count = 0
    for r in _jsonl(sourcePath, 'bloodlines.jsonl'):
        connection.execute(insert(tbl).values(
            bloodlineID            = r['_key'],
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
                tcID=11, keyID=r['_key'], languageID=lang, text=text))
        count += 1
    trans.commit()
    print("    {} rows".format(count))


def import_character_attributes(connection, metadata, sourcePath, language='en'):
    """characterAttributes.jsonl -> chrAttributes"""
    print("Importing characterAttributes")
    tbl = Table('chrAttributes', metadata)
    trans = connection.begin()
    count = 0
    for r in _jsonl(sourcePath, 'characterAttributes.jsonl'):
        connection.execute(insert(tbl).values(
            attributeID      = r['_key'],
            attributeName    = _en(r.get('name', {}), language),
            description      = r.get('description'),
            iconID           = r.get('iconID'),
            shortDescription = r.get('shortDescription'),
            notes            = r.get('notes'),
        ))
        count += 1
    trans.commit()
    print("    {} rows".format(count))


def import_factions(connection, metadata, sourcePath, language='en'):
    """factions.jsonl -> chrFactions + trnTranslations"""
    print("Importing factions")
    tbl = Table('chrFactions', metadata)
    trnTranslations = Table('trnTranslations', metadata)
    trans = connection.begin()
    count = 0
    for r in _jsonl(sourcePath, 'factions.jsonl'):
        # raceIDs is a bitmask sum of memberRaces list, matching classic SDE
        raceIDs = sum(r.get('memberRaces', []))
        connection.execute(insert(tbl).values(
            factionID            = r['_key'],
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
        ))
        for lang, text in r.get('name', {}).items():
            connection.execute(insert(trnTranslations).values(
                tcID=19, keyID=r['_key'], languageID=lang, text=text))
        count += 1
    trans.commit()
    print("    {} rows".format(count))


def import_races(connection, metadata, sourcePath, language='en'):
    """races.jsonl -> chrRaces + trnTranslations"""
    print("Importing races")
    tbl = Table('chrRaces', metadata)
    trnTranslations = Table('trnTranslations', metadata)
    trans = connection.begin()
    count = 0
    for r in _jsonl(sourcePath, 'races.jsonl'):
        connection.execute(insert(tbl).values(
            raceID           = r['_key'],
            raceName         = _en(r.get('name', {}), language),
            description      = _en(r.get('description', {}), language),
            iconID           = r.get('iconID'),
            shortDescription = None,
        ))
        for lang, text in r.get('name', {}).items():
            connection.execute(insert(trnTranslations).values(
                tcID=16, keyID=r['_key'], languageID=lang, text=text))
        count += 1
    trans.commit()
    print("    {} rows".format(count))
