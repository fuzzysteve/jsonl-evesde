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


def import_corporation_activities(connection, metadata, sourcePath, language='en'):
    """corporationActivities.jsonl -> crpActivities"""
    print("Importing corporationActivities")
    tbl = Table('crpActivities', metadata)
    trans = connection.begin()
    count = 0
    for r in _jsonl(sourcePath, 'corporationActivities.jsonl'):
        connection.execute(insert(tbl).values(
            activityID   = r['_key'],
            activityName = _en(r.get('name', {}), language),
            description  = None,
        ))
        count += 1
    trans.commit()
    print("    {} rows".format(count))


def import_npc_corporation_divisions(connection, metadata, sourcePath, language='en'):
    """npcCorporationDivisions.jsonl -> crpNPCDivisions"""
    print("Importing npcCorporationDivisions")
    tbl = Table('crpNPCDivisions', metadata)
    trnTranslations = Table('trnTranslations', metadata)
    trans = connection.begin()
    for r in _jsonl(sourcePath, 'npcCorporationDivisions.jsonl'):
        connection.execute(insert(tbl).values(
            divisionID   = r['_key'],
            divisionName = _en(r.get('name', {}), language),
            description  = _en(r.get('description', {}), language),
            leaderType   = _en(r.get('leaderTypeName', {}), language),
        ))
        for lang, text in r.get('name', {}).items():
            connection.execute(insert(trnTranslations).values(
                tcID=24, keyID=r['_key'], languageID=lang, text=text))
    trans.commit()


def import_npc_corporations(connection, metadata, sourcePath, language='en'):
    """
    npcCorporations.jsonl ->
        crpNPCCorporations
        crpNPCCorporationDivisions
        crpNPCCorporationTrades
        crpNPCCorporationResearchFields (via lpOfferTables — not in source; skipped)
        trnTranslations
    """
    print("Importing npcCorporations")

    tbl        = Table('crpNPCCorporations', metadata)
    tblDivs    = Table('crpNPCCorporationDivisions', metadata)
    tblTrades  = Table('crpNPCCorporationTrades', metadata)
    trnTranslations = Table('trnTranslations', metadata)

    trans = connection.begin()
    corps = 0
    for r in _jsonl(sourcePath, 'npcCorporations.jsonl'):
        corpID = r['_key']

        # Investors: table has 4 fixed slots
        investors = r.get('investors', [])[:4]
        inv = {}
        for i in range(4):
            inv['investorID{}'.format(i + 1)]     = investors[i]['_key']    if i < len(investors) else None
            inv['investorShares{}'.format(i + 1)] = investors[i]['_value']  if i < len(investors) else None

        connection.execute(insert(tbl).values(
            corporationID      = corpID,
            size               = r.get('size'),
            extent             = r.get('extent'),
            solarSystemID      = r.get('solarSystemID'),
            friendID           = r.get('friendID'),
            enemyID            = r.get('enemyID'),
            publicShares       = r.get('shares'),
            initialPrice       = r.get('initialPrice'),
            minSecurity        = r.get('minSecurity'),
            scattered          = None,
            fringe             = None,
            corridor           = None,
            hub                = None,
            border             = None,
            factionID          = r.get('factionID'),
            sizeFactor         = r.get('sizeFactor'),
            stationCount       = None,
            stationSystemCount = None,
            corporationName    = _en(r.get('name', {}), language),
            description        = _en(r.get('description', {}), language),
            iconID             = r.get('iconID'),
            **inv,
        ))

        for div in r.get('divisions', []):
            connection.execute(insert(tblDivs).values(
                corporationID = corpID,
                divisionID    = div['_key'],
                size          = div.get('size'),
            ))

        # corporationTrades: only typeID is stored; the _value (standing modifier) has no column
        seen_trades = set()
        for trade in r.get('corporationTrades', []):
            typeID = trade['_key']
            if typeID not in seen_trades:
                seen_trades.add(typeID)
                connection.execute(insert(tblTrades).values(
                    corporationID = corpID,
                    typeID        = typeID,
                ))

        corps += 1
        for lang, text in r.get('name', {}).items():
            connection.execute(insert(trnTranslations).values(
                tcID=20, keyID=corpID, languageID=lang, text=text))

    trans.commit()
    print("    {} corporations imported".format(corps))
