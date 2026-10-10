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

def import_corporation_activities(connection, metadata, sourcePath, language='en'):
    """corporationActivities.jsonl -> crpActivities"""
    _log("Importing corporationActivities")
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
    _log("    {} rows".format(count))


def import_npc_corporation_divisions(connection, metadata, sourcePath, language='en'):
    """npcCorporationDivisions.jsonl -> crpNPCDivisions"""
    _log("Importing npcCorporationDivisions")
    tbl = Table('crpNPCDivisions', metadata)
    trnTranslations = Table('trnTranslations', metadata)
    trnTranslationColumns = Table('trnTranslationColumns', metadata)
    trans = connection.begin()
    connection.execute(insert(trnTranslationColumns).values(tcGroupID=None, tcID=24, tableName='crpNPCDivisions', columnName='divisionName', masterID='divisionID'))
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
        crpNPCCorporationLPOfferTables
        crpNPCCorporationAllowedRaces
        crpNPCCorporationExchangeRates
        trnTranslations
    """
    _log("Importing npcCorporations")

    tbl                   = Table('crpNPCCorporations', metadata)
    tblDivs               = Table('crpNPCCorporationDivisions', metadata)
    tblTrades             = Table('crpNPCCorporationTrades', metadata)
    tblLPOffers           = Table('crpNPCCorporationLPOfferTables', metadata)
    tblRaces              = Table('crpNPCCorporationAllowedRaces', metadata)
    tblExchange           = Table('crpNPCCorporationExchangeRates', metadata)
    trnTranslations       = Table('trnTranslations', metadata)
    trnTranslationColumns = Table('trnTranslationColumns', metadata)

    trans = connection.begin()
    connection.execute(insert(trnTranslationColumns).values(tcGroupID=None, tcID=20, tableName='crpNPCCorporations', columnName='corporationName', masterID='corporationID'))
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
            tickerName         = r.get('tickerName'),
            ceoID              = r.get('ceoID'),
            mainActivityID     = r.get('mainActivityID'),
            secondaryActivityID = r.get('secondaryActivityID'),
            taxRate            = r.get('taxRate'),
            **inv,
        ))

        for div in r.get('divisions', []):
            connection.execute(insert(tblDivs).values(
                corporationID = corpID,
                divisionID    = div['_key'],
                size          = div.get('size'),
                divisionNumber = div.get('divisionNumber'),
                leaderID      = div.get('leaderID'),
            ))

        seen_trades = set()
        for trade in r.get('corporationTrades', []):
            typeID = trade['_key']
            if typeID not in seen_trades:
                seen_trades.add(typeID)
                connection.execute(insert(tblTrades).values(
                    corporationID = corpID,
                    typeID        = typeID,
                    value         = trade.get('_value'),
                ))

        for lpTableID in r.get('lpOfferTables') or []:
            connection.execute(insert(tblLPOffers).values(corporationID=corpID, lpOfferTableID=lpTableID))

        for raceID in r.get('allowedMemberRaces') or []:
            connection.execute(insert(tblRaces).values(corporationID=corpID, raceID=raceID))

        for rate in r.get('exchangeRates') or []:
            connection.execute(insert(tblExchange).values(
                corporationID   = corpID,
                toCorporationID = rate['_key'],
                rate            = rate['_value'],
            ))

        corps += 1
        for lang, text in r.get('name', {}).items():
            connection.execute(insert(trnTranslations).values(
                tcID=20, keyID=corpID, languageID=lang, text=text))

    trans.commit()
    _log("    {} corporations imported".format(corps))
