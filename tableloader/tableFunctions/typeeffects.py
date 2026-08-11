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


def import_applied_proximity_effects(connection, metadata, sourcePath, language='en'):
    """appliedProximityEffects.jsonl -> aplProximityEffects + aplProximityEffectDbuffs"""
    print("Importing appliedProximityEffects")
    tbl_main  = Table('aplProximityEffects', metadata)
    tbl_dbuff = Table('aplProximityEffectDbuffs', metadata)
    trans = connection.begin()
    main = dbuffs = 0
    for r in _jsonl(sourcePath, 'appliedProximityEffects.jsonl'):
        tid = r['_key']
        connection.execute(insert(tbl_main).values(
            typeID       = tid,
            delaySeconds = r.get('delaySeconds'),
            radius       = r.get('radius'),
        ))
        main += 1
        for d in r.get('dbuffs') or []:
            connection.execute(insert(tbl_dbuff).values(typeID=tid, dbuffID=d['_key'], value=d['_value']))
            dbuffs += 1
    trans.commit()
    print("    {} effects, {} dbuff rows".format(main, dbuffs))


def import_proximity_traps(connection, metadata, sourcePath, language='en'):
    """proximityTrap.jsonl -> proximityTraps"""
    print("Importing proximityTrap")
    tbl = Table('proximityTraps', metadata)
    trans = connection.begin()
    count = 0
    for r in _jsonl(sourcePath, 'proximityTrap.jsonl'):
        connection.execute(insert(tbl).values(
            typeID                  = r['_key'],
            dbuffDuration           = r.get('dbuffDuration'),
            showPerimeterLights     = r.get('showPerimeterLights'),
            triggerDelay            = r.get('triggerDelay'),
            triggerFilterTypeListID = r.get('triggerFilterTypeListID'),
            triggerRange            = r.get('triggerRange'),
        ))
        count += 1
    trans.commit()
    print("    {} rows".format(count))


def import_link_with_ship(connection, metadata, sourcePath, language='en'):
    """linkWithShip.jsonl -> linkWithShip + linkWithShipDbuffs"""
    print("Importing linkWithShip")
    tbl_main  = Table('linkWithShip', metadata)
    tbl_dbuff = Table('linkWithShipDbuffs', metadata)
    trans = connection.begin()
    main = dbuffs = 0
    for r in _jsonl(sourcePath, 'linkWithShip.jsonl'):
        tid = r['_key']
        connection.execute(insert(tbl_main).values(
            typeID                      = tid,
            applyPvpFlag                = r.get('applyPvpFlag'),
            canRelink                   = r.get('canRelink'),
            characterEnergyCost         = r.get('characterEnergyCost'),
            dbuffPostLinkDuration       = r.get('dbuffPostLinkDuration'),
            generateCynoInhibitor       = r.get('generateCynoInhibitor'),
            keepDbuffDurationOnLinkBreak= r.get('keepDbuffDurationOnLinkBreak'),
            linkDuration                = r.get('linkDuration'),
            linkEffectGraphicIDOverride = r.get('linkEffectGraphicIDOverride'),
            linkableShipTypeListID      = r.get('linkableShipTypeListID'),
            maxLinkRange                = r.get('maxLinkRange'),
            omegaOnly                   = r.get('omegaOnly'),
            solarsystemInterferenceCost = r.get('solarsystemInterferenceCost'),
        ))
        main += 1
        for d in r.get('dbuffs') or []:
            connection.execute(insert(tbl_dbuff).values(typeID=tid, dbuffID=d['_key'], value=d['_value']))
            dbuffs += 1
    trans.commit()
    print("    {} records, {} dbuff rows".format(main, dbuffs))


def import_system_dbuff_emitters(connection, metadata, sourcePath, language='en'):
    """systemDbuffEmitters.jsonl -> sysDbuffEmitters + sysDbuffEmitterDbuffs"""
    print("Importing systemDbuffEmitters")
    tbl_main  = Table('sysDbuffEmitters', metadata)
    tbl_dbuff = Table('sysDbuffEmitterDbuffs', metadata)
    trans = connection.begin()
    main = dbuffs = 0
    for r in _jsonl(sourcePath, 'systemDbuffEmitters.jsonl'):
        tid = r['_key']
        connection.execute(insert(tbl_main).values(
            typeID           = tid,
            duration         = r.get('duration'),
            excludeProtected = r.get('excludeProtected'),
            interval         = r.get('interval'),
        ))
        main += 1
        for d in r.get('dbuffs') or []:
            connection.execute(insert(tbl_dbuff).values(typeID=tid, dbuffID=d['_key'], value=d['_value']))
            dbuffs += 1
    trans.commit()
    print("    {} emitters, {} dbuff rows".format(main, dbuffs))


def import_system_wide_effects(connection, metadata, sourcePath, language='en'):
    """systemWideEffects.jsonl -> sysWideEffects + sysWideEffectDbuffs"""
    print("Importing systemWideEffects")
    tbl_main  = Table('sysWideEffects', metadata)
    tbl_dbuff = Table('sysWideEffectDbuffs', metadata)
    trans = connection.begin()
    main = dbuffs = 0
    for r in _jsonl(sourcePath, 'systemWideEffects.jsonl'):
        ssid = r['_key']
        connection.execute(insert(tbl_main).values(
            solarSystemID      = ssid,
            eligibleTypeListID = r.get('eligibleTypeListID'),
        ))
        main += 1
        for d in r.get('dbuffs') or []:
            connection.execute(insert(tbl_dbuff).values(solarSystemID=ssid, dbuffID=d['_key'], value=d['_value']))
            dbuffs += 1
    trans.commit()
    print("    {} systems, {} dbuff rows".format(main, dbuffs))


def import_metenox_moon_drills(connection, metadata, sourcePath, language='en'):
    """metenoxMoonDrill.jsonl -> metenoxMoonDrills"""
    print("Importing metenoxMoonDrill")
    tbl = Table('metenoxMoonDrills', metadata)
    trans = connection.begin()
    count = 0
    for r in _jsonl(sourcePath, 'metenoxMoonDrill.jsonl'):
        connection.execute(insert(tbl).values(
            typeID                   = r['_key'],
            miningCycleTime          = r.get('miningCycleTime'),
            miningEfficiency         = r.get('miningEfficiency'),
            reagentsConsumedPerCycle = r.get('reagentsConsumedPerCycle'),
        ))
        count += 1
    trans.commit()
    print("    {} rows".format(count))
