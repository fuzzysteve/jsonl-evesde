# -*- coding: utf-8 -*-

import json
import os
from sqlalchemy import Table, insert, select, text

_BATCH = 1000

# ---------------------------------------------------------------------------
# groupID lookup cache — populated once per process from the DB
# ---------------------------------------------------------------------------
_typeidcache = {}


def _grouplookup(connection, metadata, typeid):
    if typeid in _typeidcache:
        return _typeidcache[typeid]
    invTypes = Table('invTypes', metadata)
    try:
        row = connection.execute(
            select(invTypes.c.groupID).where(invTypes.c.typeID == typeid)
        ).fetchone()
        groupid = row[0] if row else -1
    except Exception:
        _log("Group lookup failed on typeID {}".format(typeid))
        groupid = -1
    _typeidcache[typeid] = groupid
    return groupid


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


def _name(record, language='en'):
    n = record.get('name', {})
    if isinstance(n, dict):
        return n.get(language) or n.get('en')
    return n


def _trunc(s, length=100):
    if s is None:
        return None
    if len(s) > length:
        _log("  WARNING: truncating name to {}: '{}'".format(length, s))
        return s[:length]
    return s


def _log(msg):
    from datetime import datetime
    print(f"[{datetime.now():%H:%M:%S}] {msg}")


def _bulk(connection, stmt, rows):
    for i in range(0, len(rows), _BATCH):
        connection.execute(stmt, rows[i:i + _BATCH])


def _norm(row_dict, col_keys):
    """Normalise a dict to exactly col_keys, filling missing keys with None.

    Required for executemany: all rows in a batch must have identical keys.
    """
    return {k: row_dict.get(k) for k in col_keys}


def import_map(connection, metadata, sourcePath, language='en'):
    _log("Importing map tables")

    mapRegions                 = Table('mapRegions', metadata)
    mapConstellations          = Table('mapConstellations', metadata)
    mapSolarSystems            = Table('mapSolarSystems', metadata)
    mapCelestialStatistics     = Table('mapCelestialStatistics', metadata)
    mapCelestialGraphics       = Table('mapCelestialGraphics', metadata)
    mapDenormalize             = Table('mapDenormalize', metadata)
    mapJumps                   = Table('mapJumps', metadata)
    mapLocationWormholeClasses = Table('mapLocationWormholeClasses', metadata)

    stat_col_keys    = {c.key for c in mapCelestialStatistics.c}
    graphic_col_keys = {c.key for c in mapCelestialGraphics.c}

    # Pre-populate the groupID cache so planet/moon/belt/stargate loops
    # never hit the DB per-row.
    _log("  Building lookups")
    invTypes_tbl = Table('invTypes', metadata)
    for row in connection.execute(
        select(invTypes_tbl.c.typeID, invTypes_tbl.c.groupID)
    ).fetchall():
        _typeidcache[row[0]] = row[1]
    # Close the autobegin opened by the SELECT above.
    connection.commit()

    region_faction = {}
    const_faction  = {}
    sys_info       = {}
    star_lookup    = {}
    dest_sys_name  = {}

    for r in _jsonl(sourcePath, 'mapRegions.jsonl'):
        region_faction[r['_key']] = r.get('factionID')

    for r in _jsonl(sourcePath, 'mapConstellations.jsonl'):
        rid = r.get('regionID')
        const_faction[r['_key']] = r.get('factionID') or region_faction.get(rid)

    for r in _jsonl(sourcePath, 'mapSolarSystems.jsonl'):
        sysName = _name(r, language)
        sys_info[r['_key']] = (
            r.get('regionID'),
            r.get('constellationID'),
            r.get('securityStatus'),
            sysName,
        )
        dest_sys_name[r['_key']] = sysName

    for r in _jsonl(sourcePath, 'mapStars.jsonl'):
        star_lookup[r.get('solarSystemID')] = r['_key']

    # Row accumulators
    region_rows  = []
    const_rows   = []
    sys_rows     = []
    stat_rows    = []   # mapCelestialStatistics — all celestial types combined
    graphic_rows = []   # mapCelestialGraphics   — planets + moons
    den_rows     = []   # mapDenormalize          — all entries combined
    jump_rows    = []   # mapJumps
    wh_rows      = []   # mapLocationWormholeClasses

    # ------------------------------------------------------------------
    # mapRegions
    # ------------------------------------------------------------------
    _log("  Importing mapRegions")
    for r in _jsonl(sourcePath, 'mapRegions.jsonl'):
        p          = r.get('position', {})
        regionName = _trunc(_name(r, language))
        region_rows.append(dict(
            regionID   = r['_key'],
            regionName = regionName,
            x=p.get('x'), y=p.get('y'), z=p.get('z'),
            xMin=None, xMax=None, yMin=None, yMax=None, zMin=None, zMax=None,
            factionID  = r.get('factionID'),
            nebula     = r.get('nebulaID'),
            radius     = None,
        ))
        den_rows.append(dict(
            itemID   = r['_key'],
            typeID   = 3,
            groupID  = 3,
            regionID = r['_key'],
            itemName = regionName,
            x=p.get('x'), y=p.get('y'), z=p.get('z'),
        ))
        if r.get('wormholeClassID') is not None:
            wh_rows.append(dict(locationID=r['_key'], wormholeClassID=r['wormholeClassID']))
    _log("    {} rows".format(len(region_rows)))

    # ------------------------------------------------------------------
    # mapConstellations
    # ------------------------------------------------------------------
    _log("  Importing mapConstellations")
    for r in _jsonl(sourcePath, 'mapConstellations.jsonl'):
        p         = r.get('position', {})
        rid       = r.get('regionID')
        faction   = r.get('factionID') or region_faction.get(rid)
        constName = _trunc(_name(r, language))
        const_rows.append(dict(
            regionID          = rid,
            constellationID   = r['_key'],
            constellationName = constName,
            x=p.get('x'), y=p.get('y'), z=p.get('z'),
            xMin=None, xMax=None, yMin=None, yMax=None, zMin=None, zMax=None,
            factionID = faction,
            radius    = None,
        ))
        den_rows.append(dict(
            itemID          = r['_key'],
            typeID          = 4,
            groupID         = 4,
            regionID        = rid,
            constellationID = r['_key'],
            itemName        = constName,
            x=p.get('x'), y=p.get('y'), z=p.get('z'),
        ))
        if r.get('wormholeClassID') is not None:
            wh_rows.append(dict(locationID=r['_key'], wormholeClassID=r['wormholeClassID']))
    _log("    {} rows".format(len(const_rows)))

    # ------------------------------------------------------------------
    # mapSolarSystems
    # ------------------------------------------------------------------
    _log("  Importing mapSolarSystems")
    for r in _jsonl(sourcePath, 'mapSolarSystems.jsonl'):
        p   = r.get('position', {})
        p2d = r.get('position2D', {})
        rid, cid, sec, sysName = sys_info.get(r['_key'], (None, None, None, None))
        sysName = sysName or _name(r, language)
        faction = r.get('factionID') or const_faction.get(cid) or region_faction.get(rid)
        sys_rows.append(dict(
            regionID        = rid,
            constellationID = cid,
            solarSystemID   = r['_key'],
            solarSystemName = _trunc(sysName),
            x=p.get('x'), y=p.get('y'), z=p.get('z'),
            xMin=p2d.get('x'), xMax=None,
            yMin=p2d.get('y'), yMax=None,
            zMin=None,         zMax=None,
            luminosity    = r.get('luminosity'),
            border        = r.get('border', False),
            fringe        = r.get('fringe', False),
            corridor      = r.get('corridor', False),
            hub           = r.get('hub', False),
            international = r.get('international', False),
            regional      = r.get('regional', False),
            constellation = None,
            security      = sec,
            factionID     = faction,
            radius        = r.get('radius'),
            sunTypeID     = r.get('starID'),
            securityClass = r.get('securityClass'),
            position2Dx   = p2d.get('x'),
            position2Dy   = p2d.get('y'),
        ))
        den_rows.append(dict(
            itemID          = r['_key'],
            typeID          = 5,
            groupID         = 5,
            solarSystemID   = r['_key'],
            constellationID = cid,
            regionID        = rid,
            x=p.get('x'), y=p.get('y'), z=p.get('z'),
            radius          = r.get('radius'),
            itemName        = _trunc(sysName),
            security        = sec,
        ))
        if r.get('wormholeClassID') is not None:
            wh_rows.append(dict(locationID=r['_key'], wormholeClassID=r['wormholeClassID']))
    _log("    {} rows".format(len(sys_rows)))

    # ------------------------------------------------------------------
    # mapStars -> mapCelestialStatistics + mapDenormalize
    # ------------------------------------------------------------------
    _log("  Importing mapStars")
    star_count = 0
    for r in _jsonl(sourcePath, 'mapStars.jsonl'):
        sid = r.get('solarSystemID')
        rid, cid, sec, sysName = sys_info.get(sid, (None, None, None, None))
        stats = dict(r.get('statistics') or {})
        stats['celestialID'] = r['_key']
        stat_rows.append(_norm(stats, stat_col_keys))
        den_rows.append(dict(
            itemID          = r['_key'],
            typeID          = r.get('typeID'),
            groupID         = 6,
            solarSystemID   = sid,
            constellationID = cid,
            regionID        = rid,
            x=0, y=0, z=0,
            radius          = r.get('radius'),
            itemName        = _trunc(sysName),
            security        = sec,
        ))
        star_count += 1
    _log("    {} rows".format(star_count))

    # ------------------------------------------------------------------
    # mapPlanets -> mapCelestialStatistics + mapCelestialGraphics + mapDenormalize
    # ------------------------------------------------------------------
    _log("  Importing mapPlanets")
    planet_count = 0
    for r in _jsonl(sourcePath, 'mapPlanets.jsonl'):
        sid   = r.get('solarSystemID')
        rid, cid, sec, sysName = sys_info.get(sid, (None, None, None, None))
        p     = r.get('position', {})
        stats = dict(r.get('statistics') or {})
        attrs = r.get('attributes') or {}
        stats['celestialID'] = r['_key']
        stat_rows.append(_norm(stats, stat_col_keys))
        graphic_rows.append(_norm(dict(
            celestialID  = r['_key'],
            heightMap1   = attrs.get('heightMap1'),
            heightMap2   = attrs.get('heightMap2'),
            shaderPreset = attrs.get('shaderPreset'),
            population   = attrs.get('population'),
        ), graphic_col_keys))
        unique = r.get('uniqueName') or {}
        if isinstance(unique, dict) and unique.get(language):
            itemName = unique.get(language) or unique.get('en')
        else:
            itemName = '{} {}'.format(sysName or '', r.get('celestialIndex', ''))
        den_rows.append(dict(
            itemID          = r['_key'],
            typeID          = r.get('typeID'),
            groupID         = _grouplookup(connection, metadata, r.get('typeID')),
            solarSystemID   = sid,
            constellationID = cid,
            regionID        = rid,
            orbitID         = star_lookup.get(sid),
            x=p.get('x'), y=p.get('y'), z=p.get('z'),
            radius          = r.get('radius'),
            itemName        = _trunc(itemName),
            security        = sec,
            celestialIndex  = r.get('celestialIndex'),
        ))
        planet_count += 1
    _log("    {} rows".format(planet_count))

    # ------------------------------------------------------------------
    # mapMoons -> mapCelestialStatistics + mapCelestialGraphics + mapDenormalize
    # ------------------------------------------------------------------
    _log("  Importing mapMoons")
    moon_count = 0
    for r in _jsonl(sourcePath, 'mapMoons.jsonl'):
        sid   = r.get('solarSystemID')
        rid, cid, sec, sysName = sys_info.get(sid, (None, None, None, None))
        p     = r.get('position', {})
        stats = dict(r.get('statistics') or {})
        attrs = r.get('attributes') or {}
        stats['celestialID'] = r['_key']
        stat_rows.append(_norm(stats, stat_col_keys))
        graphic_rows.append(_norm(dict(
            celestialID  = r['_key'],
            heightMap1   = attrs.get('heightMap1'),
            heightMap2   = attrs.get('heightMap2'),
            shaderPreset = attrs.get('shaderPreset'),
            population   = None,
        ), graphic_col_keys))
        orbitName = '{} {}'.format(sysName or '', r.get('celestialIndex', ''))
        itemName  = '{} - Moon {}'.format(orbitName, r.get('orbitIndex'))
        den_rows.append(dict(
            itemID          = r['_key'],
            typeID          = r.get('typeID'),
            groupID         = _grouplookup(connection, metadata, r.get('typeID')),
            solarSystemID   = sid,
            constellationID = cid,
            regionID        = rid,
            orbitID         = r.get('orbitID'),
            x=p.get('x'), y=p.get('y'), z=p.get('z'),
            radius          = r.get('radius'),
            itemName        = _trunc(itemName),
            security        = sec,
            celestialIndex  = r.get('celestialIndex'),
            orbitIndex      = r.get('orbitIndex'),
        ))
        moon_count += 1
    _log("    {} rows".format(moon_count))

    # ------------------------------------------------------------------
    # mapAsteroidBelts -> mapCelestialStatistics + mapDenormalize
    # ------------------------------------------------------------------
    _log("  Importing mapAsteroidBelts")
    belt_count = 0
    for r in _jsonl(sourcePath, 'mapAsteroidBelts.jsonl'):
        sid   = r.get('solarSystemID')
        rid, cid, sec, sysName = sys_info.get(sid, (None, None, None, None))
        p     = r.get('position', {})
        stats = dict(r.get('statistics') or {})
        stats['celestialID'] = r['_key']
        stat_rows.append(_norm(stats, stat_col_keys))
        unique = r.get('uniqueName') or {}
        if isinstance(unique, dict) and unique.get(language):
            itemName = unique.get(language) or unique.get('en')
        else:
            orbitName = '{} {}'.format(sysName or '', r.get('celestialIndex', ''))
            itemName  = '{} - Asteroid Belt {}'.format(orbitName, r.get('orbitIndex'))
        den_rows.append(dict(
            itemID          = r['_key'],
            typeID          = r.get('typeID'),
            groupID         = _grouplookup(connection, metadata, r.get('typeID')),
            solarSystemID   = sid,
            constellationID = cid,
            regionID        = rid,
            orbitID         = r.get('orbitID'),
            x=p.get('x'), y=p.get('y'), z=p.get('z'),
            radius          = r.get('radius'),
            itemName        = _trunc(itemName),
            security        = sec,
            celestialIndex  = r.get('celestialIndex'),
            orbitIndex      = r.get('orbitIndex'),
        ))
        belt_count += 1
    _log("    {} rows".format(belt_count))

    # ------------------------------------------------------------------
    # mapStargates -> mapJumps + mapDenormalize
    # ------------------------------------------------------------------
    _log("  Importing mapStargates")
    gate_count = 0
    for r in _jsonl(sourcePath, 'mapStargates.jsonl'):
        sid  = r.get('solarSystemID')
        rid, cid, sec, sysName = sys_info.get(sid, (None, None, None, None))
        p    = r.get('position', {})
        dest = r.get('destination') or {}
        dest_sys_id = dest.get('solarSystemID')
        dest_name   = dest_sys_name.get(dest_sys_id, str(dest_sys_id))
        jump_rows.append(dict(
            stargateID    = r['_key'],
            destinationID = dest.get('stargateID'),
        ))
        den_rows.append(dict(
            itemID          = r['_key'],
            typeID          = r.get('typeID'),
            groupID         = _grouplookup(connection, metadata, r.get('typeID')),
            solarSystemID   = sid,
            constellationID = cid,
            regionID        = rid,
            x=p.get('x'), y=p.get('y'), z=p.get('z'),
            itemName        = _trunc('Stargate ({})'.format(dest_name)),
            security        = sec,
        ))
        gate_count += 1
    _log("    {} rows".format(gate_count))

    # ------------------------------------------------------------------
    # mapSecondarySuns -> mapDenormalize
    # ------------------------------------------------------------------
    _log("  Importing mapSecondarySuns")
    sec_sun_count = 0
    for r in _jsonl(sourcePath, 'mapSecondarySuns.jsonl'):
        sid = r.get('solarSystemID')
        rid, cid, sec, sysName = sys_info.get(sid, (None, None, None, None))
        p = r.get('position', {})
        den_rows.append(dict(
            itemID          = r['_key'],
            typeID          = r.get('typeID'),
            groupID         = 995,
            solarSystemID   = sid,
            constellationID = cid,
            regionID        = rid,
            x=p.get('x'), y=p.get('y'), z=p.get('z'),
            itemName        = 'Unknown Anomaly',
            security        = 0,
        ))
        sec_sun_count += 1
    _log("    {} rows".format(sec_sun_count))

    # ------------------------------------------------------------------
    # Bulk insert everything
    # ------------------------------------------------------------------
    trans = connection.begin()
    _bulk(connection, insert(mapRegions), region_rows)
    _bulk(connection, insert(mapConstellations), const_rows)
    _bulk(connection, insert(mapSolarSystems), sys_rows)
    _bulk(connection, insert(mapCelestialStatistics), stat_rows)
    _bulk(connection, insert(mapCelestialGraphics), graphic_rows)
    _bulk(connection, insert(mapJumps), jump_rows)
    _bulk(connection, insert(mapDenormalize), den_rows)
    _bulk(connection, insert(mapLocationWormholeClasses), wh_rows)
    trans.commit()
    _log("Map import complete")


def import_npc_stations(connection, metadata, sourcePath, language='en'):
    """
    Populates staStations and mapDenormalize for NPC stations.
    """
    _log("Importing npcStations")

    staStations    = Table('staStations', metadata)
    mapDenormalize = Table('mapDenormalize', metadata)

    # Selects before begin() — avoids autobegin/begin() conflict.
    mapSolarSystems = Table('mapSolarSystems', metadata)
    rows = connection.execute(
        select(
            mapSolarSystems.c.solarSystemID,
            mapSolarSystems.c.constellationID,
            mapSolarSystems.c.regionID,
            mapSolarSystems.c.security,
        )
    ).fetchall()
    sys_info = {row[0]: (row[1], row[2], row[3]) for row in rows}

    orbit_rows = connection.execute(
        select(mapDenormalize.c.itemID, mapDenormalize.c.itemName)
    ).fetchall()
    orbit_names = {row[0]: row[1] for row in orbit_rows}

    connection.commit()

    corp_names = {}
    for r in _jsonl(sourcePath, 'npcCorporations.jsonl'):
        corp_names[r['_key']] = _name(r, language)

    op_names = {}
    for r in _jsonl(sourcePath, 'stationOperations.jsonl'):
        op_names[r['_key']] = _en(r.get('operationName') or {}, language)

    station_rows = []
    station_den_rows = []

    for r in _jsonl(sourcePath, 'npcStations.jsonl'):
        stationID = r['_key']
        sid       = r.get('solarSystemID')
        const_id, region_id, sec = sys_info.get(sid, (None, None, None))
        p         = r.get('position', {})
        typeID    = r.get('typeID')
        ownerID   = r.get('ownerID')
        opID      = r.get('operationID')

        orbitID   = r.get('orbitID')
        orbitName = orbit_names.get(orbitID, '')
        corpName  = corp_names.get(ownerID, '')
        opName    = op_names.get(opID, '')

        if r.get('useOperationName'):
            stationName = '{} - {} {}'.format(orbitName, corpName, opName).strip()
        else:
            stationName = '{} - {}'.format(orbitName, corpName).strip()

        stationName = _trunc(stationName)

        station_rows.append(dict(
            stationID                   = stationID,
            security                    = sec,
            dockingCostPerVolume        = None,
            maxShipVolumeDockable       = None,
            officeRentalCost            = None,
            operationID                 = opID,
            stationTypeID               = typeID,
            corporationID               = ownerID,
            solarSystemID               = sid,
            constellationID             = const_id,
            regionID                    = region_id,
            stationName                 = stationName,
            x                           = p.get('x'),
            y                           = p.get('y'),
            z                           = p.get('z'),
            reprocessingEfficiency      = r.get('reprocessingEfficiency'),
            reprocessingStationsTake    = r.get('reprocessingStationsTake'),
            reprocessingHangarFlag      = r.get('reprocessingHangarFlag'),
        ))
        station_den_rows.append(dict(
            itemID          = stationID,
            typeID          = typeID,
            groupID         = _grouplookup(connection, metadata, typeID),
            solarSystemID   = sid,
            constellationID = const_id,
            regionID        = region_id,
            orbitID         = orbitID,
            x               = p.get('x'),
            y               = p.get('y'),
            z               = p.get('z'),
            itemName        = stationName,
            security        = sec,
            celestialIndex  = r.get('celestialIndex'),
            orbitIndex      = r.get('orbitIndex'),
        ))

    trans = connection.begin()
    _bulk(connection, insert(staStations), station_rows)
    _bulk(connection, insert(mapDenormalize), station_den_rows)
    trans.commit()
    _log("    {} rows".format(len(station_rows)))


def import_station_services(connection, metadata, sourcePath, language='en'):
    """stationServices.jsonl -> staServices"""
    _log("Importing stationServices")
    tbl = Table('staServices', metadata)
    trans = connection.begin()
    rows = []
    for r in _jsonl(sourcePath, 'stationServices.jsonl'):
        rows.append(dict(
            serviceID   = r['_key'],
            serviceName = _en(r.get('serviceName') or {}, language),
            description = _en(r.get('description') or {}, language),
        ))
    _bulk(connection, insert(tbl), rows)
    trans.commit()
    _log("    {} rows".format(len(rows)))


def import_landmarks(connection, metadata, sourcePath, language='en'):
    """landmarks.jsonl -> mapLandmarks"""
    _log("Importing landmarks")
    tbl = Table('mapLandmarks', metadata)
    trans = connection.begin()
    rows = []
    for r in _jsonl(sourcePath, 'landmarks.jsonl'):
        p = r.get('position', {})
        rows.append(dict(
            landmarkID   = r['_key'],
            landmarkName = _en(r.get('name') or {}, language),
            description  = _en(r.get('description') or {}, language),
            locationID   = r.get('locationID'),
            x            = p.get('x'),
            y            = p.get('y'),
            z            = p.get('z'),
            iconID       = r.get('iconID'),
        ))
    _bulk(connection, insert(tbl), rows)
    trans.commit()
    _log("    {} rows".format(len(rows)))


def buildJumps(connection, connectiontype):
    """
    Populate mapSolarSystemJumps, mapRegionJumps, mapConstellationJumps
    by joining mapJumps against mapDenormalize.
    Call after import_map(). Pass the connectiontype string from Load.py.
    """
    sql = {}
    sql['postgres'] = [
        """insert into "mapSolarSystemJumps" ("fromRegionID","fromConstellationID","fromSolarSystemID","toRegionID","toConstellationID","toSolarSystemID")
        select f."regionID",f."constellationID",f."solarSystemID",t."regionID",t."constellationID",t."solarSystemID"
        from "mapJumps" join "mapDenormalize" f on "mapJumps"."stargateID"=f."itemID" join "mapDenormalize" t on "mapJumps"."destinationID"=t."itemID" """,
        """insert into "mapRegionJumps"
        select distinct f."regionID",t."regionID"
        from "mapJumps" join "mapDenormalize" f on "mapJumps"."stargateID"=f."itemID" join "mapDenormalize" t on "mapJumps"."destinationID"=t."itemID" where f."regionID"!=t."regionID" """,
        """insert into "mapConstellationJumps"
        select distinct f."regionID",f."constellationID",t."constellationID",t."regionID"
        from "mapJumps" join "mapDenormalize" f on "mapJumps"."stargateID"=f."itemID" join "mapDenormalize" t on "mapJumps"."destinationID"=t."itemID" where f."constellationID"!=t."constellationID" """,
    ]
    sql['postgresschema'] = [
        """insert into evesde."mapSolarSystemJumps" ("fromRegionID","fromConstellationID","fromSolarSystemID","toRegionID","toConstellationID","toSolarSystemID")
        select f."regionID",f."constellationID",f."solarSystemID",t."regionID",t."constellationID",t."solarSystemID"
        from evesde."mapJumps" join evesde."mapDenormalize" f on "mapJumps"."stargateID"=f."itemID" join evesde."mapDenormalize" t on "mapJumps"."destinationID"=t."itemID" """,
        """insert into evesde."mapRegionJumps"
        select distinct f."regionID",t."regionID"
        from evesde."mapJumps" join evesde."mapDenormalize" f on "mapJumps"."stargateID"=f."itemID" join evesde."mapDenormalize" t on "mapJumps"."destinationID"=t."itemID" where f."regionID"!=t."regionID" """,
        """insert into evesde."mapConstellationJumps"
        select distinct f."regionID",f."constellationID",t."constellationID",t."regionID"
        from evesde."mapJumps" join evesde."mapDenormalize" f on "mapJumps"."stargateID"=f."itemID" join evesde."mapDenormalize" t on "mapJumps"."destinationID"=t."itemID" where f."constellationID"!=t."constellationID" """,
    ]
    sql['other'] = [
        """insert into mapSolarSystemJumps (fromRegionID,fromConstellationID,fromSolarSystemID,toRegionID,toConstellationID,toSolarSystemID)
        select f.regionID,f.constellationID,f.solarSystemID,t.regionID,t.constellationID,t.solarSystemID
        from mapJumps join mapDenormalize f on mapJumps.stargateID=f.itemID join mapDenormalize t on mapJumps.destinationID=t.itemID""",
        """insert into mapRegionJumps
        select distinct f.regionID,t.regionID
        from mapJumps join mapDenormalize f on mapJumps.stargateID=f.itemID join mapDenormalize t on mapJumps.destinationID=t.itemID where f.regionID!=t.regionID""",
        """insert into mapConstellationJumps
        select distinct f.regionID,f.constellationID,t.constellationID,t.regionID
        from mapJumps join mapDenormalize f on mapJumps.stargateID=f.itemID join mapDenormalize t on mapJumps.destinationID=t.itemID where f.constellationID!=t.constellationID""",
    ]

    if connectiontype in ('sqlite', 'mysql', 'mssql'):
        connectiontype = 'other'

    _log("Building jump tables")
    trans = connection.begin()
    for statement in sql[connectiontype]:
        connection.execute(text(statement))
    trans.commit()
    _log("Jump tables complete")
