# -*- coding: utf-8 -*-

import json
import os
from sqlalchemy import Table, insert, select, text

# ---------------------------------------------------------------------------
# groupID lookup cache
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
        print("Group lookup failed on typeID {}".format(typeid))
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


def _name(record, language='en'):
    n = record.get('name', {})
    if isinstance(n, dict):
        return n.get(language) or n.get('en')
    return n


def _trunc(s, length=100):
    """Truncate a string to fit a VARCHAR column. Logs if truncation occurs."""
    if s is None:
        return None
    if len(s) > length:
        print("  WARNING: truncating name to {}: '{}'".format(length, s))
        return s[:length]
    return s


def import_map(connection, metadata, sourcePath, language='en'):
    print("Importing map tables")

    mapRegions                 = Table('mapRegions', metadata)
    mapConstellations          = Table('mapConstellations', metadata)
    mapSolarSystems            = Table('mapSolarSystems', metadata)
    mapCelestialStatistics     = Table('mapCelestialStatistics', metadata)
    mapCelestialGraphics       = Table('mapCelestialGraphics', metadata)
    mapDenormalize             = Table('mapDenormalize', metadata)
    mapJumps                   = Table('mapJumps', metadata)
    mapLocationWormholeClasses = Table('mapLocationWormholeClasses', metadata)

    # ------------------------------------------------------------------
    # Build all lookups up front
    # ------------------------------------------------------------------
    print("  Building lookups")

    region_faction = {}   # regionID  -> factionID
    const_faction  = {}   # constID   -> factionID
    sys_info       = {}   # sysID     -> (regionID, constID, security, sysName)
    star_lookup    = {}   # sysID     -> starID
    planet_info    = {}   # planetID  -> (sysID, celestialIndex)  for moon/belt naming
    dest_sys_name  = {}   # sysID     -> sysName  for stargate naming

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

    for r in _jsonl(sourcePath, 'mapPlanets.jsonl'):
        planet_info[r['_key']] = (r.get('solarSystemID'), r.get('celestialIndex'))

    trans = connection.begin()

    # ------------------------------------------------------------------
    # mapRegions
    # ------------------------------------------------------------------
    print("  Importing mapRegions")
    count = 0
    for r in _jsonl(sourcePath, 'mapRegions.jsonl'):
        p = r.get('position', {})
        regionName = _trunc(_name(r, language))
        connection.execute(insert(mapRegions).values(
            regionID   = r['_key'],
            regionName = regionName,
            x=p.get('x'), y=p.get('y'), z=p.get('z'),
            xMin=None, xMax=None, yMin=None, yMax=None, zMin=None, zMax=None,
            factionID  = r.get('factionID'),
            nebula     = r.get('nebulaID'),
            radius     = None,
        ))
        connection.execute(insert(mapDenormalize).values(
            itemID   = r['_key'],
            typeID   = 3,
            groupID  = 3,
            regionID = r['_key'],
            itemName = regionName,
            x=p.get('x'), y=p.get('y'), z=p.get('z'),
        ))
        if r.get('wormholeClassID') is not None:
            connection.execute(insert(mapLocationWormholeClasses).values(
                locationID=r['_key'], wormholeClassID=r['wormholeClassID'],
            ))
        count += 1
    print("    {} rows".format(count))

    # ------------------------------------------------------------------
    # mapConstellations
    # ------------------------------------------------------------------
    print("  Importing mapConstellations")
    count = 0
    for r in _jsonl(sourcePath, 'mapConstellations.jsonl'):
        p   = r.get('position', {})
        rid = r.get('regionID')
        faction = r.get('factionID') or region_faction.get(rid)
        constName = _trunc(_name(r, language))
        connection.execute(insert(mapConstellations).values(
            regionID          = rid,
            constellationID   = r['_key'],
            constellationName = constName,
            x=p.get('x'), y=p.get('y'), z=p.get('z'),
            xMin=None, xMax=None, yMin=None, yMax=None, zMin=None, zMax=None,
            factionID = faction,
            radius    = None,
        ))
        connection.execute(insert(mapDenormalize).values(
            itemID          = r['_key'],
            typeID          = 4,
            groupID         = 4,
            regionID        = rid,
            constellationID = r['_key'],
            itemName        = constName,
            x=p.get('x'), y=p.get('y'), z=p.get('z'),
        ))
        if r.get('wormholeClassID') is not None:
            connection.execute(insert(mapLocationWormholeClasses).values(
                locationID=r['_key'], wormholeClassID=r['wormholeClassID'],
            ))
        count += 1
    print("    {} rows".format(count))

    # ------------------------------------------------------------------
    # mapSolarSystems
    # ------------------------------------------------------------------
    print("  Importing mapSolarSystems")
    count = 0
    for r in _jsonl(sourcePath, 'mapSolarSystems.jsonl'):
        p   = r.get('position', {})
        p2d = r.get('position2D', {})
        rid, cid, sec, sysName = sys_info.get(r['_key'], (None, None, None, None))
        sysName = sysName or _name(r, language)
        faction = r.get('factionID') or const_faction.get(cid) or region_faction.get(rid)
        connection.execute(insert(mapSolarSystems).values(
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
        ))
        connection.execute(insert(mapDenormalize).values(
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
            connection.execute(insert(mapLocationWormholeClasses).values(
                locationID=r['_key'], wormholeClassID=r['wormholeClassID'],
            ))
        count += 1
    print("    {} rows".format(count))

    # ------------------------------------------------------------------
    # mapStars  ->  mapCelestialStatistics + mapDenormalize
    # Name: <solarSystemName>
    # Stars at 0,0,0 (matching universe.py)
    # ------------------------------------------------------------------
    print("  Importing mapStars")
    count = 0
    for r in _jsonl(sourcePath, 'mapStars.jsonl'):
        sid = r.get('solarSystemID')
        rid, cid, sec, sysName = sys_info.get(sid, (None, None, None, None))
        stats = r.get('statistics', {})
        stats['celestialID'] = r['_key']
        connection.execute(insert(mapCelestialStatistics).values(**stats))
        connection.execute(insert(mapDenormalize).values(
            itemID          = r['_key'],
            typeID          = r.get('typeID'),
            groupID         = 6,
            solarSystemID   = sid,
            constellationID = cid,
            regionID        = rid,
            x=0, y=0, z=0,
            radius          = r.get('radius'),
            itemName        = _trunc(sysName),       # Star name = solar system name
            security        = sec,
        ))
        count += 1
    print("    {} rows".format(count))

    # ------------------------------------------------------------------
    # mapPlanets  ->  mapCelestialStatistics + mapCelestialGraphics + mapDenormalize
    # Name: <solarSystemName> <celestialIndex>
    # Exception: use uniqueName if present
    # orbitID = starID of the system
    # ------------------------------------------------------------------
    print("  Importing mapPlanets")
    count = 0
    for r in _jsonl(sourcePath, 'mapPlanets.jsonl'):
        sid   = r.get('solarSystemID')
        rid, cid, sec, sysName = sys_info.get(sid, (None, None, None, None))
        p     = r.get('position', {})
        stats = r.get('statistics', {})
        attrs = r.get('attributes', {})
        stats['celestialID'] = r['_key']
        connection.execute(insert(mapCelestialStatistics).values(**stats))
        connection.execute(insert(mapCelestialGraphics).values(
            celestialID  = r['_key'],
            heightMap1   = attrs.get('heightMap1'),
            heightMap2   = attrs.get('heightMap2'),
            shaderPreset = attrs.get('shaderPreset'),
            population   = attrs.get('population'),
        ))
        unique = r.get('uniqueName', {})
        if isinstance(unique, dict) and unique.get(language):
            itemName = unique.get(language) or unique.get('en')
        else:
            itemName = '{} {}'.format(sysName, r.get('celestialIndex'))
        connection.execute(insert(mapDenormalize).values(
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
        count += 1
    print("    {} rows".format(count))

    # ------------------------------------------------------------------
    # mapMoons  ->  mapCelestialStatistics + mapCelestialGraphics + mapDenormalize
    # Name: <orbitName> - Moon <orbitIndex>
    # orbitName = planet name = <sysName> <celestialIndex>
    # orbitID = parent planetID
    # ------------------------------------------------------------------
    print("  Importing mapMoons")
    count = 0
    for r in _jsonl(sourcePath, 'mapMoons.jsonl'):
        sid   = r.get('solarSystemID')
        rid, cid, sec, sysName = sys_info.get(sid, (None, None, None, None))
        p     = r.get('position', {})
        stats = r.get('statistics', {})
        attrs = r.get('attributes', {})
        stats['celestialID'] = r['_key']
        connection.execute(insert(mapCelestialStatistics).values(**stats))
        connection.execute(insert(mapCelestialGraphics).values(
            celestialID  = r['_key'],
            heightMap1   = attrs.get('heightMap1'),
            heightMap2   = attrs.get('heightMap2'),
            shaderPreset = attrs.get('shaderPreset'),
        ))
        orbitName = '{} {}'.format(sysName, r.get('celestialIndex'))
        itemName  = '{} - Moon {}'.format(orbitName, r.get('orbitIndex'))
        connection.execute(insert(mapDenormalize).values(
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
        count += 1
    print("    {} rows".format(count))

    # ------------------------------------------------------------------
    # mapAsteroidBelts  ->  mapCelestialStatistics + mapDenormalize
    # Name: <orbitName> - Asteroid Belt <orbitIndex>
    # orbitName = planet name = <sysName> <celestialIndex>
    # orbitID = parent planetID
    # ------------------------------------------------------------------
    print("  Importing mapAsteroidBelts")
    count = 0
    for r in _jsonl(sourcePath, 'mapAsteroidBelts.jsonl'):
        sid   = r.get('solarSystemID')
        rid, cid, sec, sysName = sys_info.get(sid, (None, None, None, None))
        p     = r.get('position', {})
        stats = r.get('statistics', {})
        stats['celestialID'] = r['_key']
        connection.execute(insert(mapCelestialStatistics).values(**stats))
        unique = r.get('uniqueName', {})
        if isinstance(unique, dict) and unique.get(language):
            itemName = unique.get(language) or unique.get('en')
        else:
            orbitName = '{} {}'.format(sysName, r.get('celestialIndex'))
            itemName  = '{} - Asteroid Belt {}'.format(orbitName, r.get('orbitIndex'))
        connection.execute(insert(mapDenormalize).values(
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
        count += 1
    print("    {} rows".format(count))

    # ------------------------------------------------------------------
    # mapStargates  ->  mapJumps + mapDenormalize
    # Name: Stargate (<destinationSolarSystemName>)
    # ------------------------------------------------------------------
    print("  Importing mapStargates")
    count = 0
    for r in _jsonl(sourcePath, 'mapStargates.jsonl'):
        sid  = r.get('solarSystemID')
        rid, cid, sec, sysName = sys_info.get(sid, (None, None, None, None))
        p    = r.get('position', {})
        dest = r.get('destination', {})
        dest_sys_id = dest.get('solarSystemID')
        dest_name = dest_sys_name.get(dest_sys_id, str(dest_sys_id))
        connection.execute(insert(mapJumps).values(
            stargateID    = r['_key'],
            destinationID = dest.get('stargateID'),
        ))
        connection.execute(insert(mapDenormalize).values(
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
        count += 1
    print("    {} rows".format(count))

    # ------------------------------------------------------------------
    # mapSecondarySuns -> mapDenormalize
    # groupID 995, name 'Unknown Anomaly', security 0 — matching universe.py
    # ------------------------------------------------------------------
    print("  Importing mapSecondarySuns")
    count = 0
    for r in _jsonl(sourcePath, 'mapSecondarySuns.jsonl'):
        sid = r.get('solarSystemID')
        rid, cid, sec, sysName = sys_info.get(sid, (None, None, None, None))
        p = r.get('position', {})
        connection.execute(insert(mapDenormalize).values(
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
        count += 1
    print("    {} rows".format(count))

    trans.commit()
    print("Map import complete")


def import_npc_stations(connection, metadata, sourcePath, language='en'):
    """
    Populates staStations and mapDenormalize for NPC stations.

    Station name rules:
      useOperationName=True:  <orbitName> - <corporationName> <operationName>
      useOperationName=False: <orbitName> - <corporationName>

    orbitName is derived from the orbit object's mapDenormalize.itemName,
    which must already be populated (i.e. call import_map first).

    Requires invTypes to be loaded for groupID lookup.
    """
    print("Importing npcStations")

    staStations    = Table('staStations', metadata)
    mapDenormalize = Table('mapDenormalize', metadata)

    # Do all selects before calling begin() — executing a select triggers
    # SQLAlchemy autobegin, which would cause begin() to fail with
    # "already initialized a Transaction" if called afterwards.
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

    # Build orbit name lookup from mapDenormalize (planets/moons already inserted)
    orbit_rows = connection.execute(
        select(mapDenormalize.c.itemID, mapDenormalize.c.itemName)
    ).fetchall()
    orbit_names = {row[0]: row[1] for row in orbit_rows}

    # Commit the autobegin transaction opened by the selects above so we
    # can open our own explicit transaction for the inserts below.
    connection.commit()

    # Build corporation and operation name lookups from JSONL (no DB needed)
    corp_names = {}
    for r in _jsonl(sourcePath, 'npcCorporations.jsonl'):
        corp_names[r['_key']] = _name(r, language)

    op_names = {}
    for r in _jsonl(sourcePath, 'stationOperations.jsonl'):
        op_names[r['_key']] = _name(r.get('operationName', {}), language)

    trans = connection.begin()
    count = 0
    for r in _jsonl(sourcePath, 'npcStations.jsonl'):
        stationID = r['_key']
        sid   = r.get('solarSystemID')
        const_id, region_id, sec = sys_info.get(sid, (None, None, None))
        p     = r.get('position', {})
        typeID = r.get('typeID')
        ownerID = r.get('ownerID')
        opID    = r.get('operationID')

        orbitID   = r.get('orbitID')
        orbitName = orbit_names.get(orbitID, '')
        corpName  = corp_names.get(ownerID, '')
        opName    = op_names.get(opID, '')

        if r.get('useOperationName'):
            stationName = '{} - {} {}'.format(orbitName, corpName, opName).strip()
        else:
            stationName = '{} - {}'.format(orbitName, corpName).strip()

        stationName = _trunc(stationName)

        connection.execute(insert(staStations).values(
            stationID                = stationID,
            security                 = sec,
            dockingCostPerVolume     = None,
            maxShipVolumeDockable    = None,
            officeRentalCost         = None,
            operationID              = opID,
            stationTypeID            = typeID,
            corporationID            = ownerID,
            solarSystemID            = sid,
            constellationID          = const_id,
            regionID                 = region_id,
            stationName              = stationName,
            x                        = p.get('x'),
            y                        = p.get('y'),
            z                        = p.get('z'),
            reprocessingEfficiency      = r.get('reprocessingEfficiency'),
            reprocessingStationsTake    = r.get('reprocessingStationsTake'),
            reprocessingHangarFlag      = r.get('reprocessingHangarFlag'),
        ))
        connection.execute(insert(mapDenormalize).values(
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
        count += 1
    trans.commit()
    print("    {} rows".format(count))


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

    print("Building jump tables")
    trans = connection.begin()
    for statement in sql[connectiontype]:
        connection.execute(text(statement))
    trans.commit()
    print("Jump tables complete")
