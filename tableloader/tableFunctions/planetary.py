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


def import_planet_resources(connection, metadata, sourcePath, language='en'):
    """
    planetResources.jsonl -> planetResources

    New table — add definition from planetResources_table_definition.py to tables.py.
    _key = celestialID (a planet itemID from mapDenormalize).
    """
    print("Importing planetResources")
    tbl = Table('planetResources', metadata)
    trans = connection.begin()
    count = 0
    for r in _jsonl(sourcePath, 'planetResources.jsonl'):
        reagent = r.get('reagent') or {}
        connection.execute(insert(tbl).values(
            celestialID          = r['_key'],
            power                = r.get('power'),
            workforce            = r.get('workforce'),
            reagentTypeID        = reagent.get('type_id'),
            reagentAmountPerCycle= reagent.get('amount_per_cycle'),
            reagentCyclePeriod   = reagent.get('cycle_period'),
            reagentSecuredCap    = reagent.get('secured_capacity'),
            reagentUnsecuredCap  = reagent.get('unsecured_capacity'),
        ))
        count += 1
    trans.commit()
    print("    {} rows".format(count))


def import_planet_schematics(connection, metadata, sourcePath, language='en'):
    """planetSchematics.jsonl -> planetSchematics + planetSchematicsPinMap + planetSchematicsTypeMap"""
    print("Importing planetSchematics")
    tblSchematics = Table('planetSchematics', metadata)
    tblPinMap     = Table('planetSchematicsPinMap', metadata)
    tblTypeMap    = Table('planetSchematicsTypeMap', metadata)
    trans = connection.begin()
    count = 0
    for r in _jsonl(sourcePath, 'planetSchematics.jsonl'):
        sid = r['_key']
        connection.execute(insert(tblSchematics).values(
            schematicID   = sid,
            schematicName = _en(r.get('name', {}), language),
            cycleTime     = r.get('cycleTime'),
        ))
        for pinTypeID in r.get('pins', []):
            connection.execute(insert(tblPinMap).values(
                schematicID = sid,
                pinTypeID   = pinTypeID,
            ))
        for t in r.get('types', []):
            connection.execute(insert(tblTypeMap).values(
                schematicID = sid,
                typeID      = t['_key'],
                quantity    = t.get('quantity'),
                isInput     = t.get('isInput'),
            ))
        count += 1
    trans.commit()
    print("    {} schematics imported".format(count))
