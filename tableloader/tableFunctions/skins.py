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


def import_skin_materials(connection, metadata, sourcePath, language='en'):
    """
    skinMaterials.jsonl -> skinMaterials
    displayNameID is left NULL — the table expects an integer ID but the
    source only has a localised name dict with no corresponding integer key.
    """
    print("Importing skinMaterials")
    tbl = Table('skinMaterials', metadata)
    trans = connection.begin()
    count = 0
    for r in _jsonl(sourcePath, 'skinMaterials.jsonl'):
        connection.execute(insert(tbl).values(
            skinMaterialID = r['_key'],
            displayNameID  = None,
            materialSetID  = r.get('materialSetID'),
        ))
        count += 1
    trans.commit()
    print("    {} rows".format(count))


def import_skins(connection, metadata, sourcePath, language='en'):
    """skins.jsonl -> skins + skinShip"""
    print("Importing skins")
    tblSkins    = Table('skins', metadata)
    tblSkinShip = Table('skinShip', metadata)
    trans = connection.begin()
    skins = ship_rows = 0
    for r in _jsonl(sourcePath, 'skins.jsonl'):
        skinID = r['_key']
        connection.execute(insert(tblSkins).values(
            skinID         = skinID,
            internalName   = r.get('internalName'),
            skinMaterialID = r.get('skinMaterialID'),
        ))
        skins += 1
        for typeID in r.get('types', []):
            connection.execute(insert(tblSkinShip).values(
                skinID = skinID,
                typeID = typeID,
            ))
            ship_rows += 1
    trans.commit()
    print("    {} skins, {} skinShip rows".format(skins, ship_rows))


def import_skin_licenses(connection, metadata, sourcePath, language='en'):
    """skinLicenses.jsonl -> skinLicense"""
    print("Importing skinLicenses")
    tbl = Table('skinLicense', metadata)
    trans = connection.begin()
    count = 0
    for r in _jsonl(sourcePath, 'skinLicenses.jsonl'):
        connection.execute(insert(tbl).values(
            licenseTypeID = r['_key'],
            duration      = r.get('duration'),
            skinID        = r.get('skinID'),
        ))
        count += 1
    trans.commit()
    print("    {} rows".format(count))
