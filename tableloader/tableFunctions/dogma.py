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

def import_dogma_attribute_categories(connection, metadata, sourcePath, language='en'):
    """dogmaAttributeCategories.jsonl -> dgmAttributeCategories"""
    _log("Importing dogmaAttributeCategories")
    tbl = Table('dgmAttributeCategories', metadata)
    trans = connection.begin()
    for r in _jsonl(sourcePath, 'dogmaAttributeCategories.jsonl'):
        connection.execute(insert(tbl).values(
            categoryID          = r['categoryID'],
            categoryName        = r.get('name'),
            categoryDescription = r.get('description'),
        ))
    trans.commit()


def import_dogma_attributes(connection, metadata, sourcePath, language='en'):
    """dogmaAttributes.jsonl -> dgmAttributeTypes"""
    _log("Importing dogmaAttributes")
    tbl = Table('dgmAttributeTypes', metadata)
    trans = connection.begin()
    for r in _jsonl(sourcePath, 'dogmaAttributes.jsonl'):
        connection.execute(insert(tbl).values(
            attributeID   = r['attributeID'],
            attributeName = r.get('name'),
            description   = r.get('description'),
            iconID        = r.get('iconID'),
            defaultValue  = r.get('defaultValue'),
            published     = r.get('published'),
            displayName   = _en(r.get('displayName'), language),
            unitID        = r.get('unitID'),
            stackable     = r.get('stackable'),
            highIsGood    = r.get('highIsGood'),
            categoryID    = r.get('attributeCategoryID'),
            maxAttributeID = r.get('maxAttributeID'),
            minAttributeID = r.get('minAttributeID'),
            chargeRechargeTimeID = r.get('chargeRechargeTimeID'),
            dataType       = r.get('dataType'),
            displayWhenZero = r.get('displayWhenZero'),
            tooltipTitle   = _en(r.get('tooltipTitle'), language),
            tooltipDescription = _en(r.get('tooltipDescription'), language),
        ))
    trans.commit()


def import_dogma_effects(connection, metadata, sourcePath, language='en'):
    """dogmaEffects.jsonl -> dgmEffects"""
    _log("Importing dogmaEffects")
    tbl = Table('dgmEffects', metadata)
    trans = connection.begin()
    for r in _jsonl(sourcePath, 'dogmaEffects.jsonl'):
        modifier_info = r.get('modifierInfo')
        connection.execute(insert(tbl).values(
            effectID                       = r['effectID'],
            effectName                     = r.get('name'),
            effectCategory                 = r.get('effectCategoryID'),
            preExpression                  = None,
            postExpression                 = None,
            description                    = _en(r.get('description'), language),
            guid                           = r.get('guid'),
            iconID                         = r.get('iconID'),
            isOffensive                    = r.get('isOffensive'),
            isAssistance                   = r.get('isAssistance'),
            durationAttributeID            = r.get('durationAttributeID'),
            trackingSpeedAttributeID       = r.get('trackingSpeedAttributeID'),
            dischargeAttributeID           = r.get('dischargeAttributeID'),
            rangeAttributeID               = r.get('rangeAttributeID'),
            falloffAttributeID             = r.get('falloffAttributeID'),
            disallowAutoRepeat             = r.get('disallowAutoRepeat'),
            published                      = r.get('published'),
            displayName                    = _en(r.get('displayName'), language),
            isWarpSafe                     = r.get('isWarpSafe'),
            rangeChance                    = r.get('rangeChance'),
            electronicChance               = r.get('electronicChance'),
            propulsionChance               = r.get('propulsionChance'),
            distribution                   = r.get('distribution'),
            sfxName                        = None,
            npcUsageChanceAttributeID      = r.get('npcUsageChanceAttributeID'),
            npcActivationChanceAttributeID = r.get('npcActivationChanceAttributeID'),
            fittingUsageChanceAttributeID  = r.get('fittingUsageChanceAttributeID'),
            modifierInfo                   = json.dumps(modifier_info) if modifier_info else None,
            resistanceAttributeID          = r.get('resistanceAttributeID'),
        ))
    trans.commit()


def import_dogma_units(connection, metadata, sourcePath, language='en'):
    """dogmaUnits.jsonl -> eveUnits"""
    _log("Importing dogmaUnits")
    tbl = Table('eveUnits', metadata)
    trans = connection.begin()
    for r in _jsonl(sourcePath, 'dogmaUnits.jsonl'):
        connection.execute(insert(tbl).values(
            unitID      = r['unitID'],
            unitName    = r.get('name'),
            displayName = _en(r.get('displayName'), language),
            description = _en(r.get('description'), language),
        ))
    trans.commit()


def import_dynamic_item_attributes(connection, metadata, sourcePath, language='en'):
    """dynamicItemAttributes.jsonl -> dynamicItemAttributes + dynamicItemAttributeMappings"""
    _log("Importing dynamicItemAttributes")
    tbl_attr = Table('dynamicItemAttributes', metadata)
    tbl_map  = Table('dynamicItemAttributeMappings', metadata)
    trans = connection.begin()
    attrs = mappings = 0
    for r in _jsonl(sourcePath, 'dynamicItemAttributes.jsonl'):
        tid = r['typeID']
        for a in r.get('attributeIDs') or []:
            connection.execute(insert(tbl_attr).values(
                typeID      = tid,
                attributeID = a['attributeID'],
                min         = a.get('min'),
                max         = a.get('max'),
                highIsGood  = a.get('highIsGood'),
            ))
            attrs += 1
        for m in r.get('inputOutputMapping') or []:
            for applicable in m.get('applicableTypes') or []:
                connection.execute(insert(tbl_map).values(
                    typeID           = tid,
                    applicableTypeID = applicable,
                    resultingTypeID  = m.get('resultingType'),
                ))
                mappings += 1
    trans.commit()
    _log("    {} attribute rows, {} mapping rows".format(attrs, mappings))
