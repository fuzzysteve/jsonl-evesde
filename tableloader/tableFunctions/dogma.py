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


def import_dogma_attribute_categories(connection, metadata, sourcePath, language='en'):
    """dogmaAttributeCategories.jsonl -> dgmAttributeCategories"""
    print("Importing dogmaAttributeCategories")
    tbl = Table('dgmAttributeCategories', metadata)
    trans = connection.begin()
    for r in _jsonl(sourcePath, 'dogmaAttributeCategories.jsonl'):
        connection.execute(insert(tbl).values(
            categoryID          = r['_key'],
            categoryName        = r.get('name'),
            categoryDescription = r.get('description'),
        ))
    trans.commit()


def import_dogma_attributes(connection, metadata, sourcePath, language='en'):
    """dogmaAttributes.jsonl -> dgmAttributeTypes"""
    print("Importing dogmaAttributes")
    tbl = Table('dgmAttributeTypes', metadata)
    trans = connection.begin()
    for r in _jsonl(sourcePath, 'dogmaAttributes.jsonl'):
        connection.execute(insert(tbl).values(
            attributeID   = r['_key'],
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
        ))
    trans.commit()


def import_dogma_effects(connection, metadata, sourcePath, language='en'):
    """dogmaEffects.jsonl -> dgmEffects"""
    print("Importing dogmaEffects")
    tbl = Table('dgmEffects', metadata)
    trans = connection.begin()
    for r in _jsonl(sourcePath, 'dogmaEffects.jsonl'):
        modifier_info = r.get('modifierInfo')
        connection.execute(insert(tbl).values(
            effectID                       = r['_key'],
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
        ))
    trans.commit()


def import_dogma_units(connection, metadata, sourcePath, language='en'):
    """dogmaUnits.jsonl -> eveUnits"""
    print("Importing dogmaUnits")
    tbl = Table('eveUnits', metadata)
    trans = connection.begin()
    for r in _jsonl(sourcePath, 'dogmaUnits.jsonl'):
        connection.execute(insert(tbl).values(
            unitID      = r['_key'],
            unitName    = r.get('name'),
            displayName = _en(r.get('displayName'), language),
            description = _en(r.get('description'), language),
        ))
    trans.commit()
