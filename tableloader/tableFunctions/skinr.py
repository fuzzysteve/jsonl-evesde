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


def import_skinr_component_categories(connection, metadata, sourcePath, language='en'):
    """skinrComponentCategories.jsonl -> skinrComponentCategories"""
    print("Importing skinrComponentCategories")
    tbl = Table('skinrComponentCategories', metadata)
    trans = connection.begin()
    count = 0
    for r in _jsonl(sourcePath, 'skinrComponentCategories.jsonl'):
        connection.execute(insert(tbl).values(
            categoryID = r['_key'],
            name       = r.get('name'),
        ))
        count += 1
    trans.commit()
    print("    {} rows".format(count))


def import_skinr_component_rarities(connection, metadata, sourcePath, language='en'):
    """skinrComponentRarities.jsonl -> skinrComponentRarities"""
    print("Importing skinrComponentRarities")
    tbl = Table('skinrComponentRarities', metadata)
    trans = connection.begin()
    count = 0
    for r in _jsonl(sourcePath, 'skinrComponentRarities.jsonl'):
        connection.execute(insert(tbl).values(
            rarityID = r['_key'],
            name     = _en(r.get('name', {}), language),
            rank     = r.get('rank'),
        ))
        count += 1
    trans.commit()
    print("    {} rows".format(count))


def import_skinr_component_point_values(connection, metadata, sourcePath, language='en'):
    """skinrComponentPointValues.jsonl -> skinrComponentPointValues (categoryID x rarityID -> points)"""
    print("Importing skinrComponentPointValues")
    tbl = Table('skinrComponentPointValues', metadata)
    trans = connection.begin()
    count = 0
    for r in _jsonl(sourcePath, 'skinrComponentPointValues.jsonl'):
        category_id = r['_key']
        for entry in r.get('_value', []):
            connection.execute(insert(tbl).values(
                categoryID = category_id,
                rarityID   = entry['_key'],
                points     = entry['_value'],
            ))
            count += 1
    trans.commit()
    print("    {} rows".format(count))


def import_skinr_components(connection, metadata, sourcePath, language='en'):
    """skinrComponents.jsonl -> skinrComponents + skinrComponentTypes"""
    print("Importing skinrComponents")
    tblComps = Table('skinrComponents', metadata)
    tblTypes = Table('skinrComponentTypes', metadata)
    trans = connection.begin()
    components = types = 0

    for r in _jsonl(sourcePath, 'skinrComponents.jsonl'):
        comp_id = r['_key']
        seq = r.get('sequenceBinder') or {}
        connection.execute(insert(tblComps).values(
            componentID          = comp_id,
            name                 = _en(r.get('name', {}), language),
            categoryID           = r.get('category'),
            rarityID             = r.get('rarity'),
            finish               = r.get('finish'),
            published            = r.get('published'),
            iconFile             = r.get('iconFile'),
            resourceFile         = r.get('resourceFile'),
            projectionTypeU      = r.get('projectionTypeU'),
            projectionTypeV      = r.get('projectionTypeV'),
            sequenceBinderCount  = seq.get('count'),
            sequenceBinderTypeID = seq.get('itemTypeID'),
        ))
        components += 1

        for entry in r.get('associatedTypeIds', []):
            connection.execute(insert(tblTypes).values(
                componentID        = comp_id,
                typeID             = entry['typeID'],
                licenseUsesGranted = entry.get('licenseUsesGranted'),
            ))
            types += 1

    trans.commit()
    print("    {} components, {} type rows".format(components, types))


def import_skinr_slot_categories(connection, metadata, sourcePath, language='en'):
    """skinrSlotCategories.jsonl -> skinrSlotCategories"""
    print("Importing skinrSlotCategories")
    tbl = Table('skinrSlotCategories', metadata)
    trans = connection.begin()
    count = 0
    for r in _jsonl(sourcePath, 'skinrSlotCategories.jsonl'):
        connection.execute(insert(tbl).values(
            categoryID = r['_key'],
            name       = r.get('name'),
        ))
        count += 1
    trans.commit()
    print("    {} rows".format(count))


def import_skinr_slot_names(connection, metadata, sourcePath, language='en'):
    """skinrSlotNames.jsonl -> skinrSlotNames"""
    print("Importing skinrSlotNames")
    tbl = Table('skinrSlotNames', metadata)
    trans = connection.begin()
    count = 0
    for r in _jsonl(sourcePath, 'skinrSlotNames.jsonl'):
        connection.execute(insert(tbl).values(
            slotNameID = r['_key'],
            name       = r.get('name'),
        ))
        count += 1
    trans.commit()
    print("    {} rows".format(count))


def import_skinr_slots(connection, metadata, sourcePath, language='en'):
    """skinrSlots.jsonl -> skinrSlots + skinrSlotAllowedCategories"""
    print("Importing skinrSlots")
    tblSlots = Table('skinrSlots', metadata)
    tblCats  = Table('skinrSlotAllowedCategories', metadata)
    trans = connection.begin()
    slots = cats = 0

    for r in _jsonl(sourcePath, 'skinrSlots.jsonl'):
        slot_id = r['_key']
        connection.execute(insert(tblSlots).values(
            slotID     = slot_id,
            name       = _en(r.get('name', {}), language),
            categoryID = r.get('category'),
        ))
        slots += 1

        for cat_id in r.get('allowedDesignComponentCategories', []):
            connection.execute(insert(tblCats).values(
                slotID     = slot_id,
                categoryID = cat_id,
            ))
            cats += 1

    trans.commit()
    print("    {} slots, {} allowed-category rows".format(slots, cats))


def import_skinr_slot_configurations(connection, metadata, sourcePath, language='en'):
    """skinrSlotConfigurations.jsonl -> skinrSlotConfigurations + skinrSlotConfigurationSlots + skinrSlotConfigurationShips"""
    print("Importing skinrSlotConfigurations")
    tblConfigs = Table('skinrSlotConfigurations', metadata)
    tblSlots   = Table('skinrSlotConfigurationSlots', metadata)
    tblShips   = Table('skinrSlotConfigurationShips', metadata)
    trans = connection.begin()
    configs = slot_rows = ship_rows = 0

    for r in _jsonl(sourcePath, 'skinrSlotConfigurations.jsonl'):
        config_id = r['_key']
        connection.execute(insert(tblConfigs).values(
            configID      = config_id,
            name          = r.get('name'),
            priority      = r.get('priority'),
            allowAllShips = r.get('allowAllShips'),
        ))
        configs += 1

        for slot_id in r.get('config') or []:
            connection.execute(insert(tblSlots).values(
                configID = config_id,
                slotID   = slot_id,
            ))
            slot_rows += 1

        for type_id in set(r.get('ships') or []):
            connection.execute(insert(tblShips).values(
                configID = config_id,
                typeID   = type_id,
            ))
            ship_rows += 1

    trans.commit()
    print("    {} configs, {} slot rows, {} ship rows".format(configs, slot_rows, ship_rows))


def import_skinr_tier_thresholds(connection, metadata, sourcePath, language='en'):
    """skinrTierThresholds.jsonl -> skinrTierThresholds (groupID x tier -> threshold)"""
    print("Importing skinrTierThresholds")
    tbl = Table('skinrTierThresholds', metadata)
    trans = connection.begin()
    count = 0
    for r in _jsonl(sourcePath, 'skinrTierThresholds.jsonl'):
        group_id = r['_key']
        for entry in r.get('_value', []):
            connection.execute(insert(tbl).values(
                groupID   = group_id,
                tier      = entry['_key'],
                threshold = entry['_value'],
            ))
            count += 1
    trans.commit()
    print("    {} rows".format(count))
