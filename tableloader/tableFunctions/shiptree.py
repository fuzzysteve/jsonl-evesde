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


def import_ship_tree_elements(connection, metadata, sourcePath, language='en'):
    """shipTreeElements.jsonl -> shipTreeElements"""
    print("Importing shipTreeElements")
    tbl = Table('shipTreeElements', metadata)
    trans = connection.begin()
    count = 0
    for r in _jsonl(sourcePath, 'shipTreeElements.jsonl'):
        connection.execute(insert(tbl).values(
            elementID   = r['_key'],
            name        = _en(r.get('name', {}), language),
            description = _en(r.get('description', {}), language),
            icon        = r.get('icon'),
        ))
        count += 1
    trans.commit()
    print("    {} rows".format(count))


def import_ship_tree_groups(connection, metadata, sourcePath, language='en'):
    """shipTreeGroups.jsonl -> shipTreeGroups + shipTreeGroupElements + shipTreeGroupPreReqSkills"""
    print("Importing shipTreeGroups")
    tblGroups  = Table('shipTreeGroups', metadata)
    tblElems   = Table('shipTreeGroupElements', metadata)
    tblSkills  = Table('shipTreeGroupPreReqSkills', metadata)
    trans = connection.begin()
    groups = elements = skills = 0

    for r in _jsonl(sourcePath, 'shipTreeGroups.jsonl'):
        group_id = r['_key']
        connection.execute(insert(tblGroups).values(
            groupID      = group_id,
            name         = _en(r.get('name', {}), language),
            description  = _en(r.get('description', {}), language),
            icon         = r.get('icon'),
            iconLarge    = r.get('iconLarge'),
            iconSmall    = r.get('iconSmall'),
            iconSmallNPC = r.get('iconSmallNPC'),
        ))
        groups += 1

        for e in r.get('elements', []):
            connection.execute(insert(tblElems).values(
                groupID   = group_id,
                elementID = e['_key'],
                value     = e['_value'],
            ))
            elements += 1

        for faction_entry in r.get('preReqSkills', []):
            faction_id = faction_entry['_key']
            for skill in faction_entry.get('skills', []):
                connection.execute(insert(tblSkills).values(
                    groupID   = group_id,
                    factionID = faction_id,
                    skillID   = skill['_key'],
                    level     = skill.get('level'),
                    display   = skill.get('display'),
                ))
                skills += 1

    trans.commit()
    print("    {} groups, {} element rows, {} prereq-skill rows".format(groups, elements, skills))


def import_ship_tree_factions(connection, metadata, sourcePath, language='en'):
    """shipTreeFactions.jsonl -> shipTreeFactions + shipTreeFactionElements"""
    print("Importing shipTreeFactions")
    tblFactions = Table('shipTreeFactions', metadata)
    tblElems    = Table('shipTreeFactionElements', metadata)
    trans = connection.begin()
    factions = elements = 0

    for r in _jsonl(sourcePath, 'shipTreeFactions.jsonl'):
        faction_id = r['_key']
        connection.execute(insert(tblFactions).values(
            factionID   = faction_id,
            description = _en(r.get('description', {}), language),
            icon        = r.get('icon'),
        ))
        factions += 1

        for e in r.get('elements', []):
            connection.execute(insert(tblElems).values(
                factionID = faction_id,
                elementID = e['_key'],
                value     = e['_value'],
            ))
            elements += 1

    trans.commit()
    print("    {} factions, {} element rows".format(factions, elements))


def import_type_elements(connection, metadata, sourcePath, language='en'):
    """typeElements.jsonl -> typeElements"""
    print("Importing typeElements")
    tbl = Table('typeElements', metadata)
    trans = connection.begin()
    types = rows = 0

    for r in _jsonl(sourcePath, 'typeElements.jsonl'):
        type_id = r['_key']
        types += 1
        for e in r.get('elements', []):
            connection.execute(insert(tbl).values(
                typeID    = type_id,
                elementID = e['_key'],
                value     = e['_value'],
            ))
            rows += 1

    trans.commit()
    print("    {} types, {} element rows".format(types, rows))
