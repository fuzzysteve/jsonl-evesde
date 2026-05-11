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


def import_type_lists(connection, metadata, sourcePath, language='en'):
    print("Importing typeLists")
 
    header      = Table('typeListsHeader', metadata)
    incTypes    = Table('typeListsIncludedTypeIDs', metadata)
    excTypes    = Table('typeListsExcludedTypeIDs', metadata)
    incGroups   = Table('typeListsIncludedGroupIDs', metadata)
    excGroups   = Table('typeListsExcludedGroupIDs', metadata)
    incCats     = Table('typeListsIncludedCategoryIDs', metadata)
    excCats     = Table('typeListsExcludedCategoryIDs', metadata)
 
    trans = connection.begin()
    count = 0
    for r in _jsonl(sourcePath, 'typeLists.jsonl'):
        lid = r['_key']
        connection.execute(insert(header).values(
            typeListID         = lid,
            name               = r.get('name'),
            displayName        = _en(r.get('displayName'), language),
            displayDescription = _en(r.get('displayDescription'), language),
        ))
        for typeID in set(r.get('includedTypeIDs', [])):
            connection.execute(insert(incTypes).values(typeListID=lid, typeID=typeID))
        for typeID in set(r.get('excludedTypeIDs', [])):
            connection.execute(insert(excTypes).values(typeListID=lid, typeID=typeID))
        for groupID in set(r.get('includedGroupIDs', [])):
            connection.execute(insert(incGroups).values(typeListID=lid, groupID=groupID))
        for groupID in set(r.get('excludedGroupIDs', [])):
            connection.execute(insert(excGroups).values(typeListID=lid, groupID=groupID))
        for categoryID in set(r.get('includedCategoryIDs', [])):
            connection.execute(insert(incCats).values(typeListID=lid, categoryID=categoryID))
        for categoryID in set(r.get('excludedCategoryIDs', [])):
            connection.execute(insert(excCats).values(typeListID=lid, categoryID=categoryID))
        count += 1
    trans.commit()
    print("    {} type lists imported".format(count))

