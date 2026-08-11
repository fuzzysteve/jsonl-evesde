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


def import_corporation_role_groups(connection, metadata, sourcePath, language='en'):
    """corporationRoleGroups.jsonl -> crpRoleGroups + trnTranslations"""
    print("Importing corporationRoleGroups")
    tbl      = Table('crpRoleGroups', metadata)
    trn      = Table('trnTranslations', metadata)
    trn_cols = Table('trnTranslationColumns', metadata)
    trans = connection.begin()
    connection.execute(insert(trn_cols).values(tcGroupID=None, tcID=37, tableName='crpRoleGroups', columnName='name', masterID='roleGroupID'))
    count = 0
    for r in _jsonl(sourcePath, 'corporationRoleGroups.jsonl'):
        gid  = r['_key']
        name = r.get('name') or {}
        connection.execute(insert(tbl).values(
            roleGroupID        = gid,
            appliesTo          = r.get('appliesTo'),
            appliesToGrantable = r.get('appliesToGrantable'),
            isDivisional       = r.get('isDivisional'),
            isLocational       = r.get('isLocational'),
            name               = _en(name, language),
        ))
        count += 1
        if isinstance(name, dict):
            for lang, text in name.items():
                connection.execute(insert(trn).values(tcID=37, keyID=gid, languageID=lang, text=text))
    trans.commit()
    print("    {} rows".format(count))


def import_corporation_roles(connection, metadata, sourcePath, language='en'):
    """corporationRoles.jsonl -> crpRoles + crpRoleRoleGroups + trnTranslations"""
    print("Importing corporationRoles")
    tbl_roles  = Table('crpRoles', metadata)
    tbl_groups = Table('crpRoleRoleGroups', metadata)
    trn        = Table('trnTranslations', metadata)
    trn_cols   = Table('trnTranslationColumns', metadata)
    trans = connection.begin()
    connection.execute(insert(trn_cols).values(tcGroupID=None, tcID=38, tableName='crpRoles', columnName='name',        masterID='roleID'))
    connection.execute(insert(trn_cols).values(tcGroupID=None, tcID=39, tableName='crpRoles', columnName='description', masterID='roleID'))
    roles = groups = 0
    for r in _jsonl(sourcePath, 'corporationRoles.jsonl'):
        rid  = r['_key']
        name = r.get('name') or {}
        desc = r.get('description') or {}
        connection.execute(insert(tbl_roles).values(
            roleID      = rid,
            shortName   = r.get('shortName'),
            name        = _en(name, language),
            description = _en(desc, language),
        ))
        roles += 1
        if isinstance(name, dict):
            for lang, text in name.items():
                connection.execute(insert(trn).values(tcID=38, keyID=rid, languageID=lang, text=text))
        if isinstance(desc, dict):
            for lang, text in desc.items():
                connection.execute(insert(trn).values(tcID=39, keyID=rid, languageID=lang, text=text))
        for grp_id in r.get('roleGroupIDs') or []:
            connection.execute(insert(tbl_groups).values(roleID=rid, roleGroupID=grp_id))
            groups += 1
    trans.commit()
    print("    {} roles, {} role-group rows".format(roles, groups))
