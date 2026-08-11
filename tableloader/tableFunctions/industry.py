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


def _log(msg):
    from datetime import datetime
    print(f"[{datetime.now():%H:%M:%S}] {msg}")

def import_industry_activities(connection, metadata, sourcePath, language='en'):
    """industryActivities.jsonl -> ramActivities (plain strings, no translations)"""
    _log("Importing industryActivities")
    tbl = Table('ramActivities', metadata)
    trans = connection.begin()
    count = 0
    for r in _jsonl(sourcePath, 'industryActivities.jsonl'):
        connection.execute(insert(tbl).values(
            activityID   = r['_key'],
            activityName = r.get('name'),
            description  = r.get('description'),
        ))
        count += 1
    trans.commit()
    _log("    {} rows".format(count))


def import_industry_assembly_lines(connection, metadata, sourcePath, language='en'):
    """industryAssemblyLines.jsonl -> ramAssemblyLines + ramAssemblyLineTypeDetailPerGroup"""
    _log("Importing industryAssemblyLines")
    tbl_lines  = Table('ramAssemblyLines', metadata)
    tbl_groups = Table('ramAssemblyLineTypeDetailPerGroup', metadata)
    trans = connection.begin()
    lines = groups = 0
    for r in _jsonl(sourcePath, 'industryAssemblyLines.jsonl'):
        line_id = r['_key']
        connection.execute(insert(tbl_lines).values(
            assemblyLineID           = line_id,
            activityID               = r.get('activityID'),
            baseMaterialMultiplier   = r.get('baseMaterialMultiplier'),
            baseTimeMultiplier       = r.get('baseTimeMultiplier'),
            name                     = r.get('name'),
            description              = r.get('description'),
        ))
        lines += 1
        for grp in r.get('detailsPerGroup') or []:
            connection.execute(insert(tbl_groups).values(
                assemblyLineTypeID  = line_id,
                groupID             = grp['groupID'],
                costMultiplier      = grp.get('costMultiplier'),
                materialMultiplier  = grp.get('materialMultiplier'),
                timeMultiplier      = grp.get('timeMultiplier'),
            ))
            groups += 1
    trans.commit()
    _log("    {} assembly lines, {} group-detail rows".format(lines, groups))


def import_industry_installation_types(connection, metadata, sourcePath, language='en'):
    """industryInstallationTypes.jsonl -> ramInstallationTypeContents"""
    _log("Importing industryInstallationTypes")
    tbl = Table('ramInstallationTypeContents', metadata)
    trans = connection.begin()
    count = 0
    for r in _jsonl(sourcePath, 'industryInstallationTypes.jsonl'):
        type_id = r['_key']
        for entry in r.get('assemblyLines') or []:
            connection.execute(insert(tbl).values(
                installationTypeID  = type_id,
                assemblyLineTypeID  = entry['assemblyLineID'],
            ))
            count += 1
    trans.commit()
    _log("    {} rows".format(count))


def import_industry_modifier_sources(connection, metadata, sourcePath, language='en'):
    """industryModifierSources.jsonl -> indModifierSources"""
    _log("Importing industryModifierSources")
    tbl = Table('indModifierSources', metadata)
    trans = connection.begin()
    count = 0
    for r in _jsonl(sourcePath, 'industryModifierSources.jsonl'):
        type_id = r['_key']
        for activity, modifiers in r.items():
            if activity == '_key':
                continue
            for mod_type, attrs in modifiers.items():
                for attr in attrs:
                    connection.execute(insert(tbl).values(
                        typeID           = type_id,
                        activityName     = activity,
                        modifierType     = mod_type,
                        dogmaAttributeID = attr['dogmaAttributeID'],
                    ))
                    count += 1
    trans.commit()
    _log("    {} rows".format(count))


def import_industry_target_filters(connection, metadata, sourcePath, language='en'):
    """industryTargetFilters.jsonl -> indTargetFilters + indTargetFilterCategories + indTargetFilterGroups"""
    _log("Importing industryTargetFilters")
    tbl_f    = Table('indTargetFilters', metadata)
    tbl_cats = Table('indTargetFilterCategories', metadata)
    tbl_grps = Table('indTargetFilterGroups', metadata)
    trans = connection.begin()
    filters = cats = grps = 0
    for r in _jsonl(sourcePath, 'industryTargetFilters.jsonl'):
        fid = r['_key']
        connection.execute(insert(tbl_f).values(
            targetFilterID = fid,
            name           = r.get('name'),
        ))
        filters += 1
        for cat_id in r.get('categoryIDs') or []:
            connection.execute(insert(tbl_cats).values(
                targetFilterID = fid,
                categoryID     = cat_id,
            ))
            cats += 1
        for grp_id in r.get('groupIDs') or []:
            connection.execute(insert(tbl_grps).values(
                targetFilterID = fid,
                groupID        = grp_id,
            ))
            grps += 1
    trans.commit()
    _log("    {} filters, {} category rows, {} group rows".format(filters, cats, grps))


def import_station_operations(connection, metadata, sourcePath, language='en'):
    """stationOperations.jsonl -> staOperations + staOperationServices + staOperationTypes + trnTranslations"""
    _log("Importing stationOperations")
    tbl_ops  = Table('staOperations', metadata)
    tbl_svcs = Table('staOperationServices', metadata)
    tbl_types = Table('staOperationTypes', metadata)
    trn      = Table('trnTranslations', metadata)
    trn_cols = Table('trnTranslationColumns', metadata)
    trans = connection.begin()
    connection.execute(insert(trn_cols).values(tcGroupID=None, tcID=49, tableName='staOperations', columnName='operationName', masterID='operationID'))
    connection.execute(insert(trn_cols).values(tcGroupID=None, tcID=50, tableName='staOperations', columnName='description',   masterID='operationID'))
    ops = svcs = types = 0
    for r in _jsonl(sourcePath, 'stationOperations.jsonl'):
        op_id = r['_key']
        op_name = r.get('operationName') or {}
        desc    = r.get('description') or {}
        connection.execute(insert(tbl_ops).values(
            operationID        = op_id,
            activityID         = r.get('activityID'),
            operationName      = op_name.get('en') if isinstance(op_name, dict) else op_name,
            description        = desc.get('en') if isinstance(desc, dict) else desc,
            border             = r.get('border'),
            corridor           = r.get('corridor'),
            fringe             = r.get('fringe'),
            hub                = r.get('hub'),
            manufacturingFactor= r.get('manufacturingFactor'),
            ratio              = r.get('ratio'),
            researchFactor     = r.get('researchFactor'),
        ))
        ops += 1
        if isinstance(op_name, dict):
            for lang, text in op_name.items():
                connection.execute(insert(trn).values(tcID=49, keyID=op_id, languageID=lang, text=text))
        if isinstance(desc, dict):
            for lang, text in desc.items():
                connection.execute(insert(trn).values(tcID=50, keyID=op_id, languageID=lang, text=text))
        for svc_id in r.get('services') or []:
            connection.execute(insert(tbl_svcs).values(operationID=op_id, serviceID=svc_id))
            svcs += 1
        for entry in r.get('stationTypes') or []:
            connection.execute(insert(tbl_types).values(operationID=op_id, raceID=entry['_key'], typeID=entry['_value']))
            types += 1
    trans.commit()
    _log("    {} operations, {} service rows, {} type rows".format(ops, svcs, types))


def import_station_standings_restrictions(connection, metadata, sourcePath, language='en'):
    """stationStandingsRestrictions.jsonl -> staStandingsRestrictions"""
    _log("Importing stationStandingsRestrictions")
    tbl = Table('staStandingsRestrictions', metadata)
    trans = connection.begin()
    count = 0
    for r in _jsonl(sourcePath, 'stationStandingsRestrictions.jsonl'):
        type_id = r['_key']
        for entry in r.get('services') or []:
            connection.execute(insert(tbl).values(
                typeID           = type_id,
                serviceID        = entry['_key'],
                standingRequired = entry['_value'],
            ))
            count += 1
    trans.commit()
    _log("    {} rows".format(count))
