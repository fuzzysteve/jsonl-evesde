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


def import_schools(connection, metadata, sourcePath, language='en'):
    """schools.jsonl -> chrSchools + chrSchoolCareerAgents + chrSchoolStartingStations + trnTranslations"""
    print("Importing schools")
    tbl_schools  = Table('chrSchools', metadata)
    tbl_agents   = Table('chrSchoolCareerAgents', metadata)
    tbl_stations = Table('chrSchoolStartingStations', metadata)
    trn          = Table('trnTranslations', metadata)
    trn_cols     = Table('trnTranslationColumns', metadata)
    trans = connection.begin()
    connection.execute(insert(trn_cols).values(tcGroupID=None, tcID=42, tableName='chrSchools', columnName='name',                 masterID='schoolID'))
    connection.execute(insert(trn_cols).values(tcGroupID=None, tcID=43, tableName='chrSchools', columnName='description',          masterID='schoolID'))
    connection.execute(insert(trn_cols).values(tcGroupID=None, tcID=44, tableName='chrSchools', columnName='title',                masterID='schoolID'))
    connection.execute(insert(trn_cols).values(tcGroupID=None, tcID=45, tableName='chrSchools', columnName='characterDescription', masterID='schoolID'))
    schools = agents = stations = 0
    for r in _jsonl(sourcePath, 'schools.jsonl'):
        sid   = r['_key']
        name  = r.get('name') or {}
        desc  = r.get('description') or {}
        title = r.get('title') or {}
        cdesc = r.get('characterDescription') or {}
        connection.execute(insert(tbl_schools).values(
            schoolID             = sid,
            careerID             = r.get('careerID'),
            corporationID        = r.get('corporationID'),
            iconID               = r.get('iconID'),
            raceID               = r.get('raceID'),
            name                 = _en(name, language),
            description          = _en(desc, language),
            title                = _en(title, language),
            characterDescription = _en(cdesc, language),
        ))
        schools += 1
        if isinstance(name, dict):
            for lang, text in name.items():
                connection.execute(insert(trn).values(tcID=42, keyID=sid, languageID=lang, text=text))
        if isinstance(desc, dict):
            for lang, text in desc.items():
                connection.execute(insert(trn).values(tcID=43, keyID=sid, languageID=lang, text=text))
        if isinstance(title, dict):
            for lang, text in title.items():
                connection.execute(insert(trn).values(tcID=44, keyID=sid, languageID=lang, text=text))
        if isinstance(cdesc, dict):
            for lang, text in cdesc.items():
                connection.execute(insert(trn).values(tcID=45, keyID=sid, languageID=lang, text=text))
        for char_id in r.get('careerAgents') or []:
            connection.execute(insert(tbl_agents).values(schoolID=sid, characterID=char_id))
            agents += 1
        for sta_id in r.get('startingStations') or []:
            connection.execute(insert(tbl_stations).values(schoolID=sid, stationID=sta_id))
            stations += 1
    trans.commit()
    print("    {} schools, {} agent rows, {} station rows".format(schools, agents, stations))


def import_school_map(connection, metadata, sourcePath, language='en'):
    """schoolMap.jsonl -> chrSchoolMap"""
    print("Importing schoolMap")
    tbl = Table('chrSchoolMap', metadata)
    trans = connection.begin()
    count = 0
    for r in _jsonl(sourcePath, 'schoolMap.jsonl'):
        connection.execute(insert(tbl).values(
            id            = r['_key'],
            schoolID      = r.get('schoolID'),
            solarSystemID = r.get('solarSystemID'),
        ))
        count += 1
    trans.commit()
    print("    {} rows".format(count))


def import_skill_plans(connection, metadata, sourcePath, language='en'):
    """skillPlans.jsonl -> skillPlans + skillPlanMilestones + skillPlanSkillRequirements + trnTranslations"""
    print("Importing skillPlans")
    tbl_plans  = Table('skillPlans', metadata)
    tbl_ms     = Table('skillPlanMilestones', metadata)
    tbl_reqs   = Table('skillPlanSkillRequirements', metadata)
    trn        = Table('trnTranslations', metadata)
    trn_cols   = Table('trnTranslationColumns', metadata)
    trans = connection.begin()
    connection.execute(insert(trn_cols).values(tcGroupID=None, tcID=46, tableName='skillPlans', columnName='name',        masterID='skillPlanID'))
    connection.execute(insert(trn_cols).values(tcGroupID=None, tcID=47, tableName='skillPlans', columnName='description', masterID='skillPlanID'))
    plans = milestones = reqs = 0
    for r in _jsonl(sourcePath, 'skillPlans.jsonl'):
        pid  = r['_key']
        name = r.get('name') or {}
        desc = r.get('description') or {}
        connection.execute(insert(tbl_plans).values(
            skillPlanID  = pid,
            careerPathID = r.get('careerPathID'),
            factionID    = r.get('factionID'),
            internalName = r.get('internalName'),
            name         = _en(name, language),
            description  = _en(desc, language),
        ))
        plans += 1
        if isinstance(name, dict):
            for lang, text in name.items():
                connection.execute(insert(trn).values(tcID=46, keyID=pid, languageID=lang, text=text))
        if isinstance(desc, dict):
            for lang, text in desc.items():
                connection.execute(insert(trn).values(tcID=47, keyID=pid, languageID=lang, text=text))
        for ms in r.get('milestones') or []:
            connection.execute(insert(tbl_ms).values(
                skillPlanID = pid,
                typeID      = ms['typeID'],
                level       = ms.get('level'),
            ))
            milestones += 1
        seen_reqs = set()
        for req in r.get('skillRequirements') or []:
            key = (pid, req['typeID'], req['level'])
            if key in seen_reqs:
                continue
            seen_reqs.add(key)
            connection.execute(insert(tbl_reqs).values(
                skillPlanID = pid,
                typeID      = req['typeID'],
                level       = req['level'],
            ))
            reqs += 1
    trans.commit()
    print("    {} plans, {} milestone rows, {} skill-req rows".format(plans, milestones, reqs))


def import_expert_systems(connection, metadata, sourcePath, language='en'):
    """expertSystems.jsonl -> expertSystems + expertSystemSkillsGranted"""
    print("Importing expertSystems")
    tbl_es    = Table('expertSystems', metadata)
    tbl_skills = Table('expertSystemSkillsGranted', metadata)
    trans = connection.begin()
    systems = skills = 0
    for r in _jsonl(sourcePath, 'expertSystems.jsonl'):
        tid = r['_key']
        connection.execute(insert(tbl_es).values(
            typeID       = tid,
            durationDays = r.get('durationDays'),
            hidden       = r.get('hidden'),
            internalName = r.get('internalName'),
            retired      = r.get('retired'),
        ))
        systems += 1
        for sg in r.get('skillsGranted') or []:
            connection.execute(insert(tbl_skills).values(
                typeID      = tid,
                skillTypeID = sg['typeID'],
                level       = sg.get('level'),
            ))
            skills += 1
    trans.commit()
    print("    {} expert systems, {} skill rows".format(systems, skills))
