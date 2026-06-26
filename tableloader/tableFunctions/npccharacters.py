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


def import_npc_characters(connection, metadata, sourcePath, language='en'):
    """
    npcCharacters.jsonl -> agtNpcCharacters (all records)
                        -> agtAgents (records with an 'agent' sub-dict only)
    """
    print("Importing npcCharacters")
    tblChars  = Table('npcCharacters', metadata)
    tblAgents = Table('agtAgents', metadata)
    tblResearch = Table('agtResearchAgents', metadata)
    trans = connection.begin()
    total = agents = research = 0
    for r in _jsonl(sourcePath, 'npcCharacters.jsonl'):
        connection.execute(insert(tblChars).values(
            characterID   = r['_key'],
            characterName = _en(r.get('name', {}), language),
            corporationID = r.get('corporationID'),
            locationID    = r.get('locationID'),
            raceID        = r.get('raceID'),
            bloodlineID   = r.get('bloodlineID'),
            ancestryID    = r.get('ancestryID'),
            gender        = r.get('gender'),
            careerID      = r.get('careerID'),
            schoolID      = r.get('schoolID'),
            specialityID  = r.get('specialityID'),
            startDate     = r.get('startDate'),
            isCeo         = r.get('ceo'),
        ))
        total += 1
        agent = r.get('agent')
        if agent:
            connection.execute(insert(tblAgents).values(
                agentID       = r['_key'],
                divisionID    = agent.get('divisionID'),
                corporationID = r.get('corporationID'),
                locationID    = r.get('locationID'),
                level         = agent.get('level'),
                quality       = agent.get('quality'),
                agentTypeID   = agent.get('agentTypeID'),
                isLocator     = agent.get('isLocator'),
            ))
            agents += 1
        for skill in r.get('skills', []):
            connection.execute(insert(tblResearch).values(
                agentID = r['_key'],
                typeID  = skill['typeID'],
            ))
            research += 1

    trans.commit()
    print("    {} characters, {} agents".format(total, agents))
