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


def import_agent_types(connection, metadata, sourcePath, language='en'):
    """agentTypes.jsonl -> agtAgentTypes"""
    print("Importing agentTypes")
    tbl = Table('agtAgentTypes', metadata)
    trans = connection.begin()
    count = 0
    for r in _jsonl(sourcePath, 'agentTypes.jsonl'):
        connection.execute(insert(tbl).values(
            agentTypeID = r['_key'],
            agentType   = r.get('name'),
        ))
        count += 1
    trans.commit()
    print("    {} rows".format(count))


def import_agents_in_space(connection, metadata, sourcePath, language='en'):
    """agentsInSpace.jsonl -> agtAgentsInSpace"""
    print("Importing agentsInSpace")
    tbl = Table('agtAgentsInSpace', metadata)
    trans = connection.begin()
    count = 0
    for r in _jsonl(sourcePath, 'agentsInSpace.jsonl'):
        connection.execute(insert(tbl).values(
            agentID      = r['_key'],
            dungeonID    = r.get('dungeonID'),
            solarSystemID= r.get('solarSystemID'),
            spawnPointID = r.get('spawnPointID'),
            typeID       = r.get('typeID'),
        ))
        count += 1
    trans.commit()
    print("    {} rows".format(count))
