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


def import_archetypes(connection, metadata, sourcePath, language='en'):
    """archetypes.jsonl -> dungeonArchetypes"""
    print("Importing dungeonArchetypes")
    tbl = Table('dungeonArchetypes', metadata)
    trans = connection.begin()
    count = 0
    for r in _jsonl(sourcePath, 'archetypes.jsonl'):
        connection.execute(insert(tbl).values(
            archetypeID = r['_key'],
            title       = _en(r.get('title', {}), language),
            description = _en(r.get('description', {}), language),
        ))
        count += 1
    trans.commit()
    print("    {} rows".format(count))


def import_missions(connection, metadata, sourcePath, language='en'):
    """
    missions.jsonl -> mstMissions + mstMissionMessages + mstMissionExtraStandings
    """
    print("Importing missions")
    tblMissions  = Table('mstMissions', metadata)
    tblMessages  = Table('mstMissionMessages', metadata)
    tblStandings = Table('mstMissionExtraStandings', metadata)
    trans = connection.begin()
    missions = messages = standings = 0

    for r in _jsonl(sourcePath, 'missions.jsonl'):
        mid = r['_key']
        kill = r.get('killMission') or {}
        courier = r.get('courierMission') or {}
        rewards = r.get('missionRewards') or {}
        bonus_reward = rewards.get('bonusReward') or {}
        reward = rewards.get('reward') or {}

        connection.execute(insert(tblMissions).values(
            missionID               = mid,
            missionName             = _en(r.get('name', {}), language),
            hasStandingRewards      = r.get('hasStandingRewards'),
            factionID               = r.get('factionID'),
            corporationID           = r.get('corporationID'),
            agentTypeID             = r.get('agentTypeID'),
            expirationTime          = r.get('expirationTime'),
            initialAgentGiftTypeID  = r.get('initialAgentGiftTypeID'),
            initialAgentGiftQty     = r.get('initialAgentGiftQuantity'),
            killDungeonID           = kill.get('dungeonID'),
            killObjectiveQty        = kill.get('objectiveQuantity'),
            courierObjectiveTypeID  = courier.get('objectiveTypeID'),
            courierObjectiveQty     = courier.get('objectiveQuantity'),
            courierObjectiveSingleton = courier.get('objectiveSingleton'),
            bonusRewardTypeID       = bonus_reward.get('rewardTypeID'),
            bonusRewardQty          = bonus_reward.get('rewardQuantity'),
            bonusTimeInterval       = rewards.get('bonusTimeInterval'),
            rewardTypeID            = reward.get('rewardTypeID'),
            rewardQty               = reward.get('rewardQuantity'),
        ))
        missions += 1

        for msg in r.get('messages', []):
            connection.execute(insert(tblMessages).values(
                missionID  = mid,
                messageKey = msg['_key'],
                text       = _en(msg, language),
            ))
            messages += 1

        for standing in r.get('extraStandings', []):
            connection.execute(insert(tblStandings).values(
                missionID = mid,
                factionID = standing['_key'],
                standing  = standing['_value'],
            ))
            standings += 1

    trans.commit()
    print("    {} missions, {} messages, {} standing rows".format(missions, messages, standings))


def import_epic_arcs(connection, metadata, sourcePath, language='en'):
    """
    epicArcs.jsonl -> epicArcs + epicArcMissions + epicArcMissionNextMissions
    """
    print("Importing epicArcs")
    tblArcs      = Table('epicArcs', metadata)
    tblArcMiss   = Table('epicArcMissions', metadata)
    tblNextMiss  = Table('epicArcMissionNextMissions', metadata)
    trans = connection.begin()
    arcs = arc_missions = next_missions = 0

    for r in _jsonl(sourcePath, 'epicArcs.jsonl'):
        arc_id = r['_key']
        connection.execute(insert(tblArcs).values(
            arcID              = arc_id,
            arcName            = _en(r.get('name', {}), language),
            factionID          = r.get('factionID'),
            iconID             = r.get('iconID'),
            arcRestartInterval = r.get('arcRestartInterval'),
        ))
        arcs += 1

        for m in r.get('missions', []):
            mission_id = m['_key']
            connection.execute(insert(tblArcMiss).values(
                arcID         = arc_id,
                missionID     = mission_id,
                agentID       = m.get('agentID'),
                failMissionID = m.get('failMissionID'),
            ))
            arc_missions += 1

            for next_id in m.get('nextMissions', []):
                connection.execute(insert(tblNextMiss).values(
                    missionID     = mission_id,
                    nextMissionID = next_id,
                ))
                next_missions += 1

    trans.commit()
    print("    {} arcs, {} arc-mission rows, {} next-mission rows".format(arcs, arc_missions, next_missions))


def import_dungeons(connection, metadata, sourcePath, language='en'):
    """
    dungeons.jsonl -> dungeons + dungeonAllowedShips
    """
    print("Importing dungeons")
    tblDungeons = Table('dungeons', metadata)
    tblShips    = Table('dungeonAllowedShips', metadata)
    trans = connection.begin()
    dungeons = ships = 0

    for r in _jsonl(sourcePath, 'dungeons.jsonl'):
        dungeon_id = r['_key']
        connection.execute(insert(tblDungeons).values(
            dungeonID   = dungeon_id,
            dungeonName = _en(r.get('name', {}), language),
            description = _en(r.get('description', {}), language),
            archetypeID = r.get('archetypeID'),
            factionID   = r.get('factionID'),
        ))
        dungeons += 1

        for type_id in r.get('allowedShipsList', []):
            connection.execute(insert(tblShips).values(
                dungeonID = dungeon_id,
                typeID    = type_id,
            ))
            ships += 1

    trans.commit()
    print("    {} dungeons, {} allowed-ship rows".format(dungeons, ships))
