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


def import_military_campaigns(connection, metadata, sourcePath, language='en'):
    """militaryCampaigns.jsonl -> milCampaigns"""
    print("Importing militaryCampaigns")
    tbl = Table('milCampaigns', metadata)
    trans = connection.begin()
    count = 0
    for r in _jsonl(sourcePath, 'militaryCampaigns.jsonl'):
        ann = r.get('annotations', {})
        connection.execute(insert(tbl).values(
            campaignID       = r['_key'],
            title            = _en(r.get('title', {}), language),
            subtitle         = _en(r.get('subtitle', {}), language),
            factionID        = r.get('issuer', {}).get('factionID'),
            targetProgress   = r.get('targetProgress'),
            campaignSet      = ann.get('campaignSet'),
            race             = ann.get('race'),
            themePack        = ann.get('themePack'),
            mapFocusEntityID = ann.get('mapFocusEntityID'),
        ))
        count += 1
    trans.commit()
    print("    {} rows".format(count))


def import_military_campaign_objectives(connection, metadata, sourcePath, language='en'):
    """militaryCampaignObjectives.jsonl -> milCampaignObjectives + milCampaignObjContentTags"""
    print("Importing militaryCampaignObjectives")
    tblObj  = Table('milCampaignObjectives', metadata)
    tblTags = Table('milCampaignObjContentTags', metadata)
    trans = connection.begin()
    objectives = tags = 0

    for r in _jsonl(sourcePath, 'militaryCampaignObjectives.jsonl'):
        obj_id = r['_key']
        isk     = (r.get('rewards') or {}).get('isk') or {}
        lp      = (r.get('rewards') or {}).get('lp') or {}
        standing = (r.get('rewards') or {}).get('standing') or {}
        cmc     = r.get('contributionMethodConfiguration') or {}
        ann     = r.get('annotations') or {}

        connection.execute(insert(tblObj).values(
            objectiveID                 = obj_id,
            campaignID                  = r.get('campaignID'),
            title                       = _en(r.get('title', {}), language),
            subtitle                    = _en(r.get('subtitle', {}), language),
            careerPath                  = r.get('careerPath'),
            targetProgress              = r.get('targetProgress'),
            maxProgressPerParticipant   = r.get('maxProgressPerParticipant'),
            presentingCharacterID       = r.get('presentingCharacterID'),
            issuerCorporationID         = (r.get('issuer') or {}).get('corporationID'),
            contributionMethod          = cmc.get('name'),
            contributionParameters      = json.dumps(cmc.get('parameters')) if cmc.get('parameters') else None,
            requiredEnlistmentFactionID = ann.get('requiredEnlistmentWithFactionID'),
            rewardIskAmount             = isk.get('amountPerInterval'),
            rewardIskInterval           = isk.get('progressInterval'),
            rewardIskCorporationID      = (isk.get('issuer') or {}).get('corporationID'),
            rewardLpAmount              = lp.get('amountPerInterval'),
            rewardLpInterval            = lp.get('progressInterval'),
            rewardLpCorporationID       = (lp.get('issuer') or {}).get('corporationID'),
            rewardStandingPercent       = standing.get('gainPercentPerInterval'),
            rewardStandingInterval      = standing.get('progressInterval'),
            rewardStandingFactionID     = (standing.get('issuer') or {}).get('factionID'),
        ))
        objectives += 1

        for tag in r.get('contentTags', []):
            connection.execute(insert(tblTags).values(
                objectiveID = obj_id,
                tag         = tag,
            ))
            tags += 1

    trans.commit()
    print("    {} objectives, {} content-tag rows".format(objectives, tags))


def import_mercenary_tactical_operations(connection, metadata, sourcePath, language='en'):
    """mercenaryTacticalOperations.jsonl -> mercenaryTacticalOperations"""
    print("Importing mercenaryTacticalOperations")
    tbl = Table('mercenaryTacticalOperations', metadata)
    trans = connection.begin()
    count = 0
    for r in _jsonl(sourcePath, 'mercenaryTacticalOperations.jsonl'):
        connection.execute(insert(tbl).values(
            operationID      = r['_key'],
            name             = _en(r.get('name', {}), language),
            description      = _en(r.get('description', {}), language),
            dungeonID        = r.get('dungeonID'),
            anarchyImpact    = r.get('anarchyImpact'),
            developmentImpact = r.get('developmentImpact'),
            infomorphBonus   = r.get('infomorphBonus'),
        ))
        count += 1
    trans.commit()
    print("    {} rows".format(count))


def import_sovereignty_upgrades(connection, metadata, sourcePath, language='en'):
    """sovereigntyUpgrades.jsonl -> sovereigntyUpgrades"""
    print("Importing sovereigntyUpgrades")
    tbl = Table('sovereigntyUpgrades', metadata)
    trans = connection.begin()
    count = 0
    for r in _jsonl(sourcePath, 'sovereigntyUpgrades.jsonl'):
        fuel = r.get('fuel') or {}
        connection.execute(insert(tbl).values(
            typeID                = r['_key'],
            mutuallyExclusiveGroup = r.get('mutually_exclusive_group'),
            powerAllocation       = r.get('power_allocation'),
            powerProduction       = r.get('power_production'),
            workforceAllocation   = r.get('workforce_allocation'),
            workforceProduction   = r.get('workforce_production'),
            fuelTypeID            = fuel.get('type_id'),
            fuelHourlyUpkeep      = fuel.get('hourly_upkeep'),
            fuelStartupCost       = fuel.get('startup_cost'),
        ))
        count += 1
    trans.commit()
    print("    {} rows".format(count))
