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


def _log(msg):
    from datetime import datetime
    print(f"[{datetime.now():%H:%M:%S}] {msg}")


# Language codes used by the SDE's localised-string dicts (matches trnTranslationLanguages).
_LANGS = {'de', 'en', 'es', 'fr', 'ja', 'ko', 'ru', 'zh'}

# trnTranslations tcIDs owned by this module (see CLAUDE.md tcID table).
TCID_CAMPAIGN_ANNOTATIONS = 51
TCID_OBJECTIVE_ANNOTATIONS = 52


def _is_localised(v):
    return isinstance(v, dict) and bool(v) and set(v) <= _LANGS


def _is_empty(v):
    return v == {} or v == []


def _strip_empty(v):
    """Recursively drop dict entries whose value is {} or [].

    The SDE API emits unset fields as empties where the zip omitted them;
    stripping keeps JSON passed through to the DB identical to before."""
    if isinstance(v, dict):
        return {k: _strip_empty(x) for k, x in v.items() if not _is_empty(x)}
    if isinstance(v, list):
        return [_strip_empty(x) for x in v]
    return v


def _insert_annotations(connection, tbl, trn, owner_col, records, tcID, language):
    """Write every annotations entry of `records` to the key/value table `tbl`.

    Localised values store the load-language text in `value` and every language in
    trnTranslations under `tcID` (keyID = annotationID). Other scalars are stored as text;
    anything structured is stored as JSON. Returns (annotation rows, translation rows)."""
    annotation_id = 0
    translations = 0
    for r in sorted(records, key=lambda r: r[owner_col]):
        ann = r.get('annotations') or {}
        for key in sorted(ann):
            v = ann[key]
            if _is_empty(v):
                # unset in the zip; don't let it become a row and shift annotationIDs
                continue
            annotation_id += 1
            if _is_localised(v):
                value = _en(v, language)
                for lang, text in v.items():
                    connection.execute(insert(trn).values(
                        tcID=tcID, keyID=annotation_id, languageID=lang, text=text))
                    translations += 1
            elif v is None or isinstance(v, str):
                value = v
            elif isinstance(v, (dict, list)):
                value = json.dumps(v, ensure_ascii=False)
            else:
                value = str(v)
            connection.execute(insert(tbl).values(**{
                'annotationID':  annotation_id,
                owner_col:       r[owner_col],
                'annotationKey': key,
                'value':         value,
            }))
    return annotation_id, translations

def import_military_campaigns(connection, metadata, sourcePath, language='en'):
    """militaryCampaigns.jsonl -> milCampaigns + milCampaignAnnotations (+ trnTranslations tcID 51)"""
    _log("Importing militaryCampaigns")
    tbl      = Table('milCampaigns', metadata)
    tblAnn   = Table('milCampaignAnnotations', metadata)
    trn      = Table('trnTranslations', metadata)
    trn_cols = Table('trnTranslationColumns', metadata)
    trans = connection.begin()
    connection.execute(insert(trn_cols).values(tcGroupID=None, tcID=TCID_CAMPAIGN_ANNOTATIONS,
                                               tableName='milCampaignAnnotations', columnName='value',
                                               masterID='annotationID'))
    count = 0
    records = list(_jsonl(sourcePath, 'militaryCampaigns.jsonl'))
    for r in records:
        ann = r.get('annotations') or {}
        connection.execute(insert(tbl).values(
            campaignID       = r['campaignID'],
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
    annotations, translations = _insert_annotations(
        connection, tblAnn, trn, 'campaignID', records, TCID_CAMPAIGN_ANNOTATIONS, language)
    trans.commit()
    _log("    {} rows, {} annotations, {} annotation translations".format(count, annotations, translations))


def import_military_campaign_objectives(connection, metadata, sourcePath, language='en'):
    """militaryCampaignObjectives.jsonl -> milCampaignObjectives + milCampaignObjContentTags
    + milCampaignObjAnnotations (+ trnTranslations tcID 52)"""
    _log("Importing militaryCampaignObjectives")
    tblObj   = Table('milCampaignObjectives', metadata)
    tblTags  = Table('milCampaignObjContentTags', metadata)
    tblAnn   = Table('milCampaignObjAnnotations', metadata)
    trn      = Table('trnTranslations', metadata)
    trn_cols = Table('trnTranslationColumns', metadata)
    trans = connection.begin()
    connection.execute(insert(trn_cols).values(tcGroupID=None, tcID=TCID_OBJECTIVE_ANNOTATIONS,
                                               tableName='milCampaignObjAnnotations', columnName='value',
                                               masterID='annotationID'))
    objectives = tags = 0

    records = list(_jsonl(sourcePath, 'militaryCampaignObjectives.jsonl'))
    for r in records:
        obj_id = r['objectiveID']
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
            contributionParameters      = _strip_empty(cmc.get('parameters')),
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

    annotations, translations = _insert_annotations(
        connection, tblAnn, trn, 'objectiveID', records, TCID_OBJECTIVE_ANNOTATIONS, language)
    trans.commit()
    _log("    {} objectives, {} content-tag rows, {} annotations, {} annotation translations".format(
        objectives, tags, annotations, translations))


def import_mercenary_tactical_operations(connection, metadata, sourcePath, language='en'):
    """mercenaryTacticalOperations.jsonl -> mercenaryTacticalOperations"""
    _log("Importing mercenaryTacticalOperations")
    tbl = Table('mercenaryTacticalOperations', metadata)
    trans = connection.begin()
    count = 0
    for r in _jsonl(sourcePath, 'mercenaryTacticalOperations.jsonl'):
        connection.execute(insert(tbl).values(
            operationID      = r['dungeonKey'],
            name             = _en(r.get('name', {}), language),
            description      = _en(r.get('description', {}), language),
            dungeonID        = r.get('dungeonID'),
            anarchyImpact    = r.get('anarchyImpact'),
            developmentImpact = r.get('developmentImpact'),
            infomorphBonus   = r.get('infomorphBonus'),
        ))
        count += 1
    trans.commit()
    _log("    {} rows".format(count))


def import_sovereignty_upgrades(connection, metadata, sourcePath, language='en'):
    """sovereigntyUpgrades.jsonl -> sovereigntyUpgrades"""
    _log("Importing sovereigntyUpgrades")
    tbl = Table('sovereigntyUpgrades', metadata)
    trans = connection.begin()
    count = 0
    for r in _jsonl(sourcePath, 'sovereigntyUpgrades.jsonl'):
        fuel = r.get('fuel') or {}
        connection.execute(insert(tbl).values(
            typeID                = r['typeID'],
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
    _log("    {} rows".format(count))
