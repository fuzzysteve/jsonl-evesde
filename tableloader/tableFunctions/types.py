# -*- coding: utf-8 -*-



import json
import os
from sqlalchemy import Table,insert



def _trunc(s, length=100):
    """Truncate a string to fit a VARCHAR column. Logs if truncation occurs."""
    if s is None:
        return None
    if len(s) > length:
        print("  WARNING: truncating name to {}: '{}'".format(length, s))
        return s[:length]
    return s


def _en(d, language='en'):
    if isinstance(d, dict):
        return d.get(language) or d.get('en')
    return d


def _jsonl(sourcePath, filename):
    filepath = os.path.join(sourcePath, filename)
    with open(filepath, 'r', encoding='utf-8') as fh:
        for line in fh:
            line = line.strip()
            if line:
                yield json.loads(line)


def import_types(connection,metadata,sourcePath,language='en'):
    invTypes = Table('invTypes',metadata)
    trnTranslations = Table('trnTranslations',metadata)
    invMetaTypes = Table('invMetaTypes',metadata)
    print("Importing Types")
    trans = connection.begin()
    with open(os.path.join(sourcePath,'types.jsonl'), 'r') as json_file:
        json_list = list(json_file)
    for json_str in json_list:
        typedata = json.loads(json_str)
        stmt=insert(invTypes).values(typeID=typedata['_key'],
                            groupID=typedata.get('groupID',0),
                            typeName=_trunc(typedata.get('name',{}).get(language,'')),
                            description=typedata.get('description',{}).get(language,''),
                            mass=typedata.get('mass',0),
                            volume=typedata.get('volume',0),
                            capacity=typedata.get('capacity',0),
                            portionSize=typedata.get('portionSize'),
                            raceID=typedata.get('raceID'),
                            basePrice=typedata.get('basePrice'),
                            published=typedata.get('published',0),
                            marketGroupID=typedata.get('marketGroupID'),
                            graphicID=typedata.get('graphicID',0),
                            iconID=typedata.get('iconID'),
                            soundID=typedata.get('soundID'),
                            factionID=typedata.get('factionID'),
                            metaLevel=typedata.get('metaLevel'),
                            shipTreeGroupID=typedata.get('shipTreeGroupID'))
        connection.execute(stmt)
        if 'metaGroupID' in typedata or 'variationParentTypeID' in typedata:
            stmt=insert(invMetaTypes).values(typeID=typedata['_key'],metaGroupID=typedata.get('metaGroupID'),parentTypeID=typedata.get('variationParentTypeID'))
            connection.execute(stmt)
        if 'name' in typedata:
            for lang in typedata['name']:
                stmt=insert(trnTranslations).values(tcID=8,keyID=typedata['_key'],languageID=lang,text=typedata['name'][lang])
                connection.execute(stmt)
        if 'description' in typedata:
            for lang in typedata['description']:
                stmt=insert(trnTranslations).values(tcID=33,keyID=typedata['_key'],languageID=lang,text=typedata['description'][lang])
                connection.execute(stmt)
    trans.commit()

def import_bonus(connection,metadata,sourcePath,language='en'):
    invTraits = Table('invTraits',metadata)
    print("Importing bonuses")
    trans = connection.begin()
    with open(os.path.join(sourcePath,'typeBonus.jsonl'), 'r') as json_file:
        json_list = list(json_file)
    for json_str in json_list:
        typedata = json.loads(json_str)
        key=typedata['_key']
        if "roleBonuses" in typedata:
            for rolebonus in typedata["roleBonuses"]:
                stmt=insert(invTraits).values(typeID=key,
                                              skillID=-1, 
                                              bonus=rolebonus.get('bonus'),
                                              bonusText=rolebonus.get('bonusText',{}).get(language,''),
                                              unitID=rolebonus.get('unitID'))
                connection.execute(stmt)
        if "types" in typedata:
            for skillbonus in typedata["types"]:
                for skill in skillbonus['_value']:
                    stmt=insert(invTraits).values(typeID=key,
                                                  skillID=skillbonus.get('_key'), 
                                                  bonus=skill.get('bonus'),
                                                  bonusText=skill.get('bonusText',{}).get(language,''),
                                                  unitID=skill.get('unitID'))
                    connection.execute(stmt)
    trans.commit()



def import_materials(connection,metadata,sourcePath,language='en'):
    invTypeMaterials = Table('invTypeMaterials',metadata)
    print("Importing materials")
    trans = connection.begin()
    with open(os.path.join(sourcePath,'typeMaterials.jsonl'), 'r') as json_file:
        json_list = list(json_file)
    for json_str in json_list:
        typedata = json.loads(json_str)
        key=typedata['_key']

        for material in typedata.get('materials',[]):
            stmt=insert(invTypeMaterials).values(typeID=key,materialTypeID=material['materialTypeID'],quantity=material['quantity'])
            connection.execute(stmt)
    trans.commit()


def import_dogma(connection,metadata,sourcePath,language='en'):
    dgmEffects = Table('dgmTypeEffects',metadata)
    dgmAttributes = Table('dgmTypeAttributes',metadata)

    print("Importing type dogma")
    trans = connection.begin()
    with open(os.path.join(sourcePath,'typeDogma.jsonl'), 'r') as json_file:
        json_list = list(json_file)
    for json_str in json_list:
        typedata = json.loads(json_str)
        key=typedata['_key']
        if "dogmaAttributes" in typedata:
            for attribute in typedata["dogmaAttributes"]:
                stmt=insert(dgmAttributes).values(typeID=key,attributeID=attribute.get("attributeID"),valueFloat=attribute.get("value"))
                connection.execute(stmt)
        if "dogmaEffects" in typedata:
            for effect in typedata["dogmaEffects"]:
                stmt=insert(dgmEffects).values(typeID=key,effectID=effect.get("effectID"),isDefault=effect.get("isDefault"))
                connection.execute(stmt)
    trans.commit()



def import_groups(connection,metadata,sourcePath,language='en'):
    invGroups = Table('invGroups',metadata)
    trnTranslations = Table('trnTranslations',metadata)
    print("Importing Groups")
    trans = connection.begin()
    with open(os.path.join(sourcePath,'groups.jsonl'), 'r') as json_file:
        json_list = list(json_file)
    for json_str in json_list:
        groupdata = json.loads(json_str)
        stmt=insert(invGroups).values(groupID=groupdata['_key'],
                                      anchorable=groupdata['anchorable'],
                                      anchored=groupdata['anchored'],
                                      categoryID=groupdata['categoryID'],
                                      fittableNonSingleton=groupdata['fittableNonSingleton'],
                                      groupName=groupdata['name'].get(language,''),
                                      published=groupdata['published'],
                                      useBasePrice=groupdata['useBasePrice'],
                                      iconID=groupdata.get('iconID',None))
        connection.execute(stmt)
        if 'name' in groupdata:
            for lang in groupdata['name']:
                stmt=insert(trnTranslations).values(tcID=7,keyID=groupdata['_key'],languageID=lang,text=groupdata['name'][lang])
                connection.execute(stmt)
    trans.commit()

def import_categories(connection,metadata,sourcePath,language='en'):
    invCategories = Table('invCategories',metadata)
    trnTranslations = Table('trnTranslations',metadata)
    print("Importing Categories")
    trans = connection.begin()
    with open(os.path.join(sourcePath,'categories.jsonl'), 'r') as json_file:
        json_list = list(json_file)
    for json_str in json_list:
        categorydata = json.loads(json_str)
        stmt=insert(invCategories).values(categoryID=categorydata['_key'],
                                      categoryName=categorydata['name'].get(language,''),
                                      published=categorydata['published'],
                                      iconID=categorydata.get('iconID',None))
        connection.execute(stmt)
        if 'name' in categorydata:
            for lang in categorydata['name']:
                stmt=insert(trnTranslations).values(tcID=6,keyID=categorydata['_key'],languageID=lang,text=categorydata['name'][lang])
                connection.execute(stmt)
    trans.commit()

 
def import_meta_groups(connection, metadata, sourcePath, language='en'):
    """metaGroups.jsonl -> invMetaGroups"""
    print("Importing metaGroups")
    tbl = Table('invMetaGroups', metadata)
    trnTranslations = Table('trnTranslations', metadata)
    trans = connection.begin()
    for r in _jsonl(sourcePath, 'metaGroups.jsonl'):
        connection.execute(insert(tbl).values(
            metaGroupID   = r['_key'],
            metaGroupName = _en(r.get('name', {}), language),
            description   = _en(r.get('description', {}), language),
            iconID        = r.get('iconID'),
        ))
        for lang, text in r.get('name', {}).items():
            connection.execute(insert(trnTranslations).values(
                tcID=15, keyID=r['_key'], languageID=lang, text=text))
    trans.commit()
 
def import_market_groups(connection, metadata, sourcePath, language='en'):
    """marketGroups.jsonl -> invMarketGroups + trnTranslations"""
    print("Importing marketGroups")
    tbl = Table('invMarketGroups', metadata)
    trnTranslations = Table('trnTranslations', metadata)
    trans = connection.begin()
    count = 0
    for r in _jsonl(sourcePath, 'marketGroups.jsonl'):
        connection.execute(insert(tbl).values(
            marketGroupID   = r['_key'],
            parentGroupID   = r.get('parentGroupID'),
            marketGroupName = _en(r.get('name', {}), language),
            description     = _en(r.get('description', {}), language),
            iconID          = r.get('iconID'),
            hasTypes        = r.get('hasTypes'),
        ))
        for lang, text in r.get('name', {}).items():
            connection.execute(insert(trnTranslations).values(
                tcID=14, keyID=r['_key'], languageID=lang, text=text))
        count += 1
    trans.commit()
    print("    {} rows".format(count))

