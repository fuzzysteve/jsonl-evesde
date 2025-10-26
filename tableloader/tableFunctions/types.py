# -*- coding: utf-8 -*-



import json
import os
from sqlalchemy import Table,insert

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
                            typeName=typedata.get('name',{}).get(language,''),
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
                            soundID=typedata.get('soundID'))
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

        for material in typedata['materials']:
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
