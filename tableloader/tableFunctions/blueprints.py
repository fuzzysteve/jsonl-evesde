# -*- coding: utf-8 -*-



import json
import os
from sqlalchemy import Table,insert

def import_blueprints(connection,metadata,sourcePath,language='en'):


    activityIDs={"copying":5,"manufacturing":1,"research_material":4,"research_time":3,"invention":8,'reaction':11}



    industryBlueprints=Table('industryBlueprints',metadata)
    industryActivity = Table('industryActivity',metadata)
    industryActivityMaterials = Table('industryActivityMaterials',metadata)
    industryActivityProducts = Table('industryActivityProducts',metadata)
    industryActivitySkills = Table('industryActivitySkills',metadata)
    industryActivityProbabilities = Table('industryActivityProbabilities',metadata)

    print("Importing Blueprints")
    trans = connection.begin()
    with open(os.path.join(sourcePath,'blueprints.jsonl'), 'r') as json_file:
        json_list = list(json_file)
    for json_str in json_list:
        blueprints = json.loads(json_str)

        blueprintid=blueprints["_key"]

        stmt=insert(industryBlueprints).values(typeID=blueprintid,
                                          maxProductionLimit=blueprints["maxProductionLimit"]
                                          )
        connection.execute(stmt)


        for activity in blueprints["activities"]:
            stmt=insert(industryActivity).values(typeID=blueprintid,
                                                 activityID=activityIDs[activity],
                                                 time=blueprints["activities"][activity]['time'])
            connection.execute(stmt)
            if 'materials' in blueprints['activities'][activity]:
                for material in blueprints['activities'][activity]['materials']:
                    stmt=insert(industryActivityMaterials).values(
                                            typeID=blueprintid,
                                            activityID=activityIDs[activity],
                                            materialTypeID=material['typeID'],
                                            quantity=material['quantity'])
                    connection.execute(stmt)
            if 'products' in blueprints['activities'][activity]:
                for product in blueprints['activities'][activity]['products']:
                    stmt=insert(industryActivityProducts).values(
                                            typeID=blueprintid,
                                            activityID=activityIDs[activity],
                                            productTypeID=product['typeID'],
                                            quantity=product['quantity'])
                    connection.execute(stmt)
                    if 'probability' in product:
                        stmt=insert(industryActivityProbabilities).values(
                                                typeID=blueprintid,
                                                activityID=activityIDs[activity],
                                                productTypeID=product['typeID'],
                                                probability=product['probability'])
                        connection.execute(stmt)
            try:
                if 'skills' in blueprints['activities'][activity]:
                    for skill in blueprints['activities'][activity]['skills']:
                        stmt=insert(industryActivitySkills).values(
                                            typeID=blueprintid,
                                                activityID=activityIDs[activity],
                                                skillID=skill['typeID'],
                                                level=skill['level'])
                        connection.execute(stmt)
            except:
                print('{} has a bad skill'.format(blueprint))

    trans.commit()
