# -*- coding: utf-8 -*-

import json
import os
from sqlalchemy import Table, insert, select, text

def buildSkills(connection, connectiontype):
    """
    Populate the ship skills table with data from dogma
    """
    sql = {}
    sql['postgres'] = [
            """insert into "shipSkills" ("typeID","groupID","skillID",level) (
            select "typeID", groupid,max(skill) skill,max(skilllevel) skilllevel from
(
select "typeID",1 groupid, coalesce("valueInt","valueFloat",0) skill, 0 skilllevel from "dgmTypeAttributes" where "attributeID"=182
union
select "typeID",2 groupid, coalesce("valueInt","valueFloat",0) skill, 0 skilllevel from "dgmTypeAttributes" where "attributeID"=183
union
select "typeID",3 groupid, coalesce("valueInt","valueFloat",0) skill, 0 skilllevel from "dgmTypeAttributes" where "attributeID"=184
union
select "typeID",4 groupid, coalesce("valueInt","valueFloat",0) skill, 0 skilllevel from "dgmTypeAttributes" where "attributeID"=1285
union
select "typeID",5 groupid, coalesce("valueInt","valueFloat",0) skill, 0 skilllevel from "dgmTypeAttributes" where "attributeID"=1289
union
select "typeID",6 groupid, coalesce("valueInt","valueFloat",0) skill, 0 skilllevel from "dgmTypeAttributes" where "attributeID"=1290
union
select "typeID",1 groupid, 0 skill, coalesce("valueInt","valueFloat",0) skilllevel from "dgmTypeAttributes" where "attributeID"=277
union
select "typeID",2 groupid, 0 skill, coalesce("valueInt","valueFloat",0) skilllevel from "dgmTypeAttributes" where "attributeID"=278
union
select "typeID",3 groupid, 0 skill, coalesce("valueInt","valueFloat",0) skilllevel from "dgmTypeAttributes" where "attributeID"=279
union
select "typeID",4 groupid, 0 skill, coalesce("valueInt","valueFloat",0) skilllevel from "dgmTypeAttributes" where "attributeID"=1286
union
select "typeID",5 groupid, 0 skill, coalesce("valueInt","valueFloat",0) skilllevel from "dgmTypeAttributes" where "attributeID"=1287
union
select "typeID",6 groupid, 0 skill, coalesce("valueInt","valueFloat",0) skilllevel from "dgmTypeAttributes" where "attributeID"=1288
) skills group by "typeID",groupid)"""
    ]
    sql['postgresschema'] = [
            """insert into evesde."shipSkills" ("typeID","groupID","skillID",level) (
            select "typeID", groupid,max(skill) skill,max(skilllevel) skilllevel from
(
select "typeID",1 groupid, coalesce("valueInt","valueFloat",0) skill, 0 skilllevel from evesde."dgmTypeAttributes" where "attributeID"=182
union
select "typeID",2 groupid, coalesce("valueInt","valueFloat",0) skill, 0 skilllevel from evesde."dgmTypeAttributes" where "attributeID"=183
union
select "typeID",3 groupid, coalesce("valueInt","valueFloat",0) skill, 0 skilllevel from evesde."dgmTypeAttributes" where "attributeID"=184
union
select "typeID",4 groupid, coalesce("valueInt","valueFloat",0) skill, 0 skilllevel from evesde."dgmTypeAttributes" where "attributeID"=1285
union
select "typeID",5 groupid, coalesce("valueInt","valueFloat",0) skill, 0 skilllevel from evesde."dgmTypeAttributes" where "attributeID"=1289
union
select "typeID",6 groupid, coalesce("valueInt","valueFloat",0) skill, 0 skilllevel from evesde."dgmTypeAttributes" where "attributeID"=1290
union
select "typeID",1 groupid, 0 skill, coalesce("valueInt","valueFloat",0) skilllevel from evesde."dgmTypeAttributes" where "attributeID"=277
union
select "typeID",2 groupid, 0 skill, coalesce("valueInt","valueFloat",0) skilllevel from evesde."dgmTypeAttributes" where "attributeID"=278
union
select "typeID",3 groupid, 0 skill, coalesce("valueInt","valueFloat",0) skilllevel from evesde."dgmTypeAttributes" where "attributeID"=279
union
select "typeID",4 groupid, 0 skill, coalesce("valueInt","valueFloat",0) skilllevel from evesde."dgmTypeAttributes" where "attributeID"=1286
union
select "typeID",5 groupid, 0 skill, coalesce("valueInt","valueFloat",0) skilllevel from evesde."dgmTypeAttributes" where "attributeID"=1287
union
select "typeID",6 groupid, 0 skill, coalesce("valueInt","valueFloat",0) skilllevel from evesde."dgmTypeAttributes" where "attributeID"=1288
) skills group by "typeID",groupid)"""
]
    sql['other'] = [
            """insert into shipSkills (typeID,groupID,skillID,level) (
            select typeid, groupid,max(skill) skill,max(skilllevel) skilllevel from
(
select typeid,1 groupid, coalesce(valueInt,valueFloat,0) skill, 0 skilllevel from dgmTypeAttributes where attributeID=182
union
select typeid,2 groupid, coalesce(valueInt,valueFloat,0) skill, 0 skilllevel from dgmTypeAttributes where attributeID=183
union
select typeid,3 groupid, coalesce(valueInt,valueFloat,0) skill, 0 skilllevel from dgmTypeAttributes where attributeID=184
union
select typeid,4 groupid, coalesce(valueInt,valueFloat,0) skill, 0 skilllevel from dgmTypeAttributes where attributeID=1285
union
select typeid,5 groupid, coalesce(valueInt,valueFloat,0) skill, 0 skilllevel from dgmTypeAttributes where attributeID=1289
union
select typeid,6 groupid, coalesce(valueInt,valueFloat,0) skill, 0 skilllevel from dgmTypeAttributes where attributeID=1290
union
select typeid,1 groupid, 0 skill, coalesce(valueInt,valueFloat,0) skilllevel from dgmTypeAttributes where attributeID=277
union
select typeid,2 groupid, 0 skill, coalesce(valueInt,valueFloat,0) skilllevel from dgmTypeAttributes where attributeID=278
union
select typeid,3 groupid, 0 skill, coalesce(valueInt,valueFloat,0) skilllevel from dgmTypeAttributes where attributeID=279
union
select typeid,4 groupid, 0 skill, coalesce(valueInt,valueFloat,0) skilllevel from dgmTypeAttributes where attributeID=1286
union
select typeid,5 groupid, 0 skill, coalesce(valueInt,valueFloat,0) skilllevel from dgmTypeAttributes where attributeID=1287
union
select typeid,6 groupid, 0 skill, coalesce(valueInt,valueFloat,0) skilllevel from dgmTypeAttributes where attributeID=1288
) skills group by typeid,groupid)"""
    ]

    sql['sqlite'] = [
            """insert into shipSkills (typeID,groupID,skillID,level) 
            select typeid, groupid,max(skill) skill,max(skilllevel) skilllevel from
(
select typeid,1 groupid, coalesce(valueInt,valueFloat,0) skill, 0 skilllevel from dgmTypeAttributes where attributeID=182
union
select typeid,2 groupid, coalesce(valueInt,valueFloat,0) skill, 0 skilllevel from dgmTypeAttributes where attributeID=183
union
select typeid,3 groupid, coalesce(valueInt,valueFloat,0) skill, 0 skilllevel from dgmTypeAttributes where attributeID=184
union
select typeid,4 groupid, coalesce(valueInt,valueFloat,0) skill, 0 skilllevel from dgmTypeAttributes where attributeID=1285
union
select typeid,5 groupid, coalesce(valueInt,valueFloat,0) skill, 0 skilllevel from dgmTypeAttributes where attributeID=1289
union
select typeid,6 groupid, coalesce(valueInt,valueFloat,0) skill, 0 skilllevel from dgmTypeAttributes where attributeID=1290
union
select typeid,1 groupid, 0 skill, coalesce(valueInt,valueFloat,0) skilllevel from dgmTypeAttributes where attributeID=277
union
select typeid,2 groupid, 0 skill, coalesce(valueInt,valueFloat,0) skilllevel from dgmTypeAttributes where attributeID=278
union
select typeid,3 groupid, 0 skill, coalesce(valueInt,valueFloat,0) skilllevel from dgmTypeAttributes where attributeID=279
union
select typeid,4 groupid, 0 skill, coalesce(valueInt,valueFloat,0) skilllevel from dgmTypeAttributes where attributeID=1286
union
select typeid,5 groupid, 0 skill, coalesce(valueInt,valueFloat,0) skilllevel from dgmTypeAttributes where attributeID=1287
union
select typeid,6 groupid, 0 skill, coalesce(valueInt,valueFloat,0) skilllevel from dgmTypeAttributes where attributeID=1288
) skills group by typeid,groupid"""
]
    if connectiontype in ( 'mysql', 'mssql'):
        connectiontype = 'other'

    print("Building shipskills tables")
    trans = connection.begin()
    for statement in sql[connectiontype]:
        connection.execute(text(statement))
    trans.commit()
    print("shipskills complete")

