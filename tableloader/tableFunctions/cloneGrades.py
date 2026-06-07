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
 
 
def import_clone_grades(connection, metadata, sourcePath, language='en'):
    """
    cloneGrades.jsonl -> chrCloneGrades + chrCloneGradeSkills
 
    Clone grades are named skill cap presets (Alpha clone skill sets).
    _key is the raceID bitmask (1=Caldari, 2=Minmatar, 4=Amarr, 8=Gallente).
    """
    print("Importing cloneGrades")
    tblCloneGrades = Table('chrCloneGrades', metadata)
    tblSkills     = Table('chrCloneGradeSkills', metadata)
    trans = connection.begin()
    grades = skills = 0
    for r in _jsonl(sourcePath, 'cloneGrades.jsonl'):
        cloneGradeID = r['_key']
        connection.execute(insert(tblCloneGrades).values(
            cloneGradeID = cloneGradeID,
            name        = r.get('name'),
        ))
        grades += 1
        for skill in r.get('skills', []):
            connection.execute(insert(tblSkills).values(
                cloneGradeID = cloneGradeID,
                typeID      = skill['typeID'],
                level       = skill['level'],
            ))
            skills += 1
    trans.commit()
    print("    {} clone grades, {} skill rows".format(grades, skills))

