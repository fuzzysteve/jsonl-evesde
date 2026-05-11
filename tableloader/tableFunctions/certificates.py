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


# certLevelText -> certLevelInt mapping
_CERT_LEVELS = {'basic': 1, 'standard': 2, 'improved': 3, 'advanced': 4, 'elite': 5}


def import_certificates(connection, metadata, sourcePath, language='en'):
    """
    certificates.jsonl -> certCerts + certSkills + trnTranslations
    """
    print("Importing certificates")
    certCerts       = Table('certCerts', metadata)
    certSkills      = Table('certSkills', metadata)
    trnTranslations = Table('trnTranslations', metadata)
    trans = connection.begin()
    certs = skills = 0
    for r in _jsonl(sourcePath, 'certificates.jsonl'):
        certID = r['_key']
        connection.execute(insert(certCerts).values(
            certID      = certID,
            description = _en(r.get('description', {}), language),
            groupID     = r.get('groupID'),
            name        = _en(r.get('name', {}), language),
        ))
        certs += 1
        for skill in r.get('skillTypes', []):
            skillID = skill['_key']
            for levelText, certLevelInt in _CERT_LEVELS.items():
                skillLevel = skill.get(levelText)
                if skillLevel is not None:
                    connection.execute(insert(certSkills).values(
                        certID        = certID,
                        skillID       = skillID,
                        certLevelInt  = certLevelInt,
                        skillLevel    = skillLevel,
                        certLevelText = levelText,
                    ))
                    skills += 1
        for lang, text in r.get('name', {}).items():
            connection.execute(insert(trnTranslations).values(
                tcID=17, keyID=certID, languageID=lang, text=text))
    trans.commit()
    print("    {} certs, {} skill rows".format(certs, skills))


def import_masteries(connection, metadata, sourcePath, language='en'):
    """
    masteries.jsonl -> certMasteries(typeID, masteryLevel, certID)
    """
    print("Importing masteries")
    tbl = Table('certMasteries', metadata)
    trans = connection.begin()
    count = 0
    for r in _jsonl(sourcePath, 'masteries.jsonl'):
        typeID = r['_key']
        for level_entry in r.get('_value', []):
            masteryLevel = level_entry['_key']
            for certID in level_entry.get('_value', []):
                connection.execute(insert(tbl).values(
                    typeID       = typeID,
                    masteryLevel = masteryLevel,
                    certID       = certID,
                ))
                count += 1
    trans.commit()
    print("    {} rows".format(count))
