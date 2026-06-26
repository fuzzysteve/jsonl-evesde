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


def import_translation_languages(connection, metadata, sourcePath, language='en'):
    """translationLanguages.jsonl -> trnTranslationLanguages"""
    print("Importing translationLanguages")
    tbl = Table('trnTranslationLanguages', metadata)
    trans = connection.begin()
    count = 0
    for numeric_id, r in enumerate(_jsonl(sourcePath, 'translationLanguages.jsonl'), start=1):
        connection.execute(insert(tbl).values(
            numericLanguageID = numeric_id,
            languageID        = r['_key'],
            languageName      = r.get('name'),
        ))
        count += 1
    trans.commit()
    print("    {} rows".format(count))
