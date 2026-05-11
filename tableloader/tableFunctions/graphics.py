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


def import_graphics(connection, metadata, sourcePath, language='en'):
    """graphics.jsonl -> eveGraphics"""
    print("Importing graphics")
    tbl = Table('eveGraphics', metadata)
    trans = connection.begin()
    count = 0
    for r in _jsonl(sourcePath, 'graphics.jsonl'):
        connection.execute(insert(tbl).values(
            graphicID      = r['_key'],
            sofFactionName = r.get('sofFactionName'),
            graphicFile    = r.get('graphicFile'),
            sofHullName    = r.get('sofHullName'),
            sofRaceName    = r.get('sofRaceName'),
            iconFolder     = r.get('iconFolder'),
            sofMaterialSetID = r.get('sofMaterialSetID'),
            description    = None,
        ))
        count += 1
    trans.commit()
    print("    {} rows".format(count))


def import_icons(connection, metadata, sourcePath, language='en'):
    """icons.jsonl -> eveIcons"""
    print("Importing icons")
    tbl = Table('eveIcons', metadata)
    trans = connection.begin()
    count = 0
    for r in _jsonl(sourcePath, 'icons.jsonl'):
        connection.execute(insert(tbl).values(
            iconID      = r['_key'],
            iconFile    = r.get('iconFile'),
            description = None,
        ))
        count += 1
    trans.commit()
    print("    {} rows".format(count))
