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


def import_freelance_job_schemas(connection, metadata, sourcePath, language='en'):
    """
    freelanceJobSchemas.jsonl -> freelanceJobSchemas
                              -> freelanceJobSchemaContentTags
                              -> freelanceJobSchemaParameters

    The file has a single outer record whose _value is the list of job schemas.
    """
    print("Importing freelanceJobSchemas")
    tblSchemas = Table('freelanceJobSchemas', metadata)
    tblTags    = Table('freelanceJobSchemaContentTags', metadata)
    tblParams  = Table('freelanceJobSchemaParameters', metadata)
    trans = connection.begin()
    schemas = tags = params = 0

    for outer in _jsonl(sourcePath, 'freelanceJobSchemas.jsonl'):
        for j in outer.get('_value', []):
            job_id = j['_key']
            connection.execute(insert(tblSchemas).values(
                jobSchemaID         = job_id,
                title               = _en(j.get('title', {}), language),
                description         = _en(j.get('description', {}), language),
                progressDescription = _en(j.get('progressDescription', {}), language),
                rewardDescription   = _en(j.get('rewardDescription', {}), language),
                targetDescription   = _en(j.get('targetDescription', {}), language),
                iconID              = j.get('iconID'),
            ))
            schemas += 1

            for tag in j.get('contentTags', []):
                connection.execute(insert(tblTags).values(
                    jobSchemaID = job_id,
                    tag         = tag,
                ))
                tags += 1

            for p in j.get('parameters', []):
                matcher = p.get('matcher') or {}
                accepted = matcher.get('acceptedValueTypes')
                connection.execute(insert(tblParams).values(
                    jobSchemaID        = job_id,
                    paramKey           = p['_key'],
                    acceptedValueTypes = json.dumps(accepted) if accepted is not None else None,
                ))
                params += 1

    trans.commit()
    print("    {} schemas, {} tag rows, {} parameter rows".format(schemas, tags, params))
