# CLAUDE.md

## Environment

Always use `.venv/bin/python`, never system `python3`. SQLAlchemy and all DB drivers are installed only in the virtualenv.

## Architecture

`Load.py` is the entry point. It reads `sdeloader.cfg` for DB URLs and the SDE source path, then dispatches to loader modules in `tableloader/tableFunctions/`. All table schemas live in `tableloader/tables.py`.

The loader pipeline has two modes:
- **Full mode** (default): `drop_all` → `create_all` → run every loader in `LOADERS` order.
- **Update mode** (`SDE_CHANGED_FILES` env var set): for each loader whose JSONL files appear in the changed set, delete trnTranslations rows by tcID, drop/recreate only that loader's tables, then run its calls. Derived loaders (shipskills, buildjumps, invnames) fire when any of their `depends_on_modules` ran.

## Key patterns

### JSONL helpers

Every loader module has a local `_jsonl(sourcePath, filename)` generator that streams records. Each record is a dict; the primary key is always `r['_key']`.

```python
for r in _jsonl(sourcePath, 'types.jsonl'):
    typeID = r['_key']
```

### Localised strings

`_en(d, language)` (or inline `d.get(language)`) extracts a string from a language-keyed dict. Some loaders write all languages to `trnTranslations`; others just take the `en` value.

### Nullable nested dicts

When a JSON field can be an explicit `null`, `.get('key', {})` returns `None` rather than the fallback `{}`. Use the `or` idiom:

```python
hull = (r.get('colorHull') or {})
```

### Transactions

Each loader function opens a transaction explicitly:

```python
trans = connection.begin()
# ... inserts ...
trans.commit()
```

## Adding a new table

1. **`tableloader/tables.py`** — add a `Table(...)` definition inside `metadataCreator(schema)`. Follow the existing style: explicit `schema=schema`, `autoincrement=False` on PKs, `FLOAT(precision=53)` for doubles.

2. **`tableloader/tableFunctions/<module>.py`** — add a loader function. Use `Table('tableName', metadata)` to bind the existing table object, open a transaction, iterate `_jsonl(...)`, insert.

3. **`tableloader/tableFunctions/__init__.py`** — add the module name to `__all__` if it's a new file.

4. **`Load.py`** — add a new entry to `LOADERS` (or extend an existing one):
   - `module_name`: short slug
   - `files`: lowercase JSONL basenames that trigger update-mode reload
   - `tables`: all DB table names this loader writes to (for drop/recreate)
   - `calls`: list of zero-arg lambdas closing over `connection, metadata, sourcePath, language`
   - `trnTranslations_tcIDs`: if the loader writes to `trnTranslations`, list the tcIDs it uses
   - `depends_on_modules`: for derived loaders (no JSONL files), list upstream module names

## trnTranslations — shared table

Multiple modules write to `trnTranslations` using different `tcID` values. In update mode the table is **never dropped**; instead, rows for the reloading module's tcIDs are deleted before the module runs. Each tcID is owned by exactly one module:

| tcIDs | Module |
|-------|--------|
| 6, 7, 8, 14, 15, 33 | types |
| 11, 12, 16, 19 | character |
| 17 | certificates |
| 20, 24 | npccorporations |

When adding a loader that writes to `trnTranslations`, confirm the tcID isn't already used by another module and add it to `trnTranslations_tcIDs` in the LOADERS entry.

## Derived loaders

Three loaders build their tables from data already in the DB rather than from JSONL files:

- **shipskills** — calls `shipskills.buildSkills(connection, database)`; depends on `types` and `dogma`
- **buildjumps** — calls `map.buildJumps(connection, database)`; depends on `map`
- **invnames** — calls `invNames.build_inv_names(connection, database)`; depends on `map`, `types`, `npccorporations`, `npccharacters`

These use raw SQL or SQLAlchemy `select/insert` against already-populated tables. They must stay after their upstream loaders in the `LOADERS` list.

## No foreign keys

`tables.py` declares no `ForeignKey()` columns — all cross-table references are plain `INTEGER`. This means `metadata.drop_all(engine, tables=[specific_tables])` is safe to call with any subset of tables; there is no FK cascade graph to worry about.

## Database targets

| Target | Driver | Notes |
|--------|--------|-------|
| `sqlite` | pysqlite | File path from `sdeloader.cfg` |
| `mysql` | pymysql | charset=utf8 required |
| `postgres` | psycopg2 | no schema prefix |
| `postgresschema` | psycopg2 | all tables in `evesde` schema |
| `mssql` | pymssql | run inside Docker container in pipeline |

The `postgresschema` target sets `schema="evesde"` in `metadataCreator()`, which prefixes every table reference automatically.

## Secrets and config

- `sdeloader.cfg` — DB URLs and source path. Git-ignored. Copy from `sdeloader.cfg-example`.
- `run-conversion.cfg` — all credentials for the pipeline script. Git-ignored. Copy from `run-conversion.cfg-example`.
- Never add credentials to any tracked file.
