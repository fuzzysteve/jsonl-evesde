# jsonl-evesde

Loads the EVE Online Static Data Export (SDE) from CCP's JSONL format into relational databases. Supports SQLite, MySQL, PostgreSQL, and MSSQL. Used to produce the [Fuzzwork SDE conversions](https://www.fuzzwork.co.uk/dump/).

## Prerequisites

- [uv](https://docs.astral.sh/uv/) — it installs the pinned Python (3.14, see `.python-version`) if it isn't already present
- The SDE JSONL files extracted to a local directory (default `/opt/sde/files/`)
- Database servers as needed (MySQL, PostgreSQL, MSSQL)
- `flock`, `curl`, `git` for the automated pipeline script
- `pg_config` (e.g. `libpq-dev`) if `psycopg2` has to build from source

Install Python dependencies into `.venv/` (versions pinned in `uv.lock`):

```bash
uv sync --frozen
```

To change dependencies, use `uv add <pkg>` / `uv remove <pkg>`, or bump pins with `uv lock --upgrade` then `uv sync`. Commit `pyproject.toml` and `uv.lock` together.

## Configuration

Two config files are required:

### `sdeloader.cfg`

Database connection strings and the path to the SDE source files. Copy from the example and edit:

```bash
cp sdeloader.cfg-example sdeloader.cfg
```

```ini
[Database]
sqlite=sqlite+pysqlite:///eve.db
mysql=mysql+pymysql://user:pass@localhost/sdeyaml?charset=utf8
postgres=postgresql+psycopg2://user:pass@localhost/sdeyaml
postgresschema=postgresql+psycopg2://user:pass@localhost/sdeyamlschema
mssql=mssql+pymssql://user:pass@localhost/evesde?charset=utf8

[Files]
sourcePath=/opt/sde/files/
```

### `run-conversion.cfg`

Credentials and paths used by `run-conversion.sh`. Copy from the example and fill in:

```bash
cp run-conversion.cfg-example run-conversion.cfg
```

This file is git-ignored — never commit it.

## Manual usage

Load a single database target:

```bash
.venv/bin/python Load.py sqlite
.venv/bin/python Load.py mysql
.venv/bin/python Load.py postgres
.venv/bin/python Load.py postgresschema
.venv/bin/python Load.py mssql
```

An optional second argument sets the language for localised strings (default `en`):

```bash
.venv/bin/python Load.py sqlite de
```

By default this performs a **full load**: drops all tables, recreates them, and loads everything from scratch.

### Update mode

If the environment variable `SDE_CHANGED_FILES` is set to a newline-separated list of changed JSONL basenames, `Load.py` runs in **update mode**: only the modules whose source files appear in that list are dropped, recreated, and reloaded. Derived tables (ship skills, map jumps, inv names) are also refreshed if any of their upstream modules ran.

```bash
SDE_CHANGED_FILES=$'types.jsonl\ngroups.jsonl' .venv/bin/python Load.py sqlite
```

This variable is set automatically by `run-conversion.sh`; you rarely need to set it by hand.

## Automated pipeline

`run-conversion.sh` orchestrates the full end-to-end process:

1. Checks the SDE REST API (`sde.evetech.net`) for a new release: any table whose API `version` differs from the last fetch. CCP can publish several releases under one build number, so the build number alone isn't the trigger. A release that was fetched but never finished processing is retried.
2. Downloads the new or changed tables' JSONL from that pinned release (`fetch_sde.py`)
3. Commits the files to a local git repo inside the SDE directory (history only)
4. Exports `SDE_CHANGED_FILES`: the tables whose content changed since the last processed release, from `fetch_sde.py`'s manifest. A re-release of the same build that changes no loaded data is marked done here without being published.
5. Runs `Load.py` for each database target
6. Exports compressed dumps and places them under `WEB_ROOT`
7. Updates `latest-*` symlinks and generates md5sums

Run it manually:

```bash
./run-conversion.sh             # skips if no table version has changed
./run-conversion.sh --force     # runs even if build number hasn't changed
./run-conversion.sh --reprocess # skips download; reloads from the last committed SDE
```

`--reprocess` is useful after adding new loaders or fixing a bug — it re-runs the full load pipeline against the SDE files already on disk, using the changed tables recorded in the SDE manifest by the last fetch (or a full load if none are recorded).

The script uses `flock` to prevent concurrent runs. It expects `run-conversion.cfg` to exist.

### Cron setup

```
0 */4 * * * /home/scripts/jsonl-evesde/run-conversion.sh >> /var/log/sde-conversion.log 2>&1
```

## Tests

```bash
uv sync --frozen          # includes the dev group (pytest)
.venv/bin/python -m pytest
```

`tests/test_fetch_sde.py` runs `fetch_sde.py` against a local fake of the SDE API (`tests/fakesde.py`). `tests/test_run_conversion.py` runs the real `run-conversion.sh` from a temp copy with a generated config, the fake API, and stubs for everything with side effects (`Load.py`, the helper scripts, `mysql`/`mysqldump`/`pg_dump`/`docker`/`curl`), checking which loads, publishes and Discord posts each release scenario produces. No database, web root or webhook is touched.

## SDE source git repo

`run-conversion.sh` maintains a git repository inside `SDE_DIR` (default `/opt/sde/files/`). Each SDE release becomes one commit, giving a browsable history of the source files. Change detection doesn't use it: `fetch_sde.py` keeps a manifest, `.sde-versions.json`, of each table's API `version` (plus the SHA-256 of the local file), downloads only tables whose version changed, and records which tables' content changed (by SHA-256; a version bump with identical bytes doesn't count) for `SDE_CHANGED_FILES`. A release is marked done in the manifest only after a successful run; until then its changed set carries over into the next fetch. The first fetch has no previous manifest to compare with, so it is always a full load.

Initialise it once before the first run:

```bash
cd /opt/sde/files
git init
git add -A
git commit -m "initial"
```

## Project structure

```
Load.py                        # entry point; full or update mode dispatch
sdeloader.cfg                  # DB connection strings and source path (git-ignored)
sdeloader.cfg-example          # template
run-conversion.sh              # automated download → load → publish pipeline
run-conversion.cfg             # pipeline credentials/paths (git-ignored)
run-conversion.cfg-example     # template
export_csv.py                  # exports tables to CSV (called by run-conversion.sh)
cleanup_builds.py              # removes old web-root build directories
tableloader/
  tables.py                    # SQLAlchemy Table definitions (all schemas)
  tableFunctions/
    __init__.py                # __all__ listing of all loader modules
    types.py                   # invTypes, invGroups, invCategories, invMetaGroups, invMarketGroups
    map.py                     # mapSolarSystems, mapDenormalize, staStations, mapJumps, ...
    blueprints.py              # industryBlueprints, industryActivities, ...
    dogma.py                   # dgmAttributeTypes, dgmEffects, dgmUnits, ...
    graphics.py                # graphicMaterialSets, eveGraphics, eveIcons
    skinr.py                   # skinrComponents, skinrSlots, skinrSlotConfigurations, ...
    character.py               # chrAncestries, chrBloodlines, chrRaces, chrFactions, ...
    certificates.py            # certCerts, certSkills, certMasteries
    npccorporations.py         # crpNPCCorporations, crpActivities, crpNPCDivisions, ...
    npccharacters.py           # agtAgents, npcCharacters
    agents.py                  # agtAgentTypes, agtAgentsInSpace
    industry.py                # ramActivities, ramAssemblyLines, indModifierSources,
                               #   indTargetFilters, staOperations, staStandingsRestrictions, ...
    corporations.py            # crpRoleGroups, crpRoles, crpRoleRoleGroups
    accounting.py              # acctEntryTypes
    fighters.py                # fighterAbilities, fighterAbilitiesByType
    schools.py                 # chrSchools, chrSchoolMap, skillPlans, expertSystems, ...
    typeeffects.py             # aplProximityEffects, proximityTraps, linkWithShip,
                               #   sysDbuffEmitters, sysWideEffects, metenoxMoonDrills
    notifications.py           # ntfTypes
    skins.py                   # skinMaterials, skins, skinLicenses
    shiptree.py                # shipTreeElements, shipTreeGroups, shipTreeFactions
    military.py                # militaryCampaigns, militaryCampaignObjectives, ...
    missions.py                # agtMissions, dungeons, epicArcs, ...
    planetary.py               # planetResources, planetSchematics
    shipskills.py              # (derived) dspShipSkills — built from types + dogma
    invNames.py                # (derived) invNames, invUniqueNames — built from mapDenormalize
    ...                        # plus cloneGrades, characterTitles, translationlanguages,
                               #   contraband, controltower, compressible, typelist, freelance
```

## License

MIT — see `LICENSE`.
