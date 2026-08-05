# jsonl-evesde

Loads the EVE Online Static Data Export (SDE) from CCP's JSONL format into relational databases. Supports SQLite, MySQL, PostgreSQL, and MSSQL. Used to produce the [Fuzzwork SDE conversions](https://www.fuzzwork.co.uk/dump/).

## Prerequisites

- Python 3.10+ with a virtualenv at `.venv/`
- The SDE JSONL files extracted to a local directory (default `/opt/sde/files/`)
- Database servers as needed (MySQL, PostgreSQL, MSSQL)
- `flock`, `unzip`, `curl`, `git` for the automated pipeline script

Install Python dependencies:

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
```

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

1. Checks the CCP API for a new SDE build number
2. Downloads and extracts the new JSONL zip
3. Commits the files to a local git repo inside the SDE directory
4. Computes which JSONL files changed (via `git diff`) and exports `SDE_CHANGED_FILES`
5. Runs `Load.py` for each database target
6. Exports compressed dumps and places them under `WEB_ROOT`
7. Updates `latest-*` symlinks and generates md5sums

Run it manually:

```bash
./run-conversion.sh           # skips if already on latest build
./run-conversion.sh --force   # runs even if build number hasn't changed
```

The script uses `flock` to prevent concurrent runs. It expects `run-conversion.cfg` to exist.

### Cron setup

```
0 */4 * * * /home/scripts/jsonl-evesde/run-conversion.sh >> /var/log/sde-conversion.log 2>&1
```

## SDE source git repo

`run-conversion.sh` maintains a git repository inside `SDE_DIR` (default `/opt/sde/files/`). Each SDE release becomes one commit, which enables `git diff` to identify changed files and trigger update-mode loads rather than full reloads.

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
tableloader/
  tables.py                    # SQLAlchemy Table definitions (all schemas)
  tableFunctions/
    __init__.py                # __all__ listing of all loader modules
    types.py                   # invTypes, invGroups, invCategories, ...
    map.py                     # mapSolarSystems, mapDenormalize, staStations, ...
    blueprints.py              # industryBlueprints, industryActivity, ...
    dogma.py                   # dgmAttributeTypes, dgmEffects, eveUnits, ...
    graphics.py                # graphicMaterialSets, eveGraphics, eveIcons
    skinr.py                   # skinrComponents, skinrSlots, skinrSlotConfigurations, ...
    ...                        # one file per domain
```

## License

MIT — see `LICENSE`.
