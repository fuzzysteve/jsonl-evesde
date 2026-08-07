#!/bin/bash
set -e
cd /home/scripts/jsonl-evesde

# ---------------------------------------------------------------------------
# Locate script and load config
# ---------------------------------------------------------------------------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BUILD_FILE="$SCRIPT_DIR/.last_build"

CONFIG_FILE="$SCRIPT_DIR/run-conversion.cfg"
if [ ! -f "$CONFIG_FILE" ]; then
    echo "ERROR: $CONFIG_FILE not found. Copy run-conversion.cfg-example and fill in values."
    exit 1
fi
# shellcheck source=/dev/null
source "$CONFIG_FILE"

# ---------------------------------------------------------------------------
# Usage
# ---------------------------------------------------------------------------
FORCE=0
if [[ "${1:-}" == "--force" ]]; then
    FORCE=1
fi

log() { echo "[$(date '+%H:%M:%S')] $*"; }

# ---------------------------------------------------------------------------
# Single-instance lock
# ---------------------------------------------------------------------------
LOCKFILE="$SCRIPT_DIR/.run-conversion.lock"
exec 200>"$LOCKFILE"
if ! flock -n 200; then
    log "Another instance is already running ($LOCKFILE). Exiting."
    exit 1
fi

# ---------------------------------------------------------------------------
# Activate virtualenv
# ---------------------------------------------------------------------------
if [ ! -f "$VENV_DIR/bin/activate" ]; then
    log "ERROR: virtualenv not found at $VENV_DIR"
    exit 1
fi
# shellcheck disable=SC1091
source "$VENV_DIR/bin/activate"

mkdir -p "$OUTPUT_DIR" "$SDE_DIR"

# ---------------------------------------------------------------------------
# Check for new SDE version
# ---------------------------------------------------------------------------
log "Checking latest SDE build number..."
LATEST_JSON=$(curl -sf "$LATEST_URL")
LATEST_BUILD=$(echo "$LATEST_JSON" | python3 -c "
import json, sys
for line in sys.stdin:
    r = json.loads(line)
    if r.get('_key') == 'sde':
        print(r['buildNumber'])
        break
")

if [ -z "$LATEST_BUILD" ]; then
    log "ERROR: Could not determine latest build number"
    exit 1
fi

log "Latest build: $LATEST_BUILD"

LAST_BUILD=""
if [ -f "$BUILD_FILE" ]; then
    LAST_BUILD=$(cat "$BUILD_FILE")
fi

if [ "$LATEST_BUILD" = "$LAST_BUILD" ] && [ "$FORCE" -eq 0 ]; then
    log "Already on build $LATEST_BUILD — nothing to do. Use --force to run anyway."
    exit 0
fi

if [ "$FORCE" -eq 1 ] && [ "$LATEST_BUILD" = "$LAST_BUILD" ]; then
    log "Build $LATEST_BUILD already processed but --force specified, continuing."
else
    log "New build available: $LATEST_BUILD (was: ${LAST_BUILD:-none})"
fi

TIMESTAMP=$(date +%Y%m%d_%H%M%S)

# ---------------------------------------------------------------------------
# Download and extract new SDE
# ---------------------------------------------------------------------------
log "Downloading SDE build $LATEST_BUILD..."
ZIPFILE="/tmp/evesde_${LATEST_BUILD}.zip"
curl -f -L -o "$ZIPFILE" "$DOWNLOAD_URL"
log "Extracting to $SDE_DIR..."
rm -rf "${SDE_DIR:?}"/*
unzip -q "$ZIPFILE" -d "$SDE_DIR"
rm -f "$ZIPFILE"
log "SDE extracted"

# ---------------------------------------------------------------------------
# Commit new SDE files to local git repo; detect what changed
# ---------------------------------------------------------------------------
cd "$SDE_DIR"
git add -A
if git diff --cached --quiet; then
    log "No new SDE file changes to commit"
    PREV_COMMIT=""
else
    git commit -m "$LATEST_BUILD"
    PREV_COMMIT=$(git rev-parse HEAD~1 2>/dev/null || echo "")
fi

if [ -n "$PREV_COMMIT" ]; then
    SDE_CHANGED_FILES=$(git diff --name-only "$PREV_COMMIT" HEAD -- '*.jsonl' \
                        | while IFS= read -r f; do basename "$f"; done)
    if [ -n "$SDE_CHANGED_FILES" ]; then
        export SDE_CHANGED_FILES
        log "Changed JSONL files: $SDE_CHANGED_FILES"
    else
        unset SDE_CHANGED_FILES
        log "No JSONL files changed — full load"
    fi
else
    unset SDE_CHANGED_FILES
    log "Full load (no previous commit to diff against)"
fi
cd "$SCRIPT_DIR"

# ---------------------------------------------------------------------------
# MSSQL helpers — all SQL run as SA inside the container
# ---------------------------------------------------------------------------
mssql_exec() {
    docker exec "$MSSQL_CONTAINER" \
        /opt/mssql-tools18/bin/sqlcmd -No \
        -S localhost -U SA -P "$MSSQL_SA_PASS" -C \
        -Q "$1" > /dev/null
}

start_mssql() {
    log "=== Starting MSSQL container ==="
    if docker ps --format '{{.Names}}' | grep -q "^${MSSQL_CONTAINER}$"; then
        log "Container $MSSQL_CONTAINER already running"
    elif docker ps -a --format '{{.Names}}' | grep -q "^${MSSQL_CONTAINER}$"; then
        log "Starting existing container $MSSQL_CONTAINER"
        docker start "$MSSQL_CONTAINER"
    else
        log "ERROR: Container $MSSQL_CONTAINER does not exist. Please create it first."
        exit 1
    fi

    log "Waiting for MSSQL to be ready..."
    for i in $(seq 1 60); do
        if docker exec "$MSSQL_CONTAINER" \
            /opt/mssql-tools18/bin/sqlcmd \
            -S localhost -U SA -P "$MSSQL_SA_PASS" -C \
            -Q "SELECT 1" > /dev/null 2>&1; then
            log "MSSQL ready after ${i}s"
            return 0
        fi
        if [ "$i" -eq 60 ]; then
            log "ERROR: MSSQL did not become ready in time"
            exit 1
        fi
        sleep 1
    done
}

# ---------------------------------------------------------------------------
# SQLite
# ---------------------------------------------------------------------------
log "=== Loading SQLite ==="
if [ -z "${SDE_CHANGED_FILES+x}" ]; then
    rm -f "$SQLITE_FILE"
fi
python3 "$SCRIPT_DIR/Load.py" sqlite

log "Compressing SQLite database"
gzip -k -f "$SQLITE_FILE"
mv "${SQLITE_FILE}.gz" "$OUTPUT_DIR/eve_${LATEST_BUILD}_${TIMESTAMP}.db.gz"
log "SQLite done -> $OUTPUT_DIR/eve_${LATEST_BUILD}_${TIMESTAMP}.db.gz"

# ---------------------------------------------------------------------------
# MySQL
# ---------------------------------------------------------------------------
log "=== Loading MySQL ==="
if [ -z "${SDE_CHANGED_FILES+x}" ]; then
    mysql -h "$MYSQL_HOST" -u "$MYSQL_USER" -p"$MYSQL_PASS" \
        -e "DROP DATABASE IF EXISTS \`${MYSQL_DB}\`; CREATE DATABASE \`${MYSQL_DB}\` CHARACTER SET utf8mb4;"
fi
python3 "$SCRIPT_DIR/Load.py" mysql

log "Exporting MySQL backup"
mysqldump \
    -h "$MYSQL_HOST" \
    -u "$MYSQL_USER" \
    -p"$MYSQL_PASS" \
    --single-transaction \
    "$MYSQL_DB" | gzip > "$OUTPUT_DIR/sdeyaml_mysql_${LATEST_BUILD}_${TIMESTAMP}.sql.gz"
log "MySQL done -> $OUTPUT_DIR/sdeyaml_mysql_${LATEST_BUILD}_${TIMESTAMP}.sql.gz"

# ---------------------------------------------------------------------------
# PostgreSQL
# ---------------------------------------------------------------------------
log "=== Loading PostgreSQL ==="
python3 "$SCRIPT_DIR/Load.py" postgres

log "Exporting PostgreSQL backup"
PGPASSWORD="$PG_PASS" pg_dump \
    -h "$PG_HOST" -U "$PG_USER" -d "$PG_DB" \
    -F c \
    -f "$OUTPUT_DIR/sdeyaml_postgres_${LATEST_BUILD}_${TIMESTAMP}.pgdump"
log "PostgreSQL done -> $OUTPUT_DIR/sdeyaml_postgres_${LATEST_BUILD}_${TIMESTAMP}.pgdump"

# ---------------------------------------------------------------------------
# PostgreSQL with schema
# ---------------------------------------------------------------------------
log "=== Loading PostgreSQL (schema) ==="
python3 "$SCRIPT_DIR/Load.py" postgresschema

log "Exporting PostgreSQL schema backup"
PGPASSWORD="$PG_PASS" pg_dump \
    -h "$PG_HOST" -U "$PG_USER" -d "$PG_SCHEMA_DB" \
    -n evesde \
    -F c \
    -f "$OUTPUT_DIR/sdeyaml_postgresschema_${LATEST_BUILD}_${TIMESTAMP}.pgdump"
log "PostgreSQL schema done -> $OUTPUT_DIR/sdeyaml_postgresschema_${LATEST_BUILD}_${TIMESTAMP}.pgdump"

# ---------------------------------------------------------------------------
# MSSQL
# ---------------------------------------------------------------------------
start_mssql

log "=== Loading MSSQL ==="
python3 "$SCRIPT_DIR/Load.py" mssql

log "Exporting MSSQL backup"
MSSQL_BACKUP_FILE="$OUTPUT_DIR/sdeyaml_mssql_${LATEST_BUILD}_${TIMESTAMP}.bak"
mssql_exec "
BACKUP DATABASE [${MSSQL_DB}]
TO DISK = N'/var/opt/mssql/sde_backup.bak'
WITH FORMAT, COMPRESSION, STATS = 10
"
docker cp "${MSSQL_CONTAINER}:/var/opt/mssql/sde_backup.bak" "$MSSQL_BACKUP_FILE"
log "MSSQL done -> $MSSQL_BACKUP_FILE"

log "Stopping MSSQL container"
docker stop "$MSSQL_CONTAINER"

# ---------------------------------------------------------------------------
# Publish: move files, create symlinks, generate checksums, sync MySQL
# ---------------------------------------------------------------------------
log "=== Publishing (build $LATEST_BUILD) ==="
ls -lh "$OUTPUT_DIR"/*_${LATEST_BUILD}_${TIMESTAMP}*

mkdir -p "$WEB_ROOT/${LATEST_BUILD}_${TIMESTAMP}/"
mv "$OUTPUT_DIR"/*_${LATEST_BUILD}_${TIMESTAMP}* "$WEB_ROOT/${LATEST_BUILD}_${TIMESTAMP}/"

rm -f "$WEB_ROOT/latest-mysql.sql.gz"
rm -f "$WEB_ROOT/latest-mssql.bak"
rm -f "$WEB_ROOT/latest"
rm -f "$WEB_ROOT/latest-sqlite.db.gz"
rm -f "$WEB_ROOT/latest-postgres.pgdump"
rm -f "$WEB_ROOT/latest-postgresschema.pgdump"

ln -s "$WEB_ROOT/${LATEST_BUILD}_${TIMESTAMP}" "$WEB_ROOT/latest"
ln -s "$WEB_ROOT/${LATEST_BUILD}_${TIMESTAMP}/sdeyaml_mysql_${LATEST_BUILD}_${TIMESTAMP}.sql.gz"     "$WEB_ROOT/latest-mysql.sql.gz"
ln -s "$WEB_ROOT/${LATEST_BUILD}_${TIMESTAMP}/sdeyaml_mssql_${LATEST_BUILD}_${TIMESTAMP}.bak"        "$WEB_ROOT/latest-mssql.bak"
ln -s "$WEB_ROOT/${LATEST_BUILD}_${TIMESTAMP}/sdeyaml_postgres_${LATEST_BUILD}_${TIMESTAMP}.pgdump"  "$WEB_ROOT/latest-postgres.pgdump"
ln -s "$WEB_ROOT/${LATEST_BUILD}_${TIMESTAMP}/sdeyaml_postgresschema_${LATEST_BUILD}_${TIMESTAMP}.pgdump" "$WEB_ROOT/latest-postgresschema.pgdump"
ln -s "$WEB_ROOT/${LATEST_BUILD}_${TIMESTAMP}/eve_${LATEST_BUILD}_${TIMESTAMP}.db.gz"                "$WEB_ROOT/latest-sqlite.db.gz"

chmod a+r "$WEB_ROOT/${LATEST_BUILD}_${TIMESTAMP}/sdeyaml_mssql_${LATEST_BUILD}_${TIMESTAMP}.bak"

python3 export_csv.py "$WEB_ROOT/${LATEST_BUILD}_${TIMESTAMP}/csv/"

md5sum "$WEB_ROOT/latest-mysql.sql.gz"           > "$WEB_ROOT/latest-mysql.sql.gz.md5sum"
md5sum "$WEB_ROOT/latest-mssql.bak"              > "$WEB_ROOT/latest-mssql.bak.md5sum"
md5sum "$WEB_ROOT/latest-sqlite.db.gz"           > "$WEB_ROOT/latest-sqlite.db.gz.md5sum"
md5sum "$WEB_ROOT/latest-postgres.pgdump"        > "$WEB_ROOT/latest-postgres.pgdump.md5sum"
md5sum "$WEB_ROOT/latest-postgresschema.pgdump"  > "$WEB_ROOT/latest-postgresschema.pgdump.md5sum"

mysqldump sdeyaml|mysql eve
# Record success only after all work completes; any earlier failure under
# set -e aborts here, leaving BUILD_FILE unchanged so the next run retries.
echo "$LATEST_BUILD" > "$BUILD_FILE"
log "=== All done (build $LATEST_BUILD) ==="

if [ -n "${DISCORD_WEBHOOK_URL:-}" ]; then
    PAYLOAD=$(envsubst '$LATEST_BUILD $WEB_ROOT_URL' < "$SCRIPT_DIR/discord-notification.json")
    curl -sf -X POST "$DISCORD_WEBHOOK_URL" \
        -H "Content-Type: application/json" \
        -d "$PAYLOAD"
fi
