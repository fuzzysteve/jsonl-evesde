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
REPROCESS=0
PREVIEW_URL=""
PREVIEW_NOTIFY=0
DISCORD_COMMENT=""
while [[ $# -gt 0 ]]; do
    case "$1" in
        --force)      FORCE=1; shift ;;
        --reprocess)  REPROCESS=1; shift ;;
        # --preview <release-url>: convert a non-TQ SDE API release (e.g.
        # https://sde.evetech.net/<tenant>/latest/) without
        # touching the SDE git history, .last_build, latest-* links, the eve DB,
        # or build retention. Output goes to $WEB_ROOT/preview/.
        --preview)    PREVIEW_URL="$2"; shift 2 ;;
        --notify)     PREVIEW_NOTIFY=1; shift ;;
        --comment)    DISCORD_COMMENT="$2"; shift 2 ;;
        *)            shift ;;
    esac
done

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

SDE_API_URL="${SDE_API_URL:-https://sde.evetech.net/tranquility/latest/}"

TIMESTAMP=$(date +%Y%m%d_%H%M%S)

# Set when a preview run has loaded non-TQ data into the staging DBs; the next
# normal run must do a full load rather than an incremental update on top of it.
PREVIEW_DIRTY_FILE="$SCRIPT_DIR/.preview_dirty"
PREVIEW_SDE_DIR="${PREVIEW_SDE_DIR:-${SDE_DIR%/}-preview}"

if [ -n "$PREVIEW_URL" ]; then
    # ---------------------------------------------------------------------------
    # Preview mode: separate extract dir, no git commit, always a full load
    # ---------------------------------------------------------------------------
    log "Preview mode: fetching $PREVIEW_URL"
    if ! LATEST_BUILD=$(SDE_API_URL="$PREVIEW_URL" python3 "$SCRIPT_DIR/fetch_sde.py" fetch "$PREVIEW_SDE_DIR"); then
        log "ERROR: Preview fetch from $PREVIEW_URL failed"
        exit 1
    fi
    log "Preview build: $LATEST_BUILD (extracted to $PREVIEW_SDE_DIR)"
    export SDE_SOURCE_PATH="$PREVIEW_SDE_DIR/"
    unset SDE_CHANGED_FILES
    touch "$PREVIEW_DIRTY_FILE"
elif [ "$REPROCESS" -eq 1 ]; then
    # ---------------------------------------------------------------------------
    # Reprocess mode: skip download; use existing SDE files from last run
    # ---------------------------------------------------------------------------
    if [ ! -f "$BUILD_FILE" ]; then
        log "ERROR: No previous build recorded in $BUILD_FILE; cannot reprocess."
        exit 1
    fi
    LATEST_BUILD=$(cat "$BUILD_FILE")
    log "Reprocessing build $LATEST_BUILD from existing SDE at $SDE_DIR"
    cd "$SDE_DIR"
    PREV_COMMIT=$(git rev-parse HEAD~1 2>/dev/null || echo "")
    if [ -n "$PREV_COMMIT" ]; then
        SDE_CHANGED_FILES=$(git diff --name-only "$PREV_COMMIT" HEAD -- '*.jsonl' \
                            | while IFS= read -r f; do basename "$f"; done)
        if [ -n "$SDE_CHANGED_FILES" ]; then
            export SDE_CHANGED_FILES
            log "Changed JSONL files: $SDE_CHANGED_FILES"
        else
            unset SDE_CHANGED_FILES
            log "No JSONL files changed in last commit — full load"
        fi
    else
        unset SDE_CHANGED_FILES
        log "No previous commit — full load"
    fi
    cd "$SCRIPT_DIR"
else
    # ---------------------------------------------------------------------------
    # Check for new SDE version
    # ---------------------------------------------------------------------------
    log "Checking latest SDE build number..."
    LATEST_BUILD=$(SDE_API_URL="$SDE_API_URL" python3 "$SCRIPT_DIR/fetch_sde.py" build || true)

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

    # ---------------------------------------------------------------------------
    # Download and extract new SDE
    # ---------------------------------------------------------------------------
    log "Downloading SDE build $LATEST_BUILD from $SDE_API_URL..."
    # fetch_sde.py downloads every table from one pinned release and prints its
    # build number; a release published since the check above wins.
    FETCHED_BUILD=$(SDE_API_URL="$SDE_API_URL" python3 "$SCRIPT_DIR/fetch_sde.py" fetch "$SDE_DIR")
    if [ "$FETCHED_BUILD" != "$LATEST_BUILD" ]; then
        log "Build changed during check: fetched $FETCHED_BUILD (checked $LATEST_BUILD)"
        LATEST_BUILD="$FETCHED_BUILD"
    fi
    log "SDE downloaded to $SDE_DIR"

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
fi

if [ -z "$PREVIEW_URL" ] && [ -f "$PREVIEW_DIRTY_FILE" ]; then
    log "Staging DBs hold preview data — forcing full load"
    unset SDE_CHANGED_FILES
fi

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
# Preview publish: own directory tree, no latest-* links, no eve DB sync,
# no .last_build, no retention cleanup. Exits here.
# ---------------------------------------------------------------------------
if [ -n "$PREVIEW_URL" ]; then
    PREVIEW_ROOT="$WEB_ROOT/preview"
    PREVIEW_OUT="$PREVIEW_ROOT/${LATEST_BUILD}_${TIMESTAMP}"
    log "=== Publishing preview (build $LATEST_BUILD) -> $PREVIEW_OUT ==="
    mkdir -p "$PREVIEW_OUT"
    mv "$OUTPUT_DIR"/*_${LATEST_BUILD}_${TIMESTAMP}* "$PREVIEW_OUT/"
    chmod a+r "$PREVIEW_OUT"/*

    python3 export_csv.py "$PREVIEW_OUT/csv/"

    log "Exporting per-table MySQL dumps"
    mkdir -p "$PREVIEW_OUT/mysql_tables"
    mysql -h "$MYSQL_HOST" -u "$MYSQL_USER" -p"$MYSQL_PASS" -N -e "SHOW TABLES FROM \`${MYSQL_DB}\`" \
        | while IFS= read -r table; do
            mysqldump -h "$MYSQL_HOST" -u "$MYSQL_USER" -p"$MYSQL_PASS" \
                --single-transaction "$MYSQL_DB" "$table" | gzip > "$PREVIEW_OUT/mysql_tables/${table}.sql.gz"
        done

    (cd "$PREVIEW_OUT" && md5sum -- *.gz *.pgdump *.bak > md5sums.txt)
    rm -f "$PREVIEW_ROOT/latest"
    ln -s "$PREVIEW_OUT" "$PREVIEW_ROOT/latest"
    log "=== Preview done (build $LATEST_BUILD) ==="

    if [ "$PREVIEW_NOTIFY" -eq 1 ] && [ -n "${DISCORD_WEBHOOK_URL:-}" ]; then
        PAYLOAD=$(python3 -I -c "
import json, sys
build, url, comment = sys.argv[1:4]
p = {'embeds': [{'title': f'Preview SDE build {build} published',
                 'color': 15105570,
                 'description': f'Non-Tranquility preview conversion. [Download directory]({url})',
                 'fields': []}]}
if comment:
    p['embeds'][0]['fields'].append({'name': 'Note', 'value': comment})
print(json.dumps(p))
" "$LATEST_BUILD" "$WEB_ROOT_URL/preview/${LATEST_BUILD}_${TIMESTAMP}/" "$DISCORD_COMMENT")
        _DISCORD_STATUS=$(curl -s -o /dev/null -w "%{http_code}" -X POST "$DISCORD_WEBHOOK_URL" \
            -H "Content-Type: application/json" -d "$PAYLOAD")
        [ "$_DISCORD_STATUS" = "204" ] || log "Discord webhook failed: HTTP $_DISCORD_STATUS"
    fi
    exit 0
fi

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

log "Exporting per-table MySQL dumps"
MYSQL_TABLES_DIR="$WEB_ROOT/${LATEST_BUILD}_${TIMESTAMP}/mysql_tables"
mkdir -p "$MYSQL_TABLES_DIR"
mysql -h "$MYSQL_HOST" -u "$MYSQL_USER" -p"$MYSQL_PASS" -N -e "SHOW TABLES FROM \`${MYSQL_DB}\`" \
    | while IFS= read -r table; do
        mysqldump -h "$MYSQL_HOST" -u "$MYSQL_USER" -p"$MYSQL_PASS" \
            --single-transaction "$MYSQL_DB" "$table" | gzip > "$MYSQL_TABLES_DIR/${table}.sql.gz"
    done
log "Per-table MySQL dumps -> $MYSQL_TABLES_DIR"

md5sum "$WEB_ROOT/latest-mysql.sql.gz"           > "$WEB_ROOT/latest-mysql.sql.gz.md5sum"
md5sum "$WEB_ROOT/latest-mssql.bak"              > "$WEB_ROOT/latest-mssql.bak.md5sum"
md5sum "$WEB_ROOT/latest-sqlite.db.gz"           > "$WEB_ROOT/latest-sqlite.db.gz.md5sum"
md5sum "$WEB_ROOT/latest-postgres.pgdump"        > "$WEB_ROOT/latest-postgres.pgdump.md5sum"
md5sum "$WEB_ROOT/latest-postgresschema.pgdump"  > "$WEB_ROOT/latest-postgresschema.pgdump.md5sum"

mysqldump sdeyaml|mysql eve
# Record success only after all work completes; any earlier failure under
# set -e aborts here, leaving BUILD_FILE unchanged so the next run retries.
echo "$LATEST_BUILD" > "$BUILD_FILE"
rm -f "$PREVIEW_DIRTY_FILE"
log "=== All done (build $LATEST_BUILD) ==="

log "=== Cleaning up old builds ==="
python3 "$SCRIPT_DIR/cleanup_builds.py"

if [ -n "${DISCORD_WEBHOOK_URL:-}" ]; then
    GIT_STATS=""
    # Pass empty last_build when force-running on the same build, so sde_changes.py
    # reports just this build's own changes rather than walking 20 builds back.
    _SDE_LAST="${LAST_BUILD:-}"
    [ "$_SDE_LAST" = "$LATEST_BUILD" ] && _SDE_LAST=""
    if GIT_STATS=$(python3 "$SCRIPT_DIR/sde_changes.py" "$LATEST_BUILD" "$_SDE_LAST" 2>/dev/null); then
        : # fetched successfully
    else
        GIT_STATS="Build $LATEST_BUILD"
    fi
    PAYLOAD=$(LATEST_BUILD="$LATEST_BUILD" WEB_ROOT_URL="$WEB_ROOT_URL" GIT_STATS="$GIT_STATS" \
              envsubst '$LATEST_BUILD $WEB_ROOT_URL $GIT_STATS' < "$SCRIPT_DIR/discord-notification.json")
    if [ -n "${DISCORD_COMMENT:-}" ]; then
        PAYLOAD=$(python3 -c "
import json, sys
p = json.loads(sys.stdin.read())
p['embeds'][0]['fields'].append({'name': 'Note', 'value': sys.argv[1]})
print(json.dumps(p))
" "$DISCORD_COMMENT" <<< "$PAYLOAD")
    fi
    _DISCORD_STATUS=$(curl -s -o /tmp/discord_response.tmp -w "%{http_code}" \
        -X POST "$DISCORD_WEBHOOK_URL" \
        -H "Content-Type: application/json" \
        -d "$PAYLOAD")
    if [ "$_DISCORD_STATUS" != "204" ]; then
        log "Discord webhook failed: HTTP $_DISCORD_STATUS"
        log "  Response: $(cat /tmp/discord_response.tmp)"
    fi
    rm -f /tmp/discord_response.tmp
fi
