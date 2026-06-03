#!/bin/bash
# Dump the Cinderhaven Data Platform's Postgres database from Fly.io
# to a local SQL file for use with docker-compose.
#
# Prerequisites:
#   - flyctl installed and authenticated
#   - pg_dump available locally (via Postgres client tools)
#
# Usage:
#   ./scripts/dump_flyio.sh
#
# The script proxies the Fly.io Postgres through a local port, runs
# pg_dump, and writes the output to data/cinderhaven_dump.sql.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(dirname "$SCRIPT_DIR")"
DUMP_FILE="$REPO_ROOT/data/cinderhaven_dump.sql"
FLY_APP="cinderhaven-data-platform-db"
LOCAL_PORT=15432

echo "Starting Fly.io proxy on port $LOCAL_PORT ..."
flyctl proxy "$LOCAL_PORT:5432" -a "$FLY_APP" &
PROXY_PID=$!

cleanup() {
    echo "Stopping proxy (PID $PROXY_PID) ..."
    kill "$PROXY_PID" 2>/dev/null || true
}
trap cleanup EXIT

sleep 3

echo "Running pg_dump ..."
# Dump all schemas — dbt creates public_staging, public_intermediate,
# public_marts (default schema naming with target schema = public).
# Raw source tables live in the raw schema.
pg_dump \
    --host=localhost \
    --port="$LOCAL_PORT" \
    --username=postgres \
    --dbname=cinderhaven \
    --no-owner \
    --no-privileges \
    --if-exists \
    --clean \
    --exclude-schema=information_schema \
    --exclude-schema='pg_*' \
    > "$DUMP_FILE"

echo "Dump written to $DUMP_FILE ($(du -h "$DUMP_FILE" | cut -f1))"
echo ""
echo "Next: run 'docker compose up' to start local Postgres with this data."
