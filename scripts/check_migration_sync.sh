#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
PG_DIR="$REPO_ROOT/migrations"
SQLITE_DIR="$REPO_ROOT/crates/index/migrations"

pg_versions=$(ls "$PG_DIR"/*.up.sql 2>/dev/null | xargs -I{} basename {} | sed 's/_.*//' | sort)
sqlite_versions=$(ls "$SQLITE_DIR"/*.up.sql 2>/dev/null | xargs -I{} basename {} | sed 's/_.*//' | sort)

if [ -z "$pg_versions" ]; then
  echo "ERROR: No Postgres migrations found in $PG_DIR"
  exit 1
fi

if [ -z "$sqlite_versions" ]; then
  echo "ERROR: No SQLite migrations found in $SQLITE_DIR"
  exit 1
fi

only_pg=$(comm -23 <(echo "$pg_versions") <(echo "$sqlite_versions"))
only_sqlite=$(comm -13 <(echo "$pg_versions") <(echo "$sqlite_versions"))

if [ -z "$only_pg" ] && [ -z "$only_sqlite" ]; then
  echo "OK: Postgres and SQLite migrations are in sync ($(echo "$pg_versions" | wc -l | tr -d ' ') versions)"
  exit 0
fi

echo "ERROR: Migration versions are out of sync!"
echo ""
if [ -n "$only_pg" ]; then
  echo "Only in Postgres (migrations/):"
  echo "$only_pg" | sed 's/^/  - /'
fi
if [ -n "$only_sqlite" ]; then
  echo "Only in SQLite (crates/index/migrations/):"
  echo "$only_sqlite" | sed 's/^/  - /'
fi
exit 1
