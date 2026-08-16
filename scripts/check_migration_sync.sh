#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
PG_DIR="$REPO_ROOT/migrations"
SQLITE_DIR="$REPO_ROOT/crates/shardline-index/migrations"
PG_REGISTRY="$REPO_ROOT/crates/shardline-server/src/database_migration.rs"
SQLITE_REGISTRY="$REPO_ROOT/crates/shardline-index/src/local_sqlite/migration.rs"

# Extracts the `.up.sql` filenames registered in a `*MIGRATIONS` array from the
# `include_str!(...)` paths. The registration arrays are the source of truth:
# a migration file on disk that is never registered would otherwise go
# undetected (the filename-only check below misses it). Matches the quoted
# string literal itself so both single-line and multi-line `include_str!`
# layouts are handled.
registered_up_filenames() {
  local registry="$1"
  grep -o '"[^"]*\.up\.sql"' "$registry" \
    | tr -d '"' \
    | xargs -I{} basename {}
}

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

if [ -n "$only_pg" ] || [ -n "$only_sqlite" ]; then
  echo "ERROR: Migration versions are out of sync!"
  echo ""
  if [ -n "$only_pg" ]; then
    echo "Only in Postgres (migrations/):"
    echo "$only_pg" | sed 's/^/  - /'
  fi
  if [ -n "$only_sqlite" ]; then
    echo "Only in SQLite (crates/shardline-index/migrations/):"
    echo "$only_sqlite" | sed 's/^/  - /'
  fi
  exit 1
fi

# Diff the registered arrays against the on-disk filenames so a registration
# gap (an .up.sql present on disk but missing from the Rust registration list)
# fails the check.
pg_registered=$(registered_up_filenames "$PG_REGISTRY" | sort)
sqlite_registered=$(registered_up_filenames "$SQLITE_REGISTRY" | sort)

# Compare full filenames (version + name) between disk and registry.
pg_disk=$(ls "$PG_DIR"/*.up.sql 2>/dev/null | xargs -I{} basename {} | sort)
sqlite_disk=$(ls "$SQLITE_DIR"/*.up.sql 2>/dev/null | xargs -I{} basename {} | sort)

missing_pg_registration=$(comm -23 <(echo "$pg_disk") <(echo "$pg_registered"))
extra_pg_registration=$(comm -13 <(echo "$pg_disk") <(echo "$pg_registered"))
missing_sqlite_registration=$(comm -23 <(echo "$sqlite_disk") <(echo "$sqlite_registered"))
extra_sqlite_registration=$(comm -13 <(echo "$sqlite_disk") <(echo "$sqlite_registered"))

registration_errors=""
if [ -n "$missing_pg_registration" ]; then
  registration_errors+="Postgres .up.sql files missing from SHARDLINE_MIGRATIONS (database_migration.rs):\n$missing_pg_registration\n"
fi
if [ -n "$extra_pg_registration" ]; then
  registration_errors+="Postgres registrations without an on-disk .up.sql (database_migration.rs):\n$extra_pg_registration\n"
fi
if [ -n "$missing_sqlite_registration" ]; then
  registration_errors+="SQLite .up.sql files missing from LOCAL_SQLITE_MIGRATIONS (local_sqlite/migration.rs):\n$missing_sqlite_registration\n"
fi
if [ -n "$extra_sqlite_registration" ]; then
  registration_errors+="SQLite registrations without an on-disk .up.sql (local_sqlite/migration.rs):\n$extra_sqlite_registration\n"
fi

if [ -n "$registration_errors" ]; then
  echo "ERROR: Migration registration lists are out of sync with the migration files!"
  echo ""
  echo -e "$registration_errors"
  exit 1
fi

echo "OK: Postgres and SQLite migrations are in sync ($(echo "$pg_versions" | wc -l | tr -d ' ') versions)"
exit 0
