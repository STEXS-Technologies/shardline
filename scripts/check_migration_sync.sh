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
  local names
  names=$(grep -o '"[^"]*\.up\.sql"' "$registry" \
    | tr -d '"' \
    | xargs -I{} basename {} \
    || true)
  if [ -z "$names" ]; then
    echo "ERROR: No .up.sql registrations found in $registry" >&2
    exit 1
  fi
  printf '%s\n' "$names"
}

# Extracts the `version:` string literals from a `*MIGRATIONS` registration
# array. A registration entry's version field is the value recorded in the
# migration history table and compared against applied migrations, so it must
# agree with the version embedded in the registered filename — mutating only
# the version field (filename unchanged) would otherwise go undetected. The
# extraction is scoped to the array body (from the `*MIGRATIONS: [` opener to
# the closing `];`) so `version:` literals in the file's test code or other
# structs are not counted.
registered_versions() {
  local registry="$1"
  local versions
  versions=$(sed -n '/MIGRATIONS: \[/,/^];/p' "$registry" \
    | grep -o 'version: "[^"]*"' \
    | sed 's/version: "//; s/"$//' \
    || true)
  if [ -z "$versions" ]; then
    echo "ERROR: No version: literals found in $registry" >&2
    exit 1
  fi
  printf '%s\n' "$versions"
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

# Diff each registry's `version:` literals against the versions embedded in its
# registered filenames. The filename-only checks above cannot see a mutation of
# only the version field, so a registration whose version disagrees with its
# on-disk filename fails here.
pg_registered_versions=$(registered_versions "$PG_REGISTRY" | sort)
sqlite_registered_versions=$(registered_versions "$SQLITE_REGISTRY" | sort)
pg_filename_versions=$(registered_up_filenames "$PG_REGISTRY" | sed 's/_.*//' | sort)
sqlite_filename_versions=$(registered_up_filenames "$SQLITE_REGISTRY" | sed 's/_.*//' | sort)

version_field_errors=""
version_mismatch_pg=$(comm -3 <(echo "$pg_registered_versions") <(echo "$pg_filename_versions"))
version_mismatch_sqlite=$(comm -3 <(echo "$sqlite_registered_versions") <(echo "$sqlite_filename_versions"))
if [ -n "$version_mismatch_pg" ]; then
  version_field_errors+="Postgres version: literals disagree with the registered filenames (database_migration.rs):\n$version_mismatch_pg\n"
fi
if [ -n "$version_mismatch_sqlite" ]; then
  version_field_errors+="SQLite version: literals disagree with the registered filenames (local_sqlite/migration.rs):\n$version_mismatch_sqlite\n"
fi

if [ -n "$version_field_errors" ]; then
  echo "ERROR: Migration version fields do not match their registered filenames!"
  echo ""
  echo -e "$version_field_errors"
  exit 1
fi

echo "OK: Postgres and SQLite migrations are in sync ($(echo "$pg_versions" | wc -l | tr -d ' ') versions)"
exit 0
