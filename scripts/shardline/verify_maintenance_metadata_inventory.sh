#!/usr/bin/env bash
# Prototype compatibility proof; this does not install a task-storage backend.
# Usage: bash scripts/shardline/verify_maintenance_metadata_inventory.sh RELEASED
set -euo pipefail
test "$#" = 1 || { echo 'usage: verify_maintenance_metadata_inventory.sh RELEASED' >&2; exit 2; }
released=$(realpath "$1")
test -x "$released"
command -v sqlite3 >/dev/null
umask 077
fixture=$(mktemp -d "${TMPDIR:-/tmp}/shardline-task-metadata.XXXXXX")
echo "diagnostics: $fixture"
mkdir -p "$fixture/source/chunks/fixture" "$fixture/destination/chunks"
printf '%s' 'immutable copy fixture' > "$fixture/source/chunks/fixture/payload"
"$released" fsck --root "$fixture/source" > "$fixture/fsck-before.log" 2>&1
sqlite3 "$fixture/source/metadata.sqlite3" <<'SQL'
CREATE TABLE shardline_task_artifacts_prototype (
    artifact_key TEXT PRIMARY KEY,
    payload BLOB NOT NULL
) WITHOUT ROWID;
INSERT INTO shardline_task_artifacts_prototype VALUES ('fixture/run', X'616263');
SQL
"$released" fsck --root "$fixture/source" > "$fixture/fsck-after.log" 2>&1
cmp "$fixture/fsck-before.log" "$fixture/fsck-after.log"
"$released" storage migrate --from local --from-root "$fixture/source" \
    --to local --to-root "$fixture/destination" > "$fixture/copy.log" 2>&1
rg -qx 'scanned_objects: 1' "$fixture/copy.log"
rg -qx 'copied_bytes: 22' "$fixture/copy.log"
cmp "$fixture/source/chunks/fixture/payload" "$fixture/destination/chunks/fixture/payload"
test "$(sqlite3 "$fixture/source/metadata.sqlite3" 'SELECT hex(payload) FROM shardline_task_artifacts_prototype;')" = 616263
echo "PASS: an opaque SQLite metadata row preserves released fsck output, payload inventory and bytes; production task storage remains unchanged; diagnostics: $fixture"
