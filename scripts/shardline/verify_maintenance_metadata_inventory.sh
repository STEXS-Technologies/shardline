#!/usr/bin/env bash
# Actual automatic-history compatibility proof on an isolated local store.
# Usage: bash scripts/shardline/verify_maintenance_metadata_inventory.sh CURRENT RELEASED
set -euo pipefail
test "$#" = 2 || { echo 'usage: verify_maintenance_metadata_inventory.sh CURRENT RELEASED' >&2; exit 2; }
current=$(realpath "$1")
released=$(realpath "$2")
test -x "$current" && test -x "$released"
command -v sqlite3 >/dev/null
umask 077
fixture=$(mktemp -d "${TMPDIR:-/tmp}/shardline-task-metadata.XXXXXX")
echo "diagnostics: $fixture"
mkdir -p "$fixture/source/chunks/fixture" "$fixture/destination/chunks"
printf '%s' 'immutable copy fixture' > "$fixture/source/chunks/fixture/payload"
env -i "$released" fsck --root "$fixture/source" > "$fixture/fsck-before.log" 2>&1
env -i "$current" fsck --root "$fixture/source" > "$fixture/fsck-current.log" 2>&1
cmp "$fixture/fsck-before.log" "$fixture/fsck-current.log"
sqlite3 -readonly "$fixture/source/metadata.sqlite3" \
    'SELECT artifact_key,hex(payload) FROM shardline_task_artifacts_v1 ORDER BY artifact_key;' > "$fixture/history-before"
test -s "$fixture/history-before"
test "$(sqlite3 -readonly "$fixture/source/metadata.sqlite3" "SELECT count(*) FROM shardline_task_artifacts_v1 WHERE artifact_key GLOB '_shardline_tasks/v1/manifests/*';")" -eq 1
test "$(sqlite3 -readonly "$fixture/source/metadata.sqlite3" "SELECT count(*) FROM shardline_task_artifacts_v1 WHERE artifact_key GLOB '_shardline_tasks/v1/runs/*/records/*';")" -gt 3
test "$(sqlite3 -readonly "$fixture/source/metadata.sqlite3" "SELECT count(*) FROM shardline_task_artifacts_v1 WHERE artifact_key GLOB '_shardline_tasks/v1/runs/*/records/*' AND json_type(CAST(payload AS TEXT),'$.event.Completed') = 'object';")" -eq 1
test ! -e "$fixture/source/chunks/_shardline_tasks"
env -i "$released" fsck --root "$fixture/source" > "$fixture/fsck-after.log" 2>&1
cmp "$fixture/fsck-before.log" "$fixture/fsck-after.log"
env -i "$released" storage migrate --from local --from-root "$fixture/source" \
    --to local --to-root "$fixture/destination" > "$fixture/copy.log" 2>&1
rg -qx 'scanned_objects: 1' "$fixture/copy.log"
rg -qx 'copied_bytes: 22' "$fixture/copy.log"
cmp "$fixture/source/chunks/fixture/payload" "$fixture/destination/chunks/fixture/payload"
sqlite3 -readonly "$fixture/source/metadata.sqlite3" \
    'SELECT artifact_key,hex(payload) FROM shardline_task_artifacts_v1 ORDER BY artifact_key;' > "$fixture/history-after"
cmp "$fixture/history-before" "$fixture/history-after"
test ! -e "$fixture/destination/chunks/_shardline_tasks"
echo "PASS: actual automatic SQLite history preserves released fsck output, payload-only copy and exact source evidence bytes; diagnostics: $fixture"
