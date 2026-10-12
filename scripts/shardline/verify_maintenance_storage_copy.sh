#!/usr/bin/env bash
# Check ordinary copy behavior without Postgres, including a released rollback.
# Usage: bash scripts/shardline/verify_maintenance_storage_copy.sh CURRENT RELEASED
set -euo pipefail
test "$#" = 2 || { echo 'usage: verify_maintenance_storage_copy.sh CURRENT RELEASED' >&2; exit 2; }
current=$(realpath "$1")
released=$(realpath "$2")
test -x "$current" && test -x "$released"
command -v sqlite3 >/dev/null
umask 077
fixture=$(mktemp -d "${TMPDIR:-/tmp}/shardline-storage-copy.XXXXXX")
echo "diagnostics: $fixture"
sha256sum "$current" "$released" > "$fixture/binaries.sha256"
mkdir -p "$fixture/source/chunks/fixture"
printf '%s' 'immutable copy fixture' > "$fixture/source/chunks/fixture/payload"
copy_objects() {
    local binary=$1 source=$2 destination=$3 report=$4
    mkdir -p "$destination/chunks"
    env SHARDLINE_INDEX_POSTGRES_URL='postgres://fixture:fixture@127.0.0.1:1/unused_metadata' \
        "$binary" storage migrate --from local --from-root "$source" \
        --to local --to-root "$destination" > "$report" 2>&1
    cmp "$fixture/source/chunks/fixture/payload" "$destination/chunks/fixture/payload"
}
copy_objects "$current" "$fixture/source" "$fixture/current" "$fixture/current.log"
test ! -e "$fixture/current/chunks/_shardline_tasks"
test "$(sqlite3 "$fixture/current/metadata.sqlite3" 'SELECT count(*) FROM shardline_task_artifacts_v1;')" -gt 0
sqlite3 "$fixture/current/metadata.sqlite3" \
    'SELECT artifact_key, hex(payload) FROM shardline_task_artifacts_v1 ORDER BY artifact_key;' \
    > "$fixture/history-before.log"
copy_objects "$released" "$fixture/source" "$fixture/released" "$fixture/released.log"
cmp "$fixture/current.log" "$fixture/released.log"
copy_objects "$current" "$fixture/current" "$fixture/current-recopy" "$fixture/current-recopy.log"
cmp "$fixture/current.log" "$fixture/current-recopy.log"
copy_objects "$released" "$fixture/current" "$fixture/rollback" "$fixture/rollback.log"
if ! cmp -s "$fixture/current.log" "$fixture/rollback.log"; then
    echo "FAIL: released rollback copies task artifacts as payload objects; diagnostics: $fixture" >&2
    cat "$fixture/rollback.log" >&2
    exit 1
fi
sqlite3 "$fixture/current/metadata.sqlite3" \
    'SELECT artifact_key, hex(payload) FROM shardline_task_artifacts_v1 ORDER BY artifact_key;' \
    > "$fixture/history-after.log"
cmp "$fixture/history-before.log" "$fixture/history-after.log"
echo "PASS: current/released copies preserve payload inventory and output without PostgreSQL, including rollback; diagnostics: $fixture"
