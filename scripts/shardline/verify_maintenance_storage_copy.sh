#!/usr/bin/env bash
# Check ordinary copy behavior without Postgres, including a released rollback.
# Usage: bash scripts/shardline/verify_maintenance_storage_copy.sh CURRENT RELEASED
set -euo pipefail
test "$#" = 2 || { echo 'usage: verify_maintenance_storage_copy.sh CURRENT RELEASED' >&2; exit 2; }
current=$(realpath "$1")
released=$(realpath "$2")
test -x "$current" && test -x "$released"
command -v sqlite3 >/dev/null
command -v xxd >/dev/null
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
sqlite3 -separator $'\t' "$fixture/current/metadata.sqlite3" \
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
sqlite3 -separator $'\t' "$fixture/current/metadata.sqlite3" \
    'SELECT artifact_key, hex(payload) FROM shardline_task_artifacts_v1 ORDER BY artifact_key;' \
    > "$fixture/history-after.log"
cmp "$fixture/history-before.log" "$fixture/history-after.log"
# Recreate a completed object-backed history with its original signed bytes and
# original deployment root. This simulates an earlier tracker, not a new codec.
while IFS=$'\t' read -r artifact_key payload_hex; do
    case "$artifact_key" in _shardline_tasks/v1/*) ;; *) exit 1;; esac
    artifact_path="$fixture/current/chunks/$artifact_key"
    mkdir -p "$(dirname "$artifact_path")"
    printf '%s' "$payload_hex" | xxd -r -p > "$artifact_path"
done < "$fixture/history-before.log"
sqlite3 "$fixture/current/metadata.sqlite3" 'DELETE FROM shardline_task_artifacts_v1;'
touch "$fixture/local.toml"
(cd "$fixture"; env -i "$current" --config "$fixture/local.toml" index rebuild \
    --root "$fixture/current") > "$fixture/relocation.log" 2>&1
if test -d "$fixture/current/chunks/_shardline_tasks"; then
    test -z "$(find "$fixture/current/chunks/_shardline_tasks" -type f -print -quit)"
fi
sqlite3 -separator $'\t' "$fixture/current/metadata.sqlite3" \
    'SELECT artifact_key, hex(payload) FROM shardline_task_artifacts_v1 ORDER BY artifact_key;' \
    > "$fixture/history-relocated.log"
LC_ALL=C join -t $'\t' -a 1 -e MISSING -o 1.1,1.2,2.2 \
    "$fixture/history-before.log" "$fixture/history-relocated.log" \
    > "$fixture/history-byte-comparison.log"
awk -F '\t' 'NF != 3 || $2 != $3 {exit 1}' "$fixture/history-byte-comparison.log"
copy_objects "$released" "$fixture/current" "$fixture/legacy-rollback" "$fixture/legacy-rollback.log"
cmp "$fixture/current.log" "$fixture/legacy-rollback.log"
echo "PASS: current/released copies preserve payload inventory and output without PostgreSQL; completed legacy history relocates byte-for-byte before released rollback; diagnostics: $fixture"
