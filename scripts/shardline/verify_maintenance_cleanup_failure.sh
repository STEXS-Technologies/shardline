#!/usr/bin/env bash
# Linux-only owned fixture: a confirmed scratch cleanup failure cannot change work.
# Usage: bash scripts/shardline/verify_maintenance_cleanup_failure.sh CURRENT RELEASED
set -euo pipefail
test "$#" = 2 || { echo 'usage: verify_maintenance_cleanup_failure.sh CURRENT RELEASED' >&2; exit 2; }
test "$(uname -s)" = Linux
current=$(realpath "$1")
released=$(realpath "$2")
test -x "$current" && test -x "$released"
for tool in cc sqlite3 rg; do command -v "$tool" >/dev/null; done
script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
umask 077
fixture=$(mktemp -d "${TMPDIR:-/tmp}/shardline-cleanup-failure.XXXXXX")
echo "diagnostics: $fixture"
touch "$fixture/local.toml"
cc -shared -fPIC -Wall -Wextra -Werror "$script_dir/fixtures/fail_task_cleanup.c" \
    -ldl -o "$fixture/fail-task-cleanup.so"
root="$fixture/root"
env -i "$released" --config "$fixture/local.toml" index rebuild --root "$root" \
    > "$fixture/released.log" 2> "$fixture/released.stderr"
env -i LD_PRELOAD="$fixture/fail-task-cleanup.so" \
    SHARDLINE_TEST_CLEANUP_ROOT="$root/.shardline-maintenance/retired" \
    SHARDLINE_TEST_CLEANUP_MARKER="$fixture/fault-observed.log" \
    "$current" --config "$fixture/local.toml" index rebuild --root "$root" \
    > "$fixture/current.log" 2> "$fixture/current.stderr"
cmp "$fixture/released.log" "$fixture/current.log"
test -s "$fixture/fault-observed.log"
rg -q 'completed maintenance scratch could not be removed; original command result is preserved' "$fixture/current.stderr"
test "$(sqlite3 "$root/metadata.sqlite3" "SELECT count(*) FROM shardline_task_artifacts_v1 WHERE artifact_key LIKE '%/records/%' AND json_type(CAST(payload AS TEXT), '$.event.Completed') IS NOT NULL;")" = 1
test "$(find "$root/.shardline-maintenance/retired" -mindepth 1 -maxdepth 1 -type d | wc -l)" = 1
test ! -e "$root/chunks/_shardline_tasks"
env -i "$released" --config "$fixture/local.toml" index rebuild --root "$root" \
    > "$fixture/rollback.log" 2> "$fixture/rollback.stderr"
cmp "$fixture/released.log" "$fixture/rollback.log"
sqlite3 -separator $'\t' "$root/metadata.sqlite3" \
    'SELECT artifact_key, hex(payload) FROM shardline_task_artifacts_v1 ORDER BY artifact_key;' > "$fixture/history-before-retry.log"
env -i "$current" --config "$fixture/local.toml" index rebuild --root "$root" \
    > "$fixture/fresh-run.log" 2> "$fixture/fresh-run.stderr"
cmp "$fixture/released.log" "$fixture/fresh-run.log"
test -z "$(find "$root/.shardline-maintenance/retired" -mindepth 1 -maxdepth 1 -type d -print -quit)"
test "$(sqlite3 "$root/metadata.sqlite3" "SELECT count(*) FROM shardline_task_artifacts_v1 WHERE artifact_key LIKE '%/records/%' AND json_type(CAST(payload AS TEXT), '$.event.Completed') IS NOT NULL;")" = 2
sqlite3 -separator $'\t' "$root/metadata.sqlite3" \
    'SELECT artifact_key, hex(payload) FROM shardline_task_artifacts_v1 ORDER BY artifact_key;' > "$fixture/history-after-retry.log"
LC_ALL=C join -t $'\t' -a 1 -e MISSING -o 1.1,1.2,2.2 \
    "$fixture/history-before-retry.log" "$fixture/history-after-retry.log" > "$fixture/history-byte-comparison.log"
awk -F '\t' 'NF != 3 || $2 != $3 {exit 1}' "$fixture/history-byte-comparison.log"
echo "PASS: cleanup fault preserves success and history; a fresh invocation retires leftover scratch, preserves prior receipt bytes and performs its own run without replay; released rollback stays compatible; diagnostics: $fixture"
