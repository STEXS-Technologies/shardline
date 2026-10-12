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
    SHARDLINE_TEST_CLEANUP_ROOT="$root/.shardline-maintenance" \
    SHARDLINE_TEST_CLEANUP_MARKER="$fixture/fault-observed.log" \
    "$current" --config "$fixture/local.toml" index rebuild --root "$root" \
    > "$fixture/current.log" 2> "$fixture/current.stderr"
cmp "$fixture/released.log" "$fixture/current.log"
test -s "$fixture/fault-observed.log"
rg -q 'completed maintenance scratch could not be removed; original command result is preserved' "$fixture/current.stderr"
test "$(sqlite3 "$root/metadata.sqlite3" "SELECT count(*) FROM shardline_task_artifacts_v1 WHERE artifact_key LIKE '%/records/%' AND json_type(CAST(payload AS TEXT), '$.event.Completed') IS NOT NULL;")" = 1
test "$(find "$root/.shardline-maintenance" -mindepth 1 -maxdepth 1 -type d | wc -l)" = 1
test ! -e "$root/chunks/_shardline_tasks"
env -i "$released" --config "$fixture/local.toml" index rebuild --root "$root" \
    > "$fixture/rollback.log" 2> "$fixture/rollback.stderr"
cmp "$fixture/released.log" "$fixture/rollback.log"
echo "PASS: injected scratch cleanup failure preserves success, stdout, confirmed history and released rollback; diagnostics: $fixture"
