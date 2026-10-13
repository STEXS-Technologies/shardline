#!/usr/bin/env bash
# Verify ordinary CLI results while a large expired history retires over two calls.
# Usage: bash scripts/shardline/verify_maintenance_incremental_cleanup.sh CURRENT RELEASED
set -euo pipefail
test "$#" -eq 2 || { echo 'usage: verify_maintenance_incremental_cleanup.sh CURRENT RELEASED' >&2; exit 2; }
current=$(realpath "$1")
released=$(realpath "$2")
test -x "$current" && test -x "$released"
for tool in sqlite3 rg cmp sha256sum; do command -v "$tool" >/dev/null; done
umask 077
fixture=$(mktemp -d "${TMPDIR:-/tmp}/shardline-incremental-cleanup.XXXXXX")
echo "diagnostics: $fixture"
cp -- "$current" "$fixture/current-shardline"
cp -- "$released" "$fixture/released-shardline"
sha256sum "$fixture/current-shardline" "$fixture/released-shardline" > "$fixture/binaries.sha256"
root="$fixture/root"
database="$root/metadata.sqlite3"
(cd "$fixture"; env -i "$fixture/released-shardline" index rebuild --root "$root") > "$fixture/released.stdout" 2> "$fixture/released.stderr"
(cd "$fixture"; env -i "$fixture/current-shardline" index rebuild --root "$root") > "$fixture/admitted.stdout" 2> "$fixture/admitted.stderr"
cmp "$fixture/released.stdout" "$fixture/admitted.stdout"
cmp "$fixture/released.stderr" "$fixture/admitted.stderr"
id=$(sqlite3 "$database" "SELECT json_extract(payload,'$.id') FROM shardline_task_artifacts_v1 WHERE artifact_key GLOB '_shardline_tasks/v1/manifests/*';")
[[ "$id" =~ ^[0-9a-f]{32}$ ]]
# Age a verified fixture run and model the unrestricted legacy artifact backlog.
sqlite3 "$database" <<SQL
UPDATE shardline_task_artifacts_v1
SET payload=CAST(json_set(CAST(payload AS TEXT),'$.created_at',0) AS BLOB)
WHERE artifact_key='_shardline_tasks/v1/manifests/$id';
WITH RECURSIVE numbers(n) AS (VALUES(0) UNION ALL SELECT n+1 FROM numbers WHERE n<399)
INSERT INTO shardline_task_artifacts_v1(artifact_key,payload)
SELECT '_shardline_tasks/v1/runs/$id/extra/' || printf('%04d',n),CAST('fixture retained evidence' AS BLOB) FROM numbers;
SQL
count_artifacts() {
    sqlite3 "$database" "SELECT count(*) FROM shardline_task_artifacts_v1 WHERE artifact_key GLOB '_shardline_tasks/v1/runs/$id/*';"
}
before=$(count_artifacts)
test "$before" -gt 400
printf 'preserved payload bytes\n' > "$root/chunks/user-payload"
cp "$root/chunks/user-payload" "$fixture/original-payload"
for invocation in first second; do
    (cd "$fixture"; env -i "$fixture/current-shardline" index rebuild --root "$root") > "$fixture/$invocation.stdout" 2> "$fixture/$invocation.stderr"
    cmp "$fixture/released.stdout" "$fixture/$invocation.stdout"
    cmp "$fixture/released.stderr" "$fixture/$invocation.stderr"
    cmp "$fixture/original-payload" "$root/chunks/user-payload"
    test "$(sqlite3 "$database" "SELECT count(*) FROM shardline_task_artifacts_v1 WHERE artifact_key='_shardline_tasks/v1/manifests/$id';")" -eq 0
    if test "$invocation" = first; then
        test "$(count_artifacts)" -eq "$((before - 256))"
        test "$(sqlite3 "$database" "SELECT count(*) FROM shardline_task_artifacts_v1 WHERE artifact_key='_shardline_tasks/v1/retiring/$id';")" -eq 1
    else
        test "$(count_artifacts)" -eq 0
        test "$(sqlite3 "$database" "SELECT count(*) FROM shardline_task_artifacts_v1 WHERE artifact_key='_shardline_tasks/v1/retiring/$id';")" -eq 0
    fi
done
test "$(sqlite3 "$database" "SELECT count(*) FROM shardline_task_artifacts_v1 WHERE artifact_key GLOB '_shardline_tasks/v1/manifests/*';")" -eq 2
echo 'PASS: ordinary CLI preserves released results and payload bytes while retiring 256 keys, retaining its marker, and finishing on the next invocation'
