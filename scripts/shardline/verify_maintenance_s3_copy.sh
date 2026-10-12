#!/usr/bin/env bash
# Owned MinIO fixture: normal copies need only payload permissions, including rollback.
# Usage: bash scripts/shardline/verify_maintenance_s3_copy.sh CURRENT RELEASED
set -euo pipefail
test "$#" = 2 || { echo 'usage: verify_maintenance_s3_copy.sh CURRENT RELEASED' >&2; exit 2; }
current=$(realpath "$1")
released=$(realpath "$2")
test -x "$current" && test -x "$released"
test "$(id -u)" != 0 || { echo 'run as non-root to verify read-only source access' >&2; exit 2; }
engine=${CONTAINER_ENGINE:-docker}
minio_image=${MINIO_IMAGE:-ghcr.io/golithus/minio:RELEASE.2025-10-15T17-29-55Z}
mc_image=${MC_IMAGE:-ghcr.io/golithus/mc:RELEASE.2025-08-13T08-35-41Z}
command -v "$engine" >/dev/null
command -v sqlite3 >/dev/null
command -v rg >/dev/null
umask 077
fixture=$(mktemp -d "${TMPDIR:-/tmp}/shardline-s3-copy.XXXXXX")
echo "diagnostics: $fixture"
container=
cleanup() {
    local status=$?
    if test -n "$container"; then "$engine" rm -f "$container" >/dev/null || true; fi
    chmod -R u+w "$fixture/source" 2>/dev/null || true
    return "$status"
}
trap cleanup EXIT
mkdir -p "$fixture/source/chunks/fixture" "$fixture/task-state"
printf '%s' 'immutable copy fixture' > "$fixture/source/chunks/fixture/payload"
touch "$fixture/local.toml"
container=$("$engine" run -d --name "shardline-s3-copy-${fixture##*.}" \
    -e MINIO_ROOT_USER=owned_copy_admin -e MINIO_ROOT_PASSWORD=owned_copy_admin_password \
    -p 127.0.0.1::9000 "$minio_image" server /data)
port=$("$engine" inspect --format '{{(index (index .NetworkSettings.Ports "9000/tcp") 0).HostPort}}' "$container")
endpoint="http://127.0.0.1:$port"
mc() {
    "$engine" run --rm --network host \
        -e "MC_HOST_fixture=http://owned_copy_admin:owned_copy_admin_password@127.0.0.1:$port" \
        -v "$fixture:/fixture:ro" "$mc_image" "$@"
}
ready=false
for attempt in {1..30}; do
    if mc ready fixture > "$fixture/ready.log" 2>&1; then ready=true; break; fi
    sleep 1
done
test "$ready" = true
mc mb fixture/copy-target > "$fixture/setup.log"
cat > "$fixture/payload-policy.json" <<'POLICY'
{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":["s3:ListBucket"],"Resource":["arn:aws:s3:::copy-target"]},{"Effect":"Allow","Action":["s3:GetObject","s3:PutObject"],"Resource":["arn:aws:s3:::copy-target/fixture/payload"]}]}
POLICY
mc admin user add fixture copy-user owned_copy_user_password >> "$fixture/setup.log"
mc admin policy create fixture payload-only /fixture/payload-policy.json >> "$fixture/setup.log"
mc admin policy attach fixture payload-only --user copy-user >> "$fixture/setup.log"
chmod 0444 "$fixture/source/chunks/fixture/payload"
chmod 0555 "$fixture/source" "$fixture/source/chunks" "$fixture/source/chunks/fixture"
copy_temp="$fixture/task-state"
copy_objects() {
    local binary=$1 report=$2
    shift 2
    env -i PATH="$PATH" TMPDIR="$copy_temp" \
        SHARDLINE_INDEX_POSTGRES_URL='postgres://unused:unused@127.0.0.1:1/unused' \
        SHARDLINE_MIGRATE_FROM_S3_BUCKET=copy-target SHARDLINE_MIGRATE_TO_S3_BUCKET=copy-target \
        SHARDLINE_MIGRATE_FROM_S3_REGION=us-east-1 SHARDLINE_MIGRATE_TO_S3_REGION=us-east-1 \
        SHARDLINE_MIGRATE_FROM_S3_ENDPOINT="$endpoint" SHARDLINE_MIGRATE_TO_S3_ENDPOINT="$endpoint" \
        SHARDLINE_MIGRATE_FROM_S3_ALLOW_HTTP=true SHARDLINE_MIGRATE_TO_S3_ALLOW_HTTP=true \
        SHARDLINE_MIGRATE_FROM_S3_ACCESS_KEY_ID=copy-user SHARDLINE_MIGRATE_TO_S3_ACCESS_KEY_ID=copy-user \
        SHARDLINE_MIGRATE_FROM_S3_SECRET_ACCESS_KEY=owned_copy_user_password \
        SHARDLINE_MIGRATE_TO_S3_SECRET_ACCESS_KEY=owned_copy_user_password \
        "$binary" --config "$fixture/local.toml" storage migrate "$@" > "$report" 2> "$report.stderr" &
    copy_pid=$!
    wait "$copy_pid"
}
copy_objects "$current" "$fixture/current.log" --from local --from-root "$fixture/source" --to s3
test ! -e "$fixture/source/.shardline-maintenance"
archive=$(find "$fixture/task-state" -name metadata.sqlite3 -print -quit)
test -n "$archive"
test "$(sqlite3 "$archive" "SELECT count(*) FROM shardline_task_artifacts_v1 WHERE artifact_key LIKE '%/records/%' AND json_type(CAST(payload AS TEXT), '$.event.Completed') IS NOT NULL;")" -gt 0
sqlite3 "$archive" "SELECT CAST(payload AS TEXT) FROM shardline_task_artifacts_v1 WHERE artifact_key LIKE '%/manifests/%';" > "$fixture/manifest.json"
test "$(sqlite3 "$archive" "SELECT json_extract(CAST(payload AS TEXT), '$.owner.pid') FROM shardline_task_artifacts_v1 WHERE artifact_key LIKE '%/manifests/%';")" = "$copy_pid"
mkdir -p "$fixture/current-recopy/chunks" "$fixture/rollback/chunks"
copy_objects "$current" "$fixture/current-recopy.log" --from s3 --to local --to-root "$fixture/current-recopy"
copy_objects "$released" "$fixture/rollback.log" --from s3 --to local --to-root "$fixture/rollback"
cmp "$fixture/source/chunks/fixture/payload" "$fixture/current-recopy/chunks/fixture/payload"
cmp "$fixture/source/chunks/fixture/payload" "$fixture/rollback/chunks/fixture/payload"
rg -q '^scanned_objects: 1$' "$fixture/current.log"
rg -q '^scanned_bytes: 22$' "$fixture/current.log"
awk '/^(scanned_objects|scanned_bytes|inserted_objects|already_present_objects|copied_bytes):/' \
    "$fixture/current.log" > "$fixture/current-counts.log"
awk '/^(scanned_objects|scanned_bytes|inserted_objects|already_present_objects|copied_bytes):/' \
    "$fixture/current-recopy.log" > "$fixture/recopy-counts.log"
cmp "$fixture/current-counts.log" "$fixture/recopy-counts.log"
cmp "$fixture/current-recopy.log" "$fixture/rollback.log"
mkdir "$fixture/read-only-temp"
chmod 0555 "$fixture/read-only-temp"
copy_temp="$fixture/read-only-temp"
copy_objects "$current" "$fixture/no-history.log" --from local --from-root "$fixture/source" --to s3
test -z "$(find "$fixture/read-only-temp" -mindepth 1 -print -quit)"
test ! -e "$fixture/source/.shardline-maintenance"
mc ls --recursive fixture/copy-target > "$fixture/inventory.log"
test "$(wc -l < "$fixture/inventory.log")" = 1
rg -q 'fixture/payload$' "$fixture/inventory.log"
echo "PASS: restricted S3 copies preserve payload inventory and released rollback; history stays in the original process's private metadata; diagnostics: $fixture"
