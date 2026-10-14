#!/usr/bin/env bash
# Owned real services: ordinary serve shares PostgreSQL history without S3 task writes.
# Usage: verify_maintenance_postgres_s3_startup.sh CURRENT RELEASED [SERVER_LIB_TEST_HARNESS]
set -euo pipefail
test "$#" = 2 || test "$#" = 3 || { echo 'usage: verify_maintenance_postgres_s3_startup.sh CURRENT RELEASED [SERVER_LIB_TEST_HARNESS]' >&2; exit 2; }
current=$(realpath "$1")
released=$(realpath "$2")
test -x "$current" && test -x "$released"
harness=${3:-}
if test -n "$harness"; then harness=$(realpath "$harness"); test -x "$harness"; fi
for tool in docker curl jq rg shuf join; do command -v "$tool" >/dev/null; done
minio_image=${MINIO_IMAGE:-ghcr.io/golithus/minio@sha256:8793e960474071520bdb91bb9a6d1793eb229fdbc9dcb7ec75ad836b69bbab40}
mc_image=${MC_IMAGE:-ghcr.io/golithus/mc@sha256:dde55ccc0bc9e65dc582c8e652d652eea1438c6e60d774524f4da15d7b26a900}
umask 077
fixture=$(mktemp -d "${TMPDIR:-/tmp}/shardline-postgres-s3-startup.XXXXXX")
echo "diagnostics: $fixture"
postgres_id=''
minio_id=''
server_pid=''
cleanup() {
    if test -n "$server_pid"; then
        kill -TERM "$server_pid" 2>/dev/null || true
        wait "$server_pid" 2>/dev/null || true
    fi
    for container in "$postgres_id" "$minio_id"; do
        if test -n "$container"; then docker rm -f "$container" >> "$fixture/cleanup.log" 2>&1 || true; fi
    done
}
trap cleanup EXIT
sha256sum "$current" "$released" > "$fixture/binaries.sha256"
docker image inspect "$minio_image" "$mc_image" --format '{{json .RepoDigests}}' > "$fixture/service-images.log"
postgres_id=$(docker run -d --rm -e POSTGRES_PASSWORD=owned_startup_fixture \
    -e POSTGRES_DB=shardline -p 127.0.0.1::5432 postgres:16-alpine)
minio_id=$(docker run -d --rm -e MINIO_ROOT_USER=owned_startup_admin \
    -e MINIO_ROOT_PASSWORD=owned_startup_admin_password -p 127.0.0.1::9000 "$minio_image" server /data)
postgres_port=$(docker inspect --format '{{(index (index .NetworkSettings.Ports "5432/tcp") 0).HostPort}}' "$postgres_id")
s3_port=$(docker inspect --format '{{(index (index .NetworkSettings.Ports "9000/tcp") 0).HostPort}}' "$minio_id")
database_url="postgres://postgres:owned_startup_fixture@127.0.0.1:$postgres_port/shardline"
endpoint="http://127.0.0.1:$s3_port"
for attempt in $(seq 1 60); do
    if docker exec "$postgres_id" pg_isready -h 127.0.0.1 -U postgres -d shardline >/dev/null 2>&1 \
        && curl -fsS --max-time 2 "$endpoint/minio/health/ready" >/dev/null; then break; fi
    sleep 1
done
docker exec "$postgres_id" pg_isready -h 127.0.0.1 -U postgres -d shardline > "$fixture/postgres-ready.log"
curl -fsS --max-time 2 "$endpoint/minio/health/ready" > "$fixture/minio-ready.log"
mc() {
    docker run --rm --network host \
        -e "MC_HOST_fixture=http://owned_startup_admin:owned_startup_admin_password@127.0.0.1:$s3_port" \
        -v "$fixture:/fixture:ro" "$mc_image" "$@"
}
sql() {
    docker exec "$postgres_id" psql -h 127.0.0.1 -U postgres -d shardline -v ON_ERROR_STOP=1 -At -F $'\t' -c "$1"
}
touch "$fixture/empty.toml"
env -i "$released" --config "$fixture/empty.toml" db migrate up --database-url "$database_url" > "$fixture/released-bootstrap.log" 2>&1
env -i "$current" --config "$fixture/empty.toml" db migrate up --database-url "$database_url" > "$fixture/owner-upgrade.log" 2>&1
cat > "$fixture/runtime-grants.sql" <<'SQL'
CREATE ROLE startup_runtime LOGIN PASSWORD 'owned_startup_database_password';
GRANT USAGE ON SCHEMA public TO startup_runtime;
DO $grants$
DECLARE ordinary RECORD;
BEGIN
    FOR ordinary IN SELECT tablename FROM pg_tables WHERE schemaname='public'
        AND tablename LIKE 'shardline_%' AND tablename <> 'shardline_task_artifacts_v1'
    LOOP
        EXECUTE format('GRANT SELECT,INSERT,UPDATE,DELETE ON TABLE public.%I TO startup_runtime', ordinary.tablename);
    END LOOP;
END $grants$;
SQL
docker exec -i "$postgres_id" psql -h 127.0.0.1 -U postgres -d shardline -v ON_ERROR_STOP=1 \
    < "$fixture/runtime-grants.sql" > "$fixture/runtime-grants.log" 2>&1
runtime_database_url="postgres://startup_runtime:owned_startup_database_password@127.0.0.1:$postgres_port/shardline"
test "$(sql "SELECT has_table_privilege('startup_runtime','shardline_task_artifacts_v1','SELECT,INSERT,UPDATE,DELETE');")" = f
mc mb fixture/startup-payloads > "$fixture/s3-setup.log"
printf '%s' 'protected remote user payload' > "$fixture/protected-payload"
mc cp /fixture/protected-payload fixture/startup-payloads/fixture/user-payload >> "$fixture/s3-setup.log"
cat > "$fixture/runtime-policy.json" <<'POLICY'
{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":["s3:ListBucket"],"Resource":["arn:aws:s3:::startup-payloads"]},{"Effect":"Allow","Action":["s3:GetObject","s3:PutObject","s3:DeleteObject"],"Resource":["arn:aws:s3:::startup-payloads/*"]},{"Effect":"Deny","Action":["s3:PutObject","s3:DeleteObject"],"Resource":["arn:aws:s3:::startup-payloads/_shardline_tasks/*"]}]}
POLICY
mc admin user add fixture startup-runtime owned_startup_runtime_password >> "$fixture/s3-setup.log"
mc admin policy create fixture payload-runtime /fixture/runtime-policy.json >> "$fixture/s3-setup.log"
mc admin policy attach fixture payload-runtime --user startup-runtime >> "$fixture/s3-setup.log"
runtime_mc() {
    docker run --rm --network host \
        -e "MC_HOST_fixture=http://startup-runtime:owned_startup_runtime_password@127.0.0.1:$s3_port" \
        -v "$fixture:/fixture:ro" "$mc_image" "$@"
}
runtime_mc cp /fixture/protected-payload fixture/startup-payloads/fixture/runtime-write >> "$fixture/s3-setup.log"
runtime_mc rm fixture/startup-payloads/fixture/runtime-write >> "$fixture/s3-setup.log"
if runtime_mc cp /fixture/protected-payload fixture/startup-payloads/_shardline_tasks/denied-probe > "$fixture/task-write-denied.log" 2>&1; then
    echo 'FAIL: runtime S3 credentials can write task history' >&2
    exit 1
fi
rg -qi 'access denied|accessdenied|permission' "$fixture/task-write-denied.log"
printf '[]\n' > "$fixture/previous-ids.json"
touch "$fixture/history-before.tsv"
count=0
for role in all api transfer; do
    count=$((count + 1))
    root="$fixture/node-$role"
    mkdir -p "$root/tmp/lfs-patch"
    oid=$(printf '%064x' "$count")
    printf '%s' 'new expired session for this role' > "$root/tmp/lfs-patch/$oid"
    printf '0' > "$root/tmp/lfs-patch/$oid.meta"
    port=$(shuf -i 20000-60000 -n 1)
    env -i PATH="$PATH" SHARDLINE_ROOT_DIR="$root" \
        SHARDLINE_BIND_ADDR="127.0.0.1:$port" SHARDLINE_PUBLIC_BASE_URL="http://127.0.0.1:$port" \
        SHARDLINE_INDEX_POSTGRES_URL="$runtime_database_url" SHARDLINE_OBJECT_STORAGE_ADAPTER=s3 \
        SHARDLINE_S3_BUCKET=startup-payloads SHARDLINE_S3_REGION=us-east-1 \
        SHARDLINE_S3_ENDPOINT="$endpoint" SHARDLINE_S3_ALLOW_HTTP=true \
        SHARDLINE_S3_ACCESS_KEY_ID=startup-runtime SHARDLINE_S3_SECRET_ACCESS_KEY=owned_startup_runtime_password \
        SHARDLINE_ADMIN_READ_TOKEN=owned-startup-role-admin \
        "$current" --config "$fixture/empty.toml" serve --role "$role" --frontend lfs \
        > "$fixture/$role-server.log" 2>&1 &
    server_pid=$!
    ready=false
    for attempt in $(seq 1 100); do
        if ! kill -0 "$server_pid" 2>/dev/null; then cat "$fixture/$role-server.log" >&2; exit 1; fi
        if curl -fsS --max-time 1 "http://127.0.0.1:$port/readyz" > "$fixture/$role-ready.log" 2>/dev/null; then ready=true; break; fi
        sleep 0.1
    done
    test "$ready" = true
    test ! -e "$root/tmp/lfs-patch/$oid"
    test ! -e "$root/tmp/lfs-patch/$oid.meta"
    curl -fsS --max-time 5 -H 'Authorization: Bearer owned-startup-role-admin' \
        "http://127.0.0.1:$port/api/v1/status" > "$fixture/$role-status.json"
    jq -e --arg role "$role" '.state == "ready" and .server_role == $role' "$fixture/$role-status.json" >/dev/null
    curl -fsS --max-time 5 -H 'Authorization: Bearer owned-startup-role-admin' \
        "http://127.0.0.1:$port/api/v1/tasks/maintenance" > "$fixture/$role-history.json"
    jq -e --argjson count "$count" --argjson pid "$server_pid" --slurpfile previous "$fixture/previous-ids.json" '
        .supervision == "embedded" and .history_grouping == "shared_metadata" and .history_notice == null
        and (.tasks | length) == $count
        and all(.tasks[]; .state == "completed" and .execution == "embedded" and .operation == "lfs-startup-session-sweep")
        and ([.tasks[].id] | unique | length) == $count
        and (([.tasks[].id] - $previous[0]) | length) == 1
        and (($previous[0] - [.tasks[].id]) | length) == 0
        and any(.tasks[]; .owner.pid == $pid and (.id as $id | $previous[0] | index($id)) == null)
    ' "$fixture/$role-history.json" >/dev/null
    jq '[.tasks[].id]' "$fixture/$role-history.json" > "$fixture/previous-ids.json"
    test -z "$(ps -o pid= --ppid "$server_pid")"
    sql "SELECT artifact_key,encode(payload,'hex') FROM shardline_task_artifacts_v1 ORDER BY artifact_key COLLATE \"C\";" > "$fixture/$role-history.tsv"
    LC_ALL=C join -t $'\t' -a 1 -e MISSING -o 1.1,1.2,2.2 "$fixture/history-before.tsv" "$fixture/$role-history.tsv" > "$fixture/$role-preserved.tsv"
    awk -F '\t' 'NF != 3 || $2 != $3 {exit 1}' "$fixture/$role-preserved.tsv"
    cp "$fixture/$role-history.tsv" "$fixture/history-before.tsv"
    mc cat fixture/startup-payloads/fixture/user-payload > "$fixture/$role-payload"
    cmp "$fixture/protected-payload" "$fixture/$role-payload"
    mc ls --recursive --json fixture/startup-payloads > "$fixture/$role-inventory.jsonl"
    jq -s -e 'length == 1 and .[0].key == "fixture/user-payload"' "$fixture/$role-inventory.jsonl" >/dev/null
    test "$(sql 'SELECT count(*) FROM shardline_quarantine_candidates;')" = 0
    kill -TERM "$server_pid"
    wait "$server_pid"
    server_pid=''
    echo "PASS: PostgreSQL/S3 $role startup uses its server PID, shares history and performs fresh cleanup"
done
echo 'PASS: PostgreSQL/S3 all three ordinary server roles preserve payloads without a tracker child or S3 history writes'
if test -n "$harness"; then
    test_name=task_tracking::postgres_metadata::tests::each_postgres_router_role_records_fresh_same_process_startup_history
    env -i DATABASE_URL="$database_url" "$harness" --exact "$test_name" --nocapture > "$fixture/local-payload-roles.log" 2>&1
    if rg -ni 'SKIPPED:|skipping:|panicked at' "$fixture/local-payload-roles.log"; then exit 1; fi
    rg -Fx "test $test_name ... ok" "$fixture/local-payload-roles.log"
    rg -F 'test result: ok. 1 passed; 0 failed; 0 ignored;' "$fixture/local-payload-roles.log"
    echo 'PASS: PostgreSQL/local payload startup records fresh same-process history for all three roles'
fi
