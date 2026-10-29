#!/usr/bin/env bash
# Exercise released/current OCI and S3 processes against the same deployment.
# Usage: bash scripts/shardline/verify_maintenance_oci_upgrade.sh CURRENT RELEASED
set -euo pipefail
test "$#" = 2 || { echo 'usage: verify_maintenance_oci_upgrade.sh CURRENT RELEASED' >&2; exit 2; }
current=$(realpath "$1")
released=$(realpath "$2")
test -x "$current" && test -x "$released"
for tool in docker curl jq rg sha256sum shuf ps awk; do command -v "$tool" >/dev/null; done
umask 077
fixture=$(mktemp -d "${TMPDIR:-/tmp}/shardline-oci-upgrade.XXXXXX")
echo "diagnostics: $fixture"
sha256sum "$current" "$released" > "$fixture/binaries.sha256"
"$current" --version > "$fixture/current-version"
"$released" --version > "$fixture/released-version"
trap 'printf "FAIL: OCI upgrade drill at line %s; diagnostics: %s\n" "$LINENO" "$fixture" >&2' ERR
container_id=''
server_pids=()
request_pid=''
upgrade_pid=''
cleanup() {
    if test -n "$upgrade_pid"; then kill "$upgrade_pid" 2>/dev/null || true; wait "$upgrade_pid" 2>/dev/null || true; fi
    if test -n "$request_pid"; then kill "$request_pid" 2>/dev/null || true; wait "$request_pid" 2>/dev/null || true; fi
    for pid in "${server_pids[@]}"; do
        if test -n "$pid"; then kill "$pid" 2>/dev/null || true; wait "$pid" 2>/dev/null || true; fi
    done
    if test -n "$container_id"; then docker rm -f -v "$container_id" > "$fixture/cleanup.log" 2>&1 || true; fi
}
trap cleanup EXIT
container_id=$(docker run -d -e POSTGRES_PASSWORD=owned_fixture_password \
    -e POSTGRES_DB=shardline -p 127.0.0.1::5432 postgres:16-alpine)
printf '%s\n' "$container_id" > "$fixture/container-id"
for attempt in $(seq 1 60); do
    if docker exec "$container_id" pg_isready -h 127.0.0.1 -U postgres -d shardline > "$fixture/ready.log" 2>&1; then break; fi
    sleep 1
done
docker exec "$container_id" pg_isready -h 127.0.0.1 -U postgres -d shardline > "$fixture/ready.log" 2>&1
pg_port=$(docker port "$container_id" 5432/tcp | cut -d : -f 2)
database_url="postgres://postgres:owned_fixture_password@127.0.0.1:$pg_port/shardline"
sql() { docker exec "$container_id" psql -U postgres -d shardline -At -v ON_ERROR_STOP=1 -c "$1"; }
"$released" db migrate up --database-url "$database_url" > "$fixture/released-migrate.log" 2>&1
sql 'SELECT * FROM shardline_schema_migrations ORDER BY version' > "$fixture/ledger-before"
task_reader_before=$(sql "SELECT to_regprocedure('public.shardline_task_artifact_read_v1(text,text,bigint)') IS NULL")
# Model an existing publication account. The installer alone owns the schema.
# Publishing tags needs its released repository-fence permissions, but not
# tombstone INSERT. Keep those existing grants identical through the upgrade.
sql "CREATE ROLE existing_oci_writer LOGIN PASSWORD 'owned_fixture_password';
GRANT CONNECT ON DATABASE shardline TO existing_oci_writer;
GRANT USAGE ON SCHEMA public TO existing_oci_writer;
DO \$\$ DECLARE target text; BEGIN
  FOR target IN SELECT tablename FROM pg_tables WHERE schemaname='public'
    AND tablename NOT IN ('shardline_resource_fences', 'shardline_task_artifacts_v1')
  LOOP EXECUTE format('GRANT SELECT, INSERT, UPDATE, DELETE ON public.%I TO existing_oci_writer', target); END LOOP;
END \$\$;
GRANT SELECT, INSERT, UPDATE ON shardline_resource_fences TO existing_oci_writer;
REVOKE INSERT, UPDATE ON shardline_oci_object_tombstones FROM existing_oci_writer;" > "$fixture/writer-grants.log"
writer_url="postgres://existing_oci_writer:owned_fixture_password@127.0.0.1:$pg_port/shardline"
legacy_tables=$(sql "SELECT string_agg(quote_literal(tablename), ',') FROM pg_tables WHERE schemaname='public'")
sql "SELECT tablename, tableowner, coalesce(relacl::text,'') FROM pg_tables
JOIN pg_class ON pg_class.oid=format('%I.%I',schemaname,tablename)::regclass
WHERE schemaname='public' AND tablename IN ($legacy_tables) AND tablename <> 'shardline_task_artifacts_v1'
ORDER BY tablename" > "$fixture/grants-before"
check_writer_privileges() {
    test "$(sql "SELECT NOT rolsuper AND NOT rolbypassrls AND NOT rolcreatedb AND NOT rolcreaterole
        AND NOT has_schema_privilege('existing_oci_writer','public','CREATE')
        AND has_table_privilege('existing_oci_writer','shardline_resource_fences','SELECT')
        AND has_table_privilege('existing_oci_writer','shardline_resource_fences','INSERT')
        AND has_table_privilege('existing_oci_writer','shardline_resource_fences','UPDATE')
        AND NOT has_table_privilege('existing_oci_writer','shardline_resource_fences','DELETE')
        AND NOT has_table_privilege('existing_oci_writer','shardline_oci_object_tombstones','INSERT,UPDATE')
        FROM pg_roles WHERE rolname='existing_oci_writer'")" = t
}
check_writer_privileges
mkdir "$fixture/root"
signing_key=0123456789abcdef0123456789abcdef
admin_token=owned-native-task-admin-token
token=$(SHARDLINE_TOKEN_SIGNING_KEY="$signing_key" "$released" admin token \
    --issuer fixture --subject fixture --scope write --provider generic \
    --owner team --repo upgrade --key-env SHARDLINE_TOKEN_SIGNING_KEY)
ports=("$(shuf -i 20000-39999 -n 1)" "$(shuf -i 40000-59999 -n 1)")
assert_no_server_children() {
    local node=$1 phase=$2 owner=${server_pids[$1]}
    kill -0 "$owner"
    local snapshot="$fixture/children-$node-$owner-$phase.txt"
    ps -eo pid=,ppid=,comm= | awk -v owner="$owner" '$2 == owner { print }' > "$snapshot"
    test ! -s "$snapshot"
}
spawn_server() {
    local binary=$1 node=$2
    env SHARDLINE_ROOT_DIR="$fixture/root" SHARDLINE_BIND_ADDR="127.0.0.1:${ports[$node]}" \
        SHARDLINE_PUBLIC_BASE_URL="http://127.0.0.1:${ports[$node]}" \
        SHARDLINE_AUTH_PROVIDER=local SHARDLINE_TOKEN_SIGNING_KEY="$signing_key" SHARDLINE_SERVER_FRONTENDS=oci,s3 \
        SHARDLINE_RECONSTRUCTION_CACHE_ADAPTER=memory SHARDLINE_OBJECT_STORAGE_ADAPTER=local \
        SHARDLINE_INDEX_POSTGRES_URL="$writer_url" \
        SHARDLINE_ADMIN_READ_TOKEN="$admin_token" \
        "$binary" serve > "$fixture/server-$node.log" 2>&1 &
    server_pids[$node]=$!
}
start_server() {
    local binary=$1 node=$2
    spawn_server "$binary" "$node"
    for attempt in $(seq 1 40); do
        kill -0 "${server_pids[$node]}"
        if curl -fsS --max-time 2 -H "Authorization: Bearer $token" \
            "http://127.0.0.1:${ports[$node]}/v2/" > "$fixture/ready-$node.json" 2>/dev/null; then
            assert_no_server_children "$node" ready
            return
        fi
        sleep 1
    done
    echo "FAIL: server $node did not become ready" >&2
    exit 1
}
inspect_task_history() {
    local phase=$1 grouping=$2 unavailable=$3
    curl -fsS --max-time 15 -H "Authorization: Bearer $admin_token" \
        "http://127.0.0.1:${ports[1]}/api/v1/status" > "$fixture/status-$phase.json"
    jq -e '.state == "ready"' "$fixture/status-$phase.json" > "$fixture/status-$phase-checked.log"
    curl -fsS --max-time 15 -H "Authorization: Bearer $admin_token" \
        "http://127.0.0.1:${ports[1]}/api/v1/tasks/maintenance?limit=1" > "$fixture/history-$phase.json"
    jq -e --arg grouping "$grouping" --argjson unavailable "$unavailable" '
        .supervision == "embedded" and .history_grouping == $grouping
        and (.tasks | type == "array")
        and (if $unavailable then
            .history_notice | contains("an empty list does not prove maintenance did not run")
        else .history_notice == null end)
    ' "$fixture/history-$phase.json" > "$fixture/history-$phase-checked.log"
    assert_no_server_children 1 "history-$phase"
}
media_type=application/vnd.oci.image.index.v1+json
printf '%s' '{"schemaVersion":2,"mediaType":"application/vnd.oci.image.index.v1+json","manifests":[]}' > "$fixture/published.json"
printf '%s' '{"schemaVersion":2,"mediaType":"application/vnd.oci.image.index.v1+json","manifests":[],"annotations":{"fixture":"interrupted"}}' > "$fixture/interrupted.json"
published_digest=$(sha256sum "$fixture/published.json" | cut -d ' ' -f 1)
interrupted_digest=$(sha256sum "$fixture/interrupted.json" | cut -d ' ' -f 1)
request() {
    local node=$1 method=$2 reference=$3
    shift 3
    curl -sS --max-time 15 -o "$fixture/response-$node.body" -w '%{http_code}' \
        -H "Authorization: Bearer $token" -H "Content-Type: $media_type" \
        -X "$method" "$@" "http://127.0.0.1:${ports[$node]}/v2/team/upgrade/manifests/$reference"
}
read_manifest() {
    local node=$1 reference=$2 expected=$3
    test "$(request "$node" GET "$reference")" = "$expected"
    test "$(curl -sS --max-time 15 -I -o "$fixture/head-$node.headers" -w '%{http_code}' \
        -H "Authorization: Bearer $token" \
        "http://127.0.0.1:${ports[$node]}/v2/team/upgrade/manifests/$reference")" = "$expected"
}
s3_request() {
    local node=$1 method=$2 key=$3
    shift 3
    curl -sS --max-time 15 -o "$fixture/s3-response-$node.body" -w '%{http_code}' \
        -H "Authorization: AWS4-HMAC-SHA256 Credential=$token/20260813/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-date, Signature=deadbeef" \
        -X "$method" "$@" "http://127.0.0.1:${ports[$node]}/team.upgrade/$key"
}
start_server "$released" 0
test "$(s3_request 0 PUT s3-owner-loss --data-binary before)" = 200
test "$(request 0 PUT latest --data-binary "@$fixture/published.json")" = 201
read_manifest 0 "sha256:$published_digest" 200
# The candidate requires additive frontend migrations. Before installation it
# must reject startup clearly, leaving the released process and data untouched.
spawn_server "$current" 1
for attempt in $(seq 1 40); do
    if ! kill -0 "${server_pids[1]}" 2>/dev/null; then break; fi
    sleep 1
done
if kill -0 "${server_pids[1]}" 2>/dev/null; then
    echo 'FAIL: unmigrated candidate did not exit' >&2
    exit 1
fi
if wait "${server_pids[1]}"; then
    echo 'FAIL: unmigrated candidate exited successfully' >&2
    exit 1
fi
rg -q 'frontend schema is incompatible' "$fixture/server-1.log"
test ! -e "$fixture/root/chunks/_shardline_tasks"
sql 'SELECT * FROM shardline_schema_migrations ORDER BY version' > "$fixture/ledger-preupgrade-current"
cmp "$fixture/ledger-before" "$fixture/ledger-preupgrade-current"
test "$(sql "SELECT to_regprocedure('public.shardline_task_artifact_read_v1(text,text,bigint)') IS NULL")" = "$task_reader_before"
server_pids[1]=''
mv "$fixture/server-1.log" "$fixture/server-preupgrade-current.log"
# A released native writer already holds the shared maintenance barrier.
# The owner upgrade must wait there before changing protection triggers.
sql "CREATE FUNCTION fixture_pause_upgrade_writer() RETURNS trigger LANGUAGE plpgsql AS \$\$
BEGIN IF NEW.tag='upgrade-writer' THEN PERFORM pg_sleep(5); END IF; RETURN NEW; END \$\$;
CREATE TRIGGER fixture_pause_upgrade_writer BEFORE INSERT OR UPDATE ON shardline_oci_tags
FOR EACH ROW EXECUTE FUNCTION fixture_pause_upgrade_writer();" > "$fixture/upgrade-writer-install.log"
request 0 PUT upgrade-writer --data-binary "@$fixture/published.json" > "$fixture/upgrade-writer-status" &
request_pid=$!
for attempt in $(seq 1 50); do
    if test "$(sql "SELECT count(*) FROM pg_stat_activity WHERE usename='existing_oci_writer' AND wait_event='PgSleep'")" = 1; then break; fi
    sleep 0.1
done
test "$(sql "SELECT count(*) FROM pg_stat_activity WHERE usename='existing_oci_writer' AND wait_event='PgSleep'")" = 1
"$current" db migrate up --database-url "$database_url" > "$fixture/upgrade.log" 2>&1 &
upgrade_pid=$!
upgrade_waiting=false
for attempt in $(seq 1 50); do
    kill -0 "$upgrade_pid"
    upgrade_waiting=$(sql "SELECT EXISTS(SELECT 1 FROM pg_locks WHERE locktype='advisory'
        AND NOT granted AND mode='ExclusiveLock' AND objsubid=1
        AND classid::bigint=(x'5348415244474301'::bit(64)::bigint >> 32)
        AND objid::bigint=(x'5348415244474301'::bit(64)::bigint & 4294967295))")
    if test "$upgrade_waiting" = t; then break; fi
    sleep 0.1
done
test "$upgrade_waiting" = t
wait "$request_pid"
request_pid=''
test "$(cat "$fixture/upgrade-writer-status")" = 201
wait "$upgrade_pid"
upgrade_pid=''
sql 'DROP TRIGGER fixture_pause_upgrade_writer ON shardline_oci_tags; DROP FUNCTION fixture_pause_upgrade_writer();' > "$fixture/upgrade-writer-remove.log"
"$released" db migrate verify --database-url "$database_url" > "$fixture/released-verify.log" 2>&1
sql 'SELECT version,name,checksum FROM shardline_frontend_schema_v1 ORDER BY version' > "$fixture/frontend-ledger-after"
test "$(sql "SELECT count(*)=7 AND count(DISTINCT name)=7
    AND bool_and(checksum ~ '^[0-9a-f]{64}$') FROM shardline_frontend_schema_v1")" = t
test "$(sql "SELECT to_regclass('public.shardline_hub_object_locations') IS NOT NULL
    AND to_regclass('public.shardline_xet_file_aliases') IS NOT NULL
    AND to_regclass('public.shardline_record_membership') IS NOT NULL
    AND to_regclass('public.shardline_xet_xorbs') IS NOT NULL
    AND to_regclass('public.shardline_xet_chunks') IS NOT NULL")" = t
sql 'SELECT * FROM shardline_schema_migrations ORDER BY version' > "$fixture/ledger-after"
cmp "$fixture/ledger-before" "$fixture/ledger-after"
sql "SELECT tablename, tableowner, coalesce(relacl::text,'') FROM pg_tables
JOIN pg_class ON pg_class.oid=format('%I.%I',schemaname,tablename)::regclass
WHERE schemaname='public' AND tablename IN ($legacy_tables) AND tablename <> 'shardline_task_artifacts_v1'
ORDER BY tablename" > "$fixture/grants-after"
cmp "$fixture/grants-before" "$fixture/grants-after"
check_writer_privileges
test "$(sql "SELECT NOT has_table_privilege('existing_oci_writer',
    'shardline_task_artifacts_v1','SELECT,INSERT,UPDATE,DELETE')")" = t
start_server "$current" 1
inspect_task_history upgraded shared_metadata false
for node in 0 1; do read_manifest "$node" latest 200; done

# A fixture-only uncertain delete must block both native publication paths.
# The current server returns the actionable error; the released server cannot
# acquire that newer error mapping, but must still reject rather than publish.
published_scope=$(sql "SELECT scope_namespace FROM shardline_oci_tags WHERE repository='team/upgrade' AND tag='latest'")
repository_digest=$(printf '%s' team/upgrade | sha256sum | cut -d ' ' -f 1)
pending_key="protocols/oci/$published_scope/repos/$repository_digest/manifests/$published_digest"
sql "INSERT INTO shardline_resource_fences(domain,resource,epoch)
VALUES ('maintenance-remote-delete-sequence','global',1),('maintenance-remote-delete','$pending_key',1);" > "$fixture/pending-install.log"
test "$(request 1 PUT latest --data-binary "@$fixture/published.json")" = 503
rg -q 'unresolved remote delete; retry after reconciliation' "$fixture/response-1.body"
released_pending_status=$(request 0 PUT latest --data-binary "@$fixture/published.json")
# Older predecessors map this rejection to 500; 1.12.1 already returns 503.
[[ "$released_pending_status" = 500 || "$released_pending_status" = 503 ]]
for node in 0 1; do
    read_manifest "$node" latest 200
    cmp "$fixture/published.json" "$fixture/response-$node.body"
done
# There is no external effect in this disposable fixture, so cleanup is safe.
# Production unknown deletes cannot be settled by simply removing their row.
sql "DELETE FROM shardline_resource_fences WHERE domain='maintenance-remote-delete' AND resource='$pending_key';" > "$fixture/pending-remove.log"

# Fixture-only fault at publication: storage has completed, but the metadata
# transaction must roll back. No real remote delete is sent by this drill.
sql "CREATE FUNCTION fixture_reject_publication() RETURNS trigger LANGUAGE plpgsql AS \$\$
BEGIN IF NEW.tag='interrupted' THEN RAISE EXCEPTION 'fixture publication failure'; END IF; RETURN NEW; END \$\$;
CREATE TRIGGER fixture_reject_publication BEFORE INSERT OR UPDATE ON shardline_oci_tags
FOR EACH ROW EXECUTE FUNCTION fixture_reject_publication();" > "$fixture/fault-install.log"
test "$(request 1 PUT interrupted --data-binary "@$fixture/interrupted.json")" = 500
test "$(sql "SELECT count(*) FROM shardline_oci_object_tombstones WHERE repository='team/upgrade' AND object_kind='manifest' AND digest_hex='$interrupted_digest'")" = 1
scope=$(sql "SELECT scope_namespace FROM shardline_oci_object_tombstones WHERE repository='team/upgrade' AND digest_hex='$interrupted_digest'")
repository_digest=$(printf '%s' team/upgrade | sha256sum | cut -d ' ' -f 1)
objects="$fixture/root/chunks/protocols/oci/$scope/repos/$repository_digest"
cmp "$fixture/interrupted.json" "$objects/manifests/$interrupted_digest"
test "$(cat "$objects/manifest-media-types/$interrupted_digest")" = "$media_type"
for node in 0 1; do
    read_manifest "$node" "sha256:$interrupted_digest" 404
    read_manifest "$node" interrupted 404
    read_manifest "$node" latest 200
    cmp "$fixture/published.json" "$fixture/response-$node.body"
done
sql 'DROP TRIGGER fixture_reject_publication ON shardline_oci_tags; DROP FUNCTION fixture_reject_publication();' > "$fixture/fault-remove.log"
test "$(request 0 PUT interrupted --data-binary "@$fixture/interrupted.json")" = 201
test "$(request 1 PUT interrupted --data-binary "@$fixture/interrupted.json")" = 201
for node in 0 1; do
    read_manifest "$node" "sha256:$interrupted_digest" 200
    cmp "$fixture/interrupted.json" "$fixture/response-$node.body"
    read_manifest "$node" interrupted 200
    read_manifest "$node" latest 200
    cmp "$fixture/published.json" "$fixture/response-$node.body"
done

# Exercise guarded S3 publication through the real fallback router using the
# same restricted account and object storage as the released node.
for node in 0 1; do
    test "$(s3_request "$node" GET s3-owner-loss)" = 200
    test "$(cat "$fixture/s3-response-$node.body")" = before
done
sql "CREATE FUNCTION fixture_pause_s3_owner() RETURNS trigger LANGUAGE plpgsql AS \$\$
BEGIN PERFORM pg_sleep(5); RETURN NEW; END \$\$;
CREATE TRIGGER fixture_pause_s3_owner BEFORE INSERT OR UPDATE ON shardline_s3_objects
FOR EACH ROW WHEN (NEW.object_key='s3-owner-loss') EXECUTE FUNCTION fixture_pause_s3_owner();" > "$fixture/s3-owner-loss-install.log"
(s3_request 1 PUT s3-owner-loss --data-binary after > "$fixture/s3-owner-loss.status" 2> "$fixture/s3-owner-loss.log") &
request_pid=$!
s3_pid=''
for attempt in $(seq 1 50); do
    s3_pid=$(sql "SELECT pid FROM pg_stat_activity WHERE usename='existing_oci_writer'
        AND wait_event='PgSleep' AND query LIKE '%shardline_s3_objects%'")
    if test -n "$s3_pid"; then break; fi
    sleep 0.1
done
[[ "$s3_pid" =~ ^[0-9]+$ ]]
test "$(sql "SELECT EXISTS (SELECT 1 FROM pg_locks WHERE pid=$s3_pid
    AND locktype='advisory' AND mode='ShareLock' AND granted AND objsubid=1
    AND classid::bigint=1397244242 AND objid::bigint=1145520897)")" = t
test "$(sql "SELECT pg_terminate_backend($s3_pid)")" = t
wait "$request_pid"
request_pid=''
[[ "$(cat "$fixture/s3-owner-loss.status")" =~ ^5[0-9][0-9]$ ]]
kill -0 "${server_pids[1]}"
for node in 0 1; do
    test "$(s3_request "$node" GET s3-owner-loss)" = 200
    test "$(cat "$fixture/s3-response-$node.body")" = before
done
sql 'DROP TRIGGER fixture_pause_s3_owner ON shardline_s3_objects; DROP FUNCTION fixture_pause_s3_owner();' > "$fixture/s3-owner-loss-remove.log"
test "$(s3_request 1 PUT s3-owner-loss --data-binary after)" = 200
for node in 0 1; do
    test "$(s3_request "$node" GET s3-owner-loss)" = 200
    test "$(cat "$fixture/s3-response-$node.body")" = after
done
test "$(s3_request 0 PUT s3-owner-loss --data-binary released-after)" = 200
for node in 0 1; do
    test "$(s3_request "$node" GET s3-owner-loss)" = 200
    test "$(cat "$fixture/s3-response-$node.body")" = released-after
done

# Session creation must not survive losing the request owner either.
session_count=$(sql 'SELECT count(*) FROM shardline_resumable_sessions')
sql "CREATE FUNCTION fixture_pause_session() RETURNS trigger LANGUAGE plpgsql AS \$\$
BEGIN PERFORM pg_sleep(5); RETURN NEW; END \$\$;
CREATE TRIGGER fixture_pause_session BEFORE INSERT ON shardline_resumable_sessions
FOR EACH ROW EXECUTE FUNCTION fixture_pause_session();" > "$fixture/session-owner-loss-install.log"
(curl -sS --max-time 15 -o "$fixture/session-create.body" -w '%{http_code}' \
    -H "Authorization: Bearer $token" -X POST \
    "http://127.0.0.1:${ports[1]}/v2/team/upgrade/blobs/uploads/" \
    > "$fixture/session-create.status" 2> "$fixture/session-create.log") &
request_pid=$!
session_pid=''
for attempt in $(seq 1 50); do
    session_pid=$(sql "SELECT pid FROM pg_stat_activity WHERE usename='existing_oci_writer'
        AND wait_event='PgSleep' AND query LIKE '%INSERT INTO shardline_resumable_sessions%'")
    if test -n "$session_pid"; then break; fi
    sleep 0.1
done
[[ "$session_pid" =~ ^[0-9]+$ ]]
request_owner=$(sql "SELECT DISTINCT locks.pid FROM pg_locks AS locks
    JOIN pg_stat_activity AS activity ON activity.pid=locks.pid
    WHERE activity.usename='existing_oci_writer' AND locktype='advisory'
    AND mode='ShareLock' AND granted AND objsubid=1
    AND classid::bigint=1397244242 AND objid::bigint=1145520897")
[[ "$request_owner" =~ ^[0-9]+$ ]]
test "$(sql "SELECT pg_terminate_backend($request_owner)")" = t
wait "$request_pid"
request_pid=''
[[ "$(cat "$fixture/session-create.status")" =~ ^5[0-9][0-9]$ ]]
test "$(sql 'SELECT count(*) FROM shardline_resumable_sessions')" = "$session_count"
sql 'DROP TRIGGER fixture_pause_session ON shardline_resumable_sessions; DROP FUNCTION fixture_pause_session();' > "$fixture/session-owner-loss-remove.log"

# Partial uploads must also reject publication after losing the request owner.
test "$(curl -sS --max-time 15 -o "$fixture/upload-create.body" \
    -D "$fixture/upload-create.headers" -w '%{http_code}' \
    -H "Authorization: Bearer $token" -X POST \
    "http://127.0.0.1:${ports[1]}/v2/team/upgrade/blobs/uploads/")" = 202
upload_location=$(sed -n 's/^[Ll]ocation: \(.*\)\r$/\1/p' "$fixture/upload-create.headers")
[[ "$upload_location" == /v2/team/upgrade/blobs/uploads/* ]]
upload_id=${upload_location##*/}
[[ "$upload_id" =~ ^[A-Za-z0-9_-]+$ ]]
generation_before=$(sql "SELECT generation FROM shardline_resumable_sessions WHERE session_id='$upload_id'")
sql "CREATE FUNCTION fixture_pause_part() RETURNS trigger LANGUAGE plpgsql AS \$\$
BEGIN PERFORM pg_sleep(5); RETURN NEW; END \$\$;
CREATE TRIGGER fixture_pause_part BEFORE INSERT ON shardline_resumable_session_parts
FOR EACH ROW EXECUTE FUNCTION fixture_pause_part();" > "$fixture/part-owner-loss-install.log"
(curl -sS --max-time 15 -o "$fixture/upload-patch.body" -w '%{http_code}' \
    -H "Authorization: Bearer $token" -H 'Content-Type: application/octet-stream' \
    -X PATCH --data-binary abc "http://127.0.0.1:${ports[1]}$upload_location" \
    > "$fixture/upload-patch.status" 2> "$fixture/upload-patch.log") &
request_pid=$!
part_pid=''
for attempt in $(seq 1 50); do
    part_pid=$(sql "SELECT pid FROM pg_stat_activity WHERE usename='existing_oci_writer'
        AND wait_event='PgSleep' AND query LIKE '%INSERT INTO shardline_resumable_session_parts%'")
    if test -n "$part_pid"; then break; fi
    sleep 0.1
done
[[ "$part_pid" =~ ^[0-9]+$ ]]
request_owner=$(sql "SELECT DISTINCT locks.pid FROM pg_locks AS locks
    JOIN pg_stat_activity AS activity ON activity.pid=locks.pid
    WHERE activity.usename='existing_oci_writer' AND locktype='advisory'
    AND mode='ShareLock' AND granted AND objsubid=1
    AND classid::bigint=1397244242 AND objid::bigint=1145520897")
[[ "$request_owner" =~ ^[0-9]+$ ]]
test "$(sql "SELECT pg_terminate_backend($request_owner)")" = t
wait "$request_pid"
request_pid=''
[[ "$(cat "$fixture/upload-patch.status")" =~ ^5[0-9][0-9]$ ]]
test "$(sql "SELECT generation FROM shardline_resumable_sessions WHERE session_id='$upload_id'")" = "$generation_before"
test "$(sql "SELECT count(*) FROM shardline_resumable_session_parts WHERE session_id='$upload_id'")" = 0
sql 'DROP TRIGGER fixture_pause_part ON shardline_resumable_session_parts; DROP FUNCTION fixture_pause_part();' > "$fixture/part-owner-loss-remove.log"
test "$(curl -sS --max-time 15 -o "$fixture/upload-retry.body" -w '%{http_code}' \
    -H "Authorization: Bearer $token" -H 'Content-Type: application/octet-stream' \
    -X PATCH --data-binary abc "http://127.0.0.1:${ports[1]}$upload_location")" = 202

# Completion claims must preserve the active session after ownership loss.
generation_before=$(sql "SELECT generation FROM shardline_resumable_sessions WHERE session_id='$upload_id'")
blob_digest=$(printf '%s' abc | sha256sum | cut -d ' ' -f 1)
sql "CREATE FUNCTION fixture_pause_completion() RETURNS trigger LANGUAGE plpgsql AS \$\$
BEGIN PERFORM pg_sleep(5); RETURN NEW; END \$\$;
CREATE TRIGGER fixture_pause_completion BEFORE UPDATE ON shardline_resumable_sessions
FOR EACH ROW WHEN (NEW.state='completing') EXECUTE FUNCTION fixture_pause_completion();" > "$fixture/completion-owner-loss-install.log"
(curl -sS --max-time 15 -o "$fixture/upload-complete.body" -w '%{http_code}' \
    -H "Authorization: Bearer $token" -X PUT \
    "http://127.0.0.1:${ports[1]}$upload_location?digest=sha256:$blob_digest" \
    > "$fixture/upload-complete.status" 2> "$fixture/upload-complete.log") &
request_pid=$!
completion_pid=''
for attempt in $(seq 1 50); do
    completion_pid=$(sql "SELECT pid FROM pg_stat_activity WHERE usename='existing_oci_writer'
        AND wait_event='PgSleep' AND query LIKE '%UPDATE shardline_resumable_sessions%'")
    if test -n "$completion_pid"; then break; fi
    sleep 0.1
done
[[ "$completion_pid" =~ ^[0-9]+$ ]]
test "$(sql "SELECT EXISTS (SELECT 1 FROM pg_locks WHERE pid=$completion_pid
    AND locktype='advisory' AND mode='ShareLock' AND granted AND objsubid=1
    AND classid::bigint=1397244242 AND objid::bigint=1145520897)")" = t
test "$(sql "SELECT pg_terminate_backend($completion_pid)")" = t
wait "$request_pid"
request_pid=''
[[ "$(cat "$fixture/upload-complete.status")" =~ ^5[0-9][0-9]$ ]]
test "$(sql "SELECT state FROM shardline_resumable_sessions WHERE session_id='$upload_id'")" = active
test "$(sql "SELECT generation FROM shardline_resumable_sessions WHERE session_id='$upload_id'")" = "$generation_before"
sql 'DROP TRIGGER fixture_pause_completion ON shardline_resumable_sessions; DROP FUNCTION fixture_pause_completion();' > "$fixture/completion-owner-loss-remove.log"
test "$(curl -sS --max-time 15 -o "$fixture/upload-complete-retry.body" -w '%{http_code}' \
    -H "Authorization: Bearer $token" -X PUT \
    "http://127.0.0.1:${ports[1]}$upload_location?digest=sha256:$blob_digest")" = 201
for node in 0 1; do
    test "$(curl -sS --max-time 15 -o "$fixture/blob-$node.body" -w '%{http_code}' \
        -H "Authorization: Bearer $token" \
        "http://127.0.0.1:${ports[$node]}/v2/team/upgrade/blobs/sha256:$blob_digest")" = 200
    test "$(cat "$fixture/blob-$node.body")" = abc
done

# Lose the real request's database owner while its server remains alive. The
# publication session itself must own the shared GC lock: losing a separate
# middleware connection used to let this upload return 201.
test "$(request 1 PUT owner-lost --data-binary "@$fixture/published.json")" = 201
printf '%s' '{"schemaVersion":2,"mediaType":"application/vnd.oci.image.index.v1+json","manifests":[],"annotations":{"fixture":"owner-loss"}}' > "$fixture/owner-lost.json"
owner_lost_digest=$(sha256sum "$fixture/owner-lost.json" | cut -d ' ' -f 1)
sql "CREATE FUNCTION fixture_lose_owner() RETURNS trigger LANGUAGE plpgsql AS \$\$
BEGIN IF NEW.tag='owner-lost' THEN PERFORM pg_sleep(5); END IF; RETURN NEW; END \$\$;
CREATE TRIGGER fixture_lose_owner BEFORE INSERT OR UPDATE ON shardline_oci_tags
FOR EACH ROW EXECUTE FUNCTION fixture_lose_owner();" > "$fixture/owner-loss-install.log"
(request 1 PUT owner-lost --data-binary "@$fixture/owner-lost.json" > "$fixture/owner-lost-status" 2> "$fixture/owner-lost-request.log") &
request_pid=$!
publication_pid=''
for attempt in $(seq 1 50); do
    publication_pid=$(sql "SELECT pid FROM pg_stat_activity WHERE usename='existing_oci_writer'
        AND wait_event='PgSleep' AND query LIKE '%INSERT INTO shardline_oci_tags%'")
    if test -n "$publication_pid"; then break; fi
    sleep 0.1
done
[[ "$publication_pid" =~ ^[0-9]+$ ]]
test "$(sql "SELECT EXISTS (SELECT 1 FROM pg_locks WHERE pid=$publication_pid
    AND locktype='advisory' AND mode='ShareLock' AND granted AND objsubid=1
    AND classid::bigint=1397244242 AND objid::bigint=1145520897)")" = t
cmp "$fixture/owner-lost.json" "$objects/manifests/$owner_lost_digest"
test "$(sql "SELECT pg_terminate_backend($publication_pid)")" = t
wait "$request_pid"
request_pid=''
[[ "$(cat "$fixture/owner-lost-status")" =~ ^5[0-9][0-9]$ ]]
kill -0 "${server_pids[1]}"
for node in 0 1; do
    read_manifest "$node" "sha256:$owner_lost_digest" 404
    read_manifest "$node" owner-lost 200
    cmp "$fixture/published.json" "$fixture/response-$node.body"
done
sql 'DROP TRIGGER fixture_lose_owner ON shardline_oci_tags; DROP FUNCTION fixture_lose_owner();' > "$fixture/owner-loss-remove.log"
test "$(request 1 PUT owner-lost --data-binary "@$fixture/owner-lost.json")" = 201
for node in 0 1; do
    read_manifest "$node" owner-lost 200
    cmp "$fixture/owner-lost.json" "$fixture/response-$node.body"
done

# Stop a real server while its publication transaction is sleeping after the
# immutable writes. Let PostgreSQL observe the dead connection naturally;
# do not terminate its backend or manufacture rollback from an owner session.
printf '%s' '{"schemaVersion":2,"mediaType":"application/vnd.oci.image.index.v1+json","manifests":[],"annotations":{"fixture":"process-crash"}}' > "$fixture/crashed.json"
crashed_digest=$(sha256sum "$fixture/crashed.json" | cut -d ' ' -f 1)
sql "CREATE FUNCTION fixture_pause_publication() RETURNS trigger LANGUAGE plpgsql AS \$\$
BEGIN IF NEW.tag='crashed' THEN PERFORM pg_sleep(5); END IF; RETURN NEW; END \$\$;
CREATE TRIGGER fixture_pause_publication BEFORE INSERT OR UPDATE ON shardline_oci_tags
FOR EACH ROW EXECUTE FUNCTION fixture_pause_publication();" > "$fixture/crash-install.log"
(request 1 PUT crashed --data-binary "@$fixture/crashed.json" > "$fixture/crashed-status" 2> "$fixture/crashed-request.log" || true) &
request_pid=$!
publication_pid=''
for attempt in $(seq 1 50); do
    publication_pid=$(sql "SELECT pid FROM pg_stat_activity WHERE usename='existing_oci_writer'
        AND wait_event='PgSleep' AND query LIKE '%INSERT INTO shardline_oci_tags%'")
    if test -n "$publication_pid"; then break; fi
    sleep 0.1
done
[[ "$publication_pid" =~ ^[0-9]+$ ]]
cmp "$fixture/crashed.json" "$objects/manifests/$crashed_digest"
test "$(cat "$objects/manifest-media-types/$crashed_digest")" = "$media_type"
printf '%s\n' "${server_pids[1]}" > "$fixture/crashed-server-pid"
printf '%s\n' "$publication_pid" > "$fixture/crashed-database-pid"
kill -KILL "${server_pids[1]}"
if wait "${server_pids[1]}" 2>/dev/null; then
    echo 'FAIL: crashed server exited successfully instead of dying from SIGKILL' >&2
    exit 1
else
    exit_status=$?
    printf '%s\n' "$exit_status" > "$fixture/crashed-server-status"
    test "$exit_status" = 137
fi
server_pids[1]=''
wait "$request_pid"
request_pid=''
test "$(cat "$fixture/crashed-status")" = 000
for attempt in $(seq 1 60); do
    if test "$(sql "SELECT count(*) FROM pg_stat_activity WHERE pid=$publication_pid")" = 0; then break; fi
    sleep 0.2
done
test "$(sql "SELECT count(*) FROM pg_stat_activity WHERE pid=$publication_pid")" = 0
test "$(sql "SELECT count(*) FROM shardline_oci_object_tombstones WHERE repository='team/upgrade' AND digest_hex='$crashed_digest'")" = 1
read_manifest 0 "sha256:$crashed_digest" 404
read_manifest 0 crashed 404
read_manifest 0 latest 200
cmp "$fixture/published.json" "$fixture/response-0.body"
sql 'DROP TRIGGER fixture_pause_publication ON shardline_oci_tags; DROP FUNCTION fixture_pause_publication();' > "$fixture/crash-remove.log"
mv "$fixture/server-1.log" "$fixture/crashed-server.log"
start_server "$current" 1
inspect_task_history restarted shared_metadata false
read_manifest 1 "sha256:$crashed_digest" 404
test "$(request 1 PUT crashed --data-binary "@$fixture/crashed.json")" = 201
for node in 0 1; do
    read_manifest "$node" "sha256:$crashed_digest" 200
    cmp "$fixture/crashed.json" "$fixture/response-$node.body"
    read_manifest "$node" latest 200
    cmp "$fixture/published.json" "$fixture/response-$node.body"
done
assert_no_server_children 0 finished
assert_no_server_children 1 finished
echo "PASS: restricted released/current OCI and S3 writers preserve prior objects and upload state after database owner loss, publication failure and SIGKILL, and retry with the released ledger and existing table grants unchanged; the artifact table remains private; ordinary servers have no child processes at readiness and completion; diagnostics: $fixture"
