#!/usr/bin/env bash
# Exercise released/current OCI processes against the same upgraded deployment.
# Usage: bash scripts/shardline/verify_maintenance_oci_upgrade.sh CURRENT RELEASED
set -euo pipefail
test "$#" = 2 || { echo 'usage: verify_maintenance_oci_upgrade.sh CURRENT RELEASED' >&2; exit 2; }
current=$(realpath "$1")
released=$(realpath "$2")
test -x "$current" && test -x "$released"
for tool in docker curl rg sha256sum shuf; do command -v "$tool" >/dev/null; done
umask 077
fixture=$(mktemp -d "${TMPDIR:-/tmp}/shardline-oci-upgrade.XXXXXX")
echo "diagnostics: $fixture"
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
    if test -n "$container_id"; then docker rm -f "$container_id" > "$fixture/cleanup.log" 2>&1 || true; fi
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
# Model an existing publication account. The installer alone owns the schema.
# Publishing tags needs its released repository-fence permissions, but not
# tombstone INSERT. Keep those existing grants identical through the upgrade.
sql "CREATE ROLE existing_oci_writer LOGIN PASSWORD 'owned_fixture_password';
GRANT CONNECT ON DATABASE shardline TO existing_oci_writer;
GRANT USAGE ON SCHEMA public TO existing_oci_writer;
DO \$\$ DECLARE target text; BEGIN
  FOR target IN SELECT tablename FROM pg_tables WHERE schemaname='public'
    AND tablename <> 'shardline_resource_fences'
  LOOP EXECUTE format('GRANT SELECT, INSERT, UPDATE, DELETE ON public.%I TO existing_oci_writer', target); END LOOP;
END \$\$;
GRANT SELECT, INSERT, UPDATE ON shardline_resource_fences TO existing_oci_writer;
REVOKE INSERT, UPDATE ON shardline_oci_object_tombstones FROM existing_oci_writer;" > "$fixture/writer-grants.log"
writer_url="postgres://existing_oci_writer:owned_fixture_password@127.0.0.1:$pg_port/shardline"
sql "SELECT tablename, tableowner, coalesce(relacl::text,'') FROM pg_tables
JOIN pg_class ON pg_class.oid=format('%I.%I',schemaname,tablename)::regclass
WHERE schemaname='public' ORDER BY tablename" > "$fixture/grants-before"
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
token=$(SHARDLINE_TOKEN_SIGNING_KEY="$signing_key" "$released" admin token \
    --issuer fixture --subject fixture --scope write --provider generic \
    --owner team --repo upgrade --key-env SHARDLINE_TOKEN_SIGNING_KEY)
ports=("$(shuf -i 20000-39999 -n 1)" "$(shuf -i 40000-59999 -n 1)")
start_server() {
    local binary=$1 node=$2
    env SHARDLINE_ROOT_DIR="$fixture/root" SHARDLINE_BIND_ADDR="127.0.0.1:${ports[$node]}" \
        SHARDLINE_PUBLIC_BASE_URL="http://127.0.0.1:${ports[$node]}" \
        SHARDLINE_AUTH_PROVIDER=local SHARDLINE_TOKEN_SIGNING_KEY="$signing_key" SHARDLINE_SERVER_FRONTENDS=oci \
        SHARDLINE_RECONSTRUCTION_CACHE_ADAPTER=memory SHARDLINE_OBJECT_STORAGE_ADAPTER=local \
        SHARDLINE_INDEX_POSTGRES_URL="$writer_url" \
        "$binary" serve > "$fixture/server-$node.log" 2>&1 &
    server_pids[$node]=$!
    for attempt in $(seq 1 40); do
        kill -0 "${server_pids[$node]}"
        if curl -fsS --max-time 2 -H "Authorization: Bearer $token" \
            "http://127.0.0.1:${ports[$node]}/v2/" > "$fixture/ready-$node.json" 2>/dev/null; then return; fi
        sleep 1
    done
    echo "FAIL: server $node did not become ready" >&2
    exit 1
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
start_server "$released" 0
test "$(request 0 PUT latest --data-binary "@$fixture/published.json")" = 201
read_manifest 0 "sha256:$published_digest" 200
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
sql 'SELECT * FROM shardline_schema_migrations ORDER BY version' > "$fixture/ledger-after"
cmp "$fixture/ledger-before" "$fixture/ledger-after"
sql "SELECT tablename, tableowner, coalesce(relacl::text,'') FROM pg_tables
JOIN pg_class ON pg_class.oid=format('%I.%I',schemaname,tablename)::regclass
WHERE schemaname='public' ORDER BY tablename" > "$fixture/grants-after"
cmp "$fixture/grants-before" "$fixture/grants-after"
check_writer_privileges
start_server "$current" 1
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
test "$(request 0 PUT latest --data-binary "@$fixture/published.json")" = 500
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
read_manifest 1 "sha256:$crashed_digest" 404
test "$(request 1 PUT crashed --data-binary "@$fixture/crashed.json")" = 201
for node in 0 1; do
    read_manifest "$node" "sha256:$crashed_digest" 200
    cmp "$fixture/crashed.json" "$fixture/response-$node.body"
    read_manifest "$node" latest 200
    cmp "$fixture/published.json" "$fixture/response-$node.body"
done
echo "PASS: restricted released/current OCI writers preserve visibility after publication failure and SIGKILL, and retry with unchanged migrations and table grants; diagnostics: $fixture"
