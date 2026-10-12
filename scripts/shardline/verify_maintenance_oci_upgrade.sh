#!/usr/bin/env bash
# Exercise released/current OCI processes against the same upgraded deployment.
# Usage: bash scripts/shardline/verify_maintenance_oci_upgrade.sh CURRENT RELEASED
set -euo pipefail
test "$#" = 2 || { echo 'usage: verify_maintenance_oci_upgrade.sh CURRENT RELEASED' >&2; exit 2; }
current=$(realpath "$1")
released=$(realpath "$2")
test -x "$current" && test -x "$released"
for tool in docker curl sha256sum shuf; do command -v "$tool" >/dev/null; done
umask 077
fixture=$(mktemp -d "${TMPDIR:-/tmp}/shardline-oci-upgrade.XXXXXX")
echo "diagnostics: $fixture"
trap 'printf "FAIL: OCI upgrade drill at line %s; diagnostics: %s\n" "$LINENO" "$fixture" >&2' ERR
container_id=''
server_pids=()
cleanup() {
    for pid in "${server_pids[@]}"; do
        kill "$pid" 2>/dev/null || true
        wait "$pid" 2>/dev/null || true
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
        SHARDLINE_INDEX_POSTGRES_URL="$database_url" \
        "$binary" serve > "$fixture/server-$node.log" 2>&1 &
    server_pids+=("$!")
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
"$current" db migrate up --database-url "$database_url" > "$fixture/upgrade.log" 2>&1
"$released" db migrate verify --database-url "$database_url" > "$fixture/released-verify.log" 2>&1
sql 'SELECT * FROM shardline_schema_migrations ORDER BY version' > "$fixture/ledger-after"
cmp "$fixture/ledger-before" "$fixture/ledger-after"
start_server "$current" 1
for node in 0 1; do read_manifest "$node" latest 200; done

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
echo "PASS: released/current OCI readers preserve visibility and retry publication across an unchanged migration ledger; diagnostics: $fixture"
