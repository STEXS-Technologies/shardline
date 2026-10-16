#!/usr/bin/env bash
# Real PostgreSQL schema-routing proof; never uses an existing deployment database.
# Usage: bash scripts/shardline/verify_maintenance_schema_routing.sh RELEASED
set -euo pipefail
test "$#" -eq 1 || { echo 'usage: verify_maintenance_schema_routing.sh RELEASED' >&2; exit 2; }
released=$(realpath "$1")
test -x "$released"
for tool in docker cargo rg; do command -v "$tool" >/dev/null; done
umask 077
fixture=$(mktemp -d "${TMPDIR:-/tmp}/shardline-schema-routing.XXXXXX")
echo "diagnostics: $fixture"
container_id=''
cleanup() {
    if test -n "$container_id"; then
        docker rm -f "$container_id" > "$fixture/cleanup.log" 2>&1 || true
    fi
}
trap cleanup EXIT
container_id=$(docker run -d -e POSTGRES_PASSWORD=owned_schema_fixture \
    -e POSTGRES_DB=shardline -p 127.0.0.1::5432 postgres:16-alpine)
for attempt in $(seq 1 60); do
    if docker exec "$container_id" pg_isready -h 127.0.0.1 -U postgres -d shardline > "$fixture/ready.log" 2>&1; then break; fi
    sleep 1
done
docker exec "$container_id" pg_isready -h 127.0.0.1 -U postgres -d shardline > "$fixture/ready.log" 2>&1
port=$(docker port "$container_id" 5432/tcp | cut -d : -f 2)
export DATABASE_URL="postgres://postgres:owned_schema_fixture@127.0.0.1:$port/shardline"
"$released" db migrate up --database-url "$DATABASE_URL" > "$fixture/released-bootstrap.log" 2>&1
test_name=postgres_logins_can_target_distinct_default_schemas_with_identical_url_options
CARGO_BUILD_JOBS=2 cargo test --locked -p shardline-server --all-features --lib \
    "$test_name" -- --nocapture --test-threads=1 > "$fixture/test.log" 2>&1
if rg -ni 'SKIPPED:|skipping:|panicked at' "$fixture/test.log"; then
    echo 'FAIL: schema-routing fixture skipped or a background task panicked' >&2
    exit 1
fi
# A renamed or filtered-out test must not turn this verifier green.
rg -Fx "test task_tracking::tests::$test_name ... ok" "$fixture/test.log"
rg -F 'test result: ok. 1 passed; 0 failed; 0 ignored;' "$fixture/test.log"
echo 'PASS: distinct login schemas, pinned work-session history and restricted startup fallback'
