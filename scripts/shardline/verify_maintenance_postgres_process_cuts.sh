#!/usr/bin/env bash
# Usage: verify_maintenance_postgres_process_cuts.sh CURRENT RELEASED CLI_TEST_HARNESS
# Build the harness with cargo test --locked -p shardline --test maintenance_e2e --no-run.
set -euo pipefail
test "$#" = 3 || { echo 'usage: verify_maintenance_postgres_process_cuts.sh CURRENT RELEASED CLI_TEST_HARNESS' >&2; exit 2; }
current=$(realpath "$1")
released=$(realpath "$2")
harness=$(realpath "$3")
test -x "$current" && test -x "$released" && test -x "$harness"
umask 077
fixture=$(mktemp -d "${TMPDIR:-/tmp}/shardline-postgres-process-cuts.XXXXXX")
echo "diagnostics: $fixture"
container_id=''
cleanup() {
    if test -n "$container_id"; then
        docker rm -f "$container_id" > "$fixture/cleanup.log" 2>&1 || true
    fi
}
trap cleanup EXIT
container_id=$(docker run -d --rm -e POSTGRES_PASSWORD=owned_process_fixture \
    -e POSTGRES_DB=shardline -p 127.0.0.1::5432 postgres:16-alpine)
for attempt in $(seq 1 60); do
    if docker exec "$container_id" pg_isready -h 127.0.0.1 -U postgres -d shardline >/dev/null 2>&1; then break; fi
    sleep 1
done
docker exec "$container_id" pg_isready -h 127.0.0.1 -U postgres -d shardline > "$fixture/ready.log" 2>&1
port=$(docker inspect --format '{{(index (index .NetworkSettings.Ports "5432/tcp") 0).HostPort}}' "$container_id")
url="postgres://postgres:owned_process_fixture@127.0.0.1:$port/shardline"
token=$(cat /proc/sys/kernel/random/uuid)
touch "$fixture/empty.toml"
env -i "$released" --config "$fixture/empty.toml" db migrate up --database-url "$url" > "$fixture/released-bootstrap.log" 2>&1
env -i "$current" --config "$fixture/empty.toml" db migrate up --database-url "$url" > "$fixture/owner-upgrade.log" 2>&1
docker exec "$container_id" psql -h 127.0.0.1 -U postgres -d shardline -v ON_ERROR_STOP=1 \
    -c "CREATE TABLE shardline_validation_owner(token text NOT NULL); INSERT INTO shardline_validation_owner VALUES ('$token');" > "$fixture/owner.log" 2>&1
test_name=maintenance_support::postgres_interruptions::postgres_commands_preserve_actual_process_receipt_cuts
env -i CARGO_BIN_EXE_shardline="$current" SHARDLINE_OWNED_PG_FIXTURE_URL="$url" \
    SHARDLINE_OWNED_PG_FIXTURE_TOKEN="$token" "$harness" --ignored --exact "$test_name" --nocapture \
    > "$fixture/test.log" 2>&1
if rg -ni 'SKIPPED:|skipping:|panicked at' "$fixture/test.log"; then exit 1; fi
test "$(rg -c '^PASS: PostgreSQL ' "$fixture/test.log")" = 25
rg -Fx "test $test_name ... ok" "$fixture/test.log"
rg -F 'test result: ok. 1 passed; 0 failed; 0 ignored;' "$fixture/test.log"
echo 'PASS: 25 actual PostgreSQL CLI termination cuts preserve receipts and fresh work'
