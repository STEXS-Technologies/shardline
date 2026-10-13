#!/usr/bin/env bash
# Real metadata write faults in an owned PostgreSQL fixture.
# Usage: bash scripts/shardline/verify_maintenance_postgres_receipt_phases.sh CURRENT RELEASED
set -euo pipefail
test "$#" = 2 || { echo 'usage: verify_maintenance_postgres_receipt_phases.sh CURRENT RELEASED' >&2; exit 2; }
current=$(realpath "$1")
released=$(realpath "$2")
test -x "$current" && test -x "$released"
for tool in docker cargo rg; do command -v "$tool" >/dev/null; done
umask 077
fixture=$(mktemp -d "${TMPDIR:-/tmp}/shardline-postgres-receipt-phases.XXXXXX")
echo "diagnostics: $fixture"
container_id=''
cleanup() {
    if test -n "$container_id"; then
        docker rm -f "$container_id" > "$fixture/cleanup.log" 2>&1 || true
    fi
}
trap cleanup EXIT
container_id=$(docker run -d --rm -e POSTGRES_PASSWORD=owned_receipt_fixture \
    -e POSTGRES_DB=shardline -p 127.0.0.1::5432 postgres:16-alpine)
for attempt in $(seq 1 60); do
    if docker exec "$container_id" pg_isready -h 127.0.0.1 -U postgres -d shardline >/dev/null 2>&1; then break; fi
    sleep 1
done
docker exec "$container_id" pg_isready -h 127.0.0.1 -U postgres -d shardline > "$fixture/ready.log" 2>&1
port=$(docker inspect --format '{{(index (index .NetworkSettings.Ports "5432/tcp") 0).HostPort}}' "$container_id")
export DATABASE_URL="postgres://postgres:owned_receipt_fixture@127.0.0.1:$port/shardline"
touch "$fixture/empty.toml"
"$released" --config "$fixture/empty.toml" db migrate up --database-url "$DATABASE_URL" \
    > "$fixture/released-bootstrap.log" 2>&1
"$current" --config "$fixture/empty.toml" db migrate up --database-url "$DATABASE_URL" \
    > "$fixture/owner-upgrade.log" 2>&1
test_name=maintenance::shared_inline::tests::postgres_phases::postgres_receipt_faults_preserve_each_durable_boundary
CARGO_BUILD_JOBS=2 cargo test --locked -p shardline --lib "$test_name" \
    -- --exact --nocapture > "$fixture/test.log" 2>&1
if rg -ni 'SKIPPED:|skipping:|panicked at' "$fixture/test.log"; then
    echo 'FAIL: receipt phase skipped or background task panicked' >&2
    exit 1
fi
rg -Fx "test $test_name ... ok" "$fixture/test.log"
rg -F 'test result: ok. 1 passed; 0 failed; 0 ignored;' "$fixture/test.log"
echo 'PASS: PostgreSQL admission, report and completion faults retain unfinished evidence'
