#!/usr/bin/env bash
# Full shared-history capacity on an owned disposable PostgreSQL database.
# Usage: bash scripts/shardline/verify_maintenance_postgres_history_capacity.sh CURRENT
set -euo pipefail
test "$#" -eq 1 || { echo 'usage: verify_maintenance_postgres_history_capacity.sh CURRENT' >&2; exit 2; }
current=$(realpath "$1")
test -x "$current"
for tool in docker cargo rg; do command -v "$tool" >/dev/null; done
umask 077
fixture=$(mktemp -d "${TMPDIR:-/tmp}/shardline-postgres-capacity.XXXXXX")
echo "diagnostics: $fixture"
container_id=''
cleanup() {
    if test -n "$container_id"; then docker rm -f "$container_id" > "$fixture/cleanup.log" 2>&1 || true; fi
}
trap cleanup EXIT
container_id=$(docker run -d --rm -e POSTGRES_PASSWORD=owned_capacity_fixture \
    -e POSTGRES_DB=shardline -p 127.0.0.1::5432 postgres:16-alpine)
for attempt in $(seq 1 60); do
    if docker exec "$container_id" pg_isready -h 127.0.0.1 -U postgres -d shardline > "$fixture/ready.log" 2>&1; then break; fi
    sleep 1
done
docker exec "$container_id" pg_isready -h 127.0.0.1 -U postgres -d shardline > "$fixture/ready.log" 2>&1
port=$(docker inspect --format '{{(index (index .NetworkSettings.Ports "5432/tcp") 0).HostPort}}' "$container_id")
export DATABASE_URL="postgres://postgres:owned_capacity_fixture@127.0.0.1:$port/shardline"
"$current" db migrate up --database-url "$DATABASE_URL" > "$fixture/bootstrap.log" 2>&1
export CARGO_BUILD_JOBS=${CARGO_BUILD_JOBS:-2}
cargo test --release --locked -p shardline-server --all-features --lib \
    task_tracking::tests::postgres_capacity::full_postgres_history_capacity_preserves_unresolved_evidence \
    -- --ignored --exact --nocapture > "$fixture/test.log" 2>&1
if rg -ni 'SKIPPED:|skipping:|panicked at' "$fixture/test.log"; then exit 1; fi
rg -F 'test result: ok. 1 passed; 0 failed; 0 ignored;' "$fixture/test.log"
rg -F '4096-run PostgreSQL history:' "$fixture/test.log"
echo 'PASS: full PostgreSQL history admission, pagination, cleanup and preserved unresolved evidence'
