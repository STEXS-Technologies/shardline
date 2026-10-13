#!/usr/bin/env bash
# Validate configured CLI inspection against an explicitly pinned executable.
# TEST_BINARY is the compiled pending_deletes_e2e libtest executable.
set -euo pipefail
test "$#" = 2 || { echo 'usage: verify_maintenance_inspection.sh CURRENT TEST_BINARY' >&2; exit 2; }
current=$(realpath "$1")
test_binary=$(realpath "$2")
test -x "$current" && test -x "$test_binary"
command -v docker >/dev/null
umask 077
fixture=$(mktemp -d "${TMPDIR:-/tmp}/shardline-inspection.XXXXXX")
echo "diagnostics: $fixture"
cp "$current" "$fixture/shardline"
sha256sum "$fixture/shardline" > "$fixture/binary.sha256"
container_id=''
cleanup() {
    if test -n "$container_id"; then
        docker rm -f "$container_id" > "$fixture/cleanup.log" 2>&1 || true
    fi
}
trap cleanup EXIT
container_id=$(docker run -d --rm \
    -e POSTGRES_PASSWORD=owned_inspection_fixture -e POSTGRES_DB=shardline \
    -p 127.0.0.1::5432 "${SHARDLINE_TEST_POSTGRES_IMAGE:-postgres:16-alpine}")
for attempt in $(seq 1 60); do
    if docker exec "$container_id" pg_isready -h 127.0.0.1 -U postgres -d shardline \
        > "$fixture/ready.log" 2>&1; then break; fi
    sleep 1
done
docker exec "$container_id" pg_isready -h 127.0.0.1 -U postgres -d shardline \
    > "$fixture/ready.log" 2>&1
port=$(docker inspect --format '{{(index (index .NetworkSettings.Ports "5432/tcp") 0).HostPort}}' "$container_id")
database_url="postgres://postgres:owned_inspection_fixture@127.0.0.1:$port/shardline"
(
    cd "$fixture"
    env -i "$fixture/shardline" db migrate up --database-url "$database_url"
) > "$fixture/bootstrap.log" 2>&1
env -i DATABASE_URL="$database_url" CARGO_BIN_EXE_shardline="$fixture/shardline" \
    "$test_binary" --nocapture > "$fixture/test.log" 2>&1
if rg -ni 'SKIPPED:|skipping:|panicked at|0 passed;' "$fixture/test.log"; then exit 1; fi
rg -F 'configured_pending_delete_cli_paginates_without_changing_attempts ... ok' "$fixture/test.log"
rg -F 'unavailable_postgres_inspection_gives_recovery_guidance_without_starting_work ... ok' "$fixture/test.log"
rg -F 'test result: ok. 4 passed; 0 failed; 0 ignored;' "$fixture/test.log"
sha256sum -c "$fixture/binary.sha256"
echo 'PASS: pinned CLI inspection, pagination, unavailable evidence and connection failure guidance'
