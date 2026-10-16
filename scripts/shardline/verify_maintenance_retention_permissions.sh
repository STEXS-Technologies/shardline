#!/usr/bin/env bash
# Compare an existing retention-only Postgres role across an incremental upgrade.
# Usage: bash scripts/shardline/verify_maintenance_retention_permissions.sh CURRENT RELEASED
set -euo pipefail

test "$#" = 2 || { echo 'usage: verify_maintenance_retention_permissions.sh CURRENT RELEASED' >&2; exit 2; }
current_binary=$(realpath "$1")
released_binary=$(realpath "$2")
test -x "$current_binary"
test -x "$released_binary"
command -v docker >/dev/null

fixture=$(mktemp -d "${TMPDIR:-/tmp}/shardline-retention-permissions.XXXXXX")
echo "diagnostics: $fixture"
container_name=$(basename "$fixture" | tr '[:upper:].' '[:lower:]-')
container_id=''
cleanup() {
    if test -n "$container_id"; then
        docker rm -f "$container_id" > "$fixture/cleanup.log" 2>&1 || true
    fi
}
trap cleanup EXIT

container_id=$(docker run -d --name "$container_name" \
    -e POSTGRES_PASSWORD=owned_fixture_password -e POSTGRES_DB=shardline \
    -p 127.0.0.1::5432 postgres:16-alpine)
printf '%s\n' "$container_id" > "$fixture/container-id"
for attempt in $(seq 1 60); do
    if docker exec "$container_id" pg_isready -h 127.0.0.1 -U postgres -d shardline > "$fixture/ready.log" 2>&1; then
        break
    fi
    sleep 1
done
docker exec "$container_id" pg_isready -h 127.0.0.1 -U postgres -d shardline > "$fixture/ready.log" 2>&1
port=$(docker port "$container_id" 5432/tcp | cut -d : -f 2)
owner_url="postgres://postgres:owned_fixture_password@127.0.0.1:$port/shardline"
writer_url="postgres://legacy_hold_writer:owned_fixture_password@127.0.0.1:$port/shardline"
"$released_binary" db migrate up --database-url "$owner_url" > "$fixture/migrate.log" 2>&1

# This role can already maintain canonical retention evidence. It has no fence
# table grant, schema ownership, CREATE privileges, or permission to create roles.
docker exec -i "$container_id" psql -U postgres -d shardline -v ON_ERROR_STOP=1 > "$fixture/grants.log" <<'SQL'
CREATE ROLE legacy_hold_writer LOGIN PASSWORD 'owned_fixture_password';
GRANT CONNECT ON DATABASE shardline TO legacy_hold_writer;
GRANT USAGE ON SCHEMA public TO legacy_hold_writer;
GRANT SELECT, INSERT, UPDATE, DELETE ON shardline_retention_holds TO legacy_hold_writer;
GRANT SELECT ON shardline_schema_migrations TO legacy_hold_writer;
DO $$
DECLARE table_name text;
BEGIN
    FOR table_name IN
        SELECT tablename FROM pg_tables
        WHERE schemaname = 'public' AND tablename LIKE 'shardline_reliability_%'
    LOOP
        EXECUTE format('GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE public.%I TO legacy_hold_writer', table_name);
    END LOOP;
END $$;
SQL

writer_grants() {
    docker exec "$container_id" psql -U postgres -d shardline -At -c \
        "SELECT table_schema, table_name, privilege_type, is_grantable
         FROM information_schema.table_privileges
         WHERE grantee = 'legacy_hold_writer'
         ORDER BY table_schema, table_name, privilege_type, is_grantable"
}
writer_grants > "$fixture/writer-grants-before.log"

mkdir "$fixture/root"
SHARDLINE_INDEX_POSTGRES_URL="$writer_url" "$released_binary" hold set \
    --root "$fixture/root" --object-key retention/permission-regression \
    --reason 'existing retention role' > "$fixture/released.log" 2>&1

if ! SHARDLINE_INDEX_POSTGRES_URL="$writer_url" "$current_binary" hold set \
    --root "$fixture/root" --object-key retention/permission-regression \
    --reason 'existing retention role' > "$fixture/current.log" 2>&1; then
    echo "FAIL: released retention credentials no longer work; diagnostics: $fixture" >&2
    cat "$fixture/current.log" >&2
    exit 1
fi

# The usual owner-run upgrade installs the guard without changing writer grants
# or the released migration catalog. Both binaries must still accept that schema.
"$current_binary" db migrate up --database-url "$owner_url" > "$fixture/upgrade.log" 2>&1
writer_grants > "$fixture/writer-grants-after.log"
cmp "$fixture/writer-grants-before.log" "$fixture/writer-grants-after.log"
test "$(docker exec "$container_id" psql -U postgres -d shardline -At -c \
    "SELECT has_table_privilege('legacy_hold_writer', 'public.shardline_resource_fences', 'SELECT,INSERT,UPDATE,DELETE'),
            has_table_privilege('legacy_hold_writer', 'public.shardline_task_artifacts_v1', 'SELECT,INSERT,UPDATE,DELETE')")" = 'f|f'
"$released_binary" db migrate verify --database-url "$owner_url" > "$fixture/legacy-verify.log" 2>&1
for binary in "$released_binary" "$current_binary"; do
    SHARDLINE_INDEX_POSTGRES_URL="$writer_url" "$binary" hold set \
        --root "$fixture/root" --object-key retention/permission-regression \
        --reason 'existing retention role' >> "$fixture/upgraded-writers.log" 2>&1
done

# Fixture-only marker: no remote request is sent. Exercise the old writer's
# trigger path and the new writer's typed error using the same restricted role.
docker exec -i "$container_id" psql -U postgres -d shardline -v ON_ERROR_STOP=1 > "$fixture/pending.log" <<'SQL'
INSERT INTO shardline_resource_fences(domain, resource, epoch)
VALUES ('maintenance-remote-delete-sequence', 'global', 1),
       ('maintenance-remote-delete', 'retention/uncertain-delete', 1);
SQL
for label in released current; do
    binary="$released_binary"
    if test "$label" = current; then binary="$current_binary"; fi
    if SHARDLINE_INDEX_POSTGRES_URL="$writer_url" "$binary" hold set \
        --root "$fixture/root" --object-key retention/uncertain-delete \
        --reason 'must not be acknowledged' > "$fixture/$label-pending.log" 2>&1; then
        echo "FAIL: $label writer acknowledged a hold during an unresolved delete" >&2
        exit 1
    fi
done
rg -q 'unresolved remote delete; retry after reconciliation' "$fixture/current-pending.log"
test "$(docker exec "$container_id" psql -U postgres -d shardline -Atc \
    "SELECT count(*) FROM shardline_retention_holds WHERE object_key='retention/uncertain-delete'")" = 0

# This marker has no external effect to reconcile, so removing it is safe in
# this disposable fixture. Both writers must work again without a new grant.
docker exec "$container_id" psql -U postgres -d shardline -v ON_ERROR_STOP=1 -c \
    "DELETE FROM shardline_resource_fences WHERE domain='maintenance-remote-delete' AND resource='retention/uncertain-delete'" \
    > "$fixture/settled.log" 2>&1
for binary in "$released_binary" "$current_binary"; do
    SHARDLINE_INDEX_POSTGRES_URL="$writer_url" "$binary" hold set \
        --root "$fixture/root" --object-key retention/uncertain-delete \
        --reason 'settled fixture delete' >> "$fixture/settled-writers.log" 2>&1
done
echo "PASS: old and new retention writers preserve permissions and reject unresolved deletes; diagnostics: $fixture"
