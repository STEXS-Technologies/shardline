#!/usr/bin/env bash
# Measure discovery/owner-upgrade overhead with synthetic published OCI history.
# This is a SQL scan benchmark, not a cloud or end-to-end GC guarantee.
# Usage: bash scripts/shardline/measure_maintenance_oci_inventory.sh CURRENT [HEADS]
set -euo pipefail
test "$#" -ge 1 && test "$#" -le 2 || { echo 'usage: measure_maintenance_oci_inventory.sh CURRENT [HEADS]' >&2; exit 2; }
current=$(realpath "$1")
head_count=${2:-100000}
if ! [[ "$head_count" =~ ^[1-9][0-9]{0,6}$ ]] || ! test "$head_count" -le 1000000; then
    echo 'HEADS must be an integer between 1 and 1000000' >&2
    exit 2
fi
test -x "$current"
for tool in docker sha256sum; do command -v "$tool" >/dev/null; done
umask 077
fixture=$(mktemp -d "${TMPDIR:-/tmp}/shardline-oci-inventory-cost.XXXXXX")
echo "diagnostics: $fixture"
container_id=''
cleanup() {
    if test -n "$container_id"; then docker rm -f "$container_id" > "$fixture/cleanup.log" 2>&1 || true; fi
}
trap cleanup EXIT
trap 'printf "FAIL: inventory cost drill at line %s; diagnostics: %s\n" "$LINENO" "$fixture" >&2' ERR
sha256sum "$current" > "$fixture/binary.sha256"
container_id=$(docker run -d -e POSTGRES_PASSWORD=owned_fixture_password \
    -e POSTGRES_DB=shardline -p 127.0.0.1::5432 postgres:16-alpine)
printf '%s\n' "$container_id" > "$fixture/container-id"
for attempt in $(seq 1 60); do
    if docker exec "$container_id" pg_isready -h 127.0.0.1 -U postgres -d shardline > "$fixture/ready.log" 2>&1; then break; fi
    sleep 1
done
docker exec "$container_id" pg_isready -h 127.0.0.1 -U postgres -d shardline > "$fixture/ready.log" 2>&1
port=$(docker port "$container_id" 5432/tcp | cut -d : -f 2)
database_url="postgres://postgres:owned_fixture_password@127.0.0.1:$port/shardline"
"$current" db migrate up --database-url "$database_url" > "$fixture/migrate.log" 2>&1
docker exec -i "$container_id" psql -U postgres -d shardline -v ON_ERROR_STOP=1 \
    -v fixture_heads="$head_count" > "$fixture/seed.log" <<'SQL'
-- Synthetic rows deliberately omit cryptographic commitments. They are scan
-- load only, never authoritative evidence for physical reclamation. Each older
-- Deleted snapshot has a later Published head; neither may become a candidate.
ALTER TABLE shardline_reliability_events DISABLE TRIGGER shardline_oci_staged_tombstone_v1;
INSERT INTO shardline_reliability_events(operation_kind,operation_id,sequence,event_json,created_at_unix_seconds)
SELECT 'Visibility','global:benchmark/noise:manifest:'||digest,stage,
       jsonb_build_object('after',jsonb_build_object(
           'identity',jsonb_build_object('scope_namespace','global','repository','benchmark/noise',
               'object_kind','manifest','digest_hex',digest),
           'state',CASE stage WHEN 0 THEN 'Deleted' ELSE 'Published' END,
           'deleted_at_unix_seconds',CASE stage WHEN 0 THEN 1 ELSE NULL END)),
       EXTRACT(EPOCH FROM now())::bigint
FROM (SELECT md5(number::text)||md5(number::text) AS digest
      FROM generate_series(1,:fixture_heads::bigint) number) identities
CROSS JOIN generate_series(0,1) stage;
ALTER TABLE shardline_reliability_events ENABLE TRIGGER shardline_oci_staged_tombstone_v1;
ANALYZE shardline_reliability_events;
SQL
docker exec -i "$container_id" psql -U postgres -d shardline -v ON_ERROR_STOP=1 \
    > "$fixture/discovery.log" <<'SQL'
SELECT count(*) AS history_rows FROM shardline_reliability_events;
SELECT pg_size_pretty(pg_total_relation_size('shardline_reliability_events')) AS history_size;
SELECT pg_advisory_lock(x'5348415244474301'::bit(64)::bigint);
\timing on
SELECT shardline_oci_staged_gc_inventory_v1(current_schema(),64) AS warmup;
SELECT shardline_oci_staged_gc_inventory_v1(current_schema(),64) AS trial_1;
SELECT shardline_oci_staged_gc_inventory_v1(current_schema(),64) AS trial_2;
SELECT shardline_oci_staged_gc_inventory_v1(current_schema(),64) AS trial_3;
\timing off
DO $$ BEGIN
    IF EXISTS(SELECT 1 FROM shardline_oci_object_tombstones) THEN
        RAISE EXCEPTION 'published history became an unsafe deletion candidate';
    END IF;
END $$;
SQL
TIMEFORMAT='owner-upgrade seconds: %3R'
for trial in 1 2 3; do
    { time "$current" db migrate up --database-url "$database_url" > "$fixture/upgrade-$trial.log" 2>&1; } \
        2>> "$fixture/owner-upgrade-times.log"
done
test "$(docker exec "$container_id" psql -U postgres -d shardline -Atc 'SELECT count(*) FROM shardline_oci_object_tombstones')" = 0
docker exec -i "$container_id" psql -U postgres -d shardline -v ON_ERROR_STOP=1 \
    > "$fixture/head-plans.log" <<'SQL'
EXPLAIN(ANALYZE,BUFFERS) SELECT head.operation_id
FROM (SELECT DISTINCT ON(operation_id) operation_id,event_json
      FROM shardline_reliability_events WHERE operation_kind='Visibility'
        AND operation_id ~ '^(global|[0-9a-f]{64}):.+:manifest:[0-9a-f]{64}$'
      ORDER BY operation_id,sequence DESC) head
WHERE head.event_json #>> '{after,state}'='Deleted';
EXPLAIN(ANALYZE,BUFFERS) SELECT head.operation_id
FROM (SELECT DISTINCT ON(operation_id) operation_id,event_json
      FROM shardline_reliability_events WHERE operation_kind='Visibility'
        AND operation_id LIKE '%:manifest:%'
      ORDER BY operation_id,sequence DESC) head
WHERE head.event_json #>> '{after,state}'='Deleted';
SQL
cat "$fixture/discovery.log" "$fixture/owner-upgrade-times.log"
echo "PASS: $head_count published heads remain outside the deletion inventory; diagnostics: $fixture"
