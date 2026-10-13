#!/usr/bin/env bash
# Compare public embedded uploads against released PostgreSQL behavior.
# Usage: bash scripts/shardline/verify_maintenance_postgres_benchmark.sh CURRENT RELEASED
set -euo pipefail
test "$#" = 2 || { echo 'usage: verify_maintenance_postgres_benchmark.sh CURRENT RELEASED' >&2; exit 2; }
current=$(realpath "$1")
released=$(realpath "$2")
test -x "$current" && test -x "$released"
for tool in docker jq sha256sum cmp timeout; do command -v "$tool" >/dev/null; done
test -x /usr/bin/time
umask 077
fixture=$(mktemp -d "${TMPDIR:-/tmp}/shardline-postgres-benchmark.XXXXXX")
echo "diagnostics: $fixture"
container_id=''
cleanup() {
    if test -n "$container_id"; then
        docker rm -f "$container_id" > "$fixture/cleanup.log" 2>&1 || true
    fi
}
trap cleanup EXIT
trap 'printf "FAIL: PostgreSQL benchmark at line %s; diagnostics: %s\n" "$LINENO" "$fixture" >&2' ERR
container_id=$(docker run -d --rm -e POSTGRES_PASSWORD=owned_benchmark_fixture \
    -p 127.0.0.1::5432 postgres:16-alpine)
for attempt in $(seq 1 60); do
    if docker exec "$container_id" pg_isready -h 127.0.0.1 -U postgres >/dev/null 2>&1; then break; fi
    sleep 1
done
docker exec "$container_id" pg_isready -h 127.0.0.1 -U postgres > "$fixture/ready.log" 2>&1
port=$(docker inspect --format '{{(index (index .NetworkSettings.Ports "5432/tcp") 0).HostPort}}' "$container_id")
touch "$fixture/empty.toml"
sha256sum "$current" "$released" > "$fixture/binaries.sha256"
for concurrency in 1 32 128; do
    for version in released current; do
        case "$version" in released) binary="$released";; current) binary="$current";; esac
        database="${version}_${concurrency}"
        docker exec "$container_id" createdb -h 127.0.0.1 -U postgres "$database"
        database_url="postgres://postgres:owned_benchmark_fixture@127.0.0.1:$port/$database"
        "$released" --config "$fixture/empty.toml" db migrate up --database-url "$database_url" \
            > "$fixture/$database-bootstrap.log" 2>&1
        if test "$version" = current; then
            "$current" --config "$fixture/empty.toml" db migrate up --database-url "$database_url" \
                > "$fixture/$database-upgrade.log" 2>&1
        fi
        /usr/bin/time -f 'wall_seconds=%e\npeak_rss_kib=%M\nexit_code=%x' \
            -o "$fixture/$database.time" timeout 240 env -i \
            RUST_LOG=error \
            SHARDLINE_INDEX_POSTGRES_URL="$database_url" \
            "$binary" --config "$fixture/empty.toml" bench --deployment-target configured \
            --scenario full --storage-dir "$fixture/$database" --iterations 3 \
            --concurrency "$concurrency" --chunk-size-bytes 65536 \
            --base-bytes 1048576 --mutated-bytes 65536 --json \
            > "$fixture/$database.json" 2> "$fixture/$database.stderr"
        jq -e --argjson concurrency "$concurrency" '
            .metadata_backend == "postgres" and .object_backend == "local" and
            .concurrency == $concurrency and .iterations == 3 and
            (.iterations_detail | length) == 3 and .totals.cache_hit_iterations == 3
        ' "$fixture/$database.json" > "$fixture/$database.validated"
        jq -S '
            del(.storage_dir, .latency, .timing, .throughput) |
            .totals |= del(.concurrent_latest_download_scaling_per_mille,
                          .concurrent_upload_scaling_per_mille) |
            .iterations_detail |= map(del(.storage_dir, .latency, .timing))
        ' "$fixture/$database.json" > "$fixture/$database.stable.json"
    done
    cmp "$fixture/released_$concurrency.stable.json" "$fixture/current_$concurrency.stable.json"
    echo "PASS: configured PostgreSQL workload counters at concurrency $concurrency"
done
echo "PASS: configured PostgreSQL 1/32/128 workload compatibility; timings are fixture-specific"
