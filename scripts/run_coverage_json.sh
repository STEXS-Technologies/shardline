#!/usr/bin/env bash
set -euo pipefail

coverage_postgres_name="shardline-coverage-postgres-$$"

cleanup_coverage_postgres() {
    docker rm -f "${coverage_postgres_name}" >/dev/null 2>&1 || true
}

trap cleanup_coverage_postgres EXIT

cargo llvm-cov \
    --workspace \
    --all-features \
    --exclude shardline-fuzz \
    --exclude shardline-bench \
    --no-report \
    "$@"

docker run \
    --rm \
    --detach \
    --name "${coverage_postgres_name}" \
    --env POSTGRES_USER=shardline \
    --env POSTGRES_PASSWORD=shardline-dev-password \
    --env POSTGRES_DB=shardline \
    --publish 127.0.0.1::5432 \
    postgres:16-alpine >/dev/null

coverage_postgres_ready=false
for _attempt in $(seq 1 30); do
    if docker exec "${coverage_postgres_name}" \
        psql --username shardline --dbname shardline --command "SELECT 1" >/dev/null 2>&1
    then
        coverage_postgres_ready=true
        break
    fi
    sleep 1
done

if [[ "${coverage_postgres_ready}" != "true" ]]; then
    echo "coverage PostgreSQL instance did not become ready" >&2
    exit 1
fi

coverage_postgres_binding="$(docker port "${coverage_postgres_name}" 5432/tcp)"
coverage_postgres_port="${coverage_postgres_binding##*:}"
coverage_database_url="postgres://shardline:shardline-dev-password@127.0.0.1:${coverage_postgres_port}/shardline"

cargo run -p shardline -- db migrate up --database-url "${coverage_database_url}"

# Reuse the workspace coverage profile and profraw directory for the
# database-gated suites. Running these filters sequentially keeps migration
# and shared-schema tests from interfering with the normal parallel suite.
coverage_target_dir="$(pwd)/target/llvm-cov-target"
eval "$(CARGO_TARGET_DIR="${coverage_target_dir}" cargo llvm-cov show-env --sh)"
export CARGO_TARGET_DIR="${coverage_target_dir}"

DATABASE_URL="${coverage_database_url}" \
    cargo test -p shardline-index --lib -- hub_postgres
DATABASE_URL="${coverage_database_url}" \
    cargo test -p shardline-index --lib -- pg_upload_intent
DATABASE_URL="${coverage_database_url}" \
    cargo test -p shardline-server --lib -- postgres_backend::

cargo llvm-cov report \
    --json \
    --summary-only \
    --output-path target/coverage.json
