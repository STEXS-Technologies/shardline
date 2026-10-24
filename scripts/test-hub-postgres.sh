#!/usr/bin/env bash
# Own a fresh database per target: static fixture names cannot contaminate peers.
set -euo pipefail
export SHARDLINE_REQUIRE_POSTGRES_TESTS=1
container=""
cleanup() {
    local status=$?
    trap - EXIT
    if [[ -n "$container" ]]; then
        if [[ "$status" != 0 ]]; then
            docker inspect --format \
                'Hub PostgreSQL fixture: running={{.State.Running}} exit={{.State.ExitCode}} oom={{.State.OOMKilled}}' \
                "$container" >&2 || true
        fi
        docker rm -f "$container" >/dev/null 2>&1 || true
    fi
    exit "$status"
}
trap cleanup EXIT
: "${SHARDLINE_BIN:=target/debug/shardline}"
[[ -x "$SHARDLINE_BIN" ]] || { echo 'Build the current shardline binary first' >&2; exit 1; }
if [[ "$SHARDLINE_BIN" != /* ]]; then SHARDLINE_BIN="$PWD/$SHARDLINE_BIN"; fi
export SHARDLINE_BIN
container=$(docker run --rm -d -e POSTGRES_PASSWORD=hub-disposable-only \
    -p 127.0.0.1::5432 postgres:16-alpine)
ready=0
for ((attempt=0; attempt<60; attempt++)); do
    # The image's temporary initialization server accepts Unix sockets before
    # restarting. TCP readiness selects the final externally usable server.
    if docker exec "$container" pg_isready -h 127.0.0.1 -U postgres >/dev/null 2>&1; then ready=1; break; fi
    sleep 1
done

[[ "$ready" == 1 ]] || { echo 'Disposable Hub PostgreSQL did not become ready' >&2; exit 1; }
port=$(docker port "$container" 5432/tcp)
port=${port##*:}
for entry in \
    repository_search:SHARDLINE_HUB_SEARCH_TEST_DATABASE_URL \
    hub_tree_paging:SHARDLINE_TREE_PAGE_TEST_DATABASE_URL \
    hub_commit_bounds:SHARDLINE_COMMIT_BOUNDS_TEST_DATABASE_URL \
    hub_tree_hierarchy:SHARDLINE_TREE_HIERARCHY_TEST_DATABASE_URL \
    git_ref_lifecycle:SHARDLINE_GIT_REF_TEST_DATABASE_URL \
    git_head:SHARDLINE_HEAD_TEST_DATABASE_URL \
    webhook_secret_upgrade_race:DATABASE_URL; do
    target=${entry%%:*}
    variable=${entry#*:}
    docker exec "$container" createdb -h 127.0.0.1 -U postgres "$target"
    url="postgres://postgres:hub-disposable-only@127.0.0.1:$port/$target"
    "$SHARDLINE_BIN" db migrate up --database-url "$url"
    export "$variable=$url"
    cargo nextest run --locked -p shardline-hub-api --test "$target" \
        -E 'test(postgres_)' --no-fail-fast
done

# Snapshot authorization must also hold with object payloads on MinIO. This
# case owns both services and exercises the same stock-client contract locally.
cargo nextest run --locked -p shardline-hub-api --test git_suffix \
    --run-ignored only -E 'test(postgres_and_minio_snapshot_tokens_are_confined)'
