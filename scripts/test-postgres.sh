#!/usr/bin/env bash
# Uses the explicitly configured development/CI database, with its schema applied.
set -euo pipefail
: "${DATABASE_URL:?Set DATABASE_URL to a disposable, migrated PostgreSQL database}"

# Full library targets keep newly added modules from silently falling outside a
# name filter. nextest's postgres-coordination group isolates the shared barrier.
cargo nextest run --locked -p shardline-index --lib --all-features --no-fail-fast
cargo nextest run --locked -p shardline-provider-events --all-features \
    --test provider_events_integration -E 'test(postgres_)' --no-fail-fast
cargo nextest run --locked -p shardline-server --lib --all-features --no-fail-fast
cargo test --locked -p shardline-server --test protocol_completion postgres -- --ignored
cargo nextest run --locked -p shardline --all-features --test db_migrate_e2e --no-fail-fast
