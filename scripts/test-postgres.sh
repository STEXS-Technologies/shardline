#!/usr/bin/env bash
# Uses the explicitly configured development/CI database, with its schema applied.
set -euo pipefail
: "${DATABASE_URL:?Set DATABASE_URL to a disposable, migrated PostgreSQL database}"
export SHARDLINE_REQUIRE_POSTGRES_TESTS=1

# Recovery compatibility has two distinct contracts: the adjacent release may
# recover known identities, while the older release must preserve unknown ones.
# Supply both binaries so the full-library gate cannot silently omit either.
umask 077
compatibility_dir=$(mktemp -d)
trap 'rm -rf -- "$compatibility_dir"' EXIT
if [[ -z "${SHARDLINE_N_MINUS_ONE_BINARY:-}" ]]; then
    SHARDLINE_N_MINUS_ONE_BINARY=$(SHARDLINE_COMPATIBILITY_BASELINE_VERSION=1.12.1 \
        bash scripts/shardline/fetch_compatibility_baseline.sh "$compatibility_dir/adjacent")
fi
if [[ -z "${SHARDLINE_LEGACY_RECOVERY_BINARY:-}" ]]; then
    SHARDLINE_LEGACY_RECOVERY_BINARY=$(SHARDLINE_COMPATIBILITY_BASELINE_VERSION=1.11.3 \
        bash scripts/shardline/fetch_compatibility_baseline.sh "$compatibility_dir/historical")
fi
test -x "$SHARDLINE_N_MINUS_ONE_BINARY"
test -x "$SHARDLINE_LEGACY_RECOVERY_BINARY"
export SHARDLINE_N_MINUS_ONE_BINARY SHARDLINE_LEGACY_RECOVERY_BINARY

# Full library targets keep newly added modules from silently falling outside a
# name filter. nextest's postgres-coordination group isolates the shared barrier.
cargo nextest run --locked -p shardline-index --lib --all-features --no-fail-fast
cargo nextest run --locked -p shardline-provider-events --all-features \
    --test provider_events_integration -E 'test(postgres_)' --no-fail-fast
cargo nextest run --locked -p shardline-server --lib --all-features --no-fail-fast
# These contracts intentionally require DATABASE_URL and are ignored in an
# unconfigured developer run. They use local object storage, not MinIO.
cargo nextest run --locked -p shardline-server --lib --all-features \
    --run-ignored only \
    -E '(test(copy_integrity::postgres_) & !test(postgres_minio_)) | test(file_hint_observes_postgres_authoritative_replacement_and_revocation)' --no-fail-fast
# Native frontend contracts require SDKs and remote-storage fixtures; the E2E
# workflow runs them explicitly through verify-protocol-conformance.sh.
cargo nextest run --locked -p shardline --all-features --test db_migrate_e2e --no-fail-fast
