#!/usr/bin/env bash
# Native Xet client with disposable PostgreSQL metadata and MinIO payloads.
# Usage: bash scripts/shardline/verify_maintenance_native_xet.sh
set -euo pipefail
test "$#" -eq 0 || { echo 'usage: verify_maintenance_native_xet.sh' >&2; exit 2; }
for tool in docker cargo rg; do command -v "$tool" >/dev/null; done
docker version >/dev/null
umask 077
fixture=$(mktemp -d "${TMPDIR:-/tmp}/shardline-native-xet.XXXXXX")
echo "diagnostics: $fixture"
export CARGO_BUILD_JOBS=${CARGO_BUILD_JOBS:-2}
cargo test --manifest-path e2e/Cargo.toml --locked --test native_xet_client_e2e \
    shardline_accepts_native_xet_flows_with_postgres_and_s3 \
    -- --ignored --exact --nocapture > "$fixture/test.log" 2>&1
if rg -ni 'skipping:|SKIPPED:' "$fixture/test.log"; then exit 1; fi
rg -F 'test result: ok. 1 passed; 0 failed; 0 ignored;' "$fixture/test.log"
echo 'PASS: native client upload, deduplication, reconstruction and download with owner-upgraded PostgreSQL and MinIO'
