#!/usr/bin/env bash
# Full automatic SQLite history budget: native evidence, pagination and cleanup.
# This is a local scale fixture, not a deployment latency guarantee.
# Usage: bash scripts/shardline/verify_maintenance_history_capacity.sh
set -euo pipefail
test "$#" -eq 0 || { echo 'usage: verify_maintenance_history_capacity.sh' >&2; exit 2; }
for tool in cargo rg; do command -v "$tool" >/dev/null; done
umask 077
fixture=$(mktemp -d "${TMPDIR:-/tmp}/shardline-history-capacity.XXXXXX")
echo "diagnostics: $fixture"
export CARGO_BUILD_JOBS=${CARGO_BUILD_JOBS:-2}
cargo test --locked --release -p shardline-server --all-features --lib \
    task_tracking::tests::full_automatic_metadata_capacity_preserves_inspection_and_cleanup \
    -- --ignored --exact --nocapture > "$fixture/test.log" 2>&1
rg -F '4096-run metadata fixture: full paginated inspection' "$fixture/test.log"
rg -F 'test result: ok. 1 passed; 0 failed; 0 ignored;' "$fixture/test.log"
echo 'PASS: full automatic history capacity preserves pagination, oldest-first retirement, unfinished/corrupt evidence and user payload bytes'
