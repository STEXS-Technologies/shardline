#!/usr/bin/env bash
# Full automatic SQLite history budget: native evidence, pagination and cleanup.
# This is a local scale fixture, not a deployment latency guarantee.
# Usage: bash scripts/shardline/verify_maintenance_history_capacity.sh [incremental]
set -euo pipefail
test "$#" -le 1 || { echo 'usage: verify_maintenance_history_capacity.sh [incremental]' >&2; exit 2; }
test_name=task_tracking::tests::full_automatic_metadata_capacity_preserves_inspection_and_cleanup
evidence='4096-run metadata fixture: full paginated inspection'
if test "$#" -eq 1; then
    test "$1" = incremental || { echo 'unknown capacity mode' >&2; exit 2; }
    test_name=task_tracking::tests::incremental_capacity::full_incremental_metadata_capacity_preserves_unresolved_evidence
    evidence='4096-run incremental SQLite history:'
fi
for tool in cargo rg; do command -v "$tool" >/dev/null; done
umask 077
fixture=$(mktemp -d "${TMPDIR:-/tmp}/shardline-history-capacity.XXXXXX")
echo "diagnostics: $fixture"
export CARGO_BUILD_JOBS=${CARGO_BUILD_JOBS:-2}
cargo test --locked --release -p shardline-server --all-features --lib \
    "$test_name" \
    -- --ignored --exact --nocapture > "$fixture/test.log" 2>&1
if rg -ni 'skipping:|SKIPPED:|panicked at' "$fixture/test.log"; then exit 1; fi
rg -F "$evidence" "$fixture/test.log"
rg -F 'test result: ok. 1 passed; 0 failed; 0 ignored;' "$fixture/test.log"
echo 'PASS: full automatic history capacity preserves pagination, unfinished/corrupt evidence and user payload bytes'
