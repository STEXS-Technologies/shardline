#!/usr/bin/env bash
# Local history capacity and legacy cleanup; pass an optimized server lib harness.
# Usage: bash scripts/shardline/verify_maintenance_history_resources.sh SERVER_LIB_TEST_HARNESS
set -euo pipefail
test "$#" -eq 1 || { echo 'usage: verify_maintenance_history_resources.sh SERVER_LIB_TEST_HARNESS' >&2; exit 2; }
harness=$(realpath "$1")
test -x "$harness"
for tool in rg mktemp; do command -v "$tool" >/dev/null; done
umask 077
fixture=$(mktemp -d "${TMPDIR:-/tmp}/shardline-history-resources.XXXXXX")
echo "diagnostics: $fixture"
"$harness" --list > "$fixture/test-list.log"
tests=(
    task_tracking::tests::incremental_capacity::full_incremental_metadata_capacity_preserves_unresolved_evidence
    task_tracking::tests::legacy_retention_capacity::many_legacy_runs_paginate_and_retire_without_losing_unresolved_history
    task_tracking::tests::legacy_retention_capacity::large_legacy_run_retires_incrementally_without_touching_unresolved_evidence
)
for selected in "${tests[@]}"; do
    rg -Fx "$selected: test" "$fixture/test-list.log" >/dev/null
    result="$fixture/${selected##*::}.log"
    "$harness" "$selected" --ignored --exact --nocapture > "$result" 2>&1
    if rg -ni 'SKIPPED:|skipping:|panicked at' "$result"; then exit 1; fi
    rg -F 'test result: ok. 1 passed; 0 failed; 0 ignored;' "$result"
done
rg -F '4096-run incremental SQLite history:' "$fixture/${tests[0]##*::}.log"
rg -F 'legacy archive pagination:' "$fixture/${tests[1]##*::}.log"
rg -F 'legacy archive cleanup:' "$fixture/${tests[1]##*::}.log"
rg -F '10000-artifact legacy retirement:' "$fixture/${tests[2]##*::}.log"
echo 'PASS: full SQLite admission and incremental legacy cleanup preserve unresolved evidence and user payloads'
