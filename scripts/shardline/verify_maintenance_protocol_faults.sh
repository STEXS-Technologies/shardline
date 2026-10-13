#!/usr/bin/env bash
# Additional deployment faults, using only disposable Compose projects.
# Usage: bash scripts/shardline/verify_maintenance_protocol_faults.sh CURRENT FAKETIME_LIBRARY
set -euo pipefail
test "$#" -eq 2 || { echo 'usage: verify_maintenance_protocol_faults.sh CURRENT FAKETIME_LIBRARY' >&2; exit 2; }
current=$(realpath "$1")
faketime=$(realpath "$2")
test -x "$current"
test -r "$faketime"
for tool in docker cargo rg; do command -v "$tool" >/dev/null; done
cargo nextest --version >/dev/null
umask 077
fixture=$(mktemp -d "${TMPDIR:-/tmp}/shardline-protocol-faults.XXXXXX")
echo "diagnostics: $fixture"
cp -- "$current" "$fixture/current-shardline"
cp -- "$faketime" "$fixture/libfaketime.so.1"
export CARGO_BIN_EXE_shardline="$fixture/current-shardline"
export SHARDLINE_FAKETIME_LIBRARY="$fixture/libfaketime.so.1"
export SHARDLINE_CHAOS_DEPLOYMENT=1
export CARGO_BUILD_JOBS=2
cargo nextest run --locked -p shardline-server --all-features --test deployment_chaos \
    -E 'not (test(readonly_maintenance_deployment::) | test(tracker_replacement::) | test(gc_acknowledgement::) | test(drill_deploy_f_real_mixed_version_rollout_and_rollback))' \
    --test-threads 1 --no-fail-fast --success-output immediate --failure-output immediate \
    > "$fixture/test.log" 2>&1
if rg -ni 'SKIP:|SKIPPED:|skipping:|panicked at' "$fixture/test.log"; then
    echo 'FAIL: a selected protocol drill skipped or a task panicked' >&2
    exit 1
fi
rg 'Starting 12 tests across 1 binary' "$fixture/test.log"
rg 'Summary.*12 tests run: 12 passed' "$fixture/test.log"
echo 'PASS: protocol interruption, backend loss, network faults, clock skew and workload burst'
