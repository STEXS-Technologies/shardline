#!/usr/bin/env bash
# Uses disposable Compose projects; never targets an existing deployment.
# Usage: bash scripts/shardline/verify_maintenance_deployments.sh CURRENT RELEASED
set -euo pipefail
test "$#" -eq 2 || { echo 'usage: verify_maintenance_deployments.sh CURRENT RELEASED' >&2; exit 2; }
current=$(realpath "$1")
released=$(realpath "$2")
test -x "$current"
test -x "$released"
for tool in docker cargo rg; do command -v "$tool" >/dev/null; done
cargo nextest --version >/dev/null
umask 077
fixture=$(mktemp -d "${TMPDIR:-/tmp}/shardline-maintenance-deployments.XXXXXX")
echo "diagnostics: $fixture"
# Keep every drill on the same bytes even if another build replaces its output.
cp -- "$current" "$fixture/current-shardline"
cp -- "$released" "$fixture/released-shardline"
export CARGO_BIN_EXE_shardline="$fixture/current-shardline"
export SHARDLINE_N_MINUS_ONE_BINARY="$fixture/released-shardline"
export CARGO_BUILD_JOBS=2
cargo nextest run --locked -p shardline-server --all-features --test deployment_chaos \
    -E 'test(readonly_maintenance_deployment::) | test(tracker_replacement::) | test(gc_acknowledgement::) | test(drill_deploy_f_real_mixed_version_rollout_and_rollback)' \
    --test-threads 1 --no-fail-fast --success-output immediate --failure-output immediate \
    > "$fixture/test.log" 2>&1
if rg -n 'SKIPPED:' "$fixture/test.log"; then
    echo 'FAIL: a deployment drill skipped at runtime' >&2
    exit 1
fi
# Require every selected drill: a renamed test must not silently shrink coverage.
rg 'Starting 11 tests across 1 binary' "$fixture/test.log"
rg 'Summary.*11 tests run: 11 passed' "$fixture/test.log"
echo 'PASS: startup, restricted commands, concurrent ownership, replacement, remote-delete faults and rolling rollback'
