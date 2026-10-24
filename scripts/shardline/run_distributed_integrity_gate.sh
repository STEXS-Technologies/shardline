#!/usr/bin/env bash
# Run real deployment faults with pinned old/new binaries; reject runtime skips.
set -euo pipefail
test "$#" -le 1 || { echo 'usage: run_distributed_integrity_gate.sh [OUTPUT_DIRECTORY]' >&2; exit 2; }
: "${SHARDLINE_FAKETIME_LIBRARY:?Set SHARDLINE_FAKETIME_LIBRARY to libfaketime.so.1 for the clock-skew drill}"
test -r "$SHARDLINE_FAKETIME_LIBRARY"
for tool in cargo curl docker jq sha256sum tar rg; do
    command -v "$tool" >/dev/null || { echo "Missing required verification tool: $tool" >&2; exit 1; }
done
docker info >/dev/null
# These logs are checked for exact test and skip counts below. CI may force ANSI
# colors globally, which would split the numbers from the surrounding text.
export CARGO_TERM_COLOR=never
export SHARDLINE_REQUIRE_NATIVE_CLIENTS=1
umask 077
output=${1:-distributed-integrity-results}
mkdir -p "$output"
output=$(realpath "$output")

cargo build --locked --all-features -p shardline --message-format=json \
    > "$output/current-build.json" 2> "$output/current-build.log"
current=$(jq -r 'select(.reason=="compiler-artifact" and .target.name=="shardline" and ((.target.kind | index("bin")) != null)) | .executable // empty' "$output/current-build.json")
test -x "$current"
# Freeze the current executable too: concurrent builds must not change a drill.
cp -- "$current" "$output/current-shardline"
# Non-root container drills must be able to execute this non-secret binary.
chmod 0755 "$output/current-shardline"
export CARGO_BIN_EXE_shardline="$output/current-shardline"
export SHARDLINE_CHAOS_DEPLOYMENT=1
export SHARDLINE_N_MINUS_ONE_BINARY
SHARDLINE_N_MINUS_ONE_BINARY=$(bash scripts/shardline/fetch_compatibility_baseline.sh "$output")
sha256sum "$CARGO_BIN_EXE_shardline" "$SHARDLINE_N_MINUS_ONE_BINARY" \
    "$SHARDLINE_FAKETIME_LIBRARY" > "$output/binaries.sha256"

cargo nextest run --locked -p shardline-server --all-features --test deployment_chaos \
    --test-threads 1 --no-fail-fast --success-output immediate --failure-output immediate \
    > "$output/deployment.log" 2>&1
if rg -ni 'SKIP:|SKIPPED:|skipping:|panicked at' "$output/deployment.log"; then
    echo 'FAIL: a deployment drill skipped or a task panicked' >&2
    exit 1
fi
expected_deployment_tests=$(cargo nextest list --locked -p shardline-server --all-features \
    --test deployment_chaos --message-format json \
    | jq -er '[."rust-suites"[]?.testcases[]? | select(.ignored != true)] | length')
rg "Starting ${expected_deployment_tests} tests across 1 binary" "$output/deployment.log"
rg "Summary.*${expected_deployment_tests} tests run: ${expected_deployment_tests} passed" \
    "$output/deployment.log"

cargo nextest run --locked -p shardline-server --all-features --test multinode_chaos \
    --test-threads 1 --no-fail-fast --success-output immediate --failure-output immediate \
    > "$output/multinode.log" 2>&1
if rg -ni 'SKIP:|SKIPPED:|skipping:|panicked at' "$output/multinode.log"; then
    echo 'FAIL: a multi-node drill skipped or a task panicked' >&2
    exit 1
fi
expected_multinode_tests=$(cargo nextest list --locked -p shardline-server --all-features \
    --test multinode_chaos --message-format json \
    | jq -er '[."rust-suites"[]?.testcases[]? | select(.ignored != true)] | length')
rg "Starting ${expected_multinode_tests} tests across 1 binary" "$output/multinode.log"
rg "Summary.*${expected_multinode_tests} tests run: ${expected_multinode_tests} passed" \
    "$output/multinode.log"
echo 'PASS: deployment faults, mixed-version rollback and multi-node role integrity'
