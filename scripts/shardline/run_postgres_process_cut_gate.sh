#!/usr/bin/env bash
# Build and execute the same owned PostgreSQL cut gate in PR and release CI.
# Usage: run_postgres_process_cut_gate.sh [OUTPUT_DIRECTORY]
set -euo pipefail
test "$#" -le 1 || { echo 'usage: run_postgres_process_cut_gate.sh [OUTPUT_DIRECTORY]' >&2; exit 2; }
test "$(uname -s)" = Linux && test "$(uname -m)" = x86_64
for tool in cargo curl docker jq sha256sum tar rg; do command -v "$tool" >/dev/null; done
docker info >/dev/null
umask 077
output=${1:-postgres-process-cut-results}
mkdir -p "$output"
output=$(realpath "$output")

cargo build --locked --all-features -p shardline --message-format=json \
    > "$output/current-build.json" 2> "$output/current-build.log"
current=$(jq -r 'select(.reason=="compiler-artifact" and .target.name=="shardline" and ((.target.kind | index("bin")) != null)) | .executable // empty' "$output/current-build.json")
test -x "$current"
cargo test --locked --all-features -p shardline --test maintenance_e2e \
    --no-run --message-format=json > "$output/harness-build.json" 2> "$output/harness-build.log"
harness=$(jq -r 'select(.reason=="compiler-artifact" and .target.name=="maintenance_e2e" and .profile.test) | .executable // empty' "$output/harness-build.json")
test -x "$harness"

# Pin both the release and its published archive bytes. A later asset replacement
# cannot silently change the baseline used to prove migration compatibility.
released=$(bash scripts/shardline/fetch_compatibility_baseline.sh "$output")
sha256sum "$current" "$released" "$harness" > "$output/binaries.sha256"
TMPDIR="$output" bash scripts/shardline/verify_maintenance_postgres_process_cuts.sh \
    "$current" "$released" "$harness" 2>&1 | tee "$output/verifier.log"
