#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "$0")/.." && pwd)"
output_root="${1:-$repo_root/benchmark-results}"
concurrency_levels="${SHARDLINE_BENCH_MATRIX_CONCURRENCY:-1 32 128}"
iterations="${SHARDLINE_BENCH_MATRIX_ITERATIONS:-3}"
base_bytes="${SHARDLINE_BENCH_MATRIX_BASE_BYTES:-1048576}"
mutated_bytes="${SHARDLINE_BENCH_MATRIX_MUTATED_BYTES:-65536}"
chunk_size_bytes="${SHARDLINE_BENCH_MATRIX_CHUNK_SIZE_BYTES:-65536}"
deployment_target="${SHARDLINE_BENCH_MATRIX_TARGET:-isolated-local}"
timestamp="$(date -u +%Y%m%dT%H%M%SZ)"
commit="$(git -C "$repo_root" rev-parse --short=12 HEAD)"
run_dir="$output_root/run-${timestamp}-${commit}"
dirty=true
if [[ -z "$(git -C "$repo_root" status --porcelain)" ]]; then
  dirty=false
fi

if [[ -e "$run_dir" ]]; then
  echo "refusing to overwrite benchmark run: $run_dir" >&2
  exit 1
fi
mkdir -p "$run_dir/results" "$run_dir/storage"

cd "$repo_root"
cargo build --locked --release --bin shardline

python3 - "$run_dir/metadata.json" "$commit" "$dirty" "$timestamp" "$deployment_target" \
  "$concurrency_levels" "$iterations" "$base_bytes" "$mutated_bytes" \
  "$chunk_size_bytes" <<'PY'
import json
import os
import platform
import subprocess
import sys

path, commit, dirty, timestamp, target, concurrency, iterations, base, mutated, chunk = sys.argv[1:]
metadata = {
    "schema_version": 1,
    "commit": commit,
    "dirty": dirty == "true",
    "timestamp_utc": timestamp,
    "deployment_target": target,
    "concurrency_levels": [int(value) for value in concurrency.split()],
    "iterations": int(iterations),
    "base_bytes": int(base),
    "mutated_bytes": int(mutated),
    "chunk_size_bytes": int(chunk),
    "platform": platform.platform(),
    "machine": platform.machine(),
    "logical_cpus": os.cpu_count(),
    "rustc": subprocess.check_output(["rustc", "--version"], text=True).strip(),
}
with open(path, "w", encoding="utf-8") as output:
    json.dump(metadata, output, indent=2, sort_keys=True)
    output.write("\n")
PY

for concurrency in $concurrency_levels; do
  result="$run_dir/results/concurrency-${concurrency}.json"
  timing="$run_dir/results/concurrency-${concurrency}.time.txt"
  command=(
    ./target/release/shardline bench
    --deployment-target "$deployment_target"
    --scenario full
    --storage-dir "$run_dir/storage/concurrency-${concurrency}"
    --iterations "$iterations"
    --concurrency "$concurrency"
    --chunk-size-bytes "$chunk_size_bytes"
    --base-bytes "$base_bytes"
    --mutated-bytes "$mutated_bytes"
    --json
  )
  if [[ -x /usr/bin/time ]]; then
    /usr/bin/time -v -o "$timing" "${command[@]}" >"$result"
  else
    "${command[@]}" >"$result"
  fi
done

python3 scripts/benchmark-summary.py "$run_dir" >"$run_dir/summary.md"
echo "$run_dir"
