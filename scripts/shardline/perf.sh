#!/usr/bin/env bash
set -euo pipefail

mode="${1:-}"
if [[ -z "${mode}" ]]; then
  echo "usage: scripts/shardline/perf.sh <e2e|perf-e2e>" >&2
  exit 2
fi
shift

if [[ "${1:-}" == "--" ]]; then
  shift
fi

passthrough_args=()
while (($# > 0)); do
  case "$1" in
    --mode)
      [[ $# -ge 2 ]] || {
        echo "missing value for --mode" >&2
        exit 2
      }
      bench_mode="$2"
      shift 2
      ;;
    --scenario)
      [[ $# -ge 2 ]] || {
        echo "missing value for --scenario" >&2
        exit 2
      }
      scenario="$2"
      shift 2
      ;;
    --storage-dir)
      [[ $# -ge 2 ]] || {
        echo "missing value for --storage-dir" >&2
        exit 2
      }
      storage_dir="$2"
      shift 2
      ;;
    --iterations)
      [[ $# -ge 2 ]] || {
        echo "missing value for --iterations" >&2
        exit 2
      }
      iterations="$2"
      shift 2
      ;;
    --concurrency)
      [[ $# -ge 2 ]] || {
        echo "missing value for --concurrency" >&2
        exit 2
      }
      concurrency="$2"
      shift 2
      ;;
    --upload-max-in-flight-chunks)
      [[ $# -ge 2 ]] || {
        echo "missing value for --upload-max-in-flight-chunks" >&2
        exit 2
      }
      upload_max_in_flight_chunks="$2"
      shift 2
      ;;
    --chunk-size-bytes)
      [[ $# -ge 2 ]] || {
        echo "missing value for --chunk-size-bytes" >&2
        exit 2
      }
      chunk_size_bytes="$2"
      shift 2
      ;;
    --base-bytes)
      [[ $# -ge 2 ]] || {
        echo "missing value for --base-bytes" >&2
        exit 2
      }
      base_bytes="$2"
      shift 2
      ;;
    --mutated-bytes)
      [[ $# -ge 2 ]] || {
        echo "missing value for --mutated-bytes" >&2
        exit 2
      }
      mutated_bytes="$2"
      shift 2
      ;;
    --json)
      bench_json=1
      shift
      ;;
    --)
      shift
      ;;
    *)
      passthrough_args+=("$1")
      shift
      ;;
  esac
done

default_concurrency() {
  getconf _NPROCESSORS_ONLN 2>/dev/null || nproc 2>/dev/null || printf '8\n'
}

storage_dir="${storage_dir:-${SHARDLINE_BENCH_STORAGE_DIR:-/tmp/shardline-bench}}"
iterations="${iterations:-${SHARDLINE_BENCH_ITERATIONS:-10}}"
concurrency="${concurrency:-${SHARDLINE_BENCH_CONCURRENCY:-$(default_concurrency)}}"
upload_max_in_flight_chunks="${upload_max_in_flight_chunks:-${SHARDLINE_BENCH_UPLOAD_MAX_IN_FLIGHT_CHUNKS:-64}}"
chunk_size_bytes="${chunk_size_bytes:-${SHARDLINE_BENCH_CHUNK_SIZE_BYTES:-65536}}"
base_bytes="${base_bytes:-${SHARDLINE_BENCH_BASE_BYTES:-1048576}}"
mutated_bytes="${mutated_bytes:-${SHARDLINE_BENCH_MUTATED_BYTES:-4096}}"
scenario="${scenario:-${SHARDLINE_BENCH_SCENARIO:-full}}"
bench_mode="${bench_mode:-${SHARDLINE_BENCH_MODE:-e2e}}"

bench_args=(
  bench
  --mode "${bench_mode}"
  --scenario "${scenario}"
  --storage-dir "${storage_dir}"
  --iterations "${iterations}"
  --concurrency "${concurrency}"
  --upload-max-in-flight-chunks "${upload_max_in_flight_chunks}"
  --chunk-size-bytes "${chunk_size_bytes}"
  --base-bytes "${base_bytes}"
  --mutated-bytes "${mutated_bytes}"
)

if [[ "${bench_json:-0}" == "1" ]]; then
  bench_args+=(--json)
fi

case "${mode}" in
  e2e)
    cargo build --locked --bin shardline
    exec ./target/debug/shardline "${bench_args[@]}" "${passthrough_args[@]}"
    ;;
  perf-e2e)
    perf_output="${SHARDLINE_PERF_OUTPUT:-/tmp/shardline-e2e.perf.data}"
    perf_frequency="${SHARDLINE_PERF_FREQUENCY:-999}"
    cargo build --locked --profile profiling --bin shardline
    exec perf record \
      -F "${perf_frequency}" \
      -g \
      --call-graph dwarf \
      -o "${perf_output}" \
      ./target/profiling/shardline \
      "${bench_args[@]}" \
      "${passthrough_args[@]}"
    ;;
  *)
    echo "unknown mode: ${mode}" >&2
    echo "expected one of: e2e, perf-e2e" >&2
    exit 2
    ;;
esac
