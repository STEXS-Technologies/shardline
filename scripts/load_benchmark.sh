#!/usr/bin/env bash
set -euo pipefail

BENCH_URL="${BENCH_URL:-http://127.0.0.1:18080}"
BENCH_TOKEN="${BENCH_TOKEN:-}"
BENCH_DURATION="${BENCH_DURATION:-30}"
BENCH_WARMUP="${BENCH_WARMUP:-5}"
BENCH_UPLOAD_SIZE="${BENCH_UPLOAD_SIZE:-4096}"
CONCURRENCY_LEVELS="${CONCURRENCY_LEVELS:-1 5 10 20 50}"
JSON_OUTPUT="${BENCH_JSON:-}"

usage() {
    cat <<'EOF'
Usage: scripts/load_benchmark.sh [OPTIONS]

Runs a concurrent load benchmark against a running Shardline server.

Options:
  --url URL               Server base URL (default: http://127.0.0.1:18080)
  --token TOKEN           Bearer auth token (optional, for authenticated endpoints)
  --duration SECS         Test duration per scenario in seconds (default: 30)
  --warmup SECS           Warmup duration in seconds (default: 5)
  --upload-size BYTES     Upload payload size in bytes (default: 4096)
  --concurrency LEVELS    Space-separated concurrency levels (default: "1 5 10 20 50")
  --json                  Output JSON instead of human-readable tables
  -h, --help              Show this help message

Environment variables:
  BENCH_URL, BENCH_TOKEN, BENCH_DURATION, BENCH_WARMUP, BENCH_UPLOAD_SIZE,
  CONCURRENCY_LEVELS, BENCH_JSON

Examples:
  # Basic benchmark with default settings
  scripts/load_benchmark.sh

  # High-concurrency test with auth
  scripts/load_benchmark.sh --concurrency "10 50 100" --token "my-token" --duration 60

  # JSON output for CI
  BENCH_JSON=1 scripts/load_benchmark.sh --json
EOF
}

while (($# > 0)); do
    case "$1" in
        --url)
            [[ $# -ge 2 ]] || { echo "missing value for --url" >&2; exit 2; }
            BENCH_URL="$2"
            shift 2
            ;;
        --token)
            [[ $# -ge 2 ]] || { echo "missing value for --token" >&2; exit 2; }
            BENCH_TOKEN="$2"
            shift 2
            ;;
        --duration)
            [[ $# -ge 2 ]] || { echo "missing value for --duration" >&2; exit 2; }
            BENCH_DURATION="$2"
            shift 2
            ;;
        --warmup)
            [[ $# -ge 2 ]] || { echo "missing value for --warmup" >&2; exit 2; }
            BENCH_WARMUP="$2"
            shift 2
            ;;
        --upload-size)
            [[ $# -ge 2 ]] || { echo "missing value for --upload-size" >&2; exit 2; }
            BENCH_UPLOAD_SIZE="$2"
            shift 2
            ;;
        --concurrency)
            [[ $# -ge 2 ]] || { echo "missing value for --concurrency" >&2; exit 2; }
            CONCURRENCY_LEVELS="$2"
            shift 2
            ;;
        --json)
            JSON_OUTPUT=1
            shift
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            echo "unknown option: $1" >&2
            usage >&2
            exit 2
            ;;
    esac
done

echo "=== Shardline Load Benchmark ==="
echo "  URL:         ${BENCH_URL}"
echo "  Duration:    ${BENCH_DURATION}s per level"
echo "  Warmup:      ${BENCH_WARMUP}s"
echo "  Upload size: ${BENCH_UPLOAD_SIZE} bytes"
echo "  Levels:      ${CONCURRENCY_LEVELS}"
echo ""

# Build the load test binary
echo "Building load test binary..."
if ! cargo build --bin load_test --release 2>/dev/null; then
    echo "Build failed. Trying debug build..."
    cargo build --bin load_test
fi
BINARY="./target/release/load_test"
if [[ ! -x "${BINARY}" ]]; then
    BINARY="./target/debug/load_test"
fi
if [[ ! -x "${BINARY}" ]]; then
    echo "ERROR: load_test binary not found" >&2
    exit 1
fi

export BENCH_URL
export BENCH_TOKEN
export BENCH_DURATION
export BENCH_WARMUP
export BENCH_UPLOAD_SIZE

if [[ -n "${JSON_OUTPUT}" ]]; then
    export BENCH_JSON=1
    echo "["
    first=1
fi

for level in ${CONCURRENCY_LEVELS}; do
    echo ""
    echo ">>> Concurrency: ${level}"
    echo ""

    export BENCH_CONCURRENCY="${level}"

    if [[ -n "${JSON_OUTPUT}" ]]; then
        output=$("${BINARY}" 2>&1)
        exit_code=$?
        echo "${output}"
        if [[ ${exit_code} -ne 0 ]]; then
            echo "load_test exited with code ${exit_code}" >&2
        fi
    else
        "${BINARY}"
    fi
done

if [[ -n "${JSON_OUTPUT}" ]]; then
    echo "]"
fi

echo ""
echo "=== Benchmark Complete ==="
