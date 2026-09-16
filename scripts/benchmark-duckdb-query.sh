#!/usr/bin/env bash
set -euo pipefail

# Compare the bounded Hub query route with an external DuckDB S3 scan.
# Required: SHARDLINE_URL, SHARDLINE_TOKEN, SHARDLINE_REPOSITORY,
# SHARDLINE_REVISION, SHARDLINE_FILE_SHA, SHARDLINE_S3_URI.
# Optional: SHARDLINE_S3_ENDPOINT (defaults to the host in SHARDLINE_URL),
# SHARDLINE_S3_USE_SSL (defaults from SHARDLINE_URL), SHARDLINE_ITERATIONS
# (default 3), DUCKDB_BIN (default duckdb). The fixture is expected to expose
# `id` and `label` columns; those are also the columns used by the real-client
# E2E fixtures.

: "${SHARDLINE_URL:?set SHARDLINE_URL}"
: "${SHARDLINE_TOKEN:?set SHARDLINE_TOKEN}"
: "${SHARDLINE_REPOSITORY:?set SHARDLINE_REPOSITORY}"
: "${SHARDLINE_REVISION:?set SHARDLINE_REVISION}"
: "${SHARDLINE_FILE_SHA:?set SHARDLINE_FILE_SHA}"
: "${SHARDLINE_S3_URI:?set SHARDLINE_S3_URI}"

iterations="${SHARDLINE_ITERATIONS:-3}"
duckdb_bin="${DUCKDB_BIN:-duckdb}"
if ! [[ "$iterations" =~ ^[1-9][0-9]*$ ]]; then
  printf 'SHARDLINE_ITERATIONS must be a positive integer\n' >&2
  exit 2
fi
command -v curl >/dev/null || { printf 'curl is required\n' >&2; exit 2; }
command -v "$duckdb_bin" >/dev/null || {
  printf 'DuckDB executable not found: %s\n' "$duckdb_bin" >&2
  exit 2
}
benchmark_tmp_dir="$(mktemp -d "${TMPDIR:-/tmp}/shardline-duckdb-benchmark.XXXXXX")"
cleanup_benchmark_tmp() {
  rm -rf -- "$benchmark_tmp_dir"
}
trap cleanup_benchmark_tmp EXIT

s3_endpoint="${SHARDLINE_S3_ENDPOINT:-${SHARDLINE_URL#http://}}"
s3_endpoint="${s3_endpoint#https://}"
s3_endpoint="${s3_endpoint%%/*}"
if [[ -z "${SHARDLINE_S3_USE_SSL:-}" ]]; then
  if [[ "${SHARDLINE_URL}" == https://* ]]; then
    s3_use_ssl=true
  else
    s3_use_ssl=false
  fi
else
  s3_use_ssl="${SHARDLINE_S3_USE_SSL}"
fi

sql_escape() {
  local value="$1"
  printf '%s' "${value//\'/\'\'}"
}

token_sql=$(sql_escape "${SHARDLINE_TOKEN}")
endpoint_sql=$(sql_escape "${s3_endpoint}")

# Configure the external client with the same repository-scoped bearer token
# used by the native route. The fixed secret value is intentionally unused by
# Shardline's S3 compatibility layer. Keep credentials in the subprocess
# command rather than printing them in benchmark output.
duckdb_setup=$(cat <<SQL
CREATE OR REPLACE SECRET shardline_benchmark (
  TYPE S3,
  KEY_ID '${token_sql}',
  SECRET 'unused',
  ENDPOINT '${endpoint_sql}',
  REGION 'us-east-1',
  URL_STYLE 'path',
  USE_SSL ${s3_use_ssl}
);
SQL
)

printf 'benchmark,iteration,elapsed_seconds\n'
native_query() {
  local benchmark="$1" body="$2" iteration start end
  for iteration in $(seq 1 "$iterations"); do
    start=$(date +%s%N)
    curl --fail --silent --show-error \
      -H "Authorization: Bearer ${SHARDLINE_TOKEN}" \
      -H 'Content-Type: application/json' \
      -d "$body" \
      "${SHARDLINE_URL%/}/api/datasets/${SHARDLINE_REPOSITORY}/query" >/dev/null
    end=$(date +%s%N)
    awk -v s="$start" -v e="$end" -v i="$iteration" -v b="$benchmark" \
      'BEGIN { printf "native_%s,%d,%.6f\n", b, i, (e-s)/1000000000 }'
  done
}

external_query() {
  local benchmark="$1" sql="$2" iteration start end
  for iteration in $(seq 1 "$iterations"); do
    start=$(date +%s%N)
    "$duckdb_bin" -c "${duckdb_setup}${sql}" >/dev/null
    end=$(date +%s%N)
    awk -v s="$start" -v e="$end" -v i="$iteration" -v b="$benchmark" \
      'BEGIN { printf "external_%s,%d,%.6f\n", b, i, (e-s)/1000000000 }'
  done
}

request_prefix=$(printf '{"repository":"%s","revision":"%s","file_sha":"%s","config":"default","split":"train"' \
  "$SHARDLINE_REPOSITORY" "$SHARDLINE_REVISION" "$SHARDLINE_FILE_SHA")

# Native route matrix. Keep each request structured and bounded; no SQL is
# sent to Shardline.
native_query schema "${request_prefix},\"columns\":[\"id\",\"label\"],\"limit\":1}"
native_query projection "${request_prefix},\"columns\":[\"id\"],\"limit\":100}"
native_query selective_filter "${request_prefix},\"columns\":[\"id\"],\"predicates\":[{\"column\":\"id\",\"op\":\"gt\",\"value\":0}],\"limit\":100}"
native_query nonselective_filter "${request_prefix},\"columns\":[\"id\"],\"predicates\":[{\"column\":\"id\",\"op\":\"gte\",\"value\":0}],\"limit\":100}"
native_query deep_pagination "${request_prefix},\"columns\":[\"id\"],\"offset\":1000,\"limit\":100}"
native_query aggregate "${request_prefix},\"columns\":[\"id\"],\"aggregates\":[{\"function\":\"count\"}],\"limit\":1}"

# External DuckDB matrix, including multi-file globbing and Parquet export.
external_query schema "SELECT * FROM read_parquet('${SHARDLINE_S3_URI}') LIMIT 1;"
external_query projection "SELECT id FROM read_parquet('${SHARDLINE_S3_URI}') LIMIT 100;"
external_query selective_filter "SELECT id FROM read_parquet('${SHARDLINE_S3_URI}') WHERE id > 0 LIMIT 100;"
external_query nonselective_filter "SELECT id FROM read_parquet('${SHARDLINE_S3_URI}') WHERE id >= 0 LIMIT 100;"
external_query deep_pagination "SELECT id FROM read_parquet('${SHARDLINE_S3_URI}') LIMIT 100 OFFSET 1000;"
external_query aggregate "SELECT count(*), min(id), max(id) FROM read_parquet('${SHARDLINE_S3_URI}');"
external_query glob_aggregate "SELECT label, count(*) FROM read_parquet('${SHARDLINE_S3_URI}') GROUP BY label;"
external_query parquet_export "COPY (SELECT id, label FROM read_parquet('${SHARDLINE_S3_URI}')) TO '${benchmark_tmp_dir}/export.parquet' (FORMAT PARQUET);"
