#!/usr/bin/env bash
set -euo pipefail

# Compare the bounded Hub query route with an external DuckDB S3 scan.
# Required: SHARDLINE_URL, SHARDLINE_TOKEN, SHARDLINE_REPOSITORY,
# SHARDLINE_REVISION, SHARDLINE_FILE_SHA, SHARDLINE_S3_URI.
# Optional: SHARDLINE_S3_ENDPOINT (defaults to the host in SHARDLINE_URL),
# SHARDLINE_S3_USE_SSL (defaults from SHARDLINE_URL), SHARDLINE_ITERATIONS
# (default 3), DUCKDB_BIN (default duckdb).

: "${SHARDLINE_URL:?set SHARDLINE_URL}"
: "${SHARDLINE_TOKEN:?set SHARDLINE_TOKEN}"
: "${SHARDLINE_REPOSITORY:?set SHARDLINE_REPOSITORY}"
: "${SHARDLINE_REVISION:?set SHARDLINE_REVISION}"
: "${SHARDLINE_FILE_SHA:?set SHARDLINE_FILE_SHA}"
: "${SHARDLINE_S3_URI:?set SHARDLINE_S3_URI}"

iterations="${SHARDLINE_ITERATIONS:-3}"
duckdb_bin="${DUCKDB_BIN:-duckdb}"

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

query_body=$(printf '{"repository":"%s","revision":"%s","file_sha":"%s","config":"default","split":"train","columns":["id"],"predicates":[{"column":"id","op":"gt","value":0}],"limit":100}' \
  "$SHARDLINE_REPOSITORY" "$SHARDLINE_REVISION" "$SHARDLINE_FILE_SHA")

printf 'benchmark,iteration,elapsed_seconds\n'
for iteration in $(seq 1 "$iterations"); do
  start=$(date +%s%N)
  curl --fail --silent --show-error \
    -H "Authorization: Bearer ${SHARDLINE_TOKEN}" \
    -H 'Content-Type: application/json' \
    -d "$query_body" \
    "${SHARDLINE_URL%/}/api/datasets/${SHARDLINE_REPOSITORY}/query" >/dev/null
  end=$(date +%s%N)
  awk -v s="$start" -v e="$end" -v i="$iteration" 'BEGIN { printf "native,%d,%.6f\n", i, (e-s)/1000000000 }'
done

for iteration in $(seq 1 "$iterations"); do
  start=$(date +%s%N)
  "$duckdb_bin" -c "${duckdb_setup}
SELECT id FROM read_parquet('${SHARDLINE_S3_URI}') WHERE id > 0 LIMIT 100" >/dev/null
  end=$(date +%s%N)
  awk -v s="$start" -v e="$end" -v i="$iteration" 'BEGIN { printf "external_duckdb,%d,%.6f\n", i, (e-s)/1000000000 }'
done
