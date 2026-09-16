#!/usr/bin/env bash
set -euo pipefail

# Compare the bounded Hub query route with an external DuckDB S3 scan.
# Required: SHARDLINE_URL, SHARDLINE_TOKEN, SHARDLINE_REPOSITORY,
# SHARDLINE_REVISION, SHARDLINE_FILE_SHA, SHARDLINE_S3_URI.
# Optional: SHARDLINE_ITERATIONS (default 3), DUCKDB_BIN (default duckdb).

: "${SHARDLINE_URL:?set SHARDLINE_URL}"
: "${SHARDLINE_TOKEN:?set SHARDLINE_TOKEN}"
: "${SHARDLINE_REPOSITORY:?set SHARDLINE_REPOSITORY}"
: "${SHARDLINE_REVISION:?set SHARDLINE_REVISION}"
: "${SHARDLINE_FILE_SHA:?set SHARDLINE_FILE_SHA}"
: "${SHARDLINE_S3_URI:?set SHARDLINE_S3_URI}"

iterations="${SHARDLINE_ITERATIONS:-3}"
duckdb_bin="${DUCKDB_BIN:-duckdb}"

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
  "$duckdb_bin" -c "SELECT id FROM read_parquet('${SHARDLINE_S3_URI}') WHERE id > 0 LIMIT 100" >/dev/null
  end=$(date +%s%N)
  awk -v s="$start" -v e="$end" -v i="$iteration" 'BEGIN { printf "external_duckdb,%d,%.6f\n", i, (e-s)/1000000000 }'
done
