#!/usr/bin/env bash
set -euo pipefail

# Validate shardline chunk-level dedup with a 1GB parquet file
ROOT="/tmp/shardline-test"
PARQUET_ORIG="/tmp/hft_data.parquet"
PARQUET_MOD="/tmp/hft_data_modified.parquet"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
SHARDLINE="$SCRIPT_DIR/target/release/shardline"

echo "=== Step 1: Start shardline server ==="
pkill -f "shardline serve" 2>/dev/null || true
rm -rf "$ROOT" && mkdir -p "$ROOT"
export SHARDLINE_ROOT_DIR="$ROOT"
export SHARDLINE_SERVER_FRONTENDS="lfs,xet,bazel-http"
export SHARDLINE_BIND_ADDR="127.0.0.1:18080"
export SHARDLINE_PUBLIC_BASE_URL="http://127.0.0.1:18080"

"$SHARDLINE" serve &
SERVER_PID=$!
echo "Server PID: $SERVER_PID"

# Wait for server to be ready
for i in $(seq 1 10); do
  if curl -sf http://127.0.0.1:18080/healthz >/dev/null 2>&1; then
    echo "Server ready"
    break
  fi
  sleep 1
done

echo ""
echo "=== Step 2: Count chunks before any upload ==="
count_chunks() {
  find "$ROOT/data/chunks" -type f 2>/dev/null | wc -l
}
CHUNKS_BEFORE=$(count_chunks)
echo "Chunks before: $CHUNKS_BEFORE"

echo ""
echo "=== Step 3: Upload original 1GB parquet file ==="
HASH_ORIG=$(sha256sum "$PARQUET_ORIG" | cut -d' ' -f1)
SIZE=$(stat -c%s "$PARQUET_ORIG")
echo "Original file: $PARQUET_ORIG"
echo "Size: $SIZE bytes ($(echo "scale=2; $SIZE / 1000000000" | bc) GB)"
echo "SHA256: $HASH_ORIG"

# Upload via Bazel HTTP frontend (simplest - just PUT by hash)
echo "Uploading via Bazel HTTP..."
curl -sf -X PUT "http://127.0.0.1:18080/v1/bazel/cache/cas/${HASH_ORIG}" \
  --data-binary @"$PARQUET_ORIG" \
  -w "\nHTTP status: %{http_code}\n"

sleep 2

CHUNKS_AFTER_ORIG=$(count_chunks)
CHUNKS_ORIG_DELTA=$((CHUNKS_AFTER_ORIG - CHUNKS_BEFORE))
echo ""
echo "Chunks after original upload: $CHUNKS_AFTER_ORIG (delta: +$CHUNKS_ORIG_DELTA)"

echo ""
echo "=== Step 4: Verify download ==="
curl -sf -X GET "http://127.0.0.1:18080/v1/bazel/cache/cas/${HASH_ORIG}" \
  -o /tmp/hft_downloaded.parquet \
  -w "HTTP status: %{http_code}, size: %{size_download} bytes\n"

if cmp "$PARQUET_ORIG" /tmp/hft_downloaded.parquet; then
  echo "Download matches original: OK"
fi

echo ""
echo "=== Step 5: Modify a few rows using Python ==="
python3 "$SCRIPT_DIR/modify_parquet.py" "$PARQUET_ORIG" "$PARQUET_MOD"

HASH_MOD=$(sha256sum "$PARQUET_MOD" | cut -d' ' -f1)
SIZE_MOD=$(stat -c%s "$PARQUET_MOD")
echo "Modified file size: $SIZE_MOD bytes"
echo "Modified SHA256: $HASH_MOD"
echo "Hash same as original? $([ "$HASH_ORIG" = "$HASH_MOD" ] && echo YES || echo NO)"

echo ""
echo "=== Step 6: Upload modified file ==="
echo "Uploading modified file via Bazel HTTP..."
curl -sf -X PUT "http://127.0.0.1:18080/v1/bazel/cache/cas/${HASH_MOD}" \
  --data-binary @"$PARQUET_MOD" \
  -w "HTTP status: %{http_code}\n"

sleep 2

CHUNKS_AFTER_MOD=$(count_chunks)
CHUNKS_MOD_DELTA=$((CHUNKS_AFTER_MOD - CHUNKS_AFTER_ORIG))
echo ""
echo "Chunks after modified upload: $CHUNKS_AFTER_MOD (delta: +$CHUNKS_MOD_DELTA)"

echo ""
echo "=== Step 7: Results ==="
echo "----------------------------------------"
echo "Original upload:  +$CHUNKS_ORIG_DELTA chunks for 1.02 GB"
echo "Modified upload:  +$CHUNKS_MOD_DELTA new chunks"
echo "Dedup savings:    $((100 - (CHUNKS_MOD_DELTA * 100 / CHUNKS_ORIG_DELTA)))% of chunks reused"
echo ""
echo "Store size after original: $(du -sh "$ROOT" | cut -f1)"
echo "Store size after modify:   $(du -sh "$ROOT" | cut -f1)"
echo "----------------------------------------"

echo ""
echo "=== Cleanup ==="
kill "$SERVER_PID" 2>/dev/null || true
echo "Done"
