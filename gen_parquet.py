"""Generate a ~1GB parquet file simulating HFT data, then modify a few rows."""
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
import numpy as np
import os, time, sys

TARGET_SIZE = 1_000_000_000  # 1GB
PARQUET_FILE = "/tmp/hft_data.parquet"
PARQUET_MODIFIED = "/tmp/hft_data_modified.parquet"

# Schema: HFT-like tick data
schema = pa.schema([
    ("ts_ns",    pa.int64()),    # nanosecond timestamp
    ("symbol",   pa.string()),
    ("price",    pa.float64()),
    ("volume",   pa.int32()),
    ("bid",      pa.float64()),
    ("ask",      pa.float64()),
    ("side",     pa.string()),
    ("exchange", pa.string()),
])

symbols = ["AAPL", "MSFT", "GOOGL", "AMZN", "NVDA", "TSLA", "META", "JPM"]
exchanges = ["NYSE", "NASDAQ", "CME"]
sides = ["B", "S"]
batch_size = 100_000

print("Generating ~1GB parquet file...", flush=True)
writer = None
total_rows = 0
start = time.time()

# Pre-generate some random data for speed
rng = np.random.default_rng(42)

while True:
    n = batch_size
    ts = np.arange(total_rows, total_rows + n, dtype=np.int64) * 1_000 + rng.integers(0, 999, n)
    sym = rng.choice(symbols, n)
    price = 100.0 + rng.random(n) * 900.0
    vol  = rng.integers(1, 1000, n)
    bid  = price - rng.random(n) * 0.5
    ask  = price + rng.random(n) * 0.5
    side = rng.choice(sides, n)
    exch = rng.choice(exchanges, n)

    table = pa.table({
        "ts_ns":    pa.array(ts),
        "symbol":   pa.array(sym),
        "price":    pa.array(price),
        "volume":   pa.array(vol),
        "bid":      pa.array(bid),
        "ask":      pa.array(ask),
        "side":     pa.array(side),
        "exchange": pa.array(exch),
    }, schema=schema)

    if writer is None:
        writer = pq.ParquetWriter(PARQUET_FILE, schema)
    writer.write_table(table)
    total_rows += n

    if total_rows % 1_000_000 == 0:
        sz = os.path.getsize(PARQUET_FILE)
        elapsed = time.time() - start
        print(f"  {total_rows:,} rows, {sz/1e9:.2f} GB, {elapsed:.1f}s", flush=True)
        if sz >= TARGET_SIZE:
            break

writer.close()
elapsed = time.time() - start
final_size = os.path.getsize(PARQUET_FILE)
print(f"\nDone: {total_rows:,} rows, {final_size/1e9:.2f} GB in {elapsed:.1f}s", flush=True)
print(f"File: {PARQUET_FILE}", flush=True)
