"""Modify a few rows of a parquet file to simulate HFT data update."""
import pyarrow.parquet as pq
import pyarrow as pa
import pandas as pd
import sys

src = sys.argv[1]
dst = sys.argv[2]

print(f"Reading {src}...", flush=True)
table = pq.read_table(src)
df = table.to_pandas()
n = len(df)
print(f"Loaded {n:,} rows, {len(table.column_names)} columns", flush=True)

# Modify 5 rows: change price, bid, ask for a few specific rows
modify_indices = [0, n // 4, n // 2, 3 * n // 4, n - 1]
for idx in modify_indices:
    old_price = df.loc[idx, "price"]
    df.loc[idx, "price"] = round(old_price * 1.001, 2)  # 0.1% price change
    df.loc[idx, "bid"] = df.loc[idx, "price"] - 0.05
    df.loc[idx, "ask"] = df.loc[idx, "price"] + 0.05
    df.loc[idx, "volume"] = int(df.loc[idx, "volume"]) + 1

print(f"Modified {len(modify_indices)} rows at indices: {modify_indices}", flush=True)

# Write back
print(f"Writing {dst}...", flush=True)
df.to_parquet(dst, index=False)
print("Done", flush=True)
