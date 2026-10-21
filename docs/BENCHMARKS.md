# Benchmarks

Shardline has two complementary benchmark classes.

## Performance and latency

Run the reproducible local matrix:

```bash
scripts/benchmark-matrix.sh
```

The command records commit, host, toolchain, fixture shape, throughput, operation
latency, CPU utilization, cache cold/hot latency, and reuse ratios in a timestamped
directory under `benchmark-results/`. Override the matrix without changing the runner:

```bash
SHARDLINE_BENCH_MATRIX_CONCURRENCY='1 32 128' \
SHARDLINE_BENCH_MATRIX_ITERATIONS=10 \
scripts/benchmark-matrix.sh
```

For HTTP latency and sustained request behavior against an already-running deployment:

```bash
scripts/load_benchmark.sh --duration 60 --warmup 10 --concurrency '1 10 50 100'
```

By default this is a read/health latency benchmark. The runner will not invent a
write result by POSTing to the health endpoint. Supply an endpoint that is explicitly
safe for benchmark writes when one is available:

```bash
scripts/load_benchmark.sh \
  --read-url http://127.0.0.1:28080/healthz \
  --write-url http://127.0.0.1:28080/<real-write-endpoint> \
  --duration 60 --warmup 10 --concurrency '1 10 50 100'
```

The load runner reports request count, success/error rate, throughput, p50/p95/p99,
and min/max latency. Its latency ends at response headers, and its memory field is
a client RSS snapshot; use the contention runner below for complete-response latency
and server peak RSS. Write and mixed-workload results are only emitted
when `BENCH_WRITE_URL`/`--write-url` is configured; otherwise the mixed lane is
explicitly read-only.

## Uploads while GC runs

Compare two Linux binaries with disposable PostgreSQL and MinIO services:

```bash
python3 -B scripts/shardline/benchmark_gc_contention.py \
  /path/to/current/shardline /path/to/1.11.3/shardline \
  /tmp/shardline-gc-comparison
```

This runs three pairs in alternating order, with 16 clients each uploading and
reading 128 distinct 64 KiB objects while mark-and-sweep GC runs. It measures complete
PUT/GET p50/p95/p99, request throughput, server CPU/peak RSS, and GC cost. Every
object is checked again after GC finishes. Any failed request, GC failure, or byte
mismatch fails the run. Override `BENCH_CONCURRENCY`, `BENCH_OPERATIONS`, or
`BENCH_REPEATS` for larger samples.

GC starts again 0.5 seconds after each completed run and stops when the request
workload finishes. Completed GC counts can differ between binaries and are included
in each result. Compare repeated runs on an otherwise idle host before extrapolating.

The services are removed afterward; local diagnostics remain at the output path.
This measures live-object scanning and publication contention, not deletion-heavy
GC, multiple API nodes, or cloud network latency. CPU/RSS describe Shardline
processes; PostgreSQL and MinIO resource use is excluded.

## Measuring local disk savings

Avoided scratch bytes are not the same as writes reaching an SSD. For a disk
comparison, put object storage, temporary files, and the database on identified
disk-backed volumes; `/tmp` may be tmpfs. Use the same corpus, concurrency, cache
conditions, and backend configuration for alternating before/after runs. Include
database and object-store processes when reporting total deployment cost.

On an otherwise idle benchmark device, capture `/sys/block/<device>/stat` before
and after each run, including completion of its pending writes. The Linux kernel
[documents these counters](https://docs.kernel.org/admin-guide/iostats.html):
they cover the whole device, so unrelated workloads invalidate attribution.
Report device-write bytes per logical TB processed alongside throughput, tail
latency, RSS, and integrity results. Keep syscall scratch-byte counts separate.
Device-write reduction alone does not establish NAND wear or extra years of drive
life; report those only with appropriate drive telemetry and endurance assumptions.

## S3-backed upload packing

Shardline keeps each S3 upload's raw CDC chunks in a bounded in-memory batch and
tries to store the packed Xorb first. On the successful path, it no longer PUTs
each compressed CDC chunk before packing. That removes one chunk-object PUT
attempt per CDC chunk and avoids new temporary chunk objects (and their later
GC deletes) when those chunk objects did not already exist. It also removes the
chunk-level existence checks. A batch whose Xorb already exists is still read
back and fully verified by the S3 conditional-write path before Shardline
reuses it.

If packing or Xorb storage fails, Shardline writes the individual chunks, reads
them back, and verifies their content before publishing a chunk-based fallback.
The fallback record format and its integrity checks remain unchanged. A crash
before a file record is published can leave either Xorbs or fallback chunks for
garbage collection, as before; an in-progress upload is not acknowledged as a
stored file.

Raw chunk retention is bounded to roughly 32 MiB per active upload batch (a
single chunk can exceed that target within the existing chunk limit). Concurrent
uploads multiply this memory use; this bound is not a process-wide S3 memory
budget. The request-count unit test verifies one PUT and no GETs for successful
packing of a multi-chunk batch, plus chunk PUTs and verified GETs after a forced
Xorb failure. Those tests establish request behavior, not MinIO/cloud latency,
peak RSS, or billed cost. Actual storage savings depend on how many chunk
objects the old path would have newly created and later collected.

## Production-shaped incident and recovery

Run the exact incident drills as separate nextest processes:

```bash
scripts/production-reliability-benchmark.sh
```

The report measures end-to-end wall time for:

- PostgreSQL failure during upload;
- backup, destroy, restore, fsck, and byte verification;
- PostgreSQL failure during LFS, OCI, and S3 multipart operations;
- repeated kill/restart cycles and leak checks.

Each scenario has isolated stdout/stderr logs, timing data, peak RSS, exit status,
JSON output, and a Markdown summary. A failed or skipped drill makes the runner fail;
no recovery result is treated as a successful measurement.

These measurements are baselines for a fixed host and deployment shape. Compare runs
only with matching fixture, service, database, object-store, CPU, and toolchain
metadata.
