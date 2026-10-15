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
