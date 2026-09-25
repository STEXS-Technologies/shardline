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
peak RSS, and min/max latency. Write and mixed-workload results are only emitted
when `BENCH_WRITE_URL`/`--write-url` is configured; otherwise the mixed lane is
explicitly read-only.

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
