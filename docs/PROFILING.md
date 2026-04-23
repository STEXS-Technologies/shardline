# Profiling

Shardline performance work needs repeatable commands.

The goal is to make regression checks and hotspot investigation routine, not ad hoc.

## Microbenchmarks

Shardline ships Criterion-based microbenchmarks for public hot paths.

Protocol microbenchmarks:

```bash
cargo bench -p shardline-protocol --bench protocol_core -- --noplot
```

Local backend microbenchmarks:

```bash
cargo bench -p shardline-server --bench local_backend -- --noplot
```

Equivalent cargo-make tasks:

```bash
cargo make shardline-bench-protocol
cargo make shardline-bench-local-backend
```

Criterion outputs reports and machine-readable estimates under:

```text
target/criterion/
```

Those reports are suitable for local comparison and saved baselines.

## End-to-End Benchmark

Shardline also ships a deterministic end-to-end benchmark suite through the CLI:

```bash
cargo make shardline-bench-e2e
```

That task builds `shardline` and runs:

- initial upload
- sparse update
- latest-version download
- previous-version download
- ranged reconstruction planning
- concurrent latest downloads
- concurrent uploads with chunk reuse
- cached latest reconstruction after one cold cache fill

The benchmark accepts environment overrides:

```bash
SHARDLINE_BENCH_SCENARIO=full \
SHARDLINE_BENCH_STORAGE_DIR=/tmp/shardline-bench \
SHARDLINE_BENCH_ITERATIONS=10 \
SHARDLINE_BENCH_CONCURRENCY=8 \
SHARDLINE_BENCH_CHUNK_SIZE_BYTES=65536 \
SHARDLINE_BENCH_BASE_BYTES=1048576 \
SHARDLINE_BENCH_MUTATED_BYTES=4096 \
cargo make shardline-bench-e2e
```

When `SHARDLINE_BENCH_CONCURRENCY` is not set, the cargo-make benchmark and perf tasks
use the number of online CPU threads reported by the host.
Set the variable explicitly when comparing runs across machines or when profiling a
fixed concurrency level.

Each invocation allocates a fresh `run-XXXX/` directory under the configured storage
root, which avoids manual cleanup between repeat benchmark and profiling runs.

To focus one hot path while keeping the prerequisite setup untimed, pass one of:

- `full`
- `initial-upload`
- `sparse-update-upload`
- `latest-download`
- `previous-download`
- `ranged-reconstruction`
- `concurrent-latest-download`
- `concurrent-upload`
- `cross-repository-upload`
- `cached-latest-reconstruction`

Example:

```bash
cargo make shardline-bench-e2e -- --scenario cross-repository-upload
```

The benchmark output now also includes:

- bytes-per-second throughput for single-request and concurrent transfer paths
- concurrent scaling efficiency in per-mille, where `1000` means ideal linear scaling
- cached reconstruction cold-fill and hot-hit latencies
- cached reconstruction response bytes and cache-hit iteration counts

For a storage-free ingest ceiling that excludes object-store, index-store, and download
verification costs:

```bash
cargo make shardline-bench-ingest
```

That mode measures upload ingestion only:

- initial upload
- sparse update upload
- concurrent uploads

It keeps chunking and hashing in the hot path but replaces persistence with a blackhole
object store, so the result is a server-core ingest ceiling rather than a storage-backed
throughput number.
Upload bodies are materialized before timing starts, so this mode does
not include client-side buffer construction or per-worker upload-body cloning in the
reported latency.

Upload chunk fan-out can be A/B tested without changing production defaults:

```bash
shardline bench \
  --mode ingest \
  --scenario concurrent-upload \
  --iterations 5 \
  --concurrency 32 \
  --upload-max-in-flight-chunks 1

shardline bench \
  --mode ingest \
  --scenario concurrent-upload \
  --iterations 5 \
  --concurrency 32 \
  --upload-max-in-flight-chunks 64
```

## `perf record`

To capture an optimized Linux `perf` profile for the end-to-end suite:

```bash
cargo make shardline-perf-e2e
```

For the storage-free ingest suite:

```bash
cargo make shardline-perf-ingest
```

Focused perf entrypoints are also available for the most common hot paths:

```bash
cargo make shardline-perf-upload
cargo make shardline-perf-reconstruction
cargo make shardline-perf-range
cargo make shardline-perf-dedupe
```

These tasks record the benchmark command directly from a symbolized build, avoiding
Cargo process overhead in the profile while preserving debug symbols for useful
`perf report` output.

Default output:

```text
/tmp/shardline-e2e.perf.data
```

Overrides:

```bash
SHARDLINE_PERF_OUTPUT=/tmp/shardline.perf.data \
SHARDLINE_PERF_FREQUENCY=999 \
SHARDLINE_BENCH_SCENARIO=latest-download \
cargo make shardline-perf-e2e
```

Inspect the capture with:

```bash
perf report -i /tmp/shardline-e2e.perf.data
```

## Recommended Workflow

For hot-path changes:

1. run the relevant microbenchmark
2. run the end-to-end suite
3. record `perf` on the end-to-end suite when latency or allocation changes are unclear
4. keep the change only when the measurements improve or the regression is justified

That keeps optimization tied to measured behavior instead of intuition.
