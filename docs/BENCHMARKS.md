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

## Local S3 cost and replica scaling

Measure real S3-backed HTTP uploads across 1, 2, 4 and 8 Shardline API replicas:

```bash
python3 -B scripts/shardline/validate_performance.py \
  /path/to/release/shardline /tmp/shardline-performance \
  --objects 32 --clients 16 --repeats 2
```

Use a release binary. The runner requires Linux with readable cgroup v2 counters,
Docker, Python 3.11+ with `boto3`, the MinIO `mc` client, and a host CA certificate bundle.
It creates its own PostgreSQL and MinIO containers, limits each Shardline replica
to one CPU and 1 GiB, and removes its containers afterward. Output must be outside
the checkout. The default workload is larger: 128 objects and three repetitions.

The three fixtures separate unique incompressible content, four identical copies
of each incompressible object, and distinct compressible objects. Each file is
1 MiB by default; change it with `--object-bytes`. Client count and uploaded bytes
stay fixed as replicas increase. Repetitions alternate replica order and the order
of the mixed workloads with and without GC.

The example is a smoke matrix, not a sustained-load acceptance run. It gives 32
samples per HTTP method per lane, so its p99 is the slowest sample. Increase the
object count and repetitions for tail-latency studies. A successful run means
the measurements and byte checks passed, not that a scaling target was reached.

Read `summary.md` for throughput, tail latency and scaling efficiency. `results.json`
also records:

- S3 API attempts, including failed dedupe probes, per uploaded decimal GB;
- individual chunk, Xorb and evidence bytes before and after GC;
- PostgreSQL query counts/time, database size, and sampled connection/lock peaks;
- container CPU and peak memory, plus load-generator CPU/RSS;
- direct-S3 PUT/GET reference measurements with the same payloads and client count.

Request counts come from MinIO's live S3 trace, including bucket-list operations.
The trace must acknowledge a probe before each snapshot, and its API totals must
match MinIO's independent v3 server counters. The direct-S3 reference must observe
exactly one PUT and GET per object. Response timing includes the entire body.
Every acknowledged object is read back from every replica after GC. A failed
request, mismatched byte, missing trace, or failed collector fails the run.

Initial ingestion measures uploads alone, followed by two GC runs on disposable
data. The runner requests zero retention, but the CLI enforces a minimum of one
hour. These short runs measure storage during quarantine, not the eventual
footprint after retention expires. The mixed lanes then add two identical logical copies to
compare warm deduplication with and without concurrent GC. A completed collector
must have a CLI process lifetime overlapping the maintenance workload; this does
not mean every request overlaps the collector's scan or exclusive lock. The
initial stored/logical ratio excludes
those later copies. Inspect quarantine counts and retention before interpreting
individual chunk bytes as permanently retained or already reclaimable.

S3-protocol uploads using PostgreSQL metadata and S3 storage now pack each bounded
batch directly into its container. A successful upload verifies the complete stored container before
publishing metadata, without first writing individual chunk objects. Container
storage failures fall back to verified individual chunks. Public chunk-upload
APIs keep their existing behavior. Compare backend request counts and object
classes as well as bytes: avoiding temporary chunk objects lowers request and
quarantine costs even when the eventual packed footprint is unchanged.
LFS, OCI and Bazel digest uploads use the same direct-packing path with S3
storage, for both SQLite and PostgreSQL metadata. They retain source SHA-256
validation and upload-intent recovery. Native Xet already uploads containers;
the public generic upload API still stores its independently downloadable chunks.

Packed-file downloads obtain container length and data in one S3 GET, avoiding
the separate HEAD on successful reads. The complete container is still checked
before its selected bytes are exposed, using a temporary file to bound memory.
Legacy individual chunks retain their fallback path; storage failures still fail
the download when a container is known to exist.

This is a **shared-host S3 API baseline**, not evidence of eight-host production
scaling. S3 routes run on the API role; this does not benchmark native LFS/Xet
transfer replicas. Stored payload bytes exclude object-store filesystem overhead,
replication, versioning and PostgreSQL storage. Memory includes container page
cache; connection/lock samples can miss spikes shorter than 0.5 seconds. The 70%
scale-out efficiency target is a comparison target, not a supported guarantee.

Separate-host load generators, native LFS/Xet comparisons, node termination during
load, and 100 TB–1 PB metadata/object cardinality remain separate validation work.
Increasing a logical byte estimate without creating the corresponding metadata
and object population does not establish large-scale viability.

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
