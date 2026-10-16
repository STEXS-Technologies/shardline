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

Direct packing uses the existing hashing pool (`SHARDLINE_HASHING_POOL_SIZE`,
default 8). Each active batch holds a slot from its first raw chunk through
packing, stored-container verification or fallback. Waiting uploads do not copy
a packing batch, and cancelled running CPU work retains its slot until it ends.
The normal raw batch target is 32 MiB; a single CDC chunk can exceed that target
with unusually large configured chunk sizes. Encoded containers, request buffers
and metadata also consume memory, so measure process RSS under representative
concurrency rather than treating the raw batch target as a total memory limit.

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

### Operational-efficiency measurements (October 16, 2026)

These local PostgreSQL/MinIO measurements compare main-equivalent product code
(`c2301dac`, based on `01f2c9e3`) with `48727f81`. Each lane uploaded 32 objects
of 1 MiB using 16 clients, with two repeats at 1, 2, 4 and 8 API replicas.
Backend counts include attempted requests, including retries.

| Initial upload workload | Replicas | Median backend requests before → after |
| --- | --- | --- |
| Repeated content | 1 | 1,205 → 104 |
| Repeated content | 2 | 1,391 → 112 |
| Repeated content | 4 / 8 | 1,605 → 128 |
| Unique content | 1 / 2 / 4 / 8 | 1,188 → 128 |

All 5,760 post-GC full-body checks passed. These counts demonstrate lower request
amplification for this fixture; two shared-host repeats do not establish stable
tail-latency or production scale-out improvements.

The admission-qualified candidate (production code at `73a93ae6`) subsequently
passed all 24 lanes across unique, repeated and compressible data, the same four
replica counts and two repeats. All 8,640 post-GC reads matched, backend traces
matched MinIO counters, and no database deadlocks were reported. Unique and
compressible initial uploads each made 128 requests per lane; repeated uploads
made 104 / 112 / 128 / 128 at 1 / 2 / 4 / 8 replicas in both repeats. Workspace
compilation overlapped part of this run, so its timings are not used as additional
latency or scaling evidence.

Separate targeted comparisons measured unchanged-record PostgreSQL WAL at
608,616 → 61,472 bytes for sixteen 36 MiB replacements (`48727f81`); small-record
savings were negligible. Reusing verified Merkle parents (`6fb93b6c` → `8670f51a`)
reduced database calls from 155,648 to 147,456 per 4,096 small-object PUT/GET pairs.
In sequential four-replica runs, median server CPU fell from 9.14 to 8.56 seconds
and PostgreSQL CPU from 15.20 to 11.86 seconds, with unchanged backend requests
and 16,384 verified post-GC reads per version. Timing remains host-dependent.

Packing-admission qualification used 64 concurrent unique 36 MiB uploads against
one API replica with 1 CPU and a 1 GiB memory limit. All uploads and 64 post-GC
reads passed; process peak RSS was approximately 500 MiB. An earlier run ended
with a client broken pipe; a separate overload test established that request
admission timeout responses can close an upload before its body is sent.
After correcting those responses, a deliberately restricted 0.25-CPU run returned
19 successes and 45 HTTP 503 responses with retry guidance. All 64 objects retained
valid old or new content after GC, every acknowledged replacement was present,
and peak process RSS was approximately 414 MiB with no OOM kill. These are bounded
fixture results, not a universal memory or latency guarantee.

The checksum-verified published v1.12.0 Linux binary also passed a two-replica
shared-PostgreSQL/shared-bucket round trip: released writes, branch reads and
writes, released reads and writes after rollback, then branch reads after GC.
All 96 full-body reads matched across 36 MiB files. This verifies the exercised
S3 protocol representations; other protocol and fault combinations need their
own qualification.

A two-replica fault drill killed and restarted a server during an incomplete
72 MiB replacement after sending 36 MiB, preserving the previous object. It then
completed 32 identical 36 MiB uploads through both replicas and eight uploads whose
clients discarded acknowledgements. Both replicas returned the expected content
after GC. Discarding client acknowledgements does not simulate a lost S3 response.

The packing regression suite separately injects a backend that stores a container
but returns HTTP 503. Conditional retry must recover the existing object, verify
it and reconstruct the original bytes without creating individual chunk objects.
GC regression coverage also deletes a listed orphan before sweep and checks that
quarantine cleanup and a repeated sweep do not resurrect data. These controlled
cases complement the deployment drill; they do not cover every network failure.

A release-mode microbenchmark of per-key write coordination measured about
0.16 µs per acquisition with 128 other keys held, 0.49 µs with 512 and 10.4 µs
with 8,192 (10,000 acquisitions per size, one thread, no S3 traffic). Cleanup
cost grows with active keys; these timings alone do not establish end-to-end
contention or justify replacing the current coordination scheme.

Partial-overlap files expose a different limit. A separate two-replica comparison
used 32 versions of one incompressible 4 MiB file, each replacing a distinct 4 KiB
region, with 16 clients. Main-equivalent code made 4,299 backend requests during
ingestion; the admission-qualified candidate made 128. Both stored the same 32
containers totaling 134,336,312 bytes for 134,217,728 logical bytes. Main also kept
97 individual chunk objects totaling 7,259,864 bytes during quarantine; the
candidate omitted them. All 128 post-GC cross-replica reads matched across the
two versions.

This S3 protocol path packs each file's batches independently. Shared CDC chunks
inside different containers do not by themselves reduce the final container
footprint. The patch reduces request and temporary-storage amplification, but
these measurements do not demonstrate better partial-overlap storage savings.
Measure versioned and partially overlapping customer data separately from exact
duplicates before estimating a bill reduction.

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
