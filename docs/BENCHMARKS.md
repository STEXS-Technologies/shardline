# Benchmarks

Shardline has two complementary benchmark classes.

## Protocol integration versus released 1.12.1

On October 22, 2026, the published 1.12.1 Linux binary and the integrated
protocol build were compared using the operational-cost runner below. Each
used 1 and 2 API replicas, 32 objects of 1 MiB, 8 clients, unique and repeated
data, and two repetitions. The entire comparison was repeated in reverse
binary order: four observations per dataset and replica count.

| Workload | Replicas | 1.12.1 S3 attempts | Integrated S3 attempts |
| --- | ---: | ---: | ---: |
| Unique initial ingestion | 1 or 2 | 128 | 128 |
| Repeated initial ingestion | 1 | 104 | 104 |
| Repeated initial ingestion | 2 | 112 | 112 |
| Unique mixed PUT/GET with GC | 1 or 2 | 294 | 258 |
| Repeated mixed PUT/GET with GC | 1 or 2 | 198 | 186 |

Counts include backend probes and maintenance attempts, not only successful
client requests. PostgreSQL query counts also matched for each corresponding
phase. Durable Xorb payloads were identical: 32 containers totaling 33,588,848
bytes for unique data and 8 totaling 8,397,128 bytes for repeated data. Each
binary passed 2,304 post-GC response byte comparisons across both run orders.
The TLS-patched integrated build repeated the same matrix, passing another
1,152 byte comparisons with identical request counts and Xorb payloads.

Fewer requests do not mean less backend transfer during GC. The integrated
collector validates source containers instead of trusting unauthenticated
`_xorb_chunks` sidecars. MinIO response bytes during the unique mixed+GC
workload increased from 67,260,673 to 100,797,088 (about 50%); repeated data
increased from 67,206,604 to 75,591,132 (about 12%). The extra transfer is one
complete read per unique container in that scan. Normal ingestion and the mixed
workload without GC retained their backend transfer counts. These trace bytes
include protocol overhead and are not a cloud-egress billing measurement.

The integrity check remains required. Recovering this maintenance-read cost
would require verifiable membership evidence; restoring trust in the old plain
sidecars would reopen the integrity gap. Do not describe this comparison as
preserving all operational costs.

Timing varied between run orders, so these shared-host bursts do not establish a
throughput or p99 improvement, production scale-out efficiency, cloud bills,
NVMe wear, or the memory ceiling of large concurrent uploads. Keep raw runner
output outside the checkout; use the same fixture options to reproduce this
comparison with the target release binaries.

## Native shard publication requests

The October 25 MinIO qualification uses a native shard referencing two Xorbs.
Removing its preliminary existence probe saves one HEAD per registration. The
pre-change test observes one unwanted HEAD on first publication;
the updated implementation issues none on either initial publication or replay.
Its conditional write decides the existing/new response and still checks the
complete stored shard bytes before accepting a duplicate.

| Updated operation | Source GETs | Existing-shard verification GETs | Preliminary HEADs |
| --- | ---: | ---: | ---: |
| Initial publication | 2 | 0 | 0 |
| Replay through the same object-store client | 2 | 1 | 0 |

The same test injects corrupt source bytes and corrupt existing-shard bytes;
both reject registration, and the latter never invokes metadata publication.
It counts actual provider attempts with a loopback proxy and owns its MinIO
fixture. Reproduce with Docker available:

```sh
cargo nextest run --locked -p shardline-server --features docker \
  --test native_shard_reads --run-ignored only
```

This reduces request overhead, not payload bytes, decoded-chunk copies or
cross-request source validation. It is not an end-to-end throughput or cloud
billing measurement; a fresh replica can also require a conditional create
attempt before discovering an existing shard.

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
For before/after comparisons, pass `--binary-source-revision <revision>` for each
executable. Metadata records that supplied revision separately from
`harness_commit` and always records the executable's SHA-256; the checkout's
commit alone does not identify a previously built or released binary.

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
- PostgreSQL query counts/time, statement-generated WAL bytes, records and full-page
  images, database size, and sampled connection/lock peaks;
- container CPU and peak memory, plus load-generator CPU/RSS;
- direct-S3 PUT/GET reference measurements with the same payloads and client count.

WAL deltas come from `pg_stat_statements` for the lane database, excluding observer
queries. They measure WAL generated by tracked statements, not total cluster WAL,
physical disk writes, replication traffic or NVMe wear.

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

S3-protocol uploads using SQLite or PostgreSQL metadata and local or S3 storage pack each bounded
batch directly into its container. A successful upload verifies the complete stored container before
publishing metadata, without first writing individual chunk objects. Container
storage failures fall back to verified individual chunks. Public chunk-upload
APIs keep their existing behavior. Compare backend request counts and object
classes as well as bytes: avoiding temporary chunk objects lowers request and
quarantine costs even when the eventual packed footprint is unchanged.
LFS, OCI and Bazel digest uploads use the same direct-packing path with local or S3
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

### Operational-efficiency measurements

The measured savings differ by backend and workload. The detailed fixtures
below explain the boundaries; none requires weaker durability settings.

| Area | Verified reduction | Important limit |
| --- | --- | --- |
| Local object storage | Direct packing reduced process disk writes by 47.9% in the measured upload fixture | Additional verification reads remain; SSD lifetime was not measured |
| SQLite | Direct packing reduced GC database syncs and writes in the measured fixture | Ordinary publication syncs remain; skipping unchanged payload assignments did not reduce WAL |
| PostgreSQL | Less WAL for large unchanged records, fewer evidence queries and one fewer query per S3 metadata deletion | Small-record WAL savings were negligible; locks and transactional evidence remain |
| S3 | Fewer staging objects, requests and repeated fsck reads | Full verification still reads complete content; eventual retained bytes need not decrease |
| Temporary snapshots | Existing kernel copies can share extents on Btrfs while remaining isolated | Filesystem and scratch placement matter; this was already present before the patch |

These savings must not be added as percentages of a single bill. Uploads,
downloads, metadata publication and maintenance have different costs and
frequencies. Use the [cost-per-TB method](#operational-cost-per-logical-tb) to
combine measured quantities for an actual workload.

The final server library regression against product code at `e2661a64` passed
3,403 tests with no failures. Six explicit task-history capacity drills were
ignored by the normal suite and were not rerun in that check. The preceding
storage regression passed 492 tests; the index regression passed 780 with a
disposable PostgreSQL instance configured. These regressions complement the
workload-specific evidence below; they do not certify every deployment shape.

#### Shared-host replica and latency qualification

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

A separate October 17 comparison alternated the main-equivalent and
admission-qualified binaries across three pairs, with no compilation running.
Each run used 128 one-MiB objects, 16 clients, and one or two API replicas limited
to one CPU and 1 GiB each. The table shows medians across three runs; CPU seconds
cover MinIO's container during initial ingestion of 128 MiB.

| Initial upload workload | Replicas | Backend requests before → after | MinIO CPU seconds before → after |
| --- | --- | --- | --- |
| Unique content | 1 | 4,848 → 512 | 5.926 → 0.832 |
| Unique content | 2 | 4,848 → 512 | 5.309 → 0.721 |
| Repeated content | 1 | 4,828 → 416 | 2.875 → 0.368 |
| Repeated content | 2 | 5,495 → 448 | 3.490 → 0.511 |

All 13,824 post-GC full-body checks passed, and all lane traces matched MinIO's
independent counters. Shardline CPU also fell in each initial-ingestion lane,
but PostgreSQL CPU did not improve uniformly: the two-replica repeated-content
lane rose from 0.800 to 0.927 seconds. Median initial-upload throughput increased
in all four lanes, but individual timings varied substantially. Warm mixed reads
also had a regression: with two replicas and unique content, the median of the
three GET p99 measurements rose from 45.2 to 61.0 ms, despite higher aggregate
throughput. These short shared-host runs support lower request and object-store
CPU costs, not a universal latency improvement or production scaling claim.
Concurrent-GC phases perform different numbers of collections as workload
duration changes; they are not comparisons of a fixed amount of GC work.

Read-only follow-up reproduced the tail-latency trade-off. Three alternating
pairs read 128 warmed one-MiB files through two replicas with 16 clients. Each
run measured eight rounds of 1,024 complete responses. All 55,296 reads, including
warmup, matched. Main-equivalent runs used two backend requests per read and the
candidate used one; traces matched independent counters. The median round p99
within each run was 48.1–48.3 ms before and 65.5–70.7 ms after, while median
throughput increased from 530–558 to 810–990 MiB/s.

A separate quota diagnostic observed substantially more CPU throttling on the
candidate at one CPU per replica. At four CPUs neither version was throttled,
and median round p99 was 46.7 ms before and 11.3 ms after. On the one-CPU candidate,
setting the existing `SHARDLINE_BLOCKING_IO_POOL_SIZE=2` reduced p99 to 40.0 ms
with approximately 990 MiB/s throughput. These are local diagnostic results,
not default-sizing recommendations: a smaller blocking pool can limit useful
concurrency when backend requests have longer latency. Measure that trade-off
on the intended deployment before tuning; the patch keeps existing pool defaults.

A controlled proxy adding 25 ms to each backend GET/HEAD demonstrated that
trade-off on the one-CPU candidate. The default pool of 16 delivered about
463 MiB/s with median round p99 of 38.1 ms; a pool of 2 delivered 139 MiB/s
with p99 of 117.9 ms. Both configurations made one backend GET per read, all
8,192 body checks passed, and traces matched independent counters. This is an
artificial-delay diagnostic, not a cloud-provider or production benchmark.

#### Temporary storage and retention

A separate retention check uploaded 384 MiB of logical content (128 one-MiB
files each of unique, repeated and compressible data) through two replicas per
version. Repeated-data identities were separated from the unique dataset. The
test waited the actual one-hour minimum retention period without changing clocks
or quarantine timestamps, then ran GC and read every file through both replicas.
All 1,536 full-body checks passed, both quarantine tables were empty, and backend
traces matched independent MinIO counters.

Direct packing avoided 3,824 temporary chunk objects totaling 169,005,073 bytes.
After retention and GC, both versions held exactly 578 objects totaling
168,862,899 bytes: 288 containers plus 290 maintenance/evidence objects. The final
sweep made 17,641 backend requests before and 1,189 after. This fixture therefore
demonstrates lower temporary storage and cleanup request costs, with an unchanged
eventual stored footprint; it does not demonstrate additional compression or
deduplication savings.

#### Local filesystem and SQLite writes

Before the local direct-packing extension, an October 17 filesystem/SQLite check compared the same main-equivalent and
candidate binaries in three pairs on an NVMe-backed Btrfs filesystem, rather
than the host's memory-backed `/tmp`. Each version uploaded 32 unique one-MiB
files with eight clients, overwrote them with identical content, and read them
again. Syscall tracing covered those phases; timings under tracing are not
throughput measurements. The table gives median syscall counts across three
runs, excluding startup, maintenance and restart.

| Local backend work per 32-file phase | Before → after |
| --- | --- |
| Initial object filesystem writes | 579 → 579 |
| Initial SQLite sync calls | 253 → 256 |
| Identical-overwrite object filesystem writes | 0 → 0 |
| Identical-overwrite SQLite sync calls | 253 → 253 |
| Read-only SQLite database/WAL writes and sync calls | 0 → 0 |

The read-only phase still wrote SQLite's `-shm` coordination file as connections
opened and closed; these were not database-page or WAL checkpoint writes. This
fixture established no material filesystem/SQLite I/O saving before that extension.
Identical local objects already avoid rewriting payloads, while verification and
file/directory syncs remain necessary to acknowledge a durable winner safely.
SQLite retains WAL mode and `synchronous=FULL`. All 768 full-body comparisons,
including reads after GC and server restart, passed. Every deployment passed
Shardline fsck, SQLite `integrity_check` and `foreign_key_check`. Process I/O
accounting on a cached, compressed filesystem is not a cloud-volume billing
estimate; S3 and PostgreSQL savings above must not be generalized to local storage.

The subsequent local direct-packing extension was measured separately against
the preceding branch binary, using the same disk-backed workload in three pairs.
Median initial object writes fell from 579 to 32 and object sync calls from
2,477 to 296. Ordinary SQLite publication syncs were essentially unchanged
(256 to 250). GC SQLite syncs fell from 2,240 to 52 and SQLite write calls from
18,733 to 664 because redundant raw chunks no longer needed quarantine records.
All 768 full-body comparisons passed, including reads with released v1.12.0
after GC; fsck and SQLite integrity checks passed. A separate SIGKILL during a
72 MiB replacement preserved the old published content; retry, GC and released
v1.12.0 reads of the replacement also passed. These checks establish tested
compatibility, rather than an unconditional guarantee against every failure.

An October 17 follow-up measured local write bytes rather than syscall counts.
Three before/after pairs compared the preceding branch binary with the local
direct-packing binary on the host's WD_BLACK SN850X 1 TB NVMe and Btrfs filesystem.
Each fresh deployment uploaded 32 unique, incompressible one-MiB files through
eight clients and read every file back. Pair order alternated. Startup was
excluded; filesystem sync completed before the ending counter sample.

| Local upload measurement per 32 MiB input | Before median | Direct packing median | Reduction |
| --- | --- | --- | --- |
| Server kernel-accounted disk writes (`/proc/PID/io: write_bytes`) | 122.2 MiB | 63.6 MiB | 47.9% |
| Whole-drive NVMe SMART host writes | 211.9 MiB | 126.5 MiB | 40.3% |

All 192 full-body read comparisons and six subsequent fsck checks passed.
Existing file/directory syncs and SQLite `synchronous=FULL` remained enabled.
The measurements cover initial ingestion and reads, not the subsequent fsck,
GC, rollback or a crash drill; those have separate evidence above.

The process counter measures kernel-accounted storage writes, not internal NAND
programming. SMART host writes include every application on this system drive.
Three-second idle samples ranged from 27.8 to 237.3 MiB, so the whole-drive
reduction is a noisy observation, not an isolated Shardline result. No idle-rate
subtraction was applied. Filesystem compression, allocation and SSD internal
write amplification also affect the relationship between these counters and
wear. These results support lower local write pressure for this fixture;
they do not establish a drive-lifetime multiplier or annual replacement saving.
An endurance estimate needs longer isolated runs, the drive's rated write
endurance and replacement cost, and the deployment's complete write workload.

#### Local snapshot copies

An October 17 local-snapshot audit found that the current production
`materialize_object_to_tempfile` already uses Rust's file-to-file `std::io::copy`
fast path. A syscall trace of its existing large-object regression on Btrfs
showed `copy_file_range` copying 2,097,169 bytes, followed by EOF. No replacement
copy implementation was needed. These private snapshots preserve the bytes
shared by container validation and later decoding; replacing them with a source
file handle would change that guarantee.

A separate 32 MiB incompressible-file probe on the same NVMe-backed Btrfs
filesystem alternated three kernel-copy/buffered-copy pairs. Each output was
synced before reading `/proc/self/io`, then compared completely with its source:

| Snapshot method | Process `write_bytes` per copy | Elapsed time including output sync |
| --- | --- | --- |
| Existing kernel file-copy path | 81,920–114,688 bytes | 0.49–0.57 ms |
| Explicit 256 KiB buffered copy | 33,619,968–33,669,120 bytes | 11.24–12.67 ms |

All six copies matched; modifying each output left its source unchanged. A
Shardline regression additionally mutates the source after materialization and
checks that the snapshot remains unchanged, then checks isolation in the other
direction. It passes with temporary files on both tmpfs and Btrfs. On this host
the default `/tmp` is tmpfs; the Btrfs run explicitly set `TMPDIR` to a disposable
directory on the local-storage filesystem.

To check snapshot isolation with a chosen scratch filesystem, create a private
directory there and run:

```bash
TMPDIR=/path/to/private-scratch cargo test -p shardline-server-core --lib materialized_local_snapshot
```

These are measurements of an existing capability, not a before/after patch
gain or a physical NAND-wear measurement. Filesystem support, mount boundaries,
compression and temporary-file placement affect copy behavior. Keeping scratch
files on the same copy-on-write filesystem can permit cheap isolated copies;
tmpfs avoids direct SSD scratch writes but consumes memory and may use swap.
Measure the chosen layout's memory, disk accounting and capacity before applying
the numbers to a deployment. Durable object and SQLite sync policies are unchanged.

#### Footerless Xet staging

Owned request staging verifies footerless Xorb members and appends their footer
in place. The public borrowed-file API preserves its input and uses a separate
normalized copy. A Linux diagnostic with 64 MiB of uncompressed content and 256
members measured these normalization writes on file-backed scratch:

| Path | Logical bytes written (`wchar`) | Process-accounted `write_bytes` |
| --- | ---: | ---: |
| Borrowed input, normalized copy | 67,123,320 | 67,141,632 |
| Owned staging, appended footer | 12,408 | 12,288 |

The avoided logical write is exactly the 67,110,912-byte input, including chunk
headers. Both outputs have the same verified canonical content. Reproduce this
diagnostic alone, with `TMPDIR` on the scratch filesystem being evaluated:

```bash
cargo test --locked -p shardline-xet-adapter --lib \
  footerless_owned_staging_avoids_one_payload_write \
  -- --ignored --test-threads=1 --nocapture
```

This measures normalization only, excluding fixture creation and object
publication. Kernel accounting does not measure NAND wear; temporary dirty pages
may be discarded before writeback. Total upload cost still includes staging,
verification, decoded chunk storage and durable container persistence. The
stock-client workload has not demonstrated a reduction in those total costs.

#### S3 requests and upload resources

Lifecycle repair has a separate shared-container optimization: a bounded,
run-local set remembers fully verified packed sources while scanning latest and
version records. In a real MinIO fixture with four files sharing one container,
this reduces source GETs from eight to one per scan. Every file record is still
parsed, and a later repair reads the source again and detects intervening
corruption. The set holds at most 4,096 identities, with rereads after turnover;
it retains no container payloads. This fixture does not establish a reduction
in GC's transfer cost across separate commands or total production repair cost.

Protocol packing can also reuse a native Xorb with identical ordered chunks but
a different compression encoding. Reuse verifies the retained container's full
content and native identity, then records its actual packed offsets. This avoids
the standalone-chunk fallback for this case without rewriting the existing
container. It applies only when the chunk boundaries and resulting Xorb identity
match; it does not establish physical deduplication across arbitrary ingestion
layouts or remove the legacy decoded copies retained for rollback compatibility.
An encoding conflict requires a complete retained-object verification read, so
this behavior alone does not establish lower request cost for every workload.

With SQLite metadata and real MinIO, direct packing reduced upload requests for
32 unique one-MiB objects from 1,144 to 96. It replaced 540 individual chunk
PUTs and 540 chunk GETs with 32 container PUTs and 32 complete verification GETs;
32 bucket-operation requests remained: one legacy direct-object cleanup delete
per successful PUT. This preserves compatibility with older object layouts;
probing first would replace each absent-object delete with a HEAD rather than
reduce request count. Temporary object bytes fell from
67,277,422 to 33,588,512.

Two untraced pairs per workload measured server RSS at 10 ms intervals and
process I/O counters, with eight clients and SQLite on NVMe-backed Btrfs:

| Upload workload | Before | Direct packing |
| --- | --- | --- |
| 32 × 1 MiB: sampled peak RSS | 50–52 MiB | 45 MiB |
| 32 × 1 MiB: server CPU | 0.42–0.45 s | 0.25–0.27 s |
| 8 × 36 MiB: sampled peak RSS | 393–509 MiB | 185–255 MiB |
| 8 × 36 MiB: server CPU | 2.45–2.49 s | 1.18–1.22 s |
| 8 × 36 MiB: S3 upload requests | 9,474 | 40 |
| 8 × 36 MiB: process write traffic (`wchar`) | 587 MiB | 293 MiB |
| 8 × 36 MiB: process disk-write accounting | 7.7–7.8 MiB | 9.2–9.4 MiB |

The measured request saving did not increase peak RSS, but it did not reduce
every I/O counter: larger-upload disk-write accounting increased. `wchar`
includes sockets and files; it is not a disk-only measure. Disk reads were
cached, so these runs cannot establish cold-storage read savings. Local object
packing also adds complete-container verification reads while removing raw
staging writes. All resource-run reads, GC, released-v1.12.0 rollback reads and
SQLite checks passed. These shared-host runs quantify these fixtures, not
production scale-out or a universal memory ceiling.

#### Fsck reads and validation reuse

An additional fsck comparison against real MinIO used 32 one-MiB direct-packed
files, including their latest and version records. Reusing one verified
disk-backed container snapshot for adjacent references reduced container GETs
and HEADs from 1,080 each to 64 each. Total fsck S3 requests fell from 3,242 to
1,210, and object-store response traffic from 1,133,627,779 to 67,177,571 bytes.
All 1,080 member-object HEAD checks remained; range, length and required-member
validation were retained. The snapshot is discarded between records and when
the container changes. Both versions completed clean fsck, GC, released-v1.12.0
reads and SQLite integrity checks. This reduces repeated remote reads and
temporary-file copies without retaining container payloads in RAM.

A further fsck change uses the fully validated chunk descriptors instead of
decoding the temporary container again for every reference. On a local fixture
with 1,094 chunk references, process-wide `read()` calls fell from 185,391 to
17,181; both runs reported zero integrity issues. These are syscall counts,
not physical disk-byte or cloud-billing measurements. Full initial validation
still hashes every decoded chunk and verifies boundaries, lengths and the
container commitment. Per-reference range/length checks and individual member
object checks remain. Only verified metadata is retained within a record;
the temporary container file can be removed immediately after validation.

Native-Xet container inspection also avoids a separate container HEAD: one full
GET supplies presence, length and bytes for the same disk-backed verification.
Standalone native members use bounded full GETs instead of HEAD followed by GET,
retaining the 2 MiB limit, response-length validation and native content hashes.
An owned MinIO regression with an existing member observes two GETs and no HEADs,
versus two GETs and one member HEAD after the earlier container optimization.
Missing members also use GET, so their request count does not decrease. Oversized
members retain a metadata probe for their precise length diagnostic. Required
missing members, corrupt member bytes and corrupt containers still produce
findings; absent optional members remain valid. These changes reduce requests,
not payload bytes, and do not establish a throughput gain.

Ordinary standalone chunks use the same bounded reader. A MinIO fixture with
one fixed-size chunk now uses one GET and one optional-container HEAD, versus
one GET and three HEADs with the preceding scanner. The original-code control
reproduced those three HEADs before the final implementation was restored.
The layout probe remains for legacy single-chunk CDC records; ordinary fixed
chunks and native terms no longer perform that unnecessary probe. Missing and
corrupt chunks, oversized objects, legacy LZ4 data and excessive declared
decompression sizes retain their checks. This is a request-count reduction for
the measured FSCK path, not an upload or download throughput result.

The final binary also passed a PostgreSQL-metadata/local-object drill with
eight concurrent 36 MiB uploads, identical overwrites, GC and fsck. It retained
16 containers and no redundant raw objects. All 24 full-body reads passed,
including eight reads using released v1.12.0 after maintenance. This is a
single-host compatibility/correctness check, not a cluster scaling result.

Native missing-copy resolution also checks the derived descriptor and its
complete ordered member index. For an existing PostgreSQL descriptor, this
lookup now uses one statement instead of four. A lateral subquery reads at most
8,193 ordered member rows alongside the descriptor in one PostgreSQL statement
snapshot; the extra row detects an index exceeding the 8,192-member limit.
This removes the explicit read transaction's begin/commit and the second read
round trip without adding a cache. Owned PostgreSQL command logs for the same
three tests showed 137 combined lookups in place of 137 explicit read
transactions and their separate descriptor/member reads. The eight additional
member reads used by repair remained unchanged. This does not reduce
source-object integrity checks, prove lower disk writes, or establish an
end-to-end throughput improvement. Qualification includes
8,192 members, final-member corruption, excess rows and concurrent atomic
descriptor/member updates.

An additional synthetic-index check used 1,048,576 member rows across 128
containers. The maximum-size lookup read its 8,192 members through the index,
with no temporary block reads or writes at either 4 MiB or 64 KiB `work_mem`
on PostgreSQL 16. A warm shared-host Rust debug-build check, with ten warmups
and 100 samples per trial, measured median lookup times of 81.3 / 86.5 / 85.8 /
85.3 ms in candidate/previous/previous/candidate order. Tail latency was mixed.
These are index-only component measurements, not native payload verification,
release-build throughput, production scaling or an NVMe-endurance result.

#### Small metadata reads

Small-object reads used for OCI media types and legacy metadata now obtain
length and bytes from one S3 GET instead of HEAD plus GET. An HTTP-provider
fixture checks GET-only behavior for success, oversized objects, missing
objects and denied reads. The caller retains its 64 MiB buffering limit;
response tests reject partial ranges and short or overlong bodies. This removes
one request per successful affected read; missing objects still need one request.
No application-throughput improvement has been
measured for this separate change. Local object reads retain their existing
path, and no disk staging or additional payload copy is introduced.

#### Protocol integration comparison with v1.12.1

An October 24, 2026 shared-host comparison used released v1.12.1
(`3a68b53d`) and candidate production code `fd6956a6`, with harness `d21ce4b9`.
Three paired runs alternated binary order. Each used one API replica limited to
one CPU and 1 GiB, disposable PostgreSQL/MinIO, 16 clients, and 128 × 1 MiB
objects. Repeated-data lanes used four copies of each unique object.

Median complete-response throughput in MiB/s:

| Workload | v1.12.1 | Candidate | Change |
| --- | ---: | ---: | ---: |
| Unique uploads | 202.37 | 210.93 | +4.2% |
| Repeated uploads | 237.43 | 236.54 | -0.4% |
| Unique mixed upload/download | 355.74 | 362.25 | +1.8% |
| Repeated mixed upload/download | 365.86 | 352.45 | -3.7% |
| Unique mixed traffic with GC | 265.57 | 238.02 | -10.4% |
| Repeated mixed traffic with GC | 309.69 | 291.97 | -5.7% |

All twelve lanes passed byte comparisons, including 4,608 post-GC reads across
both binaries, and reconciled MinIO trace counters. Median unique-lane GC PUT
p99 increased from 319 to 465 ms. API CPU and lifetime cgroup peak memory were
similar; PostgreSQL aggregate execution time increased, with shared maintenance
lock acquisition dominating the recorded query time. Aggregate query time
includes concurrent waits and nested statements; it is not elapsed wall time.

In the first unique-data GC lane, backend attempts fell from 1,062 to 930,
while traced response bytes rose from approximately 269 to 403 MB. The candidate
verifies container payloads instead of accepting unauthenticated member-cache
sidecars. Fewer requests therefore do not establish lower backend transfer cost
or faster GC. Restoring trust in those sidecars would sacrifice the integrity
property this candidate is intended to preserve.

These approximately one-second phases identify a regression to investigate,
not a sustained performance qualification. Other host activity was not excluded.
Cgroup memory includes page cache and is not process RSS. This comparison does
not measure native Xet, cloud cost, multi-host scaling, or the external report's
18 MB/s upload and 60 MB/s download. A throughput fix is not established by these
results. Repeat with longer contention workloads and profile before changing
publication or maintenance safety checks.

A follow-up removes a second decompression pass when GC only needs the member
hashes already produced by full container validation. A separate diagnostic
repeated the same 128-object mixed workload twenty times with continuous GC,
in before/after and after/before order. Per-run median GC CPU was 0.360 seconds
before and 0.350–0.355 seconds after; median GC peak process RSS was
32.7–32.8 MiB before and 31.3 MiB after. GC elapsed time was effectively unchanged.
Throughput changes had opposite signs across the two pairs, so these shared-host
results do not establish an overall speedup. All four runs verified their
responses, including 1,536 post-GC reads. Full payload verification remains
mandatory; neither S3 transfer reduction nor increased NVMe lifetime is claimed.

A further follow-up validates contiguous container members sequentially and
uses a fixed 64 KiB read buffer when GC scans their materialized source. It still
decodes and hashes every member, checks the footer boundaries and verifies the
container commitment. No persisted format or metadata migration changes.

Summary-only `strace` measurements of the same preloaded PostgreSQL/MinIO GC
scan, before concurrent traffic, recorded:

| GC process syscalls | Before | After | Reduction |
| --- | ---: | ---: | ---: |
| `read` | 42,944 | 17,266 | 59.8% |
| `lseek` | 3,332 | 1,228 | 63.1% |

Both instrumented runs passed 384 post-GC byte comparisons. Separate,
uninstrumented twenty-iteration runs used before/after and after/before order;
all four passed another 1,536 post-GC comparisons. Per-run median GC CPU was
0.340–0.350 seconds before and 0.330–0.345 seconds after. GC peak process RSS
remained approximately 31–31.5 MiB. Mixed throughput was 258–261 MiB/s before
and 233–260 MiB/s after, with no consistent improvement across pairs. These
shared-host results establish fewer local read/seek calls, not a throughput fix.
The buffer adds at most 64 KiB per active scan; this change does not remove
source verification reads, S3 requests or scratch-file writes. Syscall counts
are not physical device writes or a measurement of NVMe lifetime.
The optimized candidate also passed five actual v1.12.1 upgrade/rollback tests
on local and PostgreSQL/MinIO storage, and a stock Hub 2.2.0 / hf_xet 1.7.0
1 GiB native upload/download with complete SHA-256 verification. These are
compatibility checks, not additional paired performance measurements.

The next follow-up removes a redundant container HEAD from GC of packed generic
records. The full validation GET supplies the existence result, including a
missing source; only that GC pass reuses the observation across latest/history
records. Corrupt or unreadable sources still stop collection. The legacy public
member-visitor API retains its existing return type and behavior.

Against `8f5d60bc`, the same preloaded 128-container PostgreSQL/MinIO scan used
414 S3 attempts before and 286 after (30.9% fewer): container HEADs fell from
128 to zero, while all 128 successful container GETs and the 134,435,501 traced
response bytes remained unchanged. Both runs reconciled MinIO traces with
server counters and passed 384 post-GC byte comparisons.

Four separate twenty-iteration contention runs, in before/after and after/before
order, measured per-run median mixed throughput of 259.93–261.19 MiB/s before
and 267.71–281.49 MiB/s after: improvements of 3.0% and 7.8% in the two pairs.
Median GC elapsed time fell from approximately 0.718 to 0.666–0.668 seconds;
GC CPU fell from 0.330–0.340 to 0.310–0.320 seconds, with peak process RSS
remaining approximately 31 MiB. All four reconciled their traces and passed
another 1,536 post-GC byte comparisons. Five actual v1.12.1 upgrade/rollback
tests also pass; native local and PostgreSQL/MinIO cases now run candidate GC
before verifying old and new content with the released binary.

These shared-host results support this specific request reduction and an
encouraging contention improvement. They do not establish production scaling,
a general p99 improvement, or a new paired throughput qualification against
released v1.12.1. Full source verification and existing ownership/deletion
barriers remain mandatory.

An additional paired comparison tests production code `7b540c35` against the
actual released v1.12.1 binary, using that same fixed-record contention fixture.
Four uninstrumented runs use released/candidate/candidate/released order, each
with twenty mixed-traffic phases and continuous GC. No build or profiler ran
alongside these measurements; unrelated host activity was not excluded.

| Per-run median | Released, first | Candidate, first | Candidate, second | Released, second |
| --- | ---: | ---: | ---: | ---: |
| Mixed throughput (MiB/s) | 297.69 | 280.31 | 266.09 | 295.09 |
| PUT p99 (ms) | 159.76 | 205.64 | 216.52 | 141.88 |
| GET p99 (ms) | 62.15 | 63.53 | 63.98 | 63.31 |
| GC CPU (seconds) | 0.180 | 0.300 | 0.300 | 0.175 |
| GC peak process RSS (MiB) | 28.01 | 31.18 | 31.10 | 28.09 |

The two pairs retain a 5.8% and 9.8% throughput deficit against v1.12.1.
PUT p99 remains higher. The candidate's settling scan uses 286 S3 attempts
versus 798 for the released binary, but source GET counts remain 128 and traced
response bytes remain approximately 134.4–134.5 MB. This small generic-upload
fixture does not reproduce the transfer-byte increase of the earlier fixture.
Full payload verification still costs more GC CPU than the released path;
lower request counts do not resolve that contention cost. Every run reconciles
its MinIO traces and passes 384 post-GC byte comparisons (1,536 total).
These results leave the latest-stable throughput gate open. The latency rows
are medians of each phase's p99, not pooled p99 values for the complete run.
They do not qualify production scaling, native Xet throughput or disk endurance.

A subsequent GC profile of the complete CLI scan recorded 953 cycle samples
with no lost samples. Approximately 14% of inclusive sampled cycles passed
through temporary-file writes (`shmem_file_write_iter`), and approximately 4%
through temporary-file eviction. This includes CLI startup and is a diagnostic
profile, not a timing comparison. It motivated using the existing bounded
materialization path with a fixed 2 MiB memory allowance for GC verification.
Every source is still fetched and fully validated before members are exposed;
larger sources spill to disk, and no source-health cache is introduced.

Four uninstrumented before/after/after/before runs of the same twenty-phase
fixture measured median GC CPU of 0.310 seconds before versus 0.250 after
(19% less). Peak GC process RSS increased from 31.00–31.22 to 33.76–33.79 MiB.
Mixed throughput changed from 281.78 to 291.55 MiB/s in the first pair, but from
282.65 to 251.29 MiB/s in the reverse-order pair. GC elapsed time stayed around
0.62 seconds. These inconsistent throughput results do not close the regression
against v1.12.1 or establish better tail latency. Other host activity was not
excluded; generating the earlier flamegraph overlapped the start of the first
baseline run.

Separate syscall-traced settling scans show read calls falling from 17,266 to
14,834, writes from 2,248 to 1,783, seeks from 1,228 to 76, and unlinks from 132
to four: the 128 small source snapshots no longer require scratch files. Both
scans still use 286 S3 attempts and transfer exactly 134,435,501 traced response
bytes. All six runs reconcile MinIO traces and pass 2,304 post-GC byte checks.
The scratch filesystem was tmpfs, so these syscall counts do not establish
physical NVMe writes or service life. The improvement trades a small fixed
memory allowance for fewer temporary-file operations and lower measured GC CPU;
local filesystem sources continue to be read directly.
All 938 server-core, adapter and GC tests pass, as do strict adapter/GC Clippy
checks and five actual v1.12.1 upgrade/rollback cases, including native local
and PostgreSQL/MinIO reads by the predecessor after candidate GC. A regression
case verifies that corruption in the final member of a source larger than the
memory allowance stops collection before any member is exposed.

A fresh profile of `615b4fd1` records 834 GC cycle samples with no lost samples.
About 13.3% of self samples fall in certificate PEM/base64 decoding. This scan
includes CLI startup. The adapter eagerly constructed a separate HTTP client
for multipart completion even when a read or GC never used it. That client now
initializes on first use, shared across adapter clones, before creating remote
multipart state. TLS verification, conditional completion headers and retry
policies remain unchanged. A separate API profile identifies MD5 compression
as its largest CPU consumer (28.4% of self samples); required S3 checksums and
ETags are retained. These profiles identify costs, not predicted speedups.

Six twenty-phase before/after runs measure median GC CPU falling from 0.260 to
0.240 seconds (about 8% less), with GC wall time effectively unchanged at
0.617–0.620 seconds. Peak GC RSS ranges from 33.28–33.53 MiB before and
33.06–33.39 MiB after. Mixed throughput varies from 273.80–286.95 MiB/s before
and 248.14–288.41 MiB/s after, so this does not establish a throughput or tail
latency improvement, or close the v1.12.1 regression. A formatting check overlapped
the first baseline; the final four ran in after/before/before/after order after
all task builds, tests and profilers stopped. Other host activity was not excluded.
Every settling scan retains 286 S3 attempts and exactly 134,435,501 traced
response bytes. All six reconcile their request traces and pass 2,304 post-GC
byte comparisons. All 515 storage tests and six actual v1.12.1 upgrade/rollback
cases pass, together with strict storage Clippy. This is a startup CPU reduction;
it does not reduce S3 request charges or establish physical disk savings.

A follow-up comparison at `f02ba074` controlled maintenance work: exactly one
overlapping GC per phase, twenty phases per run, against the released v1.12.1
binary. The continuous-GC fixture instead waits 0.5 seconds after each completion,
so faster scans can change the number of GCs performed during a comparison.

| Run order | Binary | Median MiB/s | Median phase PUT p99 (ms) |
| --- | --- | ---: | ---: |
| 1 | v1.12.1 | 238.58 | 387.84 |
| 2 | f02ba074 | 242.16 | 376.72 |
| 3 | f02ba074 | 258.93 | 358.23 |
| 4 | v1.12.1 | 256.60 | 359.07 |

These controlled runs do not establish a throughput regression: paired throughput
changes are +1.5% and +0.9%, with comparable tail latency. GC CPU remains higher,
at 0.23–0.24 seconds versus 0.18 seconds. All four reconcile MinIO traces and
pass 384 post-GC byte comparisons each. Separate 20 ms lock sampling observes
steady exclusive barrier spans around 260–300 ms for the candidate and 260–280 ms
for v1.12.1; this does not justify changing the barrier's safety contract.
The shared-host fixture still does not qualify production scaling or other workloads.

The larger-file check on October 25 compares `f815935a` with the actual released
v1.12.1 executable: eight 36 MiB objects, four clients, one API replica limited
to one CPU and 1 GiB RAM, and three repetitions for both unique and repeated
data. The candidate ran first, followed by the predecessor; this was not an
alternating-order comparison. Other host activity was not excluded.

| Measurement | Unique: v1.12.1 | Unique: candidate | Repeated: v1.12.1 | Repeated: candidate |
| --- | ---: | ---: | ---: | ---: |
| Median mixed throughput with GC (MiB/s) | 311.94 | 273.89 | 326.56 | 317.44 |
| Backend requests across two settling GCs | 227 | 127 | 107 | 79 |
| Backend response bytes across those GCs (decimal MB) | 302.60 | 604.53 | 75.66 | 151.14 |

Fewer requests therefore do not establish lower total cost: this workload
transfers approximately twice as many backend bytes and has lower observed
throughput. The candidate authenticates source containers again on each GC;
saved member sidecars do not establish current source health. Reusing them as
a health cache would bypass that check. Request counts matched MinIO traces,
and all 288 post-GC byte comparisons passed across the two versions. These runs
did not reproduce the intermittent GC failures observed in earlier trials,
so their cause remains unresolved. This larger-file throughput and transfer-cost
gate stays open; the smaller controlled result above does not close it.

#### Native Hugging Face/Xet transfers

An October 24, 2026 measurement used production binary `d61d7342`,
`huggingface_hub` 2.2.0 and `hf_xet` 1.7.0, authenticated Hub/Xet frontends,
and disposable PostgreSQL/MinIO. One server process was limited to four CPUs
and 1 GiB on a shared host. Deterministic incompressible files were generated
before timing. Each client started with an empty client cache; backend and OS
caches were not cold. Times include client startup and full transfer completion.

| Logical file | Upload MiB/s | Download MiB/s | Upload backend attempts | Download backend attempts |
| --- | ---: | ---: | ---: | ---: |
| 512 MiB, three runs | 59.70–71.88 | 530.36–559.53 | 8,636 each | 22 each |
| 1 GiB, one run | 55.53 | 483.54 | 17,030 | 42 |

The 512 MiB medians were 68.50 MiB/s upload and 559.47 MiB/s download.
Every upload increased native Xorb and shard counters without using the basic
Hub body-upload path. Downloads used native Xorb transfers and matched the
original size and SHA-256. Request totals reconciled with MinIO traces.

The 1 GiB run reached 158.9 MiB server process peak RSS. Client peaks were
796 MiB during upload and 648 MiB during download; these are separate processes.
Server process-accounted writes increased by approximately 3.23 GB during upload
and 1.28 GB during download. Those Linux I/O counters do not isolate scratch
writes, PostgreSQL/MinIO writes, physical device writes or NVMe wear.

Native ingestion still retained approximately 2.002 times the logical payload:
the 1 GiB fixture produced 16,736 standalone chunks and 17 Xorbs, plus shard
metadata. Upload traces received 2.15 GB and returned 1.07 GB; download traces
returned 1.28 GB. This independently confirms redundant native chunk storage
and read amplification remain optimization targets. Generic direct-packing
savings do not establish equivalent native-Xet savings.

These loopback measurements are not a paired v1.12.1 speedup, a reproduction of
the external 18/60 MB/s report, sustained concurrency qualification, or evidence
of multi-host scaling. They establish successful large native transfers under
this configuration, rather than resolution of the overall throughput issue.

An October 25 comparison isolates bounded native member writes against
`4ddf37fb`, using the same 512 MiB fixture, client versions, PostgreSQL/MinIO,
four-CPU server limit and 1 GiB memory limit described above. Three accepted
runs per binary comprise an A/B/B/A sequence and a B/A pair. A compiler monitor
rejected later samples when another project resumed compilation; those samples
are excluded. This remains a shared-host measurement with warm backend caches.

| Upload metric | Sequential baseline | Bounded member writes |
| --- | ---: | ---: |
| Median throughput | 46.63 MiB/s | 67.15 MiB/s |
| Server peak RSS range | 127.2–153.3 MiB | 127.9–151.3 MiB |
| Median server CPU | 4.14 seconds | 4.28 seconds |
| S3 requests | 8,636 each | 8,636 each |
| Median process-accounted writes | 1.612 GB | 1.612 GB |

The candidate overlaps at most four S3 member writes, retaining at most 256 KiB
of decoded pending members or one larger member alone. Local member writes stay
serial. Source validation and all member writes finish before container
publication. The experiment preserves standalone chunk copies and does not
reduce retained storage, API request charges or measured disk writes.

All six native downloads matched the source size and SHA-256. Download throughput
varied from 242–716 MiB/s for the baseline and 349–670 MiB/s for the candidate;
these short runs do not establish a download improvement. The candidate also
passes twelve stock-client SIGKILL/retry scenarios across local and
PostgreSQL/MinIO storage, plus seven actual v1.12.1 upgrade/rollback tests.
These checks do not close the wider throughput, concurrency or recovery matrix.

#### PostgreSQL shared-hash authorization

Signed reconstruction downloads can now check the named file's current
authoritative record before searching every file sharing its container. In a
warm, unoptimized October 25 diagnostic with one million valid records, a
range found in the last matching record took 28,639.743 ms through repository
discovery and 0.301 ms through the exact-file proof on the same dataset.
At 100,000 records, the exact-file proof took 0.360 ms. Each result was checked;
combined ranges, denials and committed revocation also passed their existing
fallback checks. These are individual synthetic samples on a shared host with
other qualification work running, not production latency or HTTP throughput.

The shortcut requires a complete current-file proof for the exact repository,
container and requested bytes. It does not authorize from the signed file name
alone. A missing or replaced file, historical-only ownership, a combined proof,
or footer access can still require repository discovery. No metadata format,
object layout or integrity check changes; a failed shortcut adds one lookup.
This does not establish constant-time authorization for every request or measure
S3, disk, WAL or monetary savings.

To compare both lookup paths on one disposable, owner-migrated PostgreSQL
database with `DATABASE_URL` exported:

```bash
cargo test --locked -p shardline-server --lib --all-features \
  record_store::xorb_range::scale::signed_file_hint_authorization_scale \
  -- --ignored --exact --nocapture
```

This diagnostic seeds latest records so the named file is directly readable;
the historical-record diagnostic below remains separate. Use the same build
profile for comparisons. Production qualification must measure optimized
executables, concurrency and tail latency separately.

An October 25 synthetic diagnostic exercises the application range proof with
2, 10,000, 100,000 and 1,000,000 valid file records sharing one member hash.
The first authoritative record covers the requested bytes. Each variant uses a
fresh PostgreSQL 16 container, the real owner migrations and the same
single-connection pool. Three checks per size include a subsequent query on that
pool, exposing work required to drain an unfinished result set.

| Matching records | Streaming lookup, median ms | Bounded first lookup, median ms |
| --- | ---: | ---: |
| 2 | 0.575 | 0.683 |
| 10,000 | 41.936 | 0.551 |
| 100,000 | 423.536 | 0.487 |
| 1,000,000 | 3,050.553 | 0.462 |

PostgreSQL-container outbound traffic across the complete isolated diagnostic
fell from 2,453,938,377 to 111,433 bytes. These counters include fixture setup,
server-side seeding, all checks and schema cleanup; they are not per-request payloads.
Both runs returned the correct proof and reused the connection successfully.
The earlier parsing-only optimization still transferred the full result set;
the bounded query avoids that transfer when its one record proves access.

This is a shared-host, debug-build diagnostic with warm repeated checks and
uncontrolled host activity. It is not an accepted production throughput
comparison, a p95/p99 result, or a cold-I/O measurement. Small candidate sets do
not show an established speedup. A partial first proof is discarded before a
fresh full traversal, which costs an extra query; multi-record and footer proofs
still need their existing complete authorization checks. This does not measure
S3 requests, source-object health, physical disk writes or storage savings.

To reproduce, use a disposable PostgreSQL database with the owner migrations
applied and `DATABASE_URL` exported:

```bash
cargo test --locked -p shardline-server --lib \
  record_store::xorb_range::scale::shared_hash_range_authorization_scale \
  -- --ignored --exact --nocapture
```

The diagnostic is explicitly ignored in ordinary unit runs. Set
`SHARDLINE_RANGE_SCALE_MAX_RECORDS=10000` for its smaller two-size run; supported
ceilings are 2, 10,000, 100,000 and 1,000,000. It creates and drops its own schema.

The first-match diagnostic does not establish the cost of authorizing a range
whose reference occurs later, combining several records, or denying access.
Run the separate diagnostic against the same disposable setup to cover those
cases, including wrong scopes, missing hashes and committed reference deletion:

```bash
cargo test --locked --release -p shardline-server --lib \
  record_store::xorb_range::scale::shared_hash_late_and_negative_authorization_scale \
  -- --ignored --exact --nocapture
```

It verifies each result and subsequent reuse of the same one-connection pool.
Each proof has a 120-second deadline. The fixture deliberately gives many valid
records the same container hash; this is a fanout diagnostic, not an HTTP load
test or a measurement of cold storage. Compare equivalent build profiles and
record late, combined and denied proofs separately from early matches. A fast
early match must not be presented as bounded lookup cost for all ranges.

An optimized, all-features diagnostic on October 25 compared the per-record
blocking handoff with bounded conversion batches. Both runs seeded one million
valid records and passed every proof, denial, committed-revocation and same-pool
reuse assertion:

| Proof at one million records | Per-record handoff, ms | Bounded batches, ms |
| --- | ---: | ---: |
| Match in last record | 9,114 | 3,489 |
| Combined range | 9,165 | 3,637 |
| Uncovered range denied | 9,805 | 3,637 |
| Deleted grant denied | 9,029 | 3,246 |

The fallback still reads and verifies authoritative records from one SQL
snapshot. Batching amortizes blocking-pool handoffs for up to sixteen already
available rows, targeting 64 KiB of raw row values plus one oversized row per
admitted batch. It retains the existing global admission budget, does not wait
for speculative network rows, and preserves ordered errors and early proof
completion. These bounds describe the owned batch, not total process RSS or
SQL driver's existing buffers.

These are single shared-host measurements, with short profiling attachments
overlapping some proofs and unrelated activity during fixture setup. Complete
test duration was about 198 seconds before and 196 seconds after; setup dominates
that total. The table measures a specific fallback, not upload/download throughput,
production tail latency or backend savings. The scan remains proportional to
candidate count, and missing-hash queries are unaffected by conversion batching.

Three additional alternating release-build pairs at 100,000 records all passed.
Their median late/combined/uncovered proof times were 974/971/1,011 ms before
and 409/418/390 ms after. Whole-process user plus system CPU medians fell from
4.43 to 1.54 seconds. Median peak RSS increased from 20,084 to 21,108 KiB;
individual paired increases were 176–1,044 KiB. Resource figures include fixture
orchestration inside the test process, but exclude PostgreSQL and measure neither
object-store I/O nor production concurrency. This exchanges a small bounded
buffer for fewer thread handoffs; it does not reduce the database result set.

The separate first-candidate probe uses an unnamed statement so PostgreSQL can
plan for the requested hash instead of reusing a generic plan trained on common
content. Its authorization join and connection-wide planner settings are
unchanged. In a subsequent optimized million-record run, wrong-scope and
missing-hash denials took 0.592 and 0.421 ms, respectively; fallback proofs still
took 3.3–3.5 seconds. This does not eliminate proportional fallback work.

Three alternating 100,000-record pairs measured these trade-offs against the
batched implementation with its cached first probe:

| Measurement | Cached probe | Unnamed probe |
| --- | ---: | ---: |
| Median wrong-scope denial, ms | 18.594 | 0.558 |
| Median missing-hash denial, ms | 4.002 | 0.436 |
| Median warm successful probe, ms | 0.1575 | 0.486 |
| Median application user + system CPU, seconds | 1.49 | 1.50 |
| Median peak application RSS, KiB | 20,476 | 30,828 |

The cached missing-hash samples varied from 3.073 to 183.081 ms. Replanning costs
an extra round trip and increases successful-probe latency in this fixture.
Separate two-record tests reproduced the RSS difference. Allocation profiling
measured almost identical peak heap allocations (about 590 KB); sampled process
maps placed the additional resident memory in executable file mappings, with
about 0.75 MiB anonymous resident memory in both processes. These observations
distinguish executable residency from heap growth; they do not establish equal
RSS, production memory requirements or PostgreSQL CPU cost. All paired tests
passed the same coverage, denial, revocation and connection-reuse assertions.

#### Denied native Xorb HEAD requests

Native Xorb HEAD now checks published repository references before reading
backend metadata. With an uploaded but unpublished 64 KiB Xorb, the original
MinIO control sent one provider HEAD before returning 404; the new path sends
none. Disposable local-metadata and PostgreSQL cases also confirm zero provider
HEAD/GET requests for another repository's object and an unreferenced missing
hash. Authorized HEAD retains HTTP 200, the correct `Content-Length`, one provider
HEAD and no GETs.

These are request counts, not a total-cost or throughput benchmark. Missing
objects now encounter the repository lookup first; the metadata fallback costs
described above remain relevant. Run the actual HTTP/provider checks with:

```bash
cargo nextest run -p shardline-server --features docker --test native_shard_reads \
  --run-ignored all -E 'test(native_head::)'
```

#### PostgreSQL publication

Separate targeted comparisons measured unchanged-record PostgreSQL WAL at
608,616 → 61,472 bytes for sixteen 36 MiB replacements (`48727f81`); small-record
savings were negligible. Reusing verified Merkle parents (`6fb93b6c` → `8670f51a`)
reduced database calls from 155,648 to 147,456 per 4,096 small-object PUT/GET pairs.
In sequential four-replica runs, median server CPU fell from 9.14 to 8.56 seconds
and PostgreSQL CPU from 15.20 to 11.86 seconds, with unchanged backend requests
and 16,384 verified post-GC reads per version. Timing remains host-dependent.

#### Duplicate S3 file and copy verification

An October 17 comparison targeted repeated file-backed S3 uploads, which still
compared an existing object with a local file through separate 256 KiB GETs.
The new path uses one full-range streamed GET, retaining complete byte equality,
the initial metadata check and conditional creation. Local reads are buffered
at 256 KiB so transport-frame sizes do not dictate disk read sizes. It adds no
temporary file and does not retain the complete object in memory.

Three alternating before/after pairs used a disposable MinIO service and three
duplicate verifications of a seeded 32 MiB object per run. Each verification
fell from one HEAD plus 128 GETs to one HEAD plus one GET: 129 to two requests
(98.4% fewer). Object response bytes were unchanged. Including the identical
seed upload, each run made 398 requests before and 17 after; all aggregate API
counts matched independent MinIO counters. Median time for the three measured
verifications, including caller-side SHA-256/BLAKE3 calculation but excluding
seeding, fell from 297.7 to 91.8 ms. These are shared-host results for this path,
not a general upload-throughput or disk-endurance measurement.

Three additional pairs measured whole-process peak RSS with GNU time: 43.3–43.6
MiB before and 43.1–43.9 MiB after. These peaks include fixture construction and
seed upload; they do not isolate comparison buffers or establish a production
memory ceiling. The request reduction does not remove the complete local or
remote byte read needed to prove equality, so no disk-read-byte or SSD-wear
saving is claimed for this change.

The HTTP-provider regression exercises both file-backed upload entry points
with a 32 MiB existing object and asserts one HEAD plus one GET. Negative cases
cover prefix/tail corruption, shorter/longer objects, missing objects, denied
reads, incorrect response ranges and a body truncated after valid headers.
File length is checked through the opened handle and at EOF. A single response
also avoids combining remote ranges fetched from different object revisions.

To reproduce the duplicate-file workload against a disposable bucket:

```bash
S3_BENCH_ENDPOINT=http://127.0.0.1:9010 \
S3_BENCH_BUCKET=shardline-bench \
cargo bench -p shardline-storage --bench s3_put_file_fixed -- existing --iterations 3
```

Set `S3_BENCH_ACCESS_KEY` and `S3_BENCH_SECRET_KEY` for the fixture. The benchmark
seeds its own unique key before timing and requires each measured call to find
that existing object. Delete the disposable bucket after collecting results.

Existing-copy verification uses the same streaming principle for two remote
objects. Previously it read each object in separate 256 KiB ranges. A 32 MiB
comparison therefore required 256 GETs plus a destination HEAD. The current
helper uses two complete GETs and obtains length from those exact responses,
without a destination HEAD for nonempty objects. Empty objects retain the
metadata-only check. A missing destination returns a conflict after one GET,
without fetching the source. The HTTP-provider regression asserts that
request count on equal 32 MiB objects and rejects a final-byte mismatch without
changing either object. Separate stream tests cover independent frame sizes,
empty frames, early/tail mismatches, short/long bodies and transport failures.
Invalid-length/range, truncated, denied and missing response cases are rejected.
The existing MinIO copy/idempotency/conflict test also passes. These checks
establish request reduction and comparison behavior, not a measured throughput
or RSS improvement. Both objects still need to be read completely on success;
no backend-byte, local-disk-write or SSD-endurance reduction is claimed.
Comparison retains one transport frame per object, adds no temporary file and
does not buffer whole objects. Each response must declare the expected complete
length and range, and both bodies must reach EOF at that length.

#### Hub PostgreSQL file metadata

Hub file metadata writes now use INSERT batches of at most 1,000 entries inside
the existing transaction. A repeated path starts another statement, preserving
ordered updates instead of dropping intermediate entries. Invalid sizes still
abort the complete transaction, including batches already executed.

A PostgreSQL statement-trigger counter observed three INSERTs for 1,003 input
entries containing repeated paths; the previous loop issued one INSERT per entry.
This measures fewer database round trips, not a throughput, WAL, storage-cost or
SSD-endurance improvement. Statement-level triggers run once per batch.
The same contract verifies that unchanged replay preserves the row's PostgreSQL
version and that a later invalid-size entry rolls back earlier writes. An invalid
intermediate duplicate is rejected even if a later entry would replace it.
An injected PostgreSQL trigger rejection in a later batch also rolls back all
earlier inserts and updates; repeated-path updates preserve the final LFS flag.

#### PostgreSQL deletion

An October 17 PostgreSQL deletion comparison replaced a listing-row SELECT
followed by DELETE with `DELETE ... RETURNING` in both S3 metadata deletion
paths. The removed row still supplies the complete before-state for reliability
evidence. Existing advisory/ownership locks, transactions, evidence verification,
deferred database gates and legacy record-deletion fallback remain in place.
An invalid returned row or invalid evidence aborts the transaction.

Two alternating before/after pairs ran the existing create/delete/delete-absent
regression against a disposable PostgreSQL 16 instance with
`pg_stat_statements.track=all`. The two deletion calls used two DELETEs and two
listing-row SELECTs before, and two returning DELETEs afterward. An unrelated
SELECT during setup remained. Across the matched listing SELECTs and DELETEs,
including that setup SELECT, shared-buffer hits fell from 12 to ten; both
versions recorded zero physical block reads and 54 WAL bytes. This establishes one fewer SQL round trip
per deletion attempt, including absent objects, rather than a WAL or SSD-wear
saving. It does not establish a production latency improvement. The S3 evidence
corruption regression exercises both deletion entry points and checks that the
listing row survives their failed transactions.

The same final audit removed a SELECT-before-DELETE from unconditional
PostgreSQL quarantine-candidate and retention-hold deletion. Each now returns
the removed row from DELETE for the unchanged lifecycle evidence checks,
saving one query per attempt. DELETE retains the row lock until transaction
completion, and retention-hold advisory locking remains in place. Conditional
`if_matches` paths retain their existing checks. A real-PostgreSQL regression
covers deletion, repeated absence, recreation and rollback on corrupt evidence
for both paths. Two reversed-order PostgreSQL 16 counter comparisons of the
quarantine baseline-repair regression confirmed one fewer SELECT. Across its
matched row SELECTs and DELETEs, WAL fell from 108 to 54 bytes because the
separate `FOR UPDATE` row-lock record was avoided. This is a small fixture-level
delta, excluding evidence writes and the rest of the transaction; it is not a
50% total-WAL reduction or a disk-endurance estimate. No WAL reduction was
measured for retention-hold deletion separately.

Stale temporary-upload cleanup now batches up to 1,000 eligible objects through
the S3 bulk-delete API. An HTTP fixture checks that 1,000 keys use one POST and
that a per-key access-denied result cannot be reported as success while the
single-delete retry is also denied. Providers without usable bulk deletion
fall back to the existing single-delete API. Repeating deletion is safe only
for these unique generated temporary keys, including partially deleted batches.
Stream-result tests
reject missing, duplicate and unexpected results; concurrent cleanup and retry
tests preserve unselected objects. Existing MinIO tests retain the age and
exact reserved-namespace selection checks. Successful counts are returned only
after every batch succeeds; partial failure may delete some artifacts, with
remaining artifacts discoverable by the next sweep. Memory holds at most 1,000
selected paths in addition to listing buffers. This saves cleanup requests when
abandoned uploads accumulate, not ordinary per-upload cleanup, payload bytes or
database operations. It does not batch destructive user-object deletions across
their safety fences or change the 32 MiB packing default.

#### Unchanged SQLite payloads

A separate SQLite SQL probe with sixteen unchanged one-MiB payloads produced
the same 65,952 WAL bytes for the current payload-plus-timestamp upsert and a
timestamp-only update. Both used WAL, `synchronous=FULL`, disabled automatic
checkpointing and a truncated WAL before measurement, and passed integrity
checks. This narrow probe does not measure complete Shardline publication;
it provides no evidence for changing SQLite publication SQL to reduce writes.

#### Admission, compatibility and failure qualification

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
All 96 full-body reads matched across 36 MiB files. A separate round trip through
LFS, OCI and Bazel endpoints used the same released binary, two replicas and
36 MiB files. All 72 full-body reads matched: 12 candidate reads of released
writes, 24 released reads after candidate writes and GC, and 36 candidate reads
after rollback writes and another GC. Backend trace totals matched MinIO
counters. These checks verify the exercised representations and transitions;
other protocol and fault combinations need their own qualification.

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

#### Workload limits and remaining costs

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

Small range reads have another cost. For one incompressible 36 MiB file, a 4 KiB
read at offset zero caused MinIO to report 33,495,294 response bytes, while a 4 KiB
read at offset 33 MiB reported 4,285,994 bytes. Both versions fetched the complete
selected container to verify integrity. The candidate reduced each read from
two backend requests to one; response bytes stayed unchanged. A full 36 MiB read
used four requests on main-equivalent code and two on the candidate. All six
responses matched the requested bytes and backend trace totals matched MinIO
counters. These are provider-reported transfer counts, not cloud billing figures.
Measure small random reads separately from full downloads when estimating cost;
fewer requests do not necessarily mean fewer backend bytes.

In that historical fixture, the offset-zero request transferred
about 8,178 backend bytes per logical byte returned. It is not a measured average
for random reads.

The current candidate authenticates small explicit ranges in large S3-backed
containers using their existing member commitments. It checks the footer against
the independently trusted Xorb root, then decodes and verifies each selected
member's hash and length before returning its bytes. Packed offsets alone are
never trusted. Full downloads continue to validate the complete container;
legacy footers and standalone chunks retain the existing read path.

A range proof establishes the requested bytes, not the health of unrequested
payloads. Corruption outside those members remains detectable by full downloads
and FSCK. Selected corruption fails before the first response bytes, or terminates
an already-started stream if encountered in a later member. This distinction is
intentional: an authenticated range read is not a complete-container scrub.
Admission bounds footer parsing, selected-member reads and decoding, including
slow readers and cancellation. Measure the extra footer/member GETs as well as
transferred bytes; lower transfer amplification alone does not prove lower bills.

An owned MinIO qualification uses one incompressible 4 MiB container with 64 KiB
members. Each tested 4 KiB read (prefix, middle and tail) fetched 68,744 provider
body bytes in three GETs, compared with 4,198,008 bytes in one complete-container
GET: 98.4% less backend transfer. These are real proxy-observed response-body
counts; the test also checks exact returned bytes and rejects selected corruption.
Full downloads still use one GET. This fixture is smaller than the historical
36 MiB benchmark above and does not establish a latency, RSS or cloud-bill result.
An additional signed Botocore workflow verifies full and small ranged reads
against PostgreSQL/MinIO, recreates the server with an empty local root, and
repeats them. After a separately verified warm-up read, the full GET and three
4 KiB ranges use ten backend GETs and less than 5 MiB of response bodies.
This excludes first-request recovery traffic and is a server recreation test,
not a process-kill recovery measurement. Partial S3 responses omit whole-object
checksum headers; selected-member verification remains enforced by Shardline.
Run the mandatory Docker-backed check with:

```bash
cargo test --locked -p shardline-server --features docker --lib \
  authenticated_ranges_on_real_minio -- --ignored --nocapture
```

An October 17 experiment compared the unchanged 32 MiB raw packing cap with
a temporary 4 MiB cap, using one incompressible 36 MiB file, PostgreSQL and real
MinIO. Two pairs reversed execution order and produced identical request and
transfer counts. The experiment changed packing size, not the read verifier or
persisted record format. Both binaries still verified each selected container
in full. The table reports MinIO response bytes and observed API attempts.

| Operation or footprint | 32 MiB cap | Experimental 4 MiB cap |
| --- | --- | --- |
| 4 KiB read at offset zero: backend bytes | 33,495,294 | 4,111,970 |
| 4 KiB read at offset 33 MiB: backend bytes | 4,285,994 | 4,191,077 |
| Each of those range reads: GETs | 1 | 1 |
| Full 36 MiB download: GETs | 2 | 10 |
| Full download: backend bytes | 37,781,288 | 37,782,248 |
| Upload: PUT / GET / HEAD / cleanup requests | 2 / 2 / 2 / 1 | 10 / 10 / 10 / 1 |
| Stored containers | 2 | 10 |
| Stored container bytes before GC | 37,781,288 | 37,782,248 |

The prefix read transferred 87.7% fewer backend bytes, but the later range
improved by only 2.2%. The 4 MiB cap also increased upload attempts from seven
to 31 and full-download GETs fivefold. CDC chunk boundaries mean a nominal
36 MiB file need not fit in nine containers capped at 4 MiB. This is a packing
trade-off, not selective chunk verification or a universal cost reduction.

All 20 candidate/baseline response comparisons passed, including ranges near
the 4 MiB and 32 MiB offsets and full downloads. All four deployments passed GC
and fsck; twelve additional range/full reads with released v1.12.0 passed after
GC. Trace API counts matched independent MinIO counters. These runs do not
establish tail latency, crash behavior or a uniformly sampled random-read mean.
The existing 55 download tests also passed, including rejection of corruption
outside the requested range. Checking only footer-declared hashes cannot detect
corruption in payload bytes that were never fetched.

The experiment was reverted; the branch retains the 32 MiB cap. For this file,
smaller packing adds eight PUTs and sixteen GET/HEADs per upload, plus eight GETs
per full download, while saving about 29.4 MB per measured prefix read. Whether
that lowers cost depends on the read mix, chargeable backend transfer and
compute/I/O costs. Measure those inputs before changing the default. Existing
objects retain their original layout; changing the cap would affect new writes
without automatically improving old-object range reads.

## Operational cost per logical TB

Report operational cost alongside throughput and latency:

`cost per logical TB = attributable operating cost / (verified logical bytes / 10^12)`

Keep upload and download results separate, and give a combined result only for
an explicitly stated workload mix. Count each acknowledged logical upload once
and each successfully verified download's returned bytes once. Failed attempts
and retries contribute cost but no additional successful logical bytes. For
ranges, count returned bytes, not the full object size or backend transfer.

| Cost component | Evidence to collect |
| --- | --- |
| Object storage | Retained and temporary byte-hours over the measured retention/GC cycle |
| API requests | Attempts by operation and storage class, including retries and failures |
| Network | Backend and client bytes, separating chargeable routes from free transfer |
| Database | Query/lock load, WAL and storage I/O, with attributable service cost |
| Compute | Server and backend CPU/memory consumption with allocated infrastructure cost |
| Local hardware wear | Isolated host-write deltas and an explicitly stated endurance/replacement model |

Apply provider, region, tier and contract prices to the measured quantities;
request counts alone are not a bill. Avoid counting database compute twice when
it is already included in infrastructure cost, or adding estimated SSD wear to
a rental price that already covers hardware replacement. For owned hardware,
an endurance-based allocation is a model, not a prediction of drive failure.

Publish the workload's object sizes, uniqueness/compressibility, read ranges,
protocols, replica count, cache state and retention policy with the result.
Include maintenance, temporary storage and unsuccessful work over the same time
window, with correctness checks passing. Storage cost per retained TB-month
remains a separate useful metric; it cannot be compared directly with cost per
TB processed without a stated retention period and access frequency.

The measurements above establish individual resource reductions. They do not
yet establish an all-in production cost per TB, a cluster scaling guarantee or
an SSD replacement saving. Small-range read amplification must be represented
in the workload rather than hidden by full-download throughput.

## Native Xet backend-cost check

Run the stock Hub/Xet client against disposable PostgreSQL and MinIO:

```bash
python3 scripts/shardline/validate_native_cost.py \
  /absolute/path/to/shardline /absolute/path/to/client-env/bin/python \
  /private/path/outside-checkout/native-cost --file-mib 36
```

The runner needs Docker, `mc`, boto3 in its own Python environment, and
`huggingface_hub` plus `hf_xet` in the client environment. It creates one
CPU-limited combined-role Hub server with private backend storage. Each upload
and download uses a fresh client process/cache. Both downloaded files must
match their SHA-256, and upload counters must establish actual native Xorb
ingestion and deduplication. Missing prerequisites or failed verification fail
the run. Backend traces are reconciled against MinIO's independent API counters.
Tokens, signed URLs and raw client diagnostics are excluded from results.

On October 26, 2026, three independent runs used Hub 2.3.0/hf_xet 1.7.0,
one combined-role replica limited to one CPU/1 GiB, and a deterministic
incompressible 36 MiB file followed by a 256 KiB tail edit. The executable
SHA-256 was
`eeab4aff879300ea6be594348e22e67e9e6a79d8c569b9d911bdb705139a767c`.
This is a resource-cost observation for that frozen candidate, not a comparison
against released v1.12.1 or a final-head qualification.

| Operation | Backend attempts (each run) | Median backend response bytes | Query calls (each run) | Median statement WAL bytes |
| --- | ---: | ---: | ---: | ---: |
| Initial native upload | 633 | 37,783,560 | 864 | 480,824 |
| Initial cold download | 6 | 37,781,414 | 39 | 0 |
| Edited version, cold upload client | 38 | 38,126,666 | 269 | 55,601 |
| Edited version, cold download client | 8 | 38,125,013 | 42 | 0 |

The edited upload sent 343,237 Xorb bytes to Shardline versus 37,753,312 for
the initial upload: about 99.1% fewer client Xorb bytes. Backend attempts fell
by about 94.0% between these two operations. These compare unique versus
overlapping content in this fixture, not before/after software performance.
The initial upload made 572 standalone chunk PUTs; the edited version made
five. Initial median retained payload was 75,567,954 bytes, about 2.002 times
the logical file size, including compatibility chunk copies. The edited
version added about 689 KB of retained payload.

The remaining cost is material: verifying the edited publication still read
about 38.1 MB from the backend, despite receiving only 343 KB of new Xorb data
from the client. Backend response bytes include protocol overhead and error
bodies; they are not a cloud billing quantity. Removing compatibility copies
or source validation requires separate integrity and released-reader proofs.
This run does not justify bypassing either check.

The fixture is sequential on one shared host with warm server/backend caches;
it does not measure tail latency or multi-host scale-out. Resource results
separate client CPU/RSS from server/backend cgroup observations. Statement WAL
is not device writes or NVMe wear. Inventories are immediate, with no claim of
retention-expired GC savings, byte-hours, cloud prices or all-in cost per TB.

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
