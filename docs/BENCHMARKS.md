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

PostgreSQL results distinguish top-level statements from SQL executed inside
triggers and functions. `calls` and `statement_wal_bytes` include both; their
totals can count nested work more than once. Use `top_level_calls` and
`top_level_statement_wal_bytes` for comparisons without that nesting overlap.
Top-level calls still do not equal network round trips, and statement WAL is
not physical disk writes, total cluster WAL, replication traffic or NVMe wear.
Older result files lack these separate counters and cannot establish a
physical-WAL increase from their summed total. Observer queries are excluded.

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

### October 26 paired replica check

Released v1.12.1 (`ede71ea4`) and the candidate executable from `97df1303`
each completed three repetitions of this matrix using harness `e57a1960`:

```bash
python3 scripts/shardline/validate_performance.py \
  /absolute/path/to/shardline /private/new-output-directory \
  --binary-source-revision <executable-source-commit> \
  --replicas 1 2 4 8 --datasets unique repeated \
  --objects 128 --object-bytes 1048576 --clients 32 --repeats 3 \
  --replica-cpus 1 --replica-memory 1g
```

The predecessor ran first, followed by the candidate, with boto3 1.43.95 and
fresh disposable PostgreSQL/MinIO deployments on the same host. All 48 lanes
passed, including 69,120 exact post-GC reads and backend trace reconciliation.
Medians below count complete upload and download responses, in MiB/s;
each cell shows **v1.12.1 → candidate**.

| Replicas | Unique mixed | Repeated mixed | Unique mixed + GC | Repeated mixed + GC |
| --- | ---: | ---: | ---: | ---: |
| 1 | 341 → 359 | 358 → 360 | 262 → 263 | 311 → 300 |
| 2 | 556 → 597 | 602 → 586 | 371 → 387 | 476 → 469 |
| 4 | 771 → 804 | 803 → 783 | 443 → 448 | 513 → 531 |
| 8 | 784 → 808 | 776 → 799 | 427 → 436 | 524 → 555 |

These results do not establish a general throughput or tail-latency gain.
Candidate eight-replica mixed scaling efficiency was about 28% for both
datasets, below the 70% engineering target. Shared clients/backends and warm
host caches limit what this fixed-size burst says about production scaling.
One-replica GC PUT p99 changed from 462 to 443 ms for unique data and from
301 to 308 ms for repeated data; three bursts do not establish sustained tails.

Ingestion backend requests were unchanged: 512 for unique data and 416 for
repeated data per 128-object lane. One-replica top-level SQL calls increased
from 2,560 to 2,688, and median top-level statement WAL rose from about
1.42 to 1.83 MB. New publication metadata and derived lookup indexes add write
work; this run does not prove a net database saving. Server cgroup
memory high-water medians remained approximately 85–87 MiB, including page cache.

Settling GC made 1,347 → 575 requests for unique data and 387 → 191 for
repeated data, but response bytes nearly doubled: approximately 135 → 269 MB
and 34 → 67 MB respectively. Fewer requests therefore do not establish a lower
total bill. Traced bytes include protocol overhead, and these short runs do
not establish retention-expired storage, physical NVMe writes or hardware life.

### Operational-efficiency measurements

A local SQLite qualification populated one million synthetic file records
through the real membership-refresh triggers, using the `b569f583` executable's
normal migrations. The production candidate query retained its 256-row batch
limit. A shared hash returned 256 candidates, a unique hash in the final record
returned one, and missing-hash/wrong-repository probes returned none. All four
plans searched the covering membership index and then the record-key index;
SQLite integrity and foreign-key checks passed. Observed query times were
1.06/0.065/0.037/0.034 ms respectively with warm host caches. These are single
SQL observations, not HTTP authorization, cold-cache latency or million-object
storage measurements. A separate disposable PostgreSQL 16 test populated one
million records through the actual refresh triggers and called
`PostgresRecordStore::first_repository_hash_record` directly. After 32 common-hash
lookups, shared, final-record, missing-hash and wrong-repository probes returned
the expected results in 0.32–0.50 ms across two runs with warm caches.
This explicitly ignored qualification can be run with a migrated disposable
`DATABASE_URL` using `cargo test -p shardline-index --all-features --lib
indexed_membership_million_record_candidate_lookups -- --ignored --nocapture`.
Neither test proves complete HTTP authorization, prompt revocation, cold-cache
latency or proportional compatibility fallbacks.

An additional October 26 comparison used actual v1.12.1 and candidate
`b569f583`, harness `605d9fcc`, eight 36-MiB objects, eight clients and three
repetitions of unique/repeated data. Each fresh PostgreSQL/MinIO deployment
had one API replica limited to one CPU/1 GiB. All 12 lanes passed, including
288 exact post-GC reads, 60 GC commands and trace/API reconciliation.

| Median observation | Unique: v1.12.1 → candidate | Repeated: v1.12.1 → candidate |
| --- | ---: | ---: |
| Ingestion backend requests | 56 → 56 | 44 → 44 |
| Ingestion top-level SQL calls | 160 → 168 | 160 → 168 |
| Ingestion top-level statement WAL bytes | 333,073 → 365,876 | 334,340 → 367,144 |
| Settling-GC requests | 227 → 127 | 107 → 79 |
| Settling-GC response bytes | 302,597,920 → 604,532,175 | 75,656,122 → 151,141,027 |

This preserves the earlier conclusion: GC uses fewer requests but transfers
more backend bytes, and publication adds database work. Concurrent unrelated
host activity prevents attributing timing deltas to the code. The earlier
intermittent GC exit-2 failure did not recur here; its root cause remains
unproven, and passing repetitions do not establish a fix. These zero-retention
fixtures do not replace the separate real-default-retention drill.

The follow-up used harness `e5ee04be` and the same binaries/workload across
1/2/4/8 API replicas, again with three repetitions per dataset. All 48 lanes
passed, including 4,320 exact post-GC reads, 222 GC commands and independent
trace/API reconciliation. Median unique-ingest requests remained 56 at every
replica count; repeated-ingest requests were 44/48/56/56 for both binaries.
The table's median SQL calls and settling-GC request/byte counts also held at
every replica count. This extends the correctness and backend-cost check;
unrelated host activity prevents an isolated timing or production scaling claim.

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

An October 26 follow-up compared actual released v1.12.1 with the executable
from `97df1303` on NVMe-backed Btrfs. Three fresh pairs alternated binary order.
Each deployment used local objects and SQLite, 32 deterministic incompressible
one-MiB files (`SHAKE-256("local-io-v1:<index>")`), eight HTTP clients, an
identical overwrite and complete reads before and after GC. Startup was excluded
from I/O deltas; normal durability settings were used without dropping caches.

| Server observation | v1.12.1 median | Candidate median |
| --- | ---: | ---: |
| Initial upload: process `write_bytes` | 63.5 MiB | 46.3 MiB |
| Identical overwrite: process `write_bytes` | 26.3 MiB | 7.2 MiB |
| Warm 32-file read phases: process `write_bytes` | 1.0–1.4 MiB | 0 MiB |
| Lifetime peak RSS after overwrite | 43.9 MiB | 55.1 MiB |

All 768 full-body comparisons passed, including reads after restarting each
deployment with v1.12.1. All six fsck runs passed; SQLite `integrity_check` and
`foreign_key_check` passed before and after those restarts. These process
counters exclude the separate GC/fsck processes and are not complete deployment
or internal NAND writes. Read caches were warm; no cold-read or drive-lifetime
gain is established. The lower server write pressure came with approximately
11 MiB higher observed peak RSS in this fixture, not a universal memory ceiling.

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

#### S3 write-coordination CPU

A Linux `perf` flamegraph of the previous coordination path with 4,096 held
keys and 32,768 transient acquisitions showed most sampled CPU in lock-map
scanning and weak-reference checks. Registration now lasts through both waiting
and ownership; the last registration removes only its own map entry. This avoids
scanning unrelated keys without adding a cleanup task or changing conditional
object writes and integrity verification.

The same unoptimized diagnostic took 1.11 seconds before and 66 milliseconds
after the change. These single shared-host observations measure coordination
only, excluding S3, hashing and payload transfer; they do not establish an
end-to-end throughput improvement. Cancellation and concurrent registry churn
also have ordinary regression tests. Reproduce the isolated diagnostic with:

```bash
cargo test --locked -p shardline-storage --all-features --lib \
  many_live_keys_coordination_cpu_diagnostic \
  -- --ignored --test-threads=1 --nocapture
```

A release-binary check used 32 concurrent HTTP clients and 32 one-MiB objects
on owned PostgreSQL/MinIO, with unique and repeated data before and after the
change. All four lanes passed, including 384 byte-verified post-GC reads;
ingest, mixed-workload and settling-GC request counts matched between builds.
A stock Hub 2.2 / `hf_xet` 1.7 one-GiB upload/edit/download run also passed,
retaining the 17,037/44/6,401/52 backend request counts and all 6,240 duplicate
chunk verification GETs. These checks qualify behavior for those workloads;
they do not establish lower cloud bills or production scale-out efficiency.

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
normalization diagnostic alone does not establish a reduction in those total costs.

Upload-intent identity preparation also uses the owned-staging path, rather than
making a normalized copy before the storage adapter receives the file. With
stock Hub 2.2.0 / `hf_xet` 1.7.0, a 36 MiB incompressible upload and verified
download succeed with a separate 64 MiB container tmpfs for staging. The earlier
copy path cannot complete the initial upload under that limit. This is a scratch
capacity check, not a throughput comparison or a measurement of NVMe writes.

Two paired 512 MiB incompressible stock-client runs compared the earlier
selected-decode executable (`c035ece4`) with owned upload-identity staging
(`111fff8c`). Run order was earlier/new, then new/earlier; each used a fresh
PostgreSQL/MinIO deployment, Hub 2.2.0 / `hf_xet` 1.7.0, one server CPU, a
1 GiB server memory limit and unchanged worker settings. Unique upload phases
measured the following server-process counters:

| Counter | Earlier staging | Owned identity staging |
| --- | ---: | ---: |
| Logical writes (`wchar`) | 2,691,685,661–2,691,696,591 | 2,154,741,381–2,154,751,965 |
| Kernel-accounted `write_bytes` | 1,612,279,808–1,613,406,208 | 1,075,167,232–1,075,630,080 |
| Logical reads (`rchar`) | 4,835,950,894 | 4,835,950,889–4,835,950,894 |
| S3 requests | 8,538 | 8,538 |

This removes approximately one payload-sized staging write: about 20% of
server logical writes and 33% of kernel-accounted writes in this fixture.
All four campaigns passed exact downloads, cold overlapping-upload dedup and
independent MinIO trace reconciliation. Retained payload remained approximately
1.075 GB, including the decoded chunk copies. Upload elapsed time was
6.07–6.17 seconds earlier and 5.64–5.69 seconds with owned staging, but two
shared-host pairs do not establish production throughput or tail latency.
Process counters exclude PostgreSQL/MinIO writes and are not NAND wear or SSD
lifetime measurements. No reduction in backend requests or retained storage,
or performance parity with upstream Xet, is established by this change.

The same owned PostgreSQL/MinIO fixture then fills that tmpfs until no bytes are
available. The failed upload leaves the Hub revision unchanged and its file
unpublished; after removing the filler, an identical retry succeeds without a
server restart and both old and new files download with matching SHA-256 hashes.
Server diagnostics confirm ENOSPC. The stock client keeps retrying HTTP 500 until
the harness cancels its process group after 30 seconds, so this does not establish
prompt client failure or a server-side transfer deadline. Only the isolated tmpfs
is filled; the host filesystem and durable backends retain space.

#### S3 requests and upload resources

Native member persistence hands its verified decoded buffer to the object
adapter as owned bytes. This avoids the S3 adapter copying a borrowed member
into a second owned buffer; member validation, conditional creation and the
256 KiB pending-write budget remain unchanged. This changes upload persistence,
not the download decoder.

Two reversed-order pairs with stock Hub 2.2.0 / `hf_xet` 1.7.0, fresh
PostgreSQL/MinIO fixtures and 512 MiB incompressible uploads passed exact
downloads, cold overlapping-upload dedup and independent trace reconciliation.
The server had one CPU and a 1 GiB memory limit. Unique-upload server cgroup CPU
was 4.47–4.49 seconds with borrowed member bodies and 4.21–4.29 seconds with
owned bodies. Elapsed times overlapped: 5.62–6.08 versus 5.35–5.93 seconds.
Every unique upload still made 8,538 S3 requests, with approximately 2.155 GB
of process logical writes. These shared-host pairs support removing the copy;
they do not prove a throughput, RSS, backend-cost or upstream-Xet advantage.

The local async object adapter also transfers an already-owned member body to
its blocking worker instead of cloning the payload. Borrowed callers still
receive an owned copy before the worker starts. The worker retains the body
through existing integrity checks and file/directory synchronization. This
removes an allocation and copy for owned inputs; no whole-client throughput,
RSS or physical-write reduction is attributed to that local change here.
Four reversed-order 512 MiB native-CAS runs comparing `260ddaa7` with
`a9e3290a` passed all eight SHA-256 downloads with unchanged retained bytes.
Initial-upload timings were 9.02/29.07 seconds before and 10.12/11.34 seconds
after; server CPU ranges also overlapped. The variable shared-host results do
not establish a throughput or CPU improvement from removing that copy.

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

The streaming candidate lookup now also uses an unnamed statement. A separate
PostgreSQL 16 adapter diagnostic populated one million valid, trigger-indexed
records, primed 64 common-hash streaming lookups, then requested common, late,
absent and wrong-repository candidates. The visitor stopped at its first valid
record; it did not drain all common-hash results. Two baseline/fixed pairs gave:

| Streaming probe at one million records | Cached plan, ms | Parameter-aware plan, ms |
| --- | ---: | ---: |
| Common hash, first record | 5.35–5.47 | 5.17–6.06 |
| Late record | 678–739 | 0.75–0.84 |
| Absent hash | 52.98–56.41 | 0.78–0.82 |
| Wrong repository | 142.68–162.63 | 0.71–0.88 |

All four runs passed the record, repository-scope and member-hash assertions.
Separate owned-fixture query plans showed that generic plans can traverse the
ordered record-key index, while parameter-aware sparse probes use the
membership lookup index. Replanning has a cost; these observations do not prove
lower aggregate PostgreSQL CPU, RSS, production tail latency or Xet transfer
times. They also do not remove proportional traversal when a valid proof
actually requires many candidates. The first-candidate probe is unchanged.
In the separate SQL-only fixture, after a million membership updates and a
completed vacuum, the sparse streaming query touched 55,291 shared buffers
with a forced generic plan versus 13 with a custom plan. Before the updates,
the counts were 65,346 versus 13. Buffer accesses include cache hits and are
not physical disk-read bytes; the synthetic SQL fixture does not validate HTTP
authorization or establish an SSD endurance improvement.
Reproduce with the ignored `indexed_membership_million_record_candidate_lookups`
test described above; a disposable, migrated PostgreSQL database is required.

#### Stopping unused PostgreSQL candidate results

Returning from a streaming visitor did not stop PostgreSQL from producing the
remaining result set. With parameter-aware plans already enabled, the same
million-record adapter workload made 68 streaming candidate queries, including
64 common-hash priming calls, and PostgreSQL produced 64,999,936 rows even
though each successful visitor stopped at its first record.

The lookup now starts with a 16-record page and grows later pages to at most
1,024 records. Ordered keyset seeks run in one read-only, repeatable-read
transaction, preserving the old single-statement snapshot across concurrent
publication and deletion. Existing record-conversion admission and byte bounds
remain in place; pages are streamed rather than buffered as complete vectors.
Both joined tables carry the keyset bound. In a separate million-record SQL
fixture, bounding only membership made a late page scan 901,024 file records
to return 1,024. Bounding both inputs reduced that scan to 1,024 records.
This prevents repeated scans of earlier file records during long traversals.

| Backend observation for the million-record workload | Unbounded stream | Paged stream |
| --- | ---: | ---: |
| Candidate SELECT calls | 68 | 68 |
| Rows produced by PostgreSQL | 64,999,936 | 1,041 |

Both runs passed the same record, member-hash and repository-scope assertions.
These counts come from `pg_stat_statements` on disposable PostgreSQL 16
instances. Paging adds transaction setup and completion commands, which are
excluded from the SELECT count; a genuinely long traversal also needs more
SELECTs. This is evidence of reduced unused result production, not a measured
whole-Xet throughput, memory, physical-disk or production cost improvement.
The snapshot regression replaces 1,499 of 1,500 records during traversal and
checks both the original snapshot and a fresh read. Separate checks cover late
malformed records, valid early exits and cancellation of a blocked read.

A separate paired run also visited every one of the 999,999 matching records.
The unpaged traversal took 28.34 seconds and the paged traversal 29.82 seconds
on the shared host, using matching unoptimized build settings. Thus this
single pair shows about 5% additional full-traversal time, not a full-scan
speedup. Including the early-exit calls, the workload used 69 versus 1,048
candidate SELECTs and produced 65,999,935 versus 1,001,040 rows. The extra
round trips are the cost of bounding unused results. These adapter diagnostics
do not establish production tail latency or stock-client throughput.

An October 27 PostgreSQL 16 diagnostic kept the real membership triggers,
disabled autovacuum, and changed the member hash in one million records.
With both keyset bounds present, a revoked-hash probe returned no rows but
visited 14,915 shared buffers and took 43.25 ms. After explicit vacuum and
analysis, the same probe visited four buffers and took 0.066 ms. Pages of
1,024 current records took 11.14–11.43 ms before vacuum and 10.71–10.94 ms
afterward. These are individual instrumented SQL observations, including
cache hits, not HTTP latency, physical I/O or production percentiles.
They identify stale-index cleanup as one remaining cost after bulk updates;
they do not justify removing the authoritative record join. Keep autovacuum
enabled and inspect dead tuples and vacuum progress when diagnosing latency
after large metadata changes. This diagnostic does not resolve every source
of post-update HTTP latency.

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

A historical release-mode microbenchmark of per-key write coordination measured about
0.16 µs per acquisition with 128 other keys held, 0.49 µs with 512 and 10.4 µs
with 8,192 (10,000 acquisitions per size, one thread, no S3 traffic). That
implementation scanned active keys during acquisition. The current implementation
removes each inactive entry directly; see [S3 write-coordination CPU](#s3-write-coordination-cpu)
for the profiling evidence and the limits of that measurement.

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

## Repeated legacy chunk reads

An HTTP-backed S3 fixture reconstructs eight consecutive occurrences of the same
64 KiB standalone chunk. Before stream-local reuse, both raw `FixedChunkV1` and
compressed `XorbCdcV1` records issue eight payload GETs. With reuse, each issues
one payload GET and returns the same complete bytes. This is an 87.5% reduction
in those payload requests, not in all requests for an arbitrary workload;
packed-container discovery can issue additional requests.

A disposable real-MinIO check also passes both representations at the size
boundary. At 64 KiB, it observes two total GETs (missing-container discovery and
one payload) and one encoded chunk's response bytes. At 65,537 bytes, it observes
nine GETs and eight encoded chunks' response bytes. Complete reconstructed bytes
are checked in every case.

The stream shares one complete verified output buffer of at most 64 KiB. It
does not reuse larger chunks or retain whole chunks from partial outputs.
Boundary tests at 65,537 bytes retain eight payload GETs, and partial first/last
chunks retain their two independent reads. New streams authenticate storage
again and reject corruption. Storage formats, retained bytes and write durability
are unchanged. These request counts do not establish RSS, physical device wear,
whole-workload throughput or native Xet upload parity.

## Native Xet backend-cost check

### Upstream local CAS reference comparison

#### Owned footerless preparation

An 8,389,632-byte uncompressed footerless fixture measures Linux thread read
characters during owned staging preparation. Before removing a duplicate
canonical validation, preparation read 25,188,844 bytes; afterward it reads
16,792,948 bytes, approximately one third less. Fixture creation and subsequent
independent digest checks are excluded from those counters.

The owned preparation path still authenticates footerless members before
appending a footer, then fully validates the resulting container and computes
its serialized digest before returning it. The standalone normalization API
retains its complete-result validation. Tests reject payload mutation and footer
removal between append and final validation; cancellation checks remain.
Immutable storage still verifies the source before publication.

These counters measure preparation only. They do not establish whole-upload
throughput, lower retained storage or S3 request counts, physical device I/O,
SSD lifetime, or parity with native Xet.

A separate same-profile 512 MiB baseline/candidate/candidate/baseline control
measures the complete initial upload. Server logical reads fell from approximately
3.246 GB to 2.709 GB (16.6%); read calls fell from 357,166–357,169 to
254,549–254,554 (28.7%). Initial retained file lengths were identical at
1,087,992,784 bytes, and all eight complete downloads passed SHA-256 checks.
No competing compilation was detected. Upload times of 6.52–7.15 seconds before
and 6.29–6.94 seconds after overlap; two observations per binary on this shared
host do not establish a reliable throughput improvement. Process read counters
include file and socket reads and do not establish physical device savings.

#### Bounded native member reads

Before the decoded-copy writer was removed, native member persistence buffered
its staging reader with a fixed 64 KiB allocation. An instrumented fixture
publishing 128 uncompressed 1 KiB members
made 256 source `Read` calls before the change and three afterward. This
measures calls to the reader, not physical device I/O, SSD lifetime or complete
upload throughput. It is separate from the reference timings below.

Every reread member retains its length, offset and content-hash checks. The
existing decoded-byte/concurrency limits, immutable-object verification and
durable-write synchronization remain. The buffer adds 64 KiB per active member
persistence operation; its size does not grow with file or container length.

A same-profile baseline/candidate/candidate/baseline control used the 512 MiB
stock-client fixture described below. All eight downloads verified; no competing
compilation was detected in this sequence. Initial-upload server read calls
fell from 429,820–429,824 to 360,287–360,292, approximately 16%. Retained file
lengths were identical at 1,088,203,472 bytes. These process counters do not
measure physical device traffic.

Initial uploads took 9.93–11.33 seconds before and 6.80–9.37 seconds afterward.
Complete verified downloads also varied, from 0.66–0.74 seconds before to
0.71–1.16 seconds afterward, although this change affects member persistence.
The shared host and two observations per binary do not establish a reliable
throughput improvement or download regression. An earlier sequence detected
competing compilation; its timing results were rejected.

Qualification passed the adapter suite and doctests, strict server/adapter
Clippy, both actual 512 MiB stock-client SIGKILL recovery scenarios and both
actual-v1.12.1 native upgrade/rollback scenarios on local and PostgreSQL/MinIO
storage. Per-member mutation rejection and cancellation tests remain enabled.

#### Matched-build upstream comparison

An October 27 control compares Shardline `82f52d53` with unmodified upstream
xet-core `629562cd9fc37432317eb57811faf13a6935c601`. Both binaries use the
same compiler and release settings: optimization level 3, thin LTO, one codegen
unit, abort-on-panic, no debug information, stripped symbols and frame pointers.
The stock hf_xet 1.7.0/Hub 2.2.0 fixture uses the same 512 MiB incompressible
file and 256 KiB tail edit as the successful whole-upload control above.
Order is upstream/Shardline/Shardline/upstream, with fresh client processes and
caches, identical CPU affinity and local filesystem storage. All eight complete
downloads passed independent SHA-256 checks; no competing compilation was
detected. An earlier attempt detected competing compilation and its timings
were rejected.

| Measurement | Shardline | Upstream local CAS |
| --- | ---: | ---: |
| Initial upload | 6.35–6.48 s | 1.14–1.16 s |
| Upload after edit | 0.72–0.75 s | 0.27–0.30 s |
| Complete initial download, including client SHA-256 | 0.67–0.69 s | 0.86–1.03 s |
| Initial-upload server peak RSS | 50–52 MiB | 207 MiB |
| Initial retained file lengths | 1,087,992,784 B | 538,731,316 B |

Matching build settings does not equalize persistence contracts: upstream's
development CAS has weaker synchronization guarantees, while Shardline retains
decoded chunk copies for released-reader compatibility. These results establish
neither upload parity nor storage parity. The upload gap remains approximately
5.6 times in these samples. This shared host also runs the preserved aging
retention fixture; two observations per implementation do not establish
production Hugging Face performance, isolated-host performance, p95/p99 latency,
physical device savings or SSD lifetime.

An updated matched-profile run at `f59d2dff`, including the upload-local
repeated-member optimization, used the same frozen upstream binary, client,
corpus and upstream/Shardline/Shardline/upstream order. All eight downloads
passed SHA-256 verification, and its monitor detected no competing compilation.

| Measurement | Updated Shardline | Upstream local CAS |
| --- | ---: | ---: |
| Initial upload | 9.49–9.90 s | 1.19–1.36 s |
| Upload after edit | 0.70–0.98 s | 0.27–0.50 s |
| Complete initial download, including client SHA-256 | 0.78–1.40 s | 0.97–1.05 s |
| Initial-upload server peak RSS | Approximately 50 MiB | Approximately 206 MiB |
| Initial retained file lengths | 1,087,992,784 B | 538,731,316 B |

These observations still show substantial upload and storage gaps. The
repeated-member change targets consecutive duplicate members; it does not
establish a speed or storage improvement for this incompressible corpus.
The shared host and two observations per implementation do not establish a
regression against the earlier run, either. A separate interleaved old/new
control encountered competing compilation, so its timings were rejected;
all of its downloads verified. An earlier attempt at the updated upstream
comparison was rejected for the same reason. The persistence-contract and
production-performance limitations above continue to apply.

#### Upload synchronization diagnostic

A separate syscall trace uses the same frozen binaries, stock client and
512 MiB corpus as the matched-build comparison. During the initial upload,
Shardline issues 91,985 `fsync` calls; upstream issues one `fdatasync` call.
Neither trace reports a synchronization error. Initial and edited downloads
from both implementations pass independent SHA-256 verification.

`strace -f -c -w` reports 26.14 seconds of summed syscall wall time for
Shardline and 0.000534 seconds for upstream. Thread waits overlap, so this is
not elapsed upload time or the fraction of the untraced performance gap caused
by synchronization. Tracing perturbs execution, and unrelated compilation was
present on the shared host; these timings are diagnostic only. The counts
identify local publication synchronization as further work to investigate,
not permission to omit payload or directory durability. No synchronization
behavior changed, and these counts are not a production cost-per-TB estimate.
The exact count also depends on storage-root depth: anchored writes synchronize
each directory in the root's path, including existing names, to protect
concurrent creation and retries after a failed directory sync. This fixture
uses a deeply nested validation directory. Its count must not be extrapolated
to a deployment with a different root or treated as 91,985 payload flushes.

#### Earlier runtime control with different build profiles

An October 27 control uses Shardline `9f3d47dc` and unmodified upstream
xet-core `629562cd9fc37432317eb57811faf13a6935c601`, with stock hf_xet 1.7.0
and Hub 2.2.0. The fixture is 512 MiB of incompressible content followed by a
256 KiB tail edit. The order is upstream/Shardline/Shardline/upstream, with
fresh client processes and caches, the same logical CPU affinity and local
filesystem. All eight complete downloads passed independent SHA-256 checks.
No competing compilation was observed during the timed sequence.

| Measurement | Shardline | Upstream local CAS |
| --- | ---: | ---: |
| Initial upload | 6.19–6.90 s | 1.07–1.17 s |
| Upload after edit | 0.73–0.75 s | 0.29–0.30 s |
| Complete download, including client SHA-256 | 0.65–0.67 s | 0.87–1.27 s |
| Initial-upload server peak RSS | 59 MiB | 208 MiB |
| Initial retained file lengths | 1,087,967,176 B | 538,730,548 B |

These binaries use different optimized native release settings: Shardline uses
thin LTO, one codegen unit, abort-on-panic and frame pointers; upstream's
recorded release uses fat LTO, debug information and no additional rustflags.
They are not identical build profiles. Upstream is a development CAS with
weaker synchronization guarantees, not Hugging Face's production service.
The host is shared, including an aging retention fixture; these two observations
per implementation do not establish isolated-host or tail-latency performance.

Shardline's initial-upload logical process reads remain approximately 3.247 GB.
Decoded chunk copies remain alongside Xorbs for old-reader compatibility.
There is no demonstrated upload parity or retained-storage reduction. This
local-storage control does not exercise or quantify the PostgreSQL connection
checkout optimization in `9f3d47dc`.

#### Owned validation across upload phases

A subsequent candidate based on `04ddedff` carries an opaque, owned validation
result from native upload identity preparation into storage. It retains staged
file ownership, cancellation/admission, footerless normalization, member hash
checks during rereads and final serialized-object verification before immutable
publication. Existing storage formats, decoded chunk copies and synchronization
remain unchanged. The existing public upload entry points remain available.

Using the same optimized build profile, 512 MiB incompressible corpus, 256 KiB
tail edit, stock hf_xet 1.7.0 and CPU affinity as the comparison below, the order
was previous/candidate/candidate/previous. All eight downloads passed independent
SHA-256 verification. No local build or qualification test overlapped the timed
comparison; this remains a shared-host measurement.

| Measurement | Previous staged-validation build | Owned-validation candidate |
| --- | ---: | ---: |
| Initial-upload process read characters | 3.783 GB | 3.246 GB |
| Initial upload | 6.25–7.58 s | 6.30–6.55 s |
| Initial-upload server peak RSS | 50–51 MiB | 48–51 MiB |
| Initial retained file lengths | 1,087,903,184 B | 1,087,903,184 B |

The approximately 537 MB (14.2%) reduction is in logical process reads, including
file and socket reads, not physical device reads. Upload timings overlap and
do not establish a throughput speedup. Process-accounted writes varied in both
directions; these runs establish neither lower disk writes nor increased SSD
lifetime. Backend API request savings and reduced retained storage are not
claimed for this change.

A separate upstream/candidate/candidate/upstream sequence used the same corpus
and unmodified upstream xet-core `629562cd9fc37432317eb57811faf13a6935c601`:

| Measurement | Owned-validation candidate | Upstream local CAS |
| --- | ---: | ---: |
| Initial upload | 6.80–6.87 s | 1.15–1.21 s |
| Upload after edit | 0.70–0.71 s | 0.28–0.33 s |
| Complete download, including client SHA-256 | 0.64–0.81 s | 0.94–1.17 s |
| Initial-upload server peak RSS | 48–49 MiB | 221–235 MiB |
| Initial retained file lengths | 1,087,903,184 B | 538,725,364 B |

All eight downloads in this second sequence also verified. Upstream's local
development server has weaker synchronization guarantees and is not Hugging
Face's production service. This is not a production parity, cost or tail-latency
result. Shardline still has substantial upload and duplicate-storage gaps.

Qualification passed 392 adapter tests, 24 doctests, seven server cancellation
and identity tests, seven configured PostgreSQL/local retry cases, strict
server/adapter Clippy, both 512 MiB stock-client process-cut scenarios and two
actual-v1.12.1 native upgrade/rollback scenarios. New mutation tests reject both
changed member payloads and changed footers after preparation, and explicitly
cover footerless persistence and abandonment cleanup. The full release and
supported-client matrices remain separate requirements.

#### Canonical staged-validation comparison

On October 27, an optimized candidate based on `ee44af54` removed the second
complete canonical-Xorb validation in staged-file preparation. It retains full
validation, the serialized storage digest, footerless normalization and final
immutable-write verification. A Linux thread-I/O regression measured
8,402,262–8,402,271 read characters for an 8,395,896-byte canonical container,
versus 16,798,158 before this change. These are logical process reads, not
physical device reads or a measurement of SSD lifetime.

The same 512 MiB incompressible corpus and 256 KiB tail edit were then tested
with stock hf_xet 1.7.0 against upstream xet-core
`629562cd9fc37432317eb57811faf13a6935c601` and the candidate, in
upstream/candidate/candidate/upstream order. Each operation used a fresh client
process and cache; servers used the same logical CPU and local filesystem.
All eight full downloads passed independent SHA-256 verification.

| Measurement | Shardline candidate | Upstream local CAS |
| --- | ---: | ---: |
| Initial upload | 6.09–6.82 s | 1.09–1.15 s |
| Upload after edit | 0.68–0.72 s | 0.28–0.30 s |
| Complete download, including client SHA-256 | 0.62–0.68 s | 0.80–0.86 s |
| Server peak RSS after initial upload | 52–54 MiB | 250 MiB |
| Retained file lengths after initial upload | 1,087,903,184 B | 538,725,364 B |

Shardline initial-upload process read characters were approximately 3.78 GB.
This leaves substantial upload work and duplicate retained storage to address;
the preparation optimization does not establish native Xet performance parity.
The upstream development server has weaker durability synchronization and is
not Hugging Face's production service. Two observations per implementation on
a shared host do not establish production throughput, tail latency or a general
speedup ratio. Retained file lengths include metadata and are not allocated
disk blocks or post-retention storage accounting.

Qualification of this candidate passed 388 adapter tests, 24 adapter doctests,
10 focused server tests, strict adapter/server Clippy, both local and remote
512 MiB process-cut scenarios, and two actual-v1.12.1 native-Xet upgrade and
rollback scenarios. These checks do not complete the broader release matrix.

#### Local member-write concurrency qualification

A subsequent local-disk experiment compared exact optimized executables with
serial member persistence (`2a77ea20`) and at most four pending member writes.
Both retain the 256 KiB pending decoded-data budget (one larger member runs
alone), member hash verification, file and parent-directory synchronization,
and admission ownership through already-started writes after cancellation.
It changes scheduling, not the persisted representation or acknowledgment rule.

Three reversed-order comparisons used the same 512 MiB incompressible corpus,
256 KiB edit and stock hf_xet 1.7 client described below. The six serial initial
uploads took 8.89–23.38 seconds; six concurrent initial uploads took
6.59–7.14 seconds. Shared-host contention affected the serial outlier, so this
does not establish a production speedup percentage. Retained file lengths
remained 1,087,903,184 bytes initially and 1,088,532,774 after the edit.
Every downloaded file passed independent SHA-256 verification.

Upload process peak RSS in the two comparisons that captured it was
41,444–43,400 KiB serially and 49,840–52,212 KiB concurrently. This is a measured
memory trade-off, not an RSS reduction. Initial single-download timings showed
a possible regression; the final control performed three fresh-client downloads
per file and server instance. Its twelve candidate downloads took 0.66–0.72
seconds, overlapping the stable baseline observations (0.67–0.89 seconds);
earlier baseline observations reached 2.04 seconds. No consistent download
regression or general download advantage was established.

Adapter tests and 17 selected actual-v1.12.1 upgrade/rollback cases passed;
31 other deployment cases were excluded by the selection. These controls do
not establish saturation safety, final-head CI, multi-host scaling, hardware
wear savings or upstream upload parity. Native decoded-chunk duplication and
the initial-upload gap against upstream remain open.

A follow-up safety control started 32 cold-client uploads of the same 36 MiB
file simultaneously, then ran 32 independently SHA-256-verified downloads
with eight concurrent readers against each exact executable. Both completed.
Baseline peak server RSS was 81,908 KiB; candidate peak RSS was 135,864 KiB.
The 583 durable chunk/Xorb objects matched by identity, length and SHA-256,
totaling 75,530,184 bytes in each run. Small total-directory differences came
from metadata, which is not included in that payload comparison. This is a
same-content contention control, not unique-file saturation or a scaling test.

A separate 32-client control used distinct incompressible 36 MiB files and the
same eight-reader download limit. Both binaries completed all uploads and 32
verified downloads. Their 18,838 durable chunk/Xorb objects matched by identity,
length and SHA-256, totaling 2,416,976,080 bytes per binary. Peak server RSS was
133,216 KiB serially and 100,172 KiB concurrently. Together with the opposite
memory result for identical files, this demonstrates workload dependence;
it does not establish a general RSS reduction or a sustained-load ceiling.
These controls did not measure 64-client or multi-host operation.

The current-source stock-client process test also passed all six lifecycle
interruption points for a 512 MiB upload: intent creation, storing, object work,
stored, metadata committed and visible. It verified recovery after SIGKILL;
this local control does not qualify remote-provider failure semantics.
The corresponding PostgreSQL/MinIO stock-client test subsequently passed the
same six boundaries in 243.27 seconds. That configured test ran against the
current source; twelve other process-test cases were excluded by its filter.
Neither result substitutes for the complete distributed-integrity gate.

An October 26 direct-CAS HTTP comparison used stock hf_xet 1.7.0 against
Shardline `07bedb55` and the unmodified upstream `local_cas_server` from
[xet-core `629562cd`](https://github.com/huggingface/xet-core/tree/629562cd9fc37432317eb57811faf13a6935c601).
Both optimized executables used local disk and the same logical server CPU
on one host, with no container memory limit. The workload used identical
incompressible 512 MiB files and a 256 KiB tail edit. Each operation started
a fresh client process and cache. Two runs per server used reversed order;
all eight downloads passed independent SHA-256 verification. Server and OS
storage caches were warm. These calls bypassed the Hub control plane.

| Direct-CAS operation | Shardline seconds | Upstream local reference seconds |
| --- | ---: | ---: |
| Initial upload | 8.83–9.01 | 1.21–1.24 |
| Initial full download, including SHA-256 | 0.70–0.82 | 0.98–1.24 |
| Tail-edit upload from a cold client | 4.13–4.58 | 0.26–0.31 |
| Edited full download, including SHA-256 | 0.87–1.10 | 1.01–1.04 |

Immediate retained file lengths, including metadata, were 1,087,903,184 bytes
for Shardline after the first upload and 1,486,840,653 after the edit. The
upstream reference recorded 538,725,364 and 538,533,091 bytes respectively;
its metadata can compact between observations. These totals are not allocated
disk blocks or retention-expired GC results. Shardline still retains decoded
chunk copies for its released compatibility contract.

The measurements establish substantial remaining upload and retained-byte
gaps. They do not establish overall parity: upstream labels this server a
testing/development reference, and it does not provide equivalent Shardline
authorization, maintenance and distributed durability guarantees. It is also
not Hugging Face's production service. In the pinned reference source,
`xet_client/src/cas_client/simulation/local_client.rs` writes Xorbs through
`SafeFileCreator`; its `close()` implementation in
`xet_runtime/src/file_utils/safe_file_creator.rs` flushes the buffered writer
and renames the temporary file without a file or parent-directory `fsync`.
Shardline's local publication synchronizes both payload and directory names
before acknowledging them. This is a durability-contract difference, not a
measurement of how much of the upload gap it explains. These results do not
describe Hugging Face's production persistence implementation.
Two shared-host runs cannot establish
tail latency, production scaling, network-byte counts or hardware lifetime.
Do not compare these local-disk numbers directly with the PostgreSQL/MinIO
measurements below as if their storage topology were equivalent.

#### Bounded related-container deduplication

A subsequent candidate returned related verified Xorb descriptors from one
indexed file record, rather than only the container containing the sampled
chunk. Every member retains its repository authorization mask; unrelated
members cannot match the public dedup key. Expansion stops at 64 containers
or 32,768 total members, subject to configured limits; an already larger
primary retains its existing single-container behavior. PostgreSQL protects the
advertised set against remote-delete admission and omits unresolved optional
sources. Deployments without the additive database guard retain the existing
single-container fallback until owner migration installs it.

Repeating the direct-CAS control with the exact same corpus produced these
two-run ranges. All eight downloads passed SHA-256 verification.

| Direct-CAS operation | Related-container candidate seconds | Upstream local reference seconds |
| --- | ---: | ---: |
| Initial upload | 9.25–9.93 | 1.09–1.29 |
| Initial full download, including SHA-256 | 0.63–0.74 | 0.95–0.96 |
| Tail-edit upload from a cold client | 0.70–0.84 | 0.26–0.29 |
| Edited full download, including SHA-256 | 0.65–0.72 | 0.93–0.98 |

Candidate retained file lengths were 1,087,903,184 bytes after initial upload
and 1,088,532,774 after the edit: 629,590 added bytes, versus 398,937,469
in the earlier single-container fixture. Initial storage duplication and
initial-upload throughput remain open gaps. These results do not establish
upstream parity or statistical download superiority.

A repeat on executable `260ddaa7`, including the later PostgreSQL candidate
planning and paging changes, used the same corpus, reference, cold clients and
reversed order. All eight downloads again passed SHA-256 verification.

| Direct-CAS operation | Shardline `260ddaa7` seconds | Upstream local reference seconds |
| --- | ---: | ---: |
| Initial upload | 8.83–9.51 | 1.09–1.17 |
| Initial full download, including SHA-256 | 0.64–0.68 | 0.88–0.90 |
| Tail-edit upload from a cold client | 0.73–0.75 | 0.28–0.29 |
| Edited full download, including SHA-256 | 0.63–0.64 | 0.88–0.90 |

Shardline retained 1,087,903,184 bytes initially and 1,088,532,774 after the
edit, unchanged from the preceding candidate. This local-disk workload does
not exercise PostgreSQL, so it cannot measure the database lookup improvement.
It confirms that initial-upload and retained-byte gaps remain in the current
executable. The same reference durability and production-comparability limits
above apply; these two-run ranges do not establish general download superiority.

The October 27 repeat on runtime `84cc1a97` used the same upstream revision,
corpus, fresh clients and reversed order. All eight downloads passed SHA-256.

| Direct-CAS operation | Shardline `84cc1a97` seconds | Upstream local reference seconds |
| --- | ---: | ---: |
| Initial upload | 6.43–7.00 | 1.13–1.21 |
| Initial full download, including SHA-256 | 0.98–1.14 | 0.87–0.98 |
| Tail-edit upload from a cold client | 0.71–0.73 | 0.30 |
| Edited full download, including SHA-256 | 0.78–0.94 | 0.91–1.15 |

Initial upload server peak RSS was 49,300–50,108 KiB for Shardline and
233,508–242,128 KiB for the upstream reference in this fixture. This is server
RSS, separate from the stock client's memory. Retained lengths were unchanged:
Shardline 1,087,903,184 bytes initially and 1,088,532,774 after the edit;
upstream 538,725,364 and 538,533,091 bytes. Initial upload and storage parity
remain open; the latest download timings do not support a general advantage.
The reference's weaker local synchronization contract, shared-host limitations
and distinction from Hugging Face production still apply.

The October 27 repeat on `85e225ca` used matched release profiles for both
servers (optimization level 3, thin LTO, one code-generation unit, aborting
panics, stripped symbols and frame pointers). Stock hf_xet 1.7.0 used the same
512 MiB incompressible fixture and 256 KiB tail edit, with fresh client
processes/caches and upstream/Shardline/Shardline/upstream ordering. All eight
downloads passed SHA-256. A first run overlapped compilation and was excluded
from timing conclusions; the repeat observed no competing compilation or
qualification tests.

| Direct-CAS operation | Shardline `85e225ca` seconds | Upstream local reference seconds |
| --- | ---: | ---: |
| Initial upload | 5.94–7.79 | 1.15–1.36 |
| Initial full download, including SHA-256 | 0.64–0.77 | 0.84–1.06 |
| Tail-edit upload from a cold client | 0.68–0.74 | 0.27–0.29 |
| Edited full download, including SHA-256 | 0.66–0.72 | 0.83–0.92 |

Initial retained lengths were 1,087,992,784 bytes for Shardline and 538,731,316
for upstream; after the edit, 1,088,534,296 and 538,495,004 bytes respectively.
Initial-upload server peak RSS was 49,192–50,908 KiB versus
211,124–211,824 KiB. These are server measurements, separate from client
memory. Upload speed and retained-storage parity remain open. The two download
observations do not establish general superiority, production performance,
tail latency or scale-out efficiency. The same weaker reference persistence
contract and shared-host qualifications apply.

For those initial uploads, Linux server-process `write_bytes` increased by
1,656,995,840–1,722,712,064 bytes for Shardline and
537,808,896–537,890,816 bytes for upstream. This accounting includes temporary
and filesystem writes; it is not physical NVMe NAND wear or a backend-inclusive
cost estimate. Reducing persisted bytes alone would not establish that the
remaining ingestion I/O amplification has been removed.

#### Native uploads without decoded copies

Runtime `62795b73` removes decoded standalone-chunk writes from native Xet
ingestion. Both memory and file inputs retain the verified canonical container;
generic standalone chunk uploads retain their separate contract. Existing data
remains readable, but new independent native chunk reads require the upgraded
reader. See [the rollout boundary](COMPATIBILITY_STATUS.md#current-source-tree-contract).

The October 27 repeat used the same checksummed 512 MiB incompressible corpus,
256 KiB tail edit, stock hf_xet 1.7.0 / Hub 2.2.0, matched release profiles and
upstream/Shardline/Shardline/upstream ordering described above. Both servers
used the same one-logical-CPU affinity. No competing compilation or qualification
was observed during the timed sequence. All eight downloads passed SHA-256.
The frozen Shardline binary's SHA-256 was
`f16dcd4dd967ac1e5d7a826149f2d1bc68bd4ca2a4c2c71eb6101fbd1f74b508`.

| Direct-CAS operation | Shardline `62795b73` seconds | Upstream local reference seconds |
| --- | ---: | ---: |
| Initial upload | 2.40–2.75 | 1.17 |
| Initial full download, including SHA-256 | 0.64–0.66 | 0.91 |
| Tail-edit upload from a cold client | 0.71–0.74 | 0.27–0.31 |
| Edited full download, including SHA-256 | 0.68–0.70 | 0.87 |

Initial retained file lengths, including metadata, were 551,121,872 bytes for
Shardline and 538,731,316 bytes for upstream, a 2.30% difference. After the
edit they were 551,393,972 and 538,495,004 bytes, a 2.40% difference. This
removes the previous approximately twofold retained-byte amplification, but
does not establish exact total-storage parity. After the edit, both stores
contained the same ten Xorb identities. Their serialized Xorb files occupied
537,608,956 and 537,541,940 bytes respectively; serialized encodings were not
byte-identical. Logical downloads were independently verified. These are
initial/post-edit lengths, not post-retention economic or production-corpus results.

Initial-upload server peak RSS was 53,020–53,284 KiB for Shardline versus
211,868–212,084 KiB for upstream. Linux server-process `write_bytes` increased
by 559,476,736–563,277,824 bytes versus 537,792,512–537,841,664 bytes.
This accounting includes scratch and filesystem writes, not physical NAND wear.
The earlier `85e225ca` run above used the same corpus and method, but was a
separate campaign; its timings are historical observations rather than a new
paired before/after measurement.

Upload-speed parity remains open. Two observations per server do not prove
general download superiority, p95/p99, multi-node scaling or hosted Hugging Face
performance. The reference development CAS has weaker persistence and
synchronization semantics; this optimization retains Shardline's immutable
publication and validation checks. Shared-host limitations still apply.

A separate symbolized CPU profile of this path used the same 512 MiB fixture,
stock client and single logical server CPU. Sampling at 199 Hz captured 274
samples with no lost samples. SHA-256 file verification accounted for about
15% of sampled cycles; BLAKE3 verification appeared across several workers.
Kernel copying, networking and filesystem allocation also appeared among the
largest individual entries. These are sampled CPU costs, not wall-clock time
attribution, and the short capture cannot explain the complete upload gap.
Instrumented timings are excluded from the table. The profile provides no
justification for skipping member authentication, file identity verification
or durable publication to match the development CAS.

A later remote profile used the copy-free runtime with shard read buffering,
PostgreSQL/MinIO, the stock Hub 2.2.0 / hf_xet 1.7.0 client and a 512 MiB
incompressible file. Native counters confirmed nine Xorbs and one shard, with
no regular Hub body upload; the complete download passed SHA-256 verification.
Sampling at 99 Hz captured 1,211 samples with none lost. Among self-cycle
entries, BLAKE3's bulk AVX-512 routine accounted for about 22%, SHA-256's digest
routine 11%, and kernel file copying 11%. Application offsets were resolved
using independently verified identical executable-code bytes and ELF mappings
from a symbol-bearing build. These short, instrumented observations locate CPU
work; they do not measure wall-clock attribution or upload-speed parity.
A separate symbol-bearing repeat encountered 414 MinIO HTTP 507 storage-reserve
responses and was stopped. Its timings and CPU samples are excluded. Repeated
reads and scratch handling remain optimization targets; authentication and
durable publication remain required.

The subsequent ingress-checksum removal (`2e443f49`), together with footerless
preparation refactoring, was exercised against the preceding optimized binary
and the pinned upstream CAS on one identical 512 MiB corpus and 256 KiB tail
edit, twice per executable. All twelve full downloads passed SHA-256. The four
Shardline lanes retained identical Xorb keys, lengths and SHA-256 hashes,
totaling 537,597,299 payload bytes after the edit. Their total file lengths,
including metadata and journals, also matched: 550,890,136 bytes initially and
551,158,971 after the edit. Competing compilation was observed during this
campaign, so its timing results are excluded. This establishes fixture-specific
payload preservation, not a quantified upload-speed improvement.

The same optimized runtime also completed a separate stock Hub 2.2.0/hf_xet
1.7.0 upload and SHA-256-verified download of 512 MiB through PostgreSQL and
MinIO. Native ingestion recorded nine Xorbs, one shard and no regular Hub body
uploads. Upload traffic remained 169 S3 requests, and retained storage comprised
16 objects totaling 537,890,475 bytes, including 537,345,752 Xorb bytes. This
confirms that the ingress refactoring preserved the tested copy-free backend
costs. Competing compilation was active, so this run supplies no throughput
comparison or native-speed parity claim.

The stopped profile's SQLite files comprised a 5,439,488-byte database,
7,774,472-byte WAL and 32,768-byte shared-memory file. An isolated copy passed
SQLite integrity checks before and after a successful truncating checkpoint;
the resulting database occupied 5,509,120 bytes. Thus this snapshot includes
reusable journal space as well as retained metadata. This diagnostic does not
change the live-server measurements above, establish steady-state storage cost
or justify forcing a checkpoint on every upload. The largest database page
consumers were verified Xorb descriptors and their chunk membership indexes.

A bounded 64 KiB read buffer for shard source validation was then compared
against the copy-free runtime in baseline/candidate/candidate/baseline order,
using the same 512 MiB corpus, cold stock clients and release profiles. No
competing compiler or nextest qualification was observed. Initial-upload Linux
process read calls fell from 246,384–246,388 to 151,972–151,982; edited-upload
calls fell from 104,550 to 10,085. Retained bytes were identical and all eight
downloads passed SHA-256. Initial upload times overlapped (2.59–3.74 seconds
versus 2.94–3.42); edited uploads took 0.711–0.712 versus 0.667–0.699 seconds.
The result establishes fewer read syscalls, not a general throughput gain or
less physical storage I/O. The buffer lasts only for validation and is dropped
before later member reads; cancellation and content checks remain in place.

A separate preparation-read buffering prototype was removed. Its 512 MiB
baseline/candidate/candidate/baseline control cut initial-upload read calls from
151,960–151,978 to 57,534–57,540, with identical retained bytes and eight verified
downloads. However, candidate uploads took 3.12–4.68 seconds versus 2.48–2.76,
and peak RSS was 52,004–54,112 versus 41,052–41,780 KiB. A repeat again verified
all bytes but observed competing compilation, excluding its timings. The
prototype passed 375 adapter tests, yet fewer syscalls did not demonstrate a
speed or memory improvement. These observations do not isolate the cause of
the RSS difference or establish a general regression.

A separate 1 GiB diagnostic used deterministic incompressible blocks and a
256 KiB tail replacement, fresh stock clients, the same optimized binaries and
one logical server CPU. Both initial and edited downloads from each server
passed independent SHA-256 checks. Shardline's retained file lengths rose from
1,096,177,888 to 1,096,491,979 bytes after the cold-client edit, a 314,091-byte
increase including metadata. Its final 18 Xorbs occupied 1,074,991,059 bytes.
This fixture did not reproduce the hundreds-of-megabytes edit amplification
observed in the historical remote fixture below. It does not establish optimal
reuse for every corpus or reproduce that earlier fixture exactly.

The same diagnostic observed initial/edited upload times of 6.23/1.29 seconds
for Shardline and 3.71/0.69 seconds for upstream. Initial-upload server peak
RSS was 51,172 versus 329,588 KiB. These are single-run, shared-host local-CAS
observations without a contention qualification, not a speed-parity result or
an aggregate memory bound. Upstream's development CAS retains weaker durability
semantics. Input checksums, inventory and verified results were preserved before
removing the completed diagnostic's payload copies.

An additional October 27 experiment buffered footer serialization in 64 KiB
blocks. A 64 MiB instrumented baseline/candidate/candidate/baseline comparison
reduced staging writes from 3,333–3,341 to 167–171, while preserving
67,167,784 staging bytes, 11,597 synchronization calls and all eight verified
downloads. These traced timings were excluded from performance conclusions.

The separate uninstrumented 512 MiB comparison observed no competing compiler
activity. Baseline uploads took 11.69 and 15.78 seconds; the candidate took
15.70 and 12.47 seconds. Candidate peak server RSS was 57–61 MiB versus
48–49 MiB for the baseline. Retained bytes were identical and all eight
downloads passed SHA-256. Adapter tests and local/remote process-interruption
checks passed, but fewer syscalls did not demonstrate faster ingestion or less
disk writing. The prototype was removed. These shared-host observations do
not establish a regression, SSD lifetime savings or native Xet parity.

#### Reusing serialized validation reads

An earlier control compared runtime `84cc1a97` with a candidate that derives
the serialized storage digest from bytes already read by native Xorb validation.
It hashes only a contiguous prefix, then reads any remaining suffix in order;
footer-first probes, backward seeks and overlapping reads cannot reorder or
duplicate digest bytes. A bounded 64 KiB buffer batches small validation reads
for BLAKE3. Native content/member validation, final immutable-write verification,
cancellation and durable publication remain in place.

The same 512 MiB corpus, stock hf_xet 1.7.0, one logical server CPU and local
disk were used in baseline/candidate/candidate/baseline order. All eight
downloads passed independent SHA-256 verification.

| Initial-upload metric | Runtime `84cc1a97` | Buffered read-reuse candidate |
| --- | ---: | ---: |
| Process-accounted bytes read (`rchar`) | 4,857,134,586–4,857,155,065 | 4,320,176,962–4,320,193,345 |
| Read system calls (`syscr`) | 658,515–658,519 | 592,968–592,971 |
| Server CPU seconds | 5.50–5.87 | 5.52–5.60 |
| Completed upload seconds | 7.05–7.35 | 6.70–6.78 |
| Upload server peak RSS, KiB | 49,736–50,680 | 48,916–51,320 |

This removes approximately 537 MB of process-accounted reads per initial
upload, about 11% in this fixture. Retained file lengths remained identical:
1,087,903,184 bytes initially and 1,088,532,774 after the edit. Measured CPU
and RSS ranges overlap. Two shared-host runs do not establish a general
throughput improvement or tail-latency bound. Warm-cache `rchar` is not physical
disk traffic; process-accounted writes did not demonstrate a reduction. This
control therefore does not establish lower S3 request charges, storage costs
or NVMe wear, and does not close the upstream upload/storage parity gaps.

An earlier unbuffered implementation removed the same read pass but increased
CPU and elapsed upload time. Batching small hash updates was necessary to avoid
that regression; fewer reads alone were not sufficient evidence of improvement.

#### Larger direct-CAS comparison

Runtime `d59c741d` and the same pinned upstream reference were also tested with
a 1,025 MiB incompressible file and a 256 KiB tail edit. Two separate
upstream/Shardline/Shardline/upstream sequences reused that corpus, with fresh
client processes and caches. All sixteen downloads passed independent SHA-256
verification. The one-CPU local-disk setup still bypassed the Hub control plane.

| Direct-CAS metric | Shardline `d59c741d` | Upstream local reference |
| --- | ---: | ---: |
| Initial upload seconds | 13.37–19.18 | 1.89–13.51 |
| Tail-edit upload seconds | 1.40–1.57 | 0.53–0.87 |
| Initial full download seconds, including SHA-256 | 1.26–1.36 | 1.73–3.83 |
| Edited full download seconds, including SHA-256 | 1.25–1.44 | 1.67–2.02 |
| Initial upload server CPU seconds | 11.20–11.64 | 1.66–5.77 |
| Initial upload server peak RSS, KiB | 65,208–68,360 | 279,232–352,664 |
| Initial retained file lengths, decimal GB | approximately 2.172 | approximately 1.077 |

The upstream initial-upload observations were 13.51, 1.89, 2.01 and 1.98
seconds; the first observation is retained rather than excluded. Shardline
observations were 19.18, 18.49, 13.37 and 14.34 seconds. The variability prevents
a robust single speed ratio. Shardline's lower download times in this fixture
do not establish general superiority, production throughput or tail latency.
Server RSS excludes the stock client's memory, and the retained totals include
metadata before retention-expired GC. Native decoded copies still account for
substantial additional storage. These controls extend local direct-CAS byte
correctness beyond 1 GiB; they do not qualify Hub-only, remote-storage,
multi-host or all supported-client large-file deployments. The upstream
durability and production-comparability qualifications above still apply.

A separate four-run PostgreSQL/MinIO comparison used baseline/candidate/
candidate/baseline order, stock Hub 2.2.0/hf_xet 1.7.0, one combined-role
server limited to one CPU/1 GiB, and 512 MiB incompressible files with a
256 KiB tail edit. Each upload and download used a fresh client cache.
All eight downloads passed SHA-256 verification, and API traces reconciled
with MinIO counters.

| Edited-upload metric | Single-container baseline | Related-container candidate |
| --- | ---: | ---: |
| Backend S3 requests | 3,384 | 61 |
| Native Xorb upload bytes | 210,709,611 | 310,157 |
| Added retained payload bytes | 211,391,666 | 623,418–623,420 |
| PostgreSQL calls, including nested calls | 3,879 | 311 |
| Statement WAL bytes | 2,903,168–2,903,169 | 87,394–87,471 |
| Client operation seconds | 2.75–2.76 | 1.29 |
| Shardline CPU seconds | 2.32–2.33 | 0.81 |
| Shardline process-accounted read bytes (`rchar`) | 2,393,132,250 | 539,824,192 |
| Shardline process-accounted physical write bytes (`write_bytes`) | 1,102,475,264 | 537,665,536 |

Initial uploads still made 8,538 backend requests and 9,402 PostgreSQL calls;
initial retained payload stayed approximately 1,074.75 MB. Edited-download
requests fell from 28 to 24. WAL is statement-attributed rather than complete
cluster WAL. Retained payload excludes metadata and is measured before GC;
it differs from the local reference's retained-file accounting. These are
shared-host fixture results, not a production cost-per-TB or scale-out claim.
Server cgroup lifetime memory peaks through the edit were 372–444 MB for
baseline and 327–339 MB for candidate, including charged cache; they are not
isolated application RSS or proof of an aggregate concurrency ceiling. Warm
cache physical read accounting was zero in both. Remaining verification and
staging writes still matter; lower process I/O does not establish device-level
write amplification, NAND endurance or a replacement interval.

### PostgreSQL and MinIO backend cost

#### Copy-free native ingestion

An October 27 comparison used frozen copy-retaining runtime `f59d2dff` and
the copy-free runtime with shard read buffering (`74406a92`). Both used the
same deterministic 512 MiB incompressible input, stock Hub 2.2.0/hf_xet 1.7.0,
fresh client homes and disposable PostgreSQL/MinIO. Each combined-role server
had four CPUs and a 1 GiB limit. Two successful runs per runtime completed
native upload, shard publication and an independently SHA-256-verified download;
MinIO traces reconciled with provider counters.

| Initial native upload | Copy-retaining | Copy-free |
| --- | ---: | ---: |
| Backend requests | 8,635 | 169 |
| Standalone chunk PUTs | 8,460 | 0 |
| Retained objects | 8,476 | 16 |
| Retained payload bytes | 1,074,761,387–1,074,761,389 | 537,890,475 |
| Trace bytes received by MinIO | 1,075,611,992–1,075,611,994 | 537,911,460 |
| PostgreSQL calls, top-level / nested | 1,029 / 8,572 | 1,029 / 8,572 |
| Top-level statement WAL | approximately 6.64 MB | approximately 6.64 MB |
| Server-process write bytes | approximately 1.075 GB | approximately 1.075 GB |
| Server-process peak RSS | 162,736–164,912 KiB | 163,640–212,264 KiB |

This fixture demonstrates 98.0% fewer S3 requests and approximately 50% less
retained payload, without a corresponding reduction in PostgreSQL work or
temporary disk writes. RSS ranges overlap but the copy-free maximum was higher;
there is no demonstrated memory saving. Uploads took 12.23–12.38 seconds versus
4.12–4.67 seconds. These are shared-host observations, not a native-CAS parity,
production latency or scale-out result. Process I/O is not physical NAND wear.
All readers must be upgraded before accepting copy-free native uploads; old
standalone-chunk readers cannot fully serve the new layout.

One additional copy-free attempt failed: MinIO returned 507 for 676 multipart
part attempts as host disk space became constrained, and the stock client
ultimately reported an error. It is excluded from successful performance
results. After reclaiming only completed benchmark payload copies, the repeat
passed. Failed-run diagnostics were retained. These retained-byte measurements
precede GC retention and are not annual cloud-bill projections.

#### Historical measurements with decoded copies

The following observations predate removal of the decoded-copy writer. Its
request-hint and repeated-member optimizations describe the old implementation,
not an additional write path retained by the copy-free runtime.

A separate October 26 run used executable `97df1303`, Hub 2.2.0/hf_xet
1.7.0 and the same runner with `--file-mib 1024`. One combined-role server
was limited to one CPU/1 GiB. Both fresh-client downloads passed SHA-256
verification, and backend traces reconciled with MinIO API counters.

| One-GiB operation | Client operation seconds | Backend requests |
| --- | ---: | ---: |
| Initial native upload | 10.32 | 17,037 |
| Initial cold-client download | 3.37 | 44 |
| 256-KiB tail edit, cold-client upload | 74.94 | 12,641 |
| Edited cold-client download | 4.43 | 52 |

This exposes a remaining large-file cost: the edit still sent 398,909,600
Xorb bytes and made 6,240 duplicate standalone-chunk PUT attempts plus 6,240
chunk GETs. Fifteen dedup shard hits establish reuse, not optimal reuse.
Initial retained payload was 2,149,499,884 bytes; the second version added
399,886,933 bytes. Compatibility chunk copies remain included. The server
cgroup lifetime peak was 708.1 MiB, including charged cache; this is not
application RSS or a concurrency ceiling. These are single-run observations
with warm backend caches, not production throughput or a before/after win.

Repeating that one-GiB fixture with the request-order hint window increased
from 512 to 32,768 keys reduced edited-upload backend requests from 12,641
to 6,401. Duplicate chunk PUTs fell from 6,240 to zero; all 6,240 verified
chunk GETs remained, alongside four new chunk PUTs. Initial-upload requests
remained 17,037, download requests remained 44/52, and retained chunk/Xorb
payloads were unchanged. Both downloads and trace reconciliation passed.
The hint stores bounded key digests, not payload bytes, and a hit still checks
remote content length and hash; stale hints retain conditional-create fallback.

The edited upload took 5.00 seconds in the repeat versus 74.94 in the control.
Other host activity was present, so this pair establishes the request change,
not a repeatable latency multiplier. Server cgroup lifetime peaks were 708.1
and 678.2 MiB respectively, including cache; their difference does not isolate
hint memory or establish an RSS reduction. Native chunk-copy storage and the
398.9 MB of edited client Xorb traffic remain separate optimization gaps.

The Linux client-memory sampler now reads the native client's own
`/proc/self/status` `VmHWM`. Earlier `getrusage` figures could inherit the
fixture generator's pre-exec memory peak and must not be used as isolated
client RSS measurements. A fresh one-GiB run with the corrected sampler again
passed both downloads and recorded 17,037/44/6,401/52 backend requests.
Client peaks were 777,144/476,040/486,464/958,784 KiB for initial upload,
initial download, edited upload and edited download respectively. These are
stock-client observations, separate from Shardline cgroup memory, and do not
establish a concurrent-client memory ceiling.

The first repeated-member optimization made consecutive identical members
within one uploaded Xorb share the first
immutable chunk write. Every member is still decoded and checked against its
native hash, offsets, length and storage digest; success waits for persistence.
The reuse state holds one identity pair and ends with the upload. A controlled
HTTP-provider fixture with 128 identical 4 KiB members measured 128 requests
before this change and one afterward. Mutation, existing-object conflict and
fresh-call retry controls pass. SQLite/filesystem and PostgreSQL/MinIO drills
also read repeated-member content with unchanged v1.12.1 after GC and rebuild.
This is a specific repeated-member saving, not a stock-client throughput result
or a reduction in unique-data storage copies.

A PostgreSQL/MinIO follow-up uploaded that same 128-member Xorb through the
native HTTP endpoint, then retried it unchanged. Initial-upload requests fell
from 133 to six; retry requests fell from 129 to two. Each phase avoided 127
standalone-chunk GETs, saving 508 KiB of backend response bytes. Both versions
retained two objects totaling 535,672 bytes, and independent reads verified the
stored chunk and complete Xorb exactly. Traces reconciled with MinIO's API
counters. This single local comparison covers Xorb upload and retry before
shard publication, using a frozen pre-change binary and the updated debug
binary. It establishes request counts, not comparable CPU, memory or latency.
Stock clients may deduplicate repetitions before constructing an Xorb; these
figures must not be applied to all native uploads or annual storage bills.

A subsequent change also shares nonconsecutive repetitions within an upload,
retaining at most 128 identity triples (native hash, storage digest and length)
and no decoded payloads. Every occurrence is still decoded and authenticated;
all required writes must complete before success. Once the set fills, other
identities use the normal storage path, with consecutive repetition sharing
retained. The set is discarded after each upload, so retries verify storage
again. No persisted format, chunk-copy requirement or durability rule changes.

For 128 alternating A/B members of 4 KiB each, a real PostgreSQL/MinIO HTTP
comparison reduced initial-upload requests from 133 to seven and unchanged
retry requests from 129 to three. Each phase avoided 126 standalone-chunk GETs
and 504 KiB of backend response bytes. Both versions retained the same three
objects totaling 539,768 bytes. Independent backend reads verified both chunk
copies and the complete Xorb. This compares a frozen optimized previous binary
with the updated debug binary: it establishes request counts, not a latency,
CPU or memory improvement. Unique-data uploads do not receive this saving.
Tests cover the identity-set limit, mutation of a repeated occurrence, failed
and cancelled persistence, and retry after six actual process-kill boundaries.
The PostgreSQL/MinIO crash drill also covers 256 alternating 128 KiB members:
two replacement replicas retry equivalent encodings after each boundary,
corrupted retries are rejected, and both legacy decoded chunk copies remain
byte-for-byte intact. These recovery checks establish neither throughput parity
nor removal of the duplicate-storage requirement.

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

Resource samples also report phase deltas from cgroup v2 `io.stat`, separately
for each block device. Compare `rbytes`, `wbytes`, `rios` and `wios` on the same
device across equivalent runs; do not sum stacked device entries. Missing or
reset counters invalidate the measurement. These are kernel-accounted block
operations during the observation window, so delayed writeback can cross phase
boundaries. They exclude device-internal write amplification and do not measure
NAND wear or predict NVMe lifetime.

Add `--slow-readers 32` for an explicit plain-HTTP pressure check. It starts
32 simultaneous readers with 32 KiB client buffers and a delay between reads,
then 32 readers that abandon their responses after the first buffer. A complete
plain-HTTP read and a cold stock-Xet download must succeed afterward. Every
completed body is length/SHA-256 verified, redirects are confined to the same
Shardline origin, and server process peak RSS must remain below 384 MiB during
the two bursts. Abandoned responses contribute backend cost but no successful
logical bytes, so their results omit requests-per-GB normalization.

The October 26 check on source `8f1b5d36`, using the same client versions and
36 MiB workload above, passed with the combined Hub replica limited to one
CPU/1 GiB and PostgreSQL/MinIO storage. The executable SHA-256 was
`6c08095cbd443a8c586cfa8a7db286bdc6775aa07f8c19e46056fe5135f5dc16`.
All 32 slow readers completed verified downloads; all 32 abandonment clients
stopped before completing the file. Observed server peak RSS was about 131 MiB
after the complete-read burst and 152 MiB after the abandonment burst and
following plain read. Backend attempts were 256 and 224 respectively, and the
trace reconciled against MinIO counters. Both subsequent download checks passed.

Abandonment still caused approximately 1.209 GB of backend response transfer
in this burst. Cancellation releases response resources, but cannot reclaim
verification reads already performed before the first client buffer. This is
one shared-host pressure qualification, not sustained-load or multi-host
performance evidence, and not a 32-reader/1 GiB-file qualification.

### Rejected serialized-digest fusion experiment

A prototype computed the normalized upload's serialized BLAKE3 digest during
existing full validation instead of rereading the complete staging file.
Four reversed-order 512 MiB PostgreSQL/MinIO campaigns passed exact downloads
and trace reconciliation. Application reads fell from 4.836 GB to 4.299 GB,
and read syscalls from 653,205 to approximately 587,665. Backend requests
remained 8,538; physical read accounting was zero in both warm-cache cases.

The prototype used 4.299/4.314 server CPU seconds versus 4.238/4.176 for the
control. Upload times were 6.150/5.452 seconds versus 5.340/5.491. These two
pairs do not establish a speedup or hardware-wear saving. The prototype was
removed rather than retaining extra machinery for an unproven overall benefit.
The existing complete validation and serialized identity checks remain.

### Adjacent native download sources

Native plain-HTTP downloads now retain one response-local Xorb source across
adjacent terms. Every term still validates the complete source and rechecks
selected members. Changing sources releases the old spool before fetching the
next one. A real MinIO regression with source order `A,A,B,A` reduces GETs from
four to three; it also checks eviction, fresh-response corruption and cancellation.
A separate test corrupts a retained local inode between terms and requires
rejection before the next member is sent.

Two alternating before/after pairs compared `78fb1fe1` with `9d6f6092`, using
stock Hub 2.2.0 / hf_xet 1.7.0, 512 MiB incompressible content, a 256 KiB tail
edit, four CPU cores and a 1 GiB server limit. Each run verified unique and
overlapping native downloads, four complete slow plain-HTTP downloads, four
abandoned responses and subsequent complete plain/native downloads. MinIO
traces reconciled against provider request counters in all four runs.

| Four complete 512 MiB plain-HTTP downloads | Before, two runs | After, two runs |
| --- | ---: | ---: |
| Backend requests | 84 | 84 |
| Backend response bytes | 3.566 GB | 3.566 GB |
| Server CPU | 6.14–6.66 s | 6.08–6.20 s |
| Server process peak RSS | 134.5–172.9 MiB | 105.8–156.7 MiB |
| Kernel-accounted writes on the same host device | 1.883–1.915 GB | 2.000–2.169 GB |

This generated workload does not exercise adjacent repeated terms, so the
change saves no backend requests or bytes here. Memory ranges overlap; the
write observations are higher afterward and affected by delayed writeback.
These runs establish neither lower disk writes nor longer SSD lifetime.
Slow-reader timings are client-paced, not an upstream-Xet throughput comparison.
The targeted request reduction must not be generalized to all native workloads.

### Selected-member decoding after full validation

A DWARF profile of four plain-HTTP native downloads attributed about 30% of
inclusive samples to full source validation and 23% to the subsequent decoding
pass. That second pass decoded unrequested members too. Reconstruction now
seeks directly to selected members after validating the complete container,
and rehashes every returned member. It retains the same source-health checks,
container spool limit, cancellation and file SHA-256 verification.

Two alternating pairs compared `9d6f6092` with this decoder using the same
512 MiB native fixture, four CPU cores and four unpaced complete HTTP readers.
Use `--slow-readers 4 --reader-delay-ms 0` to remove the default 1 ms delay
after each 32 KiB client read. All unique/overlapping uploads, complete and
cancelled downloads, subsequent reads and MinIO trace reconciliations passed.

For the logical-I/O columns below, sample `/proc/<server-pid>/io` immediately
before and after the four-reader phase and subtract `rchar`, `syscr` and
`write_bytes` independently. Read the host process counters with appropriate
permissions, rather than summing container and host counters. `rchar` counts
logical reads, including page-cache hits; it is not physical device traffic.
These extra process-counter samples were collected alongside the harness;
they are not fields in its default result JSON.

| Four complete plain-HTTP downloads | Before | Selected-member decoder |
| --- | ---: | ---: |
| Logical process file-read bytes | 7.130 GB | 5.714 GB |
| Read syscalls | 1,195,736 | 991,532 |
| Backend requests | 84 | 84 |
| Backend response bytes | 3.566 GB | 3.566 GB |
| Server CPU, two runs | 6.55–6.85 s | 6.40–6.47 s |
| Complete-response wall time, two runs | 1.87–2.86 s | 1.97–2.79 s |

Logical reads fall about 20% and read syscalls about 17%, without additional
backend calls. Timing reverses across pairs, so these measurements establish
less internal work, not a repeatable throughput gain. Accounted process writes
remain about 3.57 GB in both versions; no SSD-wear saving is established.
Process peak RSS stays below 168 MiB in these runs but includes the preceding
uploads and does not isolate this decoder's memory use. This change concerns
Shardline's native-term reconstruction for plain HTTP reads, not a measured
speedup of stock `hf_xet` CAS transfers or parity with upstream Xet.

### Native transfer metadata requests

The native serialized-member HTTP contract uses real MinIO with both local and
PostgreSQL metadata. An authorized explicit member range previously used one
HEAD and one GET. It now uses one GET and zero HEAD requests when authoritative
file metadata already covers the requested offsets. The complete GET is still
validated before response bytes are exposed; the response length comes from
that verified source, and published member offsets must match its layout.

The contract also replaces a shared Xorb with a different valid compression
encoding under the same native content hash. The old member grant is rejected
rather than widening access to the other repository's bytes. Corruption outside
the requested member also fails verification. Suffix and open-ended requests,
incomplete coverage and standalone HEAD requests retain metadata discovery.
This removes one backend request for the qualified path; it does not reduce
container bytes fetched or establish a throughput, SSD-wear or upstream-Xet
parity improvement.

A stock Hub 2.2.0/hf_xet 1.7.0 check uploaded and downloaded the same 64 MiB
incompressible file through real PostgreSQL/MinIO, with fresh clients and owned
deployments in before/after/after/before order. All four downloads passed
independent SHA-256 verification and their traces reconciled with MinIO API
counters. For the member-read change alone, backend requests fell from eight to seven in both
pairs: one Xorb GET remains, its HEAD disappears, and the three other
HEAD/GET pairs remain. Both versions retained 1,066 objects totaling
134,346,551–134,346,555 payload bytes; the few-byte variation is metadata,
with identical chunk, Xorb and shard lengths. These debug-binary controls do not
establish a production timing or memory improvement.

The following Hub archive change reuses the existing bounded single-GET reader
for Git identity and pack reads. It preserves size limits, response-length
checks, pack parsing and commit-identity verification. Two further stock-client
runs of the same fixture each used four GETs and zero HEADs for the complete
download: three identity reads and one Xorb read. Both downloads passed SHA-256
and reconciled MinIO request counters. This halves the original eight requests
for this workflow, with unchanged chunk, Xorb and shard lengths and about
67.17 MB of backend response data. It does not remove full-Xorb verification,
scratch writes or decoded legacy copies, and is not evidence of upstream upload
or storage parity.

### Rejected decoded-buffer ownership experiment

An experiment transferred decoded buffers directly into HTTP responses instead
of copying each payload. Two alternating pairs of four 512 MiB readers passed
all integrity/recovery checks but showed overlapping timing and memory ranges,
with unchanged backend requests and I/O. Under sixteen concurrent 1 GiB reads,
the candidate completed in 43.3 and 37.2 seconds; an immediate baseline control
took 32.7 seconds. All three runs verified complete files, cancellation recovery
and MinIO traces on four CPUs with a 1 GiB server limit.

The handoff was reverted before publication: removing a copy did not establish
a practical improvement. Retained decoder-buffer capacity is a possible source
of memory pressure, not a proven explanation. The existing exact-length payload
copy, bounded channel and complete-source/member checks remain.

### Large slow-reader admission limit

A subsequent 32-reader, 1 GiB run on four CPUs with a 1 GiB server limit
did not pass: the plain-HTTP phase returned HTTP 503. Readers paused 1 ms
after every 32 KiB, imposing at least 32.8 seconds per complete file before
network and verification work. Default admission weight 256 and reconstruction
weight 16 allow at most 16 active reconstructions before other work; a queued
download waits at most 30 seconds. This workload can therefore exhaust the
queue deadline even with healthy storage. Overload responses retain a
`Retry-After` header; they must not be counted as successful downloads.

This result does not qualify 32 simultaneous slow 1 GiB readers. Keep the
admission bounds when comparing workloads, report rejected requests, and
distinguish client pacing from server throughput. Higher limits require a
separate resource-pressure qualification, rather than assuming spare RAM or
disk bandwidth.

An unpaced repeat also reached HTTP 503 after both stock-client uploads and
downloads verified successfully. Removing the client delay therefore does
not establish 32-reader capacity on this four-CPU host. A separate attempt
failed during a stock-client download before pressure began; its generic
diagnostic did not identify the cause, so that failure remains unexplained.

A separate 16-reader unpaced control passed on the same four-CPU, 1 GiB
configuration with stock Hub 2.2.0 / `hf_xet` 1.7.0. All 16 complete 1 GiB
files matched SHA-256 in a 36.9-second batch. The subsequent 16 abandoned
responses and complete plain/stock-client recovery downloads also passed;
MinIO trace counts reconciled. Server process peak RSS stayed below 290 MiB
through the pressure and recovery phases. This is one shared-host capacity
observation, not a comparative throughput improvement or a p99 measurement.
Its batch duration also shows why a second wave can exceed the 30-second
queue deadline without artificial client pacing.

Two further unpaced runs with the same executable and the existing
`SHARDLINE_BLOCKING_IO_POOL_SIZE=8` override completed all 32 concurrent
1 GiB files in 51.6 and 52.4 seconds. Each run also verified 32 abandoned
responses, subsequent complete plain and stock-client downloads, and independent
MinIO trace reconciliation. Process peak RSS during complete downloads was
177 and 272 MiB. Each complete batch used 1,088 backend requests, transferring
about 59.9 GB for 32 GiB of logical content. A matched run with the default
16-worker setting returned HTTP 503.

This is a resource-sizing diagnostic on one shared host, not a default change
or an upstream Xet comparison. Fewer simultaneous source spools can relieve
memory pressure, but these runs do not isolate that mechanism or establish
physical SSD endurance savings. Smaller pools can also reduce throughput when
backend latency is higher, as the delayed-backend experiment above demonstrates.
Retain the existing admission, integrity and cancellation checks when evaluating
pool sizes for a deployment.

A 36 MiB control completed all 32 readers, cancellations and verified recovery
with both settings. Complete-download batch times were 1.15 seconds with 16
workers and 1.28 seconds with 8; both made 256 backend requests. This single
pair is insufficient to quantify a small-file regression, but it reinforces
that the large-file result does not justify lowering the global default.

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
