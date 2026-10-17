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

The final binary also passed a PostgreSQL-metadata/local-object drill with
eight concurrent 36 MiB uploads, identical overwrites, GC and fsck. It retained
16 containers and no redundant raw objects. All 24 full-body reads passed,
including eight reads using released v1.12.0 after maintenance. This is a
single-host compatibility/correctness check, not a cluster scaling result.

Small-object reads used for OCI media types and legacy metadata now obtain
length and bytes from one S3 GET instead of HEAD plus GET. An HTTP-provider
fixture checks GET-only behavior for success, oversized objects, missing
objects and denied reads. The caller retains its 64 MiB buffering limit;
response tests reject partial ranges and short or overlong bodies. This removes
one request per successful affected read; missing objects still need one request.
No application-throughput improvement has been
measured for this separate change. Local object reads retain their existing
path, and no disk staging or additional payload copy is introduced.

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

This is a remaining optimization opportunity: the offset-zero fixture transferred
about 8,178 backend bytes per logical byte returned. It is not a measured average
for random reads. Reducing this amplification must preserve verification of the
requested content against a trusted commitment, including corruption, truncation,
incorrect ranges and concurrent replacement. Fetching an unchecked byte range
instead of validating the container would weaken the current guarantee. No
reduction in range-read transfer bytes is claimed by this branch.

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
