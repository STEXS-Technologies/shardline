# Performance

Shardline only wins if it is operationally cheaper without feeling slower than the
alternatives.

That means performance work is not optional.
It is part of protocol compatibility and product viability.

## Performance Goals

Shardline should aim for:

- write paths that avoid full-blob re-upload when content changes sparsely
- read paths that reconstruct files from already-present immutable objects with bounded
  overhead
- predictable latency under concurrent repository activity
- hot paths that stay easy to profile and harden

The target user experience is:

- push feels normal
- fetch feels normal
- storage bills shrink

## Benchmark Categories

Shardline needs three layers of measurement.

## 1. Microbenchmarks

Microbenchmarks isolate hot functions such as:

- hash parsing
- range parsing
- chunk boundary computation
- reconstruction planning
- dedupe lookups
- serialization and response shaping

These measurements answer:

- where CPU time goes
- whether a refactor helps
- whether an invariant check is too expensive

Required tools:

- `perf`
- flamegraphs
- focused Rust benchmark harnesses

Shardline publishes the current benchmark and profiling commands in
[Profiling](PROFILING.md).

## 2. Component Benchmarks

Component benchmarks measure realistic server subsystems:

- xorb upload validation
- chunk insertion into the dedupe index
- reconstruction lookup
- range reads through the transfer path
- token issuance under provider checks

These measurements answer:

- whether protocol handlers allocate too much
- whether database access dominates request time
- whether object-store reads are the real bottleneck

## 3. End-to-End Benchmarks

End-to-end benchmarks must use real Git workflows.

Required scenarios:

- initial push of a large asset
- update that changes one small region of a large asset
- repeated pull of the latest version
- checkout of an older commit
- concurrent repository fetches
- concurrent uploads to the same repository

These measurements answer:

- does it actually feel as smooth as LFS
- how much data moved over the wire
- how much new storage was written
- where user-visible latency is coming from

## Benchmark Command

Shardline ships a deterministic benchmark command:

```bash
shardline bench --storage-dir /tmp/shardline-bench
```

By default the command creates isolated iteration roots under the supplied storage
directory and benchmarks the local single-node deployment shape: SQLite metadata plus
filesystem object storage.

To benchmark the configured runtime adapters instead, use:

```bash
shardline bench \
  --deployment-target configured \
  --storage-dir /tmp/shardline-bench
```

Configured mode loads the active `SHARDLINE_*` runtime config, then namespaces every
benchmark run so file IDs, repository scopes, and S3 object prefixes do not collide with
non-benchmark data. That makes the same deterministic scenario suite usable for local
SQLite plus filesystem deployments and production-style Postgres plus S3 deployments.

Both targets measure the same deterministic suite:

1. initial upload of a generated asset
2. sparse update of the same asset with only one region changed
3. download of the latest version
4. download of the previous version by content hash
5. ranged reconstruction planning for the changed window
6. concurrent latest-version downloads of the updated asset
7. concurrent uploads of distinct file IDs that reuse most chunks while forcing one
   worker-specific chunk mutation per upload
8. cross-repository upload reuse across repository scopes
9. cached latest reconstruction after one cold cache fill

Repeated runs allocate fresh `run-XXXX/iteration-YYYY/` directories (zero-padded
counters, for example `run-0001/iteration-0000/`) under the requested storage
root, so operators can compare runs without clearing the parent directory
between invocations.

Supported knobs:

```bash
shardline bench \
  --storage-dir /tmp/shardline-bench \
  --deployment-target isolated-local \
  --iterations 10 \
  --concurrency 8 \
  --chunk-size-bytes 65536 \
  --base-bytes 1048576 \
  --mutated-bytes 4096
```

JSON output is available for machine processing:

```bash
shardline bench --storage-dir /tmp/shardline-bench --json
```

For a checked, repeatable 1/32/128-client matrix with host/toolchain metadata, full
JSON reports, peak-RSS output, and a generated Markdown summary, run:

```bash
scripts/benchmark-matrix.sh /tmp/shardline-benchmark-results
# or: cargo make shardline-bench-matrix -- /tmp/shardline-benchmark-results
```

The script refuses to overwrite an existing run and records results beneath a
timestamped, commit-addressed directory. Defaults are deliberately small enough for
regular regression work; use `SHARDLINE_BENCH_MATRIX_BASE_BYTES`,
`SHARDLINE_BENCH_MATRIX_ITERATIONS`, and `SHARDLINE_BENCH_MATRIX_CONCURRENCY` for a
capacity run. Set `SHARDLINE_BENCH_MATRIX_TARGET=configured` to exercise the active
Postgres/S3/Redis configuration rather than isolated local adapters.

The monthly and manually dispatchable `Benchmarks` workflow publishes the generated
table as its job summary and retains the metadata, raw JSON, and peak-RSS reports as
one artifact. A dirty working tree is recorded explicitly in local-run metadata so a
result cannot silently masquerade as evidence for the named commit.

Checked-in reference measurements and their interpretation live in
[`docs/benchmarks/`](benchmarks/README.md). They are snapshots, not pass/fail
thresholds; use repeated measurements on the same runner for regression decisions.

The current report includes:

- deployment target
- resolved metadata backend
- resolved object-storage backend
- inventory scope for backend counters
- initial upload latency
- sparse-update upload latency
- latest download latency
- previous-version download latency
- ranged reconstruction-planning latency
- concurrent latest-download latency
- concurrent upload latency
- cross-repository upload latency
- cached reconstruction cold-fill latency
- cached reconstruction hot-hit latency
- bytes-per-second throughput for single-request and concurrent transfer paths
- concurrent scaling efficiency in per-mille
- uploaded bytes
- downloaded bytes
- newly stored bytes
- concurrent uploaded bytes
- concurrent downloaded bytes
- concurrent newly stored bytes
- cached reconstruction response bytes
- cache-hit iteration counts
- inserted chunk counts
- reused chunk counts
- concurrent upload inserted chunk counts
- concurrent upload reused chunk counts
- final chunk-object inventory per iteration

`inventory_scope` is reported explicitly because the final inventory counters are fully
isolated only when both metadata and object storage are local to the benchmark run.
The report's `newly_stored_bytes` counters are uncompressed chunk bytes attributed to
new insertions; they are useful for comparing logical deduplication, but do not measure
encoded Xorb/chunk bytes, temporary objects, filesystem allocation, or the full S3
storage delta. Use backend object inventories and request counters for storage-cost
comparisons.
On configured Postgres and S3 deployments those counters can reflect shared backend
state, while the latency and throughput timings remain benchmark-local.

## Required Metrics

Every benchmark run should record:

- wall clock duration
- user and kernel CPU
- peak memory
- bytes uploaded
- bytes downloaded
- bytes newly stored
- number of reused chunks
- number of new chunks
- database query counts where relevant
- object-store operation counts where relevant

## `perf` Policy

`perf` should be used as a standard optimization tool, not a last resort.

Recommended workflow:

1. reproduce the benchmark scenario
2. record with `perf record`
3. inspect with `perf report` or flamegraph output
4. fix the dominant hot path
5. rerun the same benchmark
6. keep the change only if the measured result improves

The goal is to remove guesswork from optimization.

Current local workflow:

```bash
perf record --call-graph dwarf -- \
  shardline bench \
  --storage-dir /tmp/shardline-bench \
  --iterations 10 \
  --concurrency 8 \
  --chunk-size-bytes 65536 \
  --base-bytes 1048576 \
  --mutated-bytes 4096

perf report
```

Because the benchmark generates deterministic asset bytes and uses isolated iteration
roots, repeated runs are comparable as long as the same command line is reused.

For a task-based workflow, use the commands documented in [Profiling](PROFILING.md).

## Performance Budgets

Shardline should maintain explicit budgets for:

- upload validation overhead
- reconstruction planning latency
- range-read overhead versus direct object-store reads
- provider authorization latency
- end-to-end push and fetch regression thresholds

Budgets force the project to reject features that quietly make the common path slower.

## Design Consequences

Performance requirements imply several design rules:

- chunk and reconstruction metadata must be cheap to look up
- uploads must avoid buffering large untrusted bodies unnecessarily
- immutable object keys must make cache behavior predictable
- repeated reconstruction planning should hit a dedicated cache before durable metadata
- large transfers must not be allowed to starve smaller concurrent reads
- file downloads should stream reconstruction chunks instead of building a full
  contiguous response buffer
- provider integration must not sit in the data hot path longer than needed
- protocol compatibility fixes must be measured, not assumed

Shardline enforces a per-upload chunk processing window with
`SHARDLINE_UPLOAD_MAX_IN_FLIGHT_CHUNKS`. Complete chunks are processed in parallel
whether they arrive as aligned request frames or are assembled from smaller frames.
The window is measured in chunks, not bytes, so memory pressure is a function of chunk
size, request-frame size, and concurrent upload count.
When the environment variable is unset, Shardline keeps a conservative host-scaled
default: it stays at `64` on common hosts such as 16-core and 32-core machines and only
scales higher on larger systems.

Shardline also enforces a transfer concurrency budget with
`SHARDLINE_TRANSFER_MAX_IN_FLIGHT_CHUNKS`. The budget is weighted by chunk-equivalent
response frame size.
Large downloads do not hold the entire shared transfer lane for the lifetime of the
response; each bounded response frame acquires capacity while it is outstanding and
releases capacity before the next frame is read.
When unset, the transfer budget scales with host parallelism so larger deployments do
not inherit the same fixed ceiling as a small single-node install.

Shardline enforces `SHARDLINE_MAX_REQUEST_BODY_BYTES` as a maximum accepted request body
size. Native xorb and shard uploads do not write request bodies to an `incoming`
directory or shard parsing workspace.
The server reads bounded request frames into memory for Xet normalization and
validation, then writes immutable objects through the configured storage adapter.
With the S3 adapter, upload-body persistence goes directly through S3; local filesystem
writes happen only when the local object adapter is the selected storage backend.
Control-plane JSON and webhook bodies still use bounded extractor buffering because they
are small metadata requests.
Native xorb and shard ingest now also pre-size those bounded in-memory buffers from the
request size hint when the client sends one, which removes avoidable growth churn
without changing the fail-closed memory ceiling.

Local bounded protocol uploads up to 256 KiB are now staged in request memory instead
of a disposable temporary file. Larger or unknown-length bodies spill to the existing
temporary-file path once the per-request memory cap is reached. The accepted payload is
still hashed before publication and the final object still uses the normal verified,
atomic storage write. This removes scratch-file bytes for small local Xorb, shard, and
OCI uploads; total device-write and NVMe-lifetime savings have not been measured.

Hub LFS uploads use the same 256 KiB per-request threshold when one of 64 process-wide
memory slots is available (at most 16 MiB of upload bodies retained at once). Larger
uploads and requests without a slot use the existing temporary-file flow. Both paths
check the requested LFS SHA-256 and the Shardline BLAKE3 digest and length before
publication. For S3, the small in-memory path uses a conditional create directly, so it
avoids writing a local scratch copy and avoids the temporary S3 object plus copy step
used by the file-backed path. For local object storage on Unix-like hosts, file-backed
Hub LFS uploads try to stage in the object store root's parent only when device IDs
show that it shares the store's filesystem. The temporary stays outside the object
root, so listing and GC cannot see an incomplete upload; publication still uses the
verified, durable atomic hard-link path. This avoids the extra full-payload copy that
the store needs when a system-temp file is on a different filesystem. If device IDs
differ, metadata cannot be read, or sibling-temp creation fails, Shardline keeps the
existing system-temp fallback. When both paths already share a filesystem, this changes
no payload-write count. These are path-level operation counts, not measured cloud-cost
or NVMe-lifetime results; repeated-object conflict behavior and provider pricing still
need a target-backend benchmark.

On Unix-like local object storage, an LFS retry for an already-present object now
streams the request against a pinned open file instead of staging another temporary
copy. Shardline still checks the request's SHA-256, compares every byte, and confirms
that the pathname still names the same file before syncing it and acknowledging the
retry. A mismatching or concurrently replaced object is rejected. For a matching
N-byte retry, this avoids N bytes of scratch-file writes; it still reads the N request
bytes and N stored bytes. S3, non-Unix, and first-time uploads keep their existing
paths. The regression tests verify no tempfile is created for the duplicate and that
corrupt or replaced stored content is not accepted. These application-level counts do
not measure device-level write amplification or NVMe lifetime.

For local filesystem object storage, file uploads retain at most one 32 MiB raw CDC
batch in memory, pack it into an Xorb, and release its memory reservation before moving
to the next batch. This lets a long upload avoid spooling its entire raw body when the
process-wide memory budget remains available. The semaphore reserves four working-set
MiB per MiB of retained raw data, up to a 128 MiB process-wide budget; that admits at
most 32 MiB of aggregate raw chunks. If another upload holds the budget, Shardline
spills the current and subsequent raw chunks to the existing temporary spool. It never
waits for memory admission.

Each durable Xorb batch remains unreferenced until the final file metadata is published.
If the upload later fails or the process exits, those objects are harmless orphans for
normal reachability cleanup; no partial file record is published. If packing or Xorb
storage fails, the current batch's original chunks are stored instead. The format and
final publication boundary are unchanged.

Protocol uploads expose whole files and can omit standalone chunk objects after
successful packing. The public `upload_file` API also advertises individual chunk
hashes for `read_chunk` and `chunk_length`; it therefore retains those verified
objects before publishing the file. It still packs the same Xorbs and preserves
the file's content identity. Packing-only savings must not be applied to this
public chunk-access path. A regression test compares the packed records of both
paths, reads every advertised public chunk, and confirms protocol uploads create
no standalone chunks. An existing corrupt advertised chunk prevents publication
even if an intact packed Xorb already exists.

A 40 MiB local regression test confirms a full batch is stored before EOF, no raw spool
is created, and every packed chunk round-trips. For a successful upload admitted to the
memory path, an N-byte body avoids N bytes of temporary-spool writes and N bytes of
packer reads. A spill incurs that write/read cost only for the spooled portion; if its
Xorb fails, fallback reads those chunks again. These are application-level byte counts:
`/tmp` may be memory-backed or disk-backed, and filesystem metadata, journaling, and
device write amplification are not included. Physical NVMe savings and lifetime impact
have not been measured.

For S3-backed file uploads, Shardline also retains the raw CDC chunks in the existing
bounded Xorb batch and attempts to store that Xorb before writing individual chunks.
Successful packing therefore removes the per-chunk S3 PUT attempts. For chunks that
did not already exist, it also avoids temporary objects and their later GC deletes. A
conditional write of an already-present Xorb still downloads and hashes that complete
Xorb before reuse, preserving the existing-object integrity check. If packing or Xorb
storage fails, Shardline writes the individual chunks and reads each back for
verification before the fallback file record can be published. The record format does
not change.

The raw S3 batch is bounded to about 32 MiB per active upload, but this is not a
process-wide memory budget; concurrent uploads multiply the retained memory. Uploads
that have not published a file record are not acknowledged as durable files. A crash
can discard their in-memory batch, while already-stored Xorbs from earlier batches may
become unreferenced and later collected. No filesystem scratch write is used by this
S3 batch path itself; other protocol staging, such as multipart-session parts, retains
its own storage and recovery behavior.

File-backed create-if-absent writes now stream directly into an unpublished multipart
upload at the canonical key. A dedicated signed completion client sends
`If-None-Match: *`; the object becomes visible only when that condition succeeds.
This removes the temporary object, its promotion-copy sequence, and its cleanup
DELETE. A deterministic one-part test observes four requests: an existence/identity
GET, multipart creation, one part PUT, and conditional completion. Existing objects
still undergo a full byte comparison; no digest, conflict, or publication check is
skipped. The adapter checks the streamed length again before completion. Source
files remain caller-validated and must remain immutable during the operation.

The upload retains at most four 8 MiB part payloads, plus transport and bookkeeping
overhead. This is a per-upload bound, not a process RSS measurement. The local file
is read, not copied or rewritten, and no local scratch payload is added. Remote
payload upload bytes are unchanged; the saving is the removed server-side promotion
and temporary-object lifecycle. A failed operation aborts its unpublished multipart
upload on a best-effort basis. As before, a bucket lifecycle rule must reclaim
incomplete multipart uploads left by a process crash or unsuccessful abort. Existing
stored objects and object keys remain readable without migration.

The streaming large-copy fallback uses the same conditional completion client.
The condition applies only to completion,
not to upload creation or individual parts. A competing destination is preserved
and compared byte-for-byte before an identical copy is accepted as successful.
This replaces the final existence-probe HEAD with an atomic provider check: one fewer
HEAD per successful fallback, with the same bounded payload transfer and no new
local payload writes. A completion failure aborts the upload on a best-effort basis;
it never retries publication without the condition.

Deterministic tests reproduce conflicting and identical concurrent writers and
completion failures. A live MinIO test checks signing, successful completion, and
rejection when a competing writer wins. These use small multipart payloads to test
the publication protocol; they do not establish performance for objects above 5 GiB
or compatibility with every cloud provider. Direct file-upload tests additionally
cover lost completion replies, rejected part uploads, source-length changes, missing
source files, and conflicting/identical concurrent publication. Live MinIO checks
cover empty files, exact 8 MiB part boundaries, and a file larger than four parts,
including byte-for-byte readback and identical/conflicting replay. These checks do
not establish production throughput, cloud billing savings, or NVMe lifespan gains.

The regression test crosses the 8,192-chunk batch boundary, observes multiple Xorb
PUTs and zero GETs, and verifies the result references multiple containers. Its
forced-Xorb-failure case verifies individual chunk PUTs and read-backs. These counts do
not establish production latency, RSS, S3/R2 request cost, or durable-byte savings.
Those depend on file size, chunk reuse, existing Xorb reuse, and backend pricing;
measure MinIO or the target cloud with the same corpus before estimating cost.

For local deployments with spare RAM, `TMPDIR` can point at a capacity-limited tmpfs
to keep spill writes off the NVMe. The spool only holds an in-progress upload: it is
not acknowledged or used as the published object, and Shardline checks the whole-file
digest before durable publication. Size the tmpfs for concurrent spills and constrain
it with the host or container memory limit; if it fills, the upload fails before its
file record is published. This trades temporary disk writes for RAM use and does not
change the durable object format.

S3 multipart parts have a different durability role from those disposable upload
spools. In the single-node filesystem-backed session path, each accepted part is
written once, synced, and atomically linked to its immutable part name before the
session metadata points to it. The hard link does not copy the payload again. Removing
that write would make an acknowledged part disappear on process restart; moving it to
tmpfs would weaken the same recovery guarantee. In Postgres+S3 deployments, part bytes
are streamed into private remote staging objects and the Postgres part map publishes
the reference, so this path does not write the part payload to local disk. Moving
single-node session bytes to S3 could save local device writes, but adds remote
staging, request, and cleanup costs; no net cost reduction has been demonstrated.

Multipart completion has two remaining read/copy costs. Legacy local sessions
without a stored part ETag first read each selected part to reconstruct its MD5,
then verify it again while ingesting. Current sessions already store that ETag
and use the streaming verification pass. Removing legacy verification would
weaken compatibility safeguards; the legacy completion and corrupted-part tests
check rejection before a replacement object is published.

Durable multipart completion streams S3 staging objects directly. Filesystem-backed
durable completion now retains open part files instead of copying each payload into
a temporary directory. It checks the opened file's size against the fenced part
descriptor and retains per-part hash/MD5 verification during ingestion. Session
ownership and publication guards remain held by the caller. This removes one
scratch payload copy per local durable part; it does not remove the original
acknowledged part write or its durability requirements. Physical device-write,
latency, and wear reductions have not yet been measured.

Durable OCI completion uses the same sequential part reader, so local OCI parts
also bypass the previous per-part scratch copies and assembled payload file.
Its final SHA-256-addressed publication still checks the client's expected digest.
LFS range assembly now also opens local staged parts directly instead of creating
an intermediate copy of each part. It retains the sparse assembled file needed
for generation-ordered overlapping ranges. Both the opened source length and
the copied byte count must match the part descriptor, and the complete assembled
object still passes the expected SHA-256 check before publication. This removes
one scratch write and reread per local staged part, including overlapping parts;
it does not remove assembly writes or the final validation pass. Physical device
I/O savings depend on filesystem caching and have not been measured.

The cross-node LFS overlap and failed-digest repair tests passed against real
PostgreSQL with local object storage; their durable sessions reached `completed`.
A syscall trace of the overlap test showed one assembled file and no intermediate
`part-*` files. The LFS route suite also passed all 83 tests; database-dependent
cases in that suite require the separate PostgreSQL run to establish execution.

A syscall trace of `durable_local_parts_stream_open_files_without_scratch_copy`
confirmed that the direct reader created no scratch payload file and issued no
`copy_file_range` or `sendfile` call. Its only payload write opens were the test's
three fixture writes (valid content, same-length corruption, and a short file).
The test also reads the retained inode after unlink and rejects both corrupted
content and the wrong length. This small `/tmp` fixture verifies the removed copy
and error handling; it does not quantify physical NVMe writes or service throughput.

SQLite keeps WAL mode with `synchronous=FULL`; do not trade away acknowledged-commit
durability to reduce writes. Connection setup already avoids repeating the WAL-mode
transition when it is active. Empty Hub file-entry batches return after the existing
recovery checks, avoiding a connection and transaction in both SQLite and PostgreSQL.
They preserve previously stored entries. This saves transaction overhead, not payload
writes or a measured amount of device wear; empty batches already wrote no rows.
The PostgreSQL integration test checks that identical replay followed by an empty
batch preserves the stored file entries and their tuple version (`xmin`).
PostgreSQL record replays also intentionally refresh their
row timestamps: SQLite trigger tests and PostgreSQL row-version tests require that
behavior, so skipping identical upsert rows is not a safe write reduction. PostgreSQL
file-record upserts now keep the existing JSONB payload when it compares equal while
still updating the row timestamp and running the normal update path.

Hub tree pagination still counts up to 100,001 entries in the complete revision on
each page request, in both SQLite and PostgreSQL. This enforces the existing
100,000-entry read ceiling even when the requested directory or page is small.
The count, cursor check, and page read share a database snapshot. Removing the count
or caching it outside that snapshot could change ceiling enforcement after concurrent
tree publication. A stored count could reduce repeated scans, but would need atomic
maintenance across every writer and compatibility with older writers. The current
patch keeps this check. These are bounded row visits, not a measurement of physical
disk reads: database caching determines the actual device I/O. No cost or wear saving
is claimed for this remaining opportunity.

SQLite OCI tag and S3 object listings now use the shared bounded evidence loader instead of a
separate query that correlates the latest sequence against historical outer rows.
The shared query seeks the latest composite key for each requested operation.
OCI listings retain their existing 900-tag verification batches; S3 evidence IDs
are also queried in batches of at most 900. Each listing retains its common transaction
snapshot. The loader checks persisted sequence and operation identity, current and
previous Merkle commitments, and missing historical commitments; the caller still
checks each typed event against the requested tag or object snapshot. No evidence is cached
or omitted. PostgreSQL already uses a bounded lateral latest-event lookup here.

A reduced in-memory SQL-shape probe linked against the local bundled SQLite 3.51.3
library requested one existing operation and one absent operation, with the normal
composite primary key and missing-Merkle partial index. At one historical event,
the historical-outer and requested-key queries executed 83 and 91 SQLite VM steps.
At 2,000 events they executed 46,065 and 96 steps. Both returned the same latest
sequence. This measures SQL selection work, excluding JSON/Merkle verification;
it is not a disk-byte, production latency, or monetary saving measurement.
A PostgreSQL `EXPLAIN (ANALYZE, BUFFERS)` probe over a disposable 2,000-event
table confirmed that its existing lateral query uses a backward primary-key
lookup with `LIMIT 1` and the missing-Merkle partial index. No PostgreSQL schema
or query change was needed for this finding.

S3 metadata compare-and-swap replays have a narrower optimization: when the expected
and replacement entries are exactly equal, including their timestamp, the adapter
can confirm the current row without rewriting it. SQLite retains its IMMEDIATE
writer transaction; PostgreSQL retains the advisory lock and locks the matching
row with `FOR UPDATE` until commit. Both still verify and append the normal
reliability transition. A stale expectation returns false, corrupted evidence is
rejected, and changing the timestamp still performs the ordinary UPDATE. Tests
observe no SQLite row-update trigger and no new PostgreSQL `xmin` for the identical
replay. This reduces statement/tuple-update work; evidence writes and PostgreSQL
row-lock WAL remain. It is not a measurement of SQLite WAL savings or NVMe wear.

S3 temporary-upload cleanup now lists only `__tmp/` below the configured tenant
prefix. Previously it listed every object in that tenant and filtered afterward.
Listing pagination is unchanged, but unrelated user objects no longer consume
cleanup list pages or response bytes. The outgoing-request regression checks both
unprefixed buckets and a tenant prefix. The MinIO cleanup tests retain coverage
for stale generated artifacts, fresh artifacts, collision-shaped user objects,
and malformed names inside the reserved subtree. Deletion still requires the
exact generated-key grammar and an old enough backend-observed modification time.
This changes neither user-object reads nor cleanup's age and safety rules. The
request saving depends on the number of unrelated objects; it is not a measured
percentage reduction in total S3 costs.

GC clock-anchor loads use bounded whole-object GETs instead of a HEAD followed
by a GET for each clock record. With both the scalar and boot-observation sidecar
present, this reduces the load from two HEADs plus two GETs to two GETs. A missing
scalar takes one GET and does not load the sidecar. The existing 64 MiB read limit,
complete-response length checks, legacy scalar format, and scalar/sidecar matching
rules remain unchanged. A failed required scalar read still fails the operation;
an unreadable optional sidecar cannot establish trusted elapsed time. Fake-S3
request tests cover the request shape, missing records, oversized responses, and
truncated scalar and sidecar bodies. This saves up to two requests per anchor
load, not half of all GC requests, and does not reduce payload I/O or disk writes.

Saved-delete recovery likewise reads its cursor and receipt objects with one
bounded GET each instead of HEAD plus ranged GET. A pass reading a present cursor
and 64 eligible receipts saves 65 HEAD requests; listing, receipt verification,
database settlement, and cleanup requests remain. Missing, empty, or oversized
records cannot authorize settlement, and truncated responses fail the read.
The cursor remains limited to 4 KiB and receipts to 16 KiB. Namespace, receipt-key
digest, signature, and exact-attempt database checks remain the authority for
settlement; neither a listing nor an absent reply proves a delete completed.
These are small-record API savings, not reduced user-data reads or disk writes.

File-backed S3 uploads also avoid a duplicate local file-length stat: their existing
destination-comparison helper already validates the source length, and multipart
publication checks the actual streamed length again. This removes one metadata
call per invocation without removing either validation boundary. It does not reduce
payload reads or establish a measurable device-write saving.

Local object operations now validate shared directory components once per parent
walk instead of walking the root and then walking that same prefix again. Every
existing component is still checked for symlinks and non-directory paths on each
operation; no path-validation result is cached. Temporary-file uploads also rely
on the installation helper's immediate parent validation instead of repeating it
at the caller. Anchored publication, full content checks, and durability syncs are
unchanged.

Local byte writes now use non-following regular-file metadata for the initial
existence observation instead of opening and immediately discarding a file
handle. The reuse path still opens the actual object through its pinned parent,
compares every byte, and syncs the verified file and directory before returning.
Temporary-file integrity verification also borrows its already-open handle
instead of duplicating the descriptor; the complete length and BLAKE3 checks
remain in place.

A Linux `strace` of the same round-trip fixture before and after these changes
observed 61 versus 59 `openat` calls, 189 versus 190 metadata calls, and 36 versus
35 `fcntl` calls. Both runs retained all 19 sync calls. A separate file-integrity
fixture retained its 21 opens and 28 metadata calls while reducing `fcntl` calls
from five to three. These whole-test counts include test setup and teardown;
they establish modest descriptor/syscall savings, not fewer payload reads,
physical NVMe writes, or a measured throughput or SSD lifetime improvement.

On this Linux host, tracing the existing
`local_object_store_roundtrips_metadata_ranges_inventory_and_delete` test with
`strace -f -e trace=newfstatat,statx` observed 193 metadata syscalls before the shared
walk change and 175 afterward. Separately, removing the temporary-upload caller's
duplicate check reduced the existing
`local_object_store_temporary_file_duplicate_is_idempotent` fixture from 54 to 48
metadata calls, with 23 `fsync`/`fdatasync` calls in both runs. Counts cover each test
process, including setup and teardown, rather than one production request. These
are syscall reductions in small fixtures, not measurements of physical disk I/O,
throughput, latency, or NVMe lifetime. Savings depend on path depth and workload.

Local OCI/LFS staging of bytes already held in memory now uses the existing local
byte writer directly. Previously it wrote a complete temporary source file before
checking for an existing staging object; crossing filesystems could also require
copying that source into the durable store. The direct path avoids that source
spool, including on retries. New objects still use the store's anchored atomic
publication and file/directory syncs; retries still compare complete stored bytes
and reject corruption. Metadata publication still follows successful durable
staging, so an interrupted operation cannot acknowledge memory-only data.

The local asynchronous writer owns a copy of the supplied part while its blocking
filesystem worker runs. This trades an additional part-sized memory allocation
for removal of the source spool; it does not establish lower RSS. These are
code-path savings, not measured physical NVMe writes or a drive-lifetime estimate.
The stored keys and bytes do not change, and the S3 staging path is unchanged.

On this Linux host, `strace -f -yy -e trace=write,pwrite64,fsync,fdatasync` of
`local_byte_staging_replay_verifies_existing_content` observed one 19-byte payload
write into the store's publication temporary file for the initial insertion, with
file and directory syncs. The identical replay performed no payload write and
synced the verified existing file. The test's separate deliberate corruption write
was rejected by the subsequent retry. This small tmpfs fixture verifies syscall
behavior, not production disk traffic or flash write amplification.

An additional exploratory checkpoint probe linked the existing local build's
SQLite 3.51.3 static library. It opened and closed a connection for each of 50
transactions, each inserting a 4,096-byte blob, with WAL and `synchronous=FULL`.
An ordinary connection then reopened the database, verified all 50 rows and
204,800 payload bytes, and required `PRAGMA integrity_check` to return `ok`.
Tracing `pwrite64`, `fsync`, and `fdatasync` included that final connection close:

| Probe configuration | Write calls | Write bytes | Sync calls | Read calls | Read bytes |
| --- | ---: | ---: | ---: | ---: | ---: |
| Default checkpoint on last connection close | 936 | 1,304,780 | 255 | 462 | 1,669,560 |
| `SQLITE_DBCONFIG_NO_CKPT_ON_CLOSE` on transaction connections | 789 | 901,804 | 109 | 4,463 | 17,952,832 |
| One attached idle connection; independent operation connections | 397 | 901,412 | 109 | 318 | 1,075,740 |
| One reused operation connection | 397 | 901,412 | 59 | 127 | 493,204 |

The candidate reduced traced write bytes by 30.9% in this tmpfs fixture, but a
second trace including `pread64` found about 10.8 times as many read bytes across
repeated connection opens. It was therefore rejected as a general efficiency
change. Shardline retains its existing checkpoint-on-close policy and full commit
durability. These are library-level syscall counts, not Shardline throughput or
physical-device measurements; page cache can absorb some of the extra reads.
The attached-idle-connection probe first read the schema while the first operation
connection was still attached; merely opening an unused handle did not achieve
the saving. It reduced traced write bytes by 30.9% and read bytes by 35.6% relative
to the original fixture. Reusing one operation connection reduced reads further,
but would require separate concurrency qualification.

Local server backends now retain one initialized idle SQLite connection, shared
across backend clones. Operations continue to open independent connections and
use their existing transactions. The idle connection holds no read transaction;
automatic checkpoints and `synchronous=FULL` remain unchanged. SQLite still
performs its normal cleanup when the final connection closes. Direct users of
`LocalIndexStore::new` retain the existing lifetime behavior; the server uses
`new_with_persistent_connection`. The additional idle connection consumes file
descriptors and SQLite memory; no production RSS reduction is claimed. This
requires no user configuration, new service, schema migration, or data rewrite.

A second trace exercised Shardline's actual `LocalIndexStore` API rather than a
reduced SQL fixture: initialize a fresh store, upsert 50 S3 object entries with
their reliability evidence, read and compare all entries, close the store, then
reopen it with the original constructor and compare all entries again. Both
configurations used the same source and bundled SQLite. The trace includes
initialization, evidence validation, shutdown, and reopening:

| Connection lifetime | Read calls | Read bytes | Write calls | Write bytes | Sync calls |
| --- | ---: | ---: | ---: | ---: | ---: |
| Original constructor | 1,631 | 6,445,796 | 2,274 | 4,679,460 | 299 |
| Initialized idle connection | 1,269 | 4,963,044 | 1,505 | 3,403,596 | 149 |

This fixture reduced traced read bytes by 23.00%, write bytes by 27.27%, and sync
calls by 50.17%. It ran on tmpfs: these are process-level syscall measurements,
not physical NVMe writes, HTTP throughput, or an SSD lifetime forecast. Tests also
verify that the idle connection does not pin a read snapshot, that a replacement
database survives removal or renaming of the original root, and that process
termination preserves committed data while discarding uncommitted data.

[SQLite documents](https://sqlite.org/c3ref/c_dbconfig_defensive.html#sqlitedbconfignockptonclose)
that the candidate suppresses the final connection's checkpoint; it does not
replace commit durability. A bundled-library SIGKILL probe retained all 50
committed rows and discarded one uncommitted row; read-only recovery, native
SQLite backup, and an ordinary connection without the setting all read the
committed data and passed integrity checks. A pinned reader allowed 4,693 WAL
frames to accumulate with only five checkpointed; releasing it allowed all 4,693
frames to checkpoint. Long readers can therefore still delay checkpoints and
increase WAL space. These library probes do not certify a released Shardline
binary rollback or hardware power-loss recovery.

Committed data can remain in the WAL after crashes or while other connections
keep it open. Use
the native SQLite backup procedure in [Disaster Recovery](DISASTER_RECOVERY.md);
copying only the main database file cannot serve as a backup. Do not delete WAL
companions to reclaim space. No database format or schema migration is required.

A separate SQLite 3.53.4 WAL probe used a roughly 320 KB text payload and 50 replay
updates with a changing timestamp. The WAL file was 997,072 bytes both when the
unchanged payload was reassigned and when a `CASE` expression retained the existing
value. This indicates that guarding only the payload does not reduce SQLite's page
writes when the row timestamp still changes. Shardline now skips the complete
reconstruction or dedupe-mapping update only when its stored value is identical;
changed values still refresh their timestamp. Trigger-backed tests confirm identical
replays do not execute an UPDATE and changed mappings still update. The WAL reduction
from skipping these full no-op updates has not been measured separately.

A disposable PostgreSQL 16 probe replayed the same roughly 320 KiB JSONB payload 50
times with a changing timestamp. Reassigning the payload generated about 17.6 MB of
WAL and left a 17.0 MB table-plus-TOAST footprint; retaining the equal payload
generated about 9 KB of WAL and left a 0.4 MB footprint. This isolates repeated
payload rewrites and is not a production forecast: it does not include network
serialization, compare cost on varied real records, concurrent load, or vacuum policy.
The Shardline Postgres record-replay test also passes with the guarded upsert, including
the required fresh row version on replay.

I also checked the PostgreSQL S3-object index, where each overwrite refreshes the row
timestamp and appends integrity evidence. In a disposable PostgreSQL 16 schema, 50
timestamp-changing updates to a 1.9 KiB serialized metadata value produced the same
31.4 KB of WAL and the same table-plus-TOAST size with either direct assignment or a
`CASE` that retained the old value. For an 8 KiB value, retaining the old TOAST value
reduced WAL from 461 KB to 9.3 KB and the table-plus-TOAST footprint from 480 KiB to
80 KiB. This is a synthetic bound: Amazon S3 limits user-defined metadata in a PUT
request to 2 KiB of key/value bytes ([AWS documentation](https://docs.aws.amazon.com/AmazonS3/latest/userguide/UsingMetadata.html)),
and Shardline's internal JSON encoding adds bytes beyond that input. The near-limit
probe showed no savings, while the larger case is outside that AWS limit. No query
change is justified for the usual S3 metadata size; larger values on other compatible
stores would need workload evidence before adding another comparison to every update.

For local Xorb-backed reads, validation and range streaming now use the same open
object file instead of copying the complete container to a temporary file first. This
removes one scratch-file write equal to the container size per read while retaining
full-container validation before response bytes are exposed. The read still scans the
container for validation, and device-level write counters have not yet been collected;
the change therefore establishes avoided scratch bytes, not a measured NVMe lifetime
or cost saving.

A paired Linux `strace` run with the same 2,097,169-byte local payload observed
the former materializer create a scratch file and copy 2,097,169 bytes with
`copy_file_range`. The direct reader created no scratch file and issued no copy
call. Both fixtures retained their six object-creation durability syncs and
returned byte-identical content. The direct-reader test additionally checks the
original device/inode identity and reads successfully after unlinking its path,
confirming that the reader retains its open file rather than reopening a name.
The fixtures used `/tmp` on tmpfs: these counts establish an avoided logical
scratch copy, not physical NVMe writes, throughput gains, or an SSD lifespan
estimate. Filesystem copy offload and page caching can also change the physical
cost of the previous copy on other deployments.

Referenced-term reconstruction and lifecycle-repair member scans now use that same
open-file approach for local Xorbs, removing their remaining container-sized scratch
copies. They use a zero-byte memory budget for remote materialization: S3 containers
still stream to a temporary file, but their length and body arrive in one GET instead
of HEAD plus GET. Full-container validation still precedes selected-term output or
reachability interpretation. The selected-term test checks byte-identical output and
rejects a corrupt container without appending output; lifecycle-repair tests retain
single- and multi-chunk reachability coverage. This removes local scratch writes and
one remote request per successful materialization, not full-container validation reads
or S3 payload bytes. No physical-device wear reduction has been measured.

Fsck's native-Xet term checks and retained-shard mapping checks, plus index
rebuild's retained-shard scan, use the same zero-memory-budget reader. Local
containers are inspected through their existing file handles rather than copied
to scratch first. S3 remains streamed to disk; fsck combines existence and
materialization into one GET. Rebuild retains its listing-observed length check
against the downloaded object. Missing-object reporting, full Xorb validation,
shard limits, and invalid-shard repair safeguards remain. The avoided local
scratch write equals the serialized container length for each inspection; full
validation reads remain, and device-level wear savings have not been measured.

One remaining fsck cost is repeated retained-shard inspection: each dedupe mapping
loads and validates its referenced shard separately, even when several mappings
point to the same object. The mapping visitors currently order by chunk hash,
not shard key, so shared shards are not necessarily adjacent. No parsed-result
cache is added here. Such an optimization needs a bounded memory policy and a
defined freshness or snapshot rule before it can replace source validation.
The current change removes scratch copies, not these repeated validation reads;
their production contribution has not yet been measured.

The Xet adapter's member scan and referenced-Xorb range-info loader also use
this reader. Local scans avoid their scratch copy and remote scans use one full
GET. Serialized-Xorb validation and shard member-reference collection still run
before their results are used. The zero-byte memory budget keeps large remote
containers on the streamed-to-disk path.

Persisted `_xorb_chunks/` member-list sidecars are no longer read or generated.
They contained unauthenticated newline lists: a truncated or incomplete list could
omit live chunk references. A checksum of the sidecar alone cannot prove complete
membership against the Xorb, and recomputing Xet's aggregate hash alone does not
authenticate the leaf structure. Member scans therefore validate the actual
container. Existing sidecars remain readable as ordinary stored objects and their
legacy key recognition is retained; stored Xorb and shard formats do not change.
The regression test checks empty, incomplete, and malformed sidecars and ensures
a damaged container fails before any member is visited.

This safety correction removes sidecar probes and writes on cold scans, but a
formerly cached scan now downloads the complete Xorb rather than the small list.
It uses one full S3 GET and no in-memory container buffer. Warm-scan backend bytes
can increase substantially; do not count this as a universal S3 cost reduction.
A future persisted-cache optimization needs an independently trusted membership
commitment before it can safely skip validation.

For S3-backed Xorb reads, Shardline now uses one full-object GET to both establish
object existence and materialize the container for validation, rather than issuing a
HEAD followed by a GET. The response length and body come from that same request, and
the complete Xorb is still validated before any requested bytes are returned. This
removes one S3 request for each successful packed read and avoids a HEAD/GET race; it
does not reduce bytes downloaded, and the request-count reduction has not yet been
measured in a production workload. A missing Xorb still falls back to individual
chunks where that legacy representation is available.
Request-count tests confirm that a successful packed read uses one GET. A legacy
two-chunk fallback still needs one Xorb probe plus one chunk GET per chunk; a single
chunk range therefore uses two requests. Some older records labeled
`FixedChunkV1` can still be Xorb-backed, so that label alone cannot safely remove the
probe without making those stored files unreadable.

S3 Xorb reads with a successfully admitted memory slot also keep serialized objects
up to 1 MiB in memory during full integrity validation instead of staging them in a
temporary file. At most 16 such reads per server process can be admitted, bounding
this serialized-object memory to 16 MiB; requests without a slot and larger objects
spill to a temporary file in the same GET. The temporary file is created lazily, so
small admitted reads do not depend on scratch-directory availability. For an
in-memory object of N bytes, this avoids N bytes of scratch-file writes and the
validation/range reads from that scratch file. It does not reduce S3 bytes, and
production scratch-device savings have not been measured. Every path still validates
the complete Xorb before returning requested bytes.

Small whole-object and chunk reads from S3 also use one bounded full-object GET in
place of a HEAD followed by a ranged GET. The 64 MiB in-memory limit remains in
force, and the GET body must match the length reported by that same response before
the bytes are returned. This saves one request per successful non-empty read without
adding temporary disk I/O or changing stored objects. It does not reduce transferred
bytes; missing objects and empty-object reads still require one request. Production
request-cost impact has not yet been measured.

When a file-backed upload targets an object that already exists, Shardline now uses
one streamed GET to obtain its length and compare all of its bytes with the already
validated local file. Previously this took a HEAD plus one ranged GET per 256 KiB
comparison block. For an existing object of N bytes, the request count falls from
`1 + ceil(N / 256 KiB)` to one; the same full object bytes are still read and checked.
The comparison uses a 256 KiB local buffer and does not trust object metadata in place
of byte verification. A missing key still costs one existence request before the
existing conditional upload-and-copy path, so this change does not add a request to
first-time uploads. A request-count test verifies that a multi-block duplicate uses
one GET and no HEAD or ranged GET; a same-length conflicting object is still rejected.
Production request-cost savings have not yet been measured.

The LFS batch endpoint still checks each object before replying. For uploads, it
must omit the `actions` field when the requested OID is already stored; for
downloads, it must return an error for a missing OID and the stored size for an
existing one ([Git LFS batch contract](https://github.com/git-lfs/git-lfs/blob/main/docs/api/batch.md)).
On S3 this is one HEAD request per object. Removing it without another durable,
cross-node source of truth would either make clients re-upload existing objects or
tell them to skip objects that are missing. A metadata index could replace the
HEAD, but its write, recovery, and consistency costs need to be measured against
the saved S3 requests first.

Repeated publication of an unchanged file reconstruction now skips the SQLite row
update and the PostgreSQL JSONB update. This avoids redundant SQLite journal/WAL and
PostgreSQL WAL/TOAST work on exact replays while preserving updates when reconstruction
terms change. The effect on production write volume has not yet been measured.

Exact replays of a dedupe-shard mapping also skip the SQLite/PostgreSQL row update when
the chunk still maps to the same shard key. A changed shard key still replaces the
mapping. The mapping timestamp is not part of the lookup contract, and reliability
evidence handling remains in the surrounding publication transaction. This avoids
materialized-row churn for repeated chunk registrations; production WAL reduction has
not yet been measured.

PostgreSQL now publishes unique dedupe-shard mappings in batches of up to 4,096 rows
inside the same metadata transaction. At the default 65,536 Xorb chunk-record limit,
that reduces this portion of publication from as many as 65,536 database statements
to at most 16. The batch still uses the conditional upsert, so unchanged mappings remain
no-ops, remapped chunks update as before, and PostgreSQL row triggers still run for
each affected mapping. Inputs containing duplicate chunk hashes retain the previous
sequential last-write-wins behavior. This reduces database round trips and statement
dispatch work; production latency and database I/O savings have not yet been measured.

Repository-scoped Xet SHA-256 identity rows now skip identical updates in both
databases. A skipped upsert checks the identity again in the same transaction to
distinguish an exact replay from a conflicting byte length. PostgreSQL retains the
conflicting row lock even when its upsert condition skips the update
([PostgreSQL INSERT contract](https://www.postgresql.org/docs/current/sql-insert.html));
SQLite retains its writer transaction. A changed native file ID still updates when
the byte length agrees, and a size conflict still rolls back the publication.
Exact replays add one indexed lookup instead of rewriting the small identity row.
This reduces row churn. Other metadata and evidence writes remain unchanged.

A PostgreSQL 16.15 SQL microbenchmark compared the original upsert with the new
skipped-update plus lookup, using the identity table's primary and secondary
indexes. Three pairs of 1,000 single-client transactions replayed one 42-byte
identity. Original WAL deltas were 171,880 bytes in each pair; the candidate used
96,352–96,384 bytes, approximately 44% less. Row-lock WAL remains. Average transaction
latencies varied from 1.524–1.906 ms for the original and 1.221–3.037 ms for the
candidate on this shared host, so this does not establish a latency improvement.
These are database-wide WAL deltas from the isolated SQL workload, not Shardline
HTTP throughput, total deployment WAL savings, or measured physical device wear.
Remote-database latency can make the additional lookup more expensive.

Exact replays of unchanged repository tree entries and revision rows also avoid a
materialized-row update in SQLite and PostgreSQL. The upsert still updates the row when
the file identity, size, or refresh timestamp changes. This reduces database row-version
churn on repeated tree publication without changing the stored tree or revision state.
Production WAL reduction has not yet been measured.

OCI tag upserts now likewise skip rewriting an identical digest in the materialized tag
row. Every operation still verifies and appends its lifecycle evidence, and a changed
digest still updates the row. This keeps the integrity history authoritative while
avoiding the duplicate row version; production WAL reduction has not yet been measured.

File-version rows are deliberately rewritten on replay even when their JSON payload is
unchanged. Their update time determines which version wins when Shardline rebuilds a
missing `latest` alias, so skipping the write could make recovery select older content.
Replay also retains row-update hooks, including same-second repetitions for which
the stored timestamp does not change. The regression test
`version_replay_refreshes_row_and_latest_replay_updates` checks both version and
latest-row update behavior. That timestamp and update behavior are part of the
existing contract; reducing this write safely would need a
separate durable ordering field and a migration, so the current patch keeps the existing
behavior.

Repeated upserts of an unchanged S3 listing entry also skip rewriting the materialized
SQLite/PostgreSQL row. The lifecycle evidence event is still appended and verified, so
the saved work is limited to the redundant listing-row version and its payload; event
history writes remain. This production WAL reduction has not yet been measured.

Provider webhook state updates now submit only the observation carried by that event.
Previously, access-change and revision-push handling read the repository state first,
then the SQLite/PostgreSQL upsert read it again under its write lock and merged the
fields atomically. The preliminary read is removed; monotonic state merging and
lifecycle evidence writes remain in the adapter transaction. This removes one database
read per provider state update without changing the stored state or weakening evidence
verification. Production query-latency and I/O savings have not yet been measured.

Shard metadata limits are separate from object byte limits.
They cap per-shard file sections, xorb sections, reconstruction terms, and xorb chunk
records so malformed or hostile shards cannot force unbounded metadata work.
Raising these limits allows larger metadata shards, but it should be paired with
capacity testing because index writes and dedupe registration scale with metadata
fanout.

Ranged xorb downloads and shard downloads stream stored object bytes through the HTTP
response body. The local object adapter serves byte ranges by seeking into the stored
object rather than reading the full object before slicing.
Server-mediated chunk and xorb downloads open local object files only after
authorization and length checks, then copy them into the response body through
fixed-size async read buffers instead of allocating one `Vec` per stored object.

Local reconstruction reads append object bytes into already reserved output buffers
instead of allocating zero-filled buffers and overwriting them.
On the concurrent latest download benchmark with a 128 MiB asset, 1 MiB chunks, 16
workers, and 3 iterations, that changed measured throughput from 5.45 GB/s to 5.90 GB/s
and reduced system CPU time from 9.26 seconds to 7.98 seconds on the validation host.

Transfer response frames are bounded to 1 MiB. This is a per-frame memory bound, not a
file-size limit. Very large objects are served as a sequence of bounded frames until the
requested byte range is complete.
S3-compatible object range reads are dispatched through Tokio's blocking pool so
synchronous adapter work does not occupy async worker threads.

Storage-free ingest benchmarking is available when the goal is to measure server-side
chunking and hashing without local disk or S3 in the timing path:

```bash
shardline bench \
  --mode ingest \
  --iterations 3 \
  --concurrency 256 \
  --chunk-size-bytes 65536 \
  --base-bytes 67108864 \
  --mutated-bytes 1048576 \
  --json
```

This mode measures request ingestion, chunk splitting, hashing, and response accounting.
It does not include object-store write latency, index write latency, network transfer
latency, or client-side Git work.
The blackhole adapter also skips the raw Xorb packing spool: writing a temporary
payload in a storage-free benchmark would add disk I/O and count the same chunks
again during packing. This does not change durable upload paths.

Operational inventory paths use the object-store visitor contract where possible.
Garbage collection, index rebuild, and storage stats should consume object metadata as
an adapter stream or page sequence instead of requiring a full object listing in memory
before useful work can begin.

## Capturing Benchmark Artifacts

Selected reproducible snapshots are published under
[`docs/benchmarks/`](benchmarks/README.md), with exact commits, hosts, toolchains,
configuration, and reproduction commands. They are evidence for regression comparison
on equivalent hardware, not universal product claims. Do not compare any other numbers
without recording the same provenance and raw JSON output.

An isolated local run uses SQLite metadata and filesystem object storage:

```bash
shardline bench \
  --deployment-target isolated-local \
  --storage-dir /tmp/shardline-bench \
  --iterations 10 \
  --concurrency 8 \
  --chunk-size-bytes 65536 \
  --base-bytes 1048576 \
  --mutated-bytes 4096 \
  --json > benchmark-isolated-local.json
```

To measure the active Postgres, S3, or other configured adapters, load the intended
`SHARDLINE_*` configuration and select the configured target explicitly:

```bash
shardline bench \
  --deployment-target configured \
  --storage-dir /tmp/shardline-bench \
  --iterations 10 \
  --concurrency 8 \
  --chunk-size-bytes 65536 \
  --base-bytes 1048576 \
  --mutated-bytes 4096 \
  --json > benchmark-configured.json
```

Each invocation allocates a fresh `run-NNNN` namespace below `--storage-dir`. Repeating
either command therefore creates another isolated run; it does not turn the second
invocation into a warm-cache measurement.

The `cached-latest-reconstruction` scenario measures a cold cache fill followed by a hot
in-memory cache hit within each iteration:

```bash
shardline bench \
  --scenario cached-latest-reconstruction \
  --storage-dir /tmp/shardline-bench \
  --iterations 10 \
  --json > benchmark-reconstruction-cache.json
```

The aggregate JSON contains arithmetic-average latency under `.latency`, throughput
under `.throughput`, process CPU measurements under `.timing`, totals under `.totals`,
and raw samples under `.iterations_detail`. For example:

```bash
jq '.latency.initial_upload_micros' benchmark-isolated-local.json
jq '[.iterations_detail[].latency.initial_upload_micros]' benchmark-isolated-local.json
```

The benchmark command does not calculate percentiles or peak memory.
Derive percentiles from `iterations_detail` with a documented analysis tool.
Measure peak resident memory outside the process, for example with `/usr/bin/time -v`,
and preserve that output beside the JSON artifact.

## Seamless Git Experience

Performance work must be judged at the Git workflow level, not only at the request
level.

If a slower provider check, reconstruction path, or transfer path makes `git add`,
`git commit`, `git push`, `git fetch`, or `git checkout` feel worse than expected, users
will pay more for a less efficient system just to avoid the pain.

That is the standard Shardline has to meet.
