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

Repeated runs allocate fresh `run-XXXX/iteration-YYYY/` directories under the requested
storage root, so operators can compare runs without clearing the parent directory
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

Operational inventory paths use the object-store visitor contract where possible.
Garbage collection, index rebuild, and storage stats should consume object metadata as
an adapter stream or page sequence instead of requiring a full object listing in memory
before useful work can begin.

## Seamless Git Experience

Performance work must be judged at the Git workflow level, not only at the request
level.

If a slower provider check, reconstruction path, or transfer path makes `git add`,
`git commit`, `git push`, `git fetch`, or `git checkout` feel worse than expected, users
will pay more for a less efficient system just to avoid the pain.

That is the standard Shardline has to meet.
