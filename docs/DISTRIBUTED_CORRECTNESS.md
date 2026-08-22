# Distributed Correctness

This document defines the writer contracts for a Shardline deployment. It separates
multi-process availability from multi-writer correctness: running several API or
transfer replicas is safe only when every mutable resource they share has the contract
listed below.

## Supported Writer Topology

The scaled topology uses:

- one shared Postgres metadata database
- one shared S3-compatible object store
- one shared `ReadWriteMany` API staging filesystem with cross-node advisory locks
- any number of API and transfer replicas running the same barrier/fence-aware version

SQLite is supported for a local deployment. Multiple local processes must share the
same root filesystem and its advisory-lock implementation; SQLite itself still limits
write throughput, so Postgres is the production multi-replica metadata backend.

## Mutable-State Contracts

| Resource | Required outcome | Enforcement |
| --- | --- | --- |
| immutable chunks, xorbs, shards, OCI blobs | identical concurrent puts converge | content-derived key plus `put_if_absent` and integrity verification |
| upload intent ID | one immutable `(object key, hash, length)` identity and monotonic lifecycle | store-level identity check and legal-state transition check |
| S3 conditional object write | exactly one matching writer wins | metadata-row insert-if-absent or compare-and-swap; loser receives `412` |
| S3 unconditional write | one complete version is visible, last metadata commit wins | immutable record first, atomic metadata-row swap second |
| Hub ref/revision mutation | stale parent loses; delete cannot interleave with a push | SQLite transaction or Postgres repository-row lock |
| OCI tag | one authoritative digest; a stale lock owner cannot retarget it | unique metadata key plus upsert/digest-guarded delete on the lock-owning fenced Postgres session |
| OCI manifest/tag/blob deletion | operations in one repository are ordered | repository-scoped local/Postgres advisory lock |
| provider reconciliation fields | timestamps never move backward and revision matches its winning timestamp | atomic field-wise maximum merge in SQLite/Postgres |
| provider rename/delete event | old and new repository identities do not interleave and all durable effects commit together | sorted repository locks plus one transaction containing the delivery claim, record mutations, retention holds, lifecycle state, and every expected fence row |
| provider push/access event | independent lifecycle observations never regress each other | repository lock plus atomic monotonic state merge |
| webhook delivery | one application per provider/repository/delivery ID | unique delivery claim, removed when application fails |
| LFS PATCH session | ranges, quota, promotion, and sweep do not race | shared staging root, global accounting lock, striped per-OID lock |
| OCI upload session | append/finalize/abort and quota accounting do not race | shared staging root and cross-process session lock |
| S3 multipart session | part write, completion, abort, quota, and sweep do not race | shared staging root, global accounting lock, per-session part lock |
| destructive GC | no visible writer can re-reference an object between final mark and delete | request-shared/GC-exclusive local or Postgres barrier |

Unconditional last-writer-wins operations do not promise request-arrival ordering. They
promise that readers observe one complete committed version, never mixed bytes and
metadata from different versions.

## Lock Domains And Ordering

Application resource locks use a stable SHA-256-derived identity. Local deployments
lock files beneath `.resource-locks`. Postgres deployments use a dedicated
session-level advisory lock and atomically advance a durable epoch in
`shardline_resource_fences`; the dedicated connection is closed on guard drop and is
never returned to the pool with a lock attached. Locks are domain-separated, so an OCI
repository and provider repository with the same text do not alias.

OCI tag mutations run on the same connection that owns the lock and epoch. Provider
rename/delete transactions run on the first lock-owning connection and lock every
expected fence row before claiming the delivery or changing metadata. That row lock
prevents a replacement owner for a second rename identity from advancing its epoch
until the transaction commits or aborts. A terminated primary session aborts the
transaction; a superseded secondary identity is detected before mutation.

Operations needing more than one resource sort their lock identities before acquiring
them. Provider rename is the current two-resource operation: it locks old and new
repository names in canonical order.

The GC barrier is outside all resource locks:

```text
request shared GC barrier
  -> zero or more sorted resource locks
    -> metadata/object mutation
```

Destructive GC takes only the exclusive GC barrier. This ordering prevents a resource
operation and GC from forming a lock cycle.

## Staging Filesystem Contract

Resumable protocol state is intentionally bounded filesystem staging, not authoritative
completed-object storage. Every API replica must see the same paths and advisory locks.
The filesystem must preserve lock exclusion across nodes; a volume that merely exposes
the same bytes without coherent locking is unsupported.

LFS uses 256 hashed lock stripes, bounding lock-file growth independently of the number
of client OIDs. S3 multipart uses one lock inside each already-bounded session directory.
Expired session sweeps take the same locks as active writers before deleting files.

## Remaining Fencing Boundary

Persisted epochs and dedicated lock-owning sessions now detect a terminated owner, and
OCI tag metadata commits are fenced on that session. The checked Postgres test
terminates the first backend, proves the replacement receives a higher epoch, and
proves the old guard can no longer validate or mutate through its owning connection.

One external-system category still prevents a Stable arbitrary-multi-writer claim:

- OCI manifest/blob deletion directly changes the object store, which cannot validate a
  Postgres epoch. A connection can theoretically fail after the final validation but
  before that external delete.
OCI deletion needs logical tombstones/deferred GC before the remaining claim can be
closed. Immutable content-addressed puts are safe:
their keys cannot be retargeted and a failed metadata commit leaves only unreachable
debris.

Until that work is complete:

- use one Postgres primary and reliable network failure detection
- stop or isolate a replica whose Postgres session is partitioned
- do not treat a successful object-store side effect without its metadata commit as a
  visible protocol commit; `fsck` and GC handle unreachable immutable debris
- keep the overall multi-replica writer topology below Stable

## Mixed-Version Rule

Older replicas do not participate in newly added resource locks or the GC barrier. During
a rolling API upgrade, route OCI manifest/blob deletion and provider webhook mutations
only to new replicas, and suspend destructive GC. Resume unrestricted routing only when
all replicas run the new version. See [Rolling Upgrade](ROLLING_UPGRADE.md).
