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
- any number of API and transfer replicas running the same barrier-aware version

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
| OCI tag | one authoritative digest | unique metadata key plus database upsert/digest-guarded delete |
| OCI manifest/tag/blob deletion | operations in one repository are ordered | repository-scoped local/Postgres advisory lock |
| provider reconciliation fields | timestamps never move backward and revision matches its winning timestamp | atomic field-wise maximum merge in SQLite/Postgres |
| provider rename/delete/push/access event | old and new repository identities do not interleave | repository-scoped locks; rename acquires both identities in sorted order |
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
lock files beneath `.resource-locks`; Postgres deployments use transaction-scoped
advisory locks. Locks are domain-separated, so an OCI repository and provider
repository with the same text do not alias.

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

## Failure Boundary Still Under Test

Postgres transaction advisory locks close normal replica races, but they are not a
fencing token. If a node loses the database connection that owns a resource lock while
continuing an object-store operation, another node can acquire the released lock before
the first node observes failure. Stable arbitrary-multi-writer status therefore remains
unclaimed until failover tests either prove every affected commit revalidates ownership
or the operation adopts a persisted epoch/fencing check.

Until that work is complete:

- use one Postgres primary and reliable network failure detection
- stop or isolate a replica whose Postgres session is partitioned
- do not treat a successful object-store side effect without its metadata commit as a
  visible protocol commit; `fsck` and GC handle unreachable immutable debris
- keep provider integration and the overall multi-replica writer topology below Stable

## Mixed-Version Rule

Older replicas do not participate in newly added resource locks or the GC barrier. During
a rolling API upgrade, route OCI manifest/blob deletion and provider webhook mutations
only to new replicas, and suspend destructive GC. Resume unrestricted routing only when
all replicas run the new version. See [Rolling Upgrade](ROLLING_UPGRADE.md).

