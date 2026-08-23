# Distributed Correctness

This document defines the writer contracts for a Shardline deployment. It separates
multi-process availability from multi-writer correctness: running several API or
transfer replicas is safe only when every mutable resource they share has the contract
listed below.

## Supported Writer Topology

The scaled topology uses:

- one shared Postgres metadata database
- one shared S3-compatible object store
- any number of API and transfer replicas running the same barrier/fence-aware version

Postgres and S3 are the complete shared durable-state envelope. API and transfer roots
are pod-local runtime state and do not require cross-node locking or shared bytes.

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
| OCI tag | one authoritative digest; a stale lock owner cannot retarget it | unique metadata key plus transactional publication on the lock-owning fenced Postgres session |
| OCI manifest/tag/blob deletion | deletion is immediately visible without an unfenced object-store side effect | repository-scoped lock plus transactional tombstone; manifest tags are removed in the same commit |
| provider reconciliation fields | timestamps never move backward and revision matches its winning timestamp | atomic field-wise maximum merge in SQLite/Postgres |
| provider rename/delete event | old and new repository identities do not interleave and all durable effects commit together | sorted repository locks plus one transaction containing the delivery claim, record mutations, retention holds, lifecycle state, and every expected fence row |
| shared retention expiry | a skewed replica cannot expire a hold early using its local wall clock | Postgres-backed lifecycle repair uses PostgreSQL `clock_timestamp()` as the shared epoch authority; local deployments remain single-writer |
| provider push/access event | independent lifecycle observations never regress each other | repository lock plus atomic monotonic state merge |
| webhook delivery | one application per provider/repository/delivery ID | unique delivery claim, removed when application fails |
| LFS PATCH session | ranges, overlap order, quota, promotion, and sweep do not race | Postgres range map and completion fence; immutable object-store fragments |
| OCI upload session | append/finalize/abort and quota accounting do not race | Postgres part map and completion fence; immutable object-store fragments |
| S3 multipart session | part replacement, completion, abort, quota, and sweep do not race | Postgres part map and completion fence; immutable object-store parts |
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

OCI publication and deletion metadata run on the same connection that owns the lock and
epoch. Publishing clears a prior tombstone and updates all requested tags in one
transaction. Deleting a manifest writes its tombstone and removes every tag still
pointing to that digest in one transaction. Provider rename/delete transactions run on
the first lock-owning connection and lock every
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

## Durable Resumable Sessions

Postgres deployments store session identity, protocol target, attributes, quotas,
part/range maps, database-clock expiry, generation, and completion fencing in Postgres.
Payload fragments are immutable private objects beneath `staging/resumable/` in the
configured object store. A staged write becomes authoritative only after its part-map
transaction commits; a losing or replaced attempt is garbage, never visible content.

Completion advances the session fence and pins one durable part snapshot. LFS replays
overlaps in generation order, OCI replays append parts in part-number order, and S3
uses the exact client-selected part sequence. Publication completes before the fenced
session changes to `completed`. Stale completion owners cannot commit the terminal
transition.

The exclusive GC/write barrier blocks mutating requests while session GC expires stale
sessions using PostgreSQL's clock, protects every object referenced by a live part map,
reclaims all other staging objects, and only then CAS-removes terminal session rows.
Crashes at any point are retry-safe. Local SQLite deployments retain bounded local
filesystem sessions and remain explicitly single-node.

## OCI Logical Deletion

Persisted epochs and dedicated lock-owning sessions now detect a terminated owner, and
OCI visibility metadata commits are fenced on that session. The checked Postgres test
terminates the first backend, proves the replacement receives a higher epoch, and
proves the old guard can no longer validate or mutate through its owning connection.

OCI request handlers never physically delete manifest, media-type, blob, or legacy tag
objects. They commit durable tombstones instead, and every OCI read and reference check
consults those tombstones. Re-uploading identical content clears the tombstone under the
same repository fence. A process that loses its Postgres session therefore has no
external destructive side effect left to race against a newer owner.

Physical reclamation is performed only by GC while it owns the exclusive writer
barrier. After the retention window expires, GC deletes the fixed digest object first
and CAS-removes the unchanged tombstone second. A crash before tombstone removal leaves
the object logically deleted and makes the next sweep idempotent; no publisher can race
the physical delete because all mutations require the shared side of the same barrier.

With all replicas on this fence-aware version, the shared Postgres/S3 writer topology
has a defined correctness contract. Availability still follows the configured
Postgres, object-store, and Redis dependencies.

## Mixed-Version Rule

Older replicas do not participate in newly added resource locks, tombstones, or the GC
barrier. During a rolling API upgrade, route OCI reads/writes/deletes and provider
webhook mutations only to new replicas, and suspend destructive GC. Resume unrestricted
routing only when all replicas run the new version. See [Rolling Upgrade](ROLLING_UPGRADE.md).
