# Resumable Session Architecture

## Purpose

This document defines the implemented replacement for the shared ReadWriteMany (RWX)
staging filesystem used by the v1.7 scaled topology. It makes
API and transfer replicas disposable: durable coordination belongs in
Postgres, and incomplete payload bytes belong in the configured object store.

This is deliberately not a migration that copies staging files to another
filesystem. A filesystem with shared bytes but incoherent advisory locks is
the failure mode being removed.

## Scope

The design covers incomplete, externally visible protocol operations:

| Protocol | Local SQLite implementation | Postgres scaled implementation |
| --- | --- | --- |
| Git LFS PATCH | sparse file and range journal | immutable range fragments plus a Postgres range map |
| OCI blob upload | body/tail files; optionally native S3 multipart | immutable append fragments or native object-store multipart state |
| S3 multipart | one local file per part and JSON metadata | immutable part objects and a Postgres part map |

Completed CAS objects remain unchanged. Session objects are private,
unreachable by normal readers, and have a bounded TTL.

## Durable model

Every session has an opaque random ID and one immutable identity:

```text
(protocol, repository/scope, target object identity, creation generation)
```

Postgres is the linearization point. A session row contains:

```text
session_id
protocol
scope/repository
target identity
state                 active | completing | completed | aborted | expired
generation            monotonically increases when ownership changes
lease_epoch           fencing token for a short mutation claim
expires_at            database-clock expiry
created_at, updated_at
```

Each staged byte range or multipart part is written to an immutable object
key containing its session ID, part identity, and content hash. Only after
the object write is known to exist does a conditional Postgres mutation make
that object authoritative for the range/part. An object written by a losing
attempt is unreachable garbage and is eligible for session GC.

```text
write immutable temporary object
        ↓
conditional row update (session generation + lease epoch)
        ↓
object becomes authoritative
```

No request may make bytes visible to a protocol reader before the final
publication path has completed.

## Fencing and ambiguous outcomes

Part publication advances a durable generation in the same transaction that swaps the
authoritative part map. Completion claims a durable, monotonically advancing fencing
epoch. Neither mechanism is held while a client streams a body.

```text
writer A writes immutable attempt A
writer B writes immutable attempt B
writer A publishes part generation 17
writer B publishes part generation 18
completion pins generation 18
writer A's bytes remain unreachable garbage
```

The persisted part map resolves an acknowledgement lost after a metadata commit.
Retrying the same part/range atomically replaces or converges on the authoritative
entry and never creates partially visible content.

All expiry decisions use PostgreSQL's clock in a Postgres deployment. A
reaper first changes `active` to `expired` with a conditional update, then
deletes unreachable temporary objects asynchronously. It must not delete an
object still referenced by the current range/part map.

## Protocol-specific publication

### S3 multipart

`UploadPart` writes one immutable temporary object. The part map is keyed by
`(session_id, part_number)` and stores its current generation, object key,
length, and ETag. Replacing a part atomically swaps that map row; old part
objects become garbage only after the swap.

`CompleteMultipartUpload` atomically changes the session from `active` to
`completing` and records the exact requested part set. It reconstructs only
from that pinned map. Final protocol-object publication remains
upload-then-metadata-swap. A crash after publication is recovered by a
durable completion record; a crash before publication returns the session to
`active` or is retried from the pinned map.

### OCI blob upload

When the backend provides native multipart upload, Postgres stores the native
upload ID, completed part list, and fencing state. Native multipart abort and
complete happen only after a fenced state transition. Backends without native
multipart use immutable append fragments and the same pinned completion
model as S3 multipart.

### Git LFS PATCH

Each PATCH stores an immutable fragment and conditionally updates a canonical
non-overlapping range map. Overlap resolution is explicit and deterministic:
the winning mutation's epoch/generation determines the bytes for every
overlapped interval. Promotion pins the complete range map, rejects gaps, and
reconstructs from the pinned fragments before normal LFS publication.

## Required invariants

- A session identifier never changes protocol, scope, or target identity.
- A stale owner cannot make a part, range, abort, expiry, or completion
  authoritative.
- A successful completion reconstructs exactly the pinned bytes.
- A failed or expired session is never externally visible.
- Session GC removes only unreferenced temporary objects.
- Lost responses are resolved by durable state inspection, never by assuming
  failure.
- Quotas are enforced transactionally from durable counters/map rows; a
  replica restart cannot bypass them.
- Local SQLite deployments may retain local staging as an explicitly
  single-node implementation. The Postgres scaled topology must require no
  shared filesystem.

## Completed rollout

1. Added the Postgres schema, typed session API, database-clock expiry, and
   deterministic state-machine tests. No request path changes yet.
2. Added object-store temporary-object primitives with streamed write/read and
   integrity verification.
3. Moved S3 multipart first and exercised two replicas, takeover, response loss, abort/complete, and
   expiry races.
4. Moved OCI sessions to immutable fragments and fenced publication.
5. Moved LFS PATCH ranges with deterministic overlap/promotion rules.
6. Removed the RWX PVC and coherent-lock requirement from scaled deployment
   manifests and documentation after all three paths passed the
   multi-replica fault matrix.

The old and new session formats must not be mutated concurrently. During each
protocol cutover, existing filesystem sessions remain readable only by the
legacy path until their TTL expires; new sessions use the durable format.
