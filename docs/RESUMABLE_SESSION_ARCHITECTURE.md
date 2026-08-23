# Resumable Session Architecture

## Purpose

This document defines the replacement for the shared ReadWriteMany (RWX)
staging filesystem required by the v1.7 scaled topology. Its goal is to make
API and transfer replicas disposable: durable coordination belongs in
Postgres, and incomplete payload bytes belong in the configured object store.

This is deliberately not a migration that copies staging files to another
filesystem. A filesystem with shared bytes but incoherent advisory locks is
the failure mode being removed.

## Scope

The design covers incomplete, externally visible protocol operations:

| Protocol | Current staging | Durable replacement |
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
key containing its session ID, generation, and a random attempt ID. Only after
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

Mutating operations claim the session with a durable, monotonically advancing
fencing epoch. The claim is short-lived and only protects metadata updates;
it is never held while a client streams a body.

```text
writer A claims session: epoch 17
writer A writes temporary object
writer A loses its database connection
writer B claims session: epoch 18
writer A receives a delayed success response
writer A's metadata compare-and-set(epoch 17) fails
```

The same rule resolves an acknowledgement lost after a metadata commit:
the caller reads the session row and determines whether its immutable attempt
became authoritative before retrying. Retrying a request is therefore
idempotent; it must never create a second visible part/range.

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

## Rollout

1. Add the Postgres schema, typed session API, database-clock expiry, and
   deterministic state-machine tests. No request path changes yet.
2. Add object-store temporary-object primitives with streamed write/read and
   integrity verification.
3. Move S3 multipart first: it has discrete parts and the smallest semantic
   gap. Exercise two replicas, takeover, response loss, abort/complete, and
   expiry races.
4. Move OCI sessions, preserving native multipart when available.
5. Move LFS PATCH ranges and their overlap/promotion rules.
6. Remove the RWX PVC and coherent-lock requirement from scaled deployment
   manifests and documentation only after all three paths pass the
   multi-replica fault matrix.

The old and new session formats must not be mutated concurrently. During each
protocol cutover, existing filesystem sessions remain readable only by the
legacy path until their TTL expires; new sessions use the durable format.
