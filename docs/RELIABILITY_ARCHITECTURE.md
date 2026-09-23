# Shardline reliability architecture

Shardline has one reliability model for durable server-side state machines:

1. The existing domain state and transition rules remain authoritative for
   protocol behaviour.
2. Every durable transition is recorded atomically with its metadata mutation
   in the index that owns that state.
3. `statechronicle` authenticates the resulting state and operation identity.
4. `penelope` authenticates the complete process boundary: operation,
   sequence, previous state, and resulting state.
5. Recovery, repair, and garbage collection read and verify that evidence
   before acting.

Reliability journal rows are namespace-keyed by `(operation_kind,
operation_id, sequence)`. The operation kind is part of the durable key, not
just an informational field in the JSON payload, so an upload intent and a
resumable session can never share a sequence by accident.

The implementation of this model is `crates/shardline-reliability`. Upload
intents use `LifecycleEvent`; LFS, OCI, and S3 resumable sessions use the same
`StateTransitionEvent` evidence format through the compatibility alias in
`shardline-index`. These are two domain views over one evidence protocol, not
two interpretations of reliability.

## Scope boundaries

The canonical state machines and durable lifecycle snapshots are:

- upload intent lifecycle (`created` through `visible` or `failed`);
- resumable session lifecycle (`active` through `completed`, `aborted`, or
  `expired`).
- provider-repository lifecycle observations (access, revision, cache,
  authorization, and drift timestamps plus the associated revision), recorded
  as complete materialized snapshots so monotonic merges cannot silently drift
  away from their evidence.
- garbage-collection quarantine candidates (`active`, `released`, and
  re-quarantined recovery), recorded as complete object-retention snapshots.
  A release event is journaled before the candidate row is removed, so a
  crash between index cleanup and object deletion remains replayable.

This same resumable lifecycle evidence is also persisted by the standalone
file-backed S3 multipart and OCI upload-session adapters. Their legacy session
files remain readable; new writes use an atomic session envelope containing the
canonical `SessionEvidenceLog`, and reads/sweeps verify it before using the
materialized progress.

The local, non-fenced LFS PATCH path uses the same lifecycle evidence through
an additive `{oid}.evidence` sidecar. Historical sessions without that sidecar
are reconstructed as an active baseline, while new range writes, promotion,
completion, abort cleanup, and stale-session sweeps validate or append the
canonical evidence. The existing `.meta`, `.ranges`, and staging files remain
the materialized data-plane representation and retain their previous layout.

All completion owners—including local LFS, OCI, and S3 Postgres completion
paths—use the same canonical transition evidence. Where the backend supports
fenced publication, publication metadata and the transition to `completed`
commit together; the local LFS path records the equivalent terminal evidence
around its atomic promotion and cleanup. A successful publication therefore
cannot exist without a verifiable terminal lifecycle event.

Resumable part publication is an `active -> active` evidence boundary in every
adapter, including both Postgres publication methods and the file-backed
session stores. Session reads, bounded listings, snapshots, recovery, and GC
verify the journal against the stored scope, session ID, target, and current
state before returning or acting on the session.

Provider metadata, manifests, tombstones, and CAS records retain their
existing transactional domain models and fencing rules. Provider-repository
lifecycle rows now also use the canonical snapshot evidence protocol: the
memory, SQLite, and Postgres adapters append and verify the complete snapshot
in the same mutation boundary, while migration backfills a self-baseline for
legacy rows. They do not create a second reliability protocol. Where they
repair or reconcile an upload or session, they consume the canonical journal
and verify it first.

GC's persisted quarantine candidates are authoritative lifecycle state and use
the same evidence protocol in memory, SQLite, and Postgres. Retention holds
remain policy records whose active/released result is derived from their
timestamps; the last-GC clock anchor is explicitly an optimization-only
materialization. OCI tombstones remain generation fences for logical deletion,
not a second lifecycle interpretation, and are protected by their existing
transactional compare-and-publish rules.

The following are data-plane recovery materializations, not independent
lifecycle state machines:

- LFS patch-range append/compaction files and the last-touched `.meta` file;
- S3 multipart part files;
- OCI staging/body and provider multipart material;
- the SDK's process-local upload/session state.

These records describe materialized progress and remain useful for restart
recovery, but must not redefine authoritative lifecycle state. Their owning
server/index state is the source of truth; reconciliation compares the
materialized progress with that state and the canonical evidence journal.

This separation preserves all existing routes, object keys, hash formats,
state spellings, and retry semantics while making durable transitions
tamper-evident and replayable.

## Coherence requirements

The current durable-state inventory is intentionally explicit:

| Durable record | Classification | Evidence boundary |
| --- | --- | --- |
| Upload intents | Lifecycle state machine | `LifecycleEvent` |
| Resumable sessions and local LFS patch sessions | Lifecycle state machine | `StateTransitionEvent` / `SessionEvidenceLog` |
| Provider repository observations | Monotonic lifecycle snapshot | `ProviderLifecycleEvent` |
| GC quarantine candidates | Retention lifecycle state machine | `QuarantineLifecycleEvent` |
| Retention holds | Policy record; active/released is timestamp-derived | Existing transactional row and invariant validation |
| OCI tombstones | Generation fence for logical deletion | Existing atomic compare-and-delete/publish fence |
| Webhook deliveries | Idempotency claim, not a lifecycle machine | Unique delivery key and transactional insert |
| Resource fences | Concurrency epoch, not domain lifecycle state | Existing fenced transaction boundary |

This inventory prevents either omission of an old state machine or accidental
creation of a second evidence interpretation for records that are deliberately
materialized or fencing-only.

Any new durable state machine must use the reliability crate's operation
identity, canonical StateChronicle digest, canonical Penelope process digest,
atomic journal write, and chain verification. It must not hash an ad-hoc JSON
shape or persist a parallel interpretation of lifecycle state. New recovery
formats may be added only as data-plane materializations with an explicit
reconciliation path.
