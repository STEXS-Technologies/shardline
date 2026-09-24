# Shardline reliability architecture

Shardline has one reliability model for durable server-side state machines:

1. The existing domain state and transition rules remain authoritative for
   protocol behaviour.
2. Every durable transition is recorded atomically with its metadata mutation
   in the index that owns that state.
3. `statechronicle` provides canonical, integrity-checkable digests for the
   resulting state and operation identity.
4. `penelope` provides a canonical process-boundary digest: operation,
   sequence, previous state, and resulting state.
5. Recovery, repair, and garbage collection read and verify that evidence
   before acting.

New evidence uses the StateChronicle canonical BCS encoding for state and
operation values, then uses Penelope's SHA-256 content digest for the process
boundary. Existing journal rows remain readable and are explicitly tagged as
legacy JSON evidence during deserialization; they are verified under their
original encoding and can be followed by canonical events without changing
the public state or retry behavior. Replaying an existing Postgres intent
never replaces valid legacy evidence, while a missing baseline for a newly
materialized intent at any valid lifecycle state is repaired atomically from
the canonical baseline when its journal is missing.

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
- retention holds (`active`, `released`, and re-created recovery), recorded as
  complete policy snapshots. Timestamp-derived activity remains the public
  behavior, while the evidence chain authenticates the policy row and its
  release/recovery boundaries.
- provider webhook delivery claims (`processed` and released retention),
  recorded as typed delivery snapshots. The delivery key remains the public
  idempotency boundary, while the evidence chain makes claims, deletion,
  purge, and re-processing after recovery verifiable.
- Hub repository refs (`main` and named refs), recorded as typed metadata
  snapshots. Ref updates and deletions use the same optimistic-concurrency
  boundary as before, while the evidence chain authenticates the materialized
  ref head and is verified on reads, writes, and repository cleanup.
- OCI tags, recorded as typed mutable-pointer snapshots. Tag retargets,
  insert-if-absent, digest-guarded deletion, reads, and listing all verify or
  append evidence in the same transaction as the tag row.
- S3 listing-index objects, recorded as complete typed materialized-row
  snapshots. Upsert, create-if-absent and conditional replacement, deletion,
  exact reads, and listings verify or append evidence in the same transaction
  as the index row; absence is an integrity-checkable state rather than a missing
  record that can silently diverge.
- provider tree entries and revision registries, which are immutable-version
  index records rebuilt from provider metadata rather than lifecycle state;
  their existing transactional pruning and rebuild contracts remain the
  source of truth and are intentionally not given a second lifecycle journal.

This same resumable lifecycle evidence is also persisted by the standalone
file-backed S3 multipart and OCI upload-session adapters. Their legacy session
files remain readable; new writes use an atomic session envelope containing the
canonical `SessionEvidenceLog` plus a persisted StateChronicle Merkle journal,
and reads/sweeps verify both before using the materialized progress. A read
that finds a legacy or evidence-free session may reconstruct an in-memory
baseline, but never silently writes it; the next successful mutation or an
explicit operator repair persists the baseline. Malformed or tampered
evidence remains fail-closed.

The local, non-fenced LFS PATCH path uses the same lifecycle evidence through
an additive `{oid}.evidence` sidecar and a companion persisted StateChronicle
Merkle journal. Historical sessions without those sidecars are reconstructed
as an active baseline, while new range writes, promotion, completion, abort
cleanup, and stale-session sweeps validate or append the canonical evidence.
Reads do not repair durable state; a successful mutation or explicit operator
repair establishes the missing baseline and Merkle commitment. The existing
`.meta`, `.ranges`, and staging files remain the materialized data-plane
representation and retain their previous layout.

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
legacy rows. If a Postgres provider row is found without its journal, the
verified canonical baseline is repaired in the read transaction before the
state is returned; the same verification is applied before destructive
provider mutations. They do not create a second reliability protocol. Where they
repair or reconcile an upload or session, they consume the canonical journal
and verify it first.

GC's persisted quarantine candidates are authoritative lifecycle state and use
the same evidence protocol in memory, SQLite, and Postgres. Retention holds
remain policy records whose active/released result is derived from their
timestamps, but their complete policy snapshots and release/recovery events
are bound by the same integrity evidence protocol; the last-GC clock anchor is
explicitly an optimization-only materialization. Webhook delivery claims use
the same typed snapshot protocol in memory, SQLite, and Postgres, including the
provider-mutation transaction, deletion, purge, and recovery re-claim paths.
OCI tombstones remain generation fences for logical deletion, but their
published/deleted/reclaimed visibility transitions now use the same
`OciObjectLifecycleEvent` evidence. Existing transactional compare-and-publish
rules remain authoritative for behavior.

When a quarantine candidate is released after its journal has been lost, the
stores reconstruct and persist both the active baseline and the release event
in one transaction. This preserves a complete sequence-0/sequence-1 chain
instead of leaving an unverifiable terminal-only record.

OCI tombstone reclaim, publish, delete, and completion use the same rule: a
missing visibility journal is rebuilt as a complete chain inside the owning
transaction before the new visibility boundary is committed. Tombstone reads
also repair a missing deleted baseline before returning inventory.

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
cryptographically integrity-checkable and replayable. These unkeyed digests
detect corruption and state/evidence disagreement; they do not authenticate a
privileged database writer or provide immutable provenance.

## Coherence requirements

The current durable-state inventory is intentionally explicit:

| Durable record | Classification | Evidence boundary |
| --- | --- | --- |
| Upload intents | Lifecycle state machine | `LifecycleEvent` |
| Resumable sessions and local LFS patch sessions | Lifecycle state machine | `StateTransitionEvent` / `SessionEvidenceLog` |
| Provider repository observations | Monotonic lifecycle snapshot | `ProviderLifecycleEvent` |
| GC quarantine candidates | Retention lifecycle state machine | `QuarantineLifecycleEvent` |
| Retention holds | Policy snapshot with timestamp-derived activity | `RetentionHoldLifecycleEvent` |
| OCI tombstones | Visibility/generation lifecycle state | `OciObjectLifecycleEvent` plus atomic compare-and-delete/publish fence |
| Webhook deliveries | Idempotency/recovery lifecycle snapshot | `WebhookDeliveryLifecycleEvent` plus unique delivery key and transactional insert |
| Hub repository refs | Metadata/ref-head snapshot | `HubRefLifecycleEvent` plus optimistic ref compare-and-swap |
| OCI tags | Mutable OCI pointer snapshot | `OciTagLifecycleEvent` plus tag-key uniqueness and digest-guarded CAS |
| S3 object listing rows | Complete mutable materialized-row snapshot | `S3ObjectLifecycleEvent` plus object-key uniqueness and conditional CAS |
| Provider tree entries and revisions | Immutable-version index/materialization | Existing transactional rebuild, pruning, and repository-key boundaries |
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
