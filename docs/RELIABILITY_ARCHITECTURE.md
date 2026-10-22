# Shardline reliability architecture

For commands, benefits and crash recovery, start with the
[operator guide](MAINTENANCE_INTEGRATION_UX.md). This page describes implementation
details. Mutating GC, rebuild, lifecycle repair and retention hold updates
keep their locks and metadata work on non-replaceable Postgres connections.
The [rolling-upgrade guide](ROLLING_UPGRADE.md) describes deployment requirements.

The 0.2.0 integration includes an embedded server tracker and automatic Linux
maintenance tracking, with an explicit bounded runner for advanced use. Default
deployment and recovery requirements are described in
[maintenance integration and operator experience](MAINTENANCE_INTEGRATION_UX.md).

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

## Published crates and Hyper

Workspace-built binaries and Docker images include the pinned Hyper HTTP/1
shutdown fix from [Hyper PR #4222](https://github.com/hyperium/hyper/pull/4222).
Cargo does not propagate a workspace patch to applications that depend on
Shardline through crates.io. Those applications use released Hyper, which
currently lacks this fix; under the affected connection-shutdown race, an HTTP
request can remain pending. The binary and library distributions therefore do
not have identical HTTP client failure behavior.

The release and coordinated-publication scripts explicitly acknowledge this
one exact override and warn before publication. Other overrides still block
publication. Applications needing the fix can use the same Hyper patch in their
own workspace until an upstream release includes it. Remove the exception once
released dependencies and an independent consumer have been verified.

## Incremental dependency upgrade

The StateChronicle and Penelope 0.2.0 dependency upgrade retains the existing
evidence protocol. `LegacyJson`, `CanonicalBcsV1`, and `CanonicalBcsV2` keep their
original meaning; the Merkle envelope remains version 1 and continues to use
the existing synthetic asset projection. This change introduces no new migration-ledger
entry, session layout, or signing configuration for existing deployments. PostgreSQL
still requires owner-run `db migrate up` to install the additive protection
functions and triggers, even when no migrations are pending.
Normal Linux maintenance is tracked in its existing CLI process; server startup
owns in-process observation of its existing background sweeps. Only the explicit
protected runner starts managed children. Neither requires a tracker service or
introduces a GC schedule.
Normal server shutdown stops optional startup housekeeping and waits for its
blocking workers while database I/O is still available. An interrupted Hub
secret sweep keeps completed atomic updates; later startup or lazy reads can
upgrade the remaining rows. Embedded hosts that own their Tokio runtime can use
`app::router_with_background_tasks`, stop HTTP serving, and await the returned
handle's `shutdown()` before dropping that runtime. Existing `app::router`
callers must keep their runtime alive until background work has finished.
The public signing method continues to accept `ed25519-dalek` version-2 keys;
the version-3 key required by StateChronicle is constructed internally from a
zeroized seed copy.

Frozen vectors in `shardline-reliability/tests/fixtures` were generated by
Shardline 1.11.3 with the 0.1.0 dependencies. They protect historical reads,
deterministic commitment reconstruction, signatures, and sequence integrity
across the upgrade. The dependency bump does not authenticate unsigned evidence
or add automatic retries after ambiguous external effects.

Native `StateProfile` semantics now validate upload and resumable event creation
and integrity checks. Both profiles use the existing transition tables, including
resumable recovery to `active`, so historical baseline and suffix events remain
valid. `LifecycleCommand::Transition` binds the expected state; initialization
admits only `created` uploads or `active` sessions on absent resources.

`native_lifecycle_registry()` registers the standard `TypedProfile` adapters for
`shardline.upload-lifecycle` and `shardline.resumable-lifecycle`, each at schema 1
with a 32-byte payload limit. Operations are namespaced per domain:
`shardline.upload.initialize`, `shardline.upload.transition`,
`shardline.resumable.initialize`, and `shardline.resumable.transition`.
Authority remains required. These adapters support explicitly versioned new
resources; they do not silently replace the historical synthetic asset profile.
Semantic replay is not signature, authorization, inclusion, or freshness
verification. New native histories still need an explicit resource identity,
retained historical verifiers, and a rollout gate before production activation.

Postgres history grouping has an additive resolved identity, separate from the
unchanged legacy deployment fingerprint. The usual owner-run upgrade installs
one random 256-bit namespace identity in the existing fence table and a
schema-bound read function. Reinstallation preserves it. Readers need schema
usage, not fence-table grants; row-level security refuses shared grouping.
`resolved_postgres_deployment_fingerprint` reads this identity on the caller's
existing metadata connection and binds it to the configured endpoint (including
Unix socket), database, schema and object target. Independently initialized
stores behind one proxy cannot collide merely because their names match.
Missing initialization, corrupt identity or an absent schema fails closed.
This is not a health proof. `EmbeddedTaskTracker::with_history_scope` can publish
an immutable companion binding signed by the run's existing key. It binds the
shared scope to the exact stored manifest bytes; readers verify the signature
before comparing scopes. Identical retries succeed, conflicting bindings fail,
and verified completed-run retirement removes the companion. A binding failure
warns while original tracking continues. Existing manifests and task formats
are not rewritten. Normal server startup resolves the scope on its existing
metadata pool and attaches bindings to its embedded task runs. Admin inspection
resolves its current metadata scope and admits matching signed records across
logins. Bound records for another scope are excluded; unbound legacy records
remain visible only under their original deployment fingerprint. Corrupt
bindings expose an incident reference without trusting the manifest fields.
Resolution is read-only and limited to five seconds. Missing owner initialization,
unsupported isolation or database failure falls back to legacy history, with a
warning and `history_grouping: "legacy"` in the admin response. Local deployments
also retain legacy grouping. This adds no pool, process or configuration.
Ordinary CLI tracking installs a task-local observation context around its
original handlers, for both shared-only receipts and local journals. GC, rebuild
and lifecycle repair resolve and publish their binding on the already acquired
barrier-owning work pool, before their metadata scan. Observation creates no
pool, never replaces a failed session, and warns on binding failures without
changing the native result or replaying work. Its context is not inherited by
spawned tasks. This resolved shared-scope binding covers these observed handlers;
it does not establish a shared PostgreSQL scope for storage-copy endpoints or
every optional-runner entry point. Server history association observes startup's
metadata target and does not fence or prove the target of every later database
operation.

The optional `shardline-reliability/task-journal` feature provides
`SqliteTaskJournal`, a synchronous Penelope `TaskJournal` port pinned to an exact
scope and complete definition fingerprint. It uses a dedicated task database,
WAL with FULL synchronization, and immediate transactions to arbitrate writers.
Each append replays the bounded history before checking sequence, generation,
clock order, policy and quotas. Referenced blobs commit with their events and
remain immutable. Identical committed records can be acknowledged again after
a lost acknowledgement; a different record at the same sequence conflicts.
Missing prefixes, checksum damage, mismatched blobs and scope substitution fail
closed. Limits are 64 KiB per blob and 4096 records per history.

`restore_local_task_supervisor::<Runtime, BUFFER_BYTES>()` composes this journal
with the real Penelope executor and a host-supplied `TaskRuntime`. Its returned
`LocalTaskSupervisor` handles submission, committed checkpoints, timeout/fault
decisions, explicit reconciliation and bounded recovery. Restoration replays
the pinned history and requires successful fencing before recording an
interrupted running generation; it never dispatches user code during replay.

The optional `process-runtime` feature supplies a Linux `ProcessTaskRuntime`
and `ProcessWorker` hosted port. An exclusive controller lock arbitrates each
task across definitions. Durable task pins reject changed input or policy;
start dispatch checks the committed journal projection and executable digest.
Workers claim a generation under an OS lock before application work. Persistent
fence tombstones reject delayed starts. Fencing checks boot and process-birth
identity, kills via a pidfd, and confirms release of the generation lock before
acknowledging termination. A PID reused by another process is never killed.
The local runtime database is bound to its machine and PID namespace; it is not
a multi-host runtime or a transferable live-execution database.

CPU ticks are process CPU seconds; a worker lowers its kernel soft and hard CPU
limit before executing. Deadline ticks are boot-monotonic milliseconds. An
absolute BOOTTIME timer and a dedicated watchdog thread terminate the worker
without an application callback or a live controller. On host reboot, the
pinned boot clock conservatively expires unfinished tasks instead of resetting
their budget. The trusted worker runs in a dedicated process, must not spawn
subprocesses or detach application work, and uses a private owner-only directory
and database. Checksums detect accidental damage; trusted storage permissions
provide authenticity. Already-issued external effects remain subject to
independent reconciliation after fencing.

The Linux `shardline maintenance` command composes these ports with the existing
fsck, index rebuild, GC, lifecycle repair and storage migration implementations.
Normal server startup starts no child processes. SQLite operations run on a blocking
thread; a protected child process performs application work. Deployment roots,
metadata/S3 targets, migration endpoints, input and executable are pinned. State
directories remain outside deployment and migration roots. The controller
retains the original absolute deadline when the same task ID is invoked again.

Checkpoints and final reports use the separate native domain
`shardline.maintenance-artifact`, schema 1. These immutable Custom resources
bind the task scope, generation, phase, artifact role, SHA-256 digest and byte
length. Proofs carry the native event and signed commit. Verification checks the
trusted local signing key, scope/generation, exact native transition, event and
state roots, and materialized file bytes. The producer is a trusted local
service; this profile does not authorize externally submitted intents.
`CheckpointArtifactVerifier` implements Penelope's StateChronicle checkpoint
verifier and binds each real checkpoint record to its authenticated native state.
Historical deployment evidence retains its original synthetic projection.

Reports stream into private digest-addressed files, bounded at 64 MiB. Proof
blobs are bounded at 64 KiB. Files are durable before proposals, and checkpoints
pause until their journal commit and acknowledgement. A report may contain
integrity issues: its authenticated outcome preserves a failure exit code even
though the scan itself completed. Completion proves publication of that report,
not snapshot consistency, freshness or absence of integrity issues.

Recovery first fences the interrupted generation. An independently verified
result proposal with its durable report can reconcile to completion. Otherwise
fsck may retry a complete fresh scan, including its repeatable legacy-evidence
backfill. Interrupted index rebuild, GC, lifecycle repair or migration without
a verified report remains unknown and requires operator inspection; process
exit alone cannot authorize completion or another mutation. Boundary checkpoints
never act as partial inventory cursors or waive maintenance barriers, fresh GC
reachability, holds, retention, or clock checks. No automatic checkpoint schema
or executable migration is performed.

Tests exercise real workers, restored-generation fencing, checkpoint pause and
acknowledgement, kernel CPU limits, watchdog expiry without a controller,
native-proof tampering, report tampering, and completed-history reuse.

Ordinary CLI commands keep their original process and domain barriers. Local
journals support later evidence export. If permissions prevent creating private
task storage, the command publishes native intent, start, checkpoint and signed
outcome records directly to its existing object adapter. This applies both to
Postgres/S3 commands without writable temporary storage and local deployments
whose database, SQLite sidecars and object directories remain writable under a
non-writable root. It creates no new root entries or mount requirement. Its
signing key stays in memory; interrupted history cannot authorize a restart.
Read-only GC and migration scans write no task history or scratch files.
If shared observation is unavailable in a deployment with no writable local
storage, the command preserves its application result and explicitly warns that
no verified outcome is available. This preserves existing availability and
permissions; it does not provide an audit-loss guarantee during storage outages.

Postgres rebuild uses a single session for its exclusive maintenance lock and
all metadata reads and writes. Its private pool rejects replacement connections:
losing that session cannot let an old scan reconnect after takeover. The lock
uses the existing advisory key and remains held across individual metadata
transactions. Normal completion closes the pool; cancelling acquisition drops
the session. Idle and lifetime retirement are disabled for this invocation so
long scans do not lose ownership merely because a pool timer expires. Failure
may leave earlier committed changes; it does not imply rollback of the entire
rebuild or authorize an automatic retry. This protection covers rebuild's
metadata mutations, not other operations' remote object-store effects.

Retention hold setting and release use the same connection binding with a shared
barrier. Their domain transaction and reliability evidence commit on that session.
The hold factory still runs after acquiring the barrier, so relative expiration
retains its existing meaning. A lost session cannot reconnect to apply a late
hold change while GC has taken exclusive ownership.

Lifecycle repair also binds its shared barrier, metadata scan and metadata
changes to one non-replaceable session. Losing that session stops repair before
it can reconnect and prune holds or quarantine records during another owner's
exclusive run. Object-store access in this operation reads reachability; this
binding does not establish fencing for other operations' remote writes or deletes.
Earlier committed repair changes may remain after a failure.

Mutating GC uses the exclusive form of the same work pool for resumable-session
metadata, reachability scans, quarantine changes and OCI metadata reclamation.
Losing that session prevents the old invocation from reconnecting for later
database work. A blocked-scan regression verifies failure after exclusive
takeover, with no new quarantine row or clock anchor. Pure GC dry runs retain
their existing pool and do not acquire this exclusive lock. This change does
not atomically fence S3 effects: an already issued request can have an unknown
outcome, and requests delayed across takeover still require protection.

Hub tree repair also uses one shared, non-replaceable session for its barrier,
reference reads and recovered-tree publication. Its blocking task owns the
barrier until the task finishes, even if the awaiting future is cancelled.
A real Postgres regression blocks the reference scan, terminates only that
session, acquires exclusive ownership elsewhere, and then releases the scan.
The old implementation published a recovered revision; the bound implementation
returns an error and preserves the original ref.
A separate Postgres cancellation test aborts the waiting caller while the scan
is blocked. Competing exclusive ownership remains queued until the blocking
repair publishes its recovered tree and releases its session.

Library callers can obtain this bound pool through
`MetadataWriteBarrier::postgres_metadata_pool()`. Existing barrier acquisition
remains available. Callers that open a separate metadata pool do not receive
the connection-loss protection automatically. Dropping the barrier closes its
pool to new acquisitions, including retained clones; graceful connection cleanup
then releases ownership. This does not establish fencing of remote requests.

The real delayed-delete regression originally reproduced this boundary: after
GC lost its database owner, a later hold was acknowledged before the old request
removed the bytes. With durable admission, the same regression now rejects the
later hold with the typed retryable error and prevents a replacement GC from
issuing a second delete. The old request can finish and its source records
failure. If its single-attempt successful reply is observed, an independent
recorder can settle the exact marker; a missing reply keeps it pending.

An ETag precondition alone cannot establish Shardline ownership. Amazon S3's
[conditional-delete contract](https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-deletes.html)
checks whether the object matches its ETag. A metadata-only hold does not modify
the stored object, so this precondition cannot distinguish the old and new
Shardline owners. Idempotency alone likewise does not authorize a delete after
the object's protection changes.

The protocol needs a durable effect gate: an uncertain issued delete
must prevent a later successful hold or reference publication from racing with
it. Later requests for the affected object must return a clear retryable error
instead of waiting indefinitely or acknowledging protection. This is the chosen
behavior. Retention, dedupe and directly named file/reconstruction references
now enforce it. Native shard publication also checks member-chunk references
extracted during xorb validation, as described below. Clearing that
gate requires proof that issued requests have settled, rather
than merely an expired timer, a dead PID or a lost database session. This must
also work during mixed-version operation and rollback. An additive database
migration is not automatically compatible: the existing
`check_database_schema_compatibility` verifier rejects unknown applied migration
versions. An isolated Postgres fixture confirms that the released 1.11.3
`db migrate up` exits with code 2 for an unknown migration record and succeeds
again after removing that fixture record. This checks the released migrator,
not a full released-server startup.

The first part is implemented in `PostgresIndexStore`: delete admission commits
an object marker with a globally unique epoch before any remote request. A
writer checks that marker while holding the same transaction through its
publication. Exact-epoch settlement removes the marker; one persistent sequence
prevents an old completion from matching a later attempt. Neither process death
nor connection loss clears an unresolved marker.

The existing owner-run `db migrate up` installs the read check, admission and
settlement functions, and publication triggers in one transaction, without adding
migration-history records or changing released checksums. The functions check
the original session role's existing retention and GC metadata permissions;
runtime roles need no direct fence-table grant. Each function is bound to its
installed schema, so a caller cannot redirect definer access to another schema.
The triggers protect older SQL writers too. Direct-reference triggers use the
existing table-write authorization and require no new retention/fence grants.
Chunk records check physical chunks and dedupe mappings; term records and
reconstructions check their xorb containers and cache sidecars. Large reference
publications acquire at most 256 advisory lock slots in sorted order, then
check exact pending keys. Slot collisions only serialize transactions; they do
not make an unrelated key pending. Single-key retention checks retain their
original locks as well. Dedupe publication uses bounded slots, so a large native
shard transaction does not accumulate one lock per mapping. This does not prove safe takeover from a GC
admission that predates installation of the new reference protection.
Delete admission refuses to create a marker if required triggers are missing.
The definer functions
use fixed search paths with temporary schemas last, following the
[Postgres function guidance](https://www.postgresql.org/docs/16/sql-createfunction.html).

Current native Postgres shard uploads also derive physical member-chunk keys
from validated xorb descriptors, ignoring the optional chunk-cache sidecar.
The additive `commit_native_shard_metadata_with_remote_delete_references`
method checks those keys and their existing dedupe mappings in the same
transaction as version, mapping and latest publication. Its schema-bound
definer check uses the caller's existing record-column write privilege, so it
requires no new retention/fence grant. Existing method signatures and record
formats remain unchanged. Legacy schemas preserve their previous behavior;
guarded GC admission requires the installed member-check function. Older
SQL-only writers do not derive member keys; embedders must supply complete
physical references before publication. The member-aware byte and file registration APIs
collect these references during the existing xorb range-validation pass.
They preserve the legacy APIs and `ParsedShardUpload` fields, and the server
no longer rereads xorbs or launches extra blocking work to collect references.
A real S3 HTTP fixture verifies identical records and HEAD/GET counts, with
one payload GET for two references to the same xorb. A byte/file regression
verifies identical metadata to the legacy API. These fixture results describe
that workload, not a deployment-wide performance guarantee. See
[benchmark workloads](BENCHMARKS.md) for contention measurements.

Normal Postgres/S3 GC uses this protocol for quarantine, OCI tombstone and
resumable-staging deletion. The guarded client sends a single-object DELETE with
retries disabled, following the dependency's
[retry configuration contract](https://docs.rs/object_store/0.14.0/object_store/struct.RetryConfig.html).
A successful response permits exact-epoch settlement. Normal GC supplies a
lazy one-connection recorder using the same pinned database URL and credentials.
If settlement on the original work session fails, only that acknowledged
attempt's marker can be cleared through the recorder. The original error still
propagates and stops GC; all admissions and ordinary metadata work remain bound
to the original non-replaceable session. A stale receipt cannot clear a newer
epoch. The permit captures the admitted schema, and settlement qualifies its
function explicitly; a changed recorder search path cannot settle a coincident
key/epoch in another schema. No receipt is inferred from process death, elapsed
time or object absence.
Remote errors, cancellation before receiving the reply, and an unavailable
recorder retain the marker. No background worker or delete replay is introduced.
The generic legacy GC library entry points preserve their signatures; embedders
wanting this boundary use `run_postgres_gc_with_remote_delete_protection` with
the same ownership-bound record and index adapters.

The additive receipt API provides a foundation for preserving a known successful
reply when database settlement is unavailable. `begin_remote_delete_with_receipt`
registers a fresh attempt's public signing key atomically with its pending marker.
The private key stays with the caller. A signed reply binds the admitted schema,
durable metadata namespace, object and epoch. Settlement verifies the signature,
namespace and registered key before clearing that exact marker under the existing
object locks. Direct settlement of these permits performs the same identity
checks. Tampered, foreign and stale receipts cannot clear a pending attempt.
Settlement removes the registered key with the marker; existing GC privileges
suffice without fence-table grants. Owner-run `db migrate up` installs the
functions without changing the released migration catalog.

Normal GC registers a fresh per-attempt signing key when the owner-installed
receipt functions are present. Older owner installations keep legacy admission.
After receiving success, GC attempts an immutable content-addressed write under
`_shardline_tasks/v1/delete-replies/{namespace}/replies/` before metadata settlement.
Evidence writes are optional: denied S3 writes warn and GC still attempts its
existing acknowledged settlement. If settlement fails, GC returns the original
error and leaves saved evidence for another invocation. Normal completion removes
the saved reply. No private key is persisted.

Ordinary Postgres/S3 sweep and `repair` process saved replies within their existing
ownership session before scanning maintenance work. Each pass examines at most
64 records, reads at most 16 KiB per reply and advances a durable object-store
cursor past invalid or unmatched evidence. The cursor wraps and cannot authorize
settlement. Strict signature, namespace, content-key and pinned-verifier checks
precede exact-attempt settlement. Only the reply object is removed afterward;
recovery never sends a user-object DELETE. A verified reply for the same target
can also retire when metadata has no pending delete for that object, checked
under the settlement locks. Foreign, corrupt and conflicting evidence remains.
The namespace reader requires existing GC privileges without exposing history
across row-security scopes or requiring direct fence-table access.

A signature records a trusted caller's observed reply; it does not prove provider
health. A reply never received, or lost before being saved while metadata is
unavailable, still cannot be recovered automatically. Such attempts can remain
blocked indefinitely. Keep their markers; the
[recovery runbook](DISASTER_RECOVERY.md#unprovable-remote-deletes) describes an
isolated restore when needed. Older GC binaries must stay paused throughout
upgrade and rollback, as required by the [rollout guide](ROLLING_UPGRADE.md).

The dedupe trigger checks both the mapped shard key and the physical chunk key
derived from a canonical 64-character lowercase hash. It locks their distinct
bounded slots in sorted order before checking exact markers. Inserts and hash
updates from older SQL writers therefore participate without new grants.
Malformed legacy hashes retain their prior behavior. This does not establish
protection for other chunk members hidden in an older writer's xorb payload.

The existing admin server exposes read-only pending-marker inspection at
`/api/v1/tasks/maintenance/deletes`. A schema-bound security-definer function
requires the caller's existing maintenance read privileges and refuses to
broaden row-level security scopes. A partial epoch index supports pages of at
most 100 rows plus one lookahead row. Globally unique attempt epochs keep
cursors compact even for long object keys. Inspection neither issues storage
requests nor settles markers; pending includes normally in-flight deletes.

Protected publication uses the transaction checks above; library callers remain
responsible for complete physical references and ownership through publication.
Saved signed successful replies provide settlement evidence; a successful new
request cannot prove that an earlier request will never apply later. Older GC
binaries do not create these markers, so rollout requires pausing those jobs.
Reserved S3 upload-temp cleanup retains its existing
path; these generated keys are outside the user-object namespace.

Both modes use PostgreSQL's advisory-lock wait queue. A queued exclusive owner
blocks later shared acquisitions. During acquisition, cancellation closes the
connection instead of leaving a detached pool-return operation queued on the
lock. Successful acquisition explicitly hands that same session back to its
private pool; normal completion closes it. These are internal changes and add
no worker process or deployment configuration.

For these bound work sessions, Shardline enables PostgreSQL's
[`client_connection_check_interval`](https://www.postgresql.org/docs/16/runtime-config-connection.html)
at one second when the setting exists and is disabled. An already configured
interval is preserved. This allows a query blocked on a metadata lock to notice
a closed client connection and release ownership. It does not set a query
timeout, and it does not bound detection of every network partition. Older
servers without the setting retain their existing disconnect-detection behavior.

An ordinary invocation carries one resolved command/configuration through
evidence production and the original handlers. Migration source and destination
configurations are captured together. The blocking-thread scope restores on
return or unwind; it does not change the configuration of another invocation.
Handlers do not reparse an environment file or reopen the selected configuration
to choose a different target after admission.

Keep large multipart state as bounded integrity snapshots rather than putting
unbounded part maps into StateChronicle's 64 KiB typed-profile payload boundary.
Checkpointed rebuilds must retain the maintenance barrier and a coherent scan
before cleanup; resumed GC must recheck live reachability, holds, quarantine
eligibility, and clock guards before each destructive action.

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
  behavior, while the evidence chain integrity-checks the policy row and its
  release/recovery boundaries.
- provider webhook delivery claims (`processed` and released retention),
  recorded as typed delivery snapshots. The delivery key remains the public
  idempotency boundary, while the evidence chain makes claims, deletion,
  purge, and re-processing after recovery verifiable.
- Hub repository refs (`main` and named refs), recorded as typed metadata
  snapshots. Ref updates and deletions use the same optimistic-concurrency
  boundary as before, while the evidence chain integrity-checks the materialized
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

## Library upload guarantees

Use `LocalBackend::upload_file` / `upload_xorb` or
`PostgresBackend::upload_file` / `upload_xorb` for storage-backed uploads. These
methods acquire maintenance protection automatically; callers need no separate
tracker or GC lock. PostgreSQL calls reuse a matching existing request owner and
reject detected ownership loss. Make a fresh request after that error.

Lower-level index and record-store methods operate on metadata. They cannot
check backing bytes without an object store. Their caller must hold the
appropriate ownership, validate physical references and retain protection
through publication. Supplying physical keys to a pending-delete check does
not prove that the keys are complete or that their bytes exist. Connection-bound
methods retain the supplied session; pool-based methods do not preserve a
separate owner held by the caller.

Shardline's HTTP upload path supplies that owner and performs its storage checks
before committing PostgreSQL publication. A successful saved maintenance result
does not certify current object presence or deployment health.
