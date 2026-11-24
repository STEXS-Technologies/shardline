# Recoverable metadata design

**Status:** accepted design; the Hub ref SHADOW foundation and signed
per-repository ref-state checkpoint path are integrated. No independent
recovery guarantee is enabled: the current candidate still relies on the R0
backup model. Archive mode defaults to `OFF`; no R1, R2, or R3 claim is
available until coverage, restore, and release evidence are complete.

This document records the architecture for the 1.14 migration. It follows
`SHARDLINE_SIMPLIFICATION_AND_RECOVERABLE_METADATA_RFC.md`, Parts II–V, and
keeps its recovery levels separate from the cleanup and range-read work.

## Authority and recovery levels

SQLite or PostgreSQL remains authoritative for current mutable state and for
the transaction that publishes it. Existing verified content addressed storage
remains authoritative for immutable payload bytes. A separately durable archive
will record enough verified metadata and committed transitions to restore a
certified history. The archive is not another live database and cannot create
publication visibility by itself.

| Level | Intended guarantee | Minimum evidence |
|---|---|---|
| R0 | Restore from a matched database and payload backup; rebuild derived indexes | Existing backup, rebuild, and fsck procedures |
| R1 | Restore covered committed immutable versions and byte exact reads without a metadata backup | Complete version manifests, externally durable receipts, verified payload closure, isolated restore |
| R2 | Restore certified mutable state such as refs, deletions, holds, and policy identifiers | R1 plus complete mutation coverage, a certified checkpoint, contiguous ordered replay, and restore time policy validation |
| R3 | Protected success is returned only after its committed mutation receipt is durable outside SQL | R2 plus all writer paths integrated, bounded archive failure behavior, and predecessor writer fencing |

The current checkpoint implementation is one signed snapshot of one
repository's refs at an archived SQL watermark. It is a useful state baseline,
but does not certify complete mutable state, immutable-version coverage, or a
restorable R1/R2 archive.

The existing `index rebuild` repairs derived indexes; it does not create
authoritative file mappings from a CAS inventory. A manifest describes
immutable metadata but does not prove that SQL ever committed or exposed it.
Only a receipt derived from an outbox row that committed in the same SQL
transaction as publication can prove that transition. A hash chain kept only
in the metadata database is lost with that database.

## Write, receipt, and restore boundaries

1. Validate authorization, protocol identities, and payload bytes using the
   existing publication path.
2. Create immutable version metadata from the existing verified records and
   publication state. Preserve the full reconstruction plan, including ordered
   Xet member ranges and offsets, file identity algorithm, logical sizes, Git
   tree and commit identities, and parent identities where the domain needs
   them. Empty files are valid versions.
3. In the owning SQL transaction, perform the current CAS/ref mutation and add
   one domain ordered outbox event. A multi ref operation remains one atomic
   event. Its sequence is allocated under the same transaction and domain lock;
   a global SQL sequence is not a commit order.
4. Commit SQL. Only now may a bounded worker turn the outbox event into an
   external receipt. The worker performs archive I/O after the SQL transaction
   has released its locks. It verifies readback or an equivalent immutable
   put result and marks the row archived only after verifying the exact bytes.
5. In `SHADOW`, legacy acknowledgments remain backup dependent and lag is
   observable. In `ENFORCED`, the caller receives protected success only after
   a durable receipt is verified. A timeout or uncertain put returns a
   retryable failure; it never silently falls back to a protected success.

The unavoidable interval between SQL commit and receipt persistence means SQL
may expose a mutation before its archive receipt exists. SHADOW does not close
that recovery window. R3 applies to protected acknowledgments, not to every
transient read of SQL visible state.

Restore starts in an isolated, non serving database. It verifies deployment
identity, archive format and provenance; verifies the complete checkpoint
inventory and payload closure; imports checkpoint state; replays only contiguous
per domain receipts to their certified high water marks; rebuilds derived
indexes with existing rebuild code; and runs fsck against stored bytes. Missing
predecessors, conflicting identities, missing tombstones, or unresolved
cross domain transactions stop replay. Restore denies access until current
provider and identity policy has been revalidated. It never selects a ref from
timestamps or infers publication from object listing.

## Record integrity and namespace

Archive objects use a dedicated versioned deployment namespace. Records use a
strict deterministic encoding with a format version, bounded sizes and item
counts, and rejection of unknown mandatory versions or fields. Object names
include a digest of canonical bytes. On create conflict the writer reads and
verifies the existing bytes; it rejects different content under the same
identity. Receipts are stored with conditional create or an equivalently
verified immutable operation. Restore verifies digest, length, deployment,
domain, sequence, and authenticated provenance before trusting a record.

Shardline has a deployment supplied Ed25519 signing path for StateChronicle
reliability commitments in `shardline-reliability`. Hub ref receipts and
per-repository ref-state checkpoints now emit signed archive records using
these primitives. This authenticates those bounded records; it does not make
the SQL reliability journal externally durable, certify every writer, or
establish an R1/R2 restore point. Signing keys must live outside the database
being recovered. No secrets, raw credentials, or unfiltered customer metadata
belong in archive records. Key rotation must retain verification material until
all certificates using an old key have expired from retention.

The archive backend must support immutable conditional writes (or a verified
equivalent), bounded verified reads, deterministic inventory or a certified
inventory object, and credentials separated from ordinary GC and delete
permissions. Existing `ObjectStore` abstractions are the first choice. A new
archive database or a mutable object store `main` pointer is out of scope.
Archive placement beside CAS protects against metadata loss only; it does not
protect against simultaneous loss of both stores.

## Domain order and coverage

Sequence and receipt identity are scoped to an authority domain. Initial
integration begins with Hub repository ref batches: all ref changes in one
SQL transaction share one sequence and operation identity. OCI tags, repository
creation/deletion, S3 listing state, LFS locks, Bazel metadata, provider policy,
retention, quarantine, and GC outcomes are separate writer paths. Until each
relevant path writes and has passed restore verification, it remains listed as
uncovered.

Coverage reports are inventories of declared writers, outbox writers, and
restore verified writers. A declaration or a boolean flag cannot qualify a
recovery level. R1 requires artifacts and isolated byte exact restoration. R2
requires complete mutation coverage and certified state replay. R3 additionally
requires a protected acknowledgment gate and proof that every supported writer
uses it.

Current coverage is therefore:

| Domain | Outbox transaction | External receipt | Restore verified | Claim |
|---|---:|---:|---:|---|
| Hub ref batches | SQLite/PostgreSQL same-transaction outbox, OFF by default; SHADOW only | Linked signed receipts are verified against a trusted genesis or predecessor; the worker verifies external readback before marking the exact event archived. SQLite/PG outbox tests and receipt-chain tests pass. | No | None |
| Hub version publications (NDJSON and Git receive-pack) | Manifest prepared before revision visibility; locator is pinned to the matching committed Hub ref event in the same SQL publication | Focused tests cover manifest serialization, PostgreSQL prepared-manifest atomicity, and receive-pack manifest linkage; destructive restore and payload-closure verification are not implemented | No | None |
| OCI, S3, LFS, Bazel, provider policy, lifecycle/GC | No | No | No | None |

The signed `HubRefStateCheckpointV1` format and `shardline backup hub-checkpoint`
operator command add a per-repository current-ref snapshot. The source is read
from one consistent SQLite or PostgreSQL snapshot and must have equal committed
and archived watermarks. For a nonzero watermark, the writer verifies the
SQL-pinned terminal receipt, signs the snapshot, writes an immutable archive
object, and verifies its readback. The command reports the pinned object key,
byte length, and BLAKE3 digest only after that verification. It requires Hub
recovery to already be in SHADOW mode and does not change SQL mode or write SQL
state.

The checkpoint contains current refs and the receipt watermark for that one
repository. It does not enumerate the repository catalog, cover deleted
repositories or other mutation domains, carry immutable-version payload
closure, verify the complete receipt chain before the terminal receipt, or
prove freshness by itself. Retain the emitted locator, digest, byte length,
deployment, repository, and archived sequence outside the replaceable archive;
the signature alone cannot distinguish the newest checkpoint from an older
valid one.

## GC, deletion, and archive retention

Until certified coverage exists, the archive cannot justify deleting payloads.
Existing SQL roots, active holds, quarantine, staging protection, maintenance
barriers, and unresolved remote delete fences remain authoritative. Once a
certified R2/R3 baseline exists, GC must preserve its payload closure and
uncheckpointed receipts in addition to current SQL roots. A deletion has its
own ordered tombstone; old manifests cannot resurrect deleted data or access.
Archive compaction requires a newer verified checkpoint, settled delete state,
no applicable hold, and an audited retention decision. Erasure obligations
remove active recovery roots and must not leave forbidden payloads in an
accessible checkpoint.

## Migration sequence and release gate

The implementation sequence is R1, then R2, then R3:

1. Freeze this authority model, recovery formats, trust boundaries, and explicit
   domain coverage.
2. Add immutable manifests based on actual verified Shardline publication
   records and verify storage readback. The initial implementation covers
   NDJSON Hub publications only; other publication paths remain uncovered.
3. Add additive SQLite and PostgreSQL outbox migrations and insert a domain
   sequenced event in the owner publication transaction. This is implemented
   for Hub ref batches only.
4. Add an ordered bounded worker, retries, and idempotence. The SHADOW worker
   is integrated for Hub refs; status and backpressure remain outstanding.
5. Publish signed state-only checkpoints for individual repositories only
   after their SQL and archive watermarks match. The CLI path is implemented.
   A resumable catalog backfill and proof of complete inventory are still
   required before a full R2 baseline can be certified.
6. Restore to a fresh isolated database, replay contiguous receipts, invoke
   existing rebuild and fsck, then verify actual bytes without consulting the
   source database or its backup.
7. Extend mutation coverage and archive aware GC roots. Only then add explicit
   ENFORCED mode and reject unsupported writers.
8. Qualify migration and rollback with released N-1 binaries, real SQLite and
   PostgreSQL, local and MinIO archive storage, client protocols, failures,
   deletion, policy revalidation, and isolated disaster recovery.

Do not enable ENFORCED while any writer in the declared scope is uncovered.
Do not advertise R1/R2/R3, zero metadata RPO, or automatic failover before the
matching destructive restore and release gates pass. Existing installations
remain on R0 until these gates are met. The released schema history and
checksums are immutable; new behavior uses forward additive migrations.

The integrated Hub slice uses an ordered SQL outbox, signed linked receipts, a
bounded background worker, and immutable version manifests for NDJSON
publications. The worker pages a bounded set of pending repositories, handles
one next event per repository per pass, verifies archive bytes and predecessor
identity, and marks an event only after verification. This is SHADOW evidence
capture: an SQL commit can still precede its archive receipt, so normal
publication acknowledgments remain backup dependent.

The sequence orders only mutations observed while SHADOW remains continuously
enabled. Turning SHADOW off after an outbox event is allowed, but re-enabling
SHADOW is rejected once history exists; a new certified baseline/epoch is not
implemented. Hub records have no independent tenant field, so the tenant slot
uses deployment identity and repository IDs remain opaque. Coverage is limited
to Hub ref batches and version metadata from NDJSON and Git receive-pack. The
per-repository signed ref checkpoint is not a catalog backfill or a full-domain
checkpoint. There is no resumable backfill, complete archive inventory/status
API, restore reader, or destructive restore qualification. These constraints
prevent this slice from qualifying R1/R2/R3.

## Failure cases that must remain fail closed

- Crash before SQL commit: no outbox means no receipt; a prepared manifest is
  not visible state.
- Crash after SQL commit and before archive persistence: keep the event pending;
  SHADOW remains backup dependent and ENFORCED cannot return protected success.
- Lost archive response: verify the exact stored object before marking archived;
  an operation identity with different bytes is a conflict.
- Receipt gap or reordered listing: stop at the last verified contiguous cut.
- Wrong deployment or domain: reject before importing private metadata.
- Missing or corrupt payload: fsck/recovery reports incomplete recovery; an
  SQL row or client supplied digest cannot stand in for verified content.
- Revoked identity or missing policy: restore read denied until revalidated.
- Archive outage: retries and queues are bounded and observable; archive I/O
  never extends an SQL lock.

## Evidence required before each claim

Preserve a frozen source SHA, binary hashes, database and object backend
topology, archive identity, client versions, dataset identity, exact commands,
raw logs, and passed/failed/skipped counts. Conditional skips do not count as
backend qualification. R1 requires destroying both the disposable metadata DB
and its backup while retaining archive and CAS, then verifying exact restored
reads. R2 adds mutable state, deletion, retention, policy, and gap cases. R3
adds real protected acknowledgment failures, queue bounds, GC overlap, and
old-writer exclusion. Each supported backend and protocol needs its own
evidence; one successful Hub ref transaction cannot establish broad coverage.
