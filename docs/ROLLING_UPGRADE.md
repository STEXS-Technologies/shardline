# Rolling Upgrade

This document describes a rolling process upgrade: one role class at a time,
verifying readiness after each step. Pending schema migrations can require a
controlled write maintenance window before that rollout.

Shardline processes are stateless in the request path. The durable boundaries are the
object-store adapter and the index and record adapters, which are external to the
process. A live upgrade also requires compatible schemas and write behavior
between the overlapping versions. Drain each process, restart it on the new
version, and confirm readiness before replacing the next process; readiness
alone does not establish mixed-version compatibility.

On Unix, both SIGTERM (the usual container stop signal) and SIGINT start the
server's graceful connection drain. By default the server waits for active
connections to finish; allow enough runtime termination grace for those transfers.
If an embedding application sets `ServerConfig::with_shutdown_timeout`, give the
runtime a longer grace period so the server's deadline runs before SIGKILL.

Process-level predecessor tests hold an authorized LFS upload body open until
the real 1.12.1 process receives SIGTERM. They check that the listener closes
while the process stays alive, finish the body, require a successful upload
response and clean process exit, then read the bytes through the candidate.
The same sequence on the candidate verifies both files through 1.12.1 after
rollback, before any upload retry. These checks pass on SQLite/filesystem and
PostgreSQL/MinIO. They verify an admitted transfer's drain and durable handoff;
they do not automate load-balancer routing or prove an entire fleet has stopped.

## Protocol frontend upgrade from 1.12.1

### Recommended coordinated upgrade

For deployments using several frontends or embedded backend readers, use a brief
coordinated cutover rather than assuming every new feature works on old replicas:

1. Back up metadata and object storage together and pause destructive maintenance.
2. Stop admitting new writes, drain active transfers and stop all old backend readers.
3. Run the owner migration command described below with the new binary.
4. Start the upgraded API and transfer replicas, verify readiness and read existing
   files through the frontends you use before resuming writes.
5. Resume traffic and maintenance after every backend reader is upgraded.

Existing data stays in place; no reupload is required. This procedure includes a
write pause and requires deployment-level routing/draining. Shardline does not
automatically discover or retire every old process. Read the rollback boundaries
below before accepting new writes; the passing predecessor tests qualify their
named workflows rather than unrestricted rollback of new features.

### Native Xet reader rollout

Existing uploads do not need to be uploaded again. The new server reads existing
Xorbs and legacy standalone chunks. Keep those legacy objects during upgrade.

The storage-layout transition concerns servers and embedded applications that
open the storage backend directly. An HTTP client does not open those objects;
the serving Shardline process interprets the layout. Updating servers does not
itself require a storage-format migration in those clients.

Use the following compatibility boundaries when planning the rollout:

| Reader and data | Qualification |
| --- | --- |
| New server reading existing 1.12.1 uploads | Tested with real released-binary fixtures; no reupload required. |
| 1.12.1 native CAS HTTP reader reading candidate-created Xorbs | Tested during simultaneous old/new publication and after rollback, with local storage and PostgreSQL/MinIO. |
| 1.12.1 Hub routes serving new native SHA-256 identity features | Not qualified; keep these requests on upgraded replicas. |
| Old embedded application reading standalone chunks directly | Not covered by native CAS HTTP tests; it may require chunk copies that new uploads omit. |

The native CAS result is a specific supported workflow, not evidence that every
old frontend or embedded reader understands new publications. Existing HTTP
clients can continue using the upgraded server; new server-side features do not
become available on an old server simply because the client is unchanged.

New native uploads omit the redundant standalone chunk copies. An old server or
embedded backend reader that requires those copies may not read that new data.
Until mixed-version compatibility is explicitly qualified for your workflow,
pause native uploads, drain the old processes, upgrade every backend reader, and
then resume uploads. Keep upgraded readers available after new writes begin.

Restarting an old binary does not recreate the omitted chunk objects. The
predecessor drills below cover their named workflows, not unrestricted rollback
of every new storage layout. Automated transition enforcement remains under
qualification; readiness alone does not prove that all old readers have stopped.

### Schema installation and protocol routing

The unreleased frontend changes add durable Xet file identities and aliases,
LFS locks, S3 multipart part counts, record membership, verified Xorb descriptors,
and Hub object locations. An owner-run command installs these additive
tables in a separate frontend schema registry. The released core migration
ledger remains compatible with 1.12.1; unknown core migrations still cause
startup to fail. Existing object records are not rewritten. Part counts are
known only for multipart objects completed by the new version; legacy records
and rows changed by an older binary report the count as unknown.

Back up metadata and object storage together. Pause destructive maintenance
during rollout. With the upgraded binary, run `shardline db migrate up` as the
PostgreSQL schema owner, or `shardline db migrate local-up --root <root>` for
local SQLite. PostgreSQL installation carries forward the existing table grant
policy for the new tables; runtime accounts do not need DDL permission.
Shardline does not yet migrate deployments that use row-level security on either
permission source table. Migration stops because those policies cannot safely
be copied to the new table shapes. Do not disable the policies to force the
upgrade; this deployment needs a policy-aware migration before rollout. Startup
also fails closed if source-table row security is enabled after installation.
Then replace processes one at a time, draining each process and checking readiness.

PostgreSQL frontend installation takes the existing exclusive maintenance write
barrier before changing tables or triggers. Active protected writes finish first;
later protected writes wait until the schema transaction commits or rolls back.
Allow for this pause in request timeout budgets. Do not run schema installation
as if it were a background operation with no effect on write latency.

Real 1.12.1/new-binary drills cover live S3-frontend writes during installation,
cross-version reads, new-version writes, and restarting 1.12.1 against the
upgraded metadata. These checks pass with SQLite and PostgreSQL. Database
triggers invalidate derived multipart-count rows when an older binary changes
the authoritative S3 object row; this preserves object access and prevents a
stale part count from being reported. They establish this storage path's tested
rolling compatibility, not every protocol's mixed-version behavior. During the
mixed-version period, route requests that use the new LFS lock or native Xet
identity behavior only to upgraded replicas. If the load balancer cannot route
those requests, hold them until every replica is upgraded. A 1.12.1 replica can
still read and write the tested object data, but it does not understand the new
lock or identity records.

Immutable Hub/Xet read credentials and their signed range URLs also require
upgraded API and transfer replicas. Keep those requests on upgraded replicas
until the rollout is complete. The latest predecessor drills install all seven
frontend migrations, preserve the released core ledger, and verify old/new
OCI and S3 access on PostgreSQL. Local drills migrate release-created SQLite
data and compare clean FSCK results from both binaries at concurrency 1, 32 and
128. These checks cover installation and existing storage compatibility; they
do not yet qualify native Hub/Xet identity replacement through mixed-version
clients or every deployment's grants and security policies.

Native-CAS drills use the actual 1.12.1 binary with both SQLite/filesystem and
PostgreSQL/MinIO. They upgrade existing files, run old and new servers together,
and upload the same bytes with different chunk layouts through both versions.
Both readers verify the resulting bytes. After GC, descriptor rebuild, rollback
and another upgrade, all six tested native file identities remain readable and
the candidate retains its historical SHA-256 aliases. Candidate SHA-256 reads
also reject access from another repository.

Version 1.12.1 does not create the newer SHA-256 identity bindings. An identical
retry through the candidate verifies an old publication and adds that binding
without losing earlier aliases. Keep requests using the new identity behavior
on upgraded replicas. These drills exercise simultaneous HTTP upload requests;
they do not qualify every publication ordering, failure timing or Hub client.
Stock Hub 1.25.1, 2.2.0 and 2.3.0 clients each pass a history drill against the
actual 1.12.1 binary and the candidate, on local storage and PostgreSQL/MinIO.
The native-upload history drill also passes with Hub 1.25.1/hf_xet 1.5.2 and
Hub 2.2.0/hf_xet 1.7.0 against the optimized candidate executable.
It creates a private repository before migration, replaces its file after
migration, writes again after rollback, and verifies all three byte-exact
versions after re-upgrade with fresh client caches.
The candidate additionally accepts a 9 MiB native Xet Hub upload. Destructive
GC with only the Hub frontend selected preserves that historical file before
rollback and after re-upgrade, including after the old binary adds a regular
file commit. The stock clients verify the complete native bytes after each
candidate-side GC. This does not claim the old Hub download route understands
the new native identity protocol.
With each of these client versions, the same local and PostgreSQL/MinIO drills also stop the server
and run candidate FSCK, index rebuild, and FSCK again before rollback and after
re-upgrade. Each command inspects native storage records and reports zero
findings; fresh-client reads then verify the regular history and native bytes.
This checks healthy-data rebuild preservation, not recovery from corrupted Hub
metadata or concurrent publication during rebuild.

A separate actual-process Git drill kills the candidate after it commits an
atomic two-branch push, before the client receives the result. On SQLite/local
and PostgreSQL/MinIO, restart preserves both branch heads; a retry succeeds and
a fresh clone has exact bytes and passes `git fsck --strict --full`. This covers
that publication boundary, not every possible crash point.
The PostgreSQL/MinIO drill also kills the process while the second ref is
blocked inside the publication transaction, after the first ref changed there.
Rollback leaves refs, revisions and reliability evidence unchanged; the same
push succeeds after restart.

Hub revision IDs have a rollout boundary: 1.12.1 does not understand the new
public Git IDs returned in SDK commit fields. It returns 404 for those IDs,
although the corresponding content remains readable by `main` and the legacy
internal revision ID from `/api/models/{namespace}/{repo}/revisions`.
Retain that listing before rollback if you need historical reads with 1.12.1.
During a mixed-version rollout, keep requests using new public Git IDs on
upgraded Hub replicas. Old saved revision IDs remain accepted after upgrading.
This drill does not qualify concurrent mixed-version Hub publication, maximum-size
native histories, repair interruption, aged-retention campaigns or signed URL rollout.

The alias migration preserves current mappings and future replacements. It
cannot recover mappings already overwritten before the upgrade.

S3 checksum and standard object HTTP metadata writes need the same routing care.
Their records are stored in the S3 object's private metadata. A 1.12.1 replica
can read the object bytes, but does not know the newer private keys are internal;
it may expose them as ordinary `x-amz-meta-*` metadata and omit the corresponding
checksum, content type, cache, disposition, encoding, expiry or website redirect
headers. During a mixed-version rollout, keep reads and writes for objects using
these properties on upgraded replicas, or wait until all replicas are upgraded
before enabling them. This does not alter object bytes, but old replicas return
different S3 metadata and response headers.

S3 object tags use another reserved private metadata entry bound by the same
object evidence. A 1.12.1 replica can still read the object's bytes, but does
not understand the tag entry and may expose it as user metadata. An older
same-key write or copy can also discard the tags. During a mixed-version
rollout, route tagged-object reads and mutations to upgraded replicas; if that
routing is unavailable, wait until all replicas are upgraded before using
object tagging. Existing object bytes and evidence formats remain unchanged,
but tag visibility and preservation are not seamless across old and new writers.

Restarting an old binary does not require dropping the additive tables or
restoring an old backup. Keep those tables: they contain persistent identities
and locks for upgraded replicas. Do not run downgrade SQL blindly or restore an
old backup over newer writes. Qualify protocol-specific rollback before rollout.

Restoring that backup recovers its contents, including its matching object
storage, but not writes acknowledged after the backup. Keep the upgraded
deployment intact until those newer writes have a recovery plan. The local
released-binary drill checks an isolated backup restoration separately from
the upgraded deployment; it does not establish a lossless downgrade.

## StateChronicle/Penelope 0.2.0 dependency upgrade

For tracking commands and recovery, see the
[maintenance guide](MAINTENANCE_INTEGRATION_UX.md).

Keep the existing server commands, maintenance commands and GC timer. Tracking
runs in their existing process, with normal maintenance output streamed live.
It needs no new service, port, mount, environment variable or signing key.
Private temporary files use the existing writable root or host temporary storage.

Run the usual owner command, `shardline db migrate up`, to install the optional
history and protection functions, even if the migration ledger has no pending
entries. Runtime accounts need no new fence-table grant. See the
[database migration guide](DATABASE_MIGRATIONS.md).

The dependencies preserve the evidence formats written by 1.11.3, including
existing journal identities, digests and signatures. New validation checks those
records without rewriting them. Recorded task history supplements the existing
metadata journals and safety barriers; it does not replace them or automatically
repeat interrupted work.

Follow the role-by-role drain and readiness procedure below. The integration
can reject an affected write while an earlier remote delete remains unresolved;
see [maintenance without another service](MAINTENANCE_INTEGRATION_UX.md) for the
retryable error and recovery steps. Earlier-version upgrades must still follow
the schema, session, writer-routing, GC and repair restrictions below.

For this integration's upgrade from released 1.11.3:

1. Pause scheduled destructive GC and wait for existing GC commands to finish.
   Normal server traffic can remain available for this integration when the
   released schema is already current. If other schema migrations are pending,
   follow their write-maintenance requirements first. An older GC binary does not create the
   new remote-delete markers, so installing the new server cannot protect a
   delete performed by that binary.
2. Run the usual owner-run `shardline db migrate up` using the upgraded binary
   and the same database/schema configuration as the deployment.
   Let it finish all pending migrations. A step-limited upgrade that leaves
   migrations pending does not install the new protection functions.
3. Roll the server roles using the drain and readiness checks below. Replace
   the binaries or images used by existing maintenance jobs as well.
4. Inspect pending deletes with `shardline history --pending-deletes` using the
   same configuration. Read `supported` and `inspection_notice`; unavailable
   inspection is inconclusive. Investigate interrupted work before resuming the
   existing destructive GC schedule. Saved successful replies can be recovered
   by ordinary `shardline repair`; missing replies remain blocked. No extra
   tracker service or schedule is required.

Use this checklist for an already-current 1.11.3 deployment. The sections below
also describe upgrades from earlier releases: apply their schema and format
restrictions when your starting version lacks the corresponding behavior.
Keep the destructive GC pause above throughout this integration's rollout.

If server rollback is necessary, pause destructive GC during the rollback and
keep uncertain deletes for reconciliation. Released servers can report a generic
error for a protected pending delete. Returning maintenance jobs to an older GC
binary also returns to its older safety behavior; it does not retain the new
delete guarantees. This dependency integration adds no migration-ledger entries
requiring `db migrate down`; do not remove protection state to bypass an error.

The optional Linux `shardline maintenance` runner supplies process isolation for
deadlines and CPU limits. Its existing jobs keep their original state directory,
signing key, request, deployment, executable and deadline. A new binary can verify
and fence an old job, but cannot execute its older pinned definition. Reboot
expires unfinished jobs; moving live job state across hosts or PID namespaces is
unsupported. Inspect uncertain effects before intentionally starting a new run.
See [durable maintenance](CLI.md#durable-maintenance-linux) for this optional workflow.
Task history and PID observations grant no permission to retry or change data.

## Roles And Upgrade Units

`shardline serve` pins a process to a role:

- `shardline serve --role api` — control-plane traffic: reconstruction planning,
  provider token issuance, provider webhook ingestion, shard registration, health,
  readiness, and metrics
- `shardline serve --role transfer` — data-plane traffic: xorb upload, chunk
  download, ranged xorb transfer
- `shardline serve --role all` — both roles in one process; the default when
  `SHARDLINE_SERVER_ROLE` is unset

The role split only partitions the frontend set across processes; it does not select
different protocols (see [Deployment](DEPLOYMENT.md#runtime-shape)).

The upgrade unit is a role class, not an individual process:

- in the [Production Scaled](DEPLOYMENT.md#production-scaled) profile, `api` and
  `transfer` are separate Deployments; each Deployment is one role class
- in the [Local Single-Node](DEPLOYMENT.md#local-single-node) profile there is a
  single `all`-role process; the procedure reduces to one drain, restart, and
  readiness check

## Recommended Order

Upgrade one role class at a time, and verify the class is fully ready before starting
the next. Recommended order: `transfer` first, then `api`.

Why:

- while one class is mid-rollout, the other stays at the previous version and keeps
  serving, so there is always a known-good version to roll back to
- transfer carries the long-lived, high-throughput upload and download connections;
  upgrading it first means the data plane moves to the new version before the control
  plane changes, and API token issuance, webhook ingestion, and reconstruction
  planning are unaffected throughout the transfer rollout
- API handles readiness for the deployment; upgrading it last means readiness
  semantics change only after the data plane is already validated on the new version

Do not upgrade both classes concurrently. If the new version contains a metadata
schema change, apply it before the process rollout with `shardline db migrate up`
(see [Database Migrations](DATABASE_MIGRATIONS.md)); the schema must remain readable
by the previous version's processes for the duration of the rollout. Evidence-bound
writes are intentionally gated until all writers are upgraded.

The PostgreSQL index migrations `20261002010000`, `20261002020000`,
`20261002030000`, `20261003000000`, and `20261003010000` use ordinary transactional index builds and
block writes on their indexed tables. When any is pending, drain writers across the deployment and apply
the migrations in the [index-build maintenance
window](DATABASE_MIGRATIONS.md#postgresql-index-builds-during-patch-upgrades) before
rolling either role class. The process rollout order alone does not provide this
write drain.

The OCI tag-index migration is additive and old processes continue to read their
object-store tag pointers. During the API-class rollout, drain OCI manifest `PUT` and
`DELETE` traffic, OCI blob deletion, and provider webhook traffic, or route those
mutations exclusively to the new API version. Reads
remain available and legacy tags are imported lazily. Resume OCI mutations after all
API replicas run the new version; this avoids two software versions using different
authoritative tag-pointer stores or bypassing repository-scoped locks during the brief
mixed-version window.

The same routing constraint applies to every evidence-bound logical resource when
the older writers lack the evidence or authoritative-store behavior introduced by
the upgrade. Do not let such N-1 writers overwrite an S3 object, OCI tag, Hub ref,
or resumable session after N has written it. N-1 may read it, but same-key writes
must be drained or routed to N until the rollout is complete; testing a different
key is not evidence that same-key compatibility exists. The format-preserving
dependency upgrade from 1.11.3 described above does not introduce this restriction.

The durable resumable-session migration is also additive, but session formats are not
shared across versions. Before replacing the API class, stop admitting new Git LFS
PATCH, OCI blob-upload, and S3 multipart sessions to the old replicas and allow their
in-flight sessions to finish or expire. Keep the legacy RWX mount attached only while
old API replicas remain. New replicas persist new sessions in Postgres/object storage;
after every API replica is upgraded and legacy sessions are drained, the RWX claim can
be removed permanently. Do not route one legacy session between old and new replicas.

When upgrading from a version without the shared/exclusive writer barrier,
suspend destructive GC (`--mark` and/or `--sweep`) for the entire mixed-version
rollout. Those older servers do not participate in the barrier. Dry-run GC remains
safe. Resume scheduled destructive GC only after every API and transfer replica
runs the barrier-aware version. The integration's GC pause above also applies
when upgrading from 1.11.3, because older GC does not create the new delete markers.

When upgrading from a version without future-object retention semantics, pause
scheduled lifecycle repair and full repair during rollout, and upgrade maintenance
binaries before resuming these jobs. Those older repair binaries can delete
unexpired finite holds for objects that have not arrived yet, and older fsck can
reject valid future-object holds. Released 1.11.3 already preserves these holds.

## Procedure

The example uses the Production Scaled profile (`kubectl`). For systemd or host-native
processes, replace the drain and restart steps with the equivalent service control
(`systemctl stop`, replace the binary, `systemctl start`).

### 1. Snapshot Before Upgrade

```bash
cd /srv/assets
mkdir -p reports

shardline backup manifest \
  --output reports/pre-upgrade-manifest.json

shardline db migrate status
```

Also take coordinated native metadata and object-store backups; the manifest
does not copy bytes or create an atomic snapshot. See [Backup](BACKUP.md).
Keep the previous image or binary available, subject to the rollback restrictions
below.

### 2. Upgrade The Transfer Class

Set the target image for the transfer server and its config-check init container.
Use the same immutable image digest that was used for the owner-run migration.
Changing the image starts the rolling replacement; `rollout restart` alone would
reuse the currently configured image.

The Deployment must drain terminating replicas while routing new traffic to ready
peers. Set the runtime termination grace long enough for active transfers.

```bash
SHARDLINE_UPGRADE_IMAGE='registry.example.com/shardline@sha256:REPLACE_WITH_IMAGE_DIGEST'
kubectl -n shardline set image deployment/shardline-transfer \
  shardline="$SHARDLINE_UPGRADE_IMAGE" config-check="$SHARDLINE_UPGRADE_IMAGE"
kubectl -n shardline rollout status deployment/shardline-transfer --timeout=10m
```

Apply the same image change to your managed manifests or GitOps configuration so
the controller does not restore the previous image. Keep destructive GC suspended;
update its job image before resuming the schedule after both role classes finish.

With a single transfer replica, the restart is a brief availability dip for transfer
traffic. The API class keeps serving throughout.

Confirm the upgraded replica is up. The following probe examples assume a
service-local connection or port-forward to that replica on port 8080; probing a
load-balanced Service can hit a different replica.

```bash
curl -fsS http://127.0.0.1:8080/healthz
```

Confirm readiness. `/readyz` returns JSON; verify the role and backends:

```bash
curl -fsS http://127.0.0.1:8080/readyz
```

```json
{
  "server_role": "transfer",
  "server_frontends": ["xet"],
  "object_backend": "s3",
  "cache_backend": "memory"
}
```

The transfer class is upgraded only when `/healthz` answers `200` and `/readyz`
reports the expected `server_role`, `object_backend`, and `cache_backend`.

### 3. Upgrade The API Class

Repeat the same steps for the API Deployment:

```bash
kubectl -n shardline set image deployment/shardline-api \
  shardline="$SHARDLINE_UPGRADE_IMAGE" config-check="$SHARDLINE_UPGRADE_IMAGE"
kubectl -n shardline rollout status deployment/shardline-api --timeout=10m

curl -fsS http://127.0.0.1:8080/healthz
curl -fsS http://127.0.0.1:8080/readyz
```

`/readyz` must report `server_role: "api"` with the expected backends before the
upgrade is considered complete.

For a single-node `--role all` deployment, run the same drain, restart, and readiness
checks on the single process.

### 4. Post-Upgrade Verification

```bash
cd /srv/assets

shardline fsck

shardline backup manifest \
  --output reports/post-upgrade-manifest.json
```

Compare the post-upgrade manifest counts and byte totals with
`reports/pre-upgrade-manifest.json`. Return full traffic after `fsck` exits `0`.

## Readiness Polling

- `GET /healthz` — liveness; always open and answers `200` while the process is up
- `GET /readyz` — readiness; reports the index store and required object-store
  operations are available, with fields `server_role`, `server_frontends`,
  `object_backend`, and `cache_backend`

Poll `/readyz` in a loop until it reports the expected role and backends, or fails
the rollback condition below. `/readyz` is for probes and service-local diagnostics,
not public ingress (see [Deployment](DEPLOYMENT.md#health-checks)).

## Rollback

To revert one class to the previous version:

1. restart that class with the previous image tag or binary (drain first, exactly as
   in the upgrade steps)
2. poll `/healthz` then `/readyz` until the class reports ready
3. verify with `shardline fsck` and the backup manifest

If a schema migration was part of the upgrade and the downgrade needs the previous
schema, revert it with `shardline db migrate down` before restarting the old
processes (see [Database Migrations](DATABASE_MIGRATIONS.md)). Because only one class
is mid-rollout at any time, rollback always has a known-good version to return to.

## Automated Evidence

`e2e/rolling_upgrade_e2e.rs` replaces each role independently, polls `/healthz` and
`/readyz`, and confirms the other role keeps serving. Role separation itself is covered
by `e2e/role_split_e2e.rs`.

The `mixed-version-upgrade` reliability job adds the binary compatibility evidence that
an in-process test cannot provide. It automatically selects the greatest preceding
SemVer tag reachable from the current commit, builds that release and the current commit
separately, starts two N-1 processes against shared Postgres and S3, then performs:

```text
N-1 + N-1 -> N + N-1 -> N + N -> N-1 + N
```

When both binaries support the same schema, the drill keeps one old node serving
while replacing its peer. Both versions write and read each other's exact bytes;
rollback also retains compatible writes. The new node's startup task outcome is
verified through the existing authenticated admin API.

When the new release adds migrations, the drill drains old nodes before applying
them. It then checks exact historical bytes and verifies that an older binary
rejects startup against an unknown migration. The transcript and exact N/N-1
commit identities are retained as CI artifacts.
This evidence applies to the tested adjacent release pair; every release reruns the
automatically advanced pair rather than assuming transitive compatibility.
