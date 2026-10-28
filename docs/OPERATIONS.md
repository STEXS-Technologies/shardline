# Operations

This document describes the production operating model for Shardline.

For ordinary maintenance commands, recorded outcomes and what to check after
a crash, start with [Maintenance: same commands, clearer results](MAINTENANCE_INTEGRATION_UX.md).
Normal startup uses an embedded tracker; it requires no additional service.

Shardline is stateless in the request path when it is backed by durable external
services:

- immutable payload bytes live in the configured object-storage adapter
- durable reconstruction, lifecycle, and record metadata live in the configured index
  and record adapters
- reconstruction cache is optional acceleration, not the source of truth

That separation defines how to run Shardline safely, how to scale it, and how to recover
it.

## Request and download deadlines

Admission and handler work have a five-minute deadline through response headers.
After headers, a response producer is stopped if it supplies no nonempty data
for five minutes. This includes clients that stop reading and stalled backend
reads. Downloads that keep making progress can take longer than five minutes.

An expired response fails as an incomplete transfer; it does not become a
successful empty download. Clients should retry through their normal protocol.
Stopping the producer releases its resources, but bytes already handed to the
HTTP transport retain their buffer charges until their last owner drops them.
This is a response-producer deadline, not a TCP acknowledgement deadline.

## High Availability

Shardline availability depends on three layers:

1. stateless API and transfer processes
2. durable metadata storage
3. durable object storage

For production deployments:

- run at least two API replicas
- run at least two transfer replicas
- keep API and transfer behind path-aware routing
- use Postgres-compatible metadata storage with regular database backups
- use durable S3-compatible object storage with bucket versioning or provider-native
  recovery controls when available
- treat the reconstruction cache as disposable

The production Kubernetes package already follows that model:

- separate `api` and `transfer` Deployments
- separate Services, HPAs, and PDBs
- rolling updates with zero unavailable replicas
- topology spread across nodes
- externalized S3, Postgres, and Redis-cache dependencies

### Integrity across nodes

Replicas in one deployment must use the same PostgreSQL metadata schema and
payload bucket/prefix. Local scratch directories and caches can differ.
PostgreSQL failover must preserve acknowledged commits and prevent two writable
primaries; Shardline's locks cannot coordinate independent database histories.

Maintenance and guarded publication cannot reconnect after losing database
ownership and continue as the old owner. An uncertain remote delete blocks
conflicting publication with a retryable error until its exact attempt is reconciled.
Neither a process restart nor an elapsed timeout clears that protection.
Task history reports observed outcomes; it does not grant mutation ownership
or promise cluster-wide exactly-once execution. These protections run in the
existing processes and require no additional Shardline service.

During upgrades and rollback, keep the existing
[GC pause and owner-upgrade procedure](MAINTENANCE_INTEGRATION_UX.md#upgrade-using-the-existing-workflow):
older GC binaries do not create the new delete markers.

## Read-only operational API

External dashboards and operators can consume versioned JSON snapshots without
embedding a management UI in Shardline. Configure the dedicated admin-read
token and keep `/api/v1/*` on an internal endpoint. These routes report storage,
GC, integrity, node, task, replication, metrics, and future plugin state; they
never start or mutate an operation. See
[Read-Only Administration API](ADMIN_READ_API.md) for endpoint schemas,
authentication, and cluster/process consistency semantics.

## Scaling Model

Shardline scales by separating control-plane traffic from transfer traffic.

API pods handle:

- reconstruction planning
- provider token issuance
- provider webhook ingestion
- shard registration
- health, readiness, and metrics

Transfer pods handle:

- xorb upload
- chunk download
- ranged xorb transfer

Scale API and transfer independently.
Do not tie transfer replica count to token issuance rate, and do not tie API replica
count to large-object throughput.

## Backup Strategy

Back up the durable boundaries, not the cache.

Required backups:

- metadata database: native PostgreSQL backups, or native SQLite backups for
  each local metadata database as described in [Disaster Recovery](DISASTER_RECOVERY.md)
- provider catalog configuration and webhook secrets
- token-signing key material
- provider bootstrap API key
- object-storage bucket or equivalent immutable object namespace

Optional backups:

- local deployment root when running local record or local index adapters
- `shardline backup manifest` exports for object and metadata inventory comparison
- exported GC retention reports, orphan inventories, and repair reports

Not required:

- reconstruction cache contents
- pod-local `/tmp`
- pod-local service state in the scaled Kubernetes profile

### Database Backups

Take regular logical or physical Postgres backups.

The backup set must include:

- reconstruction metadata
- xorb presence and chunk-mapping metadata
- latest and immutable file records
- quarantine candidates
- retention holds
- processed webhook deliveries
- provider repository state

### Object Storage Backups

Shardline treats object storage as authoritative payload storage.

Operators should enable at least one of:

- provider-native bucket versioning
- provider-native replication
- snapshot-based volume protection in the backing object system
- an external bucket-to-bucket backup process

If object storage is lost, metadata alone is not enough to reconstruct payload bytes.

### Backup Manifest

Write an inventory manifest before and after maintenance windows:

```bash
cd /srv/assets
mkdir -p reports

shardline backup manifest \
  --output reports/backup-manifest.json
```

The manifest uses the configured object-store and metadata adapters.
It inventories object metadata and durable metadata counts without reading object
bodies.

## Restore Order

Restore durable state in this order:

1. object storage
2. metadata database
3. runtime secrets and provider catalog
4. Shardline API and transfer processes
5. cache layer

After restore:

1. run `shardline config check`
2. run `shardline fsck`
3. run `shardline index rebuild` if visible latest-record state may be stale or missing
4. run `shardline gc` in dry-run mode first if lifecycle metadata may have drifted
5. return API and transfer traffic only after the validation steps pass

## Operator Recovery

For scenario-by-scenario walkthroughs of storage loss, metadata loss, crash, and
cross-node recovery, see [Disaster Recovery](DISASTER_RECOVERY.md). To move a live
deployment between versions one role class at a time, see
[Rolling Upgrade](ROLLING_UPGRADE.md).

Use these commands for recovery and verification:

```bash
cd /srv/assets

shardline config check
shardline db migrate status
shardline fsck
shardline index rebuild
shardline repair
shardline repair lifecycle
shardline gc
```

Recommended recovery flow:

1. stop destructive maintenance jobs such as scheduled GC
2. restore durable storage and metadata
3. verify configuration, secrets, and schema state
4. run `fsck`
5. run `index rebuild` if latest-file state or dedupe-shard mappings may be incomplete
6. run lifecycle repair if webhook-delivery or provider repository state may have
   drifted
7. run GC in dry-run mode before re-enabling mark-and-sweep schedules

## Kubernetes Recovery Notes

In the scaled Kubernetes profile:

- pod-local Shardline state inside API and transfer pods is not the durable source of
  truth
- the durable boundaries are S3-compatible object storage, Postgres-compatible metadata,
  and Kubernetes Secrets for runtime key material
- the GC CronJob can be suspended during restore and resumed after verification

Suspend GC during incident recovery with:

```bash
kubectl patch cronjob shardline-gc -n shardline -p '{"spec":{"suspend":true}}'
```

Resume it after recovery with:

```bash
kubectl patch cronjob shardline-gc -n shardline -p '{"spec":{"suspend":false}}'
```

## Network Scoping

Ingress should stay path-scoped:

- API routes to API pods
- transfer routes to transfer pods
- `/metrics` stays internal
- `/readyz` stays probe-only

Egress should be scoped to the runtime dependencies Shardline actually needs:

- DNS
- Postgres-compatible metadata
- Redis-compatible cache
- S3-compatible object storage

The production Kubernetes package includes an environment-specific template for this:

`docs/k8s/production-scaled/networkpolicy-allow-runtime-egress.template.yaml`

Fill the placeholder CIDRs or replace them with namespace and pod selectors that match
your actual runtime dependencies before applying it.

## Release Checklist

Before calling a deployment production-ready:

- API and transfer role split is validated
- backups exist for metadata, object storage, and secrets
- restore order is documented and rehearsed
- the disaster-recovery runbook is rehearsed, including a restore drill
  ([Disaster Recovery](DISASTER_RECOVERY.md))
- the rolling-upgrade procedure is documented and rehearsed
  ([Rolling Upgrade](ROLLING_UPGRADE.md))
- `fsck`, `index rebuild`, lifecycle repair, and GC dry-run procedures are documented
- ingress and egress scope are explicit
- GC schedule and retention window match the operator recovery window
