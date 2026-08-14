# Disaster Recovery

This runbook covers operator-driven recovery from storage loss, metadata loss, and
crash events. It assumes the durable-state model from
[Operations](OPERATIONS.md): immutable payload bytes live in the configured
object-store adapter, and reconstruction, lifecycle, and record metadata live in the
configured index and record adapters. The reconstruction cache is disposable and is
never restored.

Start with the [Decision Table](#decision-table). Pick the row that matches the loss,
run the primary tool, then run the verification commands before returning traffic.

## Decision Table

| Loss or failure | Primary tool | Also run | Reference |
| --- | --- | --- | --- |
| Object bytes lost, copy available (replica, export, second store) | `storage migrate` from the copy | `backup manifest`, `fsck` | [Storage Migration](STORAGE_MIGRATION.md), [Backup Manifest](BACKUP.md) |
| Object bytes lost, no copy | provider-native restore (versioning, replication, snapshot) | `fsck` to quantify, `gc --mark` dry-run | [Backup Manifest](BACKUP.md), [Fsck](FSCK.md) |
| Metadata lost (SQLite or Postgres) | restore metadata database from backup; `db migrate status` | `index rebuild`, `fsck`, `repair lifecycle` | [Database Migrations](DATABASE_MIGRATIONS.md), [Index Rebuild](INDEX_REBUILD.md) |
| Whole node lost (object + metadata) | `REPOSITORY_BOOTSTRAP` bring-up, then restore both stores | `backup manifest`, `fsck`, `index rebuild` | [Repository Bootstrap](REPOSITORY_BOOTSTRAP.md) |
| Node crash mid-upload | `fsck` then `repair lifecycle` to reconcile | `gc --mark` orphan inventory, `backup manifest` | [Fsck](FSCK.md), [Lifecycle Repair](LIFECYCLE_REPAIR.md) |
| Lifecycle metadata drift after an incident | `repair lifecycle` | `gc --mark` dry-run before any sweep | [Lifecycle Repair](LIFECYCLE_REPAIR.md), [Garbage Collection](GARBAGE_COLLECTION.md) |
| Integrity doubt after any restore | `fsck` | `backup manifest` before and after | [Fsck](FSCK.md), [Backup Manifest](BACKUP.md) |

The restore order in [Operations](OPERATIONS.md#restore-order) applies to every
scenario: object storage first, metadata database second, runtime secrets and
provider catalog third, then API and transfer processes, and the cache layer last.

## Scenario A: Full Storage Loss On One Node

A node that hosted both local object storage and local SQLite metadata is gone.

**Symptoms**

- the node does not answer `/healthz` or `/readyz`
- failover routing cannot reach a healthy replica on the node
- `shardline fsck` on the replacement root reports missing objects

**Impact**

All durable state the node held is unavailable. Payload bytes and metadata are both
affected; neither can be served from memory or cache.

**Recovery**

1. Confirm the loss and stop destructive maintenance jobs such as scheduled GC:
   suspend the GC CronJob, or stop the `gc schedule` entries (see
   [Operations](OPERATIONS.md#kubernetes-recovery-notes)).
2. Bring up a replacement node. Use the configuration bring-up path from
   [Repository Bootstrap](REPOSITORY_BOOTSTRAP.md) to restore the provider catalog,
   token-signing key material, and bootstrap API key.
3. Restore object bytes. If the object store is backed up by provider-native
   versioning or replication, restore from that. If another copy of the same payload
   data exists (for example an S3 replica), migrate it into the local root:

   ```bash
   cd /srv/assets
   shardline config check

   export SHARDLINE_MIGRATE_FROM_S3_BUCKET=asset-cas
   export SHARDLINE_MIGRATE_FROM_S3_REGION=us-east-1
   export SHARDLINE_MIGRATE_FROM_S3_ENDPOINT=https://s3.example.com
   export SHARDLINE_MIGRATE_FROM_S3_ACCESS_KEY_ID_FILE=/run/secrets/shardline/s3-access-key-id
   export SHARDLINE_MIGRATE_FROM_S3_SECRET_ACCESS_KEY_FILE=/run/secrets/shardline/s3-secret-access-key

   shardline storage migrate \
     --from s3 \
     --to local \
     --to-root /srv/assets/.shardline/data \
     --dry-run

   shardline storage migrate \
     --from s3 \
     --to local \
     --to-root /srv/assets/.shardline/data
   ```

   See [Storage Migration](STORAGE_MIGRATION.md#s3-compatible-endpoints) for the full
   environment-variable list.
4. Restore metadata from the database backup into the local state root (or into
   Postgres, then verify with `shardline db migrate status`). Do not rebuild from
   nothing: `index rebuild` reconstructs derived state from immutable version records,
   it does not create version records from object bytes.
5. Return traffic only after the verification steps below pass.

**Verification**

```bash
cd /srv/assets

shardline backup manifest \
  --output reports/post-restore-manifest.json

shardline fsck
shardline index rebuild
shardline repair lifecycle
shardline gc --mark \
  --retention-report reports/gc-retention.json \
  --orphan-inventory reports/gc-orphans.json
```

Compare the post-restore manifest counts and byte totals with the pre-loss manifest.
`fsck` must exit `0`. Review the GC orphan inventory before any sweep.

**Prevention**

- run at least two API and two transfer replicas on separate nodes
- keep provider-native versioning, replication, or snapshots enabled on the object
  store
- take regular database backups and write a
  [backup manifest](BACKUP.md) before and after maintenance windows

## Scenario B: Metadata Loss (SQLite Or Postgres)

The metadata database is corrupted, truncated, or deleted while object bytes remain
intact.

**Symptoms**

- `fsck` reports `missing_version_record` or `mismatched_version_record`
- latest-file state is empty or stale after `index rebuild`
- `/readyz` fails on the index-store readiness check

**Impact**

Metadata is the only source of the file-to-object mapping. Object bytes still exist,
but reconstruction and record metadata describe what those bytes are. Losing metadata
without a backup means the immutable payload bytes cannot be mapped back to files;
restore the most recent backup before anything else.

**Recovery**

1. Restore the metadata database from the database backup. For SQLite, place the
   restored file at `.shardline/data/metadata.sqlite3` in the state root. For Postgres,
   restore the database, then verify the schema matches the running binary:

   ```bash
   export SHARDLINE_INDEX_POSTGRES_URL='postgres://shardline:replace-me@postgres:5432/shardline'

   shardline db migrate status
   shardline db migrate up
   ```

   See [Database Migrations](DATABASE_MIGRATIONS.md) for the fail-closed behavior when
   versions or checksums do not match.
2. Rebuild derived latest-file state from the restored immutable version records:

   ```bash
   shardline index rebuild
   ```

   `index rebuild` exits `0` when completed without non-fatal issues, `1` when invalid
   version records were found, and `2` on operational failure (see
   [Index Rebuild](INDEX_REBUILD.md)).
3. Reconcile lifecycle metadata that may reference the restored object graph:

   ```bash
   shardline fsck
   shardline repair lifecycle
   ```

4. Run GC in dry-run mode before re-enabling any mark-and-sweep schedule.

**Verification**

- `fsck` exits `0` with no `missing_*` or `*_mismatch` issues
- `index rebuild` reports `issue_count: 0`
- `backup manifest` object and metadata counts match the pre-loss manifest

**Prevention**

- take regular logical or physical database backups (see
  [Operations](OPERATIONS.md#database-backups))
- keep the `backup manifest` inventory so metadata loss is detected early
- run `shardline db migrate status` in CI or deployment jobs so schema drift is caught
  before it becomes an incident

## Scenario C: Object-Store (S3) Loss

Objects in the S3-compatible store are deleted, versioned out, or the bucket is
destroyed while metadata survives.

**Symptoms**

- downloads fail with missing-object errors
- `fsck` reports `missing_chunk`, `chunk_hash_mismatch`, `missing_dedupe_shard_object`,
  or `missing_reconstruction_xorb`
- reconstruction lookups time out or 404

**Impact**

Metadata alone is not enough to reconstruct payload bytes. Every file version whose
bytes are gone becomes unservable until the objects are restored.

**Recovery**

1. Stop traffic and suspend GC immediately. A sweep must never run against a partially
   restored object store.
2. Restore the bucket from provider-native versioning, replication, or snapshot
   controls (see [Operations](OPERATIONS.md#object-storage-backups)). Prefer restoring
   in place so object keys are unchanged.
3. If a second copy exists in another store or local root, migrate it in:

   ```bash
   export SHARDLINE_MIGRATE_FROM_S3_BUCKET=asset-cas-replica
   export SHARDLINE_MIGRATE_FROM_S3_REGION=us-east-1
   export SHARDLINE_MIGRATE_FROM_S3_ENDPOINT=https://s3.example.com

   shardline storage migrate \
     --from s3 \
     --to local \
     --to-root /srv/assets/.shardline/data \
     --dry-run

   shardline storage migrate \
     --from s3 \
     --to local \
     --to-root /srv/assets/.shardline/data
   ```

4. After the object store is consistent, reconcile metadata with the object graph:

   ```bash
   shardline fsck
   shardline index rebuild
   shardline repair lifecycle
   ```

5. Only after `fsck` is clean, run `gc --mark` in dry-run mode to enumerate any
   genuinely orphaned objects, review the orphan inventory, then re-enable the GC
   schedule.

**Verification**

- `fsck` exits `0`
- `backup manifest` object count and byte total match the pre-loss manifest
- a test reconstruction and ranged transfer succeed end-to-end

**Prevention**

- enable provider-native bucket versioning, replication, or snapshot-based volume
  protection (see [Operations](OPERATIONS.md#object-storage-backups))
- keep a second copy of payload data in a different store or local root when the cost
  is acceptable
- write a `backup manifest` before and after any object-store maintenance

## Scenario D: Node Crash Mid-Upload

A transfer node dies while an upload is in progress.

**Symptoms**

- the client sees a connection reset or a timeout
- `/healthz` for the node stops answering
- `fsck` reports stray references or `gc --mark` later reports orphaned objects

**Impact**

This is a crash-recovery case, not a data-loss case. The durable boundaries are safe:

- object-store objects written before the crash are immutable. Writes are idempotent
  and a same-key different-bytes write fails closed, so a partially acknowledged upload
  cannot silently corrupt an existing object (see
  [Storage Migration](STORAGE_MIGRATION.md#safety-model)).
- durable version records already committed before the crash survive.
- WAL or pending metadata from the interrupted upload is simply absent; the object
  bytes for that upload may be present without any committed metadata.

The upload is retried by the client. Any object written but never referenced by
committed metadata is an orphan that GC will collect after the retention window.

**Recovery**

1. Restart the node and let failover routing re-balance. No restore of object storage
   or metadata is required.
2. Reconcile the metadata and object graph:

   ```bash
   shardline fsck
   shardline repair lifecycle

   shardline gc --mark \
     --retention-report reports/gc-retention.json \
     --orphan-inventory reports/gc-orphans.json
   ```

   `fsck` identifies partial references; `repair lifecycle` prunes stale quarantine,
   hold, and webhook-delivery claims; `gc --mark` inventories objects not reachable
   from live metadata — the aborted-upload orphans.
3. Review the orphan inventory. Sweep only objects older than the retention window:

   ```bash
   shardline gc --sweep
   ```

4. Write a post-incident manifest:

   ```bash
   shardline backup manifest \
     --output reports/post-incident-manifest.json
   ```

**Verification**

- `fsck` exits `0`
- the retried upload completes and the new version is immediately fetchable
- `gc --mark` reports no live object as an orphan

**Prevention**

- keep the GC retention window long enough to cover in-flight uploads
- keep a pre-upload `backup manifest` so aborted-upload orphans are distinguishable
  from real drift
- rely on the object-store adapter's idempotent, fail-closed write behavior; do not
  hand-delete objects during an upload incident

## Scenario E: Cross-Node Recovery

The deployment must move to a different node or set of nodes (hardware retirement,
rack failure, migration to a new host).

**Symptoms**

- the planned move of durable state to new hardware
- routing already fails over, but the new node serves no data

**Impact**

Both durable stores must move with the deployment. The cache is disposable and does
not move.

**Recovery**

1. Stand up the new node configuration first via
   [Repository Bootstrap](REPOSITORY_BOOTSTRAP.md): provider catalog, token-signing
   key, bootstrap API key.
2. Move object bytes. Copy the local object-store directories below the old state root
   into the new root, or use `storage migrate`:

   ```bash
   shardline storage migrate \
     --from local \
     --from-root /srv/old-node/.shardline/data \
     --to local \
     --to-root /srv/assets/.shardline/data \
     --dry-run

   shardline storage migrate \
     --from local \
     --from-root /srv/old-node/.shardline/data \
     --to local \
     --to-root /srv/assets/.shardline/data
   ```

   If the new node uses S3 instead, use `--to s3` with the
   `SHARDLINE_MIGRATE_TO_S3_*` environment variables
   ([Storage Migration](STORAGE_MIGRATION.md#s3-compatible-endpoints)).
3. Move metadata. Copy `metadata.sqlite3` into the new state root, or point the new
   deployment at the same Postgres and verify with `shardline db migrate status`.
4. Start the new servers and verify before re-pointing routing:

   ```bash
   cd /srv/assets

   shardline config check
   shardline backup manifest \
     --output reports/post-move-manifest.json
   shardline fsck
   shardline index rebuild
   shardline repair lifecycle
   ```

5. Cut routing to the new node only after `fsck` exits `0` and the manifest counts
   match the pre-move manifest.

**Verification**

- `fsck` exits `0` on the new root
- `backup manifest` object count, byte total, and metadata counts match the old node
- a full upload and a historical version fetch succeed from the new node

**Prevention**

- keep the pre-move `backup manifest` and a database backup at the old node until the
  new node passes verification
- rehearse the move in a staging environment with the same storage representations
- keep token-signing key material in a secrets store so the new node can mint
  identical-scope tokens immediately

## Related Reading

- [Operations](OPERATIONS.md) — restore order, backup strategy, Kubernetes recovery
- [Backup Manifest](BACKUP.md) — inventory manifests for restore comparison
- [Storage Migration](STORAGE_MIGRATION.md) — moving payload bytes between stores
- [Index Rebuild](INDEX_REBUILD.md) — reconstructing derived latest-file state
- [Database Migrations](DATABASE_MIGRATIONS.md) — schema bring-up and verification
- [Fsck](FSCK.md) — integrity verification after any restore
- [Lifecycle Repair](LIFECYCLE_REPAIR.md) — pruning stale lifecycle metadata
- [Garbage Collection](GARBAGE_COLLECTION.md) — mark-and-sweep after incidents
- [Repository Bootstrap](REPOSITORY_BOOTSTRAP.md) — new-node configuration bring-up
