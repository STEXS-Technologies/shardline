# Garbage Collection

Garbage collection is required.

Deduplication lowers storage growth, but it does not remove unreachable xorbs, shards,
or file reconstruction records on its own.

Without garbage collection, Shardline becomes a write-only archive.

## What Garbage Collection Means in Shardline

Shardline garbage collection is the process of identifying unreachable immutable objects
and deleting them without breaking any repository that can still reference them.

The collector is responsible for:

- finding live repository roots
- marking reachable reconstruction records
- marking reachable chunk and xorb objects
- sweeping only objects that stayed unreachable across a retention window

## Why It Matters

Garbage collection is not only a cost feature.
It is part of durability and operator trust.

Operators need to know:

- storage growth stays bounded
- deleted repositories can eventually release space
- interrupted cleanup does not corrupt live data
- self-hosted deployments can recover and rebuild collector state

## Root Sets

Shardline must mark reachability from explicit roots.

Those roots include:

- file versions still referenced by active repository revisions
- latest file-version mappings still visible to clients
- provider-reported repository roots
- administrative retention holds
- repair and verification jobs that temporarily pin objects

No object should be deleted merely because one metadata path disappeared.
Deletion must require the object to be unreachable from every live root set.

Derived metadata is not a root set.
In particular, dedupe-shard mappings are rebuildable index state, so they must not keep
retained-shard objects alive on their own after the live file graph no longer references
the chunk content they describe.

## Mark and Sweep Model

Shardline should use a two-phase model:

1. Mark Walk live roots and record every reachable reconstruction, chunk, xorb, and
   retained shard.
2. Sweep Delete only objects that remain unmarked after the configured grace period.

The grace period protects against:

- delayed provider webhooks
- temporary provider API outages
- replication lag in index storage
- interrupted repository updates

## Safety Rules

Garbage collection must obey these invariants:

- never delete an object still referenced by a committed visible reconstruction
- never make a visible file version unreadable
- never rely on provider webhooks as the only source of truth
- never require full object rewrites to repair collector metadata
- every deletion decision must be reproducible from durable state

## Quarantine and Retention

Deletion should be staged:

- unreferenced objects enter a garbage candidate state
- candidates remain recoverable during the retention window
- physical deletion happens only after the retention window expires

This gives operators time to:

- recover from provider integration mistakes
- re-import missed references
- restore accidentally removed repository roots

## Self-Hosted Recovery

Self-hosted operators must be able to recover collector state.

That means Shardline needs:

- index rebuild tooling
- object inventory tooling
- `fsck` to validate reachability assumptions
- a collector dry-run mode before destructive deletion

If Shardline crashes during garbage collection, restarting it must not require manual
database surgery.

## Provider Integration and Deletion

Repository deletion on GitHub, GitLab, or Gitea should not immediately delete bytes.

Provider events should instead:

- remove repository roots from the active root set
- start the retention timer
- allow a later sweep to reclaim bytes after the grace window

This keeps provider mistakes or transient event loss from causing irreversible data
loss.

The current server already uses repository-deletion webhooks this way.
A supported provider deletion event creates retention holds for the deleted repository's
referenced chunk and serialized-xorb objects, including the unpacked chunk objects
referenced by native Xet xorbs, and removes the repository's latest and immutable
version metadata from the live root set, which keeps those bytes out of sweep until the
hold window expires.

## Storage Adapter Requirements

To support garbage collection, storage adapters must provide:

- object delete
- object metadata lookup
- object listing or manifest replay support
- safe idempotent deletion behavior

Object deletion is a storage concern.
Reachability is a coordinator and index concern.

## Operational Expectations

Operators need these collector modes:

- dry run
- mark only
- sweep only
- full mark and sweep
- retention report
- orphan inventory export

Garbage collection is successful only when it is understandable and reversible enough
for self-hosted operators to trust it.

Shardline now also ships a helper for non-Kubernetes scheduled execution:

```bash
shardline gc schedule install \
  --output-dir ./systemd \
  --env-file /etc/shardline/shardline.env \
  --user shardline \
  --group shardline
shardline gc schedule uninstall --output-dir ./systemd
```

That helper writes or removes a matching `.service` and `.timer` pair for the
`shardline gc` command.
It is meant for systemd-based hosts that are not using the Kubernetes CronJob package.

The install helper validates the generated schedule inputs before it writes anything:

- the `shardline` binary path resolves to a real file
- the environment file exists
- `SHARDLINE_ROOT_DIR` from the environment file is honored as the working directory
  unless `--working-directory` explicitly overrides it
- referenced secret and provider-config files exist
- the selected service user and group exist on the host

Its default timer is `*-*-* 03:17:00`, and the default retention window is `86400`
seconds. Override them with `--calendar` and `--retention-seconds`.

## Collector Shape

Shardline ships a CLI-triggered collector.
It is not a background worker or daemon.
Operators run it explicitly with `shardline gc`, or schedule that command with an
external scheduler such as cron, systemd timers, or a Kubernetes CronJob.

The collector reads file-record, quarantine, and retention-hold metadata through the
configured adapters, and inventories/deletes payload objects through the configured
object-storage adapter:

- local deployments use local record, quarantine, and hold state under `.shardline/data`
  by default
- Postgres-backed deployments use Postgres file-record, quarantine, and hold tables when
  `SHARDLINE_INDEX_POSTGRES_URL` is set
- local object storage uses the filesystem object adapter
- S3-compatible object storage uses the S3 object adapter when S3 configuration is set

`--root` does not mean "local object storage only."
It is an override for the CLI's Shardline state root:

- with local record and index adapters, it points at the directory that holds
  `metadata.sqlite3` plus the local object root and any remaining migration-era state
- with S3-compatible object storage and local record/index adapters, it still points at
  the local metadata root while payload objects are inventoried and deleted through S3
- with Postgres-backed record and index adapters, the collector reads metadata from
  Postgres and uses the configured object-storage adapter while `--root` still selects
  the active state root for configuration and any local state

For interactive use, run the command from the project or deployment directory.
Shardline uses the nearest `.shardline/data` state root automatically, or creates that
default under the current directory for new local deployments.
Use `--root` only when operating on a different state root.

The `gc` command works across the supported adapter combinations above.

The collector supports four operator modes:

```bash
cd /srv/assets
mkdir -p reports

shardline gc
shardline gc --mark
shardline gc --sweep
shardline gc --mark --sweep
```

Optional retention can be set with:

```bash
shardline gc --mark --retention-seconds 86400
```

Operator artifacts can also be exported as JSON:

```bash
shardline gc --mark \
  --retention-report reports/gc-retention.json \
  --orphan-inventory reports/gc-orphans.json
```

Example: S3 object storage with Postgres-backed metadata still uses the same command
shape, but the adapter choice comes from environment/config rather than the `--root`
flag:

```bash
SHARDLINE_OBJECT_STORAGE_ADAPTER=s3 \
SHARDLINE_S3_BUCKET=asset-cas \
SHARDLINE_S3_REGION=us-east-1 \
SHARDLINE_S3_ENDPOINT=https://s3.example.com \
SHARDLINE_S3_ACCESS_KEY_ID=<access-key> \
SHARDLINE_S3_SECRET_ACCESS_KEY=<secret-key> \
SHARDLINE_INDEX_POSTGRES_URL=postgres://user:password@db.example.com:5432/shardline \
shardline gc --mark --sweep
```

Administrative retention holds can be managed with:

```bash
shardline hold set \
  --object-key de/de00000000000000000000000000000000000000000000000000000000000000 \
  --reason "provider deletion grace" \
  --ttl-seconds 86400
shardline hold list --active-only
shardline hold release \
  --object-key de/de00000000000000000000000000000000000000000000000000000000000000
```

Mode behavior:

- default mode is a dry run
- `--mark` records current orphan chunks as quarantine candidates
- `--sweep` deletes only quarantine candidates whose retention window already expired
- `--mark --sweep` performs both steps in one run

New quarantine candidates default to a retention window of `86400` seconds.
That default applies only when a run includes `--mark`.

Local deployments store quarantine state under:

```text
<root>/gc/quarantine/
```

Each candidate stores:

- the chunk hash
- the observed byte length
- when the chunk first became unreachable
- when it becomes eligible for deletion

Postgres-backed deployments store the same quarantine data in
`shardline_quarantine_candidates`.

Local deployments store retention holds under:

```text
<root>/gc/retention-holds/
```

Postgres-backed deployments store the same hold data in `shardline_retention_holds`.

The collector:

- inventories managed chunk, serialized-xorb, and retained-shard objects through the
  object storage adapter
- scans file-record metadata through the configured record-store adapter
- derives live chunk reachability from chunk-backed records and from currently
  referenced native Xet xorb contents
- computes the current live managed-object set
- reports orphan managed objects under `chunks/`
- treats dedupe-shard mappings as derived metadata instead of independent GC roots
- records durable quarantine manifests for unreachable managed objects through the index
  adapter
- releases quarantine manifests when a chunk becomes reachable again
- excludes objects protected by active retention holds from orphan accounting
- prunes expired time-bounded retention holds from durable metadata during mutating GC
  runs
- releases stale quarantine state when an active hold protects the same object
- deletes only expired quarantine candidates during sweep
- exports active retention windows as machine-readable JSON when requested
- exports current orphan inventory as machine-readable JSON when requested
- prunes empty chunk and quarantine prefix directories after sweep

The current lifecycle integration also:

- accepts GitHub, GitLab, Gitea, and generic normalized repository-deletion webhooks
  through the provider endpoint
- writes retention holds for the deleted repository's chunk and serialized-xorb objects
  through the configured index adapter
- keeps those objects out of quarantine and sweep while the deletion grace window is
  active

The retention report contains one entry per active quarantine candidate, including:

- chunk hash
- object-store key
- observed object length
- first-seen timestamp
- delete-after timestamp
- whether the retention window is already expired
- seconds remaining until deletion eligibility

The orphan inventory contains one entry per currently orphaned object, including:

- chunk hash
- object-store key
- observed object length
- whether the object is only untracked or already quarantined
- first-seen and delete-after timestamps when quarantine state exists

These exports are intended for lifecycle automation, reviewable batch-deletion
workflows, and operator dashboards.

Immediate deletion for local testing is still possible:

```bash
shardline gc --mark --sweep --retention-seconds 0
```

Current non-goals:

- provider root reconciliation
- automatic hold creation from external policy engines
- an embedded background collector worker
