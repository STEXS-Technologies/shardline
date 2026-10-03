# Database Migrations

Shardline ships its Postgres and local SQLite metadata schemas with the binary.

Use `shardline db migrate` to apply, inspect, or revert the bundled Postgres schema
migrations. Local SQLite migrations are applied explicitly with `local-up`.
Server startup only checks compatibility and never upgrades a stale database.

## Commands

Apply all pending migrations:

```bash
export SHARDLINE_INDEX_POSTGRES_URL='postgres://shardline:replace-me@postgres:5432/shardline'

shardline db migrate up
```

Apply local SQLite migrations for an explicitly selected deployment root:

```bash
shardline db migrate local-up --root /var/lib/shardline
```

Apply only the next migration steps:

```bash
shardline db migrate up --steps 2
```

Inspect migration state:

```bash
shardline db migrate status
```

`status` and `verify` use read-only queries and do not acquire the migration
advisory lock or create the history table. They can run on a read-only connection.
If the history table is absent, `status` reports all bundled migrations as pending;
`verify` reports that migration initialization is required. Use `up` to initialize
the database explicitly.

Revert the newest migration:

```bash
shardline db migrate down
```

Revert more than one applied migration:

```bash
shardline db migrate down --steps 2
```

Override the database URL for one command without changing the deployment environment:

```bash
shardline db migrate status \
  --database-url 'postgres://shardline:replace-me@postgres:5432/shardline'
```

Verify every persisted reliability journal without modifying it:

```bash
shardline db migrate verify
```

Backfill one bounded batch of missing reliability baselines and StateChronicle
Merkle commitments. Repeat this command until the deployment's maintenance
monitor reports no remaining work:

```bash
shardline db migrate backfill --batch-size 256
```

The batch size must be positive and fit PostgreSQL's `BIGINT` row-limit type.
Invalid batch sizes are rejected before connecting to the database.

Repair one known-corrupt reliability operation only after validating its
authoritative materialized row. The confirmation flag is required because the
selected evidence chain is discarded and rebuilt:

```bash
shardline db migrate repair \
  --operation-kind S3Object \
  --operation-id '<exact-operation-id>' \
  --confirm
```

`verify` and `fsck` fail closed on malformed, mismatched, or tampered evidence.
Neither command silently establishes a new baseline. `repair` is the explicit
operator recovery action and is serialized with other database maintenance.

## Behavior

Shardline records migration history inside the metadata database and verifies that
already-applied migrations still match the SQL bundled in the running binary.

If the database contains:

- a migration version unknown to the running binary, or
- a checksum for a known migration that no longer matches the bundled SQL

the command fails closed instead of guessing.

Before starting, a Postgres-backed server performs the same read-only check. A
stale schema produces an explicit error instructing the operator to run
`shardline db migrate up`. A local SQLite deployment uses the same policy for an
existing database; run `shardline db migrate local-up --root <root>` to apply its
pending migrations.

Each migration step runs inside its own transaction.
A failed step does not mark itself applied.
Mutating migration commands also hold one Postgres advisory lock for the full command,
so concurrent jobs serialize instead of both selecting and applying the same pending
step. If a process or database connection dies, Postgres releases that lock and the
step transaction rolls back; rerunning `up` resumes from the last committed step.

## PostgreSQL Index Builds During Patch Upgrades

A patch release can require a write maintenance window even when its schema changes
are additive. These migrations create ordinary PostgreSQL indexes:

| Migration | Table | Index |
| --- | --- | --- |
| `20261002010000` | `shardline_hub_repos` | `shardline_hub_repos_search_prefix_idx` |
| `20261002020000` | `shardline_hub_file_entries` | `shardline_hub_file_entries_page_idx` |
| `20261002030000` | `shardline_s3_objects` | `shardline_s3_objects_scope_key_c_idx` |
| `20261003000000` | `shardline_tree_entries` | `shardline_tree_entries_prefix_pattern_idx` |
| `20261003010000` | `shardline_webhook_deliveries` | `shardline_webhook_deliveries_retention_idx` |

The webhook retention index contains only the timestamp in PostgreSQL, whose
purge query filters on that column and locks heap rows. SQLite includes the
provider, owner, repository and delivery ID after the timestamp to cover its
bounded keyset pages. Keeping the PostgreSQL index narrow also permits migration
of wider schema-valid legacy receipts; those rows can exceed the current public
webhook component limits.

PostgreSQL's ordinary index build permits reads but blocks inserts, updates, and
deletes on the indexed table. Build time depends on table size and available CPU,
I/O, and memory; rehearse the upgrade against a representative restored database
before scheduling the window. See the official [PostgreSQL CREATE INDEX
documentation](https://www.postgresql.org/docs/current/sql-createindex.html).

Shardline executes each bundled migration and its history update in one transaction.
The index-build lock remains until that transaction completes; PostgreSQL describes
this lock lifetime in [Explicit
Locking](https://www.postgresql.org/docs/current/explicit-locking.html).
`CREATE INDEX CONCURRENTLY` requires execution outside a transaction block, so adding
that keyword to these bundled migrations is incompatible with the current runner.
The migration advisory lock serializes migration commands; it does not drain normal
application writers.

For a deployment where these migrations are pending, use this controlled procedure
before starting the process rollout:

1. Confirm `shardline db migrate status` and take the native database/object-store
   backups described in [Disaster Recovery](DISASTER_RECOVERY.md).
2. Schedule a write maintenance window that covers the rehearsed build time. Stop
   admitting mutating requests on every API and transfer replica; pause provider
   webhook delivery, ingestion workers, and scheduled jobs that write metadata.
   Drain in-flight writes and open write transactions, including independent database
   clients, before running the migration. Existing replicas may continue serving
   reads if their schema compatibility and routing allow it.
3. Run `shardline db migrate up` once with the new release binary. Keep admission
   closed until the command exits successfully and `shardline db migrate status`
   reports the pending steps applied. If it fails, keep writes drained, resolve the
   failure, and rerun `up`; completed steps remain committed and a failed step rolls
   back.
4. Continue the [rolling-upgrade procedure](ROLLING_UPGRADE.md), verify readiness,
   and restore write admission according to its mixed-version routing constraints.
   Resume paused workers and scheduled jobs only after those constraints are met.

Use the same window for a Kubernetes migration Job on an existing deployment; a Job
finishing before new pods start does not itself stop old replicas from writing.

## Kubernetes

Use the same command in migration jobs:

```yaml
args: ["db", "migrate", "up"]
```

That keeps the cluster bootstrap path aligned with local and Docker deployments.
