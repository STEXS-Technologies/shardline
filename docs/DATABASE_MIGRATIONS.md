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

## Kubernetes

Use the same command in migration jobs:

```yaml
args: ["db", "migrate", "up"]
```

That keeps the cluster bootstrap path aligned with local and Docker deployments.
