# Database Migrations

Shardline ships its Postgres metadata schema with the binary.

Use `shardline db migrate` to apply, inspect, or revert the bundled schema migrations.
That keeps the operational path consistent across local, Docker, and Kubernetes
deployments and avoids hand-running SQL files in the wrong order.

## Commands

Apply all pending migrations:

```bash
export SHARDLINE_INDEX_POSTGRES_URL='postgres://shardline:replace-me@postgres:5432/shardline'

shardline db migrate up
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

## Behavior

Shardline records migration history inside the metadata database and verifies that
already-applied migrations still match the SQL bundled in the running binary.

If the database contains:

- a migration version unknown to the running binary, or
- a checksum for a known migration that no longer matches the bundled SQL

the command fails closed instead of guessing.

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
