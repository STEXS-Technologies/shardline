# Maintenance without another service

Start Shardline and run maintenance as you do today:

```bash
shardline serve
shardline fsck
shardline gc --mark --sweep
shardline repair
```

Tracking runs inside Shardline. There is no extra process, service, port, mount
or configuration for normal startup. Maintenance keeps its original process
and live output. Your existing timer still starts GC; starting the server does
not start GC.

**This branch is still under development and is not ready to merge.**
The version remains 1.11.3. The remaining upgrade limits are listed below.

## What this helps with in production

You can inspect recorded maintenance results after a node disappears. History
distinguishes completed work from failed or interrupted work. It does not
automatically rerun a job.

In upgraded Postgres/S3 deployments, server history lives in the existing
Postgres database and can be read by another node using that same database,
schema and object storage. It reuses the existing connection pool. Local
deployments without Postgres save new history in their existing metadata database.

Postgres provider callbacks save the delivery ID and repository changes
together. A failed transaction can be retried with the same delivery ID;
successful deliveries are deduplicated. The startup webhook encryption sweep
also preserves concurrent secret changes and stops if its work connection is
lost. Ordinary request handling continues if that sweep fails.

If optional tracking cannot save history, Shardline warns and preserves the
command's existing behavior. There is then no verified history result to rely on.
Recorded history does not prove that the cluster is healthy now or guarantee
cluster-wide exactly-once execution.

## Why a write might return a retryable error

If an earlier remote delete has an uncertain outcome, a later hold or covered
write to that object returns HTTP 503 with `Retry-After: 1`:

```text
object has an unresolved remote delete; retry after reconciliation
```

Other objects remain available. The retry interval does not promise recovery
in one second, and repeating the write does not clear the block.

If GC saved the successful delete reply, your next sweep or ordinary repair
can verify it and clear the matching pending delete:

```bash
shardline repair
```

Recovery does not send another user-object delete or resume the failed process.
Each invocation checks up to 64 saved replies and advances through the backlog.
If the reply is missing or invalid, the object stays blocked for investigation.
An object being absent is insufficient evidence to clear the block.

Saving a successful reply adds a receipt write and cleanup request in your
existing object storage. This can increase GC time. No recovery worker is needed.

## Where to look when something fails

Use your existing admin token:

| Endpoint | What it tells you |
| --- | --- |
| `/api/v1/status` | Server readiness. |
| `/api/v1/tasks/maintenance` | Recorded maintenance results. |
| `/api/v1/tasks/maintenance/$RUN_ID/report` | Saved counts for a server job, when available. |
| `/api/v1/tasks/maintenance/deletes` | Objects with pending deletes. |

A pending delete may still belong to a running GC. Check the original process
and logs. These endpoints inspect state; they do not retry or clear a delete.

A `completed` result describes that recorded run. A `failed` or
`needs_reconciliation` result needs investigation before repeating mutations.
For `running_or_interrupted`, check whether the original process is alive.
Treat `corrupt` history as untrusted. Older and interrupted jobs may have no
saved report. CLI commands retain their existing diagnostics.

See the [admin API guide](ADMIN_READ_API.md#get-apiv1tasksmaintenance)
for authentication, pagination and response fields.

## Upgrade and compatibility limits

The usual owner-run `shardline db migrate up` installs the optional metadata
and protection functions, including when no migration entries are pending.
Installation waits for current writes and maintenance. Runtime accounts need no
new fence-table grant. See the [database migration guide](DATABASE_MIGRATIONS.md).

New local and upgraded server history stays separate from user payload data.
CLI Postgres/S3 history, earlier object-backed history and storage-copy endpoints
still need that separation verified for mixed-version upgrades and rollback.
Jobs using a different database/schema from server startup also need storage
routing to their actual work target. Narrowly privileged accounts may retain
legacy tracking; shared history does not bypass row restrictions.

Shared history grouping reports `shared_metadata` when available and `legacy`
otherwise. Storage-copy jobs and the optional runner are not yet covered by
all of the shared-storage guarantees.

Covered Postgres writes reject publication after losing database ownership.
OCI failures preserve the previous published tag; incomplete manifests stay
hidden. Unreferenced bytes may remain for later maintenance. Released 1.11.3
servers can return a generic server error where updated servers explain the
pending-delete retry. Older GC sources, remaining write paths, row-restricted
cleanup, missing-delete replies and deployment-scale limits still have open
acceptance work. See [the acceptance evidence and remaining gates](MAINTENANCE_INTEGRATION_ACCEPTANCE.md).

## When to use a separate worker

The optional `shardline maintenance ...` runner uses a child process for
deadlines and CPU limits. Use it only when you need those controls.
Normal commands do not use it. Database ownership checks provide write safety;
the extra process provides isolation for those optional controls.
