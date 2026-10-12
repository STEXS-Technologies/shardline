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
together, so retrying a successful delivery does not repeat its changes.
Covered writes stop after losing database ownership.

If optional tracking cannot save history, Shardline warns and preserves the
command's existing behavior. There is then no verified history result to rely on.
Automatic tracking does not add history objects to your payload storage when
metadata history is unavailable, including before the database owner upgrade.
Existing legacy history remains readable.
Tracking warnings go to stderr. A missing or rejected receipt does not change
the command's stdout or exit status and never causes its work to run again.
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

A completed result describes that run. Investigate failed or interrupted work
before repeating mutations; treat corrupt history as untrusted. Older and
interrupted jobs may have no saved report. CLI diagnostics remain available.

See the [admin API guide](ADMIN_READ_API.md#get-apiv1tasksmaintenance)
for authentication, pagination and response fields.

## Upgrade and compatibility limits

The usual owner-run `shardline db migrate up` installs the optional metadata
and protection functions, including when no migration entries are pending.
Installation waits for current writes and maintenance. Runtime accounts need no
new fence-table grant. See the [database migration guide](DATABASE_MIGRATIONS.md).

New local and upgraded server history stays separate from user payload data.
Ordinary Postgres GC, rebuild and repair also use metadata history when the
account supports it. Normal maintenance moves verified, finished legacy history
into metadata in bounded batches, preserving its signed bytes. Interrupted,
corrupt or unrelated history stays in place. Storage copy keeps its source
read-only; it does not require new source write permissions.
Copies to S3-only endpoints keep task history in private temporary storage on
the command's host, without adding objects or permissions to the payload bucket.
That history is not available through other nodes and can be removed by
temporary-directory cleanup. A copy to local storage keeps history in its
destination's SQLite metadata.
Different work databases or schemas, some legacy history
and narrowly privileged accounts still need compatibility work. Older GC,
remaining write paths, missing delete replies and deployment-scale behavior also
have open checks. Released 1.11.3 servers may return a generic error for a pending
delete. See [the acceptance evidence and remaining gates](MAINTENANCE_INTEGRATION_ACCEPTANCE.md)
before planning a rolling upgrade or rollback.

## When to use a separate worker

The optional `shardline maintenance ...` runner uses a child process for
deadlines and CPU limits. Use it only when you need those controls.
Normal commands do not use it. Database ownership checks provide write safety;
the extra process provides isolation for those optional controls.
