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

## What users gain

Maintenance history survives a process failure. You can distinguish completed
work from failed or interrupted work without setting up another service.
History does not automatically rerun jobs or prove current cluster health.

Upgraded Postgres deployments share history through their existing database.
Another node using the same database, schema and storage can inspect it.
Local deployments save history in their existing SQLite metadata.

The production benefits are:

- Postgres provider callbacks save the delivery ID and repository changes
  together, so retrying a successful delivery does not repeat those changes.
- Native OCI and S3 uploads reject publication after losing database ownership.
  An old process cannot use a replacement connection to finish that publication.
- Retention holds and native OCI/S3 publications reject writes to an object while
  an earlier remote delete is unresolved. Other objects remain available.
- Covered file metadata and chunk mappings also reject references to an object
  with a pending delete. A rejected publication preserves its previous metadata.

These are protections for the tested operations. Other library write paths,
older GC binaries and references hidden in legacy payloads still have open checks.

If optional history cannot be saved, Shardline warns on stderr and preserves
the command's output and exit status. It never repeats work to repair history
or claims verified completion from a missing receipt. Later maintenance can
retry temporary-file cleanup without repeating completed work.

## Handle a retryable write

If an earlier remote delete has an uncertain outcome, a later hold or covered
write to that object returns HTTP 503 with `Retry-After: 1`:

```text
object has an unresolved remote delete; retry after reconciliation
```

Other objects remain available. The retry interval does not promise recovery
in one second, and repeating the write does not clear the block.

Check the original GC process and logs. If GC saved a successful delete reply,
run ordinary repair with the same database and storage configuration:

```bash
shardline repair
```

Repair verifies saved replies without sending another user-object delete or
resuming the failed process. Each invocation checks up to 64 replies and advances
through the backlog; your next sweep can also recover them. Missing or invalid
replies leave the object blocked for investigation. Object absence cannot safely
clear it.

Clearing a delete block does not mark the earlier GC run completed. If its
outcome could not be saved during a database outage, inspect the original logs;
repair does not invent a recorded result for it.

Saving a successful reply adds a receipt write and cleanup request in your
existing object storage. This can increase GC time. No recovery worker is needed.

## Check a result

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
Each history entry includes `next_step` guidance. `history_notice` and
`inspection_notice` explain when inspection is unavailable. An empty list then
does not prove that no work ran or no deletes are pending.

History is not a permanent audit archive. Startup and ordinary maintenance can
remove verified finished runs created more than 30 days ago, up to 64 per invocation.
Interrupted and corrupt evidence is retained for investigation and can keep
growing; monitor metadata storage. An old run disappearing is not evidence
that it never ran. Save reports separately if you need longer retention.

See the [admin API guide](ADMIN_READ_API.md#get-apiv1tasksmaintenance)
for authentication, pagination and response fields.

## Upgrade using the existing workflow

The usual owner-run `shardline db migrate up` installs the optional metadata
and protection functions, including when no migration entries are pending.
Installation waits for current writes and maintenance. Runtime accounts need no
new fence-table grant. See the [database migration guide](DATABASE_MIGRATIONS.md).

New local and upgraded server history stays in metadata, outside user payload
inventory. Before the owner upgrade, unavailable metadata history causes a
warning rather than new task objects in payload storage. Existing legacy
history remains readable. Maintenance moves verified finished legacy history
in bounded batches; interrupted, corrupt or unrelated history stays in place.

Storage copy keeps its source read-only. Recognized task history and verified
saved delete replies stay at the source instead of becoming payload objects.
Unrecognized or damaged legacy objects keep their existing copy behavior.
Before switching storage endpoints, reconcile pending deletes with the original
database and object storage. Copying does not settle those deletes.
Copies to S3-only endpoints keep task history in private temporary storage on
the command's host, without adding objects or permissions to the payload bucket.
That history is not available through other nodes and can be removed by
temporary-directory cleanup. A copy to local storage keeps history in its
destination's SQLite metadata.
Different work databases or schemas, some legacy history
and narrowly privileged accounts still need compatibility work. Older GC,
remaining write paths, missing delete replies and deployment-scale behavior also
have open checks. Large histories can make inspection and cleanup slow;
performance at deployment scale is still under review.
Released 1.11.3 servers may return a generic error for a pending
delete. See [the acceptance evidence and remaining gates](MAINTENANCE_INTEGRATION_ACCEPTANCE.md)
before planning a rolling upgrade or rollback.

## Optional process isolation

The optional `shardline maintenance ...` runner uses a child process for
deadlines and CPU limits. Use it only when you need those controls.
Normal commands do not use it. Database ownership checks provide write safety;
the extra process provides isolation for those optional controls. A separate
PID does not guarantee cluster-wide exactly-once execution.
