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

In production, covered uploads and metadata writes reject a stale database
owner. They also protect an object while an earlier remote delete is unresolved.
Postgres provider callbacks record delivery IDs together with their changes,
so retrying a successful delivery does not repeat those changes. See the
[acceptance checklist](MAINTENANCE_INTEGRATION_ACCEPTANCE.md) for coverage and limits.

If optional history cannot be saved, CLI tracking warns on stderr and preserves
the command's output and exit status. Server warnings use your existing logging
setup. Shardline never repeats work to repair history or claims verified
completion from a missing receipt.

Automatic history has a 4,096-run budget per metadata database or schema and a
separate 4,096-workspace budget for private scratch, including cleanup awaiting
deletion. When full, Shardline preserves existing evidence and still runs your
command; it warns if the new run cannot be recorded. Already admitted runs can
finish recording. Cleanup runs after work, including when recording failed.
Unfinished and corrupt evidence is retained. New automatic artifacts are limited
to 64 KiB; older evidence and explicit legacy imports remain unchanged. These
limits do not bound physical database disk use or old binaries' history.

## Handle a retryable write

If an earlier remote delete has an uncertain outcome, a later hold or covered
write to that object returns HTTP 503 with `Retry-After: 1`:

```text
object has an unresolved remote delete; retry after reconciliation
```

Other objects remain available. Check the original GC process and logs, then
run ordinary repair with the same database and storage configuration:

```bash
shardline repair
```

Repair verifies up to 64 saved successful delete replies per run, without
sending another user-object delete. Your next sweep can also recover them.
Missing or invalid replies leave the object blocked for investigation; absence
alone cannot clear it. Repair does not mark an interrupted GC run completed.
`Retry-After: 1` suggests when to retry, not when recovery will finish.
Saving replies adds receipt I/O to GC, using the existing storage and process.

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
An alive process does not prove its recorded job is still running: a result may
have failed to save. Check the original command and logs before repeating work.
Each history entry includes `next_step` guidance. `history_notice` and
`inspection_notice` explain when inspection is unavailable. `history_notice`
also warns when retained history is full or recording availability could not be
checked. That warning does not change server readiness. An empty list then
does not prove that no work ran or no deletes are pending.

Verified finished history expires after 30 days; each startup or maintenance
invocation can remove up to 64 old runs. Interrupted and corrupt history stays
for investigation. Save reports separately if you need a permanent audit archive.
Large or legacy archives can still slow inspection and final command cleanup.

See the [admin API guide](ADMIN_READ_API.md#get-apiv1tasksmaintenance)
for authentication, pagination and response fields.

## Upgrade using the existing workflow

Use the usual owner-run `shardline db migrate up`, then restart upgraded nodes.
It installs the optional history and protection functions even when no migration
entries are pending. Runtime accounts need no new grants. See
[rolling upgrades](ROLLING_UPGRADE.md) for the deployment order and older-GC limits.

New local and upgraded server history stays in metadata, outside user payload
inventory. Before the owner upgrade, unavailable metadata history causes a
warning rather than new task objects in payload storage. Existing legacy
history remains readable. Maintenance moves verified finished legacy history
in bounded batches; interrupted, corrupt or unrelated history stays in place.

Storage copy keeps its source read-only and excludes recognized task history
from payload inventory. Reconcile pending deletes before switching storage;
copying does not settle them. S3-only copy history is temporary and host-local;
local-destination history uses its SQLite metadata. Damaged or unrecognized
legacy objects retain their existing copy behavior.

Compatibility, remaining write paths and deployment-scale checks are still open.
Read the [merge checklist](MAINTENANCE_INTEGRATION_ACCEPTANCE.md) before deploying
this branch. Released nodes may return a generic error for a pending delete.

## Optional process isolation

The optional `shardline maintenance ...` runner uses a child process for
deadlines and CPU limits. Use it only when you need those controls.
Normal commands do not use it. Database ownership checks provide write safety;
the extra process provides isolation for those optional controls. A separate
PID does not guarantee cluster-wide exactly-once execution.
