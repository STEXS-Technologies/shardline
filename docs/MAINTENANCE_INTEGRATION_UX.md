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

In production, covered uploads and metadata writes stop if they lose database
ownership. Writes to an object also stop while an earlier delete is uncertain.
Postgres provider callbacks save the delivery ID and its changes together,
so retrying a successful delivery does not repeat those changes. See the
[acceptance checklist](MAINTENANCE_INTEGRATION_ACCEPTANCE.md) for coverage and limits.

If optional history cannot be saved, CLI tracking warns on stderr and preserves
the command's output and exit status. Server warnings use your existing logging
setup. Shardline never repeats work to repair history or claims verified
completion from a missing receipt.

If history fills up, your command still runs. Shardline preserves existing
records and warns if it cannot record the new run. Recording failure does not
prevent cleanup or stop an already recorded run from saving its result.

Automatic history accepts up to 4,096 runs per metadata database or schema.
Private temporary workspaces have a separate combined limit of 4,096, including
workspaces awaiting deletion. New automatic artifacts are limited to 64 KiB.
These limits preserve existing and imported history; they do not cap physical
disk use. Interrupted and corrupt records stay available for investigation.

## Handle a retryable write

If an earlier remote delete has an uncertain outcome, a later hold or covered
write to that object returns HTTP 503 with `Retry-After: 1`:

```text
object has an unresolved remote delete; retry after reconciliation
```

Other objects remain available. Check the original GC process and logs, then
run ordinary repair with the same database, schema and storage configuration:

```bash
shardline repair
```

Repair verifies up to 64 saved successful delete replies per run, without
sending another user-object delete. Your next sweep can also recover them.
Missing or invalid replies leave the object blocked for investigation; absence
alone cannot clear it. Repair does not mark an interrupted GC run completed.
`Retry-After: 1` suggests when to retry, not when recovery will finish.
Saving replies adds receipt I/O to GC, using the existing storage and process.

Other retryable write errors also return HTTP 503 with `Retry-After: 1`:

| Error | What to do |
| --- | --- |
| `prepared upload content is no longer available; retry the upload` | Resend the upload. Its staged data disappeared before publication. |
| `native shard backing content is no longer available; upload missing backing content again, then retry the shard` | Upload missing xorbs, then resend the shard. Resending also restores its normalized shard body. |
| `database transaction snapshot changed; retry the request in a new transaction` | Retry the request. If your application owns the database transaction, roll it back and start a new one first. |

For covered Postgres uploads, failed content checks leave no new file metadata
or dedupe mappings visible.
Retries preserve the configured database isolation level.

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
Follow each result's `next_step` guidance. If the response includes
`history_notice` or `inspection_notice`, read that warning before trusting an
empty list: inspection or recording may be unavailable, or history may be full.
These warnings do not change server readiness. An empty list with a warning
does not prove that no work ran or no deletes are pending.

Verified finished history expires after 30 days; each startup or maintenance
invocation can remove up to 64 old runs. Interrupted and corrupt history stays
for investigation. Save reports separately if you need a permanent audit archive.
Large or legacy archives can still slow inspection and final command cleanup.
Server startup does not wait for history cleanup. Maintenance commands perform
it after their application work finishes, so it can delay command exit.
Server shutdown asks optional history cleanup to stop between storage operations.
A storage call already in progress can finish before cleanup stops.
A later startup can finish interrupted evidence cleanup.

See the [admin API guide](ADMIN_READ_API.md#get-apiv1tasksmaintenance)
for authentication, pagination and response fields.

## Upgrade using the existing workflow

1. Pause destructive GC and let existing GC commands finish.
2. Run the usual owner-run `shardline db migrate up` with the upgraded binary
   and your existing database and schema configuration.
3. Replace server roles using your normal drain and restart procedure. Replace
   the binaries or images used by existing maintenance jobs too.
4. Check pending deletes and investigate interrupted work, then resume your
   existing GC schedule.

Owner setup installs the optional history and protection functions even when no
migration entries are pending. Runtime accounts need no new grants. Upgrading
servers alone cannot protect deletes performed by an old GC binary. See
[rolling upgrades](ROLLING_UPGRADE.md) for the role order and rollback limits.

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
