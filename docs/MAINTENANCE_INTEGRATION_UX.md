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

| Your workflow | What changes |
| --- | --- |
| Start a server | It initializes maintenance tracking automatically in existing metadata. No tracker service to start. |
| Run fsck, repair or GC | Use the same command and configuration. Tracking stays in that command's process. |
| Run multiple nodes | Nodes sharing the same Postgres database, schema and storage can inspect shared history. Covered writes use database ownership checks. |
| Set a maintenance deadline or CPU limit | Explicitly use `shardline maintenance ...`. Only this optional runner starts a child process. |

The child process lets the optional runner stop or limit its own maintenance
job. Write safety comes from database ownership checks and delete protection,
not from giving a worker another PID. Normal operation needs no worker runner.
If the optional runner stops unexpectedly, inspect its recorded outcome and
storage effects before retrying. Stopping a worker cannot undo a remote request
that storage has already received.

**This branch is still under development and is not ready to merge.**
The version remains 1.11.3. The remaining upgrade limits are listed below.

## What to do when something goes wrong

Use your existing admin token to inspect the endpoints below. Read any warning
alongside the returned list; an empty list with a warning is inconclusive.
The admin API is optional and disabled by default. If you have not enabled it,
these routes return `404`; tracking still runs and command/server logs remain
available. To inspect history over HTTP, use the existing
[admin token configuration](ADMIN_READ_API.md#enable-and-authenticate).

| What you see | What to do next |
| --- | --- |
| Server readiness fails | Check server logs and its storage/database dependencies. Maintenance history does not establish readiness. |
| The server is ready, but history has a warning | Check server or command logs for the recording failure. Your work may have run without a saved result; check its effects before running it again. |
| A run is completed | Its saved result verified as successful. Use `/api/v1/status` to check the server now. |
| A run is failed, interrupted, unresolved or corrupt | Read its `next_step` and original command logs. Inspect partial effects before repeating a mutation; preserve corrupt evidence. |
| A write returns `503` for an unresolved remote delete | Check the original GC command, then run `shardline repair` with the same configuration to verify saved delete replies. Retry the write after reconciliation. |
| Repair leaves a delete pending | Investigate the original GC and storage logs. Missing replies cannot be replaced by checking that the object is absent. |

## What users gain

Saved maintenance evidence survives a process failure. When recording succeeds,
you can inspect the recorded outcome without setting up another service. If
recording fails, history can be incomplete even though the command ran.
History does not automatically rerun jobs or prove current cluster health.

Upgraded Postgres deployments share history through their existing database.
Another node using the same database, schema and storage can inspect it.
Local deployments save history in their existing SQLite metadata.

In production, covered uploads and metadata writes stop if they lose database
ownership. Writes to an object also stop while an earlier delete is uncertain.
Postgres provider callbacks save the delivery ID and its changes together,
so retrying a successful delivery does not repeat those changes. See the
[acceptance checklist](MAINTENANCE_INTEGRATION_ACCEPTANCE.md) for coverage and limits.

If optional history fails or fills up, your command still runs with its original
output and exit status. CLI warnings go to stderr; server warnings use existing
logs. Shardline preserves existing evidence and never repeats work to repair
history or invents completion when a result could not be saved.

## Handle a retryable write

If an earlier remote delete has an uncertain outcome, a later hold or covered
write to that object returns HTTP 503 with `Retry-After: 1`:

```text
object has an unresolved remote delete; retry after reconciliation
```

Other objects remain available. Check the original GC command and logs, then
run ordinary repair with the same database, schema and storage configuration:

```bash
shardline repair
```

Repair verifies up to 64 saved successful delete replies per run, without
sending another user-object delete. Your next sweep can also recover them.
Missing or invalid replies leave the object blocked for investigation; absence
alone cannot clear it. Repair does not mark an interrupted GC run completed.
`Retry-After: 1` suggests when to retry, not when recovery will finish.

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

```bash
curl -H "Authorization: Bearer $SHARDLINE_ADMIN_READ_TOKEN" \
  'https://cas.example.com/api/v1/tasks/maintenance?limit=20'
```

Replace the example address with your server. Use the dedicated admin read
token, not a repository token. Follow `page.next_cursor` to inspect later pages.

| Endpoint | What it tells you |
| --- | --- |
| `/api/v1/status` | Server readiness. |
| `/api/v1/tasks/maintenance` | Recorded maintenance results. |
| `/api/v1/tasks/maintenance/$RUN_ID/report` | Saved counts for a server job, when available. |
| `/api/v1/tasks/maintenance/deletes` | Objects with pending deletes. |

These endpoints only inspect state. A pending delete can belong to a running
GC, and an alive process can have finished work without saving its result.
Follow `next_step` and check the original logs before repeating work. Older or
interrupted runs may have no report; existing CLI diagnostics remain available.
Read `history_notice` and `inspection_notice` before trusting an empty list.

## History retention and limits

You do not need to configure a history cleanup job. Verified finished history
is eligible for cleanup after 30 days. Each startup or
maintenance invocation can remove up to 64 old runs and retire at most 256
run-artifact keys. Large histories finish cleanup over later invocations;
their retirement markers remain until cleanup succeeds. Interrupted and corrupt history stays
for investigation. Save reports separately if you need a permanent audit archive.
Large or legacy archives can still slow inspection and final command cleanup.
Server startup does not wait for history cleanup. Maintenance commands perform
it after their application work finishes, so it can delay command exit.
At shutdown, history cleanup stops between storage calls; an active call may
finish. The next startup can finish interrupted cleanup.

Automatic history accepts up to 4,096 runs per metadata database or schema.
Private temporary workspaces have a separate combined limit of 4,096, including
workspaces awaiting deletion. New automatic artifacts are limited to 64 KiB.
That limit applies to saved history, not normal live output or diagnostic exports.
Existing and imported history is preserved. These are admission limits, not
physical disk limits; see the [capacity limits](MAINTENANCE_INTEGRATION_ACCEPTANCE.md#known-gaps).

See the [admin API guide](ADMIN_READ_API.md#get-apiv1tasksmaintenance)
for authentication, pagination and response fields.

## Upgrade using the existing workflow

1. Pause destructive GC and let existing GC commands finish.
2. Run the usual owner-run `shardline db migrate up` with the upgraded binary
   and your existing database and schema configuration.
   Finish all pending migrations; follow the database migration guide's
   maintenance-window requirements if your schema is behind.
3. Replace server roles using your normal drain and restart procedure. Replace
   the binaries or images used by existing maintenance jobs too.
4. Check pending deletes and investigate interrupted work, then resume your
   existing GC schedule.

Owner setup installs the optional history and protection functions even when no
migration entries are pending. Runtime accounts need no new grants. Upgrading
servers alone cannot protect deletes performed by an old GC binary. See
[rolling upgrades](ROLLING_UPGRADE.md) for the role order and rollback limits.

New local and upgraded server history stays in metadata, outside user payload
inventory. Before the owner upgrade, unavailable history produces a warning.
Existing legacy history remains readable; interrupted and corrupt evidence stays
for investigation. See the [CLI guide](CLI.md#durable-maintenance-linux)
for storage-copy history and temporary-workspace behavior. Reconcile pending
deletes before switching storage: copying does not settle them.

Compatibility, remaining write paths and deployment-scale checks are still open.
Read the [merge checklist](MAINTENANCE_INTEGRATION_ACCEPTANCE.md) before deploying
this branch. Released nodes may return a generic error for a pending delete.

Shared history and the optional worker do not guarantee cluster-wide
exactly-once execution. Follow each run's recovery guidance before retrying
work that may already have changed storage.
