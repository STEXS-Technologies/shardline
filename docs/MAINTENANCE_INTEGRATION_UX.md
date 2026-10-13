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
| Run GC without `--mark` or `--sweep`, or copy with `--dry-run` | Read the command output. These dry runs do not save shared task history. |
| Run multiple nodes | Nodes sharing the same Postgres database, schema and storage can inspect shared history. Covered writes use database ownership checks. |
| Set a maintenance deadline or CPU limit | Explicitly use `shardline maintenance ...`. Only this optional runner starts a child process. |

Use the optional child only when you want a deadline or CPU limit. On Linux,
it stops when its controller exits or dies. Database ownership and delete
protection provide write safety. After an interruption, inspect the outcome
before retrying: a remote storage request may already have taken effect.

**This branch is still under development and is not ready to merge.**
The version remains 1.11.3. The remaining upgrade limits are listed below.

## What to do when something goes wrong

Use your existing admin token to inspect the endpoints below. Read any warning
alongside the returned list; an empty list with a warning is inconclusive.
The admin API is optional and disabled by default. If you have not enabled it,
these routes return `404`; tracking still runs and command/server logs remain
available. To inspect history over HTTP, use the existing
[admin token configuration](ADMIN_READ_API.md#enable-and-authenticate).

There is currently no CLI command for browsing shared automatic history.
Use logs when the admin API is disabled. Do not rerun maintenance just to
inspect its previous result: ordinary commands start fresh work. The optional
`shardline maintenance ...` runner's task IDs refer to its own pinned tasks;
they do not browse the history of ordinary commands.

| What you see | What to do next |
| --- | --- |
| Server readiness fails | Check server logs and its storage/database dependencies. Maintenance history does not establish readiness. |
| The server is ready, but history has a warning | Check server or command logs for the recording failure. Your work may have run without a saved result; check its effects before running it again. |
| A run is completed | Its saved result verified as successful. Use `/api/v1/status` to check the server now. |
| A run is failed, interrupted, unresolved or corrupt | Read its `next_step` and original command logs. Inspect partial effects before repeating a mutation; preserve corrupt evidence. |
| A write returns `503` for an unresolved remote delete | Check the original GC command, then run `shardline repair` with the same configuration to verify saved delete replies. Retry the write after reconciliation. |
| Repair leaves a delete pending | Check repair logs: a backlog can need several runs. Missing or invalid replies require investigation; checking that the object is absent cannot replace a reply. |

## What users gain

You get saved maintenance outcomes in existing SQLite or upgraded Postgres
metadata. Nodes sharing the same database, schema and storage can inspect the
same history. Covered writes reject lost ownership and uncertain earlier deletes.
Postgres provider callbacks commit their delivery ID with their changes, so a
retry of a successful delivery does not repeat those changes.

Automatic command history covers fsck, index rebuild, lifecycle repair, GC mark
or sweep, and storage copies. Ordinary `shardline repair` runs lifecycle repair.
Specialized Hub-tree and LFS-evidence repairs, backups and retention-hold commands
keep their existing diagnostics; this integration does not add task history to
those commands. Request and upload state remains separate from maintenance history.

If recording fails or fills up, your command keeps its original output and exit
status and emits a warning. History can therefore be incomplete. Shardline does
not replay work to fill a gap, invent completion, or treat history as proof of
current cluster health. See the [coverage and limits](MAINTENANCE_INTEGRATION_ACCEPTANCE.md).

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

Repair examines up to 64 saved replies per run, including invalid evidence,
without sending another user-object delete. Your next sweep can also recover them.
For a backlog, repeat repair with the same configuration; saved scan progress
allows later runs to reach replies beyond retained invalid evidence. Check logs
for `processed saved remote delete replies`: `settled` counts cleared attempts,
and `retained` counts evidence that could not settle an attempt. A run examining
64 replies may have more to check; it does not mean all pending objects were
recovered. Inspect pending deletes again after repair.
Repair also logs guidance when it reaches that limit or retains evidence that
could not settle an attempt. These messages do not change its exit status.
Missing or invalid replies leave the object blocked for investigation; absence
alone cannot clear it. Repair does not mark an interrupted GC run completed.
`Retry-After: 1` suggests when to retry, not when recovery will finish.

Other retryable errors explain the required action: resend an upload whose
staged content disappeared, upload missing xorbs before resending a native
shard, or retry in a new database transaction. If your application owns that
transaction, roll it back first. Covered Postgres content-check failures publish
no new file metadata or dedupe mappings; retries keep your configured isolation.

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

Follow `next_step` and read `history_notice` and `inspection_notice`. A pending
delete may belong to a running GC; a missing report may mean recording failed.
These endpoints inspect state and do not retry work. Keep the original command
logs and diagnostics when investigating an interruption.

## History retention and limits

Cleanup runs automatically; no new timer is needed. Verified finished history
becomes eligible after 30 days. Interrupted and corrupt history stays for
investigation. Save reports separately for a permanent archive.

| Limit | Effect |
| --- | --- |
| 64 old runs and 256 run-artifact keys per cleanup invocation | Larger histories continue cleanup on later invocations. |
| 4,096 automatic runs per metadata database or schema | When full, work continues with a recording warning. |
| 4,096 private workspaces, including those awaiting deletion | Separate from the shared-history limit. |
| 64 KiB per new automatic artifact | Limits saved history, not live output or diagnostic exports. |

These limits do not cap physical disk use or inspection time. Existing and
imported history is preserved. Large archives can slow inspection and command
exit: commands clean history after their work, while server startup does not
wait for cleanup. Shutdown stops cleanup between storage calls; an active call
may finish. Later startup resumes partial cleanup. See the
[capacity limits](MAINTENANCE_INTEGRATION_ACCEPTANCE.md#known-gaps).

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
