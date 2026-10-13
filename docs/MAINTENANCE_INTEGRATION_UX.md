# Maintenance without another service

Keep starting Shardline and running maintenance as you do today:

```bash
shardline serve
shardline fsck
shardline gc --mark --sweep
shardline repair
```

Tracking runs in the existing process and saves results in existing metadata.
There is no tracker service, extra port, new mount or new configuration.
Commands keep their live output and exit status. Your existing timer still
starts GC; starting the server does not start GC.

Startup checks saved upload progress. Eligible file checksums are verified in
the same process while requests are served, so that verification does not hold
up readiness. If verification fails or shutdown interrupts it, the upload's
unfinished progress is preserved.

**This branch is not ready to merge.** The version remains 1.11.3; the version
bump belongs in a separate PR. See the [merge checklist](MAINTENANCE_INTEGRATION_ACCEPTANCE.md)
for the remaining checks.

## What users gain

- Maintenance results survive restarts when recording succeeds. Inspect them
  without rerunning the work.
- Nodes sharing the same PostgreSQL database, schema and storage can inspect
  the same history.
- Protected writes return a retryable error when database ownership is lost.
  An uncertain earlier delete blocks its affected objects until reconciliation.
- Identical native content can be published to different repositories without
  conflicting upload progress. Valid footerless xorb uploads can be retried with
  their canonical encoding.

History covers fsck, index rebuild, lifecycle repair, GC mark/sweep and storage
copies. Dry runs do not save shared task history. Specialized Hub-tree and
LFS-evidence repairs, backups and retention-hold commands keep their existing
diagnostics. Request/upload progress is separate from maintenance history.

If recording fails, work keeps its original result and emits a warning. An
empty history therefore does not prove that work never ran. Saved completion
does not establish current deployment health or cluster-wide exactly-once
execution. The [coverage and limits](MAINTENANCE_INTEGRATION_ACCEPTANCE.md)
describe which write paths are protected.

## Upgrade using the existing workflow

1. Pause destructive GC and let existing GC commands finish.
2. For PostgreSQL, run the usual owner-run `shardline db migrate up` with the
   upgraded binary and your existing database/schema configuration. Do this
   before starting upgraded servers, even if no migrations are pending.
   Finish pending migrations using the migration guide's maintenance-window
   requirements. SQLite installs tracking and upgrade protection during normal
   startup; it needs no separate database command.
3. Replace server roles using your normal drain and restart procedure. Replace
   the binaries or images used by existing maintenance jobs too.
4. Inspect `shardline history --pending-deletes` and investigate interrupted
   work, then resume your existing GC schedule.

Runtime accounts need no new grants. Owner setup also protects new upload
history from older recovery code during rollback. Local startup installs that
protection automatically. An old GC binary cannot provide the new delete
protection: keep destructive GC paused during rollback until compatible job
binaries are restored. See [rolling upgrades](ROLLING_UPGRADE.md) for role order
and rollback limits.

Reconcile pending deletes before switching storage. Copying storage does not
settle them. History remains outside user payload inventory; see the
[CLI reference](CLI.md#durable-maintenance-linux) for storage-copy history.

## Check the server or a previous result

Check current readiness at your server address:

```bash
curl -f https://cas.example.com/readyz
```

`shardline health` checks process liveness through `/healthz`; it does not check
readiness. Maintenance history cannot replace either check.

Inspect saved results using the same configuration and root as your server:

```bash
shardline history
# With an explicit deployment configuration:
shardline --config /etc/shardline/shardline.toml history
```

Read `state`, `next_step` and any `history_notice`. Inspection needs no admin
token or worker setup and starts no maintenance. For a blocked write, inspect:

```bash
shardline history --pending-deletes
```

Read each attempt's `next_step`. An unavailable inspection result is inconclusive;
it does not mean writes are safe. See the [CLI reference](CLI.md#inspect-maintenance-history)
for pagination and read-only SQLite requirements. HTTP inspection is also
available through the optional [admin read API](ADMIN_READ_API.md#get-apiv1tasksmaintenance).

## What to do when something goes wrong

| What you see | Next action |
| --- | --- |
| Readiness fails | Check server logs and database/storage availability. |
| History is unavailable or recording warns | Check configuration, dependencies and original command logs. Work may have run; inspect its effects before repeating it. |
| A run completed | Read its saved result. Check `/readyz` separately for current readiness. |
| A run failed, was interrupted, is unresolved or is corrupt | Follow `next_step` and inspect partial effects before retrying. Preserve corrupt evidence. |
| A write returns `503` for an uncertain delete | Check the original GC and its logs. After it stops, run `shardline repair` with the same configuration, inspect pending deletes again, then retry after reconciliation. |
| An upload returns `503` after losing ownership | Send a fresh request after the retry delay. If failures continue, check database connectivity and server logs. |
| Repair leaves deletes pending | A backlog can need more runs. Investigate missing or invalid replies; object absence alone cannot settle an uncertain delete. |

`Retry-After: 1` suggests when to retry; it does not promise recovery in one
second. Repair checks saved replies without sending another user-object delete.
See [pending-delete repair](CLI.md#reconcile-pending-remote-deletes) for backlog
handling. If an earlier file upload cannot be verified, retry with validated
content; a newer file with the same name does not prove that attempt succeeded.
For other retryable errors, follow the response: supply missing content before
retrying, or roll back an application-owned transaction and start a fresh one.

## Optional isolation and history retention

Use `shardline maintenance ...` only when you explicitly want a deadline or
CPU limit. Its Linux child lets the controller enforce those limits, including
when work is blocked. The child stops when its controller exits or dies.
Database ownership and delete protection still provide write safety. Normal
startup and ordinary maintenance need no child process or operator task ID.

History cleanup runs automatically, without a new timer. Verified finished runs
become eligible after 30 days; unfinished and corrupt evidence is retained.
When recording capacity is full, work continues with a warning. Save reports
separately for a permanent archive. Recording limits do not limit live output
or diagnostic exports, and they are not physical disk quotas. Large archives
can slow inspection and command exit. See [history limits](CLI.md#maintenance-history-limits)
for the budgets and cleanup behavior.
