# Maintenance without extra services

Keep using your existing configuration and commands:

```bash
shardline serve
shardline fsck
shardline gc --mark --sweep
```

There is no tracker service, extra port, task ID or new state mount to manage.
Your existing GC timers still control the schedule. Starting the server does
not start GC.

## What you get

Shardline records maintenance and verifies its recorded results. You can see
which run finished, failed or needs investigation after a crash.

Each intentional command performs a fresh run. Yesterday's successful scan
does not replace today's work. A command keeps the configuration it started
with; changes apply to the next command.

Existing storage transactions and safety barriers still protect your data.
Upload and resumable-session history keeps its existing format and receives
additional validation. Read-only GC does not create task history.

## Why this helps with several nodes

In a Postgres/S3 deployment, recorded history uses the existing shared store.
A replacement node can inspect it without copying a private tracker directory
from the old container.

Suppose GC changes data and dies before recording completion. Its run stays
unresolved, even if GC printed a successful report. Starting a replacement
server does not mark that old run successful or repeat it automatically.

History cannot tell you whether an unreachable node is still working. Inspect
the affected data and establish ownership before repeating an uncertain mutation.

## Does it start another process?

Normal startup uses an embedded tracker. Ordinary maintenance runs in its
original CLI process with live output.

Only the optional `shardline maintenance ...` runner starts a child process.
Its controller can stop stuck work and enforce resource limits. Ordinary
commands do not need that process.

Idempotency keys prevent duplicate effects where an operation supports them.
Fencing rejects stale owners' writes. Both work without a separate worker.
A child process adds isolation; it does not provide exactly-once execution.

## Where to check a run

Use `/api/v1/tasks/maintenance` with your existing admin token:

| Result | Meaning and next action |
| --- | --- |
| `completed` | The recorded result verified. Read the report. |
| `failed` | Follow the command's diagnostics; some data may have changed. |
| `needs_reconciliation` | Inspect affected data before repeating a mutation. |
| `running_or_interrupted` | The owner's state is unknown here. Do not assume a retry is safe. |
| `corrupt` | The history could not verify. Investigate before relying on it. |

Use `/api/v1/status` for this server's readiness. Past maintenance success does
not prove current deployment health. See the [admin API guide](ADMIN_READ_API.md#get-apiv1tasksmaintenance)
for pagination and all states.

## If tracking fails

Shardline reports tracking failures. Later commands can export retained local
evidence without repeating the work.

Postgres/S3 tracking does not require a writable local filesystem. If shared
history is unavailable, commands warn and keep their existing behavior and
safety barriers. There is no local recovery copy in that case. An unconfirmed
outcome is not a verified success.

## Upgrade status

The version and deployment formats remain unchanged on this branch.
Verification is still in progress. Cluster-wide exactly-once execution is
not guaranteed. Maintainers can review the remaining [acceptance checks](MAINTENANCE_INTEGRATION_ACCEPTANCE.md)
and [technical reference](RELIABILITY_ARCHITECTURE.md).
