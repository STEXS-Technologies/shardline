# Maintenance: same commands, clearer results

Keep starting Shardline and running maintenance as before:

```bash
shardline serve
shardline fsck
shardline gc --mark --sweep
```

There is no tracker service, extra port or new mount to manage. Your existing
GC timers still control the schedule. Starting the server does not start GC.

## What users gain

- A history showing which maintenance runs completed, failed or need investigation.
- Shared history for Postgres/S3 deployments: a replacement node can inspect an
  old run without copying files from the old container.
- Safer crash reporting: a printed report alone does not make an interrupted
  operation count as completed.

Each intentional invocation performs fresh work. Yesterday's result cannot
replace today's scan. A command keeps the configuration it started with.

## Why a separate worker exists

Normal startup tracks work inside the server. Ordinary maintenance stays in
its original CLI process with live output.

Only the optional `shardline maintenance ...` runner creates a child process.
Its controller can terminate stuck work and enforce CPU or time limits.
Existing commands do not need it.

Data protection belongs in storage:

- **Idempotency:** recognize the same requested effect across retries and avoid
  applying it twice.
- **Fencing:** reject writes from a former owner after ownership changes.

Both must be checked atomically with the affected write. Both can work without
another PID. A child process provides isolation and termination; these storage
checks provide protection against duplicate and stale writes. External services
must support equivalent checks for their effects to have the same guarantees.

## After a crash

Suppose GC changes data and dies before recording completion. A replacement
server leaves the run unresolved and does not repeat it automatically.

Use `/api/v1/tasks/maintenance` with your existing admin token:

| State | What to do |
| --- | --- |
| `completed` | Read the verified recorded result. |
| `failed` | Read diagnostics; some data may have changed. |
| `needs_reconciliation` | Inspect affected data before repeating a mutation. |
| `running_or_interrupted` | The owner may still be working. Establish ownership before retrying. |
| `corrupt` | Investigate the history before relying on it. |

Use `/api/v1/status` for server readiness. Maintenance history does not prove
current deployment health. The [admin API guide](ADMIN_READ_API.md#get-apiv1tasksmaintenance)
covers pagination and all states.

If tracking cannot publish, Shardline warns rather than claiming a verified
success. Commands retain their existing behavior. Retained local evidence can
be exported later; deployments using only shared history have no local recovery
copy.

## Is this ready to upgrade?

Not yet. The version and deployment formats remain unchanged on this branch.

Rebuild now uses the same Postgres connection for its maintenance lock and
metadata writes. If that connection is lost, the old invocation cannot reconnect
and continue writing from its earlier scan. A two-machine fault test verifies
that the original rebuild fails and the waiting node can complete its own run.

Retention hold updates use the same protection: if their database ownership is
lost, they fail rather than applying a late change during GC's exclusive run.

Other operations' remote effects and crash boundaries, plus large-deployment
limits, still need validation before this integration is ready to merge.

Cluster-wide exactly-once execution is not guaranteed. See the
[merge checklist](MAINTENANCE_INTEGRATION_ACCEPTANCE.md) for remaining work and
the [technical reference](RELIABILITY_ARCHITECTURE.md) for internals.
