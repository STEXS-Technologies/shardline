# Maintenance without another service

Keep starting Shardline and running maintenance as before:

```bash
shardline serve
shardline fsck
shardline gc --mark --sweep
```

There is no tracker service, extra port or new mount to manage. Your existing
GC timers still control the schedule. Starting the server does not start GC.
Tracking uses your existing storage. You do not need to loosen deployment-root
permissions to create a separate tracking directory.

## What changes for you

- A history showing which maintenance runs completed, failed or need investigation.
- In a Postgres/S3 deployment, another node can inspect a run after its original
  node disappears. You do not need to recover the old container's files.
- Safer crash reporting: a printed report alone does not make an interrupted
  operation count as completed.

Your timer still runs a fresh scan each time. Shardline does not substitute an
old result or automatically repeat a run whose effects are uncertain.

## Does this start another process?

Normal startup tracks work inside the server, without another process. Your
usual maintenance command keeps its original process and live output.

The optional `shardline maintenance ...` runner uses a child process so it can
stop a task on a deadline and limit its CPU use. You only need that runner if
you want those controls. The child is not what makes database writes safe.

## Can keys and locks provide the safety instead?

- An **idempotency key** lets storage recognize a repeated request and avoid
  applying its change twice.
- **Fencing** stops an old owner from writing after another node takes over.

Yes, these protections can work inside the existing process. Storage must check
them as part of applying the change; checking a key or lock beforehand leaves a
gap. A database lock cannot by itself protect a later S3 delete or a webhook
request. Those effects need their own protection.

## After a crash

S3 can delete an object before GC receives its reply. If GC dies then, history
stays unresolved. A replacement server does not repeat that uncertain work
automatically. A missing reply is not proof that nothing changed.

Inspect `/api/v1/tasks/maintenance` with your existing admin token:

| State | What to do |
| --- | --- |
| `completed` | Read the verified recorded result. |
| `failed` | Read diagnostics; some data may have changed. |
| `needs_reconciliation` | Inspect affected data before repeating a mutation. |
| `running_or_interrupted` | Check whether the original command is still working before deciding to retry. |
| `corrupt` | Investigate the history before relying on it. |

Use `/api/v1/status` for server readiness. Maintenance history does not prove
current deployment health. The [admin API guide](ADMIN_READ_API.md#get-apiv1tasksmaintenance)
covers pagination and all states.

If Shardline cannot save the history, it warns. The command keeps its existing
behavior, but you cannot rely on a verified result in history. Shared-only
deployments have no local backup of an unpublished result.

## Is this ready to upgrade?

Not yet. The version and deployment formats remain unchanged on this branch.

Rebuild, lifecycle repair and retention hold updates now stop their database
writes if they lose the connection holding their maintenance lock. This prevents
an old invocation from continuing to write after another node takes over.

Other operations' remote effects and crash boundaries, plus large-deployment
limits, still need validation before this integration is ready to merge.

Cluster-wide exactly-once execution is not guaranteed. See the
[merge checklist](MAINTENANCE_INTEGRATION_ACCEPTANCE.md) for remaining work and
the [technical reference](RELIABILITY_ARCHITECTURE.md) for internals.
