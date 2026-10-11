# Maintenance without extra services

Start Shardline and run maintenance as before:

```bash
shardline serve
shardline fsck --root /var/lib/shardline
shardline gc --root /var/lib/shardline --mark --sweep
```

There is no tracker daemon, extra network port, or task ID to manage. Existing
GC timers remain responsible for their schedules. Starting the server does not
start a new GC job.

## What users get

Shardline records which maintenance ran and whether it finished. It checks
signed results before accepting them. Upload and resumable-session history
keeps its existing format and gains native StateChronicle validation.

An interrupted operation is not treated as successful. If its effects are
uncertain, Shardline retains the evidence instead of automatically repeating
the mutation. Existing database transactions and maintenance barriers remain
responsible for protecting deployment data.

## Why a worker process exists

Normal startup and maintenance do not add a process. The tracker runs inside
Shardline; a maintenance command runs in its original CLI process with live
output.

Only the advanced `shardline maintenance ...` runner starts a child. Its
controller can stop stuck work and enforce resource limits. Idempotency keys
and fencing protect against duplicate effects and stale writes; they do not
stop a stuck process. Use ordinary commands unless you need that extra control.

## When something goes wrong

The command keeps its normal output and exit status. A failure to export task
history does not change the application result: Shardline keeps local evidence
and warns that shared completion is unconfirmed.

The existing admin API adds `/api/v1/tasks/maintenance`. It distinguishes
finished work, failed commands, and work needing investigation. It uses the
same admin token. A result from yesterday is history, not proof that the
deployment is healthy today.

A `failed` outcome means the command finished and reported a problem. Follow
its usual diagnostics. `needs_reconciliation` means the effects are uncertain:
inspect the affected data before repeating a mutation. `running_or_interrupted`
means this node cannot confirm the owner's state; it does not authorize a retry.

## Upgrade status

This branch keeps the existing version and deployment formats. It is still
under verification; it is not a claim of cluster-wide exactly-once execution
or a completed rollout guarantee.

Maintainers can find the remaining checks in
[the acceptance checklist](MAINTENANCE_INTEGRATION_ACCEPTANCE.md) and the
[technical reliability reference](RELIABILITY_ARCHITECTURE.md).
