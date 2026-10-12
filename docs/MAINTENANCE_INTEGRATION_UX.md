# Maintenance without another service

Keep using your existing commands and timers:

```bash
shardline serve
shardline fsck
shardline gc --mark --sweep
shardline repair
```

There is no tracker service, extra port, new mount or separate configuration.
Normal startup tracks work inside the server. Maintenance keeps its original
process and live output. Starting the server does not start GC.

## What you gain

Maintenance history records completed, failed and interrupted runs. In a
Postgres/S3 deployment, another node can inspect shared history after the
original node disappears, without recovering its container files.

After the usual owner upgrade, new server jobs, GC, rebuild and repair share
history when their database sessions reach the same database, schema and object
storage. Older records, storage-copy jobs and the optional runner retain their
original login scope. If shared grouping is unavailable, the API reports
`history_grouping: legacy`; otherwise it reports `shared_metadata`.

If history cannot be saved, Shardline warns and the command keeps its existing
behavior. You cannot then rely on a verified history result.

## When a delete is interrupted

A later hold or covered write to an object with an unresolved delete returns a
clear retryable error. Other objects remain available. Rejected updates preserve
the previous metadata. Current native uploads check the chunks inside their
xorbs as well as directly named objects.

If GC received a successful delete reply, it tries to save that reply in your
existing object storage before recording completion in the database. The next
sweep or ordinary repair command can settle that saved result:

```bash
shardline repair
```

Recovery verifies the saved reply and clears only its matching pending delete.
It does not delete the object again or resume the failed GC process. The original
run stays failed even if recovery later settles its delete.

Each command checks up to 64 saved replies and logs the counts. Repeated runs
advance through the backlog. Invalid or conflicting evidence is retained for
investigation. There is no recovery worker to start.

If Shardline cannot save a reply, it warns and still tries normal database
settlement. Existing GC credentials need no additional storage write permission.
If both fail, or the reply was never received or saved, the object stays blocked.
A missing object does not prove that an earlier request can never apply later.

## Inspecting a problem

Use your existing admin token with:

- `/api/v1/tasks/maintenance/deletes` for affected objects with pending deletes.
- `/api/v1/tasks/maintenance` for recorded job results.

A pending entry may mean GC is still running. Check the original command and
logs. These endpoints inspect state; they do not retry or clear a delete.

| History state | What to do |
| --- | --- |
| `completed` | Read the verified recorded result. |
| `failed` | Read diagnostics; some data may have changed. |
| `needs_reconciliation` | Investigate before repeating a mutation. |
| `running_or_interrupted` | Check whether the original command is still working. |
| `corrupt` | Investigate before relying on the history. |

Use `/api/v1/status` for server readiness. History does not prove current
deployment health. The [admin API guide](ADMIN_READ_API.md#get-apiv1tasksmaintenance)
covers pagination and all states.

## The optional runner

`shardline maintenance ...` uses a child process for deadlines and CPU limits.
You only need it for those controls. Normal commands do not use it. Database
ownership and checks at each write provide the safety independently of that
process separation.

## Upgrade status

**This branch is not ready to merge.** The version and deployment formats remain
unchanged. Recovery of missing replies, older GC sources, remaining write paths
and deployment-scale limits still need work.

The usual owner-run `shardline db migrate up` installs the protection functions,
even with no pending migration entries. Runtime accounts need no new fence-table
grant. See the [migration guide](DATABASE_MIGRATIONS.md) and
[remaining acceptance gates](MAINTENANCE_INTEGRATION_ACCEPTANCE.md).
