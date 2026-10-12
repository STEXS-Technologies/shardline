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
history across logins that use the same database, schema and object storage.
Older records, storage-copy jobs and the optional runner still follow their
original login. If shared grouping is unavailable, the API reports
`history_grouping: legacy`; otherwise it reports `shared_metadata`.

If history cannot be saved, Shardline warns and the command keeps its existing
behavior. You cannot then rely on a verified history result.

Webhook secret encryption preserves changes made by another node, including
secret rotation and removal. Its startup sweep runs inside the server and stops
if its database connection is lost. A failed sweep leaves ordinary request
handling and lazy encryption enabled.

On Postgres, provider callbacks save their delivery ID and repository changes
together. If the database lock owner disappears before the change commits,
neither is saved: the provider can retry the same delivery. Successful retries
are deduplicated. This needs no extra process or configuration.

## When a delete is interrupted

A later hold or covered write to an object with an unresolved delete returns a
clear retryable error. Other objects remain available. Rejected updates preserve
the previous metadata. Current native uploads also check the chunk references
inside their uploaded data.

On Postgres, an S3 overwrite rejected before it commits leaves the current
object in place. Retry with your usual S3 client; no additional service or
configuration is needed.

File and native shard uploads on Postgres also reject final publication if the
request loses its database lock. Existing file records remain available; retry
with your usual client. An interrupted attempt may leave unreferenced bytes
for later maintenance to reclaim.

For OCI uploads on Postgres, a rejected publication preserves the previous tag
and manifest. Incomplete new manifests stay hidden until a retry publishes them.
If Shardline loses its database lock before an upload part or session change
commits, it rejects that change without advancing recorded progress. Check the
upload with your usual client and retry.

A pending delete also blocks covered OCI publication. New manifest uploads
check their config, layer, child-manifest and subject references before saving
them. Older tag-only writes still need additional protection. During a rolling
upgrade, released 1.11.3 servers reject covered writes with a generic server
error; updated servers explain when retry is safe.

After the owner-run database upgrade, ordinary GC can find hidden manifests
left by interrupted uploads. It keeps active holds and waits for your configured
retention period before reclaiming them. This uses the existing maintenance
account. Automatic discovery warns and remains unsupported if that account
has active row restrictions.

If GC received a successful delete reply, it tries to save that reply in your
existing object storage before recording completion in the database. The next
sweep or ordinary repair command can settle that saved result:

```bash
shardline repair
```

Recovery verifies the saved reply and clears only its matching pending delete.
It does not delete the object again or resume the failed GC process. The original
run stays failed even if recovery later settles its delete.

On upgraded Postgres/S3 deployments, saving and cleaning up a successful reply
adds one receipt write and one receipt cleanup request per deleted object.
This can increase GC runtime, especially when object storage is far away.
Your existing GC command and timer remain in charge.

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

For a server startup job, take its `id` from the history list and read the saved
counts with the same admin token:

```bash
curl -H "Authorization: Bearer $SHARDLINE_ADMIN_READ_TOKEN" \
  "https://cas.example.com/api/v1/tasks/maintenance/$RUN_ID/report"
```

A failed Hub upgrade can show, for example, `inspected: 3`, `upgraded: 2` and
`failures: 1`. This explains partial progress; the job still needs investigation.
Older failed jobs and interrupted jobs may have no saved report. Ordinary CLI
commands keep their existing output and diagnostics.

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
grant. Installation waits for current writes and maintenance to finish before
changing protection triggers. See the [migration guide](DATABASE_MIGRATIONS.md) and
[remaining acceptance gates](MAINTENANCE_INTEGRATION_ACCEPTANCE.md).
