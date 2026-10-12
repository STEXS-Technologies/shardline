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

## When a delete is interrupted

A later hold or covered write to an object with an unresolved delete returns a
clear retryable error. Other objects remain available. Rejected updates preserve
the previous metadata. Current native uploads also check the chunk references
inside their uploaded data.

On Postgres, this also covers publishing OCI blobs and manifests, changing a
manifest tag, and removing a deletion tombstone. A pending manifest or its
media-type sidecar blocks publication. Rejection preserves the previous tag
and tombstone; publication can succeed after the delete is reconciled.
During a rolling upgrade, released 1.11.3 servers reject covered OCI writes too,
but return a generic server error. Updated servers explain when retry is safe.
New manifest uploads also check their validated config, layer, child-manifest
and subject references in the publication transaction. Legacy tag-only writes
cannot inspect those payload references and still need additional protection.

New Postgres-backed manifests stay hidden until publication succeeds. If an
upload is interrupted after storing bytes, those bytes remain hidden from
manifest reads; a successful retry can publish them. Existing published
manifests remain visible during retries. This adds a storage existence check,
with no extra service or grants.
OCI publication also stops if the database connection holding its GC lock is
lost. A rejected publication preserves the previous tag and manifest.
Partial-upload writes use the same protection. If ownership is lost before a
part commits, that part does not advance recorded progress. The client can
check the upload and retry.
Upload-session creation and completion changes also stop when that connection
is lost. No extra service or configuration is needed.
The usual owner-run database upgrade adds hidden manifests to the existing OCI
deletion inventory. Ordinary GC also discovers missing entries from interrupted
uploads, including uploads made by accounts with row policies. It reclaims them
after the configured retention period and preserves active holds. This uses the
existing maintenance account. If that account also has active row restrictions,
automatic discovery remains unsupported and reports a warning.

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
