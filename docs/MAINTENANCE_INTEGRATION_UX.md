# Maintenance without another service

Start Shardline and run maintenance as before:

```bash
shardline serve
shardline fsck
shardline gc --mark --sweep
shardline repair
```

Tracking runs inside these processes. You need no tracker service, extra port,
new mount, schedule or configuration. Commands keep their live output and exit
status. Your existing timer starts GC; starting the server does not start GC.

Follow the [rolling-upgrade procedure](ROLLING_UPGRADE.md) when upgrading an
existing deployment. Pause destructive GC during upgrade and rollback.

## What users gain

- Inspect a saved maintenance result after a restart without repeating the work.
- Nodes sharing the same PostgreSQL database, schema and storage can inspect
  the same history.
- Protected writes return a retryable error when ownership is lost or an earlier
  delete remains uncertain, rather than continuing without the required protection.

History covers fsck, index rebuild, lifecycle repair, GC and storage copies.
Dry runs do not save shared history. Specialized repairs, backups and retention
holds keep their existing diagnostics. SQLite deployments save history in SQLite,
even with S3 payloads; PostgreSQL deployments save it in PostgreSQL. Storage-copy
locations are described in the [CLI reference](CLI.md#durable-maintenance-linux).
For host-local copy history, use `shardline history --local`; this works even
when your normal server configuration selects PostgreSQL. Use `--root` for a
copy's local destination, or its local source when the destination is S3.

If saving history fails, work keeps its original result and emits a warning.
Empty history does not prove that work never ran. Tracking does not make every
job execute exactly once across nodes.

## Check the server or a previous result

Check current readiness at your server address:

```bash
curl -f https://cas.example.com/readyz
```

`shardline health` checks process liveness, not readiness. A completed maintenance
run does not prove that the server or cluster is healthy now.

Inspect saved results with the same configuration and root as your server:

```bash
shardline history
# If you normally use an explicit configuration:
shardline --config /etc/shardline/shardline.toml history
```

Read `state` and follow `next_step`. Inspection needs no admin token or worker
setup and starts no maintenance. A failed or interrupted run may have made
partial changes; check its logs and affected data before repeating it. Preserve
corrupt evidence. If history is unavailable or full, check the original command
output and logs instead of assuming the work failed or did not run.
PostgreSQL row policies can make shared history unavailable. Keep those access
restrictions in place; use the original command's logs to investigate the run.

If a warning names a retained workspace, inspect that directory as its owner:

```bash
shardline history --archive --root /path/from/the/workspace/warning
```

This also reads older private copy archives. It verifies saved evidence without
rewriting files, exporting history or repeating work. Keep the original files
and logs. See the [inspection reference](CLI.md#inspect-maintenance-history)
for pagination and read-only SQLite requirements.

If the command finished but saving shared history was interrupted, a later
maintenance command can publish its verified saved result. It starts fresh work
of its own; it does not repeat the earlier job to repair its history.

## Handle a retryable write

An upload that loses database ownership returns `503` with `Retry-After: 1`.
Send a fresh request after that delay. If failures continue, check database
connectivity and server logs.

For a write blocked by an uncertain earlier delete, let the original GC stop,
then use the same deployment configuration to inspect and repair its history:

```bash
shardline history --pending-deletes
shardline repair
shardline history --pending-deletes
```

Follow each attempt's `next_step`. Repair checks saved replies without deleting
the user object again. A backlog can need more repair runs. Missing or invalid
replies remain blocked; object absence alone cannot settle the attempt.
Unavailable inspection does not establish that retrying is safe.

`Retry-After: 1` suggests when to retry; it does not promise recovery in one
second. See [pending-delete repair](CLI.md#reconcile-pending-remote-deletes).
For other retryable errors, follow the response: provide missing content or
start a fresh application-owned transaction as requested.

Without a valid saved reply, retries may remain blocked indefinitely. There is
no force-clear command. The [recovery runbook](DISASTER_RECOVERY.md#unprovable-remote-deletes)
describes restoring to an independent deployment when that is necessary.

## Upgrade using the existing workflow

1. Pause destructive GC and let existing GC commands finish.
2. For PostgreSQL, run the usual owner-run `shardline db migrate up` with the
   upgraded binary and existing database/schema configuration, even if no
   migrations are pending. Apply any pending migrations using their maintenance
   window requirements. SQLite installs protection during normal startup.
3. Drain and replace server roles using your normal restart procedure. Replace
   the binaries or images used by existing maintenance jobs too.
4. Inspect pending deletes and investigate interrupted work, then resume your
   existing GC schedule.

Runtime accounts need no new grants. Older GC lacks the new delete protection;
keep destructive GC paused during rollback until compatible job binaries are
restored. Reconcile pending deletes before switching storage: copying does not
settle them. For a storage cutover, pause writers and GC as before; saved copy
history does not coordinate the cutover. See [rolling upgrades](ROLLING_UPGRADE.md)
for role order, schema requirements and rollback limits.

Startup checks saved upload progress. Eligible file checksums are verified in
the existing server process while requests are served. Failed or interrupted
verification preserves unfinished progress.

## Optional isolation and history retention

Use `shardline maintenance ...` only when you want an enforced deadline or CPU
limit. Its Linux child lets the controller stop blocked work and dies with its
controller. Ownership checks and delete protection provide write safety.
Ordinary startup and maintenance need no child process or operator task ID.

Cleanup needs no new timer. Verified finished history becomes eligible after
30 days; unfinished and corrupt evidence is retained. Full recording capacity
produces a warning while work continues. Save reports separately for a permanent
archive. History limits do not limit live output or diagnostic exports, and they
are not disk quotas. Large archives can slow inspection and command exit.
Shutdown stops optional history cleanup between storage calls. An active call
may finish; a later normal startup can continue cleanup.
See [history limits](CLI.md#maintenance-history-limits) for details.
