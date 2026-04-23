# Lifecycle Repair

`shardline repair lifecycle` reconciles durable lifecycle metadata that is no longer
consistent with the live object graph.

It is not a replacement for `fsck` and it is not a substitute for `gc`. It sits between
them:

- `fsck` reports integrity and lifecycle drift
- `repair lifecycle` removes stale lifecycle metadata
- `gc` deletes objects only after they remain unreachable across retention

## Command

For normal operator recovery, use the top-level repair command:

```bash
shardline repair
```

It rebuilds derived index state, repairs lifecycle metadata, and runs a final `fsck`.

For lifecycle metadata only:

```bash
shardline repair lifecycle
```

Optional webhook-deduplication retention:

```bash
shardline repair lifecycle \
  --webhook-retention-seconds 2592000
```

Run the command from the project or deployment directory.
Shardline uses the nearest `.shardline/data` state root automatically.
Use `--root <path>` only when operating on a different state root.

When `SHARDLINE_INDEX_POSTGRES_URL` is set, the command reads and updates lifecycle
metadata through the configured Postgres index adapter while still resolving live object
state through the configured object-store adapter.

The command exits with:

- `0` when repair completed
- `2` when repair could not complete because of an operational failure

## What It Repairs

The repair pass derives reachability from current visible latest records, immutable
version records, and retained-shard mappings.

It reconciles three lifecycle surfaces:

- quarantine candidates
- retention holds
- processed webhook delivery claims

For quarantine candidates it removes entries when:

- the object is already missing
- the object is reachable again from live metadata
- an active retention hold already protects the same object

For retention holds it removes entries when:

- the hold is already expired
- the protected object is missing

For processed webhook delivery claims it removes entries when:

- the claim is older than the configured retention window
- the recorded processing timestamp is implausibly far in the future

Repair does not delete object bytes.
It only cleans lifecycle metadata that should no longer influence later collector runs.

## Output

The command prints a summary:

```text
root: /srv/assets/.shardline/data
webhook_retention_seconds: 2592000
scanned_records: 42
referenced_objects: 118
scanned_quarantine_candidates: 6
removed_missing_quarantine_candidates: 1
removed_reachable_quarantine_candidates: 2
removed_held_quarantine_candidates: 1
scanned_retention_holds: 4
removed_expired_retention_holds: 1
removed_missing_retention_holds: 0
scanned_webhook_deliveries: 12
removed_stale_webhook_deliveries: 3
removed_future_webhook_deliveries: 1
```

These counters are intended for operators and automation.
A repair run is safe to repeat.
If no new drift exists, the same run converges to zero removals.

## When To Run It

Typical cases:

- after `fsck` reports stale quarantine, hold, or webhook lifecycle metadata
- after recovery from interrupted lifecycle workflows
- after restoring metadata from backups
- before a destructive `gc --sweep` pass in environments that had prior lifecycle drift

## Safety Model

`repair lifecycle` is conservative:

- it never removes live file metadata
- it never deletes object payloads
- it only removes lifecycle metadata when the current live graph proves the entry is
  stale

That makes it safe to run before garbage collection in self-hosted deployments that need
to re-establish clean lifecycle state after outages or manual intervention.
