# Index Rebuild

`shardline index rebuild` reconstructs visible latest-record state from immutable
version records.

Immutable version records are the durable source of truth and latest-file records are
derived visible state used for the current file head.

When latest-file state is missing, stale, or partially lost, rebuild restores it from
the version-record store.

## Command

Current command:

```bash
shardline index rebuild
```

Run the command from the project or deployment directory.
Shardline uses the nearest `.shardline/data` state root automatically.
Use `--root <path>` only when operating on a different state root.

When `SHARDLINE_INDEX_POSTGRES_URL` is set, the command rebuilds latest-file state
inside Postgres-backed metadata while using the configured object-store adapter for
retained shard inventory.
Local deployments use the resolved state root for local metadata.

The command exits with:

- `0` when rebuild completed without non-fatal issues
- `1` when rebuild completed but found invalid version records
- `2` when rebuild could not complete because of an operational failure

## What It Rebuilds

The rebuild pass:

- scans immutable version records through the configured record-store adapter
- groups immutable version records by repository scope and file ID
- validates each version record reconstruction plan before it can become a visible head
- selects one visible head for each file
- recreates or updates the corresponding latest record through the same adapter
- removes stale latest records that no longer have any version history
- scans reconstruction rows through the configured index-store adapter
- removes stale reconstruction rows whose file IDs are no longer backed by immutable
  version records
- scans retained-shard objects through the configured object-store adapter
- rebuilds the dedupe-shard mapping index from retained-shard contents
- removes stale dedupe-shard mappings that no longer correspond to retained shards

Retained-shard scanning uses the bounded in-memory shard parser and object-store
inventory visitor.
It does not create local shard parsing workspaces, which keeps rebuild
usable with local and S3-compatible object-storage adapters that implement the visitor
contract. Reconstruction cleanup is conservative: if rebuild finds invalid version
records, it does not delete reconstruction rows.

## Head Selection Rule

The rebuild implementation picks the visible head by:

1. newest version-record modification time
2. content hash as a deterministic tie-breaker
3. record locator ordering as a final deterministic tie-breaker

That rule is sufficient for the current record-store adapters.
More advanced index adapters should store explicit head state instead of deriving it
from record timestamps.

## Output

The command prints a summary:

```text
root: /srv/assets/.shardline/data
scanned_version_records: 42
scanned_retained_shards: 18
rebuilt_latest_records: 7
unchanged_latest_records: 35
removed_stale_latest_records: 2
scanned_reconstructions: 42
unchanged_reconstructions: 40
removed_stale_reconstructions: 2
rebuilt_dedupe_shard_mappings: 4
unchanged_dedupe_shard_mappings: 96
removed_stale_dedupe_shard_mappings: 1
issue_count: 0
```

When invalid version records are encountered, each issue is reported with a stable
machine-readable kind:

```text
issue: invalid_version_record_json location=record:version:asset.bin:abcdef... detail=expected value at line 1 column 1
```

Issue kinds currently include:

- `invalid_version_record_json`
- `invalid_version_file_id`
- `invalid_version_content_hash`
- `invalid_version_repository_scope`
- `version_path_mismatch`
- `invalid_version_reconstruction_plan`
- `invalid_retained_shard`

## Scope

The implementation rebuilds latest-file state from immutable version records through the
configured record-store boundary.
Local SQLite deployments rebuild latest rows from immutable version rows inside
`.shardline/data/metadata.sqlite3`. Postgres-backed deployments rebuild latest rows from
version rows inside the same metadata store.
The rebuild pass also scans retained-shard objects through the configured object-store
adapter and reconstructs the dedupe-shard mapping index from the retained-shard
contents. Reconstruction cleanup runs through the same index-store adapter as normal
reconstruction lookup, so local and Postgres-backed deployments use the same operator
path.

It does not rebuild:

- provider root state
- retention or quarantine metadata
- object-store adapter manifests

Provider root state, retention metadata, and quarantine metadata are lifecycle state,
not derived reconstruction state.
Use `shardline fsck` to validate them and `shardline repair lifecycle` to prune stale
lifecycle entries.
