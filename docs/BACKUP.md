# Backup Manifest

`shardline backup manifest` writes a JSON inventory for the active deployment.

The manifest is adapter-neutral.
It records the configured metadata backend, object backend, object keys, object lengths,
adapter-provided checksums when available, and durable metadata counts.

It does not copy object bytes.
Large deployments should pair this manifest with the native backup or replication
mechanism of the selected object store.

## Command

```bash
cd /srv/assets
mkdir -p reports

shardline backup manifest \
  --output reports/backup-manifest.json
```

For interactive use, Shardline stores local state under `.shardline/data` in the current
project directory. When a command runs inside that project, `--root` can be omitted.
Use `--root <path>` only when operating on a different state root.

For Postgres-backed deployments, set `SHARDLINE_INDEX_POSTGRES_URL` in the environment
before running the command.

For S3-compatible object storage, set the same object-store environment variables used
by `shardline serve`.

## What The Manifest Contains

The JSON object includes:

- `manifest_version`
- `metadata_backend`
- `object_backend`
- `latest_records`
- `version_records`
- `reconstruction_rows`
- `dedupe_shard_mappings`
- `quarantine_candidates`
- `retention_holds`
- `webhook_deliveries`
- `provider_repository_states`
- `objects`
- `object_count`
- `object_bytes`

Each object entry contains:

- `key`
- `length`
- `checksum`

`checksum` is `null` when the object adapter cannot provide a stable checksum from
metadata alone.

## Scale Behavior

The command inventories object metadata through the configured object-store adapter.
It does not read object bodies and it does not stage payload data in temporary
directories.

The output file can still be large because it contains one entry per stored object.
Write it to durable operator storage with enough capacity for the deployment inventory.

## Restore Use

Use the manifest to confirm that restored metadata and object storage describe the same
deployment boundary:

```bash
cd /srv/assets
mkdir -p reports

shardline backup manifest \
  --output reports/post-restore-manifest.json

shardline repair
```

For full restore, recover object storage and metadata from their native backup systems,
then run `shardline repair` before returning traffic.
