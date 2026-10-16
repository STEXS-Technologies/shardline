# Storage Migration

`shardline storage migrate` copies immutable payload objects between object-storage
adapters.

It does not rewrite repository metadata, reconstruction rows, provider state, lifecycle
state, or cache entries.
Use it when moving payload bytes to a new local state root, S3-compatible bucket, or
S3-compatible prefix before switching the server configuration.

For an active deployment, use your existing maintenance window for the final
copy and cutover:

1. Drain writers and stop destructive GC against the source and destination.
2. Inspect pending deletes with `shardline history --pending-deletes` using the
   source deployment's configuration, or use its authenticated admin API. Run
   `shardline repair` with its existing configuration to reconcile saved replies.
   Investigate any delete that remains pending before switching storage.
3. Copy payloads and verify them with the destination configuration. Keep the
   existing metadata available; this command does not move the database.
4. Switch storage configuration, check readiness and fsck, then resume traffic
   and the existing GC schedule.

Copying an object does not settle an uncertain deletion. The copy API receives
storage endpoints, not a metadata database or schema, so it cannot enforce
the deployment's database ownership or remote-delete markers. Existing-object
checks make repeated copies safe against identical bytes; they do not make a
live copy a consistent snapshot of changing storage.

## Command

Local state root to local state root:

```bash
shardline storage migrate \
  --from local \
  --from-root /srv/assets/.shardline/data \
  --to local \
  --to-root /srv/assets-new/.shardline/data
```

Inventory without writing:

```bash
shardline storage migrate \
  --from local \
  --from-root /srv/assets/.shardline/data \
  --to local \
  --to-root /srv/assets-new/.shardline/data \
  --dry-run
```

Limit the run to one object-key prefix:

```bash
shardline storage migrate \
  --from local \
  --from-root /srv/assets/.shardline/data \
  --to local \
  --to-root /srv/assets-new/.shardline/data \
  --prefix xorbs/default/
```

For local endpoints, `--from-root` and `--to-root` are Shardline state roots.
The command copies immutable payload objects below each root's object-store directories —
`chunks/`, `xorbs/default/`, and `shards/` — filtered by `--prefix` when given (default:
all payload objects).

If `--from-root` is omitted for a local source, Shardline uses the same project-local
root discovery as other operator commands.
`--to-root` is required for a local destination so a migration cannot accidentally write
back into the active source.

## S3-Compatible Endpoints

S3-compatible endpoints use prefixed environment variables.

Source S3:

```text
SHARDLINE_MIGRATE_FROM_S3_BUCKET
SHARDLINE_MIGRATE_FROM_S3_REGION
SHARDLINE_MIGRATE_FROM_S3_ENDPOINT
SHARDLINE_MIGRATE_FROM_S3_ACCESS_KEY_ID
SHARDLINE_MIGRATE_FROM_S3_SECRET_ACCESS_KEY
SHARDLINE_MIGRATE_FROM_S3_SESSION_TOKEN
SHARDLINE_MIGRATE_FROM_S3_KEY_PREFIX
SHARDLINE_MIGRATE_FROM_S3_ALLOW_HTTP
SHARDLINE_MIGRATE_FROM_S3_VIRTUAL_HOSTED_STYLE_REQUEST
```

Destination S3:

```text
SHARDLINE_MIGRATE_TO_S3_BUCKET
SHARDLINE_MIGRATE_TO_S3_REGION
SHARDLINE_MIGRATE_TO_S3_ENDPOINT
SHARDLINE_MIGRATE_TO_S3_ACCESS_KEY_ID
SHARDLINE_MIGRATE_TO_S3_SECRET_ACCESS_KEY
SHARDLINE_MIGRATE_TO_S3_SESSION_TOKEN
SHARDLINE_MIGRATE_TO_S3_ACCESS_KEY_ID_FILE
SHARDLINE_MIGRATE_TO_S3_SECRET_ACCESS_KEY_FILE
SHARDLINE_MIGRATE_TO_S3_SESSION_TOKEN_FILE
SHARDLINE_MIGRATE_TO_S3_KEY_PREFIX
SHARDLINE_MIGRATE_TO_S3_ALLOW_HTTP
SHARDLINE_MIGRATE_TO_S3_VIRTUAL_HOSTED_STYLE_REQUEST
```

Only `BUCKET` is required.
`REGION` defaults to `us-east-1`. For credentials, prefer the `_FILE` variables on
production hosts. A direct credential variable and its matching `_FILE` variable are
mutually exclusive; the migration command rejects that configuration before building the
S3 client.

The source side supports the same credential-file indirection as the destination:
`SHARDLINE_MIGRATE_FROM_S3_ACCESS_KEY_ID_FILE`,
`SHARDLINE_MIGRATE_FROM_S3_SECRET_ACCESS_KEY_FILE`, and
`SHARDLINE_MIGRATE_FROM_S3_SESSION_TOKEN_FILE` (with the same mutual-exclusion rule
against their direct counterparts).

Example: local state root to S3-compatible storage:

```bash
export SHARDLINE_MIGRATE_TO_S3_BUCKET=asset-cas
export SHARDLINE_MIGRATE_TO_S3_REGION=us-east-1
export SHARDLINE_MIGRATE_TO_S3_ENDPOINT=https://s3.example.com
export SHARDLINE_MIGRATE_TO_S3_ACCESS_KEY_ID_FILE=/run/secrets/shardline/s3-access-key-id
export SHARDLINE_MIGRATE_TO_S3_SECRET_ACCESS_KEY_FILE=/run/secrets/shardline/s3-secret-access-key

shardline storage migrate \
  --from local \
  --from-root /srv/assets/.shardline/data \
  --to s3
```

## Safety Model

Each source object is buffered for verification and copying, with a 64 MiB
per-object limit. A larger object fails the real copy; a successful dry-run
inventory does not establish that every object can be copied. This limit is
per stored object, not per reconstructed file.

Writes are idempotent.
If the destination already has an identical object, the command counts it as already
present. If the destination has the same key with different bytes, the adapter rejects
the write and the migration fails closed.

New maintenance history stays in metadata and is outside payload inventory.
Recognized legacy task evidence and verified saved delete replies are excluded
from the copy. Unrecognized or damaged bytes under the legacy task namespace
retain the existing copy behavior; the command does not discard them merely
because of their filename. Keep the source evidence for investigation. A copy
does not establish that another deployment owns that history or completed its
work.

The command reports:

- `scanned_objects`
- `scanned_bytes`
- `inserted_objects`
- `already_present_objects`
- `copied_bytes`

Run `shardline backup manifest` against both source and destination configuration after
a migration to compare object counts and byte totals before switching production
traffic.

## Migration Testing

Every supported storage representation — local filesystem and S3-compatible object
storage, SQLite and Postgres metadata — is covered by a verification procedure in
[Storage Migration Testing](STORAGE_MIGRATION_TESTING.md). It includes the full
`from × to` migration matrix with the exact commands for each combination, the
dry-run counter comparison for `storage migrate`, and the `serve` + `fsck`
verification step after every migration.

For recovery scenarios that move payload bytes between stores (node loss, object-store
loss, cross-node recovery), see [Disaster Recovery](DISASTER_RECOVERY.md).
