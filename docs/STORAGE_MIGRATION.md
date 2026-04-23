# Storage Migration

`shardline storage migrate` copies immutable payload objects between object-storage
adapters.

It does not rewrite repository metadata, reconstruction rows, provider state, lifecycle
state, or cache entries.
Use it when moving payload bytes to a new local state root, S3-compatible bucket, or
S3-compatible prefix before switching the server configuration.

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
The command copies payload objects below each root's `chunks/` object-store directory.

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

Writes are idempotent.
If the destination already has an identical object, the command counts it as already
present. If the destination has the same key with different bytes, the adapter rejects
the write and the migration fails closed.

The command reports:

- `scanned_objects`
- `scanned_bytes`
- `inserted_objects`
- `already_present_objects`
- `copied_bytes`

Run `shardline backup manifest` against both source and destination configuration after
a migration to compare object counts and byte totals before switching production
traffic.
