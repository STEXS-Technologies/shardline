# Storage Migration Testing

This procedure verifies upgrades across every supported storage representation:

- object storage: local filesystem (`ObjectStorageAdapter::Local`) and S3-compatible
  (`ObjectStorageAdapter::S3`, selected with `SHARDLINE_OBJECT_STORAGE_ADAPTER`)
- metadata: SQLite at the state root (default) and Postgres
  (`SHARDLINE_INDEX_POSTGRES_URL`)

Every migration follows the same shape: take an inventory before, move one durable
surface, verify the target with `serve` and `fsck` before cutting traffic, and take
an inventory after. Object bytes and metadata move through different tools and are
verified separately.

## Object-Storage Migrations

Payload bytes move with `shardline storage migrate` (see
[Storage Migration](STORAGE_MIGRATION.md)). It copies immutable objects between
object-store adapters and does not rewrite metadata.

```bash
# always dry-run first: inventory without writing
shardline storage migrate \
  --from local \
  --from-root /srv/assets/.shardline/data \
  --to s3 \
  --dry-run

# compare the dry-run counters, then run the real migration
shardline storage migrate \
  --from local \
  --from-root /srv/assets/.shardline/data \
  --to s3
```

The command reports `scanned_objects`, `scanned_bytes`, `inserted_objects`,
`already_present_objects`, and `copied_bytes`. The dry-run and the real run must agree
on `scanned_objects`; `copied_bytes` plus `already_present_objects` (in bytes) must
equal the expected payload total.

After the copy, take inventory on both configurations and compare:

```bash
shardline backup manifest \
  --output reports/from-manifest.json

# same command run against the destination configuration
shardline backup manifest \
  --output reports/to-manifest.json
```

Switch the server configuration to the destination object store, then run:

```bash
shardline index rebuild
shardline fsck
```

`index rebuild` recreates derived latest-file state against the new object adapter;
`fsck` proves the metadata still matches the moved chunk bytes. Return traffic only
after `fsck` exits `0`.

## Metadata Backend Switches

Metadata moves between SQLite and Postgres at the process configuration boundary —
`SHARDLINE_INDEX_POSTGRES_URL` set or unset — with the schema managed by
`shardline db migrate` (see [Database Migrations](DATABASE_MIGRATIONS.md)).

SQLite to Postgres:

```bash
export SHARDLINE_INDEX_POSTGRES_URL='postgres://shardline:replace-me@postgres:5432/shardline'

shardline db migrate status
shardline db migrate up
```

`db migrate status` fails closed if the target database contains a version unknown to
the running binary or a checksum mismatch; `db migrate up` applies only pending
migrations, each in its own transaction.

Postgres to SQLite:

```bash
unset SHARDLINE_INDEX_POSTGRES_URL

# serve against the state-root metadata; rebuild derived state from
# immutable version records in the SQLite store
shardline index rebuild
shardline fsck
```

> **Scope note**: no inventory command bulk-copies metadata rows between SQLite and
> Postgres. The test validates schema bring-up (`db migrate up`/`status`) and that the
> running server operates correctly against the target adapter (`serve` + `fsck` +
> `backup manifest`). To move live metadata rows into a fresh target database, restore
> them from a database-native backup; `fsck` and the manifest comparison quantify any
> mismatch before traffic is cut.

## Migration Matrix

Each cell lists the object-bytes command and the metadata command for the `from → to`
move. "no change" means that surface stays on the same representation. The diagonal
is a no-op. Verification for every cell: `backup manifest` before, `serve` + `fsck`
after, `backup manifest` after.

| From → To | local object / SQLite | local object / Postgres | S3 object / SQLite | S3 object / Postgres |
| --- | --- | --- | --- | --- |
| local object / SQLite | no change | metadata: `db migrate up` + `SHARDLINE_INDEX_POSTGRES_URL` | object: `storage migrate --from local --to s3` | object: `storage migrate --from local --to s3`; metadata: `db migrate up` |
| local object / Postgres | metadata: unset `SHARDLINE_INDEX_POSTGRES_URL`; `index rebuild` | no change | object: `storage migrate --from local --to s3` | object: `storage migrate --from local --to s3` |
| S3 object / SQLite | object: `storage migrate --from s3 --to local --to-root <dir>` | object: `storage migrate --from s3 --to local --to-root <dir>`; metadata: `db migrate up` | no change | metadata: `db migrate up` + `SHARDLINE_INDEX_POSTGRES_URL` |
| S3 object / Postgres | object: `storage migrate --from s3 --to local --to-root <dir>`; metadata: unset `SHARDLINE_INDEX_POSTGRES_URL`; `index rebuild` | object: `storage migrate --from s3 --to local --to-root <dir>` | metadata: unset `SHARDLINE_INDEX_POSTGRES_URL`; `index rebuild` | no change |

S3 endpoints on either side use the `SHARDLINE_MIGRATE_FROM_S3_*` or
`SHARDLINE_MIGRATE_TO_S3_*` environment variables
([Storage Migration](STORAGE_MIGRATION.md#s3-compatible-endpoints)). For a local
destination, `--to-root` is required; for a local source, `--from-root` can be
omitted and the nearest state root is discovered.

## Verification Checklist

After every cell in the matrix:

1. `shardline backup manifest` against the source configuration (baseline)
2. run the object and metadata commands for the cell (dry-run first for
   `storage migrate`; compare `scanned_objects` and `copied_bytes` with the real run)
3. switch the server configuration to the destination representations
4. `shardline db migrate status` when the destination metadata is Postgres
5. `shardline index rebuild` when object storage or the metadata backend changed
6. `shardline fsck` — must exit `0` before cutting traffic
7. `shardline backup manifest` against the destination configuration; object count,
   byte total, and metadata counts must match the baseline
8. a live `serve` smoke test: one upload, one download of the latest version, and one
   historical version fetch

## Automated Coverage

The Postgres metadata side of the matrix is exercised end-to-end by
`crates/shardline/tests/db_migrate_e2e.rs`
(`db_migrate_applies_reports_and_reverts_live_postgres_schema`): it applies the
bundled migrations to a live Postgres, reports migration state, and reverts them,
which is the schema half of every Postgres cell above. Object-storage migration
behavior (idempotent writes, fail-closed conflicts, counter reporting) is covered by
the `storage migrate` command tests.
