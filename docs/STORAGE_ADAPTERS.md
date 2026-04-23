# Storage Adapters

Shardline separates immutable object bytes from metadata/index state.
This keeps storage providers easy to extend without weakening CAS correctness.

## Object Storage

Object storage owns immutable xorb bytes and, optionally, retained shard bytes.

Initial adapters:
- local filesystem
- S3-compatible object storage

Other object stores can fit the same contract, for example:
- Google Cloud Storage
- Azure Blob Storage
- Cloudflare R2 through the S3-compatible adapter where possible
- custom enterprise object stores

## Object Adapter Contract

The object adapter must provide:

- idempotent write by content-addressed key
- bounded streaming write
- range read without full-object buffering
- full-object read for validation and repair tooling
- object existence check
- optional native presigned ranged GET URL
- deletion for garbage collection
- metadata retrieval for object length and checksum where supported

Current Rust shape:

```rust
pub trait ObjectStore {
    type Error;

    fn put_if_absent(
        &self,
        key: &ObjectKey,
        body: ObjectBody<'_>,
        expected: &ObjectIntegrity,
    ) -> Result<PutOutcome, Self::Error>;

    fn read_range(
        &self,
        key: &ObjectKey,
        range: ByteRange,
    ) -> Result<Vec<u8>, Self::Error>;

    fn contains(&self, key: &ObjectKey) -> Result<bool, Self::Error>;

    fn metadata(
        &self,
        key: &ObjectKey,
    ) -> Result<Option<ObjectMetadata>, Self::Error>;

    fn list_prefix(
        &self,
        prefix: &ObjectPrefix,
    ) -> Result<Vec<ObjectMetadata>, Self::Error>;

    fn visit_prefix<Visitor, VisitorError>(
        &self,
        prefix: &ObjectPrefix,
        visitor: Visitor,
    ) -> Result<(), VisitorError>
    where
        Self::Error: Into<VisitorError>,
        Visitor: FnMut(ObjectMetadata) -> Result<(), VisitorError>;

    fn delete_if_present(
        &self,
        key: &ObjectKey,
    ) -> Result<DeleteOutcome, Self::Error>;
}
```

Presigned URLs and native async read streams still belong in production adapters, but
inventory, metadata, bounded range reads, and idempotent delete are now part of the
required lifecycle contract.
Use `visit_prefix` for garbage collection, index rebuild, and operational stats so an
adapter can page or stream inventory without forcing callers to allocate the full object
listing first.

## Local Filesystem Adapter

The local adapter is required for development, small self-hosted deployments, and test
fixtures.

Shardline now ships a concrete local filesystem object adapter behind the `ObjectStore`
contract.

The local server backend already uses that adapter for chunk writes, chunk reads, chunk
existence checks, and local chunk inventory.
HTTP transfer paths use the local adapter to derive validated object paths, then stream
stored chunk bytes through bounded async file reads.
Cloud adapters should expose native read streams or presigned ranged URLs so the server
does not need to allocate full objects during transfer mediation.

Rules:
- Keys must never map directly to unchecked user-provided paths.
- Object paths must be derived from validated content hashes.
- Writes use temporary files and atomic no-overwrite installation where the platform
  supports it.
- Existing objects with the same key must be verified or treated as conflict.
- Partial files left by failed writes must not be visible as valid objects.
- Range reads must enforce inclusive byte bounds.

Suggested layout:

```text
objects/
  xorbs/
    default/
      ab/
        cd/
          <hash>.xorb
  shards/
    <hash>.shard
tmp/
```

## S3-Compatible Adapter

The S3-compatible adapter is the first production cloud adapter.

Rules:
- Use conditional writes where supported.
- Store xorbs under deterministic keys.
- Accept direct in-memory payloads instead of staging local temporary files.
- Use server-mediated ranged transfer when presigning is disabled or unavailable.
- Prefer native async ranged reads or presigned ranged GET URLs when the adapter can
  provide them.
- Avoid putting secrets, tokens, or tenant identifiers in object keys when not required.
- Treat object-store ETags as provider metadata, not as protocol hashes.

Suggested key layout:

```text
xorbs/default/<first-2>/<next-2>/<hash>.xorb
shards/<first-2>/<next-2>/<hash>.shard
```

Runtime environment:

```text
SHARDLINE_OBJECT_STORAGE_ADAPTER=s3
SHARDLINE_S3_BUCKET=asset-cas
SHARDLINE_S3_REGION=us-east-1
SHARDLINE_S3_ENDPOINT=https://s3.example.com
SHARDLINE_S3_ACCESS_KEY_ID=<access-key>
SHARDLINE_S3_SECRET_ACCESS_KEY=<secret-key>
SHARDLINE_S3_ACCESS_KEY_ID_FILE=/run/secrets/shardline/s3-access-key-id
SHARDLINE_S3_SECRET_ACCESS_KEY_FILE=/run/secrets/shardline/s3-secret-access-key
SHARDLINE_S3_SESSION_TOKEN_FILE=/run/secrets/shardline/s3-session-token
SHARDLINE_S3_KEY_PREFIX=<optional-prefix>
```

For production deployments, prefer the `_FILE` variables over direct credential values.
Direct values and the matching `_FILE` variable are mutually exclusive; if both are set,
startup fails closed before the S3 client is built.
Credential files must be regular non-symlink files, are read through the bounded
secret-file path, and should not be world-readable.
The shipped Kubernetes manifests mount runtime secrets with `defaultMode: 0440` and
`fsGroup: 999`; non-Kubernetes hosts should use equivalent owner/group scoping such as
`0640` or stricter.

The native Xet e2e suite can exercise an S3-compatible adapter when the test-specific
environment is present:

```text
SHARDLINE_S3_E2E_BUCKET=shardline
SHARDLINE_S3_E2E_REGION=us-east-1
SHARDLINE_S3_E2E_ENDPOINT=http://127.0.0.1:19000
SHARDLINE_S3_E2E_ACCESS_KEY_ID=shardline
SHARDLINE_S3_E2E_SECRET_ACCESS_KEY=shardline-dev-password
SHARDLINE_S3_E2E_ALLOW_HTTP=true
```

The server routes xorb upload, shard retention, xorb metadata validation, shard dedupe
lookup, range transfer, and object inventory through the selected object adapter.
The native upload hot path does not stage request bodies in an `incoming` directory or
parse shards through a local workspace.
Local filesystem deployments may create temporary files inside the local object adapter
while atomically installing objects.
S3-backed deployments use the S3 adapter for immutable object writes and do not require
local disk for upload-body persistence.
Inventory consumers use the adapter visitor contract, so S3-backed garbage collection
and rebuild can consume listed objects incrementally through the object-store API.

## Index Storage

Index storage owns queryable metadata.

Initial adapters:
- memory adapter for contract tests and embedded development
- local SQLite metadata adapter for single-node deployments and operator tooling
- Postgres-compatible SQL for production metadata

The index must support:
- file reconstruction lookup by file ID
- xorb metadata lookup by hash
- chunk dedupe lookup by chunk hash
- shard registration idempotency
- transactional shard registration
- durable garbage collection quarantine state
- token scope and repository mapping, if auth is stored locally

Current synchronous Rust shape for local adapters:

```rust
pub trait IndexStore {
    type Error;

    fn reconstruction(
        &self,
        file_id: &FileId,
    ) -> Result<Option<FileReconstruction>, Self::Error>;

    fn contains_xorb(&self, xorb_id: &XorbId) -> Result<bool, Self::Error>;

    fn quarantine_candidate(
        &self,
        object_key: &ObjectKey,
    ) -> Result<Option<QuarantineCandidate>, Self::Error>;

    fn list_quarantine_candidates(
        &self,
    ) -> Result<Vec<QuarantineCandidate>, Self::Error>;

    fn upsert_quarantine_candidate(
        &self,
        candidate: &QuarantineCandidate,
    ) -> Result<(), Self::Error>;

    fn delete_quarantine_candidate(
        &self,
        object_key: &ObjectKey,
    ) -> Result<bool, Self::Error>;
}
```

That quarantine state is the durable source of truth for retention windows and staged
deletion planning.

Shardline now ships concrete memory and local filesystem index adapters.

The memory adapter is non-durable.
It exists for adapter contract tests, embedded development flows, and contract coverage
that does not require durable metadata.

The local filesystem adapter provides:

- reconstruction persistence
- xorb presence markers
- durable quarantine state

Both adapters satisfy the same lifecycle contract.
Postgres-compatible adapters must satisfy the asynchronous index-store contract as well.

The Postgres-compatible adapter provides the durable async path for:

- reconstruction lookup and replacement
- xorb presence lookup and idempotent insert
- durable quarantine lookup, listing, upsert, and deletion

The schema lives in `migrations/20260417000000_metadata_store.up.sql`.

## Record Storage

Record storage owns durable file-version records and visible latest-file records for
deployments that use the local metadata layout.

The file-record domain model and `RecordStore` port live with index contracts so server
operations can consume metadata through an adapter boundary.

Current adapters:

- memory record adapter for contract tests and embedded development
- local record adapter backed by SQLite metadata rows
- Postgres-compatible record adapter for durable metadata deployments

The local record adapter currently provides:

- version-record inventory in `shardline_file_records`
- latest-record inventory in `shardline_file_records`
- record reads for local integrity checks
- live chunk-reference scanning for local garbage collection
- atomic latest-record replacement for repair workflows
- stale latest-record deletion
- stable logical record locators for repository-scoped files
- record modification timestamps used by local head reconstruction

Local single-node deployments store durable metadata in
`.shardline/data/metadata.sqlite3`. Legacy filesystem metadata layouts are imported once
on first open and then retired behind the same record/index contracts.
SQL and other record/index adapters should expose equivalent rebuild operations through
their own metadata contracts instead of depending on local paths.

The Postgres-compatible record adapter stores opaque record locators, visible latest
records, immutable version records, repository scope keys, and JSON record bodies in
`shardline_file_records`.

## Index Consistency

Shard registration must be atomic from the client's perspective.
A file reconstruction must not become visible until all referenced xorbs are durable and
all metadata rows are committed.

Recommended transaction order:

1. Verify shard bytes.
2. Verify referenced xorb metadata exists.
3. Insert or confirm shard registration.
4. Insert xorb metadata discovered from the shard.
5. Insert chunk dedupe mappings.
6. Insert file reconstruction rows.
7. Commit.

If any step fails, the file reconstruction must remain invisible.

## Adapter Test Matrix

Every object adapter must pass:

- put new object
- put existing identical object
- reject or repair existing conflicting object
- read exact range
- reject invalid range
- read missing object
- interrupted write does not expose partial object
- return metadata for an existing object
- list objects under a validated prefix
- delete an existing object idempotently
- presign range when supported

Every index adapter must pass:

- register shard idempotently
- reject missing referenced xorb
- look up reconstruction by file ID
- look up dedupe by chunk hash
- enforce transaction rollback
- persist and replace quarantine candidates
- list quarantine candidates for sweep planning
- delete quarantine state after recovery or successful sweep
