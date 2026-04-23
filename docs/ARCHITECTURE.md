# Architecture

Shardline is an open, self-hostable content-addressed storage backend with
Xet-compatible protocol support.
It uses a protocol-neutral CAS coordinator with explicit frontend adapters.
Today, the production frontend is Xet: clients upload xorbs and shards, the
server verifies and indexes them, and clients later request file reconstruction
metadata to download only the byte ranges they need.

## Goals

- Speak the public Xet CAS API closely enough for existing Xet-compatible clients.
- Reduce repeated upload and storage costs through chunk-level deduplication.
- Support local and cloud object storage through explicit adapter contracts.
- Run as a single Docker container for small deployments.
- Scale to separate API, transfer, metadata, and object-storage layers for larger
  deployments.
- Keep correctness and integrity checks in the coordinator, not only in clients.

## Non-Goals

- Replacing Git itself.
- Requiring users to abandon existing version-control platforms.
- Building a full hosted code forge.
- Trusting client-provided hashes without verification.
- Making global deduplication visible across tenants by default.

## Component Model

```text
client
  |
  | Xet frontend today
  v
server
  |
  +-- protocol adapters
  +-- auth and scope checks
  +-- Xet xorb validator
  +-- Xet shard validator
  +-- CAS reconstruction planner
  +-- reconstruction cache
  +-- dedupe index
  +-- transfer URL issuer
  |
  +-- index store
  |     +-- file_id -> reconstruction terms
  |     +-- chunk_hash -> stored object hash and chunk range
  |     +-- object_hash -> object location and chunk metadata
  |     +-- retained container hash -> registration state
  |
  +-- object store
        +-- immutable object bytes
        +-- retained container bytes, if configured
```

## Persistence Model

Shardline needs three persistence categories:

- **Object storage**: immutable object bytes and retained container bytes.
- **Index storage**: metadata needed for reconstruction, deduplication, authorization,
  garbage collection, and integrity checks.
- **Record storage**: durable file-version records and derived latest-file records for
  local deployments and repair tooling.

The index crate exposes memory, local SQLite, and Postgres-compatible adapters for these
metadata contracts. Memory adapters are non-durable and intended for contract tests and
embedded development.
Local SQLite adapters support self-hosted single-node operation and operator repair
tooling while keeping payload bytes on the filesystem.
Postgres-compatible adapters provide the durable production metadata path.

The stores must be updated with explicit ordering:

1. Protocol object bytes are validated.
2. Immutable object bytes are written idempotently.
3. Container metadata is validated against existing stored objects.
4. Index rows are committed atomically.
5. File reconstructions become visible after the index commit succeeds.

Shardline can also use a non-authoritative reconstruction cache.
Cache adapters accelerate repeated reconstruction planning but must never become the
source of truth. If a cache entry is missing, stale, or unavailable, the server falls
back to durable metadata and repairs the cache lazily.

## Public API Surface

The current production server exposes the Xet-compatible CAS API:

- `GET /v1/reconstructions/{file_id}`
- `GET /v1/chunks/default/{hash}`
- `POST /v1/xorbs/default/{hash}`
- `POST /v1/shards`

When provider-backed token issuance is enabled, the server also exposes:

- `POST /v1/providers/{provider}/tokens`
- `POST /v1/providers/{provider}/webhooks`

For storage adapters that cannot issue native presigned URLs, the server also exposes a
range-enforced transfer endpoint:

- `GET /transfer/xorb/{prefix}/{hash}`

The Xet-specific route constants, hash/path validation, transfer URL construction,
reconstruction shaping, and protocol-object ingest flow are intentionally isolated
inside the server's `xet_adapter` layer rather than spread through generic backend and
routing code.

The transfer endpoint is an implementation detail.
Reconstruction responses can point to native presigned object-store URLs when an adapter
supports them.

## CLI Shape

Shardline ships as a single CLI with subcommands:

```text
shardline serve
shardline config check
shardline admin token
shardline fsck --root <path>
shardline index rebuild --root <path>
shardline gc --root <path> [--mark] [--sweep] [--retention-seconds <seconds>]
shardline bench
```

The server command is the production entrypoint.
The remaining commands support operability and correctness checks.

For scaled deployments, the same command also supports explicit runtime roles:

```text
shardline serve --role all
shardline serve --role api
shardline serve --role transfer
```

`api` serves control-plane endpoints such as reconstruction lookup, provider-backed
token issuance, and shard registration.
`transfer` serves the large request and response paths: chunk download, xorb upload, and
xorb range transfer.
`all` keeps the single-node behavior and serves both route sets from one process.

## Source Layout

```text
crates/protocol
  Xet protocol types, hash parsing, range parsing, token scopes, xorb validation.

crates/server
  HTTP handlers, generic runtime orchestration, and Xet adapter hosting.

crates/cas
  Protocol-neutral CAS domain, reconstruction planning, dedupe orchestration.

crates/storage
  Object storage traits and adapters.

crates/index
  Metadata persistence traits, file-record contracts, and adapters.

crates/cache
  Reconstruction-cache traits and adapters.

crates/vcs
  GitHub, Gitea, GitLab, and generic provider integration boundaries.

crates/cli
  CLI wiring, config loading, diagnostics, admin commands.
```

The crate boundaries keep protocol handling, server operation, storage, indexing, and
provider integration independent.

`lib.rs` and `mod.rs` files are reserved for module declarations and public re-exports
only. Concrete types, functions, trait implementations, tests, and internal helpers live
in named module files such as `hash.rs`, `store.rs`, or `coordinator.rs`. New modules
should use named files directly; do not introduce `mod.rs` files.

## Concurrency Model

The server is async-first and streams large request and response bodies.
It must not buffer full untrusted uploads or full reconstructed downloads in memory
unless the body is already within an explicit small bound.
Native Xet uploads do not use server-side `incoming` files or shard parsing workspaces
on the data path.
The coordinator consumes bounded request frames, validates protocol objects in memory
under the configured request-size limit, then commits bytes through the selected
object-storage adapter.

Expected concurrency behavior:

- native xorb and shard upload bodies are capped before Xet validation
- native shard metadata sections are counted and bounded before per-section records are
  materialized
- object writes are idempotent by content hash
- shard registration uses a transaction in the index store
- reconstruction planning is read-heavy and avoids coarse locks
- transfer responses stream reconstruction chunks and support range reads and
  backpressure
- local transfer reads use bounded async file buffers after metadata and authorization
  validation

## VCS Integration Boundary

Version-control platforms are permission and repository providers, not the CAS itself.

The integration layer supports:

- issuing read/write CAS tokens after provider permission checks
- mapping repository and revision identity into token scopes
- receiving webhooks for cleanup and lifecycle reconciliation

Provider webhooks are normalized before they reach lifecycle logic.
The current server accepts repository lifecycle events from supported providers and
turns repository deletion into time-bounded retention holds for the affected chunk and
serialized-xorb objects while removing the deleted repository's metadata roots.
That keeps provider-driven cleanup outside the data path while giving garbage collection
a durable grace window.
Repository rename is also applied durably.
`access_changed` and `revision_pushed` are persisted as provider-derived repository
lifecycle state, including the latest observed access-change timestamp and pushed
revision for each provider repository.
That durable state gives repair, auditing, token issuance, and repository-drift checks a
stable source of truth without coupling the CAS core to provider-specific webhook
payloads. Successful provider token issuance reconciles pending lifecycle signals by
recording authorization recheck, cache-invalidation, and drift-check timestamps for the
repository.

The core CAS must remain usable without any platform-specific integration.

Provider adapters are first-class extension points, just like storage adapters.
GitHub, GitLab, Gitea, and generic forges should plug into the same normalized provider
contract so repository hosting logic does not leak into chunking, reconstruction, or
storage code.

The issuance path is explicit:

- provider adapter evaluates repository access for a concrete subject
- only an allowed authorization result becomes a signed CAS token
- the signed token is then used on the normal CAS API

This keeps provider logic out of the CAS core while preserving a single authorization
model on the data plane.
