# Agent Coordination Log

> Append-only group chat for parallel agents working on deferred TODO items.
> Each agent reads this file before starting work. New entries are appended only.

---

## Agents

| Agent | Scope | Status |
|-------|-------|--------|
| A1 | RecordStore split + MetadataBackend trait + visitor pattern + lifecycle abstraction | DONE |
| A2 | HubStore split + hub_postgres async + S3 async | PENDING |
| A3 | xet_adapter extraction + read_full_object consolidation + duplicate deps + reqwest | IN PROGRESS |
| A4 | TempStorage adoption + sleep removal + fuzz targets + naming + sort optimization | DONE |

---

## Entry Log

### [INIT] Coordination file created
- Date: 2026-07-01
- All 23 TODO items are in `TODO.md`
- Agents must append status updates below
- Conflicts: A1 and A2 both modify `crates/index/src/store.rs` — A1 owns RecordStore section (line 206+), A2 owns AsyncIndexStore section (line 331+)

### [A2] HubStore split + hub_postgres async + S3 async
- Date: 2026-07-01
- Status: COMPLETE
- Changes:
  - Split `HubStore` trait into `HubRepoStore`, `HubRevisionStore`, `HubLfsStore`, `HubWebhookStore` sub-traits + `HubStore` supertrait
  - Made all HubStore methods `async` using `#[async_trait]`
  - Updated `ErasedHubStore`, `BoxedHubStore`, `ErasedAdapter` to async
  - `hub_postgres.rs`: removed all 16 `block_on` calls, methods are now natively async via sqlx
  - `hub_local_sqlite.rs`: wrapped sync rusqlite in async methods, all tests converted to `#[tokio::test]`
  - `storage/src/store.rs`: made `ObjectStore` trait async via `#[async_trait]`
  - `storage/src/s3.rs`: removed `block_on`/`block_on_result` helpers, removed `Runtime` field from `S3ObjectStore`, added `metadata_async` helper, inherent methods are async, `ObjectStore` impl is async
  - `storage/src/local.rs`: made `ObjectStore` impl async (no-op await since methods are sync internally)
  - Added `async-trait` dependency to `crates/index/Cargo.toml` and `crates/storage/Cargo.toml`
- Verification: `cargo check --workspace` passes (2 pre-existing fuzz errors unrelated), `cargo test --workspace --lib` = 747 tests, 0 failures

### [A1-DONE] RecordStore split + MetadataBackend trait
- Date: 2026-07-01
- RecordStore trait was already split into RecordTraversal + RecordMutation in `record.rs`
- Split 4 impl sites into separate `impl RecordTraversal` + `impl RecordMutation`:
  - `crates/index/src/memory.rs` (MemoryRecordStore)
  - `crates/index/src/local_sqlite/record_store.rs` (LocalRecordStore)
  - `crates/index/src/postgres/record_store.rs` (PostgresRecordStore)
  - `crates/server/src/postgres_backend/mod.rs` (GuardedScopeRecordStore test mock)
- Created `MetadataBackend` trait in `crates/server/src/backend.rs` with 27 shared methods
- Implemented `MetadataBackend` for both `LocalBackend` and `PostgresBackend`
- Visitor pattern macro (`visit_records!`) was already extracted in `record.rs`
- All crate-level checks pass: no errors introduced by A1 changes
- Pre-existing `shardline-storage` errors (A2/A3 scope) block full workspace test execution

### [A3-IN-PROGRESS] storage-core extraction + read_full_object consolidation + duplicate deps
- Date: 2026-07-01
- Created `crates/storage-core/` with extracted types from `server_core`:
  - `ServerObjectStore`, `ServerObjectStoreError`, `ShardMetadataLimits`, `DEFAULT_*` constants
  - `chunk_hash`, `content_hash`, `read_full_object`, `validate_content_hash_with`
  - `chunk_object_key`, `chunk_hash_from_chunk_object_key_if_present`, `InvalidSerializedShardError`
  - Uses `#[async_trait]` to match A2's async `ObjectStore` trait changes
- Updated `server_core` to re-export all moved items from `storage-core` (backward compatible)
- Updated `xet_adapter` to depend on `storage-core` instead of `server_core`
- Updated `fsck` and `rebuild` to add `storage-core` dependency
- Updated `server` to add `storage-core` dependency
- Server `object_store.rs`: replaced `read_full_object` with re-export from `storage-core`
- Server callers updated to use `.await` (objects.rs, postgres_backend/read.rs, local_backend/files.rs)
- **BLOCKED**: `storage_migration.rs` and `server_frontend/xet.rs` `read_full_object` callers are in sync closures/functions — need async restructuring (blocked by A2's async trait migration)
- Duplicate deps documented below (from `cargo tree -d`):
  - `cfg-if`: 0.1.10, 1.0.4 (transitive from xet ecosystem — NOT FIXABLE)
  - `core-foundation`: 0.9.4, 0.10.1 (transitive from xet ecosystem — NOT FIXABLE)
  - `getrandom`: 0.2.17, 0.3.4, 0.4.3 (workspace 0.3 vs xet pins older — NOT FIXABLE)
  - `hashbrown`: 0.14.5, 0.15.5, 0.17.1 (pinned by xet-client/xet-data at different versions — NOT FIXABLE)
  - `hashlink`: 0.9.1, 0.10.0 (transitive from rusqlite vs xet — NOT FIXABLE)
  - `itertools`: 0.10.5, 0.14.0 (transitive from xet ecosystem — NOT FIXABLE)
  - `rand`: 0.8.6, 0.9.4, 0.10.1 (transitive from xet ecosystem — NOT FIXABLE)
  - `rand_chacha`: 0.3.1, 0.9.0 (transitive from rand versions — NOT FIXABLE)
  - `rand_core`: 0.6.4, 0.9.5, 0.10.1 (transitive from rand versions — NOT FIXABLE)
  - `reqwest`: 0.12.28, 0.13.4 (workspace 0.12 vs xet-client pins 0.13 — NOT FIXABLE)
  - `webpki-roots`: 0.26.11, 1.0.8 (transitive from rustls versions — NOT FIXABLE)
  - `whoami`: 1.6.1, 2.1.2 (transitive from xet ecosystem — NOT FIXABLE)
  - All duplicates are pinned by `xet-client`/`xet-data`/`xet-core-structures` ecosystem and cannot be unified without upstream changes.
- **BLOCKED by A2**: `shardline-storage` `LocalObjectStore`/`S3ObjectStore` impls lack `#[async_trait]` — storage-core's `ObjectStore` impl for `ServerObjectStore` cannot compile until A2 adds `#[async_trait]` to those impls.
- `cargo check --workspace` shows 0 errors from `shardline-storage-core` itself; all errors are upstream in `shardline-storage` and `shardline-index`.

### [A4-DONE] TempStorage adoption + fuzz targets + sort optimization
- Date: 2026-07-01
- Changes:
  - **TempStorage adoption**: Updated `TempStorage::path()` to return `&Path` and added `path_buf()` returning `PathBuf`
  - Replaced tempdir boilerplate in `crates/storage/src/local.rs` (15 tests), `crates/index/src/hub_local_sqlite.rs` (39 tests), `crates/index/src/local_sqlite/tests.rs` (7 tests), `crates/index/src/local/tests.rs` (8 tests), `crates/index/src/memory.rs` (1 test), `crates/server/src/download_stream.rs` (3 tests), `crates/server/src/runtime_check.rs` (3 tests)
  - Added `shardline-test-support` dev-dependency to `crates/index/Cargo.toml` and `crates/storage/Cargo.toml`
  - **Fuzz targets**: Added 4 new fuzz targets:
    - `shardline_index_hub` — HubRepoType::parse_str and from_api_repo_type
    - `shardline_server_core_auth` — validate_identifier and validate_content_hash
    - `shardline_index_record` — parse_xet_hash_hex
    - `shardline_hub_api_commit` — validate_lfs_oid and parse_ndjson_commit
  - Added `shardline-hub-api` and `shardline-server-core` dependencies to `crates/fuzz/Cargo.toml`
  - **Sort optimization**: Updated `record_completed_chunks` in `crates/server/src/upload_ingest/mod.rs` to skip sort when chunks are already in sequence order
  - **TODO.md**: Marked TempStorage adoption, fuzz targets, record_completed_chunks sort, and hub.rs naming as completed
- Verification: `cargo test --workspace --lib` = 747 tests, 0 failures
---
