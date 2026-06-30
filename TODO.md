# Shardline TODO

> Not tracked by git. Living document.

## Completed

### Refactoring (all done)
- [x] backend.rs deduplication (-82 lines)
- [x] macOS /var symlink fix (platform-aware symlink resolution)
- [x] local_sqlite.rs split (4,033 → 6 files)
- [x] ServerConfigError collapse (61 → 49 variants)
- [x] Error hierarchy refactoring (shared types → server-core)
- [x] xet_adapter extraction (2,633 lines → own crate)
- [x] provider_events extraction (814 lines → own crate)
- [x] rebuild extraction (803 lines → own crate)
- [x] gc extraction (1,129 lines → own crate)
- [x] fsck extraction (1,685 lines → own crate)
- [x] OCI adapter extraction (916 lines → own crate)
- [x] Protocol adapters extraction (LFS + Bazel → own crate)
- [x] Config module reorganization (mod.rs + env.rs + secrets.rs + tests.rs)
- [x] CLI main.rs output formatting (print_summary methods)
- [x] Migration sync CI check
- [x] app/tests/ → integration test binary
- [x] bench.rs split into scenario modules
- [x] rustfmt.toml added + cargo fmt applied
- [x] Binary compile error fix (RedactedDbUrl visibility)
- [x] macOS anchored_fs.rs O_NOFOLLOW fix
- [x] README rewrite (user-facing, product phasing)
- [x] Module splits: protocol_routes (8 files), postgres_backend (4 files), upload_ingest (3 files)
- [x] Hub API metadata extraction to crates/index (HubStore trait, SQLite + Postgres)
- [x] Shared metrics crate (crates/metrics/) wired into all 20 workspace crates

### New Features
- [x] HuggingFace Hub API compatibility (crates/hub_api/)
- [x] Provider-agnostic JWT/auth integration (AuthProvider trait)
- [x] Hub API auth integration (HubAuth wrapping AuthProvider)
- [x] Hub API persistent metadata (HubStore trait, SQLite + Postgres adapters)
- [x] Hub API Git protocol support (Smart HTTP: info/refs, upload-pack, receive-pack)
- [x] Prometheus metrics endpoint (50+ metrics across 9 categories)
- [x] Tracing integration (tracing-subscriber, #[instrument], structured spans)
- [x] Updated docs for new features

## Hub API — Known Limitations

- **No model card or metadata search** — ✅ Added: model card endpoint (`GET /api/{type}/{ns}/{repo}/modelcard`), repo search (`GET /api/{type}/search?q=...`), revisions list (`GET /api/{type}/{ns}/{repo}/revisions`). Model card content served via inline_content storage.
- **No dataset viewer** — ✅ Added: parquet listing (`GET /api/datasets/{ns}/{repo}/parquet`), first-rows (`GET /api/datasets/{ns}/{repo}/first-rows`), paginated viewer (`GET /api/datasets/{ns}/{repo}/viewer/{split}`). Supports CSV and JSONL.
- **No webhooks or callbacks** — ✅ Added: webhook CRUD (`POST/GET/DELETE /api/{type}/{ns}/{repo}/webhooks`), `HubWebhook` struct, 4 HubStore methods, `shardline_hub_webhooks` table (Postgres + SQLite migrations). Delivery engine wired: POST with HMAC-SHA256 signatures on commit events.
- **Single-process only** — ✅ Hub API now shares the main server's Postgres connection pool when configured. SQLite remains per-process by design. Postgres is required for multi-process shared deployments.

## New Features

### High Priority

- [x] **Prometheus metrics endpoint**
  - `/metrics` endpoint with standard Prometheus exposition format
  - 50+ metrics across 9 categories (storage, transfer, xet, protocol, reconstruction, gc, fsck, backend, provider, system)
  - Registered as Axum middleware for automatic request tracking
  - Token-gated via `SHARDLINE_METRICS_TOKEN_FILE`

- [x] **Hub API auth integration**
  - Connected Hub API to the `AuthProvider` trait via `HubAuth`
  - Bearer token parsing, scope checks on write endpoints
  - Anonymous access when no auth provider configured

- [x] **Hub API persistent metadata**
  - Replaced in-memory stores with `HubStore` trait
  - SQLite adapter (local) and Postgres adapter (production)
  - 4 tables: repos, revisions, file_entries, lfs_objects
  - `BoxedHubStore` for type-erased storage

### Medium Priority

- [ ] **Web UI dashboard**
  - React SPA for browsing stored files, inspecting xorbs, uploading data
  - Storage statistics overview
  - File browser with reconstruction details
  - Xorb inspector with chunk metadata
  - Drag-and-drop upload with progress
  - Table preview using DuckDB WASM (query CSV/Parquet in-browser)
  - Separate `crates/web/` or static files served by the server

- [x] **Hub API Git protocol support**
  - Implemented `git push` and `git pull` via smart HTTP protocol
  - Git pack generation with real tree/blob/commit objects
  - LFS pointer blob generation for tracked files
  - 13 integration tests passing

## Codebase Modularization

Large files that have been split into submodules:

- [x] **`crates/server/src/app/protocol_routes`** — split into 8 files across 4 protocols
  - `mod.rs` (243 lines), `bazel.rs` (105 lines), `lfs.rs` (193 lines)
  - `oci/mod.rs` (226 lines), `oci/path.rs` (65 lines), `oci/blob_upload.rs` (321 lines),
    `oci/manifest.rs` (374 lines), `oci/tags.rs` (221 lines), `oci/token.rs` (324 lines)

- [x] **`crates/server/src/config`** — reorganized into `mod.rs` + `env.rs` + `secrets.rs` + `tests.rs`

- [x] **`crates/server/src/postgres_backend`** — split into 4 files
  - `mod.rs`, `upload.rs`, `read.rs`, `stats.rs`

- [x] **`crates/server/src/upload_ingest`** — split into 3 files
  - `mod.rs`, `body_reader.rs`, `chunk_store.rs`

- [x] **`crates/index/src/local_sqlite`** — split into 6 files
  - `mod.rs`, `index_store.rs`, `async_index_store.rs`, `record_store.rs`, `helpers.rs`, `tests.rs`

- [x] **`crates/server/src/local_backend`** — split into 6 files
  - `mod.rs` (core struct, construction, accessors, stats), `files.rs` (upload, reconstruction, download)
  - `xorbs.rs` (xorb/shard operations), `objects.rs` (raw object store operations)
  - `records.rs` (record lookup), `tests.rs` (16 tests)

## Observability

- [x] **Migrate to `tracing` instead of current logging**
  - Replaced `log` crate usage with `tracing` throughout server and CLI
  - `tracing-subscriber` with `env-filter` and `fmt` in CLI `main.rs`
  - `#[tracing::instrument]` on `serve()`, `read_chunk()`, `upload_xorb()`, `upload_shard()`
  - `RUST_LOG` env filter, default `info`
  - Structured spans for upload, download, reconstruction

## Low Priority

- [x] **Update ARCHITECTURE.md and DEPLOYMENT.md**
  - Updated for 20-crate structure, layered dependency graph
  - Documented the pluggable auth system, Hub API, Git Smart HTTP
  - Documented the Prometheus metrics endpoint and tracing
