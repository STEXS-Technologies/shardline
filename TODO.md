# Shardline TODO

> Not tracked by git. Living document.

## Completed

### Refactoring (all done)
- [x] backend.rs deduplication (-82 lines)
- [x] macOS /var symlink fix (platform-aware symlink resolution)
- [x] local_sqlite.rs split (4,033 → 3 files)
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

### New Features
- [x] HuggingFace Hub API compatibility (crates/hub_api/)
- [x] Provider-agnostic JWT/auth integration (AuthProvider trait)
- [x] Updated docs for new features

## Hub API — Known Limitations

- **In-memory state only** — repository and file metadata is not persisted to disk or a
  database. A server restart loses all Hub API state.
- **No authentication** — the Hub API currently accepts all requests anonymously. Token
  exchange endpoints return placeholder tokens. Production use requires pairing with an
  external reverse proxy or future auth integration.
- **No Git protocol** — the Hub API provides REST endpoints only. Direct `git push` to
  Hub-style repositories is not supported; use the CLI upload/download workflow.
- **No webhooks or callbacks** — repository event notifications are not implemented.
- **Single-process only** — the in-memory stores are not shared across scaled API
  instances. The Hub frontend is intended for single-node deployments today.
- **No model card or metadata search** — repository README, model card, and search
  endpoints are not yet implemented.
- **No dataset viewer** — dataset-specific preview and streaming endpoints are not yet
  implemented.

## New Features

### High Priority

- [ ] **Prometheus metrics endpoint**
  - `/metrics` endpoint with standard Prometheus exposition format
  - Metrics to track:
    - `shardline_http_requests_total` (counter, labels: method, path, status)
    - `shardline_http_request_duration_seconds` (histogram, labels: method, path)
    - `shardline_upload_bytes_total` (counter)
    - `shardline_download_bytes_total` (counter)
    - `shardline_objects_stored` (gauge)
    - `shardline_chunks_stored` (gauge)
    - `shardline_dedupe_ratio` (gauge)
    - `shardline_gc_runs_total` (counter)
    - `shardline_fsck_runs_total` (counter)
  - Per-frontend and per-backend breakdowns
  - Use `prometheus` or `metrics` crate
  - Register as Axum middleware for automatic request tracking
  - Enable with `SHARDLINE_METRICS_ENABLED=true` or `--metrics` flag

- [ ] **Hub API auth integration**
  - Connect Hub API to the `AuthProvider` trait
  - Token exchange endpoints should use the configured auth provider
  - Whoami endpoint should verify tokens via the provider
  - Currently accepts all requests anonymously — needs auth before production use

- [ ] **Hub API persistent metadata**
  - Replace in-memory `RepoStore`, `TreeStore`, `LfsStore` with SQLite or Postgres
  - Persist repository state, file entries, HEAD revisions across restarts
  - Reuse existing `shardline-index` crate for metadata storage

### Medium Priority

- [ ] **Web UI dashboard**
  - React SPA for browsing stored files, inspecting xorbs, uploading data
  - Storage statistics overview
  - File browser with reconstruction details
  - Xorb inspector with chunk metadata
  - Drag-and-drop upload with progress
  - Table preview using DuckDB WASM (query CSV/Parquet in-browser)
  - Separate `crates/web/` or static files served by the server

- [ ] **Hub API Git protocol support**
  - Implement `git push` and `git pull` via smart HTTP protocol
  - Git pack Negotiate, packfile upload, index generation
  - This would make Shardline a full self-hosted Git + HuggingFace Hub

## Codebase Modularization

Large files that need splitting into submodules:

- [ ] **`crates/server/src/app/protocol_routes.rs`** (1,886 lines)
  - Split by protocol frontend: xet_routes, lfs_routes, oci_routes, bazel_routes
  - Each frontend's routes in its own file

- [ ] **`crates/server/src/config.rs`** (~1,119 lines after reorg)
  - Still large after reorg. Extract ServerConfig builder pattern into config/builder.rs
  - Extract validation logic into config/validation.rs

- [ ] **`crates/server/src/postgres_backend.rs`** (1,112 lines)
  - Extract connection setup and pool management
  - Extract query helpers

- [ ] **`crates/server/src/upload_ingest.rs`** (960 lines)
  - Split into upload_ingest/chunker.rs, upload_ingest/ingestor.rs

- [ ] **`crates/server/src/local_backend.rs`** (700 lines)
  - Split into local_backend/mod.rs + local_backend/records.rs

- [ ] **`crates/cli/src/main.rs`** (575 lines)
  - Still has inline formatting for some commands
  - Move remaining command dispatch to dedicated modules

## Observability

- [ ] **Migrate to `tracing` instead of current logging**
  - Replace `log` crate usage with `tracing` throughout
  - Add structured spans for: upload, download, reconstruction, GC, fsck
  - Add span fields: file_id, hash, repository_scope, duration
  - Use `tracing-subscriber` with JSON output for production
  - Use `tracing-subscriber` with pretty output for development
  - Config: `SHARDLINE_LOG_FORMAT=json|pretty`, `SHARDLINE_LOG_LEVEL=info|debug|trace`
  - This unifies logging, metrics, and distributed tracing under one facade

## Low Priority

- [ ] **Update docs to reflect new architecture**
  - ARCHITECTURE.md, DEPLOYMENT.md need updates for 19-crate structure
  - Document the pluggable auth system
  - Document the HuggingFace Hub API frontend
  - Document the Prometheus metrics endpoint
