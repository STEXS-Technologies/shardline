# Changelog

All notable changes to Shardline are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/), and this project adheres to [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Changed

- Modularized `crates/server/src/local_backend.rs` (700 lines) into 6 focused modules: `mod.rs`, `files.rs`, `xorbs.rs`, `objects.rs`, `records.rs`, `tests.rs`
- Added `cargo-tarpaulin` coverage tooling to the task runner: `make coverage`, `make coverage-html`, `make coverage-json`
- Marked `local_backend` modularization complete in `TODO.md`

### Added

- `CHANGELOG.md` covering full project history

## [1.0.0] - 2026-06-29

### Added

- **Content-addressed storage server** for large binary assets with automatic deduplication across repositories (`f389559`)
- **Xet protocol support** as the default storage protocol with native shard upload, reconstruction, and byte-range requests (`f389559`, `c461d81`)
- **Git LFS protocol frontend** with batch API, object verification, and transfer adapter (`300387a`)
- **Bazel HTTP remote cache frontend** with AC/CAS object storage and action cache support (`300387a`)
- **OCI Distribution protocol frontend** with blob push/fetch, manifest operations, and tag resolution (`300387a`)
- **HuggingFace Hub API compatibility** with REST endpoints for repository CRUD, revision management, and file operations (`d03c763`)
- **Git Smart HTTP protocol** for Hub API: `info/refs` discovery, `upload-pack` (clone/fetch), `receive-pack` (push), pkt-line framing, and Git pack file generation (`88f2a8a`, `cc0f3cf`)
- **Pluggable authentication** via `AuthProvider` trait with 4 adapters: Ed25519 local keys, OIDC, JWKS, and passthrough (`d03c763`)
- **Pluggable storage backends**: local filesystem with symlink-safe path validation, S3-compatible with multipart upload, Postgres metadata (`f389559`, `5ff5e14`, `981b973`)
- **Provider integration** for GitHub, GitLab, and Gitea: webhook handling, token issuance, and repository state reconciliation (`d03c763`)
- **Garbage collection** with quarantine state, configurable retention, orphan inventory, and diagnostics (`d03c763`)
- **Filesystem consistency checks (fsck)** with reconstruction plan validation and lifecycle repair flows (`d03c763`)
- **Index rebuild** from stored metadata and object inventory (`d03c763`)
- **Storage migration** between local and S3 backends (`d03c763`)
- **Backup manifests** for metadata export (`d03c763`)
- **Database migration framework** for Postgres and SQLite with sync verification (`d03c763`)
- **50+ Prometheus metrics** across 9 categories: storage, transfer, xet, protocol, reconstruction, gc, fsck, backend, provider, system (`d03c763`)
- **Structured tracing** via `tracing-subscriber` with `RUST_LOG` env filter and `#[tracing::instrument]` on hot paths (`d03c763`)
- **15 fuzz targets** covering protocol parsing (Xet, LFS, Bazel, OCI), reconstruction, storage races, and CLI (`300387a`, `40ef000`)
- **Async storage fuzz target** validated to 1.2M runs with 12 concurrent access patterns (`40ef000`)
- **6 benchmark suites** for protocol, local backend, OCI, S3, and end-to-end performance (`f389559`)
- **Docker deployment** with Dockerfile, docker-compose (Postgres 16-alpine), and GHCR image publishing (`f389559`, `68e8296`, `d03c763`)
- **Windows CI compatibility** with `cargo check --workspace` guardrail (`5c69e59`)
- **70+ deny-level clippy lints** enforced workspace-wide including `unwrap_used`, `panic`, `todo`, `arithmetic_side_effects` (`f389559`)
- **GitHub issue templates** for bug reports, feature requests, and maintenance tasks (`c508065`)
- **CONTRIBUTING.md** with dev setup, PR workflow, testing, and code standards (`c508065`)
- **`cargo-deny` license and advisory policy** for dependency auditing (`78ff8ac`)
- **`cargo-make` task runner** with 50+ tasks for build, test, lint, fuzz, bench, and CI (`f389559`)

### Changed

- **Streaming SHA-256 digest uploads to S3** — eliminated full-body buffering for content-addressed uploads by streaming through `Sha256` hasher during multipart transfer (`5ff5e14`)
- **S3-native multipart OCI uploads** — bypassed local staging file for S3 backends with resumable upload sessions (`981b973`)
- **CAS-agnostic runtime foundation** — decoupled storage/index layer from protocol specifics with frontend routing via `--frontend` / `SHARDLINE_SERVER_FRONTENDS` (`c461d81`)
- **Comprehensive module splits** across 91 files: `protocol_routes` (8 files), `postgres_backend` (4 files), `upload_ingest` (3 files), `local_sqlite` (6 files), config directory restructure (`d03c763`)
- **New crates extracted** from server: `fsck`, `gc`, `hub_api`, `metrics`, `oci_adapter`, `protocol_adapters`, `server_core`, `xet_adapter` — workspace grew from 10 to 20 crates (`d03c763`)
- **All crate versions bumped to 1.0.0** with centralized `[workspace.dependencies]` (`737c625`)
- **README rewritten** as user-facing product page with validated coverage matrix (`3b86a0b`, `c461d81`)
- **ARCHITECTURE.md rewritten** for 20-crate layered dependency graph with Hub API, Git Smart HTTP, metrics, and auth sections (`1203d8e`)
- **DEPLOYMENT.md expanded** with Hub API, authentication providers, and metrics documentation (`1203d8e`)
- **`rustfmt.toml` applied** across 107 files (`d03c763`)
- **Docker entrypoint** switched to `docker-entrypoint.sh`, Compose includes Postgres on port 5532 (`c461d81`, `d03c763`)

### Fixed

- **Non-Unix compile surface restored** — removed `#[cfg(not(unix))] compile_error!` blocks from cli, index, server, and storage crates; added conservative path validation fallbacks for Windows (`5c69e59`)
- **Git pack encoding** — fixed size varint to use 4 bits in first byte per Git pack spec (`cc0f3cf`)
- **Hub API revision ordering** — added `rowid DESC` tiebreaker to `list_revisions` (`cc0f3cf`)

### Performance

- **Streaming S3 uploads** — SHA-256 digest computed incrementally during multipart transfer instead of buffering entire body (`5ff5e14`)
- **Reduced upload staging I/O** — S3-native multipart uploads skip local staging file entirely (`981b973`)

### Security

- **TOCTOU race windows documented** in S3 adapter for `begin_content_addressed_upload`, `put_file_if_absent`, `copy_object_if_absent` (`40ef000`)
- **Symlink escape prevention** — `ensure_directory_path_components_are_not_symlinked` applied to backend root, stats traversal, and metadata paths (`f389559`)
- **`cargo-deny` policy** enforced in CI for license compliance and security advisories (`78ff8ac`)

### Documentation

- Added contribution workflow templates and PR guidelines (`c508065`)
- Rewrote production readiness and compatibility claims (`3b86a0b`)
- Removed 22 files of local dev tracking docs (-1157 lines) (`818e94d`)
- Updated security audit documentation (`f5d8cac`)
- Documented async storage TOCTOU races with 1.2M-run fuzz validation (`40ef000`)
- Updated all architecture, deployment, and Hub API docs for 20-crate structure (`1203d8e`)

[Unreleased]: https://github.com/STEXS-Technologies/shardline/compare/v1.0.0...HEAD
[1.0.0]: https://github.com/STEXS-Technologies/shardline/releases/tag/v1.0.0
