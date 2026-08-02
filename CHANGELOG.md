# Changelog

All notable changes to Shardline are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/), and this project adheres to [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Upgrade Note — Storage Format Evolution

This release changes the physical storage representation from fixed-size chunking (uncompressed)
to CDC chunking with LZ4 compression and optional xorb container packing (XorbCdcV1).

**No data migration required.** Old records written by v1.2.x and earlier remain fully readable,
reconstructable, and protected from garbage collection. The upgrade is safe for existing
deployments — no downtime or maintenance window needed for data format reasons.

#### What changes

| Aspect | Before (v1.2.x) | After (this release) |
|---|---|---|
| Chunking | Fixed-size 64KB chunks (default) | CDC (content-defined), target 64KB |
| Compression | None | LZ4 (`lz4_flex::compress_prepend_size`) |
| Containerization | None | Xorb packing at upload finish |
| Chunk hash | Raw bytes | Raw bytes (unchanged — dedup works across formats) |
| Storage format per record | Implicit (fixed chunk) | Explicit `storage_repr` field (`fixed_chunk_v1` or `xorb_cdc_v1`) |

#### Backward compatibility

- **WholeFileV1** (v1.0.0–present): Single-object storage, `chunk_size=0` → `ReferencedObjectTerms` layout.
  Read path unchanged (`reconstruct_referenced_object_file_bytes`).
- **FixedChunkV1** (v1.0.0–v1.2.2): Uncompressed fixed-size chunks. Decompression is decided
  by the record's `storage_repr`, so these records are served raw without LZ4 decoding.
- **XorbCdcV1** (new): CDC + LZ4 compression + optional xorb packing. `storage_repr =
  xorb_cdc_v1` always triggers LZ4 decompression, regardless of how `packed_end` compares
  to `chunk.length` (the compressed size can equal the raw size for small or incompressible
  payloads). Xorb-backed files use a single-GET fast path.

#### GC compatibility

All three formats are handled correctly:
- Individual chunk paths and xorb container paths are referenced by the GC.
- Xorb containers are resolved to their constituent chunk hashes (new in this release).
- Old format records without the `storage_repr` field default to `FixedChunkV1`.

#### Monitoring

New metrics available:

| Metric | Type | Labels |
|---|---|---|
| `shardline_objects_by_repr_total` | Counter | `representation` (`whole_file_v1`, `fixed_chunk_v1`, `xorb_cdc_v1`) |

Operators can query format distribution across the repository:
```
shardline_objects_by_repr_total
```

#### Configuration

- `SHARDLINE_CHUNK_SIZE` now defaults to `64KB` (the pre-existing fixed-size default
  was `65536` / 64KB via `SHARDLINE_CHUNK_SIZE_BYTES`, and in practice was always
  sized per-deployment). The CDC target chunk size is 64KB; minimum chunk is 8KB;
  maximum is 128KB.

### Fixed

- **Download stream**: corrected `lz4_flex` size-header parsing (u32 LE, not u64 BE) so compressed
  payloads are decompressed and reconstructed correctly.
- **Download stream**: decompression is now selected by the record's `storage_repr`
  (`xorb_cdc_v1`) instead of comparing `packed_end` to `chunk.length`, so XorbCdcV1 payloads
  whose compressed size equals the raw size are still decompressed correctly.
- **Content identity**: chunk hashes are computed over raw bytes, not compressed bytes, so dedup
  works across compressed and uncompressed records.
- **Decompression safety**: added a decompression safety cap with warn logs on failures.

### Changed

- **GC metrics**: split into mark/sweep phases and wired to the direct-read metric.
- **GC sweep**: xorb cache sidecar files are deleted when their parent xorb is swept.

### Performance

- **S3 uploads**: skip the HEAD request for first-time chunk uploads.
- **GC**: xorb chunk hashes are cached to avoid re-parsing the container on every GC run.

### Testing

- Raised upload-ingest coverage to 93%+ and added compression metric tests.
- Added CDC chunker and xorb packer round-trip fuzz targets.
- Added e2e tests for mixed-format dedup, GC + xorb mixed-format sweep, xorb packer edge cases,
  xorb range-span reconstruction, xorb chunk-hash cache, and cache-sidecar cleanup.

## [1.2.2] - 2026-07-28

Patch release fixing Xet protocol uploads broken by stubbed BG4 compression.

### Fixed

- **Xet BG4 compression**: shardline-xet-core stubbed ByteGrouping4LZ4 as plain LZ4,
  causing all xet-data client uploads to fail with HTTP 400. Delegates to upstream
  xet-core-structures for correct BG4 interleaving/deinterleaving.
- **CI license check**: MPL-2.0 added to deny.toml allow list for xet-core-structures
  transitive dependencies.

### Testing

- Added 23 BG4 compression unit tests (roundtrips, panic safety, cross-scheme, proptest).
- Added adapter integration test for BG4 xorb validate/decode roundtrip.
- Added missing validate_xorb_object call to existing BG4 integration test.

## [1.2.1] - 2026-07-27

Patch release establishing recoverable upload lifecycles, shared chunk-backed storage,
bounded runtime resource use, and hardened release and deployment boundaries. Public LFS,
Bazel CAS, OCI, Hub, and Xet protocol surfaces remain compatible.

### Changed

- **Shared chunk-backed storage**: standard bulk-data frontends now use the shared CAS
  coordinator and fixed-size chunks, while native Xet retains content-defined chunking.
- **Authoritative upload lifecycle**: memory, SQLite, and PostgreSQL index stores persist
  legal upload-intent transitions so incomplete writes can be reconciled deterministically.
- **Bounded runtime behavior**: asynchronous object I/O, admission control, quotas, timeouts,
  and explicit overload responses bound memory, blocking work, and request concurrency.

### Fixed

- **Visibility and recovery consistency**: file records now form the publication boundary,
  and GC, FSCK, repair, and protocol handlers share the same reachability model.
- **Legacy read compatibility**: existing whole-object records remain readable while new
  writes use chunk-backed records.
- **CI reliability**: database-gated coverage is included in the coverage ratchet, shared
  backend assertions tolerate parallel test state, and Docker test identifiers are unique.

### Security

- Authentication and route policies fail closed, object and filesystem boundaries use typed
  validation, and production containers run with non-root security defaults.
- Release artifacts and container images are verified, provenance-attested, and published
  through restartable release steps with expiring dependency exceptions.

### Operations

- Apply migration `20260721000000_upload_intents` before accepting writes with this version.
- Roll API and transfer roles together. Preserve database and object-store backups if an
  emergency rollback is required after new chunk-backed records have been published.

## [1.2.0] - 2026-07-24

Minor release adding Ed25519 asymmetric token verification, Hub authentication coverage,
and more reliable container-backed CI coverage. There are no intentional breaking API or
configuration changes from `1.1.0`.

### Added

- **Ed25519 authentication**: local authentication can validate Ed25519-signed tokens,
  including PEM and PKCS#8 signing-key configuration and key-identifier selection.
- **Hub authentication coverage**: end-to-end coverage verifies that the Hub `whoami`
  endpoint requires authentication.

### Fixed

- **Authentication provider validation**: require an HMAC signing key only for the local
  authentication provider, allowing externally validated token providers to serve routes
  without an unused local key; increased the accepted token-string size limit for
  asymmetric tokens.
- **Postgres CI stability**: isolated integration-test repository state and applied database
  migrations in PostgreSQL-backed end-to-end tests.
- **S3 and GC coverage stability**: removed timing-sensitive outage assertions and corrected
  the quarantine-object fixture used by integrity validation.

### Performance

- No intentional performance changes in this release.

### Documentation

- Authentication, deployment, protocol, compatibility, and performance documentation updated
  for Ed25519 token verification.

## [1.1.0] - 2026-07-23

Minor release adding config file support (shardline.toml + .env), governance documentation, documentation drift fixes, and CI/publish pipeline hardening. There are no intentional breaking API or configuration changes from `1.0.1`.

### Added

- **Config file support**: `--env-file` flag for `.env` file loading (`ab351f3`), `--config` flag for `shardline.toml` with auto-detection at `/etc/shardline/shardline.toml` (`2271e90`, `57c74e6`)
- **TOML config with `${VAR}` interpolation**: `load_server_config_from_env_with_toml` deserializes TOML directly into `ServerConfig`, supporting shell env > `.env` file > `shardline.toml` > defaults precedence (`8749068`, `897ef14`)
- **Config test suite**: 25 unit tests for TOML config loader (`67eae9b`), 17 integration tests for config parsing and validation (`57b0892`), plus CLI e2e tests for `--env-file` and `--config` flags (`4d511d1`)
- **Governance docs**: `SECURITY.md`, `CODE_OF_CONDUCT.md`, blank issue creation enabled (`1e9c3ce`, `adcd3b2`)

### Changed

- **K8s ConfigMap**: converted from 20 separate env vars to a single `shardline.toml` mounted at `/etc/shardline/` (`dcc3cd2`, `57c74e6`)
- **K8s deployments**: `api-deployment.yaml` and `transfer-deployment.yaml` updated to auto-detect config at `/etc/shardline/` — `--config` flag removed (`57c74e6`)
- **Docker deployment**: updated for config file usage in `DEPLOYMENT.md` (`dcc3cd2`)

### Fixed

- **Documentation drift**: 31 doc files audited 4x — stale crate paths, Ed25519/HMAC references, wrong env vars, and include_str paths corrected (`078aeb4`, `a44494b`, `b453645`, `9ab4ac4`, `1e9c3ce`, `adcd3b2`)
- **Publish pipeline**: missing crates (`shardline-auth`, `shardline-validation`) added to release publish list, version extraction sed pattern fixed, migrations directory restored in release packaging
- **Audit findings**: env var name mismatches, dead config fields, and test gaps resolved (`067b582`)
- **Native Xet E2E timeout**: prevented by fixing the split-role e2e test setup (`5eaa444`)
- **Clippy warnings**: `shadow-unrelated` and other lint warnings resolved (`a71745d`)

### Performance

- No intentional performance changes in this release (focus was config file support and documentation).

### Documentation

- `CLI.md`: global `--env-file` and `--config` flags documented (`c2977ff`)
- `DEPLOYMENT.md`: config file usage, Docker examples, K8s ConfigMap documentation (`dcc3cd2`, `c2977ff`)
- K8s manifests reviewed and simplified for config file auto-detection (`57c74e6`)
- README cleaned up: removed "first" and "production ready" claims (`b700927`)
- Architecture, protocol, and ops docs audited for accuracy (`9ab4ac4`, `a44494b`)

## [1.0.1] - 2026-07-21

Patch release focused on correctness, security hardening, test coverage, and regression protection.
There are no intentional breaking API or configuration changes from `1.0.0`.

### Added

**Protocol & Feature Coverage**
- OCI blob `DELETE` per spec with reference checking: implements the full OCI lifecycle (`ae6a9e2`, `631be63`)
- Git LFS `DELETE` endpoint for object removal (`9f7adf2`)
- Git Smart HTTP reference deletion (`9b06cac`)
- Redis TLS and mutual-TLS integration support (`63d0751`)
- Native HuggingFace CLI download coverage and smoke tests (`63d0751`)
- Provider multi-webhook sequence dispatch and S3 session abort (`8be650c`)
- Codeberg provider adapter alongside GitHub, GitLab, Gitea (`c853763`)
- Hub API: model card rendering, metadata search, and macOS compatibility fixes
- `MemoryReconstructionCache` concurrency stress tests with interleaved read/write patterns (`2845e66`)
- GC and upload interleaving concurrency test (`adb666c`)
- Lifecycle repair classification fuzz target (`e89b274`)
- Postgres CI integration with container-backed jobs (`adb666c`)
- S3 OCI multipart session upload E2E test with MinIO (`f4dd1b8`)
- Container-backed MinIO and Redis integration test framework (`c853763`)
- Concurrent load benchmark with 5 new fuzz targets (`a1e91ae`)
- `xet-core` shim crate isolating xet dependencies from production builds

**Test Coverage (22 crates, ~8.1k tests)**
- Major coverage campaign pushed aggregate lib-only line coverage to **93.79%** (up from ~82%), with a CI-enforced coverage ratchet.
  - **Pushed to ≥96%**: `cas` (100%), `protocol_adapters` (100%), `rebuild` (99%), `provider_events` (99%), `metrics` (98%), `oci_adapter` (98%), `protocol` (97%), `fsck` (97%), `gc` (97%), `server` (97%), `cache` (96%), `xet_adapter` (96.7%), `hub_api` (96%), `cli` (96%), `vcs` (99.8%), `server_core` (98.4%)
  - **Infrastructure-dependent** (need real Postgres/S3/MinIO for full coverage): `storage` (77-100% per file), `index` (postgres modules constrained), `xet_core` (90-100% per file)
- **E2E test suite** grown from near-zero to comprehensive coverage:
  - 36 → 43 → 50 → 65 → 75 → 92 → 100 → 112 → 114 sequential E2E tests
  - Full protocol coverage: OCI, LFS, Bazel, Xet, Hub API, Git Smart HTTP
  - Auth-enabled tests (100+), branch audit (92), deep feature gap coverage (75)
  - Cross-protocol consistency tests (23 new, 64 total)
  - Provider webhook tests (114) and token tests (112 total)
  - OCI-specific: referrers, pagination, anonymous pulls, schema2, tag overwrite
  - LFS batch edge cases, range variants, dedup variants, body limits
  - Docker-backed: S3 LFS/OCI/Bazel, S3 dedup, Postgres OCI
  - Passthrough auth, CORS survey, cross-repo OCI mount
  - HTTP-level E2E test framework parameterized for Postgres, S3, and combined backends (`a1e91ae`)
- **15+ fuzz targets** including lifecycle classification, stateful Hub-reference fuzzing, deterministic corpus replay, and scheduled bounded campaigns
- **Property tests** for Hub references and Git pkt-line encoding
- **Coverage ratchet**: line-coverage floor enforced in CI to prevent regression
- **Unit tests** for previously untested crates: clock, cache/error, cache/disabled, frontend, index/provider, local_backend records, postgres_backend read/metrics/backend

### Changed

- **Modularized `local_backend`**: split `crates/shardline-server/src/local_backend/` into focused domain modules (`b62ec09`)
- **LLVM source-based coverage** via `cargo-llvm-cov` with CI enforcement and a line-coverage ratchet (`0997dea`)
- **Trait splits**: `RecordStore` / `HubStore` and `IndexStore` god traits split into focused sub-traits (parallel read/write, metadata, lifecycle) (`a19cbb9`)
- **Split `ServerConfig`**: removed dual-impl pattern, cleaned public re-exports (`3d0d72a`)
- **Split `ServerError`**: moved CLI output to dedicated types, added test lint overrides (`3d0d72a`)
- **Moved E2E tests** out of crate dirs into dedicated `e2e/` folder, simplified CI filtering
- **Finished CAS-agnostic runtime foundation**: decoupled storage/index from protocol specifics with frontend routing via `--frontend` / `SHARDLINE_SERVER_FRONTENDS`
- **`LocalEd25519Provider` renamed to `LocalHmacProvider`**: the implementation always used HMAC-SHA256, not Ed25519 (`1f7a7a3`)
- **Replaced all `#[allow(arithmetic_side_effects)]`** with explicit checked arithmetic (`4a0978e`)
- **308 KiB of dev tracking files removed** from workspace (`.slim/`, `.opencode/`, `AUDIT_REPORT.md`, `TEST_GAPS.md`, coverage artifacts, profraw/lcov files, TODO.md, coordination notes)
- `cargo-deny` removed from CI pipeline (advisory responsibility delegated to Dependabot)
- Workspace clippy clean enforced across all crates

### Fixed

**Security & Audit Hardening**
- **9 batches of adversarial audit findings** remediated:
  - Batch 1 & 2: error variant bounds, prefix validation, CRIT-001 `put_overwrite` non-atomic on S3, `start_after` cleanup (`ff3b778`, `8837a22`)
  - Batch 3: CRIT-002 TOCTOU, STOR-001/002/005/011/027, SHARDLINE-001 (`af304b9`)
  - Batch 4: SHARDLINE-002/003, STOR-013/025, OCI-002, 7 remaining bugs (`24184eb`)
  - Batch 5 & 6: OCI-001/003, GC-001, >5GiB copy via streaming multipart (`ea69d0f`, `3830d25`)
  - Batch 7 & 8: OCI-004/005/006/007/008, STOR-028/029/030 (`ba876fc`, `7f52f29`)
  - Batch 9: NEW-001/003/004 (`97c2149`)
  - Final audit findings NEW-001/002/003/004 + lower-confidence findings (`8d45a45`, `6858dad`)
- **TOCTOU races closed**: `ensure_directory` hardened with `O_NOFOLLOW` + fd-stat (`cac0c62`), `ensure_legacy_import_state` (`0cb31e7`), streaming TOCTOU guard in `objects.rs` (`2c03c3b`)
- **P0 audit items fixed**: exhaustive error matches, mutex poison recovery, unsafe docs (`f550b78`)
- **Deep audit (pass 2 & 3)**: JWKS refresh strategy, DRY improvements, error handling, security/performance hardening
- **Hub API audit**: 5 bugs fixed: SQL injection surface, auth bypass, permission checks (`fba6220`)
- **JWKS/OIDC hardening**: `iat`/`nbf` validation added, `Cache-Control max-age` used instead of fixed 60s interval, bearer token timing, key caching (`0374a61`, `3c366df`)
- **`#[allow(unwrap_used)]` replaced** with `HeaderValue::from_static` where applicable (`5cb6c31`)
- **6 production bugs found by E2E tests** fixed: broken `readyz`, OCI tag delete, metrics counter races, Hub schema mismatch, port retry logic, test fixture drift (`05652e5`)

**Protocol & Server Reliability**
- **Transfer limiter timeout**: `Semaphore::acquire_many_owned()` now has a configurable timeout, returning 503 on exhaustion instead of hanging forever (`0498fba`)
- **Graceful shutdown timeout**: `serve_with_listener` drains connections with configurable timeout; server no longer hangs on open connections during shutdown (`5dca9c2`)
- **Redis reconnect fix**: `get_connection()` error path hardened: handles mid-operation connection drops instead of panicking (`5dca9c2`)
- **S3 multipart leak fixed**: `streaming_large_copy` cleans up multipart upload on completion failure (`1c0012d`)
- **SQLITE-MIG-001**: missing Hub migrations added to `LOCAL_SQLITE_MIGRATIONS` (`13b0e69`)
- **LFS PATCH safety** + OOM guard + OIDC/token race fixed (`12cc125`)
- **LFS/Bazel/OCI protocol bugs** + OIDC auth outage corrected (`04ae3a8`)
- **Batch OCI fixes**: OCI Bug 1/2/3 + GC-NEW-001/002/003 + accompanying E2E tests (`a8477f2`)
- **`block_in_place` converted to `spawn_blocking`** in `objects.rs` to avoid runtime panic (`7f52f29`)
- **Task leak fixed**: spawned tasks now tracked and cancelled on shutdown (`0374a61`)
- **OCI PATCH Content-Range end mismatch** corrected to return 416 (`c110c5e`)

**Test Quality & CI**
- **All 22 test workarounds hardened**: every assertion now expects exact correct behavior (`0ac8b65`, `4e906b9`)
- **7 weak assertions and misleading test names fixed** (`338a4b5`)
- **10 remaining minor workarounds fixed**: no more silent defaults or string contains patterns (`4e906b9`)
- **`serial_test` applied** to all `git_smart_http` tests sharing a single SQLite database (`70794af`)
- **Pre-existing test failures fixed**: shift overflow, protocol adapters broken tests, CI blockers
- **CI filter added** to exclude infrastructure-dependent E2E tests from nextest runs
- **Quick-xml advisory ignored** after verification of non-impact (`4b7da99`)
- **All 18 build warnings resolved**; formatting and clippy clean across workspace

### Performance

- No intentional performance changes in this release (focus was correctness and coverage).

### Documentation

- Updated `ARCHITECTURE.md`, `DEPLOYMENT.md`, `HUGGINGFACE_HUB_API.md`, `README.md` with Hub API, auth providers, metrics sections, and AI use cases
- Documented TOCTOU race windows in S3 adapter with 1.2M-run fuzz validation (`40ef000`)
- Updated security audit documentation
- Added use cases for AI, game assets, and binary distribution to README
- Marked all completed TODO.md items as done; removed TODO.md and dev tracking files from workspace
- Removed 22 files of local dev tracking docs (-1157 lines)

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

- **Streaming SHA-256 digest uploads to S3**: eliminated full-body buffering for content-addressed uploads by streaming through `Sha256` hasher during multipart transfer (`5ff5e14`)
- **S3-native multipart OCI uploads**: bypassed local staging file for S3 backends with resumable upload sessions (`981b973`)
- **CAS-agnostic runtime foundation**: decoupled storage/index layer from protocol specifics with frontend routing via `--frontend` / `SHARDLINE_SERVER_FRONTENDS` (`c461d81`)
- **Comprehensive module splits** across 91 files: `protocol_routes` (8 files), `postgres_backend` (4 files), `upload_ingest` (3 files), `local_sqlite` (6 files), config directory restructure (`d03c763`)
- **New crates extracted** from server: `fsck`, `gc`, `hub_api`, `metrics`, `oci_adapter`, `protocol_adapters`, `server_core`, `xet_adapter`: workspace grew from 10 to 20 crates (`d03c763`)
- **All crate versions bumped to 1.0.0** with centralized `[workspace.dependencies]` (`737c625`)
- **README rewritten** as user-facing product page with validated coverage matrix (`3b86a0b`, `c461d81`)
- **ARCHITECTURE.md rewritten** for 20-crate layered dependency graph with Hub API, Git Smart HTTP, metrics, and auth sections (`1203d8e`)
- **DEPLOYMENT.md expanded** with Hub API, authentication providers, and metrics documentation (`1203d8e`)
- **`rustfmt.toml` applied** across 107 files (`d03c763`)
- **Docker entrypoint** switched to `docker-entrypoint.sh`, Compose includes Postgres on port 5532 (`c461d81`, `d03c763`)

### Fixed

- **Non-Unix compile surface restored**: removed `#[cfg(not(unix))] compile_error!` blocks from cli, index, server, and storage crates; added conservative path validation fallbacks for Windows (`5c69e59`)
- **Git pack encoding**: fixed size varint to use 4 bits in first byte per Git pack spec (`cc0f3cf`)
- **Hub API revision ordering**: added `rowid DESC` tiebreaker to `list_revisions` (`cc0f3cf`)

### Performance

- **Streaming S3 uploads**: SHA-256 digest computed incrementally during multipart transfer instead of buffering entire body (`5ff5e14`)
- **Reduced upload staging I/O**: S3-native multipart uploads skip local staging file entirely (`981b973`)

### Security

- **TOCTOU race windows documented** in S3 adapter for `begin_content_addressed_upload`, `put_file_if_absent`, `copy_object_if_absent` (`40ef000`)
- **Symlink escape prevention**: `ensure_directory_path_components_are_not_symlinked` applied to backend root, stats traversal, and metadata paths (`f389559`)
- **`cargo-deny` policy** enforced in CI for license compliance and security advisories (`78ff8ac`)

### Documentation

- Added contribution workflow templates and PR guidelines (`c508065`)
- Rewrote production readiness and compatibility claims (`3b86a0b`)
- Removed 22 files of local dev tracking docs (-1157 lines) (`818e94d`)
- Updated security audit documentation (`f5d8cac`)
- Documented async storage TOCTOU races with 1.2M-run fuzz validation (`40ef000`)
- Updated all architecture, deployment, and Hub API docs for 20-crate structure (`1203d8e`)

[Unreleased]: https://github.com/STEXS-Technologies/shardline/compare/v1.2.1...HEAD
[1.2.1]: https://github.com/STEXS-Technologies/shardline/compare/v1.2.0...v1.2.1
[1.2.0]: https://github.com/STEXS-Technologies/shardline/compare/v1.1.0...v1.2.0
[1.1.0]: https://github.com/STEXS-Technologies/shardline/compare/v1.0.1...v1.1.0
[1.0.1]: https://github.com/STEXS-Technologies/shardline/compare/v1.0.0...v1.0.1
[1.0.0]: https://github.com/STEXS-Technologies/shardline/releases/tag/v1.0.0
