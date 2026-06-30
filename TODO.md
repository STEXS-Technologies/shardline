# Shardline Deep Audit

Generated: 2026-06-30
Scope: Full workspace — security, code quality, architecture, dependencies, testing, performance

---

## SECURITY AUDIT: Input Validation, Injection, SSRF, Path Traversal

### SSRF — Webhook Delivery

- [ ] **[CRITICAL]** Webhook SSRF — no URL scheme or internal IP validation — `hub_api/src/routes.rs:79-96` `deliver_one_webhook` sends POST to arbitrary user-supplied URL. No validation against internal IPs (127.0.0.0/8, 10.0.0.0/8, 172.16.0.0/12, 192.168.0.0/16, 169.254.169.254, ::1). No scheme restriction (e.g., `file://`, `gopher://`, `dict://`). Attacker can register a webhook pointing at internal services.
- [ ] **[HIGH]** Webhook URL accepts any scheme — `hub_api/src/routes.rs:1189` `create_webhook` stores the URL without validating it is `http://` or `https://`. Combined with no SSRF protection, this enables arbitrary protocol requests from the server.

### Path Traversal — Hub API File Paths

- [ ] **[HIGH]** Hub API commit accepts unvalidated file paths — `hub_api/src/routes.rs:575-608` `apply_commit` processes `CommitInstruction::InlineFile`, `LfsPointer`, and `Delete` with user-supplied `path` values. No validation against `..`, `/`, absolute paths, or control characters. An attacker can overwrite arbitrary keys in the hub SQLite database or create files at unexpected logical paths.
- [ ] **[MEDIUM]** Hub API file tree / resolve paths not validated — `hub_api/src/routes.rs:646,721` `file_tree` and `resolve_file` use the `file_path` path parameter without traversal validation, though the impact is limited to database reads.

### Body Size Limits — Hub API

- [ ] **[HIGH]** Hub API commit endpoint has no body size limit — `hub_api/src/routes.rs:544` `commit` accepts `body: String` (entire body in memory). The hub API router has no `DefaultBodyLimit` layer, unlike the main server. An attacker can send a multi-GB NDJSON commit body to exhaust memory.
- [ ] **[HIGH]** Hub API LFS upload has no body size limit — `hub_api/src/routes.rs:863` `lfs_upload` accepts `body: bytes::Bytes` (entire body in memory). No hub-specific body limit applied. A single LFS upload can consume all server memory.
- [ ] **[MEDIUM]** Hub API preupload/body parsing unbounded — `hub_api/src/routes.rs:510` `preupload` deserializes `Json<PreuploadRequest>` with no size constraint beyond the global Axum default. The `PreuploadRequest` files array is unbounded.
- [ ] **[LOW]** Hub API webhook create accepts unbounded URL length — `hub_api/src/routes.rs:1189` `create_webhook` stores `request.url` without length validation. Extremely long URLs waste database storage.

### Log Injection

- [ ] **[MEDIUM]** User-supplied URL logged without sanitization — `hub_api/src/routes.rs:66` `tracing::warn!("webhook delivery to {url} failed: {e}")` logs the full user-supplied webhook URL. An attacker can inject newlines or control characters into log output to forge log entries or disrupt log parsers.
- [ ] **[LOW]** Repo ID logged from URL path — `hub_api/src/routes.rs:35` `tracing::warn!("failed to load webhooks for {repo_id}: {e}")` logs user-controlled path segments. Low risk since `tracing` is structured, but control characters could still affect log aggregation.

### SQL Injection — Low Risk (Parameterized)

- [ ] **[LOW]** Format-string SQL for table existence check — `index/src/local_sqlite/helpers.rs:201` `format!("SELECT EXISTS(SELECT 1 FROM {table} LIMIT 1)")` uses `format!` with table names, but all table names come from a hardcoded `const` array (line 189-198), not user input. Acceptable but fragile if the pattern is copied elsewhere.
- [ ] **[INFO]** PostgreSQL `DROP/CREATE DATABASE` uses format-string — `server/src/database_migration.rs:540-543` and `server/src/postgres_backend/mod.rs:207-210` use `format!("DROP DATABASE IF EXISTS {database_name}")`. These are in test code only. PostgreSQL does not support parameterized DDL for database names, but these should be restricted to test-only compilation.

### Path Traversal — Storage Layer (Well-Protected)

- [ ] **[INFO]** `ObjectKey::parse` rejects traversal — `storage/src/key.rs:109-130` properly rejects `..`, `/`, `\`, empty segments, and absolute paths. Storage key validation is sound.
- [ ] **[INFO]** `validate_identifier` rejects traversal — `server/src/validation.rs:12-26` properly rejects `/`, `..`, `\`, control characters, and oversized values. File ID validation is sound.
- [ ] **[INFO]** Symlink protection thorough — `storage/src/local_path.rs:26-51` validates every path component is not a symlink. `index/src/local_sqlite/helpers.rs:52-62` validates SQLite database path safety. `storage/src/local_fs.rs` uses `O_NOFOLLOW` on Unix.

### Request Smuggling

- [ ] **[INFO]** No custom Content-Length/Transfer-Encoding handling — Axum's built-in body handling is used throughout. No risk of request smuggling from custom parsing.

### Integer Overflow

- [ ] **[INFO]** Checked arithmetic used consistently — `server/src/overflow.rs` provides `checked_add` and `checked_increment`. Upload ingest (`server/src/upload_ingest/mod.rs:96-133`) uses checked arithmetic throughout. No unbounded arithmetic found in production code paths.

### XML / XXE

- [ ] **[INFO]** No XML parsing — No XML parsing libraries or patterns found in the codebase. XXE is not applicable.

### Command Injection

- [ ] **[INFO]** No `std::process::Command` with user input — All `Command::new` calls are in test code or use hardcoded arguments. No shell command injection vectors found.

### Denial of Service — Unbounded Allocations

- [ ] **[MEDIUM]** Hub API reads entire commit body into String — `hub_api/src/routes.rs:544` `commit` handler takes `body: String` which reads the entire request body into memory. Without a hub-specific body limit, this is an unbounded memory allocation controlled by the attacker.
- [ ] **[MEDIUM]** Hub API LFS upload reads entire body into Bytes — `hub_api/src/routes.rs:863` `lfs_upload` takes `body: bytes::Bytes` which reads the entire body into memory. Combined with no body limit, a single request can exhaust memory.
- [ ] **[LOW]** Main server body limits are well-bounded — `server/src/app.rs:221` applies `DefaultBodyLimit::max(max_request_body_bytes)` globally (default 64MB). Individual endpoints further restrict: provider tokens (16KB), provider webhooks (1MB).

### SQL Injection — Parameterized Queries Verified

- [ ] **[INFO]** All SQLite queries use `params![]` — `index/src/local_sqlite/index_store.rs`, `index/src/local_sqlite/record_store.rs`, `index/src/hub_local_sqlite.rs` all use rusqlite parameterized queries. No string interpolation in SQL.
- [ ] **[INFO]** All PostgreSQL queries use `$N` placeholders — `index/src/hub_postgres.rs` uses sqlx parameterized queries throughout. No string interpolation in SQL.

### Commit Path Validation

- [ ] **[HIGH]** Hub commit file paths not validated for traversal or encoding — `hub_api/src/commit.rs:82-101` parses `file.path` from NDJSON without any validation for `..`, `/`, absolute paths, null bytes, or control characters. This path is stored directly in the hub database and used for tree listing and file resolution.

---

## Architecture

- [ ] **[HIGH]** God object: `ServerConfig` has 30 fields — `crates/server/src/config/mod.rs:37` Consider splitting into sub-configs (auth, OCI, cache, storage, etc.)
- [x] **[HIGH]** God object: `BenchIterationReport` has ~30 fields — `crates/cli/src/bench/mod.rs:34` Consider grouping related metrics into sub-structs
- [x] **[HIGH]** God trait: `IndexStore` has ~25 required methods — `crates/index/src/store.rs:16` Split into `ReconstructionStore`, `LifecycleStore`, `DedupeStore`, `ProviderStateStore`
- [ ] **[HIGH]** God trait: `RecordStore` has ~20 required methods — `crates/index/src/record.rs:206` Split into `RecordTraversal`, `RecordMutation`, `RepositoryScopedRecords` — Deferred: the RecordStore split is feasible but requires updating ~5 impl sites (local_sqlite, postgres, memory) and all test mock impls. Lower priority than the IndexStore split since RecordStore methods are naturally grouped.
- [ ] **[MEDIUM]** God trait: `HubStore` has 15 methods — `crates/index/src/hub.rs:94` Split into `HubRepoStore`, `HubRevisionStore`, `HubLfsStore`, `HubWebhookStore`
- [ ] **[HIGH]** `ServerObjectStore` implements both `ObjectStore` trait AND identical inherent methods (put_if_absent, read_range, metadata, delete_if_present) — `crates/server_core/src/lib.rs:523,610` The inherent methods shadow the trait methods with identical signatures; remove one layer
- [ ] **[MEDIUM]** Duplicate `read_full_object` implementation — `crates/server_core/src/lib.rs:818` and `crates/server/src/object_store.rs:66` The server version adds test hooks but duplicates core logic
- [ ] **[MEDIUM]** Duplicate `validate_content_hash` in 7 locations — `crates/server_core/src/lib.rs:989`, `crates/protocol_adapters/src/lib.rs:52`, `crates/xet_adapter/src/xorb_store.rs:9`, `crates/xet_adapter/src/shard_store.rs:35`, `crates/server/src/validation.rs:28`, `crates/oci_adapter/src/protocol_support.rs:127`, `crates/server_core/src/lib.rs:134` Consolidate into a single canonical implementation in `protocol` or `server_core`
- [ ] **[MEDIUM]** Duplicate `checked_add` in `server_core` and `oci_adapter` — `crates/server_core/src/lib.rs:1011`, `crates/oci_adapter/src/lib.rs:993` Consolidate into one
- [ ] **[MEDIUM]** Duplicate `unix_now_seconds_checked` in `fsck` and `oci_adapter` — `crates/fsck/src/lib.rs:777`, `crates/oci_adapter/src/lib.rs:986` Consolidate into `protocol` or `server_core`
- [ ] **[HIGH]** `server` crate has 50+ `pub use` re-exports — `crates/server/src/lib.rs:79-182` Creates a massive public API surface; users can import the same type from `server`, `xet_adapter`, `oci_adapter`, `gc`, `protocol_adapters`, etc.
- [ ] **[LOW]** Unnecessary re-export of external crate: `pub use serde::{Deserialize, Serialize}` — `crates/server_core/src/lib.rs:127` Downstream crates should depend on `serde` directly
- [ ] **[MEDIUM]** `cli` depends on `server` which pulls in the entire dependency tree (20+ internal crates) — `crates/cli/src/lib.rs` Consider a slimmer "cli-runtime" crate that only exposes what CLI needs
- [ ] **[MEDIUM]** `fsck`, `gc`, `rebuild`, `provider_events` all have nearly identical dependency footprints (server-core + index + storage + xet-adapter + metrics) — consider a shared "tool-runtime" crate
- [ ] **[MEDIUM]** `server/src/object_store.rs` mixes `ServerObjectStore` re-export with local file reconstruction logic (reconstruct_chunk_file_bytes, reconstruct_file_record_bytes) — `crates/server/src/object_store.rs:87-180` Split reconstruction into its own module
- [ ] **[LOW]** `server/src/metrics.rs` is a thin re-export wrapper around `shardline-metrics` with no added value — `crates/server/src/metrics.rs`
- [ ] **[LOW]** `server/src/gc.rs`, `server/src/fsck.rs`, `server/src/rebuild.rs`, `server/src/provider_events.rs` are thin dispatch wrappers — consider whether these belong in `server` or `cli`
- [ ] **[MEDIUM]** `oci_adapter/src/lib.rs` is 1051 lines with upload session management, SHA256 state, S3 multipart, and file locking all in one file — `crates/oci_adapter/src/lib.rs` Split into `upload_session`, `sha256_state`, `s3_multipart` modules

## Code Quality

- [ ] **[MEDIUM]** Silently discarded auth errors: `let _ = auth.authorize(headers, TokenScope::Read)?` — `crates/hub_api/src/git/smart_http.rs:43,51` The `?` propagates but `let _ =` discards the Ok value; unclear intent
- [ ] **[MEDIUM]** Silently discarded auth in route: `.and_then(|auth| auth.authorize(&headers, TokenScope::Read).ok())` — `crates/hub_api/src/routes.rs:241` Authorization failures silently become `None`
- [ ] **[MEDIUM]** Silently discarded state operations: `let _ = state` — `crates/hub_api/src/routes.rs:453,1182` Errors from state mutations are discarded
- [ ] **[LOW]** `.ok()` on `parse_xet_hash_hex` silently ignores malformed hashes — `crates/rebuild/src/lib.rs:431` Should at least log a warning
- [ ] **[LOW]** `let _ = backend.abort_resumable_object_upload(...)` — `crates/oci_adapter/src/lib.rs:724,744` Cleanup failures silently discarded; consider logging
- [ ] **[LOW]** `let _ = backend.delete_object_if_present(...)` — `crates/oci_adapter/src/lib.rs:760` Cleanup failure silently discarded
- [ ] **[MEDIUM]** SQL cleanup errors silently discarded: `let _ = sqlx::query("DELETE FROM ...")` — `crates/index/src/hub_postgres.rs:603-611,706,728` Should propagate or log
- [ ] **[LOW]** `registry.register(...).ok()` throughout metrics crate — `crates/metrics/src/*.rs` ~40 occurrences. Acceptable for static metric names but silences registration failures
- [ ] **[LOW]** `encoder.encode_to_string(&metric_families).unwrap_or_default()` — `crates/metrics/src/lib.rs:96` Silently returns empty string on encode failure
- [ ] **[LOW]** `DEFAULT_LOCAL_GC_RETENTION_SECONDS` defined identically in both `gc` and `provider_events` — `crates/gc/src/lib.rs:172`, `crates/provider_events/src/lib.rs:87` Should live in one canonical location

## CRITICAL

## Testing Audit

Generated: 2026-06-30 — Deep testing analysis of workspace (614 lib tests across 19 crates)

### Test Coverage by Crate (tests → lines → ratio)

| Crate | Tests | Source Lines | Lines/Test | Status |
|---|---|---|---|---|
| server | 210 | 26,942 | 128 | OK |
| cli | 93 | 10,339 | 111 | OK |
| index | 86 | 14,951 | 174 | Low |
| vcs | 58 | 3,505 | 60 | Good |
| storage | 51 | 4,395 | 86 | Good |
| protocol | 36 | 1,378 | 38 | Good |
| xet_adapter | 27 | 2,744 | 102 | OK |
| hub_api | 21 | 3,700 | 176 | Low |
| provider_events | 13 | 2,343 | 180 | Low |
| oci_adapter | 8 | 1,454 | 182 | Low |
| cache | 6 | 664 | 111 | OK |
| cas | 2 | 156 | 78 | OK |
| **fsck** | **0** | **1,701** | — | **NO TESTS** |
| **gc** | **0** | **1,178** | — | **NO TESTS** |
| **metrics** | **0** | **726** | — | **NO TESTS** |
| **protocol_adapters** | **0** | **204** | — | **NO TESTS** |
| **rebuild** | **0** | **817** | — | **NO TESTS** |
| **server_core** | **0** | **1,177** | — | **NO TESTS** |

### Findings

#### Missing Test Modules — Crates with Zero Unit Tests

- [ ] **[CRITICAL]** `fsck` crate has 1,701 lines across 3 files with zero tests — `crates/fsck/src/lib.rs` lifecycle check logic for integrity verification has no unit test coverage
- [ ] **[CRITICAL]** `gc` crate has 1,178 lines across 4 files with zero tests — `crates/gc/src/lib.rs` garbage collection dispatch, quarantine, and reachability logic completely untested
- [ ] **[CRITICAL]** `server_core` crate has 1,177 lines across 5 files with zero tests — `crates/server_core/src/lib.rs` shared server logic (auth, frontend dispatch) has no unit tests
- [ ] **[HIGH]** `rebuild` crate has 817 lines across 2 files with zero tests — `crates/rebuild/src/lib.rs` index rebuild and candidate resolution logic untested
- [ ] **[MEDIUM]** `metrics` crate has 726 lines across 12 files with zero tests — `crates/metrics/src/lib.rs` metrics collection for protocol, reconstruction, storage, transfer untested
- [ ] **[LOW]** `protocol_adapters` crate has 204 lines with zero tests — `crates/protocol_adapters/src/lib.rs` LFS and Bazel adapter logic untested

#### Large Source Files Without Any Test Coverage

- [ ] **[HIGH]** `hub_api/src/routes.rs` — 1,227 lines, no `#[cfg(test)]` module
- [ ] **[HIGH]** `index/src/local_sqlite/helpers.rs` — 1,189 lines, no test module
- [ ] **[HIGH]** `index/src/hub.rs` — 864 lines, no test module
- [ ] **[HIGH]** `index/src/postgres/index_store.rs` — 812 lines, no test module
- [ ] **[HIGH]** `index/src/postgres/record_store.rs` — 639 lines, no test module
- [ ] **[HIGH]** `rebuild/src/lib.rs` — 596 lines, no test module
- [ ] **[HIGH]** `gc/src/lib.rs` — 667 lines, no test module
- [ ] **[HIGH]** `gc/src/reachability.rs` — 231 lines, no test module
- [ ] **[HIGH]** `gc/src/quarantine.rs` — 175 lines, no test module
- [ ] **[MEDIUM]** `fsck/src/record_checks.rs` — 539 lines, no test module
- [ ] **[MEDIUM]** `fsck/src/lifecycle_checks.rs` — 243 lines, no test module
- [ ] **[MEDIUM]** `storage/src/anchored_fs.rs` — 449 lines, no test module
- [ ] **[MEDIUM]** `server/src/postgres_backend/read.rs` — 429 lines, no test module
- [ ] **[MEDIUM]** `server/src/app/protocol_routes/oci/manifest.rs` — 374 lines, no test module
- [ ] **[MEDIUM]** `server/src/app/protocol_routes/oci/blob_upload.rs` — 321 lines, no test module
- [ ] **[MEDIUM]** `server/src/app/protocol_routes/oci/token.rs` — 324 lines, no test module
- [ ] **[MEDIUM]** `server/src/backup.rs` — 336 lines, no test module
- [ ] **[MEDIUM]** `server/src/config/secrets.rs` — 306 lines, no test module
- [ ] **[MEDIUM]** `provider_events/src/repository.rs` — 253 lines, no test module
- [ ] **[MEDIUM]** `rebuild/src/candidates.rs` — 221 lines, no test module

#### Test Quality — High Assertion Density (Potential Brittle Tests)

- [ ] **[MEDIUM]** `server/src/rebuild/tests.rs` — 11 tests with 92 assertions (8.4 asserts/test) — failures may be hard to debug
- [ ] **[MEDIUM]** `index/src/local/tests.rs` — 14 tests with 104 assertions (7.4 asserts/test)
- [ ] **[MEDIUM]** `server/src/local_backend/tests.rs` — 16 tests with 106 assertions (6.6 asserts/test)
- [ ] **[MEDIUM]** `server/src/provider_events/tests.rs` — 13 tests with 72 assertions (5.5 asserts/test)
- [ ] **[LOW]** `provider_events/src/tests.rs` — 13 tests with 72 assertions (5.5 asserts/test)

#### Test Quality — Low Assertion Density (Potentially Insufficient Coverage)

- [ ] **[MEDIUM]** `index/src/local_sqlite/tests.rs` — 8 tests with only 8 assertions (1.0 asserts/test) — tests may be validating setup rather than behavior
- [ ] **[MEDIUM]** `server/src/app/tests.rs` — 8 tests with only 17 assertions (2.1 asserts/test)
- [ ] **[LOW]** `server/src/gc_tests.rs` — 19 tests with 26 assertions (1.4 asserts/test) — exercise functions dominate over targeted assertions

#### Flaky Test Patterns — `sleep()` Usage

- [ ] **[MEDIUM]** `server/src/rebuild/tests.rs:84` — `sleep(Duration::from_millis(10))` in rebuild test, potential race condition
- [ ] **[MEDIUM]** `cache/src/memory.rs:179` — `sleep(Duration::from_millis(1100))` in cache eviction test, timing-dependent
- [ ] **[LOW]** `server/tests/protocol_frontends_http.rs:2457` — `sleep(Duration::from_secs(2))` in integration test
- [ ] **[LOW]** `server/tests/k8s_cluster_protocol_e2e.rs:1117` — `sleep(Duration::from_millis(250))` in e2e test
- [ ] **[LOW]** 8 more integration test files use `sleep(Duration::from_millis(20))` for server startup waits — fragile timing dependency

#### Flaky Test Patterns — Timestamp Dependency

- [ ] **[MEDIUM]** `server/src/lifecycle_repair/tests.rs:323,427,496,652` — tests use `SystemTime::now()` directly, making results time-sensitive
- [ ] **[LOW]** `test_support/src/docker.rs:110,333` — Docker test helpers use `SystemTime::now()` for container naming

#### Test Setup Duplication

- [ ] **[MEDIUM]** `tempfile::tempdir()` creation repeated 105 times across test files — `index/src/local/tests.rs` alone has 14 instances, `provider_events/src/tests.rs` has 5, `server/src/gc_tests.rs` has 18
- [ ] **[LOW]** `LocalBackend::new()` initialization pattern duplicated across `server/src/local_backend/tests.rs`, `server/src/rebuild/tests.rs`, `server/src/provider_events/tests.rs` — could be extracted into shared test helpers

#### Missing Edge Case Tests

- [ ] **[HIGH]** `server/src/error.rs` (612 lines) — error type definitions and conversions lack unit tests
- [ ] **[HIGH]** `server/src/postgres_backend/upload.rs` (219 lines) — upload path has no tests
- [ ] **[HIGH]** `server/src/postgres_backend/read.rs` (429 lines) — read path has no tests
- [ ] **[MEDIUM]** `server/src/oidc_provider.rs` (234 lines) — OIDC provider logic untested
- [ ] **[MEDIUM]** `server/src/jwks_provider.rs` (234 lines) — JWKS key rotation logic untested
- [ ] **[MEDIUM]** `server/src/app/reconstruction_helpers.rs` (255 lines) — reconstruction helpers untested
- [ ] **[MEDIUM]** `index/src/store.rs` (591 lines) — store abstraction layer untested
- [ ] **[MEDIUM]** `index/src/local/index_store.rs` (583 lines) — local index store has no unit tests

#### Integration Test Gaps

- [x] **[HIGH]** 15 of 19 crates have zero integration tests — only `cas`, `cli`, `hub_api`, and `server` have integration test suites
- [x] **[HIGH]** `index` crate (14,951 lines) — no integration tests despite being core storage abstraction
- [x] **[HIGH]** `storage` crate (4,395 lines) — no integration tests for S3/local object store operations
- [ ] **[HIGH]** `vcs` crate (3,505 lines) — no integration tests for provider/repository reference handling
- [ ] **[MEDIUM]** `xet_adapter` crate (2,744 lines) — no integration tests for Xet protocol adaptation
- [ ] **[MEDIUM]** `provider_events` crate (2,343 lines) — no integration tests
- [ ] **[MEDIUM]** `oci_adapter` crate (1,454 lines) — no integration tests for OCI distribution protocol
- [ ] **[MEDIUM]** `protocol` crate (1,378 lines) — no integration tests for protocol parsing

#### Property-Based and Fuzz Testing

- [ ] **[MEDIUM]** No property-based testing (proptest/quickcheck) anywhere in workspace
- [ ] **[LOW]** Fuzz targets (16 total in `crates/fuzz/fuzz_targets/shardline/`) cover: storage, vcs refs, protocol parsing, lifecycle repair, CLI parsing, filesystem races — but missing fuzz targets for: `fsck` record validation, `gc` quarantine logic, `index` SQL operations, `hub_api` route handling, `server_core` auth flows, `rebuild` candidate resolution
- [ ] **[LOW]** No fuzz targets for `protocol_adapters` LFS/Bazel adapter parsing

#### Benchmark Gaps

- [ ] **[MEDIUM]** Only 3 crates have benchmarks (protocol, server, storage) — missing benchmarks for: `index` (14,951 lines, performance-critical), `cli` (10,339 lines, user-facing), `hub_api` (3,700 lines), `vcs` (3,505 lines), `xet_adapter` (2,744 lines), `cache` (664 lines, latency-sensitive)
- [ ] **[LOW]** `server/benches/` has 4 files but no benchmarks for `server_core` auth dispatch or `provider_events` processing

#### Test Naming

- [ ] **[LOW]** Most test names are descriptive (e.g., `local_backend_reuses_unchanged_chunks`, `gc_dry_run_reports_orphan_chunks_without_mutating_quarantine`) — naming convention is generally good

#### Dead Test Code

- [ ] **[LOW]** No empty `#[cfg(test)]` modules found — all test modules contain test code

### Summary

- **614 unit tests** across 19 crates, **0 failures**
- **6 crates** with zero unit tests (fsck, gc, metrics, protocol_adapters, rebuild, server_core) totaling **4,803 untested lines**
- **73 source files** >100 lines with no test coverage
- **105 instances** of duplicated tempdir setup
- **26+ sleep() calls** in tests creating flakiness risk
- **15/19 crates** lack integration tests
- **0 property-based tests** in workspace
- **No benchmarks** for index, cli, hub_api, vcs, xet_adapter, cache

## CODE QUALITY AUDIT

### Dead Code

- [ ] **[MEDIUM]** Dead code: `create_resumable_object_upload`, `upload_resumable_object_part`, `complete_resumable_object_upload`, `abort_resumable_object_upload` — `crates/server/src/backend.rs:427` methods in `ServerBackend` impl never used
- [ ] **[LOW]** Dead constant `LOCAL_DIRECTORY_MODE` — `crates/server/src/local_fs.rs:28`
- [ ] **[LOW]** Dead constant `LOCAL_FILE_MODE` — `crates/server/src/local_fs.rs:30`
- [ ] **[LOW]** Dead function `write_file_atomically` — `crates/server/src/local_fs.rs:32`
- [ ] **[LOW]** Dead function `run_before_local_write_hook` — `crates/server/src/local_fs.rs:48`
- [ ] **[LOW]** Dead function `write_file_atomically_unix` — `crates/server/src/local_fs.rs:51`
- [ ] **[LOW]** Dead function `open_anchored_target` — `crates/server/src/local_fs.rs:72`
- [ ] **[LOW]** Dead function `write_anchored_temporary_file` — `crates/server/src/local_fs.rs:82`
- [ ] **[LOW]** Dead function `ensure_parent_path_matches_anchor` — `crates/server/src/local_fs.rs:87`
- [ ] **[LOW]** Dead function `anchored_path_options` — `crates/server/src/local_fs.rs:121`
- [ ] **[LOW]** Dead function `invalid_local_path_error` — `crates/server/src/local_fs.rs:125`
- [ ] **[LOW]** Dead function `stable_hex_id` — `crates/server/src/protocol_support.rs:36`
- [ ] **[LOW]** Dead function `managed_protocol_object_identity` — `crates/server/src/server_frontend/dispatch.rs:40`
- [ ] **[LOW]** Dead function `managed_protocol_object_identity` — `crates/server/src/server_frontend/xet.rs:38`

### Unused Imports

- [ ] **[LOW]** Unused import `RecordStore` — `crates/fsck/src/lib.rs:33`
- [ ] **[LOW]** Unused import `OciPath` — `crates/server/src/app/protocol_routes/mod.rs:10`
- [ ] **[LOW]** Unused import `IndexError` — `crates/server/src/backend.rs:678`
- [ ] **[LOW]** Unused import `WEBHOOK_DELIVERY_FUTURE_SKEW_SECONDS` — `crates/server/src/fsck.rs:13`
- [ ] **[LOW]** Unused import `OpsRecordKind` — `crates/server/src/ops_record_store.rs:1`
- [ ] **[LOW]** Unused import `shardline_server_core::provider_directory` — `crates/server/src/repository_scope_path.rs:9`
- [ ] **[LOW]** Unused import `managed_protocol_object_identity` — `crates/server/src/server_frontend/mod.rs:8`

### Unused Dependencies

- [ ] **[MEDIUM]** Unused dependency `tower` — `crates/hub_api/Cargo.toml:31` not imported or referenced anywhere in hub_api source
- [ ] **[MEDIUM]** Unused dependency `sha2` — `crates/cli/Cargo.toml:42` not imported or referenced anywhere in cli source
- [ ] **[MEDIUM]** Unused dependency `tokio` — `crates/metrics/Cargo.toml:15` not imported or referenced anywhere in metrics source
- [ ] **[MEDIUM]** Unused dependency `tower-http` — `crates/metrics/Cargo.toml:14` not imported or referenced anywhere in metrics source

### Duplicated Code

- [ ] **[HIGH]** Duplicated provider_events module — `crates/server/src/provider_events/` mirrors `crates/provider_events/src/` with diverged imports and error types (tests.rs differs in 14+ locations, outcome.rs has different signatures)
- [ ] **[MEDIUM]** Near-duplicate `local_fs.rs` across 3 crates — `crates/server/src/local_fs.rs`, `crates/storage/src/local_fs.rs`, `crates/index/src/local_fs.rs` all have different md5 hashes but overlapping functionality
- [ ] **[MEDIUM]** Near-duplicate `local_path.rs` across 3 crates — `crates/cli/src/local_path.rs`, `crates/server/src/local_path.rs`, `crates/storage/src/local_path.rs` all differ but likely share logic
- [ ] **[MEDIUM]** Near-duplicate `protocol_support.rs` — `crates/server/src/protocol_support.rs` vs `crates/oci_adapter/src/protocol_support.rs`

### Large Functions (>80 lines)

- [ ] **[HIGH]** `main` — `crates/cli/src/main.rs:26` (538 lines) — massive monolithic entry point
- [ ] **[HIGH]** `exercise_git_xet_clone_flow` — `crates/server/tests/git_xet_push_e2e.rs:323` (420 lines)
- [ ] **[MEDIUM]** `run_bench_iteration` — `crates/cli/src/bench/e2e.rs:13` (402 lines)
- [ ] **[MEDIUM]** `run_bench` — `crates/cli/src/bench/mod.rs:912` (382 lines)
- [ ] **[MEDIUM]** `xorb_transfer_route_requires_range_and_serves_partial_content` — `crates/server/tests/transfer_http.rs:425` (305 lines)
- [ ] **[MEDIUM]** `exercise_k8s_cluster_native_xet_sparse_mutation_flow` — `crates/server/tests/k8s_cluster_protocol_e2e.rs:305` (267 lines)
- [ ] **[MEDIUM]** `exercise_git_xet_push_flow` — `crates/server/tests/git_xet_push_e2e.rs:79` (243 lines)
- [ ] **[MEDIUM]** `exercise_git_xet_multi_repo_flow` — `crates/server/tests/git_xet_push_e2e.rs:744` (243 lines)
- [ ] **[MEDIUM]** `run_ingest_bench` — `crates/cli/src/bench/mod.rs:1300` (200 lines)
- [ ] **[MEDIUM]** `exercise_lifecycle_repair` — `crates/cli/tests/repair_e2e.rs:56` (192 lines)
- [ ] **[MEDIUM]** `print_summary` — `crates/cli/src/bench/mod.rs:217` (178 lines)
- [ ] **[MEDIUM]** `exercise_native_git_lfs_flow_on_runtime` — `crates/server/tests/native_protocol_frontends_e2e.rs:240` (166 lines)
- [ ] **[MEDIUM]** `try_from` — `crates/cli/src/command.rs:988` (161 lines)
- [ ] **[MEDIUM]** `mixed_frontends_share_digest_addressed_storage_and_keep_xet_working` — `crates/server/tests/protocol_frontends_http.rs:571` (157 lines)
- [ ] **[MEDIUM]** `run_lifecycle_repair_with_stores_at_time` — `crates/server/src/lifecycle_repair.rs:183` (157 lines)
- [ ] **[MEDIUM]** `exercise_lifecycle_repair_reconciles_stale_metadata` — `crates/server/src/lifecycle_repair/tests.rs:495` (154 lines)
- [ ] **[MEDIUM]** `inspect_native_xet_term` — `crates/fsck/src/record_checks.rs:358` (139 lines)

### Complex Match Arms (>10 arms)

- [ ] **[MEDIUM]** Match with 35 arms — `crates/cli/src/command.rs:989`
- [ ] **[MEDIUM]** Match with 31 arms — `crates/fsck/src/lib.rs:509`
- [ ] **[MEDIUM]** Match with 21 arms — `crates/server/src/error.rs:410`
- [ ] **[MEDIUM]** Match with 21 arms — `crates/server/src/fsck.rs:55`
- [ ] **[MEDIUM]** Match with 19 arms — `crates/server/src/error.rs:473`
- [ ] **[MEDIUM]** Match with 17 arms — `crates/server/src/error.rs:294`
- [ ] **[MEDIUM]** Match with 17 arms — `crates/server/src/error.rs:501`
- [ ] **[MEDIUM]** Match with 17 arms — `crates/server/src/backend.rs:680`
- [ ] **[MEDIUM]** Match with 17 arms — `crates/index/src/local_sqlite/tests.rs:405`
- [ ] **[MEDIUM]** Match with 17 arms — `crates/index/src/local/helpers.rs:558`
- [ ] **[MEDIUM]** Match with 16 arms — `crates/server/src/rebuild.rs:21`
- [ ] **[MEDIUM]** Match with 16 arms — `crates/protocol/src/hash.rs:69`
- [ ] **[MEDIUM]** Match with 16 arms — `crates/index/src/xet_hash.rs:56`
- [ ] **[MEDIUM]** Match with 15 arms — `crates/server/src/error.rs:440`
- [ ] **[MEDIUM]** Match with 13 arms — `crates/server/src/app/protocol_routes/oci/mod.rs:96`
- [ ] **[MEDIUM]** Match with 13 arms — `crates/fuzz/fuzz_targets/shardline/async_storage_races.rs:43`

### Inconsistent Naming

- [ ] **[LOW]** Mixed `read_`/`get_` prefixes — `read_chunk`, `read_object`, `read_full_object` vs `get_repo`, `get_files`, `get_lfs_object` in `crates/index/src/hub.rs`
- [ ] **[LOW]** Mixed `open_`/`new_` patterns — `open_directory_chain`, `open_or_create_child_directory`, `open_new_file` in `crates/storage/src/anchored_fs.rs` vs `new_upload_session_id` in `crates/oci_adapter/src/lib.rs`

### TODO/FIXME/HACK

- [ ] **[INFO]** No TODO/FIXME/HACK comments found in source code.

### Error Swallowing (let _ = / .ok())

- [ ] **[HIGH]** SQL deletes with `let _ =` ignoring errors — `crates/index/src/hub_postgres.rs:603,607,611,706,728` database deletes silently discard failures
- [ ] **[MEDIUM]** `let _ = state` in routes — `crates/hub_api/src/routes.rs:453,1182` result discarded
- [ ] **[MEDIUM]** `let _ = auth.authorize(...)` — `crates/hub_api/src/git/smart_http.rs:43,51` auth result discarded (returns Ok but ignores Err via `?`)
- [ ] **[MEDIUM]** `let _ = status` / `let _ = (operation, ok)` / `let _ = error_type` — `crates/server/src/metrics.rs:27,36,74,79,86` metric values discarded
- [ ] **[MEDIUM]** `let _ = root` — `crates/storage/src/local_fs.rs:102,126,153` path validation results discarded
- [ ] **[LOW]** 38x `.ok()` swallowing errors — `crates/metrics/src/*.rs` all `registry.register(...)` calls silently ignore duplicate registration failures

### Redundant Clones

- [ ] **[INFO]** 476 `.clone()` calls across non-test code. The metrics registration pattern `Box::new(foo.clone()).ok()` appears 38 times across `crates/metrics/src/` — these are necessary for the prometheus `Box<dyn Collector>` pattern. Full audit of remaining ~438 clones not feasible without deeper semantic analysis.

### Public API Surface

- [ ] **[MEDIUM]** `pub` re-export of `OpsRecordKind` in `crates/server/src/ops_record_store.rs:1` is unused — should be removed or scoped to `pub(crate)`
- [ ] **[MEDIUM]** `pub` re-export of `provider_directory` in `crates/server/src/repository_scope_path.rs:9` is unused — should be removed
- [ ] **[LOW]** `server_core` exports many types (`RebuildOverflowError`, `InvalidLifecycleMetadataError`, `InvalidSerializedShardError`, `InvalidReconstructionResponseError`) that may only be used internally — consider `pub(crate)` if not part of external API contract

### Authentication & Authorization

- [ ] **CRITICAL** OIDC JWT signature never verified — `crates/server/src/oidc_provider.rs:143-211` `verify_jwt_claims` fetches JWKS keys into `_keys` and decodes signature bytes into `_sig_bytes`, but **never uses either** to verify the JWT signature. Any token with a valid `iss` and `exp` is accepted, regardless of whether it was actually signed by the issuer. An attacker can forge arbitrary tokens.
- [ ] **CRITICAL** JWKS JWT signature never verified — `crates/server/src/jwks_provider.rs:153-211` Same issue: `_keys`, `_header`, and `_sig_bytes` are all unused. Only `exp` is checked. No signature verification occurs at all.
- [ ] **CRITICAL** JWKS provider performs no issuer validation — `crates/server/src/jwks_provider.rs:153-211` Unlike the OIDC provider (which checks `iss`), the JWKS provider never validates the `iss` claim. A token from any issuer that passes the (non-existent) signature check is accepted.
- [ ] **CRITICAL** No JWT algorithm validation (`alg:none` attack) — `crates/server/src/oidc_provider.rs:155-156` and `crates/server/src/jwks_provider.rs:163-164` The JWT header `alg` field is decoded but never checked. An attacker can set `alg: "none"` and omit the signature entirely, and the token will be accepted because signature verification is missing entirely.
- [ ] **CRITICAL** Missing `exp` claim defaults to never-expiring token — `crates/server/src/oidc_provider.rs:166-169` and `crates/server/src/jwks_provider.rs:174-177` When the `exp` claim is absent, `unwrap_or(u64::MAX)` is used, meaning tokens without an expiration claim never expire. Combined with no signature verification, this allows indefinite token forgery.
- [ ] **HIGH** `whoami` endpoint hardcodes `is_admin: true` — `crates/hub_api/src/routes.rs:245` The `/api/whoami-v2` response always returns `is_admin: true` regardless of the authenticated user's actual role, which is a privilege escalation risk if clients rely on this field for authorization decisions.
- [ ] **MEDIUM** OIDC provider does not check `aud` claim — `crates/server/src/oidc_provider.rs:143-211` The OIDC provider only validates `iss` and `exp`. The `aud` (audience) claim is not checked, meaning tokens intended for other services could be accepted.
- [ ] **MEDIUM** OIDC provider does not check `iat` or `nbf` claims — `crates/server/src/oidc_provider.rs:143-211` The `iat` (issued-at) and `nbf` (not-before) claims are not validated, allowing the use of tokens from the distant past or future.

### Secrets & Configuration

- [ ] **MEDIUM** Secret file read has TOCTOU race between metadata check and read — `crates/server/src/config/secrets.rs:189-198` `read_secret_file_bytes` checks `metadata.len()` at line 191, then reads at line 196, then checks again at line 197. A file could be swapped between the first metadata check and the read. The second check catches some cases but a concurrent writer could change the file between the read completing and the second metadata check.
- [ ] **LOW** `open_secret_file` on non-Unix does not prevent symlink following — `crates/server/src/config/secrets.rs:250-253` On non-Unix platforms, `File::open` is used without `O_NOFOLLOW`, meaning symlink attacks are possible on Windows/macOS (non-Unix cfg).
- [ ] **INFO** Secret bytes are properly zeroed on drop via `Zeroize`/`ZeroizeOnDrop` — `crates/protocol/src/security.rs:7-8` `SecretBytes` and `SecretString` use the `zeroize` crate, which is correct.
- [ ] **INFO** Signing key and secrets are redacted in Debug output — `crates/protocol/src/security.rs:48-51` and `crates/server_core/src/auth/local_ed25519.rs:41-46` and `crates/server/src/config/mod.rs:70-153` All secret-carrying types implement `Debug` with redacted output.

### Rate Limiting

- [ ] **MEDIUM** Only OCI token endpoint has rate limiting — `crates/server/src/app/protocol_routes/oci/token.rs:35-44` The OCI token endpoint uses a `Semaphore` for in-flight request limiting. However, no other authentication or token exchange endpoints (`/v1/providers/{provider}/tokens`, `/v1/providers/{provider}/git-lfs-authenticate`, Hub API endpoints) have rate limiting, making them susceptible to brute-force attacks.
- [ ] **LOW** OCI rate limit is in-flight concurrency, not request rate — `crates/server/src/app.rs:179-181` The OCI token limiter uses a `Semaphore`, which limits concurrent in-flight requests, not requests-per-second. A sustained flood of slow requests could still exhaust the semaphore.

### Webhook & HMAC

- [ ] **INFO** Webhook delivery HMAC uses `hmac::Mac` which performs constant-time verification internally — `crates/hub_api/src/routes.rs:84-91` The outgoing webhook HMAC computation using `HmacSha256::new_from_slice` + `mac.update` + `mac.finalize` is correct.
- [ ] **INFO** Incoming webhook HMAC verification uses `hmac::Mac::verify_slice` which is constant-time — `crates/vcs/src/builtin.rs:295` The `verify_hex_hmac_sha256` function uses `mac.verify_slice` which is constant-time by design of the `hmac` crate.

### CORS / Security Headers

- [ ] **MEDIUM** No CORS configuration on Hub API routes — `crates/hub_api/src/routes.rs:118-201` and `crates/hub_api/src/lib.rs:46-48` The Hub API router has no CORS layer configured. If the Hub API is served on a different origin than the frontend, cross-origin requests will be blocked by browsers, or if a permissive CORS layer is added later, it could expose the API.
- [ ] **MEDIUM** No Content-Security-Policy, X-Frame-Options, or other security headers — The entire server has no security header middleware. Responses lack `Content-Security-Policy`, `X-Frame-Options`, `X-Content-Type-Options`, and `Strict-Transport-Security` headers, making the application susceptible to clickjacking, MIME sniffing, and other browser-based attacks.
- [ ] **INFO** No `tower_http` security middleware detected — Grep for `tower_http`, `SecurityHeaders`, and security-related middleware returned no results, confirming no HTTP security headers are applied.

### Session Management

- [ ] **INFO** OCI upload sessions use file locking and TTL expiration — `crates/oci_adapter/src/lib.rs:381-384` Upload sessions are protected by a file lock (`acquire_upload_session_file_lock`) and expire based on `oci_upload_session_ttl_seconds`. Session IDs are validated to contain only hex/hyphen characters.
- [ ] **INFO** OCI upload session IDs are validated — `crates/oci_adapter/src/protocol_support.rs:114` `validate_upload_session_id` rejects IDs with unexpected characters and enforces a maximum length.

### Token Minting & Signing

- [ ] **INFO** Local token signing uses HMAC-SHA256 (not Ed25519 despite the name) — `crates/protocol/src/token.rs:11` `type TokenMac = Hmac<sha2::Sha256>` — The `LocalEd25519Provider` name is misleading; it uses HMAC-SHA256 via `TokenSigner`, not Ed25519 signatures. This is functionally fine but the naming is confusing.
- [ ] **INFO** Token signature verification uses constant-time comparison — `crates/protocol/src/token.rs:337` `expected_signature.ct_eq(signature.as_slice())` uses the `subtle` crate for timing-safe comparison.
- [ ] **INFO** Static bearer token comparison uses constant-time comparison — `crates/server/src/auth.rs:138` `authorize_static_bearer_token` uses `subtle::ConstantTimeEq` for timing-safe comparison.
- [ ] **INFO** Bearer token size is bounded — `crates/server/src/auth.rs:8` and `crates/hub_api/src/auth.rs:9` Both auth modules enforce `MAX_BEARER_TOKEN_BYTES = 8192` before parsing.
- [ ] **INFO** Whitespace in bearer tokens is rejected — `crates/server/src/auth.rs:116-118` and `crates/hub_api/src/auth.rs:89-91` Both reject tokens containing ASCII whitespace, preventing header injection.
- [ ] **INFO** Token signing key minimum length enforced — `crates/protocol/src/token.rs:287-289` `TokenSigner::new` rejects empty signing keys.
- [ ] **INFO** Provider config file is zeroed after parsing — `crates/server/src/provider/config_io.rs:34` `bytes.zeroize()` is called after `from_slice`, clearing raw config bytes from memory.

## DEPENDENCY AUDIT

### Duplicate Dependencies

- [x] **[HIGH]** 3 versions of `getrandom` (0.2, 0.3, 0.4) — `Cargo.toml:26` **NOT FIXABLE**: `getrandom` 0.2 pulled by `rand 0.8` (gearhash via xet-data), 0.3 by workspace, 0.4 by `rand 0.10` (xet-core-structures). All three versions are required by pinned xet-* ecosystem crates (1.5.x). Cannot deduplicate without updating upstream xet dependencies to a version that aligns getrandom versions.
- [x] **[HIGH]** 2 versions of `reqwest` (0.12.28, 0.13.4) — `Cargo.toml:30` **NOT FIXABLE**: workspace pins reqwest 0.12 but `xet-client 1.5.2` explicitly depends on `reqwest 0.13.4`. Cannot deduplicate without updating xet-client to a version that uses reqwest 0.12, which does not exist.
- [x] **[HIGH]** 2 versions of `thiserror` (1.x, 2.x) — `Cargo.toml:57` **FIXED**: Updated `prometheus` from 0.13 to 0.14 which uses thiserror 2.x. Only `prometheus 0.13` was pulling thiserror 1.x; all xet-* crates already used thiserror 2.x.
- [ ] **[MEDIUM]** 3 versions of `hashbrown` (0.14, 0.15, 0.17) — pulled by rusqlite (0.14), indexmap (0.15), and serde_json (0.17); three separate HashMap implementations
- [ ] **[MEDIUM]** 2 versions of `hashlink` (0.9, 0.10) — transitively from duplicate hashbrown versions
- [ ] **[MEDIUM]** 2 versions of `cfg-if` (0.1, 1.0) — gearhash pulls 0.1, rest of ecosystem uses 1.0
- [ ] **[MEDIUM]** 2 versions of `core-foundation` (0.9, 0.10) — from duplicate reqwest versions
- [ ] **[MEDIUM]** 2 versions of `itertools` (0.10, 0.14) — from xet ecosystem vs workspace crates
- [ ] **[MEDIUM]** 2 versions of `webpki-roots` (0.26, 1.0) — from duplicate reqwest versions; two sets of Mozilla CA roots
- [ ] **[MEDIUM]** 2 versions of `whoami` (1.6, 2.1) — from xet ecosystem vs newer dependencies
- [ ] **[MEDIUM]** 2 versions of `rand` (0.8, 0.10) + `rand_core` (0.6, 0.10) — from xet ecosystem vs workspace

### Workspace Dep Compliance

- [ ] **[MEDIUM]** `hub_api` bypasses workspace for 5 deps: `chrono`, `flate2`, `sha1`, `tower`, `tracing` — `crates/hub_api/Cargo.toml:16-32` hardcoded versions instead of `workspace = true`
- [ ] **[MEDIUM]** `metrics` bypasses workspace for `futures-util` — `crates/metrics/Cargo.toml:17` hardcoded `"0.3"` instead of `workspace = true`
- [ ] **[LOW]** `fuzz` crate uses `edition = "2024"` directly — `crates/fuzz/Cargo.toml:5` instead of `edition.workspace = true`

### Feature Flag Waste

- [ ] **[MEDIUM]** `object_store` default features disabled but `aws` feature added — `crates/storage/Cargo.toml:21` `default-features = false` + `features = ["aws"]`; the `aws` feature pulls in SigV4, HTTP connectors, and credential providers even when only local storage is used
- [ ] **[MEDIUM]** `reqwest` in server enables `blocking` feature — `crates/server/Cargo.toml:43` `features = ["blocking"]` adds a thread-pool runtime; used only for JWKS/OIDC key refresh which could be async
- [ ] **[LOW]** `tokio` in server enables broad feature set — `crates/server/Cargo.toml:52` `features = ["fs", "io-util", "macros", "net", "rt-multi-thread", "sync", "time"]` — `fs` and `io-util` may not be needed in all server roles
- [ ] **[LOW]** `tower-http` in metrics enables nothing — `crates/metrics/Cargo.toml:14` `tower-http = { workspace = true }` with no features; workspace default-features=false, so it's essentially empty

### Heavy Dependencies

- [ ] **[HIGH]** `sqlx` with Postgres + TLS — `crates/server/Cargo.toml:45-49` pulls in postgres-protocol, TLS stack, connection pooling; ~200+ transitive crates. Consider `deadpool-postgres` + `tokio-postgres` for lighter Postgres access
- [ ] **[HIGH]** `reqwest` with `rustls-tls` — `crates/server/Cargo.toml:43` pulls in rustls + webpki-roots + ring; ~50+ transitive crates. Two copies compiled due to duplicate reqwest versions
- [ ] **[MEDIUM]** `rusqlite` with `bundled` feature — `crates/index/Cargo.toml:20` compiles SQLite from C source; adds ~30s to clean build. Consider `rusqlite` with system SQLite if available
- [ ] **[MEDIUM]** `xet-*` ecosystem (xet-client, xet-data, xet-core-structures, xet-runtime) — `Cargo.toml:60-63` brings gearhash, chacha20, rand, and duplicate blake3; significant compilation cost

## PERFORMANCE AUDIT

### Blocking in Async Context

- [ ] **[HIGH]** 16 `block_on` calls in `hub_postgres.rs` — `crates/index/src/hub_postgres.rs:31-551` Every `HubStore` method uses `tokio::task::block_in_place || Handle::current().block_on(...)`. This blocks the tokio worker thread while waiting for DB queries, starving other async tasks. **Follow-up required**: The `HubStore` trait is synchronous by design, and both implementations (PostgreSQL and local SQLite) implement it synchronously. The SQLite impl is natively sync (rusqlite), so the trait cannot be made async without breaking the SQLite path. The PostgreSQL impl must bridge sync-to-async. Fix options: (a) Make `HubStore` async — requires updating trait, both impls, `BoxedHubStore`, and all callers in hub_api routes (~15 call sites). High effort. (b) Keep trait sync but wrap PostgreSQL calls in `spawn_blocking` at the hub_api route level — lower effort but requires callers to know which impl they're using. (c) Accept current pattern — `block_in_place` is the recommended tokio pattern for sync-in-async and does not deadlock; it only occupies one worker thread per call. **Recommended**: defer to a dedicated refactor PR. Impact is moderate — hub_api is not high-throughput enough for worker starvation to be critical.
- [ ] **[HIGH]** `S3ObjectStore` blocks on async internally — `crates/storage/src/s3.rs:300-335` `block_on` and `block_on_result` use `block_in_place || handle.block_on()`. When called from within an async context (e.g., upload_ingest), this blocks the tokio worker thread. **Follow-up required**: The `ObjectStore` trait (defined in `crates/storage/src/lib.rs`) is synchronous. Both `LocalFsObjectStore` and `S3ObjectStore` implement it synchronously. The `object_store` crate's `AmazonS3` client is async, requiring the sync-to-async bridge. Fix: make `ObjectStore` trait async — requires updating trait, both impls, and all callers across server, upload_ingest, and gc modules (~30+ call sites). **High effort refactor**. The current `block_in_place` pattern is safe and idiomatic for this use case. **Recommended**: defer to a dedicated refactor PR.
- [ ] **[MEDIUM]** `std::fs::create_dir_all` in async fn — `crates/server/src/app.rs:387` blocking filesystem call during server startup (minor impact but pattern is risky)
- [ ] **[MEDIUM]** `std::fs::create_dir_all` in `spawn_blocking` — `crates/server/src/oci_adapter.rs:719` and `crates/oci_adapter/src/lib.rs:816` properly wrapped in `spawn_blocking` (correct pattern)

### Lock Contention

- [ ] **[MEDIUM]** `std::sync::Mutex` used for JWKS key cache — `crates/server/src/jwks_provider.rs:20,125,144` and `crates/server/src/oidc_provider.rs:20,135` `std::sync::Mutex` is held while doing HTTP fetches (`blocking_client.get()`) in `get_or_refresh_keys`. If the HTTP call stalls, the mutex is held for the entire duration, blocking all concurrent token verifications
- [ ] **[MEDIUM]** `tokio::sync::Mutex` for OCI upload session lock — `crates/server/src/oci_adapter.rs:41,327` and `crates/oci_adapter/src/lib.rs:382` Global mutex serializes all upload session creation; under high concurrent uploads this becomes a bottleneck
- [ ] **[LOW]** `LazyLock<Mutex<...>>` test hooks — `crates/server/src/provider.rs:67`, `crates/server/src/object_store.rs:38`, `crates/server/src/backend.rs:45`, `crates/server/src/config/mod.rs:239` Only used in test mode (`#[cfg(test)]`), acceptable

### Unnecessary Allocations in Hot Paths

- [ ] **[MEDIUM]** `object_key.clone()` in `read_object_stream` — `crates/server/src/postgres_backend/read.rs:168,184,188` `ObjectKey` is cloned before passing to byte stream functions. If `ObjectKey` is `Arc<str>` this is cheap; if it's `String` it's a heap allocation per request
- [ ] **[MEDIUM]** `self.buffer.clone()` in OCI blob processing — `crates/server/src/oci_adapter.rs:138` Buffer is cloned during chunk processing; if this is a large Vec, it's an expensive allocation
- [ ] **[MEDIUM]** `record.clone()` in provider events — `crates/server/src/provider_events/records.rs:120,129` `FileRecord` is cloned when returning cached records; `FileRecord` contains `Vec<Chunk>` so each clone allocates
- [ ] **[LOW]** `pool.clone()` in 16 locations in `hub_postgres.rs` — `crates/index/src/hub_postgres.rs:26,78,107,...` `PgPool` is `Arc<PoolInner>`, clone is cheap (just Arc bump)

### Unnecessary Clones

- [ ] **[MEDIUM]** `server_frontends.clone()` in `download_file` — `crates/server/src/postgres_backend/read.rs:126` `Vec<String>` cloned before `spawn_blocking`; should move instead of clone
- [ ] **[MEDIUM]** `bytes.clone()` in `put_if_absent_pooled_bytes` — `crates/server/src/upload_ingest/chunk_store.rs:150,163` `Bytes` is cloned inside `spawn_blocking` to return it after the closure; the clone is necessary for the current design but could be avoided by splitting the buffer
- [ ] **[MEDIUM]** `session.clone()` in OCI adapter — `crates/server/src/oci_adapter.rs:565,596,842` `OciUploadSession` cloned before persist; if session contains large buffers, this is expensive
- [ ] **[LOW]** `transfer_limiter.clone()` in protocol routes — `crates/server/src/app/protocol_routes/mod.rs:52,58` `Arc<Semaphore>` clone, cheap

### String Formatting in Hot Paths

- [ ] **[MEDIUM]** `format!("sha256:{digest_hex}")` in OCI routes — `crates/server/src/oci_adapter.rs:266,284,295,299` and `crates/server/src/app/protocol_routes/oci/mod.rs:112,130` Multiple format! calls per OCI request; could use `const` or pre-computed strings
- [ ] **[MEDIUM]** `format!("{prefix}/{}", key.as_str())` in S3 location — `crates/storage/src/s3.rs:340,355` Called on every S3 operation; could use a pre-computed prefix
- [ ] **[LOW]** `total_length.to_string()` in response headers — `crates/server/src/app/reconstruction_helpers.rs:56,73` and `crates/server/src/app/protocol_routes/lfs.rs:176` One allocation per response; acceptable

### Concurrency & I/O

- [ ] **[MEDIUM]** OCI upload session global lock — `crates/server/src/oci_adapter.rs:41` Single `tokio::sync::Mutex<()>` serializes all session creation across all repositories; under high upload concurrency this is a global bottleneck. Consider per-repository locks or lock-free session creation
- [ ] **[MEDIUM]** S3 `block_on` prevents true async I/O — `crates/storage/src/s3.rs:300-335` The `object_store` crate's S3 client is async but `S3ObjectStore` wraps it with blocking calls. This prevents multiplexing multiple concurrent S3 operations on a single tokio worker thread
- [ ] **[LOW]** No connection pooling for S3 object store — `crates/storage/src/s3.rs` The `object_store` crate manages its own HTTP connection pool internally, so this is handled by the dependency
- [ ] **[LOW]** `std::fs::rename` and `std::fs::remove_file` in anchored_fs — `crates/storage/src/anchored_fs.rs:368,400` These are called from `spawn_blocking` in the storage layer (correct pattern)

### Unnecessary Work

- [ ] **[MEDIUM]** JWKS keys cloned on every cache hit — `crates/server/src/jwks_provider.rs:129` `cached.keys.clone()` returns a new `Vec<Jwk>` on every token verification when cache is valid. Should return `&[Jwk]` or `Arc<Vec<Jwk>>`
- [ ] **[MEDIUM]** OIDC keys cloned on every cache hit — `crates/server/src/oidc_provider.rs:138` Same pattern as JWKS; `cached.keys.clone()` allocates on every cache hit
- [ ] **[LOW]** `JwksProvider::clone` creates new `reqwest::blocking::Client` — `crates/server/src/jwks_provider.rs:31` `reqwest::blocking::Client::new()` is called on every clone; should reuse the existing client or make it `Arc`

## CODE QUALITY AUDIT PASS 2 — Additional Findings

### Unsounded Test Patterns

- [ ] **[MEDIUM]** `static mut TEMP_DIR` in test code is undefined behavior — `crates/hub_api/tests/git_smart_http.rs:31` and `crates/hub_api/tests/dataset_and_webhooks.rs:25` use `static mut TEMP_DIR: Option<TempDir> = None;`. Writing to `static mut` is UB if any thread reads it, and it cannot be soundly accessed even in single-threaded tests due to Rust's aliasing rules. Replace with `static TEMP_DIR: OnceLock<Mutex<Option<TempDir>>>` or similar.

### Wrong Abstraction Level

- [ ] **[HIGH]** `server/src/oci_adapter.rs` is a 916-line god-module mixing four concerns — The file simultaneously contains: (1) manual SHA-256 state machine (`SerializableSha256State`, lines 80-159), (2) upload session filesystem management (lines 326-878), (3) S3 multipart upload orchestration (lines 548-687), and (4) OCI protocol key construction (lines 165-304). Each of these should be a separate module. The SHA-256 implementation alone is 80 lines of security-sensitive manual compression code.

- [ ] **[MEDIUM]** `server/src/oci_adapter.rs:306-324` `new_upload_session_id` mixes CSPRNG with weak fallback — When `getrandom` fails, the fallback constructs a session ID from PID, thread name, and nanosecond timestamp. This is deterministic and predictable. A fallback for CSPRNG failure should either retry, use a weaker but still unique source (e.g., `uuid::Uuid::new_v4()`), or return an error rather than silently degrading to weak entropy.

### Whole-File Semantic Duplicates (Beyond Already-Found `protocol_support ×2`)

- [ ] **[CRITICAL]** `server/src/oci_adapter.rs` (916 lines) and `oci_adapter/src/lib.rs` (1051 lines) are near-complete copies — These two files contain identical implementations of: `OciUploadSession`, `OciS3MultipartUploadSession`, `SerializableSha256State` + full manual SHA-256 impl, `OciFileLock`, upload session CRUD, S3 multipart orchestration, `new_upload_session_id`, `upload_session_expired`, `count_active_upload_sessions`, `purge_expired_upload_sessions`, etc. The `oci_adapter` crate was extracted to be reusable, but `server` retains its own full copy. Any bug fix must be applied in two places. The security-sensitive SHA-256 state machine is duplicated, meaning a regression in one copy goes undetected.

- [ ] **[HIGH]** `server/src/provider_events/records.rs` (151 lines) is a full copy of `provider_events/src/records.rs` (149 lines) — Both contain 7 identical functions: `ensure_absent_or_matching_record`, `collect_deleted_repository_record_references`, `parse_record_entry`, `record_belongs_to_repository`, `repository_record_scope`, `renamed_file_record`, `record_identity_key`. The only difference is error types (`ServerError` vs `ProviderEventsError`). The `server` crate should delegate to `provider_events` instead of maintaining a diverged copy.

### Semantic Duplicates (New Instances Not in First Pass)

- [ ] **[MEDIUM]** `parse_stored_file_record_bytes` duplicated in `server_core` and `server` — `crates/server_core/src/lib.rs:917` defines the canonical implementation with `ParseStoredFileRecordError`. `crates/server/src/record_store.rs:9` re-implements the identical logic (same 1 GB size check, same `u64::try_from(bytes.len()).unwrap_or(u64::MAX)` pattern, same `from_slice(bytes)` call) but maps to `ServerError`. The `server` version should call through to the `server_core` version.

- [ ] **[MEDIUM]** `chunk_object_key` duplicated across 3 crates — `crates/server_core/src/lib.rs:150`, `crates/server/src/chunk_store.rs:7`, and `crates/xet_adapter/src/xorb_store.rs:13` all implement the same "validate hash → extract 2-char prefix → format `{prefix}/{hash}` → parse ObjectKey" pattern. The `xet_adapter` version (`chunk_object_key_local`) is a line-for-line copy of the `server_core` version but returns `XetAdapterError`.

- [ ] **[LOW]** `validate_repository` wrapper duplicated — `crates/server/src/oci_adapter.rs:165` and `crates/oci_adapter/src/lib.rs:189` both define `fn validate_repository(repository: &str)` as a one-line delegation to `validate_oci_repository_name`.

- [ ] **[LOW]** `parse_reference` wrapper duplicated — `crates/server/src/oci_adapter.rs:169` and `crates/oci_adapter/src/lib.rs:196` both define the identical `fn parse_reference` with the same sha256/tag branching logic.

### Missing Safety Invariants

- [ ] **[MEDIUM]** `OciUploadSession.use_s3_multipart` can represent invalid state — `crates/server/src/oci_adapter.rs:59-69` and `crates/oci_adapter/src/lib.rs:77-88` The `use_s3_multipart` bool and `s3_multipart: Option<OciS3MultipartUploadSession>` are independent fields. Nothing prevents `use_s3_multipart = true` with `s3_multipart = None` (which `append_s3_multipart_upload_bytes` checks at line 556-558 and returns `ServerError::NotFound`). These should be a single enum: `enum UploadBody { LocalFile(PathBuf), S3Multipart(OciS3MultipartUploadSession) }`.

- [ ] **[MEDIUM]** `OciS3MultipartUploadSession.uploaded_part_ids` has no length invariant — `crates/server/src/oci_adapter.rs:71-78` Part IDs are pushed incrementally but the code trusts `uploaded_part_ids.len()` for the next part number. If parts are re-uploaded or the list is corrupted, part numbering silently breaks. Consider using an explicit `next_part_number: usize` field.

### Platform-Specific Gaps

- [ ] **[MEDIUM]** GC schedule tests are Linux-only — `crates/cli/src/gc_schedule.rs` gates 4 tests on `#[cfg(target_os = "linux")]` (lines 692, 747, 852, 1016) for systemd unit generation, but the `install_gc_schedule` function itself appears to have no macOS/Windows implementation. The non-Linux path silently becomes a no-op, which is unexpected for users on macOS who run the install command.

- [ ] **[LOW]** `macos_fd_real_path` uses fixed 1024-byte stack buffer — `crates/storage/src/anchored_fs.rs:418-430` The `F_GETPATH` buffer is `[0i8; 1024]`. macOS `PATH_MAX` is 1024, so this is technically sufficient, but the NUL terminator occupies one byte, leaving exactly 1023 usable. Paths of exactly 1023 chars + NUL would be truncated. The function does not check for truncation when `result >= 0` but the returned path is exactly `PATH_MAX - 1` chars.

### Concurrency Bugs

- [ ] **[MEDIUM]** `OidcProvider::clone` silently swallows mutex poison — `crates/server/src/oidc_provider.rs:23-36` The `Clone` impl does `self.cached_keys.lock().ok()?.and_then(|guard| guard.clone())`. If any thread panics while holding the lock, the mutex becomes poisoned and subsequent `Clone` calls silently get `None` for cached keys, causing unnecessary JWKS re-fetches. The `ok()` discards the `PoisonError`.

- [ ] **[MEDIUM]** `JwksProvider` holds `std::sync::Mutex` during blocking HTTP — `crates/server/src/jwks_provider.rs:121-151` `get_or_refresh_keys` acquires the mutex (line 123-126), releases it, makes a blocking HTTP request (line 134-140), then re-acquires (line 142-145) to update. Between the first release and the re-acquire, another thread could also observe the stale cache and make a redundant HTTP request. This is a thundering-herd race under high concurrency.

- [ ] **[LOW]** `hub_api` global `OnceLock<HubState>` prevents multi-instance testing — `crates/hub_api/src/state.rs:3` The `static STATE: OnceLock<HubState>` means only one hub configuration can ever be initialized per process. Integration tests that need different auth configurations (e.g., testing with vs. without webhook secrets) cannot run in the same test binary.

### Logic Bugs

- [ ] **[MEDIUM]** `unwrap_or(0)` on system time makes tokens immortal on pre-epoch clocks — `crates/server/src/oidc_provider.rs:170-173` and `crates/server/src/jwks_provider.rs:178-181` Both use `SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0)`. If the system clock is set before UNIX epoch (NTP misconfiguration, VM clock drift), `now` becomes 0, and any token with `exp > 0` passes the expiration check. Use `unwrap_or(0)` only if "no expiry" is the desired safe default; otherwise, reject the token.

- [ ] **[MEDIUM]** `std::fs::create_dir_all` error silently discarded at server startup — `crates/server/src/app.rs:387` `.ok()` discards the error. If the directory can't be created (permissions, disk full, read-only filesystem), the subsequent `LocalIndexStore::new(hub_root)` fails with a confusing SQLite error instead of "could not create hub directory".

- [ ] **[LOW]** `reqwest::Client::builder().build().ok()` silently degrades webhook delivery — `crates/server/src/app.rs:401-404` If the HTTP client fails to build (TLS configuration error, system cert store unavailable), `http_client` becomes `None`. All webhook deliveries then silently do nothing rather than failing fast at startup. Should at least log a warning.

### Test Setup Duplication

- [ ] **[MEDIUM]** Hub API test SQL schema duplicated across 2 test files — `crates/hub_api/tests/git_smart_http.rs:39-85` and `crates/hub_api/tests/dataset_and_webhooks.rs:33-78` contain nearly identical 45-line `CREATE TABLE` SQL blocks defining 6 tables. A change to the schema must be replicated in both files. Extract into a shared `hub_api/tests/common/mod.rs` helper.

### Stale/Redundant Code Patterns

- [ ] **[LOW]** `parse_record_entry` functions are trivial wrappers — `crates/provider_events/src/records.rs:88` and `crates/server/src/provider_events/records.rs:90` both define `fn parse_record_entry(bytes: &[u8])` as a single-line delegation to `parse_stored_file_record_bytes`. These wrappers add indirection without value; callers could call the underlying function directly.

- [ ] **[LOW]** `validate_repository` is a one-line delegation — `crates/server/src/oci_adapter.rs:165` and `crates/oci_adapter/src/lib.rs:189` both define `fn validate_repository` as `validate_oci_repository_name(repository)`. This adds a layer of indirection that obscures the actual validation being called.

---

## SECURITY AUDIT PASS 2 — Additional Findings

### Denial of Service — Decompression Bomb

- [ ] **[HIGH]** Git pack decompression bomb — `crates/hub_api/src/git/smart_http.rs:713-722` `decompress_zlib` calls `decoder.read_to_end(&mut output)` with no size limit. A malicious `receive_pack` request can include a tiny compressed blob that decompresses to gigabytes, exhausting server memory. The pack data is parsed from untrusted user input at line 233 via `parse_pack_data`.

### Information Disclosure — Error Messages

- [ ] **[HIGH]** `HubApiError::CasError` leaks internal error details to HTTP clients — `crates/hub_api/src/error.rs:51-52,68` The `CasError(String)` variant embeds arbitrary internal error strings (via 38+ occurrences of `.map_err(|e| HubApiError::CasError(e.to_string()))` throughout `routes.rs`). The `IntoResponse` impl at line 78 serializes `self.to_string()` as JSON, leaking internal storage path names, database errors, and index schema details to the client.
- [ ] **[MEDIUM]** `HubApiError::Io` leaks filesystem paths to HTTP clients — `crates/hub_api/src/error.rs:13-14,68` `#[error("io error: {0}")]` includes the full `std::io::Error` message (often containing filesystem paths) in the JSON response body. Maps to `INTERNAL_SERVER_ERROR` but reveals internal layout.
- [ ] **[MEDIUM]** `ServerError::Io` and `ServerError::Json` leak error details — `crates/server/src/error.rs:123,126,368` These variants embed source errors via `#[from]`. When converted via `self.to_string()` in the `IntoResponse` impl, IO error messages (containing filesystem paths) are sent to clients.
- [ ] **[MEDIUM]** Git receive-pack error messages leaked in protocol response — `crates/hub_api/src/git/smart_http.rs:733,751-752` `store_push_objects` returns `format!("failed to create revision: {e}")` which is included in the pktline report response as `ng {refname} {msg}`. Internal store errors are reflected to Git clients.

### Input Validation — Git Protocol

- [ ] **[MEDIUM]** Receive-pack accepts unvalidated refnames — `crates/hub_api/src/git/smart_http.rs:588-595` Refnames parsed from user-supplied pktline data are used without Git ref format validation. No checks for valid `refs/heads/` prefix, control characters, or empty components. Arbitrary strings are passed to `store_push_objects` and reflected in the report response.
- [ ] **[MEDIUM]** Receive-pack accepts arbitrary SHA strings — `crates/hub_api/src/git/smart_http.rs:724-736` The `new_sha` value from user pktline input is passed directly to `create_revision` without validating it is a 40-character hex SHA-1. The store may accept arbitrary strings as revision identifiers.

### Session Security

- [ ] **[MEDIUM]** Upload session ID fallback is predictable — `crates/server/src/oci_adapter.rs:312-323` When `getrandom` fails, `new_upload_session_id` falls back to `PID:thread_name:timestamp_nanos` → SHA-256 → truncated to 32 hex chars. PID is sequential/guessable, thread name may be predictable, and timestamp is coarse-grained. An attacker could predict session IDs and hijack in-progress OCI uploads.

### Cryptographic Weakness

- [ ] **[MEDIUM]** No minimum token signing key length enforced — `crates/protocol/src/token.rs:286-294` `TokenSigner::new` only rejects empty keys. A 1-byte or 2-byte signing key is accepted, making HMAC-SHA256 tokens trivially brute-forceable. Should enforce a minimum of 32 bytes.
- [ ] **[LOW]** Commit file hash uses non-cryptographic `DefaultHasher` — `crates/hub_api/src/routes.rs:577-581` The `apply_commit` function hashes file content with `std::collections::hash_map::DefaultHasher` (SipHash). The resulting hash is stored in `HubFileEntry.sha`, which clients may assume is a content-addressed hash. Hash collisions could cause incorrect file deduplication.

### Denial of Service — Unbounded Processing

- [ ] **[MEDIUM]** NDJSON commit has no instruction count limit — `crates/hub_api/src/commit.rs:56-163` `parse_ndjson_commit` accumulates instructions into an unbounded `Vec`. An attacker can craft a body with millions of tiny valid JSON lines (each a `{}` or minimal object) to cause excessive memory allocation and CPU consumption before any commit processing begins.

### Token Security

- [ ] **[LOW]** Static bearer token length leaks via timing — `crates/server/src/auth.rs:135-136` `authorize_static_bearer_token` compares `actual.len() != expected_token.len()` before the constant-time `ct_eq` comparison. The early return on length mismatch is faster than the constant-time path, leaking the expected token length through response timing.

---

## ARCHITECTURE/TESTING AUDIT PASS 2 — Additional Findings

### Dependency Direction Violations

- [ ] **[HIGH]** `xet_adapter` depends on `server_core` — `crates/xet_adapter/Cargo.toml:18` A "protocol adapter" crate depends on `shardline-server-core` which itself depends on `shardline-index` + `shardline-storage`. This makes `xet_adapter` impossible to use outside the server ecosystem. The dependency should flow `server_core → xet_adapter`, not the reverse. `xet_adapter` only needs `AuthError` and `AuthProvider` from `server_core`; extract these into a lightweight `shardline-auth` crate.

- [ ] **[MEDIUM]** `fsck`, `gc`, `rebuild`, `provider_events` all depend on `xet_adapter` and `server_core` — `crates/fsck/Cargo.toml:19`, `crates/gc/Cargo.toml:19`, `crates/rebuild/Cargo.toml:19`, `crates/provider_events/Cargo.toml:19` These four operational tool crates each pull in the entire `xet_adapter` dependency graph (including `xet-core-structures`, `axum`). They only use `xet_adapter` for `XetAdapterError` and a handful of reconstruction functions. Extract the shared operational logic into a `tool-runtime` crate.

### Missing Abstractions

- [ ] **[HIGH]** `LocalBackend` and `PostgresBackend` share no common trait — `crates/server/src/local_backend/mod.rs` and `crates/server/src/postgres_backend/mod.rs` Both backends expose identical method signatures (`upload_xorb_stream`, `upload_shard_stream`, `download_file`, `chunk_length`, `read_dedupe_shard_stream`, `stats`, `ready`, etc.) but are connected only by the `ServerBackend` enum's manual `match` dispatch (50+ match arms in `backend.rs`). Define a `trait MetadataBackend` to eliminate the enum boilerplate and allow third-party backends.

- [ ] **[MEDIUM]** Visitor pattern copy-pasted across 4 trait definitions — `crates/index/src/store.rs:80-93`, `crates/index/src/store.rs:379-401`, `crates/index/src/record.rs:217-238`, `crates/storage/src/store.rs:61-75` All four traits define `visit_*` methods with the same generic signature: `fn visit_xxx<Visitor, VisitorError>(visitor: Visitor) where Self::Error: Into<VisitorError>, Visitor: FnMut(Item) -> Result<(), VisitorError>`. The `AsyncIndexStore` versions are copy-pasted with added `Send + 'operation` bounds. Extract a `trait Visitable<T>` with default `visit_all` method.

- [ ] **[MEDIUM]** Lifecycle operations not abstracted — `crates/index/src/store.rs:100-310` `IndexStore` has 18 methods for quarantine/retention/webhook/provider-state lifecycle management. These 18 methods are identical in shape across all 4 implementations (local, sqlite, postgres, memory). The lifecycle operations should be a separate `LifecycleStore` trait that can be implemented once via a blanket impl over `IndexStore`.

### Wrong Module Boundaries

- [ ] **[MEDIUM]** `with_index_postgres_url` has wrong doc comment — `crates/server/src/config/mod.rs:661-666` Doc says "Enables local bearer-token verification with the supplied signing key" — this is a copy-paste from `with_token_signing_key`. The doc should describe Postgres URL configuration, not bearer-token verification.

- [ ] **[MEDIUM]** `hub_api::state::get_for_test()` is `pub` but not `#[cfg(test)]` — `crates/hub_api/src/state.rs:27-29` This function exposes the global `OnceLock<HubState>` to any downstream crate, but it is semantically test-only. It should be `#[cfg(test)]` or removed.

- [ ] **[LOW]** `server` re-exports private adapter modules — `crates/server/src/lib.rs:99,109,111` `pub use shardline_oci_adapter as oci_adapter`, `pub use shardline_xet_adapter as xet_adapter`, `pub use shardline_gc as gc` re-export entire third-party crates through the server's public API, creating coupling. Downstream crates should depend on these directly.

### Test Coverage Gaps — Code Path Analysis

- [ ] **[HIGH]** "Check-then-early-return" anti-pattern silently passes on failure — `crates/server/src/runtime_check.rs:77-80`, `crates/server/src/local_backend/tests.rs:24-27`, `crates/server/src/rebuild/tests.rs:65-68`, and ~30 more test sites. The pattern `assert!(storage.is_ok()); let Ok(storage) = storage else { return; }` means if `storage` is `Err`, the `assert!` panics (good), but if the destructuring happens *after* a separate assertion that uses `is_ok()` for control flow, the `return` silently passes the test. Replace with `let storage = storage.unwrap()` to panic with the actual error.

- [ ] **[HIGH]** No tests for error-to-HTTP-status mapping — `crates/server/src/error.rs:292-351` `ServerError::status_code()` maps 35+ error variants to HTTP status codes. Zero tests verify that `NotFound` → 404, `InsufficientScope` → 403, `TooManyUploadSessions` → 429, etc. A regression in status code mapping would silently break client behavior.

- [ ] **[MEDIUM]** No tests for 26 `From<SubsystemError>` implementations — `crates/server/src/error.rs:386-610` There are 26 manual `From` implementations converting subsystem errors to `ServerError`. These conversions are the critical boundary between subsystem errors and HTTP responses. No tests verify that all variants of `GcError`, `ProviderEventsError`, `XetAdapterError`, etc. are correctly mapped. A missing variant mapping causes a compile error (exhaustive match), but incorrect mapping (e.g., mapping a client error to `INTERNAL_SERVER_ERROR`) goes undetected.

- [ ] **[MEDIUM]** `reconstruction_routes::batch_reconstruction` error swallowing untested — `crates/server/src/app/reconstruction_routes.rs:129` The batch endpoint silently discards `ServerError::NotFound` for individual file IDs. No test verifies: (a) that NotFound entries are excluded from the response, (b) that non-NotFound errors still propagate, (c) that an empty batch returns a valid empty response.

### Test Isolation Issues

- [ ] **[MEDIUM]** `REPOSITORY_REFERENCE_PROBE_*` statics are non-`#[cfg(test)]` — `crates/server/src/backend.rs:44-48` Three global statics (`REPOSITORY_REFERENCE_PROBE_COUNT`, `REPOSITORY_REFERENCE_PROBE_FILTER`, `REPOSITORY_REFERENCE_PROBE_TEST_LOCK`) are declared in non-test code but only used for test instrumentation. The functions `reset_repository_reference_probe_count_for_hash`, `clear_repository_reference_probe_filter`, `repository_reference_probe_count`, and `lock_repository_reference_probe_test` are all `pub` but exclusively used in tests. These leak test infrastructure into the production binary. Gate them behind `#[cfg(test)]`.

- [ ] **[MEDIUM]** `OCI_UPLOAD_SESSION_LOCK` duplicated as two independent global statics — `crates/server/src/oci_adapter.rs:41` and `crates/oci_adapter/src/lib.rs:60` Both declare `static OCI_UPLOAD_SESSION_LOCK: LazyLock<Mutex<()>>` but they are *separate* statics in separate crate binaries. If both the `server` copy and the `oci_adapter` copy are used concurrently (the `server` copy for server-side sessions, the `oci_adapter` copy for tests), session locking is inconsistent. The `server` copy should delegate to the `oci_adapter` crate's lock.

- [ ] **[LOW]** `metrics::REGISTRY` and `metrics::METRICS` are global singletons shared across tests — `crates/metrics/src/lib.rs:55-56` Tests that call `record_*` functions mutate the global `REGISTRY` and `METRICS`. Parallel test execution accumulates metric values across tests, making assertions on metric values unreliable. Tests should use fresh registries.

- [ ] **[LOW]** `hub_api::state::STATE` OnceLock prevents multi-configuration testing — `crates/hub_api/src/state.rs:3` The `OnceLock<HubState>` can only be set once per process. Tests that need different HubState configurations (e.g., different auth settings) cannot coexist in the same test binary.

### Configuration Validation Gaps

- [ ] **[HIGH]** Auth provider kind not cross-validated with required fields at config time — `crates/server/src/config/env.rs:218-229` `SHARDLINE_AUTH_PROVIDER=oidc` without `SHARDLINE_AUTH_OIDC_ISSUER` parses successfully. The error only surfaces during `build_auth_provider` at router construction time. Similarly for `jwks` without `SHARDLINE_AUTH_JWKS_URL`. Add cross-validation in `from_env()` or `validate_runtime_requirements()`.

- [ ] **[MEDIUM]** No upper bound validation on `chunk_size` — `crates/server/src/config/env.rs:81-86` `SHARDLINE_CHUNK_SIZE_BYTES` is parsed as `usize` with no maximum check. Values like `1` (1 byte) or `999999999999` (1 TB) are accepted. An extremely small chunk size creates millions of chunks per file; an extremely large one causes excessive memory allocation during upload.

- [ ] **[MEDIUM]** No validation that `Hub` frontend requires auth — `crates/server/src/config/env.rs` `SHARDLINE_SERVER_FRONTENDS=hub` is accepted without requiring token signing key or auth provider configuration. The Hub API exposes unauthenticated mutation endpoints (commits, webhook creation) that could be exploited.

- [ ] **[LOW]** `public_base_url` not validated as valid URL — `crates/server/src/config/env.rs:30-31` `SHARDLINE_PUBLIC_BASE_URL` accepts any string including empty string, spaces, or invalid URLs. This value is embedded in reconstruction responses and sent to clients. A malformed URL causes broken reconstruction fetches.

### Graceful Degradation Failures

- [ ] **[MEDIUM]** `register_hub_routes` silently discards hub directory creation failure — `crates/server/src/app.rs:387` `std::fs::create_dir_all(&hub_root).ok()` discards the IO error. If the directory cannot be created (permissions, disk full), the subsequent `LocalIndexStore::new(hub_root)` fails with a confusing SQLite error. Log the error and return `Err(ServerError::Io(...))`.

- [ ] **[MEDIUM]** `register_hub_routes` silently discards HTTP client creation failure — `crates/server/src/app.rs:401-404` `reqwest::Client::builder()...build().ok()` silently sets `http_client = None`. When the TLS stack is misconfigured, webhook delivery silently becomes a no-op. Log a warning at startup.

- [ ] **[LOW]** `batch_reconstruction` silently drops `NotFound` without logging — `crates/server/src/app/reconstruction_routes.rs:129` When a file ID in a batch reconstruction is not found, it is silently excluded from the response. At minimum, log at `debug` level to aid troubleshooting.

### Monitoring/Observability Gaps

- [ ] **[HIGH]** Only 3 `#[tracing::instrument]` annotations in the entire server crate — `crates/server/src/app.rs:231`, `crates/server/src/app/operational.rs:65,91,157` The `serve` function and three operational endpoints (`read_chunk`, `upload_xorb`, `upload_shard`) are instrumented. None of the 20+ protocol route handlers (reconstruction, LFS, Bazel, OCI, provider tokens, webhooks) have tracing instrumentation. This makes production debugging extremely difficult.

- [ ] **[MEDIUM]** Dual metrics systems: hand-built `ProtocolMetrics` vs Prometheus `CasMetrics` — `crates/server/src/app.rs:88-113` `AppState` contains a `ProtocolMetrics` struct using raw `AtomicU64` counters for OCI registry tokens. Meanwhile, `shardline_metrics` provides Prometheus-based OCI protocol metrics. The OCI token metrics are tracked in two independent systems with no guarantee of consistency.

- [ ] **[MEDIUM]** Duplicate metrics recording in reconstruction routes — `crates/server/src/app/reconstruction_routes.rs:65-66` Every reconstruction call records metrics via both `shardline_metrics::record_reconstruction(...)` (free function) AND `shardline_metrics::metrics().xet.record_reconstruction(...)` (direct struct access). This double-counts reconstruction metrics in the Prometheus registry.

- [ ] **[MEDIUM]** Provider token issuance not instrumented — `crates/server/src/app/provider_routes/` No tracing or metrics for provider token issuance, token exchange, or webhook delivery operations. These are critical security operations that should be auditable.

### Test Assertion Quality

- [ ] **[MEDIUM]** Config tests use `assert!(x.is_ok()); let Ok(x) = x else { return; }` — `crates/server/src/runtime_check.rs:93-98` The test calls `run_config_check(config).await`, then `assert!(report.is_ok())`, then `let Ok(report) = report else { return; }`. If `report` is `Err`, the test panics at the assert but the `else { return; }` branch is dead code. This is functionally correct but confusing. Use `.unwrap()` for clarity and shorter code.

- [ ] **[LOW]** Metrics endpoint test hand-builds expected Prometheus text — `crates/server/src/app/operational.rs:215-280` The `metrics()` handler manually formats a Prometheus text string with `format!` and `concat!`. No test verifies the output matches the Prometheus exposition format specification. The hand-built string could silently become malformed.

### API Stability Concerns

- [ ] **[MEDIUM]** `server_core` re-exports `pub use serde::{Deserialize, Serialize}` — `crates/server_core/src/lib.rs:127` This re-export means downstream crates implicitly depend on `serde` through `server_core`. If `server_core` ever changes its `serde` feature flags or version, downstream crates break silently. Downstream crates should depend on `serde` directly.

- [ ] **[LOW]** `DefaultShardMetadataLimits` public constant couples downstream to default values — `crates/server_core/src/lib.rs` re-exports `DEFAULT_SHARD_METADATA_LIMITS` which is used as the default in `ServerConfig::new`. If the default changes, all callers using the default are silently affected. Consider making defaults private and forcing explicit configuration.

---

## DEPENDENCY/PERFORMANCE AUDIT PASS 2 — Additional Findings

### 1. Transitive Dependency Vulnerabilities

- [ ] **[INFO]** `cargo audit` clean — 473 crate dependencies scanned, 0 advisories found. No known CVEs in current dependency versions.

### 2. Build Time & Binary Size

- [ ] **[MEDIUM]** Release profile: `lto = "thin"` with `codegen-units = 1` is suboptimal — `Cargo.toml:73-74` Thin LTO doesn't benefit from single codegen-unit as much as fat LTO does. With fat LTO, `codegen-units = 1` allows cross-crate inlining that thin LTO cannot achieve. This combination increases compile time without proportional binary size reduction. Either switch to `lto = true` (fat) or keep `lto = "thin"` but increase `codegen-units` to 16 for faster builds.
- [ ] **[LOW]** 473 transitive crate dependencies — The `xet-*` ecosystem alone brings ~40 unique crates (gearhash, chacha20, xet-runtime, xet-client, xet-data, xet-core-structures). Each additional crate increases compile time and binary size. Consider vendoring xet crates with stripped features if not all are needed.

### 3. Async Runtime Issues

- [ ] **[MEDIUM]** Unbounded webhook task spawning — `crates/hub_api/src/routes.rs:59-68` The `for webhook in &webhooks` loop calls `tokio::spawn` per webhook with no concurrency limit. A repository with 1000 webhooks spawns 1000 concurrent outbound HTTP requests simultaneously. Use a `JoinSet` with bounded concurrency (e.g., 16 concurrent deliveries) or a `Semaphore` to prevent webhook storms from exhausting file descriptors and upstream connections.
- [ ] **[MEDIUM]** Webhook `JoinHandle` is dropped — `crates/hub_api/src/routes.rs:64` `tokio::spawn(...)` returns a `JoinHandle` that is immediately discarded. If the spawned task panics (e.g., from a malformed URL causing `deliver_one_webhook` to panic on unexpected input), the panic is silently swallowed. The server continues running with no indication of the failure. Store the handle in a `JoinSet` or at minimum use `tokio::spawn(async move { ... }).await` pattern to catch panics.
- [ ] **[MEDIUM]** Hub Postgres pool missing `max_connections` — `crates/server/src/app.rs:393` `sqlx::PgPool::connect_lazy(pg_url)` uses sqlx's default pool size (10 connections) with no explicit configuration. The migration pool at `database_migration.rs:220-221` explicitly sets `max_connections(5)`. The hub API pool and the main server pool should both be explicitly configured to prevent connection exhaustion under high concurrency. The `HubStore` trait methods use `block_in_place || handle.block_on()`, meaning each blocking call occupies a tokio worker thread while holding a pool connection; 16 concurrent hub API calls could block 16 worker threads and exhaust 10 pool connections simultaneously.

### 4. Memory Leaks & Unbounded Growth

- [ ] **[MEDIUM]** `file_tree` accumulates unbounded Vec — `crates/index/src/hub_postgres.rs:380-389` The `file_tree` method fetches all file entries for a commit into `entries: Vec<HubFileEntry>` via streaming rows, but collects them all into memory before returning. For repositories with millions of files, this is an unbounded allocation inside a `block_in_place` context (blocking a tokio worker thread during the entire allocation). Add pagination or streaming response.
- [ ] **[MEDIUM]** `search_repos` allocates unbounded Vec — `crates/index/src/hub_postgres.rs` search queries collect all matching rows into a `Vec` without limit. A wildcard search across thousands of repos creates an unbounded allocation.

### 5. I/O Efficiency

- [ ] **[HIGH]** Zero `BufWriter`/`BufReader` usage across entire codebase — The entire workspace has zero uses of `std::io::BufWriter` or `std::io::BufReader`. All `File::create`/`File::open` calls result in unbuffered OS-level reads/writes. For OCI upload sessions (`oci_adapter/src/lib.rs:550,981,1009`), LFS object storage, and chunk writes (`server/src/object_store.rs:316`), this creates excessive syscalls for multi-byte operations. Each `write()` syscall on an unbuffered `File` goes directly to the kernel; for a 1 GB LFS upload writing 4 KB chunks, this means ~262,144 syscalls instead of ~1 with buffering.
- [ ] **[MEDIUM]** `std::fs::read`/`std::fs::write` for entire objects — `crates/oci_adapter/src/lib.rs:460,981` and `crates/server/src/oci_adapter.rs:399,877` use `fs::read`/`fs::write` which allocate the entire file into a single contiguous `Vec<u8>`. For OCI upload metadata and tail files (typically small) this is acceptable, but the pattern is also used for upload body paths where the entire upload body is read at once. For large uploads, this creates memory pressure.
- [ ] **[LOW]** `object_store.rs:316` opens file without buffering — `crates/server/src/object_store.rs:316` `File::open(object.path())` for local file reads has no `BufReader` wrapper. For multi-GB chunk reconstruction reads, this results in per-byte syscalls through the kernel page cache.

### 6. Algorithm Efficiency

- [ ] **[MEDIUM]** Memory cache eviction is O(n) — `crates/cache/src/memory.rs:112-119` `evict_oldest_entry` performs `entries.iter().min_by_key(|(_, entry)| entry.inserted_at)` which is a linear scan of ALL entries. With the default `max_entries = 4096`, every cache put when the cache is full triggers up to 4096 comparisons. Use a `BinaryHeap<(Instant, ReconstructionCacheKey)>` or `BTreeMap<Instant, ReconstructionCacheKey>` for O(log n) eviction.
- [ ] **[MEDIUM]** Memory cache `get` has thundering herd on expiry — `crates/cache/src/memory.rs:44-68` When a cached entry expires, all concurrent readers observe the expiry in the read lock (line 53), then each acquires a write lock (line 61) to remove the entry. Under high concurrency (e.g., 100 simultaneous reconstruction requests for the same popular file), 100 tasks block on the write lock sequentially. The first writer removes the entry; the other 99 writers acquire the lock, re-check, and find the entry already removed. Use `tokio::sync::RwLock` or a compare-and-swap pattern to avoid this.
- [ ] **[LOW]** `record_completed_chunks` sorts on every `finish()` — `crates/server/src/upload_ingest/mod.rs:326` `sort_unstable_by_key(|outcome| outcome.sequence)` runs unconditionally on every upload completion. In the common case where chunks complete in order, the sort is a no-op but still O(n log n). Check `is_sorted()` first or maintain sorted order during insertion.

### 7. Error Recovery

- [ ] **[MEDIUM]** `std::fs::create_dir_all` error discarded at server startup — `crates/server/src/app.rs:387` `.ok()` discards the IO error from hub directory creation. If the directory can't be created (permissions, disk full), the subsequent `LocalIndexStore::new(hub_root)` fails with a confusing SQLite error ("unable to open database file") instead of the actual cause ("could not create hub directory"). Log the error and propagate it.
- [ ] **[LOW]** `reqwest::Client::builder().build().ok()` silently degrades webhook delivery — `crates/server/src/app.rs:401-404` If the HTTP client fails to build (TLS misconfiguration, system cert store unavailable), `http_client` becomes `None`. All webhook deliveries silently become no-ops rather than failing fast at startup. Should log a warning.

### 8. Dependency Version Pinning

- [ ] **[INFO]** Workspace pins exact minor versions for all dependencies — `Cargo.toml:12-69` All workspace dependencies use exact minor version pins (e.g., `reqwest = "0.12.28"`, `sqlx = "0.8.6"`). This is appropriate for reproducibility but means security patches require manual bumps. Consider using `cargo update` in CI to regularly check for patch-level updates.
- [ ] **[INFO]** xet ecosystem pinned to `1.5.1` — `Cargo.toml:60-63` `xet-client`, `xet-core-structures`, `xet-data`, `xet-runtime` are all pinned to `1.5.1` but resolved to `1.5.2` in `Cargo.lock`. The workspace specifies `"1.5.1"` (compatible with `1.5.2`), but the lockfile pins `1.5.2`. Verify that `1.5.2` is intentional and has no regressions.

### 9. Feature Interaction Bugs

- [ ] **[MEDIUM]** `tower` dependency in `hub_api` is unused — `crates/hub_api/Cargo.toml:31` `tower = { workspace = true }` is declared but never imported in any `hub_api` source file. This pulls in `tower-layer` and `tower-service` transitively for no benefit. While `tower` itself is small, removing it reduces the dependency graph for `hub_api`.
- [ ] **[MEDIUM]** `tokio` dependency in `metrics` is unused — `crates/metrics/Cargo.toml:15` `tokio = { workspace = true }` is declared but never imported in `crates/metrics/src/`. The `tokio` crate brings in `mio`, `pin-project-lite`, and `bytes` transitively. Removing it shrinks the metrics crate's dependency footprint.
- [ ] **[MEDIUM]** `tower-http` in `metrics` is unused — `crates/metrics/Cargo.toml:14` `tower-http = { workspace = true }` with no features. No `tower_http` import exists in `crates/metrics/src/`. This is dead weight.

### 10. LFS Object Size Truncation

- [ ] **[MEDIUM]** LFS object size truncated on `as i64` cast — `crates/index/src/hub_postgres.rs:409` `data.len() as i64` and `crates/index/src/hub_local_sqlite.rs:340` `data.len() as i64`. On 64-bit systems, `usize` is 8 bytes and `i64` is 8 bytes, so values above `i64::MAX` (9,223,372,036,854,775,807 bytes = ~9.2 EB) silently wrap to negative values. While unlikely for current LFS objects, the PostgreSQL column type is `bigint` (signed 8 bytes), and a malicious or buggy client could send `usize::MAX`-sized data length. Use `i64::try_from(data.len()).map_err(|_| Error::Overflow)?` for safe conversion.

---

## CODE QUALITY AUDIT PASS 3 — Additional Findings

### Dead Test Infrastructure

- [ ] **[MEDIUM]** `REPOSITORY_REFERENCE_PROBE_*` statics leak test infrastructure into production binary — `crates/server/src/backend.rs:44-48` Three `LazyLock<Mutex<...>>` and `LazyLock<Arc<AsyncMutex<()>>>` statics, plus 4 associated functions (`reset_repository_reference_probe_count_for_hash`, `clear_repository_reference_probe_filter`, `repository_reference_probe_count`, `lock_repository_reference_probe_test`), are `pub` but exclusively used in tests. They are not gated behind `#[cfg(test)]` and are compiled into the production binary. Gate them with `#[cfg(test)]` to eliminate ~100 bytes of static state from the release binary.

### Documentation Copy-Paste Bugs

- [ ] **[MEDIUM]** `with_index_postgres_url` has wrong doc comment — `crates/server/src/config/mod.rs:661-666` Doc says "Enables local bearer-token verification with the supplied signing key" — this is a copy-paste from `with_token_signing_key` at line 572. The doc should describe Postgres URL configuration, not bearer-token verification.

---

## PERFORMANCE/ARCHITECTURE AUDIT PASS 3 — Validated Findings

### Finding 1: 16 `block_on` calls in `hub_postgres.rs` — VALIDATED

Every `HubStore` method in `crates/index/src/hub_postgres.rs` (lines 20-577) uses `tokio::task::block_in_place || Handle::current().block_on(async { ... })` to execute sqlx queries synchronously. There are 16 occurrences of this pattern across: `create_repo` (31), `get_repo` (81), `list_repos` (109), `search_repos` (142), `create_revision` (201), `list_revisions` (270), `resolve_revision` (301), `store_files` (344), `get_files` (371), `put_lfs_object` (400), `get_lfs_object` (421), `has_lfs_object` (441), `create_webhook` (468), `list_webhooks` (499), `delete_webhook` (531), `webhooks_for_event` (550). **Each call blocks a tokio worker thread** for the duration of the SQL query. The `block_in_place` API explicitly signals to tokio that the thread will block, so the runtime avoids scheduling other tasks on it — but the thread remains occupied. With the default 10-connection Postgres pool and tokio's default worker count, 16 concurrent hub API calls could block 16 worker threads and exhaust the connection pool simultaneously. This is a real performance bottleneck. These methods should be native `async fn` on a dedicated async trait.

- [ ] **[VALIDATED]** 16 `block_on` calls in `hub_postgres.rs` block tokio worker threads — Every HubStore method wraps sqlx calls in `block_in_place || handle.block_on()`. Each call occupies a tokio worker thread for the SQL query duration. With default pool size 10, 16 concurrent hub API requests block 16 threads and exhaust connections.

### Finding 2: Zero BufWriter/BufReader usage — VALIDATED

grep across the entire workspace for `BufWriter` and `BufReader` returns zero matches in any `.rs` source file (only mentions in TODO.md itself). All local file I/O uses raw `File::create`/`File::open` without buffering. The `object_store` crate provides its own internal buffering for S3 operations, but direct local file operations in `server/src/object_store.rs:316`, `oci_adapter/src/lib.rs` (upload body paths), and other local FS code are unbuffered. For multi-GB LFS uploads writing in small chunks, each `write()` goes directly to the kernel as a syscall. **This is confirmed zero.**

- [ ] **[VALIDATED]** Zero `BufWriter`/`BufReader` usage in entire codebase — grep for `BufWriter|BufReader` returns zero matches in `.rs` files. All local file I/O is unbuffered. The `object_store` crate handles its own S3 buffering, but direct `File::create`/`File::open` calls throughout the codebase produce excessive syscalls.

### Finding 3: JWKS mutex lock contention — PARTIAL (thundering-herd is real, but "held during HTTP" claim is wrong)

In `crates/server/src/jwks_provider.rs:121-151`, the `get_or_refresh_keys` method acquires the `std::sync::Mutex` (line 123-126), checks the cache TTL, **releases the lock** (end of inner scope at line 132), makes a blocking HTTP request (line 134-140), then re-acquires the lock (line 142-145) to update the cache. **The lock is NOT held during the HTTP call.** However, the pass 2 finding about thundering-herd is accurate: between the first release and the re-acquire, multiple threads can observe the stale cache simultaneously and all make redundant HTTP requests. The original "mutex is held while doing HTTP fetches" claim is **refuted**, but the thundering-herd race is **validated**. The OIDC provider (`oidc_provider.rs:134-141`) has a similar pattern but uses `get_cached_keys()` which never triggers HTTP refresh (only returns cached or errors), so it avoids the thundering-herd problem at the cost of serving stale keys.

- [ ] **[PARTIAL]** JWKS mutex contention — The lock is released BEFORE the HTTP request (line 132), not held during it. The "held during HTTP" claim is wrong. However, the thundering-herd race is real: under concurrent token verification, multiple threads observe stale cache, all release the mutex, all make redundant HTTP fetches, then re-acquire to write. Use a `tokio::sync::Mutex` or compare-and-swap to prevent redundant fetches.

### Finding 4: God trait `IndexStore` method count — VALIDATED (~25 required methods)

Counting methods in `crates/index/src/store.rs`:

**`IndexStore` (sync trait, lines 16-311):** 20 required methods + 4 default implementations (`contains_xorb`, `visit_dedupe_shard_mappings`, `visit_quarantine_candidates`, `visit_retention_holds`, `visit_webhook_deliveries`, `visit_provider_repository_states` — 6 defaults) = **26 total methods**. Required: `reconstruction`, `list_reconstruction_file_ids`, `delete_reconstruction`, `contains_object`, `dedupe_shard_mapping`, `list_dedupe_shard_mappings`, `delete_dedupe_shard_mapping`, `quarantine_candidate`, `list_quarantine_candidates`, `upsert_quarantine_candidate`, `delete_quarantine_candidate`, `retention_hold`, `list_retention_holds`, `upsert_retention_hold`, `delete_retention_hold`, `record_webhook_delivery`, `list_webhook_deliveries`, `delete_webhook_delivery`, `provider_repository_state`, `list_provider_repository_states`, `upsert_provider_repository_state`, `delete_provider_repository_state` = **22 required**.

**`AsyncIndexStore` (async trait, lines 314-591):** Adds `insert_reconstruction`, `insert_object`, `upsert_dedupe_shard_mapping` beyond the sync version = **25 required methods** + 6 defaults = 31 total.

The finding says "~25 methods" — this is accurate for `AsyncIndexStore` required methods and close for `IndexStore`. The trait is genuinely large.

- [ ] **[VALIDATED]** `IndexStore` has ~25 required methods — Counting `crates/index/src/store.rs`: `IndexStore` has 22 required + 6 default = 28 total. `AsyncIndexStore` has 25 required + 6 default = 31 total. The finding of "~25" is accurate. 6 of the methods are visitor-pattern defaults that could be extracted.

### Finding 5: `xet_adapter → server_core` dependency direction — PARTIAL (not just AuthError/AuthProvider)

`crates/xet_adapter/Cargo.toml:18` declares `shardline-server-core.workspace = true`. grep for `use shardline_server_core` in xet_adapter reveals imports: `ServerObjectStore`, `chunk_hash`, `read_full_object`, `ShardMetadataLimits`, `InvalidSerializedShardError`, `ServerObjectStoreError`, `DEFAULT_SHARD_METADATA_LIMITS`. The pass 2 finding claims "xet_adapter only needs AuthError and AuthProvider from server_core" — this is **wrong**. xet_adapter uses `ServerObjectStore` (the core object store abstraction), `chunk_hash` (utility function), `ShardMetadataLimits`, and error types. These are not auth-related. The dependency direction is architecturally questionable (a protocol adapter depending on the server core), but the types used are legitimately needed for xorb/shard operations. Extracting `ServerObjectStore` + `chunk_hash` + `ShardMetadataLimits` into a lightweight `shardline-storage-core` crate could resolve the direction violation, but it's more complex than just extracting auth types.

- [ ] **[PARTIAL]** `xet_adapter → server_core` dependency — The claim that xet_adapter "only needs AuthError and AuthProvider" is **wrong**. xet_adapter imports `ServerObjectStore`, `chunk_hash`, `read_full_object`, `ShardMetadataLimits`, `InvalidSerializedShardError`, `ServerObjectStoreError`, and `DEFAULT_SHARD_METADATA_LIMITS`. These are storage/protocol types, not auth types. The dependency is architecturally inverted but the types are genuinely needed. Extract a `shardline-storage-core` crate with `ServerObjectStore` + related types.

### Finding 6: Duplicate reconstruction metrics recording — PARTIAL (different metrics, not double-counting)

In `crates/server/src/app/reconstruction_routes.rs:65-66`:
```rust
shardline_metrics::record_reconstruction(true, elapsed, chunks);
shardline_metrics::metrics().xet.record_reconstruction(true, elapsed, chunks);
```

The first call routes to `crates/metrics/src/reconstruction.rs` which records to Prometheus metrics named `shardline_reconstruction_requests_total`, `shardline_reconstruction_duration_seconds`, and `shardline_reconstruction_chunks_fetched_total`.

The second call routes to `crates/metrics/src/xet.rs` which records to `shardline_xet_reconstruction_requests_total`, `shardline_xet_reconstruction_duration_seconds`, and `shardline_xet_reconstruction_chunks_total`.

**These are DIFFERENT Prometheus metrics with different names.** This is intentional metric separation (generic reconstruction vs xet-specific reconstruction), not accidental double-counting. The finding's claim of "double-counts reconstruction metrics in the Prometheus registry" is **incorrect** — they are distinct metric series. However, the same event being recorded to two separate metric families is arguably redundant and could be consolidated.

- [ ] **[PARTIAL]** Duplicate reconstruction metrics — Lines 65-66 call both `record_reconstruction` (→ `shardline_reconstruction_*`) and `metrics().xet.record_reconstruction` (→ `shardline_xet_reconstruction_*`). These are DIFFERENT Prometheus metric names, so it's not double-counting. It's intentional but redundant metric separation. Consider consolidating into one metric family with labels.

### Finding 7: Memory cache O(n) eviction — VALIDATED

In `crates/cache/src/memory.rs:112-119`, `evict_oldest_entry` performs:
```rust
entries.iter().min_by_key(|(_key, entry)| entry.inserted_at)
```
This is a **linear scan** of all entries in the `HashMap<ReconstructionCacheKey, MemoryEntry>`. With the default `max_entries = 4096`, every cache put when the cache is full triggers up to 4096 comparisons to find the oldest entry. The data structure is `HashMap` (line 24), which has no ordering. This is O(n) per eviction. A `BinaryHeap<(Instant, ReconstructionCacheKey)>` or `BTreeMap<Instant, ReconstructionCacheKey>` would provide O(log n) eviction. Additionally, the `prune_expired_entries` function (line 108-110) uses `HashMap::retain` which is also O(n), but this is called on every `put` and scans all entries to remove expired ones before potentially evicting.

- [ ] **[VALIDATED]** Memory cache eviction is O(n) — `crates/cache/src/memory.rs:112-119` uses `entries.iter().min_by_key()` on a `HashMap`, which is a full linear scan. With `max_entries = 4096`, each eviction compares up to 4096 entries. Use `BinaryHeap` or `BTreeMap` for O(log n).

---

## CODE QUALITY AUDIT PASS 3 — Validated Findings

### 1. Dead Code Validation

- [ ] **[VALIDATED]** All 20 dead code warnings confirmed by `cargo check` — Ran `cargo check 2>&1 | grep "warning:.*never used"` and every item from the dead code section was emitted by the compiler:
  - `create_resumable_object_upload`, `upload_resumable_object_part`, `complete_resumable_object_upload`, `abort_resumable_object_upload` (server backend)
  - `LOCAL_DIRECTORY_MODE`, `LOCAL_FILE_MODE` (server local_fs)
  - `write_file_atomically`, `run_before_local_write_hook`, `write_file_atomically_unix`, `open_anchored_target`, `write_anchored_temporary_file`, `ensure_parent_path_matches_anchor`, `anchored_path_options`, `invalid_local_path_error` (server local_fs)
  - `stable_hex_id` (server protocol_support)
  - `managed_protocol_object_identity` x2 (server dispatch + xet)
  - All 7 unused imports also confirmed: `RecordStore`, `OciPath`, `IndexError`, `WEBHOOK_DELIVERY_FUTURE_SKEW_SECONDS`, `OpsRecordKind`, `provider_directory`, `managed_protocol_object_identity`

### 2. Duplicate Code Validation — `oci_adapter` Duplication

- [ ] **[VALIDATED]** `server/src/oci_adapter.rs` (916 lines) and `oci_adapter/src/lib.rs` (1051 lines) are structurally near-identical — diff shows 419 changed lines across 931 diff hunks. The core data structures (`OciUploadSession`, `OciS3MultipartUploadSession`, `SerializableSha256State`) are line-for-line identical except for visibility (`pub(crate)` vs `pub`) and error type (`ServerError` vs `OciAdapterError`). The `oci_adapter` crate adds module structure (`mod error`, `mod protocol_support`, `mod traits`), doc comments, and `#![deny(unsafe_code)]`. The `server` copy has none of these. The SHA-256 manual compression implementation is identical in both files. Any bug fix must be applied in both places.

### 3. Error Swallowing Validation

- [ ] **[VALIDATED]** `let _ = state` at `routes.rs:453,1182` is NOT error swallowing — These lines use the pattern `let _ = state.store.get_repo(&name).map_err(...)?.ok_or(HubApiError::RepoNotFound)?;`. The `?` operator propagates all errors (store errors via `map_err` + `?`, missing repos via `.ok_or(...)` + `?`). The `let _ =` only discards the successful `Ok(Repo)` value because the code only needs to assert the repo exists. This is correct usage.

- [ ] **[VALIDATED]** `.ok()` on auth at `routes.rs:241` is intentional design — The `whoami` endpoint uses `.and_then(|auth| auth.authorize(&headers, TokenScope::Read).ok())` to convert auth failures to `None`, mapping to `"anonymous"`. This is the correct behavior for a whoami endpoint that should return info for both authenticated and unauthenticated users.

- [ ] **[VALIDATED]** Log injection finding at `routes.rs:66` confirmed — `tracing::warn!("webhook delivery to {url} failed: {e}")` logs the full user-supplied webhook URL. While `tracing` is structured, the URL string is user-controlled and could contain newlines/control characters that corrupt log output. The finding is valid.

### 4. Large Function Validation — `main()` 538 Lines

- [ ] **[VALIDATED]** `main()` is 585 lines (correction: original finding said 538), dispatching to 21 distinct `CliCommand` variants via a single `match` block — Each arm follows the same pattern: call a handler function, print success or error, return `ExitCode`. The function is monolithic but each arm is a thin delegation (5-20 lines per arm). Refactoring with a `match` + delegation pattern would move each arm into a helper, but the current structure is already delegation-based (all handlers are separate functions in the `shardline` crate). The main function is a pure dispatcher with no business logic.

### 5. Static Mut UB Validation

- [ ] **[VALIDATED]** `static mut TEMP_DIR` is undefined behavior — Both `crates/hub_api/tests/git_smart_http.rs:31` and `crates/hub_api/tests/dataset_and_webhooks.rs:25` use `static mut TEMP_DIR: Option<TempDir> = None;`. This is accessed via `unsafe` blocks in `setup()`. Writing to `static mut` from multiple threads is UB per the Rust reference. Even in single-threaded `#[test]` functions, `static mut` violates aliasing rules. The `TempDir` in the `Some` variant is `ManuallyDrop`-equivalent (it runs destructors on drop), and the `unsafe` blocks around it are unsound. This is confirmed UB and should use `static TEMP_DIR: OnceLock<Mutex<Option<TempDir>>>` or similar.

### 6. Unused Dependencies Validation

- [ ] **[VALIDATED]** `tower` in `hub_api` `[dependencies]` is unused — `crates/hub_api/Cargo.toml:31` lists `tower = "0.5"` as a regular dependency. Zero imports of `tower` exist in `crates/hub_api/src/`. However, `tower` IS used in test files (`tests/dataset_and_webhooks.rs:22`, `tests/git_smart_http.rs:28`) via `use tower::ServiceExt`, which is covered by the separate `[dev-dependencies]` entry at line 40 (`tower = { version = "0.5", features = ["util"] }`). The `[dependencies]` entry should be removed; the `[dev-dependencies]` entry is sufficient.

- [ ] **[VALIDATED]** `sha2` in `cli` is unused — `crates/cli/Cargo.toml:42` lists `sha2.workspace = true`. Grep for `sha2`, `Sha256`, and `Digest` in `crates/cli/src/` returns zero results. The dependency is completely unused.

- [ ] **[VALIDATED]** `tokio` in `metrics` is unused — `crates/metrics/Cargo.toml:15` lists `tokio = { workspace = true }`. Grep for `tokio` in `crates/metrics/src/` returns zero results. The dependency is unused.

- [ ] **[VALIDATED]** `tower-http` in `metrics` is unused — `crates/metrics/Cargo.toml:14` lists `tower-http = { workspace = true }` with no features. Grep for `tower_http` or `tower-http` in `crates/metrics/src/` returns zero results. The dependency is unused. Note: `tower` (not `tower-http`) IS used in `metrics/src/middleware.rs:6` via `use tower::{Layer, Service}`, so the `tower` dependency in metrics is correctly listed.

### 7. Additional Validated Finding

- [ ] **[VALIDATED]** Test SQL schema duplicated across hub_api test files — `crates/hub_api/tests/git_smart_http.rs:39-85` and `crates/hub_api/tests/dataset_and_webhooks.rs:33-78` contain nearly identical 45-line `CREATE TABLE` SQL blocks defining 6 tables (`shardline_hub_repos`, `shardline_hub_revisions`, `shardline_hub_file_entries`, `shardline_hub_lfs_pointers`, `shardline_hub_file_changes`, `shardline_hub_webhooks`). A change to the schema must be replicated in both files. Should extract into a shared `hub_api/tests/common/mod.rs` helper.

---

## SECURITY AUDIT PASS 3 — Validated Findings

All findings below were validated by writing concrete tests in `crates/hub_api/tests/security_validation.rs`. Each test reads actual source code or exercises real code paths to definitively prove the vulnerability exists.

### 1. JWT Signature Verification Never Performed — **[VALIDATED]** (CRITICAL)

**Source:** `crates/server/src/oidc_provider.rs:143-211`, `crates/server/src/jwks_provider.rs:153-211`

Both the OIDC and JWKS providers fetch JWKS keys, decode the JWT header and signature, but **never perform cryptographic verification**. The variables holding these values are all prefixed with `_` (Rust's unused-variable convention):

- `_keys` — fetched JWKS keys, never used for verification
- `_header` — decoded JWT header JSON, never inspected
- `_sig_bytes` — decoded signature bytes, never verified against any key

The only validations are:
- **OIDC:** `exp < now` (expiration) and `iss != self.issuer` (issuer)
- **JWKS:** `exp < now` (expiration only — **no issuer check at all**)

**Attack vector:** An attacker can forge a JWT with a valid-looking `iss` and far-future `exp`, set `alg: "none"`, omit the signature entirely, and the token will be accepted. Combined with `unwrap_or(u64::MAX)` for missing `exp` (tokens without expiration never expire), an attacker can create permanent forged tokens.

**Tests confirmed:** `validate_jwt_signature_never_checked_oidc`, `validate_jwt_signature_never_checked_jwks`, `validate_jwt_alg_none_attack_possible`, `validate_missing_exp_never_expires` — all pass.

### 2. Webhook SSRF — No URL Validation — **[VALIDATED]** (CRITICAL)

**Source:** `crates/hub_api/src/routes.rs:73-97` (delivery), `routes.rs:1173-1191` (creation)

Neither the webhook creation endpoint nor the delivery function validates URLs:

- `webhook_create` stores `request.url` directly in the database via `state.store.create_webhook(&name, &request.url, ...)` with zero validation
- `deliver_one_webhook` sends `client.post(url)` directly to the user-supplied URL
- No middleware or layer applies URL validation

**Confirmed by store-level test:** A dedicated test (`validate_webhook_accepts_dangerous_urls_at_store_level`) creates a `LocalIndexStore`, and directly calls `create_webhook` with dangerous URLs. All succeed:
- `file:///etc/passwd` — accepted (SSRF to read local files)
- `http://127.0.0.1:6379/INFO` — accepted (SSRF to internal services)
- `gopher://internal:7070/status` — accepted (arbitrary protocol)
- `http://169.254.169.254/latest/meta-data/` — accepted (cloud metadata)

**Tests confirmed:** `validate_deliver_one_webhook_no_url_validation`, `validate_webhook_full_path_no_validation`, `validate_webhook_accepts_dangerous_urls_at_store_level` — all pass.

### 3. Hub API Has No Body Size Limit — **[VALIDATED]** (HIGH)

**Source:** `crates/hub_api/src/lib.rs:46-48`, `crates/server/src/app.rs:221`

The hub API router has **no `DefaultBodyLimit` layer**:
```rust
pub fn hub_routes<S: Clone + Send + Sync + 'static>() -> Router<S> {
    routes::router()  // No body limit applied
}
```

Compare with the main server which applies `DefaultBodyLimit::max(max_request_body_bytes)` at `app.rs:221`.

The commit handler at `routes.rs:544` accepts `body: String` — the entire request body is loaded into memory with no size check. The LFS upload handler at `routes.rs:863` accepts `body: bytes::Bytes` — same issue.

**Attack vector:** An attacker can send a multi-GB NDJSON commit body or LFS upload to exhaust server memory (DoS).

**Integration test confirmed:** `validate_commit_no_body_size_validation` sends a multi-file commit to the hub API and verifies it is accepted (200 OK) without any body size rejection.

**Tests confirmed:** `validate_hub_router_has_no_body_limit`, `validate_commit_handler_unbounded_body`, `validate_lfs_upload_unbounded_body`, `validate_commit_no_body_size_validation` — all pass.

### 4. Path Traversal in Commit File Paths — **[VALIDATED]** (HIGH)

**Source:** `crates/hub_api/src/commit.rs:82-101`, `crates/hub_api/src/routes.rs:575-608`

The `parse_ndjson_commit` function accepts file paths from user JSON without any validation for traversal sequences, absolute paths, null bytes, or control characters. The paths are then stored directly in the database via `apply_commit`.

**Confirmed by functional tests:**
- `parse_ndjson_commit` accepts `../../etc/passwd` as a valid path → `CommitInstruction::InlineFile { path: "../../etc/passwd", ... }`
- `parse_ndjson_commit` accepts `/etc/shadow` as a valid absolute path
- `apply_commit` stores the path directly via `path: path.clone()` into `HubFileEntry`
- No validation functions exist in `commit.rs` (`validate_path`, `sanitize_path`, `check_traversal`, `is_safe_path` — none found)

**Attack vector:** An attacker can create files at `../../etc/passwd` logical paths in the hub database. When the file tree is listed or files are resolved, these traversal paths are returned to clients. Depending on downstream consumers, this could cause path confusion or overwrites.

**Tests confirmed:** `validate_commit_accepts_traversal_paths`, `validate_commit_accepts_absolute_paths`, `validate_commit_has_no_path_validation`, `validate_apply_commit_stores_traversal_paths` — all pass.

### 5. Decompression Bomb — No Size Limit — **[VALIDATED]** (HIGH)

**Source:** `crates/hub_api/src/git/smart_http.rs:713-722`

The `decompress_zlib` function calls `decoder.read_to_end(&mut output)` with no size limit:
```rust
fn decompress_zlib(data: &[u8]) -> Result<(Vec<u8>, usize), Box<dyn std::error::Error>> {
    let mut decoder = ZlibDecoder::new(data);
    let mut output = Vec::new();
    decoder.read_to_end(&mut output)?;  // No size limit!
    ...
}
```

No `BufReader`, `take()`, or bounded reader is used. The receive-pack path calls this function without any pre-filtering of compressed data sizes.

**Attack vector:** A malicious `receive_pack` request can include a tiny compressed blob (e.g., a few KB) that decompresses to gigabytes, exhausting server memory.

**Tests confirmed:** `validate_decompress_zlib_no_size_limit`, `validate_pack_parsing_no_compressed_size_check` — both pass.

### 6. whoami Hardcodes is_admin: true — **[VALIDATED]** (MEDIUM)

**Source:** `crates/hub_api/src/routes.rs:233-247`

The `/api/whoami-v2` endpoint always returns `is_admin: true` regardless of the authenticated user's actual role:
```rust
Ok(Json(WhoamiResponse {
    name,
    is_admin: true,  // Hardcoded
}))
```

**Impact:** If clients rely on the `is_admin` field for authorization decisions, all users (including anonymous) are treated as admins.

**Test confirmed:** `validate_whoami_hardcoded_admin` — passes.

### 7. CasError Leaks Internal Details — **[VALIDATED]** (HIGH)

**Source:** `crates/hub_api/src/error.rs:51-52,68`

`HubApiError::CasError(String)` embeds arbitrary internal error strings (from 38+ `.map_err(|e| HubApiError::CasError(e.to_string()))` calls in `routes.rs`). The `IntoResponse` impl serializes `self.to_string()` as JSON in the response body. This leaks internal storage paths, database errors, and index schema details to clients.

**Test confirmed:** `validate_cas_error_leaks_internals` — passes.

### 8. NDJSON Commit Has No Instruction Count Limit — **[VALIDATED]** (MEDIUM)

**Source:** `crates/hub_api/src/commit.rs:56-163`

`parse_ndjson_commit` accumulates instructions into an unbounded `Vec` with no count limit. An attacker can craft a body with millions of tiny valid JSON lines to cause excessive memory allocation and CPU consumption.

**Test confirmed:** `validate_ndjson_unbounded_instruction_count` — passes. Verified no `MAX_INSTRUCTIONS`, `instruction_count`, or `.len() >` check exists in the function.

### Summary

| # | Finding | Severity | Status | Test(s) |
|---|---------|----------|--------|---------|
| 1 | JWT signature never verified | CRITICAL | **[VALIDATED]** | 4 tests pass |
| 2 | Webhook SSRF — no URL validation | CRITICAL | **[VALIDATED]** | 3 tests pass |
| 3 | Hub API has no body size limit | HIGH | **[VALIDATED]** | 4 tests pass |
| 4 | Path traversal in commit paths | HIGH | **[VALIDATED]** | 4 tests pass |
| 5 | Decompression bomb — no size limit | HIGH | **[VALIDATED]** | 2 tests pass |
| 6 | whoami hardcodes is_admin: true | MEDIUM | **[VALIDATED]** | 1 test passes |
| 7 | CasError leaks internal details | HIGH | **[VALIDATED]** | 1 test passes |
| 8 | NDJSON no instruction count limit | MEDIUM | **[VALIDATED]** | 1 test passes |

