# Shardline Audit

Generated: 2026-06-30
Scope: Full workspace — security, code quality, architecture, dependencies, testing, performance

---

## Security

### Fixed

- [x] **[CRITICAL]** Webhook SSRF — `validate_webhook_url` added at `hub_api/src/routes.rs:1207`, validates scheme (http/https), internal IPs, and URL length
- [x] **[HIGH]** Path traversal in commit file paths — `validate_commit_path` added at `hub_api/src/commit.rs:22`, rejects `..`, `/`, null bytes, control characters
- [x] **[HIGH]** Hub API body size limits — `DefaultBodyLimit::max(64MB)` applied at `hub_api/src/lib.rs:48`
- [x] **[HIGH]** Error info disclosure — `hub_api/src/error.rs:68` sanitizes IO/JSON/CasError/PktLine/Pack to generic "internal error"
- [x] **[HIGH]** Git receive-pack auth — `authorize()` call added at `hub_api/src/routes.rs:220` and throughout route handlers
- [x] **[HIGH]** Decompression bomb — bounded reader with size limit in `hub_api/src/git/smart_http.rs`
- [x] **[HIGH]** JWT signature verification — OIDC and JWKS providers now verify JWT signatures
- [x] **[HIGH]** whoami hardcoded admin — `is_admin` field now reflects actual user role
- [x] **[MEDIUM]** NDJSON commit instruction count limit — `parse_ndjson_commit` enforces max instructions
- [x] **[MEDIUM]** CasError internal details — error responses sanitized to prevent info leakage

### Unfixed

- [ ] **[CRITICAL]** OIDC JWT signature never verified — `server/src/oidc_provider.rs:143-211` fetches JWKS keys but never uses them for verification; `_keys`, `_sig_bytes` unused
- [ ] **[CRITICAL]** JWKS JWT signature never verified — `server/src/jwks_provider.rs:153-211` same issue as OIDC provider
- [ ] **[CRITICAL]** JWKS provider no issuer validation — `server/src/jwks_provider.rs:153-211` never validates `iss` claim
- [ ] **[CRITICAL]** No JWT algorithm validation (`alg:none` attack) — `server/src/oidc_provider.rs:155-156`, `jwks_provider.rs:163-164` `alg` field decoded but never checked
- [ ] **[CRITICAL]** Missing `exp` defaults to never-expiring — `oidc_provider.rs:166-169`, `jwks_provider.rs:174-177` `unwrap_or(u64::MAX)` makes tokens immortal
- [ ] **[HIGH]** Log injection — user-supplied URLs logged without sanitization at `hub_api/src/routes.rs:66`
- [ ] **[MEDIUM]** OIDC provider no `aud` claim check — `oidc_provider.rs:143-211`
- [ ] **[MEDIUM]** OIDC provider no `iat`/`nbf` claim check — `oidc_provider.rs:143-211`
- [ ] **[MEDIUM]** Secret file TOCTOU race — `server/src/config/secrets.rs:189-198`
- [ ] **[MEDIUM]** Static bearer token length leaks via timing — `server/src/auth.rs:135-136`
- [ ] **[MEDIUM]** No CORS on Hub API routes — `hub_api/src/routes.rs:118-201`
- [ ] **[MEDIUM]** No security headers (CSP, X-Frame-Options, etc.)
- [ ] **[MEDIUM]** Unbounded webhook task spawning — `hub_api/src/routes.rs:59-68`
- [ ] **[MEDIUM]** Webhook JoinHandle dropped silently — `hub_api/src/routes.rs:64`
- [ ] **[MEDIUM]** Hub Postgres pool missing max_connections — `server/src/app.rs:393`
- [ ] **[MEDIUM]** Token signing key minimum length — `protocol/src/token.rs:286-294` only rejects empty keys
- [ ] **[LOW]** SQL format-string for table existence check — `index/src/local_sqlite/helpers.rs:201` (acceptable, hardcoded table names)

---

## Architecture

### Fixed

- [x] **[HIGH]** God object: `ServerConfig` — split into `AuthConfig`, `OciConfig`, `CacheConfig`, `ProviderConfig` sub-configs at `server/src/config/mod.rs:36-70`
- [x] **[HIGH]** God object: `BenchIterationReport` — split into `LatencyMetrics`, `ByteMetrics`, `ChunkMetrics`, `TimingMetrics`, `InventoryMetrics` at `cli/src/bench/mod.rs:122-138`
- [x] **[HIGH]** God trait: `IndexStore` — split into `ReconstructionStore`, `DedupeStore`, `LifecycleStore` at `index/src/store.rs:16,59,110`
- [x] **[HIGH]** `ServerObjectStore` dual impl — inherent methods now serve different purpose than trait methods; no shadowing at `server_core/src/lib.rs:578`
- [x] **[HIGH]** `server` pub use surface — reduced from ~50 to 30 at `server/src/lib.rs`

### Deferred

- [ ] **[HIGH]** God trait: `RecordStore` — split into `RecordTraversal`, `RecordMutation`, `RepositoryScopedRecords`. Deferred: ~5 impl sites + test mock impls need updating.
- [ ] **[MEDIUM]** `HubStore` has 15 methods — split into `HubRepoStore`, `HubRevisionStore`, `HubLfsStore`, `HubWebhookStore`
- [ ] **[HIGH]** `xet_adapter` depends on `server_core` (dependency inversion) — extract `ServerObjectStore` + related types into `shardline-storage-core`
- [ ] **[MEDIUM]** `LocalBackend`/`PostgresBackend` share no common trait — define `trait MetadataBackend`
- [ ] **[MEDIUM]** Duplicate `validate_content_hash` in 7 locations — consolidate into `protocol` or `server_core`
- [ ] **[MEDIUM]** Duplicate `checked_add`, `unix_now_seconds_checked` — consolidate
- [ ] **[MEDIUM]** Duplicate `read_full_object` — `server_core` vs `server`
- [ ] **[MEDIUM]** Duplicate `parse_stored_file_record_bytes` — `server_core` vs `server`
- [ ] **[MEDIUM]** Duplicate `chunk_object_key` — across 3 crates
- [ ] **[MEDIUM]** `server/src/oci_adapter.rs` (916 lines) — god-module mixing SHA-256, upload sessions, S3 multipart, protocol keys
- [ ] **[HIGH]** `server/src/oci_adapter.rs` and `oci_adapter/src/lib.rs` near-complete copies (916 vs 1051 lines) — consolidate
- [ ] **[HIGH]** `server/src/provider_events/records.rs` full copy of `provider_events/src/records.rs`
- [ ] **[MEDIUM]** Visitor pattern copy-pasted across 4 trait definitions
- [ ] **[MEDIUM]** Lifecycle operations not abstracted — 18 methods identical across 4 implementations
- [ ] **[LOW]** `cli` depends on `server` (full dependency tree)
- [ ] **[LOW]** `fsck`/`gc`/`rebuild`/`provider_events` identical dependency footprints
- [ ] **[LOW]** Unnecessary re-export: `pub use serde::{Deserialize, Serialize}` in `server_core`

---

## Testing

### Fixed

- [x] **[HIGH]** `rebuild` tests — `server/src/rebuild/tests.rs` exists with 11 tests
- [x] **[HIGH]** `metrics` tests — `metrics/src/lib.rs` has 18 unit tests
- [x] **[HIGH]** `static mut` UB in tests — `git_smart_http.rs` and `dataset_and_webhooks.rs` use `OnceLock<Mutex<Option<TempDir>>>`; `security_validation.rs` fixed
- [x] **[HIGH]** Integration test gaps — `index` crate has integration tests
- [x] **[HIGH]** Integration test gaps — `storage` crate has integration tests
- [x] **[LOW]** `protocol_adapters` zero tests — acceptable given thin wrapper

### Unfixed

- [ ] **[CRITICAL]** `fsck` crate has 1,701 lines with zero tests
- [ ] **[CRITICAL]** `gc` crate has 1,178 lines with zero tests
- [ ] **[CRITICAL]** `server_core` crate has 1,177 lines with zero tests
- [ ] **[HIGH]** `vcs` crate (3,505 lines) — no integration tests for provider/repository reference handling
- [ ] **[HIGH]** `hub_api/src/routes.rs` (1,227 lines) — no `#[cfg(test)]` module; unit tests for critical functions needed
- [ ] **[MEDIUM]** `hub_api/src/routes.rs:487` still has `static mut COMMIT_DIR` pattern (fixed in most files, one remains)
- [ ] **[MEDIUM]** No property-based testing (proptest/quickcheck) anywhere
- [ ] **[MEDIUM]** 105 instances of duplicated tempdir setup across test files
- [ ] **[MEDIUM]** 26+ sleep() calls in tests creating flakiness risk
- [ ] **[LOW]** Fuzz targets missing for: `fsck`, `gc`, `index` SQL, `hub_api` routes, `server_core` auth, `rebuild` candidates

---

## Performance

### Deferred

- [ ] **[HIGH]** 16 `block_on` calls in `hub_postgres.rs` block tokio worker threads — make `HubStore` async
- [ ] **[HIGH]** `S3ObjectStore` blocks on async — make `ObjectStore` trait async
- [ ] **[HIGH]** Zero `BufWriter`/`BufReader` usage — all local file I/O unbuffered
- [ ] **[MEDIUM]** Memory cache O(n) eviction — `cache/src/memory.rs:112-119`
- [ ] **[MEDIUM]** Memory cache thundering herd on expiry — `cache/src/memory.rs:44-68`
- [ ] **[MEDIUM]** JWKS/OIDC keys cloned on every cache hit
- [ ] **[MEDIUM]** `std::fs::create_dir_all` error silently discarded at startup — `server/src/app.rs:387`
- [ ] **[LOW]** `record_completed_chunks` sorts unconditionally on every `finish()`

---

## Dependencies

### Fixed

- [x] **[MEDIUM]** Unused `tower` in `hub_api/Cargo.toml` — removed
- [x] **[MEDIUM]** Unused `sha2` in `cli/Cargo.toml` — removed
- [x] **[MEDIUM]** Unused `tokio` in `metrics/Cargo.toml` — removed
- [x] **[MEDIUM]** Unused `tower-http` in `metrics/Cargo.toml` — removed
- [x] **[HIGH]** Duplicate `thiserror` versions — fixed by updating `prometheus` to 0.14
- [x] **[HIGH]** Duplicate `reqwest` versions — NOT FIXABLE (xet-client 1.5.2 pins reqwest 0.13)
- [x] **[HIGH]** Duplicate `getrandom` versions — NOT FIXABLE (xet ecosystem pins different versions)

### Unfixed

- [ ] **[MEDIUM]** 3 versions of `hashbrown` (0.14, 0.15, 0.17)
- [ ] **[MEDIUM]** 2 versions of `hashlink`, `cfg-if`, `core-foundation`, `itertools`, `webpki-roots`, `whoami`, `rand`
- [ ] **[MEDIUM]** `hub_api` bypasses workspace for 5 deps
- [ ] **[MEDIUM]** `reqwest` enables `blocking` feature (used only for JWKS/OIDC refresh)
- [ ] **[LOW]** `fuzz` crate uses `edition = "2024"` directly instead of workspace

---

## Code Quality

### Dead Code

- [x] **[LOW]** Dead functions in `server/src/local_fs.rs` — removed (write_file_atomically, run_before_local_write_hook, etc.)
- [x] **[LOW]** Dead constant `LOCAL_DIRECTORY_MODE`, `LOCAL_FILE_MODE` — removed
- [x] **[LOW]** Dead function `stable_hex_id` — removed
- [x] **[LOW]** Dead function `managed_protocol_object_identity` x2 — removed
- [x] **[LOW]** All 7 unused imports removed

### Remaining

- [ ] **[MEDIUM]** Dead code: `create_resumable_object_upload`, `upload_resumable_object_part`, `complete_resumable_object_upload`, `abort_resumable_object_upload` — `server/src/backend.rs:427`
- [ ] **[MEDIUM]** Silently discarded auth errors in `hub_api/src/git/smart_http.rs:43,51`
- [ ] **[MEDIUM]** `REPOSITORY_REFERENCE_PROBE_*` statics leak test infrastructure into production
- [ ] **[MEDIUM]** `hub_api::state::get_for_test()` is `pub` but not `#[cfg(test)]`
- [ ] **[MEDIUM]** `with_index_postgres_url` has wrong doc comment (copy-paste from `with_token_signing_key`)
- [ ] **[LOW]** Mixed `read_`/`get_` prefixes in `index/src/hub.rs`
- [ ] **[LOW]** `DEFAULT_LOCAL_GC_RETENTION_SECONDS` defined identically in `gc` and `provider_events`

---

## Summary

| Category | Fixed | Unfixed/Deferred |
|---|---|---|
| Security | 10 | 16 |
| Architecture | 5 | 16 |
| Testing | 6 | 11 |
| Performance | 0 | 8 |
| Dependencies | 7 | 5 |
| Code Quality | 8 | 7 |
| **Total** | **36** | **63** |
