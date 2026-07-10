# Shardline Test Coverage — Current State

Last updated: 2026-07-10 (post deep audit + ~600 tests added)

## Coverage Summary

| Crate | Status | Notes |
|-------|--------|-------|
| `protocol` | ✅ Good | hash, ranges, security, token all tested |
| `storage` | ✅ Good | local.rs, anchored_fs.rs (38 tests), object.rs tested |
| `storage/s3.rs` | ⚠️ E2E only | Config tests exist; ObjectStore methods tested via integration |
| `server_core` | ✅ Good | chunk keys, content_hash, Blackhole, auth providers (14 tests) |
| `index` | ✅ Good | SQLite store (34 tests), record key, memory store |
| `rebuild` | ✅ Good | All files tested (40 tests) |
| `fsck` | ✅ Good | All files tested (30 tests) |
| `gc` | ✅ Good | All files tested (58 tests) |
| `oci_adapter` | ✅ Good | Key construction, SHA256 state, upload sessions (76 tests) |
| `xet_adapter` | ✅ Good | Most files tested; ingest.rs thin wrappers |
| `vcs` | ✅ Good | All files + integration tests |
| `hub_api` | ✅ Good | auth, models, routes, git, resolve (162 tests) |
| `protocol_adapters` | ✅ Good | Both LFS + Bazel key construction tested |
| `provider_events` | ✅ Good | State management, webhook processing (30 tests) |
| `server` | ✅ Good | LFS, Bazel, OCI handlers, body_reader, chunk_store, local_backend, auth, OIDC tests (518+ tests) |
| `server/jwks_provider` | ✅ Covered | 12 tests added |
| `server/oidc_provider` | ✅ Covered | 8 tests added |
| `server/config/secrets` | ✅ Covered | Tests added |
| `cli` | ⚠️ E2E only | Commands tested via integration tests; unit tests exist for bench |
| `xet_core` | ⚠️ Partial | Lint fixed (was clippy::all); minimal tests |
| `metrics` | ⚠️ Partial | Constructor tests only |
| `cache` | ✅ Memory+Redis | Store trait untested but simple |
| `cas` | ✅ Good | Trivial crate |

## Items Previously Listed as Gaps (Now Fixed)

| File | Was | Now |
|------|-----|-----|
| `anchored_fs.rs` | ❌ Zero tests | ✅ 38 tests |
| `auth/local_ed25519.rs` | ❌ Zero tests | ✅ 7 tests |
| `auth/passthrough.rs` | ❌ Zero tests | ✅ 7 tests |
| `oidc_provider.rs` | ❌ Zero tests | ✅ 8 tests |
| `jwks_provider.rs` | ❌ Zero tests | ✅ 12 tests |
| `local_sqlite/index_store.rs` | ❌ Zero tests | ✅ 10 tests |
| `local_sqlite/record_store.rs` | ❌ Zero tests | ✅ 7 tests |
| `local_sqlite/async_index_store.rs` | ❌ Zero tests | ✅ 11 tests |
| `local_sqlite/migration.rs` | ❌ Zero tests | ✅ 6 tests |
| `bazel.rs` | ❌ Zero tests | ✅ 17 tests |
| `oci/token.rs` | ❌ Zero tests | ✅ 31 tests |
| `body_reader.rs` | ❌ Zero tests | ✅ 14 tests |
| `chunk_store.rs` | ❌ Zero tests | ✅ 18 tests |
| `oci_adapter key construction` | ❌ Zero tests | ✅ 28 tests |
| `helpers.rs` | ⚠️ Partial | ✅ 19 tests |
| `oci/manifest.rs` | ⚠️ Partial | ✅ 29 tests |
| `server_core/lib.rs ServerObjectStore` | ❌ Zero tests | ✅ 19 tests |
| `local_fs.rs` | ❌ Zero tests | ✅ 11 tests |
| `local_backend` | ❌ Zero tests | ✅ 11 tests |
| `protocol_adapters/lfs.rs` | ❌ Zero tests | ✅ 8 tests |
| `protocol_adapters/bazel.rs` | ❌ Zero tests | ✅ 9 tests |
| `provider_events/state.rs` | ❌ Zero tests | ✅ 7 tests |
| `hub_api/resolve.rs` | ❌ Zero tests | ✅ 8 tests |
| `config/secrets.rs` | ❌ Zero tests | ✅ 10 tests |

## Security Fixes Applied

| Issue | Status |
|-------|--------|
| `write_file_atomically` bypassed anchored_fs | ✅ Fixed — uses open_anchored_target + ensure_parent_path_matches_anchor |
| PassthroughProvider on non-loopback | ✅ Fixed — startup validation error |
| Security headers on all HTTP responses | ✅ Added — nosniff, X-Frame-Options, HSTS, Referrer-Policy |
| docker-compose default passwords | ✅ Warning added |
| `unimplemented!()` in auth mock | ✅ Fixed |
| xet_core `#![allow(clippy::all)]` | ✅ Replaced with specific allows |
| Upload session count inflation | ✅ Fixed — validates JSON + expiry |
| JWKS try_read() lock contention | ✅ Fixed — retry loop |
