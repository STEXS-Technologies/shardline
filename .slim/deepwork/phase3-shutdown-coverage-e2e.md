# Phase 3: Shutdown Timeout + Coverage + E2E Tests

## Goal
1. Add graceful shutdown timeout to `serve_with_listener`
2. Add unit coverage for uncovered paths in s3.rs and redis.rs
3. Add more E2E/integration tests following the pattern that found real bugs

## Context

### Graceful Shutdown
- Current: `serve_with_listener` uses `axum::serve()` with `tokio::signal::ctrl_c()` but no drain timeout. If a connection holds open, server hangs forever.
- Plan: Add `shutdown_timeout: Option<Duration>` to `ServerConfig` (default None). Use `oneshot` channel + `tokio::select!` to force-close after timeout.
- File: `crates/server/src/app.rs` (line 273-287), `crates/server/src/config/types.rs` (ServerConfig struct)

### s3.rs coverage (crates/storage/src/s3.rs)
- Current: 1500+ lines of tests, but some functions untested:
  - `put_overwrite` — direct file overwrite path
  - `streaming_large_copy` — cross-bucket copy
  - `stream_file_to_location` — internal file streaming
  - `delete_location_if_present` — conditional delete
  - `existing_object_outcome` / `existing_object_outcome_from_file` / `existing_copy_outcome` — conflict resolution helpers
  - `create_resumable_upload` / `upload_resumable_part` / `complete_resumable_upload` / `abort_resumable_upload` — S3 multipart uploads
- Can test most of these with `object_store::memory::InMemory`

### redis.rs coverage (crates/cache/src/redis.rs)
- Current: 585 lines, 30+ unit tests + redis_integration tests
- Untested:
  - `delete()` method (tested only via Docker)
  - `ready()` error path
  - `get_connection()` error handling
- Redis integration tests exist in `crates/cache/tests/redis_integration.rs` via Docker

### E2E/integration tests
- Current: 6 test files covering PG, S3, combined, config edge cases
- All P0/P1/P2/P3 items in todo.md are ✅
- Gaps to explore:
  - Redis-backed HTTP server E2E tests (start server with Redis cache, verify reconstruction caching via HTTP)
  - More cross-protocol interaction tests
  - Hub + Xet + LFS triple coexistence tests
  - Graceful shutdown behavior tests
  - Stress/edge tests that might uncover bugs

## Existing test file sizes
```
crates/server/tests/postgres_e2e_http.rs      ~4385 lines (96 tests)
crates/server/tests/s3_e2e_http.rs             ~58 tests
crates/server/tests/postgres_s3_e2e_http.rs    ~48 tests
crates/server/tests/postgres_integration.rs    ~93 tests
crates/server/tests/s3_integration.rs          ~68 tests
crates/server/tests/server_config_e2e.rs       ~3 tests
crates/cache/tests/redis_integration.rs        591 lines
```

## Files & ownership
- `crates/server/src/config/types.rs` — ServerConfig struct + builder
- `crates/server/src/app.rs` — serve_with_listener + graceful shutdown
- `crates/storage/src/s3.rs` — S3 object store tests at line 1501
- `crates/cache/src/redis.rs` — Redis cache tests at line 151
- `crates/server/tests/` — E2E test files
