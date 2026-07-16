# P0 Bug Fixes — Deep Audit Round 2

## Goal
Fix and test three P0 gaps identified in the deep audit.

## P0-1: Transfer limiter exhaustion → add timeout

**Bug**: `TransferLimiter::acquire_bytes()` calls `Semaphore::acquire_many_owned()` which blocks forever when all permits are taken. Under load, this hangs the caller indefinitely.

**Fix**: Add a configurable timeout. When timeout elapses, `acquire_bytes` returns `ServerError::TransferLimiterClosed` (already maps to 503).

**Implementation**:
- Add `acquire_timeout: Duration` field to `TransferLimiter` (or pass it as a parameter)
- Use `tokio::time::timeout(dur, semaphore.acquire_many_owned(permits)).await`
- On timeout → `ServerError::TransferLimiterClosed`
- Default timeout: pick a reasonable value (e.g., 60 seconds) or make it configurable via `ServerConfig`

**File**: `crates/server/src/transfer_limiter.rs`

## P0-2: Redis connection failure mid-operation

**Bug**: After the stale-cache fix, `get_connection()` always creates a fresh handle from `redis::Client`. But `redis::Client` may internally hold broken connections at the pool level. No test covers `ready()` failing after a working connection drops.

**Fix**: Add a `reconnect_count` metric or retry in `get_connection()`. At minimum, test the error path.

## P0-3: Server shutdown during active upload

**Bug**: Only idle shutdown tested. If a transfer is in-flight when shutdown signal arrives, the server force-closes connections, potentially leaving partial state.

**Test**: Start server, start a long upload, send shutdown, verify server exits within timeout and the upload is interrupted gracefully.
