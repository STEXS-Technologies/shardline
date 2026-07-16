# Deep Audit: Error Recovery, Security & Edge-of-Spec Blind Spots

## Priority Key
- **[P0] Critical** — exploitable security or data-loss risk  
- **[P1] High** — security boundary gap or untested error path that can lead to incorrect behavior  
- **[P2] Medium** — missing coverage for well-defined error/edge paths  
- **[P3] Low** — hardening / defense-in-depth  

---

## 1. Error Recovery Patterns

### 1.1 `crates/server/src/error.rs` — From impls & status codes

**Status**: ✅ Excellent coverage.  
- Every `From<A> for B` impl has a dedicated test.  
- Every `ServerError` variant's `status_code()` mapping has a test.  
- `IntoResponse` impls are tested: `MissingAuthorization`, `UnauthorizedChallenge`, `NotFound`, `OciError` structure.  

**Remaining gaps** (minor):

| Gap | Priority | Notes |
|-----|----------|-------|
| `server_error_to_oci()` catch-all → `Io(…)` conversion | **[P3]** | Tested only for `RequestBodyTooLarge`. The catch-all arm (lines 604-651) converts ~50+ ServerError variants to `OciAdapterError::Io`. Only 1 variant tested. |
| `HeaderValue::from_str` silent failure on bad challenge header | **[P3]** | Line 441: if custom challenge header is invalid UTF-8, it's silently dropped. All test values are valid ASCII. |

### 1.2 `crates/server/src/backend.rs` — object_store() / partial failures

**Status**: Adequate core coverage.

| Concern | Priority | Verdict |
|---------|----------|---------|
| `object_store_from_config` failure | **[P2]** | Tested indirectly via `from_config` in integration tests. No unit test for malformed S3 config. |
| `record_set()` / `record_scan()` partial failure | **[P1]** | **NOT TESTED.** If some records are written and some fail, there is no rollback (no transaction scope visible). The backend delegates to `LocalBackend`/`PostgresBackend`. Should test partial-write atomicity. |
| OCI multipart mid-way S3 failure `(create/upload/complete)` | **[P2]** | Tested at `oci_adapter` level (MockS3Backend). Not tested at the `OciBackend` impl level for the `ServerBackend`. The `server_error_to_oci` catch-all maps unexpected errors to `OciAdapterError::Io`, losing semantic detail. |

### 1.3 `crates/oci_adapter/src/lib.rs` — ensure_s3_upload_started / purge_expired / read_upload_session

**Status**: ✅ Very thorough — each path is tested.

| Path | Status | Test |
|------|--------|------|
| `ensure_s3_upload_started` → None | ✅ | `s3_ensure_started_upload_id_none_errors` (line 2001) |
| `ensure_s3_upload_started` → persist failure after None | ⚠️ **[P2]** | Line 1116: `let _result = persist_upload_session(...)` after clearing s3_multipart is silently ignored. Not tested for failure. |
| `purge_expired` → S3 abort ignored (`_result`) | ✅ | `purge_expired_s3_multipart_abort` (line 2438) |
| `purge_expired` → metadata read error | ✅ | `purge_expired_session_read_error_deletes` (line 2415) |
| `purge_expired` → corrupt JSON | ✅ | `purge_expired_corrupt_json_metadata_cleaned` (line 1453) |
| `purge_expired` → missing local body | ✅ | `purge_expired_missing_body_file_cleaned` (line 1495) |
| `purge_expired` → orphaned .bin/.tail files | ✅ | `purge_expired_orphaned_bin_files_cleaned` (line 1418) |
| `read_upload_session` → metadata missing (NotFound) | ✅ | `read_upload_session_returns_not_found_for_nonexistent` (line 377) |
| `read_upload_session` → expired | ✅ | `read_upload_session_returns_not_found_when_expired` (line 455) |
| `read_upload_session` → body file missing | ✅ | `read_upload_session_missing_local_body_returns_not_found` (line 1626) |
| `read_upload_session` → IO error on metadata | ✅ | `read_upload_session_io_error_on_inaccessible_metadata` (line 2108) |
| Concurrent append stress (racy tail file) | ⚠️ **[P2]** | `s3_multipart_concurrent_append_stress` (line 1079) — test exists but comment says "without explicit session-level locking, concurrent writers race on the file-based state". |

### 1.4 `blob_upload.rs` — mount flow / hash mismatch

**Status**: Good coverage with one gap.

| Path | Status | Test |
|------|--------|------|
| Mount: `copy_object_if_absent` returns NotFound → falls through | ✅ | `blob_upload_mount_nonexistent_blob_returns_error` (line 747) — documents local backend returns `Io` error, not `NotFound` |
| Mount: `copy_object_if_absent` returns different error | ✅ | The Err(error) => return Err(error) path is covered by the mount-non-existent test (returns 500) |
| Direct upload hash mismatch | ✅ | `blob_upload_session_put_finalize_hash_mismatch` (line 686) |
| **Direct upload: body hash doesn't match during stream (S3)** | **⚠️ [P2]** | `put_sha256_addressed_object_stream_if_absent` (backend.rs line 655) has an abort-on-mismatch path (line 680-682). **Only tested for local backend; not tested for S3 flow.** |

---

## 2. Security Boundaries

### 2.1 Auth Scope Enforcement — what scope for which endpoint?

| Endpoint | Required Scope | Test Coverage |
|----------|---------------|---------------|
| OCI: GET manifest / tags | `Read` | ✅ Basic auth tests exist |
| OCI: POST/PUT/DELETE blob | `Write` | ✅ `blob_upload_direct` tests |
| OCI: PATCH blob upload | `Write` | ✅ |
| OCI: DELETE blob upload | `Write` | ✅ |
| LFS: batch (download) | `Read` | ✅ `lfs_batch_with_valid_token` |
| LFS: batch (upload) | `Write` | ✅ `lfs_batch_with_insufficient_scope` |
| LFS: GET object | `Read` | ✅ |
| LFS: PUT object | `Write` | ✅ |
| Bazel: GET/HEAD | `Read` | ✅ `bazel_put_requires_auth` |
| Bazel: PUT | `Write` | ✅ `bazel_put_with_valid_token` |
| Provider token | Varies | ✅ |
| Reconstruction routes | `Read` | ✅ |
| Operational routes | Mixed | ✅ |

### 2.2 Namespace Isolation — can token for repo A access repo B?

| Protocol | Check exists? | Tested wrong-repo? | Verdict |
|----------|---------------|-------------------|---------|
| **OCI** | `validate_oci_repository_scope()` | ✅ `test_auth_wrong_repo_scope_returns_403` (pg_e2e) + `registry_token_rejects_different_repository_scope` | **Secure** |
| **LFS** | Only via key namespace (scope → `lfs_object_key`) | ❌ **NO TEST** | **[P1]** No explicit endpoint-level check. A token for repo A accessing `/v1/lfs/objects/{oid}` under repo B's URL will get a 404 (not 403) because the key is different — this **leaks existence** of cross-scope objects. No test verifies this boundary. |
| **Bazel** | Only via key namespace (scope → `bazel_cache_object_key`) | ❌ **NO TEST** | **[P1]** Same issue as LFS. |
| **Xet** | Only via key namespace | ❌ **NO TEST** | **[P1]** Same issue. |

### 2.3 OCI Registry Token Scope Exchange

| Attack | Status | Test |
|--------|--------|------|
| Pull token → push request | ✅ Blocked | `registry_token_insufficient_scope_errors` (returns 403) |
| Push token → pull on different repo | ✅ Blocked | `registry_token_rejects_different_repository_scope` (returns 404) |
| Token with no scope → exchange | ✅ Allowed (uses bootstrap scope) | `exchange_none_uses_actual_scope` |
| Push token → pull on same repo | ✅ Allowed | `scope_allows_oci_exchange` — Write allows Read |

### 2.4 Token parsing edge cases tested

| Input | Status |
|-------|--------|
| Missing `Authorization` header | ✅ |
| Non-Bearer scheme (e.g., `Basic` with password token) | ✅ |
| Bearer with empty token after prefix | ✅ |
| Bearer with whitespace | ✅ |
| Bearer exceeding 8KB | ✅ |
| Basic base64 with invalid encoding | ✅ |
| Basic with no colon separator | ✅ |
| Basic with empty password | ✅ |
| Bearer expired token → challenge | ✅ |

---

## 3. Edge-of-Spec Inputs

| Input | Endpoint | Status | Verdict |
|-------|----------|--------|---------|
| **1MB Authorization header** | All | ✅ `MAX_BEARER_TOKEN_BYTES = 8192`, tested | **[P3]** — but the limit is 8KB, which could still be a DoS vector for HS256 token verification. No body-size limit on the token endpoint itself. |
| **100KB URL path** | All | ❌ **NO TEST** | **[P2]** Axum may truncate or error. Need to verify behavior for extremely long paths on all protocol routes. |
| **Repeated `Authorization` header** | All | ❌ **NO TEST** | **[P1]** HTTP semantics: multiple headers with the same name are equivalent to a single header with comma-separated values. Axum `HeaderMap::get` returns the first. If `parse_bearer_token` sees `Bearer A, Bearer B`, it would fail (contains whitespace). But an attacker could send two headers where the first passes validation and the second is the real token. Need explicit test that only one `Authorization` is accepted. |
| **Multiple `Content-Type` headers** | OCI blob POST/PUT | ❌ **NO TEST** | **[P2]** OCI spec expects specific content types. Multiple Content-Type headers could confuse middleware. |
| **Negative `size` in LFS batch** | LFS batch POST | ✅ **Serde-safe** — `size: u64` | Serde rejects negative values and floats at deserialization time. Not separately tested though. |
| **Floating point `size` (3.14)** | LFS batch POST | ✅ **Serde-safe** — `u64` | Serde rejects non-integer. |
| **Very large `size` (99999999…)** | LFS batch POST | ⚠️ **[P2]** | `u64` deserializes successfully. The value flows to `checked_add` → `Overflow`. Path tested: not explicitly. |
| **Null byte in `path`/`oid`** | All | ⚠️ **[P2]** | OCI repository names validate against null bytes. LFS `oid` validation checks hex chars only (rejects null). Bazel hash validation rejects null. But no **explicit fuzz test** for null bytes in any protocol path. |
| **Unicode normalization** | All | ❌ **NO TEST** | **[P2]** Repository names, tags, and OIDs are hex-constrained or regex-constrained (ASCII only). But the OCI tag `parse_reference` and `validate_oci_tag` may accept Unicode. No normalization is applied. Two different Unicode sequences that normalize to the same string could bypass dedup or scope checks. |

---

## 4. Concurrent State Corruption

| Scenario | Test Exists? | Verdict |
|----------|-------------|---------|
| **Create repo same name concurrently** → 1 success + 1 Conflict | ❌ **NO CONCURRENT TEST** | **[P2]** `test_hub_create_same_repo_twice_returns_409` tests **sequential** duplicate. Not concurrent. Hub uses Postgres unique constraint, so it's likely safe. But no test proves it. |
| **LFS upload 2 concurrent PUTs → idempotent** | ❌ **NO CONCURRENT TEST** | **[P2]** `xorb_upload_is_idempotent` is sequential. The `put_if_absent` semantics are tested synchronously. No concurrent upload test. |
| **Delete + recreate same repo path → in-flight Hub requests** | ❌ **NO TEST** | **[P1]** If a repo is deleted and immediately recreated, in-flight Hub requests (e.g., upload references) might target the new repo's storage but reference metadata from the old one. No test for this race. |
| **OCI session upload + concurrent session list** | ⚠️ Partial | **[P2]** `s3_multipart_concurrent_append_stress` tests concurrent appends but acknowledges the race. |
| **GC running concurrent with upload** | ✅ | `gc_concurrent_upload_interleaving` (gc_tests.rs:1110) |
| **Index atomicity** | ✅ | `local_record_store_commit_file_version_metadata_is_atomic` tests SQLite atomicity |

---

## 5. Prioritized Gap List

### [P0] — None found
The codebase has no immediately exploitable vulnerabilities in the reviewed surface.

### [P1] — High Priority

1. **LFS/Bazel/Xet: no wrong-repo-scope test** — `test_auth_wrong_repo_scope_returns_403` exists only for OCI. LFS, Bazel, and Xet rely on key namespace isolation but never explicitly verify the repository name in the request URL matches the token's scope. A token for repo A attempting operations on repo B gets a 404 (not 403), leaking cross-scope existence. **Add tests**: `test_lfs_wrong_repo_scope_returns_404`, `test_bazel_wrong_repo_scope_returns_404`.
2. **Repeated `Authorization` header** — No test verifies that sending two `Authorization` headers (first valid, second attacker-controlled) doesn't bypass validation. Axum's `HeaderMap` behavior needs explicit verification.

### [P2] — Medium Priority

3. **Concurrent repo creation test** — `test_hub_create_same_repo_twice_returns_409` is sequential. Add a test that fires 5 concurrent `POST /api/repos/create` requests for the same name and asserts exactly 1 returns 201 and the rest return 409.
4. **LFS concurrent upload idempotency** — Test 2 concurrent PUTs to the same LFS OID. Assert only one succeeds (or both succeed with idempotent semantics).
5. **Delete + recreate repo during Hub operations** — Design and test: start a Hub upload, delete the repo, recreate it, verify the upload can't affect the new repo.
6. **OCI concurrent session state race** — The `s3_multipart_concurrent_append_stress` test exposes a known race on the tail file. Add session-level locking or document the acceptable staleness.
7. **`record_set()`/`record_scan()` partial failure** — If a batch write contains 10 records and the 5th fails, what state is the backend in? No test verifies atomicity/rollback behavior.
8. **Very long URL paths** — No test sends a 100KB path segment to any endpoint.
9. **Unicode normalization in tags** — `validate_oci_tag` accepts Unicode characters. No canonicalization is applied. Two visually identical but byte-different tag strings (`café` NFC vs NFD) could coexist, creating confusion.
10. **Null bytes in protocol identifiers** — While OCI repository names and OIDs validate, there's no fuzz test that probes null-byte injection in any protocol path.
11. **`ensure_s3_upload_started` persist failure after None** — Line 1116: if `persist_upload_session` fails after clearing `s3_multipart` (line 1113-1116), the error is silently ignored. Add a test where `persist_upload_session` fails (e.g., disk full) and verify the error propagates correctly.

### [P3] — Low Priority

12. **`server_error_to_oci` catch-all coverage** — Only 1 of ~50 catch-all variants tested (`RequestBodyTooLarge`). Add one more test for a catch-all variant to document the behavior.
13. **`HeaderValue::from_str` silent failure** — Not tested for invalid custom challenge header values.
14. **8KB bearer token limit still allows medium DoS** — Token verification involves HMAC-SHA256, which is fast, but 8KB of Base64 is ~6KB of input. A flood of 8KB auth headers could be a CPU DoS vector. Consider a tighter limit or rate-limiting.
15. **Very large LFS `size` field** — Deserializes as `u64` but unchecked until `checked_add`. Add a test with `size: 18446744073709551615` (max u64) to ensure the overflow path is exercised.

---

## 6. Summary of OCI Adapter Test Coverage (for reference)

| Function | Branches | Tested? |
|----------|----------|---------|
| `read_upload_session` | metadata missing | ✅ |
| `read_upload_session` | metadata expired | ✅ |
| `read_upload_session` | body file missing | ✅ |
| `read_upload_session` | IO error on metadata | ✅ |
| `ensure_s3_upload_started` | s3_multipart already set | ✅ |
| `ensure_s3_upload_started` | create returns None | ✅ |
| `ensure_s3_upload_started` | persist failure after None | ⚠️ Not explicitly |
| `purge_expired_upload_sessions` | read_dir NotFound | ✅ |
| `purge_expired_upload_sessions` | read_dir other error | ✅ |
| `purge_expired_upload_sessions` | non-UTF8 stem (.bin) | ✅ |
| `purge_expired_upload_sessions` | invalid stem (.bin) | ✅ |
| `purge_expired_upload_sessions` | orphan .bin (no metadata) | ✅ |
| `purge_expired_upload_sessions` | corrupt .json → delete | ✅ |
| `purge_expired_upload_sessions` | unreadable .json → delete | ✅ |
| `purge_expired_upload_sessions` | expired + S3 abort | ✅ |
| `purge_expired_upload_sessions` | missing body → delete | ✅ |
| `delete_upload_session` | idempotent (double delete) | ✅ |
| `delete_upload_session` | IO error propagation | ✅ |
| `append_s3_multipart_upload_bytes` | non-S3 session | ✅ |
| `append_s3_multipart_upload_bytes` | empty bytes | ✅ |
| `append_s3_multipart_upload_bytes` | triggers part upload | ✅ |
| `append_s3_multipart_upload_bytes` | concurrent stress | ⚠️ (racy) |
| `finalize_s3_multipart_upload_session` | non-S3 session | ✅ |
| `finalize_s3_multipart_upload_session` | hash mismatch (empty) | ✅ |
| `finalize_s3_multipart_upload_session` | hash mismatch (with data) | ✅ |
| `finalize_s3_multipart_upload_session` | empty parts → abort+put | ✅ |
| `finalize_s3_multipart_upload_session` | part upload error → abort | ✅ |
| `finalize_s3_multipart_upload_session` | complete error → abort | ✅ |
| `finalize_s3_multipart_upload_session` | canonical key == object key | ✅ |
