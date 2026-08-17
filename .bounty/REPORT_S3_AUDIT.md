# Shardline S3 Frontend — Deep Audit Report

Target: S3 frontend, branch `feat/s3-frontend` @ `9159eb6` (pushed 2026-08-14, all gates green)
Engagement: E-867247f1397bb02a → candidates_triaged (MCP report op blocked by harness binding; report written manually)
Method: bounty-hunter (money lane) + adversarial-audit (parsing/plumbing) + web2-finder (live loopback probing) + poc-author (measured deltas), fed by CVE-2026-42882 / CWE-444 / CWE-611 pattern research.

## Findings (all triaged accepted; PoC-verified with measured deltas)

| ID | Sev | Title | Location | PoC delta |
|----|-----|-------|----------|-----------|
| F-7 | High | CopyObject unbounded full-object read into RAM + per-request byte-ceiling bypass (only S3 write path with no cap) | object.rs:287 (read_object full Vec<u8>), :303 (from_bytes max_bytes=None) | CopyObject of 1,248,576 B object → **200** vs direct PUT same bytes → **413** (1 MiB cap) |
| F-8 | Med | Conditional-write check-then-act TOCTOU (precondition outside per-key lock) + DELETE not serialized (no per-key lock) | object.rs:229 vs :384; DELETE :612-654 | concurrent If-None-Match:* PUTs → **2×200 in 30/30** rounds; PUT→200 then GET→**404 in 15/20** (phantom delete) |
| F-9 | Med | Unbounded process-global per-key upload-lock map, never evicted — memory DoS | mod.rs:40-51 (S3_OBJECT_UPLOAD_LOCKS) | map grew **0→149** after 100 unique-key PUTs, monotonic |
| F-10 | Med | Global multipart session lock held across the entire body stream — cross-tenant lock-convoy DoS (sweep starved too) | multipart.rs:122, :157-167; adapter:233-258 | CreateMultipartUpload behind slow UploadPart → **284.7 ms** vs 0.69 ms (~410×) |
| F-11 | Low | GET/HEAD serve newer index-row ETag/metadata with older pinned-snapshot bytes — torn metadata (structural) | object.rs:464-481, :532-538 | not deterministically raced; ordering code-verified |

## Classes verified CLEAN (independent lanes + live probes)
- Auth bypass / cross-tenant R/W/D: no unauthenticated 200/204 anywhere; bucket binding exact; **CVE-2026-42882 pattern (raw-URI vs decoded-path auth split) structurally absent**.
- Path traversal: single choke points (ObjectKey::parse / S3ObjectKey::parse) reject `..`, leading `/`, control chars, >4096 B; double-encoding harmless.
- Request smuggling (CWE-444): hyper owns framing (TE+CL / dup CL → 400); aws-chunked decoder double-bounded, fails closed; no desync.
- XXE (CWE-611): hand-rolled non-expanding XML scanner; bounded input; output escaped.
- SQLi: fully parameterized; prefix via bound substr().
- Header/metadata injection: HeaderValue::to_str + re-validation; Md5Tee hashes decoded bytes; partial hash never persisted.

## Rejected / non-findings
- SigV4 signature-not-verified: documented design (access-key=token).
- x-amz-copy-source not percent-decoded: interop deviation, no escalation.
- Completed objects not counted against any quota (tenant total unbounded by config): design property to confirm.
- aws-chunked trailing-bytes drop, `+`-prefixed chunk hex: no impact without a desyncing intermediary.

## Recommended fixes
- F-7: stream the copy (pinned read stream) into a capped upload; enforce SHARDLINE_S3_MAX_PART_BYTES on CopyObject.
- F-8: re-check the precondition inside the per-key lock; DELETE takes the same lock.
- F-9: evict lock-map entries when the last reference drops (weak-key/refcount) or LRU-bounded.
- F-10: hold the global session lock only for metadata mutations; per-session lock for part-file I/O + body-duration cap.
- F-11: carry etag+metadata in the same snapshot as the bytes (single consistent read).

PoC tests: crates/shardline-server/src/app/protocol_routes/s3/poc_audit.rs (6 tests) — to be converted into regression tests asserting the FIXED behavior.
