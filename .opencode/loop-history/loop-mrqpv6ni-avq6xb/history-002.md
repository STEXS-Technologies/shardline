# Testing Gaps + Secrecy Audit — Complete

**Result:** PASS ✅

## Remaining integration test gaps
| Crate | Before | After | Tests added |
|-------|--------|-------|-------------|
| metrics (2.6K) | 0 | **23** | Metric recording, encoding, concurrent access |
| protocol (2.3K) | 0 | **56** | Hash, ranges, tokens, time, text, security |

All **24/25** crates now have integration tests (only `bench` remains — benchmark harness only).

## Secrecy audit

| # | Finding | Severity | Fixed |
|---|---------|----------|-------|
| 1 | `read_signing_key_bytes` returns plain `Vec<u8>` — dropped without zeroing | **HIGH** | ✅ Return `SecretBytes` instead, zeroized on drop |
| 2 | `app.rs` intermediate `api_key.to_vec()` non-zeroized copy | MED | ✅ Builder functions accept `impl Into<SecretBytes>` |
| 3 | Hardcoded test signing keys (~30 files) | MED | Deferred (test-only, low risk) |
| 4-6 | Minor doc/API issues | LOW | Deferred |
| 7-8 | Positive: buffer zeroization ✅, tracing skips secrets ✅ | — | — |

## Final test counts
| Metric | Value |
|--------|-------|
| **Total tests** | **7,753** |
| Integration test coverage | **24/25 crates** |
| CI pipeline | ✅ All 5 stages green |
