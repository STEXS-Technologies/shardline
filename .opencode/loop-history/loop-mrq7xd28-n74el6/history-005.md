# Loop Attempt 005 — Item 11: benchmarks

**Result:** PASS ✅

**What was done:** Added criterion benchmarks for 3 hot-path areas:
- `xet_adapter/benches/xorb_validate.rs` — validate+decode xorb (1/4/16/64 chunks)
- `server_core/benches/auth_verify.rs` — HMAC verify + mint+verify roundtrip + varying sizes
- `cache/benches/reconstruction_cache.rs` — cache hit/miss/dedup/put-get

**Verification:** clippy ✅, check ✅, bench ✅ (all 18 measurements produced valid ns/µs results)

**Next:** Item 13 — missing docs on public types
