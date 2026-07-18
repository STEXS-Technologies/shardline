# Loop Attempt 002 — Item 12: test probe statics

**Result:** PASS ✅

**What was done:** Moved 3 test-only `LazyLock`/`Mutex`/`AtomicUsize` statics from production code to `#[cfg(test)]` blocks in `crates/server/src/backend.rs`. Removed 4 test-only function exports from `lib.rs`.

**Verification:** clippy ✅, tests ✅

**Next:** Item 9 — clone audit in hot paths
