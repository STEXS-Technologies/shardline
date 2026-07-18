# Loop Attempt 001 — Item 7: crate aliasing

**Result:** PASS ✅

**What was done:** Replaced 3 blanket `pub(crate) use ... as ...` crate aliases in `crates/server/src/lib.rs` with module shims that re-export only the specific types actually used:
- `shardline_oci_adapter`: 26 types/functions (down from entire crate)
- `shardline_xet_adapter`: 26 types/functions + 6 test-only items
- `shardline_gc`: 4 public types + `run_gc_with_stores` + 6 test-only items

**Verification:** clippy ✅, check ✅, tests ✅

**Next:** Item 12 — test probe statics in public API
