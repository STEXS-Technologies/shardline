# God File Split — Attempt 001: hub_api/routes.rs

**Result:** PASS ✅

**Split:** `crates/hub_api/src/routes.rs` (5,756 lines) → 10 files under `routes/`:
- `mod.rs` (543 lines) — HubState, router(), shared helpers
- `health.rs`, `repos.rs` (461), `webhooks.rs` (615), `tokens.rs`, `commits.rs`, `tree.rs`, `lfs.rs`, `resolve.rs`, `tests.rs` (3,598)

**CI:** fmt ✅ clippy ✅ 564 tests ✅

**Next:** `storage/src/s3.rs` (3,716 lines)
