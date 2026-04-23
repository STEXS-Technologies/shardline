# Maintainability Hotspots

This tracks the current maintainability pressure points in the repository and the intended cleanup order.

## Largest Files

These are the most immediate readability and change-risk hotspots.

- `crates/server/src/app.rs` - 6260 lines
- `crates/index/src/local_sqlite.rs` - 4110 lines
- `crates/cli/src/bench.rs` - 2571 lines
- `crates/server/src/config.rs` - 2526 lines
- `crates/server/src/fsck.rs` - 2384 lines
- `crates/server/src/provider_events.rs` - 2294 lines
- `crates/index/src/local.rs` - 2253 lines
- `crates/cli/src/command.rs` - 2215 lines
- `crates/server/src/gc.rs` - 2038 lines
- `crates/index/src/postgres.rs` - 1796 lines
- `crates/server/src/rebuild.rs` - 1430 lines
- `crates/server/src/local_backend.rs` - 1350 lines
- `crates/storage/src/local.rs` - 1306 lines
- `crates/server/src/provider.rs` - 1298 lines
- `crates/server/src/lifecycle_repair.rs` - 1064 lines

## Cross-Crate Policy Duplication

These are the places where the same policy still exists in multiple crates or modules.

### Provider conversion and provider-name policy

Status:
- [x] canonical `RepositoryProvider -> string` helper moved onto the protocol enum
- [x] canonical `ProviderKind -> RepositoryProvider` helper moved onto the VCS enum
- [x] primary server/index/vcs call sites switched to the canonical helpers
- [x] remove remaining local wrappers where they no longer add value
- [ ] check index/server call sites for any provider-specific formatting that still differs

- `crates/vcs/src/token_issuer.rs`
- `crates/server/src/provider_events.rs`
- `crates/server/src/app.rs`
- `crates/index/src/local.rs`
- `crates/index/src/postgres.rs`
- `crates/index/src/local_sqlite.rs`
- `crates/server/src/repository_scope_path.rs`

Risk:
- string/name drift between path layout, database values, webhook output, and token scope conversion

Planned cleanup:
- centralize `ProviderKind -> RepositoryProvider` on the VCS/provider type
- centralize `RepositoryProvider <-> string` on the protocol/provider type
- convert remaining local helpers into thin callers or remove them

### Index record-key construction

Status:
- [x] shared record-key builder extracted into an index-internal helper module
- [x] Postgres and SQLite adapters switched to the shared builder
- [x] remove any backend-local wrappers that now only forward to the shared helper
- [ ] run broader backend-parity tests if record locator behavior changes again

- `crates/index/src/postgres.rs`
- `crates/index/src/local_sqlite.rs`

Risk:
- key format drift between metadata backends
- harder backend parity verification

Planned cleanup:
- extract shared record-key and repository-scope-key builder helpers into one index module

### Symlink-safe directory component walking

Status:
- [x] shared directory-component validator extracted into `shardline-storage`
- [x] server, CLI, storage, and index call sites switched to the shared validator with local error mapping
- [ ] fold `crates/index/src/local_sqlite.rs` and `crates/index/src/local.rs` onto fewer wrapper helpers if their invalid-path policies stay aligned
- [x] keep the higher-risk anchored write helper extraction separate from this traversal cleanup

- `crates/server/src/local_path.rs`
- `crates/cli/src/local_path.rs`
- `crates/storage/src/local.rs`
- `crates/index/src/local.rs`
- `crates/index/src/local_sqlite.rs`

Risk:
- filesystem safety policy drift
- TOCTOU hardening changes needing repeated edits

Planned cleanup:
- extract one shared traversal helper per error surface or one common core plus mappers

### Anchored local write helpers

Status:
- [x] shared anchored write helper module extracted into `crates/storage/src/anchored_fs.rs`
- [x] storage, server, index, and CLI Unix call sites switched to the shared helper module
- [x] new helper usage cleaned to use semantic imports instead of path-qualified calls in bodies
- [ ] consider whether the remaining thin per-crate wrappers should collapse further, or stay as local policy/error-message seams

- `crates/storage/src/anchored_fs.rs`
- `crates/storage/src/local_fs.rs`
- `crates/server/src/local_fs.rs`
- `crates/index/src/local_fs.rs`
- `crates/cli/src/local_output.rs`

Risk:
- filesystem race hardening changes still need careful parity checks
- local error wording/policy differences can tempt accidental behavior drift

Planned cleanup:
- keep the shared anchored core in `shardline-storage`
- only retain crate-local wrappers where mode defaults, hook points, or error text actually differ

### Timestamp helper policy

Status:
- [x] canonical lossy wall-clock helper extracted into `crates/protocol/src/time.rs`
- [x] fallible server-control-plane callers switched to `unix_now_seconds_checked()`
- [x] lossy server callers switched to the canonical protocol helper
- [x] CLI lossy callers switched to the canonical protocol helper
- [x] index SQLite lossy callers switched to the canonical protocol helper
- [ ] decide whether any remaining checked/fallible time helpers should also move lower

- `crates/server/src/app.rs`
- `crates/server/src/provider_events.rs`
- `crates/server/src/gc.rs`
- `crates/server/src/lifecycle_repair.rs`
- `crates/server/src/fsck.rs`
- `crates/cli/src/hold.rs`
- `crates/cli/src/admin.rs`
- `crates/index/src/local_sqlite.rs`

Risk:
- mixed fail-open vs fail-closed semantics
- hard to reason about time-based cleanup and lifecycle behavior

Planned cleanup:
- choose explicit policy for server/control-plane code
- keep infallible helpers only where fallback-to-zero is actually intended, and keep them single-sourced

### Environment bool parsing

Status:
- [x] shared bool parser extracted into `crates/protocol/src/text.rs`
- [x] server config env parsing switched to the shared parser
- [x] CLI storage-migration bool parsing switched to the shared parser
- [x] accepted-value policy now includes `on` and `off`
- [ ] check for any remaining ad hoc bool parsing outside env/config flows

- `crates/server/src/config.rs`
- `crates/cli/src/storage_migration.rs`

Risk:
- inconsistent accepted values across binaries

Planned cleanup:
- one shared parser and one documented accepted-value policy

## Large Behavioral Modules That Need Internal Splitting

These modules are not just long; they carry multiple responsibilities.

### `crates/server/src/app.rs`

Current responsibilities:
- HTTP routing
- request parsing
- auth-adjacent route behavior
- provider token/webhook surfaces
- response shaping
- test fixture support

Preferred split:
- route handlers by domain
- provider endpoints
- reconstruction endpoints
- shared request/response helpers

### `crates/server/src/provider_events.rs`

Current responsibilities:
- webhook delivery claim handling
- repository deletion and rename lifecycle logic
- provider state updates
- duplicate delivery behavior
- test-only fake index adapter machinery

Preferred split:
- delivery bookkeeping
- repository deletion flow
- repository rename flow
- provider state mutation helpers
- test support moved out of the main module where practical

### `crates/server/src/fsck.rs`

Current responsibilities:
- record scanning
- object validation
- reconstruction validation
- provider metadata validation
- report shaping
- extensive test fixture helpers

Preferred split:
- record checks
- object checks
- provider lifecycle checks
- report/detail types

### `crates/server/src/gc.rs`

Current responsibilities:
- reachability derivation
- quarantine lifecycle
- deletion flow
- diagnostics
- legacy manifest compatibility

Preferred split:
- reachability graph
- quarantine policy
- sweep engine
- diagnostics/reporting

## Lower-Risk Cleanup Order

1. Provider conversion and provider-name centralization
2. Index record-key extraction
3. Timestamp helper consolidation
4. Bool env parsing consolidation
5. Symlink-safe path traversal consolidation
6. Internal module splitting for `provider_events.rs`
7. Internal module splitting for `fsck.rs` and `gc.rs`
8. Filesystem anchored-write helper consolidation last
9. Decide whether the cross-crate test invariant helpers deserve a small shared test-support crate

## Notes

- Recent duplicate cleanup already improved the server crate materially:
  - content hash validation
  - provider directory naming in touched server paths
  - `u64` overflow helpers
  - xorb visit error mapping
  - repeated server test fixtures
- Cross-crate integration tests now also share one invariant-error helper via `crates/test_support`.
- The main remaining maintainability drag is now oversized files plus a few cross-crate policy forks.
