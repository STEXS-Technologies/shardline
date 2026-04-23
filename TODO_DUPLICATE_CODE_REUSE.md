# Duplicate Code Reuse TODO

This tracks duplicate helper-shaped code that should be removed or consolidated.

## Remove Instantly

These already have a reachable reusable function and should only need import/call-site cleanup.

- [x] Replace duplicate server hash validators with `validate_content_hash` from `crates/server/src/validation.rs`.
  - `crates/server/src/chunk_store.rs`: `validate_chunk_hash`
  - `crates/server/src/xorb_store.rs`: `validate_hash_hex`
  - `crates/server/src/shard_store.rs`: `validate_shard_hash`

- [x] Replace fsck's duplicate file content-hash builder with `local_backend::content_hash`.
  - Existing helper: `crates/server/src/local_backend.rs`: `content_hash`
  - Duplicate: `crates/server/src/fsck.rs`: `content_hash`

- [x] Replace server-side repository provider string mappers with `provider_directory`.
  - Existing helper: `crates/server/src/repository_scope_path.rs`: `provider_directory`
  - Duplicates:
    - `crates/server/src/provider_events.rs`: `repository_provider_name`
    - `crates/server/src/fsck.rs`: `repository_provider_name`

## Extract First

These are real duplicates, but need a neutral shared location or a small API decision before removal.

- [x] Move checked arithmetic helpers out of `local_backend` into a neutral server helper module.
  - Existing shape: `crates/server/src/local_backend.rs`: `checked_add`, `checked_increment`
  - Duplicates:
    - `crates/server/src/fsck.rs`: `checked_add`, `checked_increment`
    - `crates/server/src/gc.rs`: `checked_add`, `checked_increment`
    - `crates/server/src/backup.rs`: `checked_add`, `checked_increment`
    - `crates/server/src/rebuild.rs`: `checked_increment`
    - `crates/server/src/lifecycle_repair.rs`: `checked_increment`
    - `crates/server/src/storage_migration.rs`: `checked_add`
  - Note: `crates/server/src/shard_store.rs` has `usize` variants; either keep local or extract typed helpers.

- [x] Extract `map_xorb_visit_error` into one shared server helper.
  - `crates/server/src/xorb_store.rs`: `map_xorb_visit_error`
  - `crates/server/src/object_store.rs`: `map_xorb_visit_error`
  - `crates/server/src/fsck.rs`: `map_xorb_visit_error`

- [x] Extract provider conversion/name helpers into a shared home.
  - `ProviderKind -> RepositoryProvider` duplicates:
    - `crates/vcs/src/token_issuer.rs`: `repository_provider`
    - `crates/server/src/provider_events.rs`: `repository_provider`
    - `crates/server/src/app.rs`: inline `match` in `provider_webhook_response`
  - Provider string names are also duplicated across server/index/cache/test code.
  - Prefer a protocol/vcs-level helper if crate boundaries allow it.

- [x] Extract index record-key and repository-scope-key construction.
  - Suggested module: `crates/index/src/record_key.rs`
  - Postgres copy: `crates/index/src/postgres.rs`: `record_key`, `repository_scope_key`, `repository_record_scope_key`, `append_repository_scope_key`, `push_length_prefixed`
  - SQLite copy: `crates/index/src/local_sqlite.rs`: `local_record_key`, `repository_scope_key`, `repository_record_scope_key`, `append_repository_scope_key`, `push_length_prefixed`

- [x] Extract symlink-safe directory-component walking with error mapping.
  - `crates/server/src/local_path.rs`: `ensure_directory_path_components_are_not_symlinked`
  - `crates/cli/src/local_path.rs`: `ensure_directory_path_components_are_not_symlinked`
  - `crates/storage/src/local.rs`: `ensure_directory_path_components_are_not_symlinked`
  - `crates/index/src/local.rs`: `ensure_directory_path_components_are_not_symlinked`

- [x] Consolidate anchored local write helpers.
  - `crates/server/src/local_fs.rs`
  - `crates/index/src/local_fs.rs`
  - `crates/storage/src/local_fs.rs`
  - `crates/cli/src/local_output.rs`
  - Risk note: behavior and error surfaces differ slightly, so treat this as a careful extraction, not a blind move.

- [x] Extract or consolidate current Unix timestamp helpers after choosing one error policy.
  - Fallible server style:
    - `crates/server/src/app.rs`: `unix_now_seconds`
    - `crates/server/src/lifecycle_repair.rs`: `current_unix_seconds`
  - Infallible fallback style:
    - `crates/server/src/provider_events.rs`: `current_unix_seconds`
    - `crates/server/src/gc.rs`: `current_unix_seconds`
    - `crates/cli/src/hold.rs`: `current_unix_seconds`
    - `crates/cli/src/admin.rs`: `unix_timestamp_now`

- [x] Consolidate bool environment parsing after deciding whether `on`/`off` should be accepted everywhere.
  - `crates/server/src/config.rs`: `parse_env_bool`
  - `crates/cli/src/storage_migration.rs`: `parse_bool`

## Test Helpers

These should be extracted to test support first, then removed from individual test modules.

- [x] Create a server test fixture helper for Xet object/shard generation.
  - Repeated helpers:
    - `xet_hash_hex`
    - `single_chunk_xorb`
    - `single_file_shard`
  - Current copies:
    - `crates/server/src/local_backend.rs`
    - `crates/server/src/app.rs`
    - `crates/server/src/fsck.rs`
    - `crates/server/src/gc.rs`
    - `crates/server/src/rebuild.rs`
  - `shard_hash_hex` also repeats in:
    - `crates/server/src/fsck.rs`
    - `crates/server/src/gc.rs`

- [x] Extract a shared integration-test invariant error helper.
  - `crates/server/tests/support/mod.rs`: `ServerE2eInvariantError`
  - `crates/cli/tests/support/mod.rs`: `CliE2eInvariantError`
  - `crates/vcs/tests/support/mod.rs`: `ProviderTokenFlowInvariantError`
  - Shared helper now lives in `crates/test_support/src/lib.rs` as `InvariantError`, with crate-local aliases preserved for test readability.

## Suggested Order

1. Do the instant removals first.
2. Extract checked arithmetic helpers and `map_xorb_visit_error`.
3. Extract index key/scope-key builders.
4. Extract test Xet fixture helpers.
5. Tackle filesystem/path helper consolidation last because it has the highest regression risk.
6. Decide whether the cross-crate integration-test invariant helpers merit a tiny shared test-support crate.
