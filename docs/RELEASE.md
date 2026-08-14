# Coordinated crates.io Release

This document describes how to publish the Shardline workspace to crates.io for a
coordinated release (all publishable crates move together to the same version, e.g.
`v1.5.0`). Because every crate rewrites its in-workspace path dependencies to
crates.io requirements (`^1.5.0`), the release **must** go out bottom-up
(dependencies first).

## Prerequisites

- A crates.io API token for the account that owns all `shardline-*` crates and `sdx`.
  Log in once:
  ```bash
  cargo login
  ```
- The version bump is already applied (see [Preparing a release](#preparing-a-release)).
- Working network access to `crates.io` (required for `cargo publish`, including
  `--dry-run`, which updates the crates.io index).

## Preparing a release

1. Bump the workspace version in `Cargo.toml`:
   - `[workspace.package] version = "X.Y.Z"`
   - `[workspace.dependencies]`: bump every `shardline-*` `version = "..."` (and
     `sdx`) to the new version so published manifests are coherent.
2. Refresh the lockfile and confirm the workspace still builds:
   ```bash
   cargo check --workspace
   ```
3. Update `CHANGELOG.md`: the repo convention is to rename `## [Unreleased]` to a
   dated `## [<version>] - YYYY-MM-DD` section at release time. Do **not** add a dated
   section yourself in a normal change.
4. Confirm formatting/clippy on the crates being published:
   ```bash
   cargo fmt --all -- --check
   cargo clippy -p sdx -p shardline-xet-adapter -p shardline-server
   ```

## Publish order (bottom-up, dependencies first)

Verified from the `cargo metadata` dependency graph at `v1.5.0`. Each crate must be
on crates.io at the new version before any crate that depends on it.

1. `shardline-metrics`
2. `shardline-protocol`
3. `shardline-test-support`
4. `shardline-validation`
5. `shardline-xet-core`
6. `shardline-auth`
7. `shardline-cache`
8. `shardline-storage`
9. `shardline-vcs`
10. `shardline-index`
11. `shardline-cas`
12. `shardline-server-core`
13. `shardline-hub-api`
14. `shardline-oci-adapter`
15. `shardline-protocol-adapters`
16. `shardline-xet-adapter`  ← MUST land before `sdx`
17. `sdx`                    ← depends on the adapter (tree/path/revision route constants)
18. `shardline-s3-adapter`   ← depends on `shardline-server-core`; MUST land before `shardline-server`
19. `shardline-fsck`
20. `shardline-gc`
21. `shardline-provider-events`
22. `shardline-rebuild`
23. `shardline-server`       ← depends on `shardline-s3-adapter`
24. `shardline`              ← CLI binary; depends on `sdx`, so last

### Excluded crates

- `shardline-fuzz` — `publish = false`
- `shardline-loom-tests` — `publish = false`
- `shardline-bench` — benchmark/load-test crate, not part of the coordinated release

## The `shardline-xet-adapter` → `sdx` constraint

`sdx` imports the tree/path/revision route constants (`XET_TREE_ROUTE`,
`XET_PATH_ROUTE`, `XET_REVISIONS_ROUTE`, `XET_REVISION_ROUTE`) from
`shardline-xet-adapter`, so the adapter must be published before `sdx`.

> **Historical note (resolved):** during the `v1.4.0` release cycle these
> constants existed only on the feature branch, and the published
> `shardline-xet-adapter@1.3.0` did not have them — publishing `sdx` first failed
> because its tarball resolved the adapter to the stale 1.3.0. The constants ship
> in the `v1.5.0` adapter, so this is now just the ordinary dependency-order
> constraint: publish the adapter first.

Consequence: publishing `sdx` **before** `shardline-xet-adapter` fails — the sdx
tarball cannot resolve `^X.Y.Z` for the adapter until that version is on crates.io.
Always publish the adapter first.

`sdx` is published at position 17; `shardline` (the CLI binary) depends on `sdx` and is
therefore published last (position 24). `shardline-server` depends on
`shardline-s3-adapter`, so that adapter (position 18) must land before it.

## Verification gates between publishes

Use the provided script, which defaults to `--dry-run`:

```bash
# Dry-run the whole release in order (nothing uploaded).
./scripts/publish-coordinated.sh

# Actually publish everything, in order.
./scripts/publish-coordinated.sh --go
```

For a manual publish of a single crate:

```bash
cargo publish -p <crate> --dry-run --allow-dirty   # verify the tarball first
cargo publish -p <crate> --allow-dirty             # then upload
```

Each `cargo publish` verifies the crate's own tarball (packaging + a clean build in
isolation). `--allow-dirty` is required because the version-bump leaves uncommitted
`Cargo.toml` / `Cargo.lock` changes.

> **Important:** a `--dry-run` of a crate whose dependencies are not yet on crates.io
> at the new version will fail at dependency resolution ("failed to select a version").
> That is expected and is not a packaging problem. Re-run the dry-run for that crate
> after its dependencies have been published — it will then pass.

## Final `sdx` publish note

`sdx` is a new crate (name was free on crates.io) and publishes at the workspace
version `1.5.0`. Its dry-run can only pass after `shardline-xet-adapter@1.5.0` is
actually on crates.io. After publishing the adapter, re-run:

```bash
cargo publish -p sdx --dry-run --allow-dirty   # verify (should now pass)
cargo publish -p sdx --allow-dirty             # upload
```

`sdx` pins `xet-core-structures = "=1.5.2"`; that exact version is on crates.io and
must not drift.

## Rollback / partial release

If a publish fails partway through, the crates already published are fine to keep
(they are all backward-compatible patch/minor within `^`). Fix the failing crate and
resume from it; you do not need to re-publish anything before it.
