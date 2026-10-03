# Coordinated crates.io Release

This document describes how to publish the Shardline workspace to crates.io for a
coordinated release (all publishable crates move together to the same version, e.g.
`v1.5.0`). Because every crate rewrites its in-workspace path dependencies to
crates.io requirements (`^1.5.0`), the release **must** go out bottom-up
(dependencies first).

Publishing is triggered only by pushing a release tag. Manual runs of the Release
workflow validate the build and never publish crates, images, or release assets.
The tagged commit must be on `main`, and the tag must match the workspace version.

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

## Patch validation

Validate the final release diff before changing the version or publishing. For patches
that affect storage, transfers, or recovery, run these gates sequentially so separate
suites do not compete for the same Docker and host resources:

```bash
cargo make ci
cargo make test-docker
cargo make test-loom
cargo nextest run -p sdx --tests --all-features
cargo nextest run -p shardline-storage --test resource_pressure
cargo make test-kubernetes
```

The Docker gate includes server integration tests and the separate `e2e` workspace.
Keep its SQLite and SQLx dependencies compatible with the main workspace and commit
its refreshed lockfile when those dependencies change. The Kubernetes gate creates
and removes a disposable kind cluster and verifies persisted data after pod replacement.

Record ignored tests and runtime skips alongside the results. A test that requires
`DATABASE_URL`, live provider credentials, or an external client does not prove that
integration merely by returning successfully without its fixture. Run the relevant
fixture explicitly when it is part of the release scope, and verify that configured
fixtures actually initialize.

Review [Database Migrations](DATABASE_MIGRATIONS.md) and
[Rolling Upgrade](ROLLING_UPGRADE.md) for the specific pending migrations and mixed-version
constraints. Rehearse ordinary PostgreSQL index builds against a representative restored
database and drain writers for the documented maintenance window before rollout.
Keep local audit evidence out of release commits and the container build context.

## Publish order (bottom-up, dependencies first)

Derive the order from the release commit's `cargo metadata` dependency graph. Each crate must be
on crates.io at the new version before any crate that depends on it.

```bash
python3 scripts/publish-order.py --emit
```

The Release workflow uses this same dependency-first order. Runtime, build, and
development dependencies all count: packaging verification also needs published
development dependencies. Do not reuse a list from an earlier release.

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

`sdx` also has a development dependency on `shardline-server`, so the server must
be published before `sdx`. `shardline` (the CLI binary) depends on `sdx` and is
published last. The server's own dependencies, including `shardline-s3-adapter`,
must be published before the server.

## Verification gates between publishes

Use the provided script, which defaults to `--dry-run`:

```bash
# Dry-run the whole release in order (nothing uploaded).
./scripts/publish-coordinated.sh
```

For a packaging check of a single crate:

```bash
cargo publish -p <crate> --dry-run --allow-dirty
```

Each `cargo publish` verifies the crate's own tarball (packaging + a clean build in
isolation). `--allow-dirty` is required because the version-bump leaves uncommitted
`Cargo.toml` / `Cargo.lock` changes.

> **Important:** a `--dry-run` of a crate whose dependencies are not yet on crates.io
> at the new version will fail at dependency resolution ("failed to select a version").
> That is expected and is not a packaging problem. Re-run the dry-run for that crate
> after its dependencies have been published — it will then pass.

## SDK packaging check

`sdx` publishes at the workspace version. Its packaging check needs its workspace
dependencies, including development dependencies, published at the new version.
The tag-triggered workflow publishes them first, then verifies and publishes SDX.
For a read-only packaging check when those versions are available:

```bash
cargo publish -p sdx --dry-run --allow-dirty   # verify (should now pass)
```

`sdx` pins `xet-core-structures = "=1.5.2"`; that exact version is on crates.io and
must not drift.

## Rollback / partial release

For a transient failure, rerun the failed tag-triggered workflow. It verifies
which crate versions are already published and skips them before continuing in
dependency order.

If recovery requires source or manifest changes, prepare a new coordinated
release version and push a new release tag. Keep existing release tags and
published crate versions unchanged; do not resume through manual publishing.
