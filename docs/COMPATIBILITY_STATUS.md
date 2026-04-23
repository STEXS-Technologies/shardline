# Compatibility Status

Shardline targets the public Xet protocol and the operator workflows needed to run it in
self-hosted deployments.

## 1.0.0 Contract

Shardline `1.0.0` should be treated as a full drop-in Xet backend for the validated
workflows covered by this repository.

## Validated Today

- native Xet upload and download flows
- provider-issued repository-scoped tokens and provider webhook handling for GitHub,
  GitLab, Gitea, and the generic provider adapter
- stock `git` + `git-lfs` + `git-xet` push, clone, fetch, pull, and historical checkout
  coverage in the validated test matrix
- sparse checkout behavior for Xet-tracked files
- local SQLite + filesystem deployments and Postgres + S3-style durable deployments
- operator workflows for migrations, fsck, lifecycle repair, index rebuild, backup,
  storage migration, retention holds, and garbage collection
- fuzz coverage for protocol parsing, reconstruction, lifecycle repair, CLI parsing, and
  local filesystem race boundaries

## Current Limits

- Shardline does not claim blanket compatibility across every possible Git workflow,
  deployment topology, provider setup, or client-version combination.
- The public claim is intentionally scoped to the Xet-facing protocol and operator
  surface documented in this repo.
- The first crates.io release still has to publish the internal crate graph in
  dependency order before publishing the `shardline` CLI crate.

## Ongoing Ratchets

- keep expanding protocol-conformance coverage as new native-client behavior is observed
- keep extending the validated `git` + `git-lfs` + `git-xet` workflow matrix
- keep adding provider webhook fixtures as new primary-source payload variants are seen
- keep extending fuzzing, benchmark, and profiling coverage around new hot paths
