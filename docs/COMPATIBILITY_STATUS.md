# Compatibility Status

Shardline targets the public Xet protocol and is being built to run against existing Git
workflows and self-hosted deployments.

Current status:

- core CAS routing exists
- serialized xorb upload validation exists
- serialized shard upload validation exists
- dedupe shard fetch exists
- local and Postgres-backed metadata paths exist
- provider adapters exist for GitHub, GitLab, Gitea, and generic forges
- provider-backed runtime token issuance is covered through live HTTP routes for GitHub,
  GitLab, Gitea, and generic adapters
- provider-backed webhook handling is covered through live HTTP routes for GitHub
  repository lifecycle and revision-push events, GitLab repository lifecycle and
  revision-push events, Gitea repository lifecycle and revision-push events, and the
  generic normalized webhook contract
- provider lifecycle handling persists durable repository state for access-change and
  revision-push events, and applies deletion and rename transitions to retained metadata
- storage, index, and reconstruction-cache adapters are explicit and replaceable behind
  crate boundaries
- Docker packaging and operator tooling exist
- native Xet upload and download flows are covered by end-to-end tests
- authenticated native Xet flows are covered with provider-issued repository-scoped
  bearer tokens, ranged downloads, and cross-repository scope rejection
- native Xet refresh-route bootstrap is covered end to end through the token refresher
  path
- authenticated native Xet concurrent upload and download sessions are covered end to
  end
- authenticated native Xet long-lived upload, full-download, and ranged-download
  sessions are covered end to end through repeated refresh-route cycles
- an unpatched `git-lfs` + `git-xet` push flow is covered by end-to-end tests
- a `git` + `git-lfs` + `git-xet` clone, fresh cold clone, fetch, pull, and historical
  checkout smoke test is covered with stock tooling
- sparse checkout creation and sparse checkout expansion are covered with stock tooling
  for Xet-tracked files
- provider-mediated Git LFS download batches reconstruct file bytes through Shardline
  native CAS routes when the installed `git-xet` build routes downloads through the
  standard Git LFS download path
- provider-mediated native Xet downloads are covered for GitHub, GitLab, Gitea, and
  generic provider adapters
- operator integrity workflows exist for `fsck`, lifecycle repair, rebuild, retention
  holds, backup, storage migration, and garbage collection
- corruption-detection and garbage-collection fail-closed behavior are covered through
  tests and operator documentation
- protocol, reconstruction, lifecycle-repair, CLI, and local-filesystem boundaries are
  covered by dedicated fuzz targets and persisted corpora
- Criterion microbenchmarks, deterministic CLI end-to-end benchmarks, and repeatable
  `perf` workflows exist for hot-path profiling

Shardline is not yet claimed as a complete drop-in Xet backend across every Git workflow
and deployment matrix.

## Current Limits

- Shardline does not yet claim blanket compatibility across every possible
  forge-specific workflow, deployment topology, and client-version matrix.
- Shardline documentation intentionally scopes the public contract to the Xet-facing
  protocol and operator surface it currently implements.
- The first crates.io release must publish the internal crate graph in dependency order
  before publishing the `shardline` CLI crate.

## Ongoing Ratchets

- keep adding protocol-conformance vectors as new native-client behavior is observed
- keep verifying unpatched `git` + `git-lfs` + `git-xet` flows as compatibility coverage
  grows
- keep expanding provider webhook fixtures when new primary-source payload variants are
  confirmed
- keep extending sustained-load, fuzzing, benchmark, and profiling coverage as new hot
  paths or regressions appear

## Release Gate

Shardline should only claim full Xet compatibility after:

- the verified workflow matrix is broad enough to remove the current deployment-matrix
  caveat
- provider-backed workflows remain transparent to normal Git usage on supported forges
- unpatched native Xet clients continue to cover upload, download, clone, fetch, pull,
  push, sparse checkout, and historical checkout against a stock Shardline deployment
