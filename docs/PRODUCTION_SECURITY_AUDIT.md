# Production Security Audit

This document tracks production-preparation security review work for Shardline.
Every unchecked item must be handled with the same workflow:

1. Identify the reachable sink.
2. Add a POC or regression test that demonstrates the unsafe behavior.
3. Fix the implementation.
4. Keep the regression test or fuzz seed permanently.
5. Run `cargo make clippy`, `cargo make test`, and `cargo make format`.

## Audit Run 2026-04-21

The 2026-04-21 production audit rechecked the route table, bearer-scope enforcement,
endpoint body ceilings, local filesystem anchoring, S3 prefix isolation, storage
migration, operator output paths, secret-file ingestion, production Kubernetes mounts,
Docker runtime defaults, SQL/process execution, fuzz coverage, and dependency policy
checks.

Findings converted into permanent regressions:

- S3 credentials now support bounded file-backed `_FILE` ingestion in server startup and
  storage migration; direct credential variables conflict with matching `_FILE`
  variables.
- Production Kubernetes manifests now mount S3 credentials as files and reference them
  with `_FILE` variables instead of injecting them directly into process environment
  variables.
- S3 key prefixes now validate through the same object-prefix parser used by
  storage-boundary inventory paths, so traversal and dot-segment prefixes fail before
  client construction.
- A focused local filesystem race fuzz target now exercises parent swaps, symlink
  replacement, and anchored-write path identity drift; its corpus now includes persisted
  coverage inputs from a 600-second sanitizer-backed local run.
- Non-Unix shardline builds now fail at compile time until equivalent anchored
  filesystem hardening exists.

Supply-chain note: `cargo deny --manifest-path ../../Cargo.toml check` passed.
`cargo audit --file ../../Cargo.lock` reported RUSTSEC-2023-0071 for `rsa` through the
workspace lockfile's `sqlx-mysql` package.
Cargo's resolved tree disagreed with that lockfile-level report:
`cargo tree --manifest-path ../../Cargo.toml --target all -i rsa` and
`cargo tree --manifest-path ../../Cargo.toml --target all -i sqlx-mysql` printed no
resolved Shardline path, and Shardline's direct `sqlx` uses disable default features and
enables only Postgres.
The same audit reported unmaintained `derivative` through Shardline server dev-only
`xet-client`/`xet-data` test dependencies.
No production Shardline dependency change was available from the audit output inside
this crate.

## Reference Taxonomy

This checklist maps Shardline-specific surfaces to widely used security taxonomies:

- OWASP API Security Top 10 2023: object authorization, authentication, resource
  consumption, function authorization, SSRF, misconfiguration, inventory, and unsafe
  upstream API consumption.
- OWASP Top 10 2021: access control, cryptographic failures, injection, insecure design,
  misconfiguration, vulnerable components, authentication failures, integrity failures,
  logging gaps, and SSRF.
- CWE Top 25: injection, path traversal, missing authorization, unrestricted upload,
  input validation, resource exhaustion, improper authentication, race conditions, and
  integer/bounds issues.

## HTTP Authorization and Enumeration

- [x] Require bearer authorization before xorb metadata lookup on `HEAD /v1/xorbs` and
  `GET /transfer/xorb`, so unauthenticated clients cannot distinguish existing and
  missing objects.
- [x] Require bearer authorization before route-specific xorb hash and transfer-prefix
  validation, so unauthenticated clients cannot use validation differences as a route
  oracle.
- [x] Authenticate provider token and Git LFS bootstrap endpoints before reading or
  parsing JSON request bodies.
- [x] Authenticate provider bootstrap token routes before provider path and subject
  validation, so unauthenticated callers cannot use path/query validators as an oracle.
- [x] Re-audit every public route for object-level authorization before metadata, cache,
  or object-storage access.
- [x] Re-audit repository-scope checks for every reconstruction, chunk, xorb, shard,
  provider-token, and lifecycle endpoint.
- [x] Confirm read-only tokens cannot reach any write side effect, including repair,
  registration, cache invalidation, retention, and lifecycle paths.
- [x] Confirm write tokens only imply read where the token model intentionally permits
  it.

## Resource Exhaustion and Parser Bounds

- [x] Re-audit HTTP body limits for every endpoint, including extractor ordering before
  handler code runs.
- [x] Re-audit xorb normalization and shard parsing for bounded memory, bounded CPU, and
  no untrusted recursion.
- [x] Re-audit reconstruction response building for adversarially large metadata rows,
  range explosions, and cache poisoning.
- [x] Re-audit provider webhook parsing for header/body limits before HMAC, JSON, or
  provider-specific parsing.
- [x] Re-audit transfer streams for backpressure, bounded buffering, client disconnects,
  and worker-task lifetime.
- [x] Re-audit operator commands for bounded local file reads and untrusted adapter
  inventories.

## Storage, Filesystem, and TOCTOU

- [x] Re-audit local object, index, and record write paths for parent-directory swap
  races and keep deterministic regression tests that replace validated parents with
  symlinks before the final write syscall.
- [x] Re-audit operator-facing CLI output writes so backup manifests, GC export
  artifacts, generated systemd units, and generated shell/manpage files reject symlinked
  or special-file targets and fail closed if the validated parent directory is swapped
  before commit.
- [x] Re-audit local deployment-root and scheduled-service working-directory selection
  so symlinked root components are rejected before CLI, backup, fsck, migration, or
  generated service flows can traverse redirected local state trees.
- [x] Re-audit backup, migration, systemd, and config paths for traversal, symlink
  escape, special files, and time-of-check/time-of-use races.
- [x] Re-audit S3-compatible object store behavior for prefix isolation, key validation,
  range validation, idempotent writes, and conflict handling.
- [x] Re-audit storage migration and backup inventory so corrupted source adapters
  cannot cause unbounded work or unsafe destination writes.
- [x] Re-audit garbage collection, retention holds, quarantine, fsck, repair, and
  rebuild for fail-closed behavior under partial adapter failure.

## Injection, SSRF, and External Calls

- [x] Re-audit all shell/process execution paths for command injection and environment
  poisoning.
- [x] Re-audit SQL queries and migrations for typed parameters and no string-built SQL
  from repository, provider, or hash input.
- [x] Re-audit provider clone URLs, S3 endpoints, cache URLs, and Postgres URLs for SSRF
  and unsafe operator configuration surfaces.
- [x] Redact token signing keys, provider bootstrap keys, metrics tokens, Redis URLs,
  Postgres URLs, and S3 credentials from `Debug` output on core runtime structs and CLI
  command parsing.
- [x] Re-audit logs, metrics, and error bodies for token, provider-key, secret, URL, and
  object-existence leakage.

## Cryptography, Tokens, and Secrets

- [x] Re-audit token signing, verification, expiry, issuer, repository scope, revision,
  and scope semantics.
- [x] Re-audit constant-time comparison use for provider keys, webhook signatures,
  metrics tokens, and bearer tokens where applicable.
- [x] Reject symlinked startup secret files and provider catalog files, including
  `O_NOFOLLOW` on Unix to close symlink-swap races during open.
- [x] Re-audit secret-file loading, admin-token minting, and environment parsing for
  bounds, race handling, symlink/special-file rejection, and accidental logging.
- [x] Zeroize shardline-owned long-lived secret buffers and secret-bearing runtime
  strings, including token signing keys, metrics tokens, provider bootstrap keys,
  webhook secrets, provider-issued bearer tokens, and credential-bearing
  Redis/Postgres/S3 config fields.
- [x] Re-audit provider configuration parsing so webhook secrets deserialize straight
  into zeroizing storage and raw JSON config buffers are scrubbed immediately after
  parse.
- [x] Re-audit provider-runtime startup preflight so incomplete config, missing signing
  keys, or invalid provider TTLs fail before the provider bootstrap key file is read
  into memory.
- [x] Re-audit server and CLI S3 environment loaders for early-error secret reads, so
  missing buckets or invalid boolean flags fail before S3 credential env values are
  fetched.
- [x] Re-audit transient secret copies and third-party builder handoff paths where
  external crates may clone plaintext credentials outside shardline-owned zeroizing
  wrappers.
- [x] Re-audit env-derived secret boundaries that inherently return ordinary `String`s
  first (including S3 credentials) and decide whether shardline should prefer file-based
  secret references or another secret source for same-host memory disclosure threat
  models.
- [x] Re-audit shipped Kubernetes manifests for mounted-secret least privilege;
  production templates now pin `fsGroup`, `fsGroupChangePolicy`, and secret
  `defaultMode` instead of relying on permissive default secret file modes.
- [x] Re-audit Unix local write paths for umask-dependent disclosure; shardline-created
  local directories and files now pin restrictive modes instead of inheriting permissive
  host defaults.
- [x] Re-audit generated systemd units and remaining Kubernetes operational defaults for
  least privilege, network policy, and unsafe runtime assumptions beyond mounted-secret
  file modes.
- [x] Re-audit externally managed secret files and mounts for mode expectations, because
  shardline can now control only the files and directories it creates itself.
- [x] Re-audit internal library entrypoints that accept local roots directly, without
  the CLI config validation layer, for symlinked-root traversal and redirected
  local-state access.
- [x] Re-audit non-Unix local filesystem hardening parity, because current symlink-race
  defenses rely on Unix-only `O_NOFOLLOW` and anchored-directory operations while
  non-Unix fallbacks remain best-effort.

## Supply Chain and Build Integrity

- [x] Re-run dependency and license policy checks for vulnerable or unmaintained crates.
- [x] Re-audit Docker images for non-root execution, minimal runtime files, health
  checks, and secret-free layers.
- [x] Re-audit CI and local quality gates so Clippy-silencing attributes cannot enter
  production code.
- [x] Re-audit fuzz targets and corpora for every public parser and protocol boundary.
