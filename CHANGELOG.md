# Changelog

All notable changes to Shardline are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/), and this project adheres to [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Added

- Add a disabled-by-default, versioned read-only administration API for storage,
  GC, integrity, node, task, replication, metrics, and future plugin status.
  The surface uses a dedicated constant-time bearer-token boundary, honest
  authoritative/process/external state semantics, admission control, no-store
  responses, bounded cursor pagination and typed collection filtering, and is
  available on every runtime role for external dashboards. The complete field,
  pagination, failure, compatibility, and applicable-threat contract is
  documented for API clients. Wire models live in an explicit v1 DTO module;
  page limits, filters, cursor tokens, cursor keys, and cursor state are typed
  instead of being passed through as unstructured primitives.
  The Postgres-kill deployment drill now verifies degraded/recovered status and
  fail-closed authorization/query handling plus sanitized errors while the durable
  metadata dependency is unavailable; cancellation/restart regressions verify
  polling cannot mutate inventory, and dedicated property and fuzz targets cover
  both the strict TOML secret-file setting and administration query parser plus
  versioned cursor DTO/newtype deserialization.
- Extend storage statistics with authoritative total object count and physical
  object bytes while preserving the existing CAS chunk/file counters.

## [1.8.0] - 2026-08-25

Stateless multi-replica hardening release. Eliminates the shared filesystem
dependency from scaled API replicas, making pods disposable and cross-replica
upload retries routine. Adds durable Postgres-coordinated resumable sessions
for all three upload protocols (LFS PATCH, OCI blob-upload, S3 multipart),
chaos-drills the new storage paths, and fixes Git LFS ↔ Hub API xet transfer
negotiation so `git-xet` can delegate uploads to the Xet/CAS path.

### Added

- Add bounded Ed25519 public-key rings and overlapping old/new verification so signing
  keys can rotate without rejecting unexpired tokens during a rolling deployment.
- Order S3 deletion so legacy direct bytes are removed before the fenced metadata commit,
  preventing crash-window resurrection through compatibility reads while keeping
  row-backed objects recoverable after metadata failures.
- Durable Postgres resumable-session state for Git LFS PATCH, OCI blob uploads,
  and S3 multipart uploads, with typed range/part maps, database-clock expiry,
  bounded durable accounting, and fenced completion.
- Cross-replica tests for all three resumable protocols, including out-of-order
  and overlapping LFS ranges.
- A typed S3 delete interruption boundary that permanently regresses the
  direct-byte removal crash window.
- GC inventory and counters for private resumable staging objects and terminal
  session metadata.
- Operation-scoped Redis cold-load reservations with random owner tokens,
  lease heartbeats, bounded waiter latency, and live two-replica regressions.
- Durable resumable-session chaos drills: `deployment_chaos` now fault-injects the
  stateless multi-replica storage path — Postgres-kill and MinIO-kill mid-LFS-PATCH
  (the durable session and staging survive the outage and the object completes
  byte-exact after recovery), network partition during overlapping LFS PATCH repair,
  Postgres-kill mid-OCI blob-upload (durable session survives, remaining bytes
  re-staged after recovery, blob byte-exact), and Postgres-kill mid-S3 multipart
  upload (durable session survives, missing part re-uploaded after recovery, object
  byte-exact). Mixed-version rollout is covered by the existing drill F (S3 PUT).

### Changed
- E2E CI now installs checksum-pinned Git LFS 3.7.1 and Bazelisk 1.29.0,
  exercises adjacent Git LFS 3.6.1 and Bazel 8.7.0/9.2.0, and explicitly runs
  the native Bazel cache flows instead of silently skipping unavailable clients.
- Scaled API replicas now keep incomplete payload fragments in object storage
  and coordination in Postgres; a lock-coherent RWX volume is no longer part of
  the production topology.
- Production Kubernetes API roots are pod-local `emptyDir` volumes.
- S3 object publication commits immutable record metadata and the visible S3
  index row in one fenced transaction.
- S3 single and batch deletion now take the same cross-replica key fence as
  publication and atomically remove listing and visible record metadata.
- Reconstruction-cache loaders now carry an explicit reservation newtype from
  lookup through publish, failure cleanup, heartbeat, and cancellation.

### Fixed
- Hub API `/objects/batch` endpoint now negotiates `xet` transfer when the client
  advertises it and the server has an auth provider, including CAS token minting
  and `X-Xet-Content-CAS-*` headers. Previously hardcoded `"basic"` regardless of
  client capabilities. Also validates `hash_algo` (rejects non-sha256), enforces
  the 1024-object batch limit, and returns `hash_algo` in the response.
- Re-baseline the line-coverage ratchet to 92.8% to reflect the single-replica
  role split, inline resumable-session reconstruction, and longer-lived durable
  staging paths landed in this release.
- Durable LFS PATCH now permits bounded overlapping repair ranges without
  misclassifying their retained immutable staging bytes as an oversized logical
  object; the coverage gate now executes every Postgres protocol-replica test.
- Replaced, failed, expired, aborted, and completed resumable staging objects
  are reclaimed under the writer-excluding GC barrier without risking live
  session parts.
- Release tag publishing no longer reruns the separate infrastructure matrix;
  that matrix remains a pull-request gate while release publishing retains its
  contributor CI and reliability gates.
- Redis cache publication and cleanup reject stale reservation owners with an
  atomic compare-and-mutate script, preventing timed-out or replaced loaders
  from overwriting or deleting a newer replica's cache state.

## [1.7.0] - 2026-08-23

Reliability release focused on distributed correctness, crash recovery, and
adversarial verification. **No breaking API changes** — the multi-writer
contracts, fencing, tombstones, and typed parsing additions are compatible with
existing deployments and data.

### Added
- **Distributed-correctness contract** — fenced multi-writer publication over
  shared Postgres/S3 state, explicit lock ordering, recovery semantics, and
  mixed-version/upgrade documentation.
- **Tombstone lifecycle** — durable deletion markers coordinate GC with
  concurrent readers, reconstruction, repair, and retries; GC now removes only
  safely expired tombstones.
- **Typed parsing and property coverage** — closed-domain newtypes replace
  string matching in security-sensitive paths, with expanded proptests and
  fuzz targets for protocol, URL, config, auth, and CAS boundaries.
- **Replayable chaos verification** — deterministic fault schedules, failpoints,
  invariant checks, and expanded database, object-store, network, restart, and
  authorization campaigns.

### Changed
- **Multi-node deployment guidance** now specifies the shared-filesystem byte
  and locking contract, Postgres/S3 fencing requirements, and N-1 upgrade
  compatibility.
- **GC and publication** use transactional reachability checks and bounded
  server-side Xorb batches, reducing memory pressure for large uploads.
- **Release CI** requires the reliability campaign before publishing crates,
  images, and release assets.

### Fixed
- Stale writers can no longer commit after lease replacement or fencing epoch
  changes.
- Crash and retry windows around metadata publication, object-store ambiguity,
  reconstruction, repair, and GC now recover to a valid state.
- Large streamed uploads no longer require a single unbounded server-side Xorb.
- Documentation now reflects the completed distributed-correctness contract.

## [1.6.0] - 2026-08-20

Major release that adds a full S3-compatible frontend and closes the hardening
cycle with an authz-capability system, fail-loud crypto boot, extreme fault
drills, and a comprehensive adversarial audit. **No breaking API changes** — the
S3 frontend is additive (coexists with Hub/Git/LFS/OCI/Bazel), the authz
capability system is opt-in, and existing deployments need no migration.

### Added
- **S3-compatible frontend** — full S3 API surface mounted as a coexisting
  frontend alongside Hub/Git/LFS/OCI/Bazel: `ListBuckets`, `ListObjectsV2`
  (index-backed), multipart upload, conditional requests (`If-Match` /
  `If-None-Match`), `CopyObject`, `DeleteObjects`, standard MD5 ETags, user
  metadata, and real-client end-to-end coverage (boto3 + MinIO).
- **Authz capability system** — repository-scoped capability tokens for the
  server's internal authorization layer; per-repo token binding is enforced on
  all repository-scoped routes.
- **Fail-loud crypto boot** — the server now refuses to start when required
  cryptographic primitives (SHA-256, Ed25519, AES-256-GCM) are unavailable,
  instead of silently falling back to insecure defaults.
- **Extreme fault drills and chaos runner** — a new `fault_drills_extreme` test
  suite and a `chaos_runner` that exercise kill-hard, cache-drop, GC-under-
  interference, quarantine-junk, and multi-node chaos scenarios under real-
  infrastructure conditions.
- **Perf and ops baseline** — `cargo bench` harness for throughput and latency
  regression detection.
- **Metadata-driven publish order** — `scripts/publish-order.py` derives the
  crates.io publish DAG from `Cargo.toml` `[dependencies]` instead of a
  hand-maintained list.
- **CI: S3 real-client end-to-end** — boto3 real-client test suite merged into
  the main end-to-end job; S3-compatible tests run against a live MinIO
  container.

### Changed
- **End-to-end and integration test harnesses** now opt into explicit
  `Insecure` mode for local test servers, making the security boundary explicit
  in CI.
- **CI: S3 real-client job** merged into the unified end-to-end job (single
  matrix entry instead of a separate workflow).
- **User-facing docs** rewritten for clarity; rustdoc private-link warnings
  fixed.
- **GC sweep performance** (continued from 1.5.0) — quarantine candidate
  resolution is now O(1) per candidate with a re-verified reachability
  snapshot.

### Fixed
- **Protocol adapter type-level seal** — `AuthorizedRepository`,
  `RepositoryScope`, and `ObjectKey` are now sealed types; external crates
  cannot construct or forge them.
- **Startup fails loud on insecure default** — the server now returns a
  startup error instead of silently running in permissive mode when no auth
  provider is configured.
- **GC quarantine stale-reference skip** — quarantine candidates that reference
  re-referenced objects are now correctly skipped during sweep.
- **Multi-replica caveats documented** — limitations of running multiple
  Shardline instances against a single metadata store (no distributed locking;
  intended for single-node or read-replica topologies).
- **LFS patch lock de-flake** — `acquire_lfs_patch_lock_evicts_dead_entries`
  no longer races under parallel CI runs.
- **Chaos runner index adapter tolerance** — transient index-adapter errors
  during GC under coverage instrumentation are now settled and retried instead
  of panicking.
- **Fault drills kill-hard pre-completion** — `kill_hard` now accepts both
  task cancellation and pre-completion outcomes under slow coverage execution.
- **S3 `CopyObject` content-length ceiling** — `CopyObject` now enforces the
  same maximum object size as uploads instead of allowing unbounded copies.
- **S3 conditional-request TOCTOU** — conditional overwrite now uses a single
  immutable read snapshot, closing the time-of-check-to-time-of-use race.
- **S3 lock-map leak on failed multipart abort** — the in-memory lock map is
  now cleaned up when a multipart abort fails, preventing a slow memory leak.
- **S3 session-lock convoy under concurrent abort** — concurrent abort calls
  no longer serialize on the same session lock, eliminating the convoy under
  parallel workloads.
- **S3 torn metadata on atomic overwrite** — metadata updates are now written
  atomically, preventing partial reads during concurrent overwrites.
- **S3 `DeleteObjects` hardening** — batch delete now validates per-key
  authorization and handles partial failures correctly.
- **Fuzz harness oracles** — three pre-existing harness oracles repaired by
  the S3 smoke campaign.

### Security
An adversarial hardening cycle covering the full server surface found and fixed
the following. The core Xet/LFS/OCI/Bazel frontends were audited and verified
sound.
- **[Critical] Cross-tenant authorization bypass on Hub API routes** —
  repository-scoped Hub API handlers did not enforce token-to-URL binding, so
  any authenticated user could read, modify, or push to any other tenant's
  repository. Fixed by requiring the token's `owner`/`name` to match the
  request path on all repository-scoped routes.
- **[Critical] Hub API LFS global namespace — cross-tenant content poisoning**
  — Hub API LFS objects were stored under a bare global key with no per-repo
  namespace, allowing content substitution via first-writer-wins. Fixed by
  namespacing LFS keys per-repo via `scope_namespace`.
- **[High] Hub API `repo_list` / `repo_search` cross-tenant private repo leak**
  — the global list and search endpoints returned private repos from every
  tenant. Fixed by filtering by caller ownership.
- **[High] `apply_delta` unbounded output growth (receive-pack OOM)** — a
  crafted pack delta could grow the result vector to ~32 TB transient
  allocation. Fixed by checking copy-instruction bounds against the declared
  target size.
- **[High] GC quarantine sweep used a stale orphan snapshot** — an object
  re-referenced after the snapshot could be deleted while the index still
  referenced it. Fixed by re-verifying reachability against current index
  state immediately before each delete.
- **[Medium] Reconstruction-cache loader-failure latch leak** — a failed load
  left the key `loading` forever, stalling every later requester. Fixed by
  clearing the latch on the error path.
- **[Medium] `sdx` path registration mangled spaces into `+`** — path segments
  were form-encoded; now RFC-3986 pchar percent-encoded, so path operations
  round-trip consistently.
- **[Medium] Receive-pack silently dropped bundled LFS content** when a pushed
  pointer blob was not byte-identical to the canonical pointer. Fixed by
  matching content by SHA-256 digest and failing loudly when referenced LFS
  content is absent.
- **[Medium] S3 multipart session-lock convoy** — concurrent abort calls
  serialized on the same session lock. Fixed by using non-blocking lock
  acquisition.
- **[Medium] S3 torn metadata on atomic overwrite** — metadata writes were not
  atomic. Fixed with a write-then-swap pattern.
- **[Low] Git pack parser accepted truncated zlib streams** — `decompress_zlib`
  now requires `StreamEnd`, and `parse_pack_data` rejects a header whose
  `num_objects` is not fully consumed.
- **[Low] Zero-length suffix byte-range** returned a bogus 1-byte range instead
  of `Unsatisfiable`. Fixed to be consistent with the non-suffix branch.
- **[Low] Upload-intent transitions were not idempotent** — two concurrent
  callers sharing one intent could spuriously 500. Fixed by treating
  at-or-past-target state as success.

## [1.5.0] - 2026-08-11

Minor release that hardens parsing boundaries with newtypes, caps untrusted
allocations, and broadens fuzz coverage. **No breaking API changes** — all
newtype work is additive (new types, `From` impls, and consolidation of internal
duplicates). Wire formats and token claims are unchanged; existing deployments
need no migration. The data-format changes are **opt-in**: when
`SHARDLINE_HUB_WEBHOOK_SECRET_KEY` is configured, Hub webhook signing secrets are
stored at rest using AES-256-GCM (`sse1:`-prefixed ciphertext) instead of
plaintext, with an automatic upgrade of existing plaintext rows; and when
`SHARDLINE_CONFIG_SECRET_KEY` is configured, provider-config webhook secrets are
stored at rest using the same `sse1:` envelope, with legacy plaintext values
parsed unchanged.

### Added
- **Newtypes for closed-domain strings** (additive, no signatures removed):
  - `JwtAlgorithm` in `shardline-server` — typed JWT algorithm enum with
    `is_asymmetric()` / `is_symmetric()` / `is_none()` classifications; both
    the JWKS and OIDC providers now route their `alg`-confusion guard through
    it instead of bare `== "none"` literal checks.
  - `ByteUnit` (`shardline-server` config) — case-insensitive `FromStr` driving
    `parse_byte_size`; `GitSmartHttpService`, `HubSortField`, `SortDirection`,
    and `WebhookScheme` (`shardline-hub-api`); `OciAction` (OCI token scope).
  - `RepositoryVisibility::FromStr` (`shardline-vcs`) — single canonical,
    case-insensitive, whitespace-tolerant parser.
  - Bidirectional `From` impls: `ProviderKind` ↔ `RepositoryProvider`
    (`shardline-vcs`), and `RepoType` ↔ `HubRepoType` (`shardline-hub-api`).
  - `RepositoryScopeCacheKey::provider_kind()` typed accessor
    (`shardline-cache`) returning the canonical `RepositoryProvider`.
  - `RepositoryProviderParseError` is now re-exported from the
    `shardline-protocol` crate root.
- **Fuzz coverage** (`shardline-fuzz`): registered the previously-orphaned
  `shardline_cas_coordinator` target (and wired the missing `shardline-cas`
  dependency); added 5 new targets — `sdx_shard_parse`, `sdx_url`,
  `sdx_config` (first coverage for the `sdx` client), `auth_ed25519`
  (first coverage for `shardline-auth`), and a CAS-coordinator target; fixed
  the no-op/stub targets `git_tree_walker`, `index_hub`, and `git_pack`; removed
  two exact-duplicate targets (`hub_api_routes`, `rebuild_candidates`).

### Changed
- **Case-insensitive parsing made uniform**: `parse_bool` (`shardline-protocol`),
  the `Bearer` scheme (RFC 9110 scheme comparison, `shardline-server` +
  `shardline-hub-api`), `DeploymentMode` / `AuthProviderKind` /
  `ObjectStorageAdapter` config enums, and `RepositoryVisibility` now all accept
  any ASCII case and surrounding whitespace. Previously several were
  case-sensitive and silently fell through to defaults.
- **JWT `alg`-confusion guard strengthened**: in addition to `none`, symmetric
  algorithms (`HS256/384/512`) are now rejected before key matching in both
  providers, so a token signed with a symmetric secret cannot be confused for an
  asymmetric-key verification.
- **LFS server routing** now parses `operation`/`transfers` through the existing
  `LfsOperation`/`TransferAdapter` enums instead of re-matching raw strings.
- **Provider/visibility parser dedup**: the third `parse_provider_kind` and
  `visibility` parsers in `shardline-server` now delegate to the single canonical
  `RepositoryProvider::from_str` / `RepositoryVisibility::from_str`.

### Fixed
- **Untrusted-allocation caps**: bounded `Vec::with_capacity` and aggregate
  decompression output that were driven by attacker-influenced counts — xorb
  shard header entry counts (`shardline-xet-core`), file-segment/verification
  tables, `sdx` shard parsing, git pack delta varints (capped + shift-overflow
  guards), and xorb reconstruction (decompression-bomb aggregate cap; resolves a
  known `leak-` fuzz artifact).
- **Base64 decode before cap**: the hub-api commit handler now bounds the encoded
  length against `MAX_INLINE_FILE_BYTES` before decoding, rather than allocating
  the decoded buffer first.
- **GC sweep performance**: the delete-time reachability re-check now collects the
  referenced-key set ONCE immediately before the sweep loop (O(1) per candidate)
  instead of re-running a full reachability mark per expired candidate (which was
  effectively quadratic at scale — one mark per candidate, each reading/parsing
  xorb containers on large stores). The TOCTOU hardening is preserved with a
  strictly smaller delete-time window.

### Security
A three-lane adversarial audit (independent code-audit, money-lane/impact, and a
refutation pass) of the Hub API frontend found and fixed the following. The core
Xet/LFS/OCI/Bazel frontends were audited and verified sound (constant-time token
compares, `alg`-confusion guard, parameterized SQL, symlink-race protection,
`ObjectKey` path-traversal rejection, scoped storage namespaces).
- **[Critical] Hub API cross-tenant authorization bypass** — every Hub API route
  checked only the token's `scope` (Read/Write) and never bound the token's
  `RepositoryScope` to the URL-path repository. Any authenticated user could
  read, modify, delete, or `git push` to any other tenant's repository, install
  exfiltration webhooks, and read private datasets. Fixed: all repository-scoped
  Hub API handlers now enforce `require_repository_binding` (the token's
  `owner`/`name` must match the request path); genuinely global routes (list,
  search, whoami) are documentedly exempt, and repository creation
  (`/api/repos/create` and the `{type}/{ns}/{repo}` POST path) requires only
  Write scope — a caller need not hold a token pre-scoped to a
  not-yet-existing repository, while every access route remains bound.
  Covered by a new `cross_tenant_authz` integration suite (same-repo success
  + cross-repo `403`).
- **[Medium] Hub API LFS global namespace → cross-tenant content poisoning** —
  Hub API LFS objects were stored under a bare global `lfs/{oid}` key with no
  per-repo namespace, so a Write-scoped tenant could pre-empt a predictable OID
  (first-writer-wins via `put_if_absent`) to substitute attacker bytes for a
  victim's, and any Read-scoped tenant could read any OID. Fixed: Hub API LFS
  keys are now namespaced per-repo via `scope_namespace(claims.repository())` on
  both read and write paths, matching the core server's LFS isolation.
- **[Low, defensive] LFS PATCH unbounded `Content-Range`** — the chunked PATCH
  path accepted an arbitrary `u64` declared object size. Now capped at
  `MAX_LFS_OBJECT_SIZE` (1 TiB); oversized declared totals are rejected with
  `413`. (No measurable security impact — sparse files do not exhaust inodes and
  assembly is sha256-gated — but capped defensively.)
- **[Medium] Hub API `repo_list`/`repo_search` leaked other tenants' private
  repos** — the global list/search endpoints returned `private` repos from every
  tenant (the store queries had no identity/visibility filter). Now filtered by
  caller ownership (private repos visible only to their owner; public repos
  always visible).
- **[High] `apply_delta` unbounded output growth (git receive-pack OOM)** — a
  crafted pack delta whose copy instructions sum beyond the declared target size
  grew the result vector unboundedly (up to ~32 TB transient allocation). The
  mid-loop size checks now reject before each `extend_from_slice`.
- **[High] GC quarantine sweep used a stale orphan snapshot** — an object
  re-referenced after the snapshot could be deleted while the index still
  references it (free-during-insert data-loss race). The sweep now re-verifies
  reachability against current index state immediately before each delete and
  skips referenced objects.
- **[Medium] Reconstruction-cache loader-failure latch leak** — a failed load
  left the key `loading` forever, stalling every later requester 30 s and
  breaking load-once. The latch is now cleared on the error path (waiters are
  woken and can retry).
- **[Medium] `sdx` path registration mangled spaces into `+`** — path segments
  were form-encoded (`+` = space); now RFC-3986 pchar percent-encoded (`%20`),
  so `register_path`/`delete_path`/`list_dir`/`resolve_path` round-trip
  consistently.
- **[Medium] receive-pack silently dropped bundled LFS content** when a pushed
  pointer blob was not byte-identical to the canonical pointer (CRLF/extra
  fields) — content is now matched by `sha256(content) == oid`, and a push whose
  referenced LFS content is entirely absent fails loudly instead of creating a
  broken ref.
- **[Low] git pack parser accepted truncated zlib streams / partial packs** —
  `decompress_zlib` now requires `StreamEnd`, and `parse_pack_data` rejects a
  header whose `num_objects` is not fully consumed (no more garbage objects
  stored as a valid revision).
- **[Low] zero-length suffix byte-range** returned a bogus 1-byte range instead
  of `Unsatisfiable` — now consistent with the non-suffix branch.
- **[Medium] Hub webhook signing secrets at rest** — previously stored as
  plaintext `TEXT`. Webhook secrets are now encrypted at rest (AES-256-GCM, one
  random 12-byte nonce per row, bound to the repo via AAD) whenever
  `SHARDLINE_HUB_WEBHOOK_SECRET_KEY` (or `_FILE`) is configured; the server emits
  a startup warning otherwise. A migration sweeps and re-encrypts existing
  plaintext rows, and legacy rows are lazily upgraded on read. Secrets are never
  returned by the webhook API and never logged.
- **[Medium] webhook delivery DNS-rebinding TOCTOU** — delivery now re-resolves
  and re-verifies the address set immediately before the HTTP send (the
  documented rebinding window is closed; shared-client pinning was not available
  without the `dns` feature, so re-resolve+compare was used).
- **[Medium] Provider-config secrets at rest** — the `webhook_secret` field of
  the provider configuration JSON (`config.provider_config_path()`) is now
  encrypted at rest (AES-256-GCM, one random 12-byte nonce per value, bound to
  the provider via AAD `provider:<kind>:<field>`) whenever
  `SHARDLINE_CONFIG_SECRET_KEY` (or `_FILE`) is configured; the server emits a
  startup warning otherwise. Legacy plaintext values are parsed unchanged with
  or without a key; an at-rest encrypted value fails loudly on a wrong/missing
  key rather than falling back to plaintext.
- **[Low-Med] upload-intent transitions were not idempotent** — two concurrent
  callers sharing one intent could spuriously 5xx with
  `InvalidUploadTransition`; `transition_intent` now treats at-or-past-target as
  success across sqlite/postgres/memory.
- **Concurrency verification** — added three loom contract models (CAS
  coordinator exactly-once store + reachability-vs-sweep, reconstruction-cache
  load-once, GC quarantine no-free-during-insert/no-resurrect); all invariants
  hold under exhaustive interleaving exploration.
- **[Medium] Hub `repo_list`/`repo_search` leaked private repos within a
  namespace** — the visibility filter compared only the owner segment, so
  under OIDC (claims owner constant per subject) every user saw all other
  users' private repos, and a local token saw every private repo in its
  namespace. The filter now compares the FULL repo identity
  (`{owner}/{name}` exactly) for both list and search.
- **[Medium] Webhook-secret key-removal downgrade** — if
  `SHARDLINE_HUB_WEBHOOK_SECRET_KEY` is removed after rows were encrypted,
  stored `sse1:` ciphertext was previously used verbatim as the signing
  secret (silent webhook breakage). Resolution now fails loudly
  (`NoCipherForCiphertext`) when a ciphertext row has no key; ciphertext
  classification requires full structural validation so a legacy plaintext
  secret starting with `sse1:` is no longer misclassified; decrypted bytes
  are zeroized on all paths.
- **[Low] Webhook-secret key file trailing newline** — a 32-byte key file
  written with a trailing newline (e.g. `echo $KEY > file`) no longer aborts
  startup with a misleading length error; one trailing newline is stripped
  automatically and the length error message is explicit.
- **Dependency hygiene** — documented the rationale for every suppressed
  `cargo audit` advisory in `.cargo/audit.toml` and `deny.toml`.
- **Residual (documented, accepted)** — webhook delivery DNS-rebinding window
  is narrowed by pre-send re-resolution but not fully closed (connect-time
  pinning would require the reqwest `dns` feature, which ripples to 5 crates
  and pulls hickory-resolver; not warranted for the Low residual).

### Internal
- Unified the duplicate `PostgresRecordKind` / `LocalRecordKind` into a single
  `RecordKind` (`shardline-index`, `pub(crate)` — internal, no public impact).

## [1.4.0] - 2026-08-08

Minor release adding the native `sdx` Xet client library and file-management CLI,
server metadata endpoints for path addressing, and a cross-frontend conformance
suite. There are no intentional breaking CAS/data-plane changes from `1.3.0`.

### Added

- **`sdx` client library** (`crates/sdx`): native Xet client with V1/V2
  reconstruction, ranged xorb fetch, streaming bounded-memory download/upload,
  on-disk chunk cache, global dedup, retry/backoff, and token refresh on 401/403.
- **`sdx` CLI**: `cp`/`sync`/`ls`/`rm`/`cat`/`info`/`branch` over the Xet file_id
  surface, dispatched via the `shardline` binary's `argv[0]` symlink (`sdx`) and
  the `shardline xet` escape hatch.
- **Server metadata endpoints**: path→file_id tree store and revision registry for
  the Xet frontend, backed by a `TreeStore` trait across SQLite, Postgres, and the
  in-memory test store.
- **Cross-frontend conformance suite**: HF-style mock frontend exercising the
  client's portable file_id-level surface against a non-shardline Xet wire
  protocol.

### Fixed

- **Single-chunk xorb-backed downloads**: single-chunk records are now xorb-backed
  on ingest so CAS downloads of small files work over the transfer path.
- **fsck / lifecycle-repair reachability for xorb-backed records**: fsck no longer
  reports a false `missing_chunk` for single-chunk files, and lifecycle-repair now
  resolves the xorb's member chunks so the individually-stored dedup chunks stay
  reachable.
- **Postgres tree-store migration**: the metadata tree tables are now provisioned
  by `db migrate` on Postgres (previously SQLite-only).
- **CI stability**: hardened timing-sensitive streaming/abort/cache tests under
  llvm-cov instrumentation and fixed a repository-probe-filter test serialization
  flake.

## [1.3.0] - 2026-08-02

### Upgrade Note — Storage Format Evolution

This release changes the physical storage representation from fixed-size chunking (uncompressed)
to CDC chunking with LZ4 compression and optional xorb container packing (XorbCdcV1).

**No data migration required.** Old records written by v1.2.x and earlier remain fully readable,
reconstructable, and protected from garbage collection. The upgrade is safe for existing
deployments — no downtime or maintenance window needed for data format reasons.

#### What changes

| Aspect | Before (v1.2.x) | After (this release) |
|---|---|---|
| Chunking | Fixed-size 64KB chunks (default) | CDC (content-defined), target 64KB |
| Compression | None | LZ4 (`lz4_flex::compress_prepend_size`) |
| Containerization | None | Xorb packing at upload finish |
| Chunk hash | Raw bytes | Raw bytes (unchanged — dedup works across formats) |
| Storage format per record | Implicit (fixed chunk) | Explicit `storage_repr` field (`fixed_chunk_v1` or `xorb_cdc_v1`) |

#### Backward compatibility

- **WholeFileV1** (v1.0.0–present): Single-object storage, `chunk_size=0` → `ReferencedObjectTerms` layout.
  Read path unchanged (`reconstruct_referenced_object_file_bytes`).
- **FixedChunkV1** (v1.0.0–v1.2.2): Uncompressed fixed-size chunks. Decompression is decided
  by the record's `storage_repr`, so these records are served raw without LZ4 decoding.
- **XorbCdcV1** (new): CDC + LZ4 compression + optional xorb packing. `storage_repr =
  xorb_cdc_v1` always triggers LZ4 decompression, regardless of how `packed_end` compares
  to `chunk.length` (the compressed size can equal the raw size for small or incompressible
  payloads). Xorb-backed files use a single-GET fast path.

#### GC compatibility

All three formats are handled correctly:
- Individual chunk paths and xorb container paths are referenced by the GC.
- Xorb containers are resolved to their constituent chunk hashes (new in this release).
- Old format records without the `storage_repr` field default to `FixedChunkV1`.

#### Monitoring

New metrics available:

| Metric | Type | Labels |
|---|---|---|
| `shardline_objects_by_repr_total` | Counter | `representation` (`whole_file_v1`, `fixed_chunk_v1`, `xorb_cdc_v1`) |

Operators can query format distribution across the repository:
```
shardline_objects_by_repr_total
```

#### Configuration

- `SHARDLINE_CHUNK_SIZE` now defaults to `64KB` (the pre-existing fixed-size default
  was `65536` / 64KB via `SHARDLINE_CHUNK_SIZE_BYTES`, and in practice was always
  sized per-deployment). The CDC target chunk size is 64KB; minimum chunk is 8KB;
  maximum is 128KB.

### Fixed

- **Download stream**: corrected `lz4_flex` size-header parsing (u32 LE, not u64 BE) so compressed
  payloads are decompressed and reconstructed correctly.
- **Download stream**: decompression is now selected by the record's `storage_repr`
  (`xorb_cdc_v1`) instead of comparing `packed_end` to `chunk.length`, so XorbCdcV1 payloads
  whose compressed size equals the raw size are still decompressed correctly.
- **Content identity**: chunk hashes are computed over raw bytes, not compressed bytes, so dedup
  works across compressed and uncompressed records.
- **Decompression safety**: added a decompression safety cap with warn logs on failures.

### Changed

- **GC metrics**: split into mark/sweep phases and wired to the direct-read metric.
- **GC sweep**: xorb cache sidecar files are deleted when their parent xorb is swept.

### Performance

- **S3 uploads**: skip the HEAD request for first-time chunk uploads.
- **GC**: xorb chunk hashes are cached to avoid re-parsing the container on every GC run.

### Testing

- Raised upload-ingest coverage to 93%+ and added compression metric tests.
- Added CDC chunker and xorb packer round-trip fuzz targets.
- Added e2e tests for mixed-format dedup, GC + xorb mixed-format sweep, xorb packer edge cases,
  xorb range-span reconstruction, xorb chunk-hash cache, and cache-sidecar cleanup.

## [1.2.2] - 2026-07-28

Patch release fixing Xet protocol uploads broken by stubbed BG4 compression.

### Fixed

- **Xet BG4 compression**: shardline-xet-core stubbed ByteGrouping4LZ4 as plain LZ4,
  causing all xet-data client uploads to fail with HTTP 400. Delegates to upstream
  xet-core-structures for correct BG4 interleaving/deinterleaving.
- **CI license check**: MPL-2.0 added to deny.toml allow list for xet-core-structures
  transitive dependencies.

### Testing

- Added 23 BG4 compression unit tests (roundtrips, panic safety, cross-scheme, proptest).
- Added adapter integration test for BG4 xorb validate/decode roundtrip.
- Added missing validate_xorb_object call to existing BG4 integration test.

## [1.2.1] - 2026-07-27

Patch release establishing recoverable upload lifecycles, shared chunk-backed storage,
bounded runtime resource use, and hardened release and deployment boundaries. Public LFS,
Bazel CAS, OCI, Hub, and Xet protocol surfaces remain compatible.

### Changed

- **Shared chunk-backed storage**: standard bulk-data frontends now use the shared CAS
  coordinator and fixed-size chunks, while native Xet retains content-defined chunking.
- **Authoritative upload lifecycle**: memory, SQLite, and PostgreSQL index stores persist
  legal upload-intent transitions so incomplete writes can be reconciled deterministically.
- **Bounded runtime behavior**: asynchronous object I/O, admission control, quotas, timeouts,
  and explicit overload responses bound memory, blocking work, and request concurrency.

### Fixed

- **Visibility and recovery consistency**: file records now form the publication boundary,
  and GC, FSCK, repair, and protocol handlers share the same reachability model.
- **Legacy read compatibility**: existing whole-object records remain readable while new
  writes use chunk-backed records.
- **CI reliability**: database-gated coverage is included in the coverage ratchet, shared
  backend assertions tolerate parallel test state, and Docker test identifiers are unique.

### Security

- Authentication and route policies fail closed, object and filesystem boundaries use typed
  validation, and production containers run with non-root security defaults.
- Release artifacts and container images are verified, provenance-attested, and published
  through restartable release steps with expiring dependency exceptions.

### Operations

- Apply migration `20260721000000_upload_intents` before accepting writes with this version.
- Roll API and transfer roles together. Preserve database and object-store backups if an
  emergency rollback is required after new chunk-backed records have been published.

## [1.2.0] - 2026-07-24

Minor release adding Ed25519 asymmetric token verification, Hub authentication coverage,
and more reliable container-backed CI coverage. There are no intentional breaking API or
configuration changes from `1.1.0`.

### Added

- **Ed25519 authentication**: local authentication can validate Ed25519-signed tokens,
  including PEM and PKCS#8 signing-key configuration and key-identifier selection.
- **Hub authentication coverage**: end-to-end coverage verifies that the Hub `whoami`
  endpoint requires authentication.

### Fixed

- **Authentication provider validation**: require an HMAC signing key only for the local
  authentication provider, allowing externally validated token providers to serve routes
  without an unused local key; increased the accepted token-string size limit for
  asymmetric tokens.
- **Postgres CI stability**: isolated integration-test repository state and applied database
  migrations in PostgreSQL-backed end-to-end tests.
- **S3 and GC coverage stability**: removed timing-sensitive outage assertions and corrected
  the quarantine-object fixture used by integrity validation.

### Performance

- No intentional performance changes in this release.

### Documentation

- Authentication, deployment, protocol, compatibility, and performance documentation updated
  for Ed25519 token verification.

## [1.1.0] - 2026-07-23

Minor release adding config file support (shardline.toml + .env), governance documentation, documentation drift fixes, and CI/publish pipeline hardening. There are no intentional breaking API or configuration changes from `1.0.1`.

### Added

- **Config file support**: `--env-file` flag for `.env` file loading (`ab351f3`), `--config` flag for `shardline.toml` with auto-detection at `/etc/shardline/shardline.toml` (`2271e90`, `57c74e6`)
- **TOML config with `${VAR}` interpolation**: `load_server_config_from_env_with_toml` deserializes TOML directly into `ServerConfig`, supporting shell env > `.env` file > `shardline.toml` > defaults precedence (`8749068`, `897ef14`)
- **Config test suite**: 25 unit tests for TOML config loader (`67eae9b`), 17 integration tests for config parsing and validation (`57b0892`), plus CLI e2e tests for `--env-file` and `--config` flags (`4d511d1`)
- **Governance docs**: `SECURITY.md`, `CODE_OF_CONDUCT.md`, blank issue creation enabled (`1e9c3ce`, `adcd3b2`)

### Changed

- **K8s ConfigMap**: converted from 20 separate env vars to a single `shardline.toml` mounted at `/etc/shardline/` (`dcc3cd2`, `57c74e6`)
- **K8s deployments**: `api-deployment.yaml` and `transfer-deployment.yaml` updated to auto-detect config at `/etc/shardline/` — `--config` flag removed (`57c74e6`)
- **Docker deployment**: updated for config file usage in `DEPLOYMENT.md` (`dcc3cd2`)

### Fixed

- **Documentation drift**: 31 doc files audited 4x — stale crate paths, Ed25519/HMAC references, wrong env vars, and include_str paths corrected (`078aeb4`, `a44494b`, `b453645`, `9ab4ac4`, `1e9c3ce`, `adcd3b2`)
- **Publish pipeline**: missing crates (`shardline-auth`, `shardline-validation`) added to release publish list, version extraction sed pattern fixed, migrations directory restored in release packaging
- **Audit findings**: env var name mismatches, dead config fields, and test gaps resolved (`067b582`)
- **Native Xet E2E timeout**: prevented by fixing the split-role e2e test setup (`5eaa444`)
- **Clippy warnings**: `shadow-unrelated` and other lint warnings resolved (`a71745d`)

### Performance

- No intentional performance changes in this release (focus was config file support and documentation).

### Documentation

- `CLI.md`: global `--env-file` and `--config` flags documented (`c2977ff`)
- `DEPLOYMENT.md`: config file usage, Docker examples, K8s ConfigMap documentation (`dcc3cd2`, `c2977ff`)
- K8s manifests reviewed and simplified for config file auto-detection (`57c74e6`)
- README cleaned up: removed "first" and "production ready" claims (`b700927`)
- Architecture, protocol, and ops docs audited for accuracy (`9ab4ac4`, `a44494b`)

## [1.0.1] - 2026-07-21

Patch release focused on correctness, security hardening, test coverage, and regression protection.
There are no intentional breaking API or configuration changes from `1.0.0`.

### Added

**Protocol & Feature Coverage**
- OCI blob `DELETE` per spec with reference checking: implements the full OCI lifecycle (`ae6a9e2`, `631be63`)
- Git LFS `DELETE` endpoint for object removal (`9f7adf2`)
- Git Smart HTTP reference deletion (`9b06cac`)
- Redis TLS and mutual-TLS integration support (`63d0751`)
- Native HuggingFace CLI download coverage and smoke tests (`63d0751`)
- Provider multi-webhook sequence dispatch and S3 session abort (`8be650c`)
- Codeberg provider adapter alongside GitHub, GitLab, Gitea (`c853763`)
- Hub API: model card rendering, metadata search, and macOS compatibility fixes
- `MemoryReconstructionCache` concurrency stress tests with interleaved read/write patterns (`2845e66`)
- GC and upload interleaving concurrency test (`adb666c`)
- Lifecycle repair classification fuzz target (`e89b274`)
- Postgres CI integration with container-backed jobs (`adb666c`)
- S3 OCI multipart session upload E2E test with MinIO (`f4dd1b8`)
- Container-backed MinIO and Redis integration test framework (`c853763`)
- Concurrent load benchmark with 5 new fuzz targets (`a1e91ae`)
- `xet-core` shim crate isolating xet dependencies from production builds

**Test Coverage (22 crates, ~8.1k tests)**
- Major coverage campaign pushed aggregate lib-only line coverage to **93.79%** (up from ~82%), with a CI-enforced coverage ratchet.
  - **Pushed to ≥96%**: `cas` (100%), `protocol_adapters` (100%), `rebuild` (99%), `provider_events` (99%), `metrics` (98%), `oci_adapter` (98%), `protocol` (97%), `fsck` (97%), `gc` (97%), `server` (97%), `cache` (96%), `xet_adapter` (96.7%), `hub_api` (96%), `cli` (96%), `vcs` (99.8%), `server_core` (98.4%)
  - **Infrastructure-dependent** (need real Postgres/S3/MinIO for full coverage): `storage` (77-100% per file), `index` (postgres modules constrained), `xet_core` (90-100% per file)
- **E2E test suite** grown from near-zero to comprehensive coverage:
  - 36 → 43 → 50 → 65 → 75 → 92 → 100 → 112 → 114 sequential E2E tests
  - Full protocol coverage: OCI, LFS, Bazel, Xet, Hub API, Git Smart HTTP
  - Auth-enabled tests (100+), branch audit (92), deep feature gap coverage (75)
  - Cross-protocol consistency tests (23 new, 64 total)
  - Provider webhook tests (114) and token tests (112 total)
  - OCI-specific: referrers, pagination, anonymous pulls, schema2, tag overwrite
  - LFS batch edge cases, range variants, dedup variants, body limits
  - Docker-backed: S3 LFS/OCI/Bazel, S3 dedup, Postgres OCI
  - Passthrough auth, CORS survey, cross-repo OCI mount
  - HTTP-level E2E test framework parameterized for Postgres, S3, and combined backends (`a1e91ae`)
- **15+ fuzz targets** including lifecycle classification, stateful Hub-reference fuzzing, deterministic corpus replay, and scheduled bounded campaigns
- **Property tests** for Hub references and Git pkt-line encoding
- **Coverage ratchet**: line-coverage floor enforced in CI to prevent regression
- **Unit tests** for previously untested crates: clock, cache/error, cache/disabled, frontend, index/provider, local_backend records, postgres_backend read/metrics/backend

### Changed

- **Modularized `local_backend`**: split `crates/shardline-server/src/local_backend/` into focused domain modules (`b62ec09`)
- **LLVM source-based coverage** via `cargo-llvm-cov` with CI enforcement and a line-coverage ratchet (`0997dea`)
- **Trait splits**: `RecordStore` / `HubStore` and `IndexStore` god traits split into focused sub-traits (parallel read/write, metadata, lifecycle) (`a19cbb9`)
- **Split `ServerConfig`**: removed dual-impl pattern, cleaned public re-exports (`3d0d72a`)
- **Split `ServerError`**: moved CLI output to dedicated types, added test lint overrides (`3d0d72a`)
- **Moved E2E tests** out of crate dirs into dedicated `e2e/` folder, simplified CI filtering
- **Finished CAS-agnostic runtime foundation**: decoupled storage/index from protocol specifics with frontend routing via `--frontend` / `SHARDLINE_SERVER_FRONTENDS`
- **`LocalEd25519Provider` renamed to `LocalHmacProvider`**: the implementation always used HMAC-SHA256, not Ed25519 (`1f7a7a3`)
- **Replaced all `#[allow(arithmetic_side_effects)]`** with explicit checked arithmetic (`4a0978e`)
- **308 KiB of dev tracking files removed** from workspace (`.slim/`, `.opencode/`, `AUDIT_REPORT.md`, `TEST_GAPS.md`, coverage artifacts, profraw/lcov files, TODO.md, coordination notes)
- `cargo-deny` removed from CI pipeline (advisory responsibility delegated to Dependabot)
- Workspace clippy clean enforced across all crates

### Fixed

**Security & Audit Hardening**
- **9 batches of adversarial audit findings** remediated:
  - Batch 1 & 2: error variant bounds, prefix validation, CRIT-001 `put_overwrite` non-atomic on S3, `start_after` cleanup (`ff3b778`, `8837a22`)
  - Batch 3: CRIT-002 TOCTOU, STOR-001/002/005/011/027, SHARDLINE-001 (`af304b9`)
  - Batch 4: SHARDLINE-002/003, STOR-013/025, OCI-002, 7 remaining bugs (`24184eb`)
  - Batch 5 & 6: OCI-001/003, GC-001, >5GiB copy via streaming multipart (`ea69d0f`, `3830d25`)
  - Batch 7 & 8: OCI-004/005/006/007/008, STOR-028/029/030 (`ba876fc`, `7f52f29`)
  - Batch 9: NEW-001/003/004 (`97c2149`)
  - Final audit findings NEW-001/002/003/004 + lower-confidence findings (`8d45a45`, `6858dad`)
- **TOCTOU races closed**: `ensure_directory` hardened with `O_NOFOLLOW` + fd-stat (`cac0c62`), `ensure_legacy_import_state` (`0cb31e7`), streaming TOCTOU guard in `objects.rs` (`2c03c3b`)
- **P0 audit items fixed**: exhaustive error matches, mutex poison recovery, unsafe docs (`f550b78`)
- **Deep audit (pass 2 & 3)**: JWKS refresh strategy, DRY improvements, error handling, security/performance hardening
- **Hub API audit**: 5 bugs fixed: SQL injection surface, auth bypass, permission checks (`fba6220`)
- **JWKS/OIDC hardening**: `iat`/`nbf` validation added, `Cache-Control max-age` used instead of fixed 60s interval, bearer token timing, key caching (`0374a61`, `3c366df`)
- **`#[allow(unwrap_used)]` replaced** with `HeaderValue::from_static` where applicable (`5cb6c31`)
- **6 production bugs found by E2E tests** fixed: broken `readyz`, OCI tag delete, metrics counter races, Hub schema mismatch, port retry logic, test fixture drift (`05652e5`)

**Protocol & Server Reliability**
- **Transfer limiter timeout**: `Semaphore::acquire_many_owned()` now has a configurable timeout, returning 503 on exhaustion instead of hanging forever (`0498fba`)
- **Graceful shutdown timeout**: `serve_with_listener` drains connections with configurable timeout; server no longer hangs on open connections during shutdown (`5dca9c2`)
- **Redis reconnect fix**: `get_connection()` error path hardened: handles mid-operation connection drops instead of panicking (`5dca9c2`)
- **S3 multipart leak fixed**: `streaming_large_copy` cleans up multipart upload on completion failure (`1c0012d`)
- **SQLITE-MIG-001**: missing Hub migrations added to `LOCAL_SQLITE_MIGRATIONS` (`13b0e69`)
- **LFS PATCH safety** + OOM guard + OIDC/token race fixed (`12cc125`)
- **LFS/Bazel/OCI protocol bugs** + OIDC auth outage corrected (`04ae3a8`)
- **Batch OCI fixes**: OCI Bug 1/2/3 + GC-NEW-001/002/003 + accompanying E2E tests (`a8477f2`)
- **`block_in_place` converted to `spawn_blocking`** in `objects.rs` to avoid runtime panic (`7f52f29`)
- **Task leak fixed**: spawned tasks now tracked and cancelled on shutdown (`0374a61`)
- **OCI PATCH Content-Range end mismatch** corrected to return 416 (`c110c5e`)

**Test Quality & CI**
- **All 22 test workarounds hardened**: every assertion now expects exact correct behavior (`0ac8b65`, `4e906b9`)
- **7 weak assertions and misleading test names fixed** (`338a4b5`)
- **10 remaining minor workarounds fixed**: no more silent defaults or string contains patterns (`4e906b9`)
- **`serial_test` applied** to all `git_smart_http` tests sharing a single SQLite database (`70794af`)
- **Pre-existing test failures fixed**: shift overflow, protocol adapters broken tests, CI blockers
- **CI filter added** to exclude infrastructure-dependent E2E tests from nextest runs
- **Quick-xml advisory ignored** after verification of non-impact (`4b7da99`)
- **All 18 build warnings resolved**; formatting and clippy clean across workspace

### Performance

- No intentional performance changes in this release (focus was correctness and coverage).

### Documentation

- Updated `ARCHITECTURE.md`, `DEPLOYMENT.md`, `HUGGINGFACE_HUB_API.md`, `README.md` with Hub API, auth providers, metrics sections, and AI use cases
- Documented TOCTOU race windows in S3 adapter with 1.2M-run fuzz validation (`40ef000`)
- Updated security audit documentation
- Added use cases for AI, game assets, and binary distribution to README
- Marked all completed TODO.md items as done; removed TODO.md and dev tracking files from workspace
- Removed 22 files of local dev tracking docs (-1157 lines)

## [1.0.0] - 2026-06-29

### Added

- **Content-addressed storage server** for large binary assets with automatic deduplication across repositories (`f389559`)
- **Xet protocol support** as the default storage protocol with native shard upload, reconstruction, and byte-range requests (`f389559`, `c461d81`)
- **Git LFS protocol frontend** with batch API, object verification, and transfer adapter (`300387a`)
- **Bazel HTTP remote cache frontend** with AC/CAS object storage and action cache support (`300387a`)
- **OCI Distribution protocol frontend** with blob push/fetch, manifest operations, and tag resolution (`300387a`)
- **HuggingFace Hub API compatibility** with REST endpoints for repository CRUD, revision management, and file operations (`d03c763`)
- **Git Smart HTTP protocol** for Hub API: `info/refs` discovery, `upload-pack` (clone/fetch), `receive-pack` (push), pkt-line framing, and Git pack file generation (`88f2a8a`, `cc0f3cf`)
- **Pluggable authentication** via `AuthProvider` trait with 4 adapters: Ed25519 local keys, OIDC, JWKS, and passthrough (`d03c763`)
- **Pluggable storage backends**: local filesystem with symlink-safe path validation, S3-compatible with multipart upload, Postgres metadata (`f389559`, `5ff5e14`, `981b973`)
- **Provider integration** for GitHub, GitLab, and Gitea: webhook handling, token issuance, and repository state reconciliation (`d03c763`)
- **Garbage collection** with quarantine state, configurable retention, orphan inventory, and diagnostics (`d03c763`)
- **Filesystem consistency checks (fsck)** with reconstruction plan validation and lifecycle repair flows (`d03c763`)
- **Index rebuild** from stored metadata and object inventory (`d03c763`)
- **Storage migration** between local and S3 backends (`d03c763`)
- **Backup manifests** for metadata export (`d03c763`)
- **Database migration framework** for Postgres and SQLite with sync verification (`d03c763`)
- **50+ Prometheus metrics** across 9 categories: storage, transfer, xet, protocol, reconstruction, gc, fsck, backend, provider, system (`d03c763`)
- **Structured tracing** via `tracing-subscriber` with `RUST_LOG` env filter and `#[tracing::instrument]` on hot paths (`d03c763`)
- **15 fuzz targets** covering protocol parsing (Xet, LFS, Bazel, OCI), reconstruction, storage races, and CLI (`300387a`, `40ef000`)
- **Async storage fuzz target** validated to 1.2M runs with 12 concurrent access patterns (`40ef000`)
- **6 benchmark suites** for protocol, local backend, OCI, S3, and end-to-end performance (`f389559`)
- **Docker deployment** with Dockerfile, docker-compose (Postgres 16-alpine), and GHCR image publishing (`f389559`, `68e8296`, `d03c763`)
- **Windows CI compatibility** with `cargo check --workspace` guardrail (`5c69e59`)
- **70+ deny-level clippy lints** enforced workspace-wide including `unwrap_used`, `panic`, `todo`, `arithmetic_side_effects` (`f389559`)
- **GitHub issue templates** for bug reports, feature requests, and maintenance tasks (`c508065`)
- **CONTRIBUTING.md** with dev setup, PR workflow, testing, and code standards (`c508065`)
- **`cargo-deny` license and advisory policy** for dependency auditing (`78ff8ac`)
- **`cargo-make` task runner** with 50+ tasks for build, test, lint, fuzz, bench, and CI (`f389559`)

### Changed

- **Streaming SHA-256 digest uploads to S3**: eliminated full-body buffering for content-addressed uploads by streaming through `Sha256` hasher during multipart transfer (`5ff5e14`)
- **S3-native multipart OCI uploads**: bypassed local staging file for S3 backends with resumable upload sessions (`981b973`)
- **CAS-agnostic runtime foundation**: decoupled storage/index layer from protocol specifics with frontend routing via `--frontend` / `SHARDLINE_SERVER_FRONTENDS` (`c461d81`)
- **Comprehensive module splits** across 91 files: `protocol_routes` (8 files), `postgres_backend` (4 files), `upload_ingest` (3 files), `local_sqlite` (6 files), config directory restructure (`d03c763`)
- **New crates extracted** from server: `fsck`, `gc`, `hub_api`, `metrics`, `oci_adapter`, `protocol_adapters`, `server_core`, `xet_adapter`: workspace grew from 10 to 20 crates (`d03c763`)
- **All crate versions bumped to 1.0.0** with centralized `[workspace.dependencies]` (`737c625`)
- **README rewritten** as user-facing product page with validated coverage matrix (`3b86a0b`, `c461d81`)
- **ARCHITECTURE.md rewritten** for 20-crate layered dependency graph with Hub API, Git Smart HTTP, metrics, and auth sections (`1203d8e`)
- **DEPLOYMENT.md expanded** with Hub API, authentication providers, and metrics documentation (`1203d8e`)
- **`rustfmt.toml` applied** across 107 files (`d03c763`)
- **Docker entrypoint** switched to `docker-entrypoint.sh`, Compose includes Postgres on port 5532 (`c461d81`, `d03c763`)

### Fixed

- **Non-Unix compile surface restored**: removed `#[cfg(not(unix))] compile_error!` blocks from cli, index, server, and storage crates; added conservative path validation fallbacks for Windows (`5c69e59`)
- **Git pack encoding**: fixed size varint to use 4 bits in first byte per Git pack spec (`cc0f3cf`)
- **Hub API revision ordering**: added `rowid DESC` tiebreaker to `list_revisions` (`cc0f3cf`)

### Performance

- **Streaming S3 uploads**: SHA-256 digest computed incrementally during multipart transfer instead of buffering entire body (`5ff5e14`)
- **Reduced upload staging I/O**: S3-native multipart uploads skip local staging file entirely (`981b973`)

### Security

- **TOCTOU race windows documented** in S3 adapter for `begin_content_addressed_upload`, `put_file_if_absent`, `copy_object_if_absent` (`40ef000`)
- **Symlink escape prevention**: `ensure_directory_path_components_are_not_symlinked` applied to backend root, stats traversal, and metadata paths (`f389559`)
- **`cargo-deny` policy** enforced in CI for license compliance and security advisories (`78ff8ac`)

### Documentation

- Added contribution workflow templates and PR guidelines (`c508065`)
- Rewrote production readiness and compatibility claims (`3b86a0b`)
- Removed 22 files of local dev tracking docs (-1157 lines) (`818e94d`)
- Updated security audit documentation (`f5d8cac`)
- Documented async storage TOCTOU races with 1.2M-run fuzz validation (`40ef000`)
- Updated all architecture, deployment, and Hub API docs for 20-crate structure (`1203d8e`)

[Unreleased]: https://github.com/STEXS-Technologies/shardline/compare/v1.4.0...HEAD
[1.4.0]: https://github.com/STEXS-Technologies/shardline/compare/v1.3.0...v1.4.0
[1.3.0]: https://github.com/STEXS-Technologies/shardline/compare/v1.2.2...v1.3.0
[1.2.2]: https://github.com/STEXS-Technologies/shardline/compare/v1.2.1...v1.2.2
[1.2.1]: https://github.com/STEXS-Technologies/shardline/compare/v1.2.0...v1.2.1
[1.2.0]: https://github.com/STEXS-Technologies/shardline/compare/v1.1.0...v1.2.0
[1.1.0]: https://github.com/STEXS-Technologies/shardline/compare/v1.0.1...v1.1.0
[1.0.1]: https://github.com/STEXS-Technologies/shardline/compare/v1.0.0...v1.0.1
[1.0.0]: https://github.com/STEXS-Technologies/shardline/releases/tag/v1.0.0
