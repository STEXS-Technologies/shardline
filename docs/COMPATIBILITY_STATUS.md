# Compatibility Status

Shardline has a CAS-agnostic runtime with explicit protocol frontends.
The current compatibility contract is scoped to the protocol and operator workflows
documented in this repository.
Promotion between maturity tiers follows the checked-in evidence requirements in
[Stability Graduation Policy](STABILITY_GRADUATION.md).

## Current Source-Tree Contract

The current Shardline source tree should be treated as a content-addressed backend for
the validated workflows covered by this repository.
Release notes identify which source-tree capabilities are included in each published
version.

## Surface Maturity

| Surface | Tier | Evidence |
| --- | --- | --- |
| Xet CAS | **Stable** | Native Xet upload and download flows; checked-in `git` + `git-lfs` + `git-xet` push, clone, fetch, pull, historical checkout, and sparse checkout flows exercise Shardline CAS, while the Git/Xet test supplies a mock forge LFS control plane. CAS batch reconstruction accepts the published JSON `POST /v1/reconstructions` request while retaining Shardline's older GET form. When Hub and Xet frontends are enabled together, Hub owns overlapping `/api/.../tree` and `/api/.../revisions` routes, so native Xet metadata routes at those paths are omitted; direct CAS transfer routes remain available. |
| Git LFS | **Beta** | LFS batch negotiation plus direct object GET/HEAD/PUT routes; pinned Git LFS 3.7.1 and adjacent 3.6.1 push/pull plus separate pull and fetch --all flows; conformance-tested but limited production evidence |
| Bazel HTTP remote cache | **Beta** | `ac` and `cas` object GET and PUT routes; pinned Bazel 8.7.0 and 9.2.0 remote-cache flows with `remote_download_outputs=all` and `toplevel`; conformance-tested but limited production evidence |
| OCI Distribution | **Stable** | Full blob upload/download, manifest PUT/GET/HEAD/DELETE, tag listing with pagination, token-service flow at /v2/token, upload cancellation, scoped upload-session handling; checked-in `skopeo`, Docker, Helm, and Podman client coverage |
| Hugging Face Hub API | **Beta** | Repository create/info/delete, revision and tree lookup, size-aware preupload, NDJSON commit, resolve/download, dataset viewer routes, basic search, webhooks, Git Smart HTTP clone/fetch/push, and LFS transfers. The native `hf` CLI E2E covers small model and dataset files plus 9 MiB `hf_xet` uploads, Hub commits, and byte-for-byte downloads for both repository types. It verifies Xet shard and Xorb transfer, no ordinary LFS body-upload fallback, and repository-scoped SHA-256 to native Xet file mappings. The flows pass with Hub enabled alone and with Hub, Xet, and S3 enabled together. The Xet-token response includes current JSON connection fields and matching headers. The separate git-xet E2E uses a mock forge LFS control plane while exercising Shardline CAS. Git Smart HTTP supports protocol v2 upload-pack (`ls-refs` and `fetch`) plus the traditional v0/v1 upload-pack and receive-pack flow; receive-pack supports negotiated `push-options` but has no Git receive hooks, and protocol v2 push remains unsupported. |
| S3 frontend (protocol) | **Stable** | Scoped object, listing and multipart workflows documented in [S3_FRONTEND.md](S3_FRONTEND.md), not the entire AWS S3 service. Existing access-key-as-token and Bearer credentials remain supported. Optional configured signing identities verify SigV4, session credentials, presigned GET/PUT, signed browser PostObject policies, classic signed payload streams, and signed checksum trailers. Trailer coverage is qualified with the documented MinIO client version; broader client-version and remote-failure qualification remains. Extended service APIs remain unimplemented as listed in the S3 guide. Native client coverage includes `mc`, AWS CLI, boto3, `s3cmd`, `rclone`, and pyarrow 25. |
| Ed25519 auth provider | **Beta** | Signing and verification, bounded overlapping key rotation, verification-only mode, environment/TOML configuration, authenticated HTTP flows, and operator CLI mint-to-verify interoperability have targeted tests; broader deployment evidence remains graduation work |
| Local filesystem storage | **Stable** | Checked-in adapter, concurrency, and operator workflow coverage |
| S3-compatible storage | **Stable** | Checked-in object read/write/list and HTTP integration coverage |
| Postgres metadata | **Stable** | Checked-in index, dedupe, concurrency, and operator workflow coverage |
| SQLite metadata | **Stable** | Checked-in local single-node and operator workflow coverage |
| Redis reconstruction cache | **Beta** | TLS/mTLS connectivity, bounded per-operation timeout, corrupt-value repair, partition and kill/restart recovery, plus operation-scoped cross-replica cold-load reservations with cancellation takeover and stale-owner fencing are validated; broader production evidence remains |
| Provider integration | **Beta** | Checked-in token issuance, webhook handling, and repository-scoped authorization coverage for GitHub, GitLab, Gitea, Codeberg, and the generic adapter |

### Tier Definitions

- **Stable**: broad checked-in route, integration, and native-client coverage for the
  advertised workflows.
  This tier does not claim a particular deployment's production, load,
  failure-injection, or upgrade history.
- **Beta**: checked-in route or client coverage exists, but the compatibility surface or
  operational evidence is narrower.
- **Experimental**: implemented with targeted tests, but configuration or
  interoperability may still change.
- **Internal**: architectural component, not a user-facing promise.

### Client Test Execution

The Hub API's native Git regression compares incremental-fetch HTTP response
bytes with a fresh clone at the same commit. It requires less transfer for the
incremental fetch, exact current and historical file contents, a successful
fast-forward pull, and strict Git object checks. This small fixture does not
establish large-repository throughput or concurrent resource limits.

Git request buffering, history projection and pack generation share the existing
two-slot identity work limit. Blocking work retains its slot after request
cancellation. Download responses retain the slot until their body is consumed
or dropped, handing data to HTTP in frames of at most 64 KiB. Existing request,
projection byte and object limits remain in place. This bounds admitted Git work
and pack buffers; it is not a total process RSS or connection-count guarantee.
Git advertises its supported legacy smart-HTTP capabilities, including v1 and
v2 push status reports and receive-side `side-band-64k`. Clients requesting
sideband receive the status stream through channel 1; other clients retain the
plain status format. Native Git checks verify negotiation, ordinary pushes,
branch deletion and rejected default-branch deletion. Ref names and object IDs
are not rewritten through proc-receive hooks, so v2's optional rewrite details
are absent. SQLite and PostgreSQL support negotiated atomic pushes: payloads
and trees are prepared first, then all refs and their reliability evidence
commit in one metadata transaction. A conflict or preparation/storage failure
rejects every requested ref. Native Git checks cover multi-ref creation,
updates, deletion and protected-ref rejection. Separate native HTTP workflows
run through ordinary server startup on SQLite/local, PostgreSQL/local and
PostgreSQL/MinIO storage, then verify a fresh clone, historical file bytes and
strict Git fsck after managed restart. The PostgreSQL workflows terminate the
observed publication session before its second ref write, verify both old heads
and the default branch remain unchanged, then retry. All three backends also
drop the client status after observing a successful server commit, verify both
new heads and retry through the normal endpoint. A PostgreSQL/MinIO deployment
drill also kills the Shardline process while the second ref is paused inside the
atomic transaction, verifies both refs remain at their old heads, then restarts,
retries, clones and runs strict `git fsck`. Broader concurrent deployment
qualification remains separate.
Custom stores advertise atomic support
only when they implement the batch contract. Receive-pack negotiates and
validates push-options framing before publication. Shardline has no Git receive
hooks, so those option values currently have no hook consumer. Unimplemented
response modes remain rejected before publication.
Depth-limited clone, relative deepening, date-based and exclusion-based shallow
fetch, and unshallowing preserve verified commit
bytes. Missing or mistyped parent commits fail before depth boundaries are applied.
A native Git workflow checks history counts, complete-history identity and
strict fsck after these operations, including an earlier date cutoff restoring
full history. Date selection uses committed committer timestamps. Exclusions
resolve repository branch/tag names, reject unknown or ambiguous names and omit
their ancestry. Native Git checks exclusion by baseline tag and complete-history
recovery. Partial clones support `blob:none`, `blob:limit=<bytes>[kmg]` and
`tree:<depth>` using the minimum depth across selected history and explicit roots.
Native Git checks omitted initial blobs, size limits, lazy checkout and historical
content recovery. Explicit object requests must belong to the verified reachable
graph of the authorized repository. Native tree-depth clones verify directory
omission and lazy checkout of nested files. Protocol-v2 upload-pack now supports
`ls-refs` and `fetch`, including shallow depth/date/exclusion and the advertised
`blob:none`, blob-size and tree-depth filters, including bounded combined
blob-size/tree-depth intersections. `object:type` filters select blobs, trees,
commits or tags; explicitly wanted objects remain available, and native Git 2.52
verifies lazy fetching of an omitted tree during checkout over protocol v0, v1
and v2. Native Git checks v2 clone, incremental fetch, deepen/unshallow, shallow
date/exclusion, omitted objects, lazy checkout and strict fsck. Sparse-checkout
and other unimplemented filters are rejected explicitly. Broader shallow-history merge, backend and
failure qualification remains open; these checks do not establish full Git
protocol coverage.
Receive-pack validates the entire command section before publication: malformed
framing, invalid object IDs/ref names, duplicate ref commands and misplaced
capabilities reject the request rather than applying only its valid commands.

The E2E workflow provisions Git LFS, git-xet, Bazel, Hugging Face tooling, S3 clients,
Skopeo, Helm and Podman, and checks the Docker and Podman runtimes. Its native
protocol, git-xet, native Xet and Hugging Face CLI targets use
`SHARDLINE_REQUIRE_NATIVE_CLIENTS=1`: missing
required tools or native Xet's Docker/MinIO backend fail those tests.
Local runs without this flag retain optional-tool
behavior. Other E2E targets still need an individual skip audit. Review actual
execution logs before treating a specific revision as qualified; workflow changes
alone are not a passing CI result.

The default contributor library run leaves eighteen explicitly ignored tests:

| Checks | Where they run explicitly |
| --- | --- |
| Four PostgreSQL OCI subject-index and reclamation contracts | Protocol-conformance gate with a migrated disposable database |
| PostgreSQL corrupt-copy preservation and startup-worker shutdown | Protocol-conformance gate with that database |
| Six PostgreSQL frontend-schema compatibility and permission contracts | Protocol-conformance gate with that database |
| Six full-size or legacy maintenance-history capacity drills | Explicit release-mode capacity campaigns; ordinary CI does not establish their result |

The E2E job separately selects the ignored native Xet PostgreSQL/MinIO test and
the two Bazel flows. Kubernetes and kind tests still require their dedicated
deployment fixtures. The live GitHub/Gitea bridge test is explicitly ignored by
default because it requires external credentials and test repositories. Selecting
it explicitly requires its clients and complete configuration; missing prerequisites
or failed identity lookups fail the test. A default E2E pass does not qualify that live integration.
Likewise, optional developer runs without the required-client flag can omit
tool-dependent flows. Keep these limits separate from executed native-client
and disposable-backend results.

The required protocol-conformance step separately runs native Git LFS locking and
transfers and the official OCI Distribution v1.1.1 suite. It fails if its client
tools are missing. PostgreSQL contracts also exercise lock races across two
servers and referrer discovery after restart and deletion.

## Versioning and Deduplication Semantics

The shared CAS core stores immutable content and reuses identical content whenever a
new upload refers to it. That does not mean every frontend exposes the same version
history. Version visibility and overwrite behavior belong to the frontend contract:

| Frontend | User-visible version semantics |
| --- | --- |
| Xet | Revision-oriented workflows can address and retrieve older versions. |
| Git LFS | Version history comes from Git commits and refs; the LFS object endpoint alone is a blob store. |
| Hugging Face Hub | Repository revisions and commits expose historical states. |
| OCI | Blobs and manifests are immutable by digest; tags are mutable, database-linearized pointers and have no tag-history API here. Digest-guarded deletion cannot remove a concurrently retargeted tag. |
| S3 | A `PUT` replaces the logical object key. S3 bucket versioning and `ListObjectVersions` are not implemented. |
| Bazel HTTP cache | Digest-keyed cache entries have no user-facing version history. |

When an overwrite makes a prior record unreachable, its unique chunks become GC
candidates after the configured quarantine/retention period. Chunks shared by another
reachable record remain protected.

## Validated Route Surface

- Git LFS: batch negotiation plus direct object `GET`, `HEAD`, and `PUT`;
  durable lock create/list/verify/unlock at `/v1/lfs/locks`
- Bazel HTTP remote cache: `ac` and `cas` object `GET` and `PUT`
- OCI Distribution: blob `GET`, `HEAD`, upload, mount, and ranged read paths; manifest
  `PUT`, `GET`, `HEAD`, and digest delete; tag listing with pagination; token-service
  flow at `/v2/token`; upload cancellation and scoped upload-session handling;
  OCI referrers with artifact-type filtering and pagination. SQLite and PostgreSQL
  HTTP tests cover deletion followed by identical republication, duplicate upload
  retries, discovery after restart, missing lookup metadata and corruption rejection.
  A PostgreSQL/MinIO test covers concurrent identical uploads through two live
  servers and replacement with fresh local directories. The servers share one
  test process; this does not simulate a process crash.
  Physical-reclamation tests on SQLite/local storage and PostgreSQL/MinIO verify
  that GC removes a deleted manifest while preserving live manifests and their
  shared subject, remains idempotent, and permits republication after restart.
  The MinIO case kills separate GC workers between manifest and companion
  deletion, then after both acknowledged deletions but before metadata commit;
  it terminates the sleeping database sessions, then
  verifies recovery and restart with a fresh local directory. This covers that
  crash boundaries. A separate MinIO fault drill lets the provider complete a
  delete but drops its response: GC reports failure, retains the tombstone and
  exact-key delete fence, blocks republishing, and does not replay the uncertain
  delete. The S3 API provides no proof that would permit safe in-place recovery
  for this case; follow the isolated-restore procedure in
  [DISASTER_RECOVERY.md](DISASTER_RECOVERY.md#unprovable-remote-deletes).
  An additional crash phase observes a replacement HTTP upload waiting on the
  PostgreSQL GC barrier, kills the owner, and verifies that publication succeeds
  and a later GC preserves the exact replacement bytes.
  A durable subject index selects candidate manifests. Each request reconciles the
  immutable manifest inventory so uploads from older servers remain discoverable;
  warm reconciliation reads metadata instead of all unrelated manifest payloads.
  Returned manifests still undergo content verification and deletion visibility
  checks. Lookup metadata does not publish or restore content. This retains an
  inventory scan; it does not establish constant-time lookup or measured throughput.
  PostgreSQL installs the index through the existing owner-run `db migrate up`
  command, preserves released migration history and mirrors existing OCI tag
  read/write grants. Read-only database roles, read-only database sessions and
  schemas without this optional index use verified manifest scanning instead,
  preserving discovery without metadata writes. PostgreSQL serving performs no DDL.
  Completed manifest reclamation removes its derived subject mapping in the same
  metadata transaction. PostgreSQL skips this optional cleanup when the table or
  existing SELECT/DELETE grants are absent; no additional grant is required for
  GC. Legacy inventory reconciliation still protects discovery if a mapping is
  missing. Updated-runtime backfill checks verified lifecycle evidence transactionally and does
  not recreate a mapping after reclamation; PostgreSQL serializes that check with
  derived-row cleanup. Normal republication can index the manifest again.
  Legacy rows and roles without cleanup grants can still retain obsolete entries;
  this is not a guarantee that every orphaned cache row is immediately removed.
- Hugging Face Hub API: repository create/info/delete, revision and tree lookup,
  preupload, NDJSON commit, resolve/download, dataset viewer routes, basic search,
  webhooks, Git Smart HTTP clone/fetch/push, SDK live-reference listing, and SDK
  branch/lightweight-tag creation and deletion
- S3 frontend: object `PUT`, `GET` (with `Range`/`Content-Range` and checksum mode), checksum-enabled `HEAD`, `DELETE`, and
  partial `GetObjectAttributes` (`ETag`, `ObjectSize`, multipart total-part count,
  persisted single-PUT checksums, COMPOSITE part checksums, and CRC32/CRC32C/CRC64NVME
  FULL_OBJECT checksums); active `ListParts` returns the negotiated
  checksum algorithm and per-part checksum values, deriving missing values for older
  in-progress sessions from integrity-checked part bytes;
  multipart upload (`CreateMultipartUpload`, `UploadPart`, `CompleteMultipartUpload`,
  `AbortMultipartUpload`, `ListParts`, and `ListMultipartUploads`); `HeadBucket`/
  `GetBucketLocation`/`CreateBucket` stubs;
  `ListObjectsV2` (`prefix`/`delimiter`/`max-keys`/`continuation-token`/`start-after`)
  and `ListObjects` v1 (`marker`/`NextMarker`, the `s3cmd ls` path); standard MD5
  ETags and user metadata (`x-amz-meta-*`); with the SigV4 access-key and Bearer
  auth bridges

## Current Limits

- Hub SDK commit fields use Git IDs; historical opaque IDs remain accepted.
  The legacy `commit_id` field and `/revisions` listing retain internal IDs.
  SDK response IDs can therefore differ from opaque IDs saved before upgrading;
  resolve an old revision to obtain its corresponding Git ID before comparison.
- The compatibility claim applies to the documented route surfaces and native client
  workflows above. It does not imply implementation of unrelated upstream products or
  optional APIs such as Bazel Remote Execution, Hugging Face
  collections/profile/jobs/inference APIs, or every third-party client-version
  extension.
- Xet and OCI currently have the deepest native-client coverage in this repository.
- Git LFS, Bazel HTTP remote cache, and OCI Distribution claims are scoped to the route
  behavior and client flows covered by the repository tests.
- Patch releases publish the internal crate graph in dependency order before publishing
  the `shardline` CLI crate.
DuckDB analytics compatibility and the real-client validation workflow are
documented in [`DUCKDB.md`](DUCKDB.md). The supported production boundary is
external DuckDB over the authenticated S3 frontend; no arbitrary SQL endpoint
is exposed by the Hub API.
