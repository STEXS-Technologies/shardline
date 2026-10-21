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
| Hugging Face Hub API | **Beta** | Repository create/info/delete, revision and tree lookup, size-aware preupload, NDJSON commit, resolve/download, dataset viewer routes, basic search, webhooks, Git Smart HTTP clone/fetch/push, and LFS transfers. The native `hf` CLI E2E covers small model and dataset files plus 9 MiB `hf_xet` uploads, Hub commits, and byte-for-byte downloads for both repository types. It verifies Xet shard and Xorb transfer, no ordinary LFS body-upload fallback, and repository-scoped SHA-256 to native Xet file mappings. The flows pass with Hub enabled alone and with Hub, Xet, and S3 enabled together. The Xet-token response includes current JSON connection fields and matching headers. The separate git-xet E2E uses a mock forge LFS control plane while exercising Shardline CAS. |
| S3 frontend (protocol) | **Stable** | S3-compatible object API — Put/Get(+Range)/Head/Delete, conditional requests (If-Match/If-None-Match), CopyObject, multipart upload (create/part/complete/abort), ListObjectsV2 + ListObjects v1, DeleteObjects, ListBuckets, and bucket stubs; standard MD5 ETags (single-PUT and multipart-complete) and user metadata (`x-amz-meta-*`) round-trip; access-key-as-token auth with a Bearer auth bridge. Clients use the SigV4 request shape, but Shardline does not verify the SigV4 signature; index-backed listing has zero object-store reads; real-client e2e coverage includes `mc`, AWS CLI, boto3, `s3cmd`, `rclone`, and pyarrow 25. |
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

Some native-client E2E tests return successfully when their external client binary is
missing. The E2E workflow installs Git LFS, Bazel, Hugging Face tooling, and the listed
S3 clients, but does not install `git-xet`, `skopeo`, Helm, or Podman. Their checked-in
tests therefore show available coverage, not a guarantee that those client flows ran in
every CI job. Review the workflow logs for actual client execution before treating a
specific run as protocol qualification.

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

- Git LFS: batch negotiation plus direct object `GET`, `HEAD`, and `PUT`
- Bazel HTTP remote cache: `ac` and `cas` object `GET` and `PUT`
- OCI Distribution: blob `GET`, `HEAD`, upload, mount, and ranged read paths; manifest
  `PUT`, `GET`, `HEAD`, and digest delete; tag listing with pagination; token-service
  flow at `/v2/token`; upload cancellation and scoped upload-session handling
- Hugging Face Hub API: repository create/info/delete, revision and tree lookup,
  preupload, NDJSON commit, resolve/download, dataset viewer routes, basic search,
  webhooks, Git Smart HTTP clone/fetch/push, SDK live-reference listing, and SDK
  branch/lightweight-tag creation and deletion
- S3 frontend: object `PUT`, `GET` (with `Range`/`Content-Range`), `HEAD`, and `DELETE`;
  multipart upload (`CreateMultipartUpload`, `UploadPart`, `CompleteMultipartUpload`,
  `AbortMultipartUpload`); `HeadBucket`/`GetBucketLocation`/`CreateBucket` stubs;
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
  optional APIs such as Bazel Remote Execution, OCI referrers, Hugging Face
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
