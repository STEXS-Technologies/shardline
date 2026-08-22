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
| Xet CAS | **Stable** | Native Xet upload and download flows; checked-in `git` + `git-lfs` + `git-xet` push, clone, fetch, pull, historical checkout, and sparse checkout coverage |
| Git LFS | **Beta** | LFS batch negotiation plus direct object GET/HEAD/PUT routes; `git-lfs` push/pull plus separate pull and fetch --all flows; conformance-tested but limited production evidence |
| Bazel HTTP remote cache | **Beta** | `ac` and `cas` object GET and PUT routes; `bazel`/`bazelisk` remote-cache flows with remote_download_outputs=all and toplevel; conformance-tested but limited production evidence |
| OCI Distribution | **Stable** | Full blob upload/download, manifest PUT/GET/HEAD/DELETE, tag listing with pagination, token-service flow at /v2/token, upload cancellation, scoped upload-session handling; checked-in `skopeo`, Docker, Helm, and Podman client coverage |
| Hugging Face Hub API | **Beta** | Repository create/info/delete, revision and tree lookup, preupload, NDJSON commit, resolve/download, dataset viewer routes, basic search, webhooks, Git Smart HTTP clone/fetch/push; `hf` CLI model and dataset create, upload, download, filtered snapshot, delete-files, and delete-repository flows; conformance-tested but limited production evidence |
| S3 frontend (protocol) | **Stable** | S3-compatible object API — Put/Get(+Range)/Head/Delete, conditional requests (If-Match/If-None-Match), CopyObject, multipart upload (create/part/complete/abort), ListObjectsV2 + ListObjects v1, DeleteObjects, ListBuckets, and bucket stubs; standard MD5 ETags (single-PUT and multipart-complete) and user metadata (`x-amz-meta-*`) round-trip; SigV4 access-key=token auth plus a Bearer auth bridge; index-backed listing with zero object-store reads; validated against real clients (`mc`, AWS CLI, boto3, `s3cmd`, `rclone`, pyarrow 25) in the CI-run real-client e2e suite and security-audited (`feat/s3-frontend`) |
| Ed25519 auth provider | **Experimental** | Signing and verification, verification-only mode, environment/TOML configuration, authenticated HTTP flows, and operator CLI mint-to-verify interoperability have targeted tests; key-rotation policy and the full negative matrix remain graduation work |
| Local filesystem storage | **Stable** | Checked-in adapter, concurrency, and operator workflow coverage |
| S3-compatible storage | **Stable** | Checked-in object read/write/list and HTTP integration coverage |
| Postgres metadata | **Stable** | Checked-in index, dedupe, concurrency, and operator workflow coverage |
| SQLite metadata | **Stable** | Checked-in local single-node and operator workflow coverage |
| Redis reconstruction cache | **Beta** | TLS and mTLS connectivity, bounded per-operation timeout, cache hit/miss paths, and corrupt-value repair from durable storage are validated; partition/restart and multi-node stampede evidence remain |
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

## Versioning and Deduplication Semantics

The shared CAS core stores immutable content and reuses identical content whenever a
new upload refers to it. That does not mean every frontend exposes the same version
history. Version visibility and overwrite behavior belong to the frontend contract:

| Frontend | User-visible version semantics |
| --- | --- |
| Xet | Revision-oriented workflows can address and retrieve older versions. |
| Git LFS | Version history comes from Git commits and refs; the LFS object endpoint alone is a blob store. |
| Hugging Face Hub | Repository revisions and commits expose historical states. |
| OCI | Blobs and manifests are immutable by digest; tags are mutable pointers and have no tag-history API here. |
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
  webhooks, and Git Smart HTTP clone/fetch/push
- S3 frontend: object `PUT`, `GET` (with `Range`/`Content-Range`), `HEAD`, and `DELETE`;
  multipart upload (`CreateMultipartUpload`, `UploadPart`, `CompleteMultipartUpload`,
  `AbortMultipartUpload`); `HeadBucket`/`GetBucketLocation`/`CreateBucket` stubs;
  `ListObjectsV2` (`prefix`/`delimiter`/`max-keys`/`continuation-token`/`start-after`)
  and `ListObjects` v1 (`marker`/`NextMarker`, the `s3cmd ls` path); standard MD5
  ETags and user metadata (`x-amz-meta-*`); with the SigV4 access-key and Bearer
  auth bridges

## Current Limits

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
