# Compatibility Status

Shardline has a CAS-agnostic runtime with explicit protocol frontends.
The current compatibility contract is scoped to the protocol and operator workflows
documented in this repository.

## Current Source-Tree Contract

The current Shardline source tree should be treated as a content-addressed backend for
the validated workflows covered by this repository. Release notes identify which
source-tree capabilities are included in each published version.

## Surface Maturity

| Surface | Tier | Evidence |
|---------|------|----------|
| Xet CAS | **Stable** | Native Xet upload and download flows; checked-in `git` + `git-lfs` + `git-xet` push, clone, fetch, pull, historical checkout, and sparse checkout coverage |
| Git LFS | **Beta** | LFS batch negotiation plus direct object GET/HEAD/PUT routes; `git-lfs` push/pull plus separate pull and fetch --all flows; conformance-tested but limited production evidence |
| Bazel HTTP remote cache | **Beta** | `ac` and `cas` object GET and PUT routes; `bazel`/`bazelisk` remote-cache flows with remote_download_outputs=all and toplevel; conformance-tested but limited production evidence |
| OCI Distribution | **Stable** | Full blob upload/download, manifest PUT/GET/HEAD/DELETE, tag listing with pagination, token-service flow at /v2/token, upload cancellation, scoped upload-session handling; checked-in `skopeo`, Docker, Helm, and Podman client coverage |
| Hugging Face Hub API | **Beta** | Repository create/info/delete, revision and tree lookup, preupload, NDJSON commit, resolve/download, dataset viewer routes, basic search, webhooks, Git Smart HTTP clone/fetch/push; `hf` CLI model and dataset create, upload, download, filtered snapshot, delete-files, and delete-repository flows; conformance-tested but limited production evidence |
| Ed25519 auth provider | **Experimental** | Signing and verification, verification-only mode, environment/TOML configuration, and authenticated HTTP flows have targeted tests; the operator CLI does not mint Ed25519 tokens |
| Local filesystem storage | **Stable** | Checked-in adapter, concurrency, and operator workflow coverage |
| S3-compatible storage | **Stable** | Checked-in object read/write/list and HTTP integration coverage |
| Postgres metadata | **Stable** | Checked-in index, dedupe, concurrency, and operator workflow coverage |
| SQLite metadata | **Stable** | Checked-in local single-node and operator workflow coverage |
| Redis reconstruction cache | **Beta** | TLS and mTLS connectivity; cache hit/miss paths validated; limited production evidence |
| Provider integration | **Beta** | Checked-in token issuance, webhook handling, and repository-scoped authorization coverage for GitHub, GitLab, Gitea, Codeberg, and the generic adapter |

### Tier Definitions

- **Stable**: broad checked-in route, integration, and native-client coverage for the
  advertised workflows. This tier does not claim a particular deployment's production,
  load, failure-injection, or upgrade history.
- **Beta**: checked-in route or client coverage exists, but the compatibility surface or
  operational evidence is narrower.
- **Experimental**: implemented with targeted tests, but configuration or interoperability
  may still change.
- **Internal**: architectural component, not a user-facing promise.

## Validated Route Surface

- Git LFS: batch negotiation plus direct object `GET`, `HEAD`, and `PUT`
- Bazel HTTP remote cache: `ac` and `cas` object `GET` and `PUT`
- OCI Distribution: blob `GET`, `HEAD`, upload, mount, and ranged read paths; manifest
  `PUT`, `GET`, `HEAD`, and digest delete; tag listing with pagination; token-service
  flow at `/v2/token`; upload cancellation and scoped upload-session handling
- Hugging Face Hub API: repository create/info/delete, revision and tree lookup,
  preupload, NDJSON commit, resolve/download, dataset viewer routes, basic search,
  webhooks, and Git Smart HTTP clone/fetch/push

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
