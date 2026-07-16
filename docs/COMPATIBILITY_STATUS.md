# Compatibility Status

Shardline has a CAS-agnostic runtime with explicit protocol frontends.
The current compatibility contract is scoped to the protocol and operator workflows
documented in this repository.

## 1.0.0 Contract

Shardline `1.0.0` should be treated as a content-addressed backend for the validated
workflows covered by this repository.

## Validated Today

- native Xet upload and download flows
- Git LFS batch negotiation plus direct object upload, download, and metadata lookup
  routes
- Bazel HTTP remote-cache `ac` and `cas` read and write routes
- OCI Distribution blob upload and download, manifest upload and lookup, and tag listing
  routes
- native external-client coverage for `git-lfs`, `bazel`/`bazelisk`, and `skopeo` across
  multiple command-path variants in the repository test matrix
- native `hf` CLI coverage for model and dataset repository creation, single-file and
  filtered-directory upload, single-file and filtered snapshot download, file deletion,
  and repository cleanup
- provider-issued repository-scoped tokens and provider webhook handling for GitHub,
  GitLab, Gitea, Codeberg, and the generic provider adapter
- stock `git` + `git-lfs` + `git-xet` push, clone, fetch, pull, and historical checkout
  coverage in the validated test matrix
- sparse checkout behavior for Xet-tracked files
- local SQLite + filesystem deployments and Postgres + S3-style durable deployments
- operator workflows for migrations, fsck, lifecycle repair, index rebuild, backup,
  storage migration, retention holds, and garbage collection
- fuzz coverage for protocol parsing, protocol frontend selectors and validators,
  reconstruction, lifecycle repair, CLI parsing, and local filesystem race boundaries

## Validated Native Clients

| Frontend | Native client coverage |
| --- | --- |
| Xet | native Xet upload and download flows, plus `git` + `git-lfs` + `git-xet` push, clone, fetch, pull, historical checkout, and sparse checkout coverage |
| Git LFS | `git-lfs` push/pull plus separate `pull` and `fetch --all` flows |
| Bazel HTTP remote cache | `bazel` and `bazelisk` remote-cache flows with `remote_download_outputs=all` and `remote_download_outputs=toplevel` |
| OCI Distribution | `skopeo` push/pull/tag-list, digest-reference, multi-tag, registry-to-registry copy, multi-arch index, Docker schema2, and token-service credential flow; `helm` OCI chart push/pull; `podman` pull/push; `docker` login/pull/push |
| Hugging Face Hub API | `hf` CLI model and dataset create, upload, download, filtered snapshot, delete-files, and delete-repository flows |

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
- The first crates.io release still has to publish the internal crate graph in
  dependency order before publishing the `shardline` CLI crate.
