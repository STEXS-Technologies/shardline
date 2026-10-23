# Protocol Gap Register

This register tracks the concrete compatibility and qualification gaps still
visible in the current protocol docs. A protocol being marked Stable means the
documented workflows have evidence; it does not mean the whole upstream service
is implemented. Update this register when a gap is fixed or its product scope is
deliberately changed.

## Open implementation work

| Priority | Surface | Remaining gap | Required proof |
| --- | --- | --- | --- |
| P2 | S3 form upload | The signed path-style `PostObject` workflow is implemented and tested with Boto3. Form `Content-Type`/other HTTP metadata, redirects, ACLs, tagging, storage class, and encryption remain unsupported because their object properties or service behavior are not persisted. | Add each field only with its persistence and response contract; use the native Boto3 form test plus failure, size, checksum, and atomic replacement coverage. |
| P2 | S3 service APIs | Object tagging, versioning/`ListObjectVersions`, lifecycle, bucket policy/CORS/notifications/replication, encryption, ACL, restore/select/torrent/legal-hold/retention, and bucket deletion remain outside the current route set. | Add only with an explicit client requirement and a complete persistence, authorization, lifecycle, and native-client contract for each API family. |
| P2 | Hugging Face Hub | The Hub frontend implements the repository, commit, tree, resolve, LFS/Xet, and other routes listed in its guide; it does not implement every Hub product API. | For each additional Hub API, test with the official SDK/CLI and preserve repository scoping and Git/Xet identity behavior. |

## Open qualification work

| Priority | Surface | Remaining qualification | Required proof |
| --- | --- | --- | --- |
| P2 | Live provider bridge | Live GitHub/Gitea provider tests require external credentials and repositories and are not part of the ordinary local run. | Execute the explicitly selected live tests with isolated test repositories and verify identity, token refresh, webhook and cleanup behavior. |
| P2 | Deployment clients | Kubernetes/kind and other deployment-specific E2E targets need their dedicated fixtures; a local protocol pass does not qualify those environments. | Run the documented deployment suites and review their actual results, including all prerequisite and skip reporting. |
| P1 | S3 multipart checksum deployment coverage | Composite checksums and CRC32/CRC32C/CRC64NVME FULL_OBJECT checksums validate before publication. Botocore and Boto3 FULL_OBJECT flows pass against the SQLite-backed server, including `ListParts`, `GetObjectAttributes`, and checksum-enabled reads. PostgreSQL/MinIO persistence and post-restart reads remain unqualified. | Run the native AWS client flow against PostgreSQL/MinIO, restart the replica, and verify the checksum and object parts remain readable. |

## Clarifications

- Git protocol-v2 support is for `upload-pack` discovery and fetch. Shardline
  uses the established `receive-pack` flow for pushes; protocol-v2 push is not
  a separate implementation requirement in the current Git protocol-v2
  specification. Push compatibility must continue to be verified through real
  Git clients and the receive-pack tests.
- S3 multi-range `GetObject` is not an open gap: AWS S3 itself does not support
  multiple byte ranges in one request. Shardline rejects it with `416
  InvalidRange`, as documented in [S3 Frontend](S3_FRONTEND.md).
- OCI Distribution v1.1.1 conformance currently passes its executed cases, but
  the upstream suite deliberately skips four mutually exclusive setup cases.
  Those are reported as skipped, not counted as passing coverage. See
  [Protocol Frontends](PROTOCOLS.md) for the suite and skip conditions.
- On 2026-10-23, `bash scripts/verify-protocol-conformance.sh` passed against a
  disposable, migrated PostgreSQL database and MinIO bucket. It passed the Git
  CLI and Smart HTTP suites, 75 of 79 OCI specs (4 setup alternatives skipped),
  all selected PostgreSQL protocol-completion tests, and the configured
  PostgreSQL/MinIO tests for atomic Git after restart, signed streaming, OCI
  reclamation/referrers, multipart resume, completion/abort races, and large
  copy ranges. The durable Git test includes shallow merge history, an
  interrupted upload-pack response with native Git retry, deepening/unshallowing,
  filtered blob retrieval, and strict `fsck` after restart across local,
  PostgreSQL, and PostgreSQL/MinIO backends. The final durable Git test was
  rerun separately against all three backends after adding the filtered-clone
  checks. External provider credentials and deployment-specific clients were
  not part of this run.
- On 2026-10-23, `bash scripts/k8s/kind-smoke.sh` passed against a disposable
  kind cluster. It checked split API/transfer traffic, authenticated Xorb upload
  and range download, then deleted one pod from each role and verified readiness
  and persisted data after recovery. This qualifies that smoke profile only; it
  does not qualify the separate cluster protocol suite or live provider bridge.

For the current supported route contract, use [Compatibility Status](COMPATIBILITY_STATUS.md),
[Protocol Frontends](PROTOCOLS.md), [S3 Frontend](S3_FRONTEND.md), and
[Xet Protocol Conformance](PROTOCOL_CONFORMANCE.md).
