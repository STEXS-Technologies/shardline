# Protocol Gap Register

This register tracks the concrete compatibility and qualification gaps still
visible in the current protocol docs. A protocol being marked Stable means the
documented workflows have evidence; it does not mean the whole upstream service
is implemented. Update this register when a gap is fixed or its product scope is
deliberately changed.

## Open implementation work

| Priority | Surface | Remaining gap | Required proof |
| --- | --- | --- | --- |
| P2 | S3 form upload | Signed path-style `PostObject` supports standard object HTTP metadata (`Content-Type`, `Cache-Control`, `Content-Disposition`, `Content-Encoding`, `Expires`, website redirect), checksums, user metadata, tags and success redirects. ACLs, storage class and encryption remain unsupported. | Native Boto3 coverage checks signed POST persistence, including tags. Redirects have native Boto3 and signed-form integration coverage. |
| P2 | S3 service APIs | Versioning/`ListObjectVersions`, lifecycle, bucket policy/CORS/notifications/replication, encryption, ACL, restore/select/torrent/legal-hold/retention, and bucket deletion remain outside the current route set. Object tags are stored canonically in evidence-bound private metadata without changing historical evidence encoding. Older writers do not know that entry and may drop it on same-key updates; mixed-version writes for tagged keys need explicit routing. | For tagging, finish native S3 client verification across PostgreSQL, SQLite, restart, multipart and mixed-version deployment. For each other API family, add only with an explicit client requirement and a complete persistence, authorization, lifecycle, and native-client contract. |
| P2 | Hugging Face Hub | The Hub frontend implements the repository, commit, tree, resolve, LFS/Xet, and other routes listed in its guide; it does not implement every Hub product API. | For each additional Hub API, test with the official SDK/CLI and preserve repository scoping and Git/Xet identity behavior. |

## Open qualification work

| Priority | Surface | Remaining qualification | Required proof |
| --- | --- | --- | --- |
| P2 | Live provider bridge | Live GitHub/Gitea provider tests require external credentials and repositories and are not part of the ordinary local run. | Execute the explicitly selected live tests with isolated test repositories and verify identity, token refresh, webhook and cleanup behavior. |
| P2 | Deployment clients | The disposable kind profile now passes both split-role smoke and `k8s_cluster_protocol_e2e`, including native Xet uploads and downloads against separate API and transfer services. A customer or production cluster endpoint has not been qualified. | Run the same native protocol suite with `SHARDLINE_K8S_E2E_*` configured for each target deployment, then review its backend, readiness, sparse-transfer, and metrics results. |
| P2 | S3 object tagging | Local route tests and native Boto3 workflows cover the tagging APIs, copy directives, multipart and POST tags. SQLite and PostgreSQL/MinIO persistence across real server restarts pass. Mixed-version writer qualification remains open. | Exercise an older writer's same-key replacement during rollout and verify the documented routing prevents tag loss or exposes the changed tag state clearly. |
| P1 | S3 multipart checksum deployment coverage | Composite checksums (CRC32, CRC32C, SHA-1 and SHA-256) and FULL_OBJECT checksums (CRC32, CRC32C and CRC64NVME) validate before publication. The native Botocore restart matrix passes all these algorithms with PostgreSQL metadata and MinIO object storage, including `ListParts`, `GetObjectAttributes`, and checksum-enabled reads. Production S3 providers still need equivalent deployment coverage. | Repeat the native client matrix against each supported production S3 provider and record provider-specific compatibility results. |

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
  and persisted data after recovery. It then passed the native `k8s_cluster_protocol_e2e`
  against the deployed split-role services, with PostgreSQL, Redis, and MinIO
  forwarded from the cluster. That flow verified provider-issued tokens,
  upload, sparse mutation, reconstruction through the transfer service,
  deduplication, persisted metadata, and cold/warm transfer measurements. This
  qualifies the disposable kind profile; it does not qualify a customer or
  production cluster endpoint or the live provider bridge.
- On 2026-10-23, `cargo test --manifest-path e2e/Cargo.toml --test
  k8s_cluster_protocol_e2e -- --ignored --nocapture` passed using its disposable
  Docker PostgreSQL, MinIO and Redis services. The native Xet flow uploaded
  original and sparse-mutated files, verified deduplication and storage metadata,
  measured cold and warm transfer bytes, then confirmed reconstruction caching.
  An initial pool timeout exposed the fixture's one-connection migration pool;
  the fixture now uses the two connections required by the migration helper,
  which fails fast if given a smaller pool.
- On 2026-10-23, native AWS CLI signing/presigned-read and multipart-resume
  tests passed; native Git LFS lock/transfer tests passed with both SQLite and
  disposable PostgreSQL; and all four Hugging Face CLI tests passed, including
  Hub/Xet negotiation and S3 coexistence. These verify the named workflows,
  not every operation exposed by AWS, Git LFS or Hugging Face.
- On 2026-10-23, native Boto3 tagging tests passed for signed browser POST,
  ordinary object tags, Get/Put/DeleteObjectTagging, copy defaults and
  replacement, and multipart upload. A separate Botocore test verified tags
  survive a server stop/start against SQLite. The native Botocore PostgreSQL
  and MinIO restart test also passed with tag persistence enabled. Mixed-version
  writer routing remains unqualified.

For the current supported route contract, use [Compatibility Status](COMPATIBILITY_STATUS.md),
[Protocol Frontends](PROTOCOLS.md), [S3 Frontend](S3_FRONTEND.md), and
[Xet Protocol Conformance](PROTOCOL_CONFORMANCE.md).
