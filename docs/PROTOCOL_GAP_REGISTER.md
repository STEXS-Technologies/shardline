# Protocol Gap Register

This register tracks the concrete compatibility and qualification gaps still
visible in the current protocol docs. A protocol being marked Stable means the
documented workflows have evidence; it does not mean the whole upstream service
is implemented. Update this register when a gap is fixed or its product scope is
deliberately changed.

## Open implementation work

| Priority | Surface | Remaining gap | Required proof |
| --- | --- | --- | --- |
| P1 | S3 checksums | Multipart checksum negotiation supports CRC32 COMPOSITE only. CRC32C, CRC64NVME, SHA-1, SHA-256, FULL_OBJECT modes, and persisted checksum attributes for single PUT remain unsupported. | Native SDK tests for each advertised mode; wrong checksums must return `BadDigest`, preserve the previous object, and permit retry. Verify `GetObjectAttributes` before and after restart. |
| P1 | S3 ranges | `GetObject` accepts one byte range; multi-range requests are unsupported. | Native AWS SDK/CLI requests covering disjoint, overlapping, malformed, unsatisfiable and mixed-satisfiability ranges; exact MIME framing and bytes, with integrity validation preserved. |
| P2 | S3 form upload | `PostObject` browser form uploads are not implemented. | Signed-policy and unsigned-policy client flows, size and key constraints, bad-policy rejection, and atomic publication tests. |
| P2 | S3 service APIs | Object tagging, versioning/`ListObjectVersions`, lifecycle, bucket policy/CORS/notifications/replication, encryption, ACL, restore/select/torrent/legal-hold/retention, and bucket deletion remain outside the current route set. | Add only with an explicit client requirement and a complete persistence, authorization, lifecycle, and native-client contract for each API family. |
| P2 | Hugging Face Hub | The Hub frontend implements the repository, commit, tree, resolve, LFS/Xet, and other routes listed in its guide; it does not implement every Hub product API. | For each additional Hub API, test with the official SDK/CLI and preserve repository scoping and Git/Xet identity behavior. |

## Open qualification work

| Priority | Surface | Remaining qualification | Required proof |
| --- | --- | --- | --- |
| P1 | Git shallow/filtered fetch | Native tests cover the documented depth, date, exclusion and object-filter flows, but broader shallow-history merge, backend and failure qualification remains open. | Cross-backend clone/fetch/merge tests, interrupted transfer and retry, history identity, and `git fsck` after restart. |
| P1 | PostgreSQL and remote S3 protocol run | The local conformance run does not establish PostgreSQL/MinIO behavior. This workspace has no `DATABASE_URL` or `TEST_S3_*` configuration. | Run `scripts/verify-protocol-conformance.sh` with disposable PostgreSQL and S3-compatible storage; retain the execution result outside source control. |
| P2 | Live provider bridge | Live GitHub/Gitea provider tests require external credentials and repositories and are not part of the ordinary local run. | Execute the explicitly selected live tests with isolated test repositories and verify identity, token refresh, webhook and cleanup behavior. |
| P2 | Deployment clients | Kubernetes/kind and other deployment-specific E2E targets need their dedicated fixtures; a local protocol pass does not qualify those environments. | Run the documented deployment suites and review their actual results, including all prerequisite and skip reporting. |

## Clarifications

- Git protocol-v2 support is for `upload-pack` discovery and fetch. Shardline
  uses the established `receive-pack` flow for pushes; protocol-v2 push is not
  a separate implementation requirement in the current Git protocol-v2
  specification. Push compatibility must continue to be verified through real
  Git clients and the receive-pack tests.
- OCI Distribution v1.1.1 conformance currently passes its executed cases, but
  the upstream suite deliberately skips four mutually exclusive setup cases.
  Those are reported as skipped, not counted as passing coverage. See
  [Protocol Frontends](PROTOCOLS.md) for the suite and skip conditions.
- A local run on 2026-10-23 passed the protocol-conformance script's selected
  local checks, including 75 of 79 OCI specs (4 setup alternatives skipped),
  31 selected protocol-completion tests, Git CLI and Smart HTTP tests, and
  signed MinIO client tests. PostgreSQL and remote-S3 phases were not run.

For the current supported route contract, use [Compatibility Status](COMPATIBILITY_STATUS.md),
[Protocol Frontends](PROTOCOLS.md), [S3 Frontend](S3_FRONTEND.md), and
[Xet Protocol Conformance](PROTOCOL_CONFORMANCE.md).
