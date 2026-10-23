# S3 Frontend — Compatibility & Scope

**Status:** scoped S3 compatibility contract; see the operation matrix and
current evidence in [Compatibility Status](COMPATIBILITY_STATUS.md).

## Purpose

An S3-compatible frontend so lakehouse writers — pyarrow, pyiceberg, Spark/Trino
(S3A), DuckDB, Polars, and other `s3://`-speaking clients — can PUT/GET objects
into Shardline and receive content-defined chunking (CDC) + content-addressed
dedup for free. This is **deliberately not full S3 compatibility**: it covers
the operations the target clients actually call, and documents everything else
as out of scope until a real client need appears.

## Design decisions

1. **Bucket → repository scope.** `{bucket}` is the single dotted segment
   `{owner}.{name}` of the token's `RepositoryScope`, decoded by splitting on
   the *first* `.` (`a.b.c` → owner `a`, name `b.c`). The C1 repo-binding model
   holds exactly: every S3 request requires the bearer token's `owner`/`name`
   to match the decoded bucket (`403 AccessDenied` otherwise). An owner
   containing `.` is a hard validation error (`404 NoSuchBucket`) — never a
   silent mismatch. Routing is `/{bucket}/{*key}` (S3 keys are arbitrary-depth;
   the key is the wildcard remainder).
2. **Authentication.** S3 clients authenticate with SigV4, not bearer tokens.
   The frontend bridges: the client's `access_key` **is** the Shardline bearer
   token (minted via `shardline admin token`, scoped to `{owner}.{name}`),
   extracted from the `Authorization: AWS4-HMAC-SHA256 Credential=...` header.
   In this compatibility mode the SigV4 signature is **not verified** (the access key *is* the credential;
   requires TLS in production — documented deviation). A plain
   `Authorization: Bearer` is also accepted for tooling that supports it.
   Operators can additionally register signing identities as described below;
   those access keys require cryptographically valid signatures.
3. **Multipart + CDC.** `UploadPart` bodies stream to per-part temp files;
   `CompleteMultipartUpload` feeds the parts sequentially through **one CDC
   pass**, producing a single `FileRecord` — preserving whole-object dedup and
   the single-record read model (GetObject + Range work unchanged). Part count
   capped at 10,000. `UploadPart` accepts **any part size** (matching S3); the
   **5 MiB minimum part size** is enforced at Complete for every part except
   the last (the highest part number in the submitted list), also matching S3.
   Per-session and aggregate byte quotas (`SHARDLINE_S3_UPLOAD_SESSION_MAX_BYTES` /
   `SHARDLINE_S3_UPLOAD_TOTAL_MAX_BYTES`) and a global active-part-file cap
   (`SHARDLINE_S3_UPLOAD_MAX_ACTIVE_PART_FILES`, checked before the part file
   is written) bound disk use; session expiry is anchored to **initiation**
   (keep-alive parts do not extend it), matching S3's 7-day multipart
   lifecycle. `AbortMultipartUpload` discards the session.
4. **GetObject Range / ETag / errors.** Range reuses the existing byte-range +
   reconstruction path (`parse_http_byte_range`, 206/416 semantics). ETag is the
   **hex MD5 of the object bytes** (standard S3; identical for single-PUT and
   multipart of the same bytes, so checksum-verifying clients like `s3cmd`
   accept it). Errors are an S3 XML envelope (`<Error><Code>…`);
   codes: `NoSuchKey`, `NoSuchBucket`, `AccessDenied`, `InvalidRange`,
   `EntityTooSmall`, `InvalidPart`, `NoSuchUpload`, `NotImplemented`,
   `PreconditionFailed` (conditional mismatches), `MalformedXML`, `InternalError`,
   `TooManyParts` (global active-part-file cap reached), `SlowDown`.

## Operation matrix

### Supported (this branch)

Operations in this table are **implemented** and exercised by the
client-shaped e2e suites in `crates/shardline-server/tests/s3_frontend_e2e.rs`
and `crates/shardline-server/tests/protocol_completion.rs`: pyarrow
(PUT/GET/Range/multipart), Polars / object_store (HeadObject), S3A (bucket
probes), DuckDB (ListObjectsV2), and Boto3-generated signed browser forms.
Coverage uses both SigV4 access-key and Bearer authentication where the
operation supports them.

| Operation | Notes |
|---|---|
| `PutObject` | streamed through the CDC ingestor; body ≤ `SHARDLINE_S3_MAX_PART_BYTES` (default 1 GiB); larger must use multipart |
| `GetObject` (+ `Range`) | existing range path; `bytes=a-b`, `bytes=a-`, `bytes=-N`; 206 / 416; stored checksums when requested with `x-amz-checksum-mode: ENABLED` |
| `HeadObject` | size + ETag; stored checksum headers when requested with `x-amz-checksum-mode: ENABLED` |
| `GetObjectAttributes` (partial) | `GET ?attributes` with `x-amz-object-attributes`; selected ETag, object size and stored checksum from one verified snapshot. `StorageClass` is omitted for STANDARD objects as S3 does. Newly completed multipart uploads report `ObjectParts.PartsCount`; COMPOSITE uploads also report verified part sizes/checksums, and FULL_OBJECT uploads report the verified whole-object checksum. Unnegotiated and legacy multipart objects report the count only. Legacy objects with unknown multipart counts return `NotImplemented` rather than guessing. |
| `DeleteObject` | removes direct object **and** record (S3 semantics) |
| `CreateMultipartUpload` | `POST /{key}?uploads` |
| `UploadPart` | `PUT ?partNumber&uploadId` → part file |
| `UploadPartCopy` | same multipart PUT with `x-amz-copy-source`; pinned same-bucket source, optional explicit byte range and source conditions; normal part quotas and verified publication apply |
| `CompleteMultipartUpload` | single CDC pass → record + ETag |
| `AbortMultipartUpload` | `DELETE ?uploadId`; discards session |
| `ListParts` | `GET ?uploadId`; paginated persisted part metadata, including negotiated COMPOSITE checksums and optional CRC32/CRC32C/CRC64NVME FULL_OBJECT part checksums |
| `ListMultipartUploads` | `GET /{bucket}?uploads`; prefix, delimiter, key/upload markers and URL encoding |
| `HeadBucket` | stub (S3A / object_store connect-probe) |
| `GetBucketLocation` | stub → `us-east-1` (S3A / pyarrow region-probe) |
| `CreateBucket` | no-op `200` (S3A missing-bucket probe) |
| `ListObjectsV2` | index-backed; `prefix`/`delimiter`/`max-keys`/`continuation-token`/`start-after`; zero object-store reads — the index rows carry size/ETag/mtime |
| `ListObjects` v1 | bare `GET /{bucket}`; `prefix`/`delimiter`/`max-keys`/`marker`; emits `NextMarker` when truncated |
| `PostObject` (browser form) | path-style `POST /{bucket}` with a configured SigV4 signing identity and signed policy; checks bucket, exact/prefix fields and content-length-range before atomic publication; supports `${filename}`, standard object HTTP metadata (`Content-Type`, `Cache-Control`, `Content-Disposition`, `Content-Encoding`, `Expires`, and `x-amz-website-redirect-location`), `x-amz-meta-*`, `x-amz-tagging`, `x-ignore-*`, success status/redirect fields, `Content-MD5`, and CRC32/CRC32C/SHA-1/SHA-256 checksums |
| `DeleteObjects` (batch) | `POST /{bucket}?delete=`; ≤ 1000 distinct keys per request (`MalformedXML` beyond, `400`); invalid keys become per-key `<Error>` rows |
| `CopyObject` | `PUT` with `x-amz-copy-source`; source must be in the caller's bound bucket; dest gets a fresh ETag (same content → same ETag) |
| Conditional requests | `If-Match` / `If-None-Match` on Get/Put/Head/Delete, CompleteMultipartUpload and CopyObject destination; both conditions apply, repeated tag lists are combined, and malformed fields return `400 InvalidArgument`. Weak tags cannot satisfy `If-Match`. Mismatches return `412 PreconditionFailed`, except Get/Head `If-None-Match` returns empty `304 Not Modified` with ETag; missing `If-Match` targets return `404 NoSuchKey`. Failed multipart conditions preserve the existing object and allow retry of the upload session. |
| Copy source conditions | CopyObject evaluates `x-amz-copy-source-if-match` / `if-none-match` and `if-modified-since` / `if-unmodified-since` against validators from the same pinned source version. Tag conditions take precedence over corresponding date conditions. Legacy sources lacking validators fail closed for concrete conditions; wildcard conditions still use known source existence. |
| Read date conditions | GetObject, HeadObject and supported GetObjectAttributes honor `If-Modified-Since` (`304`) and `If-Unmodified-Since` (`412`) using the same committed timestamp as their other metadata. `If-Match` takes precedence over `If-Unmodified-Since`; `If-None-Match` takes precedence over `If-Modified-Since`. Invalid/repeated dates and dates on legacy records without a known modification time are ignored. |
| Conditional ranges | GetObject honors one matching strong `If-Range` ETag; stale, weak, malformed, repeated or date validators return the entire representation (`200`). Date validators are conservatively ignored because the route does not have a strong date validator. |
| `ListBuckets` | service-level `GET /`; lists the caller's single `{owner}.{name}` bucket |

### Planned (near-term follow-up)

The remaining protocol work includes the unsupported operations below. The current partial attributes response follows the field-selection shape of the
[S3 GetObjectAttributes API](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObjectAttributes.html).
Version selectors and unsupported encryption, payer and expected-owner options
fail explicitly instead of silently returning attributes for a different request.
The v1 listing is selected by a bare bucket GET; an explicit `list-type=1`
parameter is not accepted and returns `501 NotImplemented`.

### Not yet implemented (respond `501 NotImplemented`)

| Operation | Reconsider when |
|---|---|
| Restore / select / torrent / legal-hold / retention / ACL | not implemented |
| Bucket policy / lifecycle / versioning / CORS / notification / ACL / encryption / replication / website, `DeleteBucket`, `ListObjectVersions` | not yet implemented |

Object requests fail with `501 NotImplemented` when they include unsupported
ACL, encryption, expected-owner or requester-payer options. Object writes accept
`x-amz-storage-class: STANDARD`, which is the default storage class; other
classes are rejected. Browser `PostObject` form fields for ACL, storage class
and encryption remain unsupported and are rejected rather than ignored.

`PostObject` currently supports the signed path-style form described above. A
valid HTTP(S) `success_action_redirect` (or deprecated `redirect`) receives a
`303 See Other`; Shardline appends the stored bucket, expanded key and ETag to
the destination's query parameters. Existing query parameters are preserved.
Unparseable or non-HTTP(S) redirect values are ignored, following S3 behavior.
It does not implement other form fields that need object properties or service
behavior Shardline does not persist yet, including ACLs, storage class and
encryption. Object tags are stored in the object's evidence-bound private
metadata and are available through `GetObjectTagging`, `PutObjectTagging`, and
`DeleteObjectTagging`. `PutObjectTagging` verifies the supplied Content-MD5 or
supported request checksum before committing the tag change. These features
need upgraded replicas during a
mixed-version rollout because older writers may discard tags on same-key
updates. Forms without a valid
SigV4 policy are rejected; bearer-token authentication alone is not accepted
for browser forms. The file part must be last and is size-bounded. Its bytes are
spooled to an anonymous temporary file, then passed through the normal verified
object publication path; a failed policy or checksum does not replace an
existing object.

`CreateMultipartUpload` supports negotiated `CRC32`, `CRC32C`, `SHA1`, and `SHA256`
with the default or explicit `COMPOSITE` type, and `CRC32`, `CRC32C`, or
`CRC64NVME` with `FULL_OBJECT`. CRC64NVME defaults to `FULL_OBJECT`; its
`COMPOSITE` form is rejected. Composite uploads require and verify the echoed
part checksums. Full-object uploads accept optional part checksums and verify
the assembled object's checksum before publication; a mismatch leaves the
previous object and multipart session intact so the client can retry. The
completed checksum is retained with the object and returned by checksum-enabled
reads and `GetObjectAttributes`. S3 `Content-Type` is retained separately from
user metadata and returned by GET and HEAD, including values received from
signed browser forms. No schema migration is required. Legacy CRC32
multipart metadata remains readable. Active `ListParts` exposes negotiated
checksum data when it is available.
The in-progress `ListParts` response includes the negotiated algorithm and
checksum type, plus part checksums where available. If an older binary wrote an
active checksum-enabled upload without persisting its part checksum, the
upgraded server derives it from the stored part after verifying the part's content.
Local sessions retain checksums in their integrity-protected metadata, bound to
the immutable part filename. PostgreSQL sessions retain them with session state,
bound to the part generation. Both backends derive missing or stale values from
verified bytes.
Checksum headers on single `PutObject` are validated before publication and
retained for later `GetObjectAttributes` reads and `GetObject`/`HeadObject`
requests using `x-amz-checksum-mode: ENABLED`. During a rolling deployment, keep
each checksummed object's reads and writes on upgraded replicas, or wait until
all replicas are upgraded before enabling new checksum or content-type writes.
A 1.12.1 replica can read the object bytes but may expose private metadata as
ordinary `x-amz-meta-*` metadata and will return the default content type for
objects written with the new content-type property. Existing CRC32 uploads and
objects remain readable.
Existing objects have no durable part-count metadata; those counts remain unknown
and are not inferred from CDC chunks or legacy records. Botocore 1.x real-client
coverage exercises negotiated multipart CRC32, SHA-1 and SHA-256 COMPOSITE,
CRC64NVME FULL_OBJECT, active `ListParts`, completion, and attribute responses,
plus single-PUT checksum HEAD and attributes. The Rust HTTP integration suite
covers CRC32C and SHA-256 negotiation, CRC64NVME FULL_OBJECT checksum failure
and retry, active `ListParts`, completion, attributes, and byte-for-byte object
reads. A single-PUT object has no `ObjectParts` response.

For general-purpose S3 buckets, AWS only returns individual `ObjectParts.Part`
entries when an additional checksum was applied to the object. Shardline follows
that behavior: negotiated CRC32/CRC32C/SHA-1/SHA-256 COMPOSITE objects include checksummed parts;
FULL_OBJECT objects return a verified whole-object checksum and total count, and
part checksums when the upload supplied them. Unnegotiated and legacy multipart
objects return the verified total count only.

## Protocol deviations (documented)

AWS S3 does not support multiple byte ranges in one `GetObject` request. Shardline
rejects such a request with `416 InvalidRange`, matching its single-range parser
and the AWS service's documented limitation. See the [AWS GetObject range
documentation](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObject.html).

Multipart uploads support `GET ?uploadId=...` (`ListParts`) with
`part-number-marker` and `max-parts` pagination. Part metadata survives local
restarts and is shared through PostgreSQL in distributed deployments. Completed,
aborted, expired, and differently scoped uploads are not exposed. Older local
sessions without saved ETags recover those checksums from the protected part
files. Bucket-wide discovery supports prefix/delimiter filtering and key/upload
markers across restart, including PostgreSQL replicas with separate local roots.
New sessions retain their initiation time; legacy PostgreSQL sessions without
that timestamp remain discoverable but omit the historical `Initiated` field.
Discovery retains one response page plus one look-ahead entry while scanning
verified metadata; it does not read part bodies. PostgreSQL can continue from
an aborted marker upload while its verified session metadata is retained.
Filesystem deployments save the returned page's last upload position in the
existing SQLite metadata database before sending a truncated response. This
allows continuation after that upload is deleted, including after a restart,
without changing existing upload IDs or retaining part files. The position is
retained for the configured upload-session TTL; expired records are cleaned in
bounded batches as new positions are saved. Finish paging within that window,
or restart the listing. These corruption-checked records only guide pagination;
they never authorize an upload, a deletion or an object publication.
An expired position follows the unknown manual-marker rules: equal-key uploads
are filtered by upload-ID order. It cannot recover the earlier chronological
position, so restart the listing after the retention window to avoid omissions.
Native Botocore pagination is tested with repeated keys, page sizes of 1, 3 and
7, and stop/resume tokens on SQLite and PostgreSQL.

- **ETag** is the hex MD5 of the object bytes — the S3 convention for
  single-part PUTs, and the MD5 of the assembled bytes at multipart completion
  — so checksum-verifying clients (`s3cmd`) and conditional requests behave
  like AWS. User metadata (`x-amz-meta-*`) is captured on PutObject and
  CreateMultipartUpload, propagated by CopyObject (`x-amz-metadata-directive`),
  and returned on Head/GetObject.
- **Legacy token bridge does not verify SigV4 signatures** — the access key *is* the credential;
  production requires TLS.
- **Bucket names** are `{owner}.{name}`; owners containing `.` are not addressable.
- **`Content-MD5`** is optional and verified when supplied on PutObject,
  UploadPart, CompleteMultipartUpload and DeleteObjects. It must be one
  canonical base64 encoding of a 16-byte digest; malformed or repeated fields
  return `400 InvalidDigest`, and a payload mismatch returns `400 BadDigest`
  before publication or deletion. Upload checks hash the decoded payload,
  including AWS chunked uploads, with constant memory.
- **S3 payload checksum headers** (`x-amz-checksum-crc32`, `crc32c`,
  `crc64nvme`, `sha1`, and `sha256`) are optional and verified as the decoded
  `PutObject` or `UploadPart` body streams. A mismatch returns `400 BadDigest`
  before publication; a valid supplied checksum is echoed on success. These
  values are not persisted for later attributes reporting. AWS-chunked signed
  trailer checksums continue through the separate SigV4 trailer verifier.
- **Multipart part identity** is the quoted MD5 of that part's bytes.
  Completion accepts that MD5 with or without the surrounding quotes and requires
  the identity returned by the successful UploadPart for every selected part,
  in ascending order; missing, stale or incorrect tags
  return `400 InvalidPart` and leave the session retryable. Local replacement
  parts use immutable content-addressed files and an atomic metadata pointer
  publication, so failed body reads, checksums or publication cannot truncate
  previously acknowledged bytes. Logical byte/file quotas count the committed
  parts; replacement staging temporarily retains both versions within the
  configured per-part body ceiling.
- **Legacy in-flight multipart uploads** retain readable session metadata,
  but old opaque `uploadid-partnumber` tags cannot identify acknowledged
  content. Re-upload those parts to obtain content ETags before completing
  them. Previously overwritten or missing bytes require verified restoration;
  the server cannot infer their original content.
- **Multipart part size minimums** follow S3 (5 MiB for all but the final part);
  the part-size ceiling (`SHARDLINE_S3_MAX_PART_BYTES`) and the per-session /
  aggregate byte quotas are configurable.
- **Errors** use the S3 XML envelope (`<Error><Code>…`); codes include
  `PreconditionFailed` (`412`) for conditional-request mismatches and
  `MalformedXML` (`400`) for schema violations.

## Auth & roles

- `access_key` = Shardline bearer token scoped to the bucket's `owner.name`;
  binding mismatch → `403 AccessDenied`; undecodable bucket → `404 NoSuchBucket`.
- S3 is an **API-tier** frontend (reads + writes touch records/ingest); a
  transfer-only role does not serve S3.

### Authentication (operator)

To use real signing keys, set `SHARDLINE_S3_SIGNING_IDENTITIES_FILE` to a secret
JSON file containing an array like this:

```json
[{"access_key":"CLIENTKEY","secret_key":"replace-with-a-long-random-secret","token":"existing-scoped-Shardline-token","region":"us-east-1"}]
```

Give the client the access key and secret key. Keep the bound token on the
server. Its normal expiry, permissions and repository restrictions still apply.
Registered keys require valid SigV4 signatures and support presigned GET/PUT URLs;
signed payload hashes are checked before publication. Use TLS, including with
the standard `UNSIGNED-PAYLOAD` signing mode. An optional `session_token` field
requires that token in signed request headers or presigned URLs. Missing or
changed session tokens fail; neither signing nor session credentials can outlive
or expand the permissions of their bound Shardline token. This does not provide
an AWS STS credential-issuance service. Classic signed streaming uploads
(`STREAMING-AWS4-HMAC-SHA256-PAYLOAD`) verify every chunk signature, the final
zero chunk and the declared decoded length before publication. Corrupt or
incomplete streams cannot replace a published object. Signed trailer uploads
(`STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER`) additionally verify the trailer
signature and each declared CRC32, CRC32C, CRC64NVME, SHA-1 or SHA-256 checksum.
Missing, repeated or undeclared trailers fail before publication.
Native MinIO Go v7.0.95 signed streams and all five supported checksum trailers are exercised for object
uploads and multipart parts on SQLite, PostgreSQL with local storage, and
PostgreSQL with MinIO storage. These checks reject corrupted replacements and
verify exact object bytes after a graceful server restart. A valid signature over
an incorrect checksum returns `400 BadDigest`; corrupt signed payloads return
`403`. Mid-body TCP disconnects are also exercised for classic streams and all
five trailer algorithms without replacing existing objects or acknowledged
parts. These disconnect tests do not establish server-crash recovery.
Its LF-delimited checksum
trailer format is accepted only in signed mode; chunk headers and the trailer
signature line still require CRLF. Broader client-version and remote-failure
qualification remains separate.
`STREAMING-UNSIGNED-PAYLOAD-TRAILER` checks the same declared checksums and
decoded length but has no chunk or trailer MAC. As with `UNSIGNED-PAYLOAD`, use
TLS to protect these uploads in transit; a checksum is not authentication.
An incorrect trailer checksum returns `400 BadDigest`. Invalid signature or
stream framing is rejected before publication; neither failure replaces an
existing object or an acknowledged multipart part.
SigV4A remains unsupported for registered keys.
Native MinIO signed-session checks on SQLite and PostgreSQL accept the configured
session token and reject missing or substituted tokens without replacing stored
content. Existing Bearer reads remain available. These checks cover classic
signed streams and signed CRC32 trailers, not session-presigned URLs or STS issuance.
The file is bounded to 1 MiB, loaded
at startup, and must be readable only by the operator/server account. Restart
replicas after changing it. Existing token clients need no configuration change.

For the existing token-as-access-key setup:

The server must run with the Local auth provider and a signing key; the CLI
must mint with the **same** key material.

- Server: `SHARDLINE_AUTH_PROVIDER=local` plus either
  `SHARDLINE_TOKEN_SIGNING_KEY=<32+ byte key>` **or**
  `SHARDLINE_TOKEN_SIGNING_KEY_FILE=/path/to/key`. The signing key is
  variable-length and binary-capable, so the file bytes are used verbatim —
  there is no trailing-newline stripping. Write the key file without a trailing
  newline (`printf` rather than `echo $KEY > file`).
- CLI mint (note `--ttl-seconds`, and that a key source is required):

  ```sh
  shardline admin token \
    --issuer shardline --subject s3test --scope write \
    --provider generic --owner ac --repo assets \
    --ttl-seconds 3600 \
    --key-env SHARDLINE_TOKEN_SIGNING_KEY        # or --key-file /path/to/key
  ```

  The printed token is the S3 `access_key`; present it either as
  `Authorization: Bearer <token>` or as the SigV4 form
  `Authorization: AWS4-HMAC-SHA256 Credential=<token>/<date>/<region>/s3/aws4_request`
  (the signature is not verified). `--key-env`/`--key-file` and the server's
  env/file source must resolve to the same bytes — the CLI strips one trailing
  line terminator from a `--key-file`, so a key file shared with the server must
  not end in a newline (write it with `printf`, not `echo $KEY > file`).

## Durability & integrity

The S3 frontend does **not** get its own storage or metadata path — it rides the
same CAS pipeline as every other frontend, so it inherits the same integrity and
durability guarantees:

- **Write integrity** — `PutObject` and `CompleteMultipartUpload` stream through
  the shared `FileUploadIngestor` (CDC chunking, per-chunk BLAKE3 + BLAKE3-root
  content hash), stored content-addressed with `ObjectIntegrity` (hash + size
  verified at the store boundary). Identical to Xet/LFS/OCI/Bazel/Hub uploads.
- **Read integrity** — `GetObject` / `Range` reconstruct via the shared
  record-reconstruction path, which verifies every chunk hash against the
  `FileRecord` before returning bytes. A corrupted or wrong chunk fails rather
  than serving garbage.
- **Metadata durability** — the `FileRecord` is committed transactionally in the
  shared index (sqlite/postgres); chunks are durable in the configured storage
  backend. fsck validates the record↔chunk contract; GC reachability protects
  the chunks via the record.
- **The S3 listing index is a derived snapshot, GC inert, and not a
  reachability source** — deletion takes the same distributed per-key fence as
  publication, removes legacy direct bytes first, then atomically removes the
  listing row plus visible record aliases. A failed metadata transaction leaves
  row-backed bytes readable; a crash cannot resurrect a deleted legacy object
  through the row-absent fallback. Immutable chunks are reclaimed later by
  writer-excluding GC.
- **Listing nuance** — `ListObjectsV2` serves the snapshot (size/hash/mtime from
  the index row); `HeadObject`/`GetObject` always resolve through the
  authoritative `FileRecord`. A listing row can lag the record until the next
  write of that key (same model as the Xet `TreeStore`); reads are always
  correct.

## Multi-replica consistency

In a Postgres deployment, multipart identity, quotas, part maps, expiry, and
completion fences are durable database state. Incomplete part bytes are immutable
private objects in the configured object store. A multipart request may therefore be
retried against any API replica without a shared filesystem. Local SQLite deployments
retain bounded filesystem staging as their explicitly single-node implementation.

The S3 frontend keeps a **per-key in-process lock**
(`acquire_object_upload_lock`: a weak-valued `HashMap` of Tokio mutexes) to
avoid waste within one process. Correctness does not depend on that lock.

Conditional `PUT` and `CopyObject` use the S3 listing row as their database
linearization point:

1. read the exact `(scope_namespace, object_key)` row and evaluate
   `If-Match` / `If-None-Match`;
2. stream and commit the immutable content record;
3. atomically compare-and-swap the row from the observed value to the new
   value (or insert only if absent).

SQLite performs the compare-and-swap in a transaction. Postgres performs it
as one conditional `UPDATE` or `INSERT ... ON CONFLICT DO NOTHING`. Therefore
two replicas cannot both win `If-None-Match: *`, and a stale `If-Match` writer
cannot replace a row advanced by another replica. The loser returns
`412 PreconditionFailed`; its just-created record is removed when it is still
the latest alias, otherwise it is an unreachable version reclaimed by GC.

Unconditional overwrites and multipart completions deliberately retain S3's
last-writer-wins behavior. Concurrent deletes and unconditional writes are
ordered by their atomic metadata-row operations; reads pin the row's immutable
record version, so they never combine one version's bytes with another
version's ETag or metadata.

Checked-in adapter tests race independent SQLite connections and independent
Postgres store handles and require exactly one conditional winner. The
Postgres test runs when `DATABASE_URL` is available in the integration
environment.

## References

- `docs/DUCKDB.md` · external DuckDB workflow and query safety gate
- Issue #15 · design review (oracle, 2026-08-13)
- `docs/ARCHITECTURE.md` · `docs/COMPATIBILITY_STATUS.md`

> **Internals note:** the S3 listing index is backed by the `shardline_s3_objects`
> table in the configured index store (SQLite or Postgres). The table name is an
> implementation detail; operators and user documentation refer to it as the S3
> listing index.

`UploadPartCopy` is implemented for same-bucket sources, optional explicit byte ranges, and source conditions. It pins the source record version before reading, verifies the selected bytes, and publishes the part through the same durable quota and session path as `UploadPart`. A process-kill drill verifies that an interrupted copy does not expose a partial part and that the session can be retried after restart. Multipart operations require exactly their supported recognized operation selectors; conflicting or repeated selectors are rejected before mutation. Unrecognized extension query fields remain ignored. `CopyObject` requires one valid `x-amz-copy-source` header; malformed or repeated fields return `400 InvalidArgument` and preserve the destination.
