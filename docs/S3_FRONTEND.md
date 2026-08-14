# S3 Frontend — Compatibility & Scope

**Status:** implemented on `feat/s3-frontend` — every operation in the
SUPPORTED matrix below is implemented and covered by the client-shaped e2e
suite (`crates/shardline-server/tests/s3_frontend_e2e.rs`); targeting a
post-1.5.0 release.
**Tracking issue:** #15.
**Design review:** oracle, 2026-08-13.

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
   The SigV4 signature is **not verified** (the access key *is* the credential;
   requires TLS in production — documented deviation). A plain
   `Authorization: Bearer` is also accepted for tooling that supports it.
3. **Multipart + CDC.** `UploadPart` bodies stream to per-part temp files;
   `CompleteMultipartUpload` feeds the parts sequentially through **one CDC
   pass**, producing a single `FileRecord` — preserving whole-object dedup and
   the single-record read model (GetObject + Range work unchanged). Part count
   capped at 10,000. S3's **5 MiB minimum part size** applies to every part
   except the final one (enforced at Complete, matching S3); per-session and
   aggregate byte quotas (`SHARDLINE_S3_UPLOAD_SESSION_MAX_BYTES` /
   `SHARDLINE_S3_UPLOAD_TOTAL_MAX_BYTES`) bound disk use; session expiry is
   anchored to **initiation** (keep-alive parts do not extend it), matching
   S3's 7-day multipart lifecycle. `AbortMultipartUpload` discards the session.
4. **GetObject Range / ETag / errors.** Range reuses the existing byte-range +
   reconstruction path (`parse_http_byte_range`, 206/416 semantics). ETag is the
   **BLAKE3 root content hash** (opaque; identical for single-PUT and multipart
   of the same bytes). Errors are an S3 XML envelope (`<Error><Code>…`);
   codes: `NoSuchKey`, `NoSuchBucket`, `AccessDenied`, `InvalidRange`,
   `EntityTooSmall`, `InvalidPart`, `NoSuchUpload`, `NotImplemented`,
   `PreconditionFailed` (conditional mismatches), `MalformedXML`, `InternalError`.

## Operation matrix

### Supported (this branch)

All operations in this table are **implemented** and exercised by the
client-shaped e2e suite (`crates/shardline-server/tests/s3_frontend_e2e.rs`):
pyarrow (PUT/GET/Range/multipart), Polars / object_store (HeadObject),
S3A (bucket probes), and DuckDB (ListObjectsV2) shapes, over both the SigV4
access-key and Bearer auth forms.

| Operation | Notes |
|---|---|
| `PutObject` | streamed through the CDC ingestor; body ≤ `SHARDLINE_S3_MAX_PART_BYTES` (default 1 GiB); larger must use multipart |
| `GetObject` (+ `Range`) | existing range path; `bytes=a-b`, `bytes=a-`, `bytes=-N`; 206 / 416 |
| `HeadObject` | size + ETag |
| `DeleteObject` | removes direct object **and** record (S3 semantics) |
| `CreateMultipartUpload` | `POST /{key}?uploads` |
| `UploadPart` | `PUT ?partNumber&uploadId` → part file |
| `CompleteMultipartUpload` | single CDC pass → record + ETag |
| `AbortMultipartUpload` | `DELETE ?uploadId`; discards session |
| `HeadBucket` | stub (S3A / object_store connect-probe) |
| `GetBucketLocation` | stub → `us-east-1` (S3A / pyarrow region-probe) |
| `CreateBucket` | no-op `200` (S3A missing-bucket probe) |
| `ListObjectsV2` | index-backed; `prefix`/`delimiter`/`max-keys`/`continuation-token`/`start-after`; zero object-store reads — the index rows carry size/ETag/mtime |
| `DeleteObjects` (batch) | `POST /{bucket}?delete=`; ≤ 1000 distinct keys per request (`MalformedXML` beyond, `400`); invalid keys become per-key `<Error>` rows |
| `CopyObject` | `PUT` with `x-amz-copy-source`; source must be in the caller's bound bucket; dest gets a fresh ETag (same content → same ETag) |
| Conditional requests | `If-Match` / `If-None-Match` on Get/Put/Head/Delete; `412 PreconditionFailed` on mismatch (`404 NoSuchKey` when `If-Match` targets a missing object) |
| `ListBuckets` | service-level `GET /`; lists the caller's single `{owner}.{name}` bucket |

### Planned (near-term follow-up)

None — the originally planned operations (`DeleteObjects`, conditional
requests, `ListBuckets`) are all supported; `CopyObject` was pulled in ahead
of schedule (Spark rename-commit / `object_store::copy` shape).

### Out-until-demand (explicitly excluded; respond `501 NotImplemented`)

| Operation | Reconsider when |
|---|---|
| `ListParts` / `ListMultipartUploads` | a multipart-resume workflow |
| `ListObjectsV1` | a legacy SDK (trivial V2 alias if ever needed) |
| Multi-range `GetObject` (`bytes=a,b`) | a client using multi-range |
| `PostObject` (form upload) | — |
| Object tagging / attributes / restore / select / torrent / legal-hold / retention / ACL | — |
| Bucket policy / lifecycle / versioning / CORS / notification / ACL / encryption / tagging / replication / website, `DeleteBucket`, `ListBucketMultipartUploads`, `ListObjectVersions` | explicit ruling: no admin/AWS-side surface |

## Protocol deviations (documented)

- **ETag** is the BLAKE3 root hash, not MD5 or the multipart composite — opaque
  per the S3 spec; all six target clients treat it as opaque.
- **SigV4 signature is not verified** — the access key *is* the credential;
  production requires TLS.
- **Bucket names** are `{owner}.{name}`; owners containing `.` are not addressable.
- **`Content-MD5`** is accepted but not verified (integrity comes from the content
  address).
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

The server must run with the Local auth provider and a signing key; the CLI
must mint with the **same** key material.

- Server: `SHARDLINE_AUTH_PROVIDER=local` plus either
  `SHARDLINE_TOKEN_SIGNING_KEY=<32+ byte key>` **or**
  `SHARDLINE_TOKEN_SIGNING_KEY_FILE=/path/to/key` (the file form strips one
  trailing line terminator — the standard `echo $KEY > file` artifact).
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
  env/file source must resolve to the same bytes — when the key lives in a
  file, both sides strip the trailing newline identically.

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
  reachability source** — deleting a listing row never touches chunks or
  records. Delete ordering is crash-safe: index row first, then record+object.
- **Listing nuance** — `ListObjectsV2` serves the snapshot (size/hash/mtime from
  the index row); `HeadObject`/`GetObject` always resolve through the
  authoritative `FileRecord`. A listing row can lag the record until the next
  write of that key (same model as the Xet `TreeStore`); reads are always
  correct.

## References

- Issue #15 · design review (oracle, 2026-08-13)
- `docs/ARCHITECTURE.md` · `docs/COMPATIBILITY_STATUS.md`

> **Internals note:** the S3 listing index is backed by the `shardline_s3_objects`
> table in the configured index store (SQLite or Postgres). The table name is an
> implementation detail; operators and user documentation refer to it as the S3
> listing index.
