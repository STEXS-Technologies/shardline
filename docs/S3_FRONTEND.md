# S3 Frontend — Compatibility & Scope

**Status:** in development on `feat/s3-frontend` (targeting a post-1.5.0 release).
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
   capped at 10,000. No 5 MB minimum part size (documented deviation — harmless
   under dedup). `AbortMultipartUpload` discards the session.
4. **GetObject Range / ETag / errors.** Range reuses the existing byte-range +
   reconstruction path (`parse_http_byte_range`, 206/416 semantics). ETag is the
   **BLAKE3 root content hash** (opaque; identical for single-PUT and multipart
   of the same bytes). Errors are an S3 XML envelope (`<Error><Code>…`);
   codes: `NoSuchKey`, `NoSuchBucket`, `AccessDenied`, `InvalidRange`,
   `EntityTooSmall`, `InvalidPart`, `NoSuchUpload`, `NotImplemented`, `InternalError`.

## Operation matrix

### Supported (this branch)

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

### Planned (near-term follow-up)

| Operation | Why / trigger |
|---|---|
| `ListObjectsV2` | every directory/glob/discovery path (DuckDB glob, Polars, Spark listing, Iceberg catalog). **Gating decision:** key enumeration must be settled before the object-key layout freezes (`list_prefix` over `protocols/s3/{scope}/` vs a `protocol_key → file_id` index). |
| `DeleteObjects` (batch) | Iceberg GC, S3A bulk deletes |
| Conditional requests (`If-Match` / `If-None-Match`) | pyiceberg optimistic metadata commits, `object_store` conditional puts |
| `ListBuckets` | CLI tooling (`aws s3 ls`) |

### Out-until-demand (explicitly excluded; respond `501 NotImplemented`)

| Operation | Reconsider when |
|---|---|
| `CopyObject` | a Spark rename-commit or `object_store::copy` client path |
| `ListParts` / `ListMultipartUploads` | a multipart-resume workflow |
| `ListObjectsV1` | a legacy SDK (trivial V2 alias if ever needed) |
| Multi-range `GetObject` (`bytes=a,b`) | a client using multi-range |
| `PostObject` (form upload) | — |
| Object tagging / attributes / restore / select / torrent / legal-hold / retention / ACL | — |
| Bucket policy / lifecycle / versioning / CORS / notification / ACL / encryption / tagging / replication / website, `DeleteBucket`, `ListBucketMultipartUploads`, `ListObjectVersions` | explicit ruling: no admin/AWS-side surface |
| Service `GET /` | — |

## Protocol deviations (documented)

- **ETag** is the BLAKE3 root hash, not MD5 or the multipart composite — opaque
  per the S3 spec; all six target clients treat it as opaque.
- **No 5 MB minimum part size** (harmless under content-addressed dedup).
- **SigV4 signature is not verified** — the access key *is* the credential;
  production requires TLS.
- **Bucket names** are `{owner}.{name}`; owners containing `.` are not addressable.
- **`Content-MD5`** is accepted but not verified (integrity comes from the content
  address).

## Auth & roles

- `access_key` = Shardline bearer token scoped to the bucket's `owner.name`;
  binding mismatch → `403 AccessDenied`; undecodable bucket → `404 NoSuchBucket`.
- S3 is an **API-tier** frontend (reads + writes touch records/ingest); a
  transfer-only role does not serve S3.

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
- **The S3 listing index (`shardline_s3_objects`) is a derived snapshot, GC
  inert, and not a reachability source** — deleting a listing row never touches
  chunks or records. Delete ordering is crash-safe: index row first, then
  record+object (`delete_object_if_present`).
- **Listing nuance** — `ListObjectsV2` serves the snapshot (size/hash/mtime from
  the index row); `HeadObject`/`GetObject` always resolve through the
  authoritative `FileRecord`. A listing row can lag the record until the next
  write of that key (same model as the Xet `TreeStore`); reads are always
  correct.

## References

- Issue #15 · design review (oracle, 2026-08-13)
- `docs/ARCHITECTURE.md` · `docs/COMPATIBILITY_STATUS.md`
