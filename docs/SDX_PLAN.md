# SDX — Xet Client Library and CLI: Implementation Plan

Status: Draft implementation plan for [issue #19](https://github.com/STEXS-Technologies/shardline/issues/19) —
under review on branch `feat/sdx-native-file-management-cli`; **no implementation started**.
Companion design doc: `docs/XET_NATIVE_CLI.md` (CLI behavior, URL scheme, config, phased stubs).

## 1. Goal and scope

`sdx` is a **library-first** Rust client for the Xet protocol, plus a thin `sdx` CLI.
It must:

1. Work fully against the **shardline Xet frontend** (all features the server exposes).
2. Support **all features of the Xet frontend protocol** as implemented by the
   reference client stack (`huggingface/xet-core`: `xet-client`, `xet-data`,
   `xet-core-structures`) — i.e. talk to **any Xet-compatible frontend**, not just
   shardline (e.g. XetHub/HF CAS, openxet, future servers).
3. Be a **library** (`crate sdx`) that users depend on directly to write programs
   that connect to a shardline Xet frontend or any other Xet frontend, with
   **authentication included**.
4. Handle **rate limiting and overload responses** (429/503/Retry-After) with a
   configurable retry/backoff policy exposed to library users.
5. Provide the `sdx` CLI (`cp`/`sync`/`ls`/`rm`/`cat`/`info`/`branch`) as a thin
   wrapper, dispatched via symlink `argv[0]` (single `shardline` binary, per
   `docs/XET_NATIVE_CLI.md`).
6. Support **streaming and in-memory data**: stream bytes to/from the backend
   (`AsyncRead`/`AsyncWrite`), and exchange raw buffers (`Bytes`) for non-file
   payloads (e.g. vector binaries, embeddings, generated blobs) without touching
   disk.

Naming: `sdx` (library + CLI name). The names one would reach for first are
already taken in the ecosystem, so `sdx` is the chosen crate/CLI name.
Everything in this repo, `docs/XET_NATIVE_CLI.md`, and issue #19 uses `sdx`.

## 2. Current state (what already exists)

The **server side** of the Xet frontend is complete and protocol-conformant. The
**client side does not exist at all** in this workspace — no Xet client library,
no transfer commands, no XorbWriter/XorbReader client API.

### 2.1 Server endpoints the client will talk to

Registered in `crates/shardline-server/src/app.rs` (`register_xet_routes`,
`app.rs:391-417`); route constants in
`crates/shardline-xet-adapter/src/frontend.rs:5-8`.

**Token issuance (API role):**

| Route | Method | Purpose |
|---|---|---|
| `/api/{provider}/{owner}/{repo}/xet-read-token/{rev}` | GET | Read-scoped CAS token; `?subject=`; returns `{casUrl, exp, accessToken}` (`app/provider_routes.rs:79-98`) |
| `/api/{provider}/{owner}/{repo}/xet-write-token/{rev}` | GET | Write-scoped CAS token (`provider_routes.rs:101-120`) |

**Metadata:**

| Route | Method | Purpose |
|---|---|---|
| `/reconstructions`, `/v1/reconstructions` | GET | Batch reconstruction (`?file_id=`, max 1024 ids) |
| `/v1/reconstructions/{file_id}` | GET | v1 reconstruction (`?content_hash=`, `Range:`) |
| `/v2/reconstructions/{file_id}` | GET | v2 response (multi-range fetch descriptors; single range per xorb in practice — v2 wraps v1) |
| `/shards`, `/v1/shards` | POST | Shard upload + atomic file registration; `{result: 0\|1}` |

**CAS data plane:**

| Route | Method | Purpose |
|---|---|---|
| `/v1/chunks/default/{hash}` | GET | Raw dedupe-shard bytes (`Accept-Ranges: bytes`) |
| `/v1/chunks/default-merkledb/{hash}` | GET | Global dedup query (404 on miss) |
| `/v1/xorbs/default/{hash}` | HEAD | Xorb existence/`Content-Length` |
| `/v1/xorbs/default/{hash}` | POST | Upload serialized xorb; `{was_inserted: bool}` |
| `/transfer/xorb/{prefix}/{hash}` | GET | Ranged xorb download; **`Range:` required**; 206 + `Content-Range`; prefix must be `default` |
| `/transfer/xorb/{prefix}/{hash}` | PUT | Xorb upload (what `git-xet` uses) |

### 2.2 Server auth model

- Bearer tokens: `Authorization: Bearer <token>`; token max 16 384 bytes
  (`shardline-protocol/src/token.rs:15`).
- Token envelope is **not JWT**: `hex(JSON payload) "." hex(signature)` with
  `TokenClaims {issuer, subject, scope: Read|Write, repository, expires_at}`;
  `Write ⊃ Read` (`shardline-protocol/src/token.rs:170-233`).
- Pluggable providers: `LocalHmacProvider` (HMAC-SHA256), `Ed25519AuthProvider`
  (asymmetric), `PassthroughProvider`, `OidcProvider`/`JwksProvider`
  (`crates/shardline-auth/src/lib.rs:37-52`).
- Token issuance auth: provider bootstrap key header `x-shardline-provider-key`;
  subject from `?subject=` → `x-shardline-provider-subject` → Basic username
  (`app/provider.rs:35,178-280`).
- Errors: 401 missing/invalid token, 403 insufficient scope (`auth.rs:146-151`).

### 2.3 Server chunking/compression facts the client must match

- CDC chunker: FastCDC gear-hash, **byte-identical** to
  `xet-data::deduplication::Chunker` (`cdc.rs:281-415`); target 64 KiB default,
  min = target/8, max = target*2.
- Xorb format (`crates/shardline-xet-core/src/xorb_object/`): per-chunk header 8
  bytes (`version:u8 | compressed_len:3B | scheme:u8 | uncompressed_len:3B`);
  schemes `None|LZ4|ByteGrouping4LZ4|Auto`; object ident `"XETBLOB"`; format v2
  boundary/hash blocks.
- Hashing: chunk = **BLAKE3 keyed** with `DATA_KEY`; path hex = 64 lowercase hex
  chars, each 8-byte group byte-reversed ("Xet CAS API hexadecimal ordering",
  `shardline-index/src/xet_hash.rs:14-54`).
- Server validates xorb hash on upload, accepts footer-less xorbs, idempotent
  uploads (`was_inserted: false` on existing).
- `HEAD /v1/xorbs` → `Content-Length` = stored xorb size; ranged GET → 206 +
  `Content-Range` (ranges end-inclusive per `PROTOCOL_CONFORMANCE.md:87-102`).

### 2.4 Reference client stack (what "full Xet frontend support" means)

From `huggingface/xet-core` (shardline's workspace declares
`xet-client`/`xet-data`/`xet-core-structures` 1.5.1, and `Cargo.lock` resolves
**1.5.2** as registry dependencies — not vendored; current upstream is 1.5.4,
published **without a semver promise** — target the **wire protocol**, not
crate internals):

- `xet-client`: `cas_client::Client` trait — `get_reconstruction` (v2 with v2→v1
  fallback), `batch_get_reconstruction`, `get_file_reconstruction_info`,
  `get_file_term_data` (signed-URL xorb fetch), `query_for_global_dedup_shard`,
  `upload_shard`, `upload_xorb`, `get_file_chunk_hashes`,
  `acquire_download_permit`/`acquire_upload_permit`; `RemoteClient` is the
  production impl. Session/request correlation headers `SESSION_ID_HEADER`,
  `REQUEST_ID_HEADER`.
- `xet-data`: `deduplication` (Chunker, FileDeduper, DataAggregator),
  `file_reconstruction` (FileReconstructor, DownloadStream, UnorderedDownloadStream,
  memory-limited buffering, adaptive prefetching), `processing` (FileUploadSession,
  FileDownloadSession, CasClient trait).
- `xet-core-structures`: MerkleHash (256-bit), metadata_shard (MDBShardFile —
  48-byte header, file info section, CAS info section, 200-byte footer **omitted
  from upload body**), xorb_object, CompressionScheme.
- `hf-xet`: `XetSession`/`XetSessionBuilder` high-level sessions with
  `with_endpoint`, `with_token_info`, `with_token_refresh_url`,
  `with_custom_headers`.

### 2.5 Server additions required for path addressing (part of issue #19)

Audit finding (2026-08): the Xet frontend addresses content **only by 64-hex
`file_id`** — there is no path→hash resolution, no listing, no branch, no
deregistration endpoint. Per decision, issue #19 **includes server-side
metadata endpoints** — the only server changes in this issue:

| Capability | Endpoint (shape TBD, see §2.5) | Notes |
|---|---|---|
| Path→file_id resolution | `GET .../tree/{rev}?path=...` | Resolves `xet://.../rev/path` to a 64-hex `file_id` for reconstruction; read-token auth; revision-scoped |
| Listing | `GET .../tree/{rev}?prefix=...` | Directory listing for `ls` and `sync`; pagination |
| Deregistration | `DELETE .../path/{rev}/{path}` | `rm`; semantics vs immutable CAS (mark-deleted vs remove record; GC interaction per `docs/reachability-model.md`) |
| Branch/revision management | `GET/POST/DELETE .../revisions` | `branch --create/--delete`, `ls --branches`; depends on shardline's provider-scoped revision model (§11 Q4) |

Exact routes/shapes are designed alongside the metadata endpoints and client
modules `tree.rs` /
`revisions.rs`. Interop note: this path namespace is shardline-specific
(upstream resolves paths through git trees, which shardline does not serve);
the client's file_id-level operations remain portable (§4.4.1).

## 3. Feature parity matrix

| Feature | Official client surface | shardline server | sdx status |
|---|---|---|---|
| CDC chunking (gear-hash, 64 KiB target, 8–128 KiB bounds, mask `0xFFFF000000000000`) | `xet-data::deduplication::Chunker` | identical `CdcChunker` (server-internal only) | **missing** — client-side chunker needed |
| BLAKE3 keyed hashing + Xet hex (byte-group reversal) | `xet-core-structures::merklehash` | `xet_hash.rs` + golden vectors | **missing** |
| Xorb build/serialize (8-byte headers, LZ4/BG4, footer-less accepted) | `xet-core-structures::xorb_object` | validates + normalizes | **missing** — `XorbWriter` client API |
| Xorb download + ranged fetch | `get_file_term_data`, 206/`multipart/byteranges` | `/transfer/xorb` 206 | **missing** — `XorbReader` client API |
| v2 reconstruction (multi-range fetch descriptors) | `get_reconstruction` v2→v1 fallback | `/v2/reconstructions/{file_id}` | **missing** |
| v1 reconstruction + batch | `get_reconstruction` v1, `batch_get_reconstruction` | `/v1/reconstructions`, batch | **missing** |
| Global dedup query (`/v1/chunks/default-merkledb/{hash}`) | `query_for_global_dedup_shard` | exists | **missing** |
| Shard build + upload (`/v1/shards`, fallback v1; `{result}`) | `upload_shard` | exists; v2 streaming not required | **missing** — `XorbStore`-adjacent client logic |
| File chunk hashes for partial updates (`X-Range-Dirty`) | `get_file_chunk_hashes` | not served (shardline has no `/v2/file-chunk-hashes`) | **defer** — only needed for in-place partial overwrite |
| Token issuance (read/write) + refresh | HF: `/api/{repo_type}s/{id}/xet-{read|write}-token/{rev}`; shardline: `/api/{provider}/{owner}/{repo}/xet-{read,write}-token/{rev}` | exists | **missing** — client token service |
| Rate limiting / overload handling | RetryWrapper: jittered exp backoff, `retry_max_attempts=5`, `retry_base_delay=3s`, `retry_max_duration=6min` | concurrency admission only (429/503, see §6) | **missing** — §6 |
| Session/request correlation headers | `SESSION_ID_HEADER`, `REQUEST_ID_HEADER` | tolerated | **missing** |
| Adaptive concurrency | `ac_max_healthy_rtt` etc. | n/a | **missing** — upload/download concurrency |
| Streaming download (bounded memory, prefetch) | `xet-data` `file_reconstruction`: `DownloadStream`/`UnorderedDownloadStream`, `DataWriter`/`SequentialWriter`/`UnorderedWriter`, `FileReconstructor` | ranged `/transfer/xorb` 206 | **missing** — §4.4 |
| Streaming upload (incremental, no full-file buffering) | `xet-data` `processing`: `FileUploadSession`/`SingleFileCleaner`/`FileDeduper`; `hf-xet` `XetUploadStreamHandle` | chunked xorb upload (≤64 MiB body default) | **missing** — §4.4 |
| In-memory payloads (`Bytes`, no disk) | `hf-xet` `XetUploadStreamHandle::write(Bytes)` + `download_to_bytes` | n/a (CAS is buffer-agnostic) | **missing** — §4.4 |
| Credential/config management | `XetConfig`, token files | n/a | **missing** — §5.2 |
| CLI cp/sync/ls/rm/cat/info/branch | pyxet-style surface (deprecated upstream) | n/a | **missing** — thin wrapper |

**Conclusion:** every client-side capability in the table above is **missing**;
there is no prior client code in this workspace. The CAS/data-plane server
routes need **no changes**; the only server work in issue #19 is the **new
path/metadata endpoints** for path addressing (§2.5). `/v2/file-chunk-hashes`
remains deferred (not part of #19).

## 4. Architecture

### 4.1 Crate layout

New crate: **`crates/sdx`** (package name `sdx`). Dependencies:

- `shardline-protocol` — token types, hash types, ByteRange, SecretString
- `shardline-xet-adapter` — XorbWriter/XorbReader/XorbStore contracts (add
  client-facing constructors if the adapter stays clean; otherwise the sdx crate
  implements them); reconstruction response building lives here
  (`reconstruction.rs`), **not** `shardline-server-core`
- `shardline-validation` — `ShardMetadataLimits` and validation limits (the
  `shardline-server-core` dependency in earlier drafts was wrong)
- `xet-core-structures` (**pinned exact version**, per §11 Q3/Q6) — Merkle hash,
  `xorb_object` (`ByteGrouping4LZ4`), chunk-format constants; do **not**
  reimplement BG4
- `shardline-storage` — ObjectStore trait (contract only, not embedded)
- `tokio`, `reqwest` (HTTP), `lz4_flex`, `blake3`, `clap` (CLI only), `serde`

Must **not** depend on `shardline-server`.

**Chunker source decision:** the server's `CdcChunker`
(`shardline-server/src/upload_ingest/cdc.rs`) is the one crate sdx is forbidden
to depend on. sdx therefore uses the **upstream `xet-data` `Chunker`/`gearhash`**
(dependency of the pinned `xet-core-structures`) — the same code the server
claims byte-identity with — and proves byte-identity via E2E tests, not via
shared code.

CLI surface lives in the existing CLI binary crate (`crates/shardline`; issue #19's
`crates/cli` refers to this same surface), routed by `argv[0]` symlink detection per
`docs/XET_NATIVE_CLI.md` (dispatch model). The CLI module adds the `xet` subcommand
tree as a thin clap wrapper over `sdx`.

### 4.2 Module map (library)

```
crates/sdx/src/
  lib.rs            — re-exports, crate docs
  client.rs         — XetClient builder + handle (endpoint, auth, retry policy, concurrency)
  auth.rs           — token issuance (read/write), refresh w/ 30s buffer, credential sources
  chunk.rs          — client-side CDC chunker (FastCDC gear-hash, 64 KiB target)
  hash.rs           — BLAKE3 keyed hashing, Xet hex conversion (byte-group reversal)
  xorb.rs           — XorbWriter/XorbReader (build, serialize, parse, ranged fetch)
  shard.rs          — shard build/upload (file registration, metadata)
  reconstruction.rs — v1/v2 reconstruction orchestration, range handling, fallback
  stream.rs          — pull-based DownloadStream/UnorderedDownloadStream, DataWriter trait (Sequential/Unordered), byte-denominated buffer semaphore, RunState cancellation/error propagation
  cache.rs           — on-disk chunk cache ((prefix, xorb_hash) + first chunk range; persisted across files)
  tree.rs            — path→file_id resolution + listing via the §2.5 server metadata endpoints
  revisions.rs       — branch/revision list/create/delete via the §2.5 endpoints
  dedup.rs           — global dedup query, eligibility (first chunk; last-8-bytes-LE % 1024 == 0; ≥256-chunk spacing ≈ 16 MiB at 64 KiB target)
  transfer.rs       — HTTP layer: xorb upload/download (streaming bodies + explicit CONTENT_LENGTH), chunk GET, 206 handling, multipart/byteranges
  retry.rs          — RetryPolicy, jittered exponential backoff, error classification
  config.rs         — shardline.toml config, credential caching, env overrides
  session.rs        — UploadSession / DownloadSession (high-level file ops), stream groups, abort/status
  error.rs          — sdx::Error, retryable classification
```

### 4.3 Public API sketch

> **Amended to match the implemented API.** The upload first argument is the
> remote **path**, not a file_id: `upload_file(local_path, remote)` /
> `upload_bytes(remote, bytes)` / `upload_stream(remote, reader)`, all of which
> register the uploaded file under `remote` at `finalize`. Downloads are
> **file_id**-addressed: `download_file(file_id, dest)` /
> `download_stream(file_id, range)` / `download_range(file_id, range, dest)`.
> A `xet://…/rev/path` is resolved to its file_id via `resolve_path` (§2.5);
> `register_path(remote, file_id)` exposes the same path→file_id mapping
> directly.

```rust
use sdx::{XetClient, XetClientBuilder, RetryPolicy, UploadSession, DownloadSession, Auth};

// Library users write programs against any Xet frontend:
let client = XetClientBuilder::new()
    .endpoint("xet://host/provider/owner/repo/main")   // shardline-style URL
    .auth(Auth::token(read_or_write_token))            // or Auth::refresh_url(...)
    .retry(RetryPolicy::default().max_attempts(5).base_delay_ms(3000))  // user-tunable
    .download_concurrency(16)
    .build()
    .await?;

// Upload: the first argument is the remote *path*, not a file_id.
let session = client.upload_session().await?;
session.upload_file("local.bin", "remote.bin").await?;   // upload_file(local_path, remote)

// Download is file_id-addressed: resolve a xet://…/rev/path to its file_id first.
let path_entry = client.resolve_path("remote.bin").await?;   // §2.5 tree store
let file_id = &path_entry.file_id;
let session = client.download_session().await?;
session.download_file(file_id, "local.bin").await?;
session.download_range(file_id, 0..1024, "head.bin").await?;

// Streaming + in-memory (no temp files; mirrors xet-data pull-based streams).
// Library core is file_id-addressed; `cp`/`ls`/`cat` resolve xet://…/rev/path
// via the §2.5 server metadata endpoints (tree.rs/revisions.rs).
let mut stream = client.download_stream(file_id, None).await?;       // or Some(0..64GiB)
while let Some(chunk) = stream.next().await? { out.write_all(&chunk)?; } // cat pattern
let bytes: Bytes = client.download_bytes(file_id).await?;
// Streaming uploads feed a reader (`upload_stream(remote, reader)`); the
// push-style in-memory handle is `upload_stream_handle()` (write/finish).
let mut upload = client.upload_session()?.upload_stream_handle();
upload.write(block).await?;                                          // owned Bytes, 8 MiB slices
upload.finish().await?;
client.upload_stream("remote.bin", reader).await?;   // upload_stream(remote, reader)
client.upload_bytes("remote.bin", &bytes).await?;    // upload_bytes(remote, bytes)
```

Session types mirror the reference `XetSession`/`FileDownloadSession` concepts
(`xet_pkg/src/xet_session/session.rs`): upload commit, download group, download
stream group, abort/status.

### 4.4 Streaming and in-memory data

**Guiding principle: mirror upstream `xet-data` 1.5.4 / `hf-xet` mechanics
exactly, then map each call onto shardline's Xet frontend.** The design below
is a direct translation of the verified registry sources
(`xet-data-1.5.4/src`, `hf-xet-1.5.4/src`, `xet-client-1.5.4/src`), not an
original invention. Goal: stream files of **hundreds of gigabytes** with
**memory bounded independent of file size** — the file is chunked incrementally
and streamed out (e.g. to stdout/socket), never buffered whole in RAM.

#### 4.4.1 Download: pull-based stream with byte-denominated memory cap

Mirror `xet-data/src/file_reconstruction/` (`file_reconstructor.rs`,
`reconstruction_terms/`, and `data_writer/` — which contains
`download_stream.rs`, `unordered_download_stream.rs`,
`sequential_writer.rs`, `unordered_writer.rs`).

- **API shape is pull-based, not `AsyncRead`**: `DownloadStream` has
  `next()` (async) / `blocking_next()` (sync, usable from CLI threads)
  returning `Option<Bytes>` (`Ok(None)` = EOF or cancellation). The consumer
  pulls `Bytes` chunks one at a time and forwards them to a socket/stdout.
  `UnorderedDownloadStream::next()` yields `(u64 file_offset, Bytes)` in
  completion order, with progress probes (`total_bytes_expected()`,
  `bytes_in_progress()`, `bytes_completed()`).
- **Memory bound = byte-denominated adjustable semaphore** (mirror
  `reconstruction_download_buffer`): default `download_buffer_size = 2 GiB`
  (base), `per-file = 512 MiB`, hard `limit = 8 GiB`, scaled per active
  download (`increment_permits_to_target` on enter, shrink via guard on exit).
  Every in-flight term carries a byte-permit (`acquire_many(term_size)`) that
  is released **only after the consumer actually consumed those bytes** —
  so in-flight buffered bytes ≤ semaphore capacity, and sdx exposes the same
  knob for memory-constrained clients (e.g. `2 MiB` for tiny runners).
- **Pipeline**: `FileReconstructor::new(client, file_hash)` (builder:
  `.with_byte_range`, `.with_chunk_cache`, `.with_buffer_semaphore`,
  `.with_cancellation_token`) → background task runs (**spawned at
  construction, paused; auto-starts on the first `next()`/`blocking_next()`** —
  mirror `download_stream_handle.rs`; a dropped handle still spawned work, so
  `Drop` must cancel):
  1. `ReconstructionTermManager` **prefetches term-metadata blocks**
     (`GET /v1|v2/reconstructions/{file_id}` with `Range` header) keeping
     `prefetched_pos - active_pos ≥ min_prefetch_buffer` (default 1 GiB),
     block sizes clamped `[min_reconstruction_fetch_size, max_reconstruction_fetch_size]`
     (defaults 256 MiB / 8 GiB; estimator-driven, not fixed).
  2. For each term: xorb chunk ranges computed from the reconstruction block
     (dedup map `(xorb_hash, first_chunk_start) → XorbBlock` shares blocks
     across terms); chunk cache checked first (on-disk, key
     `(prefix, xorb_hash) + first chunk range`).
  3. Miss → `acquire_download_permit()` (CAS connection permit, adaptive
     controller: initial 4, min 1, max 64, `ac_max_healthy_rtt = 90s`) →
     **ranged `GET /transfer/xorb/{prefix}/{hash}`** with
     `Range: bytes=S-E` (single range per request by default;
     `enable_multirange_fetching=false`) → 206 body **streaming-decompressed**
     into one `Bytes` (sized via `uncompressed_size_if_known`); multi-range
      responses parse `multipart/byteranges`. **403 → refresh URLs**: on
      shardline the reconstruction-provided fetch URL never changes, so 403
      means token expiry → single-flight token refresh (§5.2), then retry the
      same URL. Best-effort async chunk-cache put.
  4. `FileTerm::extract_bytes` slices the xorb data **zero-copy**
     (`Bytes::slice`) into per-file-term bytes; the reconstruction loop hands
     `(relative_byte_range, buffer_permit, data_future)` to the writer.
- **DataWriter abstraction** (mirror `data_writer.rs`): trait
  `set_next_term_data_source(byte_range, Option<permit>, data_future)` +
  `finish() -> u64`. `SequentialWriter`: background thread enforces strict
  byte-range contiguity, `write_all`/vectorized `write_vectored` (≤24 iovecs),
  then `flush()`; **only then the buffer permit is released**. `UnorderedWriter`:
  completion order, explicit offsets. `reconstruct_to_writer<W: Write>` runs any
  `std::io::Write` sink (file, stdout) on `spawn_blocking`.
- **Cancellation**: `RunState` — `cancel()`/`Drop` aborts promptly; errors
  surface via `check_error()` at item boundaries; `abort_active_streams()`
  fires all registered stream callbacks.
- sdx public surface:
  `download_stream(file_id, Option<Range>) -> DownloadStream`,
  `download_unordered_stream(...)`, `download_to_writer(writer)`,
  `download_bytes() -> Bytes` — CLI `cat` is exactly upstream `xtool`
  `download.rs`: loop `while let Some(chunk) = stream.next()` →
  `stdout().write_all(&chunk)`.

#### 4.4.2 Upload: push-style incremental ingest, chunked on the file

Mirror `xet-data/src/processing/` (`file_upload_session.rs`, `file_cleaner.rs`,
`deduplication/file_deduplication.rs`, `deduplication/data_aggregator.rs`) and
`hf-xet/xet_session/upload_stream_handle.rs`.

- **Incremental primitive is push-style**: `SingleFileCleaner::add_data(Bytes)`
  / `add_data_from_bytes` repeatedly, then `finish()`; file uploads are a thin
  loop over it reading **8 MiB `ingestion_block_size` blocks** — the file is
  chunked on the file, never buffered whole. sdx exposes the same primitive
  (`upload_stream_handle.write(Bytes)` / `finish()` mirror), which is exactly
  the in-memory/vector-binary path: feed `Bytes`/`Vec<u8>` directly, no file.
- **CDC on a compute thread** (`spawn_blocking` → gear-hash chunker, 64 KiB
  target / 8–128 KiB bounds, zero-copy `Bytes` slices within the 8 MiB block,
  partial trailing chunk buffered in `chunkbuf`); SHA-256 updated concurrently;
  dedup stage spawned as background task.
- **Dedup loop** (per chunk batch): local session/cache shard lookup first;
  if no hit and eligible (chunk index 0, or spaced ≥ `min_spacing_between_global_dedup_queries`
  = 256 chunks ≈ **16 MiB** at 64 KiB target — note: upstream 1.5.4 hardcodes
  this field to 0 / never wires the config, so sdx applies the documented
  default itself): background `query_for_global_dedup_shard` →
  **`GET` `/v1/chunks/default-merkledb/{hash}`** (GET, not POST — matches
  shardline `app.rs:406` and upstream `remote_client.rs:159`; a POST would 405)
  with **429-no-retry** and 404 = cache miss; returned shard imported, pass
  re-run once. Defrag-prevention
  hysteresis (min 8 chunks/range, 0.5 hysteresis, 128-range window).
- **Xorb cut/flush condition** (mirror exactly): `new_data_size + n > 64 MiB
  || new_data.len() + 1 > 8192` → `cut_new_xorb` → serialize on compute thread
  (footer-less) → `acquire_upload_permit()` (adaptive, initial 2, min 1,
  max 64) → **`POST /v1/xorbs/default/{hash}` with a streaming body**
  (512 KiB progress blocks, explicit `CONTENT_LENGTH`) from a `JoinSet` of
  parallel upload tasks. **Body-limit safety margin:** the 64 MiB cut applies
  to uncompressed data; serialized size adds 8-byte chunk headers + `XETBLOB`
  header + format-v2 boundary/hash blocks, so incompressible data can exceed
  shardline's 64 MiB body cap. sdx must cut on **serialized** size ≤ ~60 MiB
  (or serialize-and-check before upload) — add a worst-case E2E (incompressible
  data, ≥8192 tiny chunks).
- **finalize**: cut session tail via `DataAggregator` (64 MiB/8192), join all
  xorb uploads, then upload merged session shards → `POST /v1/shards`
  (footer-stripped, dedicated no-read-timeout client, one permit each), then
  move to cache dir.
- **RAM bound during upload**: one 8 MiB ingest block + one in-progress xorb
  (≤64 MiB) + one session tail aggregator (≤64 MiB) — **independent of file
  size**. Same discipline for in-memory payloads: a multi-GB `Bytes` is fed in
  8 MiB slices, never cloned whole.
- In-memory / non-file data (vector binaries, embeddings, generated blobs):
  `upload_bytes(remote, &bytes)` / `upload_stream(remote, reader)` are
  first-class APIs; the CAS layer is buffer-agnostic — only bytes matter, no
  path semantics.

#### 4.4.3 Session/stream-group layer

Mirror `hf-xet/xet_session/` (`download_stream_group.rs`,
`upload_stream_handle.rs`, `session.rs`):

- `new_download_stream_group()` builder: `.with_endpoint`,
  `.with_custom_headers`, `.with_token_info`, `.with_token_refresh_url` →
  `build()`; each group owns a `FileDownloadSession` and registers weak refs
  for `XetSession::abort()` (cancels runtime subtree + `abort_active_streams`).
- `XetUploadCommit::upload_stream` → handle with `write(Bytes)`/`finish()`
  fanning into the same cleaner pipeline.
- Status via `XetSession::status()` / `XetTaskState`; streams unregister on
  `Drop`.
- **Runtime trap (upstream, must document):** `blocking_next()` /
  `write_blocking()` **panic inside an async runtime** (upstream asserts
  no-runtime context); sdx needs the same task-runtime bridging (hf-xet
  `TaskRuntime`) so CLI threads can block safely, with a documented `#[cfg]`
  story.

#### 4.4.4 Streaming-path retry/timeout behavior (mirror + shardline delta)

- RetryWrapper defaults: `retry_max_attempts = 5`, `retry_base_delay = 3 s`,
  `retry_max_duration = 6 min`, exponential + jitter; 5xx/408/429 transient,
  other 4xx fatal, connect/timeout/`IncompleteMessage`/`Canceled` transient.
  **Semantics:** upstream passes `max_attempts` to `ExponentialBackoff::take`,
  i.e. 5 **retries** → up to 6 requests; sdx `RetryPolicy.max_attempts`
  means the same (retries, documented).
- `.with_429_no_retry()` only on dedup queries; `.with_retry_on_403()` only on
  ranged xorb fetch (URL refresh); `.with_expected_416()` on reconstruction
  (past-EOF → `Ok(None)`); `.with_expected_404()` on dedup (cache miss).
- **Explicit sdx deltas:** the 401/403 token-refresh behavior (§6.3) is an
  addition — upstream treats 401/403 as fatal except the ranged-fetch 403 URL
  refresh; sdx's version is strictly more capable and must be marked as a
  delta in rustdoc so behavior differences are visible.
- **No `Retry-After` handling upstream** (pure config backoff) — shardline
  sends `Retry-After: 1` on 503s, so sdx adds honoring it (strictly better,
  still wire-compatible).
- Timeouts: read 300 s (resets per packet), connect 60 s, idle 60 s; shard
  uploads use the no-read-timeout client.
- `X-Xet-Session-Id` on every request; `X-Request-Id` read from responses for
  logging.
- Note: upstream `download_buffer_size = 2 GiB` default is generous; sdx keeps
  the same defaults but exposes them as tunables (§6.4), and the CLI `cat`
  path should run with a modest cap (e.g. 64–256 MiB) since it forwards to
  stdout in real time.

## 5. Authentication design

### 5.1 Token model (shardline)

- Tokens are opaque bearer strings to the client (the server validates them
  internally). The client must treat `accessToken` as opaque.
- Token issuance is **repo+revision-scoped** and split read/write:
  - `GET /api/{provider}/{owner}/{repo}/xet-read-token/{rev}` →
    `{casUrl, exp, accessToken}`
  - `GET /api/{provider}/{owner}/{repo}/xet-write-token/{rev}` → same shape
- Write ⊃ Read: use a write token for uploads (xorb POST, shard POST), read
  token for reconstruction/chunk/xorb GET.

### 5.2 Client-side auth service

- `Auth` sources, in priority order (mirror `docs/XET_NATIVE_CLI.md:268-291`):
  1. `--token` / `SHARDLINE_TOKEN`
  2. `--api-key` / `SHARDLINE_API_KEY` (provider bootstrap key → token issuance)
  3. `--token-file` / `SHARDLINE_TOKEN_FILE`
  4. config file `[auth]` section
- Transparent refresh: cache `accessToken` + `exp`; refresh when `now + 30s >
  exp` (reference client uses a 30-second buffer); refresh re-issues from the
  token endpoint.
- For **non-shardline frontends** (XetHub/HF): support the Hub exchange —
  `GET /api/{repo_type}s/{repo_id}/xet-{read|write}-token/{revision}` with the
  user's Hub token; the endpoint URL is configurable so users can point at any
  Xet-compatible token issuer.
- Concurrency: token refresh must be single-flight (one refresh in progress,
  others await the same future).

## 6. Rate limiting and overload handling

### 6.1 Does shardline have rate limiting?

**No — not time-window rate limiting.** Findings from server audit
(`crates/shardline-server`, `crates/shardline-oci-adapter`,
`crates/shardline-metrics`):

- **No token bucket / fixed or sliding window / per-IP or per-user limits.**
  Zero hits for `governor`, `ratelimit`, `tokio-rate-limit` in Cargo.toml/lock.
- What exists is **concurrency-based admission control**:
  - OCI token endpoint: semaphore (default 64 in-flight) → **429**
    (`protocol_routes/oci/token.rs:42-54`)
  - OCI upload sessions: active-session cap (default 1024) → **429**
    (`shardline-oci-adapter/src/session.rs:85-87`)
  - Transfer/download: `TransferLimiter` chunk-equivalent budget, 60s acquire
    timeout → **503** + `Retry-After: 1` (`transfer_limiter.rs:12-60`; nuance:
    `TransferLimiterClosed` is 503 **without** `Retry-After` — only
    timed-out/saturated paths carry the header, `error/server.rs:423-430`)
  - Weighted admission (`WeightedAdmission`, xorb=4/shard=8/reconstruction=16/
    batch=32) → **503** + `Retry-After: 1` (`admission.rs:11-81`)
  - Total request timeout 300s → **503** + `Retry-After: 1` (`app.rs:677-684`)
- **No `Retry-After` on 429** (only 503s carry `Retry-After: 1`).
- "Quotas" (per-tenant request/byte rate, storage quota) are listed as
  **required work, not implemented** (`SHARDLINE_PRODUCTION_READINESS.md:330-340`).

### 6.2 What the protocol requires

The Xet protocol spec says: *"Assume all APIs are rate limited. Lower your
request rate using a backoff strategy, then wait and retry."* — so a fully
Xet-compatible client **must** handle 429 from any frontend (XetHub, HF, future
servers), even though shardline does not emit time-window 429s today.

### 6.3 sdx retry/backoff design

- `RetryPolicy` struct, **user-configurable** via builder (see §4.3):
  - `max_attempts` (default 5), `base_delay_ms` (default 3000), `max_duration`
    (default 6 min) — matching the reference client defaults
    (`xet-runtime-1.5.4/src/config/groups/client.rs`)
  - `honor_retry_after: bool` (default true) — use `Retry-After` when present
  - `jitter: bool` (default true) — jittered exponential backoff
  - `retry_on_429: bool` (default true) — **except** dedup queries, which
    fail fast (reference client policy: `with_429_no_retry()` for
    `query_dedup_api`)
- Error classification:
  - **Retryable:** 429, 500, 503, 504, connection errors, timeouts
- **Non-retryable:** 400, 404, 416; 401/403 are **refresh triggers, not plain
  retries** (sdx delta — upstream treats them fatal): 401 → single-flight token
  refresh, retry once; 403 → re-issue with write token (or refresh the URL for
  signed-URL fetches), retry once — **loop-guarded: a repeated 403 on a
  write-token upload is surfaced, not re-issued infinitely**.
- 503 + `Retry-After: 1` → wait 1s; 429 without `Retry-After` → exponential
  backoff with jitter.
- Expose per-call override: `session.upload_file(...).retry(policy)` for users
  who need custom behavior.

### 6.4 Concurrency

- Default download/upload concurrency with adaptive scaling (reference:
  `ac_max_healthy_rtt = 90s`); expose fixed concurrency overrides
  (`download_concurrency(n)`, `upload_concurrency(n)`) so library users can
  tune, and honor shardline's admission limits naturally by retrying 503s.

## 7. Wire-level details to implement (conformance checklist)

Derived from `docs/PROTOCOL_CONFORMANCE.md` + HF Xet spec
(`huggingface.co/docs/xet/api`):

1. **Hash→path conversion:** 64 lowercase hex chars, each 8-byte group
   byte-reversed (little-endian u64, zero-padded) — `parse_xet_hash_hex`
   rejects uppercase/non-hex.
2. **Upload ordering:** all xorbs referenced by a shard MUST be uploaded before
   the shard POST; otherwise 400.
3. **Upload idempotency:** `POST /v1/xorbs/default/{hash}` returns `was_inserted`;
   a pre-existing xorb is not an error.
4. **Global dedup eligibility:** **global chunk index 0** always eligible (not
   the first chunk of every 8 MiB ingest batch); subsequent chunks eligible when
   last-8-bytes-LE `% 1024 == 0`, spaced ≥256 chunks (≈16 MiB at 64 KiB) from
   the last query — harmonized with §4.4.2.
5. **Range semantics:** reconstruction ranges end-inclusive; chunk-index ranges
   end-exclusive; xorb URL byte ranges end-inclusive; first term may include an
   offset into the first decoded chunk.
6. **206 handling:** single range → plain 206; multiple → `multipart/byteranges`;
   parse parts in order, deserialize chunks, validate `unpacked_length` per term.
7. **v2→v1 fallback:** if `/v2/reconstructions` returns 404/501, fall back to
   `/v1/reconstructions`. (No `/v2/shards` exists upstream — `upload_shard`
   only POSTs `/v1/shards`; drop the historical v2-shard fallback clause.)
8. **Footer-less xorbs:** server normalizes; client must also accept missing
   footers when parsing.
9. **Xorb size limits (upstream 1.5.4):** `TARGET_CHUNK_SIZE = 64 KiB`,
   `MAX_XORB_BYTES = 64 MiB`, `MAX_XORB_CHUNKS = 8192`, `XORB_BLOCK_SIZE = 64 MiB`
   (upstream; the 16 MiB/128 KiB values in shardline-xet-core are that crate's
   own legacy constants the client must **not** mirror). `XORB_BLOCK_SIZE` is
   only a sizing heuristic — the real cut is the 64 MiB/8192 condition
   (`file_deduplication.rs:242`), and sdx must respect the serialized-size
   safety margin (§4.4.2) against shardline's body limit.
10. **Body limits:** respect `SHARDLINE_MAX_REQUEST_BODY_BYTES` and per-endpoint
    bounds when uploading shards (metadata limits).
11. **Verification keys:** chunk = BLAKE3 keyed `DATA_KEY`; xorb = Merkle root
    with `INTERNAL_NODE_KEY` over `"{hex} : {size}\n"` lines; file = Merkle
    root then blake3 with 32 zero bytes; term verification = `VERIFICATION_KEY`
    over concatenated raw chunk-hash bytes.

## 8. CLI surface (`sdx`)

Thin clap wrapper over the library (per `docs/XET_NATIVE_CLI.md`, with the
`sdx` naming):

- `sdx cp <src> <dst>` — local ↔ remote (`xet://host/provider/owner/repo/rev/path`);
  path→file_id resolution via the §2.5 metadata endpoints
- `sdx sync <src> <dst>` — directory sync (push-only)
- `sdx ls <url>` (+ `--long`, `--branches`) — remote listing (via §2.5)
- `sdx rm <url>` (+ `--recursive`) — metadata deregistration (via §2.5)
- `sdx cat <url>` — stream remote file to stdout
- `sdx info <url>` — file/dir metadata
- `sdx branch <url> --create/--delete` — revision listing/creation/deletion
  (via §2.5)
- Flags: `--chunk-size` (default 64 KiB), `--compression none|lz4|bg4lz4`,
  `--recursive`, `--register/--no-register` (shard registration),
  `--token/--api-key/--token-file/--config`
- Config: `~/.config/shardline/shardline.toml` (`[default]` endpoint/provider/
  owner/repo/revision, `[auth]` token/api_key/token_file)
- Dispatch: `sdx` is a symlink to the `shardline` binary; `argv[0]` detection
  routes to the `xet` subcommand. Operator commands unchanged.
- Packaging: shell completions, manpage, release archive with `sdx` symlink
  (per `docs/XET_NATIVE_CLI.md:499-521`).

## 9. Work breakdown (milestones)

Mirrors `docs/XET_NATIVE_CLI.md` stub phases, with the library-first addition:

- **Crate scaffolding + hash/hex primitives.** `crates/sdx` skeleton,
  module map, BLAKE3 keyed hashing, Xet hex conversion, golden-vector unit tests
  (`shardline-index` vectors). No network.
- **Auth + token service.** Token issuance (read/write) against shardline
  token routes, refresh with 30s buffer, single-flight, credential sources,
  `Auth` builder. Unit tests with mock server.
- **Read path (download).** v2/v1 reconstruction client + fallback, ranged
  `/transfer/xorb` fetch, 206/multipart parsing, chunk deserialization,
  `unpacked_length` validation, file assembly, `DownloadSession` + `download_file`
  / `download_range`. Streaming download (mirror `xet-data` §4.4.1): pull-based
  `DownloadStream`/`UnorderedDownloadStream` (`next()`/`blocking_next()`),
  byte-denominated buffer semaphore (configurable cap), term-metadata prefetch,
  `DataWriter` (Sequential/Unordered), `reconstruct_to_writer`,
  `download_bytes`. **Stream-group layer** (§4.4.3): group builder, abort/
  status, single-flight cancellation; **on-disk chunk cache** (`cache.rs`).
  E2E: download files uploaded via server ingest; byte-identical;
  **memory-bounded cat of a large file (e.g. 64 GiB synthetic) asserting
  resident RAM stays ≪ file size**.
- **Write path (upload).** Client CDC chunker (verify byte-identity against
  server `CdcChunker`), global dedup query + eligibility, xorb build/serialize/
  upload, shard build/upload (xorbs-before-shard), `UploadSession` +
  `upload_file` + idempotency. Streaming upload (mirror §4.4.2): push-style
  `add_data(Bytes)` ingest loop (8 MiB blocks), CDC on compute thread, xorb cut
  at 64 MiB/8192, streaming-body xorb POST, session tail aggregator,
  `upload_stream` handle + `upload_bytes` (in-memory `Bytes`, zero-copy),
  `XetUploadCommit` group layer (§4.4.3). E2E:
  upload → server-side reconstruct → identical; cross-file dedup (same chunks
  stored once); **streaming upload of a large reader with bounded memory
  (assert in-flight RAM ≈ 8 MiB + ≤64 MiB xorb + tail)**; worst-case
  incompressible xorb (≥8192 tiny chunks) staying under the server body cap.
- **Retry/backoff + concurrency.** `RetryPolicy`, jittered exponential
  backoff, error classification, `Retry-After` honoring, adaptive/fixed
  concurrency, session/request correlation headers. E2E: 503 saturation tests
  against admission-limited shardline; 429 no-`Retry-After` backoff (mock).
- **Metadata operations + server path addressing.** Server (issue #19):
  implement the §2.5 metadata endpoints (path→file_id, listing, deregistration,
  branch/revision) in `crates/shardline-server`. Client: `tree.rs`/
  `revisions.rs` against them; `ls`/`info`/`rm`/`branch`; `sync` (push-only);
  `cat` streaming.
- **CLI + config + packaging.** `sdx` symlink dispatch, clap tree,
  `shardline.toml`, completions, manpage, release archive.
- **Cross-frontend conformance.** Run the full suite against a second
  Xet-compatible frontend (openxet or HF-style mock); conformance tests from
  `docs/PROTOCOL_CONFORMANCE.md` (`:136-157`).

## 10. Testing and verification

- **Unit:** golden hash vectors, Xet hex conversion, xorb parse/hash verify,
  shard parse/validate, chunker byte-identity vs server, retry policy
  classification/backoff math, token refresh single-flight.
- **E2E against real shardline server** (`e2e/`): upload idempotency, missing-
  xorb rejection, full-file + range reconstruction, dedupe hit/miss,
  unauthorized-scope rejection, token refresh/concurrent/ranged flows,
  503 admission saturation (Retry-After honored), 429 no-`Retry-After` backoff,
  worst-case incompressible xorb (≥8192 tiny chunks) staying under the body
  cap.
- **Cross-implementation:** integration test against an existing
  Xet-compatible client (`git-xet`/`hf_xet`) exchanging files through shardline
  — "no client patches" goal (`PROTOCOL_CONFORMANCE.md:136-157`).
- **Rate-limit simulation:** mock server emitting 429/503/`Retry-After` to
  validate the library's public retry knobs; document the knobs in rustdoc so
  library users can tune for their frontend.

## 11. Open questions / risks

1. **`/v2/file-chunk-hashes` not served by shardline** — partial-overwrite
   uploads (`X-Range-Dirty`) cannot be fully exercised against shardline; defer
   unless a user needs in-place overwrite semantics. CLI `cp` overwrite can
   degrade to full re-upload for now.
2. **Signed-URL xorb fetch (XetHub/HF)** vs shardline's plain ranged
   `/transfer/xorb` — upstream 1.5.4 has **no** `X-Xet-Signed-Range` header
   handling anywhere; signed URLs are carried in the reconstruction JSON
   (`fetch_info[].url`), and shardline already emits a full absolute
   `{base}/transfer/xorb/default/{hash}` URL there. The client's real job:
   **use the `url` from the reconstruction response and `Range` against it** —
   identical for shardline and HF. Keep the header-based signed-range check
   (`Expires`/`Policy`/`Signature`/`Key-Pair-Id`) as an optional XetHub/S3
   probe only (refresh on 403); on shardline a 403 means token expiry
   (§4.4.1), not URL rotation.
3. **Xorb writer parity:** shardline's xorb format is identical to upstream for
   `default` namespace; verify `ByteGrouping4LZ4` client-side via the pinned
   `xet-core-structures` (workspace declares 1.5.1, lock resolves 1.5.2; §4.1)
   rather than reimplementing BG4.
4. **Revision semantics** (`xet://.../{revision}`): token issuance is
   revision-scoped; `branch` commands assume shardline's revision model
   (provider-scoped revisions). Confirm server behavior for non-`main`
   revisions with the metadata endpoints.
5. **Rate limiting is server-policy-dependent**: shardline has no time-window
   rate limiting today, but may add it (listed as required work). The library's
   retry policy must be frontend-agnostic and user-tunable so behavior remains
   correct against any server.
6. **Crate version drift:** upstream crates publish without semver promises;
   pin exact versions for BG4/merkle primitives and keep wire-level conformance
   tests as the compatibility contract.

## 12. Out of scope (this issue)

- Server-side changes are limited to the **§2.5 path/metadata endpoints**
  (implemented in this issue); no CAS/data-plane changes.
- `/v2/shards` streaming NDJSON upload (only v1 required by shardline).
- `/v2/file-chunk-hashes` partial-overwrite support (deferred, see §11 open
  question 1).
- Storage quotas / per-tenant rate limiting on the server (tracked separately
  in `docs/SHARDLINE_PRODUCTION_READINESS.md`).
