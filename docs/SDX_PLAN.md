# SDX — Xet Client Library and CLI: Implementation Plan

Status: Implementation plan for [issue #19](https://github.com/STEXS-Technologies/shardline/issues/19).
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
| `/v2/reconstructions/{file_id}` | GET | v2 response (multi-range fetch descriptors) |
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

From `huggingface/xet-core` (shardline vendors `xet-client`/`xet-data`/
`xet-core-structures` 1.5.1; current upstream is 1.5.4, published **without a
semver promise** — target the **wire protocol**, not crate internals):

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
| Credential/config management | `XetConfig`, token files | n/a | **missing** — §5.4 |
| CLI cp/sync/ls/rm/cat/info/branch | pyxet-style surface (deprecated upstream) | n/a | **missing** — thin wrapper |

**Conclusion:** every client-side capability in the table above is **missing**;
there is no prior client code in this workspace. The server needs **no changes**
for the listed phases (consistent with `docs/XET_NATIVE_CLI.md`), except any
future features like `/v2/file-chunk-hashes` (deferred, not part of #19).

## 4. Architecture

### 4.1 Crate layout

New crate: **`crates/sdx`** (package name `sdx`). Dependencies:

- `shardline-protocol` — token types, hash types, ByteRange, SecretString
- `shardline-xet-adapter` — XorbWriter/XorbReader/XorbStore contracts (add
  client-facing constructors if the adapter stays clean; otherwise the sdx crate
  implements them)
- `shardline-server-core` — reconstruction planning, validation limits
- `shardline-storage` — ObjectStore trait (contract only, not embedded)
- `tokio`, `reqwest` (HTTP), `lz4_flex`, `blake3`, `clap` (CLI only), `serde`

Must **not** depend on `shardline-server`.

CLI surface lives in the existing CLI crate (`crates/shardline`), routed by
`argv[0]` symlink detection per `docs/XET_NATIVE_CLI.md` (dispatch model). The
CLI module adds the `xet` subcommand tree as a thin clap wrapper over `sdx`.

### 4.2 Module map (library)

```
crates/sdx/src/
  lib.rs            — re-exports, crate docs
  client.rs         — SdxClient builder + handle (endpoint, auth, retry policy, concurrency)
  auth.rs           — token issuance (read/write), refresh w/ 30s buffer, credential sources
  chunk.rs          — client-side CDC chunker (FastCDC gear-hash, 64 KiB target)
  hash.rs           — BLAKE3 keyed hashing, Xet hex conversion (byte-group reversal)
  xorb.rs           — XorbWriter/XorbReader (build, serialize, parse, ranged fetch)
  shard.rs          — shard build/upload (file registration, metadata)
  reconstruction.rs — v1/v2 reconstruction orchestration, range handling, fallback
  dedup.rs          — global dedup query, eligibility (first chunk; last-8-bytes-LE % 1024 == 0; ~1 query / 4 MiB)
  transfer.rs       — HTTP layer: xorb upload/download, chunk GET, 206 handling, multipart/byteranges
  retry.rs          — RetryPolicy, jittered exponential backoff, error classification
  config.rs         — shardline.toml config, credential caching, env overrides
  session.rs        — UploadSession / DownloadSession (high-level file ops)
  error.rs          — sdx::Error, retryable classification
```

### 4.3 Public API sketch

```rust
use sdx::{SdxClient, SdxClientBuilder, RetryPolicy, UploadSession, DownloadSession, Auth};

// Library users write programs against any Xet frontend:
let client = SdxClientBuilder::new()
    .endpoint("xet://host/provider/owner/repo/main")   // shardline-style URL
    .auth(Auth::token(read_or_write_token))            // or Auth::refresh_url(...)
    .retry(RetryPolicy::default().max_attempts(5).base_delay_ms(3000))  // user-tunable
    .download_concurrency(16)
    .build()
    .await?;

let session = client.upload_session().await?;
session.upload_file("local.bin", "remote.bin").await?;

let session = client.download_session().await?;
session.download_file("remote.bin", "local.bin").await?;
session.download_range("remote.bin", 0..1024, "head.bin").await?;
```

Session types mirror the reference `XetSession`/`FileDownloadSession` concepts
(`xet_pkg/src/xet_session/session.rs`): upload commit, download group, download
stream group, abort/status.

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
    timeout → **503** + `Retry-After: 1` (`transfer_limiter.rs:12-60`)
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
    (`xet_config/src/groups/client.rs`)
  - `honor_retry_after: bool` (default true) — use `Retry-After` when present
  - `jitter: bool` (default true) — jittered exponential backoff
  - `retry_on_429: bool` (default true) — **except** dedup queries, which
    fail fast (reference client policy: `with_429_no_retry()` for
    `query_dedup_api`)
- Error classification:
  - **Retryable:** 429, 500, 503, 504, connection errors, timeouts
  - **Non-retryable:** 400, 401 (refresh token first, then retry once), 403
    (scope too narrow — re-issue with write token; for signed-URL fetches,
    refresh the URL on 403), 404, 416
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
3. **Upload idempotency:** `POST /v1/xorbs/{hash}` returns `was_inserted`; a
   pre-existing xorb is not an error.
4. **Global dedup eligibility:** first chunk always eligible; subsequent chunks
   eligible when last-8-bytes-LE `% 1024 == 0`; throttle to ~1 query per 4 MiB.
5. **Range semantics:** reconstruction ranges end-inclusive; chunk-index ranges
   end-exclusive; xorb URL byte ranges end-inclusive; first term may include an
   offset into the first decoded chunk.
6. **206 handling:** single range → plain 206; multiple → `multipart/byteranges`;
   parse parts in order, deserialize chunks, validate `unpacked_length` per term.
7. **v2→v1 fallback:** if `/v2/reconstructions` returns 404/501, fall back to
   `/v1/reconstructions`; same for `/v2/shards` → `/v1/shards`.
8. **Footer-less xorbs:** server normalizes; client must also accept missing
   footers when parsing.
9. **Xorb size limits:** serialized xorb ≤ 64 MiB (protocol) / 16 MiB block
   (historical); ≤8192 chunks per xorb; `XORB_BLOCK_SIZE = 16 MiB`,
   `TARGET_CHUNK_SIZE = 128 KiB`.
10. **Body limits:** respect `SHARDLINE_MAX_REQUEST_BODY_BYTES` and per-endpoint
    bounds when uploading shards (metadata limits).
11. **Verification keys:** chunk = BLAKE3 keyed `DATA_KEY`; xorb = Merkle root
    with `INTERNAL_NODE_KEY` over `"{hex} : {size}\n"` lines; file = Merkle
    root then blake3 with 32 zero bytes; term verification = `VERIFICATION_KEY`
    over concatenated raw chunk-hash bytes.

## 8. CLI surface (`sdx`)

Thin clap wrapper over the library (per `docs/XET_NATIVE_CLI.md`, now renamed):

- `sdx cp <src> <dst>` — local ↔ remote (`xet://host/provider/owner/repo/rev/path`)
- `sdx sync <src> <dst>` — directory sync (push-only)
- `sdx ls <url>` (+ `--long`, `--branches`) — remote listing
- `sdx rm <url>` (+ `--recursive`) — metadata deregistration
- `sdx cat <url>` — stream remote file to stdout
- `sdx info <url>` — file/dir metadata
- `sdx branch <url> --create/--delete` — revision listing/creation/deletion
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

- **M0 — Crate scaffolding + hash/hex primitives.** `crates/sdx` skeleton,
  module map, BLAKE3 keyed hashing, Xet hex conversion, golden-vector unit tests
  (`shardline-index` vectors). No network.
- **M1 — Auth + token service.** Token issuance (read/write) against shardline
  token routes, refresh with 30s buffer, single-flight, credential sources,
  `Auth` builder. Unit tests with mock server.
- **M2 — Read path (download).** v2/v1 reconstruction client + fallback, ranged
  `/transfer/xorb` fetch, 206/multipart parsing, chunk deserialization,
  `unpacked_length` validation, file assembly, `DownloadSession` + `download_file`
  / `download_range`. E2E: download files uploaded via server ingest; byte-identical.
- **M3 — Write path (upload).** Client CDC chunker (verify byte-identity against
  server `CdcChunker`), global dedup query + eligibility, xorb build/serialize/
  upload, shard build/upload (xorbs-before-shard), `UploadSession` +
  `upload_file` + idempotency. E2E: upload → server-side reconstruct → identical;
  cross-file dedup (same chunks stored once).
- **M4 — Retry/backoff + concurrency.** `RetryPolicy`, jittered exponential
  backoff, error classification, `Retry-After` honoring, adaptive/fixed
  concurrency, session/request correlation headers. E2E: 503 saturation tests
  against admission-limited shardline; 429 no-`Retry-After` backoff (mock).
- **M5 — Metadata operations.** `ls`/`info`/`rm`/`branch` via reconstruction +
  shard metadata; `sync` (push-only); `cat` streaming.
- **M6 — CLI + config + packaging.** `sdx` symlink dispatch, clap tree,
  `shardline.toml`, completions, manpage, release archive.
- **M7 — Cross-frontend conformance.** Run the full suite against a second
  Xet-compatible frontend (openxet or HF-style mock); conformance tests from
  `docs/PROTOCOL_CONFORMANCE.md` (`:136-157`).

## 10. Testing and verification

- **Unit:** golden hash vectors, Xet hex conversion, xorb parse/hash verify,
  shard parse/validate, chunker byte-identity vs server, retry policy
  classification/backoff math, token refresh single-flight.
- **E2E against real shardline server** (`e2e/`): upload idempotency, missing-
  xorb rejection, full-file + range reconstruction, dedupe hit/miss,
  unauthorized-scope rejection, token refresh/concurrent/ranged flows,
  503 admission saturation (Retry-After honored), 429 no-`Retry-After` backoff.
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
   `/transfer/xorb` — the client must support both: if reconstruction returns
   `X-Xet-Signed-Range` + `Expires`/`Policy`/`Signature`/`Key-Pair-Id`, fetch the
   signed URL (refresh on 403); otherwise use `Range` against `casUrl`.
3. **Xorb writer parity:** shardline's xorb format is identical to upstream for
   `default` namespace; verify `ByteGrouping4LZ4` client-side (vendored
   `xet-core-structures` 1.5.1) rather than reimplementing BG4.
4. **Revision semantics** (`xet://.../{revision}`): token issuance is
   revision-scoped; `branch` commands assume shardline's revision model
   (provider-scoped revisions). Confirm server behavior for non-`main`
   revisions in M5.
5. **Rate limiting is server-policy-dependent**: shardline has no time-window
   rate limiting today, but may add it (listed as required work). The library's
   retry policy must be frontend-agnostic and user-tunable so behavior remains
   correct against any server.
6. **Crate version drift:** upstream crates publish without semver promises;
   pin exact versions for BG4/merkle primitives and keep wire-level conformance
   tests as the compatibility contract.

## 12. Out of scope (this issue)

- Server-side changes (none required for M0–M6).
- `/v2/shards` streaming NDJSON upload (only v1 required by shardline).
- `/v2/file-chunk-hashes` partial-overwrite support (deferred, see §11.1).
- Storage quotas / per-tenant rate limiting on the server (tracked separately
  in `docs/SHARDLINE_PRODUCTION_READINESS.md`).
