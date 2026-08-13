#![deny(unsafe_code)]
#![cfg_attr(
    test,
    allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::indexing_slicing,
        clippy::shadow_unrelated,
        clippy::let_underscore_must_use,
        clippy::format_push_string
    )
)]

//! `sdx` is a native Xet client library for Shardline's Xet frontend.
//!
//! It downloads files from (and uploads files to) Xet repositories through the
//! shardline CAS frontend: the client resolves a repo-scoped read/write token,
//! fetches a file's reconstruction plan, streams and decodes the chunk data,
//! and writes it out — with bounded memory and an optional on-disk chunk
//! cache. Files are addressed by their 64-hex content hash (`file_id`).
//!
//! # Quick start
//!
//! Point [`XetClientBuilder`] at a `xet://` endpoint and an [`Auth`] scope,
//! then download a file by its 64-hex content id:
//!
//! ```no_run
//! use sdx::{Auth, RepositoryId, XetClientBuilder};
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     // The credential is resolved from the SHARDLINE_TOKEN /
//!     // SHARDLINE_API_KEY / SHARDLINE_TOKEN_FILE environment variables when
//!     // the download runs, or set explicitly with `.with_token(..)` /
//!     // `.with_api_key(..)`.
//!     let auth = Auth::new(
//!         "https://xet.example.com",
//!         RepositoryId {
//!             provider: "github".to_owned(),
//!             owner: "shardline".to_owned(),
//!             repo: "assets".to_owned(),
//!             revision: "main".to_owned(),
//!         },
//!     )?;
//!
//!     let client = XetClientBuilder::new()
//!         .endpoint("xet://xet.example.com/github/shardline/assets/main")
//!         .auth(auth)
//!         .build()?;
//!
//!     // `file_id` is the 64-hex content hash of the file to fetch (from a
//!     // tree listing, an upload, or a reconstruction response).
//!     let file_id =
//!         "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
//!     let bytes = client.download_bytes(file_id).await?;
//!     println!("downloaded {} bytes", bytes.len());
//!     Ok(())
//! }
//! ```
//!
//! # Design history (milestones)
//!
//! The milestone notes below (M0–M4, `docs/SDX_PLAN.md`) are the crate's
//! design history and are not needed to use the library:
//!
//! M0 provides the hash and hexadecimal primitives the Xet wire protocol
//! requires: BLAKE3 keyed hashing for chunk data (and term verification) via
//! the pinned upstream `xet-core-structures` crate, plus strict Xet CAS API
//! hexadecimal conversion with 8-byte group reversal.
//!
//! M1 adds the authentication/token service: [`auth::TokenService`] issues and
//! caches repo+revision-scoped read/write CAS tokens against the shardline
//! token-issuance routes (single-flight refresh with a 30-second buffer), with
//! credential resolution from explicit sources, token files, and the
//! `SHARDLINE_TOKEN` / `SHARDLINE_API_KEY` / `SHARDLINE_TOKEN_FILE`
//! environment variables ([`config`]).
//!
//! M2a adds the core read path: [`client::XetClient`] / [`client::XetClientBuilder`]
//! map a `xet://` endpoint to a repository and token service;
//! [`transfer::TransferClient`] issues reconstruction requests and ranged xorb
//! fetches; [`xorb::XorbReader`] decodes serialized xorb byte ranges (footer-less
//! tolerant, via the pinned `xet-core-structures` public API);
//! [`reconstruction::reconstruct`] orchestrates V2→V1 reconstruction, term
//! resolution, `unpacked_length` validation, and byte-range assembly;
//! [`session::DownloadSession`] exposes `download_file` / `download_range`.
//!
//! M2b1 adds the streaming download core ([`stream`], `docs/SDX_PLAN.md`
//! §4.4.1): pull-based [`stream::DownloadStream`] /
//! [`stream::UnorderedDownloadStream`] (`next()`/`blocking_next()`), the
//! byte-denominated [`stream::BufferSemaphore`] memory bound, and
//! `download_stream` / `download_unordered_stream` / `download_to_writer` /
//! `download_bytes` on [`client::XetClient`] and [`session::DownloadSession`].
//!
//! M2b2 adds the on-disk chunk cache ([`cache::ChunkCache`],
//! `docs/SDX_PLAN.md` §4.4.1 step 3) and the stream-group layer
//! ([`group::XetStreamGroup`], §4.4.3). The cache is content-addressed by the
//! 64-hex xorb hash, atomic (temp+rename), LRU budget-bounded (default 2 GiB),
//! and tolerant of corruption (CRC-verified, delete-on-corrupt); every ranged
//! xorb fetch checks the cache before acquiring the download permit / hitting
//! the network. The stream group manages concurrent downloads with abort-all
//! and per-stream [`group::XetTaskState`] status probes; the upload/commit
//! side of the group layer is M3 and not implemented.
//!
//! M3a adds the offline-testable write-path foundation (all of `docs/SDX_PLAN.md`
//! §4.4.2 / §9-M3): [`chunker::Chunker`] is a streaming CDC chunker
//! **byte-identical** to the server's `CdcChunker`; [`xorb_build::build_xorb`]
//! produces serialized xorbs **byte-identical** to the server's
//! `pack_chunks_into_xorb` (via the pinned upstream `xet-core-structures`
//! crate, with the shardline format-v2 footer assembled directly — see the
//! module docs); [`dedup::DedupClient`] implements the global dedup query
//! (`GET /v1/chunks/default-merkledb/{hash}`, 404 = miss, 429 no-retry) plus
//! the eligibility and defrag-prevention hysteresis helpers.
//!
//! M3b adds the network + session layer on top: [`upload::UploadSession`]
//! runs the push-style ingest loop (8 MiB blocks, CDC on a compute thread,
//! session + eligibility-gated global dedup, HEAD-probed idempotent xorb
//! uploads with streaming bodies from a `JoinSet`, xorbs-before-shard via
//! fork-format v3 shards serialized by [`shard`]) and the streaming surface
//! ([`upload::UploadStreamHandle`], [`crate::XetClient::upload_file`] /
//! `upload_bytes` / `upload_stream`, and the group layer
//! [`group::XetUploadCommit`]). Retry/backoff/adaptive concurrency is M4.
//!
//! Later milestones add retry/adaptive concurrency (M4), chunking, shard, and
//! path addressing modules from the module map in `docs/SDX_PLAN.md` §4.2.

pub mod auth;
pub mod cache;
pub mod chunker;
pub mod client;
pub mod config;
pub mod dedup;
pub mod error;
pub mod group;
pub mod hash;
pub mod reconstruction;
pub mod retry;
pub mod revisions;
pub mod session;
pub mod shard;
pub mod stream;
pub mod transfer;
pub mod tree;
pub mod upload;
pub mod url;
pub mod xorb;
pub mod xorb_build;

pub use auth::{
    Auth, AuthError, HttpConfig, PROVIDER_KEY_HEADER_NAME, REFRESH_BUFFER_SECONDS, RepositoryId,
    ScopedToken, TokenService,
};
pub use cache::{CachedXorbRange, ChunkCache, DEFAULT_CHUNK_CACHE_BUDGET_BYTES};
pub use chunker::{Chunk, Chunker, DEFAULT_TARGET_CHUNK_SIZE};
pub use client::{XetClient, XetClientBuilder};
pub use config::{
    AuthSection, Credential, DefaultSection, SdxConfig, SdxConfigError,
    resolve_credential_from_config, resolve_credential_from_env,
};
pub use dedup::{
    DEFRAG_HYSTERESIS_FACTOR, DEFRAG_MIN_CHUNKS_PER_RANGE, DEFRAG_NUM_RANGES_WINDOW, DedupClient,
    DedupOutcome, DefragPrevention, GLOBAL_DEDUP_CHUNK_MODULUS, GLOBAL_DEDUP_PREFIX,
    MIN_SPACING_BETWEEN_GLOBAL_DEDUP_QUERIES, is_global_dedup_eligible,
};
pub use error::{SdxError, TransferError, XetHashParseError};
pub use group::{
    GroupedDownloadStream, GroupedUnorderedDownloadStream, GroupedUploadStream, XetStreamGroup,
    XetTaskState, XetUploadCommit,
};
pub use hash::{
    compute_chunk_hash, compute_term_verification_hash, parse_xet_hash_hex, xet_hash_hex_string,
};
pub use reconstruction::{ReconstructedFile, ResolvedTerm, reconstruct};
pub use retry::RetryPolicy;
pub use revisions::Revision;
pub use session::DownloadSession;
pub use shard::{ShardFileEntry, ShardSegment, ShardXorb, ShardXorbChunk, serialize_shard};
pub use stream::{
    BufferPermit, BufferSemaphore, DEFAULT_COMPLETION_RATE_ESTIMATOR_HALF_LIFE,
    DEFAULT_DOWNLOAD_BUFFER_LIMIT, DEFAULT_DOWNLOAD_BUFFER_PERFILE_SIZE,
    DEFAULT_DOWNLOAD_BUFFER_SIZE, DEFAULT_DOWNLOAD_CONCURRENCY,
    DEFAULT_MAX_RECONSTRUCTION_FETCH_SIZE, DEFAULT_MIN_PREFETCH_BUFFER,
    DEFAULT_MIN_RECONSTRUCTION_FETCH_SIZE, DEFAULT_TARGET_BLOCK_COMPLETION_TIME_SECS, DataFuture,
    DataWriter, DownloadStream, StreamLimits, UnorderedDownloadStream,
};
pub use transfer::{
    ByteRange, MultipartPart, RangedXorb, TransferClient, XORB_UPLOAD_PROGRESS_BLOCK_SIZE,
    parse_multipart_byteranges,
};
pub use tree::{DirEntry, DirListing, PathEntry, RegisterResult};
pub use upload::{
    DEFAULT_UPLOAD_CONCURRENCY, INGESTION_BLOCK_SIZE, UploadFileInfo, UploadReport, UploadSession,
    UploadStreamHandle,
};
pub use url::XetUrl;
pub use xet_core_structures::merklehash::MerkleHash;
pub use xorb::{DecodedChunk, XorbError, XorbReader};
pub use xorb_build::{
    BuiltXorb, MAX_XORB_BYTES, MAX_XORB_CHUNKS, SERIALIZED_XORB_SAFETY_CAP_BYTES, XorbChunkEntry,
    build_xorb, serialized_size_le, xorb_cut_condition, xorb_max_addable_chunk,
};
