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
//! # Module guide
//!
//! [`client::XetClientBuilder`] configures repository-scoped authentication,
//! transfer limits, caching and retries. [`auth`] and [`config`] resolve
//! credentials; [`retry`] owns retry policy.
//!
//! [`stream`] provides ordered and unordered downloads with byte-denominated
//! buffer reservations. [`cache`] stores verified chunk ranges on disk, and
//! [`group`] coordinates concurrent transfers and cancellation. The buffer
//! reservation bounds decoded payloads; it is not a process-RSS limit.
//!
//! [`upload`] manages streaming ingestion and publishes xorbs before their
//! referencing shards. [`chunker`] uses the shared CDC engine; [`dedup`],
//! [`xorb_build`] and [`shard`] handle reuse, container packing and metadata.
//!
//! [`transfer`] owns the HTTP wire protocol, [`reconstruction`] assembles file
//! ranges, and [`xorb`] decodes serialized chunk ranges through the pinned
//! upstream Xet implementation. [`tree`] and [`revisions`] expose repository
//! paths and revisions. Design history lives in `docs/SDX_PLAN.md`.

pub mod auth;
pub mod cache;
pub mod chunker;
pub mod client;
pub mod config;
pub mod dedup;
pub mod error;
pub mod group;
pub mod hash;
mod local_output;
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
