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
//! The stream-group layer, on-disk chunk cache, and retry/adaptive concurrency
//! arrive in later milestones.
//!
//! Later milestones add streaming upload, chunk caching, retry/concurrency,
//! chunking, shard, and path addressing modules from the module map in
//! `docs/SDX_PLAN.md` §4.2.

pub mod auth;
pub mod client;
pub mod config;
pub mod error;
pub mod hash;
pub mod reconstruction;
pub mod session;
pub mod stream;
pub mod transfer;
pub mod xorb;

pub use auth::{
    Auth, AuthError, HttpConfig, PROVIDER_KEY_HEADER_NAME, REFRESH_BUFFER_SECONDS, RepositoryId,
    ScopedToken, TokenService,
};
pub use client::{XetClient, XetClientBuilder};
pub use config::Credential;
pub use error::{SdxError, TransferError, XetHashParseError};
pub use hash::{
    compute_chunk_hash, compute_term_verification_hash, parse_xet_hash_hex, xet_hash_hex_string,
};
pub use reconstruction::{ReconstructedFile, ResolvedTerm, reconstruct};
pub use session::DownloadSession;
pub use stream::{
    BufferPermit, BufferSemaphore, DEFAULT_COMPLETION_RATE_ESTIMATOR_HALF_LIFE,
    DEFAULT_DOWNLOAD_BUFFER_LIMIT, DEFAULT_DOWNLOAD_BUFFER_PERFILE_SIZE,
    DEFAULT_DOWNLOAD_BUFFER_SIZE, DEFAULT_DOWNLOAD_CONCURRENCY,
    DEFAULT_MAX_RECONSTRUCTION_FETCH_SIZE, DEFAULT_MIN_PREFETCH_BUFFER,
    DEFAULT_MIN_RECONSTRUCTION_FETCH_SIZE, DEFAULT_TARGET_BLOCK_COMPLETION_TIME_SECS, DataFuture,
    DataWriter, DownloadStream, StreamLimits, UnorderedDownloadStream,
};
pub use transfer::{
    ByteRange, MultipartPart, RangedXorb, TransferClient, parse_multipart_byteranges,
};
pub use xet_core_structures::merklehash::MerkleHash;
pub use xorb::{DecodedChunk, XorbError, XorbReader};
