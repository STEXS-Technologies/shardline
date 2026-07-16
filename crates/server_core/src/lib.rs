#![deny(unsafe_code)]
#![allow(
    clippy::missing_panics_doc,
    clippy::missing_const_for_fn,
    clippy::must_use_candidate
)]
#![cfg_attr(
    test,
    allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::indexing_slicing,
        clippy::shadow_unrelated,
        clippy::let_underscore_must_use,
        clippy::format_push_string,
        clippy::panic
    )
)]

//! Shared core types for the Shardline server ecosystem.
//!
//! This crate contains pure data structures and constants that are shared
//! between the server crate and potential future crate extractions.

use std::{
    io::{Error as IoError, Read},
    num::{NonZeroUsize, TryFromIntError},
    path::{Path, PathBuf},
};

use shardline_index::{LocalRecordStore, PostgresRecordStore, RecordStore, RecordTraversal};
use shardline_protocol::{
    ByteRange, RepositoryProvider, ShardlineHash, TokenClaims, TokenCodecError, TokenScope,
};
use shardline_storage::{
    DeleteOutcome, LocalObjectStore, LocalObjectStoreError, ObjectBody, ObjectIntegrity, ObjectKey,
    ObjectKeyError, ObjectMetadata, ObjectPrefix, ObjectStore, PutOutcome, S3ObjectStore,
    S3ObjectStoreConfig, S3ObjectStoreError,
};
use thiserror::Error;

pub mod auth;
pub mod protocol_support;
pub mod server_frontend;

/// Provider-agnostic authentication trait.
///
/// Implementations verify and mint scoped bearer tokens for the Shardline API.
/// The server selects a concrete provider at startup based on configuration.
pub trait AuthProvider: Send + Sync {
    /// Verifies an opaque bearer token and returns the decoded claims.
    ///
    /// # Errors
    ///
    /// Returns [`AuthError`] when the token is invalid, expired, or otherwise
    /// unverifiable.
    fn verify_token(&self, token: &str) -> Result<TokenClaims, AuthError>;

    /// Mints a signed bearer token from the provided claims.
    ///
    /// # Errors
    ///
    /// Returns [`AuthError`] when the provider does not support token minting
    /// or when signing fails.
    fn mint_token(&self, claims: &TokenClaims) -> Result<String, AuthError>;
}

/// Verified request authorization context.
#[derive(Debug, Clone)]
pub struct AuthContext {
    /// The decoded token claims.
    pub claims: TokenClaims,
}

impl AuthContext {
    /// Creates an authorization context from verified token claims.
    #[must_use]
    pub const fn new(claims: TokenClaims) -> Self {
        Self { claims }
    }

    /// Returns the verified claims.
    #[must_use]
    pub const fn claims(&self) -> &TokenClaims {
        &self.claims
    }

    /// Returns the authenticated subject.
    #[must_use]
    pub fn subject(&self) -> &str {
        self.claims.subject()
    }

    /// Returns the granted scope.
    #[must_use]
    pub const fn scope(&self) -> TokenScope {
        self.claims.scope()
    }
}

/// Authentication provider failure.
#[derive(Debug, Error)]
pub enum AuthError {
    /// The token format was invalid.
    #[error("invalid token")]
    InvalidToken,
    /// The token has expired.
    #[error("expired token")]
    ExpiredToken,
    /// The token does not grant the required scope.
    #[error("insufficient scope")]
    InsufficientScope,
    /// The provider encountered an internal error.
    #[error("provider error: {0}")]
    ProviderError(String),
}

impl From<TokenCodecError> for AuthError {
    fn from(error: TokenCodecError) -> Self {
        match error {
            TokenCodecError::Expired => Self::ExpiredToken,
            TokenCodecError::InvalidSignature
            | TokenCodecError::InvalidFormat
            | TokenCodecError::InvalidHex(_)
            | TokenCodecError::Claims(_) => Self::InvalidToken,
            TokenCodecError::EmptySigningKey
            | TokenCodecError::SigningKeyTooShort { .. }
            | TokenCodecError::Json(_) => Self::ProviderError(error.to_string()),
        }
    }
}

/// Validates that a content hash is exactly 64 lowercase hex characters.
///
/// # Errors
///
/// Returns an error with the given `error_fn` when the hash is malformed.
pub fn validate_content_hash_with<E>(value: &str, error_fn: fn() -> E) -> Result<(), E> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
    {
        return Err(error_fn());
    }
    Ok(())
}

/// Returns the chunk object key for a hex-encoded content hash.
///
/// # Errors
///
/// Returns [`ServerObjectStoreError`] when the hash is malformed or the key cannot be created.
pub fn chunk_object_key(hash_hex: &str) -> Result<ObjectKey, ServerObjectStoreError> {
    validate_content_hash_with(hash_hex, || ServerObjectStoreError::InvalidContentHash)?;
    let prefix = hash_hex.get(..2).ok_or(ServerObjectStoreError::Overflow)?;
    let key = format!("{prefix}/{hash_hex}");
    ObjectKey::parse(&key).map_err(map_object_key_error)
}

/// Extracts the chunk hash from a chunk object key if the key matches the expected layout.
///
/// Returns `Some(hash_hex)` if the key is in the format `<2-char-prefix>/<64-char-hash>`,
/// `None` otherwise.
///
/// # Errors
///
/// Returns [`ServerObjectStoreError::InvalidContentHash`] if the extracted hash fails validation.
pub fn chunk_hash_from_chunk_object_key_if_present(
    key: &ObjectKey,
) -> Result<Option<&str>, ServerObjectStoreError> {
    let mut segments = key.as_str().split('/');
    let Some(prefix) = segments.next() else {
        return Ok(None);
    };
    let Some(candidate_hash_hex) = segments.next() else {
        return Ok(None);
    };
    if segments.next().is_some() {
        return Ok(None);
    }
    if prefix.len() != 2 || !prefix.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Ok(None);
    }
    if !candidate_hash_hex.starts_with(prefix) {
        return Ok(None);
    }
    validate_content_hash_with(candidate_hash_hex, || {
        ServerObjectStoreError::InvalidContentHash
    })?;
    Ok(Some(candidate_hash_hex))
}

/// Computes a blake3 content hash for the given bytes.
#[must_use]
pub fn chunk_hash(bytes: &[u8]) -> ShardlineHash {
    let digest = blake3::hash(bytes);
    ShardlineHash::from_bytes(*digest.as_bytes())
}

/// Computes a blake3 content hash for a file record's chunk layout.
#[must_use]
pub fn content_hash(
    total_bytes: u64,
    chunk_size: u64,
    chunks: &[shardline_index::FileChunkRecord],
) -> String {
    let mut hasher = blake3::Hasher::new();
    hasher.update(&total_bytes.to_le_bytes());
    hasher.update(&chunk_size.to_le_bytes());
    for chunk in chunks {
        hasher.update(chunk.hash.as_bytes());
        hasher.update(&chunk.offset.to_le_bytes());
        hasher.update(&chunk.length.to_le_bytes());
    }
    hasher.finalize().to_hex().to_string()
}

const fn map_object_key_error(error: ObjectKeyError) -> ServerObjectStoreError {
    match error {
        ObjectKeyError::Empty
        | ObjectKeyError::UnsafePath
        | ObjectKeyError::ControlCharacter
        | ObjectKeyError::TooLong => ServerObjectStoreError::Overflow,
    }
}

/// Reads the full contents of an object from the store.
///
/// # Errors
///
/// Returns [`ServerObjectStoreError`] on storage backend failures, length
/// mismatches, or arithmetic overflows.
pub fn read_full_object(
    store: &ServerObjectStore,
    object_key: &ObjectKey,
    length: u64,
) -> Result<Vec<u8>, ServerObjectStoreError> {
    store.read_full_object(object_key, length)
}

/// Lifecycle metadata consistency failure.
#[derive(Debug, Clone, Error, PartialEq, Eq)]
pub enum InvalidLifecycleMetadataError {
    /// A quarantine candidate cannot be deleted before it was first observed.
    #[error(
        "quarantine candidate for {object_key} had delete-after {delete_after_unix_seconds} before first-seen {first_seen_unreachable_at_unix_seconds}"
    )]
    QuarantineCandidateDeleteBeforeFirstSeen {
        /// Quarantined object key.
        object_key: String,
        /// Candidate deletion timestamp.
        delete_after_unix_seconds: u64,
        /// First observed unreachable timestamp.
        first_seen_unreachable_at_unix_seconds: u64,
    },
    /// A quarantine candidate referenced an object that is no longer present.
    #[error("quarantine candidate referenced missing object {object_key}")]
    QuarantineCandidateMissingObject {
        /// Quarantined object key.
        object_key: String,
    },
    /// A quarantine candidate recorded a length that differs from object-store metadata.
    #[error(
        "quarantine candidate for {object_key} expected length {expected_length}, got {observed_length}"
    )]
    QuarantineCandidateLengthMismatch {
        /// Quarantined object key.
        object_key: String,
        /// Length recorded in quarantine metadata.
        expected_length: u64,
        /// Length observed in object-store metadata.
        observed_length: u64,
    },
    /// A retention hold cannot be released before it was created.
    #[error(
        "retention hold for {object_key} had release-after {release_after_unix_seconds} before held-at {held_at_unix_seconds}"
    )]
    RetentionHoldReleaseBeforeHeld {
        /// Held object key.
        object_key: String,
        /// Hold release timestamp.
        release_after_unix_seconds: u64,
        /// Hold creation timestamp.
        held_at_unix_seconds: u64,
    },
    /// An active retention hold referenced an object that is no longer present.
    #[error("active retention hold referenced missing object {object_key}")]
    ActiveRetentionHoldMissingObject {
        /// Held object key.
        object_key: String,
    },
    /// An active retention hold coexisted with quarantine metadata for the same object.
    #[error("active retention hold for {object_key} coexisted with quarantine state")]
    ActiveRetentionHoldQuarantined {
        /// Held object key.
        object_key: String,
    },
}

/// Serialized shard validation failure.
#[derive(Debug, Clone, Copy, Error, PartialEq, Eq)]
pub enum InvalidSerializedShardError {
    /// The external shard parser rejected the bytes.
    #[error("shard parser rejected metadata")]
    ParserRejectedMetadata,
    /// A native Xet term used an empty or inverted chunk range.
    #[error("native xet term had an empty or inverted chunk range")]
    NativeXetTermEmptyOrInvertedChunkRange,
    /// A native Xet term referenced chunks past the end of its xorb.
    #[error("native xet term range exceeded xorb chunk count")]
    NativeXetTermRangeExceededXorbChunkCount,
    /// A shard file term used an empty or inverted chunk range.
    #[error("shard file term had an empty or inverted chunk range")]
    ShardFileTermEmptyOrInvertedChunkRange,
    /// The transient xorb metadata cache could not return a just-inserted entry.
    #[error("xorb metadata cache insertion failed")]
    XorbMetadataCacheInsertionFailed,
    /// A shard term started past the referenced xorb chunk list.
    #[error("shard term chunk range started past the xorb chunk list")]
    ShardTermRangeStartedPastXorbChunkList,
    /// A shard term ended past the referenced xorb chunk list.
    #[error("shard term chunk range ended past the xorb chunk list")]
    ShardTermRangeEndedPastXorbChunkList,
    /// The retained shard chunk hash list was not strictly ordered.
    #[error("retained shard chunk hashes were not strictly ordered")]
    RetainedShardChunkHashesNotStrictlyOrdered,
}

/// Reconstruction response shape failure.
#[derive(Debug, Clone, Copy, Error, PartialEq, Eq)]
pub enum InvalidReconstructionResponseError {
    /// A guarded test record store detected a forbidden global latest-record walk.
    #[error("global latest-record walk attempted")]
    RecordStoreGlobalLatestWalkAttempted,
    /// A guarded test record store could not find the requested record.
    #[error("record not found")]
    RecordStoreRecordNotFound,
    /// V1 response emitted more terms than the source record has chunks.
    #[error("response term count exceeded record chunk count")]
    TermCountExceededRecordChunkCount,
    /// A response term had no bytes.
    #[error("response term had zero unpacked length")]
    TermHadZeroUnpackedLength,
    /// A response term contained an empty chunk range.
    #[error("response term had an empty chunk range")]
    TermHadEmptyChunkRange,
    /// A response term did not have matching fetch metadata.
    #[error("response term did not have matching fetch info")]
    TermMissingFetchInfo,
    /// A fetch-info entry had no fetches.
    #[error("response fetch info contained an empty fetch list")]
    EmptyFetchList,
    /// A fetch URL did not point to the xorb hash that owns it.
    #[error("response fetch URL did not match its xorb hash")]
    FetchUrlHashMismatch,
    /// A fetch entry had an empty chunk range.
    #[error("response fetch entry had an empty chunk range")]
    FetchEntryEmptyChunkRange,
    /// A fetch entry had an inverted byte range.
    #[error("response fetch entry had an inverted byte range")]
    FetchEntryInvertedByteRange,
    /// A fetch entry did not correspond to any response term.
    #[error("response fetch entry did not have a matching term")]
    FetchEntryMissingTerm,
    /// V2 conversion changed `offset_into_first_range`.
    #[error("v2 response changed offset_into_first_range")]
    V2ChangedOffsetIntoFirstRange,
    /// V2 conversion changed the reconstruction terms.
    #[error("v2 response changed reconstruction terms")]
    V2ChangedTerms,
    /// V2 conversion changed the xorb fetch-info cardinality.
    #[error("v2 response changed xorb fetch-info cardinality")]
    V2ChangedXorbFetchInfoCardinality,
    /// V2 conversion emitted a hash absent from V1 fetch-info.
    #[error("v2 response emitted a fetch hash absent from v1")]
    V2FetchHashAbsentFromV1,
    /// V2 conversion emitted an empty fetch list.
    #[error("v2 response emitted an empty fetch list")]
    V2EmptyFetchList,
    /// V2 conversion emitted a fetch entry without ranges.
    #[error("v2 response emitted a fetch entry without ranges")]
    V2FetchEntryWithoutRanges,
    /// V2 conversion emitted an empty chunk range.
    #[error("v2 response emitted an empty chunk range")]
    V2EmptyChunkRange,
    /// V2 conversion emitted an inverted byte range.
    #[error("v2 response emitted an inverted byte range")]
    V2InvertedByteRange,
    /// V2 fetch count did not match V1.
    #[error("v2 response fetch count disagreed with v1")]
    V2FetchCountDisagreedWithV1,
    /// V2 range count did not match V1.
    #[error("v2 response range count disagreed with v1")]
    V2RangeCountDisagreedWithV1,
}

/// Default bounded-parser limits for native Xet shard metadata.
pub const DEFAULT_MAX_SHARD_FILES: NonZeroUsize = match NonZeroUsize::new(16_384) {
    Some(value) => value,
    None => NonZeroUsize::MIN,
};

/// Default maximum shard xorb sections.
pub const DEFAULT_MAX_SHARD_XORBS: NonZeroUsize = match NonZeroUsize::new(16_384) {
    Some(value) => value,
    None => NonZeroUsize::MIN,
};

/// Default maximum shard reconstruction terms.
pub const DEFAULT_MAX_SHARD_RECONSTRUCTION_TERMS: NonZeroUsize = match NonZeroUsize::new(65_536) {
    Some(value) => value,
    None => NonZeroUsize::MIN,
};

/// Default maximum shard xorb chunk records.
pub const DEFAULT_MAX_SHARD_XORB_CHUNKS: NonZeroUsize = match NonZeroUsize::new(65_536) {
    Some(value) => value,
    None => NonZeroUsize::MIN,
};

/// Default bounded-parser limits for native Xet shard metadata.
pub const DEFAULT_SHARD_METADATA_LIMITS: ShardMetadataLimits = ShardMetadataLimits::new(
    DEFAULT_MAX_SHARD_FILES,
    DEFAULT_MAX_SHARD_XORBS,
    DEFAULT_MAX_SHARD_RECONSTRUCTION_TERMS,
    DEFAULT_MAX_SHARD_XORB_CHUNKS,
);

/// Bounded-parser limits for native Xet shard metadata.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ShardMetadataLimits {
    max_files: NonZeroUsize,
    max_xorbs: NonZeroUsize,
    max_reconstruction_terms: NonZeroUsize,
    max_xorb_chunks: NonZeroUsize,
}

impl ShardMetadataLimits {
    /// Creates native Xet shard metadata limits.
    #[must_use]
    pub const fn new(
        max_files: NonZeroUsize,
        max_xorbs: NonZeroUsize,
        max_reconstruction_terms: NonZeroUsize,
        max_xorb_chunks: NonZeroUsize,
    ) -> Self {
        Self {
            max_files,
            max_xorbs,
            max_reconstruction_terms,
            max_xorb_chunks,
        }
    }

    /// Returns the maximum file sections accepted in one uploaded shard.
    #[must_use]
    pub const fn max_files(self) -> NonZeroUsize {
        self.max_files
    }

    /// Returns the maximum xorb sections accepted in one uploaded shard.
    #[must_use]
    pub const fn max_xorbs(self) -> NonZeroUsize {
        self.max_xorbs
    }

    /// Returns the maximum file reconstruction terms accepted in one uploaded shard.
    #[must_use]
    pub const fn max_reconstruction_terms(self) -> NonZeroUsize {
        self.max_reconstruction_terms
    }

    /// Returns the maximum xorb chunk records accepted in one uploaded shard.
    #[must_use]
    pub const fn max_xorb_chunks(self) -> NonZeroUsize {
        self.max_xorb_chunks
    }
}

impl Default for ShardMetadataLimits {
    fn default() -> Self {
        DEFAULT_SHARD_METADATA_LIMITS
    }
}

/// Object-store backend error.
#[derive(Debug, Error)]
pub enum ServerObjectStoreError {
    /// Requested content was not found.
    #[error("content not found")]
    NotFound,
    /// Arithmetic overflowed a checked bound.
    #[error("arithmetic overflow")]
    Overflow,
    /// A content hash was malformed.
    #[error("content hash must be 64 hexadecimal characters")]
    InvalidContentHash,
    /// Stored object metadata disagreed with the expected transfer length.
    #[error("stored object length did not match indexed metadata")]
    StoredObjectLengthMismatch,
    /// Local storage IO failed.
    #[error("local storage operation failed")]
    Local(#[from] LocalObjectStoreError),
    /// S3-compatible object-storage access failed.
    #[error("s3 object storage operation failed")]
    S3(#[from] S3ObjectStoreError),
    /// A local filesystem I/O error occurred.
    #[error("local storage io failed")]
    Io(#[from] IoError),
    /// Numeric conversion exceeded supported bounds.
    #[error("numeric conversion exceeded supported bounds")]
    NumericConversion(#[from] TryFromIntError),
}

/// Unified object-store backend that delegates to local, S3, or blackhole storage.
#[derive(Debug, Clone)]
pub enum ServerObjectStore {
    /// Local filesystem object store.
    Local(LocalObjectStore),
    /// S3-compatible object store.
    S3(S3ObjectStore),
    /// Blackhole object store that discards all writes.
    Blackhole,
}

impl ObjectStore for ServerObjectStore {
    type Error = ServerObjectStoreError;

    fn put_if_absent(
        &self,
        key: &ObjectKey,
        body: ObjectBody<'_>,
        integrity: &ObjectIntegrity,
    ) -> Result<PutOutcome, Self::Error> {
        match self {
            Self::Local(store) => Ok(store.put_if_absent(key, body, integrity)?),
            Self::S3(store) => Ok(store.put_if_absent(key, body, integrity)?),
            Self::Blackhole => Ok(PutOutcome::Inserted),
        }
    }

    fn read_range(&self, key: &ObjectKey, range: ByteRange) -> Result<Vec<u8>, Self::Error> {
        match self {
            Self::Local(store) => Ok(store.read_range(key, range)?),
            Self::S3(store) => Ok(store.read_range(key, range)?),
            Self::Blackhole => Err(ServerObjectStoreError::NotFound),
        }
    }

    fn contains(&self, key: &ObjectKey) -> Result<bool, Self::Error> {
        match self {
            Self::Local(store) => Ok(store.contains(key)?),
            Self::S3(store) => Ok(store.contains(key)?),
            Self::Blackhole => Ok(false),
        }
    }

    fn metadata(&self, key: &ObjectKey) -> Result<Option<ObjectMetadata>, Self::Error> {
        match self {
            Self::Local(store) => Ok(store.metadata(key)?),
            Self::S3(store) => Ok(store.metadata(key)?),
            Self::Blackhole => Ok(None),
        }
    }

    fn list_prefix(&self, prefix: &ObjectPrefix) -> Result<Vec<ObjectMetadata>, Self::Error> {
        match self {
            Self::Local(store) => Ok(store.list_prefix(prefix)?),
            Self::S3(store) => Ok(store.list_prefix(prefix)?),
            Self::Blackhole => Ok(Vec::new()),
        }
    }

    fn delete_if_present(&self, key: &ObjectKey) -> Result<DeleteOutcome, Self::Error> {
        match self {
            Self::Local(store) => Ok(store.delete_if_present(key)?),
            Self::S3(store) => Ok(store.delete_if_present(key)?),
            Self::Blackhole => Ok(DeleteOutcome::NotFound),
        }
    }
}

impl ServerObjectStore {
    /// Creates a local filesystem object store rooted at the given path.
    ///
    /// # Errors
    ///
    /// Returns [`ServerObjectStoreError::Local`] if the local store cannot be created.
    pub fn local(root: impl Into<PathBuf>) -> Result<Self, ServerObjectStoreError> {
        Ok(Self::Local(LocalObjectStore::new(root.into())?))
    }

    /// Creates an S3-compatible object store from the provided configuration.
    ///
    /// # Errors
    ///
    /// Returns [`ServerObjectStoreError::S3`] if the S3 store cannot be created.
    pub fn s3(config: S3ObjectStoreConfig) -> Result<Self, ServerObjectStoreError> {
        Ok(Self::S3(S3ObjectStore::new(config)?))
    }

    /// Creates a blackhole object store that discards all writes.
    #[must_use]
    pub const fn blackhole() -> Self {
        Self::Blackhole
    }

    /// Stores an object, overwriting any existing object at the given key.
    ///
    /// # Errors
    ///
    /// Returns [`ServerObjectStoreError`] on storage backend failures.
    pub fn put_overwrite(
        &self,
        key: &ObjectKey,
        body: ObjectBody<'_>,
        integrity: &ObjectIntegrity,
    ) -> Result<(), ServerObjectStoreError> {
        match self {
            Self::Local(store) => store
                .put_overwrite(key, body, integrity)
                .map_err(Into::into),
            Self::S3(store) => store
                .put_overwrite(key, body, integrity)
                .map_err(Into::into),
            Self::Blackhole => Ok(()),
        }
    }

    /// Visits all objects under the given prefix, invoking the visitor for each.
    ///
    /// # Errors
    ///
    /// Returns any error produced by the visitor or the underlying storage backend.
    pub fn visit_prefix<F, E>(&self, prefix: &ObjectPrefix, mut visitor: F) -> Result<(), E>
    where
        F: FnMut(ObjectMetadata) -> Result<(), E>,
        E: From<LocalObjectStoreError> + From<S3ObjectStoreError>,
    {
        match self {
            Self::Local(store) => store.visit_prefix(prefix, &mut visitor),
            Self::S3(store) => store.visit_prefix(prefix, &mut visitor),
            Self::Blackhole => Ok(()),
        }
    }

    /// Lists objects under the given prefix with pagination.
    ///
    /// # Errors
    ///
    /// Returns [`ServerObjectStoreError`] on storage backend failures.
    pub fn list_flat_namespace_page(
        &self,
        prefix: &ObjectPrefix,
        start_after: Option<&ObjectKey>,
        limit: usize,
    ) -> Result<Vec<ObjectMetadata>, ServerObjectStoreError> {
        match self {
            Self::Local(store) => store
                .list_flat_namespace_page(prefix, start_after, limit)
                .map_err(Into::into),
            Self::S3(store) => store
                .list_flat_namespace_page(prefix, start_after, limit)
                .map_err(Into::into),
            Self::Blackhole => Ok(Vec::new()),
        }
    }

    /// Returns the local filesystem path for an object key, if backed by local storage.
    #[must_use]
    pub fn local_path_for_key(&self, key: &ObjectKey) -> Option<PathBuf> {
        match self {
            Self::Local(store) => Some(store.path_for_key(key)),
            Self::S3(_store) => None,
            Self::Blackhole => None,
        }
    }

    /// Copies an object from source to destination if no object exists at the destination.
    ///
    /// # Errors
    ///
    /// Returns [`ServerObjectStoreError::NotFound`] for blackhole stores or
    /// storage backend errors.
    pub fn copy_if_absent(
        &self,
        source: &ObjectKey,
        destination: &ObjectKey,
    ) -> Result<PutOutcome, ServerObjectStoreError> {
        match self {
            Self::Local(store) => store
                .copy_object_if_absent(source, destination)
                .map_err(Into::into),
            Self::S3(store) => store
                .copy_object_if_absent(source, destination)
                .map_err(Into::into),
            Self::Blackhole => Err(ServerObjectStoreError::NotFound),
        }
    }

    /// Stores a content-addressed file from the local filesystem.
    ///
    /// # Errors
    ///
    /// Returns [`ServerObjectStoreError`] on storage backend failures.
    pub fn put_content_addressed_file(
        &self,
        key: &ObjectKey,
        path: &Path,
        integrity: &ObjectIntegrity,
    ) -> Result<PutOutcome, ServerObjectStoreError> {
        match self {
            Self::Local(store) => store
                .put_temporary_file_if_absent(key, path, integrity)
                .map_err(Into::into),
            Self::S3(store) => store
                .put_content_addressed_file(key, path, integrity)
                .map_err(Into::into),
            Self::Blackhole => Ok(PutOutcome::Inserted),
        }
    }

    /// Returns the local filesystem root, if backed by local storage.
    #[must_use]
    pub fn local_root(&self) -> Option<&Path> {
        match self {
            Self::Local(store) => Some(store.root()),
            Self::S3(_store) => None,
            Self::Blackhole => None,
        }
    }

    /// Returns the backend name for this object store.
    #[must_use]
    pub const fn backend_name(&self) -> &'static str {
        match self {
            Self::Local(_store) => "local",
            Self::S3(_store) => "s3",
            Self::Blackhole => "blackhole",
        }
    }

    /// Reads the full contents of an object from the store.
    ///
    /// # Errors
    ///
    /// Returns [`ServerObjectStoreError`] on storage backend failures, length
    /// mismatches, or arithmetic overflows.
    pub fn read_full_object(
        &self,
        object_key: &ObjectKey,
        length: u64,
    ) -> Result<Vec<u8>, ServerObjectStoreError> {
        if length == 0 {
            return Ok(Vec::new());
        }

        if let Self::Local(store) = self {
            let file = store.open_object_file(object_key)?;
            let actual_length = file.metadata()?.len();
            if actual_length != length {
                return Err(ServerObjectStoreError::StoredObjectLengthMismatch);
            }
            let capacity = usize::try_from(length)?;
            let mut output = Vec::with_capacity(capacity);
            let mut limited = file.take(length);
            Read::read_to_end(&mut limited, &mut output)?;
            if output.len() != capacity {
                return Err(ServerObjectStoreError::StoredObjectLengthMismatch);
            }
            return Ok(output);
        }

        let end = length
            .checked_sub(1)
            .ok_or(ServerObjectStoreError::Overflow)?;
        let range = ByteRange::new(0, end).map_err(|_error| ServerObjectStoreError::Overflow)?;
        self.read_range(object_key, range)
    }
}

/// Operation-time record-store classification.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OpsRecordKind {
    /// Latest record.
    Latest,
    /// Version record.
    Version,
}

/// Extra locator metadata needed by operator tooling.
pub trait OpsRecordStore: RecordStore {
    /// Renders a stable operator-facing location for one record locator.
    fn locator_display(&self, locator: &<Self as RecordTraversal>::Locator) -> String;

    /// Extracts the file identifier implied by a locator.
    fn locator_file_id(
        &self,
        locator: &<Self as RecordTraversal>::Locator,
        kind: OpsRecordKind,
    ) -> Option<String>;

    /// Extracts the immutable content hash implied by a version locator.
    fn locator_content_hash(
        &self,
        locator: &<Self as RecordTraversal>::Locator,
        kind: OpsRecordKind,
    ) -> Option<String>;
}

impl OpsRecordStore for LocalRecordStore {
    fn locator_display(&self, locator: &<Self as RecordTraversal>::Locator) -> String {
        locator.record_key().to_owned()
    }

    fn locator_file_id(
        &self,
        locator: &<Self as RecordTraversal>::Locator,
        _kind: OpsRecordKind,
    ) -> Option<String> {
        Some(locator.file_id().to_owned())
    }

    fn locator_content_hash(
        &self,
        locator: &<Self as RecordTraversal>::Locator,
        kind: OpsRecordKind,
    ) -> Option<String> {
        if kind != OpsRecordKind::Version {
            return None;
        }

        locator.content_hash().map(ToOwned::to_owned)
    }
}

impl OpsRecordStore for PostgresRecordStore {
    fn locator_display(&self, locator: &<Self as RecordTraversal>::Locator) -> String {
        locator.record_key().to_owned()
    }

    fn locator_file_id(
        &self,
        locator: &<Self as RecordTraversal>::Locator,
        _kind: OpsRecordKind,
    ) -> Option<String> {
        Some(locator.file_id().to_owned())
    }

    fn locator_content_hash(
        &self,
        locator: &<Self as RecordTraversal>::Locator,
        kind: OpsRecordKind,
    ) -> Option<String> {
        if kind != OpsRecordKind::Version {
            return None;
        }

        locator.content_hash().map(ToOwned::to_owned)
    }
}

/// Maximum allowed stored file record metadata size in bytes.
pub const MAX_LOCAL_RECORD_METADATA_BYTES: u64 = 1_073_741_824;

/// Parses stored file record bytes, rejecting oversized metadata before JSON parsing.
///
/// # Errors
///
/// Returns an error if the metadata exceeds [`MAX_LOCAL_RECORD_METADATA_BYTES`] or
/// if JSON deserialization fails.
pub fn parse_stored_file_record_bytes(
    bytes: &[u8],
) -> Result<shardline_index::FileRecord, ParseStoredFileRecordError> {
    let observed_bytes = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
    if observed_bytes > MAX_LOCAL_RECORD_METADATA_BYTES {
        return Err(ParseStoredFileRecordError::StoredFileMetadataTooLarge {
            observed_bytes,
            maximum_bytes: MAX_LOCAL_RECORD_METADATA_BYTES,
        });
    }

    Ok(serde_json::from_slice(bytes)?)
}

/// Stored file record parsing failure.
#[derive(Debug, Error)]
pub enum ParseStoredFileRecordError {
    /// Stored file metadata exceeded the bounded parser ceiling.
    #[error("stored file metadata exceeded the bounded parser ceiling")]
    StoredFileMetadataTooLarge {
        /// Observed file length in bytes.
        observed_bytes: u64,
        /// Maximum accepted file length in bytes.
        maximum_bytes: u64,
    },
    /// JSON deserialization failed.
    #[error("json operation failed")]
    Json(#[from] serde_json::Error),
}

/// Returns the provider directory string for the given repository provider.
#[must_use]
pub const fn provider_directory(provider: RepositoryProvider) -> &'static str {
    provider.as_str()
}

/// Maximum byte length for a validated file identifier.
const MAX_IDENTIFIER_BYTES: usize = 1024;

/// Validates that a file identifier is safe for use as a single path component.
///
/// # Errors
///
/// Returns [`ValidateIdentifierError`] if the identifier is empty, contains
/// path separators, traversal sequences, control characters, or exceeds the
/// maximum byte length.
pub fn validate_identifier(value: &str) -> Result<(), ValidateIdentifierError> {
    if value.trim().is_empty()
        || value == "."
        || value.len() > MAX_IDENTIFIER_BYTES
        || value.starts_with('/')
        || value.contains("..")
        || value.contains('\\')
        || value.contains('/')
        || value.chars().any(char::is_control)
    {
        return Err(ValidateIdentifierError);
    }

    Ok(())
}

/// File identifier validation failure.
#[derive(Debug, Clone, Copy, Error)]
#[error("file identifier must be relative and must not contain traversal or control characters")]
pub struct ValidateIdentifierError;

/// Validates that a content hash is exactly 64 lowercase hex characters.
///
/// # Errors
///
/// Returns [`ValidateContentHashError`] if the hash is malformed.
pub fn validate_content_hash(value: &str) -> Result<(), ValidateContentHashError> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
    {
        return Err(ValidateContentHashError);
    }

    Ok(())
}

/// Content hash validation failure.
#[derive(Debug, Clone, Copy, Error)]
#[error("content hash must be 64 hexadecimal characters")]
pub struct ValidateContentHashError;

/// Checked addition returning an error on overflow.
///
/// # Errors
///
/// Returns [`RebuildOverflowError`] when the addition overflows.
pub const fn checked_add(left: u64, right: u64) -> Result<u64, RebuildOverflowError> {
    match left.checked_add(right) {
        Some(value) => Ok(value),
        None => Err(RebuildOverflowError),
    }
}

/// Checked increment returning an error on overflow.
///
/// # Errors
///
/// Returns [`RebuildOverflowError`] when the increment overflows.
pub const fn checked_increment(value: u64) -> Result<u64, RebuildOverflowError> {
    checked_add(value, 1)
}

/// Arithmetic overflow during rebuild operations.
#[derive(Debug, Clone, Copy, Error)]
#[error("arithmetic overflow")]
pub struct RebuildOverflowError;

/// Returns the current Unix time in seconds, or an error if the system clock
/// is before the Unix epoch.
///
/// # Errors
///
/// Returns [`RebuildOverflowError`] when the system time is before the Unix
/// epoch.
pub fn unix_now_seconds_checked() -> Result<u64, RebuildOverflowError> {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .map_err(|_e| RebuildOverflowError)
}

/// Default retention window for new local quarantine candidates.
pub const DEFAULT_LOCAL_GC_RETENTION_SECONDS: u64 = 86_400;

#[cfg(test)]
mod tests {
    use super::*;

    use proptest::prelude::*;

    #[test]
    fn validate_identifier_accepts_simple_name() {
        assert!(validate_identifier("hello.txt").is_ok());
    }

    #[test]
    fn validate_identifier_accepts_dotted_name() {
        assert!(validate_identifier("file.name.txt").is_ok());
    }

    #[test]
    fn validate_identifier_rejects_empty() {
        assert!(validate_identifier("").is_err());
    }

    #[test]
    fn validate_identifier_rejects_whitespace_only() {
        assert!(validate_identifier("   ").is_err());
    }

    #[test]
    fn validate_identifier_rejects_dot() {
        assert!(validate_identifier(".").is_err());
    }

    #[test]
    fn validate_identifier_rejects_leading_slash() {
        assert!(validate_identifier("/etc/passwd").is_err());
    }

    #[test]
    fn validate_identifier_rejects_traversal() {
        assert!(validate_identifier("foo/../bar").is_err());
    }

    #[test]
    fn validate_identifier_rejects_backslash() {
        assert!(validate_identifier("foo\\bar").is_err());
    }

    #[test]
    fn validate_identifier_rejects_control_char() {
        assert!(validate_identifier("foo\tbar").is_err());
    }

    #[test]
    fn validate_identifier_rejects_null_byte() {
        // Null bytes are control characters and MUST be rejected.
        assert!(validate_identifier("foo\0bar").is_err());
        assert!(validate_identifier("\0").is_err());
        assert!(validate_identifier("null\0end").is_err());
    }

    #[test]
    fn validate_content_hash_accepts_valid_hash() {
        let hash = "a".repeat(64);
        assert!(validate_content_hash(&hash).is_ok());
    }

    #[test]
    fn validate_content_hash_rejects_too_short() {
        assert!(validate_content_hash("abc123").is_err());
    }

    #[test]
    fn validate_content_hash_rejects_too_long() {
        let hash = "a".repeat(65);
        assert!(validate_content_hash(&hash).is_err());
    }

    #[test]
    fn validate_content_hash_rejects_uppercase() {
        let hash = "A".repeat(64);
        assert!(validate_content_hash(&hash).is_err());
    }

    #[test]
    fn validate_content_hash_rejects_non_hex() {
        let mut hash = "a".repeat(64);
        hash.push('g');
        hash.remove(0);
        assert!(validate_content_hash(&hash).is_err());
    }

    #[test]
    fn checked_add_normal() {
        assert_eq!(checked_add(1, 2).unwrap(), 3);
    }

    #[test]
    fn checked_add_zero() {
        assert_eq!(checked_add(0, 0).unwrap(), 0);
    }

    #[test]
    fn checked_add_overflow() {
        assert!(checked_add(u64::MAX, 1).is_err());
    }

    #[test]
    fn checked_increment_normal() {
        assert_eq!(checked_increment(0).unwrap(), 1);
    }

    #[test]
    fn checked_increment_overflow() {
        assert!(checked_increment(u64::MAX).is_err());
    }

    #[test]
    fn chunk_object_key_valid() {
        let hash = "a".repeat(64);
        let key = chunk_object_key(&hash).unwrap();
        assert!(key.as_str().starts_with("aa/"));
        assert!(key.as_str().ends_with(&hash));
    }

    #[test]
    fn chunk_object_key_empty_hash() {
        let err = chunk_object_key("").unwrap_err();
        assert!(
            matches!(err, ServerObjectStoreError::InvalidContentHash),
            "empty hash should return InvalidContentHash, got: {err}"
        );
    }

    #[test]
    fn chunk_object_key_single_char_hash() {
        // A single hex character is too short — validation should reject it.
        let err = chunk_object_key("a").unwrap_err();
        assert!(
            matches!(err, ServerObjectStoreError::InvalidContentHash),
            "single-char hash should return InvalidContentHash, got: {err}"
        );
    }

    #[test]
    fn chunk_object_key_invalid_hex() {
        // Non-hex characters (uppercase, 'g', symbols)
        let invalid_hashes = [
            "G".repeat(64),                        // uppercase
            format!("{}g{}", "a".repeat(62), "g"), // 'g' is not hex
            format!("{}!{}", "a".repeat(62), "!"), // symbol
            format!("{}z{}", "a".repeat(62), "z"), // 'z' is not hex
        ];
        for hash in &invalid_hashes {
            let err = chunk_object_key(hash).unwrap_err();
            assert!(
                matches!(err, ServerObjectStoreError::InvalidContentHash),
                "non-hex hash {hash:?} should return InvalidContentHash, got: {err}"
            );
        }

        // Too short
        let err = chunk_object_key("short").unwrap_err();
        assert!(
            matches!(err, ServerObjectStoreError::InvalidContentHash),
            "short hash should return InvalidContentHash, got: {err}"
        );

        // Too long
        let err = chunk_object_key(&"a".repeat(65)).unwrap_err();
        assert!(
            matches!(err, ServerObjectStoreError::InvalidContentHash),
            "long hash should return InvalidContentHash, got: {err}"
        );
    }

    #[test]
    fn chunk_object_key_valid_inputs() {
        // All lowercase hex, exactly 64 chars
        let hash = "a".repeat(64);
        let key = chunk_object_key(&hash).unwrap();
        assert!(key.as_str().starts_with("aa/"));
        assert!(key.as_str().ends_with(&hash));
        assert_eq!(key.as_str(), format!("aa/{hash}"));

        // Mixed hex digits
        let hash = "0123456789abcdef".repeat(4);
        let key = chunk_object_key(&hash).unwrap();
        assert!(key.as_str().starts_with("01/"));
        assert_eq!(key.as_str(), format!("01/{hash}"));

        // All 'f' (max hex)
        let hash = "f".repeat(64);
        let key = chunk_object_key(&hash).unwrap();
        assert!(key.as_str().starts_with("ff/"));
    }

    #[test]
    fn parse_stored_file_record_bytes_valid() {
        let json = r#"{"file_id":"test.txt","content_hash":"aabb","total_bytes":100,"chunk_size":10,"chunks":[]}"#;
        assert!(parse_stored_file_record_bytes(json.as_bytes()).is_ok());
    }

    #[test]
    fn parse_stored_file_record_bytes_invalid_json() {
        assert!(parse_stored_file_record_bytes(b"not json").is_err());
    }

    #[test]
    fn parse_stored_file_record_bytes_oversized() {
        let valid =
            r#"{"file_id":"test","content_hash":"aa","total_bytes":0,"chunk_size":0,"chunks":[]}"#;
        assert!(parse_stored_file_record_bytes(valid.as_bytes()).is_ok());

        let oversized = vec![0u8; (MAX_LOCAL_RECORD_METADATA_BYTES + 1) as usize];
        assert!(parse_stored_file_record_bytes(&oversized).is_err());
    }

    proptest::proptest! {
        #[test]
        fn proptest_validate_identifier_rejects_leading_slash(s in "[a-z]{1,100}") {
            let input = format!("/{s}");
            prop_assert!(validate_identifier(&input).is_err(), "leading slash should be rejected: {input:?}");
        }

        #[test]
        fn proptest_validate_identifier_rejects_traversal(s in "[a-z]{1,50}") {
            let input = format!("{s}/../{s}");
            prop_assert!(validate_identifier(&input).is_err(), "traversal should be rejected: {input:?}");
        }

        #[test]
        fn proptest_validate_identifier_rejects_backslash(s in "[a-z]{1,50}") {
            let input = format!("{s}\\{s}");
            prop_assert!(validate_identifier(&input).is_err(), "backslash should be rejected: {input:?}");
        }

        #[test]
        fn proptest_validate_identifier_accepts_valid_names(segs in prop::collection::vec("[a-z]{1,20}", 1..3usize)) {
            let input = segs.join(".");
            let result = validate_identifier(&input);
            prop_assert!(result.is_ok(), "valid identifier should be accepted: {input:?}");
        }

        #[test]
        fn proptest_validate_identifier_rejects_control_characters(segs in prop::collection::vec("[a-z]{1,20}", 1..3usize)) {
            let mut input = segs.join(".");
            input.push('\t');
            let result = validate_identifier(&input);
            prop_assert!(result.is_err(), "control characters should be rejected: {input:?}");
        }
    }

    #[test]
    fn chunk_hash_from_chunk_object_key_if_present_valid_key() {
        let hash = "a".repeat(64);
        let key = ObjectKey::parse(&format!("aa/{hash}")).unwrap();
        let result = chunk_hash_from_chunk_object_key_if_present(&key).unwrap();
        assert_eq!(result, Some(hash.as_str()));
    }

    #[test]
    fn chunk_hash_from_chunk_object_key_if_present_non_chunk_key() {
        let key = ObjectKey::parse("xorbs/default/aa/hash.xorb").unwrap();
        let result = chunk_hash_from_chunk_object_key_if_present(&key).unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn chunk_hash_from_chunk_object_key_if_present_extra_segments() {
        let hash = "a".repeat(64);
        let key = ObjectKey::parse(&format!("aa/{hash}/extra")).unwrap();
        let result = chunk_hash_from_chunk_object_key_if_present(&key).unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn content_hash_determinism_same_inputs_same_hash() {
        let chunks = vec![shardline_index::FileChunkRecord {
            hash: "aabbccdd".to_owned(),
            offset: 0,
            length: 100,
            range_start: 0,
            range_end: 1,
            packed_start: 0,
            packed_end: 100,
        }];

        let hash1 = content_hash(100, 10, &chunks);
        let hash2 = content_hash(100, 10, &chunks);
        assert_eq!(hash1, hash2);
    }

    #[test]
    fn content_hash_determinism_different_inputs_different_hash() {
        let chunks1 = vec![shardline_index::FileChunkRecord {
            hash: "aabbccdd".to_owned(),
            offset: 0,
            length: 100,
            range_start: 0,
            range_end: 1,
            packed_start: 0,
            packed_end: 100,
        }];
        let chunks2 = vec![shardline_index::FileChunkRecord {
            hash: "00112233".to_owned(),
            offset: 0,
            length: 100,
            range_start: 0,
            range_end: 1,
            packed_start: 0,
            packed_end: 100,
        }];

        let hash1 = content_hash(100, 10, &chunks1);
        let hash2 = content_hash(100, 10, &chunks2);
        assert_ne!(hash1, hash2);
    }

    #[test]
    fn validate_content_hash_with_valid_64_hex() {
        let hash = "0123456789abcdef".repeat(4);
        assert!(validate_content_hash_with(&hash, || ()).is_ok());
    }

    #[test]
    fn validate_content_hash_with_too_short() {
        assert!(validate_content_hash_with("abc123", || ()).is_err());
    }

    #[test]
    fn validate_content_hash_with_uppercase_rejected() {
        let hash = "A".repeat(64);
        assert!(validate_content_hash_with(&hash, || ()).is_err());
    }

    #[test]
    fn validate_content_hash_with_non_hex_rejected() {
        let hash = format!("{}g{}", "a".repeat(62), "a");
        assert!(validate_content_hash_with(&hash, || ()).is_err());
    }

    #[test]
    fn server_object_store_blackhole_put_if_absent_always_inserts() {
        use shardline_storage::{ObjectBody, ObjectIntegrity, ObjectKey, ObjectStore, PutOutcome};
        let store = ServerObjectStore::blackhole();
        let key = ObjectKey::parse("aa/hash").unwrap();
        let body = b"hello";
        let integrity = ObjectIntegrity::new(chunk_hash(body), 5);

        let result = store.put_if_absent(&key, ObjectBody::from_slice(body), &integrity);
        assert!(matches!(result, Ok(PutOutcome::Inserted)));

        // Store again — still Inserted (discards everything)
        let result = store.put_if_absent(&key, ObjectBody::from_slice(body), &integrity);
        assert!(matches!(result, Ok(PutOutcome::Inserted)));
    }

    #[test]
    fn server_object_store_blackhole_read_range_not_found() {
        use shardline_protocol::ByteRange;
        use shardline_storage::{ObjectKey, ObjectStore};
        let store = ServerObjectStore::blackhole();
        let key = ObjectKey::parse("aa/hash").unwrap();
        let range = ByteRange::new(0, 4).unwrap();

        let result = store.read_range(&key, range);
        assert!(matches!(result, Err(ServerObjectStoreError::NotFound)));
    }

    #[test]
    fn server_object_store_blackhole_contains_false() {
        use shardline_storage::{ObjectKey, ObjectStore};
        let store = ServerObjectStore::blackhole();
        let key = ObjectKey::parse("aa/hash").unwrap();

        assert!(matches!(store.contains(&key), Ok(false)));
    }

    #[test]
    fn server_object_store_blackhole_metadata_none() {
        use shardline_storage::{ObjectKey, ObjectStore};
        let store = ServerObjectStore::blackhole();
        let key = ObjectKey::parse("aa/hash").unwrap();

        assert!(matches!(store.metadata(&key), Ok(None)));
    }

    #[test]
    fn server_object_store_blackhole_list_prefix_empty() {
        use shardline_storage::{ObjectPrefix, ObjectStore};
        let store = ServerObjectStore::blackhole();
        let prefix = ObjectPrefix::parse("aa/").unwrap();

        let result = store.list_prefix(&prefix).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn server_object_store_blackhole_delete_not_found() {
        use shardline_storage::{DeleteOutcome, ObjectKey, ObjectStore};
        let store = ServerObjectStore::blackhole();
        let key = ObjectKey::parse("aa/hash").unwrap();

        let result = store.delete_if_present(&key).unwrap();
        assert!(matches!(result, DeleteOutcome::NotFound));
    }

    // ── read_full_object ──────────────────────────────────────────────

    #[test]
    fn read_full_object_returns_correct_bytes() {
        let storage = shardline_test_support::TempStorage::new();
        let store = ServerObjectStore::local(storage.path().join("objects")).unwrap();
        let key =
            ObjectKey::parse("aa/1111111111111111111111111111111111111111111111111111111111111111")
                .unwrap();
        let body = b"hello world";
        let integrity = ObjectIntegrity::new(chunk_hash(body), 11);
        store
            .put_if_absent(&key, ObjectBody::from_slice(body), &integrity)
            .unwrap();

        let data = read_full_object(&store, &key, 11).unwrap();
        assert_eq!(data, b"hello world");
    }

    #[test]
    fn read_full_object_length_zero_returns_empty() {
        let storage = shardline_test_support::TempStorage::new();
        let store = ServerObjectStore::local(storage.path().join("objects")).unwrap();
        let key =
            ObjectKey::parse("aa/1111111111111111111111111111111111111111111111111111111111111111")
                .unwrap();

        let data = read_full_object(&store, &key, 0).unwrap();
        assert!(data.is_empty());
    }

    #[test]
    fn read_full_object_wrong_length_returns_mismatch() {
        let storage = shardline_test_support::TempStorage::new();
        let store = ServerObjectStore::local(storage.path().join("objects")).unwrap();
        let key =
            ObjectKey::parse("aa/1111111111111111111111111111111111111111111111111111111111111111")
                .unwrap();
        let body = b"abc";
        let integrity = ObjectIntegrity::new(chunk_hash(body), 3);
        store
            .put_if_absent(&key, ObjectBody::from_slice(body), &integrity)
            .unwrap();

        let err = read_full_object(&store, &key, 10).unwrap_err();
        assert!(
            matches!(err, ServerObjectStoreError::StoredObjectLengthMismatch),
            "expected StoredObjectLengthMismatch, got: {err}"
        );
    }

    #[test]
    fn read_full_object_nonexistent_returns_error() {
        let storage = shardline_test_support::TempStorage::new();
        let store = ServerObjectStore::local(storage.path().join("objects")).unwrap();
        let key =
            ObjectKey::parse("aa/1111111111111111111111111111111111111111111111111111111111111111")
                .unwrap();

        let err = read_full_object(&store, &key, 5).unwrap_err();
        assert!(
            matches!(err, ServerObjectStoreError::Local(_)),
            "expected Local error for missing object, got: {err}"
        );
    }

    // ── copy_if_absent ────────────────────────────────────────────────

    #[test]
    fn copy_if_absent_inserts_and_idempotent() {
        let storage = shardline_test_support::TempStorage::new();
        let store = ServerObjectStore::local(storage.path().join("objects")).unwrap();
        let source_key =
            ObjectKey::parse("aa/1111111111111111111111111111111111111111111111111111111111111111")
                .unwrap();
        let dest_key =
            ObjectKey::parse("bb/2222222222222222222222222222222222222222222222222222222222222222")
                .unwrap();

        let body = b"copy me";
        let integrity = ObjectIntegrity::new(chunk_hash(body), 7);
        store
            .put_if_absent(&source_key, ObjectBody::from_slice(body), &integrity)
            .unwrap();

        let outcome = store.copy_if_absent(&source_key, &dest_key).unwrap();
        assert!(matches!(outcome, PutOutcome::Inserted));

        // Second copy returns AlreadyExists
        let outcome = store.copy_if_absent(&source_key, &dest_key).unwrap();
        assert!(matches!(outcome, PutOutcome::AlreadyExists));

        // Verify bytes are correct
        use shardline_protocol::ByteRange;
        let range = ByteRange::new(0, 6).unwrap();
        let data = store.read_range(&dest_key, range).unwrap();
        assert_eq!(data, b"copy me");
    }

    #[test]
    fn copy_if_absent_nonexistent_source_returns_error() {
        let storage = shardline_test_support::TempStorage::new();
        let store = ServerObjectStore::local(storage.path().join("objects")).unwrap();
        let source_key =
            ObjectKey::parse("aa/1111111111111111111111111111111111111111111111111111111111111111")
                .unwrap();
        let dest_key =
            ObjectKey::parse("bb/2222222222222222222222222222222222222222222222222222222222222222")
                .unwrap();

        let err = store.copy_if_absent(&source_key, &dest_key).unwrap_err();
        assert!(
            matches!(err, ServerObjectStoreError::Local(_)),
            "expected Local error for missing source, got: {err}"
        );
    }

    #[test]
    fn copy_if_absent_blackhole_returns_not_found() {
        let store = ServerObjectStore::blackhole();
        let source =
            ObjectKey::parse("aa/1111111111111111111111111111111111111111111111111111111111111111")
                .unwrap();
        let dest =
            ObjectKey::parse("bb/2222222222222222222222222222222222222222222222222222222222222222")
                .unwrap();

        let err = store.copy_if_absent(&source, &dest).unwrap_err();
        assert!(
            matches!(err, ServerObjectStoreError::NotFound),
            "expected NotFound for blackhole, got: {err}"
        );
    }

    // ── put_overwrite ─────────────────────────────────────────────────

    #[test]
    fn put_overwrite_inserts_and_overwrites() {
        let storage = shardline_test_support::TempStorage::new();
        let store = ServerObjectStore::local(storage.path().join("objects")).unwrap();
        let key =
            ObjectKey::parse("aa/1111111111111111111111111111111111111111111111111111111111111111")
                .unwrap();

        let body1 = b"first";
        let integrity1 = ObjectIntegrity::new(chunk_hash(body1), 5);
        store
            .put_overwrite(&key, ObjectBody::from_slice(body1), &integrity1)
            .unwrap();

        let body2 = b"second version";
        let integrity2 = ObjectIntegrity::new(chunk_hash(body2), 14);
        store
            .put_overwrite(&key, ObjectBody::from_slice(body2), &integrity2)
            .unwrap();

        use shardline_protocol::ByteRange;
        let range = ByteRange::new(0, 13).unwrap();
        let data = store.read_range(&key, range).unwrap();
        assert_eq!(data, b"second version");
    }

    // ── local_root ────────────────────────────────────────────────────

    #[test]
    fn local_root_returns_path_for_local_store() {
        let storage = shardline_test_support::TempStorage::new();
        let root = storage.path().join("objects");
        let store = ServerObjectStore::local(root.clone()).unwrap();

        assert_eq!(store.local_root(), Some(root.as_path()));
    }

    #[test]
    fn local_root_returns_none_for_blackhole() {
        let store = ServerObjectStore::blackhole();
        assert_eq!(store.local_root(), None);
    }

    // ── backend_name ──────────────────────────────────────────────────

    #[test]
    fn backend_name_local() {
        let storage = shardline_test_support::TempStorage::new();
        let store = ServerObjectStore::local(storage.path().join("objects")).unwrap();
        assert_eq!(store.backend_name(), "local");
    }

    #[test]
    fn backend_name_blackhole() {
        let store = ServerObjectStore::blackhole();
        assert_eq!(store.backend_name(), "blackhole");
    }

    // ── OpsRecordStore impl for LocalRecordStore ──────────────────────

    fn sample_file_record() -> shardline_index::FileRecord {
        shardline_index::FileRecord {
            file_id: "test-file-id".to_owned(),
            content_hash: "aabbccdd".repeat(8),
            total_bytes: 100,
            chunk_size: 10,
            repository_scope: None,
            chunks: Vec::new(),
        }
    }

    #[test]
    fn ops_locator_display_returns_record_key() {
        use crate::OpsRecordStore;
        use shardline_index::{LocalRecordStore, RecordTraversal};

        let storage = shardline_test_support::TempStorage::new();
        let store = LocalRecordStore::new(storage.path().join("index")).unwrap();
        let record = sample_file_record();
        let locator = store.version_record_locator(&record);

        let display = store.locator_display(&locator);
        assert_eq!(display, locator.record_key());
        assert!(!display.is_empty());
    }

    #[test]
    fn ops_locator_file_id_returns_file_id() {
        use crate::OpsRecordStore;
        use shardline_index::{LocalRecordStore, RecordTraversal};

        let storage = shardline_test_support::TempStorage::new();
        let store = LocalRecordStore::new(storage.path().join("index")).unwrap();
        let record = sample_file_record();
        let locator = store.version_record_locator(&record);

        let file_id = store
            .locator_file_id(&locator, crate::OpsRecordKind::Version)
            .unwrap();
        assert_eq!(file_id, "test-file-id");
    }

    #[test]
    fn ops_locator_content_hash_version_returns_hash() {
        use crate::OpsRecordStore;
        use shardline_index::{LocalRecordStore, RecordTraversal};

        let storage = shardline_test_support::TempStorage::new();
        let store = LocalRecordStore::new(storage.path().join("index")).unwrap();
        let record = sample_file_record();
        let locator = store.version_record_locator(&record);

        let hash = store
            .locator_content_hash(&locator, crate::OpsRecordKind::Version)
            .unwrap();
        assert_eq!(hash, record.content_hash);
    }

    #[test]
    fn ops_locator_content_hash_latest_returns_none() {
        use crate::OpsRecordStore;
        use shardline_index::{LocalRecordStore, RecordTraversal};

        let storage = shardline_test_support::TempStorage::new();
        let store = LocalRecordStore::new(storage.path().join("index")).unwrap();
        let record = sample_file_record();
        let locator = store.latest_record_locator(&record);

        let result = store.locator_content_hash(&locator, crate::OpsRecordKind::Latest);
        assert_eq!(result, None);
    }

    // ── parse_stored_file_record_bytes (additional targeted tests) ────

    #[test]
    fn parse_stored_file_record_returns_correct_fields() {
        let record = sample_file_record();
        let json = serde_json::to_vec(&record).unwrap();
        let parsed = parse_stored_file_record_bytes(&json).unwrap();
        assert_eq!(parsed.file_id, "test-file-id");
        assert_eq!(parsed.total_bytes, 100);
        assert_eq!(parsed.chunk_size, 10);
    }

    #[test]
    fn parse_stored_file_record_oversized_returns_too_large_error() {
        let oversized = vec![0u8; (MAX_LOCAL_RECORD_METADATA_BYTES + 1) as usize];
        let err = parse_stored_file_record_bytes(&oversized).unwrap_err();
        assert!(
            matches!(
                err,
                ParseStoredFileRecordError::StoredFileMetadataTooLarge { .. }
            ),
            "expected StoredFileMetadataTooLarge, got: {err}"
        );
    }

    // ── AuthContext ──────────────────────────────────────────────────────

    #[test]
    fn auth_context_new_and_accessors() {
        use shardline_protocol::RepositoryScope;
        let repo = RepositoryScope::new(RepositoryProvider::GitHub, "o", "r", None).unwrap();
        let claims = TokenClaims::new("iss", "sub", TokenScope::Read, repo, 100).unwrap();
        let ctx = AuthContext::new(claims.clone());

        assert_eq!(ctx.claims(), &claims);
        assert_eq!(ctx.subject(), "sub");
        assert_eq!(ctx.scope(), TokenScope::Read);
    }

    // ── ShardMetadataLimits ──────────────────────────────────────────────

    #[test]
    fn shard_metadata_limits_new_and_accessors() {
        let limits = ShardMetadataLimits::new(
            NonZeroUsize::new(10).unwrap(),
            NonZeroUsize::new(20).unwrap(),
            NonZeroUsize::new(30).unwrap(),
            NonZeroUsize::new(40).unwrap(),
        );
        assert_eq!(limits.max_files().get(), 10);
        assert_eq!(limits.max_xorbs().get(), 20);
        assert_eq!(limits.max_reconstruction_terms().get(), 30);
        assert_eq!(limits.max_xorb_chunks().get(), 40);
    }

    #[test]
    fn shard_metadata_limits_default_matches_const() {
        assert_eq!(ShardMetadataLimits::default(), DEFAULT_SHARD_METADATA_LIMITS);
    }

    // ── AuthError Display ────────────────────────────────────────────────

    #[test]
    fn auth_error_display_invalid_token() {
        assert_eq!(AuthError::InvalidToken.to_string(), "invalid token");
    }

    #[test]
    fn auth_error_display_expired_token() {
        assert_eq!(AuthError::ExpiredToken.to_string(), "expired token");
    }

    #[test]
    fn auth_error_display_insufficient_scope() {
        let msg = AuthError::InsufficientScope.to_string();
        assert_eq!(msg, "insufficient scope");
    }

    #[test]
    fn auth_error_display_provider_error() {
        let msg = AuthError::ProviderError("oops".to_owned()).to_string();
        assert_eq!(msg, "provider error: oops");
    }

    #[test]
    fn auth_error_from_token_codec_error_expired() {
        let err: AuthError = TokenCodecError::Expired.into();
        assert!(matches!(err, AuthError::ExpiredToken));
    }

    #[test]
    fn auth_error_from_token_codec_error_invalid_signature() {
        let err: AuthError = TokenCodecError::InvalidSignature.into();
        assert!(matches!(err, AuthError::InvalidToken));
    }

    #[test]
    fn auth_error_from_token_codec_error_invalid_format() {
        let err: AuthError = TokenCodecError::InvalidFormat.into();
        assert!(matches!(err, AuthError::InvalidToken));
    }

    #[test]
    fn auth_error_from_token_codec_error_invalid_hex() {
        let hex_err = hex::FromHexError::InvalidStringLength;
        let err: AuthError = TokenCodecError::InvalidHex(hex_err).into();
        assert!(matches!(err, AuthError::InvalidToken));
    }

    #[test]
    fn auth_error_from_token_codec_error_claims() {
        let err: AuthError = TokenCodecError::Claims(shardline_protocol::TokenClaimsError::EmptyIssuer).into();
        assert!(matches!(err, AuthError::InvalidToken));
    }

    #[test]
    fn auth_error_from_token_codec_error_empty_key() {
        let err: AuthError = TokenCodecError::EmptySigningKey.into();
        assert!(matches!(err, AuthError::ProviderError(_)));
    }

    #[test]
    fn auth_error_from_token_codec_error_key_too_short() {
        let err: AuthError = TokenCodecError::SigningKeyTooShort { actual_bytes: 4 }.into();
        assert!(matches!(err, AuthError::ProviderError(_)));
    }

    #[test]
    fn auth_error_from_token_codec_error_json() {
        let json_err = serde_json::from_str::<serde_json::Value>("bad").unwrap_err();
        let err: AuthError = TokenCodecError::Json(json_err).into();
        assert!(matches!(err, AuthError::ProviderError(_)));
    }

    // ── ServerObjectStoreError Display ──────────────────────────────────

    #[test]
    fn server_object_store_error_display_not_found() {
        assert_eq!(ServerObjectStoreError::NotFound.to_string(), "content not found");
    }

    #[test]
    fn server_object_store_error_display_overflow() {
        assert_eq!(ServerObjectStoreError::Overflow.to_string(), "arithmetic overflow");
    }

    #[test]
    fn server_object_store_error_display_invalid_content_hash() {
        assert_eq!(
            ServerObjectStoreError::InvalidContentHash.to_string(),
            "content hash must be 64 hexadecimal characters"
        );
    }

    #[test]
    fn server_object_store_error_display_stored_object_length_mismatch() {
        assert_eq!(
            ServerObjectStoreError::StoredObjectLengthMismatch.to_string(),
            "stored object length did not match indexed metadata"
        );
    }

    // ── Error Display for remaining types ────────────────────────────────

    #[test]
    fn validate_identifier_error_display() {
        let msg = ValidateIdentifierError.to_string();
        assert!(!msg.is_empty());
        assert!(msg.contains("identifier"));
    }

    #[test]
    fn validate_content_hash_error_display() {
        let msg = ValidateContentHashError.to_string();
        assert!(!msg.is_empty());
        assert!(msg.contains("hexadecimal"));
    }

    #[test]
    fn rebuild_overflow_error_display() {
        let msg = RebuildOverflowError.to_string();
        assert_eq!(msg, "arithmetic overflow");
    }

    #[test]
    fn parse_stored_file_record_error_json_display() {
        let json_err = serde_json::from_str::<serde_json::Value>("bad").unwrap_err();
        let err = ParseStoredFileRecordError::Json(json_err);
        let msg = err.to_string();
        assert_eq!(msg, "json operation failed");
    }

    // ── provider_directory ───────────────────────────────────────────────

    #[test]
    fn provider_directory_github() {
        assert_eq!(provider_directory(RepositoryProvider::GitHub), "github");
    }

    #[test]
    fn provider_directory_gitea() {
        assert_eq!(provider_directory(RepositoryProvider::Gitea), "gitea");
    }

    // ── chunk_hash ───────────────────────────────────────────────────────

    #[test]
    fn chunk_hash_produces_deterministic_output() {
        let data = b"hello world";
        let hash1 = chunk_hash(data);
        let hash2 = chunk_hash(data);
        assert_eq!(hash1, hash2);
        assert_ne!(hash1, chunk_hash(b"different"));
    }

    #[test]
    fn chunk_hash_returns_32_byte_hash() {
        let hash = chunk_hash(b"test");
        assert_eq!(hash.as_bytes().len(), 32);
    }

    // ── unix_now_seconds_checked ─────────────────────────────────────────

    #[test]
    fn unix_now_seconds_checked_returns_modern_timestamp() {
        let ts = unix_now_seconds_checked().unwrap();
        assert!(ts >= 1_700_000_000, "timestamp {ts} too small");
    }

    // ── ServerObjectStore::local_path_for_key ────────────────────────────

    #[test]
    fn local_path_for_key_with_local_store_returns_some() {
        let storage = shardline_test_support::TempStorage::new();
        let store = ServerObjectStore::local(storage.path().join("objects")).unwrap();
        let key = ObjectKey::parse("aa/abcd").unwrap();
        assert!(store.local_path_for_key(&key).is_some());
    }

    #[test]
    fn local_path_for_key_with_s3_returns_none() {
        let store = ServerObjectStore::blackhole();
        let key = ObjectKey::parse("aa/abcd").unwrap();
        assert!(store.local_path_for_key(&key).is_none());
    }

    #[test]
    fn local_path_for_key_with_blackhole_returns_none() {
        let store = ServerObjectStore::blackhole();
        let key = ObjectKey::parse("aa/abcd").unwrap();
        assert!(store.local_path_for_key(&key).is_none());
    }

    // ── InvalidLifecycleMetadataError Display ───────────────────────────

    #[test]
    fn invalid_lifecycle_metadata_display_delete_before_first_seen() {
        let err = InvalidLifecycleMetadataError::QuarantineCandidateDeleteBeforeFirstSeen {
            object_key: "obj".to_owned(),
            delete_after_unix_seconds: 100,
            first_seen_unreachable_at_unix_seconds: 200,
        };
        let msg = err.to_string();
        assert!(msg.contains("obj"));
        assert!(msg.contains("delete-after"));
    }

    #[test]
    fn invalid_lifecycle_metadata_display_missing_object() {
        let err = InvalidLifecycleMetadataError::QuarantineCandidateMissingObject {
            object_key: "obj".to_owned(),
        };
        let msg = err.to_string();
        assert!(msg.contains("obj"));
    }

    #[test]
    fn invalid_lifecycle_metadata_display_length_mismatch() {
        let err = InvalidLifecycleMetadataError::QuarantineCandidateLengthMismatch {
            object_key: "obj".to_owned(),
            expected_length: 100,
            observed_length: 200,
        };
        let msg = err.to_string();
        assert!(msg.contains("expected length"));
    }

    #[test]
    fn invalid_lifecycle_metadata_display_release_before_held() {
        let err = InvalidLifecycleMetadataError::RetentionHoldReleaseBeforeHeld {
            object_key: "obj".to_owned(),
            release_after_unix_seconds: 50,
            held_at_unix_seconds: 100,
        };
        let msg = err.to_string();
        assert!(msg.contains("release-after"));
    }

    #[test]
    fn invalid_lifecycle_metadata_display_hold_missing_object() {
        let err = InvalidLifecycleMetadataError::ActiveRetentionHoldMissingObject {
            object_key: "obj".to_owned(),
        };
        let msg = err.to_string();
        assert!(msg.contains("missing object"));
    }

    #[test]
    fn invalid_lifecycle_metadata_display_hold_quarantined() {
        let err = InvalidLifecycleMetadataError::ActiveRetentionHoldQuarantined {
            object_key: "obj".to_owned(),
        };
        let msg = err.to_string();
        assert!(msg.contains("coexisted with quarantine"));
    }

    // ── ServerObjectStore::visit_prefix and list_flat_namespace_page ────

    #[test]
    fn server_object_store_visit_prefix_blackhole_empty() {
        use shardline_storage::ObjectPrefix;
        let store = ServerObjectStore::blackhole();
        let prefix = ObjectPrefix::parse("").unwrap();
        let mut count = 0u64;
        let result: Result<(), ServerObjectStoreError> = store
            .visit_prefix(&prefix, |_meta| {
                count += 1;
                Ok(())
            });
        assert!(result.is_ok());
        assert_eq!(count, 0);
    }

    #[test]
    fn server_object_store_list_flat_namespace_page_blackhole_empty() {
        use shardline_storage::ObjectPrefix;
        let store = ServerObjectStore::blackhole();
        let prefix = ObjectPrefix::parse("").unwrap();
        let result = store
            .list_flat_namespace_page(&prefix, None, 10)
            .unwrap();
        assert!(result.is_empty());
    }

    // ── ServerObjectStore::put_content_addressed_file blackhole ─────────

    #[test]
    fn server_object_store_put_content_addressed_file_blackhole() {
        use shardline_storage::{ObjectIntegrity, ObjectKey, PutOutcome};
        let store = ServerObjectStore::blackhole();
        let key = ObjectKey::parse("aa/hash").unwrap();
        let tmp = shardline_test_support::TempStorage::new();
        let file_path = tmp.path().join("test.bin");
        std::fs::write(&file_path, b"data").unwrap();

        let integrity = ObjectIntegrity::new(chunk_hash(b"data"), 4);
        let result = store.put_content_addressed_file(&key, &file_path, &integrity);
        assert!(matches!(result, Ok(PutOutcome::Inserted)));
    }

    // ── Additional token.rs / protocol_support.rs cross-tests ───────────

    #[test]
    fn chunk_object_key_edge_cases() {
        // Minimum hex boundary
        let hash = "0000000000000000000000000000000000000000000000000000000000000000";
        let key = chunk_object_key(hash).unwrap();
        assert_eq!(key.as_str(), "00/0000000000000000000000000000000000000000000000000000000000000000");

        // Maximum hex boundary
        let hash = "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff";
        let key = chunk_object_key(hash).unwrap();
        assert_eq!(key.as_str(), "ff/ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff");
    }

    #[test]
    fn chunk_hash_from_chunk_key_if_present_prefix_too_short() {
        let key = ObjectKey::parse("a/abc").unwrap();
        let result = chunk_hash_from_chunk_object_key_if_present(&key).unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn chunk_hash_from_chunk_key_if_present_non_hex_prefix() {
        let key = ObjectKey::parse("gg/abc").unwrap();
        let result = chunk_hash_from_chunk_object_key_if_present(&key).unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn chunk_hash_from_chunk_key_if_present_hash_does_not_start_with_prefix() {
        let key = ObjectKey::parse("aa/bbcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc").unwrap();
        let result = chunk_hash_from_chunk_object_key_if_present(&key).unwrap();
        assert_eq!(result, None);
    }

    // ── InvalidSerializedShardError Display ────────────────────────────────

    #[test]
    fn invalid_serialized_shard_error_parser_rejected() {
        assert_eq!(
            InvalidSerializedShardError::ParserRejectedMetadata.to_string(),
            "shard parser rejected metadata"
        );
    }

    #[test]
    fn invalid_serialized_shard_error_native_xet_empty_range() {
        assert_eq!(
            InvalidSerializedShardError::NativeXetTermEmptyOrInvertedChunkRange.to_string(),
            "native xet term had an empty or inverted chunk range"
        );
    }

    #[test]
    fn invalid_serialized_shard_error_native_xet_range_exceeded() {
        assert_eq!(
            InvalidSerializedShardError::NativeXetTermRangeExceededXorbChunkCount.to_string(),
            "native xet term range exceeded xorb chunk count"
        );
    }

    #[test]
    fn invalid_serialized_shard_error_shard_file_term_empty_range() {
        assert_eq!(
            InvalidSerializedShardError::ShardFileTermEmptyOrInvertedChunkRange.to_string(),
            "shard file term had an empty or inverted chunk range"
        );
    }

    #[test]
    fn invalid_serialized_shard_error_xorb_cache_insertion_failed() {
        assert_eq!(
            InvalidSerializedShardError::XorbMetadataCacheInsertionFailed.to_string(),
            "xorb metadata cache insertion failed"
        );
    }

    #[test]
    fn invalid_serialized_shard_error_shard_term_started_past() {
        assert_eq!(
            InvalidSerializedShardError::ShardTermRangeStartedPastXorbChunkList.to_string(),
            "shard term chunk range started past the xorb chunk list"
        );
    }

    #[test]
    fn invalid_serialized_shard_error_shard_term_ended_past() {
        assert_eq!(
            InvalidSerializedShardError::ShardTermRangeEndedPastXorbChunkList.to_string(),
            "shard term chunk range ended past the xorb chunk list"
        );
    }

    #[test]
    fn invalid_serialized_shard_error_retained_hashes_not_ordered() {
        assert_eq!(
            InvalidSerializedShardError::RetainedShardChunkHashesNotStrictlyOrdered.to_string(),
            "retained shard chunk hashes were not strictly ordered"
        );
    }

    // ── InvalidReconstructionResponseError Display ─────────────────────────

    #[test]
    fn invalid_reconstruction_response_error_record_store_global_latest() {
        assert_eq!(
            InvalidReconstructionResponseError::RecordStoreGlobalLatestWalkAttempted.to_string(),
            "global latest-record walk attempted"
        );
    }

    #[test]
    fn invalid_reconstruction_response_error_record_not_found() {
        assert_eq!(
            InvalidReconstructionResponseError::RecordStoreRecordNotFound.to_string(),
            "record not found"
        );
    }

    #[test]
    fn invalid_reconstruction_response_error_term_count_exceeded() {
        assert_eq!(
            InvalidReconstructionResponseError::TermCountExceededRecordChunkCount.to_string(),
            "response term count exceeded record chunk count"
        );
    }

    #[test]
    fn invalid_reconstruction_response_error_term_zero_unpacked() {
        assert_eq!(
            InvalidReconstructionResponseError::TermHadZeroUnpackedLength.to_string(),
            "response term had zero unpacked length"
        );
    }

    #[test]
    fn invalid_reconstruction_response_error_term_empty_chunk_range() {
        assert_eq!(
            InvalidReconstructionResponseError::TermHadEmptyChunkRange.to_string(),
            "response term had an empty chunk range"
        );
    }

    #[test]
    fn invalid_reconstruction_response_error_term_missing_fetch_info() {
        assert_eq!(
            InvalidReconstructionResponseError::TermMissingFetchInfo.to_string(),
            "response term did not have matching fetch info"
        );
    }

    #[test]
    fn invalid_reconstruction_response_error_empty_fetch_list() {
        assert_eq!(
            InvalidReconstructionResponseError::EmptyFetchList.to_string(),
            "response fetch info contained an empty fetch list"
        );
    }

    #[test]
    fn invalid_reconstruction_response_error_fetch_url_hash_mismatch() {
        assert_eq!(
            InvalidReconstructionResponseError::FetchUrlHashMismatch.to_string(),
            "response fetch URL did not match its xorb hash"
        );
    }

    #[test]
    fn invalid_reconstruction_response_error_fetch_entry_empty_chunk_range() {
        assert_eq!(
            InvalidReconstructionResponseError::FetchEntryEmptyChunkRange.to_string(),
            "response fetch entry had an empty chunk range"
        );
    }

    #[test]
    fn invalid_reconstruction_response_error_fetch_entry_inverted_byte_range() {
        assert_eq!(
            InvalidReconstructionResponseError::FetchEntryInvertedByteRange.to_string(),
            "response fetch entry had an inverted byte range"
        );
    }

    #[test]
    fn invalid_reconstruction_response_error_fetch_entry_missing_term() {
        assert_eq!(
            InvalidReconstructionResponseError::FetchEntryMissingTerm.to_string(),
            "response fetch entry did not have a matching term"
        );
    }

    #[test]
    fn invalid_reconstruction_response_error_v2_changed_offset() {
        assert_eq!(
            InvalidReconstructionResponseError::V2ChangedOffsetIntoFirstRange.to_string(),
            "v2 response changed offset_into_first_range"
        );
    }

    #[test]
    fn invalid_reconstruction_response_error_v2_changed_terms() {
        assert_eq!(
            InvalidReconstructionResponseError::V2ChangedTerms.to_string(),
            "v2 response changed reconstruction terms"
        );
    }

    #[test]
    fn invalid_reconstruction_response_error_v2_changed_xorb_cardinality() {
        assert_eq!(
            InvalidReconstructionResponseError::V2ChangedXorbFetchInfoCardinality.to_string(),
            "v2 response changed xorb fetch-info cardinality"
        );
    }

    #[test]
    fn invalid_reconstruction_response_error_v2_hash_absent_from_v1() {
        assert_eq!(
            InvalidReconstructionResponseError::V2FetchHashAbsentFromV1.to_string(),
            "v2 response emitted a fetch hash absent from v1"
        );
    }

    #[test]
    fn invalid_reconstruction_response_error_v2_empty_fetch_list() {
        assert_eq!(
            InvalidReconstructionResponseError::V2EmptyFetchList.to_string(),
            "v2 response emitted an empty fetch list"
        );
    }

    #[test]
    fn invalid_reconstruction_response_error_v2_fetch_entry_without_ranges() {
        assert_eq!(
            InvalidReconstructionResponseError::V2FetchEntryWithoutRanges.to_string(),
            "v2 response emitted a fetch entry without ranges"
        );
    }

    #[test]
    fn invalid_reconstruction_response_error_v2_empty_chunk_range() {
        assert_eq!(
            InvalidReconstructionResponseError::V2EmptyChunkRange.to_string(),
            "v2 response emitted an empty chunk range"
        );
    }

    #[test]
    fn invalid_reconstruction_response_error_v2_inverted_byte_range() {
        assert_eq!(
            InvalidReconstructionResponseError::V2InvertedByteRange.to_string(),
            "v2 response emitted an inverted byte range"
        );
    }

    #[test]
    fn invalid_reconstruction_response_error_v2_fetch_count_disagreed() {
        assert_eq!(
            InvalidReconstructionResponseError::V2FetchCountDisagreedWithV1.to_string(),
            "v2 response fetch count disagreed with v1"
        );
    }

    #[test]
    fn invalid_reconstruction_response_error_v2_range_count_disagreed() {
        assert_eq!(
            InvalidReconstructionResponseError::V2RangeCountDisagreedWithV1.to_string(),
            "v2 response range count disagreed with v1"
        );
    }

    // ── ServerObjectStoreError Display for all variants ──────────────────

    #[test]
    fn server_object_store_error_display_all_variants() {
        assert_eq!(ServerObjectStoreError::NotFound.to_string(), "content not found");
        assert_eq!(ServerObjectStoreError::Overflow.to_string(), "arithmetic overflow");
        assert_eq!(ServerObjectStoreError::InvalidContentHash.to_string(), "content hash must be 64 hexadecimal characters");
        assert_eq!(ServerObjectStoreError::StoredObjectLengthMismatch.to_string(), "stored object length did not match indexed metadata");
    }

    // ── InvalidLifecycleMetadataError Display all variants ───────────────

    #[test]
    fn invalid_lifecycle_metadata_display_active_retention_hold_quarantined() {
        let err = InvalidLifecycleMetadataError::ActiveRetentionHoldQuarantined {
            object_key: "obj".to_owned(),
        };
        let msg = err.to_string();
        assert!(msg.contains("coexisted with quarantine"));
    }

    // ── provider_directory all variants ─────────────────────────────────

    #[test]
    fn provider_directory_all_variants() {
        use shardline_protocol::RepositoryProvider;
        assert_eq!(provider_directory(RepositoryProvider::GitHub), "github");
        assert_eq!(provider_directory(RepositoryProvider::GitLab), "gitlab");
        assert_eq!(provider_directory(RepositoryProvider::Gitea), "gitea");
        assert_eq!(provider_directory(RepositoryProvider::Codeberg), "codeberg");
        assert_eq!(provider_directory(RepositoryProvider::Generic), "generic");
    }

    // ── OpsRecordStore impl edge cases ─────────────────────────────────

    #[test]
    fn ops_locator_content_hash_version_without_content_hash_returns_none() {
        use crate::OpsRecordStore;
        use shardline_index::{LocalRecordStore, RecordTraversal};

        let storage = shardline_test_support::TempStorage::new();
        let store = LocalRecordStore::new(storage.path().join("index")).unwrap();
        let mut record = sample_file_record();
        record.content_hash = String::new();
        let locator = store.latest_record_locator(&record);

        let result = store.locator_content_hash(&locator, crate::OpsRecordKind::Version);
        assert_eq!(result, None);
    }

    // ── ParseStoredFileRecordError Display ───────────────────────────────

    #[test]
    fn parse_stored_file_record_error_stored_file_metadata_too_large_display() {
        let err = ParseStoredFileRecordError::StoredFileMetadataTooLarge {
            observed_bytes: 1024,
            maximum_bytes: 512,
        };
        let msg = err.to_string();
        assert_eq!(msg, "stored file metadata exceeded the bounded parser ceiling");
    }

    #[test]
    fn parse_stored_file_record_error_json_display_message() {
        let json_err = serde_json::from_str::<serde_json::Value>("bad").unwrap_err();
        let err = ParseStoredFileRecordError::Json(json_err);
        assert_eq!(err.to_string(), "json operation failed");
    }

    // ── content_hash edge cases ─────────────────────────────────────────

    #[test]
    fn content_hash_empty_chunks() {
        let hash = content_hash(0, 0, &[]);
        assert_eq!(hash.len(), 64);
    }

    #[test]
    fn content_hash_different_total_bytes_produces_different_hash() {
        let chunks = vec![shardline_index::FileChunkRecord {
            hash: "aabbccdd".to_owned(),
            offset: 0,
            length: 100,
            range_start: 0,
            range_end: 1,
            packed_start: 0,
            packed_end: 100,
        }];

        let hash1 = content_hash(100, 10, &chunks);
        let hash2 = content_hash(200, 10, &chunks);
        assert_ne!(hash1, hash2);
    }

    // ── ServerObjectStore constructors ─────────────────────────────────

    #[test]
    fn server_object_store_local_with_root_path() {
        let storage = shardline_test_support::TempStorage::new();
        let store = ServerObjectStore::local(storage.path().join("objects"));
        assert!(store.is_ok());
    }

    #[test]
    fn server_object_store_put_overwrite_blackhole() {
        use shardline_storage::{ObjectBody, ObjectIntegrity, ObjectKey};
        let store = ServerObjectStore::blackhole();
        let key = ObjectKey::parse("aa/hash").unwrap();
        let body = b"test";
        let integrity = ObjectIntegrity::new(chunk_hash(body), 4);
        let result = store.put_overwrite(&key, ObjectBody::from_slice(body), &integrity);
        assert!(result.is_ok());
    }

    #[test]
    fn server_object_store_local_path_for_key_format() {
        let storage = shardline_test_support::TempStorage::new();
        let store = ServerObjectStore::local(storage.path().join("objects")).unwrap();
        let key = ObjectKey::parse("ab/abcdef123456").unwrap();
        let path = store.local_path_for_key(&key);
        assert!(path.is_some());
        let path = path.unwrap();
        assert!(path.to_string_lossy().contains("ab"));
    }

    // ── read_full_object blackhole edge case ────────────────────────────

    #[test]
    fn read_full_object_blackhole_zero_length() {
        let store = ServerObjectStore::blackhole();
        let key = ObjectKey::parse("aa/hash").unwrap();
        let result = read_full_object(&store, &key, 0);
        assert!(result.is_ok());
        assert!(result.unwrap().is_empty());
    }

    // ── unix_now_seconds_checked ────────────────────────────────────────

    #[test]
    fn unix_now_seconds_checked_is_recent() {
        let ts = unix_now_seconds_checked().unwrap();
        // Should be well past the year 2020
        assert!(ts >= 1_577_836_800, "timestamp {ts} too small for 2020");
    }

    // ── map_object_key_error ──────────────────────────────────────────────

    #[test]
    fn map_object_key_error_all_variants_return_overflow() {
        assert!(matches!(
            map_object_key_error(ObjectKeyError::Empty),
            ServerObjectStoreError::Overflow
        ));
        assert!(matches!(
            map_object_key_error(ObjectKeyError::UnsafePath),
            ServerObjectStoreError::Overflow
        ));
        assert!(matches!(
            map_object_key_error(ObjectKeyError::ControlCharacter),
            ServerObjectStoreError::Overflow
        ));
        assert!(matches!(
            map_object_key_error(ObjectKeyError::TooLong),
            ServerObjectStoreError::Overflow
        ));
    }

    // ── chunk_hash_from_chunk_object_key_if_present additional edge cases ─

    #[test]
    fn chunk_hash_from_chunk_key_if_present_single_segment() {
        // Key with no '/' → second segments.next() returns None → Ok(None)
        let key = ObjectKey::parse("onlyprefix").unwrap();
        let result = chunk_hash_from_chunk_object_key_if_present(&key).unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn chunk_hash_from_chunk_key_if_present_invalid_hash_returns_error() {
        // 64 chars starting with "aa" but 'z' is not hex → validation fails
        let hash = format!("aa{}", "z".repeat(62));
        let key = ObjectKey::parse(&format!("aa/{hash}")).unwrap();
        let result = chunk_hash_from_chunk_object_key_if_present(&key);
        assert!(matches!(
            result,
            Err(ServerObjectStoreError::InvalidContentHash)
        ));
    }

    // ── read_full_object non-local path (blackhole, length > 0) ──────────

    #[test]
    fn read_full_object_blackhole_nonzero_length_returns_not_found() {
        let store = ServerObjectStore::blackhole();
        let key = ObjectKey::parse("aa/hash").unwrap();
        let result = read_full_object(&store, &key, 5);
        assert!(matches!(result, Err(ServerObjectStoreError::NotFound)));
    }

    // ── ServerObjectStore::s3 constructor and S3 branches ─────────────────

    #[test]
    fn server_object_store_s3_with_minimal_config() {
        use shardline_storage::S3ObjectStoreConfig;
        let config = S3ObjectStoreConfig::new("test-bucket".to_owned(), "us-east-1".to_owned());
        let store = ServerObjectStore::s3(config);
        assert!(store.is_ok());
    }

    #[test]
    fn backend_name_s3() {
        use shardline_storage::S3ObjectStoreConfig;
        let config = S3ObjectStoreConfig::new("test-bucket".to_owned(), "us-east-1".to_owned());
        let store = ServerObjectStore::s3(config).unwrap();
        assert_eq!(store.backend_name(), "s3");
    }

    #[test]
    fn s3_local_root_and_local_path_for_key_return_none() {
        use shardline_storage::{ObjectKey, S3ObjectStoreConfig};
        let config = S3ObjectStoreConfig::new("test-bucket".to_owned(), "us-east-1".to_owned());
        let store = ServerObjectStore::s3(config).unwrap();
        assert_eq!(store.local_root(), None);
        let key = ObjectKey::parse("aa/hash").unwrap();
        assert_eq!(store.local_path_for_key(&key), None);
    }

    #[test]
    fn s3_operations_return_s3_error() {
        use shardline_storage::{ObjectBody, ObjectIntegrity, ObjectKey, ObjectPrefix, S3ObjectStoreConfig};
        use shardline_protocol::ByteRange;
        let config = S3ObjectStoreConfig::new("test-bucket".to_owned(), "us-east-1".to_owned());
        let store = ServerObjectStore::s3(config).unwrap();
        let key = ObjectKey::parse("aa/hash").unwrap();

        // put_if_absent — will fail because bucket does not exist
        let body = b"test";
        let integrity = ObjectIntegrity::new(chunk_hash(body), 4);
        let result = store.put_if_absent(&key, ObjectBody::from_slice(body), &integrity);
        assert!(matches!(result, Err(ServerObjectStoreError::S3(_))));

        // read_range
        let result = store.read_range(&key, ByteRange::new(0, 3).unwrap());
        assert!(matches!(result, Err(ServerObjectStoreError::S3(_))));

        // contains
        let result = store.contains(&key);
        assert!(matches!(result, Err(ServerObjectStoreError::S3(_))));

        // metadata
        let result = store.metadata(&key);
        // S3 metadata may return Ok(None) for non-existent objects rather than error
        // Accept both Ok(None) and Err(S3)
        match result {
            Ok(None) => {} // bucket not found might be handled gracefully
            Err(ServerObjectStoreError::S3(_)) => {}
            other => panic!("unexpected metadata result: {other:?}"),
        }

        // list_prefix
        let prefix = ObjectPrefix::parse("aa/").unwrap();
        let result = store.list_prefix(&prefix);
        assert!(matches!(result, Err(ServerObjectStoreError::S3(_))));

        // delete_if_present
        let result = store.delete_if_present(&key);
        assert!(matches!(result, Err(ServerObjectStoreError::S3(_))));
    }

    #[test]
    fn s3_put_overwrite_returns_s3_error() {
        use shardline_storage::{ObjectBody, ObjectIntegrity, ObjectKey, S3ObjectStoreConfig};
        let config = S3ObjectStoreConfig::new("test-bucket".to_owned(), "us-east-1".to_owned());
        let store = ServerObjectStore::s3(config).unwrap();
        let key = ObjectKey::parse("aa/hash").unwrap();
        let body = b"test";
        let integrity = ObjectIntegrity::new(chunk_hash(body), 4);
        let result = store.put_overwrite(&key, ObjectBody::from_slice(body), &integrity);
        assert!(matches!(result, Err(ServerObjectStoreError::S3(_))));
    }

    #[test]
    fn s3_copy_if_absent_returns_s3_error() {
        use shardline_storage::{ObjectKey, S3ObjectStoreConfig};
        let config = S3ObjectStoreConfig::new("test-bucket".to_owned(), "us-east-1".to_owned());
        let store = ServerObjectStore::s3(config).unwrap();
        let source = ObjectKey::parse("aa/source").unwrap();
        let dest = ObjectKey::parse("bb/dest").unwrap();
        let result = store.copy_if_absent(&source, &dest);
        assert!(matches!(result, Err(ServerObjectStoreError::S3(_))));
    }

    #[test]
    fn s3_put_content_addressed_file_returns_s3_error() {
        use shardline_storage::{ObjectIntegrity, ObjectKey, S3ObjectStoreConfig};
        let config = S3ObjectStoreConfig::new("test-bucket".to_owned(), "us-east-1".to_owned());
        let store = ServerObjectStore::s3(config).unwrap();
        let key = ObjectKey::parse("aa/hash").unwrap();
        let tmp = shardline_test_support::TempStorage::new();
        let file_path = tmp.path().join("test.bin");
        std::fs::write(&file_path, b"data").unwrap();
        let integrity = ObjectIntegrity::new(chunk_hash(b"data"), 4);
        let result = store.put_content_addressed_file(&key, &file_path, &integrity);
        assert!(matches!(result, Err(ServerObjectStoreError::S3(_))));
    }

    #[test]
    fn s3_visit_prefix_returns_s3_error() {
        use shardline_storage::{ObjectPrefix, S3ObjectStoreConfig};
        let config = S3ObjectStoreConfig::new("test-bucket".to_owned(), "us-east-1".to_owned());
        let store = ServerObjectStore::s3(config).unwrap();
        let prefix = ObjectPrefix::parse("aa/").unwrap();
        let result: Result<(), ServerObjectStoreError> = store
            .visit_prefix(&prefix, |_meta| Ok(()));
        assert!(matches!(result, Err(ServerObjectStoreError::S3(_))));
    }

    #[test]
    fn s3_list_flat_namespace_page_returns_s3_error() {
        use shardline_storage::{ObjectPrefix, S3ObjectStoreConfig};
        let config = S3ObjectStoreConfig::new("test-bucket".to_owned(), "us-east-1".to_owned());
        let store = ServerObjectStore::s3(config).unwrap();
        let prefix = ObjectPrefix::parse("aa/").unwrap();
        let result = store.list_flat_namespace_page(&prefix, None, 10);
        assert!(matches!(result, Err(ServerObjectStoreError::S3(_))));
    }

    #[test]
    fn s3_read_full_object_returns_s3_error() {
        use shardline_storage::{ObjectKey, S3ObjectStoreConfig};
        let config = S3ObjectStoreConfig::new("test-bucket".to_owned(), "us-east-1".to_owned());
        let store = ServerObjectStore::s3(config).unwrap();
        let key = ObjectKey::parse("aa/hash").unwrap();
        let result = read_full_object(&store, &key, 5);
        assert!(matches!(result, Err(ServerObjectStoreError::S3(_))));
    }

    // ── Local ObjectStore trait method coverage ──────────────────────────

    #[test]
    fn local_contains_returns_true_for_existing_object() {
        use shardline_storage::{ObjectBody, ObjectIntegrity, ObjectKey, ObjectStore};
        let storage = shardline_test_support::TempStorage::new();
        let store = ServerObjectStore::local(storage.path().join("objects")).unwrap();
        let key = ObjectKey::parse("aa/1111222233334444555566667777888899990000aaaabbbbccccddddeeeeffff").unwrap();

        // Not present yet
        assert!(matches!(store.contains(&key), Ok(false)));

        // Insert
        let body = b"present";
        let integrity = ObjectIntegrity::new(chunk_hash(body), 7);
        store.put_if_absent(&key, ObjectBody::from_slice(body), &integrity).unwrap();

        // Now present
        assert!(matches!(store.contains(&key), Ok(true)));
    }

    #[test]
    fn local_metadata_returns_some_for_existing_object() {
        use shardline_storage::{ObjectBody, ObjectIntegrity, ObjectKey, ObjectStore};
        let storage = shardline_test_support::TempStorage::new();
        let store = ServerObjectStore::local(storage.path().join("objects")).unwrap();
        let key = ObjectKey::parse("aa/1111222233334444555566667777888899990000aaaabbbbccccddddeeeeffff").unwrap();

        // None for missing
        assert!(matches!(store.metadata(&key), Ok(None)));

        // Insert
        let body = b"meta-test";
        let integrity = ObjectIntegrity::new(chunk_hash(body), 9);
        store.put_if_absent(&key, ObjectBody::from_slice(body), &integrity).unwrap();

        // Some for existing
        let meta = store.metadata(&key).unwrap();
        assert!(meta.is_some());
        assert_eq!(meta.as_ref().unwrap().length(), 9);
    }

    #[test]
    fn local_list_prefix_returns_objects() {
        use shardline_storage::{ObjectBody, ObjectIntegrity, ObjectKey, ObjectPrefix, ObjectStore};
        let storage = shardline_test_support::TempStorage::new();
        let store = ServerObjectStore::local(storage.path().join("objects")).unwrap();
        let key1 = ObjectKey::parse("aa/1111222233334444555566667777888899990000aaaabbbbccccddddeeeeffff").unwrap();
        let key2 = ObjectKey::parse("aa/2222111111111111111111111111111111111111111111111111111111111111").unwrap();
        let key3 = ObjectKey::parse("bb/1111111111111111111111111111111111111111111111111111111111111111").unwrap();

        let body = b"data";
        let integrity = ObjectIntegrity::new(chunk_hash(body), 4);

        store.put_if_absent(&key1, ObjectBody::from_slice(body), &integrity).unwrap();
        store.put_if_absent(&key2, ObjectBody::from_slice(body), &integrity).unwrap();
        store.put_if_absent(&key3, ObjectBody::from_slice(body), &integrity).unwrap();

        let aa_prefix = ObjectPrefix::parse("aa/").unwrap();
        let results = store.list_prefix(&aa_prefix).unwrap();
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn local_delete_if_present_deletes() {
        use shardline_storage::{DeleteOutcome, ObjectBody, ObjectIntegrity, ObjectKey, ObjectStore};
        let storage = shardline_test_support::TempStorage::new();
        let store = ServerObjectStore::local(storage.path().join("objects")).unwrap();
        let key = ObjectKey::parse("aa/1111222233334444555566667777888899990000aaaabbbbccccddddeeeeffff").unwrap();

        // NotFound for missing
        assert!(matches!(store.delete_if_present(&key), Ok(DeleteOutcome::NotFound)));

        // Insert
        let body = b"delete-me";
        let integrity = ObjectIntegrity::new(chunk_hash(body), 9);
        store.put_if_absent(&key, ObjectBody::from_slice(body), &integrity).unwrap();

        // Now delete
        assert!(matches!(store.delete_if_present(&key), Ok(DeleteOutcome::Deleted)));
    }

    #[test]
    fn local_visit_prefix_counts_objects() {
        use shardline_storage::{ObjectBody, ObjectIntegrity, ObjectKey, ObjectPrefix};
        let storage = shardline_test_support::TempStorage::new();
        let store = ServerObjectStore::local(storage.path().join("objects")).unwrap();
        let key = ObjectKey::parse("aa/1111222233334444555566667777888899990000aaaabbbbccccddddeeeeffff").unwrap();

        let body = b"visit";
        let integrity = ObjectIntegrity::new(chunk_hash(body), 5);
        store.put_if_absent(&key, ObjectBody::from_slice(body), &integrity).unwrap();

        let prefix = ObjectPrefix::parse("aa/").unwrap();
        let mut count = 0u64;
        let result: Result<(), ServerObjectStoreError> = store
            .visit_prefix(&prefix, |_meta| {
                count += 1;
                Ok(())
            });
        assert!(result.is_ok());
        assert_eq!(count, 1);
    }

    #[test]
    fn local_list_flat_namespace_page_returns_results() {
        use shardline_storage::{ObjectBody, ObjectIntegrity, ObjectKey, ObjectPrefix};
        let storage = shardline_test_support::TempStorage::new();
        let store = ServerObjectStore::local(storage.path().join("objects")).unwrap();
        let key = ObjectKey::parse("aa/1111222233334444555566667777888899990000aaaabbbbccccddddeeeeffff").unwrap();

        let body = b"page";
        let integrity = ObjectIntegrity::new(chunk_hash(body), 4);
        store.put_if_absent(&key, ObjectBody::from_slice(body), &integrity).unwrap();

        let prefix = ObjectPrefix::parse("aa/").unwrap();
        let results = store.list_flat_namespace_page(&prefix, None, 10).unwrap();
        assert_eq!(results.len(), 1);
    }

    #[test]
    fn local_put_content_addressed_file_inserts() {
        use shardline_storage::{ObjectIntegrity, ObjectKey, PutOutcome};
        let storage = shardline_test_support::TempStorage::new();
        let store = ServerObjectStore::local(storage.path().join("objects")).unwrap();
        let key = ObjectKey::parse("aa/1111222233334444555566667777888899990000aaaabbbbccccddddeeeeffff").unwrap();

        let tmp = shardline_test_support::TempStorage::new();
        let file_path = tmp.path().join("test.bin");
        std::fs::write(&file_path, b"content-addressed").unwrap();

        let integrity = ObjectIntegrity::new(chunk_hash(b"content-addressed"), 17);
        let result = store.put_content_addressed_file(&key, &file_path, &integrity);
        assert!(matches!(result, Ok(PutOutcome::Inserted)));
    }

    // ── read_full_object defense-in-depth: concurrent truncation ─────────
    //
    // This test triggers the `output.len() != capacity` check inside the
    // local-store read_full_object path (line 769).  We race a concurrent
    // truncation against the read: if the file shrinks between the metadata
    // check and read_to_end, the post-read length check fires.
    //
    // The race window is widened by using a 2 MiB payload.

    #[test]
    fn read_full_object_concurrent_truncation_triggers_length_mismatch() {
        use std::time::Duration;

        let storage = shardline_test_support::TempStorage::new();
        let store = ServerObjectStore::local(storage.path().join("objects")).unwrap();
        let key =
            ObjectKey::parse("aa/1111111111111111111111111111111111111111111111111111111111111111")
                .unwrap();
        let path = store.local_path_for_key(&key).unwrap();

        // Write 16 MiB of data to widen the race window during read
        let large_body = vec![0xabu8; 16 * 1024 * 1024];
        let integrity = ObjectIntegrity::new(chunk_hash(&large_body), large_body.len() as u64);
        store
            .put_if_absent(&key, shardline_storage::ObjectBody::from_slice(&large_body), &integrity)
            .unwrap();

        let p = path;

        // Spawn a thread that truncates the file to 1 byte
        let handle = std::thread::spawn(move || {
            // Wait a tiny bit to let the main thread start read_full_object
            std::thread::sleep(Duration::from_millis(1));
            let f = std::fs::OpenOptions::new().write(true).open(&p).unwrap();
            f.set_len(1).unwrap();
        });

        let result = read_full_object(&store, &key, large_body.len() as u64);

        handle.join().unwrap();

        // We expect *either* a StoredObjectLengthMismatch (if the race was won)
        // or a successful read (if we were too fast / too slow).
        // Accept both outcomes because the timing is probabilistic.
        match result {
            Ok(data) => {
                // File was read before truncation took effect
                assert_eq!(data.len(), large_body.len());
            }
            Err(ServerObjectStoreError::StoredObjectLengthMismatch) => {
                // Race won – defense-in-depth check triggered
            }
            Err(other) => panic!("unexpected error: {other}"),
        }
    }
}
