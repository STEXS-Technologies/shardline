#![deny(unsafe_code)]
#![cfg_attr(
    test,
    allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::indexing_slicing,
        clippy::arithmetic_side_effects,
        clippy::shadow_unrelated,
        clippy::let_underscore_must_use,
        clippy::format_push_string
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

use shardline_index::{LocalRecordStore, PostgresRecordStore, RecordStore};
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
            | TokenCodecError::SigningKeyTooShort
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
    validate_content_hash_with(hash_hex, || ServerObjectStoreError::Overflow)?;
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

    /// Stores an object only if no object exists at the given key.
    ///
    /// # Errors
    ///
    /// Returns [`ServerObjectStoreError`] on storage backend failures.
    pub fn put_if_absent(
        &self,
        key: &ObjectKey,
        body: ObjectBody<'_>,
        integrity: &ObjectIntegrity,
    ) -> Result<PutOutcome, ServerObjectStoreError> {
        match self {
            Self::Local(store) => store
                .put_if_absent(key, body, integrity)
                .map_err(Into::into),
            Self::S3(store) => store
                .put_if_absent(key, body, integrity)
                .map_err(Into::into),
            Self::Blackhole => Ok(PutOutcome::Inserted),
        }
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

    /// Reads a byte range from the stored object.
    ///
    /// # Errors
    ///
    /// Returns [`ServerObjectStoreError::NotFound`] for blackhole stores or
    /// [`ServerObjectStoreError::Local`]/[`ServerObjectStoreError::S3`] on backend failures.
    pub fn read_range(
        &self,
        key: &ObjectKey,
        range: ByteRange,
    ) -> Result<Vec<u8>, ServerObjectStoreError> {
        match self {
            Self::Local(store) => store.read_range(key, range).map_err(Into::into),
            Self::S3(store) => store.read_range(key, range).map_err(Into::into),
            Self::Blackhole => Err(ServerObjectStoreError::NotFound),
        }
    }

    /// Returns metadata for the stored object, or `None` if it does not exist.
    ///
    /// # Errors
    ///
    /// Returns [`ServerObjectStoreError`] on storage backend failures.
    pub fn metadata(
        &self,
        key: &ObjectKey,
    ) -> Result<Option<ObjectMetadata>, ServerObjectStoreError> {
        match self {
            Self::Local(store) => store.metadata(key).map_err(Into::into),
            Self::S3(store) => store.metadata(key).map_err(Into::into),
            Self::Blackhole => Ok(None),
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

    /// Deletes an object if it exists.
    ///
    /// # Errors
    ///
    /// Returns [`ServerObjectStoreError`] on storage backend failures.
    pub fn delete_if_present(
        &self,
        key: &ObjectKey,
    ) -> Result<DeleteOutcome, ServerObjectStoreError> {
        match self {
            Self::Local(store) => store.delete_if_present(key).map_err(Into::into),
            Self::S3(store) => store.delete_if_present(key).map_err(Into::into),
            Self::Blackhole => Ok(DeleteOutcome::NotFound),
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
    fn locator_display(&self, locator: &Self::Locator) -> String;

    /// Extracts the file identifier implied by a locator.
    fn locator_file_id(&self, locator: &Self::Locator, kind: OpsRecordKind) -> Option<String>;

    /// Extracts the immutable content hash implied by a version locator.
    fn locator_content_hash(&self, locator: &Self::Locator, kind: OpsRecordKind) -> Option<String>;
}

impl OpsRecordStore for LocalRecordStore {
    fn locator_display(&self, locator: &Self::Locator) -> String {
        locator.record_key().to_owned()
    }

    fn locator_file_id(&self, locator: &Self::Locator, _kind: OpsRecordKind) -> Option<String> {
        Some(locator.file_id().to_owned())
    }

    fn locator_content_hash(&self, locator: &Self::Locator, kind: OpsRecordKind) -> Option<String> {
        if kind != OpsRecordKind::Version {
            return None;
        }

        locator.content_hash().map(ToOwned::to_owned)
    }
}

impl OpsRecordStore for PostgresRecordStore {
    fn locator_display(&self, locator: &Self::Locator) -> String {
        locator.record_key().to_owned()
    }

    fn locator_file_id(&self, locator: &Self::Locator, _kind: OpsRecordKind) -> Option<String> {
        Some(locator.file_id().to_owned())
    }

    fn locator_content_hash(&self, locator: &Self::Locator, kind: OpsRecordKind) -> Option<String> {
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
