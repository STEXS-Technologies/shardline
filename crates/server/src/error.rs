use std::{io::Error as IoError, num::TryFromIntError};

use axum::{
    Error as AxumError, Json,
    http::{HeaderValue, StatusCode, header::WWW_AUTHENTICATE},
    response::{IntoResponse, Response},
};
use serde::Serialize;
use serde_json::Error as JsonError;
use shardline_cache::ReconstructionCacheError;
use shardline_index::{
    FileRecordInvariantError, LocalIndexStoreError, MemoryIndexStoreError, MemoryRecordStoreError,
    PostgresMetadataStoreError, QuarantineCandidateError, RetentionHoldError, WebhookDeliveryError,
};
use shardline_protocol::{HashParseError, HttpRangeParseError, TokenCodecError};
pub use shardline_server_core::{
    InvalidLifecycleMetadataError, InvalidReconstructionResponseError, InvalidSerializedShardError,
    ParseStoredFileRecordError,
};
use shardline_storage::{LocalObjectStoreError, ObjectPrefixError, S3ObjectStoreError};
use thiserror::Error;
use tokio::task::JoinError;

use crate::{
    config::ServerConfigError, provider::ProviderServiceError, xet_adapter::XorbParseError,
};
use shardline_gc::GcError;
use shardline_oci_adapter::OciAdapterError;
use shardline_provider_events::ProviderEventsError;
use shardline_xet_adapter::XetAdapterError;

/// Object-storage subsystem failure.
#[derive(Debug, Error)]
pub enum ObjectStoreError {
    /// Local storage IO failed.
    #[error("local storage operation failed")]
    Local(#[from] LocalObjectStoreError),
    /// S3-compatible object-storage adapter access failed.
    #[error("s3 object storage adapter operation failed")]
    S3(#[from] S3ObjectStoreError),
    /// Object inventory prefix validation failed.
    #[error("object storage prefix validation failed")]
    Prefix(#[from] ObjectPrefixError),
    /// S3-compatible object storage was selected without concrete configuration.
    #[error("s3 object storage configuration is missing")]
    MissingS3Config,
    /// Stored object metadata disagreed with the expected transfer length.
    #[error("stored object length did not match indexed metadata")]
    StoredLengthMismatch,
    /// Storage migration found a content-addressed source object under the wrong key.
    #[error(
        "storage migration source object hash mismatch for key {key}: expected {expected_hash}, observed {observed_hash}"
    )]
    MigrationSourceHashMismatch {
        /// Content-addressed object key being migrated.
        key: String,
        /// Hash implied by the object key.
        expected_hash: String,
        /// Hash computed from the source object bytes.
        observed_hash: String,
    },
}

/// Metadata index subsystem failure.
#[derive(Debug, Error)]
pub enum IndexError {
    /// Index adapter access failed.
    #[error("index adapter operation failed")]
    Local(#[from] LocalIndexStoreError),
    /// In-memory index adapter access failed.
    #[error("memory index adapter operation failed")]
    MemoryIndex(#[from] MemoryIndexStoreError),
    /// In-memory record adapter access failed.
    #[error("memory record adapter operation failed")]
    MemoryRecord(#[from] MemoryRecordStoreError),
    /// Postgres metadata adapter access failed.
    #[error("postgres metadata adapter operation failed")]
    PostgresMetadata(#[from] PostgresMetadataStoreError),
    /// Retention hold input was invalid.
    #[error("retention hold input was invalid")]
    RetentionHold(#[from] RetentionHoldError),
    /// Quarantine candidate input was invalid.
    #[error("quarantine candidate input was invalid")]
    QuarantineCandidate(#[from] QuarantineCandidateError),
    /// Webhook delivery metadata was invalid.
    #[error("webhook delivery metadata was invalid")]
    WebhookDelivery(#[from] WebhookDeliveryError),
    /// Stored file metadata could not produce a valid reconstruction plan.
    #[error("stored file metadata was invalid")]
    FileRecordInvariant(#[from] FileRecordInvariantError),
    /// Lifecycle metadata was internally inconsistent.
    #[error("lifecycle metadata was internally inconsistent")]
    InvalidLifecycleMetadata(#[from] InvalidLifecycleMetadataError),
    /// A reconstruction response violated an internal protocol-shape invariant.
    #[error("reconstruction response invariant failed: {0}")]
    InvalidReconstructionResponse(#[from] InvalidReconstructionResponseError),
    /// A required metadata table was missing.
    #[error("required metadata table is missing: {0}")]
    MissingRequiredMetadataTable(String),
    /// Repository rename encountered conflicting target-scope metadata.
    #[error("repository rename target already contains conflicting metadata")]
    ConflictingRenameTargetRecord,
}

/// Server runtime failure.
///
/// This is the unified error type for the Shardline server layer. It maps
/// domain-specific errors from storage, indexing, protocol, auth, and OCI
/// subsystems into a single enum with HTTP status code mapping.
///
/// # Design
///
/// Each variant maps to exactly one HTTP status code via [`ServerError::status_code`].
/// Subsystem errors (e.g., [`LocalIndexStoreError`], [`S3ObjectStoreError`]) are
/// converted via `From` implementations for ergonomic `?` usage.
///
/// When adding new error sources, prefer adding a new variant with a `#[from]`
/// attribute over manual `From` implementations unless the source error maps
/// to multiple variants.
#[derive(Debug, Error)]
pub enum ServerError {
    /// Local storage IO failed.
    #[error("local storage operation failed")]
    Io(#[from] IoError),
    /// JSON serialization or deserialization failed.
    #[error("json operation failed")]
    Json(#[from] JsonError),
    /// Request body streaming failed.
    #[error("request body stream failed")]
    RequestBodyRead(#[source] AxumError),
    /// Request body exceeded the configured maximum accepted byte count.
    #[error("request body exceeded the configured maximum accepted byte count")]
    RequestBodyTooLarge,
    /// Request query exceeded the bounded metadata parser budget.
    #[error("request query exceeded the bounded metadata parser budget")]
    RequestQueryTooLarge,
    /// Request body frame slicing exceeded checked bounds.
    #[error("request body frame exceeded checked bounds")]
    RequestBodyFrameOutOfBounds,
    /// Numeric conversion exceeded supported bounds.
    #[error("numeric conversion exceeded supported bounds")]
    NumericConversion(#[from] TryFromIntError),
    /// Hash parsing failed.
    #[error("invalid content hash")]
    HashParse(#[from] HashParseError),
    /// Object-storage subsystem failure.
    #[error("object storage operation failed")]
    ObjectStore(#[from] ObjectStoreError),
    /// Metadata index subsystem failure.
    #[error("index adapter operation failed")]
    Index(#[from] IndexError),
    /// Stored file metadata exceeded the bounded parser ceiling.
    #[error("stored file metadata exceeded the bounded parser ceiling")]
    StoredFileMetadataTooLarge {
        /// Observed file length in bytes.
        observed_bytes: u64,
        /// Maximum accepted file length in bytes.
        maximum_bytes: u64,
    },
    /// Stored file metadata changed after bounded validation.
    #[error("stored file metadata length did not match the validated length")]
    StoredFileMetadataLengthMismatch,
    /// A file identifier was unsafe.
    #[error(
        "file identifier must be relative and must not contain traversal or control characters"
    )]
    InvalidFileId,
    /// A content hash was malformed.
    #[error("content hash must be 64 hexadecimal characters")]
    InvalidContentHash,
    /// The xorb transfer path prefix was unsupported.
    #[error("xorb transfer prefix must be default")]
    InvalidXorbPrefix,
    /// The uploaded xorb bytes did not match the requested hash.
    #[error("xorb body hash did not match the requested path hash")]
    XorbHashMismatch,
    /// The uploaded xorb bytes were not a valid serialized xorb object.
    #[error("xorb body was not a valid serialized xorb object")]
    InvalidSerializedXorb,
    /// The uploaded shard bytes were not a valid serialized shard object.
    #[error("shard body was not a valid serialized shard object")]
    InvalidSerializedShard(#[from] InvalidSerializedShardError),
    /// A shard upload referenced a missing xorb.
    #[error("shard referenced a missing xorb")]
    MissingReferencedXorb,
    /// Shard metadata exceeded bounded parser safety limits.
    #[error("shard metadata exceeded bounded parser safety limits")]
    TooManyShardTerms,
    /// Batch reconstruction requested too many file identifiers.
    #[error("batch reconstruction requested too many file identifiers")]
    TooManyBatchReconstructionFileIds,
    /// Requested content was not found.
    #[error("content not found")]
    NotFound,
    /// Arithmetic overflowed a checked bound.
    #[error("arithmetic overflow")]
    Overflow,
    /// The reconstruction range header was malformed.
    #[error("range header must use bytes=<start>-<end> syntax")]
    InvalidRangeHeader,
    /// The reconstruction range start exceeded the end of the file.
    #[error("requested range is not satisfiable")]
    RangeNotSatisfiable,
    /// The request did not include an authorization header.
    #[error("authorization header is missing")]
    MissingAuthorization,
    /// The authorization header was malformed.
    #[error("authorization header must use bearer format")]
    InvalidAuthorizationHeader,
    /// The bearer token was invalid.
    #[error("bearer token was invalid")]
    InvalidToken(TokenCodecError),
    /// The token signing key is misconfigured.
    #[error("token signing key is misconfigured: {0}")]
    SigningKeyError(String),
    /// The bearer token did not grant the required scope.
    #[error("bearer token does not grant the required scope")]
    InsufficientScope,
    /// The provider issuance endpoint is not configured.
    #[error("provider token issuance endpoint is not configured")]
    ProviderTokensDisabled,
    /// The provider bootstrap key was missing.
    #[error("provider bootstrap key is missing")]
    MissingProviderApiKey,
    /// The provider bootstrap key was invalid.
    #[error("provider bootstrap key is invalid")]
    InvalidProviderApiKey,
    /// The provider subject was missing from a bootstrap request.
    #[error("provider subject is missing")]
    MissingProviderSubject,
    /// The provider token issuance request contained invalid bounded metadata.
    #[error("provider token request was invalid")]
    InvalidProviderTokenRequest,
    /// The provider webhook authentication header was missing.
    #[error("provider webhook authentication is missing")]
    MissingProviderWebhookAuthentication,
    /// The provider webhook authentication header was invalid.
    #[error("provider webhook authentication is invalid")]
    InvalidProviderWebhookAuthentication,
    /// The provider webhook payload was invalid.
    #[error("provider webhook payload was invalid")]
    InvalidProviderWebhookPayload,
    /// The requested provider is not configured.
    #[error("provider is not configured")]
    UnknownProvider,
    /// Provider authorization denied the request.
    #[error("provider denied requested repository access")]
    ProviderDenied,
    /// Provider token issuance failed due to configuration or adapter errors.
    #[error("provider token issuance failed")]
    Provider(#[from] ProviderServiceError),
    /// Reconstruction cache adapter access failed.
    #[error("reconstruction cache adapter operation failed")]
    ReconstructionCache(#[from] ReconstructionCacheError),
    /// Server configuration was invalid for the selected runtime surface.
    #[error("server configuration was invalid")]
    Config(#[from] ServerConfigError),
    /// The uploaded body did not match the expected SHA-256 identifier.
    #[error("uploaded body hash did not match the expected sha256")]
    ExpectedBodyHashMismatch,
    /// A digest string was malformed.
    #[error("digest must use sha256:<64 lowercase hex> format")]
    InvalidDigest,
    /// A repository name or namespace path was malformed.
    #[error("repository name was invalid")]
    InvalidRepositoryName,
    /// A manifest reference or tag was malformed.
    #[error("manifest reference was invalid")]
    InvalidManifestReference,
    /// The requested representation does not match any accepted media type.
    #[error("requested representation was not acceptable")]
    NotAcceptable,
    /// The request requires a protocol-specific authentication challenge.
    #[error("authorization challenge required")]
    UnauthorizedChallenge(String),
    /// An upload session identifier was malformed.
    #[error("upload session identifier was invalid")]
    InvalidUploadSession,
    /// Too many OCI upload sessions are currently active.
    #[error("too many active oci upload sessions")]
    TooManyUploadSessions,
    /// Too many OCI registry token exchanges are currently active.
    #[error("too many active oci registry token requests")]
    TooManyRegistryTokenRequests,
    /// Redis reconstruction cache was selected without a URL.
    #[error("redis reconstruction cache requires a redis url")]
    MissingReconstructionCacheRedisUrl,
    /// The transfer concurrency limiter was unexpectedly unavailable.
    #[error("transfer concurrency limiter is unavailable")]
    TransferLimiterClosed,
    /// The transfer concurrency limiter timed out waiting for capacity.
    #[error("transfer concurrency limiter timed out")]
    TransferLimiterTimedOut,
    /// A blocking worker task failed before it could finish storage work.
    #[error("blocking worker task failed")]
    BlockingTask(#[source] JoinError),
}

impl ServerError {
    /// Maps this error to the OCI Distribution specification error code.
    ///
    /// When adding a new [`ServerError`] variant, add it explicitly to this match.
    /// If the variant has an OCI-specific code, map it above the catch-all.
    /// If it is an internal error (no OCI-specific meaning), keep it in the catch-all.
    const fn oci_error_code(&self) -> &'static str {
        match self {
            Self::NotFound => "MANIFEST_UNKNOWN",
            Self::InvalidDigest => "DIGEST_INVALID",
            Self::InvalidRepositoryName => "NAME_INVALID",
            Self::InvalidManifestReference | Self::InvalidUploadSession => "MANIFEST_INVALID",
            Self::RequestBodyTooLarge => "SIZE_INVALID",
            Self::MissingAuthorization
            | Self::InvalidAuthorizationHeader
            | Self::InvalidToken(_) => "UNAUTHORIZED",
            Self::InsufficientScope => "DENIED",
            Self::NotAcceptable => "UNSUPPORTED",
            Self::ExpectedBodyHashMismatch => "DIGEST_INVALID",
            Self::TooManyUploadSessions | Self::TooManyRegistryTokenRequests => "TOO_MANY_REQUESTS",
            // All remaining variants are internal server errors with no
            // OCI-specific code. Add new OCI-mappable variants above.
            Self::Io(_)
            | Self::Json(_)
            | Self::RequestBodyRead(_)
            | Self::RequestQueryTooLarge
            | Self::RequestBodyFrameOutOfBounds
            | Self::NumericConversion(_)
            | Self::HashParse(_)
            | Self::ObjectStore(_)
            | Self::Index(_)
            | Self::StoredFileMetadataTooLarge { .. }
            | Self::StoredFileMetadataLengthMismatch
            | Self::InvalidFileId
            | Self::InvalidContentHash
            | Self::InvalidXorbPrefix
            | Self::XorbHashMismatch
            | Self::InvalidSerializedXorb
            | Self::InvalidSerializedShard(_)
            | Self::MissingReferencedXorb
            | Self::TooManyShardTerms
            | Self::TooManyBatchReconstructionFileIds
            | Self::Overflow
            | Self::InvalidRangeHeader
            | Self::RangeNotSatisfiable
            | Self::SigningKeyError(_)
            | Self::ProviderTokensDisabled
            | Self::MissingProviderApiKey
            | Self::InvalidProviderApiKey
            | Self::MissingProviderSubject
            | Self::InvalidProviderTokenRequest
            | Self::MissingProviderWebhookAuthentication
            | Self::InvalidProviderWebhookAuthentication
            | Self::InvalidProviderWebhookPayload
            | Self::UnknownProvider
            | Self::ProviderDenied
            | Self::Provider(_)
            | Self::ReconstructionCache(_)
            | Self::Config(_)
            | Self::UnauthorizedChallenge(_)
            | Self::MissingReconstructionCacheRedisUrl
            | Self::TransferLimiterClosed
            | Self::TransferLimiterTimedOut
            | Self::BlockingTask(_) => "INTERNAL",
        }
    }

    const fn status_code(&self) -> StatusCode {
        match self {
            Self::InvalidFileId
            | Self::InvalidContentHash
            | Self::InvalidDigest
            | Self::InvalidRepositoryName
            | Self::InvalidManifestReference
            | Self::InvalidUploadSession
            | Self::InvalidXorbPrefix
            | Self::HashParse(_) => StatusCode::BAD_REQUEST,
            Self::NotAcceptable => StatusCode::NOT_ACCEPTABLE,
            Self::UnauthorizedChallenge(_) => StatusCode::UNAUTHORIZED,
            Self::InvalidRangeHeader => StatusCode::BAD_REQUEST,
            Self::RangeNotSatisfiable => StatusCode::RANGE_NOT_SATISFIABLE,
            Self::XorbHashMismatch
            | Self::InvalidSerializedXorb
            | Self::InvalidSerializedShard(_)
            | Self::MissingReferencedXorb => StatusCode::BAD_REQUEST,
            Self::TooManyShardTerms
            | Self::TooManyBatchReconstructionFileIds
            | Self::RequestBodyTooLarge => StatusCode::PAYLOAD_TOO_LARGE,
            Self::RequestQueryTooLarge => StatusCode::URI_TOO_LONG,
            Self::RequestBodyRead(_) | Self::RequestBodyFrameOutOfBounds => StatusCode::BAD_REQUEST,
            Self::ExpectedBodyHashMismatch => StatusCode::BAD_REQUEST,
            Self::NotFound | Self::UnknownProvider | Self::ProviderTokensDisabled => {
                StatusCode::NOT_FOUND
            }
            Self::MissingAuthorization
            | Self::InvalidAuthorizationHeader
            | Self::InvalidToken(_)
            | Self::MissingProviderApiKey
            | Self::MissingProviderSubject
            | Self::MissingProviderWebhookAuthentication => StatusCode::UNAUTHORIZED,
            Self::InsufficientScope
            | Self::InvalidProviderApiKey
            | Self::InvalidProviderWebhookAuthentication
            | Self::ProviderDenied => StatusCode::FORBIDDEN,
            Self::InvalidProviderTokenRequest | Self::InvalidProviderWebhookPayload => {
                StatusCode::BAD_REQUEST
            }
            Self::TooManyUploadSessions | Self::TooManyRegistryTokenRequests => {
                StatusCode::TOO_MANY_REQUESTS
            }
            Self::TransferLimiterClosed | Self::TransferLimiterTimedOut => {
                StatusCode::SERVICE_UNAVAILABLE
            }
            Self::Io(_)
            | Self::Json(_)
            | Self::NumericConversion(_)
            | Self::ObjectStore(_)
            | Self::Index(_)
            | Self::StoredFileMetadataTooLarge { .. }
            | Self::StoredFileMetadataLengthMismatch
            | Self::Config(_)
            | Self::Overflow
            | Self::MissingReconstructionCacheRedisUrl
            | Self::ReconstructionCache(_)
            | Self::BlockingTask(_)
            | Self::SigningKeyError(_)
            | Self::Provider(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}

impl IntoResponse for ServerError {
    fn into_response(self) -> Response {
        let status = self.status_code();
        let should_attach_default_challenge = matches!(
            self,
            Self::MissingAuthorization | Self::InvalidAuthorizationHeader | Self::InvalidToken(_)
        );
        let custom_challenge = if let Self::UnauthorizedChallenge(custom_header) = &self {
            Some(custom_header.as_str())
        } else {
            None
        };
        let body = ErrorBody {
            error: self.to_string(),
        };
        let mut response = (status, Json(body)).into_response();
        if let Some(custom_header) = custom_challenge {
            if let Ok(header_value) = HeaderValue::from_str(custom_header) {
                response
                    .headers_mut()
                    .insert(WWW_AUTHENTICATE, header_value);
            }
        } else if should_attach_default_challenge {
            response.headers_mut().insert(
                WWW_AUTHENTICATE,
                HeaderValue::from_static("Bearer realm=\"shardline\""),
            );
        }
        response
    }
}

impl From<HttpRangeParseError> for ServerError {
    fn from(value: HttpRangeParseError) -> Self {
        match value {
            HttpRangeParseError::Unsatisfiable => Self::RangeNotSatisfiable,
            HttpRangeParseError::MissingBytesUnit
            | HttpRangeParseError::InvalidSyntax(_)
            | HttpRangeParseError::InvalidNumber(_) => Self::InvalidRangeHeader,
        }
    }
}

impl From<XorbParseError> for ServerError {
    fn from(value: XorbParseError) -> Self {
        match value {
            XorbParseError::HashMismatch => Self::XorbHashMismatch,
            XorbParseError::InvalidFormat(_)
            | XorbParseError::NumericConversion(_)
            | XorbParseError::Io(_) => Self::InvalidSerializedXorb,
        }
    }
}

impl From<XetAdapterError> for ServerError {
    fn from(value: XetAdapterError) -> Self {
        match value {
            XetAdapterError::Io(e) => Self::Io(e),
            XetAdapterError::NumericConversion(e) => Self::NumericConversion(e),
            XetAdapterError::HashParse(e) => Self::HashParse(e),
            XetAdapterError::ObjectStore(e) => Self::from(e),
            XetAdapterError::LocalObjectStore(e) => Self::ObjectStore(ObjectStoreError::Local(e)),
            XetAdapterError::S3ObjectStore(e) => Self::ObjectStore(ObjectStoreError::S3(e)),
            XetAdapterError::IndexStore(e) => Self::Index(IndexError::Local(e)),
            XetAdapterError::MemoryIndexStore(e) => Self::Index(IndexError::MemoryIndex(e)),
            XetAdapterError::MemoryRecordStore(e) => Self::Index(IndexError::MemoryRecord(e)),
            XetAdapterError::PostgresMetadata(e) => Self::Index(IndexError::PostgresMetadata(e)),
            XetAdapterError::FileRecordInvariant(e) => {
                Self::Index(IndexError::FileRecordInvariant(e))
            }
            XetAdapterError::InvalidContentHash => Self::InvalidContentHash,
            XetAdapterError::InvalidXorbPrefix => Self::InvalidXorbPrefix,
            XetAdapterError::XorbHashMismatch => Self::XorbHashMismatch,
            XetAdapterError::InvalidSerializedXorb => Self::InvalidSerializedXorb,
            XetAdapterError::InvalidSerializedShard(e) => Self::InvalidSerializedShard(e),
            XetAdapterError::MissingReferencedXorb => Self::MissingReferencedXorb,
            XetAdapterError::TooManyShardTerms => Self::TooManyShardTerms,
            XetAdapterError::NotFound => Self::NotFound,
            XetAdapterError::Overflow => Self::Overflow,
            XetAdapterError::RangeNotSatisfiable => Self::RangeNotSatisfiable,
        }
    }
}

impl From<ProviderEventsError> for ServerError {
    fn from(value: ProviderEventsError) -> Self {
        match value {
            ProviderEventsError::Overflow => Self::Overflow,
            ProviderEventsError::InvalidContentHash => Self::InvalidContentHash,
            ProviderEventsError::InvalidProviderWebhookPayload => {
                Self::InvalidProviderWebhookPayload
            }
            ProviderEventsError::ConflictingRenameTargetRecord => {
                Self::Index(IndexError::ConflictingRenameTargetRecord)
            }
            ProviderEventsError::Json(e) => Self::Json(e),
            ProviderEventsError::NumericConversion(e) => Self::NumericConversion(e),
            ProviderEventsError::RetentionHold(e) => Self::Index(IndexError::RetentionHold(e)),
            ProviderEventsError::XetAdapter(e) => Self::from(e),
            ProviderEventsError::IndexStore(e) => Self::Index(IndexError::Local(e)),
            ProviderEventsError::MemoryIndexStore(e) => Self::Index(IndexError::MemoryIndex(e)),
            ProviderEventsError::MemoryRecordStore(e) => Self::Index(IndexError::MemoryRecord(e)),
            ProviderEventsError::PostgresMetadata(e) => {
                Self::Index(IndexError::PostgresMetadata(e))
            }
            ProviderEventsError::WebhookDelivery(e) => Self::Index(IndexError::WebhookDelivery(e)),
            ProviderEventsError::ObjectStore(e) => Self::from(e),
            ProviderEventsError::ParseStoredFileRecord(e) => Self::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                e.to_string(),
            )),
        }
    }
}

impl From<GcError> for ServerError {
    fn from(err: GcError) -> Self {
        match err {
            GcError::Io(e) => Self::Io(e),
            GcError::Json(e) => Self::Json(e),
            GcError::NumericConversion(e) => Self::NumericConversion(e),
            GcError::ObjectStore(e) => Self::from(e),
            GcError::LocalObjectStore(e) => Self::ObjectStore(ObjectStoreError::Local(e)),
            GcError::S3ObjectStore(e) => Self::ObjectStore(ObjectStoreError::S3(e)),
            GcError::ObjectPrefix(e) => Self::ObjectStore(ObjectStoreError::Prefix(e)),
            GcError::IndexStore(e) => Self::Index(IndexError::Local(e)),
            GcError::MemoryIndexStore(e) => Self::Index(IndexError::MemoryIndex(e)),
            GcError::MemoryRecordStore(e) => Self::Index(IndexError::MemoryRecord(e)),
            GcError::PostgresMetadata(e) => Self::Index(IndexError::PostgresMetadata(e)),
            GcError::RetentionHold(e) => Self::Index(IndexError::RetentionHold(e)),
            GcError::QuarantineCandidate(e) => Self::Index(IndexError::QuarantineCandidate(e)),
            GcError::WebhookDelivery(e) => Self::Index(IndexError::WebhookDelivery(e)),
            GcError::FileRecordInvariant(e) => Self::Index(IndexError::FileRecordInvariant(e)),
            GcError::InvalidLifecycleMetadata(e) => {
                Self::Index(IndexError::InvalidLifecycleMetadata(e))
            }
            GcError::InvalidContentHash => Self::InvalidContentHash,
            GcError::Overflow => Self::Overflow,
            GcError::XetAdapter(e) => Self::from(e),
        }
    }
}

impl From<OciAdapterError> for ServerError {
    fn from(value: OciAdapterError) -> Self {
        match value {
            OciAdapterError::Io(e) => Self::Io(e),
            OciAdapterError::Json(e) => Self::Json(e),
            OciAdapterError::NumericConversion(e) => Self::NumericConversion(e),
            OciAdapterError::ObjectStore(e) => Self::from(e),
            OciAdapterError::S3ObjectStore(e) => Self::ObjectStore(ObjectStoreError::S3(e)),
            OciAdapterError::LocalObjectStore(e) => Self::ObjectStore(ObjectStoreError::Local(e)),
            OciAdapterError::ObjectPrefix(e) => Self::ObjectStore(ObjectStoreError::Prefix(e)),
            OciAdapterError::NotFound => Self::NotFound,
            OciAdapterError::Overflow => Self::Overflow,
            OciAdapterError::InvalidContentHash => Self::InvalidContentHash,
            OciAdapterError::InvalidDigest => Self::InvalidDigest,
            OciAdapterError::InvalidRepositoryName => Self::InvalidRepositoryName,
            OciAdapterError::InvalidManifestReference => Self::InvalidManifestReference,
            OciAdapterError::InvalidUploadSession => Self::InvalidUploadSession,
            OciAdapterError::TooManyUploadSessions => Self::TooManyUploadSessions,
            OciAdapterError::ExpectedBodyHashMismatch => Self::ExpectedBodyHashMismatch,
            OciAdapterError::BlockingTask(e) => Self::BlockingTask(e),
        }
    }
}

impl From<shardline_protocol_adapters::ProtocolError> for ServerError {
    fn from(error: shardline_protocol_adapters::ProtocolError) -> Self {
        match error {
            shardline_protocol_adapters::ProtocolError::InvalidContentHash => {
                Self::InvalidContentHash
            }
        }
    }
}

impl From<LocalObjectStoreError> for ServerError {
    fn from(e: LocalObjectStoreError) -> Self {
        Self::ObjectStore(ObjectStoreError::Local(e))
    }
}

impl From<S3ObjectStoreError> for ServerError {
    fn from(e: S3ObjectStoreError) -> Self {
        Self::ObjectStore(ObjectStoreError::S3(e))
    }
}

impl From<ObjectPrefixError> for ServerError {
    fn from(e: ObjectPrefixError) -> Self {
        Self::ObjectStore(ObjectStoreError::Prefix(e))
    }
}

impl From<LocalIndexStoreError> for ServerError {
    fn from(e: LocalIndexStoreError) -> Self {
        Self::Index(IndexError::Local(e))
    }
}

impl From<MemoryIndexStoreError> for ServerError {
    fn from(e: MemoryIndexStoreError) -> Self {
        Self::Index(IndexError::MemoryIndex(e))
    }
}

impl From<MemoryRecordStoreError> for ServerError {
    fn from(e: MemoryRecordStoreError) -> Self {
        Self::Index(IndexError::MemoryRecord(e))
    }
}

impl From<PostgresMetadataStoreError> for ServerError {
    fn from(e: PostgresMetadataStoreError) -> Self {
        Self::Index(IndexError::PostgresMetadata(e))
    }
}

impl From<RetentionHoldError> for ServerError {
    fn from(e: RetentionHoldError) -> Self {
        Self::Index(IndexError::RetentionHold(e))
    }
}

impl From<QuarantineCandidateError> for ServerError {
    fn from(e: QuarantineCandidateError) -> Self {
        Self::Index(IndexError::QuarantineCandidate(e))
    }
}

impl From<WebhookDeliveryError> for ServerError {
    fn from(e: WebhookDeliveryError) -> Self {
        Self::Index(IndexError::WebhookDelivery(e))
    }
}

impl From<FileRecordInvariantError> for ServerError {
    fn from(e: FileRecordInvariantError) -> Self {
        Self::Index(IndexError::FileRecordInvariant(e))
    }
}

impl From<InvalidLifecycleMetadataError> for ServerError {
    fn from(e: InvalidLifecycleMetadataError) -> Self {
        Self::Index(IndexError::InvalidLifecycleMetadata(e))
    }
}

impl From<InvalidReconstructionResponseError> for ServerError {
    fn from(e: InvalidReconstructionResponseError) -> Self {
        Self::Index(IndexError::InvalidReconstructionResponse(e))
    }
}

impl From<ParseStoredFileRecordError> for ServerError {
    fn from(e: ParseStoredFileRecordError) -> Self {
        match e {
            ParseStoredFileRecordError::StoredFileMetadataTooLarge {
                observed_bytes,
                maximum_bytes,
            } => Self::StoredFileMetadataTooLarge {
                observed_bytes,
                maximum_bytes,
            },
            ParseStoredFileRecordError::Json(e) => Self::Json(e),
        }
    }
}

#[derive(Debug, Serialize)]
struct ErrorBody {
    error: String,
}

/// OCI Distribution-spec formatted error wrapper.
///
/// Wraps [`ServerError`] and implements [`IntoResponse`] using the OCI error
/// envelope format mandated by the OCI Distribution specification:
///
/// ```json
/// { "errors": [{ "code": "...", "message": "...", "detail": null }] }
/// ```
#[derive(Debug)]
pub(crate) struct OciError(pub ServerError);

impl From<ServerError> for OciError {
    fn from(error: ServerError) -> Self {
        Self(error)
    }
}

impl IntoResponse for OciError {
    fn into_response(self) -> Response {
        let status = self.0.status_code();
        let should_attach_default_challenge = matches!(
            self.0,
            ServerError::MissingAuthorization
                | ServerError::InvalidAuthorizationHeader
                | ServerError::InvalidToken(_)
        );
        let custom_challenge = if let ServerError::UnauthorizedChallenge(custom_header) = &self.0 {
            Some(custom_header.as_str())
        } else {
            None
        };
        let body = Json(serde_json::json!({
            "errors": [{
                "code": self.0.oci_error_code(),
                "message": self.0.to_string(),
                "detail": null
            }]
        }));
        let mut response = (status, body).into_response();
        if let Some(custom_header) = custom_challenge {
            if let Ok(header_value) = HeaderValue::from_str(custom_header) {
                response
                    .headers_mut()
                    .insert(WWW_AUTHENTICATE, header_value);
            }
        } else if should_attach_default_challenge {
            response.headers_mut().insert(
                WWW_AUTHENTICATE,
                HeaderValue::from_static("Bearer realm=\"shardline\""),
            );
        }
        response
    }
}

#[cfg(test)]
mod tests {
    use axum::http::StatusCode;
    use axum::response::IntoResponse;

    use super::{OciError, ServerError};

    fn status_for(error: &ServerError) -> StatusCode {
        error.status_code()
    }

    #[test]
    fn not_found_maps_to_404() {
        assert_eq!(status_for(&ServerError::NotFound), StatusCode::NOT_FOUND);
    }

    #[test]
    fn insufficient_scope_maps_to_403() {
        assert_eq!(
            status_for(&ServerError::InsufficientScope),
            StatusCode::FORBIDDEN
        );
    }

    #[test]
    fn too_many_upload_sessions_maps_to_429() {
        assert_eq!(
            status_for(&ServerError::TooManyUploadSessions),
            StatusCode::TOO_MANY_REQUESTS
        );
    }

    #[test]
    fn too_many_registry_token_requests_maps_to_429() {
        assert_eq!(
            status_for(&ServerError::TooManyRegistryTokenRequests),
            StatusCode::TOO_MANY_REQUESTS
        );
    }

    #[test]
    fn missing_authorization_maps_to_401() {
        assert_eq!(
            status_for(&ServerError::MissingAuthorization),
            StatusCode::UNAUTHORIZED
        );
    }

    #[test]
    fn request_body_too_large_maps_to_413() {
        assert_eq!(
            status_for(&ServerError::RequestBodyTooLarge),
            StatusCode::PAYLOAD_TOO_LARGE
        );
    }

    #[test]
    fn invalid_file_id_maps_to_400() {
        assert_eq!(
            status_for(&ServerError::InvalidFileId),
            StatusCode::BAD_REQUEST
        );
    }

    #[test]
    fn range_not_satisfiable_maps_to_416() {
        assert_eq!(
            status_for(&ServerError::RangeNotSatisfiable),
            StatusCode::RANGE_NOT_SATISFIABLE
        );
    }

    #[test]
    fn not_acceptable_maps_to_406() {
        assert_eq!(
            status_for(&ServerError::NotAcceptable),
            StatusCode::NOT_ACCEPTABLE
        );
    }

    #[test]
    fn io_error_maps_to_500() {
        let io_err = std::io::Error::other("test");
        assert_eq!(
            status_for(&ServerError::Io(io_err)),
            StatusCode::INTERNAL_SERVER_ERROR
        );
    }

    #[test]
    fn transfer_limiter_closed_maps_to_503() {
        assert_eq!(
            status_for(&ServerError::TransferLimiterClosed),
            StatusCode::SERVICE_UNAVAILABLE
        );
    }

    #[test]
    fn transfer_limiter_timed_out_maps_to_503() {
        assert_eq!(
            status_for(&ServerError::TransferLimiterTimedOut),
            StatusCode::SERVICE_UNAVAILABLE
        );
    }

    #[test]
    fn provider_denied_maps_to_403() {
        assert_eq!(
            status_for(&ServerError::ProviderDenied),
            StatusCode::FORBIDDEN
        );
    }

    #[test]
    fn unknown_provider_maps_to_404() {
        assert_eq!(
            status_for(&ServerError::UnknownProvider),
            StatusCode::NOT_FOUND
        );
    }

    #[test]
    fn overflow_maps_to_500() {
        assert_eq!(
            status_for(&ServerError::Overflow),
            StatusCode::INTERNAL_SERVER_ERROR
        );
    }

    #[test]
    fn invalid_content_hash_maps_to_400() {
        assert_eq!(
            status_for(&ServerError::InvalidContentHash),
            StatusCode::BAD_REQUEST
        );
    }

    #[test]
    fn expected_body_hash_mismatch_maps_to_400() {
        assert_eq!(
            status_for(&ServerError::ExpectedBodyHashMismatch),
            StatusCode::BAD_REQUEST
        );
    }

    // ---- ObjectStoreError Display tests ----

    use super::ObjectStoreError;

    #[test]
    fn object_store_error_local_display() {
        let io_err = std::io::Error::other("disk full");
        let err = ObjectStoreError::Local(io_err.into());
        let display = err.to_string();
        assert_eq!(display, "local storage operation failed");
    }

    #[test]
    fn object_store_error_s3_display() {
        let err = ObjectStoreError::MissingS3Config;
        let display = err.to_string();
        assert_eq!(display, "s3 object storage configuration is missing");
    }

    #[test]
    fn object_store_error_stored_length_mismatch_display() {
        let err = ObjectStoreError::StoredLengthMismatch;
        let display = err.to_string();
        assert_eq!(
            display,
            "stored object length did not match indexed metadata"
        );
    }

    #[test]
    fn object_store_error_migration_source_hash_mismatch_display() {
        let err = ObjectStoreError::MigrationSourceHashMismatch {
            key: "test-key".to_owned(),
            expected_hash: "abc".to_owned(),
            observed_hash: "def".to_owned(),
        };
        let display = err.to_string();
        assert!(display.contains("storage migration source object hash mismatch"));
        assert!(display.contains("test-key"));
        assert!(display.contains("abc"));
        assert!(display.contains("def"));
    }

    // ---- IndexError Display tests ----

    use super::IndexError;

    #[test]
    fn index_error_local_display() {
        let err = IndexError::MissingRequiredMetadataTable("files".to_owned());
        let display = err.to_string();
        assert_eq!(display, "required metadata table is missing: files");
    }

    #[test]
    fn index_error_conflicting_rename_target_display() {
        let err = IndexError::ConflictingRenameTargetRecord;
        let display = err.to_string();
        assert_eq!(
            display,
            "repository rename target already contains conflicting metadata"
        );
    }

    // ---- ServerError Display tests for un-tested variants ----

    #[test]
    fn server_error_display_io() {
        let err = ServerError::Io(std::io::Error::other("io failure"));
        assert_eq!(err.to_string(), "local storage operation failed");
    }

    #[test]
    fn server_error_display_json() {
        let err = ServerError::Json(serde_json::from_str::<()>("invalid").unwrap_err());
        assert_eq!(err.to_string(), "json operation failed");
    }

    #[test]
    fn server_error_display_request_body_read() {
        let io_err = std::io::Error::other("stream error");
        let axum_err = axum::Error::new(io_err);
        let err = ServerError::RequestBodyRead(axum_err);
        assert_eq!(err.to_string(), "request body stream failed");
    }

    #[test]
    fn server_error_display_request_body_too_large() {
        let err = ServerError::RequestBodyTooLarge;
        assert_eq!(
            err.to_string(),
            "request body exceeded the configured maximum accepted byte count"
        );
    }

    #[test]
    fn server_error_display_numeric_conversion() {
        // Create a TryFromIntError via a known fallible conversion
        let huge = 1_000_000_000_000u64;
        let try_err = i32::try_from(huge).unwrap_err();
        let err = ServerError::NumericConversion(try_err);
        assert_eq!(
            err.to_string(),
            "numeric conversion exceeded supported bounds"
        );
    }

    #[test]
    fn server_error_display_stored_file_metadata_too_large() {
        let err = ServerError::StoredFileMetadataTooLarge {
            observed_bytes: 1_000_000,
            maximum_bytes: 10_000,
        };
        let display = err.to_string();
        assert!(display.contains("stored file metadata exceeded the bounded parser ceiling"));
    }

    #[test]
    fn server_error_display_stored_file_metadata_length_mismatch() {
        let err = ServerError::StoredFileMetadataLengthMismatch;
        assert_eq!(
            err.to_string(),
            "stored file metadata length did not match the validated length"
        );
    }

    #[test]
    fn server_error_display_invalid_file_id() {
        let err = ServerError::InvalidFileId;
        assert!(err.to_string().contains("file identifier must be relative"));
    }

    #[test]
    fn server_error_display_invalid_content_hash() {
        let err = ServerError::InvalidContentHash;
        assert_eq!(
            err.to_string(),
            "content hash must be 64 hexadecimal characters"
        );
    }

    #[test]
    fn server_error_display_invalid_xorb_prefix() {
        let err = ServerError::InvalidXorbPrefix;
        assert_eq!(err.to_string(), "xorb transfer prefix must be default");
    }

    #[test]
    fn server_error_display_xorb_hash_mismatch() {
        let err = ServerError::XorbHashMismatch;
        assert_eq!(
            err.to_string(),
            "xorb body hash did not match the requested path hash"
        );
    }

    #[test]
    fn server_error_display_invalid_serialized_xorb() {
        let err = ServerError::InvalidSerializedXorb;
        assert_eq!(
            err.to_string(),
            "xorb body was not a valid serialized xorb object"
        );
    }

    #[test]
    fn server_error_display_missing_referenced_xorb() {
        let err = ServerError::MissingReferencedXorb;
        assert_eq!(err.to_string(), "shard referenced a missing xorb");
    }

    #[test]
    fn server_error_display_too_many_shard_terms() {
        let err = ServerError::TooManyShardTerms;
        assert_eq!(
            err.to_string(),
            "shard metadata exceeded bounded parser safety limits"
        );
    }

    #[test]
    fn server_error_display_invalid_range_header() {
        let err = ServerError::InvalidRangeHeader;
        assert_eq!(
            err.to_string(),
            "range header must use bytes=<start>-<end> syntax"
        );
    }

    #[test]
    fn server_error_display_range_not_satisfiable() {
        let err = ServerError::RangeNotSatisfiable;
        assert_eq!(err.to_string(), "requested range is not satisfiable");
    }

    #[test]
    fn server_error_display_missing_authorization() {
        let err = ServerError::MissingAuthorization;
        assert_eq!(err.to_string(), "authorization header is missing");
    }

    #[test]
    fn server_error_display_invalid_authorization_header() {
        let err = ServerError::InvalidAuthorizationHeader;
        assert_eq!(
            err.to_string(),
            "authorization header must use bearer format"
        );
    }

    #[test]
    fn server_error_display_signing_key_error() {
        let err = ServerError::SigningKeyError("bad format".to_owned());
        assert_eq!(
            err.to_string(),
            "token signing key is misconfigured: bad format"
        );
    }

    #[test]
    fn server_error_display_insufficient_scope() {
        let err = ServerError::InsufficientScope;
        assert_eq!(
            err.to_string(),
            "bearer token does not grant the required scope"
        );
    }

    #[test]
    fn server_error_display_provider_tokens_disabled() {
        let err = ServerError::ProviderTokensDisabled;
        assert_eq!(
            err.to_string(),
            "provider token issuance endpoint is not configured"
        );
    }

    #[test]
    fn server_error_display_missing_provider_api_key() {
        let err = ServerError::MissingProviderApiKey;
        assert_eq!(err.to_string(), "provider bootstrap key is missing");
    }

    #[test]
    fn server_error_display_invalid_provider_api_key() {
        let err = ServerError::InvalidProviderApiKey;
        assert_eq!(err.to_string(), "provider bootstrap key is invalid");
    }

    #[test]
    fn server_error_display_missing_provider_subject() {
        let err = ServerError::MissingProviderSubject;
        assert_eq!(err.to_string(), "provider subject is missing");
    }

    #[test]
    fn server_error_display_invalid_digest() {
        let err = ServerError::InvalidDigest;
        assert_eq!(
            err.to_string(),
            "digest must use sha256:<64 lowercase hex> format"
        );
    }

    #[test]
    fn server_error_display_invalid_repository_name() {
        let err = ServerError::InvalidRepositoryName;
        assert_eq!(err.to_string(), "repository name was invalid");
    }

    #[test]
    fn server_error_display_invalid_manifest_reference() {
        let err = ServerError::InvalidManifestReference;
        assert_eq!(err.to_string(), "manifest reference was invalid");
    }

    #[test]
    fn server_error_display_not_acceptable() {
        let err = ServerError::NotAcceptable;
        assert_eq!(
            err.to_string(),
            "requested representation was not acceptable"
        );
    }

    #[test]
    fn server_error_display_invalid_upload_session() {
        let err = ServerError::InvalidUploadSession;
        assert_eq!(err.to_string(), "upload session identifier was invalid");
    }

    #[test]
    fn server_error_display_too_many_upload_sessions() {
        let err = ServerError::TooManyUploadSessions;
        assert_eq!(err.to_string(), "too many active oci upload sessions");
    }

    #[test]
    fn server_error_display_missing_reconstruction_cache_redis_url() {
        let err = ServerError::MissingReconstructionCacheRedisUrl;
        assert_eq!(
            err.to_string(),
            "redis reconstruction cache requires a redis url"
        );
    }

    #[test]
    fn server_error_display_transfer_limiter_closed() {
        let err = ServerError::TransferLimiterClosed;
        assert_eq!(
            err.to_string(),
            "transfer concurrency limiter is unavailable"
        );
    }

    #[test]
    fn server_error_display_transfer_limiter_timed_out() {
        let err = ServerError::TransferLimiterTimedOut;
        assert_eq!(err.to_string(), "transfer concurrency limiter timed out");
    }

    #[test]
    fn server_error_display_request_query_too_large() {
        let err = ServerError::RequestQueryTooLarge;
        assert_eq!(
            err.to_string(),
            "request query exceeded the bounded metadata parser budget"
        );
    }

    #[test]
    fn server_error_display_request_body_frame_out_of_bounds() {
        let err = ServerError::RequestBodyFrameOutOfBounds;
        assert_eq!(
            err.to_string(),
            "request body frame exceeded checked bounds"
        );
    }

    #[test]
    fn server_error_display_object_store() {
        let err = ServerError::ObjectStore(ObjectStoreError::MissingS3Config);
        assert_eq!(err.to_string(), "object storage operation failed");
    }

    #[test]
    fn server_error_display_index() {
        let err = ServerError::Index(IndexError::MissingRequiredMetadataTable("test".to_owned()));
        assert_eq!(err.to_string(), "index adapter operation failed");
    }

    #[test]
    fn server_error_oci_error_code_for_not_found() {
        let err = ServerError::NotFound;
        assert_eq!(err.oci_error_code(), "MANIFEST_UNKNOWN");
    }

    #[test]
    fn server_error_oci_error_code_for_invalid_digest() {
        let err = ServerError::InvalidDigest;
        assert_eq!(err.oci_error_code(), "DIGEST_INVALID");
    }

    #[test]
    fn server_error_oci_error_code_for_internal_errors() {
        let err = ServerError::Overflow;
        assert_eq!(err.oci_error_code(), "INTERNAL");
    }

    #[test]
    fn server_error_display_invalid_serialized_shard() {
        use shardline_server_core::InvalidSerializedShardError;
        let err = ServerError::InvalidSerializedShard(
            InvalidSerializedShardError::RetainedShardChunkHashesNotStrictlyOrdered,
        );
        assert_eq!(
            err.to_string(),
            "shard body was not a valid serialized shard object"
        );
    }

    #[test]
    fn server_error_display_too_many_batch_reconstruction_file_ids() {
        let err = ServerError::TooManyBatchReconstructionFileIds;
        assert_eq!(
            err.to_string(),
            "batch reconstruction requested too many file identifiers"
        );
    }

    #[test]
    fn server_error_display_invalid_token() {
        let err = ServerError::InvalidToken(shardline_protocol::TokenCodecError::Expired);
        assert_eq!(err.to_string(), "bearer token was invalid");
    }

    #[test]
    fn server_error_display_provider() {
        let err = ServerError::Provider(crate::provider::ProviderServiceError::EmptyApiKey);
        assert_eq!(err.to_string(), "provider token issuance failed");
    }

    #[test]
    fn server_error_display_reconstruction_cache() {
        let err =
            ServerError::ReconstructionCache(shardline_cache::ReconstructionCacheError::Operation);
        assert_eq!(
            err.to_string(),
            "reconstruction cache adapter operation failed"
        );
    }

    #[test]
    fn server_error_display_config() {
        let err = ServerError::Config(
            crate::config::ServerConfigError::MissingTokenSigningKeyForServedRoutes,
        );
        assert_eq!(err.to_string(), "server configuration was invalid");
    }

    #[allow(clippy::panic)]
    #[test]
    fn server_error_display_blocking_task() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        #[allow(clippy::panic)]
        let task = rt.spawn(async { panic!("test panic") });
        let join_err = rt.block_on(async { task.await.unwrap_err() });
        let err = ServerError::BlockingTask(join_err);
        assert!(err.to_string().contains("blocking worker task failed"));
    }

    #[test]
    fn server_error_display_overflow() {
        let err = ServerError::Overflow;
        assert_eq!(err.to_string(), "arithmetic overflow");
    }

    #[test]
    fn server_error_display_unauthorized_challenge() {
        let err = ServerError::UnauthorizedChallenge("Bearer realm=\"test\"".to_owned());
        assert_eq!(err.to_string(), "authorization challenge required");
    }

    #[test]
    fn server_error_display_too_many_registry_token_requests() {
        let err = ServerError::TooManyRegistryTokenRequests;
        assert_eq!(
            err.to_string(),
            "too many active oci registry token requests"
        );
    }

    #[test]
    fn server_error_display_missing_provider_webhook_authentication() {
        let err = ServerError::MissingProviderWebhookAuthentication;
        assert_eq!(
            err.to_string(),
            "provider webhook authentication is missing"
        );
    }

    #[test]
    fn server_error_display_invalid_provider_webhook_authentication() {
        let err = ServerError::InvalidProviderWebhookAuthentication;
        assert_eq!(
            err.to_string(),
            "provider webhook authentication is invalid"
        );
    }

    #[test]
    fn server_error_display_invalid_provider_webhook_payload() {
        let err = ServerError::InvalidProviderWebhookPayload;
        assert_eq!(err.to_string(), "provider webhook payload was invalid");
    }

    #[test]
    fn server_error_display_unknown_provider() {
        let err = ServerError::UnknownProvider;
        assert_eq!(err.to_string(), "provider is not configured");
    }

    #[test]
    fn server_error_display_provider_denied() {
        let err = ServerError::ProviderDenied;
        assert_eq!(
            err.to_string(),
            "provider denied requested repository access"
        );
    }

    #[test]
    fn server_error_display_invalid_provider_token_request() {
        let err = ServerError::InvalidProviderTokenRequest;
        assert_eq!(err.to_string(), "provider token request was invalid");
    }

    #[test]
    fn server_error_status_code_for_missing_provider_webhook_authentication() {
        assert_eq!(
            status_for(&ServerError::MissingProviderWebhookAuthentication),
            StatusCode::UNAUTHORIZED
        );
    }

    #[test]
    fn server_error_status_code_for_invalid_provider_api_key() {
        assert_eq!(
            status_for(&ServerError::InvalidProviderApiKey),
            StatusCode::FORBIDDEN
        );
    }

    #[test]
    fn server_error_oci_error_code_for_unauthorized() {
        let err = ServerError::MissingAuthorization;
        assert_eq!(err.oci_error_code(), "UNAUTHORIZED");
    }

    #[test]
    fn server_error_oci_error_code_for_denied() {
        let err = ServerError::InsufficientScope;
        assert_eq!(err.oci_error_code(), "DENIED");
    }

    #[test]
    fn server_error_debug_output() {
        let err = ServerError::NotFound;
        let debug = format!("{err:?}");
        assert!(debug.contains("NotFound"));
    }

    #[test]
    fn server_error_display_hash_parse() {
        use shardline_protocol::HashParseError;
        let err = ServerError::HashParse(HashParseError::InvalidLength);
        assert_eq!(err.to_string(), "invalid content hash");
    }

    #[test]
    fn server_error_display_expected_body_hash_mismatch() {
        let err = ServerError::ExpectedBodyHashMismatch;
        assert_eq!(
            err.to_string(),
            "uploaded body hash did not match the expected sha256"
        );
    }

    #[test]
    fn server_error_display_not_found() {
        let err = ServerError::NotFound;
        assert_eq!(err.to_string(), "content not found");
    }

    #[test]
    fn server_error_oci_error_code_for_size_invalid() {
        let err = ServerError::RequestBodyTooLarge;
        assert_eq!(err.oci_error_code(), "SIZE_INVALID");
    }

    #[test]
    fn server_error_oci_error_code_for_invalid_manifest_reference() {
        let err = ServerError::InvalidManifestReference;
        assert_eq!(err.oci_error_code(), "MANIFEST_INVALID");
    }

    #[test]
    fn server_error_oci_error_code_for_invalid_upload_session() {
        let err = ServerError::InvalidUploadSession;
        assert_eq!(err.oci_error_code(), "MANIFEST_INVALID");
    }

    #[test]
    fn server_error_oci_error_code_for_not_acceptable() {
        let err = ServerError::NotAcceptable;
        assert_eq!(err.oci_error_code(), "UNSUPPORTED");
    }

    #[test]
    fn server_error_oci_error_code_for_too_many_requests() {
        let err = ServerError::TooManyUploadSessions;
        assert_eq!(err.oci_error_code(), "TOO_MANY_REQUESTS");
    }

    #[test]
    fn server_error_oci_error_code_for_expected_body_hash_mismatch() {
        let err = ServerError::ExpectedBodyHashMismatch;
        assert_eq!(err.oci_error_code(), "DIGEST_INVALID");
    }

    #[test]
    fn server_error_status_code_for_invalid_range_header() {
        assert_eq!(
            status_for(&ServerError::InvalidRangeHeader),
            StatusCode::BAD_REQUEST
        );
    }

    #[test]
    fn server_error_status_code_for_request_query_too_large() {
        assert_eq!(
            status_for(&ServerError::RequestQueryTooLarge),
            StatusCode::URI_TOO_LONG
        );
    }

    #[test]
    fn server_error_status_code_for_too_many_shard_terms() {
        assert_eq!(
            status_for(&ServerError::TooManyShardTerms),
            StatusCode::PAYLOAD_TOO_LARGE
        );
    }

    #[test]
    fn server_error_status_code_for_too_many_batch_reconstruction_file_ids() {
        assert_eq!(
            status_for(&ServerError::TooManyBatchReconstructionFileIds),
            StatusCode::PAYLOAD_TOO_LARGE
        );
    }

    #[test]
    fn server_error_status_code_for_transfer_limiter_closed() {
        assert_eq!(
            status_for(&ServerError::TransferLimiterClosed),
            StatusCode::SERVICE_UNAVAILABLE
        );
    }

    #[test]
    fn server_error_status_code_for_missing_provider_subject() {
        assert_eq!(
            status_for(&ServerError::MissingProviderSubject),
            StatusCode::UNAUTHORIZED
        );
    }

    #[test]
    fn server_error_status_code_for_invalid_provider_webhook_payload() {
        assert_eq!(
            status_for(&ServerError::InvalidProviderWebhookPayload),
            StatusCode::BAD_REQUEST
        );
    }

    #[test]
    fn server_error_status_code_for_invalid_serialized_shard() {
        use shardline_server_core::InvalidSerializedShardError;
        let err = ServerError::InvalidSerializedShard(
            InvalidSerializedShardError::RetainedShardChunkHashesNotStrictlyOrdered,
        );
        assert_eq!(status_for(&err), StatusCode::BAD_REQUEST);
    }

    #[test]
    fn server_error_status_code_for_xorb_hash_mismatch() {
        assert_eq!(
            status_for(&ServerError::XorbHashMismatch),
            StatusCode::BAD_REQUEST
        );
    }

    #[test]
    fn server_error_status_code_for_invalid_digest() {
        assert_eq!(
            status_for(&ServerError::InvalidDigest),
            StatusCode::BAD_REQUEST
        );
    }

    #[test]
    fn server_error_status_code_for_missing_referenced_xorb() {
        assert_eq!(
            status_for(&ServerError::MissingReferencedXorb),
            StatusCode::BAD_REQUEST
        );
    }

    // ---- ObjectStoreError conversion StatusCode tests ----

    #[test]
    fn object_store_error_prefix_display() {
        let err = ObjectStoreError::Prefix(shardline_storage::ObjectPrefixError::UnsafePath);
        assert_eq!(err.to_string(), "object storage prefix validation failed");
    }

    // ---- Comprehensive ServerError status_code tests for every variant ----

    #[test]
    fn status_code_invalid_repository_name() {
        assert_eq!(
            status_for(&ServerError::InvalidRepositoryName),
            StatusCode::BAD_REQUEST
        );
    }

    #[test]
    fn status_code_invalid_manifest_reference() {
        assert_eq!(
            status_for(&ServerError::InvalidManifestReference),
            StatusCode::BAD_REQUEST
        );
    }

    #[test]
    fn status_code_invalid_upload_session() {
        assert_eq!(
            status_for(&ServerError::InvalidUploadSession),
            StatusCode::BAD_REQUEST
        );
    }

    #[test]
    fn status_code_invalid_xorb_prefix() {
        assert_eq!(
            status_for(&ServerError::InvalidXorbPrefix),
            StatusCode::BAD_REQUEST
        );
    }

    #[test]
    fn status_code_hash_parse() {
        use shardline_protocol::HashParseError;
        assert_eq!(
            status_for(&ServerError::HashParse(HashParseError::InvalidLength)),
            StatusCode::BAD_REQUEST
        );
    }

    #[test]
    fn status_code_unauthorized_challenge() {
        assert_eq!(
            status_for(&ServerError::UnauthorizedChallenge("x".into())),
            StatusCode::UNAUTHORIZED
        );
    }

    #[test]
    fn status_code_invalid_serialized_xorb() {
        assert_eq!(
            status_for(&ServerError::InvalidSerializedXorb),
            StatusCode::BAD_REQUEST
        );
    }

    #[test]
    fn status_code_request_body_read() {
        let io_err = std::io::Error::other("stream");
        let axum_err = axum::Error::new(io_err);
        assert_eq!(
            status_for(&ServerError::RequestBodyRead(axum_err)),
            StatusCode::BAD_REQUEST
        );
    }

    #[test]
    fn status_code_request_body_frame_out_of_bounds() {
        assert_eq!(
            status_for(&ServerError::RequestBodyFrameOutOfBounds),
            StatusCode::BAD_REQUEST
        );
    }

    #[test]
    fn status_code_provider_tokens_disabled() {
        assert_eq!(
            status_for(&ServerError::ProviderTokensDisabled),
            StatusCode::NOT_FOUND
        );
    }

    #[test]
    fn status_code_invalid_authorization_header() {
        assert_eq!(
            status_for(&ServerError::InvalidAuthorizationHeader),
            StatusCode::UNAUTHORIZED
        );
    }

    #[test]
    fn status_code_invalid_token() {
        use shardline_protocol::TokenCodecError;
        assert_eq!(
            status_for(&ServerError::InvalidToken(TokenCodecError::Expired)),
            StatusCode::UNAUTHORIZED
        );
    }

    #[test]
    fn status_code_missing_provider_api_key() {
        assert_eq!(
            status_for(&ServerError::MissingProviderApiKey),
            StatusCode::UNAUTHORIZED
        );
    }

    #[test]
    fn status_code_invalid_provider_webhook_authentication() {
        assert_eq!(
            status_for(&ServerError::InvalidProviderWebhookAuthentication),
            StatusCode::FORBIDDEN
        );
    }

    #[test]
    fn status_code_invalid_provider_token_request() {
        assert_eq!(
            status_for(&ServerError::InvalidProviderTokenRequest),
            StatusCode::BAD_REQUEST
        );
    }

    #[test]
    fn status_code_json() {
        let err = ServerError::Json(serde_json::from_str::<()>("invalid").unwrap_err());
        assert_eq!(status_for(&err), StatusCode::INTERNAL_SERVER_ERROR);
    }

    #[test]
    fn status_code_numeric_conversion() {
        let huge = 1_000_000_000_000u64;
        let try_err = i32::try_from(huge).unwrap_err();
        assert_eq!(
            status_for(&ServerError::NumericConversion(try_err)),
            StatusCode::INTERNAL_SERVER_ERROR
        );
    }

    #[test]
    fn status_code_object_store() {
        let err = ServerError::ObjectStore(ObjectStoreError::MissingS3Config);
        assert_eq!(status_for(&err), StatusCode::INTERNAL_SERVER_ERROR);
    }

    #[test]
    fn status_code_index() {
        let err = ServerError::Index(IndexError::MissingRequiredMetadataTable("t".into()));
        assert_eq!(status_for(&err), StatusCode::INTERNAL_SERVER_ERROR);
    }

    #[test]
    fn status_code_stored_file_metadata_too_large() {
        let err = ServerError::StoredFileMetadataTooLarge {
            observed_bytes: 1,
            maximum_bytes: 1,
        };
        assert_eq!(status_for(&err), StatusCode::INTERNAL_SERVER_ERROR);
    }

    #[test]
    fn status_code_stored_file_metadata_length_mismatch() {
        assert_eq!(
            status_for(&ServerError::StoredFileMetadataLengthMismatch),
            StatusCode::INTERNAL_SERVER_ERROR
        );
    }

    #[test]
    fn status_code_config() {
        let err = ServerError::Config(
            crate::config::ServerConfigError::MissingTokenSigningKeyForServedRoutes,
        );
        assert_eq!(status_for(&err), StatusCode::INTERNAL_SERVER_ERROR);
    }

    #[test]
    fn status_code_missing_reconstruction_cache_redis_url() {
        assert_eq!(
            status_for(&ServerError::MissingReconstructionCacheRedisUrl),
            StatusCode::INTERNAL_SERVER_ERROR
        );
    }

    #[test]
    fn status_code_reconstruction_cache() {
        use shardline_cache::ReconstructionCacheError;
        let err = ServerError::ReconstructionCache(ReconstructionCacheError::Operation);
        assert_eq!(status_for(&err), StatusCode::INTERNAL_SERVER_ERROR);
    }

    #[allow(clippy::panic)]
    #[test]
    fn status_code_blocking_task() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let task = rt.spawn(async { panic!("x") });
        let join_err = rt.block_on(async { task.await.unwrap_err() });
        assert_eq!(
            status_for(&ServerError::BlockingTask(join_err)),
            StatusCode::INTERNAL_SERVER_ERROR
        );
    }

    #[test]
    fn status_code_signing_key_error() {
        let err = ServerError::SigningKeyError("bad".into());
        assert_eq!(status_for(&err), StatusCode::INTERNAL_SERVER_ERROR);
    }

    #[test]
    fn status_code_provider() {
        let err = ServerError::Provider(crate::provider::ProviderServiceError::EmptyApiKey);
        assert_eq!(status_for(&err), StatusCode::INTERNAL_SERVER_ERROR);
    }

    // ---- Comprehensive OCI error code tests ----

    #[test]
    fn oci_error_code_name_invalid() {
        assert_eq!(
            ServerError::InvalidRepositoryName.oci_error_code(),
            "NAME_INVALID"
        );
    }

    #[test]
    fn oci_error_code_digest_invalid_for_expected_body_hash_mismatch() {
        assert_eq!(
            ServerError::ExpectedBodyHashMismatch.oci_error_code(),
            "DIGEST_INVALID"
        );
    }

    #[test]
    fn oci_error_code_internal_for_io() {
        let err = ServerError::Io(std::io::Error::other("x"));
        assert_eq!(err.oci_error_code(), "INTERNAL");
    }

    #[test]
    fn oci_error_code_internal_for_json() {
        let err = ServerError::Json(serde_json::from_str::<()>("x").unwrap_err());
        assert_eq!(err.oci_error_code(), "INTERNAL");
    }

    #[test]
    fn oci_error_code_internal_for_overflow() {
        assert_eq!(ServerError::Overflow.oci_error_code(), "INTERNAL");
    }

    // ---- IndexError Display tests for all variants ----

    #[test]
    fn index_error_local_display_full() {
        let io_err = std::io::Error::other("disk");
        let err = IndexError::Local(shardline_index::LocalIndexStoreError::Io(io_err));
        assert_eq!(err.to_string(), "index adapter operation failed");
    }

    #[test]
    fn index_error_memory_index_display() {
        let err = IndexError::MemoryIndex(shardline_index::MemoryIndexStoreError::LockPoisoned(
            "test".to_owned(),
        ));
        assert_eq!(err.to_string(), "memory index adapter operation failed");
    }

    #[test]
    fn index_error_memory_record_display() {
        let err = IndexError::MemoryRecord(shardline_index::MemoryRecordStoreError::LockPoisoned(
            "test".to_owned(),
        ));
        assert_eq!(err.to_string(), "memory record adapter operation failed");
    }

    #[test]
    fn index_error_postgres_metadata_display() {
        let err = IndexError::PostgresMetadata(shardline_index::PostgresMetadataStoreError::Json(
            serde_json::from_str::<()>("x").unwrap_err(),
        ));
        assert_eq!(
            err.to_string(),
            "postgres metadata adapter operation failed"
        );
    }

    #[test]
    fn index_error_retention_hold_display() {
        let err = IndexError::RetentionHold(shardline_index::RetentionHoldError::InvertedTimeline);
        assert_eq!(err.to_string(), "retention hold input was invalid");
    }

    #[test]
    fn index_error_quarantine_candidate_display() {
        let err = IndexError::QuarantineCandidate(
            shardline_index::QuarantineCandidateError::InvertedTimeline,
        );
        assert_eq!(err.to_string(), "quarantine candidate input was invalid");
    }

    #[test]
    fn index_error_webhook_delivery_display() {
        let err = IndexError::WebhookDelivery(
            shardline_index::WebhookDeliveryError::EmptyRepositoryOwner,
        );
        assert_eq!(err.to_string(), "webhook delivery metadata was invalid");
    }

    #[test]
    fn index_error_file_record_invariant_display() {
        let err = IndexError::FileRecordInvariant(
            shardline_index::FileRecordInvariantError::NonContiguousChunkOffsets,
        );
        assert_eq!(err.to_string(), "stored file metadata was invalid");
    }

    #[test]
    fn index_error_invalid_lifecycle_metadata_display() {
        let err = IndexError::InvalidLifecycleMetadata(shardline_server_core::InvalidLifecycleMetadataError::QuarantineCandidateDeleteBeforeFirstSeen {
            object_key: "test".into(),
            delete_after_unix_seconds: 10,
            first_seen_unreachable_at_unix_seconds: 20,
        });
        assert_eq!(
            err.to_string(),
            "lifecycle metadata was internally inconsistent"
        );
    }

    #[test]
    fn index_error_invalid_reconstruction_response_display() {
        let err = IndexError::InvalidReconstructionResponse(shardline_server_core::InvalidReconstructionResponseError::RecordStoreGlobalLatestWalkAttempted);
        let display = err.to_string();
        assert!(display.contains("reconstruction response invariant failed"));
    }

    // ---- From implementation tests ----

    #[test]
    fn from_xorb_parse_error_hash_mismatch() {
        use crate::xet_adapter::XorbParseError;
        let err: ServerError = XorbParseError::HashMismatch.into();
        assert!(matches!(err, ServerError::XorbHashMismatch));
    }

    #[test]
    fn from_xorb_parse_error_invalid_format() {
        use crate::xet_adapter::XorbParseError;
        // Use NumericConversion variant which also maps to InvalidSerializedXorb
        let huge = 1_000_000_000_000u64;
        let try_err = i32::try_from(huge).unwrap_err();
        let err: ServerError = XorbParseError::NumericConversion(try_err).into();
        assert!(matches!(err, ServerError::InvalidSerializedXorb));
    }

    #[test]
    fn from_http_range_parse_error_unsatisfiable() {
        use shardline_protocol::HttpRangeParseError;
        let err: ServerError = HttpRangeParseError::Unsatisfiable.into();
        assert!(matches!(err, ServerError::RangeNotSatisfiable));
    }

    #[test]
    fn from_http_range_parse_error_invalid_syntax() {
        use shardline_protocol::HttpRangeParseError;
        let err: ServerError =
            HttpRangeParseError::InvalidSyntax("test".to_owned()).into();
        assert!(matches!(err, ServerError::InvalidRangeHeader));
    }

    #[test]
    fn from_http_range_parse_error_missing_bytes_unit() {
        use shardline_protocol::HttpRangeParseError;
        let err: ServerError = HttpRangeParseError::MissingBytesUnit.into();
        assert!(matches!(err, ServerError::InvalidRangeHeader));
    }

    #[test]
    fn from_local_object_store_error() {
        let io_err = std::io::Error::other("local");
        let err: ServerError = shardline_storage::LocalObjectStoreError::Io(io_err).into();
        assert!(matches!(
            err,
            ServerError::ObjectStore(ObjectStoreError::Local(_))
        ));
    }

    #[test]
    fn from_s3_object_store_error() {
        let err: ServerError = shardline_storage::S3ObjectStoreError::IncompleteCredentials.into();
        assert!(matches!(
            err,
            ServerError::ObjectStore(ObjectStoreError::S3(_))
        ));
    }

    #[test]
    fn from_object_prefix_error() {
        let err: ServerError = shardline_storage::ObjectPrefixError::UnsafePath.into();
        assert!(matches!(
            err,
            ServerError::ObjectStore(ObjectStoreError::Prefix(_))
        ));
    }

    #[test]
    fn from_local_index_store_error() {
        let io_err = std::io::Error::other("idx");
        let err: ServerError = shardline_index::LocalIndexStoreError::Io(io_err).into();
        assert!(matches!(err, ServerError::Index(IndexError::Local(_))));
    }

    #[test]
    fn from_memory_index_store_error() {
        let err: ServerError =
            shardline_index::MemoryIndexStoreError::LockPoisoned("test".to_owned()).into();
        assert!(matches!(
            err,
            ServerError::Index(IndexError::MemoryIndex(_))
        ));
    }

    #[test]
    fn from_memory_record_store_error() {
        let err: ServerError =
            shardline_index::MemoryRecordStoreError::LockPoisoned("test".to_owned()).into();
        assert!(matches!(
            err,
            ServerError::Index(IndexError::MemoryRecord(_))
        ));
    }

    #[test]
    fn from_postgres_metadata_store_error() {
        let err: ServerError = shardline_index::PostgresMetadataStoreError::Json(
            serde_json::from_str::<()>("x").unwrap_err(),
        )
        .into();
        assert!(matches!(
            err,
            ServerError::Index(IndexError::PostgresMetadata(_))
        ));
    }

    #[test]
    fn from_retention_hold_error() {
        let err: ServerError = shardline_index::RetentionHoldError::InvertedTimeline.into();
        assert!(matches!(
            err,
            ServerError::Index(IndexError::RetentionHold(_))
        ));
    }

    #[test]
    fn from_quarantine_candidate_error() {
        let err: ServerError = shardline_index::QuarantineCandidateError::InvertedTimeline.into();
        assert!(matches!(
            err,
            ServerError::Index(IndexError::QuarantineCandidate(_))
        ));
    }

    #[test]
    fn from_webhook_delivery_error() {
        let err: ServerError = shardline_index::WebhookDeliveryError::EmptyRepositoryOwner.into();
        assert!(matches!(
            err,
            ServerError::Index(IndexError::WebhookDelivery(_))
        ));
    }

    #[test]
    fn from_file_record_invariant_error() {
        let err: ServerError =
            shardline_index::FileRecordInvariantError::NonContiguousChunkOffsets.into();
        assert!(matches!(
            err,
            ServerError::Index(IndexError::FileRecordInvariant(_))
        ));
    }

    #[test]
    fn from_invalid_lifecycle_metadata_error() {
        let err: ServerError = shardline_server_core::InvalidLifecycleMetadataError::QuarantineCandidateDeleteBeforeFirstSeen {
            object_key: "test".into(),
            delete_after_unix_seconds: 10,
            first_seen_unreachable_at_unix_seconds: 20,
        }.into();
        assert!(matches!(
            err,
            ServerError::Index(IndexError::InvalidLifecycleMetadata(_))
        ));
    }

    #[test]
    fn from_parse_stored_file_record_too_large() {
        let err: ServerError =
            shardline_server_core::ParseStoredFileRecordError::StoredFileMetadataTooLarge {
                observed_bytes: 100,
                maximum_bytes: 50,
            }
            .into();
        assert!(matches!(
            err,
            ServerError::StoredFileMetadataTooLarge {
                observed_bytes: 100,
                maximum_bytes: 50
            }
        ));
    }

    #[test]
    fn from_oci_adapter_error_invalid_digest() {
        let err: ServerError = shardline_oci_adapter::OciAdapterError::InvalidDigest.into();
        assert!(matches!(err, ServerError::InvalidDigest));
    }

    #[test]
    fn from_protocol_error_invalid_content_hash() {
        let err: ServerError =
            shardline_protocol_adapters::ProtocolError::InvalidContentHash.into();
        assert!(matches!(err, ServerError::InvalidContentHash));
    }

    // ---- IntoResponse tests ----

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn into_response_unauthorized_challenge_attaches_header() {
        use axum::http::HeaderValue;
        let err = ServerError::UnauthorizedChallenge("Bearer realm=\"test\"".to_owned());
        let response = err.into_response();
        let www_auth = response.headers().get(axum::http::header::WWW_AUTHENTICATE);
        assert_eq!(
            www_auth,
            Some(&HeaderValue::from_static("Bearer realm=\"test\""))
        );
    }

    #[test]
    fn into_response_missing_auth_attaches_default_challenge() {
        use axum::http::HeaderValue;
        let err = ServerError::MissingAuthorization;
        let response = err.into_response();
        let www_auth = response.headers().get(axum::http::header::WWW_AUTHENTICATE);
        assert_eq!(
            www_auth,
            Some(&HeaderValue::from_static("Bearer realm=\"shardline\""))
        );
    }

    #[test]
    fn into_response_not_found_has_no_www_auth() {
        let err = ServerError::NotFound;
        let response = err.into_response();
        assert!(
            response
                .headers()
                .get(axum::http::header::WWW_AUTHENTICATE)
                .is_none()
        );
    }

    // ---- OciError tests ----

    #[test]
    fn oci_error_from_server_error() {
        let oci = OciError(ServerError::NotFound);
        let response = oci.into_response();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[test]
    fn oci_error_from_server_error_unauthorized_challenge() {
        let oci = OciError(ServerError::UnauthorizedChallenge("Bearer test".to_owned()));
        let response = oci.into_response();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn oci_error_response_body_contains_code() {
        let oci = OciError(ServerError::NotFound);
        let response = oci.into_response();
        let (_parts, body) = response.into_parts();
        let body_bytes = axum::body::to_bytes(body, usize::MAX).await.unwrap();
        let text = String::from_utf8(body_bytes.to_vec()).unwrap();
        assert!(text.contains("MANIFEST_UNKNOWN"));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn oci_error_body_has_expected_structure() {
        let oci = OciError(ServerError::InvalidDigest);
        let response = oci.into_response();
        let (_parts, body) = response.into_parts();
        let body_bytes = axum::body::to_bytes(body, usize::MAX).await.unwrap();
        let parsed: serde_json::Value = serde_json::from_slice(&body_bytes).unwrap();
        assert!(parsed.get("errors").is_some());
        let errors = parsed["errors"].as_array().unwrap();
        assert_eq!(errors[0]["code"], "DIGEST_INVALID");
    }

    // ---- Additional From implementation tests ----

    #[test]
    fn from_xet_adapter_error_io() {
        let err: ServerError =
            crate::xet_adapter::XetAdapterError::Io(std::io::Error::other("io")).into();
        assert!(matches!(err, ServerError::Io(_)));
    }

    #[test]
    fn from_xet_adapter_error_numeric_conversion() {
        let huge = 1_000_000_000_000u64;
        let try_err = i32::try_from(huge).unwrap_err();
        let err: ServerError =
            crate::xet_adapter::XetAdapterError::NumericConversion(try_err).into();
        assert!(matches!(err, ServerError::NumericConversion(_)));
    }

    #[test]
    fn from_xet_adapter_error_hash_parse() {
        use shardline_protocol::HashParseError;
        let err: ServerError =
            crate::xet_adapter::XetAdapterError::HashParse(HashParseError::InvalidLength).into();
        assert!(matches!(err, ServerError::HashParse(_)));
    }

    #[test]
    fn from_xet_adapter_error_local_object_store() {
        let err: ServerError = crate::xet_adapter::XetAdapterError::LocalObjectStore(
            shardline_storage::LocalObjectStoreError::Io(std::io::Error::other("local")),
        )
        .into();
        assert!(matches!(
            err,
            ServerError::ObjectStore(ObjectStoreError::Local(_))
        ));
    }

    #[test]
    fn from_xet_adapter_error_s3_object_store() {
        let err: ServerError = crate::xet_adapter::XetAdapterError::S3ObjectStore(
            shardline_storage::S3ObjectStoreError::IncompleteCredentials,
        )
        .into();
        assert!(matches!(
            err,
            ServerError::ObjectStore(ObjectStoreError::S3(_))
        ));
    }

    #[test]
    fn from_xet_adapter_error_index_store() {
        let io_err = std::io::Error::other("idx");
        let err: ServerError = crate::xet_adapter::XetAdapterError::IndexStore(
            shardline_index::LocalIndexStoreError::Io(io_err),
        )
        .into();
        assert!(matches!(err, ServerError::Index(IndexError::Local(_))));
    }

    #[test]
    fn from_xet_adapter_error_memory_index() {
        let err: ServerError = crate::xet_adapter::XetAdapterError::MemoryIndexStore(
            shardline_index::MemoryIndexStoreError::LockPoisoned("test".to_owned()),
        )
        .into();
        assert!(matches!(
            err,
            ServerError::Index(IndexError::MemoryIndex(_))
        ));
    }

    #[test]
    fn from_xet_adapter_error_memory_record() {
        let err: ServerError = crate::xet_adapter::XetAdapterError::MemoryRecordStore(
            shardline_index::MemoryRecordStoreError::LockPoisoned("test".to_owned()),
        )
        .into();
        assert!(matches!(
            err,
            ServerError::Index(IndexError::MemoryRecord(_))
        ));
    }

    #[test]
    fn from_xet_adapter_error_postgres_metadata() {
        let err: ServerError = crate::xet_adapter::XetAdapterError::PostgresMetadata(
            shardline_index::PostgresMetadataStoreError::Json(
                serde_json::from_str::<()>("x").unwrap_err(),
            ),
        )
        .into();
        assert!(matches!(
            err,
            ServerError::Index(IndexError::PostgresMetadata(_))
        ));
    }

    #[test]
    fn from_xet_adapter_error_file_record_invariant() {
        let err: ServerError = crate::xet_adapter::XetAdapterError::FileRecordInvariant(
            shardline_index::FileRecordInvariantError::NonContiguousChunkOffsets,
        )
        .into();
        assert!(matches!(
            err,
            ServerError::Index(IndexError::FileRecordInvariant(_))
        ));
    }

    #[test]
    fn from_xet_adapter_error_invalid_content_hash() {
        let err: ServerError = crate::xet_adapter::XetAdapterError::InvalidContentHash.into();
        assert!(matches!(err, ServerError::InvalidContentHash));
    }

    #[test]
    fn from_xet_adapter_error_invalid_xorb_prefix() {
        let err: ServerError = crate::xet_adapter::XetAdapterError::InvalidXorbPrefix.into();
        assert!(matches!(err, ServerError::InvalidXorbPrefix));
    }

    #[test]
    fn from_xet_adapter_error_xorb_hash_mismatch() {
        let err: ServerError = crate::xet_adapter::XetAdapterError::XorbHashMismatch.into();
        assert!(matches!(err, ServerError::XorbHashMismatch));
    }

    #[test]
    fn from_xet_adapter_error_invalid_serialized_xorb() {
        let err: ServerError = crate::xet_adapter::XetAdapterError::InvalidSerializedXorb.into();
        assert!(matches!(err, ServerError::InvalidSerializedXorb));
    }

    #[test]
    fn from_xet_adapter_error_invalid_serialized_shard() {
        use shardline_server_core::InvalidSerializedShardError;
        let err: ServerError = crate::xet_adapter::XetAdapterError::InvalidSerializedShard(
            InvalidSerializedShardError::RetainedShardChunkHashesNotStrictlyOrdered,
        )
        .into();
        assert!(matches!(err, ServerError::InvalidSerializedShard(_)));
    }

    #[test]
    fn from_xet_adapter_error_missing_referenced_xorb() {
        let err: ServerError = crate::xet_adapter::XetAdapterError::MissingReferencedXorb.into();
        assert!(matches!(err, ServerError::MissingReferencedXorb));
    }

    #[test]
    fn from_xet_adapter_error_too_many_shard_terms() {
        let err: ServerError = crate::xet_adapter::XetAdapterError::TooManyShardTerms.into();
        assert!(matches!(err, ServerError::TooManyShardTerms));
    }

    #[test]
    fn from_xet_adapter_error_not_found() {
        let err: ServerError = crate::xet_adapter::XetAdapterError::NotFound.into();
        assert!(matches!(err, ServerError::NotFound));
    }

    #[test]
    fn from_xet_adapter_error_overflow() {
        let err: ServerError = crate::xet_adapter::XetAdapterError::Overflow.into();
        assert!(matches!(err, ServerError::Overflow));
    }

    #[test]
    fn from_xet_adapter_error_range_not_satisfiable() {
        let err: ServerError = crate::xet_adapter::XetAdapterError::RangeNotSatisfiable.into();
        assert!(matches!(err, ServerError::RangeNotSatisfiable));
    }

    #[test]
    fn from_provider_events_error_overflow() {
        use shardline_provider_events::ProviderEventsError;
        let err: ServerError = ProviderEventsError::Overflow.into();
        assert!(matches!(err, ServerError::Overflow));
    }

    #[test]
    fn from_provider_events_error_invalid_content_hash() {
        use shardline_provider_events::ProviderEventsError;
        let err: ServerError = ProviderEventsError::InvalidContentHash.into();
        assert!(matches!(err, ServerError::InvalidContentHash));
    }

    #[test]
    fn from_provider_events_error_invalid_webhook_payload() {
        use shardline_provider_events::ProviderEventsError;
        let err: ServerError = ProviderEventsError::InvalidProviderWebhookPayload.into();
        assert!(matches!(err, ServerError::InvalidProviderWebhookPayload));
    }

    #[test]
    fn from_provider_events_error_conflicting_rename() {
        use shardline_provider_events::ProviderEventsError;
        let err: ServerError = ProviderEventsError::ConflictingRenameTargetRecord.into();
        assert!(matches!(
            err,
            ServerError::Index(IndexError::ConflictingRenameTargetRecord)
        ));
    }

    #[test]
    fn from_provider_events_error_json() {
        use shardline_provider_events::ProviderEventsError;
        let err: ServerError =
            ProviderEventsError::Json(serde_json::from_str::<()>("invalid").unwrap_err()).into();
        assert!(matches!(err, ServerError::Json(_)));
    }

    #[test]
    fn from_provider_events_error_numeric_conversion() {
        use shardline_provider_events::ProviderEventsError;
        let huge = 1_000_000_000_000u64;
        let try_err = i32::try_from(huge).unwrap_err();
        let err: ServerError = ProviderEventsError::NumericConversion(try_err).into();
        assert!(matches!(err, ServerError::NumericConversion(_)));
    }

    #[test]
    fn from_provider_events_error_retention_hold() {
        use shardline_provider_events::ProviderEventsError;
        let err: ServerError = ProviderEventsError::RetentionHold(
            shardline_index::RetentionHoldError::InvertedTimeline,
        )
        .into();
        assert!(matches!(
            err,
            ServerError::Index(IndexError::RetentionHold(_))
        ));
    }

    #[test]
    fn from_provider_events_error_webhook_delivery() {
        use shardline_provider_events::ProviderEventsError;
        let err: ServerError = ProviderEventsError::WebhookDelivery(
            shardline_index::WebhookDeliveryError::EmptyRepositoryOwner,
        )
        .into();
        assert!(matches!(
            err,
            ServerError::Index(IndexError::WebhookDelivery(_))
        ));
    }

    #[test]
    fn from_provider_events_error_index_store() {
        use shardline_provider_events::ProviderEventsError;
        let io_err = std::io::Error::other("idx");
        let err: ServerError =
            ProviderEventsError::IndexStore(shardline_index::LocalIndexStoreError::Io(io_err))
                .into();
        assert!(matches!(err, ServerError::Index(IndexError::Local(_))));
    }

    #[test]
    fn from_provider_events_error_memory_index() {
        use shardline_provider_events::ProviderEventsError;
        let err: ServerError = ProviderEventsError::MemoryIndexStore(
            shardline_index::MemoryIndexStoreError::LockPoisoned("test".to_owned()),
        )
        .into();
        assert!(matches!(
            err,
            ServerError::Index(IndexError::MemoryIndex(_))
        ));
    }

    #[test]
    fn from_provider_events_error_memory_record() {
        use shardline_provider_events::ProviderEventsError;
        let err: ServerError = ProviderEventsError::MemoryRecordStore(
            shardline_index::MemoryRecordStoreError::LockPoisoned("test".to_owned()),
        )
        .into();
        assert!(matches!(
            err,
            ServerError::Index(IndexError::MemoryRecord(_))
        ));
    }

    #[test]
    fn from_provider_events_error_postgres_metadata() {
        use shardline_provider_events::ProviderEventsError;
        let err: ServerError = ProviderEventsError::PostgresMetadata(
            shardline_index::PostgresMetadataStoreError::Json(
                serde_json::from_str::<()>("x").unwrap_err(),
            ),
        )
        .into();
        assert!(matches!(
            err,
            ServerError::Index(IndexError::PostgresMetadata(_))
        ));
    }

    #[test]
    fn from_provider_events_error_parse_stored_file_record() {
        use shardline_provider_events::ProviderEventsError;
        let err: ServerError = ProviderEventsError::ParseStoredFileRecord(
            shardline_server_core::ParseStoredFileRecordError::Json(
                serde_json::from_str::<()>("x").unwrap_err(),
            ),
        )
        .into();
        assert!(matches!(err, ServerError::Io(_)));
    }

    #[test]
    fn from_gc_error_io() {
        use shardline_gc::GcError;
        let err: ServerError = GcError::Io(std::io::Error::other("gc io")).into();
        assert!(matches!(err, ServerError::Io(_)));
    }

    #[test]
    fn from_gc_error_json() {
        use shardline_gc::GcError;
        let err: ServerError = GcError::Json(serde_json::from_str::<()>("x").unwrap_err()).into();
        assert!(matches!(err, ServerError::Json(_)));
    }

    #[test]
    fn from_gc_error_numeric_conversion() {
        use shardline_gc::GcError;
        let huge = 1_000_000_000_000u64;
        let try_err = i32::try_from(huge).unwrap_err();
        let err: ServerError = GcError::NumericConversion(try_err).into();
        assert!(matches!(err, ServerError::NumericConversion(_)));
    }

    #[test]
    fn from_gc_error_local_object_store() {
        use shardline_gc::GcError;
        let err: ServerError = GcError::LocalObjectStore(
            shardline_storage::LocalObjectStoreError::Io(std::io::Error::other("local")),
        )
        .into();
        assert!(matches!(
            err,
            ServerError::ObjectStore(ObjectStoreError::Local(_))
        ));
    }

    #[test]
    fn from_gc_error_s3_object_store() {
        use shardline_gc::GcError;
        let err: ServerError =
            GcError::S3ObjectStore(shardline_storage::S3ObjectStoreError::IncompleteCredentials)
                .into();
        assert!(matches!(
            err,
            ServerError::ObjectStore(ObjectStoreError::S3(_))
        ));
    }

    #[test]
    fn from_gc_error_object_prefix() {
        use shardline_gc::GcError;
        let err: ServerError =
            GcError::ObjectPrefix(shardline_storage::ObjectPrefixError::UnsafePath).into();
        assert!(matches!(
            err,
            ServerError::ObjectStore(ObjectStoreError::Prefix(_))
        ));
    }

    #[test]
    fn from_gc_error_index_store() {
        use shardline_gc::GcError;
        let io_err = std::io::Error::other("idx");
        let err: ServerError =
            GcError::IndexStore(shardline_index::LocalIndexStoreError::Io(io_err)).into();
        assert!(matches!(err, ServerError::Index(IndexError::Local(_))));
    }

    #[test]
    fn from_gc_error_memory_index() {
        use shardline_gc::GcError;
        let err: ServerError = GcError::MemoryIndexStore(
            shardline_index::MemoryIndexStoreError::LockPoisoned("test".to_owned()),
        )
        .into();
        assert!(matches!(
            err,
            ServerError::Index(IndexError::MemoryIndex(_))
        ));
    }

    #[test]
    fn from_gc_error_memory_record() {
        use shardline_gc::GcError;
        let err: ServerError =
            GcError::MemoryRecordStore(shardline_index::MemoryRecordStoreError::LockPoisoned(
                "test".to_owned(),
            ))
            .into();
        assert!(matches!(
            err,
            ServerError::Index(IndexError::MemoryRecord(_))
        ));
    }

    #[test]
    fn from_gc_error_postgres_metadata() {
        use shardline_gc::GcError;
        let err: ServerError =
            GcError::PostgresMetadata(shardline_index::PostgresMetadataStoreError::Json(
                serde_json::from_str::<()>("x").unwrap_err(),
            ))
            .into();
        assert!(matches!(
            err,
            ServerError::Index(IndexError::PostgresMetadata(_))
        ));
    }

    #[test]
    fn from_gc_error_retention_hold() {
        use shardline_gc::GcError;
        let err: ServerError =
            GcError::RetentionHold(shardline_index::RetentionHoldError::InvertedTimeline).into();
        assert!(matches!(
            err,
            ServerError::Index(IndexError::RetentionHold(_))
        ));
    }

    #[test]
    fn from_gc_error_quarantine_candidate() {
        use shardline_gc::GcError;
        let err: ServerError = GcError::QuarantineCandidate(
            shardline_index::QuarantineCandidateError::InvertedTimeline,
        )
        .into();
        assert!(matches!(
            err,
            ServerError::Index(IndexError::QuarantineCandidate(_))
        ));
    }

    #[test]
    fn from_gc_error_webhook_delivery() {
        use shardline_gc::GcError;
        let err: ServerError =
            GcError::WebhookDelivery(shardline_index::WebhookDeliveryError::EmptyRepositoryOwner)
                .into();
        assert!(matches!(
            err,
            ServerError::Index(IndexError::WebhookDelivery(_))
        ));
    }

    #[test]
    fn from_gc_error_file_record_invariant() {
        use shardline_gc::GcError;
        let err: ServerError = GcError::FileRecordInvariant(
            shardline_index::FileRecordInvariantError::NonContiguousChunkOffsets,
        )
        .into();
        assert!(matches!(
            err,
            ServerError::Index(IndexError::FileRecordInvariant(_))
        ));
    }

    #[test]
    fn from_gc_error_invalid_lifecycle_metadata() {
        use shardline_gc::GcError;
        let err: ServerError = GcError::InvalidLifecycleMetadata(
            shardline_server_core::InvalidLifecycleMetadataError::QuarantineCandidateDeleteBeforeFirstSeen {
                object_key: "test".into(),
                delete_after_unix_seconds: 10,
                first_seen_unreachable_at_unix_seconds: 20,
            },
        ).into();
        assert!(matches!(
            err,
            ServerError::Index(IndexError::InvalidLifecycleMetadata(_))
        ));
    }

    #[test]
    fn from_gc_error_invalid_content_hash() {
        use shardline_gc::GcError;
        let err: ServerError = GcError::InvalidContentHash.into();
        assert!(matches!(err, ServerError::InvalidContentHash));
    }

    #[test]
    fn from_gc_error_overflow() {
        use shardline_gc::GcError;
        let err: ServerError = GcError::Overflow.into();
        assert!(matches!(err, ServerError::Overflow));
    }

    #[test]
    fn from_invalid_reconstruction_response_error() {
        let err: ServerError = shardline_server_core::InvalidReconstructionResponseError::TermCountExceededRecordChunkCount.into();
        assert!(matches!(
            err,
            ServerError::Index(IndexError::InvalidReconstructionResponse(_))
        ));
    }

    #[test]
    fn from_parse_stored_file_record_error_json() {
        let err: ServerError = shardline_server_core::ParseStoredFileRecordError::Json(
            serde_json::from_str::<()>("x").unwrap_err(),
        )
        .into();
        assert!(matches!(err, ServerError::Json(_)));
    }

    #[test]
    fn oci_error_from_server_error_with_missing_auth_attaches_default_challenge() {
        use axum::http::HeaderValue;
        let oci = OciError(ServerError::MissingAuthorization);
        let response = oci.into_response();
        let www_auth = response.headers().get(axum::http::header::WWW_AUTHENTICATE);
        assert_eq!(
            www_auth,
            Some(&HeaderValue::from_static("Bearer realm=\"shardline\""))
        );
    }

    #[test]
    fn oci_error_not_found_has_no_www_auth() {
        let oci = OciError(ServerError::NotFound);
        let response = oci.into_response();
        assert!(
            response
                .headers()
                .get(axum::http::header::WWW_AUTHENTICATE)
                .is_none()
        );
    }

    // ---- Additional From implementation tests for uncovered variants ----

    #[test]
    fn from_xet_adapter_error_object_store() {
        let err: ServerError = crate::xet_adapter::XetAdapterError::ObjectStore(
            crate::object_store::ServerObjectStoreError::NotFound,
        )
        .into();
        assert!(matches!(err, ServerError::NotFound));
    }

    #[test]
    fn from_provider_events_error_xet_adapter_proxies() {
        use shardline_provider_events::ProviderEventsError;
        let err: ServerError =
            ProviderEventsError::XetAdapter(shardline_xet_adapter::XetAdapterError::NotFound)
                .into();
        assert!(matches!(err, ServerError::NotFound));
    }

    #[test]
    fn from_provider_events_error_object_store_proxies() {
        use shardline_provider_events::ProviderEventsError;
        let err: ServerError =
            ProviderEventsError::ObjectStore(crate::object_store::ServerObjectStoreError::Overflow)
                .into();
        assert!(matches!(err, ServerError::Overflow));
    }

    #[test]
    fn from_gc_error_object_store_proxies() {
        use shardline_gc::GcError;
        let err: ServerError =
            GcError::ObjectStore(crate::object_store::ServerObjectStoreError::NotFound).into();
        assert!(matches!(err, ServerError::NotFound));
    }

    #[test]
    fn from_gc_error_xet_adapter_proxies() {
        use shardline_gc::GcError;
        let err: ServerError =
            GcError::XetAdapter(shardline_xet_adapter::XetAdapterError::InvalidContentHash).into();
        assert!(matches!(err, ServerError::InvalidContentHash));
    }

    // ---- OciAdapterError From implementation tests ----

    #[test]
    fn from_oci_adapter_error_io() {
        let err: ServerError =
            shardline_oci_adapter::OciAdapterError::Io(std::io::Error::other("oci io")).into();
        assert!(matches!(err, ServerError::Io(_)));
    }

    #[test]
    fn from_oci_adapter_error_json() {
        let err: ServerError = shardline_oci_adapter::OciAdapterError::Json(
            serde_json::from_str::<()>("x").unwrap_err(),
        )
        .into();
        assert!(matches!(err, ServerError::Json(_)));
    }

    #[test]
    fn from_oci_adapter_error_numeric_conversion() {
        let huge = 1_000_000_000_000u64;
        let try_err = i32::try_from(huge).unwrap_err();
        let err: ServerError =
            shardline_oci_adapter::OciAdapterError::NumericConversion(try_err).into();
        assert!(matches!(err, ServerError::NumericConversion(_)));
    }

    #[test]
    fn from_oci_adapter_error_object_store_proxies() {
        let err: ServerError = shardline_oci_adapter::OciAdapterError::ObjectStore(
            crate::object_store::ServerObjectStoreError::Overflow,
        )
        .into();
        assert!(matches!(err, ServerError::Overflow));
    }

    #[test]
    fn from_oci_adapter_error_s3_object_store() {
        let err: ServerError = shardline_oci_adapter::OciAdapterError::S3ObjectStore(
            shardline_storage::S3ObjectStoreError::IncompleteCredentials,
        )
        .into();
        assert!(matches!(
            err,
            ServerError::ObjectStore(ObjectStoreError::S3(_))
        ));
    }

    #[test]
    fn from_oci_adapter_error_local_object_store() {
        let err: ServerError = shardline_oci_adapter::OciAdapterError::LocalObjectStore(
            shardline_storage::LocalObjectStoreError::Io(std::io::Error::other("local")),
        )
        .into();
        assert!(matches!(
            err,
            ServerError::ObjectStore(ObjectStoreError::Local(_))
        ));
    }

    #[test]
    fn from_oci_adapter_error_object_prefix() {
        let err: ServerError = shardline_oci_adapter::OciAdapterError::ObjectPrefix(
            shardline_storage::ObjectPrefixError::UnsafePath,
        )
        .into();
        assert!(matches!(
            err,
            ServerError::ObjectStore(ObjectStoreError::Prefix(_))
        ));
    }

    #[test]
    fn from_oci_adapter_error_not_found() {
        let err: ServerError = shardline_oci_adapter::OciAdapterError::NotFound.into();
        assert!(matches!(err, ServerError::NotFound));
    }

    #[test]
    fn from_oci_adapter_error_overflow() {
        let err: ServerError = shardline_oci_adapter::OciAdapterError::Overflow.into();
        assert!(matches!(err, ServerError::Overflow));
    }

    #[test]
    fn from_oci_adapter_error_invalid_content_hash() {
        let err: ServerError = shardline_oci_adapter::OciAdapterError::InvalidContentHash.into();
        assert!(matches!(err, ServerError::InvalidContentHash));
    }

    #[test]
    fn from_oci_adapter_error_invalid_repository_name() {
        let err: ServerError = shardline_oci_adapter::OciAdapterError::InvalidRepositoryName.into();
        assert!(matches!(err, ServerError::InvalidRepositoryName));
    }

    #[test]
    fn from_oci_adapter_error_invalid_manifest_reference() {
        let err: ServerError =
            shardline_oci_adapter::OciAdapterError::InvalidManifestReference.into();
        assert!(matches!(err, ServerError::InvalidManifestReference));
    }

    #[test]
    fn from_oci_adapter_error_invalid_upload_session() {
        let err: ServerError = shardline_oci_adapter::OciAdapterError::InvalidUploadSession.into();
        assert!(matches!(err, ServerError::InvalidUploadSession));
    }

    #[test]
    fn from_oci_adapter_error_too_many_upload_sessions() {
        let err: ServerError = shardline_oci_adapter::OciAdapterError::TooManyUploadSessions.into();
        assert!(matches!(err, ServerError::TooManyUploadSessions));
    }

    #[test]
    fn from_oci_adapter_error_expected_body_hash_mismatch() {
        let err: ServerError =
            shardline_oci_adapter::OciAdapterError::ExpectedBodyHashMismatch.into();
        assert!(matches!(err, ServerError::ExpectedBodyHashMismatch));
    }

    #[allow(clippy::panic)]
    #[test]
    fn from_oci_adapter_error_blocking_task() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let task = rt.spawn(async { panic!("oci task panic") });
        let join_err = rt.block_on(async { task.await.unwrap_err() });
        let err: ServerError =
            shardline_oci_adapter::OciAdapterError::BlockingTask(join_err).into();
        assert!(matches!(err, ServerError::BlockingTask(_)));
    }
}
