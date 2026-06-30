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
    /// Object-storage adapter access failed.
    #[error("object storage adapter operation failed")]
    ObjectStore(#[from] LocalObjectStoreError),
    /// S3-compatible object-storage adapter access failed.
    #[error("s3 object storage adapter operation failed")]
    S3ObjectStore(#[from] S3ObjectStoreError),
    /// Object inventory prefix validation failed.
    #[error("object storage prefix validation failed")]
    ObjectPrefix(#[from] ObjectPrefixError),
    /// S3-compatible object storage was selected without concrete configuration.
    #[error("s3 object storage configuration is missing")]
    MissingS3ObjectStoreConfig,
    /// Index adapter access failed.
    #[error("index adapter operation failed")]
    IndexStore(#[from] LocalIndexStoreError),
    /// In-memory index adapter access failed.
    #[error("memory index adapter operation failed")]
    MemoryIndexStore(#[from] MemoryIndexStoreError),
    /// In-memory record adapter access failed.
    #[error("memory record adapter operation failed")]
    MemoryRecordStore(#[from] MemoryRecordStoreError),
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
    /// Lifecycle metadata was internally inconsistent for a mutating operator workflow.
    #[error("lifecycle metadata was internally inconsistent")]
    InvalidLifecycleMetadata(#[from] InvalidLifecycleMetadataError),
    /// A file identifier was unsafe.
    #[error(
        "file identifier must be relative and must not contain traversal or control characters"
    )]
    InvalidFileId,
    /// A required metadata table was missing from the configured backend.
    #[error("required metadata table is missing: {0}")]
    MissingRequiredMetadataTable(String),
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
    /// A blocking worker task failed before it could finish storage work.
    #[error("blocking worker task failed")]
    BlockingTask(#[source] JoinError),
    /// Stored object metadata disagreed with the expected transfer length.
    #[error("stored object length did not match indexed metadata")]
    StoredObjectLengthMismatch,
    /// Storage migration found a content-addressed source object under the wrong key.
    #[error(
        "storage migration source object hash mismatch for key {key}: expected {expected_hash}, observed {observed_hash}"
    )]
    StorageMigrationSourceHashMismatch {
        /// Content-addressed object key being migrated.
        key: String,
        /// Hash implied by the object key.
        expected_hash: String,
        /// Hash computed from the source object bytes.
        observed_hash: String,
    },
    /// Repository rename encountered conflicting target-scope metadata.
    #[error("repository rename target already contains conflicting metadata")]
    ConflictingRenameTargetRecord,
    /// A reconstruction response violated an internal protocol-shape invariant.
    #[error("reconstruction response invariant failed: {0}")]
    InvalidReconstructionResponse(#[from] InvalidReconstructionResponseError),
}

impl ServerError {
    const fn status_code(&self) -> StatusCode {
        match self {
            Self::InvalidFileId
            | Self::InvalidContentHash
            | Self::InvalidDigest
            | Self::InvalidRepositoryName
            | Self::InvalidManifestReference
            | Self::InvalidUploadSession
            | Self::InvalidXorbPrefix
            | Self::HashParse(_)
            | Self::ObjectPrefix(_) => StatusCode::BAD_REQUEST,
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
            Self::MissingRequiredMetadataTable(_) | Self::MissingS3ObjectStoreConfig => {
                StatusCode::INTERNAL_SERVER_ERROR
            }
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
            Self::TransferLimiterClosed => StatusCode::SERVICE_UNAVAILABLE,
            Self::Io(_)
            | Self::Json(_)
            | Self::NumericConversion(_)
            | Self::ObjectStore(_)
            | Self::S3ObjectStore(_)
            | Self::IndexStore(_)
            | Self::MemoryIndexStore(_)
            | Self::MemoryRecordStore(_)
            | Self::PostgresMetadata(_)
            | Self::RetentionHold(_)
            | Self::QuarantineCandidate(_)
            | Self::WebhookDelivery(_)
            | Self::FileRecordInvariant(_)
            | Self::StoredFileMetadataTooLarge { .. }
            | Self::StoredFileMetadataLengthMismatch
            | Self::InvalidLifecycleMetadata(_)
            | Self::Config(_)
            | Self::Overflow
            | Self::MissingReconstructionCacheRedisUrl
            | Self::ReconstructionCache(_)
            | Self::BlockingTask(_)
            | Self::StoredObjectLengthMismatch
            | Self::StorageMigrationSourceHashMismatch { .. }
            | Self::ConflictingRenameTargetRecord
            | Self::InvalidReconstructionResponse(_)
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
            | HttpRangeParseError::InvalidSyntax
            | HttpRangeParseError::InvalidNumber => Self::InvalidRangeHeader,
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
            XetAdapterError::LocalObjectStore(e) => Self::ObjectStore(e),
            XetAdapterError::S3ObjectStore(e) => Self::S3ObjectStore(e),
            XetAdapterError::IndexStore(e) => Self::IndexStore(e),
            XetAdapterError::MemoryIndexStore(e) => Self::MemoryIndexStore(e),
            XetAdapterError::MemoryRecordStore(e) => Self::MemoryRecordStore(e),
            XetAdapterError::PostgresMetadata(e) => Self::PostgresMetadata(e),
            XetAdapterError::FileRecordInvariant(e) => Self::FileRecordInvariant(e),
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
                Self::ConflictingRenameTargetRecord
            }
            ProviderEventsError::Json(e) => Self::Json(e),
            ProviderEventsError::NumericConversion(e) => Self::NumericConversion(e),
            ProviderEventsError::RetentionHold(e) => Self::RetentionHold(e),
            ProviderEventsError::XetAdapter(e) => Self::from(e),
            ProviderEventsError::IndexStore(e) => Self::IndexStore(e),
            ProviderEventsError::MemoryIndexStore(e) => Self::MemoryIndexStore(e),
            ProviderEventsError::MemoryRecordStore(e) => Self::MemoryRecordStore(e),
            ProviderEventsError::PostgresMetadata(e) => Self::PostgresMetadata(e),
            ProviderEventsError::WebhookDelivery(e) => Self::WebhookDelivery(e),
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
            GcError::LocalObjectStore(e) => Self::ObjectStore(e),
            GcError::S3ObjectStore(e) => Self::S3ObjectStore(e),
            GcError::ObjectPrefix(e) => Self::ObjectPrefix(e),
            GcError::IndexStore(e) => Self::IndexStore(e),
            GcError::MemoryIndexStore(e) => Self::MemoryIndexStore(e),
            GcError::MemoryRecordStore(e) => Self::MemoryRecordStore(e),
            GcError::PostgresMetadata(e) => Self::PostgresMetadata(e),
            GcError::RetentionHold(e) => Self::RetentionHold(e),
            GcError::QuarantineCandidate(e) => Self::QuarantineCandidate(e),
            GcError::WebhookDelivery(e) => Self::WebhookDelivery(e),
            GcError::FileRecordInvariant(e) => Self::FileRecordInvariant(e),
            GcError::InvalidLifecycleMetadata(e) => Self::InvalidLifecycleMetadata(e),
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
            OciAdapterError::S3ObjectStore(e) => Self::S3ObjectStore(e),
            OciAdapterError::LocalObjectStore(e) => Self::ObjectStore(e),
            OciAdapterError::ObjectPrefix(e) => Self::ObjectPrefix(e),
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
            shardline_protocol_adapters::ProtocolError::InvalidContentHash => Self::InvalidContentHash,
        }
    }
}

#[derive(Debug, Serialize)]
struct ErrorBody {
    error: String,
}
