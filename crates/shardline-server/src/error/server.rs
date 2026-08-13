use std::{
    io::{Error as IoError, ErrorKind},
    num::TryFromIntError,
};

use axum::{
    Error as AxumError, Json,
    http::{
        HeaderValue, StatusCode,
        header::{RETRY_AFTER, WWW_AUTHENTICATE},
    },
    response::{IntoResponse, Response},
};
use serde_json::Error as JsonError;
use shardline_cache::ReconstructionCacheError;
use shardline_cas::CasError;
use shardline_gc::GcError;
use shardline_index::{
    FileRecordInvariantError, LocalIndexStoreError, MemoryIndexStoreError, MemoryRecordStoreError,
    PostgresMetadataStoreError, QuarantineCandidateError, RetentionHoldError, WebhookDeliveryError,
};
use shardline_oci_adapter::OciAdapterError;
use shardline_protocol::{HashParseError, HttpRangeParseError, TokenCodecError};
use shardline_protocol_adapters::ProtocolError;
use shardline_provider_events::ProviderEventsError;
use shardline_server_core::{
    InvalidLifecycleMetadataError, InvalidReconstructionResponseError, InvalidSerializedShardError,
    ParseStoredFileRecordError,
};
use shardline_storage::{LocalObjectStoreError, ObjectPrefixError, S3ObjectStoreError};
use shardline_xet_adapter::XetAdapterError;
use thiserror::Error;
use tokio::task::JoinError;

use super::body::ErrorBody;
use super::{IndexError, ObjectStoreError};

use crate::{
    config::ServerConfigError, provider::ProviderServiceError, xet_adapter::XorbParseError,
};

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
    /// A durable upload intent could not advance through its lifecycle.
    #[error("upload intent state conflict")]
    UploadIntentConflict,
    /// Redis reconstruction cache was selected without a URL.
    #[error("redis reconstruction cache requires a redis url")]
    MissingReconstructionCacheRedisUrl,
    /// The transfer concurrency limiter was unexpectedly unavailable.
    #[error("transfer concurrency limiter is unavailable")]
    TransferLimiterClosed,
    /// The transfer concurrency limiter timed out waiting for capacity.
    #[error("transfer concurrency limiter timed out")]
    TransferLimiterTimedOut,
    /// A bounded execution pool had no remaining capacity.
    #[error("server work queue is saturated")]
    WorkQueueSaturated,
    /// A request exceeded the server's total execution deadline.
    #[error("request exceeded the server execution deadline")]
    RequestTimedOut,
    /// A blocking worker task failed before it could finish storage work.
    #[error("blocking worker task failed")]
    BlockingTask(#[source] JoinError),
    /// A path, prefix, or revision string was malformed.
    #[error("invalid path or revision")]
    InvalidPath,
    /// The referenced file has no record in the revision scope.
    #[error("file is not registered in revision {0}")]
    UnregisteredFile(String),
    /// The revision already exists.
    #[error("revision already exists")]
    RevisionConflict,
}

impl ServerError {
    /// Maps this error to the OCI Distribution specification error code.
    ///
    /// When adding a new [`ServerError`] variant, add it explicitly to this match.
    /// If the variant has an OCI-specific code, map it above the catch-all.
    /// If it is an internal error (no OCI-specific meaning), keep it in the catch-all.
    pub(crate) const fn oci_error_code(&self) -> &'static str {
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
            | Self::UploadIntentConflict
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
            | Self::WorkQueueSaturated
            | Self::RequestTimedOut
            | Self::BlockingTask(_)
            | Self::InvalidPath
            | Self::UnregisteredFile(_)
            | Self::RevisionConflict => "INTERNAL",
        }
    }

    pub(crate) const fn status_code(&self) -> StatusCode {
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
            Self::UploadIntentConflict => StatusCode::CONFLICT,
            Self::InvalidPath | Self::UnregisteredFile(_) => StatusCode::BAD_REQUEST,
            Self::RevisionConflict => StatusCode::CONFLICT,
            Self::TransferLimiterClosed
            | Self::TransferLimiterTimedOut
            | Self::WorkQueueSaturated
            | Self::RequestTimedOut => StatusCode::SERVICE_UNAVAILABLE,
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

impl From<CasError> for ServerError {
    fn from(value: CasError) -> Self {
        match value {
            CasError::BodyTooLarge { .. } => Self::RequestBodyTooLarge,
            CasError::InvalidUploadTransition => Self::UploadIntentConflict,
            CasError::Overflow => Self::Overflow,
            CasError::ObjectStore(message)
            | CasError::Index(message)
            | CasError::Record(message)
            | CasError::Internal(message) => Self::Io(IoError::other(message)),
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
        if matches!(
            self,
            Self::TransferLimiterTimedOut | Self::WorkQueueSaturated | Self::RequestTimedOut
        ) {
            response
                .headers_mut()
                .insert(RETRY_AFTER, HeaderValue::from_static("1"));
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
            ProviderEventsError::ParseStoredFileRecord(e) => {
                Self::Io(IoError::new(ErrorKind::InvalidData, e.to_string()))
            }
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

impl From<ProtocolError> for ServerError {
    fn from(error: ProtocolError) -> Self {
        match error {
            ProtocolError::InvalidContentHash => Self::InvalidContentHash,
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

/// Classifies a [`ServerError`] into the S3 error envelope's coarse classes.
///
/// Lives in the server crate (not the adapter) so the S3 adapter never depends
/// on `shardline-server`. The generic `From<E: S3ErrorClassify> for S3Error`
/// in the adapter then produces the XML `<Error>` response:
///
/// - `RangeNotSatisfiable` → `416 InvalidRange`
/// - `NotFound` → `404 NoSuchKey`
/// - authorization failures → `403 AccessDenied`
/// - everything else → `500 InternalError`
impl shardline_s3_adapter::S3ErrorClassify for ServerError {
    fn s3_class(&self) -> shardline_s3_adapter::S3ErrorClass {
        use shardline_s3_adapter::S3ErrorClass;
        match self {
            Self::RangeNotSatisfiable => S3ErrorClass::RangeNotSatisfiable,
            Self::NotFound => S3ErrorClass::NotFound,
            Self::MissingAuthorization
            | Self::InvalidAuthorizationHeader
            | Self::InvalidToken(_)
            | Self::InsufficientScope
            | Self::ProviderDenied => S3ErrorClass::AccessDenied,
            // All remaining variants are internal server errors with no S3
            // meaning. Add new S3-mappable variants above this catch-all.
            Self::Io(_)
            | Self::Json(_)
            | Self::RequestBodyRead(_)
            | Self::RequestBodyTooLarge
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
            | Self::UploadIntentConflict
            | Self::Overflow
            | Self::InvalidRangeHeader
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
            | Self::Provider(_)
            | Self::ReconstructionCache(_)
            | Self::Config(_)
            | Self::ExpectedBodyHashMismatch
            | Self::InvalidDigest
            | Self::InvalidRepositoryName
            | Self::InvalidManifestReference
            | Self::NotAcceptable
            | Self::UnauthorizedChallenge(_)
            | Self::InvalidUploadSession
            | Self::TooManyUploadSessions
            | Self::TooManyRegistryTokenRequests
            | Self::MissingReconstructionCacheRedisUrl
            | Self::TransferLimiterClosed
            | Self::TransferLimiterTimedOut
            | Self::WorkQueueSaturated
            | Self::RequestTimedOut
            | Self::BlockingTask(_)
            | Self::InvalidPath
            | Self::UnregisteredFile(_)
            | Self::RevisionConflict => S3ErrorClass::Internal,
        }
    }
}
