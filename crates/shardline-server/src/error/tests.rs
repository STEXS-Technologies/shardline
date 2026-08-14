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
    let err =
        IndexError::WebhookDelivery(shardline_index::WebhookDeliveryError::EmptyRepositoryOwner);
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
    let err: ServerError = HttpRangeParseError::InvalidSyntax("test".to_owned()).into();
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
    let err: ServerError = shardline_protocol_adapters::ProtocolError::InvalidContentHash.into();
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

#[test]
fn into_response_saturated_includes_retry_guidance() {
    let response = ServerError::WorkQueueSaturated.into_response();
    assert_eq!(
        response.status(),
        axum::http::StatusCode::SERVICE_UNAVAILABLE
    );
    assert_eq!(
        response.headers().get(axum::http::header::RETRY_AFTER),
        Some(&axum::http::HeaderValue::from_static("1"))
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
    let err: ServerError = crate::xet_adapter::XetAdapterError::NumericConversion(try_err).into();
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
    let err: ServerError =
        ProviderEventsError::RetentionHold(shardline_index::RetentionHoldError::InvertedTimeline)
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
        ProviderEventsError::IndexStore(shardline_index::LocalIndexStoreError::Io(io_err)).into();
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
    let err: ServerError =
        ProviderEventsError::PostgresMetadata(shardline_index::PostgresMetadataStoreError::Json(
            serde_json::from_str::<()>("x").unwrap_err(),
        ))
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
    let err: ServerError = GcError::LocalObjectStore(shardline_storage::LocalObjectStoreError::Io(
        std::io::Error::other("local"),
    ))
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
        GcError::S3ObjectStore(shardline_storage::S3ObjectStoreError::IncompleteCredentials).into();
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
    let err: ServerError = GcError::MemoryRecordStore(
        shardline_index::MemoryRecordStoreError::LockPoisoned("test".to_owned()),
    )
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
    let err: ServerError =
        GcError::QuarantineCandidate(shardline_index::QuarantineCandidateError::InvertedTimeline)
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
        ProviderEventsError::XetAdapter(shardline_xet_adapter::XetAdapterError::NotFound).into();
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
    let err: ServerError =
        shardline_oci_adapter::OciAdapterError::Json(serde_json::from_str::<()>("x").unwrap_err())
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
    let err: ServerError = shardline_oci_adapter::OciAdapterError::InvalidManifestReference.into();
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
    let err: ServerError = shardline_oci_adapter::OciAdapterError::ExpectedBodyHashMismatch.into();
    assert!(matches!(err, ServerError::ExpectedBodyHashMismatch));
}

#[allow(clippy::panic)]
#[test]
fn from_oci_adapter_error_blocking_task() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let task = rt.spawn(async { panic!("oci task panic") });
    let join_err = rt.block_on(async { task.await.unwrap_err() });
    let err: ServerError = shardline_oci_adapter::OciAdapterError::BlockingTask(join_err).into();
    assert!(matches!(err, ServerError::BlockingTask(_)));
}

// ── ServerError → S3Error mapping ──────────────────────────────────────────

use shardline_s3_adapter::{S3Error, S3ErrorClass, S3ErrorClassify};

#[test]
fn s3_classify_range_not_satisfiable_maps_to_invalid_range() {
    assert_eq!(
        ServerError::RangeNotSatisfiable.s3_class(),
        S3ErrorClass::RangeNotSatisfiable
    );
    let error = S3Error::from(ServerError::RangeNotSatisfiable);
    assert_eq!(error.code, "InvalidRange");
    assert_eq!(error.status, StatusCode::RANGE_NOT_SATISFIABLE);
}

#[test]
fn s3_classify_not_found_maps_to_no_such_key() {
    assert_eq!(ServerError::NotFound.s3_class(), S3ErrorClass::NotFound);
    let error = S3Error::from(ServerError::NotFound);
    assert_eq!(error.code, "NoSuchKey");
    assert_eq!(error.status, StatusCode::NOT_FOUND);
}

#[test]
fn s3_classify_authz_failures_map_to_access_denied() {
    for server_error in [
        ServerError::MissingAuthorization,
        ServerError::InvalidAuthorizationHeader,
        ServerError::InvalidToken(shardline_protocol::TokenCodecError::InvalidFormat),
        ServerError::InsufficientScope,
        ServerError::ProviderDenied,
    ] {
        assert_eq!(
            server_error.s3_class(),
            S3ErrorClass::AccessDenied,
            "variant must classify as AccessDenied"
        );
        let error = S3Error::from(server_error);
        assert_eq!(error.code, "AccessDenied");
        assert_eq!(error.status, StatusCode::FORBIDDEN);
    }
}

#[test]
fn s3_classify_io_error_maps_to_internal() {
    let server_error = ServerError::Io(std::io::Error::other("boom"));
    assert_eq!(server_error.s3_class(), S3ErrorClass::Internal);
    let error = S3Error::from(server_error);
    assert_eq!(error.code, "InternalError");
    assert_eq!(error.status, StatusCode::INTERNAL_SERVER_ERROR);
}

#[test]
fn s3_classify_body_too_large_maps_to_internal() {
    // The S3 handlers translate oversized bodies to a 413 EntityTooLarge
    // directly; through the generic mapping it is an internal error.
    let error = S3Error::from(ServerError::RequestBodyTooLarge);
    assert_eq!(error.code, "InternalError");
    assert_eq!(error.status, StatusCode::INTERNAL_SERVER_ERROR);
}
