use std::{
    env::var,
    io::Error as IoError,
    num::{NonZeroU64, NonZeroUsize, ParseIntError},
    path::{Path, PathBuf},
};

use shardline_protocol::{SecretBytes, SecretString};

use super::file::ShardlineTomlConfig;
use super::secrets::{
    configure_provider_runtime_from_paths, ensure_secret_size_within_limit,
    load_redis_tls_config_from_env, load_s3_object_store_config_from_env, read_secret_file_bytes,
};
use super::{
    AuthProviderKind, CONFIG_SECRET_KEY_BYTES, DEFAULT_LFS_PATCH_MAX_ACTIVE_SESSIONS,
    DEFAULT_LFS_PATCH_MAX_SEEK_AHEAD_BYTES, DEFAULT_LFS_PATCH_TOTAL_MAX_BYTES,
    DEFAULT_LFS_PATCH_TTL_SECONDS, DEFAULT_MAX_REQUEST_BODY_BYTES, DEFAULT_MAX_REVISIONS_PER_REPO,
    DEFAULT_MAX_SHARD_FILES, DEFAULT_MAX_SHARD_RECONSTRUCTION_TERMS, DEFAULT_MAX_SHARD_XORB_CHUNKS,
    DEFAULT_MAX_SHARD_XORBS, DEFAULT_S3_MAX_PART_BYTES, DEFAULT_S3_MIN_PART_BYTES,
    DEFAULT_S3_UPLOAD_MAX_ACTIVE_PART_FILES, DEFAULT_S3_UPLOAD_MAX_ACTIVE_SESSIONS,
    DEFAULT_S3_UPLOAD_SESSION_MAX_BYTES, DEFAULT_S3_UPLOAD_SESSION_TTL_SECONDS,
    DEFAULT_S3_UPLOAD_TOTAL_MAX_BYTES, DeploymentMode, HUB_WEBHOOK_SECRET_KEY_BYTES,
    MAX_ED25519_KEY_BYTES, MAX_METRICS_TOKEN_BYTES, MAX_TOKEN_SIGNING_KEY_BYTES,
    ObjectStorageAdapter, ServerConfig, ServerConfigError, ShardMetadataLimits,
    default_transfer_max_in_flight_chunks, default_upload_max_in_flight_chunks, parse_byte_size,
};
use crate::{
    reconstruction_cache::ReconstructionCacheAdapter, server_frontend::ServerFrontend,
    server_role::ServerRole,
};

pub(super) fn load_server_config_from_env() -> Result<ServerConfig, ServerConfigError> {
    let bind_addr = match var("SHARDLINE_BIND_ADDR") {
        Ok(value) => value.parse()?,
        Err(_error) => "0.0.0.0:8080".parse()?,
    };
    let public_base_url = var("SHARDLINE_PUBLIC_BASE_URL")
        .unwrap_or_else(|_error| "http://127.0.0.1:8080".to_owned());
    // Validate public_base_url is a valid URL before consuming it.
    if url::Url::parse(&public_base_url).is_err() {
        return Err(ServerConfigError::InvalidPublicBaseUrl(public_base_url));
    }
    let server_role =
        ServerRole::parse(&var("SHARDLINE_SERVER_ROLE").unwrap_or_else(|_error| "all".to_owned()))
            .map_err(|_error| ServerConfigError::InvalidServerRole)?;
    let server_frontends = parse_server_frontends_env(
        &var("SHARDLINE_SERVER_FRONTENDS").unwrap_or_else(|_error| "xet".to_owned()),
    )?;
    let root_dir = var("SHARDLINE_ROOT_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_error| PathBuf::from("/var/lib/shardline"));
    let object_storage_adapter = ObjectStorageAdapter::parse(
        &var("SHARDLINE_OBJECT_STORAGE_ADAPTER").unwrap_or_else(|_error| "local".to_owned()),
    )?;
    let s3_object_store_config = match object_storage_adapter {
        ObjectStorageAdapter::Local => None,
        ObjectStorageAdapter::S3 => Some(load_s3_object_store_config_from_env()?),
    };
    let raw_max_request_body_bytes = var("SHARDLINE_MAX_REQUEST_BODY_BYTES")
        .unwrap_or_else(|_error| DEFAULT_MAX_REQUEST_BODY_BYTES.get().to_string())
        .parse::<usize>()
        .map_err(ServerConfigError::MaxRequestBodyBytes)?;
    let Some(max_request_body_bytes) = NonZeroUsize::new(raw_max_request_body_bytes) else {
        return Err(ServerConfigError::ZeroMaxRequestBodyBytes);
    };
    let shard_metadata_limits = ShardMetadataLimits::new(
        load_non_zero_usize_env(
            "SHARDLINE_MAX_SHARD_FILES",
            DEFAULT_MAX_SHARD_FILES,
            ServerConfigError::MaxShardFiles,
            || ServerConfigError::ZeroMaxShardFiles,
        )?,
        load_non_zero_usize_env(
            "SHARDLINE_MAX_SHARD_XORBS",
            DEFAULT_MAX_SHARD_XORBS,
            ServerConfigError::MaxShardXorbs,
            || ServerConfigError::ZeroMaxShardXorbs,
        )?,
        load_non_zero_usize_env(
            "SHARDLINE_MAX_SHARD_RECONSTRUCTION_TERMS",
            DEFAULT_MAX_SHARD_RECONSTRUCTION_TERMS,
            ServerConfigError::MaxShardReconstructionTerms,
            || ServerConfigError::ZeroMaxShardReconstructionTerms,
        )?,
        load_non_zero_usize_env(
            "SHARDLINE_MAX_SHARD_XORB_CHUNKS",
            DEFAULT_MAX_SHARD_XORB_CHUNKS,
            ServerConfigError::MaxShardXorbChunks,
            || ServerConfigError::ZeroMaxShardXorbChunks,
        )?,
    );
    let max_revisions_per_repo = load_non_zero_usize_env(
        "SHARDLINE_MAX_REVISIONS_PER_REPO",
        DEFAULT_MAX_REVISIONS_PER_REPO,
        ServerConfigError::MaxRevisionsPerRepo,
        || ServerConfigError::ZeroMaxRevisionsPerRepo,
    )?;
    let raw_chunk_size_str = var("SHARDLINE_CHUNK_SIZE")
        .or_else(|_| var("SHARDLINE_CHUNK_SIZE_BYTES"))
        .unwrap_or_else(|_error| "64KiB".to_owned());
    let raw_chunk_size = parse_byte_size(&raw_chunk_size_str)?;
    let Some(chunk_size) = NonZeroUsize::new(raw_chunk_size) else {
        return Err(ServerConfigError::ZeroChunkSize);
    };
    let raw_upload_max_in_flight_chunks = var("SHARDLINE_UPLOAD_MAX_IN_FLIGHT_CHUNKS")
        .unwrap_or_else(|_error| default_upload_max_in_flight_chunks().get().to_string())
        .parse::<usize>()
        .map_err(ServerConfigError::UploadMaxInFlightChunks)?;
    let raw_upload_max_in_flight_chunks = raw_upload_max_in_flight_chunks.min(1_000_000);
    let Some(upload_max_in_flight_chunks) = NonZeroUsize::new(raw_upload_max_in_flight_chunks)
    else {
        return Err(ServerConfigError::ZeroUploadMaxInFlightChunks);
    };
    let raw_transfer_max_in_flight_chunks = var("SHARDLINE_TRANSFER_MAX_IN_FLIGHT_CHUNKS")
        .unwrap_or_else(|_error| default_transfer_max_in_flight_chunks().get().to_string())
        .parse::<usize>()
        .map_err(ServerConfigError::TransferMaxInFlightChunks)?;
    let raw_transfer_max_in_flight_chunks = raw_transfer_max_in_flight_chunks.min(1_000_000);
    let Some(transfer_max_in_flight_chunks) = NonZeroUsize::new(raw_transfer_max_in_flight_chunks)
    else {
        return Err(ServerConfigError::ZeroTransferMaxInFlightChunks);
    };
    let reconstruction_cache_adapter = ReconstructionCacheAdapter::parse(
        &var("SHARDLINE_RECONSTRUCTION_CACHE_ADAPTER").unwrap_or_else(|_error| "memory".to_owned()),
    )?;
    let raw_reconstruction_cache_ttl_seconds = var("SHARDLINE_RECONSTRUCTION_CACHE_TTL_SECONDS")
        .unwrap_or_else(|_error| "30".to_owned())
        .parse::<u64>()
        .map_err(ServerConfigError::ReconstructionCacheTtl)?;
    let Some(reconstruction_cache_ttl_seconds) =
        NonZeroU64::new(raw_reconstruction_cache_ttl_seconds)
    else {
        return Err(ServerConfigError::ZeroReconstructionCacheTtlSeconds);
    };
    let raw_reconstruction_cache_memory_max_entries =
        var("SHARDLINE_RECONSTRUCTION_CACHE_MEMORY_MAX_ENTRIES")
            .unwrap_or_else(|_error| "4096".to_owned())
            .parse::<usize>()
            .map_err(ServerConfigError::ReconstructionCacheMemoryMaxEntries)?;
    let Some(reconstruction_cache_memory_max_entries) =
        NonZeroUsize::new(raw_reconstruction_cache_memory_max_entries)
    else {
        return Err(ServerConfigError::ZeroReconstructionCacheMemoryMaxEntries);
    };
    let raw_oci_upload_session_ttl_seconds = var("SHARDLINE_OCI_UPLOAD_SESSION_TTL_SECONDS")
        .unwrap_or_else(|_error| "3600".to_owned())
        .parse::<u64>()
        .map_err(ServerConfigError::OciUploadSessionTtl)?;
    let Some(oci_upload_session_ttl_seconds) = NonZeroU64::new(raw_oci_upload_session_ttl_seconds)
    else {
        return Err(ServerConfigError::ZeroOciUploadSessionTtlSeconds);
    };
    let raw_oci_upload_max_active_sessions = var("SHARDLINE_OCI_UPLOAD_MAX_ACTIVE_SESSIONS")
        .unwrap_or_else(|_error| "1024".to_owned())
        .parse::<usize>()
        .map_err(ServerConfigError::OciUploadMaxActiveSessions)?;
    let Some(oci_upload_max_active_sessions) =
        NonZeroUsize::new(raw_oci_upload_max_active_sessions)
    else {
        return Err(ServerConfigError::ZeroOciUploadMaxActiveSessions);
    };
    let raw_oci_registry_token_ttl_seconds = var("SHARDLINE_OCI_REGISTRY_TOKEN_TTL_SECONDS")
        .unwrap_or_else(|_error| "300".to_owned())
        .parse::<u64>()
        .map_err(ServerConfigError::OciRegistryTokenTtl)?;
    let Some(oci_registry_token_ttl_seconds) = NonZeroU64::new(raw_oci_registry_token_ttl_seconds)
    else {
        return Err(ServerConfigError::ZeroOciRegistryTokenTtlSeconds);
    };
    let raw_oci_registry_token_max_in_flight_requests =
        var("SHARDLINE_OCI_REGISTRY_TOKEN_MAX_IN_FLIGHT_REQUESTS")
            .unwrap_or_else(|_error| "64".to_owned())
            .parse::<usize>()
            .map_err(ServerConfigError::OciRegistryTokenMaxInFlightRequests)?;
    let raw_oci_registry_token_max_in_flight_requests =
        raw_oci_registry_token_max_in_flight_requests.min(1_000_000);
    let Some(oci_registry_token_max_in_flight_requests) =
        NonZeroUsize::new(raw_oci_registry_token_max_in_flight_requests)
    else {
        return Err(ServerConfigError::ZeroOciRegistryTokenMaxInFlightRequests);
    };
    let raw_s3_max_part_bytes = var("SHARDLINE_S3_MAX_PART_BYTES")
        .unwrap_or_else(|_error| DEFAULT_S3_MAX_PART_BYTES.get().to_string())
        .parse::<u64>()
        .map_err(ServerConfigError::S3MaxPartBytes)?;
    let Some(s3_max_part_bytes) = NonZeroU64::new(raw_s3_max_part_bytes) else {
        return Err(ServerConfigError::ZeroS3MaxPartBytes);
    };
    let raw_s3_upload_session_ttl_seconds = var("SHARDLINE_S3_UPLOAD_SESSION_TTL_SECONDS")
        .unwrap_or_else(|_error| DEFAULT_S3_UPLOAD_SESSION_TTL_SECONDS.get().to_string())
        .parse::<u64>()
        .map_err(ServerConfigError::S3UploadSessionTtl)?;
    let Some(s3_upload_session_ttl_seconds) = NonZeroU64::new(raw_s3_upload_session_ttl_seconds)
    else {
        return Err(ServerConfigError::ZeroS3UploadSessionTtlSeconds);
    };
    let raw_s3_upload_max_active_sessions = var("SHARDLINE_S3_UPLOAD_MAX_ACTIVE_SESSIONS")
        .unwrap_or_else(|_error| DEFAULT_S3_UPLOAD_MAX_ACTIVE_SESSIONS.get().to_string())
        .parse::<usize>()
        .map_err(ServerConfigError::S3UploadMaxActiveSessions)?;
    let Some(s3_upload_max_active_sessions) = NonZeroUsize::new(raw_s3_upload_max_active_sessions)
    else {
        return Err(ServerConfigError::ZeroS3UploadMaxActiveSessions);
    };
    let raw_s3_min_part_bytes = var("SHARDLINE_S3_MIN_PART_BYTES")
        .unwrap_or_else(|_error| DEFAULT_S3_MIN_PART_BYTES.get().to_string())
        .parse::<u64>()
        .map_err(ServerConfigError::S3MinPartBytes)?;
    let Some(s3_min_part_bytes) = NonZeroU64::new(raw_s3_min_part_bytes) else {
        return Err(ServerConfigError::ZeroS3MinPartBytes);
    };
    let raw_s3_upload_session_max_bytes = var("SHARDLINE_S3_UPLOAD_SESSION_MAX_BYTES")
        .unwrap_or_else(|_error| DEFAULT_S3_UPLOAD_SESSION_MAX_BYTES.get().to_string())
        .parse::<u64>()
        .map_err(ServerConfigError::S3UploadSessionMaxBytes)?;
    let Some(s3_upload_session_max_bytes) = NonZeroU64::new(raw_s3_upload_session_max_bytes) else {
        return Err(ServerConfigError::ZeroS3UploadSessionMaxBytes);
    };
    let raw_s3_upload_total_max_bytes = var("SHARDLINE_S3_UPLOAD_TOTAL_MAX_BYTES")
        .unwrap_or_else(|_error| DEFAULT_S3_UPLOAD_TOTAL_MAX_BYTES.get().to_string())
        .parse::<u64>()
        .map_err(ServerConfigError::S3UploadTotalMaxBytes)?;
    let Some(s3_upload_total_max_bytes) = NonZeroU64::new(raw_s3_upload_total_max_bytes) else {
        return Err(ServerConfigError::ZeroS3UploadTotalMaxBytes);
    };
    let raw_s3_upload_max_active_part_files = var("SHARDLINE_S3_UPLOAD_MAX_ACTIVE_PART_FILES")
        .unwrap_or_else(|_error| DEFAULT_S3_UPLOAD_MAX_ACTIVE_PART_FILES.get().to_string())
        .parse::<usize>()
        .map_err(ServerConfigError::S3UploadMaxActivePartFiles)?;
    let Some(s3_upload_max_active_part_files) =
        NonZeroUsize::new(raw_s3_upload_max_active_part_files)
    else {
        return Err(ServerConfigError::ZeroS3UploadMaxActivePartFiles);
    };
    let raw_lfs_patch_ttl_seconds = var("SHARDLINE_LFS_PATCH_TTL_SECONDS")
        .unwrap_or_else(|_error| DEFAULT_LFS_PATCH_TTL_SECONDS.get().to_string())
        .parse::<u64>()
        .map_err(ServerConfigError::LfsPatchTtl)?;
    let Some(lfs_patch_ttl_seconds) = NonZeroU64::new(raw_lfs_patch_ttl_seconds) else {
        return Err(ServerConfigError::ZeroLfsPatchTtlSeconds);
    };
    let raw_lfs_patch_max_active_sessions = var("SHARDLINE_LFS_PATCH_MAX_ACTIVE_SESSIONS")
        .unwrap_or_else(|_error| DEFAULT_LFS_PATCH_MAX_ACTIVE_SESSIONS.get().to_string())
        .parse::<usize>()
        .map_err(ServerConfigError::LfsPatchMaxActiveSessions)?;
    let Some(lfs_patch_max_active_sessions) = NonZeroUsize::new(raw_lfs_patch_max_active_sessions)
    else {
        return Err(ServerConfigError::ZeroLfsPatchMaxActiveSessions);
    };
    let raw_lfs_patch_total_max_bytes = var("SHARDLINE_LFS_PATCH_TOTAL_MAX_BYTES")
        .unwrap_or_else(|_error| DEFAULT_LFS_PATCH_TOTAL_MAX_BYTES.get().to_string())
        .parse::<u64>()
        .map_err(ServerConfigError::LfsPatchTotalMaxBytes)?;
    let Some(lfs_patch_total_max_bytes) = NonZeroU64::new(raw_lfs_patch_total_max_bytes) else {
        return Err(ServerConfigError::ZeroLfsPatchTotalMaxBytes);
    };
    let raw_lfs_patch_max_seek_ahead_bytes = var("SHARDLINE_LFS_PATCH_MAX_SEEK_AHEAD_BYTES")
        .unwrap_or_else(|_error| DEFAULT_LFS_PATCH_MAX_SEEK_AHEAD_BYTES.get().to_string())
        .parse::<u64>()
        .map_err(ServerConfigError::LfsPatchMaxSeekAheadBytes)?;
    let Some(lfs_patch_max_seek_ahead_bytes) = NonZeroU64::new(raw_lfs_patch_max_seek_ahead_bytes)
    else {
        return Err(ServerConfigError::ZeroLfsPatchMaxSeekAheadBytes);
    };
    let reconstruction_cache_redis_url = var("SHARDLINE_RECONSTRUCTION_CACHE_REDIS_URL").ok();
    let reconstruction_cache_redis_tls = load_redis_tls_config_from_env()?;
    let index_postgres_url = var("SHARDLINE_INDEX_POSTGRES_URL").ok();
    let token_signing_key = load_secret_from_env_or_file_with_conflict_check(
        (
            "SHARDLINE_TOKEN_SIGNING_KEY",
            "SHARDLINE_TOKEN_SIGNING_KEY_FILE",
        ),
        MAX_TOKEN_SIGNING_KEY_BYTES,
        true,
        ServerConfigError::EmptyTokenSigningKey,
        |env, file_env| ServerConfigError::SecretSourceConflict { env, file_env },
        ServerConfigError::TokenSigningKey,
        |observed, maximum| ServerConfigError::TokenSigningKeyTooLarge {
            observed_bytes: observed,
            maximum_bytes: maximum,
        },
        |expected, observed| ServerConfigError::TokenSigningKeyLengthMismatch {
            expected_bytes: expected,
            observed_bytes: observed,
        },
    )?;
    let hub_webhook_secret_key = load_secret_from_env_or_file_with_conflict_check(
        (
            "SHARDLINE_HUB_WEBHOOK_SECRET_KEY",
            "SHARDLINE_HUB_WEBHOOK_SECRET_KEY_FILE",
        ),
        HUB_WEBHOOK_SECRET_KEY_BYTES,
        true,
        ServerConfigError::EmptyHubWebhookSecretKey,
        |env, file_env| ServerConfigError::SecretSourceConflict { env, file_env },
        ServerConfigError::HubWebhookSecretKey,
        |observed, maximum| ServerConfigError::HubWebhookSecretKeyTooLarge {
            observed_bytes: observed,
            maximum_bytes: maximum,
        },
        |expected, observed| ServerConfigError::HubWebhookSecretKeyLengthMismatch {
            expected_bytes: expected,
            observed_bytes: observed,
        },
    )?;
    let config_secret_key = load_secret_from_env_or_file_with_conflict_check(
        (
            "SHARDLINE_CONFIG_SECRET_KEY",
            "SHARDLINE_CONFIG_SECRET_KEY_FILE",
        ),
        CONFIG_SECRET_KEY_BYTES,
        true,
        ServerConfigError::EmptyConfigSecretKey,
        |env, file_env| ServerConfigError::SecretSourceConflict { env, file_env },
        ServerConfigError::ConfigSecretKey,
        |observed, maximum| ServerConfigError::ConfigSecretKeyTooLarge {
            observed_bytes: observed,
            maximum_bytes: maximum,
        },
        |expected, observed| ServerConfigError::ConfigSecretKeyLengthMismatch {
            expected_bytes: expected,
            observed_bytes: observed,
        },
    )?;
    let ed25519_private_key = load_secret_from_env_or_file_with_conflict_check(
        (
            "SHARDLINE_ED25519_PRIVATE_KEY",
            "SHARDLINE_ED25519_PRIVATE_KEY_FILE",
        ),
        MAX_ED25519_KEY_BYTES,
        false,
        ServerConfigError::EmptyEd25519PrivateKey,
        |env, file_env| ServerConfigError::SecretSourceConflict { env, file_env },
        ServerConfigError::Ed25519PrivateKey,
        |observed, maximum| ServerConfigError::Ed25519PrivateKeyTooLarge {
            observed_bytes: observed,
            maximum_bytes: maximum,
        },
        |expected, observed| ServerConfigError::Ed25519PrivateKeyLengthMismatch {
            expected_bytes: expected,
            observed_bytes: observed,
        },
    )?;
    let ed25519_public_key = load_secret_from_env_or_file_with_conflict_check(
        (
            "SHARDLINE_ED25519_PUBLIC_KEY",
            "SHARDLINE_ED25519_PUBLIC_KEY_FILE",
        ),
        MAX_ED25519_KEY_BYTES,
        false,
        ServerConfigError::EmptyEd25519PublicKey,
        |env, file_env| ServerConfigError::SecretSourceConflict { env, file_env },
        ServerConfigError::Ed25519PublicKey,
        |observed, maximum| ServerConfigError::Ed25519PublicKeyTooLarge {
            observed_bytes: observed,
            maximum_bytes: maximum,
        },
        |expected, observed| ServerConfigError::Ed25519PublicKeyLengthMismatch {
            expected_bytes: expected,
            observed_bytes: observed,
        },
    )?;
    let metrics_token = load_secret_from_env_or_file_with_conflict_check(
        ("SHARDLINE_METRICS_TOKEN", "SHARDLINE_METRICS_TOKEN_FILE"),
        MAX_METRICS_TOKEN_BYTES,
        false,
        ServerConfigError::EmptyMetricsToken,
        |env, file_env| ServerConfigError::SecretSourceConflict { env, file_env },
        ServerConfigError::MetricsToken,
        |observed, maximum| ServerConfigError::MetricsTokenTooLarge {
            observed_bytes: observed,
            maximum_bytes: maximum,
        },
        |expected, observed| ServerConfigError::MetricsTokenLengthMismatch {
            expected_bytes: expected,
            observed_bytes: observed,
        },
    )?;
    let provider_config_path = var("SHARDLINE_PROVIDER_CONFIG_FILE")
        .ok()
        .map(PathBuf::from);
    let provider_api_key_path = var("SHARDLINE_PROVIDER_API_KEY_FILE")
        .ok()
        .map(PathBuf::from);

    let mut config = ServerConfig::new(bind_addr, public_base_url, root_dir, chunk_size)
        .with_server_role(server_role)
        .with_server_frontends(server_frontends)?
        .with_object_storage(object_storage_adapter, s3_object_store_config)
        .with_max_request_body_bytes(max_request_body_bytes)
        .with_shard_metadata_limits(shard_metadata_limits)
        .with_upload_max_in_flight_chunks(upload_max_in_flight_chunks)
        .with_transfer_max_in_flight_chunks(transfer_max_in_flight_chunks)
        .with_oci_upload_session_ttl_seconds(oci_upload_session_ttl_seconds)
        .with_oci_upload_max_active_sessions(oci_upload_max_active_sessions)
        .with_oci_registry_token_ttl_seconds(oci_registry_token_ttl_seconds)
        .with_oci_registry_token_max_in_flight_requests(oci_registry_token_max_in_flight_requests)
        .with_reconstruction_cache_memory(
            reconstruction_cache_ttl_seconds,
            reconstruction_cache_memory_max_entries,
        )
        .with_s3_max_part_bytes(s3_max_part_bytes)?
        .with_s3_upload_session_ttl_seconds(s3_upload_session_ttl_seconds)?
        .with_s3_upload_max_active_sessions(s3_upload_max_active_sessions)?
        .with_s3_min_part_bytes(s3_min_part_bytes)?
        .with_s3_upload_session_max_bytes(s3_upload_session_max_bytes)?
        .with_s3_upload_total_max_bytes(s3_upload_total_max_bytes)?
        .with_s3_upload_max_active_part_files(s3_upload_max_active_part_files)?
        .with_lfs_patch_ttl_seconds(lfs_patch_ttl_seconds)?
        .with_lfs_patch_max_active_sessions(lfs_patch_max_active_sessions)?
        .with_lfs_patch_total_max_bytes(lfs_patch_total_max_bytes)?
        .with_lfs_patch_max_seek_ahead_bytes(lfs_patch_max_seek_ahead_bytes)?
        .with_max_revisions_per_repo(max_revisions_per_repo)?
        .with_admission_max_weight(admission_max_weight_from_env());
    config.cache.adapter = reconstruction_cache_adapter;
    config.cache.redis_url = reconstruction_cache_redis_url.map(SecretString::new);
    config.cache.redis_tls = reconstruction_cache_redis_tls;
    if config.cache.adapter == ReconstructionCacheAdapter::Redis
        && config
            .cache
            .redis_url
            .as_ref()
            .map(SecretString::expose_secret)
            .is_none_or(|value| value.trim().is_empty())
    {
        return Err(ServerConfigError::MissingReconstructionCacheRedisUrl);
    }
    if let Some(index_postgres_url) = index_postgres_url {
        config = config.with_index_postgres_url(index_postgres_url)?;
    }
    if let Some(signing_key) = token_signing_key {
        config = config.with_token_signing_key(signing_key)?;
    }
    if let Some(webhook_key) = hub_webhook_secret_key {
        config = config.with_hub_webhook_secret_key(webhook_key)?;
    }
    if let Some(config_key) = config_secret_key {
        config = config.with_config_secret_key(config_key)?;
    }

    // Validate chunk size bounds: the CDC chunker requires a power of two
    // (see `upload_ingest::cdc::CdcChunker`), so a misconfigured value must
    // fail startup with a clear error instead of panicking on the first
    // upload. Upper bound is 1 GB.
    const MAX_CHUNK_SIZE: usize = 1_073_741_824;
    if chunk_size.get() > MAX_CHUNK_SIZE {
        return Err(ServerConfigError::ChunkSizeTooLarge);
    }
    if !chunk_size.get().is_power_of_two() {
        return Err(ServerConfigError::ChunkSizeNotPowerOfTwo);
    }

    // Validate auth provider configuration.
    let auth_provider = AuthProviderKind::parse(
        &var("SHARDLINE_AUTH_PROVIDER").unwrap_or_else(|_error| "local".to_owned()),
    )?;
    let auth_oidc_issuer = var("SHARDLINE_AUTH_OIDC_ISSUER")
        .ok()
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty());
    let auth_oidc_audience = var("SHARDLINE_AUTH_OIDC_AUDIENCE")
        .ok()
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty());
    // Comma-separated allowlist of extra hosts (besides the issuer's own host)
    // whose JWKS endpoints the OIDC discovery document may advertise. Defaults
    // to requiring the issuer's host (fail-closed for unknown cross-hosts).
    let auth_oidc_jwks_host_allowlist = var("SHARDLINE_AUTH_OIDC_JWKS_HOST_ALLOWLIST")
        .ok()
        .map(|value| {
            value
                .split(',')
                .map(|host| host.trim().to_owned())
                .filter(|host| !host.is_empty())
                .collect::<Vec<_>>()
        })
        .filter(|hosts| !hosts.is_empty());
    let auth_jwks_url = var("SHARDLINE_AUTH_JWKS_URL").ok();
    let auth_jwks_issuer = var("SHARDLINE_AUTH_JWKS_ISSUER").ok();
    match auth_provider {
        AuthProviderKind::Oidc => match auth_oidc_issuer.as_deref() {
            None => return Err(ServerConfigError::MissingOidcIssuer),
            Some(issuer) if !is_https_url(issuer) => {
                return Err(ServerConfigError::OidcIssuerMustUseHttps {
                    issuer: issuer.to_owned(),
                });
            }
            Some(_) => {}
        },
        AuthProviderKind::Jwks => {
            if auth_jwks_url.is_none() {
                return Err(ServerConfigError::MissingJwksUrl);
            }
        }
        AuthProviderKind::Ed25519 => {
            match (ed25519_private_key.is_some(), ed25519_public_key.is_some()) {
                (false, false) => return Err(ServerConfigError::MissingEd25519Key),
                (true, true) => return Err(ServerConfigError::ConflictingEd25519Keys),
                (true, false) | (false, true) => {}
            }
        }
        AuthProviderKind::Local | AuthProviderKind::Passthrough => {}
    }
    config = config.with_auth_provider(auth_provider);
    if let Some(issuer) = auth_oidc_issuer {
        config = config.with_auth_oidc_issuer(issuer);
    }
    if let Some(audience) = auth_oidc_audience {
        config = config.with_auth_oidc_audience(audience);
    }
    if let Some(hosts) = auth_oidc_jwks_host_allowlist {
        config = config.with_auth_oidc_jwks_host_allowlist(hosts);
    }
    if let Some(url) = auth_jwks_url {
        config = config.with_auth_jwks_url(url);
    }
    if let Some(issuer) = auth_jwks_issuer {
        config = config.with_auth_jwks_issuer(issuer);
    }
    if let Some(key) = ed25519_private_key {
        config = config.with_ed25519_private_key(key)?;
    }
    if let Some(key) = ed25519_public_key {
        config = config.with_ed25519_public_key(key)?;
    }
    if let Some(metrics_token) = metrics_token {
        config = config.with_metrics_token(metrics_token)?;
    }

    if let Some(deployment_mode) = deployment_mode_from_env()? {
        config = config.with_deployment_mode(deployment_mode);
    }

    // Fail-safe override for the at-rest secret encryption gate: only the
    // exact documented value `true` (case-insensitive) disarms the gate.
    // Unlike the generic boolean parser, the aliases `1`/`yes`/`on` are NOT
    // accepted — a stray or legacy value must keep the gate armed (treated as
    // false), matching the fail-closed deployment-mode parse.
    let allow_plaintext = match var("SHARDLINE_ALLOW_PLAINTEXT_SECRETS_IN_PRODUCTION")
        .ok()
        .as_deref()
    {
        Some(value) if value.eq_ignore_ascii_case("true") => true,
        Some(value) if value.eq_ignore_ascii_case("false") => false,
        Some(_) | None => {
            tracing::warn!(
                "invalid SHARDLINE_ALLOW_PLAINTEXT_SECRETS_IN_PRODUCTION value, treating as false"
            );
            false
        }
    };
    config = config.with_allow_plaintext_secrets_in_production(allow_plaintext);

    // Validate Hub frontend requires auth configuration.
    if config.server_frontends().contains(&ServerFrontend::Hub)
        && config.token_signing_key().is_none()
        && auth_provider == AuthProviderKind::Local
    {
        return Err(ServerConfigError::HubRequiresAuth);
    }

    let issuer_identity = var("SHARDLINE_PROVIDER_TOKEN_ISSUER")
        .unwrap_or_else(|_error| "shardline-provider".to_owned());
    let provider_ttl_seconds = var("SHARDLINE_PROVIDER_TOKEN_TTL_SECONDS")
        .unwrap_or_else(|_error| "300".to_owned())
        .parse::<u64>()
        .map_err(|_error| ServerConfigError::ProviderTokenTtl)
        .and_then(|raw_ttl_seconds| {
            NonZeroU64::new(raw_ttl_seconds).ok_or(ServerConfigError::ZeroProviderTokenTtl)
        });
    config = configure_provider_runtime_from_paths(
        config,
        provider_config_path,
        provider_api_key_path,
        issuer_identity,
        provider_ttl_seconds,
    )?;

    Ok(config)
}

/// Loads a bounded pool size from an environment variable, falling back to `default`.
pub(crate) fn bounded_pool_size_from_env(name: &str, default: usize) -> NonZeroUsize {
    let fallback = NonZeroUsize::new(default).unwrap_or(NonZeroUsize::MIN);
    var(name).map_or_else(
        |_| fallback,
        |v| {
            v.parse().unwrap_or_else(|_| {
                tracing::warn!("invalid {name} value '{v}', using default {default}");
                fallback
            })
        },
    )
}

/// Parses the `SHARDLINE_ADMISSION_MAX_WEIGHT` environment variable.
pub(crate) fn admission_max_weight_from_env() -> NonZeroUsize {
    let fallback = NonZeroUsize::new(256).unwrap_or(NonZeroUsize::MIN);
    var("SHARDLINE_ADMISSION_MAX_WEIGHT").map_or_else(
        |_| fallback,
        |v| {
            v.parse().unwrap_or_else(|_| {
                tracing::warn!(
                    "invalid SHARDLINE_ADMISSION_MAX_WEIGHT value '{v}', using default 256"
                );
                fallback
            })
        },
    )
}

/// Parses the `SHARDLINE_DEPLOYMENT_MODE` environment variable.
///
/// Returns `Ok(None)` when the variable is unset (the caller keeps the
/// insecure default, which is fine for local/dev). A set-but-invalid value is
/// a startup error, matching the fail-closed sibling gate
/// `SHARDLINE_ALLOW_PLAINTEXT_SECRETS_IN_PRODUCTION` (only the exact value
/// `true` disarms it; anything else, including `1`/`yes`/`on`, is treated as
/// false) rather than a silent fallback to the insecure default.
///
/// # Errors
///
/// Returns [`ServerConfigError::InvalidDeploymentMode`] when the variable is
/// set to an unsupported value.
pub(crate) fn deployment_mode_from_env() -> Result<Option<DeploymentMode>, ServerConfigError> {
    let Ok(value) = var("SHARDLINE_DEPLOYMENT_MODE") else {
        return Ok(None);
    };
    let Some(mode) = DeploymentMode::parse(&value) else {
        return Err(ServerConfigError::InvalidDeploymentMode { value });
    };
    Ok(Some(mode))
}

/// Returns true when `value` parses as a URL using the https scheme.
///
/// OIDC issuers must be served over https (RFC 8414 §2); a misconfigured
/// http issuer is rejected at startup instead of silently downgrading token
/// validation. Note this check is intentionally strict (no loopback
/// exemption): production OIDC issuers must always be https.
fn is_https_url(value: &str) -> bool {
    url::Url::parse(value).is_ok_and(|parsed| parsed.scheme() == "https")
}

fn parse_server_frontends_env(value: &str) -> Result<Vec<ServerFrontend>, ServerConfigError> {
    let mut parsed = Vec::new();
    for token in value.split(',').map(str::trim) {
        if token.is_empty() {
            continue;
        }
        let frontend = ServerFrontend::parse(token)
            .map_err(|_error| ServerConfigError::InvalidServerFrontend)?;
        if !parsed.contains(&frontend) {
            parsed.push(frontend);
        }
    }

    if parsed.is_empty() {
        return Err(ServerConfigError::MissingServerFrontends);
    }

    Ok(parsed)
}

/// Loads a secret from a direct env var or a file-indirection env var.
///
/// Returns `None` when neither env var is set. Returns an error when both
/// are set (source conflict), the file cannot be read, or the file content
/// exceeds the size limit.
///
/// When `strip_trailing_newline` is `true`, a single trailing line terminator
/// is stripped from a file-sourced value. Enable it only for fixed-length keys
/// (e.g. the 32-byte Hub webhook secret); variable-length secrets must pass
/// `false` so a trailing newline is never silently altered.
// The shared loader legitimately carries several error-mapping callbacks; the
// additional `strip_trailing_newline` flag keeps it one argument over clippy's
// default threshold.
#[allow(clippy::too_many_arguments)]
pub(super) fn load_secret_from_env_or_file_with_conflict_check(
    env_names: (&'static str, &'static str),
    maximum_bytes: u64,
    strip_trailing_newline: bool,
    empty_error: ServerConfigError,
    source_conflict_error: impl Fn(&'static str, &'static str) -> ServerConfigError + Copy,
    read_error: impl Fn(IoError) -> ServerConfigError + Copy,
    too_large_error: impl Fn(u64, u64) -> ServerConfigError + Copy,
    length_mismatch_error: impl Fn(u64, u64) -> ServerConfigError + Copy,
) -> Result<Option<SecretBytes>, ServerConfigError> {
    let (env_name, file_env_name) = env_names;
    let direct = var(env_name).ok();
    let file = var(file_env_name).ok();
    match (direct, file) {
        (Some(_direct), Some(_file)) => Err(source_conflict_error(env_name, file_env_name)),
        (Some(value), None) => {
            let bytes = SecretBytes::new(value.into_bytes());
            if bytes.expose_secret().is_empty() {
                return Err(empty_error);
            }
            ensure_secret_size_within_limit(
                u64::try_from(bytes.len()).unwrap_or(u64::MAX),
                maximum_bytes,
                too_large_error,
            )?;
            Ok(Some(bytes))
        }
        (None, Some(path)) => {
            let bytes = read_secret_file_bytes(
                Path::new(&path),
                maximum_bytes,
                strip_trailing_newline,
                read_error,
                too_large_error,
                length_mismatch_error,
            )?;
            if bytes.expose_secret().is_empty() {
                return Err(empty_error);
            }
            Ok(Some(bytes))
        }
        (None, None) => Ok(None),
    }
}

fn load_non_zero_usize_env(
    name: &str,
    default: NonZeroUsize,
    parse_error: fn(ParseIntError) -> ServerConfigError,
    zero_error: impl FnOnce() -> ServerConfigError,
) -> Result<NonZeroUsize, ServerConfigError> {
    let raw = var(name)
        .unwrap_or_else(|_error| default.get().to_string())
        .parse::<usize>()
        .map_err(parse_error)?;
    NonZeroUsize::new(raw).ok_or_else(zero_error)
}

/// Loads server configuration from environment variables, applying optional
/// TOML config file values as defaults. Environment variables already set in
/// the process take precedence over TOML values.
///
/// This is a thin wrapper around `load_server_config_from_env` that pre-fills
/// environment variables from a parsed `shardline.toml` before delegating to
/// the standard env-based loader.
///
/// # Errors
///
/// Returns [`ServerConfigError`] when required configuration is missing or
/// invalid.
pub fn load_server_config_from_env_with_toml(
    toml: &ShardlineTomlConfig,
) -> Result<ServerConfig, ServerConfigError> {
    use std::io::Write;

    // Collect TOML values into a buffer as quoted dotenv assignments. Quoting
    // keeps TOML strings containing whitespace, `#`, quotes, or newlines from
    // being interpreted as dotenv syntax or additional assignments.
    let mut buf = Vec::new();

    let mut set_if_unset = |key: &str, value: Option<String>| {
        if let Some(value) = value
            && var(key).is_err()
        {
            let interpolated = interpolate_env_vars(&value);
            let _ignored = writeln!(buf, "{key}={interpolated:?}");
        }
    };

    if let Some(srv) = &toml.server {
        set_if_unset("SHARDLINE_BIND_ADDR", srv.bind_addr.clone());
        set_if_unset("SHARDLINE_PUBLIC_BASE_URL", srv.public_base_url.clone());
        set_if_unset("SHARDLINE_SERVER_ROLE", srv.server_role.clone());
        if let Some(frontends) = &srv.frontends {
            set_if_unset("SHARDLINE_SERVER_FRONTENDS", Some(frontends.join(",")));
        }
        set_if_unset("SHARDLINE_ROOT_DIR", srv.root_dir.clone());
        set_if_unset(
            "SHARDLINE_MAX_REQUEST_BODY_BYTES",
            srv.max_request_body_bytes.map(|v| v.to_string()),
        );
        set_if_unset("SHARDLINE_CHUNK_SIZE", srv.chunk_size.clone());
        set_if_unset(
            "SHARDLINE_CHUNK_SIZE_BYTES",
            srv.chunk_size_bytes.map(|v| v.to_string()),
        );
        set_if_unset(
            "SHARDLINE_UPLOAD_MAX_IN_FLIGHT_CHUNKS",
            srv.upload_max_in_flight_chunks.map(|v| v.to_string()),
        );
        set_if_unset(
            "SHARDLINE_TRANSFER_MAX_IN_FLIGHT_CHUNKS",
            srv.transfer_max_in_flight_chunks.map(|v| v.to_string()),
        );
    }

    if let Some(stg) = &toml.storage {
        set_if_unset("SHARDLINE_OBJECT_STORAGE_ADAPTER", stg.adapter.clone());
        if let Some(s3) = &stg.s3 {
            set_if_unset("SHARDLINE_S3_ENDPOINT", s3.endpoint.clone());
            set_if_unset("SHARDLINE_S3_REGION", s3.region.clone());
            set_if_unset("SHARDLINE_S3_BUCKET", s3.bucket.clone());
            set_if_unset("SHARDLINE_S3_KEY_PREFIX", s3.prefix.clone());
            set_if_unset(
                "SHARDLINE_S3_ALLOW_HTTP",
                s3.allow_http.map(|v| v.to_string()),
            );
            set_if_unset(
                "SHARDLINE_S3_VIRTUAL_HOSTED_STYLE_REQUEST",
                s3.virtual_hosted_style.map(|v| v.to_string()),
            );
        }
    }

    if let Some(idx) = &toml.index {
        set_if_unset("SHARDLINE_INDEX_POSTGRES_URL", idx.postgres_url.clone());
    }

    if let Some(cch) = &toml.cache {
        set_if_unset(
            "SHARDLINE_RECONSTRUCTION_CACHE_ADAPTER",
            cch.adapter.clone(),
        );
        set_if_unset(
            "SHARDLINE_RECONSTRUCTION_CACHE_REDIS_URL",
            cch.redis_url.clone(),
        );
        set_if_unset(
            "SHARDLINE_RECONSTRUCTION_CACHE_TTL_SECONDS",
            cch.ttl_seconds.map(|v| v.to_string()),
        );
    }

    if let Some(auth) = &toml.auth {
        set_if_unset("SHARDLINE_AUTH_PROVIDER", auth.provider.clone());
        set_if_unset(
            "SHARDLINE_TOKEN_SIGNING_KEY_FILE",
            auth.token_signing_key_path.clone(),
        );
        set_if_unset(
            "SHARDLINE_PROVIDER_API_KEY_FILE",
            auth.provider_api_key_path.clone(),
        );
        set_if_unset(
            "SHARDLINE_PROVIDER_TOKEN_ISSUER",
            auth.provider_token_issuer.clone(),
        );
        set_if_unset(
            "SHARDLINE_PROVIDER_TOKEN_TTL_SECONDS",
            auth.provider_token_ttl_seconds.map(|v| v.to_string()),
        );
        if let Some(jwks) = &auth.jwks {
            set_if_unset("SHARDLINE_AUTH_JWKS_URL", jwks.url.clone());
        }
        if let Some(oidc) = &auth.oidc {
            set_if_unset("SHARDLINE_AUTH_OIDC_ISSUER", oidc.issuer_url.clone());
            set_if_unset("SHARDLINE_AUTH_OIDC_AUDIENCE", oidc.audience.clone());
            if let Some(hosts) = &oidc.jwks_host_allowlist {
                set_if_unset(
                    "SHARDLINE_AUTH_OIDC_JWKS_HOST_ALLOWLIST",
                    Some(hosts.join(",")),
                );
            }
        }
        if let Some(ed25519) = &auth.ed25519 {
            set_if_unset(
                "SHARDLINE_ED25519_PRIVATE_KEY_FILE",
                ed25519.private_key_path.clone(),
            );
            set_if_unset(
                "SHARDLINE_ED25519_PUBLIC_KEY_FILE",
                ed25519.public_key_path.clone(),
            );
        }
    }

    // Apply TOML values to the process environment for keys not already set.
    if !buf.is_empty() {
        dotenvy::from_read(std::io::Cursor::new(buf)).map_err(|_error| {
            ServerConfigError::ConfigFileError(
                "failed to apply validated TOML configuration values".to_owned(),
            )
        })?;
    }

    load_server_config_from_env()
}

/// Interpolates `${VAR_NAME}` patterns in `value` using the current process
/// environment. Returns the original value when no patterns are found.
fn interpolate_env_vars(value: &str) -> String {
    let mut result = String::with_capacity(value.len());
    let mut chars = value.chars().peekable();

    'outer: while let Some(ch) = chars.next() {
        if ch == '$' && chars.peek() == Some(&'{') {
            chars.next(); // consume '{'
            let mut var_name = String::new();
            loop {
                match chars.next() {
                    Some('}') => {
                        let resolved =
                            var(&var_name).unwrap_or_else(|_| format!("${{{var_name}}}"));
                        result.push_str(&resolved);
                        break;
                    }
                    Some(c) => var_name.push(c),
                    None => {
                        // Unclosed ${ — preserve original text
                        result.push('$');
                        result.push('{');
                        result.push_str(&var_name);
                        break 'outer;
                    }
                }
            }
        } else {
            result.push(ch);
        }
    }
    result
}

#[cfg(test)]
mod interpolate_tests {
    use super::interpolate_env_vars;

    #[test]
    fn test_no_vars() {
        assert_eq!(interpolate_env_vars("plain text"), "plain text");
    }

    #[test]
    fn test_known_var() {
        let content = "_TEST_INTERP_VAR=resolved".to_owned();
        let _ = dotenvy::from_read(std::io::Cursor::new(content.as_bytes()));
        assert_eq!(
            interpolate_env_vars("prefix-${_TEST_INTERP_VAR}-suffix"),
            "prefix-resolved-suffix"
        );
    }

    #[test]
    fn test_missing_var() {
        assert_eq!(
            interpolate_env_vars("${_NONEXISTENT_VAR_XYZ}"),
            "${_NONEXISTENT_VAR_XYZ}"
        );
    }

    #[test]
    fn test_empty_var_name() {
        assert_eq!(interpolate_env_vars("${}"), "${}");
    }

    #[test]
    fn test_dollar_without_brace() {
        assert_eq!(interpolate_env_vars("$VAR"), "$VAR");
        assert_eq!(interpolate_env_vars("$$"), "$$");
    }

    #[test]
    fn test_multiple_vars() {
        let content = "_TEST_A=hello\n_TEST_B=world";
        let _ = dotenvy::from_read(std::io::Cursor::new(content.as_bytes()));
        let result = interpolate_env_vars("${_TEST_A} ${_TEST_B}");
        assert_eq!(result, "hello world");
    }

    #[test]
    fn test_unclosed_brace_preserves_text() {
        // Malformed input without closing } should be preserved
        assert_eq!(interpolate_env_vars("${HOST"), "${HOST");
    }

    #[test]
    fn test_unclosed_brace_at_end() {
        assert_eq!(interpolate_env_vars("prefix-${VAR"), "prefix-${VAR");
    }

    #[test]
    fn test_nested_dollar_signs() {
        assert_eq!(interpolate_env_vars("$${VAR}"), "$${VAR}");
    }

    #[test]
    fn test_empty_input() {
        assert_eq!(interpolate_env_vars(""), "");
    }

    #[test]
    fn test_only_brace_no_var() {
        assert_eq!(interpolate_env_vars("${}"), "${}");
    }
}

#[cfg(test)]
mod tests {
    #![allow(unsafe_code)]
    use crate::ServerFrontend;

    use super::{
        load_non_zero_usize_env, load_server_config_from_env_with_toml, parse_server_frontends_env,
    };

    fn set_env_var(key: &str, value: &str) {
        // SAFETY: Must only be called from `#[serial_test::serial]` tests to
        // prevent data races on the global environment.
        unsafe { std::env::set_var(key, value) };
    }

    fn remove_env_var(key: &str) {
        // SAFETY: Same threading constraints as `set_env_var`.
        unsafe { std::env::remove_var(key) };
    }

    #[test]
    #[serial_test::serial]
    fn toml_s3_values_use_the_runtime_environment_keys() {
        const S3_KEYS: &[&str] = &[
            "SHARDLINE_OBJECT_STORAGE_ADAPTER",
            "SHARDLINE_S3_BUCKET",
            "SHARDLINE_S3_REGION",
            "SHARDLINE_S3_ENDPOINT",
            "SHARDLINE_S3_KEY_PREFIX",
            "SHARDLINE_S3_PREFIX",
            "SHARDLINE_S3_ALLOW_HTTP",
            "SHARDLINE_S3_VIRTUAL_HOSTED_STYLE_REQUEST",
            "SHARDLINE_S3_VIRTUAL_HOSTED_STYLE",
            "SHARDLINE_AUTH_PROVIDER",
        ];
        for key in S3_KEYS {
            remove_env_var(key);
        }

        let toml: super::ShardlineTomlConfig = toml::from_str(
            r#"
[storage]
adapter = "s3"
[storage.s3]
bucket = "test-bucket"
region = "eu-west-1"
endpoint = "http://localhost:9000"
prefix = "toml-prefix/"
allow_http = true
virtual_hosted_style = true
"#,
        )
        .unwrap();
        let config = load_server_config_from_env_with_toml(&toml).unwrap();
        let rendered = format!("{:?}", config.s3_object_store_config().unwrap());
        assert!(rendered.contains("key_prefix: Some(\"toml-prefix\")"));
        assert!(rendered.contains("allow_http: true"));
        assert!(rendered.contains("virtual_hosted_style_request: true"));

        for key in S3_KEYS {
            remove_env_var(key);
        }
    }

    #[test]
    #[serial_test::serial]
    fn toml_values_with_dotenv_syntax_are_applied_as_single_values() {
        const KEYS: &[&str] = &[
            "SHARDLINE_ROOT_DIR",
            "SHARDLINE_AUTH_PROVIDER",
            "SHARDLINE_INJECTED_VALUE",
        ];
        for key in KEYS {
            remove_env_var(key);
        }

        let toml: super::ShardlineTomlConfig = toml::from_str(
            r#"
[server]
root_dir = "runtime#dir\nSHARDLINE_INJECTED_VALUE=unexpected"
"#,
        )
        .unwrap();
        let config = load_server_config_from_env_with_toml(&toml).unwrap();
        assert_eq!(
            config.root_dir(),
            std::path::Path::new("runtime#dir\nSHARDLINE_INJECTED_VALUE=unexpected")
        );
        assert_eq!(config.auth_provider(), super::AuthProviderKind::Local);
        assert!(std::env::var("SHARDLINE_INJECTED_VALUE").is_err());

        for key in KEYS {
            remove_env_var(key);
        }
    }

    // ── deployment_mode_from_env ───────────────────────────────────────────

    #[test]
    #[serial_test::serial]
    fn deployment_mode_from_env_is_case_insensitive_and_whitespace_tolerant() {
        for (value, expected) in [
            ("insecure", super::DeploymentMode::Insecure),
            (" AUTHENTICATED ", super::DeploymentMode::Authenticated),
            ("Strict", super::DeploymentMode::Strict),
        ] {
            set_env_var("SHARDLINE_DEPLOYMENT_MODE", value);
            assert_eq!(super::deployment_mode_from_env().unwrap(), Some(expected));
        }
        remove_env_var("SHARDLINE_DEPLOYMENT_MODE");
    }

    #[test]
    #[serial_test::serial]
    fn deployment_mode_from_env_is_unset_when_variable_missing() {
        remove_env_var("SHARDLINE_DEPLOYMENT_MODE");
        assert_eq!(super::deployment_mode_from_env().unwrap(), None);
    }

    #[test]
    #[serial_test::serial]
    fn deployment_mode_from_env_rejects_unknown_value() {
        set_env_var("SHARDLINE_DEPLOYMENT_MODE", "nonsense");
        assert!(matches!(
            super::deployment_mode_from_env(),
            Err(super::ServerConfigError::InvalidDeploymentMode { .. })
        ));
        remove_env_var("SHARDLINE_DEPLOYMENT_MODE");
    }

    // ── parse_server_frontends_env ─────────────────────────────────────────

    #[test]
    fn parse_server_frontends_env_accepts_one_or_more_tokens() {
        let single = parse_server_frontends_env("xet");
        assert!(single.is_ok());
        assert_eq!(single.ok(), Some(vec![ServerFrontend::Xet]));
        let deduplicated = parse_server_frontends_env("xet, xet");
        assert!(deduplicated.is_ok());
        assert_eq!(deduplicated.ok(), Some(vec![ServerFrontend::Xet]));
        let multiple = parse_server_frontends_env("xet,lfs,bazel-http,oci");
        assert!(multiple.is_ok());
        assert_eq!(
            multiple.ok(),
            Some(vec![
                ServerFrontend::Xet,
                ServerFrontend::Lfs,
                ServerFrontend::BazelHttp,
                ServerFrontend::Oci,
            ])
        );
    }

    #[test]
    fn parse_server_frontends_env_rejects_invalid_or_empty_tokens() {
        assert!(parse_server_frontends_env("").is_err());
        let trailing_comma = parse_server_frontends_env("xet,");
        assert!(trailing_comma.is_ok());
        assert_eq!(trailing_comma.ok(), Some(vec![ServerFrontend::Xet]));
        let empty_segments = parse_server_frontends_env(",xet,,");
        assert!(empty_segments.is_ok());
        assert_eq!(empty_segments.ok(), Some(vec![ServerFrontend::Xet]));
        assert!(parse_server_frontends_env("unknown").is_err());
    }

    #[test]
    fn parse_server_frontends_env_accepts_single_xet() {
        let result = parse_server_frontends_env("xet");
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), vec![ServerFrontend::Xet]);
    }

    #[test]
    fn parse_server_frontends_env_rejects_unknown_frontend() {
        let result = parse_server_frontends_env("unknown");
        assert!(matches!(
            result,
            Err(super::ServerConfigError::InvalidServerFrontend)
        ));
    }

    #[test]
    fn parse_server_frontends_env_multiple_with_dedup() {
        let result = parse_server_frontends_env("xet,lfs,oci,lfs");
        assert!(result.is_ok());
        let frontends = result.unwrap();
        assert_eq!(frontends.len(), 3);
        assert!(frontends.contains(&crate::ServerFrontend::Xet));
        assert!(frontends.contains(&crate::ServerFrontend::Lfs));
        assert!(frontends.contains(&crate::ServerFrontend::Oci));
    }

    #[test]
    fn parse_server_frontends_env_all_known() {
        let result = parse_server_frontends_env("xet,lfs,bazel-http,oci,hub");
        assert!(result.is_ok());
        let frontends = result.unwrap();
        assert_eq!(frontends.len(), 5);
    }

    #[test]
    #[serial_test::serial]
    fn load_server_config_metrics_token_from_file() {
        // Lines 175-183: metrics token loaded from SHARDLINE_METRICS_TOKEN_FILE
        use std::io::Write;
        let mut tmp = tempfile::NamedTempFile::new().unwrap();
        tmp.write_all(b"my-metrics-token").unwrap();
        tmp.flush().unwrap();

        set_env_var("SHARDLINE_METRICS_TOKEN_FILE", tmp.path().to_str().unwrap());
        set_env_var("SHARDLINE_ROOT_DIR", "/tmp/shardline_test");
        set_env_var("SHARDLINE_PUBLIC_BASE_URL", "http://localhost:8080");
        let result = super::load_server_config_from_env();
        // Should succeed since other required env vars are defaulted
        assert!(result.is_ok(), "expected Ok, got {result:?}");
        let config = result.unwrap();
        assert!(config.metrics_token().is_some());

        remove_env_var("SHARDLINE_METRICS_TOKEN_FILE");
        remove_env_var("SHARDLINE_ROOT_DIR");
        remove_env_var("SHARDLINE_PUBLIC_BASE_URL");
    }

    #[test]
    #[serial_test::serial]
    fn load_server_config_metrics_token_from_direct_env() {
        // SHARDLINE_METRICS_TOKEN (direct) must be honored for parity with the
        // other secret knobs, not just the _FILE indirection.
        set_env_var("SHARDLINE_METRICS_TOKEN", "direct-metrics-token");
        remove_env_var("SHARDLINE_METRICS_TOKEN_FILE");
        set_env_var("SHARDLINE_ROOT_DIR", "/tmp/shardline_test");
        set_env_var("SHARDLINE_PUBLIC_BASE_URL", "http://localhost:8080");
        let result = super::load_server_config_from_env();
        assert!(result.is_ok(), "expected Ok, got {result:?}");
        let config = result.unwrap();
        assert_eq!(
            config.metrics_token(),
            Some(b"direct-metrics-token".as_slice()),
            "SHARDLINE_METRICS_TOKEN must flow into the config"
        );

        remove_env_var("SHARDLINE_METRICS_TOKEN");
        remove_env_var("SHARDLINE_ROOT_DIR");
        remove_env_var("SHARDLINE_PUBLIC_BASE_URL");
    }

    #[test]
    #[serial_test::serial]
    fn load_server_config_metrics_token_rejects_both_sources() {
        // Both the direct and file-indirection env vars set -> source conflict,
        // matching every other secret knob.
        set_env_var("SHARDLINE_METRICS_TOKEN", "direct-metrics-token");
        set_env_var(
            "SHARDLINE_METRICS_TOKEN_FILE",
            "/tmp/does-not-need-to-exist",
        );
        set_env_var("SHARDLINE_ROOT_DIR", "/tmp/shardline_test");
        set_env_var("SHARDLINE_PUBLIC_BASE_URL", "http://localhost:8080");
        let result = super::load_server_config_from_env();
        assert!(matches!(
            result,
            Err(super::ServerConfigError::SecretSourceConflict { .. })
        ));
        remove_env_var("SHARDLINE_METRICS_TOKEN");
        remove_env_var("SHARDLINE_METRICS_TOKEN_FILE");
        remove_env_var("SHARDLINE_ROOT_DIR");
        remove_env_var("SHARDLINE_PUBLIC_BASE_URL");
    }

    // ── SHARDLINE_MAX_REVISIONS_PER_REPO ───────────────────────────────────

    #[test]
    #[serial_test::serial]
    fn load_server_config_max_revisions_per_repo_from_env() {
        set_env_var("SHARDLINE_MAX_REVISIONS_PER_REPO", "77");
        set_env_var("SHARDLINE_ROOT_DIR", "/tmp/shardline_test");
        set_env_var("SHARDLINE_PUBLIC_BASE_URL", "http://localhost:8080");
        let result = super::load_server_config_from_env();
        assert!(result.is_ok(), "expected Ok, got {result:?}");
        let config = result.unwrap();
        assert_eq!(
            config.max_revisions_per_repo(),
            std::num::NonZeroUsize::new(77).unwrap()
        );

        remove_env_var("SHARDLINE_MAX_REVISIONS_PER_REPO");
        remove_env_var("SHARDLINE_ROOT_DIR");
        remove_env_var("SHARDLINE_PUBLIC_BASE_URL");
    }

    #[test]
    #[serial_test::serial]
    fn load_server_config_max_revisions_per_repo_defaults() {
        remove_env_var("SHARDLINE_MAX_REVISIONS_PER_REPO");
        set_env_var("SHARDLINE_ROOT_DIR", "/tmp/shardline_test");
        set_env_var("SHARDLINE_PUBLIC_BASE_URL", "http://localhost:8080");
        let result = super::load_server_config_from_env();
        assert!(result.is_ok(), "expected Ok, got {result:?}");
        let config = result.unwrap();
        assert_eq!(
            config.max_revisions_per_repo(),
            super::DEFAULT_MAX_REVISIONS_PER_REPO
        );

        remove_env_var("SHARDLINE_ROOT_DIR");
        remove_env_var("SHARDLINE_PUBLIC_BASE_URL");
    }

    #[test]
    #[serial_test::serial]
    fn load_server_config_max_revisions_per_repo_rejects_zero() {
        set_env_var("SHARDLINE_MAX_REVISIONS_PER_REPO", "0");
        set_env_var("SHARDLINE_ROOT_DIR", "/tmp/shardline_test");
        set_env_var("SHARDLINE_PUBLIC_BASE_URL", "http://localhost:8080");
        let result = super::load_server_config_from_env();
        assert!(matches!(
            result,
            Err(super::ServerConfigError::ZeroMaxRevisionsPerRepo)
        ));

        remove_env_var("SHARDLINE_MAX_REVISIONS_PER_REPO");
        remove_env_var("SHARDLINE_ROOT_DIR");
        remove_env_var("SHARDLINE_PUBLIC_BASE_URL");
    }

    #[test]
    #[serial_test::serial]
    fn load_server_config_max_revisions_per_repo_rejects_unparsable() {
        set_env_var("SHARDLINE_MAX_REVISIONS_PER_REPO", "not-a-number");
        set_env_var("SHARDLINE_ROOT_DIR", "/tmp/shardline_test");
        set_env_var("SHARDLINE_PUBLIC_BASE_URL", "http://localhost:8080");
        let result = super::load_server_config_from_env();
        assert!(matches!(
            result,
            Err(super::ServerConfigError::MaxRevisionsPerRepo(_))
        ));

        remove_env_var("SHARDLINE_MAX_REVISIONS_PER_REPO");
        remove_env_var("SHARDLINE_ROOT_DIR");
        remove_env_var("SHARDLINE_PUBLIC_BASE_URL");
    }

    // ── env-provided secrets are never newline-trimmed ─────────────────────

    #[test]
    #[serial_test::serial]
    fn hub_webhook_secret_from_env_is_not_newline_trimmed() {
        // Trailing-newline stripping applies only to secret *files*. An
        // env-provided value (32 key bytes + `\n` = 33 bytes) must exceed the
        // 32-byte bound and be rejected, proving env values are left untouched.
        // SAFETY: serialized env test
        set_env_var(
            "SHARDLINE_HUB_WEBHOOK_SECRET_KEY",
            "0123456789abcdef0123456789abcdef\n",
        );
        remove_env_var("SHARDLINE_HUB_WEBHOOK_SECRET_KEY_FILE");
        let result = super::load_secret_from_env_or_file_with_conflict_check(
            (
                "SHARDLINE_HUB_WEBHOOK_SECRET_KEY",
                "SHARDLINE_HUB_WEBHOOK_SECRET_KEY_FILE",
            ),
            super::HUB_WEBHOOK_SECRET_KEY_BYTES,
            true,
            super::ServerConfigError::EmptyHubWebhookSecretKey,
            |env, file_env| super::ServerConfigError::SecretSourceConflict { env, file_env },
            super::ServerConfigError::HubWebhookSecretKey,
            |observed, maximum| super::ServerConfigError::HubWebhookSecretKeyTooLarge {
                observed_bytes: observed,
                maximum_bytes: maximum,
            },
            |expected, observed| super::ServerConfigError::HubWebhookSecretKeyLengthMismatch {
                expected_bytes: expected,
                observed_bytes: observed,
            },
        );
        remove_env_var("SHARDLINE_HUB_WEBHOOK_SECRET_KEY");
        assert!(result.is_err());
    }

    // ── provider-config secret key env handling ────────────────────────────

    #[test]
    #[serial_test::serial]
    fn config_secret_from_env_is_not_newline_trimmed() {
        // Trailing-newline stripping applies only to secret *files*. An
        // env-provided value (32 key bytes + `\n` = 33 bytes) must exceed the
        // 32-byte bound and be rejected, proving env values are left untouched.
        // SAFETY: serialized env test
        set_env_var(
            "SHARDLINE_CONFIG_SECRET_KEY",
            "0123456789abcdef0123456789abcdef\n",
        );
        remove_env_var("SHARDLINE_CONFIG_SECRET_KEY_FILE");
        let result = super::load_secret_from_env_or_file_with_conflict_check(
            (
                "SHARDLINE_CONFIG_SECRET_KEY",
                "SHARDLINE_CONFIG_SECRET_KEY_FILE",
            ),
            super::CONFIG_SECRET_KEY_BYTES,
            true,
            super::ServerConfigError::EmptyConfigSecretKey,
            |env, file_env| super::ServerConfigError::SecretSourceConflict { env, file_env },
            super::ServerConfigError::ConfigSecretKey,
            |observed, maximum| super::ServerConfigError::ConfigSecretKeyTooLarge {
                observed_bytes: observed,
                maximum_bytes: maximum,
            },
            |expected, observed| super::ServerConfigError::ConfigSecretKeyLengthMismatch {
                expected_bytes: expected,
                observed_bytes: observed,
            },
        );
        remove_env_var("SHARDLINE_CONFIG_SECRET_KEY");
        assert!(result.is_err());
    }

    #[test]
    #[serial_test::serial]
    fn config_secret_key_from_env_is_applied_to_config() {
        // A valid 32-byte key configured via the environment must flow through
        // the full loader into `ServerConfig::with_config_secret_key`.
        // SAFETY: serialized env test
        set_env_var(
            "SHARDLINE_CONFIG_SECRET_KEY",
            "0123456789abcdef0123456789abcdef",
        );
        remove_env_var("SHARDLINE_CONFIG_SECRET_KEY_FILE");
        set_env_var("SHARDLINE_ROOT_DIR", "/tmp/shardline_test");
        set_env_var("SHARDLINE_PUBLIC_BASE_URL", "http://localhost:8080");

        let result = super::load_server_config_from_env();
        remove_env_var("SHARDLINE_CONFIG_SECRET_KEY");
        remove_env_var("SHARDLINE_ROOT_DIR");
        remove_env_var("SHARDLINE_PUBLIC_BASE_URL");

        let config = result.expect("config loads with a config secret key");
        assert_eq!(
            config.config_secret_key(),
            Some(b"0123456789abcdef0123456789abcdef".as_slice())
        );
    }

    #[test]
    #[serial_test::serial]
    fn config_secret_key_from_file_is_applied_to_config() {
        use std::io::Write;
        let mut tmp = tempfile::NamedTempFile::new().unwrap();
        tmp.write_all(b"0123456789abcdef0123456789abcdef").unwrap();
        tmp.flush().unwrap();

        // SAFETY: serialized env test
        set_env_var(
            "SHARDLINE_CONFIG_SECRET_KEY_FILE",
            tmp.path().to_str().unwrap(),
        );
        remove_env_var("SHARDLINE_CONFIG_SECRET_KEY");
        set_env_var("SHARDLINE_ROOT_DIR", "/tmp/shardline_test");
        set_env_var("SHARDLINE_PUBLIC_BASE_URL", "http://localhost:8080");

        let result = super::load_server_config_from_env();
        remove_env_var("SHARDLINE_CONFIG_SECRET_KEY_FILE");
        remove_env_var("SHARDLINE_ROOT_DIR");
        remove_env_var("SHARDLINE_PUBLIC_BASE_URL");

        let config = result.expect("config loads with a config secret key file");
        assert_eq!(
            config.config_secret_key(),
            Some(b"0123456789abcdef0123456789abcdef".as_slice())
        );
    }

    #[test]
    #[serial_test::serial]
    fn allow_plaintext_secrets_in_production_from_env_true() {
        // A valid boolean override configured via the environment must flow
        // through the full loader into `ServerConfig`.
        // SAFETY: serialized env test
        set_env_var("SHARDLINE_ALLOW_PLAINTEXT_SECRETS_IN_PRODUCTION", "true");
        set_env_var("SHARDLINE_ROOT_DIR", "/tmp/shardline_test");
        set_env_var("SHARDLINE_PUBLIC_BASE_URL", "http://localhost:8080");

        let result = super::load_server_config_from_env();
        remove_env_var("SHARDLINE_ALLOW_PLAINTEXT_SECRETS_IN_PRODUCTION");
        remove_env_var("SHARDLINE_ROOT_DIR");
        remove_env_var("SHARDLINE_PUBLIC_BASE_URL");

        let config = result.expect("config loads with the plaintext override set");
        assert!(config.allow_plaintext_secrets_in_production());
    }

    #[test]
    #[serial_test::serial]
    fn allow_plaintext_secrets_in_production_from_env_invalid_is_false() {
        // An invalid override value must fail safe and keep the gate armed.
        // SAFETY: serialized env test
        set_env_var("SHARDLINE_ALLOW_PLAINTEXT_SECRETS_IN_PRODUCTION", "banana");
        set_env_var("SHARDLINE_ROOT_DIR", "/tmp/shardline_test");
        set_env_var("SHARDLINE_PUBLIC_BASE_URL", "http://localhost:8080");

        let result = super::load_server_config_from_env();
        remove_env_var("SHARDLINE_ALLOW_PLAINTEXT_SECRETS_IN_PRODUCTION");
        remove_env_var("SHARDLINE_ROOT_DIR");
        remove_env_var("SHARDLINE_PUBLIC_BASE_URL");

        let config = result.expect("config loads with an invalid plaintext override");
        assert!(!config.allow_plaintext_secrets_in_production());
    }

    #[test]
    #[serial_test::serial]
    fn allow_plaintext_secrets_in_production_from_env_uppercase_true() {
        // The exact documented value is matched case-insensitively.
        // SAFETY: serialized env test
        set_env_var("SHARDLINE_ALLOW_PLAINTEXT_SECRETS_IN_PRODUCTION", "TRUE");
        set_env_var("SHARDLINE_ROOT_DIR", "/tmp/shardline_test");
        set_env_var("SHARDLINE_PUBLIC_BASE_URL", "http://localhost:8080");

        let result = super::load_server_config_from_env();
        remove_env_var("SHARDLINE_ALLOW_PLAINTEXT_SECRETS_IN_PRODUCTION");
        remove_env_var("SHARDLINE_ROOT_DIR");
        remove_env_var("SHARDLINE_PUBLIC_BASE_URL");

        let config = result.expect("config loads with the plaintext override set");
        assert!(config.allow_plaintext_secrets_in_production());
    }

    #[test]
    #[serial_test::serial]
    fn allow_plaintext_secrets_in_production_from_env_one_is_false() {
        // A stray `1` must NOT disarm the gate (generic boolean alias).
        // SAFETY: serialized env test
        set_env_var("SHARDLINE_ALLOW_PLAINTEXT_SECRETS_IN_PRODUCTION", "1");
        set_env_var("SHARDLINE_ROOT_DIR", "/tmp/shardline_test");
        set_env_var("SHARDLINE_PUBLIC_BASE_URL", "http://localhost:8080");

        let result = super::load_server_config_from_env();
        remove_env_var("SHARDLINE_ALLOW_PLAINTEXT_SECRETS_IN_PRODUCTION");
        remove_env_var("SHARDLINE_ROOT_DIR");
        remove_env_var("SHARDLINE_PUBLIC_BASE_URL");

        let config = result.expect("config loads with a legacy plaintext override");
        assert!(
            !config.allow_plaintext_secrets_in_production(),
            "the generic true-alias `1` must not disarm the plaintext gate"
        );
    }

    #[test]
    #[serial_test::serial]
    fn allow_plaintext_secrets_in_production_from_env_yes_is_false() {
        // A stray `yes` must NOT disarm the gate (generic boolean alias).
        // SAFETY: serialized env test
        set_env_var("SHARDLINE_ALLOW_PLAINTEXT_SECRETS_IN_PRODUCTION", "yes");
        set_env_var("SHARDLINE_ROOT_DIR", "/tmp/shardline_test");
        set_env_var("SHARDLINE_PUBLIC_BASE_URL", "http://localhost:8080");

        let result = super::load_server_config_from_env();
        remove_env_var("SHARDLINE_ALLOW_PLAINTEXT_SECRETS_IN_PRODUCTION");
        remove_env_var("SHARDLINE_ROOT_DIR");
        remove_env_var("SHARDLINE_PUBLIC_BASE_URL");

        let config = result.expect("config loads with a legacy plaintext override");
        assert!(
            !config.allow_plaintext_secrets_in_production(),
            "the generic true-alias `yes` must not disarm the plaintext gate"
        );
    }

    #[test]
    #[serial_test::serial]
    fn allow_plaintext_secrets_in_production_from_env_on_is_false() {
        // A stray `on` must NOT disarm the gate (generic boolean alias).
        // SAFETY: serialized env test
        set_env_var("SHARDLINE_ALLOW_PLAINTEXT_SECRETS_IN_PRODUCTION", "on");
        set_env_var("SHARDLINE_ROOT_DIR", "/tmp/shardline_test");
        set_env_var("SHARDLINE_PUBLIC_BASE_URL", "http://localhost:8080");

        let result = super::load_server_config_from_env();
        remove_env_var("SHARDLINE_ALLOW_PLAINTEXT_SECRETS_IN_PRODUCTION");
        remove_env_var("SHARDLINE_ROOT_DIR");
        remove_env_var("SHARDLINE_PUBLIC_BASE_URL");

        let config = result.expect("config loads with a legacy plaintext override");
        assert!(
            !config.allow_plaintext_secrets_in_production(),
            "the generic true-alias `on` must not disarm the plaintext gate"
        );
    }

    #[test]
    fn server_config_with_config_secret_key_rejects_empty() {
        let config = test_server_config();
        let result = config.with_config_secret_key(Vec::new());
        assert!(matches!(
            result,
            Err(super::ServerConfigError::EmptyConfigSecretKey)
        ));
    }

    #[test]
    fn server_config_with_config_secret_key_rejects_wrong_length() {
        let config = test_server_config();
        let result = config.with_config_secret_key(b"too-short".to_vec());
        assert!(matches!(
            result,
            Err(super::ServerConfigError::ConfigSecretKeyLength {
                expected,
                observed,
            }) if expected == usize::try_from(super::CONFIG_SECRET_KEY_BYTES).unwrap_or(0)
                && observed == 9
        ));
    }

    #[test]
    fn server_config_with_config_secret_key_accepts_32_bytes() {
        let config = test_server_config();
        let config = config
            .with_config_secret_key(b"0123456789abcdef0123456789abcdef".to_vec())
            .expect("32-byte key is accepted");
        assert_eq!(
            config.config_secret_key(),
            Some(b"0123456789abcdef0123456789abcdef".as_slice())
        );
    }

    fn test_server_config() -> super::ServerConfig {
        use std::net::{IpAddr, Ipv4Addr, SocketAddr};
        super::ServerConfig::new(
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8080),
            "http://127.0.0.1:8080".to_owned(),
            std::path::PathBuf::from("/tmp/shardline"),
            std::num::NonZeroUsize::MIN,
        )
    }

    // ── load_non_zero_usize_env ────────────────────────────────────────────

    // ── load_non_zero_usize_env ────────────────────────────────────────────

    #[test]
    #[serial_test::serial]
    fn load_non_zero_usize_env_uses_default_when_env_unset() {
        let key = "SHARDLINE_TEST_NON_ZERO_UNSET";
        remove_env_var(key);
        let default = std::num::NonZeroUsize::new(42).unwrap();
        let result = load_non_zero_usize_env(
            key,
            default,
            super::ServerConfigError::MaxShardFiles,
            || super::ServerConfigError::ZeroMaxShardFiles,
        );
        assert!(result.is_ok());
        assert_eq!(result.unwrap().get(), 42);
        remove_env_var(key);
    }

    #[test]
    #[serial_test::serial]
    fn load_non_zero_usize_env_reads_env_value() {
        let key = "SHARDLINE_TEST_NON_ZERO_VALID";
        set_env_var(key, "99");
        let default = std::num::NonZeroUsize::new(1).unwrap();
        let result = load_non_zero_usize_env(
            key,
            default,
            super::ServerConfigError::MaxShardFiles,
            || super::ServerConfigError::ZeroMaxShardFiles,
        );
        assert!(result.is_ok());
        assert_eq!(result.unwrap().get(), 99);
        remove_env_var(key);
    }

    #[test]
    #[serial_test::serial]
    fn load_non_zero_usize_env_rejects_non_numeric() {
        let key = "SHARDLINE_TEST_NON_ZERO_INVALID";
        set_env_var(key, "not-a-number");
        let default = std::num::NonZeroUsize::new(5).unwrap();
        let result = load_non_zero_usize_env(
            key,
            default,
            super::ServerConfigError::MaxShardFiles,
            || super::ServerConfigError::ZeroMaxShardFiles,
        );
        assert!(result.is_err());
        remove_env_var(key);
    }

    #[test]
    #[serial_test::serial]
    fn load_non_zero_usize_env_rejects_zero_value() {
        let key = "SHARDLINE_TEST_NON_ZERO_ZERO";
        set_env_var(key, "0");
        let default = std::num::NonZeroUsize::new(5).unwrap();
        let result = load_non_zero_usize_env(
            key,
            default,
            super::ServerConfigError::MaxShardFiles,
            || super::ServerConfigError::ZeroMaxShardFiles,
        );
        assert!(result.is_err());
        // SAFETY: test-only env var cleanup under serial_test
        remove_env_var(key);
    }

    // ── load_server_config_from_env error paths ────────────────────────────

    #[test]
    #[serial_test::serial]
    fn load_server_config_invalid_public_base_url() {
        // SAFETY: test-only env var manipulation under serial_test
        set_env_var("SHARDLINE_PUBLIC_BASE_URL", "not-a-valid-url");
        // SAFETY: test-only env var manipulation under serial_test
        set_env_var("SHARDLINE_ROOT_DIR", "/tmp/shardline_test");
        let result = super::load_server_config_from_env();
        assert!(matches!(
            result,
            Err(super::ServerConfigError::InvalidPublicBaseUrl(_))
        ));
        // SAFETY: test-only env var cleanup under serial_test
        remove_env_var("SHARDLINE_PUBLIC_BASE_URL");
        // SAFETY: test-only env var cleanup under serial_test
        remove_env_var("SHARDLINE_ROOT_DIR");
    }

    #[test]
    #[serial_test::serial]
    fn load_server_config_zero_max_request_body_bytes() {
        // SAFETY: test-only env var manipulation under serial_test
        set_env_var("SHARDLINE_MAX_REQUEST_BODY_BYTES", "0");
        // SAFETY: test-only env var manipulation under serial_test
        set_env_var("SHARDLINE_ROOT_DIR", "/tmp/shardline_test");
        // SAFETY: test-only env var manipulation under serial_test
        set_env_var("SHARDLINE_PUBLIC_BASE_URL", "http://localhost:8080");
        let result = super::load_server_config_from_env();
        assert!(matches!(
            result,
            Err(super::ServerConfigError::ZeroMaxRequestBodyBytes)
        ));
        // SAFETY: test-only env var cleanup under serial_test
        remove_env_var("SHARDLINE_MAX_REQUEST_BODY_BYTES");
        // SAFETY: test-only env var cleanup under serial_test
        remove_env_var("SHARDLINE_ROOT_DIR");
        // SAFETY: test-only env var cleanup under serial_test
        remove_env_var("SHARDLINE_PUBLIC_BASE_URL");
    }

    #[test]
    #[serial_test::serial]
    fn load_server_config_invalid_max_request_body_bytes() {
        // SAFETY: test-only env var manipulation under serial_test
        set_env_var("SHARDLINE_MAX_REQUEST_BODY_BYTES", "not-a-number");
        // SAFETY: test-only env var manipulation under serial_test
        set_env_var("SHARDLINE_ROOT_DIR", "/tmp/shardline_test");
        // SAFETY: test-only env var manipulation under serial_test
        set_env_var("SHARDLINE_PUBLIC_BASE_URL", "http://localhost:8080");
        let result = super::load_server_config_from_env();
        assert!(result.is_err());
        // SAFETY: test-only env var cleanup under serial_test
        remove_env_var("SHARDLINE_MAX_REQUEST_BODY_BYTES");
        // SAFETY: test-only env var cleanup under serial_test
        remove_env_var("SHARDLINE_ROOT_DIR");
        // SAFETY: test-only env var cleanup under serial_test
        remove_env_var("SHARDLINE_PUBLIC_BASE_URL");
    }

    #[test]
    #[serial_test::serial]
    fn load_server_config_zero_chunk_size() {
        // SAFETY: test-only env var manipulation under serial_test
        set_env_var("SHARDLINE_CHUNK_SIZE_BYTES", "0");
        // SAFETY: test-only env var manipulation under serial_test
        set_env_var("SHARDLINE_ROOT_DIR", "/tmp/shardline_test");
        // SAFETY: test-only env var manipulation under serial_test
        set_env_var("SHARDLINE_PUBLIC_BASE_URL", "http://localhost:8080");
        let result = super::load_server_config_from_env();
        assert!(matches!(
            result,
            Err(super::ServerConfigError::ZeroChunkSize)
        ));
        // SAFETY: test-only env var cleanup under serial_test
        remove_env_var("SHARDLINE_CHUNK_SIZE_BYTES");
        // SAFETY: test-only env var cleanup under serial_test
        remove_env_var("SHARDLINE_ROOT_DIR");
        // SAFETY: test-only env var cleanup under serial_test
        remove_env_var("SHARDLINE_PUBLIC_BASE_URL");
    }

    #[test]
    #[serial_test::serial]
    fn load_server_config_chunk_size_too_large() {
        // 2 GB exceeds MAX_CHUNK_SIZE (1 GB)
        // SAFETY: test-only env var manipulation under serial_test
        set_env_var("SHARDLINE_CHUNK_SIZE_BYTES", "2147483648");
        // SAFETY: test-only env var manipulation under serial_test
        set_env_var("SHARDLINE_ROOT_DIR", "/tmp/shardline_test");
        // SAFETY: test-only env var manipulation under serial_test
        set_env_var("SHARDLINE_PUBLIC_BASE_URL", "http://localhost:8080");
        let result = super::load_server_config_from_env();
        assert!(matches!(
            result,
            Err(super::ServerConfigError::ChunkSizeTooLarge)
        ));
        // SAFETY: test-only env var cleanup under serial_test
        remove_env_var("SHARDLINE_CHUNK_SIZE_BYTES");
        // SAFETY: test-only env var cleanup under serial_test
        remove_env_var("SHARDLINE_ROOT_DIR");
        // SAFETY: test-only env var cleanup under serial_test
        remove_env_var("SHARDLINE_PUBLIC_BASE_URL");
    }

    #[test]
    #[serial_test::serial]
    fn load_server_config_invalid_chunk_size() {
        // SAFETY: test-only env var manipulation under serial_test
        set_env_var("SHARDLINE_CHUNK_SIZE_BYTES", "not-a-size");
        // SAFETY: test-only env var manipulation under serial_test
        set_env_var("SHARDLINE_ROOT_DIR", "/tmp/shardline_test");
        // SAFETY: test-only env var manipulation under serial_test
        set_env_var("SHARDLINE_PUBLIC_BASE_URL", "http://localhost:8080");
        let result = super::load_server_config_from_env();
        assert!(result.is_err());
        // SAFETY: test-only env var cleanup under serial_test
        remove_env_var("SHARDLINE_CHUNK_SIZE_BYTES");
        // SAFETY: test-only env var cleanup under serial_test
        remove_env_var("SHARDLINE_ROOT_DIR");
        // SAFETY: test-only env var cleanup under serial_test
        remove_env_var("SHARDLINE_PUBLIC_BASE_URL");
    }

    #[test]
    #[serial_test::serial]
    fn load_server_config_invalid_server_role() {
        // SAFETY: test-only env var manipulation under serial_test
        set_env_var("SHARDLINE_SERVER_ROLE", "invalid-role");
        // SAFETY: test-only env var manipulation under serial_test
        set_env_var("SHARDLINE_ROOT_DIR", "/tmp/shardline_test");
        // SAFETY: test-only env var manipulation under serial_test
        set_env_var("SHARDLINE_PUBLIC_BASE_URL", "http://localhost:8080");
        let result = super::load_server_config_from_env();
        assert!(matches!(
            result,
            Err(super::ServerConfigError::InvalidServerRole)
        ));
        // SAFETY: test-only env var cleanup under serial_test
        remove_env_var("SHARDLINE_SERVER_ROLE");
        // SAFETY: test-only env var cleanup under serial_test
        remove_env_var("SHARDLINE_ROOT_DIR");
        // SAFETY: test-only env var cleanup under serial_test
        remove_env_var("SHARDLINE_PUBLIC_BASE_URL");
    }

    #[test]
    #[serial_test::serial]
    fn load_server_config_bind_addr_parse_error() {
        // SAFETY: test-only env var manipulation under serial_test
        set_env_var("SHARDLINE_BIND_ADDR", "not-a-valid-addr");
        // SAFETY: test-only env var manipulation under serial_test
        set_env_var("SHARDLINE_ROOT_DIR", "/tmp/shardline_test");
        // SAFETY: test-only env var manipulation under serial_test
        set_env_var("SHARDLINE_PUBLIC_BASE_URL", "http://localhost:8080");
        let result = super::load_server_config_from_env();
        assert!(result.is_err());
        // SAFETY: test-only env var cleanup under serial_test
        remove_env_var("SHARDLINE_BIND_ADDR");
        // SAFETY: test-only env var cleanup under serial_test
        remove_env_var("SHARDLINE_ROOT_DIR");
        // SAFETY: test-only env var cleanup under serial_test
        remove_env_var("SHARDLINE_PUBLIC_BASE_URL");
    }

    #[test]
    #[serial_test::serial]
    fn load_server_config_upload_max_in_flight_zero() {
        // SAFETY: test-only env var manipulation under serial_test
        set_env_var("SHARDLINE_UPLOAD_MAX_IN_FLIGHT_CHUNKS", "0");
        // SAFETY: test-only env var manipulation under serial_test
        set_env_var("SHARDLINE_ROOT_DIR", "/tmp/shardline_test");
        // SAFETY: test-only env var manipulation under serial_test
        set_env_var("SHARDLINE_PUBLIC_BASE_URL", "http://localhost:8080");
        let result = super::load_server_config_from_env();
        assert!(matches!(
            result,
            Err(super::ServerConfigError::ZeroUploadMaxInFlightChunks)
        ));
        // SAFETY: test-only env var cleanup under serial_test
        remove_env_var("SHARDLINE_UPLOAD_MAX_IN_FLIGHT_CHUNKS");
        // SAFETY: test-only env var cleanup under serial_test
        remove_env_var("SHARDLINE_ROOT_DIR");
        // SAFETY: test-only env var cleanup under serial_test
        remove_env_var("SHARDLINE_PUBLIC_BASE_URL");
    }

    #[test]
    #[serial_test::serial]
    fn load_server_config_transfer_max_in_flight_zero() {
        // SAFETY: test-only env var manipulation under serial_test
        set_env_var("SHARDLINE_TRANSFER_MAX_IN_FLIGHT_CHUNKS", "0");
        // SAFETY: test-only env var manipulation under serial_test
        set_env_var("SHARDLINE_ROOT_DIR", "/tmp/shardline_test");
        // SAFETY: test-only env var manipulation under serial_test
        set_env_var("SHARDLINE_PUBLIC_BASE_URL", "http://localhost:8080");
        let result = super::load_server_config_from_env();
        assert!(matches!(
            result,
            Err(super::ServerConfigError::ZeroTransferMaxInFlightChunks)
        ));
        // SAFETY: test-only env var cleanup under serial_test
        remove_env_var("SHARDLINE_TRANSFER_MAX_IN_FLIGHT_CHUNKS");
        // SAFETY: test-only env var cleanup under serial_test
        remove_env_var("SHARDLINE_ROOT_DIR");
        // SAFETY: test-only env var cleanup under serial_test
        remove_env_var("SHARDLINE_PUBLIC_BASE_URL");
    }

    #[test]
    #[serial_test::serial]
    fn load_server_config_missing_server_frontends() {
        // SAFETY: test-only env var manipulation under serial_test
        set_env_var("SHARDLINE_SERVER_FRONTENDS", "");
        // SAFETY: test-only env var manipulation under serial_test
        set_env_var("SHARDLINE_ROOT_DIR", "/tmp/shardline_test");
        // SAFETY: test-only env var manipulation under serial_test
        set_env_var("SHARDLINE_PUBLIC_BASE_URL", "http://localhost:8080");
        let result = super::load_server_config_from_env();
        assert!(matches!(
            result,
            Err(super::ServerConfigError::MissingServerFrontends)
        ));
        // SAFETY: test-only env var cleanup under serial_test
        remove_env_var("SHARDLINE_SERVER_FRONTENDS");
        // SAFETY: test-only env var cleanup under serial_test
        remove_env_var("SHARDLINE_ROOT_DIR");
        // SAFETY: test-only env var cleanup under serial_test
        remove_env_var("SHARDLINE_PUBLIC_BASE_URL");
    }

    #[test]
    #[serial_test::serial]
    fn load_server_config_invalid_server_frontends() {
        // SAFETY: test-only env var manipulation under serial_test
        set_env_var("SHARDLINE_SERVER_FRONTENDS", "invalid-frontend");
        // SAFETY: test-only env var manipulation under serial_test
        set_env_var("SHARDLINE_ROOT_DIR", "/tmp/shardline_test");
        // SAFETY: test-only env var manipulation under serial_test
        set_env_var("SHARDLINE_PUBLIC_BASE_URL", "http://localhost:8080");
        let result = super::load_server_config_from_env();
        assert!(matches!(
            result,
            Err(super::ServerConfigError::InvalidServerFrontend)
        ));
        // SAFETY: test-only env var cleanup under serial_test
        remove_env_var("SHARDLINE_SERVER_FRONTENDS");
        // SAFETY: test-only env var cleanup under serial_test
        remove_env_var("SHARDLINE_ROOT_DIR");
        // SAFETY: test-only env var cleanup under serial_test
        remove_env_var("SHARDLINE_PUBLIC_BASE_URL");
    }

    #[test]
    #[serial_test::serial]
    fn load_server_config_zero_reconstruction_cache_ttl() {
        // SAFETY: test-only env var manipulation under serial_test
        set_env_var("SHARDLINE_RECONSTRUCTION_CACHE_TTL_SECONDS", "0");
        // SAFETY: test-only env var manipulation under serial_test
        set_env_var("SHARDLINE_ROOT_DIR", "/tmp/shardline_test");
        // SAFETY: test-only env var manipulation under serial_test
        set_env_var("SHARDLINE_PUBLIC_BASE_URL", "http://localhost:8080");
        let result = super::load_server_config_from_env();
        assert!(matches!(
            result,
            Err(super::ServerConfigError::ZeroReconstructionCacheTtlSeconds)
        ));
        // SAFETY: test-only env var cleanup under serial_test
        remove_env_var("SHARDLINE_RECONSTRUCTION_CACHE_TTL_SECONDS");
        // SAFETY: test-only env var cleanup under serial_test
        remove_env_var("SHARDLINE_ROOT_DIR");
        // SAFETY: test-only env var cleanup under serial_test
        remove_env_var("SHARDLINE_PUBLIC_BASE_URL");
    }

    #[test]
    #[serial_test::serial]
    fn load_server_config_zero_oci_upload_session_ttl() {
        // SAFETY: test-only env var manipulation under serial_test
        set_env_var("SHARDLINE_OCI_UPLOAD_SESSION_TTL_SECONDS", "0");
        // SAFETY: test-only env var manipulation under serial_test
        set_env_var("SHARDLINE_ROOT_DIR", "/tmp/shardline_test");
        // SAFETY: test-only env var manipulation under serial_test
        set_env_var("SHARDLINE_PUBLIC_BASE_URL", "http://localhost:8080");
        let result = super::load_server_config_from_env();
        assert!(matches!(
            result,
            Err(super::ServerConfigError::ZeroOciUploadSessionTtlSeconds)
        ));
        // SAFETY: test-only env var cleanup under serial_test
        remove_env_var("SHARDLINE_OCI_UPLOAD_SESSION_TTL_SECONDS");
        // SAFETY: test-only env var cleanup under serial_test
        remove_env_var("SHARDLINE_ROOT_DIR");
        // SAFETY: test-only env var cleanup under serial_test
        remove_env_var("SHARDLINE_PUBLIC_BASE_URL");
    }

    #[test]
    #[serial_test::serial]
    fn load_server_config_zero_provider_token_ttl() {
        set_env_var("SHARDLINE_PROVIDER_TOKEN_TTL_SECONDS", "0");
        set_env_var("SHARDLINE_ROOT_DIR", "/tmp/shardline_test");
        set_env_var("SHARDLINE_PUBLIC_BASE_URL", "http://localhost:8080");
        // Provider TTL is only validated when both config + api key paths supplied
        set_env_var(
            "SHARDLINE_PROVIDER_CONFIG_FILE",
            "/tmp/test_provider_config.yml",
        );
        set_env_var(
            "SHARDLINE_PROVIDER_API_KEY_FILE",
            "/tmp/test_provider_api_key",
        );
        // Token signing key required before TTL validation
        set_env_var(
            "SHARDLINE_TOKEN_SIGNING_KEY",
            "test-signing-key-32-bytes-long!!",
        );
        let result = super::load_server_config_from_env();
        assert!(matches!(
            result,
            Err(super::ServerConfigError::ZeroProviderTokenTtl)
        ));
        remove_env_var("SHARDLINE_PROVIDER_TOKEN_TTL_SECONDS");
        remove_env_var("SHARDLINE_ROOT_DIR");
        remove_env_var("SHARDLINE_PUBLIC_BASE_URL");
        remove_env_var("SHARDLINE_PROVIDER_CONFIG_FILE");
        remove_env_var("SHARDLINE_PROVIDER_API_KEY_FILE");
        remove_env_var("SHARDLINE_TOKEN_SIGNING_KEY");
    }

    #[test]
    #[serial_test::serial]
    fn load_server_config_invalid_provider_token_ttl() {
        set_env_var("SHARDLINE_PROVIDER_TOKEN_TTL_SECONDS", "not-a-ttl");
        set_env_var("SHARDLINE_ROOT_DIR", "/tmp/shardline_test");
        set_env_var("SHARDLINE_PUBLIC_BASE_URL", "http://localhost:8080");
        set_env_var(
            "SHARDLINE_PROVIDER_CONFIG_FILE",
            "/tmp/test_provider_config.yml",
        );
        set_env_var(
            "SHARDLINE_PROVIDER_API_KEY_FILE",
            "/tmp/test_provider_api_key",
        );
        set_env_var(
            "SHARDLINE_TOKEN_SIGNING_KEY",
            "test-signing-key-32-bytes-long!!",
        );
        let result = super::load_server_config_from_env();
        assert!(matches!(
            result,
            Err(super::ServerConfigError::ProviderTokenTtl)
        ));
        remove_env_var("SHARDLINE_PROVIDER_TOKEN_TTL_SECONDS");
        remove_env_var("SHARDLINE_ROOT_DIR");
        remove_env_var("SHARDLINE_PUBLIC_BASE_URL");
        remove_env_var("SHARDLINE_PROVIDER_CONFIG_FILE");
        remove_env_var("SHARDLINE_PROVIDER_API_KEY_FILE");
        remove_env_var("SHARDLINE_TOKEN_SIGNING_KEY");
    }

    #[test]
    #[serial_test::serial]
    fn load_server_config_missing_reconstruction_cache_redis_url() {
        // SAFETY: test-only env var manipulation under serial_test
        set_env_var("SHARDLINE_RECONSTRUCTION_CACHE_ADAPTER", "redis");
        // SAFETY: test-only env var manipulation under serial_test
        set_env_var("SHARDLINE_RECONSTRUCTION_CACHE_REDIS_URL", "");
        // SAFETY: test-only env var manipulation under serial_test
        set_env_var("SHARDLINE_ROOT_DIR", "/tmp/shardline_test");
        // SAFETY: test-only env var manipulation under serial_test
        set_env_var("SHARDLINE_PUBLIC_BASE_URL", "http://localhost:8080");
        let result = super::load_server_config_from_env();
        assert!(matches!(
            result,
            Err(super::ServerConfigError::MissingReconstructionCacheRedisUrl)
        ));
        // SAFETY: test-only env var cleanup under serial_test
        remove_env_var("SHARDLINE_RECONSTRUCTION_CACHE_ADAPTER");
        // SAFETY: test-only env var cleanup under serial_test
        remove_env_var("SHARDLINE_RECONSTRUCTION_CACHE_REDIS_URL");
        // SAFETY: test-only env var cleanup under serial_test
        remove_env_var("SHARDLINE_ROOT_DIR");
        // SAFETY: test-only env var cleanup under serial_test
        remove_env_var("SHARDLINE_PUBLIC_BASE_URL");
    }

    // ── load_server_config_from_env: OCI zero-value edge cases ───────────

    #[test]
    #[serial_test::serial]
    fn load_server_config_zero_oci_registry_token_ttl() {
        set_env_var("SHARDLINE_OCI_REGISTRY_TOKEN_TTL_SECONDS", "0");
        set_env_var("SHARDLINE_ROOT_DIR", "/tmp/shardline_test");
        set_env_var("SHARDLINE_PUBLIC_BASE_URL", "http://localhost:8080");
        let result = super::load_server_config_from_env();
        assert!(matches!(
            result,
            Err(super::ServerConfigError::ZeroOciRegistryTokenTtlSeconds)
        ));
        remove_env_var("SHARDLINE_OCI_REGISTRY_TOKEN_TTL_SECONDS");
        remove_env_var("SHARDLINE_ROOT_DIR");
        remove_env_var("SHARDLINE_PUBLIC_BASE_URL");
    }

    #[test]
    #[serial_test::serial]
    fn load_server_config_zero_oci_upload_max_active_sessions() {
        set_env_var("SHARDLINE_OCI_UPLOAD_MAX_ACTIVE_SESSIONS", "0");
        set_env_var("SHARDLINE_ROOT_DIR", "/tmp/shardline_test");
        set_env_var("SHARDLINE_PUBLIC_BASE_URL", "http://localhost:8080");
        let result = super::load_server_config_from_env();
        assert!(matches!(
            result,
            Err(super::ServerConfigError::ZeroOciUploadMaxActiveSessions)
        ));
        remove_env_var("SHARDLINE_OCI_UPLOAD_MAX_ACTIVE_SESSIONS");
        remove_env_var("SHARDLINE_ROOT_DIR");
        remove_env_var("SHARDLINE_PUBLIC_BASE_URL");
    }

    #[test]
    #[serial_test::serial]
    fn load_server_config_zero_oci_registry_token_max_in_flight() {
        set_env_var("SHARDLINE_OCI_REGISTRY_TOKEN_MAX_IN_FLIGHT_REQUESTS", "0");
        set_env_var("SHARDLINE_ROOT_DIR", "/tmp/shardline_test");
        set_env_var("SHARDLINE_PUBLIC_BASE_URL", "http://localhost:8080");
        let result = super::load_server_config_from_env();
        assert!(matches!(
            result,
            Err(super::ServerConfigError::ZeroOciRegistryTokenMaxInFlightRequests)
        ));
        remove_env_var("SHARDLINE_OCI_REGISTRY_TOKEN_MAX_IN_FLIGHT_REQUESTS");
        remove_env_var("SHARDLINE_ROOT_DIR");
        remove_env_var("SHARDLINE_PUBLIC_BASE_URL");
    }

    // ── load_server_config_from_env: auth provider branches ─────────────

    #[test]
    #[serial_test::serial]
    fn load_server_config_oidc_requires_issuer() {
        set_env_var("SHARDLINE_AUTH_PROVIDER", "oidc");
        set_env_var("SHARDLINE_ROOT_DIR", "/tmp/shardline_test");
        set_env_var("SHARDLINE_PUBLIC_BASE_URL", "http://localhost:8080");
        let result = super::load_server_config_from_env();
        assert!(matches!(
            result,
            Err(super::ServerConfigError::MissingOidcIssuer)
        ));
        remove_env_var("SHARDLINE_AUTH_PROVIDER");
        remove_env_var("SHARDLINE_ROOT_DIR");
        remove_env_var("SHARDLINE_PUBLIC_BASE_URL");
    }

    #[test]
    #[serial_test::serial]
    fn load_server_config_oidc_audience_from_env() {
        set_env_var("SHARDLINE_AUTH_PROVIDER", "oidc");
        set_env_var("SHARDLINE_AUTH_OIDC_ISSUER", "https://accounts.example.com");
        set_env_var("SHARDLINE_AUTH_OIDC_AUDIENCE", "shardline-web");
        set_env_var("SHARDLINE_ROOT_DIR", "/tmp/shardline_test");
        set_env_var("SHARDLINE_PUBLIC_BASE_URL", "http://localhost:8080");
        let result = super::load_server_config_from_env();
        assert!(result.is_ok(), "expected Ok, got {result:?}");
        let config = result.unwrap();
        assert_eq!(
            config.auth_oidc_audience(),
            Some("shardline-web"),
            "SHARDLINE_AUTH_OIDC_AUDIENCE must flow into the config"
        );
        remove_env_var("SHARDLINE_AUTH_PROVIDER");
        remove_env_var("SHARDLINE_AUTH_OIDC_ISSUER");
        remove_env_var("SHARDLINE_AUTH_OIDC_AUDIENCE");
        remove_env_var("SHARDLINE_ROOT_DIR");
        remove_env_var("SHARDLINE_PUBLIC_BASE_URL");
    }

    #[test]
    #[serial_test::serial]
    fn load_server_config_oidc_audience_unset_by_default() {
        set_env_var("SHARDLINE_AUTH_PROVIDER", "oidc");
        set_env_var("SHARDLINE_AUTH_OIDC_ISSUER", "https://accounts.example.com");
        remove_env_var("SHARDLINE_AUTH_OIDC_AUDIENCE");
        set_env_var("SHARDLINE_ROOT_DIR", "/tmp/shardline_test");
        set_env_var("SHARDLINE_PUBLIC_BASE_URL", "http://localhost:8080");
        let result = super::load_server_config_from_env();
        assert!(result.is_ok(), "expected Ok, got {result:?}");
        let config = result.unwrap();
        assert_eq!(
            config.auth_oidc_audience(),
            None,
            "audience must default to unset (permissive aud validation)"
        );
        remove_env_var("SHARDLINE_AUTH_PROVIDER");
        remove_env_var("SHARDLINE_AUTH_OIDC_ISSUER");
        remove_env_var("SHARDLINE_ROOT_DIR");
        remove_env_var("SHARDLINE_PUBLIC_BASE_URL");
    }

    #[test]
    #[serial_test::serial]
    fn load_server_config_oidc_audience_empty_behaves_as_unset() {
        // Regression (F-55): SHARDLINE_AUTH_OIDC_AUDIENCE="" must behave
        // exactly like the variable being unset. An empty string used to flow
        // through as `Some("")`, silently enabling aud validation against an
        // empty audience (every aud-bearing token rejected) while the startup
        // aud warning never fired. `config.auth_oidc_audience() == None` is
        // precisely the condition that triggers the app.rs startup warning and
        // the permissive (validate_aud disabled) provider behavior.
        set_env_var("SHARDLINE_AUTH_PROVIDER", "oidc");
        set_env_var("SHARDLINE_AUTH_OIDC_ISSUER", "https://accounts.example.com");
        set_env_var("SHARDLINE_AUTH_OIDC_AUDIENCE", "");
        set_env_var("SHARDLINE_ROOT_DIR", "/tmp/shardline_test");
        set_env_var("SHARDLINE_PUBLIC_BASE_URL", "http://localhost:8080");
        let result = super::load_server_config_from_env();
        assert!(result.is_ok(), "expected Ok, got {result:?}");
        let config = result.unwrap();
        assert_eq!(
            config.auth_oidc_audience(),
            None,
            "an empty audience must behave exactly like unset"
        );
        remove_env_var("SHARDLINE_AUTH_PROVIDER");
        remove_env_var("SHARDLINE_AUTH_OIDC_ISSUER");
        remove_env_var("SHARDLINE_AUTH_OIDC_AUDIENCE");
        remove_env_var("SHARDLINE_ROOT_DIR");
        remove_env_var("SHARDLINE_PUBLIC_BASE_URL");
    }

    #[test]
    #[serial_test::serial]
    fn load_server_config_oidc_audience_whitespace_only_behaves_as_unset() {
        set_env_var("SHARDLINE_AUTH_PROVIDER", "oidc");
        set_env_var("SHARDLINE_AUTH_OIDC_ISSUER", "https://accounts.example.com");
        set_env_var("SHARDLINE_AUTH_OIDC_AUDIENCE", "   ");
        set_env_var("SHARDLINE_ROOT_DIR", "/tmp/shardline_test");
        set_env_var("SHARDLINE_PUBLIC_BASE_URL", "http://localhost:8080");
        let result = super::load_server_config_from_env();
        assert!(result.is_ok(), "expected Ok, got {result:?}");
        let config = result.unwrap();
        assert_eq!(
            config.auth_oidc_audience(),
            None,
            "a whitespace-only audience must behave exactly like unset"
        );
        remove_env_var("SHARDLINE_AUTH_PROVIDER");
        remove_env_var("SHARDLINE_AUTH_OIDC_ISSUER");
        remove_env_var("SHARDLINE_AUTH_OIDC_AUDIENCE");
        remove_env_var("SHARDLINE_ROOT_DIR");
        remove_env_var("SHARDLINE_PUBLIC_BASE_URL");
    }

    #[test]
    #[serial_test::serial]
    fn load_server_config_oidc_audience_whitespace_is_trimmed() {
        set_env_var("SHARDLINE_AUTH_PROVIDER", "oidc");
        set_env_var("SHARDLINE_AUTH_OIDC_ISSUER", "https://accounts.example.com");
        set_env_var("SHARDLINE_AUTH_OIDC_AUDIENCE", "  shardline-web  ");
        set_env_var("SHARDLINE_ROOT_DIR", "/tmp/shardline_test");
        set_env_var("SHARDLINE_PUBLIC_BASE_URL", "http://localhost:8080");
        let result = super::load_server_config_from_env();
        assert!(result.is_ok(), "expected Ok, got {result:?}");
        let config = result.unwrap();
        assert_eq!(
            config.auth_oidc_audience(),
            Some("shardline-web"),
            "surrounding whitespace around the audience must be trimmed"
        );
        remove_env_var("SHARDLINE_AUTH_PROVIDER");
        remove_env_var("SHARDLINE_AUTH_OIDC_ISSUER");
        remove_env_var("SHARDLINE_AUTH_OIDC_AUDIENCE");
        remove_env_var("SHARDLINE_ROOT_DIR");
        remove_env_var("SHARDLINE_PUBLIC_BASE_URL");
    }

    #[test]
    #[serial_test::serial]
    fn load_server_config_oidc_jwks_host_allowlist_from_env() {
        set_env_var("SHARDLINE_AUTH_PROVIDER", "oidc");
        set_env_var("SHARDLINE_AUTH_OIDC_ISSUER", "https://accounts.example.com");
        set_env_var(
            "SHARDLINE_AUTH_OIDC_JWKS_HOST_ALLOWLIST",
            "www.googleapis.com, api.example.com",
        );
        set_env_var("SHARDLINE_ROOT_DIR", "/tmp/shardline_test");
        set_env_var("SHARDLINE_PUBLIC_BASE_URL", "http://localhost:8080");
        let result = super::load_server_config_from_env();
        assert!(result.is_ok(), "expected Ok, got {result:?}");
        let config = result.unwrap();
        assert_eq!(
            config.auth_oidc_jwks_host_allowlist(),
            Some(
                &[
                    "www.googleapis.com".to_owned(),
                    "api.example.com".to_owned()
                ][..]
            ),
            "SHARDLINE_AUTH_OIDC_JWKS_HOST_ALLOWLIST must flow into the config"
        );
        remove_env_var("SHARDLINE_AUTH_PROVIDER");
        remove_env_var("SHARDLINE_AUTH_OIDC_ISSUER");
        remove_env_var("SHARDLINE_AUTH_OIDC_JWKS_HOST_ALLOWLIST");
        remove_env_var("SHARDLINE_ROOT_DIR");
        remove_env_var("SHARDLINE_PUBLIC_BASE_URL");
    }

    #[test]
    #[serial_test::serial]
    fn load_server_config_oidc_jwks_host_allowlist_unset_by_default() {
        set_env_var("SHARDLINE_AUTH_PROVIDER", "oidc");
        set_env_var("SHARDLINE_AUTH_OIDC_ISSUER", "https://accounts.example.com");
        remove_env_var("SHARDLINE_AUTH_OIDC_JWKS_HOST_ALLOWLIST");
        set_env_var("SHARDLINE_ROOT_DIR", "/tmp/shardline_test");
        set_env_var("SHARDLINE_PUBLIC_BASE_URL", "http://localhost:8080");
        let result = super::load_server_config_from_env();
        assert!(result.is_ok(), "expected Ok, got {result:?}");
        let config = result.unwrap();
        assert_eq!(
            config.auth_oidc_jwks_host_allowlist(),
            None,
            "the jwks host allowlist must default to unset (issuer host only)"
        );
        remove_env_var("SHARDLINE_AUTH_PROVIDER");
        remove_env_var("SHARDLINE_AUTH_OIDC_ISSUER");
        remove_env_var("SHARDLINE_ROOT_DIR");
        remove_env_var("SHARDLINE_PUBLIC_BASE_URL");
    }

    #[test]
    #[serial_test::serial]
    fn load_server_config_oidc_jwks_host_allowlist_trims_and_skips_empty() {
        set_env_var("SHARDLINE_AUTH_PROVIDER", "oidc");
        set_env_var("SHARDLINE_AUTH_OIDC_ISSUER", "https://accounts.example.com");
        set_env_var(
            "SHARDLINE_AUTH_OIDC_JWKS_HOST_ALLOWLIST",
            "  ,,www.googleapis.com,,  ",
        );
        set_env_var("SHARDLINE_ROOT_DIR", "/tmp/shardline_test");
        set_env_var("SHARDLINE_PUBLIC_BASE_URL", "http://localhost:8080");
        let result = super::load_server_config_from_env();
        assert!(result.is_ok(), "expected Ok, got {result:?}");
        let config = result.unwrap();
        assert_eq!(
            config.auth_oidc_jwks_host_allowlist(),
            Some(&["www.googleapis.com".to_owned()][..]),
            "empty entries must be skipped and hosts trimmed"
        );
        remove_env_var("SHARDLINE_AUTH_PROVIDER");
        remove_env_var("SHARDLINE_AUTH_OIDC_ISSUER");
        remove_env_var("SHARDLINE_AUTH_OIDC_JWKS_HOST_ALLOWLIST");
        remove_env_var("SHARDLINE_ROOT_DIR");
        remove_env_var("SHARDLINE_PUBLIC_BASE_URL");
    }

    #[test]
    #[serial_test::serial]
    fn load_server_config_oidc_rejects_http_issuer() {
        // Regression (F-64): OIDC issuers must be https (RFC 8414 §2). A
        // plain-http issuer is a startup error rather than a silent downgrade.
        set_env_var("SHARDLINE_AUTH_PROVIDER", "oidc");
        set_env_var("SHARDLINE_AUTH_OIDC_ISSUER", "http://accounts.example.com");
        set_env_var("SHARDLINE_ROOT_DIR", "/tmp/shardline_test");
        set_env_var("SHARDLINE_PUBLIC_BASE_URL", "http://localhost:8080");
        let result = super::load_server_config_from_env();
        assert!(matches!(
            result,
            Err(super::ServerConfigError::OidcIssuerMustUseHttps { .. })
        ));
        remove_env_var("SHARDLINE_AUTH_PROVIDER");
        remove_env_var("SHARDLINE_AUTH_OIDC_ISSUER");
        remove_env_var("SHARDLINE_ROOT_DIR");
        remove_env_var("SHARDLINE_PUBLIC_BASE_URL");
    }

    #[test]
    #[serial_test::serial]
    fn load_server_config_oidc_accepts_https_issuer() {
        set_env_var("SHARDLINE_AUTH_PROVIDER", "oidc");
        set_env_var("SHARDLINE_AUTH_OIDC_ISSUER", "https://accounts.example.com");
        set_env_var("SHARDLINE_ROOT_DIR", "/tmp/shardline_test");
        set_env_var("SHARDLINE_PUBLIC_BASE_URL", "http://localhost:8080");
        let result = super::load_server_config_from_env();
        assert!(result.is_ok(), "expected Ok, got {result:?}");
        let config = result.unwrap();
        assert_eq!(
            config.auth_oidc_issuer(),
            Some("https://accounts.example.com"),
            "an https issuer must be accepted"
        );
        remove_env_var("SHARDLINE_AUTH_PROVIDER");
        remove_env_var("SHARDLINE_AUTH_OIDC_ISSUER");
        remove_env_var("SHARDLINE_ROOT_DIR");
        remove_env_var("SHARDLINE_PUBLIC_BASE_URL");
    }

    #[test]
    #[serial_test::serial]
    fn load_server_config_jwks_requires_url() {
        set_env_var("SHARDLINE_AUTH_PROVIDER", "jwks");
        set_env_var("SHARDLINE_ROOT_DIR", "/tmp/shardline_test");
        set_env_var("SHARDLINE_PUBLIC_BASE_URL", "http://localhost:8080");
        let result = super::load_server_config_from_env();
        assert!(matches!(
            result,
            Err(super::ServerConfigError::MissingJwksUrl)
        ));
        remove_env_var("SHARDLINE_AUTH_PROVIDER");
        remove_env_var("SHARDLINE_ROOT_DIR");
        remove_env_var("SHARDLINE_PUBLIC_BASE_URL");
    }

    #[test]
    #[serial_test::serial]
    fn load_server_config_lower_max_request_body_bytes_parse_error() {
        set_env_var("SHARDLINE_MAX_REQUEST_BODY_BYTES", "0");
        set_env_var("SHARDLINE_ROOT_DIR", "/tmp/shardline_test");
        set_env_var("SHARDLINE_PUBLIC_BASE_URL", "http://localhost:8080");
        let result = super::load_server_config_from_env();
        assert!(matches!(
            result,
            Err(super::ServerConfigError::ZeroMaxRequestBodyBytes)
        ));
        remove_env_var("SHARDLINE_MAX_REQUEST_BODY_BYTES");
        remove_env_var("SHARDLINE_ROOT_DIR");
        remove_env_var("SHARDLINE_PUBLIC_BASE_URL");
    }

    #[test]
    #[serial_test::serial]
    fn load_server_config_zero_shard_metadata_limits() {
        set_env_var("SHARDLINE_MAX_SHARD_FILES", "0");
        set_env_var("SHARDLINE_ROOT_DIR", "/tmp/shardline_test");
        set_env_var("SHARDLINE_PUBLIC_BASE_URL", "http://localhost:8080");
        let result = super::load_server_config_from_env();
        assert!(matches!(
            result,
            Err(super::ServerConfigError::ZeroMaxShardFiles)
        ));
        remove_env_var("SHARDLINE_MAX_SHARD_FILES");
        remove_env_var("SHARDLINE_ROOT_DIR");
        remove_env_var("SHARDLINE_PUBLIC_BASE_URL");
    }

    #[test]
    #[serial_test::serial]
    fn load_server_config_zero_shard_xorbs() {
        set_env_var("SHARDLINE_MAX_SHARD_XORBS", "0");
        set_env_var("SHARDLINE_ROOT_DIR", "/tmp/shardline_test");
        set_env_var("SHARDLINE_PUBLIC_BASE_URL", "http://localhost:8080");
        let result = super::load_server_config_from_env();
        assert!(matches!(
            result,
            Err(super::ServerConfigError::ZeroMaxShardXorbs)
        ));
        remove_env_var("SHARDLINE_MAX_SHARD_XORBS");
        remove_env_var("SHARDLINE_ROOT_DIR");
        remove_env_var("SHARDLINE_PUBLIC_BASE_URL");
    }

    #[test]
    #[serial_test::serial]
    fn load_server_config_zero_shard_reconstruction_terms() {
        set_env_var("SHARDLINE_MAX_SHARD_RECONSTRUCTION_TERMS", "0");
        set_env_var("SHARDLINE_ROOT_DIR", "/tmp/shardline_test");
        set_env_var("SHARDLINE_PUBLIC_BASE_URL", "http://localhost:8080");
        let result = super::load_server_config_from_env();
        assert!(matches!(
            result,
            Err(super::ServerConfigError::ZeroMaxShardReconstructionTerms)
        ));
        remove_env_var("SHARDLINE_MAX_SHARD_RECONSTRUCTION_TERMS");
        remove_env_var("SHARDLINE_ROOT_DIR");
        remove_env_var("SHARDLINE_PUBLIC_BASE_URL");
    }

    #[test]
    #[serial_test::serial]
    fn load_server_config_zero_shard_xorb_chunks() {
        set_env_var("SHARDLINE_MAX_SHARD_XORB_CHUNKS", "0");
        set_env_var("SHARDLINE_ROOT_DIR", "/tmp/shardline_test");
        set_env_var("SHARDLINE_PUBLIC_BASE_URL", "http://localhost:8080");
        let result = super::load_server_config_from_env();
        assert!(matches!(
            result,
            Err(super::ServerConfigError::ZeroMaxShardXorbChunks)
        ));
        remove_env_var("SHARDLINE_MAX_SHARD_XORB_CHUNKS");
        remove_env_var("SHARDLINE_ROOT_DIR");
        remove_env_var("SHARDLINE_PUBLIC_BASE_URL");
    }

    #[test]
    #[serial_test::serial]
    fn load_server_config_zero_reconstruction_cache_memory_max_entries() {
        set_env_var("SHARDLINE_RECONSTRUCTION_CACHE_MEMORY_MAX_ENTRIES", "0");
        set_env_var("SHARDLINE_ROOT_DIR", "/tmp/shardline_test");
        set_env_var("SHARDLINE_PUBLIC_BASE_URL", "http://localhost:8080");
        let result = super::load_server_config_from_env();
        assert!(matches!(
            result,
            Err(super::ServerConfigError::ZeroReconstructionCacheMemoryMaxEntries)
        ));
        remove_env_var("SHARDLINE_RECONSTRUCTION_CACHE_MEMORY_MAX_ENTRIES");
        remove_env_var("SHARDLINE_ROOT_DIR");
        remove_env_var("SHARDLINE_PUBLIC_BASE_URL");
    }

    #[test]
    #[serial_test::serial]
    fn load_server_config_hub_requires_auth() {
        set_env_var("SHARDLINE_SERVER_FRONTENDS", "hub");
        set_env_var("SHARDLINE_AUTH_PROVIDER", "local");
        set_env_var("SHARDLINE_ROOT_DIR", "/tmp/shardline_test");
        set_env_var("SHARDLINE_PUBLIC_BASE_URL", "http://localhost:8080");
        // No token signing key set — with Local auth, hub requires signing key
        let result = super::load_server_config_from_env();
        assert!(matches!(
            result,
            Err(super::ServerConfigError::HubRequiresAuth)
        ));
        remove_env_var("SHARDLINE_SERVER_FRONTENDS");
        remove_env_var("SHARDLINE_AUTH_PROVIDER");
        remove_env_var("SHARDLINE_ROOT_DIR");
        remove_env_var("SHARDLINE_PUBLIC_BASE_URL");
    }

    #[test]
    #[serial_test::serial]
    fn load_server_config_invalid_auth_provider() {
        set_env_var("SHARDLINE_AUTH_PROVIDER", "invalid");
        set_env_var("SHARDLINE_ROOT_DIR", "/tmp/shardline_test");
        set_env_var("SHARDLINE_PUBLIC_BASE_URL", "http://localhost:8080");
        let result = super::load_server_config_from_env();
        assert!(matches!(
            result,
            Err(super::ServerConfigError::InvalidAuthProvider)
        ));
        remove_env_var("SHARDLINE_AUTH_PROVIDER");
        remove_env_var("SHARDLINE_ROOT_DIR");
        remove_env_var("SHARDLINE_PUBLIC_BASE_URL");
    }

    // ── load_server_config_from_env: empty index postgres url ─────────────

    #[test]
    #[serial_test::serial]
    fn load_server_config_empty_index_postgres_url() {
        // SAFETY: test-only env var manipulation under serial_test
        set_env_var("SHARDLINE_INDEX_POSTGRES_URL", "");
        set_env_var("SHARDLINE_ROOT_DIR", "/tmp/shardline_test");
        set_env_var("SHARDLINE_PUBLIC_BASE_URL", "http://localhost:8080");
        let result = super::load_server_config_from_env();
        assert!(
            matches!(result, Err(super::ServerConfigError::EmptyIndexPostgresUrl)),
            "expected EmptyIndexPostgresUrl, got {result:?}"
        );
        // SAFETY: test-only env var cleanup under serial_test
        remove_env_var("SHARDLINE_INDEX_POSTGRES_URL");
        remove_env_var("SHARDLINE_ROOT_DIR");
        remove_env_var("SHARDLINE_PUBLIC_BASE_URL");
    }

    // ── load_server_config_from_env: whitespace reconstruction cache redis url ─

    #[test]
    #[serial_test::serial]
    fn load_server_config_whitespace_reconstruction_cache_redis_url() {
        // SAFETY: test-only env var manipulation under serial_test
        set_env_var("SHARDLINE_RECONSTRUCTION_CACHE_ADAPTER", "redis");
        set_env_var("SHARDLINE_RECONSTRUCTION_CACHE_REDIS_URL", "   ");
        set_env_var("SHARDLINE_ROOT_DIR", "/tmp/shardline_test");
        set_env_var("SHARDLINE_PUBLIC_BASE_URL", "http://localhost:8080");
        let result = super::load_server_config_from_env();
        assert!(
            matches!(
                result,
                Err(super::ServerConfigError::MissingReconstructionCacheRedisUrl)
            ),
            "expected MissingReconstructionCacheRedisUrl, got {result:?}"
        );
        // SAFETY: test-only env var cleanup under serial_test
        remove_env_var("SHARDLINE_RECONSTRUCTION_CACHE_ADAPTER");
        remove_env_var("SHARDLINE_RECONSTRUCTION_CACHE_REDIS_URL");
        remove_env_var("SHARDLINE_ROOT_DIR");
        remove_env_var("SHARDLINE_PUBLIC_BASE_URL");
    }

    // ── load_server_config_from_env: empty provider token issuer ──────────

    #[test]
    #[serial_test::serial]
    fn load_server_config_empty_provider_token_issuer() {
        // SAFETY: test-only env var and tempfile under serial_test
        use std::io::Write;

        // Provider config file and api key file must exist for the
        // validation to reach with_provider_runtime.
        let mut api_key_file = tempfile::NamedTempFile::new().unwrap();
        api_key_file.write_all(b"valid-api-key").unwrap();
        api_key_file.flush().unwrap();
        let mut config_file = tempfile::NamedTempFile::new().unwrap();
        config_file.write_all(b"config: {}").unwrap();
        config_file.flush().unwrap();

        set_env_var("SHARDLINE_PROVIDER_TOKEN_ISSUER", "");
        set_env_var(
            "SHARDLINE_PROVIDER_CONFIG_FILE",
            config_file.path().to_str().unwrap(),
        );
        set_env_var(
            "SHARDLINE_PROVIDER_API_KEY_FILE",
            api_key_file.path().to_str().unwrap(),
        );
        set_env_var(
            "SHARDLINE_TOKEN_SIGNING_KEY",
            "test-signing-key-32-bytes-long!!",
        );
        set_env_var("SHARDLINE_PROVIDER_TOKEN_TTL_SECONDS", "300");
        set_env_var("SHARDLINE_ROOT_DIR", "/tmp/shardline_test");
        set_env_var("SHARDLINE_PUBLIC_BASE_URL", "http://localhost:8080");
        let result = super::load_server_config_from_env();
        assert!(
            matches!(
                result,
                Err(super::ServerConfigError::EmptyProviderTokenIssuer)
            ),
            "expected EmptyProviderTokenIssuer, got {result:?}"
        );
        // SAFETY: test-only env var cleanup under serial_test
        remove_env_var("SHARDLINE_PROVIDER_TOKEN_ISSUER");
        remove_env_var("SHARDLINE_PROVIDER_CONFIG_FILE");
        remove_env_var("SHARDLINE_PROVIDER_API_KEY_FILE");
        remove_env_var("SHARDLINE_TOKEN_SIGNING_KEY");
        remove_env_var("SHARDLINE_PROVIDER_TOKEN_TTL_SECONDS");
        remove_env_var("SHARDLINE_ROOT_DIR");
        remove_env_var("SHARDLINE_PUBLIC_BASE_URL");
    }

    // ── load_server_config_from_env: Ed25519 auth provider ───────────────

    #[test]
    #[serial_test::serial]
    fn env_ed25519_private_key_from_direct_env() {
        remove_env_var("SHARDLINE_ED25519_PRIVATE_KEY_FILE");
        remove_env_var("SHARDLINE_ED25519_PUBLIC_KEY");
        remove_env_var("SHARDLINE_ED25519_PUBLIC_KEY_FILE");
        set_env_var("SHARDLINE_ED25519_PRIVATE_KEY", &hex::encode([0u8; 32]));
        set_env_var("SHARDLINE_AUTH_PROVIDER", "ed25519");
        set_env_var("SHARDLINE_ROOT_DIR", "/tmp/shardline-test");
        set_env_var("SHARDLINE_PUBLIC_BASE_URL", "http://localhost:8080");

        let config = super::load_server_config_from_env()
            .expect("config should load with Ed25519 private key");
        assert_eq!(config.auth_provider(), super::AuthProviderKind::Ed25519);
        assert!(config.ed25519_private_key().is_some());

        remove_env_var("SHARDLINE_ED25519_PRIVATE_KEY");
        remove_env_var("SHARDLINE_AUTH_PROVIDER");
        remove_env_var("SHARDLINE_ROOT_DIR");
        remove_env_var("SHARDLINE_PUBLIC_BASE_URL");
    }

    #[test]
    #[serial_test::serial]
    fn env_ed25519_private_key_from_file_env() {
        remove_env_var("SHARDLINE_ED25519_PRIVATE_KEY");
        remove_env_var("SHARDLINE_ED25519_PUBLIC_KEY");
        remove_env_var("SHARDLINE_ED25519_PUBLIC_KEY_FILE");
        let tmp = tempfile::NamedTempFile::new().expect("temp file");
        std::fs::write(tmp.path(), [0u8; 32]).expect("write key");
        set_env_var(
            "SHARDLINE_ED25519_PRIVATE_KEY_FILE",
            tmp.path().to_str().unwrap(),
        );
        set_env_var("SHARDLINE_AUTH_PROVIDER", "ed25519");
        set_env_var("SHARDLINE_ROOT_DIR", "/tmp/shardline-test");
        set_env_var("SHARDLINE_PUBLIC_BASE_URL", "http://localhost:8080");

        let config = super::load_server_config_from_env()
            .expect("config should load with Ed25519 private key file");
        assert_eq!(config.auth_provider(), super::AuthProviderKind::Ed25519);
        assert!(config.ed25519_private_key().is_some());

        remove_env_var("SHARDLINE_ED25519_PRIVATE_KEY_FILE");
        remove_env_var("SHARDLINE_AUTH_PROVIDER");
        remove_env_var("SHARDLINE_ROOT_DIR");
        remove_env_var("SHARDLINE_PUBLIC_BASE_URL");
    }

    #[test]
    #[serial_test::serial]
    fn env_ed25519_public_key_from_direct_env() {
        remove_env_var("SHARDLINE_ED25519_PRIVATE_KEY");
        remove_env_var("SHARDLINE_ED25519_PRIVATE_KEY_FILE");
        remove_env_var("SHARDLINE_ED25519_PUBLIC_KEY_FILE");
        set_env_var("SHARDLINE_ED25519_PUBLIC_KEY", &hex::encode([0u8; 32]));
        set_env_var("SHARDLINE_AUTH_PROVIDER", "ed25519");
        set_env_var("SHARDLINE_ROOT_DIR", "/tmp/shardline-test");
        set_env_var("SHARDLINE_PUBLIC_BASE_URL", "http://localhost:8080");

        let config = super::load_server_config_from_env()
            .expect("config should load with Ed25519 public key");
        assert_eq!(config.auth_provider(), super::AuthProviderKind::Ed25519);
        assert!(config.ed25519_public_key().is_some());
        assert!(config.ed25519_private_key().is_none());

        remove_env_var("SHARDLINE_ED25519_PUBLIC_KEY");
        remove_env_var("SHARDLINE_AUTH_PROVIDER");
        remove_env_var("SHARDLINE_ROOT_DIR");
        remove_env_var("SHARDLINE_PUBLIC_BASE_URL");
    }

    #[test]
    #[serial_test::serial]
    fn env_ed25519_missing_both_keys_errors() {
        remove_env_var("SHARDLINE_ED25519_PRIVATE_KEY");
        remove_env_var("SHARDLINE_ED25519_PRIVATE_KEY_FILE");
        remove_env_var("SHARDLINE_ED25519_PUBLIC_KEY");
        remove_env_var("SHARDLINE_ED25519_PUBLIC_KEY_FILE");
        set_env_var("SHARDLINE_AUTH_PROVIDER", "ed25519");
        set_env_var("SHARDLINE_ROOT_DIR", "/tmp/shardline-test");
        set_env_var("SHARDLINE_PUBLIC_BASE_URL", "http://localhost:8080");

        let result = super::load_server_config_from_env();
        assert!(
            matches!(result, Err(super::ServerConfigError::MissingEd25519Key)),
            "expected MissingEd25519Key, got: {:?}",
            result.err()
        );

        remove_env_var("SHARDLINE_AUTH_PROVIDER");
        remove_env_var("SHARDLINE_ROOT_DIR");
        remove_env_var("SHARDLINE_PUBLIC_BASE_URL");
    }

    #[test]
    #[serial_test::serial]
    fn env_ed25519_private_and_public_key_conflict_errors() {
        remove_env_var("SHARDLINE_ED25519_PRIVATE_KEY_FILE");
        remove_env_var("SHARDLINE_ED25519_PUBLIC_KEY_FILE");
        set_env_var("SHARDLINE_ED25519_PRIVATE_KEY", &hex::encode([1_u8; 32]));
        set_env_var("SHARDLINE_ED25519_PUBLIC_KEY", &hex::encode([2_u8; 32]));
        set_env_var("SHARDLINE_AUTH_PROVIDER", "ed25519");

        let result = super::load_server_config_from_env();
        assert!(matches!(
            result,
            Err(super::ServerConfigError::ConflictingEd25519Keys)
        ));

        remove_env_var("SHARDLINE_ED25519_PRIVATE_KEY");
        remove_env_var("SHARDLINE_ED25519_PUBLIC_KEY");
        remove_env_var("SHARDLINE_AUTH_PROVIDER");
    }

    // ── load_server_config_from_env: end-to-end integration ───────────────

    #[test]
    #[serial_test::serial]
    fn load_server_config_integration_end_to_end() {
        // SAFETY: test-only env var manipulation under serial_test
        set_env_var("SHARDLINE_BIND_ADDR", "127.0.0.1:9090");
        set_env_var("SHARDLINE_PUBLIC_BASE_URL", "https://example.com:9090");
        set_env_var("SHARDLINE_SERVER_ROLE", "all");
        set_env_var("SHARDLINE_SERVER_FRONTENDS", "xet,lfs,oci");
        set_env_var("SHARDLINE_ROOT_DIR", "/tmp/shardline_e2e");
        set_env_var("SHARDLINE_OBJECT_STORAGE_ADAPTER", "local");
        set_env_var("SHARDLINE_MAX_REQUEST_BODY_BYTES", "2097152");
        set_env_var("SHARDLINE_CHUNK_SIZE_BYTES", "65536");
        set_env_var("SHARDLINE_UPLOAD_MAX_IN_FLIGHT_CHUNKS", "256");
        set_env_var("SHARDLINE_TRANSFER_MAX_IN_FLIGHT_CHUNKS", "128");
        set_env_var("SHARDLINE_RECONSTRUCTION_CACHE_ADAPTER", "memory");
        set_env_var("SHARDLINE_RECONSTRUCTION_CACHE_TTL_SECONDS", "60");
        set_env_var("SHARDLINE_RECONSTRUCTION_CACHE_MEMORY_MAX_ENTRIES", "8192");
        set_env_var("SHARDLINE_OCI_UPLOAD_SESSION_TTL_SECONDS", "7200");
        set_env_var("SHARDLINE_OCI_UPLOAD_MAX_ACTIVE_SESSIONS", "500");
        set_env_var("SHARDLINE_OCI_REGISTRY_TOKEN_TTL_SECONDS", "600");
        set_env_var("SHARDLINE_OCI_REGISTRY_TOKEN_MAX_IN_FLIGHT_REQUESTS", "128");
        set_env_var("SHARDLINE_MAX_SHARD_FILES", "1000");
        set_env_var("SHARDLINE_MAX_SHARD_XORBS", "1000");
        set_env_var("SHARDLINE_MAX_SHARD_RECONSTRUCTION_TERMS", "5000");
        set_env_var("SHARDLINE_MAX_SHARD_XORB_CHUNKS", "5000");
        set_env_var("SHARDLINE_AUTH_PROVIDER", "oidc");
        set_env_var("SHARDLINE_AUTH_OIDC_ISSUER", "https://accounts.example.com");
        set_env_var(
            "SHARDLINE_TOKEN_SIGNING_KEY",
            "test-signing-key-32-bytes-long!!",
        );

        let result = super::load_server_config_from_env();
        assert!(result.is_ok(), "expected Ok, got {result:?}");
        let config = result.unwrap();

        assert_eq!(config.bind_addr().to_string(), "127.0.0.1:9090");
        assert_eq!(config.public_base_url(), "https://example.com:9090");
        assert_eq!(config.server_role(), crate::ServerRole::All);
        assert_eq!(config.server_frontends().len(), 3);
        assert!(
            config
                .server_frontends()
                .contains(&crate::ServerFrontend::Xet)
        );
        assert!(
            config
                .server_frontends()
                .contains(&crate::ServerFrontend::Lfs)
        );
        assert!(
            config
                .server_frontends()
                .contains(&crate::ServerFrontend::Oci)
        );
        assert_eq!(
            config.root_dir(),
            std::path::Path::new("/tmp/shardline_e2e")
        );
        assert_eq!(
            config.object_storage_adapter(),
            super::ObjectStorageAdapter::Local
        );
        assert!(config.s3_object_store_config().is_none());
        assert_eq!(config.auth_provider(), super::AuthProviderKind::Oidc);
        assert_eq!(
            config.auth_oidc_issuer(),
            Some("https://accounts.example.com")
        );
        assert!(config.token_signing_key().is_some());

        // SAFETY: test-only env var cleanup under serial_test
        remove_env_var("SHARDLINE_BIND_ADDR");
        remove_env_var("SHARDLINE_PUBLIC_BASE_URL");
        remove_env_var("SHARDLINE_SERVER_ROLE");
        remove_env_var("SHARDLINE_SERVER_FRONTENDS");
        remove_env_var("SHARDLINE_ROOT_DIR");
        remove_env_var("SHARDLINE_OBJECT_STORAGE_ADAPTER");
        remove_env_var("SHARDLINE_MAX_REQUEST_BODY_BYTES");
        remove_env_var("SHARDLINE_CHUNK_SIZE_BYTES");
        remove_env_var("SHARDLINE_UPLOAD_MAX_IN_FLIGHT_CHUNKS");
        remove_env_var("SHARDLINE_TRANSFER_MAX_IN_FLIGHT_CHUNKS");
        remove_env_var("SHARDLINE_RECONSTRUCTION_CACHE_ADAPTER");
        remove_env_var("SHARDLINE_RECONSTRUCTION_CACHE_TTL_SECONDS");
        remove_env_var("SHARDLINE_RECONSTRUCTION_CACHE_MEMORY_MAX_ENTRIES");
        remove_env_var("SHARDLINE_OCI_UPLOAD_SESSION_TTL_SECONDS");
        remove_env_var("SHARDLINE_OCI_UPLOAD_MAX_ACTIVE_SESSIONS");
        remove_env_var("SHARDLINE_OCI_REGISTRY_TOKEN_TTL_SECONDS");
        remove_env_var("SHARDLINE_OCI_REGISTRY_TOKEN_MAX_IN_FLIGHT_REQUESTS");
        remove_env_var("SHARDLINE_MAX_SHARD_FILES");
        remove_env_var("SHARDLINE_MAX_SHARD_XORBS");
        remove_env_var("SHARDLINE_MAX_SHARD_RECONSTRUCTION_TERMS");
        remove_env_var("SHARDLINE_MAX_SHARD_XORB_CHUNKS");
        remove_env_var("SHARDLINE_AUTH_PROVIDER");
        remove_env_var("SHARDLINE_AUTH_OIDC_ISSUER");
        remove_env_var("SHARDLINE_TOKEN_SIGNING_KEY");
    }

    #[test]
    #[serial_test::serial]
    fn toml_ed25519_section_maps_to_env_vars() {
        use std::io::Write;

        // Clean relevant env vars
        for key in &[
            "SHARDLINE_ED25519_PRIVATE_KEY_FILE",
            "SHARDLINE_ED25519_PUBLIC_KEY_FILE",
            "SHARDLINE_ED25519_PRIVATE_KEY",
            "SHARDLINE_ED25519_PUBLIC_KEY",
            "SHARDLINE_AUTH_PROVIDER",
            "SHARDLINE_TOKEN_SIGNING_KEY",
        ] {
            remove_env_var(key);
        }

        // Create temporary key files so the config loader can read them.
        let mut priv_key_file = tempfile::NamedTempFile::new().expect("temp private key");
        priv_key_file
            .write_all(&[0u8; 32])
            .expect("write private key");
        priv_key_file.flush().expect("flush");
        let priv_path = priv_key_file.path().to_str().unwrap().to_owned();

        let toml_content = format!(
            r#"
[auth]
provider = "ed25519"

[auth.ed25519]
private_key_path = "{priv_path}"
"#
        );

        let toml: super::ShardlineTomlConfig =
            toml::from_str(&toml_content).expect("TOML should parse");

        set_env_var("SHARDLINE_PUBLIC_BASE_URL", "http://localhost:8080");
        let _config = load_server_config_from_env_with_toml(&toml)
            .expect("config should load with ed25519 TOML section");

        // The TOML values should be set as env vars for downstream loading
        assert_eq!(
            std::env::var("SHARDLINE_ED25519_PRIVATE_KEY_FILE").as_deref(),
            Ok(priv_path.as_str())
        );
        assert!(std::env::var("SHARDLINE_ED25519_PUBLIC_KEY_FILE").is_err());

        // Clean up
        remove_env_var("SHARDLINE_ED25519_PRIVATE_KEY_FILE");
        remove_env_var("SHARDLINE_ED25519_PUBLIC_KEY_FILE");
        remove_env_var("SHARDLINE_PUBLIC_BASE_URL");
    }
}
