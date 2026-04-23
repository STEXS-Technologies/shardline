use std::{
    env::var,
    num::{NonZeroU64, NonZeroUsize, ParseIntError},
    path::{Path, PathBuf},
};

use shardline_protocol::SecretString;

use super::secrets::{
    configure_provider_runtime_from_paths, load_s3_object_store_config_from_env,
    read_secret_file_bytes,
};
use super::{
    DEFAULT_MAX_REQUEST_BODY_BYTES, DEFAULT_MAX_SHARD_FILES,
    DEFAULT_MAX_SHARD_RECONSTRUCTION_TERMS, DEFAULT_MAX_SHARD_XORB_CHUNKS, DEFAULT_MAX_SHARD_XORBS,
    MAX_METRICS_TOKEN_BYTES, MAX_TOKEN_SIGNING_KEY_BYTES, ObjectStorageAdapter, ServerConfig,
    ServerConfigError, ShardMetadataLimits, default_transfer_max_in_flight_chunks,
    default_upload_max_in_flight_chunks,
};
use crate::{reconstruction_cache::ReconstructionCacheAdapter, server_role::ServerRole};

pub(super) fn load_server_config_from_env() -> Result<ServerConfig, ServerConfigError> {
    let bind_addr = match var("SHARDLINE_BIND_ADDR") {
        Ok(value) => value.parse()?,
        Err(_error) => "0.0.0.0:8080".parse()?,
    };
    let public_base_url = var("SHARDLINE_PUBLIC_BASE_URL")
        .unwrap_or_else(|_error| "http://127.0.0.1:8080".to_owned());
    let server_role =
        ServerRole::parse(&var("SHARDLINE_SERVER_ROLE").unwrap_or_else(|_error| "all".to_owned()))
            .map_err(|_error| ServerConfigError::InvalidServerRole)?;
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
    let raw_chunk_size = var("SHARDLINE_CHUNK_SIZE_BYTES")
        .unwrap_or_else(|_error| "65536".to_owned())
        .parse::<usize>()?;
    let Some(chunk_size) = NonZeroUsize::new(raw_chunk_size) else {
        return Err(ServerConfigError::ZeroChunkSize);
    };
    let raw_upload_max_in_flight_chunks = var("SHARDLINE_UPLOAD_MAX_IN_FLIGHT_CHUNKS")
        .unwrap_or_else(|_error| default_upload_max_in_flight_chunks().get().to_string())
        .parse::<usize>()?;
    let Some(upload_max_in_flight_chunks) = NonZeroUsize::new(raw_upload_max_in_flight_chunks)
    else {
        return Err(ServerConfigError::ZeroUploadMaxInFlightChunks);
    };
    let raw_transfer_max_in_flight_chunks = var("SHARDLINE_TRANSFER_MAX_IN_FLIGHT_CHUNKS")
        .unwrap_or_else(|_error| default_transfer_max_in_flight_chunks().get().to_string())
        .parse::<usize>()?;
    let Some(transfer_max_in_flight_chunks) = NonZeroUsize::new(raw_transfer_max_in_flight_chunks)
    else {
        return Err(ServerConfigError::ZeroTransferMaxInFlightChunks);
    };
    let reconstruction_cache_adapter = ReconstructionCacheAdapter::parse(
        &var("SHARDLINE_RECONSTRUCTION_CACHE_ADAPTER").unwrap_or_else(|_error| "memory".to_owned()),
    )?;
    let raw_reconstruction_cache_ttl_seconds = var("SHARDLINE_RECONSTRUCTION_CACHE_TTL_SECONDS")
        .unwrap_or_else(|_error| "30".to_owned())
        .parse::<u64>()?;
    let Some(reconstruction_cache_ttl_seconds) =
        NonZeroU64::new(raw_reconstruction_cache_ttl_seconds)
    else {
        return Err(ServerConfigError::ZeroReconstructionCacheTtlSeconds);
    };
    let raw_reconstruction_cache_memory_max_entries =
        var("SHARDLINE_RECONSTRUCTION_CACHE_MEMORY_MAX_ENTRIES")
            .unwrap_or_else(|_error| "4096".to_owned())
            .parse::<usize>()?;
    let Some(reconstruction_cache_memory_max_entries) =
        NonZeroUsize::new(raw_reconstruction_cache_memory_max_entries)
    else {
        return Err(ServerConfigError::ZeroReconstructionCacheMemoryMaxEntries);
    };
    let reconstruction_cache_redis_url = var("SHARDLINE_RECONSTRUCTION_CACHE_REDIS_URL").ok();
    let index_postgres_url = var("SHARDLINE_INDEX_POSTGRES_URL").ok();
    let token_signing_key = optional_token_signing_key_from_sources(
        var("SHARDLINE_TOKEN_SIGNING_KEY").ok(),
        var("SHARDLINE_TOKEN_SIGNING_KEY_FILE").ok(),
    )?;
    let metrics_token = match var("SHARDLINE_METRICS_TOKEN_FILE") {
        Ok(path) => Some(read_secret_file_bytes(
            Path::new(&path),
            MAX_METRICS_TOKEN_BYTES,
            ServerConfigError::MetricsToken,
            |observed_bytes, maximum_bytes| ServerConfigError::MetricsTokenTooLarge {
                observed_bytes,
                maximum_bytes,
            },
            |expected_bytes, observed_bytes| ServerConfigError::MetricsTokenLengthMismatch {
                expected_bytes,
                observed_bytes,
            },
        )?),
        Err(_error) => None,
    };
    let provider_config_path = var("SHARDLINE_PROVIDER_CONFIG_FILE")
        .ok()
        .map(PathBuf::from);
    let provider_api_key_path = var("SHARDLINE_PROVIDER_API_KEY_FILE")
        .ok()
        .map(PathBuf::from);

    let mut config = ServerConfig::new(bind_addr, public_base_url, root_dir, chunk_size)
        .with_server_role(server_role)
        .with_object_storage(object_storage_adapter, s3_object_store_config)
        .with_max_request_body_bytes(max_request_body_bytes)
        .with_shard_metadata_limits(shard_metadata_limits)
        .with_upload_max_in_flight_chunks(upload_max_in_flight_chunks)
        .with_transfer_max_in_flight_chunks(transfer_max_in_flight_chunks)
        .with_reconstruction_cache_memory(
            reconstruction_cache_ttl_seconds,
            reconstruction_cache_memory_max_entries,
        );
    config.reconstruction_cache_adapter = reconstruction_cache_adapter;
    config.reconstruction_cache_redis_url = reconstruction_cache_redis_url.map(SecretString::new);
    if config.reconstruction_cache_adapter == ReconstructionCacheAdapter::Redis
        && config
            .reconstruction_cache_redis_url
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
    if let Some(metrics_token) = metrics_token {
        config = config.with_metrics_token(metrics_token)?;
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

pub(super) fn optional_token_signing_key_from_sources(
    direct: Option<String>,
    file: Option<String>,
) -> Result<Option<Vec<u8>>, ServerConfigError> {
    match (direct, file) {
        (Some(_direct), Some(_file)) => Err(ServerConfigError::TokenSigningKeySourceConflict {
            env: "SHARDLINE_TOKEN_SIGNING_KEY",
            file_env: "SHARDLINE_TOKEN_SIGNING_KEY_FILE",
        }),
        (Some(value), None) => Ok(Some(value.into_bytes())),
        (None, Some(path)) => Ok(Some(read_secret_file_bytes(
            Path::new(&path),
            MAX_TOKEN_SIGNING_KEY_BYTES,
            ServerConfigError::TokenSigningKey,
            |observed_bytes, maximum_bytes| ServerConfigError::TokenSigningKeyTooLarge {
                observed_bytes,
                maximum_bytes,
            },
            |expected_bytes, observed_bytes| ServerConfigError::TokenSigningKeyLengthMismatch {
                expected_bytes,
                observed_bytes,
            },
        )?)),
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
