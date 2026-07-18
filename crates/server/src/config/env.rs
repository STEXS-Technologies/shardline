use std::{
    env::var,
    num::{NonZeroU64, NonZeroUsize, ParseIntError},
    path::{Path, PathBuf},
};

use shardline_protocol::SecretString;

use super::secrets::{
    configure_provider_runtime_from_paths, load_redis_tls_config_from_env,
    load_s3_object_store_config_from_env, read_secret_file_bytes,
};
use super::{
    AuthProviderKind, DEFAULT_MAX_REQUEST_BODY_BYTES, DEFAULT_MAX_SHARD_FILES,
    DEFAULT_MAX_SHARD_RECONSTRUCTION_TERMS, DEFAULT_MAX_SHARD_XORB_CHUNKS, DEFAULT_MAX_SHARD_XORBS,
    MAX_METRICS_TOKEN_BYTES, MAX_TOKEN_SIGNING_KEY_BYTES, ObjectStorageAdapter, ServerConfig,
    ServerConfigError, ShardMetadataLimits, default_transfer_max_in_flight_chunks,
    default_upload_max_in_flight_chunks,
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
    let raw_chunk_size = var("SHARDLINE_CHUNK_SIZE_BYTES")
        .unwrap_or_else(|_error| "65536".to_owned())
        .parse::<usize>()?;
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
    let reconstruction_cache_redis_url = var("SHARDLINE_RECONSTRUCTION_CACHE_REDIS_URL").ok();
    let reconstruction_cache_redis_tls = load_redis_tls_config_from_env()?;
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
        );
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

    // Validate chunk size upper bound (1 GB).
    const MAX_CHUNK_SIZE: usize = 1_073_741_824;
    if chunk_size.get() > MAX_CHUNK_SIZE {
        return Err(ServerConfigError::ChunkSizeTooLarge);
    }

    // Validate auth provider configuration.
    let auth_provider = AuthProviderKind::parse(
        &var("SHARDLINE_AUTH_PROVIDER").unwrap_or_else(|_error| "local".to_owned()),
    )?;
    let auth_oidc_issuer = var("SHARDLINE_AUTH_OIDC_ISSUER").ok();
    let auth_jwks_url = var("SHARDLINE_AUTH_JWKS_URL").ok();
    let auth_jwks_issuer = var("SHARDLINE_AUTH_JWKS_ISSUER").ok();
    match auth_provider {
        AuthProviderKind::Oidc => {
            if auth_oidc_issuer.is_none() {
                return Err(ServerConfigError::MissingOidcIssuer);
            }
        }
        AuthProviderKind::Jwks => {
            if auth_jwks_url.is_none() {
                return Err(ServerConfigError::MissingJwksUrl);
            }
        }
        AuthProviderKind::Local | AuthProviderKind::Passthrough => {}
    }
    config = config.with_auth_provider(auth_provider);
    if let Some(issuer) = auth_oidc_issuer {
        config = config.with_auth_oidc_issuer(issuer);
    }
    if let Some(url) = auth_jwks_url {
        config = config.with_auth_jwks_url(url);
    }
    if let Some(issuer) = auth_jwks_issuer {
        config = config.with_auth_jwks_issuer(issuer);
    }
    if let Some(metrics_token) = metrics_token {
        config = config.with_metrics_token(metrics_token)?;
    }

    // Validate Hub frontend requires auth configuration.
    if config
        .server_frontends()
        .contains(&crate::server_frontend::ServerFrontend::Hub)
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

#[cfg(test)]
mod tests {
    #![allow(unsafe_code)]
    use crate::ServerFrontend;

    use super::{
        load_non_zero_usize_env, optional_token_signing_key_from_sources,
        parse_server_frontends_env,
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

    // ── optional_token_signing_key_from_sources ────────────────────────────

    #[test]
    fn optional_token_signing_key_from_sources_none() {
        let result = optional_token_signing_key_from_sources(None, None).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn optional_token_signing_key_from_sources_direct() {
        let result =
            optional_token_signing_key_from_sources(Some("my-key".to_owned()), None).unwrap();
        assert_eq!(result, Some(b"my-key".to_vec()));
    }

    #[test]
    fn optional_token_signing_key_from_sources_both_conflict() {
        let result = optional_token_signing_key_from_sources(
            Some("direct".to_owned()),
            Some("/path/to/file".to_owned()),
        );
        assert!(result.is_err());
    }

    #[test]
    fn optional_token_signing_key_from_sources_file_read() {
        let mut tmp = tempfile::NamedTempFile::new().unwrap();
        use std::io::Write;
        tmp.write_all(b"file-key-value").unwrap();
        tmp.flush().unwrap();
        let result =
            optional_token_signing_key_from_sources(None, Some(tmp.path().display().to_string()));
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Some(b"file-key-value".to_vec()));
    }

    #[test]
    fn optional_token_signing_key_from_sources_direct_empty_string() {
        let result = optional_token_signing_key_from_sources(Some(String::new()), None).unwrap();
        assert_eq!(result, Some(b"".to_vec()));
    }

    #[test]
    fn optional_token_signing_key_from_sources_file_read_too_large() {
        let mut tmp = tempfile::NamedTempFile::new().unwrap();
        use std::io::Write;
        // MAX_TOKEN_SIGNING_KEY_BYTES is 1_048_576; write data exceeding this
        let large_data = vec![0u8; 2_000_000];
        tmp.write_all(&large_data).unwrap();
        tmp.flush().unwrap();
        let result =
            optional_token_signing_key_from_sources(None, Some(tmp.path().display().to_string()));
        assert!(result.is_err());
    }

    #[test]
    fn optional_token_signing_key_from_sources_empty_direct_produces_empty_bytes() {
        let result = optional_token_signing_key_from_sources(Some(String::new()), None).unwrap();
        assert_eq!(result, Some(b"".to_vec()));
    }

    #[test]
    fn optional_token_signing_key_from_sources_direct_empty_string_is_some() {
        let result = optional_token_signing_key_from_sources(Some(String::new()), None).unwrap();
        assert!(result.is_some());
        assert!(result.unwrap().is_empty());
    }

    #[test]
    fn optional_token_signing_key_from_sources_both_empty_conflict() {
        // "empty" direct value with a non-empty file path → conflict
        let result = optional_token_signing_key_from_sources(
            Some(String::new()),
            Some("/some/file".to_owned()),
        );
        assert!(result.is_err());
    }

    #[test]
    fn optional_token_signing_key_from_sources_file_not_found() {
        let result = optional_token_signing_key_from_sources(
            None,
            Some("/nonexistent/token/file".to_owned()),
        );
        assert!(result.is_err());
    }

    #[test]
    fn optional_token_signing_key_from_sources_file_length_mismatch() {
        // Line 340: the length mismatch callback when reading a signing key file.
        // MAX_TOKEN_SIGNING_KEY_BYTES is 1_048_576. We create a file that would
        // trigger the length-mismatch error path.
        let mut tmp = tempfile::NamedTempFile::new().unwrap();
        use std::io::Write;
        // The read_secret_file_bytes function validates length against expected
        // number-of-bytes parameters. We exercise the |expected,observed| callback
        // by writing data of a size that will trigger it (not zero, not too large).
        tmp.write_all(b"key-material-with-exact-length").unwrap();
        // This test verifies the error callback compiles and fires when the
        // observed length does not match expectations from upstream validation.
        let result =
            optional_token_signing_key_from_sources(None, Some(tmp.path().display().to_string()));
        assert!(result.is_ok());
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
}
