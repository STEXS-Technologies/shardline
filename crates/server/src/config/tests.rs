#[cfg(unix)]
use std::os::unix::fs::symlink;
use std::{
    fs::{OpenOptions, write as write_file},
    io::Write,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    num::{NonZeroU64, NonZeroUsize},
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use shardline_storage::S3ObjectStoreConfig;

use super::{
    DEFAULT_MAX_REQUEST_BODY_BYTES, DEFAULT_SHARD_METADATA_LIMITS, ObjectStorageAdapter,
    PendingS3ObjectStoreConfig, ServerConfig, ShardMetadataLimits,
    adaptive_default_in_flight_chunks_for_parallelism, configure_provider_runtime_from_paths,
    configure_s3_object_store_config, default_transfer_max_in_flight_chunks,
    default_upload_max_in_flight_chunks, env, optional_s3_secret_from_sources,
};
use crate::{
    ServerFrontend, ServerRole,
    reconstruction_cache::{
        DEFAULT_RECONSTRUCTION_CACHE_MEMORY_MAX_ENTRIES, DEFAULT_RECONSTRUCTION_CACHE_TTL_SECONDS,
        ReconstructionCacheAdapter,
    },
};

fn set_before_secret_file_read_hook_for_tests(path: PathBuf, hook: impl FnOnce() + Send + 'static) {
    let mut slot = match super::BEFORE_SECRET_FILE_READ_HOOK.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    if let Some(index) = slot
        .iter()
        .position(|registration| registration.path == path)
    {
        drop(slot.remove(index));
    }
    slot.push(super::SecretFileReadHookRegistration {
        path,
        hook: Box::new(hook),
    });
}

#[test]
fn server_config_keeps_bind_address_and_public_url() {
    let bind_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8080);
    let root_dir = PathBuf::from("/tmp/shardline");
    let chunk_size = NonZeroUsize::MIN;
    let config = ServerConfig::new(
        bind_addr,
        "https://assets.example.test".to_owned(),
        root_dir.clone(),
        chunk_size,
    );

    assert_eq!(config.bind_addr(), bind_addr);
    assert_eq!(config.server_role(), ServerRole::All);
    assert_eq!(config.server_frontends(), &[ServerFrontend::Xet]);
    assert_eq!(config.public_base_url(), "https://assets.example.test");
    assert_eq!(config.root_dir(), &root_dir);
    assert_eq!(
        config.max_request_body_bytes(),
        DEFAULT_MAX_REQUEST_BODY_BYTES
    );
    assert_eq!(
        config.shard_metadata_limits(),
        DEFAULT_SHARD_METADATA_LIMITS
    );
    assert_eq!(config.chunk_size(), chunk_size);
    assert_eq!(
        config.upload_max_in_flight_chunks(),
        default_upload_max_in_flight_chunks()
    );
    assert_eq!(
        config.transfer_max_in_flight_chunks(),
        default_transfer_max_in_flight_chunks()
    );
    assert_eq!(config.object_storage_adapter(), ObjectStorageAdapter::Local);
    assert!(config.s3_object_store_config().is_none());
    assert_eq!(
        config.reconstruction_cache_ttl_seconds(),
        DEFAULT_RECONSTRUCTION_CACHE_TTL_SECONDS
    );
    assert_eq!(
        config.reconstruction_cache_memory_max_entries(),
        DEFAULT_RECONSTRUCTION_CACHE_MEMORY_MAX_ENTRIES
    );
    assert_eq!(config.index_postgres_url(), None);
    assert_eq!(config.token_signing_key(), None);
    assert_eq!(config.metrics_token(), None);
    assert_eq!(config.provider_config_path(), None);
}

#[test]
fn server_config_allows_metrics_token() {
    let bind_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8080);
    let root_dir = PathBuf::from("/tmp/shardline");
    let config = ServerConfig::new(
        bind_addr,
        "https://assets.example.test".to_owned(),
        root_dir,
        NonZeroUsize::MIN,
    )
    .with_metrics_token(b"metrics-token".to_vec());

    assert!(config.is_ok());
    let Ok(config) = config else {
        return;
    };
    assert_eq!(config.metrics_token(), Some(b"metrics-token".as_slice()));
}

#[test]
fn server_config_rejects_empty_metrics_token() {
    let bind_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8080);
    let root_dir = PathBuf::from("/tmp/shardline");
    let config = ServerConfig::new(
        bind_addr,
        "https://assets.example.test".to_owned(),
        root_dir,
        NonZeroUsize::MIN,
    )
    .with_metrics_token(Vec::new());

    assert!(matches!(
        config,
        Err(super::ServerConfigError::EmptyMetricsToken)
    ));
}

#[test]
fn server_config_allows_non_zero_upload_parallelism_override() {
    let bind_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8080);
    let root_dir = PathBuf::from("/tmp/shardline");
    let chunk_size = NonZeroUsize::MIN;
    let upload_budget = NonZeroUsize::new(32).map_or(NonZeroUsize::MIN, |value| value);
    let config = ServerConfig::new(
        bind_addr,
        "https://assets.example.test".to_owned(),
        root_dir,
        chunk_size,
    )
    .with_upload_max_in_flight_chunks(upload_budget);

    assert_eq!(config.upload_max_in_flight_chunks(), upload_budget);
}

#[test]
fn server_config_allows_non_zero_chunk_size_override() {
    let bind_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8080);
    let root_dir = PathBuf::from("/tmp/shardline");
    let chunk_size = NonZeroUsize::MIN;
    let override_chunk_size = NonZeroUsize::new(4_096).map_or(NonZeroUsize::MIN, |value| value);
    let config = ServerConfig::new(
        bind_addr,
        "https://assets.example.test".to_owned(),
        root_dir,
        chunk_size,
    )
    .with_chunk_size(override_chunk_size);

    assert_eq!(config.chunk_size(), override_chunk_size);
}

#[test]
fn server_config_allows_non_zero_transfer_budget_override() {
    let bind_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8080);
    let root_dir = PathBuf::from("/tmp/shardline");
    let chunk_size = NonZeroUsize::MIN;
    let transfer_budget = NonZeroUsize::new(32).map_or(NonZeroUsize::MIN, |value| value);
    let config = ServerConfig::new(
        bind_addr,
        "https://assets.example.test".to_owned(),
        root_dir,
        chunk_size,
    )
    .with_transfer_max_in_flight_chunks(transfer_budget);

    assert_eq!(config.transfer_max_in_flight_chunks(), transfer_budget);
}

#[test]
fn adaptive_upload_default_scales_with_parallelism_and_caps() {
    let minimum = NonZeroUsize::new(64).unwrap_or(NonZeroUsize::MIN);
    let maximum = NonZeroUsize::new(256).unwrap_or(NonZeroUsize::MIN);

    assert_eq!(
        adaptive_default_in_flight_chunks_for_parallelism(1, 4, minimum, maximum),
        minimum
    );
    assert_eq!(
        adaptive_default_in_flight_chunks_for_parallelism(16, 2, minimum, maximum),
        minimum
    );
    assert_eq!(
        adaptive_default_in_flight_chunks_for_parallelism(32, 2, minimum, maximum),
        minimum
    );
    assert_eq!(
        adaptive_default_in_flight_chunks_for_parallelism(64, 2, minimum, maximum).get(),
        128
    );
    assert_eq!(
        adaptive_default_in_flight_chunks_for_parallelism(256, 2, minimum, maximum),
        maximum
    );
}

#[test]
fn adaptive_transfer_default_scales_with_parallelism_and_caps() {
    let minimum = NonZeroUsize::new(64).unwrap_or(NonZeroUsize::MIN);
    let maximum = NonZeroUsize::new(1024).unwrap_or(NonZeroUsize::MIN);

    assert_eq!(
        adaptive_default_in_flight_chunks_for_parallelism(1, 8, minimum, maximum),
        minimum
    );
    assert_eq!(
        adaptive_default_in_flight_chunks_for_parallelism(32, 8, minimum, maximum).get(),
        256
    );
    assert_eq!(
        adaptive_default_in_flight_chunks_for_parallelism(256, 8, minimum, maximum),
        maximum
    );
}

#[test]
fn server_config_allows_max_request_body_override() {
    let bind_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8080);
    let root_dir = PathBuf::from("/tmp/shardline");
    let max_request_body_bytes = NonZeroUsize::new(1024).map_or(NonZeroUsize::MIN, |value| value);
    let config = ServerConfig::new(
        bind_addr,
        "https://assets.example.test".to_owned(),
        root_dir,
        NonZeroUsize::MIN,
    )
    .with_max_request_body_bytes(max_request_body_bytes);

    assert_eq!(config.max_request_body_bytes(), max_request_body_bytes);
}

#[test]
fn server_config_allows_shard_metadata_limit_override() {
    let bind_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8080);
    let root_dir = PathBuf::from("/tmp/shardline");
    let limits = ShardMetadataLimits::new(
        NonZeroUsize::new(1_000_000).map_or(NonZeroUsize::MIN, |value| value),
        NonZeroUsize::new(1_000_000).map_or(NonZeroUsize::MIN, |value| value),
        NonZeroUsize::new(4_000_000).map_or(NonZeroUsize::MIN, |value| value),
        NonZeroUsize::new(4_000_000).map_or(NonZeroUsize::MIN, |value| value),
    );
    let config = ServerConfig::new(
        bind_addr,
        "https://assets.example.test".to_owned(),
        root_dir,
        NonZeroUsize::MIN,
    )
    .with_shard_metadata_limits(limits);

    assert_eq!(config.shard_metadata_limits(), limits);
}

#[test]
fn server_config_allows_role_override() {
    let bind_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8080);
    let config = ServerConfig::new(
        bind_addr,
        "https://assets.example.test".to_owned(),
        PathBuf::from("/tmp/shardline"),
        NonZeroUsize::MIN,
    )
    .with_server_role(ServerRole::Transfer);

    assert_eq!(config.server_role(), ServerRole::Transfer);
}

#[test]
fn server_config_allows_frontend_override_and_deduplicates() {
    let bind_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8080);
    let config = ServerConfig::new(
        bind_addr,
        "https://assets.example.test".to_owned(),
        PathBuf::from("/tmp/shardline"),
        NonZeroUsize::MIN,
    )
    .with_server_frontends([ServerFrontend::Xet, ServerFrontend::Xet]);

    assert!(config.is_ok());
    let Ok(config) = config else {
        return;
    };
    assert_eq!(config.server_frontends(), &[ServerFrontend::Xet]);
}

#[test]
fn object_storage_adapter_parses_supported_tokens() {
    assert!(matches!(
        ObjectStorageAdapter::parse("local"),
        Ok(ObjectStorageAdapter::Local)
    ));
    assert!(matches!(
        ObjectStorageAdapter::parse("s3"),
        Ok(ObjectStorageAdapter::S3)
    ));
    assert!(matches!(
        ObjectStorageAdapter::parse("filesystem"),
        Err(super::ServerConfigError::InvalidObjectStorageAdapter)
    ));
}

#[test]
fn server_config_allows_s3_object_storage_selection() {
    let bind_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8080);
    let root_dir = PathBuf::from("/tmp/shardline");
    let s3_config = S3ObjectStoreConfig::new("assets".to_owned(), "us-east-1".to_owned())
        .with_key_prefix(Some("tenant-a"));
    let config = ServerConfig::new(
        bind_addr,
        "https://assets.example.test".to_owned(),
        root_dir,
        NonZeroUsize::MIN,
    )
    .with_object_storage(ObjectStorageAdapter::S3, Some(s3_config));

    assert_eq!(config.object_storage_adapter(), ObjectStorageAdapter::S3);
    let configured = config.s3_object_store_config();
    assert!(configured.is_some());
    if let Some(configured) = configured {
        assert_eq!(configured.bucket(), "assets");
        assert_eq!(configured.key_prefix(), Some("tenant-a"));
    }
}

#[test]
fn server_config_can_disable_reconstruction_cache() {
    let bind_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8080);
    let root_dir = PathBuf::from("/tmp/shardline");
    let config = ServerConfig::new(
        bind_addr,
        "https://assets.example.test".to_owned(),
        root_dir,
        NonZeroUsize::MIN,
    )
    .with_reconstruction_cache_disabled();

    assert_eq!(
        config.reconstruction_cache_adapter(),
        ReconstructionCacheAdapter::Disabled
    );
}

#[test]
fn server_config_accepts_non_empty_redis_reconstruction_cache_url() {
    let bind_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8080);
    let root_dir = PathBuf::from("/tmp/shardline");
    let ttl = NonZeroU64::new(60).map_or(NonZeroU64::MIN, |value| value);
    let config = ServerConfig::new(
        bind_addr,
        "https://assets.example.test".to_owned(),
        root_dir,
        NonZeroUsize::MIN,
    )
    .with_reconstruction_cache_redis(
        "redis://default:dev_password@127.0.0.1:6379".to_owned(),
        ttl,
    );

    assert!(config.is_ok());
    let Ok(config) = config else {
        return;
    };
    assert_eq!(
        config.reconstruction_cache_redis_url(),
        Some("redis://default:dev_password@127.0.0.1:6379")
    );
}

#[test]
fn server_config_accepts_non_empty_index_postgres_url() {
    let bind_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8080);
    let root_dir = PathBuf::from("/tmp/shardline");
    let chunk_size = NonZeroUsize::MIN;
    let config = ServerConfig::new(
        bind_addr,
        "https://assets.example.test".to_owned(),
        root_dir,
        chunk_size,
    )
    .with_index_postgres_url("postgres://user:password@localhost:5432/shardline".to_owned());

    assert!(config.is_ok());
    let Ok(config) = config else {
        return;
    };
    assert_eq!(
        config.index_postgres_url(),
        Some("postgres://user:password@localhost:5432/shardline")
    );
}

#[test]
fn server_config_accepts_non_empty_token_signing_key() {
    let bind_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8080);
    let root_dir = PathBuf::from("/tmp/shardline");
    let chunk_size = NonZeroUsize::MIN;
    let config = ServerConfig::new(
        bind_addr,
        "https://assets.example.test".to_owned(),
        root_dir,
        chunk_size,
    )
    .with_token_signing_key(b"signing-key".to_vec());

    assert!(config.is_ok());
    let Ok(config) = config else {
        return;
    };
    assert_eq!(config.token_signing_key(), Some("signing-key".as_bytes()));
}

#[test]
fn server_config_accepts_provider_runtime_when_signing_is_enabled() {
    let bind_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8080);
    let root_dir = PathBuf::from("/tmp/shardline");
    let chunk_size = NonZeroUsize::MIN;
    let config = ServerConfig::new(
        bind_addr,
        "https://assets.example.test".to_owned(),
        root_dir,
        chunk_size,
    )
    .with_token_signing_key(b"signing-key".to_vec())
    .and_then(|config| {
        config.with_provider_runtime(
            PathBuf::from("/tmp/providers.json"),
            b"bootstrap".to_vec(),
            "provider".to_owned(),
            NonZeroU64::MIN,
        )
    });

    assert!(config.is_ok());
    let Ok(config) = config else {
        return;
    };
    assert_eq!(
        config.provider_config_path(),
        Some(Path::new("/tmp/providers.json"))
    );
    assert_eq!(config.provider_api_key(), Some("bootstrap".as_bytes()));
    assert_eq!(config.provider_token_issuer(), Some("provider"));
    assert_eq!(config.provider_token_ttl_seconds(), Some(NonZeroU64::MIN));
}

#[test]
fn server_config_debug_redacts_secret_material() {
    let bind_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8080);
    let root_dir = PathBuf::from("/tmp/shardline");
    let chunk_size = NonZeroUsize::MIN;
    let config = ServerConfig::new(
        bind_addr,
        "https://assets.example.test".to_owned(),
        root_dir,
        chunk_size,
    )
    .with_token_signing_key(vec![1, 2, 3, 4])
    .and_then(|config| config.with_metrics_token(vec![5, 6, 7, 8]))
    .and_then(|config| {
        config.with_reconstruction_cache_redis(
            "redis://:cache-secret@cache.example.test:6379/0".to_owned(),
            NonZeroU64::MIN,
        )
    })
    .and_then(|config| {
        config.with_index_postgres_url(
            "postgres://user:db-secret@db.example.test:5432/shardline".to_owned(),
        )
    })
    .and_then(|config| {
        config.with_provider_runtime(
            PathBuf::from("/tmp/providers.json"),
            vec![9, 10, 11, 12],
            "provider".to_owned(),
            NonZeroU64::MIN,
        )
    });
    assert!(config.is_ok());
    let Ok(config) = config else {
        return;
    };

    let rendered = format!("{config:?}");

    assert!(!rendered.contains("[1, 2, 3, 4]"));
    assert!(!rendered.contains("[5, 6, 7, 8]"));
    assert!(!rendered.contains("[9, 10, 11, 12]"));
    assert!(!rendered.contains("cache-secret"));
    assert!(!rendered.contains("db-secret"));
    assert!(rendered.contains("***"));
}

#[test]
fn server_config_rejects_empty_token_signing_key() {
    let bind_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8080);
    let root_dir = PathBuf::from("/tmp/shardline");
    let chunk_size = NonZeroUsize::MIN;
    let config = ServerConfig::new(
        bind_addr,
        "https://assets.example.test".to_owned(),
        root_dir,
        chunk_size,
    )
    .with_token_signing_key(Vec::new());

    assert!(matches!(
        config,
        Err(super::ServerConfigError::EmptyTokenSigningKey)
    ));
}

#[test]
fn server_config_rejects_oversized_token_signing_key() {
    let bind_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8080);
    let root_dir = PathBuf::from("/tmp/shardline");
    let chunk_size = NonZeroUsize::MIN;
    let oversized = vec![0_u8; 1_048_577];
    let config = ServerConfig::new(
        bind_addr,
        "https://assets.example.test".to_owned(),
        root_dir,
        chunk_size,
    )
    .with_token_signing_key(oversized);

    assert!(matches!(
        config,
        Err(super::ServerConfigError::TokenSigningKeyTooLarge {
            maximum_bytes: 1_048_576,
            ..
        })
    ));
}

#[test]
fn server_config_rejects_empty_index_postgres_url() {
    let bind_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8080);
    let root_dir = PathBuf::from("/tmp/shardline");
    let chunk_size = NonZeroUsize::MIN;
    let config = ServerConfig::new(
        bind_addr,
        "https://assets.example.test".to_owned(),
        root_dir,
        chunk_size,
    )
    .with_index_postgres_url("   ".to_owned());

    assert!(matches!(
        config,
        Err(super::ServerConfigError::EmptyIndexPostgresUrl)
    ));
}

#[test]
fn server_config_rejects_provider_runtime_without_signing_key() {
    let bind_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8080);
    let root_dir = PathBuf::from("/tmp/shardline");
    let chunk_size = NonZeroUsize::MIN;
    let config = ServerConfig::new(
        bind_addr,
        "https://assets.example.test".to_owned(),
        root_dir,
        chunk_size,
    )
    .with_provider_runtime(
        PathBuf::from("/tmp/providers.json"),
        b"bootstrap".to_vec(),
        "provider".to_owned(),
        NonZeroU64::MIN,
    );

    assert!(matches!(
        config,
        Err(super::ServerConfigError::ProviderTokensRequireSigningKey)
    ));
}

#[test]
fn server_config_rejects_oversized_provider_api_key() {
    let bind_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8080);
    let root_dir = PathBuf::from("/tmp/shardline");
    let chunk_size = NonZeroUsize::MIN;
    let oversized = vec![0_u8; 4097];
    let config = ServerConfig::new(
        bind_addr,
        "https://assets.example.test".to_owned(),
        root_dir,
        chunk_size,
    )
    .with_token_signing_key(b"signing-key".to_vec())
    .and_then(|config| {
        config.with_provider_runtime(
            PathBuf::from("/tmp/providers.json"),
            oversized,
            "provider".to_owned(),
            NonZeroU64::MIN,
        )
    });

    assert!(matches!(
        config,
        Err(super::ServerConfigError::ProviderApiKeyTooLarge {
            maximum_bytes: 4096,
            ..
        })
    ));
}

#[test]
fn s3_config_rejects_missing_bucket_before_loading_credentials() {
    let credentials_loaded = Arc::new(AtomicBool::new(false));
    let credentials_loaded_for_hook = Arc::clone(&credentials_loaded);

    let configured = configure_s3_object_store_config(
        Err(super::ServerConfigError::MissingS3Bucket),
        PendingS3ObjectStoreConfig {
            region: "us-east-1".to_owned(),
            endpoint: None,
            key_prefix: None,
            allow_http: Ok(None),
            virtual_hosted_style_request: Ok(None),
        },
        move || {
            credentials_loaded_for_hook.store(true, Ordering::SeqCst);
            Ok((Some("access".to_owned()), Some("secret".to_owned()), None))
        },
    );

    assert!(matches!(
        configured,
        Err(super::ServerConfigError::MissingS3Bucket)
    ));
    assert!(!credentials_loaded.load(Ordering::SeqCst));
}

#[test]
fn s3_config_rejects_invalid_allow_http_before_loading_credentials() {
    let credentials_loaded = Arc::new(AtomicBool::new(false));
    let credentials_loaded_for_hook = Arc::clone(&credentials_loaded);

    let configured = configure_s3_object_store_config(
        Ok("assets".to_owned()),
        PendingS3ObjectStoreConfig {
            region: "us-east-1".to_owned(),
            endpoint: None,
            key_prefix: None,
            allow_http: Err(super::ServerConfigError::InvalidS3AllowHttp),
            virtual_hosted_style_request: Ok(None),
        },
        move || {
            credentials_loaded_for_hook.store(true, Ordering::SeqCst);
            Ok((Some("access".to_owned()), Some("secret".to_owned()), None))
        },
    );

    assert!(matches!(
        configured,
        Err(super::ServerConfigError::InvalidS3AllowHttp)
    ));
    assert!(!credentials_loaded.load(Ordering::SeqCst));
}

#[test]
fn s3_config_rejects_invalid_virtual_hosted_style_before_loading_credentials() {
    let credentials_loaded = Arc::new(AtomicBool::new(false));
    let credentials_loaded_for_hook = Arc::clone(&credentials_loaded);

    let configured = configure_s3_object_store_config(
        Ok("assets".to_owned()),
        PendingS3ObjectStoreConfig {
            region: "us-east-1".to_owned(),
            endpoint: None,
            key_prefix: None,
            allow_http: Ok(None),
            virtual_hosted_style_request: Err(
                super::ServerConfigError::InvalidS3VirtualHostedStyleRequest,
            ),
        },
        move || {
            credentials_loaded_for_hook.store(true, Ordering::SeqCst);
            Ok((Some("access".to_owned()), Some("secret".to_owned()), None))
        },
    );

    assert!(matches!(
        configured,
        Err(super::ServerConfigError::InvalidS3VirtualHostedStyleRequest)
    ));
    assert!(!credentials_loaded.load(Ordering::SeqCst));
}

#[test]
fn s3_config_accepts_file_backed_credentials_without_debug_leak() {
    let access_key_file = tempfile::NamedTempFile::new();
    assert!(access_key_file.is_ok());
    let Ok(access_key_file) = access_key_file else {
        return;
    };
    let write = write_file(access_key_file.path(), b"file-access-key");
    assert!(write.is_ok());

    let configured = configure_s3_object_store_config(
        Ok("assets".to_owned()),
        PendingS3ObjectStoreConfig {
            region: "us-east-1".to_owned(),
            endpoint: None,
            key_prefix: None,
            allow_http: Ok(None),
            virtual_hosted_style_request: Ok(None),
        },
        || {
            Ok((
                optional_s3_secret_from_sources(
                    "SHARDLINE_S3_ACCESS_KEY_ID",
                    None,
                    "SHARDLINE_S3_ACCESS_KEY_ID_FILE",
                    Some(access_key_file.path().display().to_string()),
                )?,
                None,
                None,
            ))
        },
    );

    assert!(configured.is_ok());
    let debug = format!("{configured:?}");
    assert!(!debug.contains("file-access-key"));
}

#[test]
fn s3_config_rejects_direct_and_file_credential_conflict() {
    let configured = configure_s3_object_store_config(
        Ok("assets".to_owned()),
        PendingS3ObjectStoreConfig {
            region: "us-east-1".to_owned(),
            endpoint: None,
            key_prefix: None,
            allow_http: Ok(None),
            virtual_hosted_style_request: Ok(None),
        },
        || {
            Ok((
                optional_s3_secret_from_sources(
                    "SHARDLINE_S3_ACCESS_KEY_ID",
                    Some("direct-access-key".to_owned()),
                    "SHARDLINE_S3_ACCESS_KEY_ID_FILE",
                    Some("/run/secrets/access-key".to_owned()),
                )?,
                None,
                None,
            ))
        },
    );

    assert!(matches!(
        configured,
        Err(super::ServerConfigError::S3CredentialSourceConflict {
            env: "SHARDLINE_S3_ACCESS_KEY_ID",
            file_env: "SHARDLINE_S3_ACCESS_KEY_ID_FILE",
        })
    ));
}

#[test]
fn s3_config_rejects_invalid_utf8_file_backed_credential() {
    let access_key_file = tempfile::NamedTempFile::new();
    assert!(access_key_file.is_ok());
    let Ok(access_key_file) = access_key_file else {
        return;
    };
    let write = write_file(access_key_file.path(), [0xff, 0xfe]);
    assert!(write.is_ok());

    let credential = optional_s3_secret_from_sources(
        "SHARDLINE_S3_ACCESS_KEY_ID",
        None,
        "SHARDLINE_S3_ACCESS_KEY_ID_FILE",
        Some(access_key_file.path().display().to_string()),
    );

    assert!(matches!(
        credential,
        Err(super::ServerConfigError::S3CredentialUtf8 {
            name: "SHARDLINE_S3_ACCESS_KEY_ID_FILE"
        })
    ));
}

#[test]
fn token_signing_key_accepts_direct_env_source() {
    let loaded =
        env::optional_token_signing_key_from_sources(Some("direct-signing-key".to_owned()), None);

    assert!(loaded.is_ok());
    let Ok(loaded) = loaded else {
        return;
    };
    assert_eq!(loaded, Some(b"direct-signing-key".to_vec()));
}

#[test]
fn token_signing_key_rejects_direct_and_file_source_conflict() {
    let loaded = env::optional_token_signing_key_from_sources(
        Some("direct-signing-key".to_owned()),
        Some("/run/secrets/token-signing-key".to_owned()),
    );

    assert!(matches!(
        loaded,
        Err(super::ServerConfigError::TokenSigningKeySourceConflict {
            env: "SHARDLINE_TOKEN_SIGNING_KEY",
            file_env: "SHARDLINE_TOKEN_SIGNING_KEY_FILE",
        })
    ));
}

#[test]
fn from_env_rejects_incomplete_provider_runtime_before_reading_provider_api_key_file() {
    let api_key_file = tempfile::NamedTempFile::new();
    assert!(api_key_file.is_ok());
    let Ok(api_key_file) = api_key_file else {
        return;
    };
    let write = write_file(api_key_file.path(), b"bootstrap");
    assert!(write.is_ok());
    let read_attempted = Arc::new(AtomicBool::new(false));
    let config = ServerConfig::new(
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8080),
        "https://assets.example.test".to_owned(),
        PathBuf::from("/tmp/shardline"),
        NonZeroUsize::MIN,
    );
    let read_attempted_for_hook = Arc::clone(&read_attempted);
    set_before_secret_file_read_hook_for_tests(api_key_file.path().to_path_buf(), move || {
        read_attempted_for_hook.store(true, Ordering::SeqCst);
    });

    let loaded = configure_provider_runtime_from_paths(
        config,
        None,
        Some(api_key_file.path().to_path_buf()),
        "issuer".to_owned(),
        Ok(NonZeroU64::MIN),
    );

    assert!(matches!(
        loaded,
        Err(super::ServerConfigError::IncompleteProviderTokenConfig)
    ));
    assert!(!read_attempted.load(Ordering::SeqCst));
}

#[test]
fn from_env_rejects_missing_signing_key_before_reading_provider_api_key_file() {
    let api_key_file = tempfile::NamedTempFile::new();
    assert!(api_key_file.is_ok());
    let Ok(api_key_file) = api_key_file else {
        return;
    };
    let write = write_file(api_key_file.path(), b"bootstrap");
    assert!(write.is_ok());
    let read_attempted = Arc::new(AtomicBool::new(false));
    let config = ServerConfig::new(
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8080),
        "https://assets.example.test".to_owned(),
        PathBuf::from("/tmp/shardline"),
        NonZeroUsize::MIN,
    );
    let read_attempted_for_hook = Arc::clone(&read_attempted);
    set_before_secret_file_read_hook_for_tests(api_key_file.path().to_path_buf(), move || {
        read_attempted_for_hook.store(true, Ordering::SeqCst);
    });

    let loaded = configure_provider_runtime_from_paths(
        config,
        Some(PathBuf::from("/tmp/providers.json")),
        Some(api_key_file.path().to_path_buf()),
        "issuer".to_owned(),
        Ok(NonZeroU64::MIN),
    );

    assert!(matches!(
        loaded,
        Err(super::ServerConfigError::ProviderTokensRequireSigningKey)
    ));
    assert!(!read_attempted.load(Ordering::SeqCst));
}

#[test]
fn from_env_rejects_invalid_provider_ttl_before_reading_provider_api_key_file() {
    let api_key_file = tempfile::NamedTempFile::new();
    assert!(api_key_file.is_ok());
    let Ok(api_key_file) = api_key_file else {
        return;
    };
    let api_key_write = write_file(api_key_file.path(), b"bootstrap");
    assert!(api_key_write.is_ok());
    let read_attempted = Arc::new(AtomicBool::new(false));
    let config = ServerConfig::new(
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8080),
        "https://assets.example.test".to_owned(),
        PathBuf::from("/tmp/shardline"),
        NonZeroUsize::MIN,
    )
    .with_token_signing_key(b"signing-key".to_vec());
    assert!(config.is_ok());
    let Ok(config) = config else {
        return;
    };
    let read_attempted_for_hook = Arc::clone(&read_attempted);
    set_before_secret_file_read_hook_for_tests(api_key_file.path().to_path_buf(), move || {
        read_attempted_for_hook.store(true, Ordering::SeqCst);
    });

    let loaded = configure_provider_runtime_from_paths(
        config,
        Some(PathBuf::from("/tmp/providers.json")),
        Some(api_key_file.path().to_path_buf()),
        "issuer".to_owned(),
        Err(super::ServerConfigError::ProviderTokenTtl),
    );

    assert!(matches!(
        loaded,
        Err(super::ServerConfigError::ProviderTokenTtl)
    ));
    assert!(!read_attempted.load(Ordering::SeqCst));
}

#[test]
fn read_secret_file_bytes_rejects_oversized_signing_key_before_reading() {
    let temp = tempfile::NamedTempFile::new();
    assert!(temp.is_ok());
    let Ok(mut temp) = temp else {
        return;
    };
    let resized = temp.as_file_mut().set_len(1_048_577);
    assert!(resized.is_ok());

    let bytes = super::read_secret_file_bytes(
        temp.path(),
        1_048_576,
        super::ServerConfigError::TokenSigningKey,
        |observed_bytes, maximum_bytes| super::ServerConfigError::TokenSigningKeyTooLarge {
            observed_bytes,
            maximum_bytes,
        },
        |expected_bytes, observed_bytes| super::ServerConfigError::TokenSigningKeyLengthMismatch {
            expected_bytes,
            observed_bytes,
        },
    );

    assert!(matches!(
        bytes,
        Err(super::ServerConfigError::TokenSigningKeyTooLarge {
            maximum_bytes: 1_048_576,
            ..
        })
    ));
}

#[cfg(unix)]
#[test]
fn read_secret_file_bytes_accepts_projected_secret_symlink_within_directory() {
    let temp = tempfile::tempdir();
    assert!(temp.is_ok());
    let Ok(temp) = temp else {
        return;
    };
    let data_dir = temp.path().join("..data");
    let data_dir_created = std::fs::create_dir(&data_dir);
    assert!(data_dir_created.is_ok());
    let target = data_dir.join("target-secret");
    let link = temp.path().join("linked-secret");
    let write = write_file(&target, b"signing-key");
    assert!(write.is_ok());
    let linked = symlink(Path::new("..data").join("target-secret"), &link);
    assert!(linked.is_ok());

    let bytes = super::read_secret_file_bytes(
        &link,
        1_048_576,
        super::ServerConfigError::TokenSigningKey,
        |observed_bytes, maximum_bytes| super::ServerConfigError::TokenSigningKeyTooLarge {
            observed_bytes,
            maximum_bytes,
        },
        |expected_bytes, observed_bytes| super::ServerConfigError::TokenSigningKeyLengthMismatch {
            expected_bytes,
            observed_bytes,
        },
    );

    assert!(bytes.is_ok());
    let Ok(bytes) = bytes else {
        return;
    };
    assert_eq!(bytes, b"signing-key".to_vec());
}

#[cfg(unix)]
#[test]
fn read_secret_file_bytes_rejects_symlinked_secret_path_outside_directory() {
    let temp = tempfile::tempdir();
    assert!(temp.is_ok());
    let Ok(temp) = temp else {
        return;
    };
    let outside = tempfile::NamedTempFile::new();
    assert!(outside.is_ok());
    let Ok(outside) = outside else {
        return;
    };
    let write = write_file(outside.path(), b"signing-key");
    assert!(write.is_ok());
    let link = temp.path().join("linked-secret");
    let linked = symlink(outside.path(), &link);
    assert!(linked.is_ok());

    let bytes = super::read_secret_file_bytes(
        &link,
        1_048_576,
        super::ServerConfigError::TokenSigningKey,
        |observed_bytes, maximum_bytes| super::ServerConfigError::TokenSigningKeyTooLarge {
            observed_bytes,
            maximum_bytes,
        },
        |expected_bytes, observed_bytes| super::ServerConfigError::TokenSigningKeyLengthMismatch {
            expected_bytes,
            observed_bytes,
        },
    );

    assert!(matches!(
        bytes,
        Err(super::ServerConfigError::TokenSigningKey(_))
    ));
}

#[test]
fn read_secret_file_bytes_rejects_growth_after_validation_without_retaining_appended_bytes() {
    let temp = tempfile::NamedTempFile::new();
    assert!(temp.is_ok());
    let Ok(temp) = temp else {
        return;
    };
    let initial = b"signing-key";
    let write = write_file(temp.path(), initial);
    assert!(write.is_ok());

    let path = temp.path().to_path_buf();
    set_before_secret_file_read_hook_for_tests(path.clone(), move || {
        let opened = OpenOptions::new().append(true).open(&path);
        assert!(opened.is_ok());
        let Ok(mut file) = opened else {
            return;
        };
        let appended = file.write_all(b"-rotated");
        assert!(appended.is_ok());
    });

    let bytes = super::read_secret_file_bytes(
        temp.path(),
        1_048_576,
        super::ServerConfigError::TokenSigningKey,
        |observed_bytes, maximum_bytes| super::ServerConfigError::TokenSigningKeyTooLarge {
            observed_bytes,
            maximum_bytes,
        },
        |expected_bytes, observed_bytes| super::ServerConfigError::TokenSigningKeyLengthMismatch {
            expected_bytes,
            observed_bytes,
        },
    );

    assert!(matches!(
        bytes,
        Err(super::ServerConfigError::TokenSigningKeyLengthMismatch {
            expected_bytes,
            observed_bytes,
        }) if expected_bytes == u64::try_from(initial.len()).unwrap_or(u64::MAX)
            && observed_bytes > expected_bytes
    ));
}

#[test]
fn server_config_runtime_validation_rejects_missing_signing_key_for_all_role() {
    let bind_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8080);
    let root_dir = PathBuf::from("/tmp/shardline");
    let chunk_size = NonZeroUsize::MIN;
    let config = ServerConfig::new(
        bind_addr,
        "https://assets.example.test".to_owned(),
        root_dir,
        chunk_size,
    );

    let validation = config.validate_runtime_requirements();

    assert!(matches!(
        validation,
        Err(super::ServerConfigError::MissingTokenSigningKeyForServedRoutes)
    ));
}

#[test]
fn server_config_runtime_validation_accepts_signed_transfer_role() {
    let bind_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8080);
    let root_dir = PathBuf::from("/tmp/shardline");
    let chunk_size = NonZeroUsize::MIN;
    let config = ServerConfig::new(
        bind_addr,
        "https://assets.example.test".to_owned(),
        root_dir,
        chunk_size,
    )
    .with_server_role(ServerRole::Transfer)
    .with_token_signing_key(b"signing-key".to_vec());
    assert!(config.is_ok());
    let Ok(config) = config else {
        return;
    };

    let validation = config.validate_runtime_requirements();

    assert!(validation.is_ok());
}
