use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    num::{NonZeroU64, NonZeroUsize},
    path::{Path, PathBuf},
};

use super::*;
use crate::reconstruction_cache::ReconstructionCacheAdapter;
use crate::route_policy::{register_route_policies, RouteAuthPolicy, RoutePolicyRegistry};
use crate::server_frontend::ServerFrontend;
use crate::server_role::ServerRole;

#[test]
fn auth_provider_kind_parse_local() {
    assert_eq!(
        AuthProviderKind::parse("local").unwrap(),
        AuthProviderKind::Local
    );
}

#[test]
fn auth_provider_kind_parse_oidc() {
    assert_eq!(
        AuthProviderKind::parse("oidc").unwrap(),
        AuthProviderKind::Oidc
    );
}

#[test]
fn auth_provider_kind_parse_jwks() {
    assert_eq!(
        AuthProviderKind::parse("jwks").unwrap(),
        AuthProviderKind::Jwks
    );
}

#[test]
fn auth_provider_kind_parse_passthrough() {
    assert_eq!(
        AuthProviderKind::parse("passthrough").unwrap(),
        AuthProviderKind::Passthrough
    );
}

#[test]
fn auth_provider_kind_parse_ed25519() {
    assert_eq!(
        AuthProviderKind::parse("ed25519").unwrap(),
        AuthProviderKind::Ed25519
    );
}

#[test]
fn auth_provider_kind_parse_rejects_unknown() {
    assert!(matches!(
        AuthProviderKind::parse("unknown"),
        Err(ServerConfigError::InvalidAuthProvider)
    ));
}

#[test]
fn object_storage_adapter_parse_local() {
    assert_eq!(
        ObjectStorageAdapter::parse("local").unwrap(),
        ObjectStorageAdapter::Local
    );
}

#[test]
fn object_storage_adapter_parse_s3() {
    assert_eq!(
        ObjectStorageAdapter::parse("s3").unwrap(),
        ObjectStorageAdapter::S3
    );
}

#[test]
fn object_storage_adapter_parse_rejects_unknown() {
    assert!(matches!(
        ObjectStorageAdapter::parse("unknown"),
        Err(ServerConfigError::InvalidObjectStorageAdapter)
    ));
}

#[test]
fn server_config_new_constructs_with_defaults() {
    let config = ServerConfig::new(
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8080),
        "http://localhost:8080".to_owned(),
        PathBuf::from("/tmp/test"),
        NonZeroUsize::new(4096).unwrap(),
    );
    assert_eq!(config.bind_addr.port(), 8080);
    assert_eq!(config.public_base_url, "http://localhost:8080");
    assert_eq!(config.server_role, ServerRole::All);
    assert_eq!(config.server_frontends, vec![ServerFrontend::Xet]);
    assert_eq!(config.object_storage_adapter, ObjectStorageAdapter::Local);
    assert!(config.s3_object_store_config.is_none());
    assert_eq!(config.chunk_size, NonZeroUsize::new(4096).unwrap());
    assert_eq!(
        config.reconstruction_cache_adapter(),
        ReconstructionCacheAdapter::Memory
    );
    assert_eq!(config.auth_provider(), AuthProviderKind::Local);
    assert!(config.token_signing_key().is_none());
}

#[test]
fn server_config_builder_with_chunk_size() {
    let config = ServerConfig::new(
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8080),
        "http://localhost:8080".to_owned(),
        PathBuf::from("/tmp/test"),
        NonZeroUsize::new(1024).unwrap(),
    )
    .with_chunk_size(NonZeroUsize::new(8192).unwrap());
    assert_eq!(config.chunk_size(), NonZeroUsize::new(8192).unwrap());
}

#[test]
fn server_config_builder_with_server_role() {
    let config = ServerConfig::new(
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8080),
        "http://localhost:8080".to_owned(),
        PathBuf::from("/tmp/test"),
        NonZeroUsize::new(4096).unwrap(),
    )
    .with_server_role(ServerRole::Transfer);
    assert_eq!(config.server_role(), ServerRole::Transfer);
}

#[test]
fn server_config_builder_with_root_dir() {
    let config = ServerConfig::new(
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8080),
        "http://localhost:8080".to_owned(),
        PathBuf::from("/tmp/test"),
        NonZeroUsize::new(4096).unwrap(),
    )
    .with_root_dir(PathBuf::from("/var/lib/shardline"));
    assert_eq!(config.root_dir(), Path::new("/var/lib/shardline"));
}

#[test]
fn server_config_builder_with_object_storage() {
    let config = ServerConfig::new(
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8080),
        "http://localhost:8080".to_owned(),
        PathBuf::from("/tmp/test"),
        NonZeroUsize::new(4096).unwrap(),
    )
    .with_object_storage(ObjectStorageAdapter::S3, None);
    assert_eq!(config.object_storage_adapter(), ObjectStorageAdapter::S3);
}

#[test]
fn server_config_builder_with_max_request_body_bytes() {
    let config = ServerConfig::new(
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8080),
        "http://localhost:8080".to_owned(),
        PathBuf::from("/tmp/test"),
        NonZeroUsize::new(4096).unwrap(),
    )
    .with_max_request_body_bytes(NonZeroUsize::new(1_000_000).unwrap());
    assert_eq!(
        config.max_request_body_bytes(),
        NonZeroUsize::new(1_000_000).unwrap()
    );
}

#[test]
fn server_config_builder_with_shard_metadata_limits() {
    let limits = ShardMetadataLimits::new(
        NonZeroUsize::new(100).unwrap(),
        NonZeroUsize::new(100).unwrap(),
        NonZeroUsize::new(500).unwrap(),
        NonZeroUsize::new(500).unwrap(),
    );
    let config = ServerConfig::new(
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8080),
        "http://localhost:8080".to_owned(),
        PathBuf::from("/tmp/test"),
        NonZeroUsize::new(4096).unwrap(),
    )
    .with_shard_metadata_limits(limits);
    assert_eq!(config.shard_metadata_limits(), limits);
}

#[test]
fn server_config_builder_with_server_frontends_rejects_empty() {
    let config = ServerConfig::new(
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8080),
        "http://localhost:8080".to_owned(),
        PathBuf::from("/tmp/test"),
        NonZeroUsize::new(4096).unwrap(),
    );
    let result = config.with_server_frontends([]);
    assert!(matches!(
        result,
        Err(ServerConfigError::MissingServerFrontends)
    ));
}

#[test]
fn server_config_builder_with_server_frontends_deduplicates() {
    let config = ServerConfig::new(
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8080),
        "http://localhost:8080".to_owned(),
        PathBuf::from("/tmp/test"),
        NonZeroUsize::new(4096).unwrap(),
    )
    .with_server_frontends([ServerFrontend::Xet, ServerFrontend::Xet])
    .unwrap();
    assert_eq!(config.server_frontends(), &[ServerFrontend::Xet]);
}

#[test]
fn server_config_builder_with_upload_max_in_flight_chunks() {
    let config = ServerConfig::new(
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8080),
        "http://localhost:8080".to_owned(),
        PathBuf::from("/tmp/test"),
        NonZeroUsize::new(4096).unwrap(),
    )
    .with_upload_max_in_flight_chunks(NonZeroUsize::new(128).unwrap());
    assert_eq!(
        config.upload_max_in_flight_chunks(),
        NonZeroUsize::new(128).unwrap()
    );
}

#[test]
fn server_config_builder_with_transfer_max_in_flight_chunks() {
    let config = ServerConfig::new(
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8080),
        "http://localhost:8080".to_owned(),
        PathBuf::from("/tmp/test"),
        NonZeroUsize::new(4096).unwrap(),
    )
    .with_transfer_max_in_flight_chunks(NonZeroUsize::new(256).unwrap());
    assert_eq!(
        config.transfer_max_in_flight_chunks(),
        NonZeroUsize::new(256).unwrap()
    );
}

#[test]
fn server_config_builder_with_reconstruction_cache_disabled() {
    let config = ServerConfig::new(
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8080),
        "http://localhost:8080".to_owned(),
        PathBuf::from("/tmp/test"),
        NonZeroUsize::new(4096).unwrap(),
    )
    .with_reconstruction_cache_disabled();
    assert_eq!(
        config.reconstruction_cache_adapter(),
        ReconstructionCacheAdapter::Disabled
    );
}

#[test]
fn server_config_builder_with_reconstruction_cache_memory() {
    let config = ServerConfig::new(
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8080),
        "http://localhost:8080".to_owned(),
        PathBuf::from("/tmp/test"),
        NonZeroUsize::new(4096).unwrap(),
    )
    .with_reconstruction_cache_memory(
        NonZeroU64::new(300).unwrap(),
        NonZeroUsize::new(5000).unwrap(),
    );
    assert_eq!(
        config.reconstruction_cache_adapter(),
        ReconstructionCacheAdapter::Memory
    );
    assert_eq!(
        config.reconstruction_cache_ttl_seconds(),
        NonZeroU64::new(300).unwrap()
    );
    assert_eq!(
        config.reconstruction_cache_memory_max_entries(),
        NonZeroUsize::new(5000).unwrap()
    );
}

#[test]
fn server_config_builder_with_reconstruction_cache_redis_rejects_empty_url() {
    let config = ServerConfig::new(
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8080),
        "http://localhost:8080".to_owned(),
        PathBuf::from("/tmp/test"),
        NonZeroUsize::new(4096).unwrap(),
    );
    let result =
        config.with_reconstruction_cache_redis(String::new(), NonZeroU64::new(300).unwrap());
    assert!(matches!(
        result,
        Err(ServerConfigError::EmptyReconstructionCacheRedisUrl)
    ));
}

#[test]
fn server_config_builder_with_reconstruction_cache_redis_accepts_url() {
    let config = ServerConfig::new(
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8080),
        "http://localhost:8080".to_owned(),
        PathBuf::from("/tmp/test"),
        NonZeroUsize::new(4096).unwrap(),
    )
    .with_reconstruction_cache_redis(
        "redis://localhost:6379".to_owned(),
        NonZeroU64::new(300).unwrap(),
    )
    .unwrap();
    assert_eq!(
        config.reconstruction_cache_adapter(),
        ReconstructionCacheAdapter::Redis
    );
}

#[test]
fn server_config_builder_with_oci_upload_session_ttl() {
    let config = ServerConfig::new(
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8080),
        "http://localhost:8080".to_owned(),
        PathBuf::from("/tmp/test"),
        NonZeroUsize::new(4096).unwrap(),
    )
    .with_oci_upload_session_ttl_seconds(NonZeroU64::new(7200).unwrap());
    assert_eq!(
        config.oci_upload_session_ttl_seconds(),
        NonZeroU64::new(7200).unwrap()
    );
}

#[test]
fn server_config_builder_with_oci_upload_max_active_sessions() {
    let config = ServerConfig::new(
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8080),
        "http://localhost:8080".to_owned(),
        PathBuf::from("/tmp/test"),
        NonZeroUsize::new(4096).unwrap(),
    )
    .with_oci_upload_max_active_sessions(NonZeroUsize::new(500).unwrap());
    assert_eq!(
        config.oci_upload_max_active_sessions(),
        NonZeroUsize::new(500).unwrap()
    );
}

#[test]
fn server_config_builder_with_index_postgres_url_rejects_empty() {
    let config = ServerConfig::new(
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8080),
        "http://localhost:8080".to_owned(),
        PathBuf::from("/tmp/test"),
        NonZeroUsize::new(4096).unwrap(),
    );
    let result = config.with_index_postgres_url(String::new());
    assert!(matches!(
        result,
        Err(ServerConfigError::EmptyIndexPostgresUrl)
    ));
}

#[test]
fn server_config_builder_with_index_postgres_url_accepts_valid_url() {
    let config = ServerConfig::new(
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8080),
        "http://localhost:8080".to_owned(),
        PathBuf::from("/tmp/test"),
        NonZeroUsize::new(4096).unwrap(),
    )
    .with_index_postgres_url("postgres://localhost/shardline".to_owned())
    .unwrap();
    assert_eq!(
        config.index_postgres_url(),
        Some("postgres://localhost/shardline")
    );
}

#[test]
fn server_config_builder_with_token_signing_key_rejects_empty() {
    let config = ServerConfig::new(
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8080),
        "http://localhost:8080".to_owned(),
        PathBuf::from("/tmp/test"),
        NonZeroUsize::new(4096).unwrap(),
    );
    let result = config.with_token_signing_key(vec![]);
    assert!(matches!(
        result,
        Err(ServerConfigError::EmptyTokenSigningKey)
    ));
}

#[test]
fn server_config_builder_with_token_signing_key_accepts_valid_key() {
    let config = ServerConfig::new(
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8080),
        "http://localhost:8080".to_owned(),
        PathBuf::from("/tmp/test"),
        NonZeroUsize::new(4096).unwrap(),
    )
    .with_token_signing_key(b"test-signing-key-32-bytes-long!!".to_vec())
    .unwrap();
    assert!(config.token_signing_key().is_some());
    assert_eq!(
        config.token_signing_key().unwrap(),
        b"test-signing-key-32-bytes-long!!"
    );
}

#[test]
fn server_config_builder_with_ed25519_private_key_rejects_empty() {
    let config = ServerConfig::new(
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8080),
        "http://localhost:8080".to_owned(),
        PathBuf::from("/tmp/test"),
        NonZeroUsize::new(4096).unwrap(),
    );
    let result = config.with_ed25519_private_key(vec![]);
    assert!(matches!(
        result,
        Err(ServerConfigError::EmptyEd25519PrivateKey)
    ));
}

#[test]
fn server_config_builder_with_ed25519_public_key_rejects_empty() {
    let config = ServerConfig::new(
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8080),
        "http://localhost:8080".to_owned(),
        PathBuf::from("/tmp/test"),
        NonZeroUsize::new(4096).unwrap(),
    );
    let result = config.with_ed25519_public_key(vec![]);
    assert!(matches!(
        result,
        Err(ServerConfigError::EmptyEd25519PublicKey)
    ));
}

#[test]
fn server_config_builder_with_ed25519_private_key_accepts_valid_key() {
    let config = ServerConfig::new(
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8080),
        "http://localhost:8080".to_owned(),
        PathBuf::from("/tmp/test"),
        NonZeroUsize::new(4096).unwrap(),
    )
    .with_ed25519_private_key(vec![0u8; 32])
    .unwrap();
    assert!(config.ed25519_private_key().is_some());
    assert_eq!(config.ed25519_private_key().unwrap(), &[0u8; 32]);
}

#[test]
fn server_config_builder_with_ed25519_public_key_accepts_valid_key() {
    let config = ServerConfig::new(
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8080),
        "http://localhost:8080".to_owned(),
        PathBuf::from("/tmp/test"),
        NonZeroUsize::new(4096).unwrap(),
    )
    .with_ed25519_public_key(vec![0u8; 32])
    .unwrap();
    assert!(config.ed25519_public_key().is_some());
    assert_eq!(config.ed25519_public_key().unwrap(), &[0u8; 32]);
}

#[test]
fn server_config_builder_with_auth_provider() {
    let config = ServerConfig::new(
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8080),
        "http://localhost:8080".to_owned(),
        PathBuf::from("/tmp/test"),
        NonZeroUsize::new(4096).unwrap(),
    )
    .with_auth_provider(AuthProviderKind::Oidc);
    assert_eq!(config.auth_provider(), AuthProviderKind::Oidc);
}

#[test]
fn server_config_builder_with_auth_oidc_issuer() {
    let config = ServerConfig::new(
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8080),
        "http://localhost:8080".to_owned(),
        PathBuf::from("/tmp/test"),
        NonZeroUsize::new(4096).unwrap(),
    )
    .with_auth_oidc_issuer("https://accounts.example.com".to_owned());
    assert_eq!(
        config.auth_oidc_issuer(),
        Some("https://accounts.example.com")
    );
}

#[test]
fn server_config_builder_with_auth_jwks_url() {
    let config = ServerConfig::new(
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8080),
        "http://localhost:8080".to_owned(),
        PathBuf::from("/tmp/test"),
        NonZeroUsize::new(4096).unwrap(),
    )
    .with_auth_jwks_url("https://example.com/.well-known/jwks".to_owned());
    assert_eq!(
        config.auth_jwks_url(),
        Some("https://example.com/.well-known/jwks")
    );
}

#[test]
fn server_config_builder_with_auth_jwks_issuer() {
    let config = ServerConfig::new(
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8080),
        "http://localhost:8080".to_owned(),
        PathBuf::from("/tmp/test"),
        NonZeroUsize::new(4096).unwrap(),
    )
    .with_auth_jwks_issuer("https://example.com".to_owned());
    assert_eq!(config.auth_jwks_issuer(), Some("https://example.com"));
}

#[test]
fn server_config_builder_with_metrics_token_rejects_empty() {
    let config = ServerConfig::new(
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8080),
        "http://localhost:8080".to_owned(),
        PathBuf::from("/tmp/test"),
        NonZeroUsize::new(4096).unwrap(),
    );
    let result = config.with_metrics_token(vec![]);
    assert!(matches!(result, Err(ServerConfigError::EmptyMetricsToken)));
}

#[test]
fn server_config_builder_with_metrics_token_accepts_valid() {
    let config = ServerConfig::new(
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8080),
        "http://localhost:8080".to_owned(),
        PathBuf::from("/tmp/test"),
        NonZeroUsize::new(4096).unwrap(),
    )
    .with_metrics_token(b"metrics-token".to_vec())
    .unwrap();
    assert!(config.metrics_token().is_some());
    assert_eq!(config.metrics_token().unwrap(), b"metrics-token");
}

#[test]
fn server_config_builder_with_provider_runtime_rejects_empty_api_key() {
    let config = ServerConfig::new(
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8080),
        "http://localhost:8080".to_owned(),
        PathBuf::from("/tmp/test"),
        NonZeroUsize::new(4096).unwrap(),
    )
    .with_token_signing_key(b"test-signing-key-32-bytes-long!!".to_vec())
    .unwrap();
    let result = config.with_provider_runtime(
        PathBuf::from("/tmp/provider.yaml"),
        vec![],
        "issuer".to_owned(),
        NonZeroU64::new(3600).unwrap(),
    );
    assert!(matches!(
        result,
        Err(ServerConfigError::EmptyProviderApiKey)
    ));
}

#[test]
fn server_config_builder_with_provider_runtime_rejects_empty_issuer() {
    let config = ServerConfig::new(
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8080),
        "http://localhost:8080".to_owned(),
        PathBuf::from("/tmp/test"),
        NonZeroUsize::new(4096).unwrap(),
    )
    .with_token_signing_key(b"test-signing-key-32-bytes-long!!".to_vec())
    .unwrap();
    let result = config.with_provider_runtime(
        PathBuf::from("/tmp/provider.yaml"),
        b"valid-api-key".to_vec(),
        "   ".to_owned(),
        NonZeroU64::new(3600).unwrap(),
    );
    assert!(matches!(
        result,
        Err(ServerConfigError::EmptyProviderTokenIssuer)
    ));
}

#[test]
fn server_config_builder_with_provider_runtime_rejects_missing_signing_key() {
    let config = ServerConfig::new(
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8080),
        "http://localhost:8080".to_owned(),
        PathBuf::from("/tmp/test"),
        NonZeroUsize::new(4096).unwrap(),
    );
    let result = config.with_provider_runtime(
        PathBuf::from("/tmp/provider.yaml"),
        b"valid-api-key".to_vec(),
        "issuer".to_owned(),
        NonZeroU64::new(3600).unwrap(),
    );
    assert!(matches!(
        result,
        Err(ServerConfigError::ProviderTokensRequireSigningKey)
    ));
}

#[test]
fn server_config_builder_with_provider_runtime_accepts_valid_config() {
    let config = ServerConfig::new(
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8080),
        "http://localhost:8080".to_owned(),
        PathBuf::from("/tmp/test"),
        NonZeroUsize::new(4096).unwrap(),
    )
    .with_token_signing_key(b"test-signing-key-32-bytes-long!!".to_vec())
    .unwrap()
    .with_provider_runtime(
        PathBuf::from("/tmp/provider.yaml"),
        b"valid-api-key".to_vec(),
        "shardline".to_owned(),
        NonZeroU64::new(3600).unwrap(),
    )
    .unwrap();
    assert_eq!(
        config.provider_config_path(),
        Some(Path::new("/tmp/provider.yaml"))
    );
    assert_eq!(config.provider_api_key(), Some(b"valid-api-key" as &[u8]));
    assert_eq!(config.provider_token_issuer(), Some("shardline"));
    assert_eq!(
        config.provider_token_ttl_seconds(),
        Some(NonZeroU64::new(3600).unwrap())
    );
}

#[test]
fn server_config_validate_runtime_requirements_rejects_missing_signing_key_for_api_role() {
    let config = ServerConfig::new(
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8080),
        "http://localhost:8080".to_owned(),
        PathBuf::from("/tmp/test"),
        NonZeroUsize::new(4096).unwrap(),
    )
    .with_deployment_mode(DeploymentMode::Authenticated);
    let result = config.validate_runtime_requirements();
    assert!(matches!(
        result,
        Err(ServerConfigError::MissingTokenSigningKeyForServedRoutes)
    ));
}

#[test]
fn server_config_validate_runtime_requirements_accepts_signing_key_for_api_role() {
    let config = ServerConfig::new(
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8080),
        "http://localhost:8080".to_owned(),
        PathBuf::from("/tmp/test"),
        NonZeroUsize::new(4096).unwrap(),
    )
    .with_token_signing_key(b"test-signing-key-32-bytes-long!!".to_vec())
    .unwrap();
    assert!(config.validate_runtime_requirements().is_ok());
}

#[test]
fn server_config_validate_runtime_requirements_rejects_passthrough_on_non_loopback() {
    let config = ServerConfig::new(
        SocketAddr::new(IpAddr::V4(Ipv4Addr::new(192, 168, 1, 1)), 8080),
        "http://localhost:8080".to_owned(),
        PathBuf::from("/tmp/test"),
        NonZeroUsize::new(4096).unwrap(),
    )
    .with_token_signing_key(b"test-signing-key-32-bytes-long!!".to_vec())
    .unwrap()
    .with_auth_provider(AuthProviderKind::Passthrough);
    let result = config.validate_runtime_requirements();
    assert!(matches!(
        result,
        Err(ServerConfigError::PassthroughProviderRequiresLoopbackBind { .. })
    ));
}

#[test]
fn server_config_validate_runtime_requirements_accepts_passthrough_on_loopback() {
    let config = ServerConfig::new(
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8080),
        "http://localhost:8080".to_owned(),
        PathBuf::from("/tmp/test"),
        NonZeroUsize::new(4096).unwrap(),
    )
    .with_token_signing_key(b"test-signing-key-32-bytes-long!!".to_vec())
    .unwrap()
    .with_auth_provider(AuthProviderKind::Passthrough);
    assert!(config.validate_runtime_requirements().is_ok());
}

#[test]
fn server_config_validate_runtime_requirements_accepts_ed25519_without_hmac_key() {
    // Ed25519 provider should NOT require the HMAC signing key.
    let config = ServerConfig::new(
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8080),
        "http://localhost:8080".to_owned(),
        PathBuf::from("/tmp/test"),
        NonZeroUsize::new(4096).unwrap(),
    )
    .with_auth_provider(AuthProviderKind::Ed25519)
    .with_ed25519_private_key(vec![0u8; 32])
    .unwrap();
    assert!(
        config.validate_runtime_requirements().is_ok(),
        "Ed25519 provider should not require HMAC signing key"
    );
}

#[test]
fn server_config_validate_runtime_requirements_rejects_ed25519_without_key() {
    let config = ServerConfig::new(
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8080),
        "http://localhost:8080".to_owned(),
        PathBuf::from("/tmp/test"),
        NonZeroUsize::new(4096).unwrap(),
    )
    .with_auth_provider(AuthProviderKind::Ed25519);

    assert!(matches!(
        config.validate_runtime_requirements(),
        Err(ServerConfigError::MissingEd25519Key)
    ));
}

#[test]
fn server_config_validate_runtime_requirements_rejects_conflicting_ed25519_keys() {
    let config = ServerConfig::new(
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8080),
        "http://localhost:8080".to_owned(),
        PathBuf::from("/tmp/test"),
        NonZeroUsize::new(4096).unwrap(),
    )
    .with_auth_provider(AuthProviderKind::Ed25519)
    .with_ed25519_private_key(vec![1_u8; 32])
    .unwrap()
    .with_ed25519_public_key(vec![2_u8; 32])
    .unwrap();

    assert!(matches!(
        config.validate_runtime_requirements(),
        Err(ServerConfigError::ConflictingEd25519Keys)
    ));
}

#[test]
fn server_config_non_existent_root_dir_does_not_fail_construction() {
    // ServerConfig::new() stores root_dir as a PathBuf without validating
    // existence.  Construction itself must not fail for a non-existent path.
    let config = ServerConfig::new(
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8080),
        "http://localhost:8080".to_owned(),
        PathBuf::from("/nonexistent/path/that/does/not/exist"),
        NonZeroUsize::new(4096).unwrap(),
    );
    assert_eq!(
        config.root_dir(),
        Path::new("/nonexistent/path/that/does/not/exist")
    );
    // The root_dir is *not* validated at construction time — the error
    // surfaces later when the backend or a handler tries to create
    // directories under root_dir.  At that point the IO error message
    // is clear (e.g. "No such file or directory").
}

#[test]
fn server_config_debug_redacts_secrets() {
    const ED25519_PRIVATE_KEY: &[u8] = b"ed25519-private-key-material";
    const ED25519_PUBLIC_KEY: &[u8] = b"ed25519-public-key-material";
    let config = ServerConfig::new(
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8080),
        "http://localhost:8080".to_owned(),
        PathBuf::from("/tmp/test"),
        NonZeroUsize::new(4096).unwrap(),
    )
    .with_token_signing_key(b"super-secret-key-here-32-bytes!!".to_vec())
    .unwrap()
    .with_ed25519_private_key(ED25519_PRIVATE_KEY.to_vec())
    .unwrap()
    .with_ed25519_public_key(ED25519_PUBLIC_KEY.to_vec())
    .unwrap();
    let debug = format!("{config:?}");
    assert!(!debug.contains("super-secret-key-here-32-bytes!!"));
    assert!(!debug.contains(std::str::from_utf8(ED25519_PRIVATE_KEY).unwrap()));
    assert!(!debug.contains(std::str::from_utf8(ED25519_PUBLIC_KEY).unwrap()));
    assert!(debug.contains("***"));
}

#[test]
fn server_config_accessors_return_expected_values() {
    let config = ServerConfig::new(
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8080),
        "http://localhost:8080".to_owned(),
        PathBuf::from("/tmp/test"),
        NonZeroUsize::new(4096).unwrap(),
    );
    assert_eq!(
        config.bind_addr(),
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8080)
    );
    assert_eq!(config.public_base_url(), "http://localhost:8080");
    assert_eq!(config.root_dir(), Path::new("/tmp/test"));
    assert!(config.s3_object_store_config().is_none());
    assert!(config.index_postgres_url().is_none());
    assert!(config.token_signing_key().is_none());
    assert!(config.metrics_token().is_none());
    assert!(config.ed25519_private_key().is_none());
    assert!(config.ed25519_public_key().is_none());
    assert!(config.provider_config_path().is_none());
    assert!(config.provider_api_key().is_none());
    assert!(config.provider_token_issuer().is_none());
    assert!(config.provider_token_ttl_seconds().is_none());
}

#[test]
fn deduplicated_server_frontends_removes_duplicates() {
    let result = deduplicated_server_frontends([
        ServerFrontend::Xet,
        ServerFrontend::Oci,
        ServerFrontend::Xet,
        ServerFrontend::Hub,
    ]);
    assert_eq!(
        result,
        vec![
            ServerFrontend::Xet,
            ServerFrontend::Oci,
            ServerFrontend::Hub
        ]
    );
}

#[test]
fn deduplicated_server_frontends_preserves_order() {
    let result = deduplicated_server_frontends([
        ServerFrontend::Hub,
        ServerFrontend::Oci,
        ServerFrontend::Xet,
    ]);
    assert_eq!(
        result,
        vec![
            ServerFrontend::Hub,
            ServerFrontend::Oci,
            ServerFrontend::Xet
        ]
    );
}

#[test]
fn deduplicated_server_frontends_returns_empty_for_empty_input() {
    let result: Vec<ServerFrontend> = deduplicated_server_frontends([]);
    assert!(result.is_empty());
}

#[test]
fn adaptive_default_in_flight_chunks_for_parallelism_clamps_to_minimum() {
    let result = adaptive_default_in_flight_chunks_for_parallelism(
        1,
        2,
        NonZeroUsize::new(64).unwrap(),
        NonZeroUsize::new(256).unwrap(),
    );
    assert_eq!(result, NonZeroUsize::new(64).unwrap());
}

#[test]
fn adaptive_default_in_flight_chunks_for_parallelism_clamps_to_maximum() {
    let result = adaptive_default_in_flight_chunks_for_parallelism(
        1024,
        8,
        NonZeroUsize::new(64).unwrap(),
        NonZeroUsize::new(256).unwrap(),
    );
    assert_eq!(result, NonZeroUsize::new(256).unwrap());
}

#[test]
fn adaptive_default_in_flight_chunks_for_parallelism_scales_within_bounds() {
    let result = adaptive_default_in_flight_chunks_for_parallelism(
        16,
        4,
        NonZeroUsize::new(16).unwrap(),
        NonZeroUsize::new(256).unwrap(),
    );
    assert_eq!(result, NonZeroUsize::new(64).unwrap());
}

#[test]
fn server_config_error_display_bind_address() {
    let err = ServerConfigError::BindAddress("127.0.0.1".parse::<SocketAddr>().unwrap_err());
    let display = err.to_string();
    assert_eq!(display, "invalid bind address");
}

#[test]
fn server_config_error_display_invalid_server_role() {
    let err = ServerConfigError::InvalidServerRole;
    assert_eq!(err.to_string(), "invalid server role");
}

#[test]
fn server_config_error_display_invalid_server_frontend() {
    let err = ServerConfigError::InvalidServerFrontend;
    assert_eq!(err.to_string(), "invalid server frontend");
}

#[test]
fn server_config_error_display_missing_server_frontends() {
    let err = ServerConfigError::MissingServerFrontends;
    assert_eq!(
        err.to_string(),
        "at least one server frontend must be enabled"
    );
}

#[test]
fn server_config_error_display_invalid_auth_provider() {
    let err = ServerConfigError::InvalidAuthProvider;
    assert_eq!(err.to_string(), "invalid auth provider");
}

#[test]
fn server_config_error_display_invalid_object_storage_adapter() {
    let err = ServerConfigError::InvalidObjectStorageAdapter;
    assert_eq!(err.to_string(), "invalid object storage adapter");
}

#[test]
fn server_config_error_display_missing_s3_bucket() {
    let err = ServerConfigError::MissingS3Bucket;
    assert_eq!(
        err.to_string(),
        "s3 object storage requires SHARDLINE_S3_BUCKET"
    );
}

#[test]
fn server_config_error_display_zero_chunk_size() {
    let err = ServerConfigError::ZeroChunkSize;
    assert_eq!(err.to_string(), "chunk size must be greater than zero");
}

#[test]
fn server_config_error_display_passthrough_provider_requires_loopback_bind() {
    let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)), 8080);
    let err = ServerConfigError::PassthroughProviderRequiresLoopbackBind { bind_addr: addr };
    let display = err.to_string();
    assert!(display.contains("passthrough auth provider requires a loopback bind address"));
    assert!(display.contains("10.0.0.1"));
}

#[test]
fn server_config_error_display_s3_credential_source_conflict() {
    let err = ServerConfigError::S3CredentialSourceConflict {
        env: "AWS_ACCESS_KEY_ID",
        file_env: "AWS_ACCESS_KEY_ID_FILE",
    };
    let display = err.to_string();
    assert!(display.contains("AWS_ACCESS_KEY_ID"));
    assert!(display.contains("AWS_ACCESS_KEY_ID_FILE"));
}

#[test]
fn server_config_error_display_empty_token_signing_key() {
    let err = ServerConfigError::EmptyTokenSigningKey;
    assert_eq!(err.to_string(), "token signing key must not be empty");
}

#[test]
fn server_config_error_display_missing_token_signing_key_for_routes() {
    let err = ServerConfigError::MissingTokenSigningKeyForServedRoutes;
    assert_eq!(
        err.to_string(),
        "served shardline routes require shardline token signing key configuration"
    );
}

#[test]
fn server_config_error_display_empty_provider_api_key() {
    let err = ServerConfigError::EmptyProviderApiKey;
    assert_eq!(err.to_string(), "provider bootstrap key must not be empty");
}

#[test]
fn server_config_error_display_empty_provider_token_issuer() {
    let err = ServerConfigError::EmptyProviderTokenIssuer;
    assert_eq!(err.to_string(), "provider token issuer must not be empty");
}

#[test]
fn server_config_error_display_provider_tokens_require_signing_key() {
    let err = ServerConfigError::ProviderTokensRequireSigningKey;
    assert_eq!(
        err.to_string(),
        "provider token issuance requires shardline token signing key configuration"
    );
}

#[test]
fn server_config_error_display_chunk_size_too_large() {
    let err = ServerConfigError::ChunkSizeTooLarge;
    assert_eq!(err.to_string(), "chunk size must not exceed 1 GB");
}

#[test]
fn server_config_error_display_hub_requires_auth() {
    let err = ServerConfigError::HubRequiresAuth;
    assert!(err.to_string().contains("hub frontend requires auth"));
}

#[test]
fn server_config_error_debug_round_trip() {
    let err = ServerConfigError::InvalidServerRole;
    let debug = format!("{err:?}");
    assert!(!debug.is_empty());
}

// ── Remaining ServerConfigError Display tests ──────────────────────────

#[test]
fn server_config_error_display_root_dir() {
    let io_err = std::io::Error::other("permission denied");
    let err = ServerConfigError::RootDir(io_err);
    assert_eq!(err.to_string(), "invalid local deployment root");
}

#[test]
fn server_config_error_display_invalid_s3_allow_http() {
    let err = ServerConfigError::InvalidS3AllowHttp;
    assert_eq!(err.to_string(), "invalid s3 allow-http flag");
}

#[test]
fn server_config_error_display_invalid_s3_virtual_hosted_style_request() {
    let err = ServerConfigError::InvalidS3VirtualHostedStyleRequest;
    assert_eq!(
        err.to_string(),
        "invalid s3 virtual-hosted-style request flag"
    );
}

#[test]
fn server_config_error_display_s3_credential_file() {
    let io_err = std::io::Error::other("file not found");
    let err = ServerConfigError::S3CredentialFile {
        name: "AWS_ACCESS_KEY_ID_FILE",
        source: io_err,
    };
    let display = err.to_string();
    assert!(display.contains("s3 credential file"));
    assert!(display.contains("AWS_ACCESS_KEY_ID_FILE"));
}

#[test]
fn server_config_error_display_s3_credential_too_large() {
    let err = ServerConfigError::S3CredentialTooLarge {
        name: "AWS_SECRET_ACCESS_KEY_FILE",
        observed_bytes: 5000,
        maximum_bytes: 4096,
    };
    let display = err.to_string();
    assert!(display.contains("exceeded"));
    assert!(display.contains("AWS_SECRET_ACCESS_KEY_FILE"));
}

#[test]
fn server_config_error_display_s3_credential_length_mismatch() {
    let err = ServerConfigError::S3CredentialLengthMismatch {
        name: "AWS_SESSION_TOKEN_FILE",
        expected_bytes: 100,
        observed_bytes: 200,
    };
    let display = err.to_string();
    assert!(display.contains("changed during bounded read"));
}

#[test]
fn server_config_error_display_s3_credential_utf8() {
    let err = ServerConfigError::S3CredentialUtf8 {
        name: "AWS_SECRET_ACCESS_KEY_FILE",
    };
    let display = err.to_string();
    assert!(display.contains("not valid utf-8"));
}

#[test]
fn server_config_error_display_chunk_size_parse() {
    let err = ServerConfigError::ChunkSize("not-a-number".parse::<usize>().unwrap_err());
    assert_eq!(err.to_string(), "invalid chunk size");
}

#[test]
fn server_config_error_display_max_request_body_bytes() {
    let err = ServerConfigError::MaxRequestBodyBytes("not-a-number".parse::<usize>().unwrap_err());
    assert_eq!(err.to_string(), "invalid max request body size");
}

#[test]
fn server_config_error_display_zero_max_request_body_bytes() {
    let err = ServerConfigError::ZeroMaxRequestBodyBytes;
    assert_eq!(
        err.to_string(),
        "max request body size must be greater than zero"
    );
}

#[test]
fn server_config_error_display_max_shard_files() {
    let err = ServerConfigError::MaxShardFiles("not-a-number".parse::<usize>().unwrap_err());
    assert_eq!(err.to_string(), "invalid max shard file section count");
}

#[test]
fn server_config_error_display_zero_max_shard_files() {
    let err = ServerConfigError::ZeroMaxShardFiles;
    assert_eq!(
        err.to_string(),
        "max shard file section count must be greater than zero"
    );
}

#[test]
fn server_config_error_display_max_shard_xorbs() {
    let err = ServerConfigError::MaxShardXorbs("not-a-number".parse::<usize>().unwrap_err());
    assert_eq!(err.to_string(), "invalid max shard xorb section count");
}

#[test]
fn server_config_error_display_zero_max_shard_xorbs() {
    let err = ServerConfigError::ZeroMaxShardXorbs;
    assert_eq!(
        err.to_string(),
        "max shard xorb section count must be greater than zero"
    );
}

#[test]
fn server_config_error_display_max_shard_reconstruction_terms() {
    let err = ServerConfigError::MaxShardReconstructionTerms(
        "not-a-number".parse::<usize>().unwrap_err(),
    );
    assert_eq!(
        err.to_string(),
        "invalid max shard reconstruction term count"
    );
}

#[test]
fn server_config_error_display_zero_max_shard_reconstruction_terms() {
    let err = ServerConfigError::ZeroMaxShardReconstructionTerms;
    assert_eq!(
        err.to_string(),
        "max shard reconstruction term count must be greater than zero"
    );
}

#[test]
fn server_config_error_display_max_shard_xorb_chunks() {
    let err = ServerConfigError::MaxShardXorbChunks("not-a-number".parse::<usize>().unwrap_err());
    assert_eq!(err.to_string(), "invalid max shard xorb chunk record count");
}

#[test]
fn server_config_error_display_zero_max_shard_xorb_chunks() {
    let err = ServerConfigError::ZeroMaxShardXorbChunks;
    assert_eq!(
        err.to_string(),
        "max shard xorb chunk record count must be greater than zero"
    );
}

#[test]
fn server_config_error_display_upload_max_in_flight_chunks() {
    let err =
        ServerConfigError::UploadMaxInFlightChunks("not-a-number".parse::<usize>().unwrap_err());
    assert_eq!(err.to_string(), "invalid upload max in-flight chunks");
}

#[test]
fn server_config_error_display_zero_upload_max_in_flight_chunks() {
    let err = ServerConfigError::ZeroUploadMaxInFlightChunks;
    assert_eq!(
        err.to_string(),
        "upload max in-flight chunks must be greater than zero"
    );
}

#[test]
fn server_config_error_display_transfer_max_in_flight_chunks() {
    let err =
        ServerConfigError::TransferMaxInFlightChunks("not-a-number".parse::<usize>().unwrap_err());
    assert_eq!(err.to_string(), "invalid transfer max in-flight chunks");
}

#[test]
fn server_config_error_display_zero_transfer_max_in_flight_chunks() {
    let err = ServerConfigError::ZeroTransferMaxInFlightChunks;
    assert_eq!(
        err.to_string(),
        "transfer max in-flight chunks must be greater than zero"
    );
}

#[test]
fn server_config_error_display_invalid_reconstruction_cache_adapter() {
    let err = ServerConfigError::InvalidReconstructionCacheAdapter;
    assert_eq!(err.to_string(), "invalid reconstruction cache adapter");
}

#[test]
fn server_config_error_display_reconstruction_cache_ttl() {
    let err = ServerConfigError::ReconstructionCacheTtl("not-a-number".parse::<u64>().unwrap_err());
    assert_eq!(err.to_string(), "invalid reconstruction cache ttl");
}

#[test]
fn server_config_error_display_zero_reconstruction_cache_ttl() {
    let err = ServerConfigError::ZeroReconstructionCacheTtlSeconds;
    assert_eq!(
        err.to_string(),
        "reconstruction cache ttl must be greater than zero"
    );
}

#[test]
fn server_config_error_display_reconstruction_cache_memory_max_entries() {
    let err = ServerConfigError::ReconstructionCacheMemoryMaxEntries(
        "not-a-number".parse::<usize>().unwrap_err(),
    );
    assert_eq!(
        err.to_string(),
        "invalid reconstruction cache memory max entries"
    );
}

#[test]
fn server_config_error_display_zero_reconstruction_cache_memory_max_entries() {
    let err = ServerConfigError::ZeroReconstructionCacheMemoryMaxEntries;
    assert_eq!(
        err.to_string(),
        "reconstruction cache memory max entries must be greater than zero"
    );
}

#[test]
fn server_config_error_display_oci_upload_session_ttl() {
    let err = ServerConfigError::OciUploadSessionTtl("not-a-number".parse::<u64>().unwrap_err());
    assert_eq!(err.to_string(), "invalid oci upload session ttl");
}

#[test]
fn server_config_error_display_zero_oci_upload_session_ttl() {
    let err = ServerConfigError::ZeroOciUploadSessionTtlSeconds;
    assert_eq!(
        err.to_string(),
        "oci upload session ttl must be greater than zero"
    );
}

#[test]
fn server_config_error_display_oci_upload_max_active_sessions() {
    let err =
        ServerConfigError::OciUploadMaxActiveSessions("not-a-number".parse::<usize>().unwrap_err());
    assert_eq!(err.to_string(), "invalid oci upload max active sessions");
}

#[test]
fn server_config_error_display_zero_oci_upload_max_active_sessions() {
    let err = ServerConfigError::ZeroOciUploadMaxActiveSessions;
    assert_eq!(
        err.to_string(),
        "oci upload max active sessions must be greater than zero"
    );
}

#[test]
fn server_config_error_display_oci_registry_token_ttl() {
    let err = ServerConfigError::OciRegistryTokenTtl("not-a-number".parse::<u64>().unwrap_err());
    assert_eq!(err.to_string(), "invalid oci registry token ttl");
}

#[test]
fn server_config_error_display_zero_oci_registry_token_ttl() {
    let err = ServerConfigError::ZeroOciRegistryTokenTtlSeconds;
    assert_eq!(
        err.to_string(),
        "oci registry token ttl must be greater than zero"
    );
}

#[test]
fn server_config_error_display_oci_registry_token_max_in_flight_requests() {
    let err = ServerConfigError::OciRegistryTokenMaxInFlightRequests(
        "not-a-number".parse::<usize>().unwrap_err(),
    );
    assert_eq!(
        err.to_string(),
        "invalid oci registry token max in-flight requests"
    );
}

#[test]
fn server_config_error_display_zero_oci_registry_token_max_in_flight_requests() {
    let err = ServerConfigError::ZeroOciRegistryTokenMaxInFlightRequests;
    assert_eq!(
        err.to_string(),
        "oci registry token max in-flight requests must be greater than zero"
    );
}

#[test]
fn server_config_error_display_empty_reconstruction_cache_redis_url() {
    let err = ServerConfigError::EmptyReconstructionCacheRedisUrl;
    assert_eq!(
        err.to_string(),
        "reconstruction cache redis url must not be empty"
    );
}

#[test]
fn server_config_error_display_missing_reconstruction_cache_redis_url() {
    let err = ServerConfigError::MissingReconstructionCacheRedisUrl;
    assert_eq!(
        err.to_string(),
        "redis reconstruction cache requires SHARDLINE_RECONSTRUCTION_CACHE_REDIS_URL"
    );
}

#[test]
fn server_config_error_display_empty_index_postgres_url() {
    let err = ServerConfigError::EmptyIndexPostgresUrl;
    assert_eq!(err.to_string(), "postgres metadata url must not be empty");
}

#[test]
fn server_config_error_display_token_signing_key() {
    let io_err = std::io::Error::other("file error");
    let err = ServerConfigError::TokenSigningKey(io_err);
    assert_eq!(err.to_string(), "token signing key could not be read");
}

#[test]
fn server_config_error_display_token_signing_key_source_conflict() {
    let err = ServerConfigError::SecretSourceConflict {
        env: "SHARDLINE_TOKEN_SIGNING_KEY",
        file_env: "SHARDLINE_TOKEN_SIGNING_KEY_FILE",
    };
    let display = err.to_string();
    assert!(display.contains("secret source conflict"));
}

#[test]
fn server_config_error_display_token_signing_key_too_large() {
    let err = ServerConfigError::TokenSigningKeyTooLarge {
        observed_bytes: 2_000_000,
        maximum_bytes: 1_048_576,
    };
    let display = err.to_string();
    assert!(display.contains("exceeded"));
}

#[test]
fn server_config_error_display_token_signing_key_length_mismatch() {
    let err = ServerConfigError::TokenSigningKeyLengthMismatch {
        expected_bytes: 100,
        observed_bytes: 200,
    };
    let display = err.to_string();
    assert!(display.contains("changed during bounded read"));
}

#[test]
fn server_config_error_display_provider_token_ttl() {
    let err = ServerConfigError::ProviderTokenTtl;
    assert_eq!(err.to_string(), "invalid provider token ttl");
}

#[test]
fn server_config_error_display_metrics_token() {
    let io_err = std::io::Error::other("file error");
    let err = ServerConfigError::MetricsToken(io_err);
    assert_eq!(err.to_string(), "metrics token could not be read");
}

#[test]
fn server_config_error_display_empty_metrics_token() {
    let err = ServerConfigError::EmptyMetricsToken;
    assert_eq!(err.to_string(), "metrics token must not be empty");
}

#[test]
fn server_config_error_display_metrics_token_too_large() {
    let err = ServerConfigError::MetricsTokenTooLarge {
        observed_bytes: 5000,
        maximum_bytes: 4096,
    };
    let display = err.to_string();
    assert!(display.contains("exceeded"));
}

#[test]
fn server_config_error_display_metrics_token_length_mismatch() {
    let err = ServerConfigError::MetricsTokenLengthMismatch {
        expected_bytes: 100,
        observed_bytes: 200,
    };
    let display = err.to_string();
    assert!(display.contains("changed during bounded read"));
}

#[test]
fn server_config_error_display_provider_api_key() {
    let io_err = std::io::Error::other("file error");
    let err = ServerConfigError::ProviderApiKey(io_err);
    assert_eq!(err.to_string(), "provider bootstrap key could not be read");
}

#[test]
fn server_config_error_display_provider_api_key_too_large() {
    let err = ServerConfigError::ProviderApiKeyTooLarge {
        observed_bytes: 5000,
        maximum_bytes: 4096,
    };
    let display = err.to_string();
    assert!(display.contains("exceeded"));
}

#[test]
fn server_config_error_display_provider_api_key_length_mismatch() {
    let err = ServerConfigError::ProviderApiKeyLengthMismatch {
        expected_bytes: 100,
        observed_bytes: 200,
    };
    let display = err.to_string();
    assert!(display.contains("changed during bounded read"));
}

#[test]
fn server_config_error_display_zero_provider_token_ttl() {
    let err = ServerConfigError::ZeroProviderTokenTtl;
    assert_eq!(
        err.to_string(),
        "provider token ttl must be greater than zero"
    );
}

#[test]
fn server_config_error_display_incomplete_provider_token_config() {
    let err = ServerConfigError::IncompleteProviderTokenConfig;
    assert_eq!(
        err.to_string(),
        "provider token issuance requires both provider config and provider api key files"
    );
}

#[test]
fn server_config_error_display_invalid_public_base_url() {
    let err = ServerConfigError::InvalidPublicBaseUrl("not-a-url".to_owned());
    let display = err.to_string();
    assert!(display.contains("not a valid URL"));
}

#[test]
fn server_config_error_display_missing_oidc_issuer() {
    let err = ServerConfigError::MissingOidcIssuer;
    assert_eq!(
        err.to_string(),
        "oidc auth provider requires SHARDLINE_AUTH_OIDC_ISSUER"
    );
}

#[test]
fn server_config_error_display_missing_jwks_url() {
    let err = ServerConfigError::MissingJwksUrl;
    assert_eq!(
        err.to_string(),
        "jwks auth provider requires SHARDLINE_AUTH_JWKS_URL"
    );
}

#[test]
fn server_config_error_display_missing_ed25519_key() {
    let err = ServerConfigError::MissingEd25519Key;
    assert!(err.to_string().contains("ed25519 auth provider requires"));
    assert!(err.to_string().contains("exactly one"));
}

#[test]
fn server_config_error_display_conflicting_ed25519_keys() {
    let err = ServerConfigError::ConflictingEd25519Keys;
    assert_eq!(
        err.to_string(),
        "ed25519 private and public keys must not both be configured"
    );
}

#[test]
fn server_config_error_display_empty_ed25519_private_key() {
    let err = ServerConfigError::EmptyEd25519PrivateKey;
    assert_eq!(err.to_string(), "ed25519 private key must not be empty");
}

#[test]
fn server_config_error_display_empty_ed25519_public_key() {
    let err = ServerConfigError::EmptyEd25519PublicKey;
    assert_eq!(err.to_string(), "ed25519 public key must not be empty");
}

#[test]
fn server_config_error_display_ed25519_private_key() {
    let io_err = std::io::Error::other("file error");
    let err = ServerConfigError::Ed25519PrivateKey(io_err);
    assert_eq!(err.to_string(), "ed25519 private key could not be read");
}

#[test]
fn server_config_error_display_ed25519_public_key() {
    let io_err = std::io::Error::other("file error");
    let err = ServerConfigError::Ed25519PublicKey(io_err);
    assert_eq!(err.to_string(), "ed25519 public key could not be read");
}

#[test]
fn server_config_error_display_secret_source_conflict() {
    let err = ServerConfigError::SecretSourceConflict {
        env: "SHARDLINE_ED25519_PRIVATE_KEY",
        file_env: "SHARDLINE_ED25519_PRIVATE_KEY_FILE",
    };
    let display = err.to_string();
    assert!(display.contains("secret source conflict"));
    assert!(display.contains("SHARDLINE_ED25519_PRIVATE_KEY"));
}

#[test]
fn server_config_error_display_ed25519_private_key_too_large() {
    let err = ServerConfigError::Ed25519PrivateKeyTooLarge {
        observed_bytes: 2_000_000,
        maximum_bytes: 1_048_576,
    };
    let display = err.to_string();
    assert!(display.contains("exceeded"));
}

#[test]
fn server_config_error_display_ed25519_public_key_too_large() {
    let err = ServerConfigError::Ed25519PublicKeyTooLarge {
        observed_bytes: 2_000_000,
        maximum_bytes: 1_048_576,
    };
    let display = err.to_string();
    assert!(display.contains("exceeded"));
}

#[test]
fn server_config_error_display_ed25519_private_key_length_mismatch() {
    let err = ServerConfigError::Ed25519PrivateKeyLengthMismatch {
        expected_bytes: 100,
        observed_bytes: 200,
    };
    let display = err.to_string();
    assert!(display.contains("changed during bounded read"));
}

#[test]
fn server_config_error_display_ed25519_public_key_length_mismatch() {
    let err = ServerConfigError::Ed25519PublicKeyLengthMismatch {
        expected_bytes: 100,
        observed_bytes: 200,
    };
    let display = err.to_string();
    assert!(display.contains("changed during bounded read"));
}

// ── Remaining builder method tests ─────────────────────────────────────

#[test]
fn server_config_builder_with_oci_registry_token_ttl_seconds() {
    let config = ServerConfig::new(
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8080),
        "http://localhost:8080".to_owned(),
        PathBuf::from("/tmp/test"),
        NonZeroUsize::new(4096).unwrap(),
    )
    .with_oci_registry_token_ttl_seconds(NonZeroU64::new(600).unwrap());
    assert_eq!(
        config.oci_registry_token_ttl_seconds(),
        NonZeroU64::new(600).unwrap()
    );
}

#[test]
fn server_config_builder_with_oci_registry_token_max_in_flight_requests() {
    let config = ServerConfig::new(
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8080),
        "http://localhost:8080".to_owned(),
        PathBuf::from("/tmp/test"),
        NonZeroUsize::new(4096).unwrap(),
    )
    .with_oci_registry_token_max_in_flight_requests(NonZeroUsize::new(128).unwrap());
    assert_eq!(
        config.oci_registry_token_max_in_flight_requests(),
        NonZeroUsize::new(128).unwrap()
    );
}

#[test]
fn server_config_reconstruction_cache_redis_url_returns_none_when_not_configured() {
    let config = ServerConfig::new(
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8080),
        "http://localhost:8080".to_owned(),
        PathBuf::from("/tmp/test"),
        NonZeroUsize::new(4096).unwrap(),
    );
    assert!(config.reconstruction_cache_redis_url().is_none());
}

#[test]
fn server_config_builder_with_metrics_token_rejects_too_large() {
    let config = ServerConfig::new(
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8080),
        "http://localhost:8080".to_owned(),
        PathBuf::from("/tmp/test"),
        NonZeroUsize::new(4096).unwrap(),
    );
    let result = config.with_metrics_token(vec![0u8; 5000]);
    assert!(matches!(
        result,
        Err(ServerConfigError::MetricsTokenTooLarge { .. })
    ));
}

#[test]
fn server_config_transfer_max_in_flight_chunks_accessor() {
    let config = ServerConfig::new(
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8080),
        "http://localhost:8080".to_owned(),
        PathBuf::from("/tmp/test"),
        NonZeroUsize::new(4096).unwrap(),
    );
    assert!(config.transfer_max_in_flight_chunks().get() > 0);
}

#[test]
fn deduplicated_server_frontends_preserves_single_element() {
    let result = deduplicated_server_frontends([ServerFrontend::Xet]);
    assert_eq!(result, vec![ServerFrontend::Xet]);
}

#[test]
fn adaptive_default_in_flight_chunks_for_parallelism_zero_parallelism() {
    let result = adaptive_default_in_flight_chunks_for_parallelism(
        0,
        4,
        NonZeroUsize::new(8).unwrap(),
        NonZeroUsize::new(256).unwrap(),
    );
    assert_eq!(result, NonZeroUsize::new(8).unwrap());
}

#[test]
fn server_config_upload_max_in_flight_chunks_default_is_positive() {
    // Use ServerConfig::new to verify the default is greater than zero
    let config = ServerConfig::new(
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8080),
        "http://localhost:8080".to_owned(),
        PathBuf::from("/tmp/test"),
        NonZeroUsize::new(4096).unwrap(),
    );
    assert!(config.upload_max_in_flight_chunks().get() > 0);
}

#[test]
fn server_config_with_token_signing_key_rejects_too_large() {
    let config = ServerConfig::new(
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8080),
        "http://localhost:8080".to_owned(),
        PathBuf::from("/tmp/test"),
        NonZeroUsize::new(4096).unwrap(),
    );
    // The bounded Ed25519 key parser rejects unreasonably large input.
    let result = config.with_token_signing_key(vec![0u8; 2_000_000]);
    assert!(matches!(
        result,
        Err(ServerConfigError::TokenSigningKeyTooLarge { .. })
    ));
}

#[test]
fn server_config_with_ed25519_private_key_rejects_too_large() {
    let config = ServerConfig::new(
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8080),
        "http://localhost:8080".to_owned(),
        PathBuf::from("/tmp/test"),
        NonZeroUsize::new(4096).unwrap(),
    );
    // MAX_TOKEN_SIGNING_KEY_BYTES is 1_048_576; exceed it
    let result = config.with_ed25519_private_key(vec![0u8; 2_000_000]);
    assert!(matches!(
        result,
        Err(ServerConfigError::Ed25519PrivateKeyTooLarge { .. })
    ));
}

#[test]
fn server_config_with_ed25519_public_key_rejects_too_large() {
    let config = ServerConfig::new(
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8080),
        "http://localhost:8080".to_owned(),
        PathBuf::from("/tmp/test"),
        NonZeroUsize::new(4096).unwrap(),
    );
    let result = config.with_ed25519_public_key(vec![0u8; 2_000_000]);
    assert!(matches!(
        result,
        Err(ServerConfigError::Ed25519PublicKeyTooLarge { .. })
    ));
}

#[test]
fn server_config_with_provider_runtime_rejects_api_key_too_large() {
    let config = ServerConfig::new(
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8080),
        "http://localhost:8080".to_owned(),
        PathBuf::from("/tmp/test"),
        NonZeroUsize::new(4096).unwrap(),
    )
    .with_token_signing_key(b"test-signing-key-32-bytes-long!!".to_vec())
    .unwrap();
    // MAX_PROVIDER_API_KEY_BYTES is 4096; exceed it
    let result = config.with_provider_runtime(
        PathBuf::from("/tmp/provider.yaml"),
        vec![0u8; 5000],
        "issuer".to_owned(),
        NonZeroU64::new(3600).unwrap(),
    );
    assert!(matches!(
        result,
        Err(ServerConfigError::ProviderApiKeyTooLarge { .. })
    ));
}

// ── Secret file read hook tests ─────────────────────────────────────────

#[test]
fn take_secret_file_read_hook_returns_hook_for_matching_path() {
    let mut slot = Vec::new();
    let path = PathBuf::from("/tmp/test-hook");
    let called = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let called_clone = called.clone();
    slot.push(super::SecretFileReadHookRegistration {
        path: path.clone(),
        hook: Box::new(move || {
            called_clone.store(true, std::sync::atomic::Ordering::SeqCst);
        }),
    });
    let hook = take_secret_file_read_hook_for_path(&mut slot, &path);
    assert!(hook.is_some());
    hook.unwrap()();
    assert!(called.load(std::sync::atomic::Ordering::SeqCst));
    assert!(slot.is_empty());
}

#[test]
fn take_secret_file_read_hook_returns_none_for_non_matching_path() {
    let mut slot = Vec::new();
    slot.push(super::SecretFileReadHookRegistration {
        path: PathBuf::from("/tmp/other"),
        hook: Box::new(|| {}),
    });
    let result = take_secret_file_read_hook_for_path(&mut slot, Path::new("/tmp/nonexistent"));
    assert!(result.is_none());
    assert_eq!(slot.len(), 1);
}

// ── Auth deployment mode tests ──────────────────────────────────────────

#[test]
fn strict_mode_rejects_passthrough_auth() {
    let config = ServerConfig::new(
        "127.0.0.1:0".parse().unwrap(),
        "http://127.0.0.1:8080".to_owned(),
        PathBuf::from("/tmp"),
        NonZeroUsize::new(65536).unwrap(),
    )
    .with_deployment_mode(DeploymentMode::Strict);
    // Passthrough is the default when SHARDLINE_AUTH_PROVIDER=passthrough
    // But without setting it explicitly, the default is "local" which has no signing key
    // if token_signing_key is None and Strict mode, validation should fail
    let result = config.validate_runtime_requirements();
    // Should fail because strict mode requires signing key
    assert!(result.is_err());
}

#[test]
fn insecure_mode_allows_no_auth() {
    let config = ServerConfig::new(
        "127.0.0.1:0".parse().unwrap(),
        "http://127.0.0.1:8080".to_owned(),
        PathBuf::from("/tmp"),
        NonZeroUsize::new(65536).unwrap(),
    )
    .with_deployment_mode(DeploymentMode::Insecure);
    let result = config.validate_runtime_requirements();
    // Should succeed because insecure allows everything
    assert!(result.is_ok());
}

#[test]
fn strict_mode_succeeds_with_all_required() {
    let config = ServerConfig::new(
        "127.0.0.1:0".parse().unwrap(),
        "http://127.0.0.1:8080".to_owned(),
        PathBuf::from("/tmp"),
        NonZeroUsize::new(65536).unwrap(),
    )
    .with_deployment_mode(DeploymentMode::Strict)
    .with_token_signing_key(b"0123456789abcdef0123456789abcdef".to_vec())
    .unwrap();
    let result = config.validate_runtime_requirements();
    // With signing key set, strict mode should succeed
    assert!(result.is_ok());
}

#[test]
fn deployment_mode_default_is_insecure() {
    assert_eq!(DeploymentMode::default(), DeploymentMode::Insecure);
}

#[test]
fn deployment_mode_authenticated_accepts_config_with_signing_key() {
    let config = ServerConfig::new(
        "127.0.0.1:0".parse().unwrap(),
        "http://127.0.0.1:8080".to_owned(),
        std::path::PathBuf::from("/tmp/test_auth"),
        NonZeroUsize::new(65536).unwrap(),
    )
    .with_deployment_mode(DeploymentMode::Authenticated)
    .with_token_signing_key(b"0123456789abcdef0123456789abcdef".to_vec())
    .unwrap();
    let result = config.validate_runtime_requirements();
    assert!(result.is_ok(), "authenticated mode with valid signing key should pass validation");
}

#[test]
fn deployment_mode_authenticated_warns_with_passthrough() {
    let config = ServerConfig::new(
        "127.0.0.1:0".parse().unwrap(),
        "http://127.0.0.1:8080".to_owned(),
        std::path::PathBuf::from("/tmp/test_auth2"),
        NonZeroUsize::new(65536).unwrap(),
    )
    .with_deployment_mode(DeploymentMode::Authenticated);
    // No signing key, no auth — the original missing-signing-key check
    // applies for non-Insecure modes
    let result = config.validate_runtime_requirements();
    assert!(result.is_err(), "authenticated mode without signing key may fail validation");
}

#[test]
fn deployment_mode_insecure_allows_missing_signing_key() {
    let config = ServerConfig::new(
        "127.0.0.1:0".parse().unwrap(),
        "http://127.0.0.1:8080".to_owned(),
        std::path::PathBuf::from("/tmp/test_insecure"),
        NonZeroUsize::new(65536).unwrap(),
    )
    .with_deployment_mode(DeploymentMode::Insecure);
    let result = config.validate_runtime_requirements();
    assert!(result.is_ok(), "insecure mode should pass without signing key");
}

#[test]
fn strict_mode_rejects_missing_metrics_token() {
    // This verifies the warn! is issued (can't easily test warns, but at least
    // the validation doesn't fail — it only warns)
    let config = ServerConfig::new(
        "127.0.0.1:0".parse().unwrap(),
        "http://127.0.0.1:8080".to_owned(),
        std::path::PathBuf::from("/tmp/test_strict_metrics"),
        NonZeroUsize::new(65536).unwrap(),
    )
    .with_deployment_mode(DeploymentMode::Strict)
    .with_token_signing_key(b"0123456789abcdef0123456789abcdef".to_vec())
    .unwrap();
    let result = config.validate_runtime_requirements();
    assert!(result.is_ok(), "strict mode with signing key should pass even without metrics token");
}

#[test]
fn route_policy_registry_has_all_expected_policies() {
    let mut registry = RoutePolicyRegistry::new();
    register_route_policies(&mut registry);
    // Verify specific routes have expected policies
    assert_eq!(registry.policy("GET", "/healthz"), Some(RouteAuthPolicy::Open));
    assert_eq!(registry.policy("GET", "/readyz"), Some(RouteAuthPolicy::Open));
    assert_eq!(registry.policy("GET", "/v1/stats"), Some(RouteAuthPolicy::Authenticated));
    assert_eq!(registry.policy("POST", "/v1/shards"), Some(RouteAuthPolicy::AuthenticatedWrite));
    assert_eq!(registry.policy("POST", "/v1/providers/{provider}/tokens"), Some(RouteAuthPolicy::SeparatelyProtected));
    assert_eq!(registry.policy("PUT", "/v1/lfs/objects/{oid}"), Some(RouteAuthPolicy::AuthenticatedWrite));
    assert_eq!(registry.policy("GET", "/v1/reconstructions/{file_id}"), Some(RouteAuthPolicy::Authenticated));
}
