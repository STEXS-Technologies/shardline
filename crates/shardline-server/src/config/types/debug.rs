use std::fmt;

use super::config::ServerConfig;
use super::enums::{AuthConfig, CacheConfig, OciConfig, ProviderConfig};

impl fmt::Debug for ServerConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ServerConfig")
            .field("bind_addr", &self.bind_addr)
            .field("server_role", &self.server_role)
            .field("server_frontends", &self.server_frontends)
            .field("public_base_url", &self.public_base_url)
            .field("root_dir", &self.root_dir)
            .field("object_storage_adapter", &self.object_storage_adapter)
            .field("s3_object_store_config", &self.s3_object_store_config)
            .field("max_request_body_bytes", &self.max_request_body_bytes)
            .field("shard_metadata_limits", &self.shard_metadata_limits)
            .field("chunk_size", &self.chunk_size)
            .field(
                "upload_max_in_flight_chunks",
                &self.upload_max_in_flight_chunks,
            )
            .field(
                "transfer_max_in_flight_chunks",
                &self.transfer_max_in_flight_chunks,
            )
            .field(
                "index_postgres_url",
                &self.index_postgres_url.as_ref().map(|_url| "***"),
            )
            .field(
                "metrics_token",
                &self.metrics_token.as_ref().map(|_token| "***"),
            )
            .field("auth", &self.auth)
            .field("oci", &self.oci)
            .field("cache", &self.cache)
            .field("provider", &self.provider)
            .field("shutdown_timeout", &self.shutdown_timeout)
            .finish()
    }
}

impl fmt::Debug for AuthConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AuthConfig")
            .field(
                "token_signing_key",
                &self.token_signing_key.as_ref().map(|_key| "***"),
            )
            .field("auth_provider", &self.auth_provider)
            .field("auth_oidc_issuer", &self.auth_oidc_issuer)
            .field("auth_oidc_audience", &self.auth_oidc_audience)
            .field("auth_jwks_url", &self.auth_jwks_url)
            .field("auth_jwks_issuer", &self.auth_jwks_issuer)
            .field(
                "ed25519_private_key",
                &self.ed25519_private_key.as_ref().map(|_key| "***"),
            )
            .field(
                "ed25519_public_key",
                &self.ed25519_public_key.as_ref().map(|_key| "***"),
            )
            .finish()
    }
}

impl fmt::Debug for OciConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OciConfig")
            .field(
                "upload_session_ttl_seconds",
                &self.upload_session_ttl_seconds,
            )
            .field(
                "upload_max_active_sessions",
                &self.upload_max_active_sessions,
            )
            .field(
                "registry_token_ttl_seconds",
                &self.registry_token_ttl_seconds,
            )
            .field(
                "registry_token_max_in_flight_requests",
                &self.registry_token_max_in_flight_requests,
            )
            .finish()
    }
}

impl fmt::Debug for CacheConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CacheConfig")
            .field("adapter", &self.adapter)
            .field("ttl_seconds", &self.ttl_seconds)
            .field("memory_max_entries", &self.memory_max_entries)
            .field("redis_url", &self.redis_url.as_ref().map(|_url| "***"))
            .field("redis_tls", &self.redis_tls)
            .finish()
    }
}

impl fmt::Debug for ProviderConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ProviderConfig")
            .field("config_path", &self.config_path)
            .field("api_key", &self.api_key.as_ref().map(|_key| "***"))
            .field("token_issuer", &self.token_issuer)
            .field("token_ttl_seconds", &self.token_ttl_seconds)
            .finish()
    }
}
