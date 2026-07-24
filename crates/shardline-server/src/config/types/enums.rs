use std::{
    num::{NonZeroU64, NonZeroUsize},
    path::PathBuf,
};

use shardline_cache::RedisTlsConfig;
use shardline_protocol::{SecretBytes, SecretString};

use super::error::ServerConfigError;
use crate::reconstruction_cache::ReconstructionCacheAdapter;

/// Authentication provider selection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthProviderKind {
    /// Local shared-key HMAC-SHA256 signing (default).
    Local,
    /// OpenID Connect issuer validation.
    Oidc,
    /// Static JWKS endpoint validation.
    Jwks,
    /// Trust-all passthrough for development mode.
    Passthrough,
    /// Ed25519 asymmetric-key signing and verification.
    Ed25519,
}

impl AuthProviderKind {
    /// Parses an auth provider token.
    ///
    /// # Errors
    ///
    /// Returns [`ServerConfigError::InvalidAuthProvider`] when the token is not
    /// supported.
    pub fn parse(value: &str) -> Result<Self, ServerConfigError> {
        match value {
            "local" => Ok(Self::Local),
            "oidc" => Ok(Self::Oidc),
            "jwks" => Ok(Self::Jwks),
            "passthrough" => Ok(Self::Passthrough),
            "ed25519" => Ok(Self::Ed25519),
            _other => Err(ServerConfigError::InvalidAuthProvider),
        }
    }
}

/// Immutable object-storage adapter selection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ObjectStorageAdapter {
    /// Store immutable CAS objects on the local filesystem.
    Local,
    /// Store immutable CAS objects in an S3-compatible bucket.
    S3,
}

impl ObjectStorageAdapter {
    /// Parses an object-storage adapter token.
    ///
    /// # Errors
    ///
    /// Returns [`ServerConfigError::InvalidObjectStorageAdapter`] when the token is not
    /// supported.
    pub fn parse(value: &str) -> Result<Self, ServerConfigError> {
        match value {
            "local" => Ok(Self::Local),
            "s3" => Ok(Self::S3),
            _other => Err(ServerConfigError::InvalidObjectStorageAdapter),
        }
    }
}

/// Authentication configuration.
#[derive(Clone, PartialEq, Eq)]
pub struct AuthConfig {
    pub token_signing_key: Option<SecretBytes>,
    pub auth_provider: AuthProviderKind,
    pub auth_oidc_issuer: Option<String>,
    pub auth_jwks_url: Option<String>,
    pub auth_jwks_issuer: Option<String>,
    pub ed25519_private_key: Option<SecretBytes>,
    pub ed25519_public_key: Option<SecretBytes>,
}

/// OCI registry configuration.
#[derive(Clone, PartialEq, Eq)]
pub struct OciConfig {
    pub upload_session_ttl_seconds: NonZeroU64,
    pub upload_max_active_sessions: NonZeroUsize,
    pub registry_token_ttl_seconds: NonZeroU64,
    pub registry_token_max_in_flight_requests: NonZeroUsize,
}

/// Reconstruction cache configuration.
#[derive(Clone, PartialEq, Eq)]
pub struct CacheConfig {
    pub adapter: ReconstructionCacheAdapter,
    pub ttl_seconds: NonZeroU64,
    pub memory_max_entries: NonZeroUsize,
    pub redis_url: Option<SecretString>,
    pub redis_tls: Option<RedisTlsConfig>,
}

/// Provider token issuance configuration.
#[derive(Clone, PartialEq, Eq)]
pub struct ProviderConfig {
    pub config_path: Option<PathBuf>,
    pub api_key: Option<SecretBytes>,
    pub token_issuer: Option<String>,
    pub token_ttl_seconds: Option<NonZeroU64>,
}
