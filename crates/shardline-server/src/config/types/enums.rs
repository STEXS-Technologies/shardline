use std::{
    num::{NonZeroU64, NonZeroUsize},
    path::PathBuf,
};

use serde::{Deserialize, Serialize};
use shardline_cache::RedisTlsConfig;
use shardline_protocol::{SecretBytes, SecretString};

use super::error::ServerConfigError;
use crate::reconstruction_cache::ReconstructionCacheAdapter;

/// Compares two strings ignoring ASCII case and surrounding whitespace.
///
/// Used to make config token parsing uniform across auth providers, storage
/// adapters, and deployment modes.
pub(crate) fn caseless_eq(left: &str, right: &str) -> bool {
    left.trim().eq_ignore_ascii_case(right.trim())
}

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
        if caseless_eq(value, "local") {
            Ok(Self::Local)
        } else if caseless_eq(value, "oidc") {
            Ok(Self::Oidc)
        } else if caseless_eq(value, "jwks") {
            Ok(Self::Jwks)
        } else if caseless_eq(value, "passthrough") {
            Ok(Self::Passthrough)
        } else if caseless_eq(value, "ed25519") {
            Ok(Self::Ed25519)
        } else {
            Err(ServerConfigError::InvalidAuthProvider)
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
        if caseless_eq(value, "local") {
            Ok(Self::Local)
        } else if caseless_eq(value, "s3") {
            Ok(Self::S3)
        } else {
            Err(ServerConfigError::InvalidObjectStorageAdapter)
        }
    }
}

/// Server deployment security mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
pub enum DeploymentMode {
    /// Allow all requests, no auth required. For local development only.
    Insecure,
    /// Require authentication for all data-plane routes.
    /// Passthrough provider is allowed for development behind a trusted proxy.
    Authenticated,
    /// Strictest production mode. Passthrough provider is rejected.
    /// Signing key, metrics token, and explicit auth provider are all required.
    Strict,
}

impl DeploymentMode {
    /// Returns the default deployment mode.
    #[must_use]
    pub const fn default() -> Self {
        Self::Insecure
    }

    /// Parses a deployment mode token, ignoring ASCII case and surrounding whitespace.
    ///
    /// Returns `None` when the token is not a supported deployment mode.
    #[must_use]
    pub fn parse(value: &str) -> Option<Self> {
        if caseless_eq(value, "insecure") {
            Some(Self::Insecure)
        } else if caseless_eq(value, "authenticated") {
            Some(Self::Authenticated)
        } else if caseless_eq(value, "strict") {
            Some(Self::Strict)
        } else {
            None
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
