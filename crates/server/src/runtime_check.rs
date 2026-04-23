use crate::{
    ServerConfig, ServerError, backend::ServerBackend,
    reconstruction_cache::ReconstructionCacheService,
};

/// Result of validating a Shardline runtime configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigCheckReport {
    /// Whether validation succeeded.
    pub status: String,
    /// Selected runtime role.
    pub server_role: String,
    /// Enabled runtime protocol frontends.
    pub server_frontends: Vec<String>,
    /// Selected metadata backend.
    pub metadata_backend: String,
    /// Selected immutable object-storage backend.
    pub object_backend: String,
    /// Selected reconstruction-cache backend.
    pub cache_backend: String,
    /// Whether signed CAS bearer tokens are enabled.
    pub auth_enabled: bool,
    /// Whether provider token issuance is enabled.
    pub provider_tokens_enabled: bool,
}

/// Validates the selected runtime configuration and backend reachability.
///
/// # Errors
///
/// Returns [`ServerError`] when the configured backend cannot initialize or fails its
/// readiness checks.
pub async fn run_config_check(config: ServerConfig) -> Result<ConfigCheckReport, ServerError> {
    config.validate_runtime_requirements()?;
    let auth_enabled = config.token_signing_key().is_some();
    let provider_tokens_enabled = config.server_role().serves_api()
        && config.provider_config_path().is_some()
        && config.provider_api_key().is_some();
    let backend = ServerBackend::from_config(&config).await?;
    let reconstruction_cache = if config.server_role().uses_reconstruction_cache() {
        ReconstructionCacheService::from_config(&config)?
    } else {
        ReconstructionCacheService::disabled()
    };
    backend.ready().await?;
    reconstruction_cache.ready().await?;

    Ok(ConfigCheckReport {
        status: "ok".to_owned(),
        server_role: config.server_role().as_str().to_owned(),
        server_frontends: config
            .server_frontends()
            .iter()
            .map(|frontend| frontend.as_str().to_owned())
            .collect(),
        metadata_backend: backend.backend_name().to_owned(),
        object_backend: backend.object_backend_name().to_owned(),
        cache_backend: reconstruction_cache.backend_name().to_owned(),
        auth_enabled,
        provider_tokens_enabled,
    })
}

#[cfg(test)]
mod tests {
    use std::{
        net::{IpAddr, Ipv4Addr, SocketAddr},
        num::NonZeroUsize,
    };

    use super::run_config_check;
    use crate::{ServerConfig, ServerConfigError, ServerError, ServerRole};

    #[tokio::test]
    async fn config_check_reports_local_backend_for_initialized_storage() {
        let storage = tempfile::tempdir();
        assert!(storage.is_ok());
        let Ok(storage) = storage else {
            return;
        };
        let config = ServerConfig::new(
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8080),
            "http://127.0.0.1:8080".to_owned(),
            storage.path().to_path_buf(),
            NonZeroUsize::MIN,
        )
        .with_token_signing_key(b"signing-key".to_vec());
        assert!(config.is_ok());
        let Ok(config) = config else {
            return;
        };

        let report = run_config_check(config).await;

        assert!(report.is_ok());
        let Ok(report) = report else {
            return;
        };
        assert_eq!(report.status, "ok");
        assert_eq!(report.server_role, "all");
        assert_eq!(report.server_frontends, vec!["xet".to_owned()]);
        assert_eq!(report.metadata_backend, "local");
        assert_eq!(report.object_backend, "local");
        assert_eq!(report.cache_backend, "memory");
        assert!(report.auth_enabled);
        assert!(!report.provider_tokens_enabled);
    }

    #[tokio::test]
    async fn transfer_role_config_check_disables_reconstruction_cache() {
        let storage = tempfile::tempdir();
        assert!(storage.is_ok());
        let Ok(storage) = storage else {
            return;
        };
        let config = ServerConfig::new(
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8080),
            "http://127.0.0.1:8080".to_owned(),
            storage.path().to_path_buf(),
            NonZeroUsize::MIN,
        )
        .with_server_role(ServerRole::Transfer)
        .with_token_signing_key(b"signing-key".to_vec());
        assert!(config.is_ok());
        let Ok(config) = config else {
            return;
        };

        let report = run_config_check(config).await;

        assert!(report.is_ok());
        let Ok(report) = report else {
            return;
        };
        assert_eq!(report.server_role, "transfer");
        assert_eq!(report.server_frontends, vec!["xet".to_owned()]);
        assert_eq!(report.cache_backend, "disabled");
        assert!(report.auth_enabled);
        assert!(!report.provider_tokens_enabled);
    }

    #[tokio::test]
    async fn config_check_rejects_missing_signing_key_for_served_routes() {
        let storage = tempfile::tempdir();
        assert!(storage.is_ok());
        let Ok(storage) = storage else {
            return;
        };
        let config = ServerConfig::new(
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8080),
            "http://127.0.0.1:8080".to_owned(),
            storage.path().to_path_buf(),
            NonZeroUsize::MIN,
        );

        let report = run_config_check(config).await;

        assert!(matches!(
            report,
            Err(ServerError::Config(
                ServerConfigError::MissingTokenSigningKeyForServedRoutes
            ))
        ));
    }
}
