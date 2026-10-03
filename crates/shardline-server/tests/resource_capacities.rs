#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::arithmetic_side_effects
)]
use shardline_server::{DeploymentMode, ServerConfig, ServerConfigError, ServerError, app};
use std::num::NonZeroUsize;

#[tokio::test]
async fn oversized_resource_config_returns_error_before_backend_initialization() {
    let root = tempfile::tempdir().unwrap();
    let counted_maximum = (u32::MAX as usize).min(tokio::sync::Semaphore::MAX_PERMITS);
    for (name, maximum) in [
        ("admission_max_weight", counted_maximum),
        ("transfer_max_in_flight_chunks", counted_maximum),
        (
            "oci_registry_token_max_in_flight_requests",
            tokio::sync::Semaphore::MAX_PERMITS,
        ),
    ] {
        let backend_root = root.path().join(name);
        let config = ServerConfig::new(
            "127.0.0.1:0".parse().unwrap(),
            "http://localhost".to_owned(),
            backend_root.clone(),
            NonZeroUsize::new(65536).unwrap(),
        )
        .with_deployment_mode(DeploymentMode::Insecure);
        let capacity = NonZeroUsize::new(maximum + 1).unwrap();
        let config = match name {
            "admission_max_weight" => config.with_admission_max_weight(capacity),
            "transfer_max_in_flight_chunks" => config.with_transfer_max_in_flight_chunks(capacity),
            _ => config.with_oci_registry_token_max_in_flight_requests(capacity),
        };
        assert!(!backend_root.exists());
        let error = app::router(config).await.unwrap_err();
        assert!(
            matches!(error, ServerError::Config(ServerConfigError::ResourceCapacityOutOfRange { name: rejected, capacity: actual, maximum: limit }) if rejected == name && actual == capacity.get() && limit == maximum)
        );
        assert!(
            !backend_root.exists(),
            "invalid configuration must not initialize storage"
        );
    }
}

#[test]
fn representable_resource_config_boundaries_validate() {
    let maximum = (u32::MAX as usize).min(tokio::sync::Semaphore::MAX_PERMITS);
    let config = ServerConfig::new(
        "127.0.0.1:0".parse().unwrap(),
        "http://localhost".to_owned(),
        "unused-capacity-test".into(),
        NonZeroUsize::new(65536).unwrap(),
    )
    .with_deployment_mode(DeploymentMode::Insecure)
    .with_admission_max_weight(NonZeroUsize::new(maximum).unwrap())
    .with_transfer_max_in_flight_chunks(NonZeroUsize::new(maximum).unwrap())
    .with_oci_registry_token_max_in_flight_requests(
        NonZeroUsize::new(tokio::sync::Semaphore::MAX_PERMITS).unwrap(),
    );
    config.validate_runtime_requirements().unwrap();
}
