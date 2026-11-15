pub(crate) mod native_upload_rollback;
pub(crate) mod postgres_fixture;
pub(crate) mod released_startup;

#[allow(clippy::expect_used)]
pub(crate) async fn json_body(response: axum::response::Response) -> serde_json::Value {
    let bytes = axum::body::to_bytes(response.into_body(), 1_048_576)
        .await
        .expect("response body");
    serde_json::from_slice(&bytes).expect("JSON response")
}

#[allow(clippy::expect_used)]
pub(crate) fn protocol_state(
    config: crate::ServerConfig,
    backend: crate::ServerBackend,
    auth: Option<crate::auth::ServerAuth>,
    transfer_limiter: crate::TransferLimiter,
) -> std::sync::Arc<crate::app::AppState> {
    std::sync::Arc::new(crate::app::AppState {
        config,
        role: crate::ServerRole::All,
        backend,
        auth,
        provider_tokens: None,
        reconstruction_cache: crate::ReconstructionCacheService::disabled(),
        transfer_limiter,
        oci_registry_token_limiter: std::sync::Arc::new(tokio::sync::Semaphore::new(64)),
        admission: crate::admission::WeightedAdmission::new(
            std::num::NonZeroUsize::new(256).expect("nonzero admission size"),
        ),
        pools: crate::admission::ExecutionPools::default_sizes(),
        protocol_metrics: crate::ProtocolMetrics::default(),
    })
}
