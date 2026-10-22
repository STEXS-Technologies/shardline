use axum::Json;

/// Health check endpoint — returns `{"status": "ok"}`.
pub(crate) async fn health() -> Json<serde_json::Value> {
    Json(serde_json::json!({ "status": "ok" }))
}

/// Empty Hugging Face agent-harness registry. Newer Hub clients query this
/// optional endpoint for telemetry labels; Shardline does not register
/// third-party agent harnesses.
pub(crate) async fn agent_harnesses() -> Json<serde_json::Value> {
    Json(serde_json::json!({
        "standardEnvVars": [],
        "harnesses": {}
    }))
}
