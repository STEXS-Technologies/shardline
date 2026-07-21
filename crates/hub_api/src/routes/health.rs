use axum::Json;

/// Health check endpoint — returns `{"status": "ok"}`.
pub(crate) async fn health() -> Json<serde_json::Value> {
    Json(serde_json::json!({ "status": "ok" }))
}
