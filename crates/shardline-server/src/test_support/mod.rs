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
