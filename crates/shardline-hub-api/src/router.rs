use crate::routes;
use axum::Router;
use axum::extract::DefaultBodyLimit;
use axum::http::{Method, header::HeaderValue};
use tower_http::cors::{Any, CorsLayer};
use tower_http::set_header::SetResponseHeaderLayer;

/// Builds the Hub API router with all registered routes.
///
/// `register_xet_token_routes` controls whether the `xet-read-token` and
/// `xet-write-token` routes are registered. Set to `false` when the Xet
/// protocol frontend is already serving these routes.
///
/// The returned [`Router`] is stateless (type [`Router<()>`]) and can be merged
/// into any Axum router. Call this with the [`HubState`](super::routes::HubState) that
/// should back all handlers.
pub fn hub_routes(state: routes::HubState, register_xet_token_routes: bool) -> Router {
    hub_routes_with_dataset_query(
        state,
        register_xet_token_routes,
        routes::router::dataset_query_enabled_from_environment(),
    )
}

/// Builds Hub routes with an explicit native dataset-query feature gate.
/// Passing `false` keeps all existing Hub and legacy dataset-preview routes but
/// omits the structured Parquet query endpoint.
pub fn hub_routes_with_dataset_query(
    state: routes::HubState,
    register_xet_token_routes: bool,
    dataset_query_enabled: bool,
) -> Router {
    let cors = CorsLayer::new()
        .allow_origin([
            HeaderValue::from_static("http://127.0.0.1:8080"),
            HeaderValue::from_static("http://localhost:8080"),
        ])
        .allow_methods([
            Method::GET,
            Method::POST,
            Method::PUT,
            Method::DELETE,
            Method::HEAD,
        ])
        .allow_headers(Any);

    let security_headers = SetResponseHeaderLayer::overriding(
        axum::http::header::X_CONTENT_TYPE_OPTIONS,
        axum::http::HeaderValue::from_static("nosniff"),
    );

    routes::router_with_dataset_query(register_xet_token_routes, dataset_query_enabled)
        .with_state(state)
        .route_layer(DefaultBodyLimit::max(64 * 1024 * 1024)) // 64 MB
        .layer(cors)
        .layer(security_headers)
}
