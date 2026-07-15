#![deny(unsafe_code)]
#![cfg_attr(
    test,
    allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::indexing_slicing,
        clippy::shadow_unrelated,
        clippy::let_underscore_must_use,
        clippy::format_push_string,
        clippy::vec_init_then_push,
        clippy::useless_format,
        clippy::same_item_push,
        clippy::useless_vec,
        clippy::str_to_string
    )
)]

//! HuggingFace Hub API compatibility layer for Shardline.
//!
//! This crate provides an Axum-based HTTP API that makes Shardline a drop-in
//! HuggingFace Hub alternative. Users can point `huggingface-cli` at a Shardline
//! server and upload/download models as if it were the real Hub.
//!
//! # Example
//!
//! ```no_run
//! use axum::Router;
//! use shardline_hub_api::hub_routes;
//! use shardline_hub_api::routes::HubState;
//!
//! # fn example(state: HubState) {
//! let app: Router = hub_routes(state);
//! # }
//! ```

pub mod auth;
pub mod commit;
pub mod error;
pub mod git;
pub mod models;
pub mod resolve;
pub mod routes;
pub mod state;

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
/// into any Axum router. Call this with the [`HubState`](routes::HubState) that
/// should back all handlers.
pub fn hub_routes(state: routes::HubState, register_xet_token_routes: bool) -> Router {
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

    routes::router(register_xet_token_routes)
        .with_state(state)
        .route_layer(DefaultBodyLimit::max(64 * 1024 * 1024)) // 64 MB
        .layer(cors)
        .layer(security_headers)
}
