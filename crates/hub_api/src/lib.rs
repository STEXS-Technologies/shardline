#![deny(unsafe_code)]
#![cfg_attr(
    test,
    allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::indexing_slicing,
        clippy::arithmetic_side_effects,
        clippy::shadow_unrelated,
        clippy::let_underscore_must_use,
        clippy::format_push_string
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
//!
//! let app: Router = hub_routes();
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
/// The returned [`Router`] is state-generic and can be merged into any
/// Axum router. Call [`state::init`] with a [`routes::HubState`] before
/// serving requests.
pub fn hub_routes<S: Clone + Send + Sync + 'static>() -> Router<S> {
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

    routes::router()
        .route_layer(DefaultBodyLimit::max(64 * 1024 * 1024)) // 64 MB
        .layer(cors)
        .layer(security_headers)
}

/// Initializes the Hub API with the given state.
///
/// This must be called once before the server starts accepting requests.
pub fn init(state: routes::HubState) {
    state::init(state);
}
