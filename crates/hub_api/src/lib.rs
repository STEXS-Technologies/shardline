#![deny(unsafe_code)]

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

/// Builds the Hub API router with all registered routes.
///
/// The returned [`Router`] is state-generic and can be merged into any
/// Axum router. Call [`state::init`] with a [`routes::HubState`] before
/// serving requests.
#[must_use]
pub fn hub_routes<S: Clone + Send + Sync + 'static>() -> Router<S> {
    routes::router()
}

/// Initializes the Hub API with the given state.
///
/// This must be called once before the server starts accepting requests.
pub fn init(state: routes::HubState) {
    state::init(state);
}
