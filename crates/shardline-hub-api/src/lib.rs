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
//! # Quick start
//!
//! The commit-parsing helpers are pure and are the easiest place to start:
//! they turn the Hub's NDJSON commit stream into typed instructions that the
//! rest of the API applies to storage.
//!
//! ```
//! use shardline_hub_api::commit::parse_ndjson_commit;
//! use shardline_hub_api::commit::CommitInstruction;
//! use base64::Engine;
//!
//! let content = base64::engine::general_purpose::STANDARD.encode(b"hello world");
//! let body = format!(
//!     "{{\"header\":{{\"summary\":\"add readme\",\"parentCommit\":\"\"}}}}\n\
//!      {{\"file\":{{\"path\":\"README.md\",\"content\":\"{content}\"}}}}"
//! );
//!
//! let commit = parse_ndjson_commit(&body)?;
//! assert_eq!(commit.message, "add readme");
//! assert!(matches!(
//!     &commit.instructions[0],
//!     CommitInstruction::InlineFile { path, content } if path == "README.md" && content == b"hello world"
//! ));
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```
//!
//! To serve the API itself, build a [`HubState`](routes::HubState) backed by a
//! Shardline index store and object store, then mount [`hub_routes`] into your
//! Axum application:
//!
//! ```no_run
//! use axum::Router;
//! use shardline_hub_api::hub_routes;
//! use shardline_hub_api::routes::HubState;
//!
//! # fn example(state: HubState) {
//! let app: Router = hub_routes(state, true);
//! # }
//! ```

pub mod auth;
pub mod commit;
pub mod error;
pub mod git;
pub mod models;
mod parquet_preview;
pub mod query;
pub mod resolve;
mod router;
pub mod routes;
pub mod secrets;
pub mod state;
pub mod types;

pub use router::{hub_routes, hub_routes_with_dataset_query};
