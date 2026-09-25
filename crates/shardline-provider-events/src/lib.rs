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
        clippy::items_after_test_module
    )
)]

//! Provider webhook event processing for the Shardline server.
//!
//! This crate handles incoming provider webhook events (repository deleted,
//! renamed, access changed, revision pushed) and applies the corresponding
//! metadata mutations.
//!
//! # Quick start
//!
//! The outcome types are pure data describing what one webhook application
//! did, so they are the easiest entry point:
//!
//! ```
//! use shardline_provider_events::{ProviderWebhookOutcome, ProviderWebhookOutcomeKind};
//! use shardline_vcs::ProviderKind;
//!
//! let outcome = ProviderWebhookOutcome {
//!     provider: ProviderKind::GitHub,
//!     owner: "acme".to_owned(),
//!     repo: "assets".to_owned(),
//!     delivery_id: "delivery-123".to_owned(),
//!     event_kind: ProviderWebhookOutcomeKind::RepositoryDeleted,
//!     affected_file_versions: 12,
//!     affected_chunks: 34,
//!     applied_holds: 1,
//!     retention_seconds: Some(3600),
//! };
//!
//! assert_eq!(outcome.owner, "acme");
//! assert!(matches!(outcome.event_kind, ProviderWebhookOutcomeKind::RepositoryDeleted));
//! ```
//!
//! A rename carries the new location:
//!
//! ```
//! use shardline_provider_events::ProviderWebhookOutcomeKind;
//!
//! let kind = ProviderWebhookOutcomeKind::RepositoryRenamed {
//!     new_owner: "acme".to_owned(),
//!     new_repo: "assets-v2".to_owned(),
//! };
//! assert!(matches!(
//!     kind,
//!     ProviderWebhookOutcomeKind::RepositoryRenamed { new_repo, .. } if new_repo == "assets-v2"
//! ));
//! ```
//!
//! To apply a webhook to real state, call [`apply_provider_webhook_with_stores`]
//! with explicit record, index, and object-store adapters.

mod outcome;
mod records;
mod repository;
mod state;
#[cfg(test)]
mod tests;

mod provider_events;
#[cfg(test)]
pub(crate) use provider_events::duplicate_webhook_outcome;
pub use provider_events::*;
