#![deny(unsafe_code)]
#![cfg_attr(
    test,
    allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::indexing_slicing,
        clippy::shadow_unrelated,
        clippy::let_underscore_must_use,
        clippy::format_push_string
    )
)]

//! Version-control provider boundaries for Shardline.
//!
//! Shardline can issue repository-scoped CAS tokens from several Git hosting
//! providers (GitHub, Gitea, GitLab, Codeberg, and a generic boundary). This
//! crate keeps the provider-facing layer isolated from the HTTP server:
//!
//! - [`ProviderAdapter`] normalizes provider metadata and authorization checks.
//! - [`RepositoryRef`] and [`RevisionRef`] validate repository identity before it
//!   is persisted or embedded into tokens.
//! - [`ProviderTokenIssuer`] converts an authorization decision into a signed
//!   Shardline token.
//! - [`BuiltInProviderCatalog`] wires the GitHub, Gitea, GitLab, and generic
//!   adapters from deployment configuration.
//!
//! # Quick start
//!
//! The core value types are pure and can be validated without any network
//! access, which makes them the ideal starting point:
//!
//! ```
//! use shardline_vcs::{
//!     ProviderKind, RepositoryAccess, RepositoryRef, RepositoryVisibility, RevisionRef,
//! };
//! use std::str::FromStr;
//!
//! // Validate repository and revision identity before embedding them in tokens.
//! let repository = RepositoryRef::new(ProviderKind::GitHub, "acme", "assets")?;
//! let revision = RevisionRef::new("refs/heads/main")?;
//!
//! assert_eq!(repository.owner(), "acme");
//! assert_eq!(repository.name(), "assets");
//! assert_eq!(revision.as_str(), "refs/heads/main");
//!
//! // Provider-reported visibility parses case-insensitively.
//! let visibility = RepositoryVisibility::from_str(" Private ")?;
//! assert_eq!(visibility, RepositoryVisibility::Private);
//!
//! // Access levels are distinct and copyable.
//! assert_ne!(RepositoryAccess::Read, RepositoryAccess::Write);
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```
//!
//! From there, plug the validated references into a [`ProviderAdapter`]
//! implementation (for example [`GitHubAdapter`] or [`GenericAdapter`]) to
//! resolve repository metadata, or into a [`ProviderTokenIssuer`] to mint
//! repository-scoped CAS tokens.

mod adapter;
mod authorization;
mod builtin;
mod codeberg;
mod generic;
mod gitea;
mod github;
mod gitlab;
mod provider;
mod reference;
mod token_issuer;

pub use adapter::{
    AuthorizationDecision, CanonicalCloneUrl, ProviderAdapter, ProviderBoundaryError,
    ProviderSubject, RepositoryMetadata, RepositoryVisibility, RepositoryWebhookEvent,
    RepositoryWebhookEventKind, WebhookDeliveryId, WebhookRequest,
};
pub use authorization::AuthorizationRequest;
pub use builtin::{
    BuiltInProviderCatalog, BuiltInProviderError, ProviderRepositoryPolicy, configured_metadata,
};
pub use codeberg::CodebergAdapter;
pub use generic::GenericAdapter;
pub use gitea::GiteaAdapter;
pub use github::GitHubAdapter;
pub use gitlab::{GitLabAdapter, metadata_from_project_payload};
pub use provider::ProviderKind;
pub use reference::{RepositoryAccess, RepositoryRef, RevisionRef, VcsReferenceError};
pub use token_issuer::{
    GrantedRepositoryAccess, ProviderIssuedToken, ProviderTokenIssuanceError, ProviderTokenIssuer,
};
