#![deny(unsafe_code)]

//! Version-control provider boundaries for Shardline.
//!
//! Shardline can issue repository-scoped CAS tokens from several Git hosting
//! providers. This crate keeps the provider-facing boundary isolated from the
//! HTTP server:
//!
//! - [`ProviderAdapter`] normalizes provider metadata and authorization checks.
//! - [`RepositoryRef`] and [`RevisionRef`] validate repository identity before it
//!   is persisted or embedded into tokens.
//! - [`ProviderTokenIssuer`] converts an authorization decision into a signed
//!   Shardline token.
//! - [`BuiltInProviderCatalog`] wires the GitHub, Gitea, GitLab, and generic
//!   adapters from deployment configuration.
//!
//! # Example
//!
//! ```
//! use shardline_vcs::{ProviderKind, RepositoryAccess, RepositoryRef, RevisionRef};
//!
//! let repository = RepositoryRef::new(ProviderKind::GitHub, "acme", "assets")?;
//! let revision = RevisionRef::new("refs/heads/main")?;
//!
//! assert_eq!(repository.owner(), "acme");
//! assert_eq!(revision.as_str(), "refs/heads/main");
//! assert_eq!(RepositoryAccess::Read, RepositoryAccess::Read);
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```

mod adapter;
mod authorization;
mod builtin;
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
pub use generic::GenericAdapter;
pub use gitea::GiteaAdapter;
pub use github::GitHubAdapter;
pub use gitlab::{GitLabAdapter, metadata_from_project_payload};
pub use provider::ProviderKind;
pub use reference::{RepositoryAccess, RepositoryRef, RevisionRef, VcsReferenceError};
pub use token_issuer::{
    GrantedRepositoryAccess, ProviderIssuedToken, ProviderTokenIssuanceError, ProviderTokenIssuer,
};
