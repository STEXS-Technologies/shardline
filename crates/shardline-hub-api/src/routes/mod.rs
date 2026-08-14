// ── Submodules ──────────────────────────────────────────────────────────────

pub mod commits;
pub mod dataset;
pub mod handlers;
pub mod health;
pub mod helpers;
pub mod lfs;
pub mod repos;
pub mod resolve;
pub mod router;
pub mod state;
pub mod tokens;
pub mod tree;
pub mod webhooks;

// Re-export all handler functions, shared types, and helpers so that sibling
// submodules and `crate::routes::router::router()` can reference them by name.
pub(crate) use self::commits::*;
pub(crate) use self::dataset::*;
pub(crate) use self::handlers::{git_head, whoami};
pub(crate) use self::health::*;
#[cfg(test)]
pub(crate) use self::helpers::test_repo;
pub(crate) use self::helpers::{
    HubRepository, authorize, repo_type_path, require_repository_binding,
};
// Re-export the canonical LFS object-key builder from the shared protocol
// adapters so sibling modules and `crate::routes::lfs_object_key` references
// resolve to a single implementation (no duplicated key derivation).
pub(crate) use self::lfs::*;
pub(crate) use self::repos::*;
pub(crate) use self::resolve::*;
pub use self::router::router;
pub use self::state::HubState;
pub(crate) use self::tokens::*;
pub(crate) use self::tree::*;
pub(crate) use self::webhooks::*;
pub(crate) use shardline_protocol_adapters::lfs_object_key;

#[cfg(test)]
pub(crate) use crate::auth::HubAuth;
#[cfg(test)]
pub(crate) use crate::models::*;
#[cfg(test)]
pub(crate) use shardline_protocol::TokenScope;

// ── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests;
