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

//! Index rebuild logic for the Shardline server ecosystem.
//!
//! This crate provides pure rebuild functions that operate on explicit
//! store parameters rather than server configuration.

mod candidates;
mod error;
mod report;
mod runner;
mod types;

// Crate-internal re-exports needed by sibling production modules.
pub(crate) use candidates::{VersionCandidate, collect_candidate};
pub(crate) use report::push_issue;

// Re-exports needed only by integration tests (these are pub(super) in their
// original modules and are brought into crate-root scope for `use super::*`).
#[cfg(test)]
pub(crate) use runner::{
    desired_reconstruction_file_ids, prune_stale_reconstructions, rebuild_dedupe_shard_mappings,
};

// Public API re-exports.
pub use error::RebuildError;
pub use report::IndexRebuildReport;
pub use runner::run_index_rebuild_with_stores;
pub use types::{
    IndexRebuildIssue, IndexRebuildIssueDetail, IndexRebuildIssueKind,
    IndexRebuildReconstructionPlanDetail, LocalIndexRebuildIssue, LocalIndexRebuildIssueKind,
    LocalIndexRebuildReport,
};

#[cfg(test)]
mod tests;
