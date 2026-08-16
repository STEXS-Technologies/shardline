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
//!
//! # Quick start
//!
//! The issue and report types are pure data that describe what a rebuild
//! found and fixed:
//!
//! ```
//! use shardline_rebuild::{
//!     IndexRebuildIssue, IndexRebuildIssueDetail, IndexRebuildIssueKind,
//!     IndexRebuildReport,
//! };
//!
//! let issue = IndexRebuildIssue {
//!     kind: IndexRebuildIssueKind::InvalidVersionRecordJson,
//!     location: "records/latest/acme-assets.json".to_owned(),
//!     detail: IndexRebuildIssueDetail::RecordJsonInvalid,
//! };
//! assert_eq!(issue.kind.as_str(), "invalid_version_record_json");
//!
//! let report = IndexRebuildReport {
//!     scanned_version_records: 120,
//!     scanned_retained_shards: 4,
//!     rebuilt_latest_records: 1,
//!     unchanged_latest_records: 119,
//!     removed_stale_latest_records: 0,
//!     scanned_reconstructions: 4,
//!     unchanged_reconstructions: 4,
//!     removed_stale_reconstructions: 0,
//!     rebuilt_dedupe_shard_mappings: 0,
//!     unchanged_dedupe_shard_mappings: 0,
//!     removed_stale_dedupe_shard_mappings: 0,
//!     preserved_latest_records_unreadable_version: Vec::new(),
//!     issues: vec![issue],
//! };
//! assert!(!report.is_clean());
//! assert_eq!(report.issue_count(), 1);
//! ```
//!
//! To run a real rebuild, use [`run_index_rebuild_with_stores`] with explicit
//! record, index, and object-store adapters.

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
