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

//! Storage integrity checking logic for the Shardline server ecosystem.
//!
//! This crate provides pure fsck functions that operate on explicit
//! store parameters rather than server configuration.

mod error;
mod report;
mod runner;
#[cfg(test)]
mod tests;
mod types;

// Internal submodules kept for their original structure.
mod lifecycle_checks;
mod record_checks;

// ── Public API re-exports ────────────────────────────────────────────

pub use error::FsckError;
pub use report::FsckReport;
pub use runner::{run_fsck_with_stores, run_local_fsck};
pub use types::{
    FsckIssue, FsckIssueDetail, FsckIssueKind, FsckReconstructionPlanDetail,
    ProviderRepositoryStateTimestampField,
};

// ── Backward-compatible local aliases ─────────────────────────────────

/// Backward-compatible local fsck report alias.
pub type LocalFsckReport = FsckReport;

/// Backward-compatible local fsck issue alias.
pub type LocalFsckIssue = FsckIssue;

/// Backward-compatible local fsck issue-kind alias.
pub type LocalFsckIssueKind = FsckIssueKind;

// ── Constants ────────────────────────────────────────────────────────

pub const WEBHOOK_DELIVERY_FUTURE_SKEW_SECONDS: u64 = 300;

// ── Crate-internal re-exports (for sibling modules & tests) ──────────

pub(crate) use runner::{
    object_location_display, push_issue, push_reconstruction_plan_issue, record_path,
    unix_now_seconds_checked,
};

#[cfg(test)]
pub(crate) use runner::{object_key_storage_path, reconstruction_plan_error_detail};
pub(crate) use types::{
    FsckObjectContext, FsckReachability, PendingVersionRecordCheck, RecordKind,
};
