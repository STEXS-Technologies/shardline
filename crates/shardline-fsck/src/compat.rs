use crate::{
    report::FsckReport,
    types::{FsckIssue, FsckIssueKind},
};

/// Backward-compatible local fsck report alias.
pub type LocalFsckReport = FsckReport;

/// Backward-compatible local fsck issue alias.
pub type LocalFsckIssue = FsckIssue;

/// Backward-compatible local fsck issue-kind alias.
pub type LocalFsckIssueKind = FsckIssueKind;

pub const WEBHOOK_DELIVERY_FUTURE_SKEW_SECONDS: u64 = 300;
