use super::RebuildError;
use super::types::{IndexRebuildIssue, IndexRebuildIssueDetail, IndexRebuildIssueKind};

/// Index-rebuild report.
///
/// Aggregates the outcome of one rebuild run: how many version records and
/// retained shards were scanned, how many latest records were recreated or
/// left unchanged, and which non-fatal issues were collected.
///
/// # Examples
///
/// ```
/// use shardline_rebuild::IndexRebuildReport;
///
///     let report = IndexRebuildReport {
///         scanned_version_records: 100,
///         scanned_retained_shards: 5,
///         rebuilt_latest_records: 0,
///         unchanged_latest_records: 100,
///         removed_stale_latest_records: 0,
///         scanned_reconstructions: 5,
///         unchanged_reconstructions: 5,
///         removed_stale_reconstructions: 0,
///         rebuilt_dedupe_shard_mappings: 0,
///         unchanged_dedupe_shard_mappings: 0,
///         removed_stale_dedupe_shard_mappings: 0,
///         preserved_latest_records_unreadable_version: Vec::new(),
///         issues: Vec::new(),
///     };
///     assert!(report.is_clean());
///     assert!(report.preserved_latest_records_unreadable_version.is_empty());
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexRebuildReport {
    /// Number of version records scanned through the configured record store.
    pub scanned_version_records: u64,
    /// Number of retained shard objects scanned through the object-store adapter.
    pub scanned_retained_shards: u64,
    /// Number of latest records recreated or updated through the configured record store.
    pub rebuilt_latest_records: u64,
    /// Number of latest records that already matched the rebuilt head.
    pub unchanged_latest_records: u64,
    /// Number of stale latest records removed because no version record remained.
    pub removed_stale_latest_records: u64,
    /// Number of reconstruction rows inspected through the index adapter.
    pub scanned_reconstructions: u64,
    /// Number of reconstruction rows still backed by immutable version records.
    pub unchanged_reconstructions: u64,
    /// Number of stale reconstruction rows removed because no version record remained.
    pub removed_stale_reconstructions: u64,
    /// Number of dedupe-shard mappings inserted or updated.
    pub rebuilt_dedupe_shard_mappings: u64,
    /// Number of dedupe-shard mappings that already matched the rebuilt view.
    pub unchanged_dedupe_shard_mappings: u64,
    /// Number of stale dedupe-shard mappings removed because no retained shard contained them.
    pub removed_stale_dedupe_shard_mappings: u64,
    /// Locator displays of version records that could not be parsed. Their
    /// existing latest records were preserved ("kept because version
    /// unreadable"): the stale-latest-record deletion phase is skipped when the
    /// run is not clean, so an unreadable version row can never cause a fully
    /// intact latest record — and the file it indexes — to be deleted.
    pub preserved_latest_records_unreadable_version: Vec<String>,
    /// Collected non-fatal rebuild issues.
    pub issues: Vec<IndexRebuildIssue>,
}

impl IndexRebuildReport {
    /// Returns the total issue count.
    #[must_use]
    pub const fn issue_count(&self) -> usize {
        self.issues.len()
    }

    /// Returns whether the rebuild completed without non-fatal issues.
    #[must_use]
    pub const fn is_clean(&self) -> bool {
        self.issues.is_empty()
    }
}

pub(crate) fn push_issue(
    report: &mut IndexRebuildReport,
    kind: IndexRebuildIssueKind,
    location: String,
    detail: IndexRebuildIssueDetail,
) -> Result<(), RebuildError> {
    let _count = u64::try_from(report.issues.len())?;
    report.issues.push(IndexRebuildIssue {
        kind,
        location,
        detail,
    });
    Ok(())
}
