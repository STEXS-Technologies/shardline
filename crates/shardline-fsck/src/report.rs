use crate::FsckIssue;

/// Integrity-check report.
///
/// Aggregates the outcome of one fsck run: how many records, chunk references,
/// shard mappings, and webhook deliveries were inspected, plus the issues that
/// were found.
///
/// # Examples
///
/// ```
/// use shardline_fsck::FsckReport;
///
/// let report = FsckReport {
///     latest_records: 100,
///     version_records: 100,
///     inspected_chunk_references: 300,
///     inspected_dedupe_shard_mappings: 5,
///     inspected_reconstructions: 5,
///     inspected_webhook_deliveries: 0,
///     inspected_provider_repository_states: 0,
///     issues: Vec::new(),
/// };
/// assert!(report.is_clean());
/// assert_eq!(report.issue_count(), 0);
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FsckReport {
    /// Number of latest records scanned through the configured record store.
    pub latest_records: u64,
    /// Number of immutable version records scanned through the configured record store.
    pub version_records: u64,
    /// Number of chunk references inspected across all records.
    pub inspected_chunk_references: u64,
    /// Number of dedupe-shard mappings inspected through the index adapter.
    pub inspected_dedupe_shard_mappings: u64,
    /// Number of durable reconstruction rows inspected through the index adapter.
    pub inspected_reconstructions: u64,
    /// Number of processed provider webhook deliveries inspected through the index adapter.
    pub inspected_webhook_deliveries: u64,
    /// Number of provider repository lifecycle states inspected through the index adapter.
    pub inspected_provider_repository_states: u64,
    /// Collected integrity issues.
    pub issues: Vec<FsckIssue>,
}

impl FsckReport {
    /// Returns the total issue count.
    #[must_use]
    pub const fn issue_count(&self) -> usize {
        self.issues.len()
    }

    /// Returns whether the storage root passed every check.
    #[must_use]
    pub const fn is_clean(&self) -> bool {
        self.issues.is_empty()
    }
}
