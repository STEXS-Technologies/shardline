//! GC types and constants.

use serde::{Deserialize, Serialize};

/// Default retention window for new local quarantine candidates.
pub use shardline_server_core::DEFAULT_LOCAL_GC_RETENTION_SECONDS;

/// Minimum retention window in seconds for GC quarantine entries.
///
/// Prevents data loss from the TOCTOU race between the GC mark phase and
/// concurrent uploads that have written chunks on disk but not yet committed
/// their file records.  A non-zero retention gives concurrent uploads time
/// to finish before orphaned chunks are physically deleted.
///
/// See [`run_gc_with_stores`] for the clamping logic.
pub const MINIMUM_GC_RETENTION_SECONDS: u64 = 3600; // 1 hour

/// Local filesystem garbage-collection execution options.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LocalGcOptions {
    /// Whether to persist newly discovered orphan chunks into quarantine state.
    pub mark: bool,
    /// Whether to delete expired quarantine candidates.
    pub sweep: bool,
    /// Retention window applied to newly created quarantine candidates.
    pub retention_seconds: u64,
}

impl Default for LocalGcOptions {
    fn default() -> Self {
        Self {
            mark: false,
            sweep: false,
            retention_seconds: DEFAULT_LOCAL_GC_RETENTION_SECONDS,
        }
    }
}

impl LocalGcOptions {
    /// Returns dry-run options.
    #[must_use]
    pub const fn dry_run() -> Self {
        Self {
            mark: false,
            sweep: false,
            retention_seconds: DEFAULT_LOCAL_GC_RETENTION_SECONDS,
        }
    }

    /// Returns mark-only options.
    #[must_use]
    pub const fn mark_only(retention_seconds: u64) -> Self {
        Self {
            mark: true,
            sweep: false,
            retention_seconds,
        }
    }

    /// Returns sweep-only options.
    #[must_use]
    pub const fn sweep_only() -> Self {
        Self {
            mark: false,
            sweep: true,
            retention_seconds: DEFAULT_LOCAL_GC_RETENTION_SECONDS,
        }
    }

    /// Returns mark-and-sweep options.
    #[must_use]
    pub const fn mark_and_sweep(retention_seconds: u64) -> Self {
        Self {
            mark: true,
            sweep: true,
            retention_seconds,
        }
    }

    /// Returns the operator-facing mode label.
    #[must_use]
    pub const fn mode_name(&self) -> &'static str {
        match (self.mark, self.sweep) {
            (false, false) => "dry-run",
            (true, false) => "mark",
            (false, true) => "sweep",
            (true, true) => "mark-and-sweep",
        }
    }
}

/// Local filesystem garbage-collection report.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LocalGcReport {
    /// Number of file and file-version records scanned.
    pub scanned_records: u64,
    /// Number of distinct chunk hashes referenced by records.
    pub referenced_chunks: u64,
    /// Number of orphan chunk files discovered in this run.
    pub orphan_chunks: u64,
    /// Number of bytes held by orphan chunk files in this run.
    pub orphan_chunk_bytes: u64,
    /// Number of active quarantine candidates after the run completes.
    pub active_quarantine_candidates: u64,
    /// Number of quarantine candidates created during this run.
    pub new_quarantine_candidates: u64,
    /// Number of previously quarantined candidates still waiting for expiry.
    pub retained_quarantine_candidates: u64,
    /// Number of quarantine candidates released because they were deleted, missing, or
    /// reachable again.
    pub released_quarantine_candidates: u64,
    /// Number of orphan chunk files deleted during this run.
    pub deleted_chunks: u64,
    /// Number of bytes reclaimed during this run.
    pub deleted_bytes: u64,
}

/// One active retention-window entry after a GC run.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GcRetentionReportEntry {
    /// Chunk hash derived from the object key.
    pub hash: String,
    /// Object-store key tracked by the retention entry.
    pub object_key: String,
    /// Observed object length when the object became unreachable.
    pub observed_length: u64,
    /// When the object first became unreachable.
    pub first_seen_unreachable_at_unix_seconds: u64,
    /// When the object becomes eligible for deletion.
    pub delete_after_unix_seconds: u64,
    /// Whether the retention window is already expired.
    pub expired: bool,
    /// Seconds remaining until the object becomes eligible for deletion.
    pub seconds_until_delete: u64,
}

/// One currently orphaned object after a GC run.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GcOrphanInventoryEntry {
    /// Chunk hash derived from the object key.
    pub hash: String,
    /// Object-store key for the orphaned object.
    pub object_key: String,
    /// Observed object length.
    pub bytes: u64,
    /// Whether the object already has durable quarantine state.
    pub quarantine_state: GcOrphanQuarantineState,
    /// When the object first became unreachable, if it is quarantined.
    pub first_seen_unreachable_at_unix_seconds: Option<u64>,
    /// When the object becomes eligible for deletion, if it is quarantined.
    pub delete_after_unix_seconds: Option<u64>,
}

/// Quarantine state for one orphaned object.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum GcOrphanQuarantineState {
    /// The object is orphaned but not yet recorded in durable quarantine state.
    Untracked,
    /// The object is orphaned and already recorded in durable quarantine state.
    Quarantined,
}

/// Detailed GC diagnostics intended for operators and automation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocalGcDiagnostics {
    /// Human-readable GC summary.
    pub report: LocalGcReport,
    /// Active quarantine entries after the run.
    pub retention_report: Vec<GcRetentionReportEntry>,
    /// Current orphan inventory after the run.
    pub orphan_inventory: Vec<GcOrphanInventoryEntry>,
}

/// Re-exported index types for convenience.
pub use shardline_index::LocalRecordStore;
