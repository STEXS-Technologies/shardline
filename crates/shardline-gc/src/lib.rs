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

//! Garbage collection for Shardline chunk storage.
//!
//! This crate provides local filesystem garbage collection including orphan
//! chunk discovery, quarantine lifecycle management, and retention window
//! enforcement.
//!
//! # Quick start
//!
//! GC is driven by a [`LocalGcOptions`] mode selection and reports back through
//! [`LocalGcReport`]. The options and report types are pure and safe to build
//! directly:
//!
//! ```
//! use shardline_gc::{
//!     LocalGcDiagnostics, LocalGcOptions, LocalGcReport, MINIMUM_GC_RETENTION_SECONDS,
//! };
//!
//! // Default options are a no-op dry run: nothing is marked or deleted.
//! let options = LocalGcOptions::default();
//! assert_eq!(options.mode_name(), "dry-run");
//! assert!(!options.mark && !options.sweep);
//!
//! // A real cleanup run marks orphans and sweeps expired quarantine entries,
//! // with a retention window that protects in-flight uploads.
//! let options = LocalGcOptions::mark_and_sweep(MINIMUM_GC_RETENTION_SECONDS);
//! assert_eq!(options.mode_name(), "mark-and-sweep");
//!
//! // Reports and diagnostics are plain data you can inspect after a run.
//! let report = LocalGcReport::default();
//! assert_eq!(report.deleted_chunks, 0);
//! let diagnostics = LocalGcDiagnostics {
//!     report,
//!     retention_report: Vec::new(),
//!     orphan_inventory: Vec::new(),
//! };
//! assert!(diagnostics.orphan_inventory.is_empty());
//! ```
//!
//! To run GC against real storage, drive [`run_gc_with_stores`] with explicit
//! record, index, and object-store adapters (the `shardline-server` crate wires
//! this up from its own configuration).

// ---------------------------------------------------------------------------
// Submodules
// ---------------------------------------------------------------------------

mod dispatch;
mod error;
mod oci_tombstones;
mod quarantine;
mod reachability;
mod runner;
mod types;

#[cfg(test)]
mod tests;

// ---------------------------------------------------------------------------
// Re-exports
// ---------------------------------------------------------------------------

pub use error::GcError;
#[doc(hidden)]
pub use oci_tombstones::OciRecordReclaimer;
pub use runner::{
    quarantine_record_path, quarantine_root, run_gc_with_oci_tombstones, run_gc_with_stores,
    run_local_gc, run_local_gc_diagnostics,
};
pub use shardline_server_core::server_frontend::{
    ServerFrontend, ServerFrontend as ServerFrontendKind, ServerFrontendParseError,
};
pub use types::DEFAULT_LOCAL_GC_RETENTION_SECONDS;
pub use types::{
    GcOrphanInventoryEntry, GcOrphanQuarantineState, GcRetentionReportEntry, LocalGcDiagnostics,
    LocalGcOptions, LocalGcReport, LocalRecordStore, MINIMUM_GC_RETENTION_SECONDS,
};
