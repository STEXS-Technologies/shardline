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

// ---------------------------------------------------------------------------
// Submodules
// ---------------------------------------------------------------------------

mod dispatch;
mod error;
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
pub use runner::{
    quarantine_record_path, quarantine_root, run_gc_with_stores, run_local_gc,
    run_local_gc_diagnostics,
};
pub use shardline_server_core::server_frontend::{
    ServerFrontend, ServerFrontend as ServerFrontendKind, ServerFrontendParseError,
};
pub use types::DEFAULT_LOCAL_GC_RETENTION_SECONDS;
pub use types::{
    GcOrphanInventoryEntry, GcOrphanQuarantineState, GcRetentionReportEntry, LocalGcDiagnostics,
    LocalGcOptions, LocalGcReport, LocalRecordStore, MINIMUM_GC_RETENTION_SECONDS,
};
