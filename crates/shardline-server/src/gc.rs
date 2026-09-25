pub(crate) use shardline_gc::run_gc_with_oci_tombstones;
pub use shardline_gc::{
    DEFAULT_LOCAL_GC_RETENTION_SECONDS, LocalGcDiagnostics, LocalGcOptions, LocalGcReport,
};

#[cfg(test)]
pub(crate) use shardline_gc::{
    GcOrphanQuarantineState, quarantine_record_path, quarantine_root, run_local_gc,
    run_local_gc_diagnostics,
};
