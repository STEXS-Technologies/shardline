mod mapping;
mod scanner;

#[cfg(test)]
mod tests;

// ── Re-exports ────────────────────────────────────────────────────────

/// Crate-visible entry point called by runner.rs.
pub(super) use scanner::scan_record_tree;

// Items below are only needed during test compilation so that
// tests.rs's `use super::*;` can resolve them.  The linter does
// not see cross-module usage via super::*, so we allow the
// unused-imports lint exclusively inside cfg(test).
//
// This is not an #[allow] on non-test code — it applies only when
// the items themselves are active.

#[cfg_attr(test, allow(unused_imports))]
#[cfg(test)]
pub(super) use mapping::map_xorb_visit_error_fsck;

#[cfg_attr(test, allow(unused_imports))]
#[cfg(test)]
pub(super) use scanner::{
    inspect_chunks, inspect_latest_record, inspect_matching_version_record,
    inspect_native_xet_term, inspect_record_bytes,
};

#[cfg_attr(test, allow(unused_imports))]
#[cfg(test)]
pub(super) use crate::{
    FsckError, FsckIssueDetail, FsckIssueKind, FsckObjectContext, FsckReachability, FsckReport,
    PendingVersionRecordCheck, RecordKind, object_location_display, push_issue,
    push_reconstruction_plan_issue, record_path,
};

#[cfg_attr(test, allow(unused_imports))]
#[cfg(test)]
pub(super) use shardline_index::{
    FileChunkRecord, FileRecord, RecordTraversal, StoredRecord, parse_xet_hash_hex,
    xet_hash_hex_string,
};

#[cfg_attr(test, allow(unused_imports))]
#[cfg(test)]
pub(super) use shardline_server_core::{
    OpsRecordStore, ServerObjectStore, checked_add, checked_increment, chunk_hash,
    chunk_object_key, content_hash, parse_stored_file_record_bytes, read_full_object,
    validate_content_hash, validate_identifier,
};

#[cfg_attr(test, allow(unused_imports))]
#[cfg(test)]
pub(super) use shardline_storage::ObjectStore;

#[cfg_attr(test, allow(unused_imports))]
#[cfg(test)]
pub(super) use shardline_xet_adapter::{
    XorbVisitError, try_for_each_serialized_xorb_chunk, validate_serialized_xorb, xorb_object_key,
};
