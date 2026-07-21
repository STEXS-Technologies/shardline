#![deny(unsafe_code)]

//! OCI Distribution protocol adapter for the Shardline server ecosystem.
//!
//! This crate provides OCI registry protocol support: upload session management,
//! manifest and blob key construction, content-addressed storage helpers, and
//! S3 multipart upload orchestration.

mod error;
mod protocol_support;
mod traits;

mod fs;
mod key;
mod multipart;
mod session;
mod types;

#[cfg(test)]
mod tests;

pub use error::OciAdapterError;
pub use traits::OciBackend;

// ── Re-exports ───────────────────────────────────────────────────────────────

pub use types::{
    OciReference, OciS3MultipartUploadSession, OciUploadSession, OciUploadSessionLock,
    SerializableSha256State,
};

pub use key::{
    oci_blob_key, oci_blob_location, oci_manifest_key, oci_manifest_location,
    oci_manifest_media_type_key, oci_manifest_prefix, oci_tag_key, oci_tag_prefix,
    oci_tag_target_key, oci_tag_target_prefix, parse_reference, upload_session_location,
    validate_repository,
};

pub use session::{
    append_upload_bytes, create_upload_session, delete_upload_session, lock_upload_sessions,
    new_upload_session_id, purge_expired_upload_sessions, read_upload_session,
    touch_upload_session, upload_body_integrity, upload_body_path_for_session, upload_length,
    upload_session_length,
};

pub use multipart::{
    abort_s3_multipart_upload_session, append_s3_multipart_upload_bytes,
    finalize_s3_multipart_upload_session,
};

// ── Crate-internal re-exports for test access ───────────────────────────────
//
// These are re-exported at the crate root so that `tests.rs` (declared as
// `#[cfg(test)] mod tests;` inside `lib.rs`) can reach them via `crate::` or
// `super::`.  Production sibling modules import directly from the submodule.

#[cfg(test)]
pub(crate) use fs::{
    append_file_anchored, delete_file_anchored, open_anchored_file, persist_upload_session,
    read_file_anchored, upload_body_path, upload_dir, upload_file_exists_async,
    upload_file_len_async, upload_metadata_path, upload_tail_path, write_file_atomically,
};
#[cfg(test)]
pub(crate) use multipart::write_upload_tail;
#[cfg(test)]
pub(crate) use session::{count_active_upload_sessions, upload_session_expired};
#[cfg(test)]
pub(crate) use types::global_scope_namespace;
