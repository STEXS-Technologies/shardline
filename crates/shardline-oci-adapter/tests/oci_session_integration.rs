//! Integration tests for the OCI upload session lifecycle.
//!
//! These tests exercise the public session API (`create_upload_session`,
//! `append_upload_bytes`, `read_upload_session`, `upload_body_integrity`,
//! `delete_upload_session`, etc.) from an external consumer's perspective.
//! The filesystem-backed upload directory lives inside a `tempfile::TempDir`.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::shadow_unrelated,
    clippy::let_underscore_must_use,
    clippy::format_push_string,
    clippy::panic,
    clippy::match_wild_err_arm,
    clippy::ignored_unit_patterns,
    clippy::missing_const_for_fn
)]

use std::num::{NonZeroU64, NonZeroUsize};
use std::path::Path;

use sha2::Digest;
use shardline_oci_adapter::{
    OciAdapterError, OciBackend, append_upload_bytes, create_upload_session, delete_upload_session,
    read_upload_session, upload_body_integrity, upload_length,
};
use tempfile::TempDir;

#[path = "support/backend.rs"]
mod backend;
use backend::NO_BACKEND;

// ── Helper functions ──────────────────────────────────────────────────────

fn temp_root() -> TempDir {
    tempfile::tempdir().expect("failed to create temp dir")
}

fn ttl() -> NonZeroU64 {
    NonZeroU64::new(3600).expect("ttl is non-zero")
}

fn max_sessions() -> NonZeroUsize {
    NonZeroUsize::new(100).expect("max sessions is non-zero")
}

/// Create a session and return the session id.
async fn create_session(root: &Path) -> String {
    create_upload_session(
        root,
        NO_BACKEND,
        "integration/repo",
        None,
        ttl(),
        max_sessions(),
        false,
    )
    .await
    .expect("create_upload_session should succeed")
}

// ── Tests ─────────────────────────────────────────────────────────────────

#[tokio::test]
async fn session_create_and_read() {
    let root = temp_root();
    let session_id = create_session(root.path()).await;

    let session = read_upload_session(root.path(), &session_id, ttl())
        .await
        .expect("read_upload_session should succeed");
    assert_eq!(session.repository, "integration/repo");
    assert!(!session.use_s3_multipart);
    assert!(session.created_at_unix_seconds > 0);
    assert_eq!(
        session.created_at_unix_seconds,
        session.last_touched_unix_seconds
    );
}

#[tokio::test]
async fn session_append_and_verify_length() {
    let root = temp_root();
    let session_id = create_session(root.path()).await;

    let len = append_upload_bytes(root.path(), &session_id, b"hello")
        .await
        .unwrap();
    assert_eq!(len, 5);

    let len = append_upload_bytes(root.path(), &session_id, b" world")
        .await
        .unwrap();
    assert_eq!(len, 11);

    let stored_len = upload_length(root.path(), &session_id).await.unwrap();
    assert_eq!(stored_len, 11);
}

#[tokio::test]
async fn session_append_and_verify_integrity() {
    let root = temp_root();
    let session_id = create_session(root.path()).await;
    let data = b"integration test data for sha256 verification";

    append_upload_bytes(root.path(), &session_id, data)
        .await
        .unwrap();

    let (sha256_hex, integrity) = upload_body_integrity(root.path(), &session_id)
        .await
        .expect("upload_body_integrity should succeed");

    // Compute expected SHA-256
    let mut hasher = sha2::Sha256::new();
    hasher.update(data);
    let expected = hex::encode(hasher.finalize());

    assert_eq!(sha256_hex, expected);
    assert_eq!(integrity.length(), data.len() as u64);
}

#[tokio::test]
async fn session_delete_removes_all_files() {
    let root = temp_root();
    let session_id = create_session(root.path()).await;

    // Confirm it exists
    let _session = read_upload_session(root.path(), &session_id, ttl())
        .await
        .unwrap();

    delete_upload_session(root.path(), &session_id)
        .await
        .expect("delete_upload_session should succeed");

    // Read should now fail with NotFound
    let result = read_upload_session(root.path(), &session_id, ttl()).await;
    assert!(matches!(result, Err(OciAdapterError::NotFound)));
}

#[tokio::test]
async fn session_delete_is_idempotent() {
    let root = temp_root();
    let session_id = create_session(root.path()).await;

    delete_upload_session(root.path(), &session_id)
        .await
        .unwrap();
    // Second delete should also succeed (no error for already-deleted)
    delete_upload_session(root.path(), &session_id)
        .await
        .unwrap();
}

#[tokio::test]
async fn session_read_nonexistent_returns_not_found() {
    let root = temp_root();
    let result = read_upload_session(root.path(), "00000000000000000000000000000000", ttl()).await;
    assert!(matches!(result, Err(OciAdapterError::NotFound)));
}

#[tokio::test]
async fn session_append_to_nonexistent_returns_not_found() {
    let root = temp_root();
    let result =
        append_upload_bytes(root.path(), "deadbeef0000000000000000deadbeef", b"data").await;
    assert!(matches!(result, Err(OciAdapterError::NotFound)));
}

#[tokio::test]
async fn session_full_lifecycle() {
    // Create → append → read → verify → delete
    let root = temp_root();
    let session_id = create_session(root.path()).await;

    // Append data
    let data = b"full lifecycle payload";
    append_upload_bytes(root.path(), &session_id, data)
        .await
        .unwrap();

    // Read and verify metadata
    let session = read_upload_session(root.path(), &session_id, ttl())
        .await
        .unwrap();
    assert_eq!(session.repository, "integration/repo");

    // Verify integrity
    let (sha256_hex, integrity) = upload_body_integrity(root.path(), &session_id)
        .await
        .unwrap();
    let mut hasher = sha2::Sha256::new();
    hasher.update(data);
    assert_eq!(sha256_hex, hex::encode(hasher.finalize()));
    assert_eq!(integrity.length(), data.len() as u64);

    // Delete
    delete_upload_session(root.path(), &session_id)
        .await
        .unwrap();
    let result = read_upload_session(root.path(), &session_id, ttl()).await;
    assert!(matches!(result, Err(OciAdapterError::NotFound)));
}

#[tokio::test]
async fn session_create_rejects_invalid_repository() {
    let root = temp_root();
    let result = create_upload_session(
        root.path(),
        NO_BACKEND,
        "",
        None,
        ttl(),
        max_sessions(),
        false,
    )
    .await;
    assert!(matches!(
        result,
        Err(OciAdapterError::InvalidRepositoryName)
    ));
}

#[tokio::test]
async fn session_upload_length_nonexistent() {
    let root = temp_root();
    let result = upload_length(root.path(), "00000000000000000000000000000000").await;
    assert!(matches!(result, Err(OciAdapterError::NotFound)));
}

#[tokio::test]
async fn session_append_invalid_session_id() {
    let root = temp_root();
    let result = append_upload_bytes(root.path(), "bad/session/id!", b"data").await;
    assert!(matches!(result, Err(OciAdapterError::InvalidUploadSession)));
}

#[tokio::test]
async fn session_read_invalid_session_id() {
    let root = temp_root();
    let result = read_upload_session(root.path(), "bad/session/id!", ttl()).await;
    assert!(matches!(result, Err(OciAdapterError::InvalidUploadSession)));
}
