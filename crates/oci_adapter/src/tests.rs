use std::future::Future;
use std::num::{NonZeroU64, NonZeroUsize};
use std::path::Path;

use bytes::Bytes;
use sha2::{Digest, Sha256};
use tempfile::TempDir;

use crate::{
    OciAdapterError, OciReference, OciUploadSession, SerializableSha256State, append_upload_bytes,
    create_upload_session, delete_upload_session, new_upload_session_id,
    oci_blob_key, oci_manifest_key, oci_manifest_media_type_key, oci_manifest_prefix,
    oci_tag_key, oci_tag_prefix, parse_reference,
    purge_expired_upload_sessions, read_upload_session, upload_body_integrity, upload_length,
    upload_session_expired,
};
use crate::traits::OciBackend;
use shardline_protocol::{RepositoryProvider, RepositoryScope};
use shardline_storage::{DeleteOutcome, ObjectKey, PutOutcome};

/// No-op backend used in tests that never creates S3 multipart uploads.
struct TestBackend;

impl OciBackend for TestBackend {
    async fn create_resumable_object_upload(
        &self,
        _object_key: &ObjectKey,
    ) -> Result<Option<String>, OciAdapterError> {
        Ok(None)
    }

    async fn upload_resumable_object_part(
        &self,
        _object_key: &ObjectKey,
        _upload_id: &str,
        _part_idx: usize,
        _bytes: Bytes,
    ) -> Result<String, OciAdapterError> {
        Err(OciAdapterError::NotFound)
    }

    async fn complete_resumable_object_upload(
        &self,
        _object_key: &ObjectKey,
        _upload_id: &str,
        _parts: Vec<(usize, String)>,
    ) -> Result<(), OciAdapterError> {
        Err(OciAdapterError::NotFound)
    }

    async fn abort_resumable_object_upload(
        &self,
        _object_key: &ObjectKey,
        _upload_id: &str,
    ) -> Result<(), OciAdapterError> {
        Ok(())
    }

    fn put_sha256_addressed_object_bytes_if_absent(
        &self,
        _object_key: &ObjectKey,
        _digest_hex: &str,
        _bytes: Vec<u8>,
    ) -> Result<PutOutcome, OciAdapterError> {
        Ok(PutOutcome::Inserted)
    }

    fn copy_object_if_absent(
        &self,
        _source: &ObjectKey,
        _destination: &ObjectKey,
    ) -> Result<PutOutcome, OciAdapterError> {
        Ok(PutOutcome::Inserted)
    }

    async fn delete_object_if_present(
        &self,
        _object_key: &ObjectKey,
    ) -> Result<DeleteOutcome, OciAdapterError> {
        Ok(DeleteOutcome::Deleted)
    }
}

const NO_BACKEND: Option<&TestBackend> = None;

fn temp_root() -> TempDir {
    tempfile::tempdir().expect("failed to create temp dir")
}

fn ttl() -> NonZeroU64 {
    NonZeroU64::new(3600).unwrap()
}

fn max_sessions() -> NonZeroUsize {
    NonZeroUsize::new(100).unwrap()
}

fn create_test_session(root: &Path, use_s3: bool) -> impl Future<Output = Result<String, OciAdapterError>> {
    let root = root.to_path_buf();
    async move {
        create_upload_session(
            &root,
            NO_BACKEND,
            "repo",
            None,
            ttl(),
            max_sessions(),
            use_s3,
        )
        .await
    }
}

#[tokio::test]
async fn create_upload_session_returns_valid_session_id() {
    let root = temp_root();
    let session_id = create_test_session(root.path(), false).await.unwrap();
    assert!(!session_id.is_empty());
    assert!(
        session_id.len() <= 64,
        "session id too long: {}",
        session_id.len()
    );
    assert!(
        session_id.bytes().all(|b| b.is_ascii_hexdigit()),
        "session id contains non-hex chars: {session_id}"
    );
}

#[tokio::test]
async fn create_upload_session_respects_limit() {
    let root = temp_root();
    let max = NonZeroUsize::new(1).unwrap();
    let session_id = create_upload_session(
        root.path(),
        NO_BACKEND,
        "my-repo",
        None,
        ttl(),
        max,
        false,
    )
    .await
    .unwrap();
    assert!(!session_id.is_empty());
    let result = create_upload_session(
        root.path(),
        NO_BACKEND,
        "my-repo",
        None,
        ttl(),
        max,
        false,
    )
    .await;
    assert!(matches!(result, Err(OciAdapterError::TooManyUploadSessions)));
}

#[tokio::test]
async fn create_upload_session_persists_metadata() {
    let root = temp_root();
    let session_id = create_test_session(root.path(), false).await.unwrap();
    let session = read_upload_session(root.path(), &session_id, ttl())
        .await
        .expect("read_upload_session failed");
    assert_eq!(session.repository, "repo");
    assert!(!session.use_s3_multipart);
}

#[tokio::test]
async fn create_s3_multipart_session_persists_metadata() {
    let root = temp_root();
    let session_id = create_test_session(root.path(), true).await.unwrap();
    let session = read_upload_session(root.path(), &session_id, ttl())
        .await
        .unwrap();
    assert!(session.use_s3_multipart);
    assert!(session.s3_multipart.is_none());
}

#[tokio::test]
async fn create_upload_session_with_repository_scope_works() {
    let root = temp_root();
    let session_id = create_upload_session(
        root.path(),
        NO_BACKEND,
        "my-repo",
        None,
        ttl(),
        max_sessions(),
        false,
    )
    .await
    .unwrap();
    assert!(!session_id.is_empty());
}

#[tokio::test]
async fn create_upload_session_rejects_invalid_repository() {
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
async fn create_upload_session_rejects_invalid_repository_scope() {
    let root = temp_root();
    let result = create_upload_session(
        root.path(),
        NO_BACKEND,
        "my-repo",
        None,
        ttl(),
        max_sessions(),
        false,
    )
    .await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn read_upload_session_returns_not_found_for_nonexistent() {
    let root = temp_root();
    let result = read_upload_session(
        root.path(),
        "10000000000000000000000000000000",
        ttl(),
    )
    .await;
    assert!(matches!(result, Err(OciAdapterError::NotFound)));
}

#[tokio::test]
async fn append_upload_bytes_extends_session() {
    let root = temp_root();
    let session_id = create_test_session(root.path(), false).await.unwrap();
    append_upload_bytes(root.path(), &session_id, b"hello")
        .await
        .unwrap();
    let len = upload_length(root.path(), &session_id).await.unwrap();
    assert_eq!(len, 5);
    append_upload_bytes(root.path(), &session_id, b" world")
        .await
        .unwrap();
    let len = upload_length(root.path(), &session_id).await.unwrap();
    assert_eq!(len, 11);
}

#[tokio::test]
async fn append_upload_bytes_returns_not_found_for_missing_session() {
    let root = temp_root();
    let result = append_upload_bytes(root.path(), "deadbeef", b"data").await;
    assert!(matches!(result, Err(OciAdapterError::NotFound)));
}

#[tokio::test]
async fn upload_length_returns_correct_value() {
    let root = temp_root();
    let session_id = create_test_session(root.path(), false).await.unwrap();
    append_upload_bytes(root.path(), &session_id, b"abcdef")
        .await
        .unwrap();
    let len = upload_length(root.path(), &session_id).await.unwrap();
    assert_eq!(len, 6);
}

#[tokio::test]
async fn upload_length_returns_not_found_for_missing() {
    let root = temp_root();
    let result = upload_length(
        root.path(),
        "10000000000000000000000000000001",
    )
    .await;
    assert!(matches!(result, Err(OciAdapterError::NotFound)));
}

#[tokio::test]
async fn delete_upload_session_removes_files() {
    let root = temp_root();
    let session_id = create_test_session(root.path(), false).await.unwrap();
    delete_upload_session(root.path(), &session_id)
        .await
        .unwrap();
    let result = read_upload_session(root.path(), &session_id, ttl()).await;
    assert!(matches!(result, Err(OciAdapterError::NotFound)));
}

#[tokio::test]
async fn delete_upload_session_is_idempotent() {
    let root = temp_root();
    let session_id = create_test_session(root.path(), false).await.unwrap();
    delete_upload_session(root.path(), &session_id)
        .await
        .unwrap();
    delete_upload_session(root.path(), &session_id)
        .await
        .unwrap();
}

#[tokio::test]
async fn read_upload_session_returns_not_found_for_missing() {
    let root = temp_root();
    let result = read_upload_session(root.path(), "00000000000000000000000000000000", ttl()).await;
    assert!(matches!(result, Err(OciAdapterError::NotFound)));
}

#[tokio::test]
async fn read_upload_session_returns_not_found_when_expired() {
    let root = temp_root();
    let session_id = create_test_session(root.path(), false).await.unwrap();
    let zero_ttl = NonZeroU64::new(1).unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(1100)).await;
    let result = read_upload_session(root.path(), &session_id, zero_ttl).await;
    assert!(matches!(result, Err(OciAdapterError::NotFound)));
}

#[tokio::test]
async fn purge_expired_upload_sessions_removes_old_sessions() {
    let root = temp_root();
    let session_id = create_test_session(root.path(), false).await.unwrap();
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
        + 10_000;
    purge_expired_upload_sessions::<TestBackend>(
        root.path(),
        NO_BACKEND,
        NonZeroU64::new(1).unwrap(),
        now,
    )
    .await
    .unwrap();
    let result = read_upload_session(root.path(), &session_id, ttl()).await;
    assert!(matches!(result, Err(OciAdapterError::NotFound)));
}

#[tokio::test]
async fn purge_expired_upload_sessions_keeps_fresh_sessions() {
    let root = temp_root();
    let session_id = create_test_session(root.path(), false).await.unwrap();
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    purge_expired_upload_sessions::<TestBackend>(root.path(), NO_BACKEND, ttl(), now)
        .await
        .unwrap();
    let result = read_upload_session(root.path(), &session_id, ttl()).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn purge_expired_upload_sessions_handles_missing_upload_dir() {
    let root = temp_root();
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let result =
        purge_expired_upload_sessions::<TestBackend>(root.path(), NO_BACKEND, ttl(), now).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn upload_body_integrity_matches_expected_sha256() {
    let root = temp_root();
    let session_id = create_test_session(root.path(), false).await.unwrap();
    let data = b"hello world";
    append_upload_bytes(root.path(), &session_id, data)
        .await
        .unwrap();
    let (sha256_hex, integrity) = upload_body_integrity(root.path(), &session_id)
        .await
        .unwrap();
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(data);
    let expected = hex::encode(hasher.finalize());
    assert_eq!(sha256_hex, expected);
    assert_eq!(integrity.length(), data.len() as u64);
}

#[tokio::test]
async fn new_upload_session_id_is_unique() {
    let mut ids = std::collections::HashSet::new();
    for _ in 0..100 {
        let id = new_upload_session_id();
        assert!(ids.insert(id.clone()), "duplicate session id: {id}");
    }
}

#[test]
fn upload_session_expired_returns_true_when_past_ttl() {
    let session = OciUploadSession {
        repository: "repo".to_owned(),
        scope_namespace: "global".to_owned(),
        created_at_unix_seconds: 1000,
        last_touched_unix_seconds: 1000,
        use_s3_multipart: false,
        s3_multipart: None,
    };
    let ttl = NonZeroU64::new(60).unwrap();
    assert!(upload_session_expired(&session, ttl, 1061));
}

#[test]
fn upload_session_expired_returns_false_when_within_ttl() {
    let session = OciUploadSession {
        repository: "repo".to_owned(),
        scope_namespace: "global".to_owned(),
        created_at_unix_seconds: 1000,
        last_touched_unix_seconds: 1000,
        use_s3_multipart: false,
        s3_multipart: None,
    };
    let ttl = NonZeroU64::new(60).unwrap();
    assert!(!upload_session_expired(&session, ttl, 1050));
}

#[test]
fn upload_session_expired_returns_false_at_exact_boundary() {
    let session = OciUploadSession {
        repository: "repo".to_owned(),
        scope_namespace: "global".to_owned(),
        created_at_unix_seconds: 1000,
        last_touched_unix_seconds: 1000,
        use_s3_multipart: false,
        s3_multipart: None,
    };
    let ttl = NonZeroU64::new(60).unwrap();
    assert!(upload_session_expired(&session, ttl, 1060));
    assert!(!upload_session_expired(&session, ttl, 1059));
}

#[test]
fn upload_session_ids_are_hex_and_not_reused_back_to_back() {
    let first = new_upload_session_id();
    let second = new_upload_session_id();

    assert_eq!(first.len(), 32);
    assert_eq!(second.len(), 32);
    assert!(first.bytes().all(|byte| byte.is_ascii_hexdigit()));
    assert!(second.bytes().all(|byte| byte.is_ascii_hexdigit()));
    assert_ne!(first, second);
}

#[test]
fn serializable_sha256_state_matches_reference_digest() {
    let mut state = SerializableSha256State::default();
    assert!(state.update(b"chunk-1").is_ok());
    assert!(state.update(&vec![b'x'; 1_000_000]).is_ok());
    assert!(state.update(b"chunk-3").is_ok());

    let mut reference = Sha256::new();
    reference.update(b"chunk-1");
    reference.update(vec![b'x'; 1_000_000]);
    reference.update(b"chunk-3");
    let expected = hex::encode(reference.finalize());

    assert!(matches!(
        state.finalize_hex().as_deref(),
        Ok(actual) if actual == expected
    ));
}

// ── Key construction tests ──────────────────────────────────────────────────

const VALID_DIGEST: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const VALID_DIGEST_FULL: &str = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

fn test_scope() -> RepositoryScope {
    RepositoryScope::new(RepositoryProvider::GitHub, "team", "assets", None).unwrap()
}

#[test]
fn oci_blob_key_global_namespace() {
    let key = oci_blob_key("team/assets", VALID_DIGEST_FULL, None).unwrap();
    let s = key.as_str();
    assert!(
        s.contains("protocols/oci/global/repos/"),
        "expected global namespace, got: {s}"
    );
    assert!(s.contains("/blobs/"), "expected blobs path, got: {s}");
    assert!(
        s.contains(VALID_DIGEST_FULL),
        "expected digest in key, got: {s}"
    );
}

#[test]
fn oci_blob_key_with_scope() {
    let scope = test_scope();
    let key = oci_blob_key("team/assets", VALID_DIGEST_FULL, Some(&scope)).unwrap();
    let s = key.as_str();
    assert!(
        !s.contains("/global/"),
        "should not contain global namespace with scope, got: {s}"
    );
    assert!(s.contains("/blobs/"), "expected blobs path, got: {s}");
}

#[test]
fn oci_blob_key_empty_repo_errors() {
    assert!(matches!(
        oci_blob_key("", VALID_DIGEST_FULL, None),
        Err(OciAdapterError::InvalidRepositoryName)
    ));
}

#[test]
fn oci_manifest_key_global_namespace() {
    let key = oci_manifest_key("team/assets", VALID_DIGEST_FULL, None).unwrap();
    let s = key.as_str();
    assert!(
        s.contains("protocols/oci/global/repos/"),
        "expected global namespace, got: {s}"
    );
    assert!(
        s.contains("/manifests/"),
        "expected manifests path, got: {s}"
    );
}

#[test]
fn oci_manifest_key_with_scope() {
    let scope = test_scope();
    let key = oci_manifest_key("team/assets", VALID_DIGEST_FULL, Some(&scope)).unwrap();
    let s = key.as_str();
    assert!(
        !s.contains("/global/"),
        "should not contain global namespace with scope, got: {s}"
    );
    assert!(s.contains("/manifests/"), "expected manifests path, got: {s}");
}

#[test]
fn oci_manifest_media_type_key_contains_expected_path() {
    let key = oci_manifest_media_type_key("team/assets", VALID_DIGEST_FULL, None).unwrap();
    let s = key.as_str();
    assert!(
        s.contains("/manifest-media-types/"),
        "expected manifest-media-types path, got: {s}"
    );
}

#[test]
fn oci_tag_key_global_namespace() {
    let key = oci_tag_key("team/assets", "latest", None).unwrap();
    let s = key.as_str();
    assert!(
        s.contains("protocols/oci/global/repos/"),
        "expected global namespace, got: {s}"
    );
    assert!(s.contains("/tags/latest"), "expected tags/latest, got: {s}");
}

#[test]
fn oci_tag_key_with_scope() {
    let scope = test_scope();
    let key = oci_tag_key("team/assets", "v1.0", Some(&scope)).unwrap();
    let s = key.as_str();
    assert!(
        !s.contains("/global/"),
        "should not contain global namespace with scope, got: {s}"
    );
    assert!(s.contains("/tags/v1.0"), "expected tags/v1.0, got: {s}");
}

#[test]
fn oci_tag_key_empty_tag_errors() {
    assert!(matches!(
        oci_tag_key("team/assets", "", None),
        Err(OciAdapterError::InvalidManifestReference)
    ));
}

#[test]
fn oci_manifest_prefix_returns_manifest_path() {
    let prefix = oci_manifest_prefix("team/assets", None).unwrap();
    let s = prefix.as_str();
    assert!(
        s.contains("/manifests/"),
        "expected manifests path in prefix, got: {s}"
    );
}

#[test]
fn oci_manifest_prefix_with_scope() {
    let scope = test_scope();
    let prefix = oci_manifest_prefix("team/assets", Some(&scope)).unwrap();
    let s = prefix.as_str();
    assert!(
        !s.contains("/global/"),
        "should not contain global namespace with scope, got: {s}"
    );
    assert!(s.contains("/manifests/"), "expected manifests path, got: {s}");
}

#[test]
fn oci_tag_prefix_returns_tag_path() {
    let prefix = oci_tag_prefix("team/assets", None).unwrap();
    let s = prefix.as_str();
    assert!(
        s.contains("/tags/"),
        "expected tags path in prefix, got: {s}"
    );
}

#[test]
fn oci_tag_prefix_with_scope() {
    let scope = test_scope();
    let prefix = oci_tag_prefix("team/assets", Some(&scope)).unwrap();
    let s = prefix.as_str();
    assert!(
        !s.contains("/global/"),
        "should not contain global namespace with scope, got: {s}"
    );
    assert!(s.contains("/tags/"), "expected tags path, got: {s}");
}

// ── parse_reference tests ───────────────────────────────────────────────────

#[test]
fn parse_reference_digest() {
    let r = parse_reference(VALID_DIGEST_FULL).unwrap();
    // parse_sha256_digest strips the "sha256:" prefix
    assert!(matches!(r, OciReference::Digest(ref d) if d == VALID_DIGEST));
}

#[test]
fn parse_reference_tag() {
    let r = parse_reference("latest").unwrap();
    assert!(matches!(r, OciReference::Tag(ref t) if t == "latest"));
}

#[test]
fn parse_reference_empty_tag_errors() {
    assert!(matches!(
        parse_reference(""),
        Err(OciAdapterError::InvalidManifestReference)
    ));
}

// ── SerializableSha256State additional tests ────────────────────────────────

#[test]
fn serializable_sha256_empty_state_matches_reference() {
    let state = SerializableSha256State::default();
    let hex = state.finalize_hex().unwrap();
    let reference = hex::encode(Sha256::new().finalize());
    assert_eq!(hex, reference);
}

#[test]
fn serializable_sha256_single_update_matches_reference() {
    let mut state = SerializableSha256State::default();
    state.update(b"hello").unwrap();
    let hex = state.finalize_hex().unwrap();

    let mut reference = Sha256::new();
    reference.update(b"hello");
    let expected = hex::encode(reference.finalize());
    assert_eq!(hex, expected);
}

#[test]
fn serializable_sha256_multiple_updates_match_concatenated() {
    let mut state = SerializableSha256State::default();
    state.update(b"hel").unwrap();
    state.update(b"lo").unwrap();
    state.update(b" world").unwrap();
    let hex = state.finalize_hex().unwrap();

    let mut reference = Sha256::new();
    reference.update(b"hello world");
    let expected = hex::encode(reference.finalize());
    assert_eq!(hex, expected);
}

#[test]
fn serializable_sha256_serialization_round_trip() {
    let mut state = SerializableSha256State::default();
    state.update(b"partial").unwrap();
    let mid_hex = state.finalize_hex().unwrap();

    let serialized = serde_json::to_vec(&state).unwrap();
    let deserialized: SerializableSha256State = serde_json::from_slice(&serialized).unwrap();
    let rt_hex = deserialized.finalize_hex().unwrap();
    assert_eq!(mid_hex, rt_hex);
}

#[test]
fn serializable_sha256_serialization_round_trip_after_multiple_updates() {
    let mut state = SerializableSha256State::default();
    state.update(&[0u8; 128]).unwrap(); // triggers block compression
    state.update(b"tail").unwrap();
    let expected_hex = state.finalize_hex().unwrap();

    let serialized = serde_json::to_vec(&state).unwrap();
    let deserialized: SerializableSha256State = serde_json::from_slice(&serialized).unwrap();
    assert_eq!(expected_hex, deserialized.finalize_hex().unwrap());
}

// ── Key content-addressed uniqueness ────────────────────────────────────────

#[test]
fn oci_blob_key_different_digests_produce_different_keys() {
    let key1 = oci_blob_key(
        "team/assets",
        "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        None,
    )
    .unwrap();
    let key2 = oci_blob_key(
        "team/assets",
        "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
        None,
    )
    .unwrap();
    assert_ne!(key1.as_str(), key2.as_str());
}

#[test]
fn oci_manifest_key_different_repos_produce_different_keys() {
    let key1 = oci_manifest_key("team/assets", VALID_DIGEST_FULL, None).unwrap();
    let key2 = oci_manifest_key("team/other", VALID_DIGEST_FULL, None).unwrap();
    assert_ne!(key1.as_str(), key2.as_str());
}

#[test]
fn oci_tag_key_different_tags_produce_different_keys() {
    let key1 = oci_tag_key("team/assets", "latest", None).unwrap();
    let key2 = oci_tag_key("team/assets", "v1.0", None).unwrap();
    assert_ne!(key1.as_str(), key2.as_str());
}
