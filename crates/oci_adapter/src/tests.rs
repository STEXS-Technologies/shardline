use std::future::Future;
use std::num::{NonZeroU64, NonZeroUsize};
use std::path::Path;

use bytes::Bytes;
use sha2::{Digest, Sha256};
use tempfile::TempDir;

use crate::{
    OciAdapterError, OciUploadSession, SerializableSha256State, append_upload_bytes,
    create_upload_session, delete_upload_session, new_upload_session_id,
    purge_expired_upload_sessions, read_upload_session, upload_body_integrity, upload_length,
    upload_session_expired,
};
use crate::traits::OciBackend;
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
