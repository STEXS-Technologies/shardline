use std::num::{NonZeroU64, NonZeroUsize};

use sha2::{Digest, Sha256};
use tempfile::TempDir;

use crate::{
    OciAdapterError, OciUploadSession, SerializableSha256State, append_upload_bytes,
    create_upload_session, delete_upload_session, new_upload_session_id,
    purge_expired_upload_sessions, read_upload_session, upload_body_integrity, upload_length,
    upload_session_expired,
};

fn temp_root() -> TempDir {
    tempfile::tempdir().expect("failed to create temp dir")
}

fn ttl() -> NonZeroU64 {
    NonZeroU64::new(3600).unwrap()
}

fn max_sessions() -> NonZeroUsize {
    NonZeroUsize::new(100).unwrap()
}

#[tokio::test]
async fn create_upload_session_returns_valid_session_id() {
    let root = temp_root();
    let session_id =
        create_upload_session(root.path(), "my-repo", None, ttl(), max_sessions(), false)
            .await
            .expect("create_upload_session failed");
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
async fn create_upload_session_persists_metadata() {
    let root = temp_root();
    let session_id =
        create_upload_session(root.path(), "test-repo", None, ttl(), max_sessions(), false)
            .await
            .unwrap();
    let session = read_upload_session(root.path(), &session_id, ttl())
        .await
        .expect("read_upload_session failed");
    assert_eq!(session.repository, "test-repo");
    assert!(!session.use_s3_multipart);
}

#[tokio::test]
async fn create_s3_multipart_session_persists_metadata() {
    let root = temp_root();
    let session_id =
        create_upload_session(root.path(), "s3-repo", None, ttl(), max_sessions(), true)
            .await
            .unwrap();
    let session = read_upload_session(root.path(), &session_id, ttl())
        .await
        .unwrap();
    assert!(session.use_s3_multipart);
    assert!(session.s3_multipart.is_none());
}

#[tokio::test]
async fn create_upload_session_rejects_invalid_repository() {
    let root = temp_root();
    let result = create_upload_session(root.path(), "", None, ttl(), max_sessions(), false).await;
    assert!(matches!(
        result,
        Err(OciAdapterError::InvalidRepositoryName)
    ));
}

#[tokio::test]
async fn append_upload_bytes_increases_length() {
    let root = temp_root();
    let session_id = create_upload_session(root.path(), "repo", None, ttl(), max_sessions(), false)
        .await
        .unwrap();
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
    let session_id = create_upload_session(root.path(), "repo", None, ttl(), max_sessions(), false)
        .await
        .unwrap();
    append_upload_bytes(root.path(), &session_id, b"abcdef")
        .await
        .unwrap();
    let len = upload_length(root.path(), &session_id).await.unwrap();
    assert_eq!(len, 6);
}

#[tokio::test]
async fn upload_length_returns_not_found_for_missing() {
    let root = temp_root();
    let result = upload_length(root.path(), "00000000000000000000000000000000").await;
    assert!(matches!(result, Err(OciAdapterError::NotFound)));
}

#[tokio::test]
async fn delete_upload_session_removes_files() {
    let root = temp_root();
    let session_id = create_upload_session(root.path(), "repo", None, ttl(), max_sessions(), false)
        .await
        .unwrap();
    delete_upload_session(root.path(), &session_id)
        .await
        .unwrap();
    let result = read_upload_session(root.path(), &session_id, ttl()).await;
    assert!(matches!(result, Err(OciAdapterError::NotFound)));
}

#[tokio::test]
async fn delete_upload_session_is_idempotent() {
    let root = temp_root();
    let session_id = create_upload_session(root.path(), "repo", None, ttl(), max_sessions(), false)
        .await
        .unwrap();
    delete_upload_session(root.path(), &session_id)
        .await
        .unwrap();
    // Second delete should not fail
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
    let session_id = create_upload_session(root.path(), "repo", None, ttl(), max_sessions(), false)
        .await
        .unwrap();
    // Read with ttl=0 to force expiry
    let zero_ttl = NonZeroU64::new(1).unwrap();
    // Wait so that the session is expired
    tokio::time::sleep(std::time::Duration::from_millis(1100)).await;
    let result = read_upload_session(root.path(), &session_id, zero_ttl).await;
    assert!(matches!(result, Err(OciAdapterError::NotFound)));
}

#[tokio::test]
async fn purge_expired_upload_sessions_removes_old_sessions() {
    let root = temp_root();
    let session_id = create_upload_session(root.path(), "repo", None, ttl(), max_sessions(), false)
        .await
        .unwrap();
    // Purge with now = session creation + ttl + 1 should remove it
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
        + 10_000;
    purge_expired_upload_sessions(root.path(), NonZeroU64::new(1).unwrap(), now)
        .await
        .unwrap();
    let result = read_upload_session(root.path(), &session_id, ttl()).await;
    assert!(matches!(result, Err(OciAdapterError::NotFound)));
}

#[tokio::test]
async fn purge_expired_upload_sessions_keeps_fresh_sessions() {
    let root = temp_root();
    let session_id = create_upload_session(root.path(), "repo", None, ttl(), max_sessions(), false)
        .await
        .unwrap();
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    purge_expired_upload_sessions(root.path(), ttl(), now)
        .await
        .unwrap();
    let result = read_upload_session(root.path(), &session_id, ttl()).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn purge_expired_upload_sessions_handles_missing_upload_dir() {
    let root = temp_root();
    // No upload dir created yet - should not error
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let result = purge_expired_upload_sessions(root.path(), ttl(), now).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn upload_body_integrity_matches_expected_sha256() {
    let root = temp_root();
    let session_id = create_upload_session(root.path(), "repo", None, ttl(), max_sessions(), false)
        .await
        .unwrap();
    let data = b"hello world";
    append_upload_bytes(root.path(), &session_id, data)
        .await
        .unwrap();
    let (sha256_hex, integrity) = upload_body_integrity(root.path(), &session_id)
        .await
        .unwrap();
    // SHA-256 of "hello world"
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
    // last_touched + ttl = 1060; now = 1050 -> not expired
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
    // last_touched + ttl = 1060; now = 1060 -> expired (<=)
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
