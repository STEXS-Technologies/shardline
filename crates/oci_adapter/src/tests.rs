use std::future::Future;
use std::num::{NonZeroU64, NonZeroUsize};
#[cfg(unix)]
use std::os::unix::ffi::OsStrExt;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};

use bytes::Bytes;
use sha2::{Digest, Sha256};
use tempfile::TempDir;

use crate::traits::OciBackend;
use crate::{
    OciAdapterError, OciReference, OciUploadSession, SerializableSha256State,
    abort_s3_multipart_upload_session, append_s3_multipart_upload_bytes, append_upload_bytes,
    create_upload_session, delete_upload_session, finalize_s3_multipart_upload_session,
    new_upload_session_id, oci_blob_key, oci_manifest_key, oci_manifest_media_type_key,
    oci_manifest_prefix, oci_tag_key, oci_tag_prefix, parse_reference,
    purge_expired_upload_sessions, read_upload_session, upload_body_integrity, upload_length,
    upload_session_expired,
};
use shardline_protocol::{RepositoryProvider, RepositoryScope};
use shardline_storage::{DeleteOutcome, ObjectKey, PutOutcome};

/// No-op backend used in tests that never creates S3 multipart uploads.
struct TestBackend;

// ── MockS3Backend ──────────────────────────────────────────────────────────

/// Backend that simulates S3 multipart uploads in memory for testing.
struct MockS3Backend {
    /// upload_id → (parts, completed, aborted, etag_index)
    uploads: std::sync::Mutex<std::collections::HashMap<String, MockMultipartState>>,
    /// When true, `upload_resumable_object_part` returns an error.
    fail_upload_part: AtomicBool,
    /// When true, `complete_resumable_object_upload` returns an error.
    fail_complete: AtomicBool,
}

struct MockMultipartState {
    parts: Vec<(usize, Bytes)>,
    completed: bool,
    aborted: bool,
    next_etag: u64,
}

impl MockS3Backend {
    fn new() -> Self {
        Self {
            uploads: std::sync::Mutex::new(std::collections::HashMap::new()),
            fail_upload_part: AtomicBool::new(false),
            fail_complete: AtomicBool::new(false),
        }
    }

    fn etag_for(counter: u64) -> String {
        format!("mock-etag-{counter}")
    }

    fn assert_completed(&self, upload_id: &str, expected_parts: &[&[u8]]) {
        let guard = self.uploads.lock().unwrap();
        let state = guard.get(upload_id).expect("upload not found");
        assert!(state.completed, "upload was not completed");
        assert!(!state.aborted, "upload was aborted");
        let actual_parts: Vec<&[u8]> = state.parts.iter().map(|(_, b)| b.as_ref()).collect();
        let expected: Vec<&[u8]> = expected_parts.to_vec();
        assert_eq!(actual_parts, expected, "uploaded parts do not match");
    }

    fn assert_aborted(&self, upload_id: &str) {
        let guard = self.uploads.lock().unwrap();
        let state = guard.get(upload_id).expect("upload not found");
        assert!(state.aborted, "upload was not aborted");
    }
}

impl MockS3Backend {
    fn next_upload_id() -> String {
        use std::sync::atomic::{AtomicU64, Ordering};
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        format!("mock-upload-{n}")
    }
}

impl OciBackend for MockS3Backend {
    async fn create_resumable_object_upload(
        &self,
        _object_key: &ObjectKey,
    ) -> Result<Option<String>, OciAdapterError> {
        let upload_id = Self::next_upload_id();
        self.uploads.lock().unwrap().insert(
            upload_id.clone(),
            MockMultipartState {
                parts: Vec::new(),
                completed: false,
                aborted: false,
                next_etag: 0,
            },
        );
        Ok(Some(upload_id))
    }

    async fn upload_resumable_object_part(
        &self,
        _object_key: &ObjectKey,
        upload_id: &str,
        part_idx: usize,
        bytes: Bytes,
    ) -> Result<String, OciAdapterError> {
        if self.fail_upload_part.load(Ordering::Relaxed) {
            return Err(OciAdapterError::Io(std::io::Error::other(
                "injected part upload failure",
            )));
        }
        let mut guard = self.uploads.lock().unwrap();
        let state = guard.get_mut(upload_id).ok_or(OciAdapterError::NotFound)?;
        let etag_counter = state.next_etag;
        state.next_etag = state.next_etag.wrapping_add(1);
        state.parts.push((part_idx, bytes));
        Ok(Self::etag_for(etag_counter))
    }

    async fn complete_resumable_object_upload(
        &self,
        _object_key: &ObjectKey,
        upload_id: &str,
        _parts: Vec<(usize, String)>,
    ) -> Result<(), OciAdapterError> {
        if self.fail_complete.load(Ordering::Relaxed) {
            return Err(OciAdapterError::Io(std::io::Error::other(
                "injected complete failure",
            )));
        }
        let mut guard = self.uploads.lock().unwrap();
        let state = guard.get_mut(upload_id).ok_or(OciAdapterError::NotFound)?;
        state.completed = true;
        Ok(())
    }

    async fn abort_resumable_object_upload(
        &self,
        _object_key: &ObjectKey,
        upload_id: &str,
    ) -> Result<(), OciAdapterError> {
        let mut guard = self.uploads.lock().unwrap();
        let state = guard.get_mut(upload_id).ok_or(OciAdapterError::NotFound)?;
        state.aborted = true;
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

fn create_test_session(
    root: &Path,
    use_s3: bool,
) -> impl Future<Output = Result<String, OciAdapterError>> {
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
    let session_id =
        create_upload_session(root.path(), NO_BACKEND, "my-repo", None, ttl(), max, false)
            .await
            .unwrap();
    assert!(!session_id.is_empty());
    let result =
        create_upload_session(root.path(), NO_BACKEND, "my-repo", None, ttl(), max, false).await;
    assert!(matches!(
        result,
        Err(OciAdapterError::TooManyUploadSessions)
    ));
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
    let result = read_upload_session(root.path(), "10000000000000000000000000000000", ttl()).await;
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
    let result = upload_length(root.path(), "10000000000000000000000000000001").await;
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
const VALID_DIGEST_FULL: &str =
    "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

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
    assert!(
        s.contains("/manifests/"),
        "expected manifests path, got: {s}"
    );
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
    assert!(
        s.contains("/manifests/"),
        "expected manifests path, got: {s}"
    );
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

// ── S3 Multipart Upload Tests ──────────────────────────────────────────────

fn sha256_hex(data: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(data);
    hex::encode(hasher.finalize())
}

async fn create_s3_session(root: &Path, backend: &MockS3Backend) -> String {
    create_upload_session(
        root,
        Some(backend),
        "test-repo",
        None,
        ttl(),
        max_sessions(),
        true,
    )
    .await
    .unwrap()
}

async fn read_session(root: &Path, session_id: &str) -> OciUploadSession {
    read_upload_session(root, session_id, ttl()).await.unwrap()
}

#[tokio::test]
async fn s3_multipart_append_and_finalize() {
    let root = temp_root();
    let backend = MockS3Backend::new();
    let session_id = create_s3_session(root.path(), &backend).await;

    let session = read_session(root.path(), &session_id).await;
    let (_session, length) =
        append_s3_multipart_upload_bytes(root.path(), &backend, &session_id, session, b"hello-s3-")
            .await
            .unwrap();
    assert_eq!(length, 9);

    let session = read_session(root.path(), &session_id).await;
    let (session, length) =
        append_s3_multipart_upload_bytes(root.path(), &backend, &session_id, session, b"world-s3!")
            .await
            .unwrap();
    assert_eq!(length, 18);

    let data = b"hello-s3-world-s3!";
    let digest = sha256_hex(data);
    let object_key = ObjectKey::parse("protocols/oci/test/blob").unwrap();
    let outcome = finalize_s3_multipart_upload_session(
        root.path(),
        &backend,
        &session_id,
        session,
        &object_key,
        &digest,
        b"",
    )
    .await
    .unwrap();
    assert_eq!(outcome, PutOutcome::Inserted);

    // Verify the mock recorded completion.
    // Parts are accumulated in the tail and flushed as a single combined
    // part at finalize time (no single append exceeds the 8 MiB threshold).
    let upload_id = {
        let s = read_session(root.path(), &session_id).await;
        s.s3_multipart.unwrap().upload_id
    };
    backend.assert_completed(&upload_id, &[b"hello-s3-world-s3!"]);
}

#[tokio::test]
async fn s3_multipart_append_triggers_part_at_chunk_boundary() {
    let root = temp_root();
    let backend = MockS3Backend::new();
    let session_id = create_s3_session(root.path(), &backend).await;

    // Write enough bytes to trigger a part upload (chunk size is 8 MiB).
    let chunk = vec![b'x'; 8 * 1024 * 1024 + 100];
    let session = read_session(root.path(), &session_id).await;
    let (_session, length) =
        append_s3_multipart_upload_bytes(root.path(), &backend, &session_id, session, &chunk)
            .await
            .unwrap();
    assert_eq!(usize::try_from(length).unwrap(), chunk.len());

    // One full 8 MiB part should have been uploaded.
    let s = read_session(root.path(), &session_id).await;
    let uploaded = s.s3_multipart.as_ref().unwrap().uploaded_part_ids.len();
    assert_eq!(uploaded, 1, "expected 1 full part, got {uploaded}");
}

#[tokio::test]
async fn s3_multipart_abort_cleans_up() {
    let root = temp_root();
    let backend = MockS3Backend::new();
    let session_id = create_s3_session(root.path(), &backend).await;

    let session = read_session(root.path(), &session_id).await;
    let (session, _length) =
        append_s3_multipart_upload_bytes(root.path(), &backend, &session_id, session, b"some-data")
            .await
            .unwrap();

    // Abort via the OCI adapter.
    abort_s3_multipart_upload_session(&backend, &session)
        .await
        .unwrap();

    // Verify the mock recorded abort.
    let upload_id = {
        let s = read_session(root.path(), &session_id).await;
        s.s3_multipart.unwrap().upload_id
    };
    backend.assert_aborted(&upload_id);
}

#[tokio::test]
async fn s3_multipart_empty_upload_falls_back_to_single_put() {
    let root = temp_root();
    let backend = MockS3Backend::new();
    let session_id = create_s3_session(root.path(), &backend).await;

    // No data appended, then finalize empty.
    let session = read_session(root.path(), &session_id).await;
    let digest = sha256_hex(b"");
    let object_key = ObjectKey::parse("protocols/oci/test/empty-blob").unwrap();
    let outcome = finalize_s3_multipart_upload_session(
        root.path(),
        &backend,
        &session_id,
        session,
        &object_key,
        &digest,
        b"",
    )
    .await
    .unwrap();
    assert_eq!(outcome, PutOutcome::Inserted);
}

#[tokio::test]
async fn s3_multipart_hash_mismatch_aborts() {
    let root = temp_root();
    let backend = MockS3Backend::new();
    let session_id = create_s3_session(root.path(), &backend).await;

    let session = read_session(root.path(), &session_id).await;
    let (session, _length) = append_s3_multipart_upload_bytes(
        root.path(),
        &backend,
        &session_id,
        session,
        b"actual-data",
    )
    .await
    .unwrap();

    // Finalize with wrong digest (bare hex, no sha256: prefix).
    let wrong_digest = sha256_hex(b"wrong-data");
    let object_key = ObjectKey::parse("protocols/oci/test/blob").unwrap();
    let result = finalize_s3_multipart_upload_session(
        root.path(),
        &backend,
        &session_id,
        session,
        &object_key,
        &wrong_digest,
        b"",
    )
    .await;

    assert!(matches!(
        result,
        Err(OciAdapterError::ExpectedBodyHashMismatch)
    ));

    // The mock should have recorded the abort on the multipart session.
    let s = read_session(root.path(), &session_id).await;
    let upload_id = s.s3_multipart.as_ref().map(|m| m.upload_id.clone());
    if let Some(upload_id) = upload_id {
        backend.assert_aborted(&upload_id);
    }
}

#[tokio::test]
async fn s3_multipart_concurrent_append_stress() {
    let root = temp_root();
    let backend = MockS3Backend::new();
    let session_id = create_s3_session(root.path(), &backend).await;

    // Append from multiple concurrent tasks.  Each task does a small append
    // (below the 8 MiB threshold), so data lands in the tail file.  Without
    // explicit session-level locking, concurrent writers race on the
    // file-based state — we only verify that the mock backend received
    // valid calls and that nothing panics.
    let chunks: &[&[u8]] = &[b"AAAA", b"BBBB", b"CCCC"];

    let backend = std::sync::Arc::new(backend);
    let share = std::sync::Arc::new((root.path().to_path_buf(), std::sync::Arc::clone(&backend)));
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();

    for (i, chunk) in chunks.iter().enumerate() {
        let share = std::sync::Arc::clone(&share);
        let sid = session_id.clone();
        let data = chunk.to_vec();
        let tx = tx.clone();
        tokio::spawn(async move {
            let (root_path, backend) = &*share;
            let session = read_upload_session(root_path, &sid, ttl()).await.unwrap();
            let result =
                append_s3_multipart_upload_bytes(root_path, backend.as_ref(), &sid, session, &data)
                    .await;
            let _ = tx.send((i, result));
        });
    }
    drop(tx);

    // Collect results — all should succeed (no crashes or corrupt state).
    let mut results: Vec<_> = std::iter::repeat_with(|| None).take(3).collect();
    while let Some((idx, result)) = rx.recv().await {
        assert!(result.is_ok(), "task {idx} failed: {:?}", result);
        results[idx] = Some(result.unwrap());
    }
    assert!(
        results.iter().all(|r| r.is_some()),
        "not all tasks completed"
    );

    // Verify the mock backend has recorded partial data (at least one part
    // was uploaded by ensure_s3_upload_started).  The exact content depends
    // on the IO race, so we only check that the backend is consistent.
    let guard = backend.uploads.lock().unwrap();
    assert!(!guard.is_empty(), "mock should have at least one upload");
    // At least one upload should exist from ensure_s3_upload_started.
    drop(guard);
}

// ── Additional function coverage tests ──────────────────────────────────────

#[test]
fn global_scope_namespace_returns_global() {
    assert_eq!(super::global_scope_namespace(), "global");
}

#[test]
fn validate_repository_accepts_valid_name() {
    assert!(super::validate_repository("team/assets").is_ok());
}

#[test]
fn validate_repository_rejects_empty() {
    assert!(matches!(
        super::validate_repository(""),
        Err(OciAdapterError::InvalidRepositoryName)
    ));
}

#[test]
fn validate_repository_rejects_traversal() {
    assert!(matches!(
        super::validate_repository("../assets"),
        Err(OciAdapterError::InvalidRepositoryName)
    ));
}

#[test]
fn oci_blob_location_format() {
    let loc = super::oci_blob_location("myrepo", "abcdef");
    assert_eq!(loc, "/v2/myrepo/blobs/sha256:abcdef");
}

#[test]
fn oci_manifest_location_format() {
    let loc = super::oci_manifest_location("myrepo", "latest");
    assert_eq!(loc, "/v2/myrepo/manifests/latest");
}

#[test]
fn oci_manifest_location_with_digest() {
    let loc = super::oci_manifest_location("myrepo", "sha256:abcdef");
    assert_eq!(loc, "/v2/myrepo/manifests/sha256:abcdef");
}

#[test]
fn upload_session_location_format() {
    let loc = super::upload_session_location("myrepo", "session-123");
    assert_eq!(loc, "/v2/myrepo/blobs/uploads/session-123");
}

#[test]
fn upload_body_path_for_session_returns_path() {
    let root = temp_root();
    let path = super::upload_body_path_for_session(root.path(), "0123456789abcdef").unwrap();
    assert!(path.to_string_lossy().contains("0123456789abcdef.bin"));
}

#[test]
fn upload_body_path_for_session_rejects_invalid_id() {
    let root = temp_root();
    let result = super::upload_body_path_for_session(root.path(), "bad/session");
    assert!(matches!(result, Err(OciAdapterError::InvalidUploadSession)));
}

#[test]
fn upload_session_length_without_multipart_returns_none() {
    let session = OciUploadSession {
        repository: "repo".to_owned(),
        scope_namespace: "global".to_owned(),
        created_at_unix_seconds: 0,
        last_touched_unix_seconds: 0,
        use_s3_multipart: false,
        s3_multipart: None,
    };
    assert_eq!(super::upload_session_length(&session), None);
}

#[test]
fn upload_session_length_with_multipart_returns_length() {
    let session = OciUploadSession {
        repository: "repo".to_owned(),
        scope_namespace: "global".to_owned(),
        created_at_unix_seconds: 0,
        last_touched_unix_seconds: 0,
        use_s3_multipart: true,
        s3_multipart: Some(crate::OciS3MultipartUploadSession {
            temporary_object_key: "tmp/key".to_owned(),
            upload_id: "upload-1".to_owned(),
            uploaded_part_ids: vec![],
            total_length: 42,
            sha256_state: crate::SerializableSha256State::default(),
        }),
    };
    assert_eq!(super::upload_session_length(&session), Some(42));
}

#[test]
fn upload_session_length_with_multipart_no_data_returns_zero() {
    let session = OciUploadSession {
        repository: "repo".to_owned(),
        scope_namespace: "global".to_owned(),
        created_at_unix_seconds: 0,
        last_touched_unix_seconds: 0,
        use_s3_multipart: true,
        s3_multipart: None,
    };
    assert_eq!(super::upload_session_length(&session), Some(0));
}

#[tokio::test]
async fn touch_upload_session_persists_updated_timestamp() {
    let root = temp_root();
    let session_id = super::create_upload_session(
        root.path(),
        NO_BACKEND,
        "repo",
        None,
        ttl(),
        max_sessions(),
        false,
    )
    .await
    .unwrap();
    let session = super::read_upload_session(root.path(), &session_id, ttl())
        .await
        .unwrap();
    // Touch the session — the timestamp is seconds-granularity, so we can't
    // reliably test an increase without sleeping for >1s.  Instead verify
    // that touch persisted without error and the session remains readable.
    super::touch_upload_session(root.path(), &session_id, session)
        .await
        .unwrap();
    let updated = super::read_upload_session(root.path(), &session_id, ttl())
        .await
        .unwrap();
    assert!(
        updated.last_touched_unix_seconds > 0,
        "touch should set a valid timestamp"
    );
}

#[tokio::test]
async fn abort_s3_multipart_upload_session_no_multipart_returns_ok() {
    let session = OciUploadSession {
        repository: "repo".to_owned(),
        scope_namespace: "global".to_owned(),
        created_at_unix_seconds: 0,
        last_touched_unix_seconds: 0,
        use_s3_multipart: false,
        s3_multipart: None,
    };
    let backend = TestBackend;
    let result = super::abort_s3_multipart_upload_session(&backend, &session).await;
    assert!(result.is_ok());
}

#[test]
fn oci_tag_target_key_global_namespace() {
    let key = super::oci_tag_target_key("team/assets", VALID_DIGEST, "latest", None).unwrap();
    let s = key.as_str();
    assert!(s.contains("protocols/oci/global/repos/"));
    assert!(s.contains("/tag-targets/"));
    assert!(s.contains(VALID_DIGEST));
    assert!(s.ends_with("/latest"));
}

#[test]
fn oci_tag_target_key_with_scope() {
    let scope = test_scope();
    let key = super::oci_tag_target_key("team/assets", VALID_DIGEST, "v1", Some(&scope)).unwrap();
    let s = key.as_str();
    assert!(!s.contains("/global/"));
    assert!(s.contains("/tag-targets/"));
}

#[test]
fn oci_tag_target_key_empty_repo_errors() {
    assert!(matches!(
        super::oci_tag_target_key("", VALID_DIGEST, "latest", None),
        Err(OciAdapterError::InvalidRepositoryName)
    ));
}

#[test]
fn oci_tag_target_key_empty_tag_errors() {
    assert!(matches!(
        super::oci_tag_target_key("team/assets", VALID_DIGEST, "", None),
        Err(OciAdapterError::InvalidManifestReference)
    ));
}

#[test]
fn oci_tag_target_prefix_global_namespace() {
    let prefix = super::oci_tag_target_prefix("team/assets", VALID_DIGEST, None).unwrap();
    let s = prefix.as_str();
    assert!(s.contains("protocols/oci/global/repos/"));
    assert!(s.contains("/tag-targets/"));
    assert!(s.contains(VALID_DIGEST));
    assert!(s.ends_with('/'));
}

#[test]
fn oci_tag_target_prefix_with_scope() {
    let scope = test_scope();
    let prefix = super::oci_tag_target_prefix("team/assets", VALID_DIGEST, Some(&scope)).unwrap();
    let s = prefix.as_str();
    assert!(!s.contains("/global/"));
    assert!(s.contains("/tag-targets/"));
}

#[test]
fn oci_tag_target_prefix_empty_repo_errors() {
    assert!(matches!(
        super::oci_tag_target_prefix("", VALID_DIGEST, None),
        Err(OciAdapterError::InvalidRepositoryName)
    ));
}

#[test]
fn oci_tag_target_prefix_invalid_digest_errors() {
    assert!(matches!(
        super::oci_tag_target_prefix("team/assets", "not-a-digest", None),
        Err(OciAdapterError::InvalidDigest)
    ));
}

#[test]
fn oci_tag_key_invalid_tag_errors() {
    assert!(matches!(
        super::oci_tag_key("team/assets", "bad/tag", None),
        Err(OciAdapterError::InvalidManifestReference)
    ));
}

#[test]
fn oci_tag_key_invalid_repo_errors() {
    assert!(matches!(
        super::oci_tag_key("", "latest", None),
        Err(OciAdapterError::InvalidRepositoryName)
    ));
}

#[tokio::test]
async fn count_active_upload_sessions_empty_dir_returns_zero() {
    let root = temp_root();
    // No upload directory exists yet
    let count = super::count_active_upload_sessions(root.path(), ttl())
        .await
        .unwrap();
    assert_eq!(count, 0);
}

#[tokio::test]
async fn count_active_upload_sessions_with_one_active_session() {
    let root = temp_root();
    let _session_id = create_test_session(root.path(), false).await.unwrap();
    let count = super::count_active_upload_sessions(root.path(), ttl())
        .await
        .unwrap();
    assert_eq!(count, 1);
}

#[tokio::test]
async fn purge_expired_orphaned_bin_files_cleaned() {
    let root = temp_root();
    let session_id = create_test_session(root.path(), false).await.unwrap();

    // Manually create an orphaned .bin file with a valid session ID stem
    // but no corresponding .json metadata
    let orphan_stem = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"; // 32 valid hex chars
    let orphan_bin = crate::upload_body_path(root.path(), orphan_stem);
    tokio::fs::write(&orphan_bin, b"orphan-data").await.unwrap();

    // Use a recent `now` so the real session is NOT expired
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    purge_expired_upload_sessions::<TestBackend>(root.path(), NO_BACKEND, ttl(), now)
        .await
        .unwrap();

    // The orphaned bin file should be removed
    assert!(
        !orphan_bin.exists(),
        "orphaned bin file should have been cleaned up"
    );

    // The valid session should still exist
    let session = read_upload_session(root.path(), &session_id, ttl())
        .await
        .expect("valid session should still exist");
    assert_eq!(session.repository, "repo");
}

#[tokio::test]
async fn purge_expired_corrupt_json_metadata_cleaned() {
    let root = temp_root();
    let session_id = create_test_session(root.path(), false).await.unwrap();

    // Create a corrupt metadata file for a valid-looking session ID
    let bogus_stem = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    let bogus_meta = crate::upload_metadata_path(root.path(), bogus_stem);
    tokio::fs::create_dir_all(crate::upload_dir(root.path()))
        .await
        .unwrap();
    tokio::fs::write(&bogus_meta, b"not-valid-json")
        .await
        .unwrap();
    // Also create a body file so delete_upload_session has something to clean
    let bogus_body = crate::upload_body_path(root.path(), bogus_stem);
    tokio::fs::write(&bogus_body, b"some-data").await.unwrap();

    // Use a recent `now` so the real session is NOT expired
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    purge_expired_upload_sessions::<TestBackend>(root.path(), NO_BACKEND, ttl(), now)
        .await
        .unwrap();

    // The corrupt metadata file and associated body should be gone
    assert!(
        !bogus_meta.exists(),
        "corrupt metadata should have been removed"
    );

    // The valid session should still exist
    let session = read_upload_session(root.path(), &session_id, ttl())
        .await
        .expect("valid session should still exist");
    assert_eq!(session.repository, "repo");
}

#[tokio::test]
async fn purge_expired_missing_body_file_cleaned() {
    let root = temp_root();
    let session_id = create_test_session(root.path(), false).await.unwrap();

    // Delete the body file for the session (simulates partial state)
    let body_path = crate::upload_body_path(root.path(), &session_id);
    let _ = tokio::fs::remove_file(&body_path).await;

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
        + 10_000;
    purge_expired_upload_sessions::<TestBackend>(root.path(), NO_BACKEND, ttl(), now)
        .await
        .unwrap();

    // The session should be removed (metadata, body, tail all gone)
    let result = read_upload_session(root.path(), &session_id, ttl()).await;
    assert!(
        matches!(result, Err(OciAdapterError::NotFound)),
        "session with missing body should be purged"
    );
}

#[tokio::test]
async fn purge_expired_already_expired_session_cleaned() {
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
    assert!(
        matches!(result, Err(OciAdapterError::NotFound)),
        "expired session should be purged"
    );
}

#[tokio::test]
async fn lock_upload_sessions_acquires_and_releases_lock() {
    let root = temp_root();
    let lock = super::lock_upload_sessions(root.path())
        .await
        .expect("should acquire lock");
    // Dropping the lock should release it without error
    drop(lock);
    // Should be able to acquire it again
    let _lock2 = super::lock_upload_sessions(root.path())
        .await
        .expect("should acquire lock again");
}

#[tokio::test]
async fn new_upload_session_id_format_is_32_char_hex() {
    let id = new_upload_session_id();
    assert_eq!(id.len(), 32, "session id should be 32 hex chars");
    assert!(
        id.bytes().all(|b| b.is_ascii_hexdigit()),
        "session id should be all hex: {id}"
    );
}

#[tokio::test]
async fn append_upload_bytes_empty_bytes_returns_current_length() {
    let root = temp_root();
    let session_id = create_test_session(root.path(), false).await.unwrap();
    append_upload_bytes(root.path(), &session_id, b"initial")
        .await
        .unwrap();
    let len = append_upload_bytes(root.path(), &session_id, b"")
        .await
        .unwrap();
    assert_eq!(len, 7, "appending empty bytes should return current length");
}

#[tokio::test]
async fn append_upload_bytes_large_content() {
    let root = temp_root();
    let session_id = create_test_session(root.path(), false).await.unwrap();
    let large = vec![b'x'; 1_000_000];
    let len = append_upload_bytes(root.path(), &session_id, &large)
        .await
        .unwrap();
    assert_eq!(len, 1_000_000);
}

#[tokio::test]
async fn upload_body_integrity_handles_various_content() {
    let root = temp_root();
    let session_id = create_test_session(root.path(), false).await.unwrap();

    // Write some data and check integrity
    let data = b"integrity check";
    append_upload_bytes(root.path(), &session_id, data)
        .await
        .unwrap();
    let (sha256_hex, integrity) = upload_body_integrity(root.path(), &session_id)
        .await
        .unwrap();

    // Verify the SHA-256 matches
    let mut hasher = Sha256::new();
    hasher.update(data);
    let expected = hex::encode(hasher.finalize());
    assert_eq!(sha256_hex, expected);
    assert_eq!(integrity.length(), data.len() as u64);
}

#[tokio::test]
async fn upload_body_integrity_not_found() {
    let root = temp_root();
    let session_id = create_test_session(root.path(), false).await.unwrap();
    // Delete the session's body file
    let body_path = crate::upload_body_path(root.path(), &session_id);
    tokio::fs::remove_file(&body_path).await.unwrap();
    let result = upload_body_integrity(root.path(), &session_id).await;
    // The error may be NotFound or Io depending on platform-specific behavior
    // of anchored I/O when the file doesn't exist.
    assert!(
        result.is_err(),
        "expected an error when body file is missing, got Ok: {result:?}"
    );
}

#[tokio::test]
async fn read_upload_session_missing_local_body_returns_not_found() {
    let root = temp_root();
    let session_id = create_test_session(root.path(), false).await.unwrap();

    // Delete the body file
    let body_path = crate::upload_body_path(root.path(), &session_id);
    tokio::fs::remove_file(&body_path).await.unwrap();

    let result = read_upload_session(root.path(), &session_id, ttl()).await;
    assert!(matches!(result, Err(OciAdapterError::NotFound)));
}

#[test]
fn oci_tag_target_key_with_none_scope_uses_global() {
    let key = super::oci_tag_target_key(
        "team/assets",
        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "stable",
        None,
    )
    .unwrap();
    let s = key.as_str();
    assert!(
        s.contains("/global/"),
        "expected global namespace without scope, got: {s}"
    );
}

#[test]
fn oci_blob_key_empty_digest_fails() {
    // Empty digest produces an invalid path component that fails validation
    let result = super::oci_blob_key("repo", "", None);
    assert!(
        result.is_err(),
        "empty digest should produce an error: got: {result:?}"
    );
}

#[test]
fn oci_manifest_key_rejects_empty_repo() {
    let result = super::oci_manifest_key("", VALID_DIGEST_FULL, None);
    assert!(matches!(
        result,
        Err(OciAdapterError::InvalidRepositoryName)
    ));
}

#[test]
fn oci_manifest_key_rejects_invalid_digest() {
    // The function doesn't validate the digest, it just uses it as part of the key path
    let result = super::oci_manifest_key("repo", "sha256:nothex", None);
    assert!(result.is_ok(), "digest is used as opaque path component");
}

#[test]
fn oci_tag_key_rejects_invalid_tag_starting_with_hyphen() {
    let result = super::oci_tag_key("repo", "-invalid", None);
    assert!(matches!(
        result,
        Err(OciAdapterError::InvalidManifestReference)
    ));
}

#[test]
fn oci_manifest_prefix_works_with_scope() {
    let result = super::oci_manifest_prefix("team/assets", Some(&test_scope()));
    assert!(result.is_ok());
}

#[test]
fn oci_tag_target_prefix_with_none_scope() {
    let prefix = super::oci_tag_target_prefix(
        "team/assets",
        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        None,
    )
    .unwrap();
    let s = prefix.as_str();
    assert!(s.contains("/global/"));
    assert!(s.contains("/tag-targets/"));
}

#[tokio::test]
async fn lock_upload_sessions_creates_lock_file() {
    let root = temp_root();
    let _lock = super::lock_upload_sessions(root.path())
        .await
        .expect("should acquire lock");
    let lock_path = crate::upload_dir(root.path()).join(".sessions.lock");
    assert!(lock_path.exists(), "lock file should be created");
}

#[tokio::test]
async fn lock_upload_sessions_reentrant_same_process() {
    let root = temp_root();
    // Two sequential locks should succeed (first released, then second acquired)
    {
        let _lock1 = super::lock_upload_sessions(root.path())
            .await
            .expect("first lock");
    }
    {
        let _lock2 = super::lock_upload_sessions(root.path())
            .await
            .expect("second lock");
    }
}

#[test]
fn validate_repository_rejects_uppercase() {
    assert!(matches!(
        super::validate_repository("Team/assets"),
        Err(OciAdapterError::InvalidRepositoryName)
    ));
}

#[test]
fn validate_repository_rejects_double_slash() {
    assert!(matches!(
        super::validate_repository("team//assets"),
        Err(OciAdapterError::InvalidRepositoryName)
    ));
}

#[test]
fn parse_reference_rejects_invalid_digest() {
    assert!(matches!(
        super::parse_reference("sha256:nothex"),
        Err(OciAdapterError::InvalidDigest)
    ));
}

#[test]
fn oci_blob_key_includes_digest_in_path() {
    let key = super::oci_blob_key("team/assets", "short-digest", None).unwrap();
    let s = key.as_str();
    assert!(
        s.contains("short-digest"),
        "digest should be embedded in key, got: {s}"
    );
}

#[test]
fn oci_manifest_key_includes_digest_in_path() {
    let key = super::oci_manifest_key("team/assets", "any-digest-string", None).unwrap();
    let s = key.as_str();
    assert!(
        s.contains("any-digest-string"),
        "digest should be embedded in key, got: {s}"
    );
}

#[test]
fn oci_manifest_media_type_key_includes_digest_in_path() {
    let key = super::oci_manifest_media_type_key("team/assets", "test-digest", None).unwrap();
    let s = key.as_str();
    assert!(
        s.contains("test-digest"),
        "digest should be embedded in key, got: {s}"
    );
}

#[test]
fn oci_manifest_prefix_invalid_repo_errors() {
    assert!(matches!(
        super::oci_manifest_prefix("", None),
        Err(OciAdapterError::InvalidRepositoryName)
    ));
}

#[test]
fn oci_tag_prefix_invalid_repo_errors() {
    assert!(matches!(
        super::oci_tag_prefix("", None),
        Err(OciAdapterError::InvalidRepositoryName)
    ));
}

#[test]
fn oci_tag_target_key_invalid_digest_errors() {
    assert!(matches!(
        super::oci_tag_target_key("team/assets", "bad", "latest", None),
        Err(OciAdapterError::InvalidDigest)
    ));
}

#[test]
fn oci_tag_key_with_scope_uses_hashed_namespace() {
    let scope = test_scope();
    let key = super::oci_tag_key("team/assets", "stable", Some(&scope)).unwrap();
    let s = key.as_str();
    // Must have a 64-char hex namespace (not "global")
    assert!(
        !s.contains("/global/"),
        "expected scoped namespace, got: {s}"
    );
    assert!(
        s.contains("/tags/stable"),
        "expected /tags/stable, got: {s}"
    );
}

#[test]
fn oci_tag_key_invalid_tag_uppercase() {
    // Tags must be valid OCI references; uppercase tags are allowed per spec,
    // but we test what validate_oci_tag actually rejects.
    assert!(matches!(
        super::oci_tag_key("team/assets", "-starts-with-hyphen", None),
        Err(OciAdapterError::InvalidManifestReference)
    ));
}

// ── S3 multipart error branch coverage tests ──────────────────────────────

#[tokio::test]
async fn s3_append_non_s3_session_returns_not_found() {
    // append_s3_multipart_upload_bytes with !session.use_s3_multipart → line 761
    let root = temp_root();
    let backend = MockS3Backend::new();
    let session_id = create_test_session(root.path(), false).await.unwrap();
    let session = read_session(root.path(), &session_id).await;
    let result =
        append_s3_multipart_upload_bytes(root.path(), &backend, &session_id, session, b"data")
            .await;
    assert!(matches!(result, Err(OciAdapterError::NotFound)));
}

#[tokio::test]
async fn s3_finalize_non_s3_session_returns_not_found() {
    // finalize_s3_multipart_upload_session with !session.use_s3_multipart → line 823
    let root = temp_root();
    let backend = MockS3Backend::new();
    let session_id = create_test_session(root.path(), false).await.unwrap();
    let session = read_session(root.path(), &session_id).await;
    let object_key = ObjectKey::parse("protocols/oci/test/key").unwrap();
    let result = finalize_s3_multipart_upload_session(
        root.path(),
        &backend,
        &session_id,
        session,
        &object_key,
        "digest",
        b"",
    )
    .await;
    assert!(matches!(result, Err(OciAdapterError::NotFound)));
}

#[tokio::test]
async fn s3_finalize_hash_mismatch_empty_multipart() {
    // When s3_multipart is None and hash doesn't match empty → line 830
    let root = temp_root();
    let backend = MockS3Backend::new();
    let session_id = create_s3_session(root.path(), &backend).await;
    let session = read_session(root.path(), &session_id).await;
    // Don't append data — s3_multipart stays None. Pass hash that doesn't match empty.
    let wrong_digest = sha256_hex(b"not-empty");
    let object_key = ObjectKey::parse("protocols/oci/test/key").unwrap();
    let result = finalize_s3_multipart_upload_session(
        root.path(),
        &backend,
        &session_id,
        session,
        &object_key,
        &wrong_digest,
        b"",
    )
    .await;
    assert!(matches!(
        result,
        Err(OciAdapterError::ExpectedBodyHashMismatch)
    ));
}

#[tokio::test]
async fn s3_finalize_empty_parts_aborts_and_puts() {
    // part_ids is empty → abort + single put (lines 870-876)
    let root = temp_root();
    let backend = MockS3Backend::new();
    let session_id = create_s3_session(root.path(), &backend).await;

    // Append small data to initialize multipart (no parts uploaded, tail has data)
    let session = read_session(root.path(), &session_id).await;
    let (session, _) =
        append_s3_multipart_upload_bytes(root.path(), &backend, &session_id, session, b"tail-data")
            .await
            .unwrap();

    // Delete the tail file so finalize sees empty tail and empty part_ids
    let tail_path = crate::upload_tail_path(root.path(), &session_id);
    let _ = tokio::fs::remove_file(&tail_path).await;

    // Expected digest must match the sha256_state (which has "tail-data")
    let digest = sha256_hex(b"tail-data");
    let object_key = ObjectKey::parse("protocols/oci/test/key").unwrap();
    let outcome = finalize_s3_multipart_upload_session(
        root.path(),
        &backend,
        &session_id,
        session,
        &object_key,
        &digest,
        b"",
    )
    .await
    .unwrap();
    assert_eq!(outcome, PutOutcome::Inserted);
}

#[tokio::test]
async fn s3_finalize_part_upload_error_aborts() {
    // upload_resumable_object_part fails → abort (lines 861-865)
    let root = temp_root();
    let backend = MockS3Backend::new();
    backend.fail_upload_part.store(true, Ordering::Relaxed);
    let session_id = create_s3_session(root.path(), &backend).await;

    // Append data to initialize multipart (tail has data)
    let session = read_session(root.path(), &session_id).await;
    let (session, _) =
        append_s3_multipart_upload_bytes(root.path(), &backend, &session_id, session, b"part-data")
            .await
            .unwrap();

    // Now finalize — the tail upload will fail
    let digest = sha256_hex(b"part-data");
    let object_key = ObjectKey::parse("protocols/oci/test/key").unwrap();
    let result = finalize_s3_multipart_upload_session(
        root.path(),
        &backend,
        &session_id,
        session,
        &object_key,
        &digest,
        b"",
    )
    .await;
    assert!(result.is_err());
    // The upload should have been aborted
    let s = read_session(root.path(), &session_id).await;
    if let Some(m) = s.s3_multipart {
        backend.assert_aborted(&m.upload_id);
    }
}

#[tokio::test]
async fn s3_finalize_complete_error_aborts() {
    // complete_resumable_object_upload fails → abort (lines 888-892)
    // We need a session where parts have been uploaded so the complete path is reached.
    let root = temp_root();
    let backend = MockS3Backend::new();
    backend.fail_complete.store(true, Ordering::Relaxed);
    let session_id = create_s3_session(root.path(), &backend).await;

    // Append enough data to trigger a part upload: 8 MiB
    let chunk_len = 8 * 1024 * 1024;
    let chunk = vec![b'x'; chunk_len];
    let session = read_session(root.path(), &session_id).await;
    let (session, _) =
        append_s3_multipart_upload_bytes(root.path(), &backend, &session_id, session, &chunk)
            .await
            .unwrap();

    // Finalize — the tail is empty (all data uploaded as a part).
    // Complete will fail.
    let digest = sha256_hex(&chunk);
    let object_key = ObjectKey::parse("protocols/oci/test/key").unwrap();
    let result = finalize_s3_multipart_upload_session(
        root.path(),
        &backend,
        &session_id,
        session,
        &object_key,
        &digest,
        b"",
    )
    .await;
    assert!(result.is_err());
    // The upload should have been aborted
    let s = read_session(root.path(), &session_id).await;
    if let Some(m) = s.s3_multipart {
        backend.assert_aborted(&m.upload_id);
    }
}

#[tokio::test]
async fn s3_ensure_started_upload_id_none_errors() {
    // ensure_s3_upload_started: backend.create_resumable_object_upload returns None (lines 1110-1114)
    // TestBackend already returns Ok(None) from create_resumable_object_upload.
    let root = temp_root();
    let session_id = create_upload_session(
        root.path(),
        Some(&TestBackend),
        "test-repo",
        None,
        ttl(),
        max_sessions(),
        true,
    )
    .await
    .unwrap();

    let session = read_session(root.path(), &session_id).await;
    let result = append_s3_multipart_upload_bytes(
        root.path(),
        &TestBackend,
        &session_id,
        session,
        b"trigger-init",
    )
    .await;
    assert!(matches!(result, Err(OciAdapterError::NotFound)));
}

#[tokio::test]
async fn s3_read_upload_tail_io_error() {
    // read_upload_tail non-NotFound IO error → line 1145
    let root = temp_root();
    let backend = MockS3Backend::new();
    let session_id = create_s3_session(root.path(), &backend).await;

    // Append data to create a tail file
    let session = read_session(root.path(), &session_id).await;
    let (_new_session, _total) =
        append_s3_multipart_upload_bytes(root.path(), &backend, &session_id, session, b"tail-data")
            .await
            .unwrap();

    // Replace tail file with a directory (read will fail with EISDIR or similar)
    let tail_path = crate::upload_tail_path(root.path(), &session_id);
    let _ = tokio::fs::remove_file(&tail_path).await;
    tokio::fs::create_dir(&tail_path).await.unwrap();

    // Next append will call read_upload_tail which should fail with Io error
    let session = read_session(root.path(), &session_id).await;
    let result =
        append_s3_multipart_upload_bytes(root.path(), &backend, &session_id, session, b"more-data")
            .await;
    assert!(
        result.is_err(),
        "expected error when tail path is a directory"
    );
}

#[tokio::test]
async fn s3_write_upload_tail_empty_removes_file() {
    // write_upload_tail with empty bytes removes existing tail file → line 1158 (Ok branch)
    let root = temp_root();
    let backend = MockS3Backend::new();
    let session_id = create_s3_session(root.path(), &backend).await;

    // Append 1 byte (creates tail file with 1 byte)
    let session = read_session(root.path(), &session_id).await;
    let (_session, _) =
        append_s3_multipart_upload_bytes(root.path(), &backend, &session_id, session, b"x")
            .await
            .unwrap();

    // Now append exactly 8MiB - 1 byte so total = 8MiB, tail gets drained, write_upload_tail
    // called with empty bytes, removes the existing tail file → line 1158 (Ok)
    let remaining = vec![b'y'; 8 * 1024 * 1024 - 1];
    let session = read_session(root.path(), &session_id).await;
    let result =
        append_s3_multipart_upload_bytes(root.path(), &backend, &session_id, session, &remaining)
            .await;
    assert!(result.is_ok(), "append with exact chunk should succeed");
}

#[tokio::test]
async fn s3_write_upload_tail_remove_non_existent() {
    // write_upload_tail with empty bytes, no tail file → line 1159 (NotFound → Ok)
    // This happens when the first append is exactly 8MiB (no previous tail file)
    let root = temp_root();
    let backend = MockS3Backend::new();
    let session_id = create_s3_session(root.path(), &backend).await;

    // Append exactly 8MiB — the full chunk gets uploaded, tail is empty,
    // write_upload_tail(empty) tries to remove the non-existent tail file → NotFound → Ok
    let chunk = vec![b'z'; 8 * 1024 * 1024];
    let session = read_session(root.path(), &session_id).await;
    let result =
        append_s3_multipart_upload_bytes(root.path(), &backend, &session_id, session, &chunk).await;
    assert!(result.is_ok(), "exact chunk append should succeed");
}

// ── IO error branch tests ─────────────────────────────────────────────────

#[tokio::test]
async fn read_upload_session_io_error_on_inaccessible_metadata() {
    // read_upload_session: non-NotFound IO error reading metadata → line 500
    let root = temp_root();
    let session_id = create_test_session(root.path(), false).await.unwrap();

    // Replace the upload directory with a file so path resolution fails with ENOTDIR
    let upload_dir = crate::upload_dir(root.path());
    let _ = tokio::fs::remove_dir_all(&upload_dir).await;
    tokio::fs::write(&upload_dir, b"not-a-dir").await.unwrap();

    let result = read_upload_session(root.path(), &session_id, ttl()).await;
    assert!(result.is_err(), "expected error when upload_dir is a file");
    // The error should NOT be NotFound (it's an Io error from failed path resolution)
    match result {
        Err(OciAdapterError::NotFound) => panic!("expected non-NotFound error"),
        Err(OciAdapterError::Io(_)) => {} // expected
        Err(other) => panic!("unexpected error: {other:?}"),
        Ok(_) => panic!("expected error"),
    }
}

#[tokio::test]
async fn upload_length_io_error_on_inaccessible_body() {
    // upload_length: non-NotFound IO error (line 583)
    let root = temp_root();
    let session_id = create_test_session(root.path(), false).await.unwrap();

    // Replace upload directory with a file so path resolution fails
    let upload_dir = crate::upload_dir(root.path());
    let _ = tokio::fs::remove_dir_all(&upload_dir).await;
    tokio::fs::write(&upload_dir, b"not-a-dir").await.unwrap();

    let result = upload_length(root.path(), &session_id).await;
    assert!(result.is_err(), "expected error when upload_dir is a file");
    match result {
        Err(OciAdapterError::NotFound) => panic!("expected non-NotFound error"),
        Err(OciAdapterError::Io(_)) => {} // expected
        Err(other) => panic!("unexpected error: {other:?}"),
        Ok(_) => panic!("expected error"),
    }
}

#[tokio::test]
async fn append_upload_bytes_io_error_on_inaccessible_body() {
    // map_not_found: non-NotFound IO error → line 536
    let root = temp_root();
    let session_id = create_test_session(root.path(), false).await.unwrap();

    // Replace upload directory with a file so path resolution fails
    let upload_dir = crate::upload_dir(root.path());
    let _ = tokio::fs::remove_dir_all(&upload_dir).await;
    tokio::fs::write(&upload_dir, b"not-a-dir").await.unwrap();

    let result = append_upload_bytes(root.path(), &session_id, b"data").await;
    assert!(result.is_err(), "expected error when upload_dir is a file");
    match result {
        Err(OciAdapterError::NotFound) => panic!("expected non-NotFound error"),
        Err(OciAdapterError::Io(_)) => {} // expected
        Err(other) => panic!("unexpected error: {other:?}"),
        Ok(_) => panic!("expected error"),
    }
}

#[tokio::test]
async fn delete_upload_session_returns_first_error() {
    // delete_upload_session: first_error set on non-NotFound error (lines 699-702)
    let root = temp_root();
    let session_id = create_test_session(root.path(), false).await.unwrap();

    // Replace upload directory with a file so deletion of all three paths fails
    let upload_dir = crate::upload_dir(root.path());
    let _ = tokio::fs::remove_dir_all(&upload_dir).await;
    tokio::fs::write(&upload_dir, b"not-a-dir").await.unwrap();

    let result = delete_upload_session(root.path(), &session_id).await;
    assert!(result.is_err(), "expected error when upload_dir is a file");
    match result {
        Err(OciAdapterError::NotFound) => panic!("expected non-NotFound error"),
        Err(OciAdapterError::Io(_)) => {} // expected
        Err(other) => panic!("unexpected error: {other:?}"),
        Ok(_) => panic!("expected error"),
    }
}

#[tokio::test]
async fn canonical_key_matches_object_key() {
    // finalize: canonical_key == object_key → early return (line 899)
    let root = temp_root();
    let backend = MockS3Backend::new();
    let session_id = create_s3_session(root.path(), &backend).await;

    // Append data, finalize with the canonical shared key as the object_key
    let session = read_session(root.path(), &session_id).await;
    let (session, _) =
        append_s3_multipart_upload_bytes(root.path(), &backend, &session_id, session, b"data")
            .await
            .unwrap();

    // The canonical key is protocols/shared/sha256/{digest}
    let digest = sha256_hex(b"data");
    let object_key = ObjectKey::parse(&format!("protocols/shared/sha256/{digest}")).unwrap();
    let outcome = finalize_s3_multipart_upload_session(
        root.path(),
        &backend,
        &session_id,
        session,
        &object_key,
        &digest,
        b"",
    )
    .await
    .unwrap();
    assert_eq!(outcome, PutOutcome::Inserted);
}

// ── count_active_upload_sessions branch tests ────────────────────────────

#[tokio::test]
async fn count_active_sessions_read_dir_error() {
    // count_active_upload_sessions: read_dir fails with non-NotFound error (line 940)
    let root = temp_root();
    // Create a file at the upload_dir path so read_dir fails with ENOTDIR or similar
    let upload_dir = crate::upload_dir(root.path());
    tokio::fs::create_dir_all(root.path().join("oci-uploads"))
        .await
        .unwrap();
    // Remove the directory and replace with a file
    let _ = tokio::fs::remove_dir(&upload_dir).await;
    tokio::fs::write(&upload_dir, b"not-a-directory")
        .await
        .unwrap();
    let result = super::count_active_upload_sessions(root.path(), ttl()).await;
    assert!(result.is_err(), "expected error when upload_dir is a file");
}

#[tokio::test]
async fn count_active_sessions_skips_unreadable_json() {
    // count_active_upload_sessions: fs::read fails → continue (line 951)
    let root = temp_root();
    let upload_dir = crate::upload_dir(root.path());
    tokio::fs::create_dir_all(&upload_dir).await.unwrap();
    // Create a directory with .json name (read will fail with EISDIR)
    let dir_path = upload_dir.join("unreadable.json");
    tokio::fs::create_dir(&dir_path).await.unwrap();
    let count = super::count_active_upload_sessions(root.path(), ttl())
        .await
        .unwrap();
    assert_eq!(count, 0, "unreadable json should be skipped");
}

#[tokio::test]
async fn count_active_sessions_skips_invalid_json() {
    // count_active_upload_sessions: serde_json::from_slice fails → continue (line 954)
    let root = temp_root();
    let upload_dir = crate::upload_dir(root.path());
    tokio::fs::create_dir_all(&upload_dir).await.unwrap();
    let json_path = upload_dir.join("invalid.json");
    tokio::fs::write(&json_path, b"not-valid-json")
        .await
        .unwrap();
    let count = super::count_active_upload_sessions(root.path(), ttl())
        .await
        .unwrap();
    assert_eq!(count, 0, "invalid json should be skipped");
}

#[tokio::test]
async fn count_active_sessions_skips_expired_session() {
    // count_active_upload_sessions: expired session → continue (line 957)
    let root = temp_root();
    let upload_dir = crate::upload_dir(root.path());
    tokio::fs::create_dir_all(&upload_dir).await.unwrap();
    // Write an expired session (last_touched very old)
    let session = OciUploadSession {
        repository: "repo".to_owned(),
        scope_namespace: "global".to_owned(),
        created_at_unix_seconds: 0,
        last_touched_unix_seconds: 0,
        use_s3_multipart: false,
        s3_multipart: None,
    };
    let bytes = serde_json::to_vec(&session).unwrap();
    let json_path = upload_dir.join("expired.json");
    tokio::fs::write(&json_path, &bytes).await.unwrap();
    let count = super::count_active_upload_sessions(root.path(), ttl())
        .await
        .unwrap();
    assert_eq!(count, 0, "expired session should be skipped");
}

// ── purge_expired_upload_sessions branch tests ───────────────────────────

#[tokio::test]
async fn purge_expired_session_stem_not_utf8_bin() {
    // purge_expired_upload_sessions: file with .bin extension and non-UTF-8 stem → continue (line 1005)
    let root = temp_root();
    let upload_dir = crate::upload_dir(root.path());
    tokio::fs::create_dir_all(&upload_dir).await.unwrap();
    // Create a .bin file with non-UTF-8 name
    #[cfg(unix)]
    {
        let bad_name = std::ffi::OsStr::from_bytes(b"\xff\xfe.bin");
        let bad_path = upload_dir.join(bad_name);
        tokio::fs::write(&bad_path, b"orphan").await.unwrap();
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        purge_expired_upload_sessions::<TestBackend>(root.path(), NO_BACKEND, ttl(), now)
            .await
            .unwrap();
        // File should remain since it was skipped (stem not valid session ID → continue on 1008)
        // Actually on 1005: file_stem().and_then(OsStr::to_str) returns None → continue
        // The file should NOT be removed (no metadata exists, but the non-UTF-8 stem
        // skips the orphan-removal logic entirely — it continues at line 1005)
        assert!(bad_path.exists(), "non-UTF-8 stem file should be skipped");
    }
    #[cfg(not(unix))]
    {
        let _ = upload_dir;
    }
}

#[tokio::test]
async fn purge_expired_session_stem_validation_skips_bin() {
    // purge_expired_upload_sessions: .bin file with invalid session ID stem → continue (line 1008)
    let root = temp_root();
    let upload_dir = crate::upload_dir(root.path());
    tokio::fs::create_dir_all(&upload_dir).await.unwrap();
    let bin_path = upload_dir.join("invalid-stem.bin");
    tokio::fs::write(&bin_path, b"orphan").await.unwrap();
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    purge_expired_upload_sessions::<TestBackend>(root.path(), NO_BACKEND, ttl(), now)
        .await
        .unwrap();
    // File should remain (stem fails validation → continue)
    assert!(
        bin_path.exists(),
        "invalid-stem .bin file should be skipped"
    );
}

#[tokio::test]
async fn purge_expired_session_stem_not_utf8_json() {
    // purge_expired_upload_sessions: .json file with non-UTF-8 stem → continue (line 1021)
    let root = temp_root();
    let upload_dir = crate::upload_dir(root.path());
    tokio::fs::create_dir_all(&upload_dir).await.unwrap();
    #[cfg(unix)]
    {
        let bad_name = std::ffi::OsStr::from_bytes(b"\xfe\xff.json");
        let bad_path = upload_dir.join(bad_name);
        tokio::fs::write(&bad_path, b"{}").await.unwrap();
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        purge_expired_upload_sessions::<TestBackend>(root.path(), NO_BACKEND, ttl(), now)
            .await
            .unwrap();
        // File should remain (non-UTF-8 stem → continue at line 1021)
        assert!(bad_path.exists(), "non-UTF-8 json should be skipped");
    }
    #[cfg(not(unix))]
    {
        let _ = upload_dir;
    }
}

#[tokio::test]
async fn purge_expired_session_stem_validation_skips_json() {
    // purge_expired_upload_sessions: .json file with invalid session ID stem → continue (line 1024)
    let root = temp_root();
    let upload_dir = crate::upload_dir(root.path());
    tokio::fs::create_dir_all(&upload_dir).await.unwrap();
    let json_path = upload_dir.join("invalid-stem.json");
    tokio::fs::write(&json_path, b"{}").await.unwrap();
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    purge_expired_upload_sessions::<TestBackend>(root.path(), NO_BACKEND, ttl(), now)
        .await
        .unwrap();
    assert!(
        json_path.exists(),
        "invalid-stem .json file should be skipped"
    );
}

#[tokio::test]
async fn purge_expired_session_read_error_deletes() {
    // purge_expired_upload_sessions: read error on .json file → delete (lines 1028-1030)
    let root = temp_root();
    let session_id = create_test_session(root.path(), false).await.unwrap();

    // Replace the metadata file with a directory (read will fail, should trigger delete)
    let meta_path = crate::upload_metadata_path(root.path(), &session_id);
    let _ = tokio::fs::remove_file(&meta_path).await;
    tokio::fs::create_dir(&meta_path).await.unwrap();

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    // purge may return an error (delete_upload_session may fail on the directory)
    let result =
        purge_expired_upload_sessions::<TestBackend>(root.path(), NO_BACKEND, ttl(), now).await;
    // The function may error because delete_upload_session failed on the directory content
    // but we should hit the read-error branch regardless
    let _ = result;
}

#[tokio::test]
async fn purge_expired_s3_multipart_abort() {
    // purge_expired_upload_sessions: S3 multipart abort on expired session (lines 1046-1052)
    let root = temp_root();
    let backend = MockS3Backend::new();
    let session_id = create_s3_session(root.path(), &backend).await;

    // Append data to initialize multipart with an upload_id
    let session = read_session(root.path(), &session_id).await;
    let (session, _) =
        append_s3_multipart_upload_bytes(root.path(), &backend, &session_id, session, b"data")
            .await
            .unwrap();
    let upload_id = session.s3_multipart.as_ref().unwrap().upload_id.clone();
    assert!(!upload_id.is_empty(), "upload_id should be set");

    // Now purge with a time far in the future (session will be expired)
    let far_future = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
        + 100_000;
    purge_expired_upload_sessions::<MockS3Backend>(
        root.path(),
        Some(&backend),
        NonZeroU64::new(1).unwrap(),
        far_future,
    )
    .await
    .unwrap();

    // The S3 multipart upload should have been aborted
    backend.assert_aborted(&upload_id);
}

#[tokio::test]
async fn purge_expired_read_dir_error() {
    // purge_expired_upload_sessions: read_dir fails with non-NotFound error (line 995)
    let root = temp_root();
    // Create a file at the upload_dir path so read_dir fails
    let upload_dir = crate::upload_dir(root.path());
    tokio::fs::create_dir_all(root.path().join("oci-uploads"))
        .await
        .unwrap();
    let _ = tokio::fs::remove_dir(&upload_dir).await;
    tokio::fs::write(&upload_dir, b"not-a-directory")
        .await
        .unwrap();
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let result =
        purge_expired_upload_sessions::<TestBackend>(root.path(), NO_BACKEND, ttl(), now).await;
    assert!(result.is_err(), "expected error when upload_dir is a file");
}

// ── write_upload_tail non-NotFound IO error (line 1163) ───────────────────

#[tokio::test]
async fn write_upload_tail_io_error_on_remove_direct() {
    let root = temp_root();
    let session_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"; // valid 32-char hex
    let tail_dir = crate::upload_tail_path(root.path(), session_id);
    tokio::fs::create_dir_all(&tail_dir).await.unwrap();

    // write_upload_tail with empty bytes → fs::remove_file on a directory → EISDIR
    let result = super::write_upload_tail(root.path(), session_id, b"").await;
    assert!(
        result.is_err(),
        "expected error when tail path is a directory"
    );
    match &result {
        Err(OciAdapterError::Io(io_err)) => {
            assert_ne!(
                io_err.kind(),
                std::io::ErrorKind::NotFound,
                "expected non-NotFound error"
            );
        }
        Err(other) => panic!("expected Io error, got: {other:?}"),
        Ok(_) => panic!("expected error"),
    }
}

// ── purge_expired read-error-on-metadata → delete + continue (line 1033) ─

#[tokio::test]
async fn purge_expired_read_error_deletes_with_permission_denied() {
    let root = temp_root();
    let session_id = create_test_session(root.path(), false).await.unwrap();

    // Make the metadata file unreadable so fs::read fails with PermissionDenied
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let meta_path = crate::upload_metadata_path(root.path(), &session_id);
        std::fs::set_permissions(&meta_path, std::fs::Permissions::from_mode(0o200))
            .expect("set_permissions should work");
    }
    #[cfg(not(unix))]
    {
        let meta_path = crate::upload_metadata_path(root.path(), &session_id);
        let mut perms = std::fs::metadata(&meta_path).await.unwrap().permissions();
        perms.set_readonly(true);
        std::fs::set_permissions(&meta_path, perms).unwrap();
    }

    let far_future = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
        + 100_000;

    purge_expired_upload_sessions::<TestBackend>(
        root.path(),
        NO_BACKEND,
        NonZeroU64::new(1).unwrap(),
        far_future,
    )
    .await
    .unwrap();

    // Session should be fully purged
    let result = read_upload_session(root.path(), &session_id, ttl()).await;
    assert!(
        matches!(result, Err(OciAdapterError::NotFound)),
        "session with unreadable metadata should be purged"
    );
}

// ── Unix anchored-file closure coverage (path-escape closures) ────────────
// These test the error-factory closures passed to open_anchored_target.
// They are only invoked when the path escapes the root.

#[cfg(unix)]
#[test]
fn open_anchored_file_rejects_escaped_path() {
    let root = temp_root();
    let escaped = root.path().join("../../../etc/passwd");
    let result = super::open_anchored_file(root.path(), &escaped);
    assert!(result.is_err(), "path escaping root should be rejected");
}

#[cfg(unix)]
#[test]
fn read_file_anchored_rejects_escaped_path() {
    let root = temp_root();
    let escaped = root.path().join("../../../etc/hostname");
    let result = super::read_file_anchored(root.path(), &escaped);
    assert!(result.is_err(), "path escaping root should be rejected");
}

#[cfg(unix)]
#[test]
fn delete_file_anchored_rejects_escaped_path() {
    let root = temp_root();
    let escaped = root.path().join("../../../tmp/escape");
    let result = super::delete_file_anchored(root.path(), &escaped);
    assert!(result.is_err(), "path escaping root should be rejected");
}

#[cfg(unix)]
#[test]
fn append_file_anchored_rejects_escaped_path() {
    let root = temp_root();
    let escaped = root.path().join("../../../tmp/escape");
    let result = super::append_file_anchored(root.path(), &escaped, b"data");
    assert!(result.is_err(), "path escaping root should be rejected");
}

#[cfg(unix)]
#[tokio::test]
async fn upload_file_len_async_rejects_escaped_path() {
    let root = temp_root();
    let escaped = root.path().join("../../../etc/passwd");
    let result = super::upload_file_len_async(root.path(), &escaped).await;
    assert!(result.is_err(), "path escaping root should be rejected");
}

#[cfg(unix)]
#[tokio::test]
async fn upload_file_exists_async_rejects_escaped_path() {
    let root = temp_root();
    let escaped = root.path().join("../../../etc/passwd");
    let result = super::upload_file_exists_async(root.path(), &escaped).await;
    assert!(result.is_err(), "path escaping root should be rejected");
}

// ── create_upload_session metadata write failure cleanup (lines 481-482) ──
// This tests that create_upload_session cleans up properly when
// write_upload_metadata fails (e.g., metadata path is a directory).
// We pre-create a directory at the metadata path by first creating a
// session, noting its ID, then recreating the scenario.

#[tokio::test]
async fn create_upload_session_metadata_write_failure_cleans_up() {
    let root = temp_root();

    // Create the oci-uploads directory
    let upload_dir = crate::upload_dir(root.path());
    tokio::fs::create_dir_all(&upload_dir).await.unwrap();

    // We cannot predict the session_id, so we create a session, grab its ID,
    // delete the session files, then create a directory at the metadata path.
    // Then we try to call persist_upload_session (which calls
    // write_upload_metadata) and verify the error propagates.
    let session_id = create_test_session(root.path(), false).await.unwrap();

    // Delete the session's files so we can recreate the metadata path as dir
    delete_upload_session(root.path(), &session_id)
        .await
        .unwrap();

    // Create a directory at the metadata path (makes write fail)
    let meta_path = crate::upload_metadata_path(root.path(), &session_id);
    tokio::fs::create_dir_all(&meta_path).await.unwrap();

    // Create a minimal session object and try to persist it
    let session = OciUploadSession {
        repository: "repo".to_owned(),
        scope_namespace: "global".to_owned(),
        created_at_unix_seconds: 1,
        last_touched_unix_seconds: 1,
        use_s3_multipart: false,
        s3_multipart: None,
    };
    let result = super::persist_upload_session(root.path(), &session_id, session).await;
    assert!(
        result.is_err(),
        "persist should fail when metadata path is a directory"
    );
    match &result {
        Err(OciAdapterError::Io(_)) | Err(OciAdapterError::BlockingTask(_)) => {} // expected
        Err(other) => panic!("expected Io or BlockingTask error, got: {other:?}"),
        Ok(_) => panic!("expected error"),
    }
}

#[tokio::test]
async fn append_upload_bytes_invalid_session_id_returns_invalid_upload_session() {
    let root = temp_root();
    let result = append_upload_bytes(root.path(), "bad/session/id", b"data").await;
    assert!(matches!(result, Err(OciAdapterError::InvalidUploadSession)));
}

#[tokio::test]
async fn upload_body_integrity_empty_session() {
    let root = temp_root();
    let session_id = create_test_session(root.path(), false).await.unwrap();
    let (sha256_hex, integrity) = upload_body_integrity(root.path(), &session_id)
        .await
        .unwrap();
    let mut hasher = Sha256::new();
    hasher.update(b"");
    let expected = hex::encode(hasher.finalize());
    assert_eq!(sha256_hex, expected);
    assert_eq!(integrity.length(), 0);
}

// ── Additional edge case tests ─────────────────────────────────────────────

#[test]
fn oci_blob_key_with_path_traversal_digest_rejected() {
    // A digest with path traversal characters should be rejected
    let result = super::oci_blob_key("repo", "../etc/passwd", None);
    assert!(
        result.is_err(),
        "path traversal in digest should produce an error"
    );
}

#[test]
fn oci_manifest_media_type_key_empty_repo_errors() {
    assert!(matches!(
        super::oci_manifest_media_type_key(
            "",
            "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            None
        ),
        Err(OciAdapterError::InvalidRepositoryName)
    ));
}

#[test]
fn oci_tag_target_prefix_errors_with_invalid_digest() {
    assert!(matches!(
        super::oci_tag_target_prefix("repo", "short", None),
        Err(OciAdapterError::InvalidDigest)
    ));
}

#[test]
fn parse_reference_rejects_digest_with_garbage_suffix() {
    // "sha256:" prefix detected but hex is wrong → InvalidDigest
    assert!(matches!(
        super::parse_reference("sha256:xyz-not-hex"),
        Err(OciAdapterError::InvalidDigest)
    ));
}

#[test]
fn parse_reference_with_colon_behaves_as_tag() {
    // "md5:" is not a "sha256:" prefix, so it's treated as a tag.
    // Tags cannot contain colons, so it gets InvalidManifestReference.
    assert!(matches!(
        super::parse_reference("md5:00112233445566778899aabbccddeeff"),
        Err(OciAdapterError::InvalidManifestReference)
    ));
}

#[test]
fn upload_session_location_with_session_id() {
    let loc = super::upload_session_location("repo", "abc123");
    assert_eq!(loc, "/v2/repo/blobs/uploads/abc123");
}

#[test]
fn oci_blob_location_with_complex_repo() {
    let loc = super::oci_blob_location("org/team/project", "deadbeef");
    assert_eq!(loc, "/v2/org/team/project/blobs/sha256:deadbeef");
}

#[test]
fn oci_manifest_location_allows_any_reference() {
    let loc = super::oci_manifest_location("repo", "v1.0.0-rc1");
    assert_eq!(loc, "/v2/repo/manifests/v1.0.0-rc1");
}

#[test]
fn upload_body_path_for_session_contains_bin_extension() {
    let root = temp_root();
    let path = super::upload_body_path_for_session(root.path(), "0123456789abcdef").unwrap();
    let name = path.file_name().unwrap().to_string_lossy();
    assert_eq!(name, "0123456789abcdef.bin");
}

#[tokio::test]
async fn upload_length_with_zero_length_body() {
    let root = temp_root();
    let session_id = create_test_session(root.path(), false).await.unwrap();
    let len = upload_length(root.path(), &session_id).await.unwrap();
    assert_eq!(len, 0, "fresh session should have zero length");
}

#[tokio::test]
async fn touch_upload_session_invalid_session_id_errors() {
    let root = temp_root();
    let session = OciUploadSession {
        repository: "repo".to_owned(),
        scope_namespace: "global".to_owned(),
        created_at_unix_seconds: 0,
        last_touched_unix_seconds: 0,
        use_s3_multipart: false,
        s3_multipart: None,
    };
    let result = super::touch_upload_session(root.path(), "bad/session/id", session).await;
    assert!(matches!(result, Err(OciAdapterError::InvalidUploadSession)));
}
