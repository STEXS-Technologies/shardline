#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::let_underscore_must_use,
    clippy::ignored_unit_patterns
)]

use std::num::{NonZeroU64, NonZeroUsize};
#[cfg(unix)]
use std::os::unix::ffi::OsStrExt;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};

use bytes::Bytes;

#[test]
fn cancelled_owned_append_and_tail_keep_payload_until_queued_writer_finishes() {
    use std::sync::Arc;
    struct Payload {
        bytes: Vec<u8>,
        dropped: Arc<AtomicBool>,
    }
    impl AsRef<[u8]> for Payload {
        fn as_ref(&self) -> &[u8] {
            &self.bytes
        }
    }
    impl Drop for Payload {
        fn drop(&mut self) {
            self.dropped.store(true, Ordering::Release);
        }
    }
    for tail in [false, true] {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .max_blocking_threads(1)
            .enable_all()
            .build()
            .unwrap();
        let root = temp_root();
        let session = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let path = if tail {
            crate::upload_tail_path(root.path(), session)
        } else {
            crate::upload_body_path(root.path(), session)
        };
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        if !tail {
            std::fs::write(&path, b"").unwrap();
        }
        runtime.block_on(async {
            let (release, wait) = std::sync::mpsc::channel();
            let (started, ready) = tokio::sync::oneshot::channel();
            let blocker = tokio::task::spawn_blocking(move || {
                started.send(()).unwrap();
                wait.recv().unwrap();
            });
            ready.await.unwrap();
            let dropped = Arc::new(AtomicBool::new(false));
            let bytes = Bytes::from_owner(Payload {
                bytes: b"retained payload".to_vec(),
                dropped: dropped.clone(),
            });
            let operation = async {
                if tail {
                    super::write_upload_tail(root.path(), session, bytes).await
                } else {
                    super::append_upload_bytes_owned(root.path(), session, bytes)
                        .await
                        .map(|_| ())
                }
            };
            let mut operation = Box::pin(operation);
            assert!(
                std::future::poll_fn(|cx| std::task::Poll::Ready(
                    operation.as_mut().poll(cx).is_pending()
                ))
                .await
            );
            drop(operation);
            assert!(!dropped.load(Ordering::Acquire));
            release.send(()).unwrap();
            blocker.await.unwrap();
            tokio::time::timeout(std::time::Duration::from_secs(5), async {
                while !dropped.load(Ordering::Acquire) {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            assert_eq!(std::fs::read(&path).unwrap(), b"retained payload");
        });
    }
}
use sha2::{Digest, Sha256};
use tempfile::TempDir;

use crate::traits::OciBackend;
use crate::{
    OciAdapterError, OciReference, OciUploadSession, SerializableSha256State,
    abort_s3_multipart_upload_session, append_s3_multipart_upload_bytes, append_upload_bytes,
    create_upload_session, delete_upload_session, finalize_s3_multipart_upload_session,
    new_upload_session_id, oci_blob_key, oci_manifest_key, oci_manifest_media_type_key,
    oci_manifest_prefix, oci_tag_key, oci_tag_prefix, parse_reference,
    purge_expired_upload_sessions, read_upload_session, touch_upload_session,
    upload_body_integrity, upload_length, upload_session_expired,
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

fn upload_session_fixture() -> OciUploadSession {
    OciUploadSession {
        repository: "repo".to_owned(),
        scope_namespace: "global".to_owned(),
        created_at_unix_seconds: 0,
        last_touched_unix_seconds: 0,
        use_s3_multipart: false,
        s3_multipart: None,
    }
}

async fn create_test_session(root: &Path, use_s3: bool) -> Result<String, OciAdapterError> {
    create_upload_session(
        root,
        NO_BACKEND,
        "repo",
        None,
        ttl(),
        max_sessions(),
        use_s3,
    )
    .await
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
    let journal_path = crate::fs::upload_evidence_journal_path(root.path(), &session_id);
    let records: Vec<shardline_reliability::PersistedMerkleJournalRecord> =
        tokio::fs::read(journal_path)
            .await
            .unwrap()
            .split(|byte| *byte == b'\n')
            .filter(|line| !line.is_empty())
            .map(serde_json::from_slice)
            .collect::<Result<_, _>>()
            .unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].merkle_commits.len(), 1);
    assert_eq!(records[0].snapshot_merkle_commits.len(), 1);
}

#[tokio::test]
async fn read_upload_session_does_not_write_missing_canonical_evidence() {
    let root = temp_root();
    let session_id = create_test_session(root.path(), false).await.unwrap();
    let metadata_path = crate::upload_metadata_path(root.path(), &session_id);
    let mut metadata: serde_json::Value =
        serde_json::from_slice(&tokio::fs::read(&metadata_path).await.unwrap()).unwrap();
    metadata.as_object_mut().unwrap().remove("evidence");
    metadata
        .as_object_mut()
        .unwrap()
        .remove("snapshot_evidence");
    tokio::fs::write(&metadata_path, serde_json::to_vec(&metadata).unwrap())
        .await
        .unwrap();

    read_upload_session(root.path(), &session_id, ttl())
        .await
        .expect("legacy session should remain readable");
    let unchanged: serde_json::Value =
        serde_json::from_slice(&tokio::fs::read(&metadata_path).await.unwrap()).unwrap();
    assert!(unchanged.get("evidence").is_none());
    assert!(unchanged.get("snapshot_evidence").is_none());
}

#[tokio::test]
async fn malformed_reliability_envelope_is_not_downgraded_to_legacy() {
    let root = temp_root();
    let session_id = create_test_session(root.path(), false).await.unwrap();
    let metadata_path = crate::upload_metadata_path(root.path(), &session_id);
    let mut metadata: serde_json::Value =
        serde_json::from_slice(&tokio::fs::read(&metadata_path).await.unwrap()).unwrap();
    metadata["evidence"] = serde_json::json!("corrupt");
    tokio::fs::write(&metadata_path, serde_json::to_vec(&metadata).unwrap())
        .await
        .unwrap();

    let error = read_upload_session(root.path(), &session_id, ttl())
        .await
        .unwrap_err();
    assert!(matches!(error, OciAdapterError::Json(_)));
}

#[tokio::test]
async fn tampered_merkle_journal_is_rejected() {
    for (field, missing) in [
        ("merkle_commits", false),
        ("snapshot_merkle_commits", false),
        ("merkle_commits", true),
        ("snapshot_merkle_commits", true),
    ] {
        let root = temp_root();
        let session_id = create_test_session(root.path(), false).await.unwrap();
        // Exercise the historical full-journal reader, not a file-length check.
        let metadata_path = crate::upload_metadata_path(root.path(), &session_id);
        let mut metadata: serde_json::Value =
            serde_json::from_slice(&tokio::fs::read(&metadata_path).await.unwrap()).unwrap();
        metadata["reliability_head_metadata"] = serde_json::json!(false);
        tokio::fs::write(&metadata_path, serde_json::to_vec(&metadata).unwrap())
            .await
            .unwrap();
        read_upload_session(root.path(), &session_id, ttl())
            .await
            .unwrap();
        let path = crate::fs::upload_evidence_journal_path(root.path(), &session_id);
        let mut value: serde_json::Value = serde_json::from_slice(
            tokio::fs::read(&path)
                .await
                .unwrap()
                .split(|byte| *byte == b'\n')
                .find(|line| !line.is_empty())
                .unwrap(),
        )
        .unwrap();
        if missing {
            value[field] = serde_json::json!([]);
        } else {
            value[field][0]["body"]["sequence"] = serde_json::json!(9);
        }
        tokio::fs::write(&path, serde_json::to_vec(&value).unwrap())
            .await
            .unwrap();

        let error = read_upload_session(root.path(), &session_id, ttl())
            .await
            .unwrap_err();
        assert!(matches!(error, OciAdapterError::Reliability(_)));

        crate::repair_upload_session_evidence(root.path(), &session_id)
            .await
            .unwrap();
        read_upload_session(root.path(), &session_id, ttl())
            .await
            .unwrap();
    }
}

#[tokio::test]
async fn oci_mutations_append_snapshot_evidence_instead_of_resetting_it() {
    let root = temp_root();
    let session_id = create_test_session(root.path(), false).await.unwrap();
    let session = read_upload_session(root.path(), &session_id, ttl())
        .await
        .unwrap();
    touch_upload_session(root.path(), &session_id, session)
        .await
        .unwrap();
    let journal_path = crate::fs::upload_evidence_journal_path(root.path(), &session_id);
    let records: Vec<shardline_reliability::PersistedMerkleJournalRecord> =
        tokio::fs::read(journal_path)
            .await
            .unwrap()
            .split(|byte| *byte == b'\n')
            .filter(|line| !line.is_empty())
            .map(serde_json::from_slice)
            .collect::<Result<_, _>>()
            .unwrap();
    assert_eq!(
        records
            .iter()
            .map(|record| record.snapshot_evidence.len())
            .sum::<usize>(),
        2
    );
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
    let bound_scope = test_scope();
    for (repository, scope) in [
        ("my-repo", None),
        ("team/assets", Some(&bound_scope)),
        ("team/assets/cache", Some(&bound_scope)),
    ] {
        let root = temp_root();
        let session_id = create_upload_session(
            root.path(),
            NO_BACKEND,
            repository,
            scope,
            ttl(),
            max_sessions(),
            false,
        )
        .await
        .unwrap();
        assert!(!session_id.is_empty());
        let session = read_session(root.path(), &session_id).await;
        assert_eq!(session.repository, repository);
        assert_eq!(
            session.scope_namespace,
            crate::protocol_support::scope_namespace(scope)
        );
    }
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
    let scope = test_scope();
    for repository in ["team/other", "other/assets", "team/assets-suffix"] {
        let root = temp_root();
        let result = create_upload_session(
            root.path(),
            NO_BACKEND,
            repository,
            Some(&scope),
            ttl(),
            max_sessions(),
            false,
        )
        .await;
        assert!(
            matches!(result, Err(OciAdapterError::NotFound)),
            "{repository}: {result:?}"
        );
    }
}

#[tokio::test]
async fn read_upload_session_returns_not_found_for_missing() {
    for session_id in [
        "10000000000000000000000000000000",
        "00000000000000000000000000000000",
    ] {
        let root = temp_root();
        let result = read_upload_session(root.path(), session_id, ttl()).await;
        assert!(
            matches!(result, Err(OciAdapterError::NotFound)),
            "{session_id}: {result:?}"
        );
    }
}

#[allow(clippy::shadow_unrelated)]
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
async fn upload_length_and_integrity_match_empty_text_binary_and_multibuffer_content() {
    for data in [
        Vec::new(),
        b"abcdef".to_vec(),
        b"hello world".to_vec(),
        b"integrity check".to_vec(),
        (0_u8..=255).collect(),
        vec![0xa5; 2 * 256 * 1024 + 1],
    ] {
        let root = temp_root();
        let session_id = create_test_session(root.path(), false).await.unwrap();
        let expected_length = data.len() as u64;
        if !data.is_empty() {
            assert_eq!(
                append_upload_bytes(root.path(), &session_id, &data)
                    .await
                    .unwrap(),
                expected_length,
            );
        }
        assert_eq!(
            upload_length(root.path(), &session_id).await.unwrap(),
            expected_length
        );
        let (sha256_hex, integrity) = upload_body_integrity(root.path(), &session_id)
            .await
            .unwrap();
        assert_eq!(sha256_hex, hex::encode(Sha256::digest(&data)));
        assert_eq!(
            integrity,
            shardline_storage::ObjectIntegrity::new(
                shardline_server_core::chunk_hash(&data),
                expected_length
            )
        );
    }
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
async fn tampered_upload_session_cannot_be_read_or_deleted() {
    for (field, value) in [
        ("repository", serde_json::json!("tampered")),
        ("last_touched_unix_seconds", serde_json::json!(0_u64)),
    ] {
        let root = temp_root();
        let session_id = create_test_session(root.path(), false).await.unwrap();
        let metadata_path = crate::upload_metadata_path(root.path(), &session_id);
        let mut metadata: serde_json::Value =
            serde_json::from_slice(&tokio::fs::read(&metadata_path).await.unwrap()).unwrap();
        metadata[field] = value;
        tokio::fs::write(&metadata_path, serde_json::to_vec(&metadata).unwrap())
            .await
            .unwrap();

        let read = read_upload_session(root.path(), &session_id, NonZeroU64::new(1).unwrap()).await;
        assert!(
            matches!(read, Err(OciAdapterError::Reliability(_))),
            "{field}: {read:?}"
        );
        let deletion = delete_upload_session(root.path(), &session_id).await;
        assert!(
            matches!(deletion, Err(OciAdapterError::Reliability(_))),
            "{field}: {deletion:?}"
        );
        assert!(tokio::fs::try_exists(&metadata_path).await.unwrap());
        assert!(
            tokio::fs::try_exists(crate::upload_body_path(root.path(), &session_id))
                .await
                .unwrap()
        );
    }
}

#[tokio::test]
async fn read_upload_session_returns_not_found_when_expired() {
    let root = temp_root();
    let session_id = create_test_session(root.path(), false).await.unwrap();
    let mut session = read_session(root.path(), &session_id).await;
    session.last_touched_unix_seconds = 1;
    super::persist_upload_session(root.path(), &session_id, &session)
        .await
        .unwrap();
    let result = read_upload_session(root.path(), &session_id, ttl()).await;
    assert!(matches!(result, Err(OciAdapterError::NotFound)));
}

#[tokio::test]
async fn purge_expires_sessions_at_the_ttl_boundary() {
    let root = temp_root();
    let session_id = create_test_session(root.path(), false).await.unwrap();
    let session = read_session(root.path(), &session_id).await;
    let expires_at = session.last_touched_unix_seconds + ttl().get();
    for (now, retained) in [(expires_at - 1, true), (expires_at, false)] {
        purge_expired_upload_sessions::<TestBackend>(root.path(), NO_BACKEND, ttl(), now)
            .await
            .unwrap();
        for path in [
            crate::upload_body_path(root.path(), &session_id),
            crate::upload_metadata_path(root.path(), &session_id),
            crate::fs::upload_evidence_journal_path(root.path(), &session_id),
        ] {
            assert_eq!(path.is_file(), retained, "{path:?} at {now}");
        }
        let result = read_upload_session(root.path(), &session_id, ttl()).await;
        if retained {
            assert!(result.is_ok());
        } else {
            assert!(matches!(result, Err(OciAdapterError::NotFound)));
        }
    }
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

#[test]
fn upload_session_ids_are_unique_hex_values() {
    let mut ids = std::collections::HashSet::new();
    for _ in 0..100 {
        let id = new_upload_session_id();
        assert_eq!(id.len(), 32);
        assert!(id.bytes().all(|byte| byte.is_ascii_hexdigit()), "{id}");
        assert!(ids.insert(id.clone()), "duplicate session id: {id}");
    }
}

#[test]
fn upload_session_expiry_obeys_the_ttl_boundary() {
    let session = OciUploadSession {
        created_at_unix_seconds: 1000,
        last_touched_unix_seconds: 1000,
        ..upload_session_fixture()
    };
    let ttl = NonZeroU64::new(60).unwrap();
    for (now, expired) in [(1050, false), (1059, false), (1060, true), (1061, true)] {
        assert_eq!(upload_session_expired(&session, ttl, now), expired, "{now}");
    }
}

#[test]
fn serializable_sha256_matches_reference_and_resumes_after_each_fragment() {
    let large = vec![b'x'; 1_000_000];
    let cases: [(&str, &[&[u8]]); 6] = [
        ("empty", &[]),
        ("single", &[b"hello"]),
        ("fragmented", &[b"hel", b"lo", b" world"]),
        ("partial block", &[b"partial"]),
        ("full blocks and tail", &[&[0u8; 128], b"tail"]),
        ("large", &[b"chunk-1", &large, b"chunk-3"]),
    ];
    for (case, chunks) in cases {
        let mut state = SerializableSha256State::default();
        let mut reference = Sha256::new();
        for chunk in chunks {
            state.update(chunk).unwrap();
            reference.update(chunk);
            let before = state.finalize_hex().unwrap();
            assert_eq!(before, hex::encode(reference.clone().finalize()), "{case}");
            state = serde_json::from_slice(&serde_json::to_vec(&state).unwrap()).unwrap();
            assert_eq!(state.finalize_hex().unwrap(), before, "{case}");
        }
        assert_eq!(
            state.finalize_hex().unwrap(),
            hex::encode(reference.finalize()),
            "{case}"
        );
    }
}

// ── Key construction tests ──────────────────────────────────────────────────

const VALID_DIGEST: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const VALID_DIGEST_FULL: &str =
    "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

fn test_scope() -> RepositoryScope {
    RepositoryScope::new(RepositoryProvider::GitHub, "team", "assets", None).unwrap()
}

fn assert_oci_path(path: &str, scoped: bool, suffix: &str) {
    let (namespace, _) = path
        .strip_prefix("protocols/oci/")
        .unwrap()
        .split_once("/repos/")
        .unwrap();
    assert_eq!(namespace == "global", !scoped, "{path}");
    if scoped {
        assert_eq!(namespace.len(), 64, "{path}");
        assert!(
            namespace
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)),
            "{path}"
        );
    }
    assert!(path.ends_with(suffix), "{path} must end with {suffix}");
}

#[test]
fn oci_key_and_prefix_layouts_preserve_namespaces_and_references() {
    let repository_scope = test_scope();
    for scope in [None, Some(&repository_scope)] {
        // Object keys preserve historical opaque digest strings; tag-target
        // tombstones separately require canonical SHA-256 digests.
        for digest in [
            VALID_DIGEST_FULL,
            "short-digest",
            "any-digest-string",
            "test-digest",
            "sha256:nothex",
        ] {
            for (key, kind) in [
                (oci_blob_key("team/assets", digest, scope), "blobs"),
                (oci_manifest_key("team/assets", digest, scope), "manifests"),
                (
                    oci_manifest_media_type_key("team/assets", digest, scope),
                    "manifest-media-types",
                ),
            ] {
                assert_oci_path(
                    key.unwrap_or_else(|error| {
                        panic!("{kind}/{digest}, scoped={}: {error}", scope.is_some())
                    })
                    .as_str(),
                    scope.is_some(),
                    &format!("/{kind}/{digest}"),
                );
            }
        }
        for tag in ["latest", "v1.0", "v1", "stable"] {
            assert_oci_path(
                oci_tag_key("team/assets", tag, scope).unwrap().as_str(),
                scope.is_some(),
                &format!("/tags/{tag}"),
            );
            assert_oci_path(
                super::oci_tag_target_key("team/assets", VALID_DIGEST, tag, scope)
                    .unwrap()
                    .as_str(),
                scope.is_some(),
                &format!("/tag-targets/{VALID_DIGEST}/{tag}"),
            );
        }
        for (prefix, suffix) in [
            (
                oci_manifest_prefix("team/assets", scope),
                "/manifests/".to_owned(),
            ),
            (oci_tag_prefix("team/assets", scope), "/tags/".to_owned()),
            (
                super::oci_tag_target_prefix("team/assets", VALID_DIGEST, scope),
                format!("/tag-targets/{VALID_DIGEST}/"),
            ),
        ] {
            assert_oci_path(prefix.unwrap().as_str(), scope.is_some(), &suffix);
        }
    }
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
fn parse_reference_rejects_invalid_tags() {
    // Other digest prefixes follow tag validation, which rejects colons.
    for reference in ["", "md5:00112233445566778899aabbccddeeff"] {
        assert!(
            matches!(
                parse_reference(reference),
                Err(OciAdapterError::InvalidManifestReference)
            ),
            "{reference:?}"
        );
    }
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

#[allow(clippy::shadow_unrelated)]
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
    let part_size = crate::types::OCI_S3_MULTIPART_CHUNK_BYTES;
    for length in [
        part_size - 1,
        part_size,
        part_size + 100,
        3 * part_size + 100,
    ] {
        let root = temp_root();
        let backend = MockS3Backend::new();
        let session_id = create_s3_session(root.path(), &backend).await;
        let chunk: Vec<u8> = (0..length).map(|i| ((i / 997) % 251) as u8).collect();
        let session = read_session(root.path(), &session_id).await;
        let (session, observed_length) =
            append_s3_multipart_upload_bytes(root.path(), &backend, &session_id, session, &chunk)
                .await
                .unwrap();
        assert_eq!(usize::try_from(observed_length).unwrap(), length);
        let multipart = session.s3_multipart.as_ref().unwrap();
        assert_eq!(multipart.uploaded_part_ids.len(), length / part_size);
        let upload_id = multipart.upload_id.clone();
        let digest = sha256_hex(&chunk);
        let object_key = ObjectKey::parse("protocols/oci/test/blob").unwrap();
        assert_eq!(
            finalize_s3_multipart_upload_session(
                root.path(),
                &backend,
                &session_id,
                session,
                &object_key,
                &digest,
                b"",
            )
            .await
            .unwrap(),
            PutOutcome::Inserted
        );
        backend.assert_completed(&upload_id, &chunk.chunks(part_size).collect::<Vec<_>>());
    }
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
    backend.assert_aborted(&s.s3_multipart.unwrap().upload_id);
}

#[allow(clippy::shadow_unrelated)]
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
fn validate_repository_rejects_invalid_names() {
    for repository in ["", "../assets", "Team/assets", "team//assets"] {
        assert!(
            matches!(
                super::validate_repository(repository),
                Err(OciAdapterError::InvalidRepositoryName)
            ),
            "{repository:?}"
        );
    }
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
fn upload_session_length_tracks_multipart_state() {
    for (use_s3_multipart, total_length, expected) in [
        (false, None, None),
        (true, None, Some(0)),
        (true, Some(0), Some(0)),
        (true, Some(42), Some(42)),
    ] {
        let session = OciUploadSession {
            use_s3_multipart,
            s3_multipart: total_length.map(|total_length| crate::OciS3MultipartUploadSession {
                temporary_object_key: "tmp/key".to_owned(),
                upload_id: "upload-1".to_owned(),
                uploaded_part_ids: vec![],
                total_length,
                sha256_state: crate::SerializableSha256State::default(),
            }),
            ..upload_session_fixture()
        };
        assert_eq!(
            super::upload_session_length(&session),
            expected,
            "{session:?}"
        );
    }
}

#[tokio::test]
async fn touch_upload_session_persists_updated_timestamp() {
    let root = temp_root();
    let session_id = create_test_session(root.path(), false).await.unwrap();
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
    let session = upload_session_fixture();
    let backend = TestBackend;
    let result = super::abort_s3_multipart_upload_session(&backend, &session).await;
    assert!(result.is_ok());
}

#[test]
fn oci_keys_reject_invalid_repositories() {
    for (case, result) in [
        (
            "oci_manifest_key_rejects_empty_repo",
            super::oci_manifest_key("", VALID_DIGEST_FULL, None).err(),
        ),
        (
            "oci_blob_key_empty_repo_errors",
            oci_blob_key("", VALID_DIGEST_FULL, None).err(),
        ),
        (
            "oci_tag_target_key_empty_repo_errors",
            super::oci_tag_target_key("", VALID_DIGEST, "latest", None).err(),
        ),
        (
            "oci_tag_target_prefix_empty_repo_errors",
            super::oci_tag_target_prefix("", VALID_DIGEST, None).err(),
        ),
        (
            "oci_tag_key_invalid_repo_errors",
            super::oci_tag_key("", "latest", None).err(),
        ),
        (
            "oci_manifest_prefix_invalid_repo_errors",
            super::oci_manifest_prefix("", None).err(),
        ),
        (
            "oci_tag_prefix_invalid_repo_errors",
            super::oci_tag_prefix("", None).err(),
        ),
        (
            "oci_manifest_media_type_key_empty_repo_errors",
            super::oci_manifest_media_type_key(
                "",
                "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                None,
            )
            .err(),
        ),
    ] {
        assert!(
            matches!(result, Some(OciAdapterError::InvalidRepositoryName)),
            "{case}: {result:?}"
        );
    }
}

#[test]
fn oci_keys_reject_invalid_tags() {
    for (case, result) in [
        (
            "oci_tag_key_invalid_tag_uppercase",
            super::oci_tag_key("team/assets", "-starts-with-hyphen", None).err(),
        ),
        (
            "oci_tag_key_rejects_invalid_tag_starting_with_hyphen",
            super::oci_tag_key("repo", "-invalid", None).err(),
        ),
        (
            "oci_tag_key_empty_tag_errors",
            oci_tag_key("team/assets", "", None).err(),
        ),
        (
            "oci_tag_target_key_empty_tag_errors",
            super::oci_tag_target_key("team/assets", VALID_DIGEST, "", None).err(),
        ),
        (
            "oci_tag_key_invalid_tag_errors",
            super::oci_tag_key("team/assets", "bad/tag", None).err(),
        ),
    ] {
        assert!(
            matches!(result, Some(OciAdapterError::InvalidManifestReference)),
            "{case}: {result:?}"
        );
    }
}

#[test]
fn oci_keys_reject_invalid_digests() {
    for (case, result) in [
        (
            "oci_tag_target_prefix_invalid_digest_errors",
            super::oci_tag_target_prefix("team/assets", "not-a-digest", None).err(),
        ),
        (
            "oci_tag_target_key_invalid_digest_errors",
            super::oci_tag_target_key("team/assets", "bad", "latest", None).err(),
        ),
        (
            "oci_tag_target_prefix_errors_with_invalid_digest",
            super::oci_tag_target_prefix("repo", "short", None).err(),
        ),
    ] {
        assert!(
            matches!(result, Some(OciAdapterError::InvalidDigest)),
            "{case}: {result:?}"
        );
    }
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
async fn purge_expired_corrupt_json_metadata_is_preserved() {
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
    let result =
        purge_expired_upload_sessions::<TestBackend>(root.path(), NO_BACKEND, ttl(), now).await;
    assert!(matches!(result, Err(OciAdapterError::Json(_))));

    // Corrupt metadata and its associated body remain available for repair.
    assert!(
        bogus_meta.exists(),
        "corrupt metadata should be preserved for repair"
    );
    assert!(bogus_body.exists(), "associated body should be preserved");

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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn persistence_locks_for_different_roots_run_in_parallel() {
    let first_root = temp_root();
    let second_root = temp_root();
    let first = super::fs::session_persist_lock(first_root.path(), "same-session-id");
    let _first_guard = first.lock().await;
    let second = super::fs::session_persist_lock(second_root.path(), "same-session-id");

    let _second_guard = tokio::time::timeout(std::time::Duration::from_secs(2), second.lock())
        .await
        .expect("different deployment roots must not share a persistence lock");
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
fn oci_blob_key_empty_digest_fails() {
    // Empty digest produces an invalid path component that fails validation
    let result = super::oci_blob_key("repo", "", None);
    assert!(
        result.is_err(),
        "empty digest should produce an error: got: {result:?}"
    );
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
fn parse_reference_rejects_invalid_digest() {
    for reference in ["sha256:nothex", "sha256:xyz-not-hex"] {
        assert!(
            matches!(
                parse_reference(reference),
                Err(OciAdapterError::InvalidDigest)
            ),
            "{reference:?}"
        );
    }
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
    backend.assert_aborted(&s.s3_multipart.unwrap().upload_id);
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
    backend.assert_aborted(&s.s3_multipart.unwrap().upload_id);
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

#[allow(clippy::shadow_unrelated)]
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

#[allow(clippy::shadow_unrelated)]
#[tokio::test]
async fn exact_multipart_chunk_leaves_no_tail_with_or_without_existing_tail() {
    for prefix in [b"".as_slice(), b"x"] {
        let root = temp_root();
        let backend = MockS3Backend::new();
        let session_id = create_s3_session(root.path(), &backend).await;
        let session = read_session(root.path(), &session_id).await;
        let (session, _) =
            append_s3_multipart_upload_bytes(root.path(), &backend, &session_id, session, prefix)
                .await
                .unwrap();
        let tail_path = crate::upload_tail_path(root.path(), &session_id);
        assert_eq!(tail_path.is_file(), !prefix.is_empty());
        let remaining = vec![b'y'; crate::types::OCI_S3_MULTIPART_CHUNK_BYTES - prefix.len()];
        let (_, length) = append_s3_multipart_upload_bytes(
            root.path(),
            &backend,
            &session_id,
            session,
            &remaining,
        )
        .await
        .unwrap();
        assert_eq!(length, crate::types::OCI_S3_MULTIPART_CHUNK_BYTES as u64);
        assert!(!tail_path.exists());
        let persisted = read_session(root.path(), &session_id).await;
        assert_eq!(persisted.s3_multipart.unwrap().uploaded_part_ids.len(), 1);
    }
}

#[tokio::test]
async fn upload_operations_preserve_io_errors_when_upload_directory_is_a_file() {
    let root = temp_root();
    let session_id = create_test_session(root.path(), false).await.unwrap();
    let upload_dir = crate::upload_dir(root.path());
    tokio::fs::remove_dir_all(&upload_dir).await.unwrap();
    tokio::fs::write(&upload_dir, b"not-a-dir").await.unwrap();

    for (operation, result) in [
        (
            "read",
            read_upload_session(root.path(), &session_id, ttl())
                .await
                .map(|_| ()),
        ),
        (
            "length",
            upload_length(root.path(), &session_id).await.map(|_| ()),
        ),
        (
            "append",
            append_upload_bytes(root.path(), &session_id, b"data")
                .await
                .map(|_| ()),
        ),
        (
            "delete",
            delete_upload_session(root.path(), &session_id).await,
        ),
        (
            "count",
            super::count_active_upload_sessions(root.path(), ttl())
                .await
                .map(|_| ()),
        ),
        (
            "purge",
            purge_expired_upload_sessions::<TestBackend>(
                root.path(),
                NO_BACKEND,
                ttl(),
                crate::fs::unix_now_seconds_checked().unwrap(),
            )
            .await,
        ),
    ] {
        assert!(
            matches!(result, Err(OciAdapterError::Io(_))),
            "{operation}: {result:?}"
        );
    }
    assert_eq!(tokio::fs::read(&upload_dir).await.unwrap(), b"not-a-dir");
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
async fn count_active_sessions_skips_unreadable_invalid_and_expired_metadata() {
    let root = temp_root();
    create_test_session(root.path(), false).await.unwrap();
    let unreadable = crate::upload_metadata_path(root.path(), "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
    tokio::fs::create_dir(&unreadable).await.unwrap();
    let invalid = crate::upload_metadata_path(root.path(), "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");
    tokio::fs::write(&invalid, b"not-valid-json").await.unwrap();
    let expired_id = "cccccccccccccccccccccccccccccccc";
    let expired_session = OciUploadSession {
        created_at_unix_seconds: 1,
        last_touched_unix_seconds: 1,
        ..upload_session_fixture()
    };
    super::persist_upload_session(root.path(), expired_id, &expired_session)
        .await
        .unwrap();
    // The expiry case must pass evidence validation before the counter skips it.
    crate::fs::read_persisted_upload_session(root.path(), expired_id)
        .await
        .unwrap();
    assert_eq!(
        super::count_active_upload_sessions(root.path(), ttl())
            .await
            .unwrap(),
        1,
    );
    assert!(unreadable.is_dir());
    assert_eq!(tokio::fs::read(&invalid).await.unwrap(), b"not-valid-json");
    assert!(crate::upload_metadata_path(root.path(), expired_id).is_file());
}

#[tokio::test]
async fn purge_skips_invalid_session_stems() {
    for extension in ["bin", "json", "tail"] {
        let root = temp_root();
        let path = crate::upload_dir(root.path()).join(format!("invalid-stem.{extension}"));
        tokio::fs::create_dir_all(path.parent().unwrap())
            .await
            .unwrap();
        tokio::fs::write(&path, b"retained").await.unwrap();
        purge_expired_upload_sessions::<TestBackend>(
            root.path(),
            NO_BACKEND,
            ttl(),
            crate::fs::unix_now_seconds_checked().unwrap(),
        )
        .await
        .unwrap();
        assert_eq!(
            tokio::fs::read(&path).await.unwrap(),
            b"retained",
            "{extension}"
        );
    }
}

#[cfg(unix)]
#[tokio::test]
async fn purge_skips_non_utf8_session_stems() {
    for name in [
        b"\xff\xfe.bin".as_slice(),
        b"\xfe\xff.json",
        b"\xff\xfe.tail",
    ] {
        let root = temp_root();
        let path = crate::upload_dir(root.path()).join(std::ffi::OsStr::from_bytes(name));
        tokio::fs::create_dir_all(path.parent().unwrap())
            .await
            .unwrap();
        tokio::fs::write(&path, b"retained").await.unwrap();
        purge_expired_upload_sessions::<TestBackend>(
            root.path(),
            NO_BACKEND,
            ttl(),
            crate::fs::unix_now_seconds_checked().unwrap(),
        )
        .await
        .unwrap();
        assert_eq!(
            tokio::fs::read(&path).await.unwrap(),
            b"retained",
            "{name:?}"
        );
    }
}

#[tokio::test]
async fn purge_preserves_session_files_when_metadata_read_fails() {
    let root = temp_root();
    let session_id = create_test_session(root.path(), false).await.unwrap();
    append_upload_bytes(root.path(), &session_id, b"retained body")
        .await
        .unwrap();
    let metadata_path = crate::upload_metadata_path(root.path(), &session_id);
    tokio::fs::remove_file(&metadata_path).await.unwrap();
    tokio::fs::create_dir(&metadata_path).await.unwrap();

    let result = purge_expired_upload_sessions::<TestBackend>(
        root.path(),
        NO_BACKEND,
        ttl(),
        crate::fs::unix_now_seconds_checked().unwrap(),
    )
    .await;
    assert!(matches!(result, Err(OciAdapterError::Io(_))));
    assert!(metadata_path.is_dir());
    assert_eq!(
        tokio::fs::read(crate::upload_body_path(root.path(), &session_id))
            .await
            .unwrap(),
        b"retained body",
    );
    assert!(crate::fs::upload_evidence_journal_path(root.path(), &session_id).is_file());
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

// ── write_upload_tail non-NotFound IO error (line 1163) ───────────────────

#[tokio::test]
async fn write_upload_tail_io_error_on_remove_direct() {
    let root = temp_root();
    let session_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"; // valid 32-char hex
    let tail_dir = crate::upload_tail_path(root.path(), session_id);
    tokio::fs::create_dir_all(&tail_dir).await.unwrap();

    // write_upload_tail with empty bytes → fs::remove_file on a directory → EISDIR
    let result = super::write_upload_tail(root.path(), session_id, Bytes::new()).await;
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
async fn purge_expired_read_error_is_preserved_with_permission_denied() {
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

    let result = purge_expired_upload_sessions::<TestBackend>(
        root.path(),
        NO_BACKEND,
        NonZeroU64::new(1).unwrap(),
        far_future,
    )
    .await;
    assert!(matches!(result, Err(OciAdapterError::Io(_))));

    // The unreadable session remains available for repair.
    let read_result = read_upload_session(root.path(), &session_id, ttl()).await;
    assert!(
        read_result.is_err(),
        "unreadable metadata should not be purged"
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

#[tokio::test]
async fn persist_upload_session_rejects_metadata_directory() {
    let root = temp_root();
    let session_id = "0123456789abcdef";
    let metadata_path = crate::upload_metadata_path(root.path(), session_id);
    tokio::fs::create_dir_all(&metadata_path).await.unwrap();
    let session = OciUploadSession {
        created_at_unix_seconds: 1,
        last_touched_unix_seconds: 1,
        ..upload_session_fixture()
    };
    assert!(matches!(
        super::persist_upload_session(root.path(), session_id, &session).await,
        Err(OciAdapterError::Io(_) | OciAdapterError::BlockingTask(_))
    ));
    assert!(metadata_path.is_dir());
}

#[tokio::test]
async fn append_upload_bytes_invalid_session_id_returns_invalid_upload_session() {
    let root = temp_root();
    let result = append_upload_bytes(root.path(), "bad/session/id", b"data").await;
    assert!(matches!(result, Err(OciAdapterError::InvalidUploadSession)));
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
async fn touch_upload_session_invalid_session_id_errors() {
    let root = temp_root();
    let session = upload_session_fixture();
    let result = super::touch_upload_session(root.path(), "bad/session/id", session).await;
    assert!(matches!(result, Err(OciAdapterError::InvalidUploadSession)));
}

// ── write_file_atomically tests (moved from inline module) ──────────────────

#[cfg(test)]
mod write_file_atomically_tests {
    use std::path::{Path, PathBuf};
    use tempfile::TempDir;

    fn temp_root() -> (TempDir, PathBuf) {
        let dir = TempDir::new().unwrap();
        let root = dir.path().to_path_buf();
        (dir, root)
    }

    #[test]
    fn creates_and_overwrites_files_with_expected_content() {
        for (relative_path, payload) in [
            ("metadata.json", b"{\"key\":\"value\"}".as_slice()),
            ("data.json", b"first"),
            ("empty.json", b""),
            ("sub/dir/file.json", b"nested"),
        ] {
            let (_dir, root) = temp_root();
            let path = root.join(relative_path);
            crate::write_file_atomically(&root, &path, payload).unwrap();
            assert_eq!(std::fs::read(&path).unwrap(), payload, "{relative_path}");
            crate::write_file_atomically(&root, &path, b"second").unwrap();
            assert_eq!(std::fs::read(&path).unwrap(), b"second", "{relative_path}");
        }
    }

    #[test]
    fn rejects_path_escaping_root() {
        let (_dir, root) = temp_root();
        // A path that resolves outside of root via `..`
        let path = root.join("../outside.json");
        let result = crate::write_file_atomically(&root, &path, b"escape");
        assert!(result.is_err(), "must reject path escaping root");
    }

    #[test]
    fn rejects_path_absolute_outside_root() {
        let (_dir, root) = temp_root();
        let path = Path::new("/tmp/not-under-root.json").to_path_buf();
        let result = crate::write_file_atomically(&root, &path, b"escape");
        assert!(result.is_err(), "must reject absolute path outside root");
    }

    #[test]
    fn respects_root_distinct_dirs() {
        let dir1 = TempDir::new().unwrap();
        let dir2 = TempDir::new().unwrap();
        let path1 = dir1.path().join("file.json");
        let path2 = dir2.path().join("file.json");
        crate::write_file_atomically(dir1.path(), &path1, b"alpha").unwrap();
        crate::write_file_atomically(dir2.path(), &path2, b"beta").unwrap();
        assert_eq!(std::fs::read(&path1).unwrap(), b"alpha");
        assert_eq!(std::fs::read(&path2).unwrap(), b"beta");
    }

    #[cfg(unix)]
    #[test]
    fn rename_failure_cleans_up_temporary() {
        let (_dir, root) = temp_root();
        // Create a non-empty directory at the target path so rename fails
        // (rename(2) cannot overwrite a non-empty directory with a file).
        let target = root.join("target.json");
        std::fs::create_dir(&target).unwrap();
        std::fs::write(target.join("child"), b"x").unwrap();

        let result = crate::write_file_atomically(&root, &target, b"data");
        assert!(
            result.is_err(),
            "rename should fail when target is a non-empty directory"
        );
        assert_eq!(std::fs::read(target.join("child")).unwrap(), b"x");
        let remaining: Vec<_> = std::fs::read_dir(&root)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .collect();
        assert_eq!(remaining, [target], "temporary sibling must be removed");
    }
}

#[test]
fn upload_snapshot_v1_preserves_the_serialized_session_schema() {
    let mut session = upload_session_fixture();
    for multipart in [false, true] {
        session.use_s3_multipart = multipart;
        if multipart {
            let mut sha256_state = super::SerializableSha256State::default();
            sha256_state.update(b"abc").unwrap();
            session.s3_multipart = Some(super::OciS3MultipartUploadSession {
                temporary_object_key: "temporary/object".into(),
                upload_id: "upload-id".into(),
                uploaded_part_ids: vec!["first".into(), "second".into()],
                total_length: 3,
                sha256_state,
            });
        }
        assert_eq!(
            serde_json::to_vec(&session.reliability_snapshot_v1()).unwrap(),
            serde_json::to_vec(&session).unwrap(),
        );
    }
}
