#[cfg(unix)]
use std::os::unix::fs::symlink;
use std::{io::ErrorKind, num::NonZeroUsize};

use axum::body::Bytes;
use serde_json::to_vec;
use shardline_index::{FileChunkRecord, FileRecord, LocalIndexStoreError};
use shardline_protocol::{RepositoryProvider, RepositoryScope};
use shardline_storage::LocalObjectStoreError;
use tokio::fs;

use super::LocalBackend;
use crate::{
    ServerError, ShardMetadataLimits,
    error::{IndexError, ObjectStoreError},
    test_fixtures::{single_chunk_xorb, single_file_shard},
    upload_ingest::RequestBodyReader,
};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn local_backend_reuses_unchanged_chunks() {
    let temp = tempfile::tempdir();
    assert!(temp.is_ok());
    let Ok(temp) = temp else {
        return;
    };
    let chunk_size = NonZeroUsize::new(128);
    assert!(chunk_size.is_some());
    let Some(chunk_size) = chunk_size else {
        return;
    };
    let backend = LocalBackend::new(
        temp.path().to_path_buf(),
        "http://127.0.0.1:8080".to_owned(),
        chunk_size,
    )
    .await;
    assert!(backend.is_ok());
    let Ok(backend) = backend else {
        return;
    };

    let first_payload: Vec<u8> = (0_u16..450).map(|x| x as u8).collect();
    let second_payload: Vec<u8> = {
        let mut v: Vec<u8> = (0_u16..150).map(|x| x as u8).collect();
        v.extend_from_slice(&[0xFF; 150]);
        v.extend_from_slice(&(300_u16..450).map(|x| x as u8).collect::<Vec<u8>>());
        v
    };

    let first = backend
        .upload_file("asset.bin", Bytes::from(first_payload.clone()), None)
        .await;
    let second = backend
        .upload_file("asset.bin", Bytes::from(second_payload.clone()), None)
        .await;
    let latest_bytes = backend.download_file("asset.bin", None, None).await;
    let stats = backend.stats().await;

    assert!(first.is_ok());
    assert!(second.is_ok());
    assert!(latest_bytes.is_ok());
    assert!(stats.is_ok());
    let (Ok(first), Ok(second), Ok(latest_bytes), Ok(stats)) = (first, second, latest_bytes, stats)
    else {
        return;
    };
    let first_bytes = backend
        .download_file("asset.bin", Some(&first.content_hash), None)
        .await;
    assert!(first_bytes.is_ok());
    let Ok(first_bytes) = first_bytes else {
        return;
    };

    assert!(
        first.inserted_chunks > 0,
        "first upload must insert at least 1 chunk"
    );
    assert!(
        second.reused_chunks + second.inserted_chunks >= first.inserted_chunks,
        "second upload should reuse or insert at least as many total chunks as first"
    );
    assert_eq!(latest_bytes, second_payload.as_slice());
    assert_eq!(first_bytes, first_payload.as_slice());

    // Total chunks in store must be at least the number from first upload.
    assert!(
        stats.chunks >= first.inserted_chunks,
        "stats.chunks ({}) should be >= first.inserted_chunks ({})",
        stats.chunks,
        first.inserted_chunks
    );
}

#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn local_backend_stats_ignore_non_authoritative_legacy_file_inventory() {
    let temp = tempfile::tempdir();
    assert!(temp.is_ok());
    let Ok(temp) = temp else {
        return;
    };
    let chunk_size = NonZeroUsize::new(128);
    assert!(chunk_size.is_some());
    let Some(chunk_size) = chunk_size else {
        return;
    };
    let backend = LocalBackend::new(
        temp.path().to_path_buf(),
        "http://127.0.0.1:8080".to_owned(),
        chunk_size,
    )
    .await;
    assert!(backend.is_ok());
    let Ok(backend) = backend else {
        return;
    };
    let escaped_dir = temp.path().join("escaped-file-inventory");
    let create = fs::create_dir_all(&escaped_dir).await;
    assert!(create.is_ok());
    let escaped_file = escaped_dir.join("outside.bin");
    let write = fs::write(&escaped_file, b"outside").await;
    assert!(write.is_ok());

    let files_root = temp.path().join("files");
    let created_files_root = fs::create_dir_all(&files_root).await;
    assert!(created_files_root.is_ok());
    let symlink_path = files_root.join("escape");
    let linked = symlink(&escaped_dir, &symlink_path);
    assert!(linked.is_ok());

    let stats = backend.stats().await;

    assert!(stats.is_ok());
    assert_eq!(stats.unwrap().files, 0);
}

#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn local_backend_ready_rejects_symlinked_metadata_database_path() {
    let temp = tempfile::tempdir();
    assert!(temp.is_ok());
    let Ok(temp) = temp else {
        return;
    };
    let chunk_size = NonZeroUsize::new(128);
    assert!(chunk_size.is_some());
    let Some(chunk_size) = chunk_size else {
        return;
    };
    let backend = LocalBackend::new(
        temp.path().to_path_buf(),
        "http://127.0.0.1:8080".to_owned(),
        chunk_size,
    )
    .await;
    assert!(backend.is_ok());
    let Ok(_backend) = backend else {
        return;
    };
    let removed = fs::remove_file(temp.path().join("metadata.sqlite3")).await;
    assert!(removed.is_ok());
    let external_database = temp.path().join("external-metadata.sqlite3");
    let linked = symlink(&external_database, temp.path().join("metadata.sqlite3"));
    assert!(linked.is_ok());

    let restarted = LocalBackend::new(
        temp.path().to_path_buf(),
        "http://127.0.0.1:8080".to_owned(),
        chunk_size,
    )
    .await;
    assert!(matches!(
        restarted,
        Err(ServerError::Index(IndexError::Local(LocalIndexStoreError::Io(error))))
            if error.kind() == ErrorKind::InvalidData
    ));
}

#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn local_backend_new_rejects_symlinked_root_ancestor() {
    let temp = tempfile::tempdir();
    assert!(temp.is_ok());
    let Ok(temp) = temp else {
        return;
    };
    let chunk_size = NonZeroUsize::new(128);
    assert!(chunk_size.is_some());
    let Some(chunk_size) = chunk_size else {
        return;
    };
    let target = temp.path().join("target");
    let create = fs::create_dir_all(&target).await;
    assert!(create.is_ok());
    let link = temp.path().join("link");
    let linked = symlink(&target, &link);
    assert!(linked.is_ok());

    let backend = LocalBackend::new(
        link.join("root"),
        "http://127.0.0.1:8080".to_owned(),
        chunk_size,
    )
    .await;

    assert!(matches!(
        backend,
        Err(ServerError::ObjectStore(ObjectStoreError::Local(
            LocalObjectStoreError::InvalidObjectPath
        )))
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn local_backend_file_record_rejects_oversized_metadata_before_reading() {
    use shardline_server_core::MAX_LOCAL_RECORD_METADATA_BYTES;
    let temp = tempfile::tempdir();
    assert!(temp.is_ok());
    let Ok(temp) = temp else {
        return;
    };
    let chunk_size = NonZeroUsize::new(128);
    assert!(chunk_size.is_some());
    let Some(chunk_size) = chunk_size else {
        return;
    };
    let latest_path = temp.path().join("files").join("asset.bin");
    let created_parent = fs::create_dir_all(temp.path().join("files")).await;
    assert!(created_parent.is_ok());
    let created = fs::File::create(&latest_path).await;
    assert!(created.is_ok());
    let Ok(file) = created else {
        return;
    };
    let resized = file.set_len(MAX_LOCAL_RECORD_METADATA_BYTES + 1).await;
    assert!(resized.is_ok());

    let backend = LocalBackend::new(
        temp.path().to_path_buf(),
        "http://127.0.0.1:8080".to_owned(),
        chunk_size,
    )
    .await;

    assert!(matches!(
        backend,
        Err(ServerError::Index(IndexError::Local(
            LocalIndexStoreError::MetadataTooLarge {
                maximum_bytes: MAX_LOCAL_RECORD_METADATA_BYTES,
                ..
            }
        )))
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn xorb_upload_is_idempotent_and_keeps_serialized_body_readable() {
    let temp = tempfile::tempdir();
    assert!(temp.is_ok());
    let Ok(temp) = temp else {
        return;
    };
    let chunk_size = NonZeroUsize::new(128);
    assert!(chunk_size.is_some());
    let Some(chunk_size) = chunk_size else {
        return;
    };
    let backend = LocalBackend::new(
        temp.path().to_path_buf(),
        "http://127.0.0.1:8080".to_owned(),
        chunk_size,
    )
    .await;
    assert!(backend.is_ok());
    let Ok(backend) = backend else {
        return;
    };
    let (body, hash) = single_chunk_xorb(b"xor");

    let first = backend.upload_xorb(&hash, body.clone()).await;
    let second = backend.upload_xorb(&hash, body.clone()).await;
    let stored_length = backend.xorb_length(&hash).await;

    assert!(first.is_ok());
    assert!(second.is_ok());
    assert!(stored_length.is_ok());
    let (Ok(first), Ok(second), Ok(stored_length)) = (first, second, stored_length) else {
        return;
    };

    assert!(first.was_inserted);
    assert!(!second.was_inserted);
    assert_eq!(stored_length, u64::try_from(body.len()).unwrap_or(0));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shard_registration_rejects_missing_xorb_without_creating_file() {
    let temp = tempfile::tempdir();
    assert!(temp.is_ok());
    let Ok(temp) = temp else {
        return;
    };
    let chunk_size = NonZeroUsize::new(128);
    assert!(chunk_size.is_some());
    let Some(chunk_size) = chunk_size else {
        return;
    };
    let backend = LocalBackend::new(
        temp.path().to_path_buf(),
        "http://127.0.0.1:8080".to_owned(),
        chunk_size,
    )
    .await;
    assert!(backend.is_ok());
    let Ok(backend) = backend else {
        return;
    };
    let (_missing_xorb, missing_hash) = single_chunk_xorb(b"missing");
    let (shard, file_hash) = single_file_shard(&[(b"missing", &missing_hash)]);

    let result = backend
        .upload_shard_stream(
            RequestBodyReader::from_bytes(shard),
            None,
            ShardMetadataLimits::default(),
        )
        .await;
    let latest = backend.reconstruction(&file_hash, None, None, None).await;

    assert!(matches!(result, Err(ServerError::MissingReferencedXorb)));
    assert!(matches!(latest, Err(ServerError::NotFound)));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shard_registration_creates_reconstruction_after_xorbs_exist() {
    let temp = tempfile::tempdir();
    assert!(temp.is_ok());
    let Ok(temp) = temp else {
        return;
    };
    let chunk_size = NonZeroUsize::new(128);
    assert!(chunk_size.is_some());
    let Some(chunk_size) = chunk_size else {
        return;
    };
    let backend = LocalBackend::new(
        temp.path().to_path_buf(),
        "http://127.0.0.1:8080".to_owned(),
        chunk_size,
    )
    .await;
    assert!(backend.is_ok());
    let Ok(backend) = backend else {
        return;
    };
    let (first, first_hash) = single_chunk_xorb(b"aaaa");
    let (second, second_hash) = single_chunk_xorb(b"bbbb");
    let (shard, file_hash) = single_file_shard(&[(b"aaaa", &first_hash), (b"bbbb", &second_hash)]);
    let first_upload = backend.upload_xorb(&first_hash, first).await;
    let second_upload = backend.upload_xorb(&second_hash, second).await;

    assert!(first_upload.is_ok());
    assert!(second_upload.is_ok());
    let response = backend
        .upload_shard_stream(
            RequestBodyReader::from_bytes(shard),
            None,
            ShardMetadataLimits::default(),
        )
        .await;
    let reconstruction = backend.reconstruction(&file_hash, None, None, None).await;
    let bytes = backend.download_file(&file_hash, None, None).await;

    assert!(response.is_ok());
    assert!(reconstruction.is_ok());
    assert!(bytes.is_ok());
    let (Ok(response), Ok(reconstruction), Ok(bytes)) = (response, reconstruction, bytes) else {
        return;
    };

    assert_eq!(response.result, 1);
    assert_eq!(reconstruction.terms.len(), 2);
    assert_eq!(
        reconstruction.terms.first().map(|term| term.hash.as_str()),
        Some(first_hash.as_str())
    );
    assert_eq!(
        reconstruction.terms.get(1).map(|term| term.hash.as_str()),
        Some(second_hash.as_str())
    );
    assert_eq!(bytes, b"aaaabbbb");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn successful_xorb_upload_does_not_create_incoming_body_file() {
    let temp = tempfile::tempdir();
    assert!(temp.is_ok());
    let Ok(temp) = temp else {
        return;
    };
    let chunk_size = NonZeroUsize::new(128);
    assert!(chunk_size.is_some());
    let Some(chunk_size) = chunk_size else {
        return;
    };
    let backend = LocalBackend::new(
        temp.path().to_path_buf(),
        "http://127.0.0.1:8080".to_owned(),
        chunk_size,
    )
    .await;
    assert!(backend.is_ok());
    let Ok(backend) = backend else {
        return;
    };
    let (body, hash) = single_chunk_xorb(b"xor");

    let uploaded = backend.upload_xorb(&hash, body).await;

    assert!(uploaded.is_ok());
    let incoming_exists = temp.path().join("incoming").try_exists();
    assert!(matches!(incoming_exists, Ok(false)));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn successful_shard_upload_does_not_create_staging_directories() {
    let temp = tempfile::tempdir();
    assert!(temp.is_ok());
    let Ok(temp) = temp else {
        return;
    };
    let chunk_size = NonZeroUsize::new(128);
    assert!(chunk_size.is_some());
    let Some(chunk_size) = chunk_size else {
        return;
    };
    let backend = LocalBackend::new(
        temp.path().to_path_buf(),
        "http://127.0.0.1:8080".to_owned(),
        chunk_size,
    )
    .await;
    assert!(backend.is_ok());
    let Ok(backend) = backend else {
        return;
    };
    let (body, hash) = single_chunk_xorb(b"xor");
    let uploaded_xorb = backend.upload_xorb(&hash, body).await;
    assert!(uploaded_xorb.is_ok());
    let (shard, _file_hash) = single_file_shard(&[(b"xor", &hash)]);

    let uploaded_shard = backend
        .upload_shard_stream(
            RequestBodyReader::from_bytes(shard),
            None,
            ShardMetadataLimits::default(),
        )
        .await;

    assert!(uploaded_shard.is_ok());
    let incoming_exists = temp.path().join("incoming").try_exists();
    assert!(matches!(incoming_exists, Ok(false)));
    let shard_workspace_exists = temp.path().join("shards").try_exists();
    assert!(matches!(shard_workspace_exists, Ok(false)));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn repository_scope_namespaces_records_for_same_file_id() {
    let temp = tempfile::tempdir();
    assert!(temp.is_ok());
    let Ok(temp) = temp else {
        return;
    };
    let chunk_size = NonZeroUsize::new(128);
    assert!(chunk_size.is_some());
    let Some(chunk_size) = chunk_size else {
        return;
    };
    let backend = LocalBackend::new(
        temp.path().to_path_buf(),
        "http://127.0.0.1:8080".to_owned(),
        chunk_size,
    )
    .await;
    assert!(backend.is_ok());
    let Ok(backend) = backend else {
        return;
    };
    let left_scope =
        RepositoryScope::new(RepositoryProvider::GitHub, "team-a", "assets", Some("main"));
    let right_scope =
        RepositoryScope::new(RepositoryProvider::GitHub, "team-b", "assets", Some("main"));
    assert!(left_scope.is_ok());
    assert!(right_scope.is_ok());
    let (Ok(left_scope), Ok(right_scope)) = (left_scope, right_scope) else {
        return;
    };

    let left_payload: Vec<u8> = (0_u16..300).map(|x| x as u8).collect();
    let right_payload: Vec<u8> = (300_u16..600).map(|x| x as u8).collect();

    let left = backend
        .upload_file(
            "asset.bin",
            Bytes::from(left_payload.clone()),
            Some(&left_scope),
        )
        .await;
    let right = backend
        .upload_file(
            "asset.bin",
            Bytes::from(right_payload.clone()),
            Some(&right_scope),
        )
        .await;
    assert!(left.is_ok());
    assert!(right.is_ok());

    let left_bytes = backend
        .download_file("asset.bin", None, Some(&left_scope))
        .await;
    let right_bytes = backend
        .download_file("asset.bin", None, Some(&right_scope))
        .await;

    assert!(left_bytes.is_ok());
    assert!(right_bytes.is_ok());
    let (Ok(left_bytes), Ok(right_bytes)) = (left_bytes, right_bytes) else {
        return;
    };
    assert_eq!(left_bytes, left_payload.as_slice());
    assert_eq!(right_bytes, right_payload.as_slice());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn repository_references_xorb_ignores_non_authoritative_legacy_scope_metadata() {
    let temp = tempfile::tempdir();
    assert!(temp.is_ok());
    let Ok(temp) = temp else {
        return;
    };
    let chunk_size = NonZeroUsize::new(128);
    assert!(chunk_size.is_some());
    let Some(chunk_size) = chunk_size else {
        return;
    };
    let backend = LocalBackend::new(
        temp.path().to_path_buf(),
        "http://127.0.0.1:8080".to_owned(),
        chunk_size,
    )
    .await;
    assert!(backend.is_ok());
    let Ok(backend) = backend else {
        return;
    };
    let left_scope =
        RepositoryScope::new(RepositoryProvider::GitHub, "team-a", "assets", Some("main"));
    let right_scope =
        RepositoryScope::new(RepositoryProvider::GitHub, "team-b", "assets", Some("main"));
    assert!(left_scope.is_ok());
    assert!(right_scope.is_ok());
    let (Ok(left_scope), Ok(right_scope)) = (left_scope, right_scope) else {
        return;
    };
    let xorb_hash = "a".repeat(64);
    let misplaced_record = FileRecord {
        file_id: "asset.bin".to_owned(),
        content_hash: "b".repeat(64),
        total_bytes: 4,
        chunk_size: 0,
        storage_repr: shardline_index::StorageRepresentation::FixedChunkV1,
        repository_scope: Some(right_scope),
        chunks: vec![FileChunkRecord {
            hash: xorb_hash.clone(),
            offset: 0,
            length: 4,
            range_start: 0,
            range_end: 1,
            packed_start: 0,
            packed_end: 4,
        }],
    };
    let scope_path = temp
        .path()
        .join("files")
        .join("github")
        .join(hex::encode("team-a"))
        .join(hex::encode("assets"))
        .join(hex::encode("main"));
    let created_scope_path = fs::create_dir_all(&scope_path).await;
    assert!(created_scope_path.is_ok());
    let written = fs::write(
        scope_path.join("asset.bin"),
        to_vec(&misplaced_record).unwrap_or_default(),
    )
    .await;
    assert!(written.is_ok());

    let reachable = backend
        .repository_references_xorb(&xorb_hash, &left_scope)
        .await;

    assert!(matches!(reachable, Ok(false)));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn read_chunk_for_file_version_rejects_unreferenced_chunk() {
    let temp = tempfile::tempdir();
    assert!(temp.is_ok());
    let Ok(temp) = temp else {
        return;
    };
    let chunk_size = NonZeroUsize::new(128);
    assert!(chunk_size.is_some());
    let Some(chunk_size) = chunk_size else {
        return;
    };
    let backend = LocalBackend::new(
        temp.path().to_path_buf(),
        "http://127.0.0.1:8080".to_owned(),
        chunk_size,
    )
    .await;
    assert!(backend.is_ok());
    let Ok(backend) = backend else {
        return;
    };
    let (first, first_hash) = single_chunk_xorb(b"aaaa");
    let (second, second_hash) = single_chunk_xorb(b"bbbb");
    let (shard, file_hash) = single_file_shard(&[(b"aaaa", &first_hash)]);
    let first_upload = backend.upload_xorb(&first_hash, first).await;
    let second_upload = backend.upload_xorb(&second_hash, second).await;
    assert!(first_upload.is_ok());
    assert!(second_upload.is_ok());
    let response = backend
        .upload_shard_stream(
            RequestBodyReader::from_bytes(shard),
            None,
            ShardMetadataLimits::default(),
        )
        .await;
    assert!(response.is_ok());
    let file_record = backend.file_record(&file_hash, None, None).await;
    assert!(file_record.is_ok());
    let Ok(file_record) = file_record else {
        return;
    };

    let read = backend
        .read_chunk_for_file_version(&second_hash, &file_hash, &file_record.content_hash, None)
        .await;

    assert!(matches!(read, Err(ServerError::NotFound)));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn local_backend_ready_succeeds_for_initialized_storage() {
    let temp = tempfile::tempdir();
    assert!(temp.is_ok());
    let Ok(temp) = temp else {
        return;
    };
    let chunk_size = NonZeroUsize::new(128);
    assert!(chunk_size.is_some());
    let Some(chunk_size) = chunk_size else {
        return;
    };
    let backend = LocalBackend::new(
        temp.path().to_path_buf(),
        "http://127.0.0.1:8080".to_owned(),
        chunk_size,
    )
    .await;
    assert!(backend.is_ok());
    let Ok(backend) = backend else {
        return;
    };

    let ready = backend.ready().await;

    assert!(ready.is_ok());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn local_backend_ready_fails_when_local_chunk_root_is_missing() {
    let temp = tempfile::tempdir();
    assert!(temp.is_ok());
    let Ok(temp) = temp else {
        return;
    };
    let chunk_size = NonZeroUsize::new(128);
    assert!(chunk_size.is_some());
    let Some(chunk_size) = chunk_size else {
        return;
    };
    let backend = LocalBackend::new(
        temp.path().to_path_buf(),
        "http://127.0.0.1:8080".to_owned(),
        chunk_size,
    )
    .await;
    assert!(backend.is_ok());
    let Ok(backend) = backend else {
        return;
    };
    let removed = fs::remove_dir_all(temp.path().join("chunks")).await;
    assert!(removed.is_ok());

    let ready = backend.ready().await;

    assert!(ready.is_ok(), "ready() should recreate missing chunk root");
    let chunks_exist = fs::metadata(temp.path().join("chunks")).await;
    assert!(chunks_exist.is_ok(), "chunks directory should be recreated");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn local_backend_ready_fails_when_metadata_database_path_is_directory() {
    let temp = tempfile::tempdir();
    assert!(temp.is_ok());
    let Ok(temp) = temp else {
        return;
    };
    let chunk_size = NonZeroUsize::new(128);
    assert!(chunk_size.is_some());
    let Some(chunk_size) = chunk_size else {
        return;
    };
    let backend = LocalBackend::new(
        temp.path().to_path_buf(),
        "http://127.0.0.1:8080".to_owned(),
        chunk_size,
    )
    .await;
    assert!(backend.is_ok());
    let Ok(backend) = backend else {
        return;
    };
    let removed = fs::remove_file(temp.path().join("metadata.sqlite3")).await;
    assert!(removed.is_ok());
    let created = fs::create_dir_all(temp.path().join("metadata.sqlite3")).await;
    assert!(created.is_ok());

    let ready = backend.ready().await;

    assert!(ready.is_err());
}

// ---------------------------------------------------------------------------
// Focused unit tests for core backend operations
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn local_backend_new_creates_root_and_chunks_directories() {
    let temp = tempfile::tempdir().expect("tempdir");
    let chunk_size = NonZeroUsize::new(128).expect("chunk_size");

    let backend = LocalBackend::new(
        temp.path().to_path_buf(),
        "http://127.0.0.1:8080".to_owned(),
        chunk_size,
    )
    .await
    .expect("backend");

    // The root directory must exist.
    let root_exists = fs::metadata(temp.path()).await.is_ok();
    assert!(
        root_exists,
        "root directory must exist after backend creation"
    );

    // The chunks directory (object store root) must exist.
    let chunks_dir = temp.path().join("chunks");
    let chunks_exists = fs::metadata(&chunks_dir).await.is_ok();
    assert!(
        chunks_exists,
        "chunks directory must exist after backend creation"
    );

    // The backend should be healthy.
    let ready = backend.ready().await;
    assert!(ready.is_ok(), "backend must be ready after creation");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn upload_file_returns_non_empty_content_hash() {
    let temp = tempfile::tempdir().expect("tempdir");
    let chunk_size = NonZeroUsize::new(128).expect("chunk_size");
    let backend = LocalBackend::new(
        temp.path().to_path_buf(),
        "http://127.0.0.1:8080".to_owned(),
        chunk_size,
    )
    .await
    .expect("backend");

    let response = backend
        .upload_file("hello.txt", Bytes::from_static(b"hello world"), None)
        .await
        .expect("upload");

    assert!(
        !response.content_hash.is_empty(),
        "content_hash must not be empty"
    );
    assert_eq!(
        response.total_bytes, 11,
        "total_bytes must match input length"
    );
    assert_eq!(response.file_id, "hello.txt");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn upload_same_file_twice_produces_identical_content_hash() {
    let temp = tempfile::tempdir().expect("tempdir");
    let chunk_size = NonZeroUsize::new(128).expect("chunk_size");
    let backend = LocalBackend::new(
        temp.path().to_path_buf(),
        "http://127.0.0.1:8080".to_owned(),
        chunk_size,
    )
    .await
    .expect("backend");

    let data = b"identical payload for idempotency check";

    let first = backend
        .upload_file("doc.bin", Bytes::from_static(data), None)
        .await
        .expect("first upload");
    let second = backend
        .upload_file("doc.bin", Bytes::from_static(data), None)
        .await
        .expect("second upload");

    assert_eq!(
        first.content_hash, second.content_hash,
        "same file content must yield the same content hash"
    );
    assert_eq!(
        first.total_bytes, second.total_bytes,
        "total_bytes must be stable across uploads"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn download_file_returns_correct_bytes() {
    let temp = tempfile::tempdir().expect("tempdir");
    let chunk_size = NonZeroUsize::new(128).expect("chunk_size");
    let backend = LocalBackend::new(
        temp.path().to_path_buf(),
        "http://127.0.0.1:8080".to_owned(),
        chunk_size,
    )
    .await
    .expect("backend");

    let payload: Vec<u8> = (0_u16..300).map(|x| x as u8).collect();
    let response = backend
        .upload_file("rt.bin", Bytes::from(payload.clone()), None)
        .await
        .expect("upload");

    let downloaded = backend
        .download_file("rt.bin", None, None)
        .await
        .expect("download");

    assert_eq!(downloaded, payload.as_slice());

    // Also verify pinning to a specific content hash.
    let pinned = backend
        .download_file("rt.bin", Some(&response.content_hash), None)
        .await
        .expect("pinned download");
    assert_eq!(pinned, payload.as_slice());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn file_record_returns_correct_metadata_after_upload() {
    let temp = tempfile::tempdir().expect("tempdir");
    let chunk_size = NonZeroUsize::new(128).expect("chunk_size");
    let backend = LocalBackend::new(
        temp.path().to_path_buf(),
        "http://127.0.0.1:8080".to_owned(),
        chunk_size,
    )
    .await
    .expect("backend");

    let payload = b"metadata round-trip payload";
    let upload_response = backend
        .upload_file("meta.bin", Bytes::from_static(payload), None)
        .await
        .expect("upload");

    let record = backend
        .file_record("meta.bin", None, None)
        .await
        .expect("file_record");

    assert_eq!(record.file_id, "meta.bin");
    assert_eq!(
        record.content_hash, upload_response.content_hash,
        "record content_hash must match upload response"
    );
    assert_eq!(
        record.total_bytes,
        u64::try_from(payload.len()).unwrap(),
        "record total_bytes must match input length"
    );
    assert!(
        !record.chunks.is_empty(),
        "record must reference at least one chunk"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn file_record_nonexistent_file_returns_not_found() {
    let temp = tempfile::tempdir().expect("tempdir");
    let chunk_size = NonZeroUsize::new(128).expect("chunk_size");
    let backend = LocalBackend::new(
        temp.path().to_path_buf(),
        "http://127.0.0.1:8080".to_owned(),
        chunk_size,
    )
    .await
    .expect("backend");

    let result = backend.file_record("does-not-exist.bin", None, None).await;

    assert!(
        matches!(result, Err(ServerError::NotFound)),
        "expected NotFound for non-existent file, got: {result:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn file_record_with_content_hash_nonexistent_returns_not_found() {
    let temp = tempfile::tempdir().expect("tempdir");
    let chunk_size = NonZeroUsize::new(128).expect("chunk_size");
    let backend = LocalBackend::new(
        temp.path().to_path_buf(),
        "http://127.0.0.1:8080".to_owned(),
        chunk_size,
    )
    .await
    .expect("backend");

    // Upload a real file first, then request with a bogus content hash.
    backend
        .upload_file("exists.bin", Bytes::from_static(b"data"), None)
        .await
        .expect("upload");

    let result = backend
        .file_record("exists.bin", Some(&"a".repeat(64)), None)
        .await;

    assert!(
        matches!(result, Err(ServerError::NotFound)),
        "expected NotFound for non-existent content hash, got: {result:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn xorb_upload_stored_and_length_matches() {
    let temp = tempfile::tempdir().expect("tempdir");
    let chunk_size = NonZeroUsize::new(128).expect("chunk_size");
    let backend = LocalBackend::new(
        temp.path().to_path_buf(),
        "http://127.0.0.1:8080".to_owned(),
        chunk_size,
    )
    .await
    .expect("backend");

    let (body, hash) = single_chunk_xorb(b"xorb test payload");
    let expected_length = u64::try_from(body.len()).expect("body length");

    let response = backend
        .upload_xorb(&hash, body.clone())
        .await
        .expect("upload xorb");
    assert!(response.was_inserted, "first upload must insert");

    let stored_length = backend.xorb_length(&hash).await.expect("xorb length");
    assert_eq!(stored_length, expected_length);

    // Second upload must be idempotent.
    let second = backend
        .upload_xorb(&hash, body)
        .await
        .expect("second upload");
    assert!(!second.was_inserted, "second upload must not re-insert");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn xorb_read_returns_correct_bytes() {
    let temp = tempfile::tempdir().expect("tempdir");
    let chunk_size = NonZeroUsize::new(128).expect("chunk_size");
    let backend = LocalBackend::new(
        temp.path().to_path_buf(),
        "http://127.0.0.1:8080".to_owned(),
        chunk_size,
    )
    .await
    .expect("backend");

    let (body, hash) = single_chunk_xorb(b"readback verification");

    backend
        .upload_xorb(&hash, body.clone())
        .await
        .expect("upload xorb");

    // Read back via the xorb object key and object store.
    let object_key = crate::xet_adapter::xorb_object_key(&hash).expect("xorb object key");
    let read_back = backend
        .read_object(&object_key)
        .await
        .expect("read xorb object");
    assert_eq!(
        read_back,
        body.as_ref(),
        "read xorb bytes must match uploaded body"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn upload_file_with_repository_scope_round_trip() {
    let temp = tempfile::tempdir().expect("tempdir");
    let chunk_size = NonZeroUsize::new(128).expect("chunk_size");
    let backend = LocalBackend::new(
        temp.path().to_path_buf(),
        "http://127.0.0.1:8080".to_owned(),
        chunk_size,
    )
    .await
    .expect("backend");

    let scope = RepositoryScope::new(RepositoryProvider::GitHub, "org", "repo", Some("main"))
        .expect("scope");

    let payload: Vec<u8> = (0_u16..300).map(|x| x as u8).collect();
    let response = backend
        .upload_file("scoped.bin", Bytes::from(payload.clone()), Some(&scope))
        .await
        .expect("upload");

    // Record retrieval must use the same scope.
    let record = backend
        .file_record("scoped.bin", None, Some(&scope))
        .await
        .expect("file_record with scope");
    assert_eq!(record.content_hash, response.content_hash);
    assert_eq!(record.total_bytes, u64::try_from(payload.len()).unwrap());

    // Download must use the same scope.
    let downloaded = backend
        .download_file("scoped.bin", None, Some(&scope))
        .await
        .expect("download with scope");
    assert_eq!(downloaded, payload.as_slice());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn download_file_nonexistent_returns_not_found() {
    let temp = tempfile::tempdir().expect("tempdir");
    let chunk_size = NonZeroUsize::new(128).expect("chunk_size");
    let backend = LocalBackend::new(
        temp.path().to_path_buf(),
        "http://127.0.0.1:8080".to_owned(),
        chunk_size,
    )
    .await
    .expect("backend");

    let result = backend.download_file("ghost.bin", None, None).await;

    assert!(
        matches!(result, Err(ServerError::NotFound)),
        "expected NotFound for non-existent file, got: {result:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn record_chunk_metadata_matches_chunk_size() {
    let temp = tempfile::tempdir().expect("tempdir");
    let chunk_size = NonZeroUsize::new(128).expect("chunk_size");
    let backend = LocalBackend::new(
        temp.path().to_path_buf(),
        "http://127.0.0.1:8080".to_owned(),
        chunk_size,
    )
    .await
    .expect("backend");

    // 450 bytes with CDC chunk_size=128 should produce 3+ chunks.
    let payload: Vec<u8> = (0_u16..450).map(|x| x as u8).collect();
    let payload_bytes = Bytes::from(payload.clone());
    backend
        .upload_file("three.bin", payload_bytes, None)
        .await
        .expect("upload");

    let record = backend
        .file_record("three.bin", None, None)
        .await
        .expect("file_record");

    assert!(
        record.chunks.len() >= 3,
        "must produce at least 3 chunks, got {}",
        record.chunks.len()
    );
    assert_eq!(record.total_bytes, 450);
    assert_eq!(record.chunk_size, chunk_size.get() as u64);

    // Each chunk's offset must be contiguous.
    let mut expected_offset = 0_u64;
    for (i, chunk) in record.chunks.iter().enumerate() {
        assert_eq!(
            chunk.offset, expected_offset,
            "chunk {i} must start at correct offset"
        );
        expected_offset = expected_offset
            .checked_add(chunk.length)
            .expect("offset overflow");
    }
    assert_eq!(expected_offset, record.total_bytes);
}

/// Regression: a 12-byte (single CDC chunk) upload is xorb-backed on ingest. The
/// record references the xorb hash; the download path must resolve the xorb and
/// return byte-identical content (and correct sub-ranges).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn download_single_chunk_xorb_backed_file_roundtrips() {
    use futures_util::TryStreamExt;
    use shardline_protocol::ByteRange;

    let temp = tempfile::tempdir().expect("tempdir");
    let chunk_size = NonZeroUsize::new(1024).expect("chunk_size");
    let backend = LocalBackend::new(
        temp.path().to_path_buf(),
        "http://127.0.0.1:8080".to_owned(),
        chunk_size,
    )
    .await
    .expect("backend");

    let payload: Vec<u8> = b"aaaabbbbcccc".to_vec();
    backend
        .upload_file("small.bin", Bytes::from(payload.clone()), None)
        .await
        .expect("upload");

    let downloaded = backend
        .download_file("small.bin", None, None)
        .await
        .expect("download");
    assert_eq!(downloaded, payload.as_slice());

    // Sub-range read over the same single-chunk xorb-backed record.
    let (stream, _total) = backend
        .read_file_stream(
            "small.bin",
            None,
            Some(ByteRange::new(4, 7).expect("range")),
        )
        .await
        .expect("range stream");
    let mut stream = stream;
    let mut range_bytes = Vec::new();
    while let Some(chunk) = stream.try_next().await.expect("stream item") {
        range_bytes.extend_from_slice(&chunk);
    }
    assert_eq!(range_bytes, &payload[4..=7]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stats_scoped_reports_only_the_requested_repository() {
    let temp = tempfile::tempdir();
    assert!(temp.is_ok());
    let Ok(temp) = temp else {
        return;
    };
    let chunk_size = NonZeroUsize::new(128);
    assert!(chunk_size.is_some());
    let Some(chunk_size) = chunk_size else {
        return;
    };
    let backend = LocalBackend::new(
        temp.path().to_path_buf(),
        "http://127.0.0.1:8080".to_owned(),
        chunk_size,
    )
    .await;
    assert!(backend.is_ok());
    let Ok(backend) = backend else {
        return;
    };

    let repo_a = RepositoryScope::new(RepositoryProvider::Generic, "team-a", "assets", None)
        .expect("valid repo scope");
    let repo_b = RepositoryScope::new(RepositoryProvider::Generic, "team-b", "assets", None)
        .expect("valid repo scope");
    let repo_c = RepositoryScope::new(RepositoryProvider::Generic, "team-c", "assets", None)
        .expect("valid repo scope");

    // Same payload in both repositories: chunk hashes deduplicate globally,
    // and the chunk pool is shared CAS infrastructure, so every scoped view
    // reports the whole-store chunk count.
    let payload: Vec<u8> = (0_u16..450).map(|x| x as u8).collect();
    backend
        .upload_file("a.bin", Bytes::from(payload.clone()), Some(&repo_a))
        .await
        .expect("upload to repo a");
    backend
        .upload_file("b.bin", Bytes::from(payload.clone()), Some(&repo_b))
        .await
        .expect("upload to repo b");

    let scoped_a = backend.stats_scoped(&repo_a).await.expect("scoped stats a");
    let scoped_b = backend.stats_scoped(&repo_b).await.expect("scoped stats b");
    let scoped_c = backend.stats_scoped(&repo_c).await.expect("scoped stats c");
    let whole = backend.stats().await.expect("whole-store stats");

    assert_eq!(scoped_a.files, 1, "repo a sees only its own file");
    assert_eq!(scoped_b.files, 1, "repo b sees only its own file");
    assert_eq!(scoped_c.files, 0, "empty repo sees zero files");
    assert_eq!(whole.files, 2, "whole-store view counts both repositories");
    assert_eq!(
        scoped_a.chunks, whole.chunks,
        "the chunk pool is shared CAS infrastructure: scoped views report it whole-store"
    );
    assert_eq!(
        scoped_a.chunk_bytes, whole.chunk_bytes,
        "the chunk pool is shared CAS infrastructure: scoped views report it whole-store"
    );
    assert_eq!(
        scoped_c.chunks, whole.chunks,
        "even an empty repository sees the shared chunk pool"
    );
    assert_eq!(
        scoped_c.chunk_bytes, whole.chunk_bytes,
        "even an empty repository sees the shared chunk pool"
    );
}
