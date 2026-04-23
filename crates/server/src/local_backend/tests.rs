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
    record_store::MAX_LOCAL_RECORD_METADATA_BYTES,
    test_fixtures::{single_chunk_xorb, single_file_shard},
    upload_ingest::RequestBodyReader,
};

#[tokio::test]
async fn local_backend_reuses_unchanged_chunks() {
    let temp = tempfile::tempdir();
    assert!(temp.is_ok());
    let Ok(temp) = temp else {
        return;
    };
    let chunk_size = NonZeroUsize::new(4);
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

    let first = backend
        .upload_file("asset.bin", Bytes::from_static(b"aaaabbbbcccc"), None)
        .await;
    let second = backend
        .upload_file("asset.bin", Bytes::from_static(b"aaaabZZZcccc"), None)
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

    assert_eq!(first.inserted_chunks, 3);
    assert_eq!(second.inserted_chunks, 1);
    assert_eq!(second.reused_chunks, 2);
    assert_eq!(latest_bytes, b"aaaabZZZcccc");
    assert_eq!(first_bytes, b"aaaabbbbcccc");
    assert_eq!(stats.chunks, 4);
}

#[cfg(unix)]
#[tokio::test]
async fn local_backend_stats_fail_closed_on_symlinked_file_inventory_escape() {
    let temp = tempfile::tempdir();
    assert!(temp.is_ok());
    let Ok(temp) = temp else {
        return;
    };
    let chunk_size = NonZeroUsize::new(4);
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

    assert!(matches!(
        stats,
        Err(ServerError::IndexStore(LocalIndexStoreError::Io(error)))
            if error.kind() == ErrorKind::InvalidData
    ));
}

#[cfg(unix)]
#[tokio::test]
async fn local_backend_ready_rejects_symlinked_metadata_database_path() {
    let temp = tempfile::tempdir();
    assert!(temp.is_ok());
    let Ok(temp) = temp else {
        return;
    };
    let chunk_size = NonZeroUsize::new(4);
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
    let external_database = temp.path().join("external-metadata.sqlite3");
    let linked = symlink(&external_database, temp.path().join("metadata.sqlite3"));
    assert!(linked.is_ok());

    let restarted = LocalBackend::new(
        temp.path().to_path_buf(),
        "http://127.0.0.1:8080".to_owned(),
        chunk_size,
    )
    .await;
    assert!(restarted.is_ok());
    let Ok(restarted) = restarted else {
        return;
    };

    let ready = restarted.ready().await;

    assert!(matches!(
        ready,
        Err(ServerError::IndexStore(LocalIndexStoreError::Io(error)))
            if error.kind() == ErrorKind::InvalidData
    ));
}

#[cfg(unix)]
#[tokio::test]
async fn local_backend_new_rejects_symlinked_root_ancestor() {
    let temp = tempfile::tempdir();
    assert!(temp.is_ok());
    let Ok(temp) = temp else {
        return;
    };
    let chunk_size = NonZeroUsize::new(4);
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
        Err(ServerError::ObjectStore(
            LocalObjectStoreError::InvalidObjectPath
        ))
    ));
}

#[tokio::test]
async fn local_backend_file_record_rejects_oversized_metadata_before_reading() {
    let temp = tempfile::tempdir();
    assert!(temp.is_ok());
    let Ok(temp) = temp else {
        return;
    };
    let chunk_size = NonZeroUsize::new(4);
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

    let record = backend.file_record("asset.bin", None, None).await;

    assert!(matches!(
        record,
        Err(ServerError::IndexStore(
            LocalIndexStoreError::MetadataTooLarge {
                maximum_bytes: MAX_LOCAL_RECORD_METADATA_BYTES,
                ..
            }
        ))
    ));
}

#[tokio::test]
async fn xorb_upload_is_idempotent_and_keeps_serialized_body_readable() {
    let temp = tempfile::tempdir();
    assert!(temp.is_ok());
    let Ok(temp) = temp else {
        return;
    };
    let chunk_size = NonZeroUsize::new(4);
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

#[tokio::test]
async fn shard_registration_rejects_missing_xorb_without_creating_file() {
    let temp = tempfile::tempdir();
    assert!(temp.is_ok());
    let Ok(temp) = temp else {
        return;
    };
    let chunk_size = NonZeroUsize::new(4);
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

#[tokio::test]
async fn shard_registration_creates_reconstruction_after_xorbs_exist() {
    let temp = tempfile::tempdir();
    assert!(temp.is_ok());
    let Ok(temp) = temp else {
        return;
    };
    let chunk_size = NonZeroUsize::new(4);
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

#[tokio::test]
async fn successful_xorb_upload_does_not_create_incoming_body_file() {
    let temp = tempfile::tempdir();
    assert!(temp.is_ok());
    let Ok(temp) = temp else {
        return;
    };
    let chunk_size = NonZeroUsize::new(4);
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

#[tokio::test]
async fn successful_shard_upload_does_not_create_staging_directories() {
    let temp = tempfile::tempdir();
    assert!(temp.is_ok());
    let Ok(temp) = temp else {
        return;
    };
    let chunk_size = NonZeroUsize::new(4);
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

#[tokio::test]
async fn repository_scope_namespaces_records_for_same_file_id() {
    let temp = tempfile::tempdir();
    assert!(temp.is_ok());
    let Ok(temp) = temp else {
        return;
    };
    let chunk_size = NonZeroUsize::new(4);
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

    let left = backend
        .upload_file(
            "asset.bin",
            Bytes::from_static(b"aaaabbbb"),
            Some(&left_scope),
        )
        .await;
    let right = backend
        .upload_file(
            "asset.bin",
            Bytes::from_static(b"ccccdddd"),
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
    assert_eq!(left_bytes, b"aaaabbbb");
    assert_eq!(right_bytes, b"ccccdddd");
}

#[tokio::test]
async fn repository_references_xorb_fails_closed_on_misplaced_legacy_scope_metadata() {
    let temp = tempfile::tempdir();
    assert!(temp.is_ok());
    let Ok(temp) = temp else {
        return;
    };
    let chunk_size = NonZeroUsize::new(4);
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

    assert!(matches!(
        reachable,
        Err(ServerError::IndexStore(LocalIndexStoreError::Io(error)))
            if error.kind() == ErrorKind::InvalidData
    ));
}

#[tokio::test]
async fn read_chunk_for_file_version_rejects_unreferenced_chunk() {
    let temp = tempfile::tempdir();
    assert!(temp.is_ok());
    let Ok(temp) = temp else {
        return;
    };
    let chunk_size = NonZeroUsize::new(4);
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

#[tokio::test]
async fn local_backend_ready_succeeds_for_initialized_storage() {
    let temp = tempfile::tempdir();
    assert!(temp.is_ok());
    let Ok(temp) = temp else {
        return;
    };
    let chunk_size = NonZeroUsize::new(4);
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

#[tokio::test]
async fn local_backend_ready_fails_when_local_chunk_root_is_missing() {
    let temp = tempfile::tempdir();
    assert!(temp.is_ok());
    let Ok(temp) = temp else {
        return;
    };
    let chunk_size = NonZeroUsize::new(4);
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

    assert!(ready.is_err());
}

#[tokio::test]
async fn local_backend_ready_fails_when_metadata_database_path_is_directory() {
    let temp = tempfile::tempdir();
    assert!(temp.is_ok());
    let Ok(temp) = temp else {
        return;
    };
    let chunk_size = NonZeroUsize::new(4);
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
    let created = fs::create_dir_all(temp.path().join("metadata.sqlite3")).await;
    assert!(created.is_ok());

    let ready = backend.ready().await;

    assert!(ready.is_err());
}
