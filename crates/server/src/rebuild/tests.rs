use std::{error::Error, num::NonZeroUsize, time::Duration};

use axum::body::Bytes;
use rusqlite::{Connection, params};
use serde_json::{from_slice, to_vec};
use shardline_index::{
    FileChunkRecord, FileId, FileReconstruction, FileRecord, IndexStore, LocalIndexStore,
    ReconstructionTerm, RecordStore, StoredObjectId, parse_xet_hash_hex, xet_hash_hex_string,
};
use shardline_protocol::{ChunkRange, RepositoryProvider, RepositoryScope, ShardlineHash};
use tokio::{fs, time::sleep};

use super::{LocalIndexRebuildIssueKind, run_local_index_rebuild};
use crate::{
    LocalBackend, ServerError, ShardMetadataLimits,
    record_store::LocalRecordStore,
    test_fixtures::{single_chunk_xorb, single_file_shard},
    test_invariant_error::ServerTestInvariantError,
    upload_ingest::RequestBodyReader,
};

const LARGE_METADATA_RECORD_COUNT: usize = 128;

fn checked_inventory_value(index: u64, delta: u64) -> Result<u64, Box<dyn Error>> {
    index
        .checked_add(delta)
        .ok_or_else(|| ServerTestInvariantError::new("inventory index overflow").into())
}

fn synthetic_large_inventory_record(index: usize) -> Result<FileRecord, Box<dyn Error>> {
    let index_u64 = u64::try_from(index)?;
    let provider = match index % 3 {
        0 => RepositoryProvider::GitHub,
        1 => RepositoryProvider::GitLab,
        _ => RepositoryProvider::Gitea,
    };
    let owner = format!("team-{}", index % 8);
    let name = format!("assets-{}", index % 5);
    let revision = format!("rev-{}", index % 13);
    let scope = RepositoryScope::new(provider, &owner, &name, Some(&revision))?;
    let content_hash = format!("{:064x}", checked_inventory_value(index_u64, 1)?);
    let chunk_hash = format!("{:064x}", checked_inventory_value(index_u64, 1_000)?);

    Ok(FileRecord {
        file_id: format!("asset-{index:04}.bin"),
        content_hash,
        total_bytes: 16,
        chunk_size: 16,
        repository_scope: Some(scope),
        chunks: vec![FileChunkRecord {
            hash: chunk_hash,
            offset: 0,
            length: 16,
            range_start: 0,
            range_end: 1,
            packed_start: 0,
            packed_end: 16,
        }],
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn index_rebuild_restores_missing_latest_record() {
    let storage = tempfile::tempdir();
    assert!(storage.is_ok());
    let Ok(storage) = storage else {
        return;
    };
    let backend = LocalBackend::new(
        storage.path().to_path_buf(),
        "http://127.0.0.1:8080".to_owned(),
        NonZeroUsize::new(4).unwrap_or(NonZeroUsize::MIN),
    )
    .await;
    assert!(backend.is_ok());
    let Ok(backend) = backend else {
        return;
    };

    let first = backend
        .upload_file("asset.bin", Bytes::from_static(b"aaaabbbbcccc"), None)
        .await;
    assert!(first.is_ok());
    sleep(Duration::from_millis(10)).await;
    let second = backend
        .upload_file("asset.bin", Bytes::from_static(b"aaaaZZZZcccc"), None)
        .await;
    assert!(second.is_ok());

    let record_store = LocalRecordStore::open(storage.path().to_path_buf());
    let latest_record = backend.file_record("asset.bin", None, None).await;
    assert!(latest_record.is_ok());
    let Ok(latest_record) = latest_record else {
        return;
    };
    let latest_locator = RecordStore::latest_record_locator(&record_store, &latest_record);
    let removed = RecordStore::delete_record_locator(&record_store, &latest_locator).await;
    assert!(removed.is_ok());

    let report = run_local_index_rebuild(storage.path().to_path_buf()).await;
    assert!(report.is_ok());
    let Ok(report) = report else {
        return;
    };
    let latest = backend.download_file("asset.bin", None, None).await;
    assert!(latest.is_ok());
    let Ok(latest) = latest else {
        return;
    };

    assert_eq!(report.scanned_version_records, 2);
    assert_eq!(report.rebuilt_latest_records, 1);
    assert_eq!(report.unchanged_latest_records, 0);
    assert_eq!(report.removed_stale_latest_records, 0);
    assert!(report.is_clean());
    assert_eq!(latest, b"aaaaZZZZcccc");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn index_rebuild_reaches_a_fixed_point_on_second_run() {
    let storage = tempfile::tempdir();
    assert!(storage.is_ok());
    let Ok(storage) = storage else {
        return;
    };
    let record_store = LocalRecordStore::open(storage.path().to_path_buf());
    let record = FileRecord {
        file_id: "asset.bin".to_owned(),
        content_hash: xet_hash_hex_string(ShardlineHash::from_bytes([71; 32])),
        total_bytes: 4,
        chunk_size: 4,
        repository_scope: None,
        chunks: vec![FileChunkRecord {
            hash: xet_hash_hex_string(ShardlineHash::from_bytes([72; 32])),
            offset: 0,
            length: 4,
            range_start: 0,
            range_end: 1,
            packed_start: 0,
            packed_end: 4,
        }],
    };
    let written = RecordStore::write_version_record(&record_store, &record).await;
    assert!(written.is_ok());

    let first_report = run_local_index_rebuild(storage.path().to_path_buf()).await;
    let second_report = run_local_index_rebuild(storage.path().to_path_buf()).await;
    assert!(first_report.is_ok());
    assert!(second_report.is_ok());
    let (Ok(first_report), Ok(second_report)) = (first_report, second_report) else {
        return;
    };

    assert_eq!(first_report.scanned_version_records, 1);
    assert_eq!(first_report.rebuilt_latest_records, 1);
    assert_eq!(first_report.unchanged_latest_records, 0);
    assert!(first_report.is_clean());
    assert_eq!(second_report.scanned_version_records, 1);
    assert_eq!(second_report.rebuilt_latest_records, 0);
    assert_eq!(second_report.unchanged_latest_records, 1);
    assert!(second_report.is_clean());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn index_rebuild_prunes_stale_latest_record_without_versions() {
    let storage = tempfile::tempdir();
    assert!(storage.is_ok());
    let Ok(storage) = storage else {
        return;
    };
    let backend = LocalBackend::new(
        storage.path().to_path_buf(),
        "http://127.0.0.1:8080".to_owned(),
        NonZeroUsize::new(4).unwrap_or(NonZeroUsize::MIN),
    )
    .await;
    assert!(backend.is_ok());
    let Ok(backend) = backend else {
        return;
    };

    let uploaded = backend
        .upload_file("asset.bin", Bytes::from_static(b"aaaabbbbcccc"), None)
        .await;
    assert!(uploaded.is_ok());
    let Ok(uploaded) = uploaded else {
        return;
    };

    let record_store = LocalRecordStore::open(storage.path().to_path_buf());
    let version_record = backend
        .file_record("asset.bin", Some(&uploaded.content_hash), None)
        .await;
    assert!(version_record.is_ok());
    let Ok(version_record) = version_record else {
        return;
    };
    let version_locator = RecordStore::version_record_locator(&record_store, &version_record);
    let removed = RecordStore::delete_record_locator(&record_store, &version_locator).await;
    assert!(removed.is_ok());

    let report = run_local_index_rebuild(storage.path().to_path_buf()).await;
    assert!(report.is_ok());
    let Ok(report) = report else {
        return;
    };
    let latest = backend.download_file("asset.bin", None, None).await;

    assert_eq!(report.scanned_version_records, 0);
    assert_eq!(report.rebuilt_latest_records, 0);
    assert_eq!(report.unchanged_latest_records, 0);
    assert_eq!(report.removed_stale_latest_records, 1);
    assert!(report.is_clean());
    assert!(matches!(latest, Err(ServerError::NotFound)));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn index_rebuild_prunes_stale_reconstruction_rows() {
    let storage = tempfile::tempdir();
    assert!(storage.is_ok());
    let Ok(storage) = storage else {
        return;
    };
    let index_store = LocalIndexStore::open(storage.path().to_path_buf());
    let stale_file_id = FileId::new(ShardlineHash::from_bytes([31; 32]));
    let object_id = StoredObjectId::new(ShardlineHash::from_bytes([32; 32]));
    let range = ChunkRange::new(0, 1);
    assert!(range.is_ok());
    let Ok(range) = range else {
        return;
    };
    let reconstruction =
        FileReconstruction::new(vec![ReconstructionTerm::new(object_id, range, 16)]);
    let inserted = index_store.insert_reconstruction(&stale_file_id, &reconstruction);
    assert!(inserted.is_ok());

    let report = run_local_index_rebuild(storage.path().to_path_buf()).await;
    assert!(report.is_ok());
    let Ok(report) = report else {
        return;
    };

    let loaded = IndexStore::reconstruction(&index_store, &stale_file_id);
    assert!(matches!(loaded, Ok(None)));
    assert_eq!(report.scanned_reconstructions, 1);
    assert_eq!(report.unchanged_reconstructions, 0);
    assert_eq!(report.removed_stale_reconstructions, 1);
    assert!(report.is_clean());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn index_rebuild_preserves_reconstruction_rows_backed_by_version_records() {
    let storage = tempfile::tempdir();
    assert!(storage.is_ok());
    let Ok(storage) = storage else {
        return;
    };
    let record_store = LocalRecordStore::open(storage.path().to_path_buf());
    let index_store = LocalIndexStore::open(storage.path().to_path_buf());
    let file_hash = ShardlineHash::from_bytes([33; 32]);
    let file_id = FileId::new(file_hash);
    let record = FileRecord {
        file_id: xet_hash_hex_string(file_hash),
        content_hash: xet_hash_hex_string(ShardlineHash::from_bytes([34; 32])),
        total_bytes: 0,
        chunk_size: 0,
        repository_scope: None,
        chunks: Vec::new(),
    };
    let written = RecordStore::write_version_record(&record_store, &record).await;
    assert!(written.is_ok());
    let object_id = StoredObjectId::new(ShardlineHash::from_bytes([35; 32]));
    let range = ChunkRange::new(0, 1);
    assert!(range.is_ok());
    let Ok(range) = range else {
        return;
    };
    let reconstruction =
        FileReconstruction::new(vec![ReconstructionTerm::new(object_id, range, 16)]);
    let inserted = index_store.insert_reconstruction(&file_id, &reconstruction);
    assert!(inserted.is_ok());

    let report = run_local_index_rebuild(storage.path().to_path_buf()).await;
    assert!(report.is_ok());
    let Ok(report) = report else {
        return;
    };

    let loaded = IndexStore::reconstruction(&index_store, &file_id);
    assert!(matches!(loaded, Ok(Some(ref loaded)) if loaded == &reconstruction));
    assert_eq!(report.scanned_reconstructions, 1);
    assert_eq!(report.unchanged_reconstructions, 1);
    assert_eq!(report.removed_stale_reconstructions, 0);
    assert!(report.is_clean());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn index_rebuild_restores_repository_scoped_latest_records() {
    let storage = tempfile::tempdir();
    assert!(storage.is_ok());
    let Ok(storage) = storage else {
        return;
    };
    let backend = LocalBackend::new(
        storage.path().to_path_buf(),
        "http://127.0.0.1:8080".to_owned(),
        NonZeroUsize::new(4).unwrap_or(NonZeroUsize::MIN),
    )
    .await;
    assert!(backend.is_ok());
    let Ok(backend) = backend else {
        return;
    };
    let team_assets =
        RepositoryScope::new(RepositoryProvider::GitHub, "team", "assets", Some("main"));
    assert!(team_assets.is_ok());
    let Ok(team_assets) = team_assets else {
        return;
    };
    let team_backup =
        RepositoryScope::new(RepositoryProvider::GitHub, "team", "backup", Some("main"));
    assert!(team_backup.is_ok());
    let Ok(team_backup) = team_backup else {
        return;
    };

    let first = backend
        .upload_file(
            "asset.bin",
            Bytes::from_static(b"aaaabbbbcccc"),
            Some(&team_assets),
        )
        .await;
    let second = backend
        .upload_file(
            "asset.bin",
            Bytes::from_static(b"111122223333"),
            Some(&team_backup),
        )
        .await;
    assert!(first.is_ok());
    assert!(second.is_ok());

    let record_store = LocalRecordStore::open(storage.path().to_path_buf());
    let first_record = backend
        .file_record("asset.bin", None, Some(&team_assets))
        .await;
    let second_record = backend
        .file_record("asset.bin", None, Some(&team_backup))
        .await;
    assert!(first_record.is_ok());
    assert!(second_record.is_ok());
    let (Ok(first_record), Ok(second_record)) = (first_record, second_record) else {
        return;
    };
    let first_locator = RecordStore::latest_record_locator(&record_store, &first_record);
    let second_locator = RecordStore::latest_record_locator(&record_store, &second_record);
    let removed_first = RecordStore::delete_record_locator(&record_store, &first_locator).await;
    let removed_second = RecordStore::delete_record_locator(&record_store, &second_locator).await;
    assert!(removed_first.is_ok());
    assert!(removed_second.is_ok());

    let report = run_local_index_rebuild(storage.path().to_path_buf()).await;
    assert!(report.is_ok());
    let Ok(report) = report else {
        return;
    };
    let restored_first = backend
        .download_file("asset.bin", None, Some(&team_assets))
        .await;
    let restored_second = backend
        .download_file("asset.bin", None, Some(&team_backup))
        .await;
    assert!(restored_first.is_ok());
    assert!(restored_second.is_ok());
    let (Ok(restored_first), Ok(restored_second)) = (restored_first, restored_second) else {
        return;
    };

    assert_eq!(report.scanned_version_records, 2);
    assert_eq!(report.rebuilt_latest_records, 2);
    assert!(report.is_clean());
    assert_eq!(restored_first, b"aaaabbbbcccc");
    assert_eq!(restored_second, b"111122223333");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn index_rebuild_scans_large_repository_metadata_inventory() {
    let result = exercise_index_rebuild_scans_large_repository_metadata_inventory().await;
    let error = result.as_ref().err().map(ToString::to_string);
    assert!(
        result.is_ok(),
        "large metadata inventory index rebuild failed: {error:?}"
    );
}

async fn exercise_index_rebuild_scans_large_repository_metadata_inventory()
-> Result<(), Box<dyn Error>> {
    let storage = tempfile::tempdir()?;
    let record_store = LocalRecordStore::open(storage.path().to_path_buf());
    let mut records = Vec::with_capacity(LARGE_METADATA_RECORD_COUNT);

    for index in 0..LARGE_METADATA_RECORD_COUNT {
        let record = synthetic_large_inventory_record(index)?;
        RecordStore::write_version_record(&record_store, &record).await?;
        records.push(record);
    }

    let report = run_local_index_rebuild(storage.path().to_path_buf()).await?;

    assert_eq!(
        report.scanned_version_records,
        u64::try_from(LARGE_METADATA_RECORD_COUNT)?
    );
    assert_eq!(
        report.rebuilt_latest_records,
        u64::try_from(LARGE_METADATA_RECORD_COUNT)?
    );
    assert_eq!(report.unchanged_latest_records, 0);
    assert_eq!(report.removed_stale_latest_records, 0);
    assert!(report.is_clean());

    for record in records.iter().step_by(17) {
        let latest_bytes = RecordStore::read_latest_record_bytes(&record_store, record)
            .await?
            .ok_or_else(|| ServerTestInvariantError::new("missing rebuilt latest record"))?;
        let latest_record = from_slice::<FileRecord>(&latest_bytes)?;
        assert_eq!(latest_record, *record);
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn index_rebuild_reports_invalid_version_record_json() {
    let storage = tempfile::tempdir();
    assert!(storage.is_ok());
    let Ok(storage) = storage else {
        return;
    };
    let record_store = LocalRecordStore::open(storage.path().to_path_buf());
    let record = FileRecord {
        file_id: "asset.bin".to_owned(),
        content_hash: "a".repeat(64),
        total_bytes: 12,
        chunk_size: 4,
        repository_scope: None,
        chunks: vec![FileChunkRecord {
            hash: "b".repeat(64),
            offset: 0,
            length: 12,
            range_start: 0,
            range_end: 1,
            packed_start: 0,
            packed_end: 12,
        }],
    };
    let written = RecordStore::write_version_record(&record_store, &record).await;
    assert!(written.is_ok());
    let connection = Connection::open(storage.path().join("metadata.sqlite3"));
    assert!(connection.is_ok());
    let Ok(connection) = connection else {
        return;
    };
    let updated = connection.execute(
        "UPDATE shardline_file_records
             SET record = ?1
             WHERE record_kind = ?2 AND file_id = ?3 AND content_hash = ?4",
        params!["{not-json", "version", "asset.bin", "a".repeat(64)],
    );
    assert!(updated.is_ok());

    let report = run_local_index_rebuild(storage.path().to_path_buf()).await;
    assert!(report.is_ok());
    let Ok(report) = report else {
        return;
    };

    assert_eq!(report.scanned_version_records, 1);
    assert_eq!(report.rebuilt_latest_records, 0);
    assert_eq!(report.issue_count(), 1);
    let first_issue = report.issues.first();
    assert!(first_issue.is_some());
    let Some(first_issue) = first_issue else {
        return;
    };
    assert_eq!(
        first_issue.kind,
        LocalIndexRebuildIssueKind::InvalidVersionRecordJson
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn index_rebuild_skips_invalid_version_reconstruction_plan() {
    let storage = tempfile::tempdir();
    assert!(storage.is_ok());
    let Ok(storage) = storage else {
        return;
    };
    let backend = LocalBackend::new(
        storage.path().to_path_buf(),
        "http://127.0.0.1:8080".to_owned(),
        NonZeroUsize::new(4).unwrap_or(NonZeroUsize::MIN),
    )
    .await;
    assert!(backend.is_ok());
    let Ok(backend) = backend else {
        return;
    };
    let uploaded = backend
        .upload_file("asset.bin", Bytes::from_static(b"aaaabbbbcccc"), None)
        .await;
    assert!(uploaded.is_ok());
    let Ok(uploaded) = uploaded else {
        return;
    };

    let record_store = LocalRecordStore::open(storage.path().to_path_buf());
    let version_record = backend
        .file_record("asset.bin", Some(&uploaded.content_hash), None)
        .await;
    assert!(version_record.is_ok());
    let Ok(mut version_record) = version_record else {
        return;
    };
    let first_chunk = version_record.chunks.first_mut();
    assert!(first_chunk.is_some());
    let Some(first_chunk) = first_chunk else {
        return;
    };
    first_chunk.packed_end = first_chunk.packed_start;
    let corrupted = to_vec(&version_record);
    assert!(corrupted.is_ok());
    let Ok(corrupted) = corrupted else {
        return;
    };
    let corrupted = String::from_utf8(corrupted);
    assert!(corrupted.is_ok());
    let Ok(corrupted) = corrupted else {
        return;
    };
    let connection = Connection::open(storage.path().join("metadata.sqlite3"));
    assert!(connection.is_ok());
    let Ok(connection) = connection else {
        return;
    };
    let written = connection.execute(
        "UPDATE shardline_file_records
             SET record = ?1
             WHERE record_kind = ?2 AND file_id = ?3 AND content_hash = ?4",
        params![corrupted, "version", "asset.bin", uploaded.content_hash],
    );
    assert!(written.is_ok());

    let latest_locator = RecordStore::latest_record_locator(&record_store, &version_record);
    let removed = RecordStore::delete_record_locator(&record_store, &latest_locator).await;
    assert!(removed.is_ok());

    let report = run_local_index_rebuild(storage.path().to_path_buf()).await;
    assert!(report.is_ok());
    let Ok(report) = report else {
        return;
    };
    let latest = backend.download_file("asset.bin", None, None).await;

    assert_eq!(report.scanned_version_records, 1);
    assert_eq!(report.rebuilt_latest_records, 0);
    assert!(report.issues.iter().any(
            |issue| issue.kind == LocalIndexRebuildIssueKind::InvalidVersionReconstructionPlan
        ));
    assert!(matches!(latest, Err(ServerError::NotFound)));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn index_rebuild_restores_dedupe_shard_mapping_from_retained_shard_objects() {
    let storage = tempfile::tempdir();
    assert!(storage.is_ok());
    let Ok(storage) = storage else {
        return;
    };
    let backend = LocalBackend::new(
        storage.path().to_path_buf(),
        "http://127.0.0.1:8080".to_owned(),
        NonZeroUsize::new(4).unwrap_or(NonZeroUsize::MIN),
    )
    .await;
    assert!(backend.is_ok());
    let Ok(backend) = backend else {
        return;
    };
    let (xorb_body, xorb_hash) = single_chunk_xorb(b"aaaa");
    let (shard_body, _file_hash) = single_file_shard(&[(b"aaaa", &xorb_hash)]);
    let uploaded_xorb = backend.upload_xorb(&xorb_hash, xorb_body).await;
    assert!(uploaded_xorb.is_ok());
    let uploaded_shard = backend
        .upload_shard_stream(
            RequestBodyReader::from_bytes(shard_body),
            None,
            ShardMetadataLimits::default(),
        )
        .await;
    assert!(uploaded_shard.is_ok());

    let index_store = LocalIndexStore::open(storage.path().to_path_buf());
    let chunk_hash = parse_xet_hash_hex(&xorb_hash);
    assert!(chunk_hash.is_ok());
    let Ok(chunk_hash) = chunk_hash else {
        return;
    };
    let deleted = IndexStore::delete_dedupe_shard_mapping(&index_store, &chunk_hash);
    assert!(matches!(deleted, Ok(true)));
    let missing = IndexStore::dedupe_shard_mapping(&index_store, &chunk_hash);
    assert!(matches!(missing, Ok(None)));

    let report = run_local_index_rebuild(storage.path().to_path_buf()).await;
    assert!(report.is_ok());
    let Ok(report) = report else {
        return;
    };

    let restored = IndexStore::dedupe_shard_mapping(&index_store, &chunk_hash);
    assert!(matches!(restored, Ok(Some(_))));
    assert_eq!(report.scanned_retained_shards, 1);
    assert_eq!(report.rebuilt_dedupe_shard_mappings, 1);
    assert_eq!(report.unchanged_dedupe_shard_mappings, 0);
    assert_eq!(report.removed_stale_dedupe_shard_mappings, 0);
    let shard_workspace_exists = storage.path().join("shards").try_exists();
    assert!(matches!(shard_workspace_exists, Ok(false)));
    assert!(report.is_clean());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn index_rebuild_does_not_mutate_dedupe_mappings_when_retained_shard_is_corrupt() {
    let storage = tempfile::tempdir();
    assert!(storage.is_ok());
    let Ok(storage) = storage else {
        return;
    };
    let backend = LocalBackend::new(
        storage.path().to_path_buf(),
        "http://127.0.0.1:8080".to_owned(),
        NonZeroUsize::new(4).unwrap_or(NonZeroUsize::MIN),
    )
    .await;
    assert!(backend.is_ok());
    let Ok(backend) = backend else {
        return;
    };
    let (xorb_body, xorb_hash) = single_chunk_xorb(b"aaaa");
    let (shard_body, _file_hash) = single_file_shard(&[(b"aaaa", &xorb_hash)]);
    let uploaded_xorb = backend.upload_xorb(&xorb_hash, xorb_body).await;
    assert!(uploaded_xorb.is_ok());
    let uploaded_shard = backend
        .upload_shard_stream(
            RequestBodyReader::from_bytes(shard_body),
            None,
            ShardMetadataLimits::default(),
        )
        .await;
    assert!(uploaded_shard.is_ok());

    let index_store = LocalIndexStore::open(storage.path().to_path_buf());
    let chunk_hash = parse_xet_hash_hex(&xorb_hash);
    assert!(chunk_hash.is_ok());
    let Ok(chunk_hash) = chunk_hash else {
        return;
    };
    let existing = IndexStore::dedupe_shard_mapping(&index_store, &chunk_hash);
    assert!(matches!(existing, Ok(Some(_))));
    let Ok(Some(existing)) = existing else {
        return;
    };
    let shard_path = storage
        .path()
        .join("chunks")
        .join(existing.shard_object_key().as_str());
    let corrupted = fs::write(&shard_path, b"not a valid xet shard").await;
    assert!(corrupted.is_ok());

    let report = run_local_index_rebuild(storage.path().to_path_buf()).await;
    assert!(report.is_ok());
    let Ok(report) = report else {
        return;
    };

    let preserved = IndexStore::dedupe_shard_mapping(&index_store, &chunk_hash);
    assert!(matches!(preserved, Ok(Some(_))));
    let Ok(Some(preserved)) = preserved else {
        return;
    };
    assert_eq!(preserved, existing);
    assert_eq!(report.scanned_retained_shards, 1);
    assert_eq!(report.rebuilt_dedupe_shard_mappings, 0);
    assert_eq!(report.unchanged_dedupe_shard_mappings, 0);
    assert_eq!(report.removed_stale_dedupe_shard_mappings, 0);
    assert!(
        report
            .issues
            .iter()
            .any(|issue| { issue.kind == LocalIndexRebuildIssueKind::InvalidRetainedShard })
    );
}
