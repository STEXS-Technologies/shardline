use std::{
    error::Error,
    io::Cursor,
    sync::atomic::{AtomicBool, Ordering},
};

use serde_json::to_vec;
use shardline_index::{
    AsyncIndexStore, DedupeShardMapping, FileChunkRecord, FileId, FileReconstruction, FileRecord,
    IndexStore, IndexStoreFuture, LocalIndexStore, LocalIndexStoreError, ProviderRepositoryState,
    QuarantineCandidate, RecordStore, RetentionHold, WebhookDelivery, XorbId,
};
use shardline_protocol::{
    RepositoryProvider, RepositoryScope, ShardlineHash, try_for_each_serialized_xorb_chunk,
    validate_serialized_xorb,
};
use shardline_storage::ObjectKey;
use shardline_vcs::{
    ProviderKind, RepositoryRef, RepositoryWebhookEvent, RepositoryWebhookEventKind, RevisionRef,
    WebhookDeliveryId,
};
use xet_core_structures::xorb_object::{
    CompressionScheme, SerializedXorbObject,
    xorb_format_test_utils::{ChunkSize, build_raw_xorb},
};

use super::{ProviderWebhookOutcomeKind, apply_provider_webhook_with_stores};
use crate::{
    ServerError,
    chunk_store::chunk_object_key,
    object_store::ServerObjectStore,
    record_store::LocalRecordStore,
    test_invariant_error::ServerTestInvariantError,
    xorb_store::{normalize_serialized_xorb, store_uploaded_xorb, xorb_object_key},
};

async fn local_latest_record_exists(
    record_store: &LocalRecordStore,
    record: &FileRecord,
) -> Result<bool, Box<dyn Error>> {
    let locator = RecordStore::latest_record_locator(record_store, record);
    Ok(RecordStore::record_locator_exists(record_store, &locator).await?)
}

async fn local_version_record_exists(
    record_store: &LocalRecordStore,
    record: &FileRecord,
) -> Result<bool, Box<dyn Error>> {
    let locator = RecordStore::version_record_locator(record_store, record);
    Ok(RecordStore::record_locator_exists(record_store, &locator).await?)
}

async fn local_version_record_bytes(
    record_store: &LocalRecordStore,
    record: &FileRecord,
) -> Result<Vec<u8>, Box<dyn Error>> {
    let locator = RecordStore::version_record_locator(record_store, record);
    Ok(RecordStore::read_record_bytes(record_store, &locator).await?)
}

async fn local_latest_record_bytes(
    record_store: &LocalRecordStore,
    record: &FileRecord,
) -> Result<Vec<u8>, Box<dyn Error>> {
    let locator = RecordStore::latest_record_locator(record_store, record);
    Ok(RecordStore::read_record_bytes(record_store, &locator).await?)
}

#[tokio::test]
async fn repository_deleted_creates_holds_for_matching_repository_versions() {
    let result = exercise_repository_deleted_creates_holds_for_matching_repository_versions().await;
    let error = result.as_ref().err().map(ToString::to_string);
    assert!(
        result.is_ok(),
        "provider webhook repository deletion hold flow failed: {error:?}"
    );
}

#[tokio::test]
async fn repository_deleted_removes_stale_latest_without_version_record() {
    let result = exercise_repository_deleted_removes_stale_latest_without_version_record().await;
    let error = result.as_ref().err().map(ToString::to_string);
    assert!(
        result.is_ok(),
        "provider webhook repository deletion stale latest cleanup failed: {error:?}"
    );
}

#[tokio::test]
async fn repository_deleted_holds_native_xet_xorb_and_unpacked_chunks() {
    let result = exercise_repository_deleted_holds_native_xet_xorb_and_unpacked_chunks().await;
    let error = result.as_ref().err().map(ToString::to_string);
    assert!(
        result.is_ok(),
        "provider webhook repository deletion native xet retention failed: {error:?}"
    );
}

#[tokio::test]
async fn access_changed_records_provider_repository_state_without_mutating_file_metadata() {
    let result =
        exercise_access_changed_records_provider_repository_state_without_mutating_file_metadata()
            .await;
    let error = result.as_ref().err().map(ToString::to_string);
    assert!(
        result.is_ok(),
        "provider webhook access-changed state update contract failed: {error:?}"
    );
}

#[tokio::test]
async fn revision_pushed_records_provider_repository_state_without_mutating_file_metadata() {
    let result =
        exercise_revision_pushed_records_provider_repository_state_without_mutating_file_metadata()
            .await;
    let error = result.as_ref().err().map(ToString::to_string);
    assert!(
        result.is_ok(),
        "provider webhook revision-pushed state update contract failed: {error:?}"
    );
}

#[tokio::test]
async fn access_change_and_revision_push_state_survives_reordering() {
    let result = exercise_access_change_and_revision_push_state_survives_reordering().await;
    let error = result.as_ref().err().map(ToString::to_string);
    assert!(
        result.is_ok(),
        "provider webhook state reordering flow failed: {error:?}"
    );
}

#[tokio::test]
async fn duplicate_webhook_delivery_is_ignored_after_first_application() {
    let result = exercise_duplicate_webhook_delivery_is_ignored_after_first_application().await;
    let error = result.as_ref().err().map(ToString::to_string);
    assert!(
        result.is_ok(),
        "duplicate webhook delivery no-op flow failed: {error:?}"
    );
}

#[tokio::test]
async fn matching_delivery_ids_in_different_repositories_are_not_replay_collisions() {
    let result =
        exercise_matching_delivery_ids_in_different_repositories_are_not_replay_collisions().await;
    let error = result.as_ref().err().map(ToString::to_string);
    assert!(
        result.is_ok(),
        "repository-scoped webhook replay flow failed: {error:?}"
    );
}

#[tokio::test]
async fn failed_webhook_application_can_retry_same_delivery() {
    let result = exercise_failed_webhook_application_can_retry_same_delivery().await;
    let error = result.as_ref().err().map(ToString::to_string);
    assert!(
        result.is_ok(),
        "failed webhook retry flow failed: {error:?}"
    );
}

#[tokio::test]
async fn repository_rename_migrates_records_to_new_scope() {
    let result = exercise_repository_rename_migrates_records_to_new_scope().await;
    let error = result.as_ref().err().map(ToString::to_string);
    assert!(
        result.is_ok(),
        "provider webhook repository rename migration failed: {error:?}"
    );
}

#[tokio::test]
async fn repository_rename_removes_old_scope_latest_without_version_record() {
    let result = exercise_repository_rename_removes_old_scope_latest_without_version_record().await;
    let error = result.as_ref().err().map(ToString::to_string);
    assert!(
        result.is_ok(),
        "provider webhook repository rename stale latest cleanup failed: {error:?}"
    );
}

#[tokio::test]
async fn repository_rename_rejects_conflicting_target_metadata() {
    let result = exercise_repository_rename_rejects_conflicting_target_metadata().await;
    let error = result.as_ref().err().map(ToString::to_string);
    assert!(
        result.is_ok(),
        "provider webhook repository rename conflict detection failed: {error:?}"
    );
}

#[tokio::test]
async fn previously_recorded_webhook_delivery_is_a_no_op() {
    let result = exercise_previously_recorded_webhook_delivery_is_a_no_op().await;
    let error = result.as_ref().err().map(ToString::to_string);
    assert!(
        result.is_ok(),
        "pre-recorded webhook delivery no-op flow failed: {error:?}"
    );
}

async fn exercise_repository_deleted_creates_holds_for_matching_repository_versions()
-> Result<(), Box<dyn Error>> {
    let storage = tempfile::tempdir()?;
    let record_store = LocalRecordStore::open(storage.path().to_path_buf());
    let index_store = LocalIndexStore::new(storage.path().to_path_buf())?;
    let object_store = ServerObjectStore::local(storage.path().join("chunks"))?;

    let matching_scope =
        RepositoryScope::new(RepositoryProvider::GitHub, "team", "assets", Some("main"))?;
    let matching_release_scope = RepositoryScope::new(
        RepositoryProvider::GitHub,
        "team",
        "assets",
        Some("release"),
    )?;
    let other_scope =
        RepositoryScope::new(RepositoryProvider::GitHub, "team", "other", Some("main"))?;
    let matching_record = FileRecord {
        file_id: "asset.bin".to_owned(),
        content_hash: "a".repeat(64),
        total_bytes: 8,
        chunk_size: 4,
        repository_scope: Some(matching_scope),
        chunks: vec![
            FileChunkRecord {
                hash: "b".repeat(64),
                offset: 0,
                length: 4,
                range_start: 0,
                range_end: 1,
                packed_start: 0,
                packed_end: 4,
            },
            FileChunkRecord {
                hash: "c".repeat(64),
                offset: 4,
                length: 4,
                range_start: 0,
                range_end: 1,
                packed_start: 0,
                packed_end: 4,
            },
        ],
    };
    let matching_release_record = FileRecord {
        file_id: "asset-release.bin".to_owned(),
        content_hash: "f".repeat(64),
        total_bytes: 4,
        chunk_size: 4,
        repository_scope: Some(matching_release_scope),
        chunks: vec![FileChunkRecord {
            hash: "1".repeat(64),
            offset: 0,
            length: 4,
            range_start: 0,
            range_end: 1,
            packed_start: 0,
            packed_end: 4,
        }],
    };
    let other_record = FileRecord {
        file_id: "asset.bin".to_owned(),
        content_hash: "d".repeat(64),
        total_bytes: 4,
        chunk_size: 4,
        repository_scope: Some(other_scope),
        chunks: vec![FileChunkRecord {
            hash: "e".repeat(64),
            offset: 0,
            length: 4,
            range_start: 0,
            range_end: 1,
            packed_start: 0,
            packed_end: 4,
        }],
    };
    RecordStore::write_latest_record(&record_store, &matching_record).await?;
    RecordStore::write_latest_record(&record_store, &matching_release_record).await?;
    RecordStore::write_latest_record(&record_store, &other_record).await?;
    RecordStore::write_version_record(&record_store, &matching_record).await?;
    RecordStore::write_version_record(&record_store, &matching_release_record).await?;
    RecordStore::write_version_record(&record_store, &other_record).await?;

    let event = RepositoryWebhookEvent::new(
        RepositoryRef::new(ProviderKind::GitHub, "team", "assets")?,
        WebhookDeliveryId::new("delivery-1")?,
        RepositoryWebhookEventKind::RepositoryDeleted,
    );

    let outcome =
        apply_provider_webhook_with_stores(&record_store, &index_store, &object_store, &event)
            .await?;

    assert_eq!(outcome.affected_file_versions, 2);
    assert_eq!(outcome.affected_chunks, 3);
    assert_eq!(outcome.applied_holds, 6);
    assert_eq!(
        outcome.event_kind,
        ProviderWebhookOutcomeKind::RepositoryDeleted
    );

    let matching_first = chunk_object_key(&"b".repeat(64))?;
    let matching_second = chunk_object_key(&"c".repeat(64))?;
    let matching_first_xorb = xorb_object_key(&"b".repeat(64))?;
    let matching_second_xorb = xorb_object_key(&"c".repeat(64))?;
    let matching_release_chunk = chunk_object_key(&"1".repeat(64))?;
    let matching_release_xorb = xorb_object_key(&"1".repeat(64))?;
    let other_chunk = chunk_object_key(&"e".repeat(64))?;
    assert!(matches!(
        IndexStore::retention_hold(&index_store, &matching_first),
        Ok(Some(_))
    ));
    assert!(matches!(
        IndexStore::retention_hold(&index_store, &matching_second),
        Ok(Some(_))
    ));
    assert!(matches!(
        IndexStore::retention_hold(&index_store, &matching_first_xorb),
        Ok(Some(_))
    ));
    assert!(matches!(
        IndexStore::retention_hold(&index_store, &matching_second_xorb),
        Ok(Some(_))
    ));
    assert!(matches!(
        IndexStore::retention_hold(&index_store, &matching_release_chunk),
        Ok(Some(_))
    ));
    assert!(matches!(
        IndexStore::retention_hold(&index_store, &matching_release_xorb),
        Ok(Some(_))
    ));
    assert!(matches!(
        IndexStore::retention_hold(&index_store, &other_chunk),
        Ok(None)
    ));
    assert!(!local_latest_record_exists(&record_store, &matching_record).await?);
    assert!(!local_latest_record_exists(&record_store, &matching_release_record).await?);
    assert!(local_latest_record_exists(&record_store, &other_record).await?);
    assert!(!local_version_record_exists(&record_store, &matching_record).await?);
    assert!(!local_version_record_exists(&record_store, &matching_release_record).await?);
    assert!(local_version_record_exists(&record_store, &other_record).await?);

    Ok(())
}

async fn exercise_repository_deleted_removes_stale_latest_without_version_record()
-> Result<(), Box<dyn Error>> {
    let storage = tempfile::tempdir()?;
    let record_store = LocalRecordStore::open(storage.path().to_path_buf());
    let index_store = LocalIndexStore::new(storage.path().to_path_buf())?;
    let object_store = ServerObjectStore::local(storage.path().join("chunks"))?;
    let scope = RepositoryScope::new(RepositoryProvider::GitHub, "team", "assets", Some("main"))?;
    let stale_record = FileRecord {
        file_id: "stale.bin".to_owned(),
        content_hash: "e".repeat(64),
        total_bytes: 4,
        chunk_size: 4,
        repository_scope: Some(scope.clone()),
        chunks: vec![FileChunkRecord {
            hash: "f".repeat(64),
            offset: 0,
            length: 4,
            range_start: 0,
            range_end: 1,
            packed_start: 0,
            packed_end: 4,
        }],
    };
    let healthy_record = FileRecord {
        file_id: "asset.bin".to_owned(),
        content_hash: "a".repeat(64),
        total_bytes: 4,
        chunk_size: 4,
        repository_scope: Some(scope),
        chunks: vec![FileChunkRecord {
            hash: "b".repeat(64),
            offset: 0,
            length: 4,
            range_start: 0,
            range_end: 1,
            packed_start: 0,
            packed_end: 4,
        }],
    };
    RecordStore::write_latest_record(&record_store, &stale_record).await?;
    RecordStore::write_version_record(&record_store, &healthy_record).await?;
    RecordStore::write_latest_record(&record_store, &healthy_record).await?;

    let event = RepositoryWebhookEvent::new(
        RepositoryRef::new(ProviderKind::GitHub, "team", "assets")?,
        WebhookDeliveryId::new("delivery-delete-2")?,
        RepositoryWebhookEventKind::RepositoryDeleted,
    );

    let outcome =
        apply_provider_webhook_with_stores(&record_store, &index_store, &object_store, &event)
            .await?;
    assert_eq!(outcome.affected_file_versions, 2);
    assert_eq!(outcome.affected_chunks, 2);
    assert_eq!(outcome.applied_holds, 4);

    let stale_chunk_key = chunk_object_key(&"f".repeat(64))?;
    let stale_xorb_key = xorb_object_key(&"f".repeat(64))?;
    assert!(matches!(
        IndexStore::retention_hold(&index_store, &stale_chunk_key),
        Ok(Some(_))
    ));
    assert!(matches!(
        IndexStore::retention_hold(&index_store, &stale_xorb_key),
        Ok(Some(_))
    ));
    assert!(!local_latest_record_exists(&record_store, &stale_record).await?);
    assert!(!local_latest_record_exists(&record_store, &healthy_record).await?);
    assert!(!local_version_record_exists(&record_store, &healthy_record).await?);

    Ok(())
}

async fn exercise_repository_deleted_holds_native_xet_xorb_and_unpacked_chunks()
-> Result<(), Box<dyn Error>> {
    let storage = tempfile::tempdir()?;
    let record_store = LocalRecordStore::open(storage.path().to_path_buf());
    let index_store = LocalIndexStore::new(storage.path().to_path_buf())?;
    let object_store = ServerObjectStore::local(storage.path().join("chunks"))?;

    let raw_xorb = build_raw_xorb(2, ChunkSize::Fixed(1024));
    let serialized =
        SerializedXorbObject::from_xorb_with_compression(raw_xorb, CompressionScheme::LZ4, false)?;
    let xorb_hash = serialized.hash.hex();
    let expected_xorb_hash = ShardlineHash::parse_api_hex(&xorb_hash)?;
    let normalized_xorb =
        normalize_serialized_xorb(expected_xorb_hash, &serialized.serialized_data)?;
    let mut reader = Cursor::new(normalized_xorb.as_slice());
    let validated = validate_serialized_xorb(&mut reader, expected_xorb_hash)?;
    let range_end = u32::try_from(validated.chunks().len())?;
    let mut chunk_hashes = Vec::new();
    try_for_each_serialized_xorb_chunk(&mut reader, &validated, |decoded_chunk| {
        chunk_hashes.push(decoded_chunk.descriptor().hash().api_hex_string());
        Ok::<(), ServerError>(())
    })?;
    let upload = store_uploaded_xorb(&object_store, &xorb_hash, &serialized.serialized_data)?;
    assert!(upload.was_inserted);

    let scope = RepositoryScope::new(RepositoryProvider::GitHub, "team", "assets", Some("main"))?;
    let record = FileRecord {
        file_id: "asset.bin".to_owned(),
        content_hash: "a".repeat(64),
        total_bytes: validated.unpacked_length(),
        chunk_size: 0,
        repository_scope: Some(scope),
        chunks: vec![FileChunkRecord {
            hash: xorb_hash.clone(),
            offset: 0,
            length: validated.unpacked_length(),
            range_start: 0,
            range_end,
            packed_start: 0,
            packed_end: u64::try_from(normalized_xorb.len())?,
        }],
    };
    RecordStore::write_version_record(&record_store, &record).await?;
    RecordStore::write_latest_record(&record_store, &record).await?;

    let event = RepositoryWebhookEvent::new(
        RepositoryRef::new(ProviderKind::GitHub, "team", "assets")?,
        WebhookDeliveryId::new("delivery-native-xet-delete-1")?,
        RepositoryWebhookEventKind::RepositoryDeleted,
    );

    let outcome =
        apply_provider_webhook_with_stores(&record_store, &index_store, &object_store, &event)
            .await?;
    assert_eq!(outcome.affected_file_versions, 1);
    assert_eq!(outcome.affected_chunks, u64::try_from(chunk_hashes.len())?);
    assert_eq!(
        outcome.applied_holds,
        u64::try_from(
            chunk_hashes
                .len()
                .checked_add(1)
                .ok_or(ServerError::Overflow)?
        )?
    );

    let xorb_key = xorb_object_key(&xorb_hash)?;
    assert!(matches!(
        IndexStore::retention_hold(&index_store, &xorb_key),
        Ok(Some(_))
    ));
    for chunk_hash in &chunk_hashes {
        let chunk_key = chunk_object_key(chunk_hash)?;
        assert!(matches!(
            IndexStore::retention_hold(&index_store, &chunk_key),
            Ok(Some(_))
        ));
    }
    assert!(!local_latest_record_exists(&record_store, &record).await?);
    assert!(!local_version_record_exists(&record_store, &record).await?);

    Ok(())
}

async fn exercise_access_changed_records_provider_repository_state_without_mutating_file_metadata()
-> Result<(), Box<dyn Error>> {
    let storage = tempfile::tempdir()?;
    let record_store = LocalRecordStore::open(storage.path().to_path_buf());
    let index_store = LocalIndexStore::new(storage.path().to_path_buf())?;
    let object_store = ServerObjectStore::local(storage.path().join("chunks"))?;
    let scope = RepositoryScope::new(RepositoryProvider::GitHub, "team", "assets", Some("main"))?;
    let record = FileRecord {
        file_id: "asset.bin".to_owned(),
        content_hash: "a".repeat(64),
        total_bytes: 4,
        chunk_size: 4,
        repository_scope: Some(scope),
        chunks: vec![FileChunkRecord {
            hash: "b".repeat(64),
            offset: 0,
            length: 4,
            range_start: 0,
            range_end: 1,
            packed_start: 0,
            packed_end: 4,
        }],
    };
    RecordStore::write_version_record(&record_store, &record).await?;
    RecordStore::write_latest_record(&record_store, &record).await?;

    let event = RepositoryWebhookEvent::new(
        RepositoryRef::new(ProviderKind::GitHub, "team", "assets")?,
        WebhookDeliveryId::new("delivery-access-1")?,
        RepositoryWebhookEventKind::AccessChanged,
    );

    let outcome =
        apply_provider_webhook_with_stores(&record_store, &index_store, &object_store, &event)
            .await?;
    assert_eq!(
        outcome.event_kind,
        ProviderWebhookOutcomeKind::AccessChanged
    );
    assert_eq!(outcome.affected_file_versions, 0);
    assert_eq!(outcome.affected_chunks, 0);
    assert_eq!(outcome.applied_holds, 0);
    assert_eq!(outcome.retention_seconds, None);
    assert!(local_version_record_exists(&record_store, &record).await?);
    assert!(local_latest_record_exists(&record_store, &record).await?);
    let chunk_key = chunk_object_key(&"b".repeat(64))?;
    assert!(matches!(
        IndexStore::retention_hold(&index_store, &chunk_key),
        Ok(None)
    ));
    let repository_state = IndexStore::provider_repository_state(
        &index_store,
        RepositoryProvider::GitHub,
        "team",
        "assets",
    )?;
    assert!(repository_state.is_some());
    let Some(repository_state) = repository_state else {
        return Err(ServerTestInvariantError::new("missing provider repository state").into());
    };
    assert!(
        repository_state
            .last_access_changed_at_unix_seconds()
            .is_some()
    );
    assert_eq!(
        repository_state.last_revision_pushed_at_unix_seconds(),
        None
    );
    assert_eq!(repository_state.last_pushed_revision(), None);

    Ok(())
}

async fn exercise_revision_pushed_records_provider_repository_state_without_mutating_file_metadata()
-> Result<(), Box<dyn Error>> {
    let storage = tempfile::tempdir()?;
    let record_store = LocalRecordStore::open(storage.path().to_path_buf());
    let index_store = LocalIndexStore::new(storage.path().to_path_buf())?;
    let object_store = ServerObjectStore::local(storage.path().join("chunks"))?;
    let scope = RepositoryScope::new(RepositoryProvider::GitHub, "team", "assets", Some("main"))?;
    let record = FileRecord {
        file_id: "asset.bin".to_owned(),
        content_hash: "a".repeat(64),
        total_bytes: 4,
        chunk_size: 4,
        repository_scope: Some(scope),
        chunks: vec![FileChunkRecord {
            hash: "b".repeat(64),
            offset: 0,
            length: 4,
            range_start: 0,
            range_end: 1,
            packed_start: 0,
            packed_end: 4,
        }],
    };
    RecordStore::write_version_record(&record_store, &record).await?;
    RecordStore::write_latest_record(&record_store, &record).await?;

    let event = RepositoryWebhookEvent::new(
        RepositoryRef::new(ProviderKind::GitHub, "team", "assets")?,
        WebhookDeliveryId::new("delivery-revision-1")?,
        RepositoryWebhookEventKind::RevisionPushed {
            revision: RevisionRef::new("refs/heads/main")?,
        },
    );

    let outcome =
        apply_provider_webhook_with_stores(&record_store, &index_store, &object_store, &event)
            .await?;
    assert_eq!(
        outcome.event_kind,
        ProviderWebhookOutcomeKind::RevisionPushed {
            revision: "refs/heads/main".to_owned(),
        }
    );
    assert_eq!(outcome.affected_file_versions, 0);
    assert_eq!(outcome.affected_chunks, 0);
    assert_eq!(outcome.applied_holds, 0);
    assert_eq!(outcome.retention_seconds, None);
    assert!(local_version_record_exists(&record_store, &record).await?);
    assert!(local_latest_record_exists(&record_store, &record).await?);
    let chunk_key = chunk_object_key(&"b".repeat(64))?;
    assert!(matches!(
        IndexStore::retention_hold(&index_store, &chunk_key),
        Ok(None)
    ));
    let repository_state = IndexStore::provider_repository_state(
        &index_store,
        RepositoryProvider::GitHub,
        "team",
        "assets",
    )?;
    assert!(repository_state.is_some());
    let Some(repository_state) = repository_state else {
        return Err(ServerTestInvariantError::new("missing provider repository state").into());
    };
    assert!(
        repository_state
            .last_revision_pushed_at_unix_seconds()
            .is_some()
    );
    assert_eq!(
        repository_state.last_pushed_revision(),
        Some("refs/heads/main")
    );

    Ok(())
}

async fn exercise_access_change_and_revision_push_state_survives_reordering()
-> Result<(), Box<dyn Error>> {
    let storage = tempfile::tempdir()?;
    let record_store = LocalRecordStore::open(storage.path().to_path_buf());
    let index_store = LocalIndexStore::new(storage.path().to_path_buf())?;
    let object_store = ServerObjectStore::local(storage.path().join("chunks"))?;
    let revision_event = RepositoryWebhookEvent::new(
        RepositoryRef::new(ProviderKind::GitHub, "team", "assets")?,
        WebhookDeliveryId::new("delivery-revision-first")?,
        RepositoryWebhookEventKind::RevisionPushed {
            revision: RevisionRef::new("refs/heads/main")?,
        },
    );
    let access_event = RepositoryWebhookEvent::new(
        RepositoryRef::new(ProviderKind::GitHub, "team", "assets")?,
        WebhookDeliveryId::new("delivery-access-second")?,
        RepositoryWebhookEventKind::AccessChanged,
    );

    let revision_outcome = apply_provider_webhook_with_stores(
        &record_store,
        &index_store,
        &object_store,
        &revision_event,
    )
    .await?;
    let access_outcome = apply_provider_webhook_with_stores(
        &record_store,
        &index_store,
        &object_store,
        &access_event,
    )
    .await?;

    assert!(matches!(
        revision_outcome.event_kind,
        ProviderWebhookOutcomeKind::RevisionPushed { .. }
    ));
    assert_eq!(
        access_outcome.event_kind,
        ProviderWebhookOutcomeKind::AccessChanged
    );
    let repository_state = IndexStore::provider_repository_state(
        &index_store,
        RepositoryProvider::GitHub,
        "team",
        "assets",
    )?;
    assert!(repository_state.is_some());
    let Some(repository_state) = repository_state else {
        return Err(ServerTestInvariantError::new("missing provider repository state").into());
    };
    assert!(
        repository_state
            .last_access_changed_at_unix_seconds()
            .is_some()
    );
    assert!(
        repository_state
            .last_revision_pushed_at_unix_seconds()
            .is_some()
    );
    assert_eq!(
        repository_state.last_pushed_revision(),
        Some("refs/heads/main")
    );

    Ok(())
}

async fn exercise_duplicate_webhook_delivery_is_ignored_after_first_application()
-> Result<(), Box<dyn Error>> {
    let storage = tempfile::tempdir()?;
    let record_store = LocalRecordStore::open(storage.path().to_path_buf());
    let index_store = LocalIndexStore::new(storage.path().to_path_buf())?;
    let object_store = ServerObjectStore::local(storage.path().join("chunks"))?;
    let scope = RepositoryScope::new(RepositoryProvider::GitHub, "team", "assets", Some("main"))?;
    let record = FileRecord {
        file_id: "asset.bin".to_owned(),
        content_hash: "a".repeat(64),
        total_bytes: 4,
        chunk_size: 4,
        repository_scope: Some(scope),
        chunks: vec![FileChunkRecord {
            hash: "b".repeat(64),
            offset: 0,
            length: 4,
            range_start: 0,
            range_end: 1,
            packed_start: 0,
            packed_end: 4,
        }],
    };
    RecordStore::write_latest_record(&record_store, &record).await?;
    RecordStore::write_version_record(&record_store, &record).await?;

    let event = RepositoryWebhookEvent::new(
        RepositoryRef::new(ProviderKind::GitHub, "team", "assets")?,
        WebhookDeliveryId::new("delivery-1")?,
        RepositoryWebhookEventKind::RepositoryDeleted,
    );

    let first =
        apply_provider_webhook_with_stores(&record_store, &index_store, &object_store, &event)
            .await?;
    let second =
        apply_provider_webhook_with_stores(&record_store, &index_store, &object_store, &event)
            .await?;

    assert_eq!(
        first.event_kind,
        ProviderWebhookOutcomeKind::RepositoryDeleted
    );
    assert_eq!(first.affected_file_versions, 1);
    assert_eq!(first.affected_chunks, 1);
    assert_eq!(first.applied_holds, 2);
    assert_eq!(
        second.event_kind,
        ProviderWebhookOutcomeKind::RepositoryDeleted
    );
    assert_eq!(second.affected_file_versions, 0);
    assert_eq!(second.affected_chunks, 0);
    assert_eq!(second.applied_holds, 0);
    assert_eq!(second.retention_seconds, None);

    Ok(())
}

async fn exercise_matching_delivery_ids_in_different_repositories_are_not_replay_collisions()
-> Result<(), Box<dyn Error>> {
    let storage = tempfile::tempdir()?;
    let record_store = LocalRecordStore::open(storage.path().to_path_buf());
    let index_store = LocalIndexStore::new(storage.path().to_path_buf())?;
    let object_store = ServerObjectStore::local(storage.path().join("chunks"))?;
    let first_event = RepositoryWebhookEvent::new(
        RepositoryRef::new(ProviderKind::GitHub, "team", "assets")?,
        WebhookDeliveryId::new("delivery-shared")?,
        RepositoryWebhookEventKind::AccessChanged,
    );
    let second_event = RepositoryWebhookEvent::new(
        RepositoryRef::new(ProviderKind::GitHub, "team", "other-assets")?,
        WebhookDeliveryId::new("delivery-shared")?,
        RepositoryWebhookEventKind::AccessChanged,
    );

    let first = apply_provider_webhook_with_stores(
        &record_store,
        &index_store,
        &object_store,
        &first_event,
    )
    .await?;
    let second = apply_provider_webhook_with_stores(
        &record_store,
        &index_store,
        &object_store,
        &second_event,
    )
    .await?;

    assert_eq!(first.event_kind, ProviderWebhookOutcomeKind::AccessChanged);
    assert_eq!(second.event_kind, ProviderWebhookOutcomeKind::AccessChanged);
    let first_state = IndexStore::provider_repository_state(
        &index_store,
        RepositoryProvider::GitHub,
        "team",
        "assets",
    )?;
    let second_state = IndexStore::provider_repository_state(
        &index_store,
        RepositoryProvider::GitHub,
        "team",
        "other-assets",
    )?;
    assert!(first_state.is_some());
    assert!(second_state.is_some());
    let deliveries = IndexStore::list_webhook_deliveries(&index_store)?;
    assert_eq!(deliveries.len(), 2);
    assert!(deliveries.iter().any(|delivery| {
        delivery.owner() == "team"
            && delivery.repo() == "assets"
            && delivery.delivery_id() == "delivery-shared"
    }));
    assert!(deliveries.iter().any(|delivery| {
        delivery.owner() == "team"
            && delivery.repo() == "other-assets"
            && delivery.delivery_id() == "delivery-shared"
    }));

    Ok(())
}

async fn exercise_failed_webhook_application_can_retry_same_delivery() -> Result<(), Box<dyn Error>>
{
    let storage = tempfile::tempdir()?;
    let record_store = LocalRecordStore::open(storage.path().to_path_buf());
    let index_store =
        FailFirstRetentionHoldIndexStore::new(LocalIndexStore::new(storage.path().to_path_buf())?);
    let object_store = ServerObjectStore::local(storage.path().join("chunks"))?;
    let scope = RepositoryScope::new(RepositoryProvider::GitHub, "team", "assets", Some("main"))?;
    let record = FileRecord {
        file_id: "asset.bin".to_owned(),
        content_hash: "a".repeat(64),
        total_bytes: 4,
        chunk_size: 4,
        repository_scope: Some(scope),
        chunks: vec![FileChunkRecord {
            hash: "b".repeat(64),
            offset: 0,
            length: 4,
            range_start: 0,
            range_end: 1,
            packed_start: 0,
            packed_end: 4,
        }],
    };
    RecordStore::write_latest_record(&record_store, &record).await?;
    RecordStore::write_version_record(&record_store, &record).await?;

    let event = RepositoryWebhookEvent::new(
        RepositoryRef::new(ProviderKind::GitHub, "team", "assets")?,
        WebhookDeliveryId::new("delivery-1")?,
        RepositoryWebhookEventKind::RepositoryDeleted,
    );

    let first =
        apply_provider_webhook_with_stores(&record_store, &index_store, &object_store, &event)
            .await;
    assert!(matches!(
        first,
        Err(ServerError::IndexStore(
            LocalIndexStoreError::InvalidLegacyImportState
        ))
    ));

    let second =
        apply_provider_webhook_with_stores(&record_store, &index_store, &object_store, &event)
            .await?;
    assert_eq!(
        second.event_kind,
        ProviderWebhookOutcomeKind::RepositoryDeleted
    );
    assert_eq!(second.affected_file_versions, 1);
    assert_eq!(second.affected_chunks, 1);
    assert_eq!(second.applied_holds, 2);

    let third =
        apply_provider_webhook_with_stores(&record_store, &index_store, &object_store, &event)
            .await?;
    assert_eq!(
        third.event_kind,
        ProviderWebhookOutcomeKind::RepositoryDeleted
    );
    assert_eq!(third.affected_file_versions, 0);
    assert_eq!(third.affected_chunks, 0);
    assert_eq!(third.applied_holds, 0);
    assert_eq!(third.retention_seconds, None);

    Ok(())
}

async fn exercise_repository_rename_migrates_records_to_new_scope() -> Result<(), Box<dyn Error>> {
    let storage = tempfile::tempdir()?;
    let record_store = LocalRecordStore::open(storage.path().to_path_buf());
    let index_store = LocalIndexStore::new(storage.path().to_path_buf())?;
    let object_store = ServerObjectStore::local(storage.path().join("chunks"))?;
    let main_scope =
        RepositoryScope::new(RepositoryProvider::GitHub, "team", "assets", Some("main"))?;
    let release_scope = RepositoryScope::new(
        RepositoryProvider::GitHub,
        "team",
        "assets",
        Some("release"),
    )?;
    let main_record = FileRecord {
        file_id: "asset.bin".to_owned(),
        content_hash: "a".repeat(64),
        total_bytes: 4,
        chunk_size: 4,
        repository_scope: Some(main_scope.clone()),
        chunks: vec![FileChunkRecord {
            hash: "b".repeat(64),
            offset: 0,
            length: 4,
            range_start: 0,
            range_end: 1,
            packed_start: 0,
            packed_end: 4,
        }],
    };
    let release_record = FileRecord {
        file_id: "asset.bin".to_owned(),
        content_hash: "c".repeat(64),
        total_bytes: 4,
        chunk_size: 4,
        repository_scope: Some(release_scope.clone()),
        chunks: vec![FileChunkRecord {
            hash: "d".repeat(64),
            offset: 0,
            length: 4,
            range_start: 0,
            range_end: 1,
            packed_start: 0,
            packed_end: 4,
        }],
    };
    RecordStore::write_version_record(&record_store, &main_record).await?;
    RecordStore::write_latest_record(&record_store, &main_record).await?;
    RecordStore::write_version_record(&record_store, &release_record).await?;
    RecordStore::write_latest_record(&record_store, &release_record).await?;

    let event = RepositoryWebhookEvent::new(
        RepositoryRef::new(ProviderKind::GitHub, "team", "assets")?,
        WebhookDeliveryId::new("delivery-rename-1")?,
        RepositoryWebhookEventKind::RepositoryRenamed {
            new_repository: RepositoryRef::new(ProviderKind::GitHub, "team", "assets-renamed")?,
        },
    );

    let outcome =
        apply_provider_webhook_with_stores(&record_store, &index_store, &object_store, &event)
            .await?;
    assert_eq!(outcome.affected_file_versions, 2);
    assert_eq!(outcome.affected_chunks, 2);
    assert_eq!(outcome.applied_holds, 0);
    assert_eq!(
        outcome.event_kind,
        ProviderWebhookOutcomeKind::RepositoryRenamed {
            new_owner: "team".to_owned(),
            new_repo: "assets-renamed".to_owned(),
        }
    );

    let renamed_main_scope = RepositoryScope::new(
        RepositoryProvider::GitHub,
        "team",
        "assets-renamed",
        Some("main"),
    )?;
    let renamed_release_scope = RepositoryScope::new(
        RepositoryProvider::GitHub,
        "team",
        "assets-renamed",
        Some("release"),
    )?;
    let renamed_main_record = FileRecord {
        repository_scope: Some(renamed_main_scope),
        ..main_record.clone()
    };
    let renamed_release_record = FileRecord {
        repository_scope: Some(renamed_release_scope),
        ..release_record.clone()
    };

    assert!(!local_latest_record_exists(&record_store, &main_record).await?);
    assert!(!local_latest_record_exists(&record_store, &release_record).await?);
    assert!(local_latest_record_exists(&record_store, &renamed_main_record).await?);
    assert!(local_latest_record_exists(&record_store, &renamed_release_record).await?);
    assert!(local_version_record_exists(&record_store, &renamed_main_record).await?);
    assert!(local_version_record_exists(&record_store, &renamed_release_record).await?);
    assert!(!local_version_record_exists(&record_store, &main_record).await?);
    assert!(!local_version_record_exists(&record_store, &release_record).await?);

    Ok(())
}

async fn exercise_previously_recorded_webhook_delivery_is_a_no_op() -> Result<(), Box<dyn Error>> {
    let storage = tempfile::tempdir()?;
    let record_store = LocalRecordStore::open(storage.path().to_path_buf());
    let index_store = LocalIndexStore::new(storage.path().to_path_buf())?;
    let object_store = ServerObjectStore::local(storage.path().join("chunks"))?;
    let scope = RepositoryScope::new(RepositoryProvider::GitHub, "team", "assets", Some("main"))?;
    let record = FileRecord {
        file_id: "asset.bin".to_owned(),
        content_hash: "a".repeat(64),
        total_bytes: 4,
        chunk_size: 4,
        repository_scope: Some(scope),
        chunks: vec![FileChunkRecord {
            hash: "b".repeat(64),
            offset: 0,
            length: 4,
            range_start: 0,
            range_end: 1,
            packed_start: 0,
            packed_end: 4,
        }],
    };
    RecordStore::write_version_record(&record_store, &record).await?;
    RecordStore::write_latest_record(&record_store, &record).await?;

    let recorded_delivery = WebhookDelivery::new(
        RepositoryProvider::GitHub,
        "team".to_owned(),
        "assets".to_owned(),
        "delivery-duplicate-1".to_owned(),
        100,
    )?;
    assert!(IndexStore::record_webhook_delivery(
        &index_store,
        &recorded_delivery
    )?);

    let event = RepositoryWebhookEvent::new(
        RepositoryRef::new(ProviderKind::GitHub, "team", "assets")?,
        WebhookDeliveryId::new("delivery-duplicate-1")?,
        RepositoryWebhookEventKind::RepositoryDeleted,
    );

    let outcome =
        apply_provider_webhook_with_stores(&record_store, &index_store, &object_store, &event)
            .await?;
    assert_eq!(
        outcome.event_kind,
        ProviderWebhookOutcomeKind::RepositoryDeleted
    );
    assert_eq!(outcome.affected_file_versions, 0);
    assert_eq!(outcome.affected_chunks, 0);
    assert_eq!(outcome.applied_holds, 0);
    assert_eq!(outcome.retention_seconds, None);

    let chunk_key = chunk_object_key(&"b".repeat(64))?;
    let xorb_key = xorb_object_key(&"b".repeat(64))?;
    assert!(matches!(
        IndexStore::retention_hold(&index_store, &chunk_key),
        Ok(None)
    ));
    assert!(matches!(
        IndexStore::retention_hold(&index_store, &xorb_key),
        Ok(None)
    ));

    Ok(())
}

async fn exercise_repository_rename_removes_old_scope_latest_without_version_record()
-> Result<(), Box<dyn Error>> {
    let storage = tempfile::tempdir()?;
    let record_store = LocalRecordStore::open(storage.path().to_path_buf());
    let index_store = LocalIndexStore::new(storage.path().to_path_buf())?;
    let object_store = ServerObjectStore::local(storage.path().join("chunks"))?;
    let main_scope =
        RepositoryScope::new(RepositoryProvider::GitHub, "team", "assets", Some("main"))?;
    let stale_record = FileRecord {
        file_id: "stale.bin".to_owned(),
        content_hash: "e".repeat(64),
        total_bytes: 4,
        chunk_size: 4,
        repository_scope: Some(main_scope.clone()),
        chunks: vec![FileChunkRecord {
            hash: "f".repeat(64),
            offset: 0,
            length: 4,
            range_start: 0,
            range_end: 1,
            packed_start: 0,
            packed_end: 4,
        }],
    };
    let healthy_record = FileRecord {
        file_id: "asset.bin".to_owned(),
        content_hash: "a".repeat(64),
        total_bytes: 4,
        chunk_size: 4,
        repository_scope: Some(main_scope),
        chunks: vec![FileChunkRecord {
            hash: "b".repeat(64),
            offset: 0,
            length: 4,
            range_start: 0,
            range_end: 1,
            packed_start: 0,
            packed_end: 4,
        }],
    };
    RecordStore::write_latest_record(&record_store, &stale_record).await?;
    RecordStore::write_version_record(&record_store, &healthy_record).await?;
    RecordStore::write_latest_record(&record_store, &healthy_record).await?;

    let event = RepositoryWebhookEvent::new(
        RepositoryRef::new(ProviderKind::GitHub, "team", "assets")?,
        WebhookDeliveryId::new("delivery-rename-2")?,
        RepositoryWebhookEventKind::RepositoryRenamed {
            new_repository: RepositoryRef::new(ProviderKind::GitHub, "team", "assets-renamed")?,
        },
    );

    let outcome =
        apply_provider_webhook_with_stores(&record_store, &index_store, &object_store, &event)
            .await?;
    assert_eq!(outcome.affected_file_versions, 1);
    assert_eq!(outcome.affected_chunks, 1);

    let renamed_scope = RepositoryScope::new(
        RepositoryProvider::GitHub,
        "team",
        "assets-renamed",
        Some("main"),
    )?;
    let renamed_healthy_record = FileRecord {
        repository_scope: Some(renamed_scope.clone()),
        ..healthy_record
    };
    let renamed_stale_record = FileRecord {
        repository_scope: Some(renamed_scope),
        ..stale_record.clone()
    };

    assert!(!local_latest_record_exists(&record_store, &stale_record).await?);
    assert!(local_latest_record_exists(&record_store, &renamed_healthy_record).await?);
    assert!(!local_latest_record_exists(&record_store, &renamed_stale_record).await?);

    Ok(())
}

async fn exercise_repository_rename_rejects_conflicting_target_metadata()
-> Result<(), Box<dyn Error>> {
    let storage = tempfile::tempdir()?;
    let record_store = LocalRecordStore::open(storage.path().to_path_buf());
    let index_store = LocalIndexStore::new(storage.path().to_path_buf())?;
    let object_store = ServerObjectStore::local(storage.path().join("chunks"))?;
    let source_scope =
        RepositoryScope::new(RepositoryProvider::GitHub, "team", "assets", Some("main"))?;
    let target_scope = RepositoryScope::new(
        RepositoryProvider::GitHub,
        "team",
        "assets-renamed",
        Some("main"),
    )?;
    let source_record = FileRecord {
        file_id: "asset.bin".to_owned(),
        content_hash: "a".repeat(64),
        total_bytes: 4,
        chunk_size: 4,
        repository_scope: Some(source_scope),
        chunks: vec![FileChunkRecord {
            hash: "b".repeat(64),
            offset: 0,
            length: 4,
            range_start: 0,
            range_end: 1,
            packed_start: 0,
            packed_end: 4,
        }],
    };
    let conflicting_target_record = FileRecord {
        file_id: "asset.bin".to_owned(),
        content_hash: "c".repeat(64),
        total_bytes: 4,
        chunk_size: 4,
        repository_scope: Some(target_scope),
        chunks: vec![FileChunkRecord {
            hash: "d".repeat(64),
            offset: 0,
            length: 4,
            range_start: 0,
            range_end: 1,
            packed_start: 0,
            packed_end: 4,
        }],
    };
    RecordStore::write_version_record(&record_store, &source_record).await?;
    RecordStore::write_latest_record(&record_store, &source_record).await?;
    RecordStore::write_version_record(&record_store, &conflicting_target_record).await?;
    RecordStore::write_latest_record(&record_store, &conflicting_target_record).await?;

    let event = RepositoryWebhookEvent::new(
        RepositoryRef::new(ProviderKind::GitHub, "team", "assets")?,
        WebhookDeliveryId::new("delivery-rename-3")?,
        RepositoryWebhookEventKind::RepositoryRenamed {
            new_repository: RepositoryRef::new(ProviderKind::GitHub, "team", "assets-renamed")?,
        },
    );

    let outcome =
        apply_provider_webhook_with_stores(&record_store, &index_store, &object_store, &event)
            .await;
    assert!(matches!(
        outcome,
        Err(ServerError::ConflictingRenameTargetRecord)
    ));
    assert!(local_version_record_exists(&record_store, &source_record).await?);
    assert!(local_latest_record_exists(&record_store, &source_record).await?);
    assert_eq!(
        local_version_record_bytes(&record_store, &conflicting_target_record).await?,
        to_vec(&conflicting_target_record)?
    );
    assert_eq!(
        local_latest_record_bytes(&record_store, &conflicting_target_record).await?,
        to_vec(&conflicting_target_record)?
    );

    Ok(())
}

#[derive(Debug)]
struct FailFirstRetentionHoldIndexStore {
    inner: LocalIndexStore,
    fail_first_upsert: AtomicBool,
}

impl FailFirstRetentionHoldIndexStore {
    fn new(inner: LocalIndexStore) -> Self {
        Self {
            inner,
            fail_first_upsert: AtomicBool::new(true),
        }
    }
}

impl AsyncIndexStore for FailFirstRetentionHoldIndexStore {
    type Error = LocalIndexStoreError;

    fn reconstruction<'operation>(
        &'operation self,
        file_id: &'operation FileId,
    ) -> IndexStoreFuture<'operation, Option<FileReconstruction>, Self::Error> {
        Box::pin(async move { AsyncIndexStore::reconstruction(&self.inner, file_id).await })
    }

    fn insert_reconstruction<'operation>(
        &'operation self,
        file_id: &'operation FileId,
        reconstruction: &'operation FileReconstruction,
    ) -> IndexStoreFuture<'operation, (), Self::Error> {
        Box::pin(async move {
            AsyncIndexStore::insert_reconstruction(&self.inner, file_id, reconstruction).await
        })
    }

    fn list_reconstruction_file_ids(&self) -> IndexStoreFuture<'_, Vec<FileId>, Self::Error> {
        Box::pin(async move { AsyncIndexStore::list_reconstruction_file_ids(&self.inner).await })
    }

    fn delete_reconstruction<'operation>(
        &'operation self,
        file_id: &'operation FileId,
    ) -> IndexStoreFuture<'operation, bool, Self::Error> {
        Box::pin(async move { AsyncIndexStore::delete_reconstruction(&self.inner, file_id).await })
    }

    fn contains_xorb<'operation>(
        &'operation self,
        xorb_id: &'operation XorbId,
    ) -> IndexStoreFuture<'operation, bool, Self::Error> {
        Box::pin(async move { AsyncIndexStore::contains_xorb(&self.inner, xorb_id).await })
    }

    fn insert_xorb<'operation>(
        &'operation self,
        xorb_id: &'operation XorbId,
    ) -> IndexStoreFuture<'operation, (), Self::Error> {
        Box::pin(async move { AsyncIndexStore::insert_xorb(&self.inner, xorb_id).await })
    }

    fn dedupe_shard_mapping<'operation>(
        &'operation self,
        chunk_hash: &'operation ShardlineHash,
    ) -> IndexStoreFuture<'operation, Option<DedupeShardMapping>, Self::Error> {
        Box::pin(
            async move { AsyncIndexStore::dedupe_shard_mapping(&self.inner, chunk_hash).await },
        )
    }

    fn list_dedupe_shard_mappings(
        &self,
    ) -> IndexStoreFuture<'_, Vec<DedupeShardMapping>, Self::Error> {
        Box::pin(async move { AsyncIndexStore::list_dedupe_shard_mappings(&self.inner).await })
    }

    fn visit_dedupe_shard_mappings<'operation, Visitor, VisitorError>(
        &'operation self,
        visitor: Visitor,
    ) -> IndexStoreFuture<'operation, (), VisitorError>
    where
        Self::Error: Into<VisitorError> + 'operation,
        Visitor: FnMut(DedupeShardMapping) -> Result<(), VisitorError> + Send + 'operation,
        VisitorError: Send + 'operation,
    {
        Box::pin(
            async move { AsyncIndexStore::visit_dedupe_shard_mappings(&self.inner, visitor).await },
        )
    }

    fn upsert_dedupe_shard_mapping<'operation>(
        &'operation self,
        mapping: &'operation DedupeShardMapping,
    ) -> IndexStoreFuture<'operation, (), Self::Error> {
        Box::pin(
            async move { AsyncIndexStore::upsert_dedupe_shard_mapping(&self.inner, mapping).await },
        )
    }

    fn delete_dedupe_shard_mapping<'operation>(
        &'operation self,
        chunk_hash: &'operation ShardlineHash,
    ) -> IndexStoreFuture<'operation, bool, Self::Error> {
        Box::pin(async move {
            AsyncIndexStore::delete_dedupe_shard_mapping(&self.inner, chunk_hash).await
        })
    }

    fn quarantine_candidate<'operation>(
        &'operation self,
        object_key: &'operation ObjectKey,
    ) -> IndexStoreFuture<'operation, Option<QuarantineCandidate>, Self::Error> {
        Box::pin(
            async move { AsyncIndexStore::quarantine_candidate(&self.inner, object_key).await },
        )
    }

    fn list_quarantine_candidates(
        &self,
    ) -> IndexStoreFuture<'_, Vec<QuarantineCandidate>, Self::Error> {
        Box::pin(async move { AsyncIndexStore::list_quarantine_candidates(&self.inner).await })
    }

    fn visit_quarantine_candidates<'operation, Visitor, VisitorError>(
        &'operation self,
        visitor: Visitor,
    ) -> IndexStoreFuture<'operation, (), VisitorError>
    where
        Self::Error: Into<VisitorError> + 'operation,
        Visitor: FnMut(QuarantineCandidate) -> Result<(), VisitorError> + Send + 'operation,
        VisitorError: Send + 'operation,
    {
        Box::pin(
            async move { AsyncIndexStore::visit_quarantine_candidates(&self.inner, visitor).await },
        )
    }

    fn upsert_quarantine_candidate<'operation>(
        &'operation self,
        candidate: &'operation QuarantineCandidate,
    ) -> IndexStoreFuture<'operation, (), Self::Error> {
        Box::pin(async move {
            AsyncIndexStore::upsert_quarantine_candidate(&self.inner, candidate).await
        })
    }

    fn delete_quarantine_candidate<'operation>(
        &'operation self,
        object_key: &'operation ObjectKey,
    ) -> IndexStoreFuture<'operation, bool, Self::Error> {
        Box::pin(async move {
            AsyncIndexStore::delete_quarantine_candidate(&self.inner, object_key).await
        })
    }

    fn retention_hold<'operation>(
        &'operation self,
        object_key: &'operation ObjectKey,
    ) -> IndexStoreFuture<'operation, Option<RetentionHold>, Self::Error> {
        Box::pin(async move { AsyncIndexStore::retention_hold(&self.inner, object_key).await })
    }

    fn list_retention_holds(&self) -> IndexStoreFuture<'_, Vec<RetentionHold>, Self::Error> {
        Box::pin(async move { AsyncIndexStore::list_retention_holds(&self.inner).await })
    }

    fn visit_retention_holds<'operation, Visitor, VisitorError>(
        &'operation self,
        visitor: Visitor,
    ) -> IndexStoreFuture<'operation, (), VisitorError>
    where
        Self::Error: Into<VisitorError> + 'operation,
        Visitor: FnMut(RetentionHold) -> Result<(), VisitorError> + Send + 'operation,
        VisitorError: Send + 'operation,
    {
        Box::pin(async move { AsyncIndexStore::visit_retention_holds(&self.inner, visitor).await })
    }

    fn upsert_retention_hold<'operation>(
        &'operation self,
        hold: &'operation RetentionHold,
    ) -> IndexStoreFuture<'operation, (), Self::Error> {
        Box::pin(async move {
            if self.fail_first_upsert.swap(false, Ordering::SeqCst) {
                return Err(LocalIndexStoreError::InvalidLegacyImportState);
            }

            AsyncIndexStore::upsert_retention_hold(&self.inner, hold).await
        })
    }

    fn delete_retention_hold<'operation>(
        &'operation self,
        object_key: &'operation ObjectKey,
    ) -> IndexStoreFuture<'operation, bool, Self::Error> {
        Box::pin(
            async move { AsyncIndexStore::delete_retention_hold(&self.inner, object_key).await },
        )
    }

    fn record_webhook_delivery<'operation>(
        &'operation self,
        delivery: &'operation WebhookDelivery,
    ) -> IndexStoreFuture<'operation, bool, Self::Error> {
        Box::pin(
            async move { AsyncIndexStore::record_webhook_delivery(&self.inner, delivery).await },
        )
    }

    fn list_webhook_deliveries(&self) -> IndexStoreFuture<'_, Vec<WebhookDelivery>, Self::Error> {
        Box::pin(async move { AsyncIndexStore::list_webhook_deliveries(&self.inner).await })
    }

    fn delete_webhook_delivery<'operation>(
        &'operation self,
        delivery: &'operation WebhookDelivery,
    ) -> IndexStoreFuture<'operation, bool, Self::Error> {
        Box::pin(
            async move { AsyncIndexStore::delete_webhook_delivery(&self.inner, delivery).await },
        )
    }

    fn provider_repository_state<'operation>(
        &'operation self,
        provider: RepositoryProvider,
        owner: &'operation str,
        repo: &'operation str,
    ) -> IndexStoreFuture<'operation, Option<ProviderRepositoryState>, Self::Error> {
        Box::pin(async move {
            AsyncIndexStore::provider_repository_state(&self.inner, provider, owner, repo).await
        })
    }

    fn list_provider_repository_states(
        &self,
    ) -> IndexStoreFuture<'_, Vec<ProviderRepositoryState>, Self::Error> {
        Box::pin(async move { AsyncIndexStore::list_provider_repository_states(&self.inner).await })
    }

    fn upsert_provider_repository_state<'operation>(
        &'operation self,
        state: &'operation ProviderRepositoryState,
    ) -> IndexStoreFuture<'operation, (), Self::Error> {
        Box::pin(async move {
            AsyncIndexStore::upsert_provider_repository_state(&self.inner, state).await
        })
    }

    fn delete_provider_repository_state<'operation>(
        &'operation self,
        provider: RepositoryProvider,
        owner: &'operation str,
        repo: &'operation str,
    ) -> IndexStoreFuture<'operation, bool, Self::Error> {
        Box::pin(async move {
            AsyncIndexStore::delete_provider_repository_state(&self.inner, provider, owner, repo)
                .await
        })
    }
}
