use std::{error::Error, fs, path::Path, slice::from_ref};

use rusqlite::{Connection, config::DbConfig, params};
use serde::Serialize;
use serde_json::{from_slice, json, to_vec};
use shardline_protocol::{ChunkRange, RepositoryProvider, RepositoryScope, ShardlineHash};
use shardline_storage::ObjectKey;

use super::{
    DedupeShardRecord, FileReconstructionRecord, LEGACY_IMPORT_COMPLETED_KEY,
    LOCAL_METADATA_DATABASE_FILE_NAME, LOCAL_SCHEMA_MIGRATIONS_TABLE, LOCAL_SQLITE_MIGRATIONS,
    LegacyQuarantineCandidateRecord, LocalIndexStore, LocalIndexStoreError, LocalRecordKind,
    LocalRecordStore, StoredObjectPresenceRecord,
};
use super::helpers::legacy_record_path;
use crate::{
    DedupeShardMapping, FileChunkRecord, FileId, FileReconstruction, FileRecord, IndexStore,
    MemoryIndexStore, MemoryRecordStore, ProviderRepositoryState, QuarantineCandidate,
    ReconstructionTerm, RecordStore, RetentionHold, WebhookDelivery, XorbId,
    parse_xet_hash_hex, test_invariant_error::LocalSqliteInvariantError, xet_hash_hex_string,
};

fn sample_repository_scope() -> Result<RepositoryScope, Box<dyn Error>> {
    Ok(RepositoryScope::new(
        RepositoryProvider::GitHub,
        "team",
        "assets",
        Some("main"),
    )?)
}

fn sample_record(repository_scope: Option<RepositoryScope>) -> FileRecord {
    FileRecord {
        file_id: "asset.bin".to_owned(),
        content_hash: "a".repeat(64),
        total_bytes: 4,
        chunk_size: 4,
        repository_scope,
        chunks: vec![FileChunkRecord {
            hash: "b".repeat(64),
            offset: 0,
            length: 4,
            range_start: 0,
            range_end: 1,
            packed_start: 0,
            packed_end: 4,
        }],
    }
}

fn open_sqlite_connection(root: &Path) -> Result<Connection, Box<dyn Error>> {
    Ok(Connection::open(
        root.join(LOCAL_METADATA_DATABASE_FILE_NAME),
    )?)
}

fn write_json(path: &Path, value: &impl Serialize) -> Result<(), Box<dyn Error>> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(path, to_vec(value)?)?;
    Ok(())
}

#[tokio::test]
async fn local_record_store_commit_file_version_metadata_is_atomic() {
    let result = exercise_local_record_store_commit_file_version_metadata_is_atomic().await;
    let error = result.as_ref().err().map(ToString::to_string);
    assert!(
        result.is_ok(),
        "local sqlite file-version commit atomicity regression: {error:?}"
    );
}

#[tokio::test]
async fn local_record_store_commit_native_shard_metadata_is_atomic() {
    let result = exercise_local_record_store_commit_native_shard_metadata_is_atomic().await;
    let error = result.as_ref().err().map(ToString::to_string);
    assert!(
        result.is_ok(),
        "local sqlite native shard commit atomicity regression: {error:?}"
    );
}

#[test]
fn local_metadata_root_normalizes_gc_suffix_to_parent_database() {
    let result = exercise_local_metadata_root_normalizes_gc_suffix_to_parent_database();
    let error = result.as_ref().err().map(ToString::to_string);
    assert!(
        result.is_ok(),
        "local sqlite root normalization regression: {error:?}"
    );
}

#[tokio::test]
async fn local_sqlite_imports_legacy_filesystem_metadata() {
    let result = exercise_local_sqlite_imports_legacy_filesystem_metadata().await;
    let error = result.as_ref().err().map(ToString::to_string);
    assert!(
        result.is_ok(),
        "local sqlite legacy filesystem import regression: {error:?}"
    );
}

#[test]
fn local_sqlite_connection_enables_defensive_settings() {
    let result = exercise_local_sqlite_connection_enables_defensive_settings();
    let error = result.as_ref().err().map(ToString::to_string);
    assert!(
        result.is_ok(),
        "local sqlite connection hardening regression: {error:?}"
    );
}

#[test]
fn local_sqlite_rejects_invalid_legacy_import_state() {
    let result = exercise_local_sqlite_rejects_invalid_legacy_import_state();
    let error = result.as_ref().err().map(ToString::to_string);
    assert!(
        result.is_ok(),
        "local sqlite invalid legacy import state regression: {error:?}"
    );
}

#[tokio::test]
async fn local_record_store_reads_corrupt_sqlite_bytes_verbatim() {
    let result = exercise_local_record_store_reads_corrupt_sqlite_bytes_verbatim().await;
    let error = result.as_ref().err().map(ToString::to_string);
    assert!(
        result.is_ok(),
        "local sqlite raw record byte preservation regression: {error:?}"
    );
}

#[tokio::test]
async fn local_sqlite_matches_memory_adapters_across_state_machine_operations() {
    let result =
        exercise_local_sqlite_matches_memory_adapters_across_state_machine_operations().await;
    let error = result.as_ref().err().map(ToString::to_string);
    assert!(
        result.is_ok(),
        "local sqlite state-machine parity regression: {error:?}"
    );
}

async fn exercise_local_record_store_commit_file_version_metadata_is_atomic()
-> Result<(), Box<dyn Error>> {
    let storage = tempfile::tempdir()?;
    let store = LocalRecordStore::new(storage.path().to_path_buf())?;
    let record = sample_record(Some(sample_repository_scope()?));
    let connection = open_sqlite_connection(storage.path())?;
    connection.execute_batch(
        "CREATE TRIGGER fail_latest_insert
         BEFORE INSERT ON shardline_file_records
         WHEN NEW.record_kind = 'latest'
         BEGIN
             SELECT RAISE(FAIL, 'fail latest insert');
         END;",
    )?;

    let result = store.commit_file_version_metadata(&record).await;
    if !matches!(result, Err(LocalIndexStoreError::Sqlite(_))) {
        return Err(LocalSqliteInvariantError::new("expected sqlite trigger failure").into());
    }

    let latest_locator = RecordStore::latest_record_locator(&store, &record);
    let version_locator = RecordStore::version_record_locator(&store, &record);
    if RecordStore::record_locator_exists(&store, &latest_locator).await? {
        return Err(LocalSqliteInvariantError::new(
            "latest locator survived failed transaction",
        )
        .into());
    }
    if RecordStore::record_locator_exists(&store, &version_locator).await? {
        return Err(LocalSqliteInvariantError::new(
            "version locator survived failed transaction",
        )
        .into());
    }

    Ok(())
}

async fn exercise_local_record_store_commit_native_shard_metadata_is_atomic()
-> Result<(), Box<dyn Error>> {
    let storage = tempfile::tempdir()?;
    let record_store = LocalRecordStore::new(storage.path().to_path_buf())?;
    let index_store = LocalIndexStore::new(storage.path().to_path_buf())?;
    let record = sample_record(None);
    let shard_object_key = ObjectKey::parse("shards/aa/native.shard")?;
    let mapping = DedupeShardMapping::new(ShardlineHash::from_bytes([7; 32]), shard_object_key);
    let connection = open_sqlite_connection(storage.path())?;
    connection.execute_batch(
        "CREATE TRIGGER fail_dedupe_insert
         BEFORE INSERT ON shardline_dedupe_shards
         BEGIN
             SELECT RAISE(FAIL, 'fail dedupe insert');
         END;",
    )?;

    let result = record_store
        .commit_native_shard_metadata(from_ref(&record), from_ref(&mapping))
        .await;
    if !matches!(result, Err(LocalIndexStoreError::Sqlite(_))) {
        return Err(
            LocalSqliteInvariantError::new("expected sqlite dedupe trigger failure").into(),
        );
    }
    let latest_locator = RecordStore::latest_record_locator(&record_store, &record);
    let version_locator = RecordStore::version_record_locator(&record_store, &record);
    if RecordStore::record_locator_exists(&record_store, &latest_locator).await? {
        return Err(LocalSqliteInvariantError::new(
            "latest locator survived failed shard transaction",
        )
        .into());
    }
    if RecordStore::record_locator_exists(&record_store, &version_locator).await? {
        return Err(LocalSqliteInvariantError::new(
            "version locator survived failed shard transaction",
        )
        .into());
    }
    if IndexStore::dedupe_shard_mapping(&index_store, &mapping.chunk_hash())?.is_some() {
        return Err(LocalSqliteInvariantError::new(
            "dedupe mapping survived failed shard transaction",
        )
        .into());
    }

    Ok(())
}

fn exercise_local_metadata_root_normalizes_gc_suffix_to_parent_database()
-> Result<(), Box<dyn Error>> {
    let storage = tempfile::tempdir()?;

    let _store = LocalIndexStore::new(storage.path().join("gc"))?;

    if !storage
        .path()
        .join(LOCAL_METADATA_DATABASE_FILE_NAME)
        .is_file()
    {
        return Err(LocalSqliteInvariantError::new(
            "normalized root did not create metadata database",
        )
        .into());
    }
    if storage
        .path()
        .join("gc")
        .join(LOCAL_METADATA_DATABASE_FILE_NAME)
        .exists()
    {
        return Err(LocalSqliteInvariantError::new(
            "gc child directory unexpectedly received sqlite database",
        )
        .into());
    }

    Ok(())
}

fn exercise_local_sqlite_connection_enables_defensive_settings() -> Result<(), Box<dyn Error>> {
    let storage = tempfile::tempdir()?;
    let store = LocalIndexStore::new(storage.path().to_path_buf())?;
    let connection = store.open_connection()?;

    if !connection.db_config(DbConfig::SQLITE_DBCONFIG_DEFENSIVE)? {
        return Err(
            LocalSqliteInvariantError::new("sqlite defensive mode was not enabled").into(),
        );
    }

    let trusted_schema =
        connection.pragma_query_value(None, "trusted_schema", |row| row.get::<_, i64>(0))?;
    if trusted_schema != 0 {
        return Err(
            LocalSqliteInvariantError::new("sqlite trusted_schema remained enabled").into(),
        );
    }

    let cell_size_check =
        connection.pragma_query_value(None, "cell_size_check", |row| row.get::<_, i64>(0))?;
    if cell_size_check != 1 {
        return Err(
            LocalSqliteInvariantError::new("sqlite cell_size_check was not enabled").into(),
        );
    }

    Ok(())
}

fn exercise_local_sqlite_rejects_invalid_legacy_import_state() -> Result<(), Box<dyn Error>> {
    let storage = tempfile::tempdir()?;
    let _store = LocalIndexStore::new(storage.path().to_path_buf())?;
    let connection = open_sqlite_connection(storage.path())?;
    connection.execute(
        "UPDATE shardline_local_metadata_meta
         SET value = ?1
         WHERE key = ?2",
        params!["not-a-boolean", LEGACY_IMPORT_COMPLETED_KEY],
    )?;

    let reopened = LocalIndexStore::new(storage.path().to_path_buf());
    if !matches!(
        reopened,
        Err(LocalIndexStoreError::InvalidLegacyImportState)
    ) {
        return Err(
            LocalSqliteInvariantError::new("invalid import state did not fail closed").into(),
        );
    }

    Ok(())
}

async fn exercise_local_record_store_reads_corrupt_sqlite_bytes_verbatim()
-> Result<(), Box<dyn Error>> {
    let storage = tempfile::tempdir()?;
    let store = LocalRecordStore::new(storage.path().to_path_buf())?;
    let record = sample_record(Some(sample_repository_scope()?));
    RecordStore::write_version_record(&store, &record).await?;
    RecordStore::write_latest_record(&store, &record).await?;

    let corrupt_bytes = b"{not-json".to_vec();
    let connection = open_sqlite_connection(storage.path())?;
    connection.execute(
        "UPDATE shardline_file_records
         SET record = ?1
         WHERE record_kind = ?2 AND file_id = ?3 AND content_hash = ?4",
        params![
            corrupt_bytes.clone(),
            "version",
            record.file_id,
            record.content_hash,
        ],
    )?;

    let version_locator = RecordStore::version_record_locator(&store, &record);
    let loaded = RecordStore::read_record_bytes(&store, &version_locator).await?;
    if loaded != corrupt_bytes {
        return Err(LocalSqliteInvariantError::new(
            "sqlite record read normalized corrupt bytes",
        )
        .into());
    }

    Ok(())
}

async fn exercise_local_sqlite_matches_memory_adapters_across_state_machine_operations()
-> Result<(), Box<dyn Error>> {
    let storage = tempfile::tempdir()?;
    let local_index_store = LocalIndexStore::new(storage.path().to_path_buf())?;
    let local_record_store = LocalRecordStore::new(storage.path().to_path_buf())?;
    let memory_index_store = MemoryIndexStore::new();
    let memory_record_store = MemoryRecordStore::new();
    let fixtures = StateMachineFixtures::load()?;

    let mut lcg_state = 0x5eed_cafe_d15c_a11e_u64;
    for step in 0..96_usize {
        lcg_state = next_state_machine_value(lcg_state);
        let record = state_machine_item(
            fixtures.records.as_slice(),
            state_machine_index(lcg_state, 0, fixtures.records.len())?,
            "record",
        )?;
        let reconstruction = state_machine_item(
            fixtures.reconstructions.as_slice(),
            state_machine_index(lcg_state, 8, fixtures.reconstructions.len())?,
            "reconstruction",
        )?;
        let mapping = state_machine_item(
            fixtures.dedupe_mappings.as_slice(),
            state_machine_index(lcg_state, 16, fixtures.dedupe_mappings.len())?,
            "dedupe mapping",
        )?;
        let quarantine_candidate = state_machine_item(
            fixtures.quarantine_candidates.as_slice(),
            state_machine_index(lcg_state, 24, fixtures.quarantine_candidates.len())?,
            "quarantine candidate",
        )?;
        let retention_hold = state_machine_item(
            fixtures.retention_holds.as_slice(),
            state_machine_index(lcg_state, 32, fixtures.retention_holds.len())?,
            "retention hold",
        )?;
        let webhook_delivery = state_machine_item(
            fixtures.webhook_deliveries.as_slice(),
            state_machine_index(lcg_state, 40, fixtures.webhook_deliveries.len())?,
            "webhook delivery",
        )?;
        let provider_state = state_machine_item(
            fixtures.provider_states.as_slice(),
            state_machine_index(lcg_state, 48, fixtures.provider_states.len())?,
            "provider repository state",
        )?;
        let xorb_id = state_machine_item(
            fixtures.xorb_ids.as_slice(),
            state_machine_index(lcg_state, 12, fixtures.xorb_ids.len())?,
            "xorb marker",
        )?;

        match step & 15 {
            0 => {
                RecordStore::write_latest_record(&local_record_store, record).await?;
                RecordStore::write_latest_record(&memory_record_store, record).await?;
            }
            1 => {
                RecordStore::write_version_record(&local_record_store, record).await?;
                RecordStore::write_version_record(&memory_record_store, record).await?;
            }
            2 => {
                let local_locator =
                    RecordStore::latest_record_locator(&local_record_store, record);
                let memory_locator =
                    RecordStore::latest_record_locator(&memory_record_store, record);
                let local_exists =
                    RecordStore::record_locator_exists(&local_record_store, &local_locator)
                        .await?;
                let memory_exists =
                    RecordStore::record_locator_exists(&memory_record_store, &memory_locator)
                        .await?;
                if local_exists != memory_exists {
                    return Err(LocalSqliteInvariantError::new(
                        "local and memory latest-record existence diverged",
                    )
                    .into());
                }
                if local_exists {
                    RecordStore::delete_record_locator(&local_record_store, &local_locator)
                        .await?;
                    RecordStore::delete_record_locator(&memory_record_store, &memory_locator)
                        .await?;
                }
            }
            3 => {
                let local_locator =
                    RecordStore::version_record_locator(&local_record_store, record);
                let memory_locator =
                    RecordStore::version_record_locator(&memory_record_store, record);
                let local_exists =
                    RecordStore::record_locator_exists(&local_record_store, &local_locator)
                        .await?;
                let memory_exists =
                    RecordStore::record_locator_exists(&memory_record_store, &memory_locator)
                        .await?;
                if local_exists != memory_exists {
                    return Err(LocalSqliteInvariantError::new(
                        "local and memory version-record existence diverged",
                    )
                    .into());
                }
                if local_exists {
                    RecordStore::delete_record_locator(&local_record_store, &local_locator)
                        .await?;
                    RecordStore::delete_record_locator(&memory_record_store, &memory_locator)
                        .await?;
                }
            }
            4 => {
                local_index_store
                    .insert_reconstruction(&reconstruction.0, &reconstruction.1)?;
                memory_index_store
                    .insert_reconstruction(&reconstruction.0, &reconstruction.1)?;
            }
            5 => {
                let local_deleted =
                    IndexStore::delete_reconstruction(&local_index_store, &reconstruction.0)?;
                let memory_deleted =
                    IndexStore::delete_reconstruction(&memory_index_store, &reconstruction.0)?;
                if local_deleted != memory_deleted {
                    return Err(LocalSqliteInvariantError::new(
                        "reconstruction delete behavior diverged from memory",
                    )
                    .into());
                }
            }
            6 => {
                local_index_store.insert_xorb(xorb_id)?;
                memory_index_store.insert_xorb(xorb_id)?;
            }
            7 => {
                local_index_store.upsert_dedupe_shard_mapping(mapping)?;
                memory_index_store.upsert_dedupe_shard_mapping(mapping)?;
            }
            8 => {
                let local_deleted = IndexStore::delete_dedupe_shard_mapping(
                    &local_index_store,
                    &mapping.chunk_hash(),
                )?;
                let memory_deleted = IndexStore::delete_dedupe_shard_mapping(
                    &memory_index_store,
                    &mapping.chunk_hash(),
                )?;
                if local_deleted != memory_deleted {
                    return Err(LocalSqliteInvariantError::new(
                        "dedupe mapping delete behavior diverged from memory",
                    )
                    .into());
                }
            }
            9 => {
                local_index_store.upsert_quarantine_candidate(quarantine_candidate)?;
                memory_index_store.upsert_quarantine_candidate(quarantine_candidate)?;
            }
            10 => {
                let local_deleted = IndexStore::delete_quarantine_candidate(
                    &local_index_store,
                    quarantine_candidate.object_key(),
                )?;
                let memory_deleted = IndexStore::delete_quarantine_candidate(
                    &memory_index_store,
                    quarantine_candidate.object_key(),
                )?;
                if local_deleted != memory_deleted {
                    return Err(LocalSqliteInvariantError::new(
                        "quarantine delete behavior diverged from memory",
                    )
                    .into());
                }
            }
            11 => {
                local_index_store.upsert_retention_hold(retention_hold)?;
                memory_index_store.upsert_retention_hold(retention_hold)?;
            }
            12 => {
                let local_deleted = IndexStore::delete_retention_hold(
                    &local_index_store,
                    retention_hold.object_key(),
                )?;
                let memory_deleted = IndexStore::delete_retention_hold(
                    &memory_index_store,
                    retention_hold.object_key(),
                )?;
                if local_deleted != memory_deleted {
                    return Err(LocalSqliteInvariantError::new(
                        "retention hold delete behavior diverged from memory",
                    )
                    .into());
                }
            }
            13 => {
                let local_recorded =
                    local_index_store.record_webhook_delivery(webhook_delivery)?;
                let memory_recorded =
                    memory_index_store.record_webhook_delivery(webhook_delivery)?;
                if local_recorded != memory_recorded {
                    return Err(LocalSqliteInvariantError::new(
                        "webhook delivery insert behavior diverged from memory",
                    )
                    .into());
                }
            }
            14 => {
                let local_deleted =
                    IndexStore::delete_webhook_delivery(&local_index_store, webhook_delivery)?;
                let memory_deleted =
                    IndexStore::delete_webhook_delivery(&memory_index_store, webhook_delivery)?;
                if local_deleted != memory_deleted {
                    return Err(LocalSqliteInvariantError::new(
                        "webhook delivery delete behavior diverged from memory",
                    )
                    .into());
                }
            }
            15 => {
                let upsert_even_step = ((lcg_state >> 20) & 1) == 0;
                if upsert_even_step {
                    local_index_store.upsert_provider_repository_state(provider_state)?;
                    memory_index_store.upsert_provider_repository_state(provider_state)?;
                } else {
                    let local_deleted = IndexStore::delete_provider_repository_state(
                        &local_index_store,
                        provider_state.provider(),
                        provider_state.owner(),
                        provider_state.repo(),
                    )?;
                    let memory_deleted = IndexStore::delete_provider_repository_state(
                        &memory_index_store,
                        provider_state.provider(),
                        provider_state.owner(),
                        provider_state.repo(),
                    )?;
                    if local_deleted != memory_deleted {
                        return Err(LocalSqliteInvariantError::new(
                            "provider state delete behavior diverged from memory",
                        )
                        .into());
                    }
                }
            }
            _other => {
                return Err(LocalSqliteInvariantError::new(
                    "state-machine operation selection overflowed",
                )
                .into());
            }
        }

        assert_local_sqlite_matches_memory_state(
            &local_record_store,
            &memory_record_store,
            &local_index_store,
            &memory_index_store,
            &fixtures,
        )
        .await?;
    }

    Ok(())
}

async fn exercise_local_sqlite_imports_legacy_filesystem_metadata() -> Result<(), Box<dyn Error>> {
    let storage = tempfile::tempdir()?;
    let scope = sample_repository_scope()?;
    let record = sample_record(Some(scope.clone()));
    write_json(
        &legacy_record_path(storage.path(), LocalRecordKind::Latest, &record),
        &record,
    )?;
    write_json(
        &legacy_record_path(storage.path(), LocalRecordKind::Version, &record),
        &record,
    )?;

    let reconstruction_hash = ShardlineHash::from_bytes([3; 32]);
    let file_id = FileId::new(reconstruction_hash);
    let xorb_id = XorbId::new(reconstruction_hash);
    let reconstruction = FileReconstruction::new(vec![ReconstructionTerm::new(
        xorb_id,
        ChunkRange::new(0, 1)?,
        4,
    )]);
    write_json(
        &storage
            .path()
            .join("gc")
            .join("reconstructions")
            .join(format!("{}.json", xet_hash_hex_string(file_id.hash()))),
        &FileReconstructionRecord::from_domain(&reconstruction),
    )?;

    let xorb_hash = xet_hash_hex_string(ShardlineHash::from_bytes([4; 32]));
    write_json(
        &storage
            .path()
            .join("gc")
            .join("xorbs")
            .join(format!("{xorb_hash}.json")),
        &StoredObjectPresenceRecord {
            hash: xorb_hash.clone(),
        },
    )?;

    let dedupe_chunk_hash = xet_hash_hex_string(ShardlineHash::from_bytes([5; 32]));
    let dedupe_mapping = DedupeShardMapping::new(
        parse_xet_hash_hex(&dedupe_chunk_hash)?,
        ObjectKey::parse("shards/aa/native.shard")?,
    );
    write_json(
        &storage
            .path()
            .join("gc")
            .join("dedupe-shards")
            .join(format!("{dedupe_chunk_hash}.json")),
        &DedupeShardRecord {
            chunk_hash: dedupe_chunk_hash.clone(),
            shard_object_key: dedupe_mapping.shard_object_key().as_str().to_owned(),
        },
    )?;

    let quarantine_hash = xet_hash_hex_string(ShardlineHash::from_bytes([6; 32]));
    let quarantine_candidate = QuarantineCandidate::new(
        ObjectKey::parse(&format!("{}/{}", &quarantine_hash[..2], quarantine_hash))?,
        9,
        10,
        20,
    )?;
    write_json(
        &storage
            .path()
            .join("gc")
            .join("quarantine")
            .join("candidate.json"),
        &LegacyQuarantineCandidateRecord {
            hash: quarantine_hash.clone(),
            bytes: quarantine_candidate.observed_length(),
            first_seen_unreachable_at_unix_seconds: quarantine_candidate
                .first_seen_unreachable_at_unix_seconds(),
            delete_after_unix_seconds: quarantine_candidate.delete_after_unix_seconds(),
        },
    )?;

    let retention_hold = RetentionHold::new(
        ObjectKey::parse("chunks/aa/held-object")?,
        "provider deletion grace".to_owned(),
        30,
        Some(40),
    )?;
    write_json(
        &storage
            .path()
            .join("gc")
            .join("retention-holds")
            .join("hold.json"),
        &json!({
            "object_key": retention_hold.object_key().as_str(),
            "reason": retention_hold.reason(),
            "held_at_unix_seconds": retention_hold.held_at_unix_seconds(),
            "release_after_unix_seconds": retention_hold.release_after_unix_seconds(),
        }),
    )?;

    let webhook_delivery = WebhookDelivery::new(
        RepositoryProvider::GitHub,
        "team".to_owned(),
        "assets".to_owned(),
        "delivery-1".to_owned(),
        50,
    )?;
    write_json(
        &storage
            .path()
            .join("gc")
            .join("webhook-deliveries")
            .join("delivery.json"),
        &json!({
            "provider": webhook_delivery.provider().as_str(),
            "owner": webhook_delivery.owner(),
            "repo": webhook_delivery.repo(),
            "delivery_id": webhook_delivery.delivery_id(),
            "processed_at_unix_seconds": webhook_delivery.processed_at_unix_seconds(),
        }),
    )?;

    let provider_state = ProviderRepositoryState::new(
        RepositoryProvider::GitHub,
        "team".to_owned(),
        "assets".to_owned(),
        Some(60),
        Some(70),
        Some("refs/heads/main".to_owned()),
    )
    .with_reconciliation(Some(80), Some(81), Some(82));
    write_json(
        &storage
            .path()
            .join("gc")
            .join("provider-repository-states")
            .join("state.json"),
        &json!({
            "provider": provider_state.provider().as_str(),
            "owner": provider_state.owner(),
            "repo": provider_state.repo(),
            "last_access_changed_at_unix_seconds": provider_state.last_access_changed_at_unix_seconds(),
            "last_revision_pushed_at_unix_seconds": provider_state.last_revision_pushed_at_unix_seconds(),
            "last_pushed_revision": provider_state.last_pushed_revision(),
            "last_cache_invalidated_at_unix_seconds": provider_state.last_cache_invalidated_at_unix_seconds(),
            "last_authorization_rechecked_at_unix_seconds": provider_state.last_authorization_rechecked_at_unix_seconds(),
            "last_drift_checked_at_unix_seconds": provider_state.last_drift_checked_at_unix_seconds(),
        }),
    )?;

    let record_store = LocalRecordStore::new(storage.path().to_path_buf())?;
    let index_store = LocalIndexStore::new(storage.path().to_path_buf())?;

    let latest_locator = RecordStore::latest_record_locator(&record_store, &record);
    let version_locator = RecordStore::version_record_locator(&record_store, &record);
    if !RecordStore::record_locator_exists(&record_store, &latest_locator).await? {
        return Err(
            LocalSqliteInvariantError::new("latest legacy record was not imported").into(),
        );
    }
    if !RecordStore::record_locator_exists(&record_store, &version_locator).await? {
        return Err(
            LocalSqliteInvariantError::new("version legacy record was not imported").into(),
        );
    }
    let latest_record = from_slice::<FileRecord>(
        &RecordStore::read_record_bytes(&record_store, &latest_locator).await?,
    )?;
    if latest_record != record {
        return Err(LocalSqliteInvariantError::new(
            "latest legacy record contents changed during import",
        )
        .into());
    }
    if IndexStore::reconstruction(&index_store, &file_id)? != Some(reconstruction) {
        return Err(
            LocalSqliteInvariantError::new("reconstruction row was not imported").into(),
        );
    }
    if !IndexStore::contains_xorb(&index_store, &XorbId::new(parse_xet_hash_hex(&xorb_hash)?))?
    {
        return Err(LocalSqliteInvariantError::new("xorb marker was not imported").into());
    }
    if IndexStore::dedupe_shard_mapping(&index_store, &parse_xet_hash_hex(&dedupe_chunk_hash)?)?
        != Some(dedupe_mapping)
    {
        return Err(LocalSqliteInvariantError::new("dedupe mapping was not imported").into());
    }
    if IndexStore::quarantine_candidate(&index_store, quarantine_candidate.object_key())?
        != Some(quarantine_candidate)
    {
        return Err(
            LocalSqliteInvariantError::new("quarantine candidate was not imported").into(),
        );
    }
    if IndexStore::retention_hold(&index_store, retention_hold.object_key())?
        != Some(retention_hold)
    {
        return Err(LocalSqliteInvariantError::new("retention hold was not imported").into());
    }
    if IndexStore::list_webhook_deliveries(&index_store)? != vec![webhook_delivery] {
        return Err(LocalSqliteInvariantError::new("webhook delivery was not imported").into());
    }
    if IndexStore::provider_repository_state(
        &index_store,
        provider_state.provider(),
        provider_state.owner(),
        provider_state.repo(),
    )? != Some(provider_state)
    {
        return Err(LocalSqliteInvariantError::new(
            "provider repository state was not imported",
        )
        .into());
    }

    let connection = open_sqlite_connection(storage.path())?;
    let applied_versions = connection.query_row(
        &format!(
            "SELECT COUNT(*)
             FROM {LOCAL_SCHEMA_MIGRATIONS_TABLE}"
        ),
        [],
        |row| row.get::<_, i64>(0),
    )?;
    let expected_applied_versions = i64::try_from(LOCAL_SQLITE_MIGRATIONS.len())?;
    if applied_versions != expected_applied_versions {
        return Err(
            LocalSqliteInvariantError::new("sqlite migrations were not fully applied").into(),
        );
    }

    Ok(())
}

fn sample_state_machine_records() -> Result<Vec<FileRecord>, Box<dyn Error>> {
    let scope_a = RepositoryScope::new(
        RepositoryProvider::GitHub,
        "team-a",
        "assets-a",
        Some("main"),
    )?;
    let scope_b = RepositoryScope::new(
        RepositoryProvider::Gitea,
        "team-b",
        "assets-b",
        Some("release"),
    )?;
    Ok(vec![
        FileRecord {
            file_id: "alpha.bin".to_owned(),
            content_hash: xet_hash_hex_string(ShardlineHash::from_bytes([11; 32])),
            total_bytes: 4,
            chunk_size: 4,
            repository_scope: None,
            chunks: vec![FileChunkRecord {
                hash: xet_hash_hex_string(ShardlineHash::from_bytes([21; 32])),
                offset: 0,
                length: 4,
                range_start: 0,
                range_end: 1,
                packed_start: 0,
                packed_end: 4,
            }],
        },
        FileRecord {
            file_id: "beta.bin".to_owned(),
            content_hash: xet_hash_hex_string(ShardlineHash::from_bytes([12; 32])),
            total_bytes: 8,
            chunk_size: 4,
            repository_scope: Some(scope_a),
            chunks: vec![
                FileChunkRecord {
                    hash: xet_hash_hex_string(ShardlineHash::from_bytes([22; 32])),
                    offset: 0,
                    length: 4,
                    range_start: 0,
                    range_end: 1,
                    packed_start: 0,
                    packed_end: 4,
                },
                FileChunkRecord {
                    hash: xet_hash_hex_string(ShardlineHash::from_bytes([23; 32])),
                    offset: 4,
                    length: 4,
                    range_start: 1,
                    range_end: 2,
                    packed_start: 4,
                    packed_end: 8,
                },
            ],
        },
        FileRecord {
            file_id: "gamma.bin".to_owned(),
            content_hash: xet_hash_hex_string(ShardlineHash::from_bytes([13; 32])),
            total_bytes: 6,
            chunk_size: 6,
            repository_scope: Some(scope_b),
            chunks: vec![FileChunkRecord {
                hash: xet_hash_hex_string(ShardlineHash::from_bytes([24; 32])),
                offset: 0,
                length: 6,
                range_start: 0,
                range_end: 1,
                packed_start: 0,
                packed_end: 6,
            }],
        },
    ])
}

fn sample_state_machine_reconstructions()
-> Result<Vec<(FileId, FileReconstruction)>, Box<dyn Error>> {
    Ok(vec![
        (
            FileId::new(ShardlineHash::from_bytes([41; 32])),
            FileReconstruction::new(vec![ReconstructionTerm::new(
                XorbId::new(ShardlineHash::from_bytes([51; 32])),
                ChunkRange::new(0, 1)?,
                4,
            )]),
        ),
        (
            FileId::new(ShardlineHash::from_bytes([42; 32])),
            FileReconstruction::new(vec![
                ReconstructionTerm::new(
                    XorbId::new(ShardlineHash::from_bytes([52; 32])),
                    ChunkRange::new(0, 1)?,
                    8,
                ),
                ReconstructionTerm::new(
                    XorbId::new(ShardlineHash::from_bytes([53; 32])),
                    ChunkRange::new(1, 2)?,
                    8,
                ),
            ]),
        ),
    ])
}

fn sample_state_machine_xorbs() -> Vec<XorbId> {
    vec![
        XorbId::new(ShardlineHash::from_bytes([51; 32])),
        XorbId::new(ShardlineHash::from_bytes([52; 32])),
        XorbId::new(ShardlineHash::from_bytes([53; 32])),
        XorbId::new(ShardlineHash::from_bytes([54; 32])),
    ]
}

fn sample_state_machine_dedupe_mappings() -> Result<Vec<DedupeShardMapping>, Box<dyn Error>> {
    Ok(vec![
        DedupeShardMapping::new(
            ShardlineHash::from_bytes([61; 32]),
            ObjectKey::parse("shards/aa/native-a.shard")?,
        ),
        DedupeShardMapping::new(
            ShardlineHash::from_bytes([62; 32]),
            ObjectKey::parse("shards/bb/native-b.shard")?,
        ),
    ])
}

fn sample_state_machine_quarantine_candidates()
-> Result<Vec<QuarantineCandidate>, Box<dyn Error>> {
    Ok(vec![
        QuarantineCandidate::new(ObjectKey::parse("aa/aaaaaaaa")?, 4, 10, 20)?,
        QuarantineCandidate::new(ObjectKey::parse("bb/bbbbbbbb")?, 8, 11, 21)?,
    ])
}

fn sample_state_machine_retention_holds() -> Result<Vec<RetentionHold>, Box<dyn Error>> {
    Ok(vec![
        RetentionHold::new(
            ObjectKey::parse("cc/cccccccc")?,
            "provider deletion grace".to_owned(),
            12,
            Some(22),
        )?,
        RetentionHold::new(
            ObjectKey::parse("dd/dddddddd")?,
            "manual retain".to_owned(),
            13,
            None,
        )?,
    ])
}

fn sample_state_machine_webhook_deliveries() -> Result<Vec<WebhookDelivery>, Box<dyn Error>> {
    Ok(vec![
        WebhookDelivery::new(
            RepositoryProvider::GitHub,
            "team-a".to_owned(),
            "assets-a".to_owned(),
            "delivery-a".to_owned(),
            30,
        )?,
        WebhookDelivery::new(
            RepositoryProvider::GitLab,
            "team-b".to_owned(),
            "assets-b".to_owned(),
            "delivery-b".to_owned(),
            31,
        )?,
    ])
}

fn sample_state_machine_provider_states() -> Vec<ProviderRepositoryState> {
    vec![
        ProviderRepositoryState::new(
            RepositoryProvider::GitHub,
            "team-a".to_owned(),
            "assets-a".to_owned(),
            Some(40),
            Some(41),
            Some("refs/heads/main".to_owned()),
        )
        .with_reconciliation(Some(42), Some(43), Some(44)),
        ProviderRepositoryState::new(
            RepositoryProvider::Gitea,
            "team-b".to_owned(),
            "assets-b".to_owned(),
            Some(45),
            None,
            None,
        )
        .with_reconciliation(None, Some(46), None),
    ]
}

type CanonicalChunk = (String, u64, u64, u32, u32, u64, u64);
type CanonicalRecord = (
    Option<String>,
    String,
    String,
    u64,
    u64,
    Vec<CanonicalChunk>,
);
type CanonicalProviderState = (
    String,
    String,
    String,
    Option<u64>,
    Option<u64>,
    Option<String>,
    Option<u64>,
    Option<u64>,
    Option<u64>,
);

struct StateMachineFixtures {
    records: Vec<FileRecord>,
    reconstructions: Vec<(FileId, FileReconstruction)>,
    xorb_ids: Vec<XorbId>,
    dedupe_mappings: Vec<DedupeShardMapping>,
    quarantine_candidates: Vec<QuarantineCandidate>,
    retention_holds: Vec<RetentionHold>,
    webhook_deliveries: Vec<WebhookDelivery>,
    provider_states: Vec<ProviderRepositoryState>,
}

impl StateMachineFixtures {
    fn load() -> Result<Self, Box<dyn Error>> {
        Ok(Self {
            records: sample_state_machine_records()?,
            reconstructions: sample_state_machine_reconstructions()?,
            xorb_ids: sample_state_machine_xorbs(),
            dedupe_mappings: sample_state_machine_dedupe_mappings()?,
            quarantine_candidates: sample_state_machine_quarantine_candidates()?,
            retention_holds: sample_state_machine_retention_holds()?,
            webhook_deliveries: sample_state_machine_webhook_deliveries()?,
            provider_states: sample_state_machine_provider_states(),
        })
    }
}

const fn next_state_machine_value(state: u64) -> u64 {
    state
        .wrapping_mul(6_364_136_223_846_793_005)
        .wrapping_add(1_442_695_040_888_963_407)
}

fn state_machine_index(state: u64, shift: u32, len: usize) -> Result<usize, Box<dyn Error>> {
    let len_u64 = u64::try_from(len)?;
    if len_u64 == 0 {
        return Err(
            LocalSqliteInvariantError::new("state-machine fixture list was empty").into(),
        );
    }
    let shifted = state.checked_shr(shift).unwrap_or(0);
    let bounded = shifted
        .checked_rem(len_u64)
        .ok_or_else(|| LocalSqliteInvariantError::new("state-machine modulus overflowed"))?;
    Ok(usize::try_from(bounded)?)
}

fn state_machine_item<'values, T>(
    values: &'values [T],
    index: usize,
    what: &str,
) -> Result<&'values T, Box<dyn Error>> {
    values.get(index).ok_or_else(|| {
        LocalSqliteInvariantError::new(format!("missing state-machine {what} at index {index}"))
            .into()
    })
}

async fn assert_local_sqlite_matches_memory_state(
    local_record_store: &LocalRecordStore,
    memory_record_store: &MemoryRecordStore,
    local_index_store: &LocalIndexStore,
    memory_index_store: &MemoryIndexStore,
    fixtures: &StateMachineFixtures,
) -> Result<(), Box<dyn Error>> {
    if canonical_record_entries(local_record_store, true).await?
        != canonical_record_entries(memory_record_store, true).await?
    {
        return Err(
            LocalSqliteInvariantError::new("latest-record state diverged from memory").into(),
        );
    }
    if canonical_record_entries(local_record_store, false).await?
        != canonical_record_entries(memory_record_store, false).await?
    {
        return Err(LocalSqliteInvariantError::new(
            "version-record state diverged from memory",
        )
        .into());
    }

    for record in &fixtures.records {
        if RecordStore::read_latest_record_bytes(local_record_store, record).await?
            != RecordStore::read_latest_record_bytes(memory_record_store, record).await?
        {
            return Err(LocalSqliteInvariantError::new(
                "latest-record lookup diverged from memory",
            )
            .into());
        }
    }

    let reconstruction_file_ids = fixtures
        .reconstructions
        .iter()
        .map(|(file_id, _reconstruction)| *file_id)
        .collect::<Vec<_>>();
    for file_id in reconstruction_file_ids {
        if IndexStore::reconstruction(local_index_store, &file_id)?
            != IndexStore::reconstruction(memory_index_store, &file_id)?
        {
            return Err(LocalSqliteInvariantError::new(
                "reconstruction state diverged from memory",
            )
            .into());
        }
    }

    for xorb_id in &fixtures.xorb_ids {
        if IndexStore::contains_xorb(local_index_store, xorb_id)?
            != IndexStore::contains_xorb(memory_index_store, xorb_id)?
        {
            return Err(LocalSqliteInvariantError::new(
                "xorb marker state diverged from memory",
            )
            .into());
        }
    }

    if canonical_dedupe_mappings(IndexStore::list_dedupe_shard_mappings(local_index_store)?)
        != canonical_dedupe_mappings(IndexStore::list_dedupe_shard_mappings(
            memory_index_store,
        )?)
    {
        return Err(LocalSqliteInvariantError::new(
            "dedupe mapping state diverged from memory",
        )
        .into());
    }

    if canonical_quarantine_candidates(IndexStore::list_quarantine_candidates(
        local_index_store,
    )?) != canonical_quarantine_candidates(IndexStore::list_quarantine_candidates(
        memory_index_store,
    )?) {
        return Err(LocalSqliteInvariantError::new(
            "quarantine candidate state diverged from memory",
        )
        .into());
    }

    if canonical_retention_holds(IndexStore::list_retention_holds(local_index_store)?)
        != canonical_retention_holds(IndexStore::list_retention_holds(memory_index_store)?)
    {
        return Err(LocalSqliteInvariantError::new(
            "retention hold state diverged from memory",
        )
        .into());
    }

    if canonical_webhook_deliveries(IndexStore::list_webhook_deliveries(local_index_store)?)
        != canonical_webhook_deliveries(IndexStore::list_webhook_deliveries(
            memory_index_store,
        )?)
    {
        return Err(LocalSqliteInvariantError::new(
            "webhook delivery state diverged from memory",
        )
        .into());
    }

    if canonical_provider_states(IndexStore::list_provider_repository_states(
        local_index_store,
    )?) != canonical_provider_states(IndexStore::list_provider_repository_states(
        memory_index_store,
    )?) {
        return Err(LocalSqliteInvariantError::new(
            "provider repository state diverged from memory",
        )
        .into());
    }

    let mut expected_provider_keys = fixtures
        .provider_states
        .iter()
        .map(|state| {
            (
                state.provider(),
                state.owner().to_owned(),
                state.repo().to_owned(),
            )
        })
        .collect::<Vec<_>>();
    expected_provider_keys.sort_by(|left, right| {
        (left.0.as_str(), left.1.as_str(), left.2.as_str()).cmp(&(
            right.0.as_str(),
            right.1.as_str(),
            right.2.as_str(),
        ))
    });
    expected_provider_keys.dedup();
    for (provider, owner, repo) in expected_provider_keys {
        if IndexStore::provider_repository_state(local_index_store, provider, &owner, &repo)?
            != IndexStore::provider_repository_state(
                memory_index_store,
                provider,
                &owner,
                &repo,
            )?
        {
            return Err(LocalSqliteInvariantError::new(
                "provider repository lookup diverged from memory",
            )
            .into());
        }
    }

    Ok(())
}

async fn canonical_record_entries<Store>(
    store: &Store,
    latest: bool,
) -> Result<Vec<CanonicalRecord>, Box<dyn Error>>
where
    Store: RecordStore,
    Store::Error: Error + Send + Sync + 'static,
{
    let locators = if latest {
        RecordStore::list_latest_record_locators(store).await?
    } else {
        RecordStore::list_version_record_locators(store).await?
    };
    let mut entries = Vec::with_capacity(locators.len());
    for locator in locators {
        let bytes = RecordStore::read_record_bytes(store, &locator).await?;
        let record = from_slice::<FileRecord>(&bytes)?;
        entries.push(canonical_file_record(&record));
    }
    entries.sort();
    Ok(entries)
}

fn canonical_file_record(record: &FileRecord) -> CanonicalRecord {
    let scope = record.repository_scope.as_ref().map(|repository_scope| {
        format!(
            "{}:{}/{}/{}",
            repository_scope.provider().as_str(),
            repository_scope.owner(),
            repository_scope.name(),
            repository_scope.revision().unwrap_or("")
        )
    });
    let chunks = record
        .chunks
        .iter()
        .map(|chunk| {
            (
                chunk.hash.clone(),
                chunk.offset,
                chunk.length,
                chunk.range_start,
                chunk.range_end,
                chunk.packed_start,
                chunk.packed_end,
            )
        })
        .collect::<Vec<_>>();
    (
        scope,
        record.file_id.clone(),
        record.content_hash.clone(),
        record.total_bytes,
        record.chunk_size,
        chunks,
    )
}

fn canonical_dedupe_mappings(mappings: Vec<DedupeShardMapping>) -> Vec<(String, String)> {
    let mut canonical = mappings
        .into_iter()
        .map(|mapping| {
            (
                xet_hash_hex_string(mapping.chunk_hash()),
                mapping.shard_object_key().as_str().to_owned(),
            )
        })
        .collect::<Vec<_>>();
    canonical.sort();
    canonical
}

fn canonical_quarantine_candidates(
    candidates: Vec<QuarantineCandidate>,
) -> Vec<(String, u64, u64, u64)> {
    let mut canonical = candidates
        .into_iter()
        .map(|candidate| {
            (
                candidate.object_key().as_str().to_owned(),
                candidate.observed_length(),
                candidate.first_seen_unreachable_at_unix_seconds(),
                candidate.delete_after_unix_seconds(),
            )
        })
        .collect::<Vec<_>>();
    canonical.sort();
    canonical
}

fn canonical_retention_holds(
    holds: Vec<RetentionHold>,
) -> Vec<(String, String, u64, Option<u64>)> {
    let mut canonical = holds
        .into_iter()
        .map(|hold| {
            (
                hold.object_key().as_str().to_owned(),
                hold.reason().to_owned(),
                hold.held_at_unix_seconds(),
                hold.release_after_unix_seconds(),
            )
        })
        .collect::<Vec<_>>();
    canonical.sort();
    canonical
}

fn canonical_webhook_deliveries(
    deliveries: Vec<WebhookDelivery>,
) -> Vec<(String, String, String, String, u64)> {
    let mut canonical = deliveries
        .into_iter()
        .map(|delivery| {
            (
                delivery.provider().as_str().to_owned(),
                delivery.owner().to_owned(),
                delivery.repo().to_owned(),
                delivery.delivery_id().to_owned(),
                delivery.processed_at_unix_seconds(),
            )
        })
        .collect::<Vec<_>>();
    canonical.sort();
    canonical
}

fn canonical_provider_states(
    states: Vec<ProviderRepositoryState>,
) -> Vec<CanonicalProviderState> {
    let mut canonical = states
        .into_iter()
        .map(|state| {
            (
                state.provider().as_str().to_owned(),
                state.owner().to_owned(),
                state.repo().to_owned(),
                state.last_access_changed_at_unix_seconds(),
                state.last_revision_pushed_at_unix_seconds(),
                state.last_pushed_revision().map(ToOwned::to_owned),
                state.last_cache_invalidated_at_unix_seconds(),
                state.last_authorization_rechecked_at_unix_seconds(),
                state.last_drift_checked_at_unix_seconds(),
            )
        })
        .collect::<Vec<_>>();
    canonical.sort();
    canonical
}
