use std::fs::{self, OpenOptions};
use std::io::Write as _;
use std::path::PathBuf;

use serde::Serialize;
use shardline_protocol::{ChunkRange, RepositoryProvider, ShardlineHash};
use shardline_storage::ObjectKey;

use super::{
    LocalIndexStore, LocalIndexStoreError, MAX_CONTROL_PLANE_METADATA_BYTES,
    MAX_RECONSTRUCTION_METADATA_BYTES, hex_encode_component, read_file_if_exists_bounded,
    set_before_local_metadata_read_hook,
};
use crate::local_fs::set_before_local_write_hook;
use crate::{
    DedupeShardMapping, FileId, FileReconstruction, IndexStore, QuarantineCandidate,
    QuarantineCandidateError, ReconstructionTerm, RetentionHold, WebhookDelivery, XorbId,
};

#[test]
fn local_index_store_roundtrips_reconstruction_xorb_and_quarantine_state() {
    let storage = shardline_test_support::TempStorage::new();
    let store = LocalIndexStore::new(storage.path_buf()).unwrap();

    let hash = ShardlineHash::from_bytes([3; 32]);
    let file_id = FileId::new(hash);
    let xorb_id = XorbId::new(hash);
    let range = ChunkRange::new(1, 3).unwrap();
    let reconstruction =
        FileReconstruction::new(vec![ReconstructionTerm::new(xorb_id, range, 64)]);
    store.insert_reconstruction(&file_id, &reconstruction).unwrap();
    store.insert_xorb(&xorb_id).unwrap();
    let dedupe_key = ObjectKey::parse("shards/aa/hash.shard").unwrap();
    let dedupe_mapping = DedupeShardMapping::new(hash, dedupe_key);
    store.upsert_dedupe_shard_mapping(&dedupe_mapping).unwrap();

    let key = ObjectKey::parse("xorbs/default/aa/hash.xorb").unwrap();
    let candidate = QuarantineCandidate::new(key.clone(), 64, 10, 20).unwrap();
    store.upsert_quarantine_candidate(&candidate).unwrap();
    let hold = RetentionHold::new(
        key.clone(),
        "provider deletion grace".to_owned(),
        30,
        Some(90),
    )
    .unwrap();
    store.upsert_retention_hold(&hold).unwrap();

    let loaded_reconstruction = store.reconstruction(&file_id);
    assert!(matches!(loaded_reconstruction, Ok(Some(_))));
    if let Ok(Some(loaded_reconstruction)) = loaded_reconstruction {
        assert_eq!(loaded_reconstruction, reconstruction);
    }
    assert!(matches!(store.contains_xorb(&xorb_id), Ok(true)));
    let loaded_dedupe_key = store.dedupe_shard_mapping(&hash);
    assert!(matches!(loaded_dedupe_key, Ok(Some(_))));
    if let Ok(Some(loaded_dedupe_key)) = loaded_dedupe_key {
        assert_eq!(loaded_dedupe_key, dedupe_mapping);
    }
    let listed_dedupe = store.list_dedupe_shard_mappings();
    assert!(listed_dedupe.is_ok());
    if let Ok(listed_dedupe) = listed_dedupe {
        assert_eq!(listed_dedupe, vec![dedupe_mapping.clone()]);
    }
    let mut visited_dedupe = Vec::new();
    let visited_dedupe_result = store.visit_dedupe_shard_mappings(|mapping| {
        visited_dedupe.push(mapping);
        Ok::<(), LocalIndexStoreError>(())
    });
    assert!(visited_dedupe_result.is_ok());
    assert_eq!(visited_dedupe, vec![dedupe_mapping]);
    let loaded_candidate = store.quarantine_candidate(&key);
    assert!(matches!(loaded_candidate, Ok(Some(_))));
    if let Ok(Some(loaded_candidate)) = loaded_candidate {
        assert_eq!(loaded_candidate, candidate);
    }
    let listed_candidates = store.list_quarantine_candidates();
    assert!(listed_candidates.is_ok());
    if let Ok(listed_candidates) = listed_candidates {
        assert_eq!(listed_candidates, vec![candidate.clone()]);
    }
    let mut visited_candidates = Vec::new();
    let visited_candidates_result = store.visit_quarantine_candidates(|entry| {
        visited_candidates.push(entry);
        Ok::<(), LocalIndexStoreError>(())
    });
    assert!(visited_candidates_result.is_ok());
    assert_eq!(visited_candidates, vec![candidate]);
    let loaded_hold = store.retention_hold(&key);
    assert!(matches!(loaded_hold, Ok(Some(_))));
    if let Ok(Some(loaded_hold)) = loaded_hold {
        assert_eq!(loaded_hold, hold);
    }
    let listed_holds = store.list_retention_holds();
    assert!(listed_holds.is_ok());
    if let Ok(listed_holds) = listed_holds {
        assert_eq!(listed_holds, vec![hold.clone()]);
    }
    let mut visited_holds = Vec::new();
    let visited_holds_result = store.visit_retention_holds(|entry| {
        visited_holds.push(entry);
        Ok::<(), LocalIndexStoreError>(())
    });
    assert!(visited_holds_result.is_ok());
    assert_eq!(visited_holds, vec![hold]);
    assert!(matches!(store.delete_quarantine_candidate(&key), Ok(true)));
    assert!(matches!(store.delete_quarantine_candidate(&key), Ok(false)));
    assert!(matches!(store.quarantine_candidate(&key), Ok(None)));
    assert!(matches!(store.delete_retention_hold(&key), Ok(true)));
    assert!(matches!(store.delete_retention_hold(&key), Ok(false)));
    assert!(matches!(store.retention_hold(&key), Ok(None)));
}

#[test]
fn local_index_store_rejects_invalid_stored_range() {
        let storage = shardline_test_support::TempStorage::new();
        let store = LocalIndexStore::new(storage.path_buf());
        assert!(store.is_ok());
        let Ok(store) = store else {
            return;
        };

    let hash = ShardlineHash::from_bytes([4; 32]);
    let path = store
        .root
        .join("reconstructions")
        .join(format!("{}.json", xet_hash_hex_string(&hash)));
    let Some(parent) = path.parent() else {
        return;
    };
    let created = fs::create_dir_all(parent);
    assert!(created.is_ok());
    let written = fs::write(
        path,
        r#"{"terms":[{"object_hash":"0404040404040404040404040404040404040404040404040404040404040404","chunk_start":2,"chunk_end_exclusive":2,"unpacked_length":5}]}"#,
    );
    assert!(written.is_ok());

    let file_id = FileId::new(hash);
    let loaded = store.reconstruction(&file_id);
    assert!(matches!(loaded, Err(LocalIndexStoreError::Range(_))));
}

#[cfg(unix)]
#[test]
fn local_index_store_rejects_symlinked_reconstruction_metadata() {
    let storage = shardline_test_support::TempStorage::new();
    let outside = tempfile::NamedTempFile::new();
    assert!(outside.is_ok());
    let Ok(outside) = outside else {
        return;
    };
    let written = fs::write(outside.path(), r#"{"terms":[]}"#);
    assert!(written.is_ok());
    let store = LocalIndexStore::new(storage.path());
    assert!(store.is_ok());
    let Ok(store) = store else {
        return;
    };
    let hash = ShardlineHash::from_bytes([11; 32]);
    let file_id = FileId::new(hash);
    let path = store.reconstruction_path(&file_id);
    let Some(parent) = path.parent() else {
        return;
    };
    let created = fs::create_dir_all(parent);
    assert!(created.is_ok());
    let linked = symlink(outside.path(), &path);
    assert!(linked.is_ok());

    let loaded = store.reconstruction(&file_id);

    assert!(
        loaded.is_err(),
        "local index read followed symlinked reconstruction metadata"
    );
}

#[cfg(unix)]
#[test]
fn local_index_store_rejects_symlinked_reconstruction_parent_directory() {
    let storage = shardline_test_support::TempStorage::new();
    let outside = shardline_test_support::TempStorage::new();
    let store = LocalIndexStore::open(storage.path());
    let link = storage.path().join("reconstructions");
    let linked = symlink(outside.path(), &link);
    assert!(linked.is_ok());
    let hash = ShardlineHash::from_bytes([12; 32]);
    let file_id = FileId::new(hash);
    let reconstruction = FileReconstruction::new(Vec::new());
    let inserted = store.insert_reconstruction(&file_id, &reconstruction);

    assert!(
        inserted.is_err(),
        "local index write followed a symlinked reconstruction parent directory"
    );
    let escaped = outside
        .path()
        .join(format!("{}.json", xet_hash_hex_string(&hash)));
    assert!(
        !escaped.exists(),
        "local index write escaped into a symlink target outside the index root"
    );
}

#[cfg(unix)]
#[test]
fn local_index_store_rejects_reconstruction_parent_swap_race() {
    let storage = shardline_test_support::TempStorage::new();
    let outside = shardline_test_support::TempStorage::new();
    let store = LocalIndexStore::open(storage.path());
    let hash = ShardlineHash::from_bytes([11; 32]);
    let file_id = FileId::new(hash);
    let path = store.reconstruction_path(&file_id);
    let parent = path.parent().map(PathBuf::from);
    assert!(parent.is_some());
    let Some(parent) = parent else {
        return;
    };
    let moved_parent = storage.path().join("swapped-reconstruction-parent");
    let moved_parent_for_hook = moved_parent.clone();
    let escape_dir = outside.path().to_path_buf();

    set_before_local_write_hook(path, move || {
        let renamed = fs::rename(&parent, &moved_parent_for_hook);
        assert!(renamed.is_ok());
        let linked = symlink(&escape_dir, &parent);
        assert!(linked.is_ok());
    });

    let reconstruction = FileReconstruction::new(Vec::new());
    let inserted = store.insert_reconstruction(&file_id, &reconstruction);

    assert!(matches!(
        inserted,
        Err(LocalIndexStoreError::Io(error)) if error.kind() == std::io::ErrorKind::InvalidData
    ));
    assert!(
        !outside
            .path()
            .join(format!("{}.json", xet_hash_hex_string(&hash)))
            .exists(),
        "local index write escaped into an attacker-controlled symlink target"
    );
    assert!(
        !moved_parent
            .join(format!("{}.json", xet_hash_hex_string(&hash)))
            .exists(),
        "local index write left a committed file behind in the detached original directory"
    );
}

#[cfg(unix)]
#[test]
fn local_index_store_new_rejects_symlinked_root_ancestor() {
    let storage = shardline_test_support::TempStorage::new();
    let target = storage.path().join("target");
    let created = fs::create_dir_all(&target);
    assert!(created.is_ok());
    let link = storage.path().join("link");
    let linked = symlink(&target, &link);
    assert!(linked.is_ok());

    let store = LocalIndexStore::new(link.join("gc"));

    assert!(matches!(
        store,
        Err(LocalIndexStoreError::Io(error))
            if error.kind() == std::io::ErrorKind::InvalidData
    ));
}

#[cfg(unix)]
#[test]
fn local_index_store_list_reconstruction_file_ids_rejects_symlinked_root_ancestor() {
    let storage = shardline_test_support::TempStorage::new();
    let target = storage.path().join("target");
    let directory = target.join("gc/reconstructions");
    let created = fs::create_dir_all(&directory);
    assert!(created.is_ok());
    let record_path =
        directory.join("0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef.json");
    let written = fs::write(&record_path, br#"{"terms":[]}"#);
    assert!(written.is_ok());
    let link = storage.path().join("link");
    let linked = symlink(&target, &link);
    assert!(linked.is_ok());
    let store = LocalIndexStore::open(link.join("gc"));

    let listed = store.list_reconstruction_file_ids();

    assert!(matches!(
        listed,
        Err(LocalIndexStoreError::Io(error))
            if error.kind() == std::io::ErrorKind::InvalidData
    ));
}

#[test]
fn local_index_store_open_is_non_mutating_until_write() {
    let storage = shardline_test_support::TempStorage::new();
    let root = storage.path().join("index");

    let store = LocalIndexStore::open(root.clone());
    assert!(!root.exists());

    let hash = ShardlineHash::from_bytes([7; 32]);
    let xorb_id = XorbId::new(hash);
    let inserted = store.insert_xorb(&xorb_id);

    assert!(inserted.is_ok());
    assert!(root.join("xorbs").is_dir());
}

#[test]
fn local_index_store_rejects_invalid_stored_dedupe_shard_object_key() {
        let storage = shardline_test_support::TempStorage::new();
        let store = LocalIndexStore::new(storage.path_buf());
        assert!(store.is_ok());
        let Ok(store) = store else {
            return;
        };

    let hash = ShardlineHash::from_bytes([8; 32]);
    let hash_hex = xet_hash_hex_string(&hash);
    let path = storage
        .path()
        .join("dedupe-shards")
        .join(&hash_hex[..2])
        .join(format!("{hash_hex}.json"));
    let Some(parent) = path.parent() else {
        return;
    };
    let created = fs::create_dir_all(parent);
    assert!(created.is_ok());
    let written = fs::write(
        path,
        format!("{{\"chunk_hash\":\"{hash_hex}\",\"shard_object_key\":\"../invalid\"}}"),
    );
    assert!(written.is_ok());

    let loaded = store.dedupe_shard_mapping(&hash);
    assert!(matches!(loaded, Err(LocalIndexStoreError::ObjectKey(_))));
}

#[test]
fn local_index_store_reads_legacy_quarantine_record_shape() {
        let storage = shardline_test_support::TempStorage::new();
        let store = LocalIndexStore::new(storage.path_buf());
        assert!(store.is_ok());
        let Ok(store) = store else {
            return;
        };
    let hash = "de".repeat(32);
    let path = storage
        .path()
        .join("quarantine")
        .join("de")
        .join(format!("{hash}.json"));
    let parent = path.parent();
    assert!(parent.is_some());
    let Some(parent) = parent else {
        return;
    };
    let created = fs::create_dir_all(parent);
    assert!(created.is_ok());
    let written = fs::write(
        &path,
        serde_json::to_vec(&LegacyQuarantineCandidateRecordForTest {
            hash: hash.clone(),
            bytes: 64,
            first_seen_unreachable_at_unix_seconds: 10,
            delete_after_unix_seconds: 20,
        })
        .unwrap_or_default(),
    );
    assert!(written.is_ok());

    let listed = store.list_quarantine_candidates();

    assert!(matches!(listed, Ok(ref candidates) if candidates.len() == 1));
    if let Ok(candidates) = listed {
        let expected_key = ObjectKey::parse(&format!("de/{hash}"));
        assert!(expected_key.is_ok());
        let Ok(expected_key) = expected_key else {
            return;
        };
        let expected = QuarantineCandidate::new(expected_key, 64, 10, 20);
        assert!(expected.is_ok());
        let Ok(expected) = expected else {
            return;
        };
        assert_eq!(candidates, vec![expected]);
    }
}

#[test]
fn local_index_store_rejects_inverted_quarantine_timeline() {
        let storage = shardline_test_support::TempStorage::new();
        let store = LocalIndexStore::new(storage.path_buf());
        assert!(store.is_ok());
        let Ok(store) = store else {
            return;
        };
    let hash = "ab".repeat(32);
    let path = storage
        .path()
        .join("quarantine")
        .join("ab")
        .join(format!("{hash}.json"));
    let parent = path.parent();
    assert!(parent.is_some());
    let Some(parent) = parent else {
        return;
    };
    let created = fs::create_dir_all(parent);
    assert!(created.is_ok());
    let record = format!(
        "{{\"object_key\":\"ab/{hash}\",\"observed_length\":64,\
         \"first_seen_unreachable_at_unix_seconds\":20,\
         \"delete_after_unix_seconds\":10}}"
    );
    let written = fs::write(&path, record);
    assert!(written.is_ok());

    let listed = store.list_quarantine_candidates();

    assert!(matches!(
        listed,
        Err(LocalIndexStoreError::QuarantineCandidate(
            QuarantineCandidateError::InvertedTimeline
        ))
    ));
}

#[test]
fn local_index_store_rejects_oversized_webhook_delivery_metadata_before_reading() {
        let storage = shardline_test_support::TempStorage::new();
        let store = LocalIndexStore::new(storage.path_buf());
        assert!(store.is_ok());
        let Ok(store) = store else {
            return;
        };
    let delivery = WebhookDelivery::new(
        RepositoryProvider::GitHub,
        "team".to_owned(),
        "assets".to_owned(),
        "delivery-oversized".to_owned(),
        100,
    );
    assert!(delivery.is_ok());
    let Ok(delivery) = delivery else {
        return;
    };
    let recorded = store.record_webhook_delivery(&delivery);
    assert!(matches!(recorded, Ok(true)));
    let path = storage
        .path()
        .join("webhook-deliveries")
        .join("github")
        .join(hex_encode_component(delivery.owner()))
        .join(hex_encode_component(delivery.repo()))
        .join(format!(
            "{}.json",
            hex_encode_component(delivery.delivery_id())
        ));
    let written = fs::write(
        &path,
        vec![b'{'; MAX_CONTROL_PLANE_METADATA_BYTES as usize + 1],
    );
    assert!(written.is_ok());

    let listed = store.list_webhook_deliveries();

    assert!(matches!(
        listed,
        Err(LocalIndexStoreError::MetadataTooLarge {
            maximum_bytes: MAX_CONTROL_PLANE_METADATA_BYTES,
            ..
        })
    ));
}

#[test]
fn local_index_store_rejects_oversized_reconstruction_metadata_before_reading() {
        let storage = shardline_test_support::TempStorage::new();
        let store = LocalIndexStore::new(storage.path_buf());
        assert!(store.is_ok());
        let Ok(store) = store else {
            return;
        };

    let hash = ShardlineHash::from_bytes([9; 32]);
    let path = store
        .root
        .join("reconstructions")
        .join(format!("{}.json", xet_hash_hex_string(&hash)));
    let file = fs::File::create(&path);
    assert!(file.is_ok());
    let Ok(file) = file else {
        return;
    };
    let resized = file.set_len(MAX_RECONSTRUCTION_METADATA_BYTES + 1);
    assert!(resized.is_ok());

    let loaded = store.reconstruction(&FileId::new(hash));

    assert!(matches!(
        loaded,
        Err(LocalIndexStoreError::MetadataTooLarge {
            maximum_bytes: MAX_RECONSTRUCTION_METADATA_BYTES,
            ..
        })
    ));
}

#[test]
fn local_index_store_rejects_metadata_growth_after_length_validation() {
        let storage = shardline_test_support::TempStorage::new();
        let store = LocalIndexStore::new(storage.path_buf());
        assert!(store.is_ok());
        let Ok(store) = store else {
            return;
        };

    let hash = ShardlineHash::from_bytes([10; 32]);
    let path = store
        .root
        .join("reconstructions")
        .join(format!("{}.json", xet_hash_hex_string(&hash)));
    let written = fs::write(&path, br#"{"terms":[]}"#);
    assert!(written.is_ok());

    let append_path = path.clone();
    set_before_local_metadata_read_hook(path.clone(), move || {
        let opened = OpenOptions::new().append(true).open(&append_path);
        assert!(opened.is_ok());
        let Ok(mut file) = opened else {
            return;
        };
        let appended = file.write_all(b"\n");
        assert!(appended.is_ok());
        let synced = file.sync_all();
        assert!(synced.is_ok());
    });

    let loaded = read_file_if_exists_bounded(&path, MAX_RECONSTRUCTION_METADATA_BYTES);

    assert!(matches!(
        loaded,
        Err(LocalIndexStoreError::MetadataLengthMismatch { .. })
    ));
}

#[derive(Debug, Clone, Serialize)]
struct LegacyQuarantineCandidateRecordForTest {
    hash: String,
    bytes: u64,
    first_seen_unreachable_at_unix_seconds: u64,
    delete_after_unix_seconds: u64,
}
