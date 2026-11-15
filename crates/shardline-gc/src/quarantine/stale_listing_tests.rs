use super::*;
use shardline_index::{MemoryIndexStore, MemoryRecordStore};

#[tokio::test]
async fn sweep_settles_a_listed_object_deleted_before_sweep() {
    let directory = tempfile::tempdir().unwrap();
    let objects = ServerObjectStore::local(directory.path().join("objects")).unwrap();
    let records = MemoryRecordStore::new();
    let index = MemoryIndexStore::new();
    let bytes = b"orphan removed after listing";
    let hash = "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee".to_owned();
    let key = ObjectKey::parse(&format!("{}/{}", &hash[..2], hash)).unwrap();
    objects
        .put_if_absent(
            &key,
            ObjectBody::Borrowed(bytes),
            &ObjectIntegrity::new(chunk_hash(bytes), bytes.len() as u64),
        )
        .unwrap();
    let orphan = OrphanObject {
        hash,
        object_key: key.clone(),
        bytes: bytes.len() as u64,
    };
    let candidate = QuarantineCandidate::new(key.clone(), orphan.bytes, 1, 2).unwrap();
    index.upsert_quarantine_candidate(&candidate).await.unwrap();
    let listing = HashMap::from([(key.as_str().to_owned(), orphan)]);
    let mut candidates = HashMap::from([(key.as_str().to_owned(), candidate)]);
    objects.delete_if_present(&key).unwrap();
    let mut report = LocalGcReport::default();
    sweep_quarantine_entries(
        &records,
        &objects,
        &index,
        &[ServerFrontend::Xet],
        &listing,
        3,
        &mut candidates,
        &mut report,
    )
    .await
    .unwrap();
    assert!(candidates.is_empty());
    assert!(index.quarantine_candidate(&key).await.unwrap().is_none());
    assert!(objects.metadata(&key).unwrap().is_none());
    // A second sweep of the stale listing cannot resurrect metadata or storage.
    sweep_quarantine_entries(
        &records,
        &objects,
        &index,
        &[ServerFrontend::Xet],
        &listing,
        4,
        &mut candidates,
        &mut report,
    )
    .await
    .unwrap();
    assert!(index.quarantine_candidate(&key).await.unwrap().is_none());
    assert!(objects.metadata(&key).unwrap().is_none());
}

#[tokio::test]
async fn quarantine_release_preserves_replacements_and_requires_a_matching_row() {
    let index = MemoryIndexStore::new();
    let key =
        ObjectKey::parse("aa/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
            .unwrap();
    let original = QuarantineCandidate::new(key.clone(), 8, 1, 2).unwrap();
    let replacement = QuarantineCandidate::new(key.clone(), 16, 3, 4).unwrap();
    index
        .upsert_quarantine_candidate(&replacement)
        .await
        .unwrap();
    let mut entries = HashMap::from([(key.as_str().to_owned(), original)]);
    assert!(
        !release_quarantine_candidate(&index, &mut entries, key.as_str())
            .await
            .unwrap()
    );
    assert_eq!(entries.get(key.as_str()), Some(&replacement));
    assert_eq!(
        index.quarantine_candidate(&key).await.unwrap(),
        Some(replacement)
    );
    assert!(
        release_quarantine_candidate(&index, &mut entries, key.as_str())
            .await
            .unwrap()
    );
    assert!(entries.is_empty());
    assert!(index.quarantine_candidate(&key).await.unwrap().is_none());
    assert!(
        !release_quarantine_candidate(&index, &mut entries, key.as_str())
            .await
            .unwrap()
    );
}
