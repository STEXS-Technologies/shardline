//! A complete range proof need not inspect unrelated candidate records.

use super::repository_covers_xorb_range;
use shardline_index::{LocalRecordStore, RecordTraversal};
use shardline_protocol::{ByteRange, RepositoryProvider, RepositoryScope};

#[tokio::test]
async fn complete_range_proof_stops_before_unrelated_corrupt_candidate() {
    let root = tempfile::tempdir().unwrap();
    let store = LocalRecordStore::new(root.path().to_owned()).unwrap();
    let scope = RepositoryScope::new(RepositoryProvider::Generic, "team", "range", None).unwrap();
    let hash = "a".repeat(64);
    let mut record = super::test_support::record("aa", &hash, &scope);
    store.commit_file_version_metadata(&record).await.unwrap();
    record.file_id = "bb".into();
    store.commit_file_version_metadata(&record).await.unwrap();
    let connection = rusqlite::Connection::open(root.path().join("metadata.sqlite3")).unwrap();
    for locator in [
        store.latest_record_locator(&record),
        store.version_record_locator(&record),
    ] {
        let key = locator.record_key();
        connection
            .execute(
                "UPDATE shardline_file_records SET record='{}' WHERE record_key=?1",
                [key],
            )
            .unwrap();
        // A stale derived hint must never grant access. Place it after the
        // intact authoritative record to detect unnecessary traversal.
        connection
            .execute(
                "INSERT INTO shardline_record_membership(record_key,scope_key,member_hash)
             SELECT record_key,scope_key,?2 FROM shardline_file_records WHERE record_key=?1",
                [key, hash.as_str()],
            )
            .unwrap();
    }
    assert!(
        repository_covers_xorb_range(&store, &hash, &scope, ByteRange::new(0, 11).unwrap())
            .await
            .unwrap()
    );
    // Incomplete coverage must keep searching and propagate invalid metadata;
    // it cannot infer permission for a gap or the container footer.
    assert!(
        repository_covers_xorb_range(&store, &hash, &scope, ByteRange::new(0, 12).unwrap())
            .await
            .is_err()
    );
}
