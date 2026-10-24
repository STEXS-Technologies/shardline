#![allow(clippy::unwrap_used)]

use super::{LocalIndexStoreError, LocalRecordStore};
use crate::{FileChunkRecord, FileRecord, RecordTraversal, StorageRepresentation};
use shardline_protocol::{RepositoryProvider, RepositoryScope};

fn record(scope: &RepositoryScope, hash: &str) -> FileRecord {
    FileRecord {
        file_id: "file".into(),
        content_hash: "c".repeat(64),
        total_bytes: 4,
        chunk_size: 0,
        storage_repr: StorageRepresentation::WholeFileV1,
        repository_scope: Some(scope.clone()),
        chunks: vec![FileChunkRecord {
            hash: hash.into(),
            offset: 0,
            length: 4,
            range_start: 0,
            range_end: 1,
            packed_start: 0,
            packed_end: 12,
        }],
    }
}

async fn candidates(store: &LocalRecordStore, scope: &RepositoryScope, hash: &str) -> usize {
    let mut count = 0_usize;
    store
        .visit_repository_hash_records(scope, hash, |stored| {
            let parsed: FileRecord = serde_json::from_slice(&stored.bytes)?;
            assert_eq!(parsed.repository_scope.as_ref(), Some(scope));
            assert!(parsed.chunks.iter().any(|member| member.hash == hash));
            count = count.saturating_add(1);
            Ok::<_, LocalIndexStoreError>(())
        })
        .await
        .unwrap();
    count
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn indexed_membership_backfills_and_tracks_legacy_sql_writes_atomically() {
    let root = tempfile::tempdir().unwrap();
    let store = LocalRecordStore::new(root.path().to_owned()).unwrap();
    let scope =
        RepositoryScope::new(RepositoryProvider::GitHub, "team", "repo", Some("main")).unwrap();
    let other =
        RepositoryScope::new(RepositoryProvider::GitHub, "team", "repo", Some("other")).unwrap();
    let mut original = record(&scope, &"a".repeat(64));
    store.commit_file_version_metadata(&original).await.unwrap();
    store
        .commit_file_version_metadata(&record(&other, &"a".repeat(64)))
        .await
        .unwrap();
    let mut connection = store.open_connection().unwrap();
    connection
        .execute_batch(include_str!(
            "../../migrations/20261023010000_record_membership.down.sql"
        ))
        .unwrap();
    // These rows predate the additive index. Reinstallation must find both
    // aliases without rewriting their authoritative payloads.
    connection
        .execute_batch(include_str!(
            "../../migrations/20261023010000_record_membership.up.sql"
        ))
        .unwrap();
    assert_eq!(
        candidates(&store, &scope, &original.chunks.first().unwrap().hash).await,
        2
    );

    let key = store
        .latest_record_locator(&original)
        .record_key()
        .to_owned();
    let previous_changes = connection.total_changes();
    connection.execute("UPDATE shardline_file_records SET record=record, updated_at_unix_seconds=updated_at_unix_seconds+1 WHERE record_key=?1", [&key]).unwrap();
    assert_eq!(
        connection.total_changes() - previous_changes,
        1,
        "exact replay must not rewrite membership rows"
    );

    original.chunks.first_mut().unwrap().hash = "b".repeat(64);
    let json = serde_json::to_string(&original).unwrap();
    {
        let transaction = connection.transaction().unwrap();
        transaction
            .execute(
                "UPDATE shardline_file_records SET record=?1 WHERE record_key=?2",
                rusqlite::params![json, key],
            )
            .unwrap();
        transaction.rollback().unwrap();
    }
    assert_eq!(candidates(&store, &scope, &"b".repeat(64)).await, 0);
    // An old binary knows only the existing file-record table. Its writes must
    // still maintain the new derived index through the database trigger.
    connection
        .execute(
            "UPDATE shardline_file_records SET record=?1 WHERE record_key=?2",
            rusqlite::params![json, key],
        )
        .unwrap();
    assert_eq!(candidates(&store, &scope, &"a".repeat(64)).await, 1);
    assert_eq!(candidates(&store, &scope, &"b".repeat(64)).await, 1);
    assert_eq!(candidates(&store, &other, &"a".repeat(64)).await, 2);
    connection
        .execute(
            "DELETE FROM shardline_file_records WHERE record_key=?1",
            [&key],
        )
        .unwrap();
    assert_eq!(candidates(&store, &scope, &"b".repeat(64)).await, 0);
    assert_eq!(
        candidates(&store, &scope, &"a".repeat(64)).await,
        1,
        "historical version remains reachable"
    );

    let plan: Vec<String> = connection
        .prepare(
            "EXPLAIN QUERY PLAN SELECT r.record_key, r.record FROM shardline_record_membership m
         JOIN shardline_file_records r ON r.record_key=m.record_key
         WHERE m.scope_key=?1 AND m.member_hash=?2 AND r.scope_key=?1
           AND m.record_key>?3 ORDER BY m.record_key LIMIT 128",
        )
        .unwrap()
        .query_map(["scope", "hash", ""], |row| row.get(3))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert!(
        plan.iter()
            .any(|step| step.contains("COVERING INDEX shardline_record_membership_lookup_idx"))
    );
}
