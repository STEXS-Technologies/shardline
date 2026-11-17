#![allow(clippy::unwrap_used)]

use super::{LocalIndexStoreError, LocalRecordStore};
use crate::{FileChunkRecord, FileRecord, RecordTraversal, StorageRepresentation};
use shardline_protocol::{RepositoryProvider, RepositoryScope};

fn record(scope: &RepositoryScope, hash: &str) -> FileRecord {
    FileRecord {
        deduplication_domain: None,
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

#[tokio::test]
async fn explicit_domain_migration_backfills_without_relocating_or_reassigning_legacy_records() {
    let root = tempfile::tempdir().unwrap();
    let store = LocalRecordStore::new(root.path().to_owned()).unwrap();
    let scope = RepositoryScope::new(RepositoryProvider::Generic, "team", "repo", None).unwrap();
    let domain = crate::DeduplicationDomain::from_scope(Some(&scope));
    let mut protocol = record(&scope, &"d".repeat(64));
    protocol.repository_scope = None;
    protocol.deduplication_domain = Some(domain.clone());
    store.commit_file_version_metadata(&protocol).await.unwrap();
    let mut connection = store.open_connection().unwrap();
    let domains = |database: &rusqlite::Connection| {
        let mut statement = database
            .prepare(
                "SELECT DISTINCT scope_key FROM shardline_record_membership ORDER BY scope_key",
            )
            .unwrap();
        statement
            .query_map([], |row| row.get::<_, String>(0))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap()
    };
    assert_eq!(domains(&connection), vec![domain.as_str()]);
    connection
        .execute_batch(include_str!(
            "../../migrations/20261115000000_deduplication_domains.down.sql"
        ))
        .unwrap();
    let global = crate::DeduplicationDomain::from_scope(None);
    assert_eq!(domains(&connection), vec![global.as_str()]);
    let mut legacy = protocol.clone();
    legacy.file_id = "legacy-unattributed".into();
    legacy.deduplication_domain = None;
    store.commit_file_version_metadata(&legacy).await.unwrap();
    connection
        .execute_batch(include_str!(
            "../../migrations/20261115000000_deduplication_domains.up.sql"
        ))
        .unwrap();
    let mut expected = vec![domain.as_str().to_owned(), global.as_str().to_owned()];
    expected.sort();
    assert_eq!(domains(&connection), expected);
    let transaction = connection.transaction().unwrap();
    transaction.execute("UPDATE shardline_file_records SET record=json_set(record,'$.deduplication_domain','uncommitted') WHERE file_id=?1", [&protocol.file_id]).unwrap();
    assert!(
        domains(&transaction)
            .iter()
            .any(|value| value == "uncommitted")
    );
    transaction.rollback().unwrap();
    assert_eq!(domains(&connection), expected);
    assert_eq!(
        store
            .read_record_bytes(&store.latest_record_locator(&protocol))
            .await
            .unwrap(),
        serde_json::to_vec(&protocol).unwrap()
    );
}

#[test]
fn domain_once_migration_preserves_membership_and_transaction_boundaries() {
    let mut connection = rusqlite::Connection::open_in_memory().unwrap();
    connection.execute_batch("CREATE TABLE shardline_file_records (record_key TEXT PRIMARY KEY, scope_key TEXT, record TEXT);
        CREATE TABLE shardline_record_membership (record_key TEXT, scope_key TEXT, member_hash TEXT);").unwrap();
    let previous =
        include_str!("../../migrations/20261117000000_record_membership_domain_once.down.sql");
    let optimized =
        include_str!("../../migrations/20261117000000_record_membership_domain_once.up.sql");
    let membership = |db: &rusqlite::Connection| -> Vec<(String, String, String)> {
        db.prepare("SELECT record_key, scope_key, member_hash FROM shardline_record_membership ORDER BY 1, 2, 3")
            .unwrap().query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))
            .unwrap().collect::<Result<_, _>>().unwrap()
    };
    // Both historical SQL writers and current writers must retain the same
    // domain fallback and malformed-record behavior after upgrade or downgrade.
    for payload in [
        "invalid",
        "null",
        "[]",
        "{}",
        r#"{"chunks":[{"hash":"a"},{"hash":"a"},{"hash":4},null,[],true]}"#,
        r#"{"deduplication_domain":null,"chunks":[{"hash":"a"}]}"#,
        r#"{"deduplication_domain":"tenant/repo/main","chunks":[{"hash":"a"},{"hash":"b"}]}"#,
    ] {
        let mut expected = None;
        for triggers in [previous, optimized, previous, optimized] {
            connection
                .execute_batch(
                    "DELETE FROM shardline_file_records; DELETE FROM shardline_record_membership;",
                )
                .unwrap();
            connection.execute_batch(triggers).unwrap();
            connection
                .execute(
                    "INSERT INTO shardline_file_records VALUES ('key','legacy',?1)",
                    [payload],
                )
                .unwrap();
            connection
                .execute(
                    "UPDATE shardline_file_records SET scope_key='changed', record_key='renamed'",
                    [],
                )
                .unwrap();
            let rows = membership(&connection);
            if let Some(expected) = &expected {
                assert_eq!(&rows, expected, "payload: {payload}");
            } else {
                expected = Some(rows.clone());
            }
            let changes = connection.total_changes();
            connection
                .execute("UPDATE shardline_file_records SET record=record", [])
                .unwrap();
            assert_eq!(
                connection.total_changes() - changes,
                1,
                "replay must not rewrite membership"
            );
            let transaction = connection.transaction().unwrap();
            transaction
                .execute(
                    "UPDATE shardline_file_records SET record=?1",
                    [r#"{"deduplication_domain":"uncommitted","chunks":[{"hash":"x"}]}"#],
                )
                .unwrap();
            assert_eq!(
                membership(&transaction),
                vec![("renamed".into(), "uncommitted".into(), "x".into())]
            );
            transaction.rollback().unwrap();
            assert_eq!(membership(&connection), rows);
            let retained: String = connection
                .query_row("SELECT record FROM shardline_file_records", [], |row| {
                    row.get(0)
                })
                .unwrap();
            assert_eq!(
                retained, payload,
                "authoritative payload must remain unchanged"
            );
        }
    }
}
