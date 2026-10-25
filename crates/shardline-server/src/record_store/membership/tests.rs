use super::any_repository_hash_record;
use crate::ServerError;
use shardline_index::{FileChunkRecord, FileRecord, LocalRecordStore, StorageRepresentation};
use shardline_protocol::{RepositoryProvider, RepositoryScope};

fn scope() -> RepositoryScope {
    RepositoryScope::new(RepositoryProvider::Generic, "team", "repo", None).unwrap()
}

fn verifies(bytes: &[u8], scope: &RepositoryScope, hash: &str) -> Result<bool, ServerError> {
    let record = crate::record_store::parse_stored_file_record_bytes(bytes)?;
    Ok(record.repository_scope.as_ref() == Some(scope)
        && record.chunks.iter().any(|chunk| chunk.hash == hash))
}

async fn seed(store: &LocalRecordStore, count: usize) {
    for index in 0..count {
        store
            .commit_file_version_metadata(&FileRecord {
                file_id: "shared-file".into(),
                content_hash: format!("{index:064x}"),
                total_bytes: 4,
                chunk_size: 0,
                storage_repr: StorageRepresentation::WholeFileV1,
                repository_scope: Some(scope()),
                chunks: vec![FileChunkRecord {
                    hash: "a".repeat(64),
                    offset: 0,
                    length: 4,
                    range_start: 0,
                    range_end: 1,
                    packed_start: 0,
                    packed_end: 12,
                }],
            })
            .await
            .unwrap();
    }
}

#[tokio::test]
async fn postgres_verified_match_short_circuits_and_releases_its_connection() {
    let Some(fixture) = crate::test_support::postgres_fixture::PostgresFixture::new().await else {
        assert!(
            std::env::var_os("SHARDLINE_REQUIRE_POSTGRES_TESTS").is_none(),
            "required PostgreSQL membership qualification needs DATABASE_URL"
        );
        return;
    };
    fixture.migrate().await;
    let store = shardline_index::PostgresRecordStore::new(fixture.index().pool().clone());
    let root = tempfile::tempdir().unwrap();
    let local = LocalRecordStore::new(root.path().to_owned()).unwrap();
    seed(&local, 16).await;
    let connection = rusqlite::Connection::open(root.path().join("metadata.sqlite3")).unwrap();
    let mut statement = connection
        .prepare("SELECT record FROM shardline_file_records WHERE record_kind='version'")
        .unwrap();
    let records: Vec<FileRecord> = statement
        .query_map([], |row| row.get::<_, String>(0))
        .unwrap()
        .map(|row| serde_json::from_str(&row.unwrap()).unwrap())
        .collect();
    for record in records {
        store.commit_file_version_metadata(&record).await.unwrap();
    }
    let scope = scope();
    let hash = "a".repeat(64);
    // Repetition exercises stream cleanup and pool reuse after early termination.
    for _ in 0..32 {
        let mut visits = 0;
        assert!(
            any_repository_hash_record(&store, &scope, &hash, |bytes| {
                visits += 1;
                verifies(bytes, &scope, &hash)
            })
            .await
            .unwrap()
        );
        assert_eq!(visits, 1);
    }
    let mut visits = 0;
    assert!(
        !any_repository_hash_record(&store, &scope, &hash, |_bytes| {
            visits += 1;
            Ok(false)
        })
        .await
        .unwrap()
    );
    assert_eq!(visits, 17);
    assert!(matches!(
        any_repository_hash_record(&store, &scope, &hash, |_bytes| {
            Err(ServerError::NotFound)
        })
        .await,
        Err(ServerError::NotFound)
    ));
    fixture.finish().await;
}

#[tokio::test]
async fn shared_version_lookup_stops_after_first_verified_match() {
    let root = tempfile::tempdir().unwrap();
    let store = LocalRecordStore::new(root.path().to_owned()).unwrap();
    seed(&store, 128).await;
    let scope = scope();
    let hash = "a".repeat(64);
    let mut visits = 0;
    assert!(
        any_repository_hash_record(&store, &scope, &hash, |bytes| {
            visits += 1;
            verifies(bytes, &scope, &hash)
        })
        .await
        .unwrap()
    );
    assert_eq!(visits, 1);

    visits = 0;
    assert!(
        !any_repository_hash_record(&store, &scope, &hash, |_bytes| {
            visits += 1;
            Ok(false)
        })
        .await
        .unwrap()
    );
    assert_eq!(
        visits, 129,
        "no match must exhaust latest and historical candidates"
    );

    visits = 0;
    assert!(matches!(
        any_repository_hash_record(&store, &scope, &hash, |_bytes| {
            visits += 1;
            Err(ServerError::NotFound)
        })
        .await,
        Err(ServerError::NotFound)
    ));
    assert_eq!(visits, 1, "record verification errors must propagate");
}

#[tokio::test]
async fn derived_candidate_scope_never_grants_access() {
    let root = tempfile::tempdir().unwrap();
    let store = LocalRecordStore::new(root.path().to_owned()).unwrap();
    seed(&store, 1).await;
    let connection = rusqlite::Connection::open(root.path().join("metadata.sqlite3")).unwrap();
    let original_scope: String = connection
        .query_row(
            "SELECT scope_key FROM shardline_file_records LIMIT 1",
            [],
            |row| row.get(0),
        )
        .unwrap();
    let wrong = RepositoryScope::new(RepositoryProvider::Generic, "other", "repo", None).unwrap();
    // Move only the derived candidate scope, leaving the authoritative JSON unchanged.
    // Compute its canonical key through a real record publication in another store.
    let other_root = tempfile::tempdir().unwrap();
    let other_store = LocalRecordStore::new(other_root.path().to_owned()).unwrap();
    let encoded: String = connection
        .query_row(
            "SELECT record FROM shardline_file_records LIMIT 1",
            [],
            |row| row.get(0),
        )
        .unwrap();
    let mut record: FileRecord = serde_json::from_str(&encoded).unwrap();
    record.repository_scope = Some(wrong.clone());
    other_store
        .commit_file_version_metadata(&record)
        .await
        .unwrap();
    let other_connection =
        rusqlite::Connection::open(other_root.path().join("metadata.sqlite3")).unwrap();
    let wrong_key: String = other_connection
        .query_row(
            "SELECT scope_key FROM shardline_file_records LIMIT 1",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_ne!(original_scope, wrong_key);
    connection
        .execute(
            "UPDATE shardline_file_records SET scope_key=?1",
            [&wrong_key],
        )
        .unwrap();
    let hash = "a".repeat(64);
    let mut visits = 0;
    assert!(
        !any_repository_hash_record(&store, &wrong, &hash, |bytes| {
            visits += 1;
            verifies(bytes, &wrong, &hash)
        })
        .await
        .unwrap()
    );
    assert_eq!(
        visits, 2,
        "both forged hints must be checked against real metadata"
    );
}
