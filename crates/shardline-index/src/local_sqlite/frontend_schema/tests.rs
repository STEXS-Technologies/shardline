#![allow(clippy::unwrap_used, clippy::indexing_slicing)]
use super::*;

fn fixture() -> Connection {
    let connection = Connection::open_in_memory().unwrap();
    super::super::helpers::ensure_local_schema_migrations_table(&connection).unwrap();
    connection
        .execute_batch(
            "CREATE TABLE shardline_file_records (
                record_key TEXT PRIMARY KEY, scope_key TEXT NOT NULL, record TEXT NOT NULL,
                file_id TEXT NOT NULL, record_kind TEXT NOT NULL
            );
            CREATE TABLE shardline_hub_file_entries (
                commit_sha TEXT NOT NULL, sha TEXT NOT NULL, is_lfs INTEGER NOT NULL,
                size INTEGER NOT NULL
            );
            CREATE TABLE shardline_s3_objects (
                scope_namespace TEXT NOT NULL,
                object_key TEXT NOT NULL,
                file_id TEXT NOT NULL,
                size_bytes INTEGER NOT NULL,
                content_hash TEXT NOT NULL,
                updated_at_unix_seconds INTEGER NOT NULL,
                etag TEXT NOT NULL,
                user_metadata TEXT NOT NULL,
                PRIMARY KEY (scope_namespace, object_key)
            );",
        )
        .unwrap();
    connection
}

#[test]
fn fresh_install_is_idempotent_and_keeps_core_history_unchanged() {
    let mut connection = fixture();
    assert!(check(&connection).is_err());
    install(&mut connection).unwrap();
    install(&mut connection).unwrap();
    check(&connection).unwrap();
    let count: i64 = connection
        .query_row(
            &format!("SELECT count(*) FROM {LOCAL_SCHEMA_MIGRATIONS_TABLE}"),
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(count, 0);
}

#[test]
fn adoption_preserves_prerelease_history_and_durable_rows() {
    let mut connection = fixture();
    for migration in FRONTEND_MIGRATIONS {
        connection.execute_batch(migration.up_sql).unwrap();
        connection
            .execute(
                &format!("INSERT INTO {LOCAL_SCHEMA_MIGRATIONS_TABLE} VALUES (?1,?2,17)"),
                params![migration.version, migration.name],
            )
            .unwrap();
    }
    connection.execute_batch("INSERT INTO shardline_lfs_locks VALUES ('scope','id','path','owner','name','time'); INSERT INTO shardline_xet_file_identities VALUES ('scope','sha','file',42);").unwrap();
    install(&mut connection).unwrap();
    let timestamps: i64 = connection
        .query_row(
            &format!("SELECT count(*) FROM {HISTORY} WHERE applied_at_unix_seconds=17"),
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(
        timestamps,
        i64::try_from(FRONTEND_MIGRATIONS.len()).unwrap()
    );
    let lock: String = connection
        .query_row("SELECT id FROM shardline_lfs_locks", [], |r| r.get(0))
        .unwrap();
    assert_eq!(lock, "id");
    let identity: String = connection
        .query_row(
            "SELECT file_id FROM shardline_xet_file_identities",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(identity, "file");
    let count: i64 = connection
        .query_row(
            &format!("SELECT count(*) FROM {LOCAL_SCHEMA_MIGRATIONS_TABLE}"),
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(count, 0);
}

#[test]
fn s3_multipart_migration_preserves_legacy_object_rows_without_guessing_parts() {
    let mut connection = fixture();
    connection
        .execute(
            "INSERT INTO shardline_s3_objects
             (scope_namespace,object_key,file_id,size_bytes,content_hash,
              updated_at_unix_seconds,etag,user_metadata)
             VALUES ('scope','legacy.bin','file-id',12,'content-hash',42,'etag','[]')",
            [],
        )
        .unwrap();

    install(&mut connection).unwrap();

    let legacy: (String, i64, String) = connection
        .query_row(
            "SELECT file_id,size_bytes,etag FROM shardline_s3_objects
             WHERE scope_namespace='scope' AND object_key='legacy.bin'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!(legacy, ("file-id".to_owned(), 12, "etag".to_owned()));
    let sidecars: i64 = connection
        .query_row(
            "SELECT count(*) FROM shardline_s3_multipart_counts",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(sidecars, 0);
}

#[test]
fn conflicting_history_rolls_back_the_entire_installation() {
    let mut connection = fixture();
    connection
        .execute(
            &format!("INSERT INTO {LOCAL_SCHEMA_MIGRATIONS_TABLE} VALUES (?1,'different',17)"),
            [FRONTEND_MIGRATIONS[1].version],
        )
        .unwrap();
    assert!(install(&mut connection).is_err());
    let count: i64 = connection.query_row("SELECT count(*) FROM sqlite_master WHERE name IN ('shardline_frontend_schema_v1','shardline_xet_file_identities')", [], |r| r.get(0)).unwrap();
    assert_eq!(count, 0);
}

#[test]
fn physical_schema_corruption_and_definition_tampering_fail_closed() {
    let mut connection = fixture();
    install(&mut connection).unwrap();
    connection
        .execute_batch("DROP INDEX shardline_xet_file_identities_file_id_idx")
        .unwrap();
    assert!(check(&connection).is_err());
    assert!(install(&mut connection).is_err());
    connection
        .execute_batch(FRONTEND_MIGRATIONS[0].up_sql)
        .unwrap();
    check(&connection).unwrap();
    connection
        .execute(&format!("UPDATE {HISTORY} SET definition='different'"), [])
        .unwrap();
    assert!(check(&connection).is_err());
    assert!(install(&mut connection).is_err());
}

#[test]
fn incompatible_prerelease_table_preserves_all_original_history() {
    let mut connection = fixture();
    for migration in FRONTEND_MIGRATIONS {
        connection
            .execute(
                &format!("INSERT INTO {LOCAL_SCHEMA_MIGRATIONS_TABLE} VALUES (?1,?2,17)"),
                params![migration.version, migration.name],
            )
            .unwrap();
    }
    connection
        .execute_batch("CREATE TABLE shardline_lfs_locks (id TEXT)")
        .unwrap();
    assert!(install(&mut connection).is_err());
    let count: i64 = connection.query_row(
        &format!("SELECT count(*) FROM {LOCAL_SCHEMA_MIGRATIONS_TABLE} WHERE applied_at_unix_seconds=17"), [], |r| r.get(0)
    ).unwrap();
    assert_eq!(count, i64::try_from(FRONTEND_MIGRATIONS.len()).unwrap());
    let registry: i64 = connection
        .query_row(
            "SELECT count(*) FROM sqlite_master WHERE name='shardline_frontend_schema_v1'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(registry, 0);
}

#[test]
fn unrelated_unknown_core_migrations_still_prevent_startup() {
    let root = tempfile::tempdir().unwrap();
    let store = super::super::LocalIndexStore::new(root.path().to_path_buf()).unwrap();
    let connection = Connection::open(
        root.path()
            .join(super::super::LOCAL_METADATA_DATABASE_FILE_NAME),
    )
    .unwrap();
    connection
        .execute(
            &format!(
                "INSERT INTO {LOCAL_SCHEMA_MIGRATIONS_TABLE} VALUES ('99999999999999','unknown',17)"
            ),
            [],
        )
        .unwrap();
    assert!(
        matches!(store.check_schema_compatibility(), Err(LocalIndexStoreError::UnknownSchemaMigration(version)) if version == "99999999999999")
    );
    let count: i64 = connection.query_row(&format!("SELECT count(*) FROM {LOCAL_SCHEMA_MIGRATIONS_TABLE} WHERE version='99999999999999'"), [], |r| r.get(0)).unwrap();
    assert_eq!(count, 1);
}

#[test]
fn unknown_frontend_history_fails_check_and_install_without_deleting_it() {
    let mut connection = fixture();
    install(&mut connection).unwrap();
    connection
        .execute(
            &format!("INSERT INTO {HISTORY} VALUES ('99999999999999','future','future',17)"),
            [],
        )
        .unwrap();

    assert!(check(&connection).is_err());
    assert!(install(&mut connection).is_err());
    let preserved: i64 = connection
        .query_row(
            &format!("SELECT count(*) FROM {HISTORY} WHERE version='99999999999999'"),
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(preserved, 1);
}
