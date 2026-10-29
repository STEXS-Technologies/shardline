use shardline_index::hub::{BoxedHubStore, HubFileEntry};

pub(super) async fn contract(store: &BoxedHubStore, pool: &sqlx::PgPool) {
    for statement in [
        "CREATE TABLE shardline_test_insert_statements (counted boolean)",
        "CREATE FUNCTION shardline_test_count_file_insert() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN INSERT INTO shardline_test_insert_statements VALUES (true); RETURN NULL; END $$",
        "CREATE TRIGGER shardline_test_file_insert AFTER INSERT ON shardline_hub_file_entries FOR EACH STATEMENT EXECUTE FUNCTION shardline_test_count_file_insert()",
    ] {
        sqlx::query(statement).execute(pool).await.unwrap();
    }
    let entry = |path: String, size, sha: &str| HubFileEntry {
        path,
        size,
        sha: sha.to_owned(),
        is_lfs: false,
    };
    let mut files: Vec<_> = (0..1001)
        .map(|number| entry(format!("batch-{number}"), 1, "initial"))
        .collect();
    files.push(entry("batch-0".to_owned(), 2, "intermediate"));
    let mut final_entry = entry("batch-0".to_owned(), 3, "final");
    final_entry.is_lfs = true;
    files.push(final_entry);
    store.store_files("batched-write-contract", &files).unwrap();
    let retained = store.get_files("batched-write-contract").unwrap();
    assert_eq!(retained.len(), 1001);
    assert!(retained.iter().any(|file| file.path == "batch-0"
        && file.sha == "final"
        && file.size == 3
        && file.is_lfs));
    let statements: i64 =
        sqlx::query_scalar("SELECT count(*) FROM shardline_test_insert_statements")
            .fetch_one(pool)
            .await
            .unwrap();
    assert_eq!(
        statements, 3,
        "1003 ordered entries require three bounded INSERTs"
    );
    let row_version = || {
        sqlx::query_scalar::<_, String>(
            "SELECT xmin::text FROM shardline_hub_file_entries WHERE commit_sha='batched-write-contract' AND path='batch-1'",
        )
    };
    let before_replay = row_version().fetch_one(pool).await.unwrap();
    store
        .store_files("batched-write-contract", &retained)
        .unwrap();
    assert_eq!(row_version().fetch_one(pool).await.unwrap(), before_replay);
    let statements_after_replay: i64 =
        sqlx::query_scalar("SELECT count(*) FROM shardline_test_insert_statements")
            .fetch_one(pool)
            .await
            .unwrap();
    assert_eq!(statements_after_replay, 5);

    // The first batch modifies existing metadata and inserts new entries. A
    // later overflow must roll all of it back, including the statement audit.
    let mut invalid = vec![entry("batch-0".to_owned(), 4, "must-rollback")];
    invalid.extend((0..1000).map(|number| entry(format!("rollback-{number}"), 1, "new")));
    invalid.push(entry("invalid-size".to_owned(), u64::MAX, "overflow"));
    assert!(
        store
            .store_files("batched-write-contract", &invalid)
            .is_err()
    );
    assert_eq!(store.get_files("batched-write-contract").unwrap(), retained);
    let statements_after: i64 =
        sqlx::query_scalar("SELECT count(*) FROM shardline_test_insert_statements")
            .fetch_one(pool)
            .await
            .unwrap();
    assert_eq!(statements_after, statements_after_replay);
    // An invalid intermediate duplicate must not disappear during batching.
    assert!(
        store
            .store_files(
                "batched-write-contract",
                &[
                    entry("batch-0".to_owned(), u64::MAX, "invalid"),
                    entry("batch-0".to_owned(), 3, "final"),
                ]
            )
            .is_err()
    );
    assert_eq!(store.get_files("batched-write-contract").unwrap(), retained);

    // Also fail inside PostgreSQL, after a successful first batch. A client-side
    // validation error alone would not qualify database failure handling.
    for statement in [
        "CREATE FUNCTION shardline_test_reject_file_insert() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.path = 'reject-sql' THEN RAISE EXCEPTION 'injected file batch rejection'; END IF; RETURN NEW; END $$",
        "CREATE TRIGGER shardline_test_reject_file BEFORE INSERT ON shardline_hub_file_entries FOR EACH ROW EXECUTE FUNCTION shardline_test_reject_file_insert()",
    ] {
        sqlx::query(statement).execute(pool).await.unwrap();
    }
    let mut rejected = vec![entry("batch-0".to_owned(), 4, "must-rollback")];
    rejected.extend((0..1000).map(|number| entry(format!("sql-rollback-{number}"), 1, "new")));
    rejected.push(entry("reject-sql".to_owned(), 1, "rejected"));
    let error = store
        .store_files("batched-write-contract", &rejected)
        .unwrap_err();
    // Public metadata errors deliberately redact database details; verify the
    // private source chain without exposing it in assertion output.
    let mut cause: Option<&(dyn std::error::Error + 'static)> = Some(error.as_ref());
    let mut injected = false;
    while let Some(current) = cause {
        injected |= current
            .to_string()
            .contains("injected file batch rejection");
        cause = current.source();
    }
    assert!(injected, "expected the injected PostgreSQL rejection");
    assert_eq!(store.get_files("batched-write-contract").unwrap(), retained);
    let statements_after_sql_failure: i64 =
        sqlx::query_scalar("SELECT count(*) FROM shardline_test_insert_statements")
            .fetch_one(pool)
            .await
            .unwrap();
    assert_eq!(statements_after_sql_failure, statements_after_replay);
}
