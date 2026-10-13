#![allow(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing)]
use super::*;

const PREFIX: &str = "_shardline_tasks/v1/";

#[tokio::test]
async fn bounded_publication_holds_manifest_and_exact_retry_rows_until_commit() {
    let Some(pool) = super::super::connect_isolated_postgres().await else {
        return;
    };
    let store = PostgresIndexStore::new(pool);
    store.initialize_task_history_scope().await.unwrap();
    let manifest = format!("{PREFIX}manifests/publication-race");
    let record = format!("{PREFIX}runs/publication-race/records/0000000001");
    let mut publisher = store.pool().acquire().await.unwrap();
    assert!(
        PostgresIndexStore::publish_task_artifact_bounded_on_connection(
            &mut publisher,
            &manifest,
            b"manifest",
            1
        )
        .await
        .unwrap()
    );
    let mut cleaner = store.pool().acquire().await.unwrap();
    sqlx::query("SET statement_timeout='100ms'")
        .execute(&mut *cleaner)
        .await
        .unwrap();
    for (key, target) in [(&record, &manifest), (&record, &record)] {
        let mut transaction = publisher.begin().await.unwrap();
        assert!(
            PostgresIndexStore::publish_task_artifact_bounded_on_connection(
                &mut transaction,
                key,
                b"record",
                1
            )
            .await
            .unwrap()
        );
        let error = PostgresIndexStore::delete_task_artifact_on_connection(&mut cleaner, target)
            .await
            .expect_err("retirement must wait for publication to commit");
        let code = if let PostgresMetadataStoreError::Sqlx(error) = error {
            error
                .as_database_error()
                .and_then(|error| error.code())
                .map(|code| code.into_owned())
        } else {
            None
        };
        assert_eq!(code.as_deref(), Some("57014"));
        transaction.commit().await.unwrap();
    }
    assert!(store.delete_task_artifact(&manifest).await.unwrap());
    assert!(
        !PostgresIndexStore::publish_task_artifact_bounded_on_connection(
            &mut publisher,
            &format!("{PREFIX}runs/publication-race/records/0000000002"),
            b"late record",
            1
        )
        .await
        .unwrap()
    );
    assert!(store.delete_task_artifact(&record).await.unwrap());
    assert!(
        store
            .task_artifact_page(PREFIX, None)
            .await
            .unwrap()
            .is_empty()
    );
    drop(cleaner);
    drop(publisher);
    store.pool().close().await;
}

#[tokio::test]
async fn bounded_deletion_preserves_atomicity_outer_transaction_and_timeout() {
    let Some(pool) = super::super::connect_isolated_postgres().await else {
        return;
    };
    let store = PostgresIndexStore::new(pool);
    store.initialize_task_history_scope().await.unwrap();
    let keys: Vec<String> = (0..64)
        .map(|index| format!("{PREFIX}batch/{index:02}"))
        .collect();
    for key in &keys {
        store.publish_task_artifact(key, b"evidence").await.unwrap();
    }
    let mut connection = store.pool().acquire().await.unwrap();
    sqlx::query("SET statement_timeout='17s'")
        .execute(&mut *connection)
        .await
        .unwrap();
    let mut outer = connection.begin().await.unwrap();
    assert_eq!(
        PostgresIndexStore::delete_task_artifacts_on_connection(&mut outer, &keys)
            .await
            .unwrap(),
        64
    );
    let outer_timeout: String = sqlx::query_scalar("SHOW statement_timeout")
        .fetch_one(&mut *outer)
        .await
        .unwrap();
    assert_eq!(outer_timeout, "17s");
    outer.rollback().await.unwrap();
    assert!(
        store
            .read_task_artifact(&keys[0], 8)
            .await
            .unwrap()
            .is_some()
    );
    // Absent keys are no-ops. Inject failure at the last real deletion to
    // verify that earlier deletions in the same batch roll back too.
    sqlx::raw_sql(sqlx::AssertSqlSafe(
        "CREATE FUNCTION reject_batch_delete_fixture() RETURNS trigger LANGUAGE plpgsql AS $$
         BEGIN
           IF OLD.artifact_key = '_shardline_tasks/v1/batch/63' THEN
             RAISE EXCEPTION 'injected final artifact deletion failure';
           END IF;
           RETURN OLD;
         END $$;
         CREATE TRIGGER reject_batch_delete_fixture BEFORE DELETE ON shardline_task_artifacts_v1
         FOR EACH ROW EXECUTE FUNCTION reject_batch_delete_fixture()"
            .to_owned(),
    ))
    .execute(&mut *connection)
    .await
    .unwrap();
    assert!(
        PostgresIndexStore::delete_task_artifacts_on_connection(&mut connection, &keys)
            .await
            .is_err()
    );
    assert!(
        store
            .read_task_artifact(&keys[0], 8)
            .await
            .unwrap()
            .is_some()
    );
    sqlx::raw_sql(sqlx::AssertSqlSafe(
        "DROP TRIGGER reject_batch_delete_fixture ON shardline_task_artifacts_v1;
         DROP FUNCTION reject_batch_delete_fixture()"
            .to_owned(),
    ))
    .execute(&mut *connection)
    .await
    .unwrap();
    assert!(
        PostgresIndexStore::delete_task_artifacts_on_connection(
            &mut connection,
            &vec![keys[0].clone(); 65]
        )
        .await
        .is_err()
    );
    assert_eq!(
        PostgresIndexStore::delete_task_artifacts_on_connection(&mut connection, &keys)
            .await
            .unwrap(),
        64
    );
    assert!(
        store
            .task_artifact_page(PREFIX, None)
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        PostgresIndexStore::delete_task_artifacts_on_connection(&mut connection, &keys)
            .await
            .unwrap(),
        0
    );
    let timeout: String = sqlx::query_scalar("SHOW statement_timeout")
        .fetch_one(&mut *connection)
        .await
        .unwrap();
    assert_eq!(timeout, "17s");
    drop(connection);
    store.pool().close().await;
}

#[tokio::test]
async fn borrowed_session_preserves_target_transaction_and_timeout() {
    let Some(pool) = super::super::connect_isolated_postgres().await else {
        return;
    };
    let source = PostgresIndexStore::new(pool);
    source.initialize_task_history_scope().await.unwrap();
    let target = PostgresIndexStore::new(super::super::connect_isolated_postgres().await.unwrap());
    target.initialize_task_history_scope().await.unwrap();
    let schema: String = sqlx::query_scalar("SELECT current_schema()")
        .fetch_one(target.pool())
        .await
        .unwrap();
    let mut connection = source.pool().acquire().await.unwrap();
    sqlx::query("SELECT set_config('search_path', $1, false)")
        .bind(format!("\"{}\", public", schema.replace('"', "\"\"")))
        .execute(&mut *connection)
        .await
        .unwrap();
    sqlx::query("SET statement_timeout='17s'")
        .execute(&mut *connection)
        .await
        .unwrap();
    let key = format!("{PREFIX}borrowed-session");
    let mut publication_transaction = connection.begin().await.unwrap();
    PostgresIndexStore::publish_task_artifact_on_connection(
        &mut publication_transaction,
        &key,
        b"original",
    )
    .await
    .unwrap();
    assert!(
        PostgresIndexStore::publish_task_artifact_on_connection(
            &mut publication_transaction,
            &key,
            b"conflict"
        )
        .await
        .is_err()
    );
    // The failed publication rolls back only its savepoint. Subsequent reads
    // still work on the original transaction, rather than a replacement session.
    let timeout: String = sqlx::query_scalar("SHOW statement_timeout")
        .fetch_one(&mut *publication_transaction)
        .await
        .unwrap();
    assert_eq!(timeout, "17s");
    assert_eq!(
        PostgresIndexStore::read_task_artifact_on_connection(&mut publication_transaction, &key, 8)
            .await
            .unwrap()
            .unwrap()
            .bytes
            .as_deref(),
        Some(b"original".as_slice())
    );
    assert_eq!(
        PostgresIndexStore::task_artifact_page_on_connection(
            &mut publication_transaction,
            PREFIX,
            None
        )
        .await
        .unwrap(),
        vec![(key.clone(), 8)]
    );
    assert!(target.read_task_artifact(&key, 8).await.unwrap().is_none());
    let bounded = PostgresIndexStore::task_artifact_bounded_page_on_connection(
        &mut publication_transaction,
        PREFIX,
        None,
    )
    .await
    .unwrap();
    assert_eq!(
        bounded,
        vec![(
            key.clone(),
            PostgresTaskArtifact {
                length: 8,
                bytes: Some(b"original".to_vec()),
            }
        )]
    );
    publication_transaction.rollback().await.unwrap();
    assert!(target.read_task_artifact(&key, 8).await.unwrap().is_none());
    PostgresIndexStore::publish_task_artifact_on_connection(&mut connection, &key, b"committed")
        .await
        .unwrap();
    assert!(source.read_task_artifact(&key, 9).await.unwrap().is_none());
    assert_eq!(
        target
            .read_task_artifact(&key, 9)
            .await
            .unwrap()
            .unwrap()
            .bytes
            .as_deref(),
        Some(b"committed".as_slice())
    );
    let mut deletion_transaction = connection.begin().await.unwrap();
    assert!(
        PostgresIndexStore::delete_task_artifact_on_connection(&mut deletion_transaction, &key)
            .await
            .unwrap()
    );
    deletion_transaction.rollback().await.unwrap();
    assert!(target.read_task_artifact(&key, 9).await.unwrap().is_some());
    assert!(
        PostgresIndexStore::delete_task_artifact_on_connection(&mut connection, &key)
            .await
            .unwrap()
    );
    let timeout_after_deletion: String = sqlx::query_scalar("SHOW statement_timeout")
        .fetch_one(&mut *connection)
        .await
        .unwrap();
    assert_eq!(timeout_after_deletion, "17s");

    // A short work deadline must not be relaxed to the evidence ceiling.
    // Hold the table from another session so publication actually encounters
    // the deadline, rather than only inspecting a restored configuration value.
    sqlx::query("SET statement_timeout='100ms'")
        .execute(&mut *connection)
        .await
        .unwrap();
    let mut blocker = target.pool().begin().await.unwrap();
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "LOCK TABLE \"{}\".shardline_task_artifacts_v1 IN ACCESS EXCLUSIVE MODE",
        schema.replace('"', "\"\"")
    )))
    .execute(&mut *blocker)
    .await
    .unwrap();
    let mut blocked_transaction = connection.begin().await.unwrap();
    let error = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        PostgresIndexStore::publish_task_artifact_on_connection(
            &mut blocked_transaction,
            &key,
            b"blocked",
        ),
    )
    .await
    .unwrap()
    .unwrap_err();
    let code = if let PostgresMetadataStoreError::Sqlx(error) = error {
        error
            .as_database_error()
            .unwrap()
            .code()
            .map(|code| code.into_owned())
    } else {
        None
    };
    assert_eq!(code.as_deref(), Some("57014"));
    let timeout_after_blocking: String = sqlx::query_scalar("SHOW statement_timeout")
        .fetch_one(&mut *blocked_transaction)
        .await
        .unwrap();
    assert_eq!(timeout_after_blocking, "100ms");
    blocked_transaction.rollback().await.unwrap();
    blocker.rollback().await.unwrap();
}

#[tokio::test]
async fn owner_defaults_cannot_bypass_private_artifact_functions() {
    let Some(pool) = super::super::connect_isolated_postgres().await else {
        return;
    };
    let store = PostgresIndexStore::new(pool);
    let schema: String = sqlx::query_scalar("SELECT current_schema()")
        .fetch_one(store.pool())
        .await
        .unwrap();
    let quoted_schema = format!("\"{}\"", schema.replace('"', "\"\""));
    let mut nonce = [0_u8; 8];
    getrandom::fill(&mut nonce).unwrap();
    let role = format!("artifact_default_{}", hex::encode(nonce));
    sqlx::raw_sql(sqlx::AssertSqlSafe(format!(
        "CREATE ROLE \"{role}\"; ALTER DEFAULT PRIVILEGES IN SCHEMA {quoted_schema}
         GRANT SELECT,INSERT,UPDATE,DELETE ON TABLES TO \"{role}\";"
    )))
    .execute(store.pool())
    .await
    .unwrap();
    store.initialize_task_history_scope().await.unwrap();
    let direct: bool = sqlx::query_scalar("SELECT has_table_privilege($1,format('%I.shardline_task_artifacts_v1',$2),'SELECT,INSERT,UPDATE,DELETE')")
        .bind(&role).bind(&schema).fetch_one(store.pool()).await.unwrap();
    assert!(!direct);
    // Declared owner defaults still apply to unrelated future tables.
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "CREATE TABLE {quoted_schema}.future_metadata(value TEXT)"
    )))
    .execute(store.pool())
    .await
    .unwrap();
    let retained: bool = sqlx::query_scalar("SELECT has_table_privilege($1,format('%I.future_metadata',$2),'SELECT,INSERT,UPDATE,DELETE')")
        .bind(&role).bind(&schema).fetch_one(store.pool()).await.unwrap();
    assert!(retained);
    sqlx::raw_sql(sqlx::AssertSqlSafe(format!(
        "DROP OWNED BY \"{role}\"; DROP ROLE \"{role}\";"
    )))
    .execute(store.pool())
    .await
    .unwrap();
}

#[tokio::test]
async fn immutable_publication_bounds_and_concurrent_conflicts() {
    let Some(pool) = super::super::connect_isolated_postgres().await else {
        return;
    };
    let store = PostgresIndexStore::new(pool);
    store.initialize_task_history_scope().await.unwrap();
    let key = format!("{PREFIX}test-record");
    assert!(store.read_task_artifact(&key, 0).await.unwrap().is_none());
    store
        .publish_task_artifact(&key, b"exact signed bytes")
        .await
        .unwrap();
    store
        .publish_task_artifact(&key, b"exact signed bytes")
        .await
        .unwrap();
    assert!(
        store
            .publish_task_artifact(&key, b"replacement")
            .await
            .is_err()
    );
    assert_eq!(
        store.read_task_artifact(&key, 2).await.unwrap(),
        Some(PostgresTaskArtifact {
            length: 18,
            bytes: None
        })
    );
    assert_eq!(
        store
            .read_task_artifact(&key, 18)
            .await
            .unwrap()
            .unwrap()
            .bytes
            .as_deref(),
        Some(b"exact signed bytes".as_slice())
    );
    let racing = format!("{PREFIX}racing");
    let (first, second) = tokio::join!(
        store.publish_task_artifact(&racing, b"first"),
        store.publish_task_artifact(&racing, b"second")
    );
    assert_ne!(first.is_ok(), second.is_ok());
    let winner = if first.is_ok() {
        b"first".as_slice()
    } else {
        b"second".as_slice()
    };
    let reopened = PostgresIndexStore::new(store.pool().clone());
    assert_eq!(
        reopened
            .read_task_artifact(&racing, 100)
            .await
            .unwrap()
            .unwrap()
            .bytes
            .as_deref(),
        Some(winner)
    );
    assert!(
        store
            .publish_task_artifact("payload/forbidden", b"body")
            .await
            .is_err()
    );
    assert!(store.delete_task_artifact(&key).await.unwrap());
    assert!(!store.delete_task_artifact(&key).await.unwrap());
}

#[tokio::test]
async fn bounded_payload_pages_preserve_literal_prefix_and_oversize_evidence() {
    let Some(pool) = super::super::connect_isolated_postgres().await else {
        return;
    };
    let store = PostgresIndexStore::new(pool);
    store.initialize_task_history_scope().await.unwrap();
    let prefix = format!("{PREFIX}batch%/");
    for index in 0..65 {
        store
            .publish_task_artifact(&format!("{prefix}{index:03}"), b"opaque")
            .await
            .unwrap();
    }
    store
        .publish_task_artifact(&format!("{prefix}065"), &vec![7; 65536])
        .await
        .unwrap();
    store
        .publish_task_artifact(&format!("{prefix}066"), &vec![8; 65537])
        .await
        .unwrap();
    store
        .publish_task_artifact(&format!("{PREFIX}batchX/keep"), b"keep")
        .await
        .unwrap();
    let mut connection = store.pool().acquire().await.unwrap();
    let first = PostgresIndexStore::task_artifact_bounded_page_on_connection(
        &mut connection,
        &prefix,
        None,
    )
    .await
    .unwrap();
    assert_eq!(first.len(), 64);
    assert!(first.iter().all(|(key, artifact)| key.starts_with(&prefix)
        && artifact.length == 6
        && artifact.bytes.as_deref() == Some(b"opaque".as_slice())));
    let second = PostgresIndexStore::task_artifact_bounded_page_on_connection(
        &mut connection,
        &prefix,
        Some(&first.last().unwrap().0),
    )
    .await
    .unwrap();
    assert_eq!(second.len(), 3);
    assert_eq!(
        second[1].1.bytes.as_deref(),
        Some(vec![7; 65536].as_slice())
    );
    assert_eq!(second[2].1.length, 65537);
    assert!(second[2].1.bytes.is_none());
    assert!(
        PostgresIndexStore::task_artifact_bounded_page_on_connection(
            &mut connection,
            &prefix,
            Some(&second.last().unwrap().0),
        )
        .await
        .unwrap()
        .is_empty()
    );
}

#[tokio::test]
async fn literal_prefix_pages_allow_retention_without_loading_bodies() {
    let Some(pool) = super::super::connect_isolated_postgres().await else {
        return;
    };
    let store = PostgresIndexStore::new(pool);
    store.initialize_task_history_scope().await.unwrap();
    let prefix = format!("{PREFIX}test%/");
    for index in 0..130 {
        store
            .publish_task_artifact(&format!("{prefix}{index:03}"), b"opaque")
            .await
            .unwrap();
    }
    let unrelated = format!("{PREFIX}testX/keep");
    store
        .publish_task_artifact(&unrelated, b"keep")
        .await
        .unwrap();
    let mut after: Option<String> = None;
    let mut count = 0_usize;
    'pages: loop {
        let page = store
            .task_artifact_page(&prefix, after.as_deref())
            .await
            .unwrap();
        assert!(page.len() <= 64);
        if page.is_empty() {
            break;
        }
        for (key, length) in page {
            if !key.starts_with(&prefix) {
                break 'pages;
            }
            assert_eq!(length, 6);
            assert!(store.delete_task_artifact(&key).await.unwrap());
            count = count.saturating_add(1);
            after = Some(key);
        }
    }
    assert_eq!(count, 130);
    assert_eq!(
        store
            .read_task_artifact(&unrelated, 4)
            .await
            .unwrap()
            .unwrap()
            .bytes
            .as_deref(),
        Some(b"keep".as_slice())
    );
}

#[tokio::test]
async fn owner_reinstallation_preserves_released_ledger_and_existing_grants() {
    let Some(pool) = super::super::connect_isolated_postgres().await else {
        return;
    };
    let store = PostgresIndexStore::new(pool);
    let schema: String = sqlx::query_scalar("SELECT current_schema()")
        .fetch_one(store.pool())
        .await
        .unwrap();
    let ledger: Vec<String> = sqlx::query_scalar("SELECT row_to_json(ledger)::text FROM public.shardline_schema_migrations AS ledger ORDER BY version").fetch_all(store.pool()).await.unwrap();
    let acl_query = "SELECT target.relname, coalesce(target.relacl::text,'') FROM pg_class target JOIN pg_namespace namespace ON namespace.oid=target.relnamespace WHERE namespace.nspname=$1 AND target.relkind='r' AND target.relname <> 'shardline_task_artifacts_v1' ORDER BY target.relname";
    let grants: Vec<(String, String)> = sqlx::query_as(acl_query)
        .bind(&schema)
        .fetch_all(store.pool())
        .await
        .unwrap();
    store.initialize_task_history_scope().await.unwrap();
    let identity_query = "SELECT shardline_task_history_scope_v1(current_schema())";
    let identity: String = sqlx::query_scalar(identity_query)
        .fetch_one(store.pool())
        .await
        .unwrap();
    let key = format!("{PREFIX}retained");
    store
        .publish_task_artifact(&key, b"preserved")
        .await
        .unwrap();
    store.initialize_task_history_scope().await.unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, String>(identity_query)
            .fetch_one(store.pool())
            .await
            .unwrap(),
        identity
    );
    assert_eq!(
        sqlx::query_as::<_, (String, String)>(acl_query)
            .bind(&schema)
            .fetch_all(store.pool())
            .await
            .unwrap(),
        grants
    );
    assert_eq!(sqlx::query_scalar::<_, String>("SELECT row_to_json(ledger)::text FROM public.shardline_schema_migrations AS ledger ORDER BY version").fetch_all(store.pool()).await.unwrap(), ledger);
    assert_eq!(
        store
            .read_task_artifact(&key, 9)
            .await
            .unwrap()
            .unwrap()
            .bytes
            .as_deref(),
        Some(b"preserved".as_slice())
    );
    let rls = format!(
        "ALTER TABLE \"{}\".shardline_reliability_events ENABLE ROW LEVEL SECURITY",
        schema.replace('"', "\"\"")
    );
    sqlx::query(sqlx::AssertSqlSafe(rls))
        .execute(store.pool())
        .await
        .unwrap();
    let error = store.read_task_artifact(&key, 9).await.unwrap_err();
    assert!(
        matches!(error, PostgresMetadataStoreError::Sqlx(error) if error.as_database_error().and_then(sqlx::error::DatabaseError::code).as_deref() == Some("SLD04"))
    );
}

#[tokio::test]
async fn existing_role_permissions_control_publication_reads_and_retention() {
    use std::fmt::Write as _;
    use std::str::FromStr;
    let Some(pool) = super::super::connect_isolated_postgres().await else {
        return;
    };
    let store = PostgresIndexStore::new(pool);
    store.initialize_task_history_scope().await.unwrap();
    let schema: String = sqlx::query_scalar("SELECT current_schema()")
        .fetch_one(store.pool())
        .await
        .unwrap();
    let quoted_schema = format!("\"{}\"", schema.replace('"', "\"\""));
    let url = std::env::var("DATABASE_URL")
        .or_else(|_| std::env::var("SHARDLINE_INDEX_POSTGRES_URL"))
        .unwrap();
    let mut roles = Vec::new();
    for profile in ["reader", "writer", "gc", "unprivileged"] {
        let mut nonce = [0_u8; 8];
        getrandom::fill(&mut nonce).unwrap();
        let role = format!("artifact_{profile}_{}", hex::encode(nonce));
        let quoted_role = format!("\"{role}\"");
        let mut ddl = format!(
            "CREATE ROLE {quoted_role} LOGIN PASSWORD 'fixture_artifact_role'; GRANT USAGE ON SCHEMA {quoted_schema} TO {quoted_role};"
        );
        if profile != "unprivileged" {
            write!(
                ddl,
                "GRANT SELECT ON {quoted_schema}.shardline_reliability_events TO {quoted_role};"
            )
            .unwrap();
        }
        if profile == "writer" {
            write!(
                ddl,
                "GRANT INSERT ON {quoted_schema}.shardline_reliability_events TO {quoted_role};"
            )
            .unwrap();
        }
        if profile == "gc" {
            write!(ddl, "GRANT SELECT,DELETE ON {quoted_schema}.shardline_quarantine_candidates TO {quoted_role};").unwrap();
        }
        sqlx::raw_sql(sqlx::AssertSqlSafe(ddl))
            .execute(store.pool())
            .await
            .unwrap();
        let options = sqlx::postgres::PgConnectOptions::from_str(&url)
            .unwrap()
            .username(&role)
            .password("fixture_artifact_role")
            .options([("search_path", schema.as_str())]);
        let role_pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(1)
            .connect_with(options)
            .await
            .unwrap();
        let direct: bool = sqlx::query_scalar("SELECT has_table_privilege(current_user, format('%I.shardline_task_artifacts_v1', current_schema()), 'SELECT,INSERT,DELETE')").fetch_one(&role_pool).await.unwrap();
        assert!(!direct);
        roles.push((profile, role, PostgresIndexStore::new(role_pool)));
    }
    let key = format!("{PREFIX}authorized");
    let reader = &roles[0].2;
    let writer = &roles[1].2;
    let gc = &roles[2].2;
    let unprivileged = &roles[3].2;
    writer
        .publish_task_artifact(&key, b"preserved")
        .await
        .unwrap();
    assert_eq!(
        reader
            .read_task_artifact(&key, 9)
            .await
            .unwrap()
            .unwrap()
            .bytes
            .as_deref(),
        Some(b"preserved".as_slice())
    );
    assert!(
        reader
            .publish_task_artifact(&key, b"preserved")
            .await
            .is_err()
    );
    assert!(reader.delete_task_artifact(&key).await.is_err());
    assert!(writer.delete_task_artifact(&key).await.is_err());
    for restricted_store in [&reader, &writer] {
        let mut restricted_connection = restricted_store.pool().acquire().await.unwrap();
        assert!(
            PostgresIndexStore::delete_task_artifacts_on_connection(
                &mut restricted_connection,
                std::slice::from_ref(&key)
            )
            .await
            .is_err()
        );
    }
    assert!(unprivileged.read_task_artifact(&key, 9).await.is_err());
    assert!(unprivileged.task_artifact_page(PREFIX, None).await.is_err());
    gc.publish_task_artifact(&format!("{PREFIX}gc"), b"gc evidence")
        .await
        .unwrap();
    assert!(gc.delete_task_artifact(&key).await.unwrap());
    let mut gc_connection = gc.pool().acquire().await.unwrap();
    assert_eq!(
        PostgresIndexStore::delete_task_artifacts_on_connection(
            &mut gc_connection,
            &[format!("{PREFIX}gc")]
        )
        .await
        .unwrap(),
        1
    );
    drop(gc_connection);
    let spoof = sqlx::query(sqlx::AssertSqlSafe(format!(
        "SELECT {quoted_schema}.shardline_task_artifact_read_v1($1,$2,$3)"
    )))
    .bind("public")
    .bind(&key)
    .bind(9_i64)
    .execute(reader.pool())
    .await;
    assert!(spoof.is_err());
    for (_, role, role_store) in roles {
        role_store.pool().close().await;
        sqlx::raw_sql(sqlx::AssertSqlSafe(format!(
            "DROP OWNED BY \"{role}\"; DROP ROLE \"{role}\";"
        )))
        .execute(store.pool())
        .await
        .unwrap();
    }
}
