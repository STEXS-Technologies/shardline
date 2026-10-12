#![allow(clippy::unwrap_used, clippy::expect_used)]
use super::*;

#[tokio::test]
async fn older_owner_installation_preserves_legacy_admission_without_receipt_functions() {
    let Some(pool) = super::super::connect_isolated_postgres().await else {
        return;
    };
    sqlx::raw_sql(
        "DROP FUNCTION shardline_remote_delete_receipt_namespace_v1(TEXT);
         DROP FUNCTION shardline_remote_delete_receipt_begin_v1(TEXT,TEXT,TEXT);
         DROP FUNCTION shardline_remote_delete_receipt_pending_v1(TEXT,TEXT);
         DROP FUNCTION shardline_remote_delete_receipt_key_v1(TEXT,TEXT,BIGINT);
         DROP FUNCTION shardline_remote_delete_receipt_finish_v1(TEXT,TEXT,BIGINT,TEXT);",
    )
    .execute(&pool)
    .await
    .unwrap();
    let store = PostgresIndexStore::new(pool.clone());
    assert!(!store.supports_remote_delete_receipts().await.unwrap());
    assert_eq!(store.remote_delete_receipt_namespace().await.unwrap(), None);
    let object = ObjectKey::parse("receipt/older-owner-installation").unwrap();
    let permit = store.begin_remote_delete(&object).await.unwrap();
    assert!(
        store
            .begin_remote_delete(&object)
            .await
            .unwrap_err()
            .is_remote_delete_pending()
    );
    // The same owner upgrade enables receipts without rewriting pending history
    // or requiring runtime schema creation.
    store.initialize_remote_delete_guard().await.unwrap();
    assert!(store.supports_remote_delete_receipts().await.unwrap());
    assert!(
        store
            .remote_delete_receipt_namespace()
            .await
            .unwrap()
            .is_some()
    );
    assert!(
        store
            .begin_remote_delete(&object)
            .await
            .unwrap_err()
            .is_remote_delete_pending()
    );
    assert!(store.finish_remote_delete(&permit).await.unwrap());
    pool.close().await;
}

#[tokio::test]
async fn receipts_pin_admission_and_reject_tampering_stale_attempts_and_other_targets() {
    let Some(pool) = super::super::connect_isolated_postgres().await else {
        return;
    };
    let store = PostgresIndexStore::new(pool.clone());
    let object = ObjectKey::parse("receipt/known-reply").unwrap();
    let key = SigningKey::from_bytes(&[21; 32]);
    let other_key = SigningKey::from_bytes(&[22; 32]);
    let legacy = store.begin_remote_delete(&object).await.unwrap();
    assert!(legacy.sign_acknowledged_receipt(&key).is_err());
    assert!(store.finish_remote_delete(&legacy).await.unwrap());
    let permit = store
        .begin_remote_delete_with_receipt(&object, *key.verifying_key().as_bytes())
        .await
        .unwrap();
    let pinned: Option<String> =
        sqlx::query_scalar("SELECT shardline_remote_delete_receipt_key_v1(current_schema(),$1,$2)")
            .bind(object.as_str())
            .bind(permit.epoch())
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(pinned, Some(hex::encode(key.verifying_key().as_bytes())));
    assert!(permit.sign_acknowledged_receipt(&other_key).is_err());
    let receipt = permit.sign_acknowledged_receipt(&key).unwrap();
    let receipt: RemoteDeleteReceipt =
        serde_json::from_slice(&serde_json::to_vec(&receipt).unwrap()).unwrap();
    let mut corrupt = Vec::new();
    {
        let mut changed = receipt.clone();
        changed.object_key = "receipt/different-object".to_owned();
        corrupt.push(changed);
    }
    {
        let mut changed = receipt.clone();
        changed.epoch += 1;
        corrupt.push(changed);
    }
    {
        let mut changed = receipt.clone();
        changed.schema.push_str("_other");
        corrupt.push(changed);
    }
    {
        let mut changed = receipt.clone();
        changed.namespace.push('x');
        corrupt.push(changed);
    }
    {
        let mut changed = receipt.clone();
        changed.public_key = *other_key.verifying_key().as_bytes();
        corrupt.push(changed);
    }
    {
        let mut changed = receipt.clone();
        *changed.signature.first_mut().unwrap() ^= 1;
        corrupt.push(changed);
    }
    for changed in corrupt {
        assert!(store.settle_remote_delete_receipt(&changed).await.is_err());
    }
    assert!(
        store
            .begin_remote_delete(&object)
            .await
            .unwrap_err()
            .is_remote_delete_pending()
    );
    // A changed durable namespace cannot be treated as the original backend,
    // even with a coincident schema, object, attempt and verification key.
    let replacement = if receipt.namespace == "00".repeat(32) {
        "01".repeat(32)
    } else {
        "00".repeat(32)
    };
    sqlx::query("UPDATE shardline_resource_fences SET resource=$1 WHERE domain='maintenance-task-history-scope'")
        .bind(replacement).execute(&pool).await.unwrap();
    assert!(!store.settle_remote_delete_receipt(&receipt).await.unwrap());
    assert!(!store.finish_remote_delete(&permit).await.unwrap());
    sqlx::query("UPDATE shardline_resource_fences SET resource=$1 WHERE domain='maintenance-task-history-scope'")
        .bind(&receipt.namespace).execute(&pool).await.unwrap();
    let alternate_pool = super::super::connect_isolated_postgres().await.unwrap();
    let alternate = PostgresIndexStore::new(alternate_pool.clone());
    let alternate_permit = alternate
        .begin_remote_delete_with_receipt(&object, *key.verifying_key().as_bytes())
        .await
        .unwrap();
    assert!(
        !alternate
            .settle_remote_delete_receipt(&receipt)
            .await
            .unwrap()
    );
    assert!(
        alternate
            .begin_remote_delete(&object)
            .await
            .unwrap_err()
            .is_remote_delete_pending()
    );
    // Changing the pinned verifier cannot be hidden by a self-signed receipt.
    sqlx::query("UPDATE shardline_resource_fences SET resource=$1 WHERE domain='maintenance-remote-delete-receipt-key' AND epoch=$2")
        .bind(hex::encode(other_key.verifying_key().as_bytes())).bind(permit.epoch())
        .execute(&pool).await.unwrap();
    assert!(!store.settle_remote_delete_receipt(&receipt).await.unwrap());
    assert!(!store.finish_remote_delete(&permit).await.unwrap());
    sqlx::query("UPDATE shardline_resource_fences SET resource=$1 WHERE domain='maintenance-remote-delete-receipt-key' AND epoch=$2")
        .bind(hex::encode(key.verifying_key().as_bytes())).bind(permit.epoch())
        .execute(&pool).await.unwrap();
    assert!(store.settle_remote_delete_receipt(&receipt).await.unwrap());
    assert!(!store.settle_remote_delete_receipt(&receipt).await.unwrap());
    let next = store
        .begin_remote_delete_with_receipt(&object, *other_key.verifying_key().as_bytes())
        .await
        .unwrap();
    assert!(!store.settle_remote_delete_receipt(&receipt).await.unwrap());
    assert!(
        store
            .begin_remote_delete(&object)
            .await
            .unwrap_err()
            .is_remote_delete_pending()
    );
    // The original settlement API also retires the companion verifier.
    assert!(store.finish_remote_delete(&next).await.unwrap());
    let retained: i64 = sqlx::query_scalar("SELECT count(*) FROM shardline_resource_fences WHERE domain='maintenance-remote-delete-receipt-key'")
        .fetch_one(&pool).await.unwrap();
    assert_eq!(retained, 0);
    assert!(
        alternate
            .finish_remote_delete(&alternate_permit)
            .await
            .unwrap()
    );
    alternate_pool.close().await;
    pool.close().await;
}

#[tokio::test]
async fn existing_gc_role_can_settle_saved_receipts_without_fence_table_grants() {
    let Some(pool) = super::super::connect_isolated_postgres().await else {
        return;
    };
    let schema: String = sqlx::query_scalar("SELECT current_schema()")
        .fetch_one(&pool)
        .await
        .unwrap();
    let role = format!(
        "receipt_role_{}_{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    // Generated identifiers are ASCII alphanumeric plus underscores.
    sqlx::raw_sql(sqlx::AssertSqlSafe(format!(
        "CREATE ROLE {role} LOGIN PASSWORD 'owned_receipt_fixture';
         GRANT USAGE ON SCHEMA {schema} TO {role};
         GRANT SELECT ON {schema}.shardline_retention_holds TO {role};
         GRANT SELECT, DELETE ON {schema}.shardline_quarantine_candidates TO {role};"
    )))
    .execute(&pool)
    .await
    .unwrap();
    let configured_schema = schema.clone();
    let work_pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .after_connect(move |connection, _metadata| {
            let session_schema = configured_schema.clone();
            Box::pin(async move {
                sqlx::query(sqlx::AssertSqlSafe(format!(
                    "SET search_path TO {session_schema}, public"
                )))
                .execute(connection)
                .await?;
                Ok(())
            })
        })
        .connect_with(
            (*pool.connect_options())
                .clone()
                .username(&role)
                .password("owned_receipt_fixture"),
        )
        .await
        .unwrap();
    assert!(
        sqlx::query("SELECT * FROM shardline_resource_fences")
            .execute(&work_pool)
            .await
            .is_err()
    );
    let work = PostgresIndexStore::new(work_pool.clone());
    let object = ObjectKey::parse("receipt/closed-work-session").unwrap();
    let key = SigningKey::from_bytes(&[23; 32]);
    let permit = work
        .begin_remote_delete_with_receipt(&object, *key.verifying_key().as_bytes())
        .await
        .unwrap();
    // Persisted bytes outlive the source connection and private key.
    let bytes = serde_json::to_vec(&permit.sign_acknowledged_receipt(&key).unwrap()).unwrap();
    drop(key);
    work_pool.close().await;
    assert!(work.finish_remote_delete(&permit).await.is_err());
    let options = (*pool.connect_options())
        .clone()
        .username(&role)
        .password("owned_receipt_fixture");
    let replay_schema = schema.clone();
    let recorder_pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .after_connect(move |connection, _metadata| {
            let session_schema = replay_schema.clone();
            Box::pin(async move {
                sqlx::query(sqlx::AssertSqlSafe(format!(
                    "SET search_path TO {session_schema}, public"
                )))
                .execute(connection)
                .await?;
                Ok(())
            })
        })
        .connect_with(options)
        .await
        .unwrap();
    let recorder = PostgresIndexStore::new(recorder_pool.clone());
    assert!(
        recorder
            .settle_remote_delete_receipt(&serde_json::from_slice(&bytes).unwrap())
            .await
            .unwrap()
    );
    assert!(work.finish_remote_delete(&permit).await.is_err());
    assert!(
        sqlx::query("SELECT * FROM shardline_resource_fences")
            .execute(&recorder_pool)
            .await
            .is_err()
    );
    recorder_pool.close().await;
    sqlx::raw_sql(sqlx::AssertSqlSafe(format!(
        "DROP OWNED BY {role}; DROP ROLE {role};"
    )))
    .execute(&pool)
    .await
    .unwrap();
    pool.close().await;
}
