//! Saved-reply replay with real PostgreSQL and a deterministic evidence backlog.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use ed25519_dalek::SigningKey;
use shardline_index::{PostgresIndexStore, RemoteDeleteReceipt};
use shardline_protocol::ShardlineHash;
use shardline_server::{StorageMigrationEndpoint, StorageMigrationOptions, run_storage_migration};
use shardline_server_core::{
    ServerObjectStore, remote_delete::recover_saved_remote_delete_replies,
};
use shardline_storage::{ObjectBody, ObjectIntegrity, ObjectKey, ObjectStore};
use sqlx::{PgPool, postgres::PgPoolOptions};

fn write(objects: &ServerObjectStore, key: &ObjectKey, bytes: &[u8]) {
    objects
        .put_if_absent(
            key,
            ObjectBody::from_slice(bytes),
            &ObjectIntegrity::new(
                ShardlineHash::from_bytes(*blake3::hash(bytes).as_bytes()),
                u64::try_from(bytes.len()).unwrap(),
            ),
        )
        .unwrap();
}

fn save(objects: &ServerObjectStore, receipt: &RemoteDeleteReceipt) -> ObjectKey {
    let bytes = serde_json::to_vec(receipt).unwrap();
    let key = ObjectKey::parse(&format!(
        "_shardline_tasks/v1/delete-replies/{}/replies/{}.json",
        receipt.verified_namespace().unwrap(),
        blake3::hash(&bytes).to_hex()
    ))
    .unwrap();
    write(objects, &key, &bytes);
    key
}

fn read(objects: &ServerObjectStore, key: &ObjectKey) -> Vec<u8> {
    let length = objects.metadata(key).unwrap().unwrap().length();
    objects
        .read_range(
            key,
            shardline_protocol::ByteRange::new(0, length.saturating_sub(1)).unwrap(),
        )
        .unwrap()
}

async fn schema_pool(url: &str, schema: String) -> PgPool {
    PgPoolOptions::new()
        .max_connections(1)
        .after_connect(move |connection, _metadata| {
            let session_schema = schema.clone();
            Box::pin(async move {
                sqlx::query(sqlx::AssertSqlSafe(format!(
                    "SET search_path TO {session_schema}, public"
                )))
                .execute(connection)
                .await?;
                Ok(())
            })
        })
        .connect(url)
        .await
        .expect("owned recovery fixture connection")
}

#[tokio::test]
async fn recovery_advances_past_invalid_evidence_and_never_infers_missing_replies() {
    let Ok(url) = std::env::var("DATABASE_URL") else {
        eprintln!("SKIPPED: saved-reply recovery needs the PostgreSQL fixture");
        return;
    };
    let admin = PgPoolOptions::new()
        .max_connections(1)
        .connect(&url)
        .await
        .expect("owned recovery fixture administrator");
    let schema = format!(
        "reply_recovery_{}_{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    sqlx::query(sqlx::AssertSqlSafe(format!("CREATE SCHEMA {schema}")))
        .execute(&admin)
        .await
        .unwrap();
    let tables: Vec<String> = sqlx::query_scalar(
        "SELECT tablename FROM pg_tables WHERE schemaname='public' AND tablename LIKE 'shardline_%'",
    ).fetch_all(&admin).await.unwrap();
    assert!(
        !tables.is_empty(),
        "fixture must have released metadata migrations"
    );
    for table in tables {
        let table = format!("\"{}\"", table.replace('"', "\"\""));
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "CREATE TABLE {schema}.{table} (LIKE public.{table} INCLUDING ALL)"
        )))
        .execute(&admin)
        .await
        .unwrap();
    }
    let work_pool = schema_pool(&url, schema.clone()).await;
    let work = PostgresIndexStore::new(work_pool.clone());
    work.initialize_remote_delete_guard().await.unwrap();
    let namespace = work
        .remote_delete_receipt_namespace()
        .await
        .unwrap()
        .unwrap();
    // Row-scoped task history cannot be shared. Delete receipts nevertheless
    // remain scoped to their exact GC attempt, without enumerating user rows.
    sqlx::query("ALTER TABLE shardline_file_records ENABLE ROW LEVEL SECURITY")
        .execute(&work_pool)
        .await
        .unwrap();
    assert_eq!(
        work.remote_delete_receipt_namespace().await.unwrap(),
        Some(namespace.clone())
    );
    let key = SigningKey::from_bytes(&[39; 32]);
    let object = ObjectKey::parse("reply-recovery/known-success").unwrap();
    let permit = work
        .begin_remote_delete_with_receipt(&object, *key.verifying_key().as_bytes())
        .await
        .unwrap();
    let receipt = permit.sign_acknowledged_receipt(&key).unwrap();
    let unknown = ObjectKey::parse("reply-recovery/missing-reply").unwrap();
    let unknown_permit = work.begin_remote_delete(&unknown).await.unwrap();
    let directory = tempfile::tempdir().unwrap();
    let objects = ServerObjectStore::local(directory.path().join("chunks")).unwrap();
    // Recovery may only consume reply evidence. Even a user object recreated
    // outside a covered write path must never be deleted by reconciliation.
    let known_payload = b"user bytes present during saved-reply recovery";
    let unknown_payload = b"user bytes protected by an unresolved attempt";
    write(&objects, &object, known_payload);
    write(&objects, &unknown, unknown_payload);
    let receipt_key = save(&objects, &receipt);
    // Export only public signed fixture evidence when a native S3 verifier
    // explicitly requests it. Preserve the exact bytes used by its hashed key.
    if let Ok(path) = std::env::var("SHARDLINE_TEST_DELETE_REPLY_EXPORT") {
        std::fs::write(
            path,
            serde_json::to_vec(&serde_json::json!({
                "key": receipt_key.as_str(),
                "payload": String::from_utf8(serde_json::to_vec(&receipt).unwrap()).unwrap(),
            }))
            .unwrap(),
        )
        .unwrap();
    }
    // End the source session before recovery; no private key or source process
    // is needed to settle the persisted observation.
    work_pool.close().await;
    drop(key);
    let replay_pool = schema_pool(&url, schema.clone()).await;
    let replay = PostgresIndexStore::new(replay_pool.clone());
    let mut poison_keys = Vec::new();
    for index in 0..64 {
        let poison = ObjectKey::parse(&format!(
            "_shardline_tasks/v1/delete-replies/{namespace}/replies/{}-{index:03}.json",
            "0".repeat(64)
        ))
        .unwrap();
        assert!(poison.as_str() < receipt_key.as_str());
        write(&objects, &poison, b"invalid evidence");
        poison_keys.push(poison);
    }
    // A payload copy must not export a verified saved reply into another
    // deployment. Invalid or misplaced evidence retains old copy semantics.
    let namespace_alias = if namespace == "00".repeat(32) {
        "11".repeat(32)
    } else {
        "00".repeat(32)
    };
    let receipt_bytes = serde_json::to_vec(&receipt).unwrap();
    let misplaced = ObjectKey::parse(&format!(
        "_shardline_tasks/v1/delete-replies/{namespace_alias}/replies/{}.json",
        blake3::hash(&receipt_bytes).to_hex()
    ))
    .unwrap();
    write(&objects, &misplaced, &receipt_bytes);
    let destination = tempfile::tempdir().unwrap();
    let options = StorageMigrationOptions::new(
        StorageMigrationEndpoint::LocalStateRoot(directory.path().to_path_buf()),
        StorageMigrationEndpoint::LocalStateRoot(destination.path().to_path_buf()),
    );
    let inventory = run_storage_migration(&options.clone().with_dry_run(true)).unwrap();
    assert_eq!(inventory.scanned_objects, 67);
    assert_eq!(inventory.inserted_objects, 0);
    let copied = run_storage_migration(&options).unwrap();
    assert_eq!(copied.scanned_objects, 67);
    assert_eq!(copied.inserted_objects, 67);
    let copied_objects = ServerObjectStore::local(destination.path().join("chunks")).unwrap();
    assert!(!copied_objects.contains(&receipt_key).unwrap());
    assert!(objects.contains(&receipt_key).unwrap());
    assert_eq!(read(&copied_objects, &object), known_payload);
    assert_eq!(read(&copied_objects, &unknown), unknown_payload);
    assert_eq!(read(&copied_objects, &misplaced), receipt_bytes);
    assert_eq!(
        read(&copied_objects, poison_keys.first().unwrap()),
        b"invalid evidence"
    );
    assert!(
        replay
            .begin_remote_delete(&object)
            .await
            .unwrap_err()
            .is_remote_delete_pending()
    );
    let first = recover_saved_remote_delete_replies(&replay, &objects)
        .await
        .unwrap();
    assert_eq!((first.examined, first.settled, first.retained), (64, 0, 64));
    assert!(
        replay
            .begin_remote_delete(&object)
            .await
            .unwrap_err()
            .is_remote_delete_pending()
    );
    let second = recover_saved_remote_delete_replies(&replay, &objects)
        .await
        .unwrap();
    assert_eq!(
        (second.examined, second.settled, second.retained),
        (1, 1, 0)
    );
    assert!(!objects.contains(&receipt_key).unwrap());
    assert!(
        replay
            .begin_remote_delete(&unknown)
            .await
            .unwrap_err()
            .is_remote_delete_pending()
    );
    assert!(objects.contains(poison_keys.first().unwrap()).unwrap());
    assert_eq!(read(&objects, &object), known_payload);
    assert_eq!(read(&objects, &unknown), unknown_payload);
    assert!(work.finish_remote_delete(&permit).await.is_err());
    for poison in poison_keys {
        objects.delete_if_present(&poison).unwrap();
    }
    // A source error can be returned even when the independent recorder settled
    // the marker. Such leftover evidence must retire without another DELETE.
    save(&objects, &receipt);
    let retired = recover_saved_remote_delete_replies(&replay, &objects)
        .await
        .unwrap();
    assert_eq!(
        (retired.examined, retired.settled, retired.retired),
        (1, 0, 1)
    );
    assert!(!objects.contains(&receipt_key).unwrap());
    assert_eq!(read(&objects, &object), known_payload);
    assert_eq!(read(&objects, &unknown), unknown_payload);
    assert!(
        replay
            .begin_remote_delete(&unknown)
            .await
            .unwrap_err()
            .is_remote_delete_pending()
    );
    assert!(replay.finish_remote_delete(&unknown_permit).await.unwrap());
    replay_pool.close().await;
    sqlx::query(sqlx::AssertSqlSafe(format!("DROP SCHEMA {schema} CASCADE")))
        .execute(&admin)
        .await
        .unwrap();
    admin.close().await;
}
