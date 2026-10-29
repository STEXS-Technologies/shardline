#![allow(clippy::unwrap_used, clippy::expect_used)]
use crate::local_backend::LocalBackend;
use axum::body::Bytes;
use sha2::Digest;
use shardline_index::{UploadIntent, UploadIntentState, UploadIntentStore};
use shardline_protocol::{RepositoryProvider, RepositoryScope};
use std::num::NonZeroUsize;

#[tokio::test]
async fn historical_server_preserves_new_file_evidence_and_recovers_legacy_files() {
    let Some(binary) =
        crate::test_support::released_startup::binary("SHARDLINE_LEGACY_RECOVERY_BINARY")
    else {
        return;
    };
    verify_file_recovery(binary, true).await;
}

#[tokio::test]
async fn released_server_recovers_verified_file_checkpoints_without_inventing_completion() {
    let Some(binary) =
        crate::test_support::released_startup::binary("SHARDLINE_N_MINUS_ONE_BINARY")
    else {
        return;
    };
    verify_file_recovery(binary, false).await;
}

async fn verify_file_recovery(binary: String, historical: bool) {
    let root = tempfile::tempdir().unwrap();
    let backend = LocalBackend::new(
        root.path().to_path_buf(),
        "http://localhost:8080".to_owned(),
        NonZeroUsize::new(128).unwrap(),
    )
    .await
    .unwrap();
    let scope = RepositoryScope::new(RepositoryProvider::GitHub, "team", "assets", None).unwrap();
    for (scope, bytes) in [
        (Some(&scope), b"scoped rollback bytes".as_slice()),
        (None, b"default rollback bytes".as_slice()),
    ] {
        backend
            .upload_file("rollback-file", Bytes::copy_from_slice(bytes), scope)
            .await
            .unwrap();
    }
    let mut checkpoints = Vec::new();
    let connection = rusqlite::Connection::open(root.path().join("metadata.sqlite3")).unwrap();
    for (scope, bytes) in [
        (Some(&scope), b"scoped rollback bytes".as_slice()),
        (None, b"default rollback bytes".as_slice()),
    ] {
        let repository =
            crate::upload_intent_recovery::file_intent_repository_scope(scope).unwrap();
        for (ordinal, checkpoint) in [
            UploadIntentState::Created,
            UploadIntentState::Storing,
            UploadIntentState::Stored,
            UploadIntentState::MetadataCommitted,
        ]
        .into_iter()
        .enumerate()
        {
            // Scoped evidence also protects callers using an arbitrary intent ID.
            let id = if scope.is_some() {
                format!("scoped-rollback-{ordinal}")
            } else {
                crate::upload_ingest::upload_attempt_id("rollback-file")
            };
            let intent = UploadIntent::new(
                id,
                "record/rollback-file".to_owned(),
                hex::encode(sha2::Sha256::digest(bytes)),
                0,
            );
            backend
                .index_store
                .create_intent_scoped(&intent, "shardline", &repository)
                .await
                .unwrap();
            for state in [
                UploadIntentState::Storing,
                UploadIntentState::Stored,
                UploadIntentState::MetadataCommitted,
            ]
            .into_iter()
            .take(ordinal)
            {
                assert!(
                    backend
                        .index_store
                        .transition_intent(intent.intent_id(), state)
                        .await
                        .unwrap()
                );
            }
            connection.execute("UPDATE shardline_upload_intents SET created_at_unix_seconds=unixepoch()-60 WHERE intent_id=?1", [intent.intent_id()]).unwrap();
            let before = serde_json::to_value(
                backend
                    .index_store
                    .reliability_events(intent.intent_id())
                    .await
                    .unwrap(),
            )
            .unwrap();
            checkpoints.push((intent, checkpoint, before));
        }
    }
    let legacy = UploadIntent::new(
        "legacy-file-rollback".to_owned(),
        "record/rollback-file".to_owned(),
        hex::encode(sha2::Sha256::digest(b"default rollback bytes")),
        0,
    );
    backend.index_store.create_intent(&legacy).await.unwrap();
    connection.execute("UPDATE shardline_upload_intents SET created_at_unix_seconds=unixepoch()-60 WHERE intent_id=?1", [legacy.intent_id()]).unwrap();
    drop(connection);
    let logs =
        crate::test_support::released_startup::run(binary.into(), root.path().to_path_buf(), None)
            .await;
    for (intent, checkpoint, before) in &checkpoints {
        assert!(
            logs.contains(intent.intent_id()),
            "released recovery skipped {}: {logs}",
            intent.intent_id()
        );
        let recovered = backend
            .index_store
            .intent_by_id(intent.intent_id())
            .await
            .unwrap()
            .unwrap()
            .state();
        if historical
            || !matches!(
                checkpoint,
                UploadIntentState::Stored | UploadIntentState::MetadataCommitted
            )
        {
            assert_eq!(recovered, *checkpoint);
        } else {
            // Readiness can precede bounded background file verification.
            // Killing the released process can also stop between its two
            // durable recovery transitions. The chain/prefix checks below
            // still authenticate this intermediate checkpoint, and current
            // recovery must finish it after the released process stops.
            assert!(
                recovered == *checkpoint
                    || recovered == UploadIntentState::Visible
                    || (*checkpoint == UploadIntentState::Stored
                        && recovered == UploadIntentState::MetadataCommitted),
                "released recovery changed checkpoint {checkpoint:?} to {recovered:?}"
            );
        }
        let events = backend
            .index_store
            .reliability_events(intent.intent_id())
            .await
            .unwrap();
        shardline_reliability::verify_lifecycle_chain_ends_at(&events, recovered).unwrap();
        let actual = serde_json::to_value(events).unwrap();
        let original = before.as_array().unwrap();
        assert_eq!(
            actual.as_array().unwrap().get(..original.len()).unwrap(),
            original.as_slice()
        );
        if recovered == *checkpoint {
            assert_eq!(actual, *before);
        }
    }
    assert_eq!(
        backend
            .index_store
            .intent_by_id(legacy.intent_id())
            .await
            .unwrap()
            .unwrap()
            .state(),
        if historical {
            UploadIntentState::Visible
        } else {
            UploadIntentState::Created
        }
    );
    assert_eq!(
        backend
            .index_store
            .reliability_events(legacy.intent_id())
            .await
            .unwrap()
            .len(),
        if historical { 5 } else { 1 }
    );
    backend.reconcile_stuck_upload_intents().await.unwrap();
    for (intent, checkpoint, _) in checkpoints {
        let expected = if matches!(
            checkpoint,
            UploadIntentState::Stored | UploadIntentState::MetadataCommitted
        ) {
            UploadIntentState::Visible
        } else {
            checkpoint
        };
        assert_eq!(
            backend
                .index_store
                .intent_by_id(intent.intent_id())
                .await
                .unwrap()
                .unwrap()
                .state(),
            expected
        );
    }
    let connection = rusqlite::Connection::open(root.path().join("metadata.sqlite3")).unwrap();
    assert_eq!(
        connection
            .query_row(
                "SELECT COUNT(*) FROM shardline_upload_writers_v2",
                [],
                |row| row.get::<_, i64>(0)
            )
            .unwrap(),
        0
    );
    assert_eq!(
        backend
            .read_record("rollback-file", None, Some(&scope))
            .await
            .unwrap()
            .total_bytes,
        21
    );
    assert_eq!(
        backend
            .read_record("rollback-file", None, None)
            .await
            .unwrap()
            .total_bytes,
        22
    );
}
