#![allow(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing)]

use shardline_reliability::upload_lifecycle_event;

use super::*;
use crate::{UploadIntent, UploadIntentStore};

fn completion(intent: &UploadIntent) -> Vec<(UploadIntentState, LifecycleEvent)> {
    let mut previous = UploadIntentState::Storing;
    [
        UploadIntentState::Stored,
        UploadIntentState::MetadataCommitted,
        UploadIntentState::Visible,
    ]
    .into_iter()
    .map(|next| {
        let event = upload_lifecycle_event(
            "shardline",
            "default",
            intent.intent_id(),
            intent.object_key(),
            intent.object_hash(),
            previous,
            next,
        )
        .unwrap();
        previous = next;
        (next, event)
    })
    .collect()
}

fn intent() -> UploadIntent {
    UploadIntent::new(
        "xorb-v2-atomic-completion".to_owned(),
        "xorbs/default/atomic.xorb".to_owned(),
        "a".repeat(64),
        42,
    )
}

#[tokio::test]
async fn completion_preserves_sequential_evidence_and_reopens_cleanly() {
    let roots = [tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap()];
    let intent = intent();
    let transitions = completion(&intent);
    let mut evidence = Vec::new();
    for (index, root) in roots.iter().enumerate() {
        let store = LocalIndexStore::new(root.path().to_owned()).unwrap();
        store.create_intent(&intent).await.unwrap();
        assert!(
            store
                .transition_intent(intent.intent_id(), UploadIntentState::Storing)
                .await
                .unwrap()
        );
        if index == 0 {
            assert!(
                store
                    .transition_intent_batch_with_events(intent.intent_id(), &transitions)
                    .await
                    .unwrap()
            );
        } else {
            for (state, event) in &transitions {
                assert!(
                    store
                        .transition_intent_with_event(intent.intent_id(), *state, event)
                        .await
                        .unwrap()
                );
            }
        }
        store.verify_reliability_events().unwrap();
        evidence.push(store.reliability_events(intent.intent_id()).await.unwrap());
        drop(store);
        let reopened = LocalIndexStore::new(root.path().to_owned()).unwrap();
        assert_eq!(
            reopened
                .intent_by_id(intent.intent_id())
                .await
                .unwrap()
                .unwrap()
                .state(),
            UploadIntentState::Visible
        );
    }
    assert_eq!(evidence[0], evidence[1]);
    assert_eq!(evidence[0].len(), 5);
}

#[tokio::test]
async fn failure_at_each_completion_event_rolls_back_entire_batch() {
    let intent = intent();
    let transitions = completion(&intent);
    for (_, failing_event) in &transitions {
        let root = tempfile::tempdir().unwrap();
        let store = LocalIndexStore::new(root.path().to_owned()).unwrap();
        store.create_intent(&intent).await.unwrap();
        assert!(
            store
                .transition_intent(intent.intent_id(), UploadIntentState::Storing)
                .await
                .unwrap()
        );
        let original = store.reliability_events(intent.intent_id()).await.unwrap();
        let connection = store.open_connection().unwrap();
        connection
            .execute_batch(&format!(
            "CREATE TRIGGER reject_completion_event BEFORE INSERT ON shardline_reliability_events
             WHEN NEW.operation_id = 'xorb-v2-atomic-completion' AND NEW.sequence = {}
             BEGIN SELECT RAISE(ABORT, 'injected completion event failure'); END;",
            failing_event.sequence,
        ))
            .unwrap();
        assert!(
            store
                .transition_intent_batch_with_events(intent.intent_id(), &transitions)
                .await
                .is_err()
        );
        assert_eq!(
            store
                .intent_by_id(intent.intent_id())
                .await
                .unwrap()
                .unwrap()
                .state(),
            UploadIntentState::Storing
        );
        assert_eq!(
            store.reliability_events(intent.intent_id()).await.unwrap(),
            original
        );
        let markers: i64 = connection
            .query_row(
                "SELECT COUNT(*) FROM shardline_upload_writers_v2",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(markers, 0);
        connection
            .execute_batch("DROP TRIGGER reject_completion_event")
            .unwrap();
        assert!(
            store
                .transition_intent_batch_with_events(intent.intent_id(), &transitions)
                .await
                .unwrap()
        );
        assert_eq!(
            store
                .intent_by_id(intent.intent_id())
                .await
                .unwrap()
                .unwrap()
                .state(),
            UploadIntentState::Visible
        );
        store.verify_reliability_events().unwrap();
    }
}
