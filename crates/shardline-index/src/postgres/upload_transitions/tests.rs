#![allow(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing)]

use super::*;
use crate::{UploadIntent, UploadIntentStore};
use shardline_reliability::upload_lifecycle_event;

#[tokio::test]
async fn postgres_completion_rolls_back_late_failure_and_retains_all_events() {
    let Some(pool) = crate::postgres::connect_isolated_postgres().await else {
        return;
    };
    let store = PostgresIndexStore::new(pool.clone());
    let intent = UploadIntent::new(
        "xorb-v2-atomic-completion".to_owned(),
        "xorbs/default/atomic.xorb".to_owned(),
        "a".repeat(64),
        42,
    );
    store.create_intent(&intent).await.unwrap();
    assert!(
        store
            .transition_intent(intent.intent_id(), UploadIntentState::Storing)
            .await
            .unwrap()
    );
    let original = store.reliability_events(intent.intent_id()).await.unwrap();
    let mut previous = UploadIntentState::Storing;
    let transitions: Vec<_> = [
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
    .collect();
    sqlx::query("UPDATE shardline_upload_intents SET object_length = -1 WHERE intent_id = $1")
        .bind(intent.intent_id())
        .execute(&pool)
        .await
        .unwrap();
    assert!(matches!(
        store
            .transition_intent_batch_with_events(intent.intent_id(), &transitions)
            .await,
        Err(PostgresMetadataStoreError::IntegerOutOfRange(_))
    ));
    sqlx::query("UPDATE shardline_upload_intents SET object_length = 42 WHERE intent_id = $1")
        .bind(intent.intent_id())
        .execute(&pool)
        .await
        .unwrap();
    sqlx::raw_sql(
        "CREATE FUNCTION reject_completion_event() RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN IF NEW.operation_id = 'xorb-v2-atomic-completion' AND NEW.sequence = 4 THEN
        RAISE EXCEPTION 'injected completion failure'; END IF; RETURN NEW; END $$;
        CREATE TRIGGER reject_completion_event BEFORE INSERT ON shardline_reliability_events
        FOR EACH ROW EXECUTE FUNCTION reject_completion_event();",
    )
    .execute(&pool)
    .await
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
    sqlx::query("DROP TRIGGER reject_completion_event ON shardline_reliability_events")
        .execute(&pool)
        .await
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
    let actual = store.reliability_events(intent.intent_id()).await.unwrap();
    let mut expected = original;
    expected.extend(transitions.into_iter().map(|(_, event)| event));
    assert_eq!(actual, expected);
    pool.close().await;
}
