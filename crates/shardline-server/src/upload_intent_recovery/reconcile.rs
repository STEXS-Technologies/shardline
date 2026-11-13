//! Shared recovery policy; backend callbacks retain scoped record lookup.

use std::{future::Future, time::Duration};

use shardline_index::{FileRecord, UploadIntentState, UploadIntentStore};
use shardline_protocol::RepositoryScope;
use shardline_reliability::verify_lifecycle_chain_ends_at;

use crate::{ServerError, admission::BoundedPool, object_store::ServerObjectStore};

pub(crate) async fn reconcile<Store, Read, ReadFuture>(
    store: &Store,
    object_store: ServerObjectStore,
    pool: BoundedPool,
    minimum_age: Duration,
    pass: &super::startup::RecoveryPass,
    read_record: Read,
) -> Result<bool, ServerError>
where
    Store: UploadIntentStore,
    Store::Error: std::fmt::Display,
    ServerError: From<Store::Error>,
    Read: Fn(String, Option<RepositoryScope>) -> ReadFuture + Send + Sync,
    ReadFuture: Future<Output = Result<FileRecord, ServerError>> + Send,
{
    use shardline_storage::{AsyncObjectStore, ObjectKey};
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    let mut reconciled = 0u64;
    let mut pending_files = false;
    for state in &[
        UploadIntentState::Created,
        UploadIntentState::Storing,
        UploadIntentState::Stored,
        UploadIntentState::MetadataCommitted,
    ] {
        if pass.stopped() {
            return Ok(pending_files);
        }
        let intents = match store.intents_by_state(*state).await {
            Ok(intents) => intents,
            Err(e) => {
                // If the intents table does not exist yet (no migration has created it),
                // there is nothing to reconcile. Log and continue.
                tracing::debug!("intent query skipped (table may not exist): {e}");
                continue;
            }
        };
        for intent in &intents {
            if pass.stopped() {
                return Ok(pending_files);
            }
            if !pass.visits(intent) {
                continue;
            }
            let reliability_events = store.reliability_events(intent.intent_id()).await?;
            if let Err(error) = verify_lifecycle_chain_ends_at(&reliability_events, intent.state())
            {
                let current = store.intent_by_id(intent.intent_id()).await?;
                if current
                    .as_ref()
                    .is_none_or(|current| current.state() != intent.state())
                {
                    // Another node committed a valid transition after this
                    // recovery scan loaded its row. The next pass must use
                    // the authoritative state rather than rejecting startup
                    // for a stale snapshot.
                    continue;
                }
                return Err(ServerError::Io(std::io::Error::other(error.to_string())));
            }
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or(Duration::ZERO);
            let age = now.saturating_sub(intent.created_at());
            if age < minimum_age {
                continue;
            }
            tracing::warn!(
                intent_id = %intent.intent_id(),
                state = ?intent.state(),
                age_secs = age.as_secs(),
                "reconciling stuck intent"
            );
            let file_intent = intent.object_key().starts_with("record/");
            if file_intent && !super::file_publication_may_have_completed(intent) {
                tracing::warn!(intent_id = %intent.intent_id(), "file upload has no Stored checkpoint; preserving unfinished intent for a fresh validated request");
                continue;
            }
            let visible_file_record = if let Some(file_id) =
                intent.object_key().strip_prefix("record/")
            {
                let super::FileRecoveryScope::Known(scope) =
                    super::file_recovery_scope(&reliability_events)
                else {
                    tracing::warn!(intent_id = %intent.intent_id(), "legacy file scope cannot be verified; preserving unfinished intent");
                    continue;
                };
                match read_record(file_id.to_owned(), scope).await {
                    Ok(record) => {
                        if pass.defers_file_bytes() {
                            pending_files |= super::file_record_has_expected_identity(
                                intent,
                                &reliability_events,
                                &record,
                            );
                            continue;
                        }
                        match super::file_record_matches_intent(
                            intent,
                            &reliability_events,
                            record,
                            object_store.clone(),
                            pool.clone(),
                            pass,
                        )
                        .await
                        {
                            Ok(matches) => matches,
                            Err(error) => {
                                tracing::warn!(intent_id = %intent.intent_id(), %error, "file upload verification unavailable; preserving unfinished intent");
                                false
                            }
                        }
                    }
                    Err(ServerError::NotFound) => false,
                    Err(error) => return Err(error),
                }
            } else {
                false
            };
            if pass.stopped() {
                return Ok(pending_files);
            }
            if visible_file_record {
                // The atomic file-record commit is the visibility boundary.
                // Walk every durable boundary so recovery remains valid even
                // when the process stopped while the intent was still Stored.
                let mut recovery_from = intent.state();
                for recovery_state in [
                    UploadIntentState::Storing,
                    UploadIntentState::Stored,
                    UploadIntentState::MetadataCommitted,
                    UploadIntentState::Visible,
                ] {
                    if pass.stopped() {
                        return Ok(pending_files);
                    }
                    let transitioned = transition_intent_for_recovery(
                        store,
                        intent.intent_id(),
                        recovery_from,
                        recovery_state,
                    )
                    .await;
                    match transitioned {
                        Ok(true) => recovery_from = recovery_state,
                        Ok(false) => {}
                        Err(error) => {
                            tracing::warn!(
                                intent_id = %intent.intent_id(),
                                %error,
                                "intent recovery transition failed"
                            );
                            return Err(error);
                        }
                    }
                }
                reconciled = reconciled.saturating_add(1);
                continue;
            }
            if file_intent {
                // This is a logical record key, not a physical CAS object.
                // Missing or unrelated latest metadata cannot settle it.
                tracing::warn!(intent_id = %intent.intent_id(), "file upload outcome cannot be verified; preserving unfinished intent for a fresh validated request");
                continue;
            }
            let target_state = match ObjectKey::parse(intent.object_key()) {
                Ok(key) => match AsyncObjectStore::metadata(&object_store, &key).await? {
                    Some(metadata)
                        if metadata.length() != intent.object_length()
                            && !super::native_xorb_has_equivalent_encoding(
                                intent,
                                object_store.clone(),
                                &pool,
                            )
                            .await? =>
                    {
                        Some(UploadIntentState::Failed)
                    }
                    Some(_) if intent.state() == UploadIntentState::MetadataCommitted => {
                        Some(UploadIntentState::Visible)
                    }
                    Some(_) => {
                        // The immutable write may have committed before its
                        // response was lost. Earlier states do not prove that
                        // protocol metadata is visible (notably for shards),
                        // so preserve the retryable state instead of guessing.
                        tracing::warn!(
                            intent_id = %intent.intent_id(),
                            state = ?intent.state(),
                            "durable object found for in-flight intent; preserving for retry"
                        );
                        None
                    }
                    None if super::missing_request_key_is_inconclusive(intent) => {
                        tracing::warn!(intent_id = %intent.intent_id(), "native shard request key is absent; normalized storage may use another key, preserving for validated retry");
                        None
                    }
                    None => Some(UploadIntentState::Failed),
                },
                Err(_) => Some(UploadIntentState::Failed),
            };
            let Some(target_state) = target_state else {
                continue;
            };
            match transition_intent_for_recovery(
                store,
                intent.intent_id(),
                intent.state(),
                target_state,
            )
            .await
            {
                Ok(_) => reconciled = reconciled.saturating_add(1),
                Err(error) => tracing::warn!(
                    intent_id = %intent.intent_id(),
                    error = %error,
                    "intent transition failed, skipping"
                ),
            }
        }
    }
    if reconciled > 0 {
        tracing::info!(count = reconciled, "intent reconciliation complete");
    }
    Ok(pending_files)
}

async fn transition_intent_for_recovery<Store: UploadIntentStore>(
    store: &Store,
    intent_id: &str,
    before: shardline_index::UploadIntentState,
    after: shardline_index::UploadIntentState,
) -> Result<bool, ServerError>
where
    ServerError: From<Store::Error>,
{
    if !before.can_transition_to(after) {
        return Ok(false);
    }
    Ok(shardline_index::UploadIntentStore::transition_intent(store, intent_id, after).await?)
}
