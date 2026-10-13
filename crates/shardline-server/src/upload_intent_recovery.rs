//! Recovery verifies publication identity without inventing completion.

use crate::{ServerError, admission::BoundedPool, object_store::ServerObjectStore};
use futures_util::StreamExt;
use sha2::Digest;
use shardline_index::{FileRecord, UploadIntent, UploadIntentState};
use shardline_protocol::RepositoryScope;
use shardline_reliability::LifecycleEvent;

const FILE_SCOPE_PREFIX: &str = "scope-json-v1:";

pub(crate) enum FileRecoveryScope {
    Known(Option<RepositoryScope>),
    Unresolved,
}

/// New file evidence preserves exact components, including absent revisions.
/// Existing opaque repository labels remain untouched.
pub(crate) fn file_intent_repository_scope(
    scope: Option<&RepositoryScope>,
) -> Result<String, ServerError> {
    scope.map_or_else(
        || Ok("default".to_owned()),
        |scope| {
            Ok(format!(
                "{FILE_SCOPE_PREFIX}{}",
                serde_json::to_string(scope)?
            ))
        },
    )
}

pub(crate) fn file_recovery_scope(events: &[LifecycleEvent]) -> FileRecoveryScope {
    let Some(event) = events.last() else {
        return FileRecoveryScope::Unresolved;
    };
    if event.operation.repository == "default" {
        return FileRecoveryScope::Known(None);
    }
    event
        .operation
        .repository
        .strip_prefix(FILE_SCOPE_PREFIX)
        .and_then(|value| serde_json::from_str::<RepositoryScope>(value).ok())
        .map_or(FileRecoveryScope::Unresolved, |scope| {
            FileRecoveryScope::Known(Some(scope))
        })
}

/// File publication follows Stored. Earlier progress cannot prove its own
/// publication merely because some other upload made the filename visible.
pub(crate) const fn file_publication_may_have_completed(intent: &UploadIntent) -> bool {
    matches!(
        intent.state(),
        UploadIntentState::Stored | UploadIntentState::MetadataCommitted
    )
}

/// Verify the candidate's bytes before recovering a logical file intent.
/// Opaque legacy repository identities cannot safely identify a scoped record.
pub(crate) async fn file_record_matches_intent(
    intent: &UploadIntent,
    events: &[LifecycleEvent],
    record: FileRecord,
    objects: ServerObjectStore,
    pool: BoundedPool,
) -> Result<bool, ServerError> {
    let FileRecoveryScope::Known(scope) = file_recovery_scope(events) else {
        return Ok(false);
    };
    if record.repository_scope != scope
        || intent.object_key().strip_prefix("record/") != Some(record.file_id.as_str())
        || (intent.object_length() != 0 && intent.object_length() != record.total_bytes)
        || intent.object_hash().len() != 64
        || !intent
            .object_hash()
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Ok(false);
    }
    let expected_length = record.total_bytes;
    let mut stream =
        crate::download_stream::file_record_byte_stream(objects, record, None, pool).await?;
    let mut digest = sha2::Sha256::new();
    let mut length = 0_u64;
    while let Some(bytes) = stream.next().await {
        let bytes = bytes?;
        length = crate::overflow::checked_add(length, u64::try_from(bytes.len())?)?;
        digest.update(&bytes);
    }
    Ok(length == expected_length && hex::encode(digest.finalize()) == intent.object_hash())
}

/// Missing original request bytes cannot establish that a native shard failed.
/// Preserve the in-flight intent so a fresh validated upload can inspect reality.
pub(crate) fn missing_request_key_is_inconclusive(intent: &shardline_index::UploadIntent) -> bool {
    intent.intent_id().starts_with("shard-") && intent.object_key().starts_with("shards/")
}
