//! Optional saved-reply processing within an existing maintenance invocation.

use shardline_index::PostgresIndexStore;

use crate::ServerObjectStore;

pub(crate) async fn recover_saved_replies(index: &PostgresIndexStore, objects: &ServerObjectStore) {
    if !matches!(objects, ServerObjectStore::S3(_)) {
        return;
    }
    match shardline_server_core::remote_delete::recover_saved_remote_delete_replies(index, objects)
        .await
    {
        Ok(recovery) if recovery.examined > 0 => {
            tracing::info!(
                examined = recovery.examined,
                settled = recovery.settled,
                retired = recovery.retired,
                retained = recovery.retained,
                "processed saved remote delete replies"
            );
        }
        Ok(_) => {}
        Err(error) => {
            tracing::warn!(%error, "saved reply recovery unavailable; unresolved objects remain protected");
        }
    }
}
