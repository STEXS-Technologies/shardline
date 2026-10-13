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
            if recovery.retained > 0 {
                tracing::warn!(
                    retained = recovery.retained,
                    "some saved delete replies could not settle their attempts; inspect the original GC and storage logs and pending deletes before retrying affected writes"
                );
            }
            if recovery.examined == 64 {
                tracing::info!(
                    "saved delete reply recovery reached its per-run limit; repeat repair with the same configuration to check later replies, then inspect pending deletes"
                );
            }
        }
        Ok(_) => {}
        Err(error) => {
            tracing::warn!(%error, "saved reply recovery unavailable; unresolved objects remain protected");
        }
    }
}
