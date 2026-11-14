//! Storage inventory policy shared by both metadata backends.

use shardline_index::RecordTraversal;
use shardline_storage::ObjectPrefix;

use crate::{
    ServerError,
    chunk_store::chunk_hash_from_chunk_object_key_if_present,
    model::ServerStatsResponse,
    object_store::{ServerObjectStore, visit_object_prefix},
    overflow::{checked_add, checked_increment},
};

pub(crate) async fn storage_stats<Records>(
    objects: &ServerObjectStore,
    records: &Records,
) -> Result<ServerStatsResponse, ServerError>
where
    Records: RecordTraversal + Sync,
    ServerError: From<Records::Error>,
{
    let prefix = ObjectPrefix::parse("").map_err(|_error| ServerError::InvalidContentHash)?;
    let mut stats = ServerStatsResponse {
        objects: 0,
        object_bytes: 0,
        chunks: 0,
        chunk_bytes: 0,
        files: 0,
    };
    visit_object_prefix(objects, &prefix, |metadata| {
        stats.objects = checked_increment(stats.objects)?;
        stats.object_bytes = checked_add(stats.object_bytes, metadata.length())?;
        if chunk_hash_from_chunk_object_key_if_present(metadata.key())?.is_some() {
            stats.chunks = checked_increment(stats.chunks)?;
            stats.chunk_bytes = checked_add(stats.chunk_bytes, metadata.length())?;
        }
        Ok(())
    })?;
    // PostgreSQL pages through authoritative latest aliases. The local adapter
    // uses its existing locator listing; immutable versions are never file counts.
    records
        .visit_latest_record_locators(|_locator| {
            stats.files = checked_increment(stats.files)?;
            Ok::<(), ServerError>(())
        })
        .await?;
    Ok(stats)
}

#[cfg(test)]
mod tests;
