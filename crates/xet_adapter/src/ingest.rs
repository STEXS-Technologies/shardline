use std::future::Future;

use shardline_index::{DedupeShardMapping, FileRecord};
use shardline_protocol::RepositoryScope;
use shardline_server_core::{ServerObjectStore, ShardMetadataLimits};

use crate::{
    error::XetAdapterError,
    model::{ShardUploadResponse, XorbUploadResponse},
};

use super::{dedupe_shard_mapping, parse_uploaded_shard, store_uploaded_xorb};

/// # Errors
///
/// Returns an error when the xorb upload fails validation or storage.
pub fn store_uploaded_xorb_bytes(
    object_store: &ServerObjectStore,
    expected_hash: &str,
    uploaded_body: &[u8],
) -> Result<XorbUploadResponse, XetAdapterError> {
    let stored = store_uploaded_xorb(object_store, expected_hash, uploaded_body)?;

    Ok(XorbUploadResponse {
        was_inserted: stored.was_inserted,
    })
}

/// # Errors
///
/// Returns an error when shard parsing or commit fails.
pub async fn register_uploaded_shard_bytes<Commit, CommitFuture>(
    object_store: &ServerObjectStore,
    uploaded_body: &[u8],
    repository_scope: Option<&RepositoryScope>,
    shard_metadata_limits: ShardMetadataLimits,
    commit_metadata: Commit,
) -> Result<ShardUploadResponse, XetAdapterError>
where
    Commit: FnOnce(Vec<FileRecord>, Vec<DedupeShardMapping>) -> CommitFuture,
    CommitFuture: Future<Output = Result<(), XetAdapterError>>,
{
    let parsed = parse_uploaded_shard(
        object_store,
        uploaded_body,
        repository_scope,
        shard_metadata_limits,
    )?;
    let mappings = parsed
        .dedupe_chunk_hashes
        .iter()
        .map(|chunk_hash_hex| dedupe_shard_mapping(chunk_hash_hex, &parsed.shard_key))
        .collect::<Result<Vec<_>, _>>()?;
    commit_metadata(parsed.records, mappings).await?;

    Ok(ShardUploadResponse {
        result: parsed.result,
    })
}
