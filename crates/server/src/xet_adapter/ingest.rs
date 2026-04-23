use std::future::Future;

use shardline_index::{DedupeShardMapping, FileRecord};
use shardline_protocol::RepositoryScope;

use crate::{
    ServerError, ShardMetadataLimits,
    model::{ShardUploadResponse, XorbUploadResponse},
    object_store::ServerObjectStore,
    upload_ingest::{RequestBodyReader, read_body_to_bytes},
};

use super::{dedupe_shard_mapping, parse_uploaded_shard, store_uploaded_xorb};

pub(crate) async fn store_uploaded_xorb_stream(
    object_store: &ServerObjectStore,
    expected_hash: &str,
    mut body: RequestBodyReader,
) -> Result<XorbUploadResponse, ServerError> {
    let uploaded_body = read_body_to_bytes(&mut body).await?;
    let stored = store_uploaded_xorb(object_store, expected_hash, &uploaded_body)?;

    Ok(XorbUploadResponse {
        was_inserted: stored.was_inserted,
    })
}

pub(crate) async fn register_uploaded_shard_stream<Commit, CommitFuture>(
    object_store: &ServerObjectStore,
    mut body: RequestBodyReader,
    repository_scope: Option<&RepositoryScope>,
    shard_metadata_limits: ShardMetadataLimits,
    commit_metadata: Commit,
) -> Result<ShardUploadResponse, ServerError>
where
    Commit: FnOnce(Vec<FileRecord>, Vec<DedupeShardMapping>) -> CommitFuture,
    CommitFuture: Future<Output = Result<(), ServerError>>,
{
    let uploaded_body = read_body_to_bytes(&mut body).await?;
    let parsed = parse_uploaded_shard(
        object_store,
        &uploaded_body,
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
