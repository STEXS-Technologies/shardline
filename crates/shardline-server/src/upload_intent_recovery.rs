//! Native shard storage can use a normalized key distinct from the request hash.

/// Missing original request bytes cannot establish that a native shard failed.
/// Preserve the in-flight intent so a fresh validated upload can inspect reality.
pub(crate) fn missing_request_key_is_inconclusive(intent: &shardline_index::UploadIntent) -> bool {
    intent.intent_id().starts_with("shard-") && intent.object_key().starts_with("shards/")
}
