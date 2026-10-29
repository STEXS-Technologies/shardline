pub use shardline_xet_adapter::{
    BatchReconstructionResponse, FileReconstructionResponse, FileReconstructionV2Response,
    XorbUploadResponse, decode_serialized_xorb_chunks, try_for_each_serialized_xorb_chunk,
    try_for_each_serialized_xorb_chunk_async_trusted, try_for_each_serialized_xorb_chunk_trusted,
    validate_serialized_xorb,
};

#[cfg(test)]
pub(crate) use shardline_xet_adapter::{
    ReconstructionChunkRange, ReconstructionFetchInfo, ReconstructionTerm, ReconstructionUrlRange,
    register_uploaded_shard_bytes_with_member_references, shard_object_key, store_uploaded_xorb,
    store_uploaded_xorb_bytes,
};

pub(crate) use shardline_xet_adapter::{
    ShardUploadResponse, XET_METADATA_ACCEPT, XET_PATH_ROUTE, XET_READ_TOKEN_ROUTE,
    XET_REVISION_ROUTE, XET_REVISIONS_ROUTE, XET_TREE_ROUTE, XET_WRITE_TOKEN_ROUTE,
    XORB_TRANSFER_ROUTE, XetAdapterError, XorbParseError, XorbVisitError,
    build_batch_reconstruction_response, build_reconstruction_response, reconstruction_v2_from_v1,
    resolve_dedupe_shard_object, shard_hash_from_object_key_if_present,
    try_for_each_serialized_xorb_chunk_range, validate_hash_path, validate_optional_content_hash,
    validate_xorb_transfer_namespace, visit_stored_xorb_chunk_hashes,
    xorb_hash_from_object_key_if_present, xorb_object_key,
};

#[cfg(feature = "fuzzing")]
pub(crate) use shardline_xet_adapter::{
    build_xorb_transfer_url, normalize_serialized_xorb, retained_shard_chunk_hashes,
};
