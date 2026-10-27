#![deny(unsafe_code)]
#![cfg_attr(
    test,
    allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::indexing_slicing,
        clippy::shadow_unrelated,
        clippy::let_underscore_must_use,
        clippy::format_push_string
    )
)]

//! Xet xorb and shard adapter for the Shardline server ecosystem.
//!
//! This crate provides validated xorb parsing, shard metadata normalization,
//! reconstruction response building, and xorb transfer URL construction for
//! the Shardline server.
//!
//! # Quick start
//!
//! The validation helpers are pure — they check hashes and transfer
//! namespaces before anything touches storage:
//!
//! ```
//! use shardline_xet_adapter::{
//!     build_xorb_transfer_url, validate_hash_path, validate_xorb_transfer_namespace,
//! };
//!
//! let hash = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
//!
//! // Hash paths must be 64 lowercase hex characters.
//! assert!(validate_hash_path(hash).is_ok());
//! assert!(validate_hash_path("not-a-hash").is_err());
//!
//! // Transfer URLs use the `default` namespace.
//! assert!(validate_xorb_transfer_namespace("default").is_ok());
//! assert!(validate_xorb_transfer_namespace("other").is_err());
//!
//! let url = build_xorb_transfer_url("http://localhost:8080/", hash);
//! assert_eq!(url, format!("http://localhost:8080/transfer/xorb/default/{hash}"));
//! ```
//!
//! Parsing and validating actual xorb containers starts with
//! [`validate_serialized_xorb`] and [`decode_serialized_xorb_chunks`];
//! reconstruction responses are built with
//! [`build_batch_reconstruction_response`].

mod checkpoint_reader;
mod error;
mod frontend;
mod ingest;
mod model;
mod reconstruction;
mod shard_store;
mod xorb;
mod xorb_store;
mod xorb_visit;

pub use shard_store::{
    PreparedShardUpload, prepare_uploaded_shard, prepare_uploaded_shard_with_checkpoint,
};
pub use shard_store::{
    dedup_xorb_candidates, recover_dedup_xorb_descriptor,
    recover_dedup_xorb_descriptor_from_candidates, recover_xorb_descriptor,
};
pub use xorb_store::read_verified_xorb_chunk;
pub use xorb_store::{
    normalize_owned_uploaded_xorb_file, normalize_owned_uploaded_xorb_file_with_checkpoint,
    normalize_uploaded_xorb_file,
};

pub use error::XetAdapterError;
pub use frontend::{
    XET_METADATA_ACCEPT, XET_PATH_ROUTE, XET_READ_TOKEN_ROUTE, XET_REVISION_ROUTE,
    XET_REVISIONS_ROUTE, XET_TREE_ROUTE, XET_WRITE_TOKEN_ROUTE, XORB_TRANSFER_ROUTE,
    build_xorb_transfer_url, validate_hash_path, validate_optional_content_hash,
    validate_xorb_transfer_namespace,
};
pub use ingest::{
    register_uploaded_shard_bytes, register_uploaded_shard_bytes_with_member_references,
    register_uploaded_shard_bytes_with_verified_descriptors, register_uploaded_shard_file,
    register_uploaded_shard_file_with_member_references,
    register_uploaded_shard_file_with_verified_descriptors, store_uploaded_xorb_bytes,
    store_uploaded_xorb_file_path,
};
pub use model::{
    BatchReconstructionResponse, FileReconstructionResponse, FileReconstructionV2Response,
    ReconstructionChunkRange, ReconstructionFetchInfo, ReconstructionMultiRangeFetch,
    ReconstructionRangeDescriptor, ReconstructionTerm, ReconstructionUrlRange, ShardUploadResponse,
    XorbUploadResponse,
};
pub use reconstruction::{
    build_batch_reconstruction_response, build_reconstruction_response,
    build_reconstruction_response_with_metrics, reconstruction_v2_from_v1,
};
pub use shard_store::{
    dedupe_shard_mapping, parse_uploaded_shard, parse_uploaded_shard_file,
    parse_uploaded_shard_with_metrics, resolve_dedupe_shard_object, retained_shard_chunk_hashes,
    retained_shard_chunk_hashes_from_reader, shard_hash_from_object_key_if_present,
    shard_object_key,
};
pub use xorb::{
    AuthenticatedXorbFooter, DecodedXorbChunk, ValidatedXorb, ValidatedXorbChunk, XorbParseError,
    XorbVisitError, decode_authenticated_xorb_chunk, decode_serialized_xorb_chunks,
    try_for_each_serialized_xorb_chunk, try_for_each_serialized_xorb_chunk_async,
    try_for_each_serialized_xorb_chunk_async_trusted, try_for_each_serialized_xorb_chunk_range,
    try_for_each_serialized_xorb_chunk_trusted, validate_serialized_xorb,
    validate_serialized_xorb_footer, validate_serialized_xorb_with_data,
};
pub use xorb_store::{
    PreparedXorbFile, canonical_uploaded_xorb_file_length, canonical_uploaded_xorb_length,
    normalize_serialized_xorb, prepare_owned_xorb_file_with_checkpoint, store_owned_xorb_file,
    store_prepared_xorb_file, store_uploaded_xorb, store_uploaded_xorb_file,
    store_uploaded_xorb_with_metrics, store_xorb_bytes_with_resources,
    visit_stored_xorb_chunk_hashes, visit_stored_xorb_chunk_hashes_if_present,
    xorb_chunks_cache_hash_from_key_if_present, xorb_hash_from_object_key_if_present,
    xorb_object_key,
};
pub use xorb_visit::map_xorb_visit_error;
