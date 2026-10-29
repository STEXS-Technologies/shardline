//! Shared upload processing for all frontends.
//!
//! Raw file protocols use `FileUploadIngestor` for streaming CDC, verification
//! and bounded packing. Native Xet inputs use the format-specific Xorb/shard
//! preparers here, sharing body admission and immutable storage machinery.
//! Frontends own wire negotiation and protocol responses; backends own fenced
//! metadata publication. Preparation alone never makes a file visible.

mod body_reader;
mod buffer_admission;
pub mod cdc;
mod chunk_store;
mod durable_chunks;
mod ingestor;
mod native_body;
mod native_shard;
mod native_xorb;
mod packed_protocol;
mod upload_attempt;
pub mod xorb_packer;

pub(super) use body_reader::{
    RequestBodyReader, StagedRequestBody, read_body_to_bytes, stage_bounded_body,
};
pub(crate) use ingestor::FileUploadIngestor;
pub(crate) use native_body::{NativeBody, prepare_native_xorb, read_native_body};
pub(crate) use native_shard::prepare_native_shard;
pub(crate) use native_xorb::store_admitted_xorb;
pub(crate) use upload_attempt::upload_attempt_id;

/// Public chunk responses require standalone objects; protocol responses do not.
#[derive(Clone, Copy)]
pub(crate) enum UploadStorage {
    IndividualChunks,
    ProtocolContainers,
}
