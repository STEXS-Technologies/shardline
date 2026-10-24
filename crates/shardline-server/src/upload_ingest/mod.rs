mod body_reader;
pub mod cdc;
mod chunk_store;
mod durable_chunks;
mod ingestor;
mod native_shard;
mod native_xorb;
mod packed_protocol;
mod upload_attempt;
pub mod xorb_packer;

pub(super) use body_reader::{
    RequestBodyReader, StagedRequestBody, read_body_to_bytes, stage_body_for_object_store,
    stage_body_to_tempfile, stage_body_with_spill,
};
pub(crate) use ingestor::FileUploadIngestor;
pub(crate) use native_shard::prepare_native_shard;
pub(crate) use native_xorb::store_admitted_xorb;
pub(crate) use upload_attempt::upload_attempt_id;

/// Public chunk responses require standalone objects; protocol responses do not.
#[derive(Clone, Copy)]
pub(crate) enum UploadStorage {
    IndividualChunks,
    ProtocolContainers,
}
