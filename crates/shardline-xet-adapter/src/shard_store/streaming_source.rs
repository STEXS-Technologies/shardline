//! Authenticate remote sources directly from one bounded transport stream.
use super::*;
use crate::xorb::{ValidatedXorb, streaming::StreamingXorb};
use shardline_protocol::{ByteRange, ShardlineHash};
use shardline_storage::S3ObjectStore;
#[cfg(test)]
mod tests;

pub(super) fn validate(
    store: &S3ObjectStore,
    key: &ObjectKey,
    expected: ShardlineHash,
    checkpoint: &(dyn Fn() -> std::io::Result<()> + Sync),
    visitor: &mut (dyn FnMut(u64, &[u8]) -> std::io::Result<()> + Send),
) -> Result<ValidatedXorb, XetAdapterError> {
    checkpoint()?;
    let mut stream = StreamingXorb::new(checkpoint, visitor);
    let copied = store.copy_object_to_writer_if_present(key, &mut stream);
    if let Some(error) = stream.take_failure() {
        return Err(error);
    }
    checkpoint()?;
    let length = copied
        .map_err(shardline_server_core::ServerObjectStoreError::from)?
        .ok_or(XetAdapterError::MissingReferencedXorb)?;
    stream.finish_full(expected, length)
}

pub(super) fn visit_range(
    store: &S3ObjectStore,
    key: &ObjectKey,
    members: &[ValidatedXorbChunk],
    checkpoint: &(dyn Fn() -> std::io::Result<()> + Sync),
    visitor: &mut (dyn FnMut(u64, &[u8]) -> std::io::Result<()> + Send),
) -> Result<(), XetAdapterError> {
    checkpoint()?;
    let first = members
        .first()
        .ok_or(XetAdapterError::InvalidSerializedXorb)?;
    let last = members
        .last()
        .ok_or(XetAdapterError::InvalidSerializedXorb)?;
    let end = last
        .packed_end()
        .checked_sub(1)
        .ok_or(XetAdapterError::Overflow)?;
    let range = ByteRange::new(first.packed_start(), end)
        .map_err(|_error| XetAdapterError::InvalidSerializedXorb)?;
    let mut stream = StreamingXorb::new(checkpoint, visitor);
    let copied = store.copy_range_to_writer(key, range, &mut stream);
    if let Some(error) = stream.take_failure() {
        return Err(error);
    }
    checkpoint()?;
    copied.map_err(shardline_server_core::ServerObjectStoreError::from)?;
    stream.finish_range(members)
}
