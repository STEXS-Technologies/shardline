//! Retain bounded raw data until a packed container has been verified in storage.
use std::borrow::Cow;

use shardline_xet_core::xorb_object::{CompressionScheme, RawXorbData, SerializedXorbObject};

use super::{PackedXorb, packed_xorb_from_serialized};
use crate::ServerError;

pub(in crate::upload_ingest) struct RecoverableXorbBatch {
    raw: RawXorbData,
    offsets: Vec<u64>,
}

impl RecoverableXorbBatch {
    pub(in crate::upload_ingest) fn new(chunks: Vec<(Vec<u8>, u64)>) -> Self {
        let offsets: Vec<_> = chunks.iter().map(|(_, offset)| *offset).collect();
        let boundaries = offsets.iter().map(|offset| *offset as usize).collect();
        let data = chunks
            .into_iter()
            .map(|(data, _)| Cow::Owned(data))
            .collect();
        Self {
            raw: RawXorbData::from_owned_data(data, boundaries),
            offsets,
        }
    }

    pub(in crate::upload_ingest) fn pack(&self) -> Result<PackedXorb, ServerError> {
        let serialized = SerializedXorbObject::from_xorb_ref_with_compression(
            &self.raw,
            CompressionScheme::ByteGrouping4LZ4,
            true,
        )
        .map_err(|error| {
            ServerError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("xorb serialization failed: {error}"),
            ))
        })?;
        let metadata: Vec<_> = self
            .raw
            .data
            .iter()
            .zip(&self.offsets)
            .map(|(data, offset)| (data.len(), *offset))
            .collect();
        packed_xorb_from_serialized(serialized, &metadata)
    }

    pub(in crate::upload_ingest) fn into_chunks(self) -> Vec<(Vec<u8>, u64)> {
        self.raw
            .data
            .into_iter()
            .zip(self.offsets)
            .map(|(data, offset)| (data.into_owned(), offset))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn packing_retains_original_raw_allocations_for_fallback() {
        let bytes = b"recoverable bounded payload".to_vec();
        let address = bytes.as_ptr();
        let batch = RecoverableXorbBatch::new(vec![(bytes, 0)]);
        let packed = batch.pack().unwrap();
        let recovered = batch.into_chunks();
        assert_eq!(recovered[0].0.as_ptr(), address);
        assert_eq!(recovered[0].0, b"recoverable bounded payload");
        let expected = super::super::pack_chunks_into_xorb(&recovered).unwrap();
        assert_eq!(packed.serialized, expected.serialized);
        assert_eq!(packed.xorb_hash_hex, expected.xorb_hash_hex);
        assert_eq!(packed.chunk_entries, expected.chunk_entries);
    }
}
