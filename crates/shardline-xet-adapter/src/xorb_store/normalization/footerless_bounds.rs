//! Reject over-limit or incomplete footerless input before decompression.

use std::io::{Read, Seek, SeekFrom};

use shardline_xet_core::xorb_object::{XORB_CHUNK_HEADER_LENGTH, deserialize_chunk_header};

use crate::XetAdapterError;

pub(super) fn check(
    input: &mut (impl Read + Seek),
    length: u64,
    checkpoint: &dyn Fn() -> std::io::Result<()>,
) -> Result<(), XetAdapterError> {
    // Canonical inputs retain their wider acceptance. These limits apply only
    // to footerless normalization, before any payload decompression.
    const MAX_WIRE_BYTES: u64 = 64 * 1024 * 1024 + 8192 * 8;
    const MAX_DECODED_BYTES: u64 = 64 * 1024 * 1024;
    if length > MAX_WIRE_BYTES {
        tracing::debug!(
            length,
            maximum = MAX_WIRE_BYTES,
            "footerless Xorb wire size exceeded normalization limit"
        );
        return Err(XetAdapterError::InvalidSerializedXorb);
    }
    input.seek(SeekFrom::Start(0))?;
    let mut position = 0_u64;
    let mut decoded = 0_u64;
    let mut count = 0_usize;
    while position < length {
        checkpoint()?;
        // Every accepted header consumes exactly the protocol header length.
        // Track that position instead of querying the file twice per member.
        // Trailing footer bytes must still parse as complete bounded chunks;
        // never authenticate a valid prefix while discarding an invalid tail.
        let header = deserialize_chunk_header(input).map_err(super::normalization_error)?;
        count = count.checked_add(1).ok_or(XetAdapterError::Overflow)?;
        if count > shardline_index::MAX_XET_DESCRIPTOR_CHUNKS {
            return Err(XetAdapterError::TooManyShardTerms);
        }
        decoded = decoded
            .checked_add(u64::from(header.get_uncompressed_length()))
            .ok_or(XetAdapterError::Overflow)?;
        if decoded > MAX_DECODED_BYTES {
            tracing::debug!(
                decoded,
                count,
                maximum = MAX_DECODED_BYTES,
                "footerless Xorb decoded size exceeded normalization limit"
            );
            return Err(XetAdapterError::XorbUnpackedLengthExceedsCap);
        }
        let end = position
            .checked_add(u64::try_from(XORB_CHUNK_HEADER_LENGTH)?)
            .and_then(|end| end.checked_add(u64::from(header.get_compressed_length())))
            .ok_or(XetAdapterError::Overflow)?;
        if end > length {
            return Err(XetAdapterError::InvalidSerializedXorb);
        }
        input.seek(SeekFrom::Start(end))?;
        position = end;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use shardline_xet_core::xorb_object::{
        CompressionScheme, SerializedXorbObject,
        xorb_format_test_utils::{ChunkSize, build_raw_xorb},
    };
    use std::io::Cursor;

    struct CountedSource {
        bytes: Cursor<Vec<u8>>,
        seeks: usize,
        position_queries: usize,
    }

    impl Read for CountedSource {
        fn read(&mut self, output: &mut [u8]) -> std::io::Result<usize> {
            // Exercise read_exact across short header reads too.
            let limit = output.len().min(3);
            self.bytes.read(&mut output[..limit])
        }
    }

    impl Seek for CountedSource {
        fn seek(&mut self, position: SeekFrom) -> std::io::Result<u64> {
            self.position_queries = self
                .position_queries
                .saturating_add(usize::from(matches!(position, SeekFrom::Current(0))));
            self.seeks = self.seeks.saturating_add(1);
            self.bytes.seek(position)
        }
    }

    #[test]
    fn checks_exact_member_bounds_with_one_seek_per_member_and_no_position_queries() {
        for compression in [CompressionScheme::None, CompressionScheme::LZ4] {
            let serialized = SerializedXorbObject::from_xorb_with_compression(
                build_raw_xorb(16, ChunkSize::Fixed(65536)),
                compression,
                false,
            )
            .unwrap();
            let length = serialized.serialized_data.len() as u64;
            let mut source = CountedSource {
                bytes: Cursor::new(serialized.serialized_data),
                seeks: 0,
                position_queries: 0,
            };
            check(&mut source, length, &|| Ok(())).unwrap();
            assert_eq!(source.bytes.position(), length);
            assert_eq!(source.seeks, 17);
            assert_eq!(source.position_queries, 0);
        }
    }
}
