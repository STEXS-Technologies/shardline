use super::*;
use crate::xorb_object::xorb_format_test_utils::{ChunkSize, build_raw_xorb};

struct ShortReader {
    input: Cursor<Vec<u8>>,
    seeks: usize,
    positions: usize,
}

impl Read for ShortReader {
    fn read(&mut self, output: &mut [u8]) -> std::io::Result<usize> {
        let length = output.len().min(31);
        self.input.read(&mut output[..length])
    }
}

impl Seek for ShortReader {
    fn seek(&mut self, position: SeekFrom) -> std::io::Result<u64> {
        self.seeks += 1;
        self.input.seek(position)
    }

    fn stream_position(&mut self) -> std::io::Result<u64> {
        self.positions += 1;
        self.input.stream_position()
    }
}

#[test]
fn footerless_inspection_tracks_short_reads_without_position_queries() {
    for compression in [CompressionScheme::None, CompressionScheme::LZ4] {
        let encoded = SerializedXorbObject::from_xorb_with_compression(
            build_raw_xorb(32, ChunkSize::Fixed(65536)),
            compression,
            false,
        )
        .unwrap();
        let length = encoded.serialized_data.len() as u64;
        let mut reader = ShortReader {
            input: Cursor::new(encoded.serialized_data),
            seeks: 0,
            positions: 0,
        };
        let info = inspect_footerless_xorb_from_reader(&mut reader, length).unwrap();
        assert_eq!(info.xorb_hash, encoded.hash);
        assert_eq!(info.num_chunks, 32);
        assert_eq!(info.chunk_boundary_offsets.last(), Some(&length));
        assert_eq!(reader.seeks, 1);
        assert_eq!(reader.positions, 0);
        // A claimed complete frame must never conceal a truncated payload.
        reader.input.get_mut().pop();
        assert!(inspect_footerless_xorb_from_reader(&mut reader, length).is_err());
    }
}
