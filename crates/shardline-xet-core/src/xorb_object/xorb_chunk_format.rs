use std::borrow::Cow;
use std::io::{Read, Write};
use std::mem::size_of;
use std::sync::atomic::Ordering;

use super::CompressionScheme;
use super::constants::MAX_CHUNK_SIZE;
use super::xorb_object_format::XORB_OBJECT_FORMAT_IDENT;
use crate::error::CoreError;

pub const XORB_CHUNK_HEADER_LENGTH: usize = size_of::<XorbChunkHeader>();
const CURRENT_VERSION: u8 = 0;

#[repr(C, packed)]
#[derive(Debug, Copy, Clone, Default, PartialEq, Eq)]
pub struct XorbChunkHeader {
    pub version: u8,
    compressed_length: [u8; 3],
    compression_scheme: u8,
    uncompressed_length: [u8; 3],
}

impl XorbChunkHeader {
    pub fn new(
        compression_scheme: CompressionScheme,
        compressed_length: u32,
        uncompressed_length: u32,
    ) -> Self {
        let mut result = XorbChunkHeader {
            version: CURRENT_VERSION,
            ..Default::default()
        };
        result.set_compression_scheme(compression_scheme);
        result.set_compressed_length(compressed_length);
        result.set_uncompressed_length(uncompressed_length);
        result
    }

    pub fn set_compressed_length(&mut self, length: u32) {
        copy_three_byte_num(&mut self.compressed_length, length);
    }

    pub fn get_compressed_length(&self) -> u32 {
        convert_three_byte_num(&self.compressed_length)
    }

    pub fn set_uncompressed_length(&mut self, length: u32) {
        copy_three_byte_num(&mut self.uncompressed_length, length);
    }

    pub fn get_uncompressed_length(&self) -> u32 {
        convert_three_byte_num(&self.uncompressed_length)
    }

    pub fn get_compression_scheme(&self) -> Result<CompressionScheme, CoreError> {
        CompressionScheme::try_from(self.compression_scheme)
    }

    pub fn set_compression_scheme(&mut self, compression_scheme: CompressionScheme) {
        self.compression_scheme = compression_scheme as u8;
    }

    fn validate(&self) -> Result<(), CoreError> {
        let _ = self.get_compression_scheme()?;
        let max_chunk = MAX_CHUNK_SIZE.load(Ordering::Relaxed) as usize;
        if self.version > CURRENT_VERSION {
            return Err(CoreError::MalformedData(format!(
                "chunk header version too high at {}, current version is {}",
                self.version, CURRENT_VERSION
            )));
        }
        if self.get_compressed_length() as usize > max_chunk * 2 {
            return Err(CoreError::MalformedData(format!(
                "chunk header compressed length too large at {}, maximum: {}",
                self.get_compressed_length(),
                max_chunk
            )));
        }
        if self.get_uncompressed_length() as usize > max_chunk {
            return Err(CoreError::MalformedData(format!(
                "chunk header uncompressed length too large at {}, maximum: {}",
                self.get_uncompressed_length(),
                max_chunk
            )));
        }
        Ok(())
    }
}

#[inline]
fn copy_three_byte_num(buf: &mut [u8; 3], num: u32) {
    let bytes = num.to_le_bytes();
    buf.copy_from_slice(&bytes[0..3]);
}

#[inline]
fn convert_three_byte_num(buf: &[u8; 3]) -> u32 {
    let mut bytes = [0u8; 4];
    bytes[0..3].copy_from_slice(buf);
    u32::from_le_bytes(bytes)
}

pub fn write_chunk_header<W: Write>(
    w: &mut W,
    chunk_header: &XorbChunkHeader,
) -> std::io::Result<()> {
    w.write_all(&[chunk_header.version])?;
    w.write_all(&chunk_header.compressed_length)?;
    w.write_all(&[chunk_header.compression_scheme])?;
    w.write_all(&chunk_header.uncompressed_length)
}

pub fn serialize_chunk<W: Write>(
    chunk: &[u8],
    w: &mut W,
    compression_scheme: CompressionScheme,
) -> Result<usize, CoreError> {
    let compression_scheme = compression_scheme.resolve_for_data(chunk);

    let compressed = compression_scheme.compress_from_slice(chunk)?;

    let (compression_scheme, compressed) = if compressed.len() >= chunk.len() {
        (CompressionScheme::None, Cow::from(chunk))
    } else {
        (compression_scheme, compressed)
    };
    let header = XorbChunkHeader::new(
        compression_scheme,
        compressed.len() as u32,
        chunk.len() as u32,
    );
    write_chunk_header(w, &header)?;
    w.write_all(&compressed)?;

    Ok(size_of::<XorbChunkHeader>() + compressed.len())
}

pub fn parse_chunk_header(
    chunk_header_bytes: [u8; XORB_CHUNK_HEADER_LENGTH],
) -> Result<XorbChunkHeader, CoreError> {
    if chunk_header_bytes[..XORB_OBJECT_FORMAT_IDENT.len()] == XORB_OBJECT_FORMAT_IDENT {
        return Err(CoreError::ChunkHeaderParse);
    }
    let mut header = XorbChunkHeader {
        version: chunk_header_bytes[0],
        ..Default::default()
    };
    header
        .compressed_length
        .copy_from_slice(&chunk_header_bytes[1..4]);
    header.compression_scheme = chunk_header_bytes[4];
    header
        .uncompressed_length
        .copy_from_slice(&chunk_header_bytes[5..8]);
    header.validate()?;
    Ok(header)
}

pub fn deserialize_chunk_header<R: Read>(reader: &mut R) -> Result<XorbChunkHeader, CoreError> {
    let mut buf = [0u8; size_of::<XorbChunkHeader>()];
    reader.read_exact(&mut buf)?;
    parse_chunk_header(buf)
}

pub fn deserialize_chunk<R: Read>(reader: &mut R) -> Result<(Vec<u8>, usize, u32), CoreError> {
    let mut buf = Vec::new();
    let (compressed_chunk_size, uncompressed_chunk_size) =
        deserialize_chunk_to_writer(reader, &mut buf)?;
    Ok((buf, compressed_chunk_size, uncompressed_chunk_size))
}

pub fn deserialize_chunk_to_writer<R: Read, W: Write>(
    reader: &mut R,
    writer: &mut W,
) -> Result<(usize, u32), CoreError> {
    let header = deserialize_chunk_header(reader)?;
    deserialize_chunk_with_header_to_writer(reader, writer, header)
}

fn deserialize_chunk_with_header_to_writer<R: Read, W: Write>(
    reader: &mut R,
    writer: &mut W,
    header: XorbChunkHeader,
) -> Result<(usize, u32), CoreError> {
    // The header has already been validated (`compressed_length` is capped at
    // 2 * MAX_CHUNK_SIZE), but clamp the read ceiling anyway so a header that
    // was constructed without validation cannot drive an oversized take().
    let max_chunk = MAX_CHUNK_SIZE.load(Ordering::Relaxed) as usize;
    let compressed_len = header.get_compressed_length() as usize;
    let take_len = compressed_len.min(max_chunk.saturating_mul(2)) as u64;
    let mut compressed_data_reader = reader.take(take_len);

    let declared_uncompressed_len = u64::from(header.get_uncompressed_length());
    // Enforce the declared uncompressed length DURING decompression, not after:
    // the bounded decompressor aborts as soon as the output would exceed the
    // header's declaration, so a lying header (or a crafted compressed frame
    // that expands far beyond the declaration) cannot drive unbounded
    // allocation. Per-chunk output is therefore capped at <= MAX_CHUNK_SIZE.
    let uncompressed_len = header
        .get_compression_scheme()?
        .decompress_from_reader_bounded(
            &mut compressed_data_reader,
            writer,
            declared_uncompressed_len,
        )?;

    if uncompressed_len != declared_uncompressed_len {
        return Err(CoreError::MalformedData(
            "chunk is corrupted, uncompressed bytes len doesn't agree with chunk header"
                .to_string(),
        ));
    }

    Ok((
        header.get_compressed_length() as usize + XORB_CHUNK_HEADER_LENGTH,
        uncompressed_len as u32,
    ))
}

fn try_read_chunk_header<R: Read>(reader: &mut R) -> Result<Option<XorbChunkHeader>, CoreError> {
    let mut header_buf = [0u8; XORB_CHUNK_HEADER_LENGTH];
    let n = match reader.read(&mut header_buf) {
        Ok(0) => return Ok(None),
        Ok(n) => n,
        Err(e) => return Err(CoreError::Io(e)),
    };
    if n < XORB_CHUNK_HEADER_LENGTH {
        reader.read_exact(&mut header_buf[n..])?;
    }
    parse_chunk_header(header_buf).map(Some)
}

pub fn deserialize_chunks_to_writer<R: Read, W: Write>(
    reader: &mut R,
    writer: &mut W,
) -> Result<(usize, Vec<u32>), CoreError> {
    let mut num_compressed_written = 0;
    let mut num_uncompressed_written = 0;
    let mut chunk_byte_indices = Vec::<u32>::new();
    chunk_byte_indices.push(num_uncompressed_written);

    while let Some(header) = try_read_chunk_header(reader)? {
        let (delta_written, uncompressed_chunk_len) =
            deserialize_chunk_with_header_to_writer(reader, writer, header)?;
        num_compressed_written += delta_written;
        num_uncompressed_written += uncompressed_chunk_len;
        chunk_byte_indices.push(num_uncompressed_written);
    }

    Ok((num_compressed_written, chunk_byte_indices))
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use super::*;

    fn make_chunk_data(data: &[u8], scheme: CompressionScheme) -> Vec<u8> {
        let mut w = Cursor::new(Vec::new());
        serialize_chunk(data, &mut w, scheme).unwrap();
        w.into_inner()
    }

    #[test]
    fn xorb_chunk_header_new_and_accessors() {
        let h = XorbChunkHeader::new(CompressionScheme::LZ4, 100, 200);
        assert_eq!(h.get_compressed_length(), 100);
        assert_eq!(h.get_uncompressed_length(), 200);
        assert_eq!(h.get_compression_scheme().unwrap(), CompressionScheme::LZ4);
        assert_eq!(h.version, 0);
    }

    #[test]
    fn xorb_chunk_header_setters() {
        let mut h = XorbChunkHeader::new(CompressionScheme::None, 0, 0);
        h.set_compressed_length(0xFFFFFF);
        h.set_uncompressed_length(0x1FFFFF);
        h.set_compression_scheme(CompressionScheme::ByteGrouping4LZ4);
        assert_eq!(h.get_compressed_length(), 0xFFFFFF);
        assert_eq!(h.get_uncompressed_length(), 0x1FFFFF);
        assert_eq!(
            h.get_compression_scheme().unwrap(),
            CompressionScheme::ByteGrouping4LZ4
        );
    }

    #[test]
    fn xorb_chunk_header_three_byte_edge_cases() {
        let mut h = XorbChunkHeader::default();
        assert_eq!(h.get_compressed_length(), 0);
        assert_eq!(h.get_uncompressed_length(), 0);
        // Max 3-byte value
        h.set_compressed_length(0xFFFFFF);
        assert_eq!(h.get_compressed_length(), 0xFFFFFF);
        h.set_uncompressed_length(0xFFFFFF);
        assert_eq!(h.get_uncompressed_length(), 0xFFFFFF);
    }

    #[test]
    fn write_and_read_chunk_header() {
        let h = XorbChunkHeader::new(CompressionScheme::LZ4, 100, 200);
        let mut buf = Vec::new();
        write_chunk_header(&mut buf, &h).unwrap();
        assert_eq!(buf.len(), XORB_CHUNK_HEADER_LENGTH);
        let mut r = Cursor::new(&buf);
        let h2 = deserialize_chunk_header(&mut r).unwrap();
        assert_eq!(h.get_compressed_length(), h2.get_compressed_length());
        assert_eq!(h.get_uncompressed_length(), h2.get_uncompressed_length());
        assert_eq!(
            h.get_compression_scheme().unwrap(),
            h2.get_compression_scheme().unwrap()
        );
    }

    #[test]
    fn parse_chunk_header_roundtrip() {
        let h = XorbChunkHeader::new(CompressionScheme::None, 42, 84);
        let mut bytes = [0u8; XORB_CHUNK_HEADER_LENGTH];
        bytes[0] = h.version;
        bytes[1..4].copy_from_slice(&h.get_compressed_length().to_le_bytes()[..3]);
        bytes[4] = h.get_compression_scheme().unwrap() as u8;
        bytes[5..8].copy_from_slice(&h.get_uncompressed_length().to_le_bytes()[..3]);
        let parsed = parse_chunk_header(bytes).unwrap();
        assert_eq!(parsed, h);
    }

    #[test]
    fn parse_chunk_header_rejects_xetblob_ident() {
        let mut bytes = [0u8; XORB_CHUNK_HEADER_LENGTH];
        bytes[..7].copy_from_slice(b"XETBLOB");
        assert!(matches!(
            parse_chunk_header(bytes).unwrap_err(),
            CoreError::ChunkHeaderParse
        ));
    }

    #[test]
    fn parse_chunk_header_version_too_high() {
        let mut bytes = [0u8; XORB_CHUNK_HEADER_LENGTH];
        bytes[0] = 1; // version > CURRENT_VERSION (0)
        bytes[1..4].copy_from_slice(&100u32.to_le_bytes()[..3]);
        bytes[4] = CompressionScheme::None as u8;
        bytes[5..8].copy_from_slice(&100u32.to_le_bytes()[..3]);
        let err = parse_chunk_header(bytes).unwrap_err();
        assert!(matches!(err, CoreError::MalformedData(_)));
        assert!(err.to_string().contains("version too high"));
    }

    #[test]
    fn parse_chunk_header_invalid_compression_scheme() {
        let mut bytes = [0u8; XORB_CHUNK_HEADER_LENGTH];
        bytes[4] = 255;
        bytes[1..4].copy_from_slice(&10u32.to_le_bytes()[..3]);
        bytes[5..8].copy_from_slice(&10u32.to_le_bytes()[..3]);
        assert!(parse_chunk_header(bytes).is_err());
    }

    #[test]
    fn serialize_chunk_all_schemes_roundtrip() {
        let data = b"Hello, chunk serialization roundtrip test!";
        for scheme in &[
            CompressionScheme::None,
            CompressionScheme::LZ4,
            CompressionScheme::ByteGrouping4LZ4,
            CompressionScheme::Auto,
        ] {
            let mut w = Cursor::new(Vec::new());
            let written = serialize_chunk(data, &mut w, *scheme).unwrap();
            assert!(written >= XORB_CHUNK_HEADER_LENGTH + data.len().saturating_sub(10));

            w.set_position(0);
            let (dec, comp_size, uncomp_size) = deserialize_chunk(&mut w).unwrap();
            assert_eq!(dec, data, "roundtrip failed for {scheme:?}");
            assert_eq!(uncomp_size as usize, data.len());
            assert_eq!(comp_size, written);
        }
    }

    #[test]
    fn serialize_chunk_none_does_not_compress() {
        let data = b"small data";
        let mut w = Cursor::new(Vec::new());
        let written = serialize_chunk(data, &mut w, CompressionScheme::None).unwrap();
        assert_eq!(written, XORB_CHUNK_HEADER_LENGTH + data.len());
        w.set_position(0);
        let (dec, _, _) = deserialize_chunk(&mut w).unwrap();
        assert_eq!(dec, data);
    }

    #[test]
    fn serialize_chunk_auto_falls_back_to_none_when_compression_not_helpful() {
        // Tiny data that LZ4 can't compress
        let data = b"tiny";
        let mut w = Cursor::new(Vec::new());
        let written = serialize_chunk(data, &mut w, CompressionScheme::Auto).unwrap();
        assert_eq!(written, XORB_CHUNK_HEADER_LENGTH + data.len());
    }

    #[test]
    fn deserialize_chunk_header_empty_reader_errors() {
        let mut r = Cursor::new(Vec::new());
        assert!(deserialize_chunk_header(&mut r).is_err());
    }

    #[test]
    fn deserialize_chunk_truncated_data_errors() {
        let data = make_chunk_data(b"some test data", CompressionScheme::None);
        // Truncate the data portion
        let truncated = &data[..data.len() - 5];
        let mut r = Cursor::new(truncated);
        assert!(deserialize_chunk(&mut r).is_err());
    }

    #[test]
    fn deserialize_chunk_uncompressed_length_mismatch() {
        let data = b"real data";
        let mut buf = Vec::new();
        let header = XorbChunkHeader::new(CompressionScheme::None, data.len() as u32, 9999);
        write_chunk_header(&mut buf, &header).unwrap();
        buf.extend_from_slice(data);
        let mut r = Cursor::new(buf);
        assert!(deserialize_chunk(&mut r).is_err());
    }

    #[test]
    fn deserialize_chunk_bomb_exceeding_declared_length_rejected_early() {
        // A highly compressible payload that expands far beyond the header's
        // declared uncompressed length. The compressed frame is tiny, but
        // decompressing it would materialize megabytes unless the declared
        // length is enforced DURING decompression.
        let payload = vec![0u8; 4 * 1024 * 1024];
        let compressed = CompressionScheme::LZ4
            .compress_from_slice(&payload)
            .unwrap();
        assert!(
            compressed.len() < payload.len(),
            "repetitive payload must compress well"
        );
        let declared_len = 64u32; // far smaller than the real 4 MiB

        let mut chunk = Vec::new();
        let header = XorbChunkHeader::new(
            CompressionScheme::LZ4,
            compressed.len() as u32,
            declared_len,
        );
        write_chunk_header(&mut chunk, &header).unwrap();
        chunk.extend_from_slice(&compressed);

        let mut reader = Cursor::new(chunk);
        let mut writer = Vec::new();
        let result = deserialize_chunk_to_writer(&mut reader, &mut writer);

        // The lying header is rejected with bounded allocation: decompression
        // aborts as soon as the declared length is exceeded instead of
        // materializing the full 4 MiB.
        assert!(
            matches!(result, Err(CoreError::MalformedData(_))),
            "expected MalformedData for bomb chunk, got {result:?}"
        );
        assert!(
            writer.len() <= declared_len as usize,
            "writer received {} bytes but the declared length was {declared_len}",
            writer.len()
        );
    }

    #[test]
    fn deserialize_chunk_bomb_at_max_declared_length_rejected() {
        // A chunk whose header declares MAX_CHUNK_SIZE but whose frame expands
        // beyond it must be rejected rather than allocating past the cap.
        let payload = vec![0xABu8; 20 * 1024 * 1024];
        let compressed = CompressionScheme::LZ4
            .compress_from_slice(&payload)
            .unwrap();
        let declared_len = MAX_CHUNK_SIZE.load(Ordering::Relaxed) as u32;

        let mut chunk = Vec::new();
        let header = XorbChunkHeader::new(
            CompressionScheme::LZ4,
            compressed.len() as u32,
            declared_len,
        );
        write_chunk_header(&mut chunk, &header).unwrap();
        chunk.extend_from_slice(&compressed);

        let mut reader = Cursor::new(chunk);
        let mut writer = Vec::new();
        let result = deserialize_chunk_to_writer(&mut reader, &mut writer);

        assert!(
            result.is_err(),
            "bomb beyond MAX_CHUNK_SIZE must be rejected"
        );
        assert!(writer.len() <= declared_len as usize);
    }

    #[test]
    fn deserialize_chunk_to_writer_roundtrip() {
        let data = b"deserialize_chunk_to_writer test";
        let mut src = Cursor::new(Vec::new());
        serialize_chunk(data, &mut src, CompressionScheme::LZ4).unwrap();
        let serialized = src.into_inner();

        let mut reader = Cursor::new(&serialized);
        let mut writer = Vec::new();
        let (comp_size, uncomp_size) =
            deserialize_chunk_to_writer(&mut reader, &mut writer).unwrap();
        assert_eq!(&writer, data);
        assert_eq!(uncomp_size as usize, data.len());
        assert!(comp_size > 0);
    }

    #[test]
    fn deserialize_chunks_to_writer_empty_input() {
        let mut r = Cursor::new(Vec::new());
        let mut w = Vec::new();
        let (comp, indices) = deserialize_chunks_to_writer(&mut r, &mut w).unwrap();
        assert_eq!(comp, 0);
        assert_eq!(indices, vec![0]);
        assert!(w.is_empty());
    }

    #[test]
    fn deserialize_chunks_to_writer_single_chunk() {
        let data = b"single chunk for deserialize_chunks_to_writer";
        let serialized = make_chunk_data(data, CompressionScheme::None);
        let mut r = Cursor::new(&serialized);
        let mut w = Vec::new();
        let (comp, indices) = deserialize_chunks_to_writer(&mut r, &mut w).unwrap();
        assert!(comp > 0);
        assert_eq!(&w, data);
        assert_eq!(indices, vec![0, data.len() as u32]);
    }

    #[test]
    fn deserialize_chunks_to_writer_multiple_chunks_v2() {
        let data1 = b"first chunk data";
        let data2 = b"second chunk data that is longer";
        let mut combined = Vec::new();
        combined.extend_from_slice(&make_chunk_data(data1, CompressionScheme::LZ4));
        combined.extend_from_slice(&make_chunk_data(data2, CompressionScheme::LZ4));

        let mut r = Cursor::new(&combined);
        let mut w = Vec::new();
        let (comp, indices) = deserialize_chunks_to_writer(&mut r, &mut w).unwrap();
        assert!(comp > 0);
        let expected = [data1.as_slice(), data2.as_slice()].concat();
        assert_eq!(&w, &expected);
        assert_eq!(
            indices,
            vec![0, data1.len() as u32, (data1.len() + data2.len()) as u32]
        );
    }

    #[test]
    fn deserialize_chunks_to_writer_partial_header_errors() {
        let partial = [1u8, 2, 3];
        let mut r = Cursor::new(partial);
        let mut w = Vec::new();
        assert!(deserialize_chunks_to_writer(&mut r, &mut w).is_err());
    }

    #[test]
    fn deserialize_chunks_to_writer_io_error_propagates() {
        struct FailReader;
        impl std::io::Read for FailReader {
            fn read(&mut self, _: &mut [u8]) -> std::io::Result<usize> {
                Err(std::io::Error::other("read fail"))
            }
        }
        let mut r = FailReader;
        let mut w = Vec::new();
        let err = deserialize_chunks_to_writer(&mut r, &mut w).unwrap_err();
        assert!(matches!(err, CoreError::Io(_)));
    }

    #[test]
    fn try_read_chunk_header_via_deserialize_chunks() {
        // Header with XETBLOB ident causes ChunkHeaderParse error which propagates
        let mut r = Cursor::new(b"XETBLOBextra");
        let mut w = Vec::new();
        let result = deserialize_chunks_to_writer(&mut r, &mut w);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), CoreError::ChunkHeaderParse));
    }

    #[test]
    fn all_functions_deserialize_with_none_compression() {
        let data = b"comprehensive test of chunk format functions";
        let serialized = make_chunk_data(data, CompressionScheme::None);

        // deserialize_chunk
        let mut r = Cursor::new(&serialized);
        let (dec, _, _) = deserialize_chunk(&mut r).unwrap();
        assert_eq!(dec, data);

        // write_chunk_header + deserialize_chunk_header
        let mut buf = Vec::new();
        write_chunk_header(
            &mut buf,
            &XorbChunkHeader::new(
                CompressionScheme::None,
                data.len() as u32,
                data.len() as u32,
            ),
        )
        .unwrap();
        buf.extend_from_slice(data);
        let mut r2 = Cursor::new(&buf);
        let header = deserialize_chunk_header(&mut r2).unwrap();
        assert_eq!(header.get_compressed_length(), data.len() as u32);
    }
}
