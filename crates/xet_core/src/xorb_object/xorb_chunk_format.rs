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
    let header =
        XorbChunkHeader::new(compression_scheme, compressed.len() as u32, chunk.len() as u32);
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
    let mut header = XorbChunkHeader::default();
    header.version = chunk_header_bytes[0];
    header.compressed_length.copy_from_slice(&chunk_header_bytes[1..4]);
    header.compression_scheme = chunk_header_bytes[4];
    header.uncompressed_length.copy_from_slice(&chunk_header_bytes[5..8]);
    header.validate()?;
    Ok(header)
}

pub fn deserialize_chunk_header<R: Read>(reader: &mut R) -> Result<XorbChunkHeader, CoreError> {
    let mut buf = [0u8; size_of::<XorbChunkHeader>()];
    reader.read_exact(&mut buf)?;
    parse_chunk_header(buf)
}

pub fn deserialize_chunk<R: Read>(
    reader: &mut R,
) -> Result<(Vec<u8>, usize, u32), CoreError> {
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
    let mut compressed_data_reader = reader.take(header.get_compressed_length().into());

    let uncompressed_len = header
        .get_compression_scheme()?
        .decompress_from_reader(&mut compressed_data_reader, writer)?;

    if uncompressed_len != header.get_uncompressed_length() as u64 {
        return Err(CoreError::MalformedData(
            "chunk is corrupted, uncompressed bytes len doesn't agree with chunk header".to_string(),
        ));
    }

    Ok((
        header.get_compressed_length() as usize + XORB_CHUNK_HEADER_LENGTH,
        uncompressed_len as u32,
    ))
}

fn try_read_chunk_header<R: Read>(
    reader: &mut R,
) -> Result<Option<XorbChunkHeader>, CoreError> {
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
