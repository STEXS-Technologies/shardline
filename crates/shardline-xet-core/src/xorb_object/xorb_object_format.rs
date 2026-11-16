use std::io::{Cursor, Read, Seek, SeekFrom, Write};
use std::mem::{size_of, size_of_val};
use std::sync::atomic::Ordering;

use serde::Serialize;
use tracing::warn;

use super::constants::{TARGET_CHUNK_SIZE, XORB_BLOCK_SIZE};
use super::xorb_chunk_format::{
    deserialize_chunk, deserialize_chunk_header, serialize_chunk, write_chunk_header,
};
use super::{CompressionScheme, XorbChunkHeader};
use crate::{
    error::{CoreError, Validate},
    merklehash::{MerkleHash, aggregated_hashes::xorb_hash_from_owned, compute_data_hash},
    utils::serialization_utils::*,
};

#[cfg(test)]
mod footerless_io_tests;

pub type XorbObjectIdent = [u8; 7];
pub(crate) const XORB_OBJECT_FORMAT_IDENT: XorbObjectIdent = *b"XETBLOB";
pub(crate) const XORB_OBJECT_FORMAT_VERSION_V0: u8 = 0;
pub(crate) const XORB_OBJECT_FORMAT_IDENT_HASHES: XorbObjectIdent = *b"XBLBHSH";
pub(crate) const XORB_OBJECT_FORMAT_IDENT_BOUNDARIES: XorbObjectIdent = *b"XBLBBND";
pub(crate) const XORB_OBJECT_FORMAT_VERSION: u8 = 2;
pub(crate) const XORB_OBJECT_FORMAT_HASHES_VERSION: u8 = 0;
pub(crate) const XORB_OBJECT_FORMAT_BOUNDARIES_VERSION_NO_UNPACKED_INFO: u8 = 0;
pub(crate) const XORB_OBJECT_FORMAT_BOUNDARIES_VERSION: u8 = 1;
const XORB_OBJECT_INFO_DEFAULT_LENGTH: u64 = 92;

/// Maximum total decompressed output accepted when reconstructing a xorb via
/// `reconstruct_xorb_with_footer`. Chunks are validated individually against
/// `MAX_CHUNK_SIZE`, but the aggregate output across the loop was previously
/// unbounded (a decompression bomb). A single xorb holds at most one
/// `XORB_BLOCK_SIZE` (16 MiB) of uncompressed data, so 64 MiB allows up to four
/// full xorb-blocks while bounding decompression memory.
const MAX_DECOMPRESSED_OUTPUT_BYTES: u64 = 64 * 1024 * 1024;

#[inline]
fn prealloc_num_chunks(declared_size: usize) -> usize {
    let average_num_chunks_per_xorb: usize = XORB_BLOCK_SIZE.load(Ordering::Relaxed) as usize
        / TARGET_CHUNK_SIZE.load(Ordering::Relaxed) as usize;
    declared_size.min(average_num_chunks_per_xorb * 9 / 8)
}

#[derive(Clone, PartialEq, Eq, Debug, Serialize)]
pub struct XorbObjectInfoV0 {
    pub ident: XorbObjectIdent,
    pub version: u8,
    #[serde(rename = "cashash")]
    pub xorb_hash: MerkleHash,
    pub num_chunks: u32,
    pub chunk_boundary_offsets: Vec<u32>,
    pub chunk_hashes: Vec<MerkleHash>,
    #[serde(skip)]
    _buffer: [u8; 16],
}

impl Default for XorbObjectInfoV0 {
    fn default() -> Self {
        XorbObjectInfoV0 {
            ident: XORB_OBJECT_FORMAT_IDENT,
            version: XORB_OBJECT_FORMAT_VERSION_V0,
            xorb_hash: MerkleHash::default(),
            num_chunks: 0,
            chunk_boundary_offsets: Vec::new(),
            chunk_hashes: Vec::new(),
            _buffer: Default::default(),
        }
    }
}

impl XorbObjectInfoV0 {
    #[deprecated]
    pub fn serialize<W: Write>(&self, writer: &mut W) -> Result<usize, CoreError> {
        let mut total_bytes_written = 0;
        let mut write_bytes = |data: &[u8]| -> Result<(), CoreError> {
            writer.write_all(data)?;
            total_bytes_written += data.len();
            Ok(())
        };

        write_bytes(&self.ident)?;
        write_bytes(&[self.version])?;
        let xorb_hash_bytes: [u8; 32] = self.xorb_hash.into();
        write_bytes(&xorb_hash_bytes)?;
        write_bytes(&self.num_chunks.to_le_bytes())?;

        for offset in &self.chunk_boundary_offsets {
            write_bytes(&offset.to_le_bytes())?;
        }
        for hash in &self.chunk_hashes {
            let hash_bytes: [u8; 32] = (*hash).into();
            write_bytes(&hash_bytes)?;
        }

        write_bytes(&self._buffer)?;
        Ok(total_bytes_written)
    }

    #[deprecated]
    pub fn deserialize<R: Read>(reader: &mut R) -> Result<(Self, u32), CoreError> {
        let mut total_bytes_read: u32 = 0;
        let mut read_bytes = |data: &mut [u8]| -> Result<(), CoreError> {
            reader.read_exact(data)?;
            total_bytes_read += data.len() as u32;
            Ok(())
        };

        let mut ident = [0u8; 7];
        read_bytes(&mut ident)?;
        if ident != XORB_OBJECT_FORMAT_IDENT {
            return Err(CoreError::MalformedData("Xorb Invalid Ident".to_string()));
        }

        let mut version = [0u8; 1];
        read_bytes(&mut version)?;
        if version[0] != XORB_OBJECT_FORMAT_VERSION_V0 {
            return Err(CoreError::MalformedData(
                "Xorb Invalid Format Version".to_string(),
            ));
        }

        let (s, bytes_read_v0) = Self::deserialize_v0(reader)?;
        Ok((s, total_bytes_read + bytes_read_v0))
    }

    pub fn deserialize_v0<R: Read>(reader: &mut R) -> Result<(Self, u32), CoreError> {
        let mut total_bytes_read: u32 = 0;
        let mut read_bytes = |data: &mut [u8]| -> Result<(), CoreError> {
            reader.read_exact(data)?;
            total_bytes_read += data.len() as u32;
            Ok(())
        };

        let mut buf = [0u8; size_of::<MerkleHash>()];
        read_bytes(&mut buf)?;
        let xorb_hash = MerkleHash::from(&buf);

        let mut num_chunks = [0u8; size_of::<u32>()];
        read_bytes(&mut num_chunks)?;
        let num_chunks = u32::from_le_bytes(num_chunks);

        let mut chunk_boundary_offsets =
            Vec::with_capacity(prealloc_num_chunks(num_chunks as usize));
        for _ in 0..num_chunks {
            let mut offset = [0u8; size_of::<u32>()];
            read_bytes(&mut offset)?;
            chunk_boundary_offsets.push(u32::from_le_bytes(offset));
        }
        let mut chunk_hashes = Vec::with_capacity(prealloc_num_chunks(num_chunks as usize));
        for _ in 0..num_chunks {
            let mut hash = [0u8; size_of::<MerkleHash>()];
            read_bytes(&mut hash)?;
            chunk_hashes.push(MerkleHash::from(&hash));
        }

        let mut _buffer = [0u8; 16];
        read_bytes(&mut _buffer)?;

        Ok((
            XorbObjectInfoV0 {
                ident: XORB_OBJECT_FORMAT_IDENT,
                version: XORB_OBJECT_FORMAT_VERSION_V0,
                xorb_hash,
                num_chunks,
                chunk_boundary_offsets,
                chunk_hashes,
                _buffer,
            },
            total_bytes_read,
        ))
    }
}

#[derive(Clone, PartialEq, Eq, Debug, Serialize)]
pub struct XorbObjectInfoV1 {
    pub ident: XorbObjectIdent,
    pub version: u8,
    #[serde(rename = "cashash")]
    pub xorb_hash: MerkleHash,
    pub ident_hash_section: XorbObjectIdent,
    pub hashes_version: u8,
    pub chunk_hashes: Vec<MerkleHash>,
    pub ident_boundary_section: XorbObjectIdent,
    pub boundaries_version: u8,
    pub chunk_boundary_offsets: Vec<u64>,
    pub unpacked_chunk_offsets: Vec<u64>,
    pub num_chunks: u64,
    pub hashes_section_offset_from_end: u64,
    pub boundary_section_offset_from_end: u64,
    #[serde(skip)]
    _buffer: [u8; 16],
}

impl Default for XorbObjectInfoV1 {
    fn default() -> Self {
        let mut s = XorbObjectInfoV1 {
            ident: XORB_OBJECT_FORMAT_IDENT,
            version: XORB_OBJECT_FORMAT_VERSION,
            xorb_hash: MerkleHash::default(),
            ident_hash_section: XORB_OBJECT_FORMAT_IDENT_HASHES,
            hashes_version: XORB_OBJECT_FORMAT_HASHES_VERSION,
            chunk_hashes: Vec::new(),
            ident_boundary_section: XORB_OBJECT_FORMAT_IDENT_BOUNDARIES,
            boundaries_version: XORB_OBJECT_FORMAT_BOUNDARIES_VERSION,
            chunk_boundary_offsets: Vec::new(),
            unpacked_chunk_offsets: Vec::new(),
            num_chunks: 0,
            hashes_section_offset_from_end: 0,
            boundary_section_offset_from_end: 0,
            _buffer: Default::default(),
        };
        s.fill_in_boundary_offsets();
        s
    }
}

impl XorbObjectInfoV1 {
    pub fn serialized_length(&self) -> usize {
        size_of::<XorbObjectIdent>() * 3
            + size_of::<u8>() * 3
            + size_of::<u64>() * 5
            + size_of::<MerkleHash>() * self.chunk_hashes.len()
            + size_of_val(&self._buffer)
            + self.chunk_boundary_offsets.len() * size_of::<u64>()
            + self.unpacked_chunk_offsets.len() * size_of::<u64>()
            + size_of::<MerkleHash>()
    }

    pub fn serialize<W: Write>(&self, writer: &mut W) -> Result<usize, CoreError> {
        let mut counting_writer = countio::Counter::new(writer);
        let w = &mut counting_writer;

        write_bytes(w, &self.ident)?;
        write_u8(w, self.version)?;
        write_hash(w, &self.xorb_hash)?;

        write_bytes(w, &self.ident_hash_section)?;
        write_u8(w, self.hashes_version)?;
        write_u64(w, self.num_chunks)?;

        if self.num_chunks as usize != self.chunk_hashes.len() {
            return Err(CoreError::MalformedData(format!(
                "Chunk hash vector not correct length on serialization. ({}, expected {})",
                self.chunk_hashes.len(),
                self.num_chunks
            )));
        }

        for hash in &self.chunk_hashes {
            write_hash(w, hash)?;
        }

        write_bytes(w, &self.ident_boundary_section)?;
        write_u8(w, self.boundaries_version)?;
        write_u64(w, self.num_chunks)?;

        if self.num_chunks as usize != self.chunk_boundary_offsets.len() {
            return Err(CoreError::MalformedData(format!(
                "Chunk boundary offset vector not correct length on serialization. ({}, expected {})",
                self.chunk_boundary_offsets.len(),
                self.num_chunks
            )));
        }
        write_u64s(w, &self.chunk_boundary_offsets)?;

        if self.num_chunks as usize != self.unpacked_chunk_offsets.len() {
            return Err(CoreError::MalformedData(format!(
                "Unpacked chunk offset vector not correct length on serialization. ({}, expected {})",
                self.unpacked_chunk_offsets.len(),
                self.num_chunks
            )));
        }
        write_u64s(w, &self.unpacked_chunk_offsets)?;

        write_u64(w, self.num_chunks)?;
        write_u64(w, self.hashes_section_offset_from_end)?;
        write_u64(w, self.boundary_section_offset_from_end)?;

        write_bytes(w, &self._buffer)?;

        Ok(w.writer_bytes())
    }

    pub fn deserialize<R: Read>(reader: &mut R) -> Result<(Self, u32), CoreError> {
        let mut counting_reader = countio::Counter::new(reader);
        let r = &mut counting_reader;

        let mut s = Self::default();

        read_bytes(r, &mut s.ident)?;
        if s.ident != XORB_OBJECT_FORMAT_IDENT {
            return Err(CoreError::MalformedData("Xorb Invalid Ident".to_string()));
        }

        s.version = read_u8(r)?;
        if s.version == XORB_OBJECT_FORMAT_VERSION_V0 {
            let (sv0, _) = XorbObjectInfoV0::deserialize_v0(r)?;
            return Ok((Self::from_v0(sv0), r.reader_bytes() as u32));
        } else if s.version != XORB_OBJECT_FORMAT_VERSION && s.version != 1 {
            return Err(CoreError::MalformedData(
                "Xorb Invalid Format Version".to_string(),
            ));
        }

        let is_v1 = s.version == 1;
        let read_metadata_integer = |reader: &mut _| {
            if is_v1 {
                read_u32(reader).map(u64::from)
            } else {
                read_u64(reader)
            }
        };

        s.xorb_hash = read_hash(r)?;

        let hash_section_begin_byte_offset = r.reader_bytes();

        read_bytes(r, &mut s.ident_hash_section)?;
        if s.ident_hash_section != XORB_OBJECT_FORMAT_IDENT_HASHES {
            return Err(CoreError::MalformedData(
                "Xorb Invalid Ident for Hash Metadata Section".to_string(),
            ));
        }

        s.hashes_version = read_u8(r)?;
        if s.hashes_version != XORB_OBJECT_FORMAT_HASHES_VERSION {
            return Err(CoreError::MalformedData(
                "Xorb Invalid Format Version for Hash Metadata Section".to_string(),
            ));
        }

        let num_chunks_2 = read_metadata_integer(r)?;

        s.chunk_hashes
            .reserve(prealloc_num_chunks(num_chunks_2 as usize));
        for _ in 0..num_chunks_2 {
            s.chunk_hashes.push(read_hash(r)?);
        }

        let boundary_section_begin_byte_offset = r.reader_bytes();

        read_bytes(r, &mut s.ident_boundary_section)?;
        if s.ident_boundary_section != XORB_OBJECT_FORMAT_IDENT_BOUNDARIES {
            return Err(CoreError::MalformedData(
                "Xorb Invalid Ident for Boundary Metadata Section".to_string(),
            ));
        }

        s.boundaries_version = read_u8(r)?;
        if s.boundaries_version != XORB_OBJECT_FORMAT_BOUNDARIES_VERSION {
            return Err(CoreError::MalformedData(
                "Xorb Invalid Format Version for Boundaries Metadata Section".to_string(),
            ));
        }

        let num_chunks_3 = read_metadata_integer(r)?;
        if num_chunks_2 != num_chunks_3 {
            return Err(CoreError::MalformedData(
                "Xorb Invalid: inconsistent num_chunks between hashes and boundaries section."
                    .to_string(),
            ));
        }

        s.chunk_boundary_offsets
            .reserve(prealloc_num_chunks(num_chunks_3 as usize));
        for _ in 0..num_chunks_3 {
            s.chunk_boundary_offsets.push(read_metadata_integer(r)?);
        }

        s.unpacked_chunk_offsets
            .reserve(prealloc_num_chunks(num_chunks_3 as usize));
        for _ in 0..num_chunks_3 {
            s.unpacked_chunk_offsets.push(read_metadata_integer(r)?);
        }

        s.num_chunks = read_metadata_integer(r)?;
        if s.num_chunks != num_chunks_2 {
            return Err(CoreError::MalformedData(
                "Xorb Invalid: inconsistent num_chunks between metadata and hashes section."
                    .to_string(),
            ));
        }

        s.hashes_section_offset_from_end = read_metadata_integer(r)?;
        s.boundary_section_offset_from_end = read_metadata_integer(r)?;

        read_bytes(r, &mut s._buffer)?;

        let end_byte_offset = r.reader_bytes();

        if end_byte_offset - hash_section_begin_byte_offset
            != s.hashes_section_offset_from_end as usize
        {
            return Err(CoreError::MalformedData(
                "Xorb Invalid: incorrect hashes_section_offset_from_end.".to_string(),
            ));
        }

        if end_byte_offset - boundary_section_begin_byte_offset
            != s.boundary_section_offset_from_end as usize
        {
            return Err(CoreError::MalformedData(
                "Xorb Invalid: incorrect boundary_section_offset_from_end.".to_string(),
            ));
        }

        Ok((s, r.reader_bytes() as u32))
    }

    pub fn from_v0(src: XorbObjectInfoV0) -> Self {
        let mut s = Self {
            ident: src.ident,
            version: XORB_OBJECT_FORMAT_VERSION,
            xorb_hash: src.xorb_hash,
            ident_hash_section: XORB_OBJECT_FORMAT_IDENT_HASHES,
            hashes_version: XORB_OBJECT_FORMAT_HASHES_VERSION,
            chunk_hashes: src.chunk_hashes,
            ident_boundary_section: XORB_OBJECT_FORMAT_IDENT_BOUNDARIES,
            boundaries_version: XORB_OBJECT_FORMAT_BOUNDARIES_VERSION_NO_UNPACKED_INFO,
            chunk_boundary_offsets: src
                .chunk_boundary_offsets
                .into_iter()
                .map(|o| o as u64)
                .collect(),
            unpacked_chunk_offsets: Vec::new(),
            num_chunks: src.num_chunks as u64,
            hashes_section_offset_from_end: 0,
            boundary_section_offset_from_end: 0,
            _buffer: src._buffer,
        };
        s.fill_in_boundary_offsets();
        s
    }

    pub fn fill_in_boundary_offsets(&mut self) {
        self.boundary_section_offset_from_end = (size_of_val(&self.ident_boundary_section)
            + size_of_val(&self.boundaries_version)
            + size_of::<u64>()
            + self.chunk_boundary_offsets.len() * size_of::<u64>()
            + self.unpacked_chunk_offsets.len() * size_of::<u64>()
            + size_of_val(&self.num_chunks)
            + size_of_val(&self.hashes_section_offset_from_end)
            + size_of_val(&self.boundary_section_offset_from_end)
            + size_of_val(&self._buffer)) as u64;

        self.hashes_section_offset_from_end = (size_of_val(&self.ident_hash_section)
            + size_of_val(&self.hashes_version)
            + size_of::<u64>()
            + self.chunk_hashes.len() * size_of::<MerkleHash>())
            as u64
            + self.boundary_section_offset_from_end;
    }

    pub fn has_chunk_hashes(&self) -> bool {
        !self.chunk_hashes.is_empty()
    }
}

#[derive(Clone, PartialEq, Eq, Debug, Serialize)]
pub struct XorbObject {
    pub info: XorbObjectInfoV1,
    pub info_length: u64,
}

/// Fully validated container metadata and optionally retained decoded content.
pub struct ValidatedXorbWithData {
    pub xorb: XorbObject,
    pub decoded: Option<Vec<u8>>,
}

impl Default for XorbObject {
    fn default() -> Self {
        Self {
            info: Default::default(),
            info_length: XORB_OBJECT_INFO_DEFAULT_LENGTH,
        }
    }
}

impl XorbObject {
    fn checked_info_start(container_length: u64, info_length: u64) -> Result<u64, CoreError> {
        container_length
            .checked_sub(size_of::<u64>() as u64)
            .and_then(|end| end.checked_sub(info_length))
            .ok_or_else(|| {
                CoreError::MalformedData("Xorb info length exceeds container length".to_owned())
            })
    }

    pub fn get_info_length<R: Read + Seek>(reader: &mut R) -> Result<u64, CoreError> {
        reader.seek(SeekFrom::End(-(size_of::<u64>() as i64)))?;
        Ok(read_u64(reader)?)
    }

    pub fn deserialize<R: Read + Seek>(reader: &mut R) -> Result<Self, CoreError> {
        let info_length = Self::get_info_length(reader)?;
        // Reading the terminal length left the cursor at the container end.
        // Check unsigned bounds before seeking; a client-supplied u64 length
        // must never overflow signed relative-offset arithmetic.
        let info_start = Self::checked_info_start(reader.stream_position()?, info_length)?;
        reader.seek(SeekFrom::Start(info_start))?;
        let (info, total_bytes_read) = XorbObjectInfoV1::deserialize(reader)?;
        if u64::from(total_bytes_read) != info_length {
            return Err(CoreError::MalformedData(
                "Xorb Info Format Error".to_string(),
            ));
        }
        Ok(Self { info, info_length })
    }

    pub fn serialize_given_info<W: Write>(
        w: &mut W,
        info: XorbObjectInfoV1,
    ) -> Result<(Self, usize), CoreError> {
        let written = info.serialize(w)?;
        let info_length = written as u64;
        write_u64(w, info_length)?;
        Ok((Self { info, info_length }, written + size_of::<u64>()))
    }

    pub fn from_info(info: XorbObjectInfoV1) -> Self {
        let info_length = info.serialized_length() as u64;
        Self { info, info_length }
    }

    pub fn validate_xorb_object<R: Read + Seek>(
        reader: &mut R,
        hash: &MerkleHash,
    ) -> Result<Option<XorbObject>, CoreError> {
        let Some(xorb) = XorbObject::deserialize(reader).ok_for_format_error()? else {
            return Ok(None);
        };

        Self::validate_xorb_object_with_info(reader, xorb, hash)
    }

    /// Validates xorb content using metadata that has already been parsed.
    ///
    /// This preserves the checks performed by [`Self::validate_xorb_object`]
    /// while allowing callers that already inspected the footer to avoid
    /// parsing the same metadata twice.
    pub fn validate_xorb_object_with_info<R: Read + Seek>(
        reader: &mut R,
        xorb: XorbObject,
        hash: &MerkleHash,
    ) -> Result<Option<XorbObject>, CoreError> {
        Ok(
            Self::validate_xorb_object_with_info_and_data(reader, xorb, hash, 0, Vec::new())?
                .map(|validated| validated.xorb),
        )
    }

    /// Performs full validation and optionally retains decoded content within
    /// the caller's byte budget. No content is returned on validation failure.
    /// If the content exceeds the budget, validation continues without retaining it.
    /// The supplied scratch buffer is cleared before use.
    pub fn validate_xorb_object_with_info_and_data<R: Read + Seek>(
        reader: &mut R,
        xorb: XorbObject,
        hash: &MerkleHash,
        max_decoded_bytes: usize,
        decoded_buffer: Vec<u8>,
    ) -> Result<Option<ValidatedXorbWithData>, CoreError> {
        Self::validate_xorb_object_with_info_data_and_visitor(
            reader,
            xorb,
            hash,
            max_decoded_bytes,
            decoded_buffer,
            &mut |_, _| Ok(()),
        )
    }

    /// Visits decoded members after their individual checks, without retaining
    /// payloads. Visitor results are provisional until full validation succeeds:
    /// a later member or the aggregate container commitment can still fail.
    /// Visitors must not publish content or other durable effects.
    pub fn validate_xorb_object_with_info_data_and_visitor<R: Read + Seek>(
        reader: &mut R,
        xorb: XorbObject,
        hash: &MerkleHash,
        max_decoded_bytes: usize,
        mut decoded_buffer: Vec<u8>,
        visitor: &mut dyn FnMut(u64, &[u8]) -> std::io::Result<()>,
    ) -> Result<Option<ValidatedXorbWithData>, CoreError> {
        if xorb.info.num_chunks == 0 {
            return Ok(None);
        }
        decoded_buffer.clear();
        let mut decoded = (max_decoded_bytes != 0).then_some(decoded_buffer);
        let mut hash_chunks = Vec::with_capacity(xorb.info.num_chunks as usize);
        let mut cumulative_compressed_length: u64 = 0;
        let mut unpacked_chunk_offset: u64 = 0;

        // Encoded chunks are contiguous. Validate their declared boundaries
        // against bytes consumed below, without discarding a buffered reader's
        // remaining payload before every member.
        reader.seek(SeekFrom::Start(0))?;
        for idx in 0..xorb.info.num_chunks {
            let Some((data, compressed_chunk_length, chunk_uncompressed_length)) =
                deserialize_chunk(reader).ok_for_format_error()?
            else {
                return Ok(None);
            };

            let chunk_hash = compute_data_hash(&data);
            hash_chunks.push((chunk_hash, chunk_uncompressed_length as u64));

            cumulative_compressed_length += compressed_chunk_length as u64;
            unpacked_chunk_offset += chunk_uncompressed_length as u64;

            if *xorb
                .info
                .chunk_hashes
                .get(idx as usize)
                .ok_or_else(|| CoreError::MalformedData("missing chunk hash".into()))?
                != chunk_hash
            {
                warn!("XORB Validation: Chunk hash does not match Info object.");
                return Ok(None);
            }

            let boundary = *xorb
                .info
                .chunk_boundary_offsets
                .get(idx as usize)
                .ok_or_else(|| CoreError::MalformedData("missing chunk boundary offset".into()))?;
            if cumulative_compressed_length != boundary {
                warn!("XORB Validation: Chunk boundary byte index does not match Info object.");
                return Ok(None);
            }

            if xorb.info.boundaries_version == XORB_OBJECT_FORMAT_BOUNDARIES_VERSION
                && unpacked_chunk_offset
                    != *xorb
                        .info
                        .unpacked_chunk_offsets
                        .get(idx as usize)
                        .ok_or_else(|| {
                            CoreError::MalformedData("missing unpacked chunk offset".into())
                        })?
            {
                warn!("XORB Validation: Chunk unpacked byte offset does not match Info object.");
                return Ok(None);
            }
            visitor(idx, &data)?;
            if let Some(retained) = &mut decoded {
                if data.len() > max_decoded_bytes.saturating_sub(retained.len()) {
                    decoded = None;
                } else {
                    retained.reserve_exact(data.len());
                    retained.extend_from_slice(&data);
                }
            }
        }

        let cur_position = reader.stream_position()?;
        let expected_from_end_position =
            Self::checked_info_start(reader.seek(SeekFrom::End(0))?, xorb.info_length)?;
        if cur_position != cumulative_compressed_length
            || cur_position != expected_from_end_position
        {
            warn!("XORB Validation: Content bytes after known chunks in Info object.");
            return Ok(None);
        }

        let xorb_hash = xorb_hash_from_owned(hash_chunks);
        if xorb_hash != *hash || xorb_hash != xorb.info.xorb_hash {
            warn!("XORB Validation: Computed hash does not match provided hash or Info hash.");
            return Ok(None);
        }

        Ok(Some(ValidatedXorbWithData { xorb, decoded }))
    }

    pub fn get_contents_length(&self) -> Result<u64, CoreError> {
        self.validate_xorb_object_info()?;
        match self.info.chunk_boundary_offsets.last() {
            Some(c) => Ok(*c),
            None => Err(CoreError::MalformedData(
                "Cannot retrieve content length".to_string(),
            )),
        }
    }

    fn validate_xorb_object_info(&self) -> Result<(), CoreError> {
        if self.info.num_chunks == 0 {
            return Err(CoreError::MalformedData(
                "Invalid XorbObjectInfo, no chunks in XorbObject.".to_string(),
            ));
        }

        if self.info.num_chunks != self.info.chunk_boundary_offsets.len() as u64
            || self.info.num_chunks != self.info.chunk_hashes.len() as u64
            || (self.info.boundaries_version == XORB_OBJECT_FORMAT_BOUNDARIES_VERSION
                && self.info.num_chunks != self.info.unpacked_chunk_offsets.len() as u64)
        {
            return Err(CoreError::MalformedData(
                "Invalid XorbObjectInfo, num chunks not matching boundaries or hashes.".to_string(),
            ));
        }

        if self.info.xorb_hash == MerkleHash::default() {
            return Err(CoreError::MalformedData(
                "Invalid XorbObjectInfo, Missing xorb_hash.".to_string(),
            ));
        }

        Ok(())
    }
}

#[derive(Clone, PartialEq, Eq, Debug, Serialize)]
pub struct SerializedXorbObject {
    pub serialized_data: Vec<u8>,
    pub hash: MerkleHash,
    pub raw_num_bytes: u64,
    pub num_chunks: usize,
    pub footer_start: Option<u64>,
}

impl SerializedXorbObject {
    pub fn from_xorb_with_compression(
        xorb: super::RawXorbData,
        compression_scheme: CompressionScheme,
        serialize_footer: bool,
    ) -> Result<Self, CoreError> {
        Self::from_xorb_ref_with_compression(&xorb, compression_scheme, serialize_footer)
    }

    /// Serializes a bounded batch while retaining its input for storage fallback.
    pub fn from_xorb_ref_with_compression(
        xorb: &super::RawXorbData,
        compression_scheme: CompressionScheme,
        serialize_footer: bool,
    ) -> Result<Self, CoreError> {
        let (hash, chunk_hashes) = xorb.hash_with_chunk_hashes();
        let raw_num_bytes = xorb.num_bytes();
        let num_chunks = xorb.data.len();
        let mut xorb_object_info = XorbObjectInfoV1 {
            xorb_hash: hash,
            num_chunks: num_chunks as u64,
            chunk_boundary_offsets: Vec::with_capacity(num_chunks),
            chunk_hashes,
            unpacked_chunk_offsets: xorb
                .xorb_info
                .chunk_boundaries
                .iter()
                .map(|&b| u64::from(b))
                .collect(),
            ..XorbObjectInfoV1::default()
        };

        let size_upper_bound = raw_num_bytes
            + size_of::<XorbObjectInfoV1>()
            + (32 + 2 * size_of::<u64>() + size_of::<MerkleHash>() + size_of::<XorbChunkHeader>())
                * num_chunks;

        let mut serialized_data = Vec::with_capacity(size_upper_bound);

        for chunk in &xorb.data {
            serialize_chunk(chunk, &mut serialized_data, compression_scheme)?;
            xorb_object_info
                .chunk_boundary_offsets
                .push(serialized_data.len() as u64);
        }

        xorb_object_info.fill_in_boundary_offsets();

        let footer_start = if serialize_footer {
            let start = serialized_data.len() as u64;
            XorbObject::serialize_given_info(&mut serialized_data, xorb_object_info)?;
            Some(start)
        } else {
            None
        };

        Ok(Self {
            serialized_data,
            hash,
            raw_num_bytes: raw_num_bytes as u64,
            num_chunks,
            footer_start,
        })
    }
}

pub fn reconstruct_xorb_with_footer(
    writer: &mut impl Write,
    raw_data: &[u8],
) -> Result<(XorbObject, MerkleHash), CoreError> {
    let mut reader = Cursor::new(raw_data);
    reconstruct_xorb_with_footer_from_reader(writer, &mut reader, raw_data.len() as u64)
}

/// Normalizes a seekable Xorb stream without buffering the serialized container.
/// Decompression is bounded to one declared chunk and 64 MiB in aggregate.
/// The caller must bound the serialized input length and chunk-table size.
pub fn reconstruct_xorb_with_footer_from_reader(
    writer: &mut impl Write,
    reader: &mut (impl Read + Seek),
    raw_length: u64,
) -> Result<(XorbObject, MerkleHash), CoreError> {
    let info = inspect_footerless_chunks(reader, raw_length, |header, payload| {
        write_chunk_header(writer, header)?;
        writer.write_all(payload)?;
        Ok(())
    })?;
    let computed_hash = info.xorb_hash;
    let (xorb_obj, _) = XorbObject::serialize_given_info(writer, info)?;
    Ok((xorb_obj, computed_hash))
}

/// Verifies a complete standard footerless chunk stream without copying its
/// encoded payload. The caller must compare the computed root with its trusted
/// expected hash before publishing or appending the returned footer metadata.
///
/// # Errors
/// Rejects malformed/truncated input, excessive decompression, more than 8,192
/// chunks, trailing non-chunk bytes, and I/O failures.
pub fn inspect_footerless_xorb_from_reader(
    reader: &mut (impl Read + Seek),
    raw_length: u64,
) -> Result<XorbObjectInfoV1, CoreError> {
    let mut count = 0_usize;
    let info = inspect_footerless_chunks(reader, raw_length, |_, _| {
        count += 1;
        if count > 8192 {
            return Err(CoreError::MalformedData(
                "footerless chunk limit exceeded".to_owned(),
            ));
        }
        Ok(())
    })?;
    if info.chunk_boundary_offsets.last().copied() != Some(raw_length) || count == 0 {
        return Err(CoreError::MalformedData(
            "incomplete footerless chunk stream".to_owned(),
        ));
    }
    Ok(info)
}

fn inspect_footerless_chunks(
    reader: &mut (impl Read + Seek),
    raw_length: u64,
    mut visit: impl FnMut(&XorbChunkHeader, &[u8]) -> Result<(), CoreError>,
) -> Result<XorbObjectInfoV1, CoreError> {
    reader.seek(SeekFrom::Start(0))?;
    let estimated_chunks = usize::try_from(raw_length)
        .unwrap_or(usize::MAX)
        .checked_div(TARGET_CHUNK_SIZE.load(Ordering::Relaxed) as usize)
        .unwrap_or(0)
        .min(8192);
    let mut chunk_hash_and_size: Vec<(MerkleHash, u64)> =
        Vec::with_capacity(estimated_chunks.max(16));
    let mut info = XorbObjectInfoV1::default();
    let mut total_decompressed_bytes: u64 = 0;

    let mut position = 0_u64;
    while position < raw_length {
        let chunk_header = match deserialize_chunk_header(reader) {
            Ok(header) => header,
            Err(CoreError::ChunkHeaderParse) => break,
            Err(e) => return Err(e),
        };

        let compressed_len = chunk_header.get_compressed_length() as usize;
        let packed_end = position
            .checked_add(size_of::<XorbChunkHeader>() as u64)
            .and_then(|start| start.checked_add(compressed_len as u64))
            .filter(|end| *end <= raw_length);
        let Some(packed_end) = packed_end else {
            return Err(CoreError::MalformedData(
                "chunk exceeds bounded serialized input".to_owned(),
            ));
        };
        let mut compressed_buf = vec![0u8; compressed_len];
        reader
            .read_exact(&mut compressed_buf)
            .map_err(|e| CoreError::MalformedData(format!("Failed to read chunk data: {e}")))?;
        position = packed_end;

        let declared_uncompressed_len = u64::from(chunk_header.get_uncompressed_length());
        let next_decompressed_bytes = total_decompressed_bytes
            .checked_add(declared_uncompressed_len)
            .filter(|total| *total <= MAX_DECOMPRESSED_OUTPUT_BYTES)
            .ok_or_else(|| {
                CoreError::MalformedData(
                    "decompressed output exceeds normalization limit".to_owned(),
                )
            })?;
        let uncompressed_data = {
            // Bound per-chunk decompression to the header's declared
            // uncompressed length DURING decompression (not after), so a crafted
            // compressed frame that expands far beyond the declaration cannot
            // allocate an unbounded per-chunk buffer.
            let mut compressed_cursor = Cursor::new(compressed_buf.as_slice());
            let mut dest = Vec::new();
            let uncompressed_len = chunk_header
                .get_compression_scheme()?
                .decompress_from_reader_bounded(
                    &mut compressed_cursor,
                    &mut dest,
                    declared_uncompressed_len,
                )
                .map_err(|e| {
                    CoreError::MalformedData(format!("Failed to decompress chunk: {e}"))
                })?;
            if uncompressed_len != declared_uncompressed_len {
                return Err(CoreError::MalformedData(
                    "chunk is corrupted, uncompressed bytes len doesn't agree with chunk header"
                        .to_string(),
                ));
            }
            dest
        };

        // Bounded decompression and the exact-length check above establish
        // that the checked declared total is also the actual decoded total.
        total_decompressed_bytes = next_decompressed_bytes;
        let chunk_hash = compute_data_hash(&uncompressed_data);
        chunk_hash_and_size.push((chunk_hash, declared_uncompressed_len));
        info.chunk_hashes.push(chunk_hash);
        info.chunk_boundary_offsets.push(position);
        info.unpacked_chunk_offsets.push(total_decompressed_bytes);

        visit(&chunk_header, &compressed_buf)?;
    }

    info.num_chunks = chunk_hash_and_size.len() as u64;
    info.xorb_hash = xorb_hash_from_owned(chunk_hash_and_size);
    info.fill_in_boundary_offsets();

    Ok(info)
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use super::*;
    use crate::xorb_object::RawXorbData;
    use crate::xorb_object::xorb_chunk_format::serialize_chunk;

    fn make_v1_info(num_chunks: u64, hash_data: &[u8]) -> XorbObjectInfoV1 {
        let mut info = XorbObjectInfoV1 {
            xorb_hash: compute_data_hash(hash_data),
            num_chunks,
            chunk_hashes: (0..num_chunks)
                .map(|i| compute_data_hash(&[i as u8]))
                .collect(),
            chunk_boundary_offsets: (0..num_chunks).map(|i| (i + 1) * 100).collect(),
            unpacked_chunk_offsets: (0..num_chunks).map(|i| (i + 1) * 50).collect(),
            ..Default::default()
        };
        info.fill_in_boundary_offsets();
        info
    }

    fn make_v0_info(num_chunks: u32, hash_data: &[u8]) -> XorbObjectInfoV0 {
        XorbObjectInfoV0 {
            xorb_hash: compute_data_hash(hash_data),
            num_chunks,
            chunk_boundary_offsets: (0..num_chunks).map(|i| (i + 1) * 100).collect(),
            chunk_hashes: (0..num_chunks)
                .map(|i| compute_data_hash(&[i as u8]))
                .collect(),
            ..Default::default()
        }
    }

    // ======= XorbObjectInfoV0 =======

    #[test]
    fn v0_default() {
        let info = XorbObjectInfoV0::default();
        assert_eq!(info.version, 0);
        assert_eq!(info.num_chunks, 0);
        assert!(info.chunk_boundary_offsets.is_empty());
        assert!(info.chunk_hashes.is_empty());
    }

    #[test]
    fn v0_roundtrips_preserve_metadata_and_byte_counts() {
        for (chunks, seed) in [
            (0, b"zero".as_slice()),
            (1, b"single".as_slice()),
            (2, b"v0test".as_slice()),
            (3, b"v0dir".as_slice()),
        ] {
            let info = make_v0_info(chunks, seed);
            let mut bytes = Vec::new();
            #[allow(deprecated)]
            let written = info.serialize(&mut bytes).unwrap();
            assert_eq!(written, bytes.len());
            #[allow(deprecated)]
            let (decoded, read) = XorbObjectInfoV0::deserialize(&mut Cursor::new(&bytes)).unwrap();
            assert_eq!(decoded, info);
            assert_eq!(read as usize, bytes.len());
            let (decoded, read) =
                XorbObjectInfoV0::deserialize_v0(&mut Cursor::new(&bytes[8..])).unwrap();
            assert_eq!(decoded, info);
            assert_eq!(read as usize, bytes.len() - 8);
        }
    }

    #[allow(deprecated)]
    #[test]
    fn v0_rejects_invalid_ident_and_version() {
        for (ident, version, expected) in [
            (b"INVALID", 0, "Xorb Invalid Ident"),
            (b"XETBLOB", 99, "Xorb Invalid Format Version"),
        ] {
            let mut bytes = vec![0; 100];
            bytes[..7].copy_from_slice(ident);
            bytes[7] = version;
            let error = XorbObjectInfoV0::deserialize(&mut Cursor::new(bytes)).unwrap_err();
            let CoreError::MalformedData(message) = error else {
                panic!("{error}");
            };
            assert_eq!(message, expected);
        }
    }

    // ======= XorbObjectInfoV1 =======

    #[test]
    fn v1_default() {
        let info = XorbObjectInfoV1::default();
        assert_eq!(info.version, 2);
        assert_eq!(info.num_chunks, 0);
        assert!(info.chunk_boundary_offsets.is_empty());
        assert!(info.chunk_hashes.is_empty());
        assert!(info.hashes_section_offset_from_end > 0);
        assert!(info.boundary_section_offset_from_end > 0);
    }

    #[test]
    fn v1_fill_in_boundary_offsets() {
        let mut info = make_v1_info(1, b"fill");
        info.fill_in_boundary_offsets();
        assert!(info.boundary_section_offset_from_end > 0);
        assert!(info.hashes_section_offset_from_end > info.boundary_section_offset_from_end);
    }

    #[test]
    fn v1_serialized_length() {
        let info = make_v1_info(1, b"len");
        assert!(info.serialized_length() > 0);
        let info2 = XorbObjectInfoV1::default();
        assert!(info.serialized_length() > info2.serialized_length());
    }

    #[test]
    fn v1_has_chunk_hashes() {
        let mut info = XorbObjectInfoV1::default();
        assert!(!info.has_chunk_hashes());
        info.chunk_hashes = vec![compute_data_hash(b"a")];
        assert!(info.has_chunk_hashes());
    }

    #[test]
    fn metadata_integer_widths_preserve_v1_and_v2_wire_formats() {
        for (version, width, hashes_offset, boundary_offset) in [(1, 4, 132, 56), (2, 8, 168, 88)] {
            let mut info = make_v1_info(2, b"v1ser");
            info.version = version;
            info.hashes_section_offset_from_end = hashes_offset;
            info.boundary_section_offset_from_end = boundary_offset;
            let mut bytes = b"XETBLOB".to_vec();
            bytes.push(version);
            bytes.extend_from_slice(&info.xorb_hash.to_bytes());
            bytes.extend_from_slice(b"XBLBHSH\0");
            let append_integer = |bytes: &mut Vec<u8>, value: u64| {
                bytes.extend_from_slice(&value.to_le_bytes()[..width]);
            };
            append_integer(&mut bytes, 2);
            for hash in &info.chunk_hashes {
                bytes.extend_from_slice(&hash.to_bytes());
            }
            bytes.extend_from_slice(b"XBLBBND\x01");
            for value in [2, 100, 200, 50, 100, 2, hashes_offset, boundary_offset] {
                append_integer(&mut bytes, value);
            }
            bytes.extend_from_slice(&[0; 16]);
            let (decoded, read) = XorbObjectInfoV1::deserialize(&mut Cursor::new(&bytes)).unwrap();
            assert_eq!(read as usize, bytes.len());
            assert_eq!(decoded, info);
            if version == 2 {
                let mut serialized = Vec::new();
                assert_eq!(info.serialize(&mut serialized).unwrap(), bytes.len());
                assert_eq!(serialized, bytes);
            }
        }
    }

    #[test]
    fn v1_serialize_zero_chunks() {
        let info = XorbObjectInfoV1::default();
        let mut buf = Vec::new();
        let written = info.serialize(&mut buf).unwrap();
        let mut r = Cursor::new(&buf);
        let (info2, read) = XorbObjectInfoV1::deserialize(&mut r).unwrap();
        assert_eq!(info2, info);
        assert_eq!(written, buf.len());
        assert_eq!(read as usize, written);
    }

    #[test]
    fn v1_serialize_chunk_hash_mismatch() {
        let mut info = make_v1_info(2, b"mismatch");
        info.chunk_hashes = vec![compute_data_hash(b"c1")]; // only 1 hash, but num_chunks=2
        assert!(info.serialize(&mut Cursor::new(Vec::new())).is_err());
    }

    #[test]
    fn v1_serialize_boundary_mismatch() {
        let mut info = make_v1_info(2, b"bound");
        info.chunk_boundary_offsets = vec![50]; // only 1
        assert!(info.serialize(&mut Cursor::new(Vec::new())).is_err());
    }

    #[test]
    fn v1_serialize_unpacked_mismatch() {
        let mut info = make_v1_info(2, b"unpack");
        info.unpacked_chunk_offsets = vec![50]; // only 1
        assert!(info.serialize(&mut Cursor::new(Vec::new())).is_err());
    }

    #[test]
    fn metadata_corruption_reports_the_rejected_field() {
        for count in [0, 1, 2] {
            let mut valid = Vec::new();
            make_v1_info(count as u64, b"test")
                .serialize(&mut valid)
                .unwrap();
            // Version 2: 40-byte header, two section headers, chunk tables,
            // three trailing u64 values and 16 reserved bytes.
            let boundaries_start = 56 + 32 * count;
            let trailer_start = 72 + 48 * count;
            assert_eq!(valid.len(), trailer_start + 40);
            for (offset, expected) in [
                (0, "Xorb Invalid Ident"),
                (7, "Xorb Invalid Format Version"),
                (40, "Xorb Invalid Ident for Hash Metadata Section"),
                (47, "Xorb Invalid Format Version for Hash Metadata Section"),
                (
                    boundaries_start,
                    "Xorb Invalid Ident for Boundary Metadata Section",
                ),
                (
                    boundaries_start + 7,
                    "Xorb Invalid Format Version for Boundaries Metadata Section",
                ),
                (
                    boundaries_start + 8,
                    "Xorb Invalid: inconsistent num_chunks between hashes and boundaries section.",
                ),
                (
                    trailer_start,
                    "Xorb Invalid: inconsistent num_chunks between metadata and hashes section.",
                ),
                (
                    trailer_start + 8,
                    "Xorb Invalid: incorrect hashes_section_offset_from_end.",
                ),
                (
                    trailer_start + 16,
                    "Xorb Invalid: incorrect boundary_section_offset_from_end.",
                ),
            ] {
                let mut corrupted = valid.clone();
                corrupted[offset] ^= 0xFF;
                let error = XorbObjectInfoV1::deserialize(&mut Cursor::new(corrupted)).unwrap_err();
                let CoreError::MalformedData(message) = error else {
                    panic!("offset {offset}: {error}");
                };
                assert_eq!(message, expected, "offset {offset}");
            }
        }
    }

    #[test]
    fn legacy_conversion_preserves_metadata_and_byte_counts() {
        for count in [0, 1, 2, 3] {
            let v0 = make_v0_info(count, b"legacy");
            let mut expected = XorbObjectInfoV1 {
                xorb_hash: v0.xorb_hash,
                num_chunks: u64::from(count),
                chunk_hashes: v0.chunk_hashes.clone(),
                chunk_boundary_offsets: v0
                    .chunk_boundary_offsets
                    .iter()
                    .map(|&offset| u64::from(offset))
                    .collect(),
                boundaries_version: XORB_OBJECT_FORMAT_BOUNDARIES_VERSION_NO_UNPACKED_INFO,
                ..Default::default()
            };
            expected.fill_in_boundary_offsets();
            assert_eq!(XorbObjectInfoV1::from_v0(v0.clone()), expected);

            let mut bytes = Vec::new();
            #[allow(deprecated)]
            let written = v0.serialize(&mut bytes).unwrap();
            let mut reader = Cursor::new(&bytes);
            let (decoded, read) = XorbObjectInfoV1::deserialize(&mut reader).unwrap();
            assert_eq!(decoded, expected);
            assert_eq!(written, bytes.len());
            assert_eq!(read as usize, written);
            assert_eq!(reader.position() as usize, written);
        }
    }

    // ======= XorbObject =======

    #[test]
    fn xorb_object_default() {
        let obj = XorbObject::default();
        assert_eq!(obj.info.version, 2);
        assert_eq!(obj.info_length, 92);
    }

    #[test]
    fn xorb_object_from_info() {
        for info in [XorbObjectInfoV1::default(), make_v1_info(1, b"frominfo")] {
            let len = info.serialized_length() as u64;
            let obj = XorbObject::from_info(info);
            assert_eq!(obj.info_length, len);
        }
    }

    #[test]
    fn xorb_object_serialize_given_info_roundtrip() {
        let info = make_v1_info(1, b"serobj");
        let mut buf = Vec::new();
        let (obj, written) = XorbObject::serialize_given_info(&mut buf, info.clone()).unwrap();
        assert!(written > 0);
        assert_eq!(obj.info.xorb_hash, info.xorb_hash);

        let mut r = Cursor::new(&buf);
        let obj2 = XorbObject::deserialize(&mut r).unwrap();
        assert_eq!(obj2.info.xorb_hash, info.xorb_hash);
        assert_eq!(obj2.info.num_chunks, 1);
    }

    #[test]
    fn xorb_object_serialize_given_info_zero_chunks() {
        let info = XorbObjectInfoV1::default();
        let mut buf = Vec::new();
        let (obj, written) = XorbObject::serialize_given_info(&mut buf, info).unwrap();
        assert!(written > 0);
        assert_eq!(obj.info.num_chunks, 0);
    }

    #[test]
    fn xorb_object_get_info_length() {
        let info = make_v1_info(0, b"len");
        let mut buf = Vec::new();
        XorbObject::serialize_given_info(&mut buf, info).unwrap();
        let len = XorbObject::get_info_length(&mut Cursor::new(&buf)).unwrap();
        assert!(len > 0);
    }

    #[test]
    fn xorb_object_get_contents_length() {
        let info = make_v1_info(1, b"contents");
        let obj = XorbObject::from_info(info);
        assert_eq!(obj.get_contents_length().unwrap(), 100);
    }

    #[test]
    fn xorb_object_get_contents_length_uses_last_boundary() {
        let mut info = make_v1_info(3, b"clen");
        info.chunk_boundary_offsets = vec![50, 120, 200];
        info.fill_in_boundary_offsets();
        let obj = XorbObject::from_info(info);
        assert_eq!(obj.get_contents_length().unwrap(), 200);
    }

    #[test]
    fn xorb_object_get_contents_length_no_chunks_errors() {
        let obj = XorbObject::default();
        assert!(obj.get_contents_length().is_err());
    }

    #[test]
    fn xorb_object_deserialize_info_length_mismatch() {
        let info = make_v1_info(0, b"test");
        let mut buf = Vec::new();
        info.serialize(&mut buf).unwrap();
        // Write footer with 0 length (truncated)
        use crate::utils::serialization_utils::write_u32;
        write_u32(&mut buf, 0u32).unwrap();
        assert!(XorbObject::deserialize(&mut Cursor::new(&buf)).is_err());
    }

    #[test]
    fn xorb_object_deserialize_totally_wrong_footer() {
        let buf = vec![0u8; 20];
        // Last 4 bytes say info_length=50 but there's only 20 bytes
        // This will fail on seek or read
        let mut r = Cursor::new(buf);
        let result = XorbObject::deserialize(&mut r);
        assert!(result.is_err());
    }

    // ======= validate_xorb_object paths =======

    #[test]
    fn validation_checks_every_chunk_boundary() {
        use crate::xorb_object::xorb_format_test_utils::{ChunkSize, build_xorb_object};
        let (obj, chunk_data, _, _) =
            build_xorb_object(3, ChunkSize::Fixed(100), CompressionScheme::LZ4).unwrap();
        let mut buf = Vec::new();
        buf.extend_from_slice(&chunk_data);
        let info_len = obj.info.serialize(&mut buf).unwrap();
        buf.extend_from_slice(&(info_len as u64).to_le_bytes());
        let hash = obj.info.xorb_hash;
        let result = XorbObject::validate_xorb_object(&mut Cursor::new(&buf), &hash).unwrap();
        assert!(result.is_some());

        let parsed = XorbObject::deserialize(&mut Cursor::new(&buf)).unwrap();
        let reused =
            XorbObject::validate_xorb_object_with_info(&mut Cursor::new(&buf), parsed, &hash)
                .unwrap();
        assert_eq!(result, reused);
        for index in 0..3 {
            for delta in [-1, 1] {
                let mut invalid = obj.clone();
                invalid.info.chunk_boundary_offsets[index] = invalid.info.chunk_boundary_offsets
                    [index]
                    .checked_add_signed(delta)
                    .unwrap();
                assert!(
                    XorbObject::validate_xorb_object_with_info(
                        &mut Cursor::new(&buf),
                        invalid,
                        &hash
                    )
                    .unwrap()
                    .is_none()
                );
            }
        }
    }

    #[test]
    fn buffered_validation_reads_members_sequentially_and_rejects_middle_corruption() {
        use crate::xorb_object::xorb_format_test_utils::{ChunkSize, build_xorb_object};

        struct CountingReader {
            bytes: Cursor<Vec<u8>>,
            reads: usize,
            bytes_read: usize,
            absolute_seeks: usize,
        }
        impl Read for CountingReader {
            fn read(&mut self, output: &mut [u8]) -> std::io::Result<usize> {
                self.reads += 1;
                let length = self.bytes.read(output)?;
                self.bytes_read += length;
                Ok(length)
            }
        }
        impl Seek for CountingReader {
            fn seek(&mut self, position: SeekFrom) -> std::io::Result<u64> {
                self.absolute_seeks += usize::from(matches!(position, SeekFrom::Start(_)));
                self.bytes.seek(position)
            }
        }

        for scheme in [
            CompressionScheme::None,
            CompressionScheme::LZ4,
            CompressionScheme::ByteGrouping4LZ4,
        ] {
            let (object, payload, _, _) =
                build_xorb_object(512, ChunkSize::Fixed(512), scheme).unwrap();
            let mut serialized = payload;
            let footer_length = object.info.serialize(&mut serialized).unwrap();
            serialized.extend_from_slice(&(footer_length as u64).to_le_bytes());
            let length = serialized.len();
            let hash = object.info.xorb_hash;
            let mut source = CountingReader {
                bytes: Cursor::new(serialized.clone()),
                reads: 0,
                bytes_read: 0,
                absolute_seeks: 0,
            };
            let validated = XorbObject::validate_xorb_object(
                &mut std::io::BufReader::with_capacity(65536, &mut source),
                &hash,
            )
            .unwrap()
            .unwrap();
            assert_eq!(validated.info.num_chunks, 512);
            assert!(
                source.absolute_seeks <= 4,
                "seeks must not scale with member count"
            );
            assert!(source.reads < 20, "small headers must use buffered reads");
            assert!(
                source.bytes_read <= length * 2,
                "do not repeatedly prefetch each member"
            );

            let middle = usize::try_from(object.info.chunk_boundary_offsets[255]).unwrap()
                + std::mem::size_of::<XorbChunkHeader>();
            serialized[middle] ^= 1;
            let result = XorbObject::validate_xorb_object(
                &mut std::io::BufReader::new(Cursor::new(serialized)),
                &hash,
            );
            assert!(result.is_err() || result.unwrap().is_none());
        }
    }

    #[test]
    fn validate_xorb_object_bad_footer_returns_none() {
        let mut buf = vec![0u8; 100];
        let footer_len = 50u32;
        buf[96..100].copy_from_slice(&footer_len.to_le_bytes());
        let result =
            XorbObject::validate_xorb_object(&mut Cursor::new(&buf), &MerkleHash::default());
        assert!(result.is_err() || result.unwrap().is_none());
    }

    #[test]
    fn validate_xorb_object_empty_chunks_errors() {
        use crate::utils::serialization_utils::write_u64;
        let mut buf = Vec::new();
        let info = XorbObjectInfoV1::default();
        let info_len = info.serialize(&mut buf).unwrap() as u64;
        write_u64(&mut buf, info_len).unwrap();
        let result =
            XorbObject::validate_xorb_object(&mut Cursor::new(&buf), &MerkleHash::default());
        assert!(result.is_err() || result.unwrap().is_none());

        // Pre-parsed metadata must reject an empty container too, independently
        // of the input cursor's position.
        let parsed = XorbObject::deserialize(&mut Cursor::new(&buf)).unwrap();
        let result = XorbObject::validate_xorb_object_with_info(
            &mut Cursor::new(&buf),
            parsed,
            &MerkleHash::default(),
        );
        assert!(result.is_err() || result.unwrap().is_none());
    }

    #[test]
    fn validate_xorb_object_missing_chunk_hash_errors() {
        // Build a corrupt xorb by manually constructing the buffer
        // where chunk_hashes has fewer entries than num_chunks
        let mut info = XorbObjectInfoV1 {
            xorb_hash: compute_data_hash(b"test"),
            num_chunks: 2,
            chunk_hashes: vec![compute_data_hash(b"c1")], // only 1 hash but num_chunks=2
            chunk_boundary_offsets: vec![50, 100],
            unpacked_chunk_offsets: vec![25, 50],
            ..Default::default()
        };
        info.fill_in_boundary_offsets();

        // Serialization will fail due to mismatch
        let mut buf = Vec::new();
        assert!(info.serialize(&mut buf).is_err());
    }

    // ======= SerializedXorbObject =======

    #[test]
    fn serialized_from_xorb_with_compression_no_footer() {
        let chunks = vec![
            crate::xorb_object::Chunk {
                hash: compute_data_hash(b"c1"),
                data: vec![1u8; 50].into(),
            },
            crate::xorb_object::Chunk {
                hash: compute_data_hash(b"c2"),
                data: vec![2u8; 100].into(),
            },
        ];
        let raw = RawXorbData::from_chunks(&chunks, vec![0, 50]);
        let serialized =
            SerializedXorbObject::from_xorb_with_compression(raw, CompressionScheme::None, false)
                .unwrap();
        assert!(serialized.footer_start.is_none());
        assert_eq!(serialized.num_chunks, 2);
        assert!(!serialized.serialized_data.is_empty());
    }

    #[test]
    fn serialized_from_xorb_with_compression_with_footer() {
        let chunks = vec![crate::xorb_object::Chunk {
            hash: compute_data_hash(b"x1"),
            data: vec![0xABu8; 200].into(),
        }];
        let raw = RawXorbData::from_chunks(&chunks, vec![0]);
        let serialized =
            SerializedXorbObject::from_xorb_with_compression(raw, CompressionScheme::LZ4, true)
                .unwrap();
        assert!(serialized.footer_start.is_some());
        assert_eq!(serialized.num_chunks, 1);
    }

    #[test]
    fn serialized_from_xorb_with_compression_with_bg4lz4() {
        let chunks = vec![crate::xorb_object::Chunk {
            hash: compute_data_hash(b"x1"),
            data: vec![0xABu8; 500].into(),
        }];
        let raw = RawXorbData::from_chunks(&chunks, vec![0]);
        let serialized = SerializedXorbObject::from_xorb_with_compression(
            raw,
            CompressionScheme::ByteGrouping4LZ4,
            true,
        )
        .unwrap();
        assert!(serialized.footer_start.is_some());
        assert_eq!(serialized.num_chunks, 1);
    }

    // ======= reconstruct_xorb_with_footer =======

    #[test]
    fn reconstruct_with_footer_empty_input() {
        let mut output = Vec::new();
        let (obj, hash) = reconstruct_xorb_with_footer(&mut output, b"").unwrap();
        assert_eq!(obj.info.num_chunks, 0);
        assert_eq!(hash, MerkleHash::default());
    }

    #[test]
    fn reconstruct_with_footer_single_chunk() {
        let data = b"reconstruct test data here";
        let mut chunk_buf = Cursor::new(Vec::new());
        serialize_chunk(data, &mut chunk_buf, CompressionScheme::None).unwrap();
        let chunk_data = chunk_buf.into_inner();
        let mut output = Cursor::new(Vec::new());
        let (obj, hash) = reconstruct_xorb_with_footer(&mut output, &chunk_data).unwrap();
        assert_eq!(obj.info.num_chunks, 1);
        assert_eq!(obj.info.xorb_hash, hash);
        assert_ne!(hash, MerkleHash::default());
    }

    #[test]
    fn reconstruct_with_footer_xetblob_ident_breaks() {
        let mut output = Vec::new();
        let (obj, hash) = reconstruct_xorb_with_footer(&mut output, b"XETBLOBextra").unwrap();
        assert_eq!(obj.info.num_chunks, 0);
        assert_eq!(hash, MerkleHash::default());
    }

    #[test]
    fn reconstruct_with_footer_malformed_data_errors() {
        // Header says compressed_length=100 but data is short
        use crate::xorb_object::xorb_chunk_format::write_chunk_header;
        let mut chunk = Vec::new();
        let header = XorbChunkHeader::new(CompressionScheme::None, 100, 100);
        write_chunk_header(&mut chunk, &header).unwrap();
        chunk.extend_from_slice(b"SHORT");
        let mut output = Vec::new();
        assert!(reconstruct_xorb_with_footer(&mut output, &chunk).is_err());
    }

    // ======= test_utils =======

    #[test]
    fn test_utils_serialized_xorb_object_from_components() {
        use test_utils::serialized_xorb_object_from_components;
        let hash = compute_data_hash(b"test_hash");
        let data = vec![0u8; 1024];
        let chunk_boundaries = vec![
            (compute_data_hash(b"c1"), 512),
            (compute_data_hash(b"c2"), 1024),
        ];
        let serialized = serialized_xorb_object_from_components(
            &hash,
            data.clone(),
            chunk_boundaries,
            CompressionScheme::None,
        )
        .unwrap();
        assert_eq!(serialized.hash, hash);
        assert_eq!(serialized.num_chunks, 2);
        assert_eq!(serialized.raw_num_bytes, 1024);
        assert!(serialized.footer_start.is_some());
        // Can deserialize
        let mut r = Cursor::new(&serialized.serialized_data);
        let obj = XorbObject::deserialize(&mut r).unwrap();
        assert_eq!(obj.info.xorb_hash, hash);
    }

    #[test]
    fn test_utils_build_raw_xorb() {
        use test_utils::{ChunkSize, build_raw_xorb};
        let raw = build_raw_xorb(3, ChunkSize::Fixed(512));
        assert_eq!(raw.data.len(), 3);
        assert_eq!(raw.num_bytes(), 3 * 512);
        let raw2 = build_raw_xorb(2, ChunkSize::Random(500, 1000));
        assert_eq!(raw2.data.len(), 2);
        assert_eq!(raw2.num_bytes(), 2 * 1024); // Random falls back to 1024
    }

    #[test]
    fn test_utils_build_xorb_object() {
        use test_utils::{ChunkSize, build_xorb_object};
        let (obj, chunk_data, raw_data, boundaries) =
            build_xorb_object(2, ChunkSize::Fixed(512), CompressionScheme::None).unwrap();
        assert_eq!(obj.info.num_chunks, 2);
        assert_ne!(obj.info.xorb_hash, MerkleHash::default());
        assert!(!chunk_data.is_empty());
        assert_eq!(raw_data.len(), 2 * 512);
        assert_eq!(boundaries.len(), 2);
    }

    #[test]
    fn test_utils_chunk_size_display() {
        use test_utils::ChunkSize;
        assert_eq!(format!("{}", ChunkSize::Fixed(1024)), "1024");
        assert_eq!(format!("{}", ChunkSize::Random(100, 200)), "[100, 200]");
    }

    #[test]
    fn xorb_format_constants() {
        assert_eq!(XORB_OBJECT_FORMAT_IDENT, *b"XETBLOB");
        assert_eq!(XORB_OBJECT_FORMAT_IDENT_HASHES, *b"XBLBHSH");
        assert_eq!(XORB_OBJECT_FORMAT_IDENT_BOUNDARIES, *b"XBLBBND");
        assert_eq!(XORB_OBJECT_FORMAT_VERSION, 2);
        assert_eq!(XORB_OBJECT_FORMAT_VERSION_V0, 0);
        assert_eq!(XORB_OBJECT_FORMAT_HASHES_VERSION, 0);
        assert_eq!(XORB_OBJECT_FORMAT_BOUNDARIES_VERSION, 1);
        assert_eq!(XORB_OBJECT_FORMAT_BOUNDARIES_VERSION_NO_UNPACKED_INFO, 0);
    }
}

pub mod test_utils {
    use super::super::xorb_chunk_format::serialize_chunk;
    use super::*;
    use crate::merklehash::xorb_hash;
    use crate::xorb_object::RawXorbData;

    pub fn serialized_xorb_object_from_components(
        hash: &MerkleHash,
        data: Vec<u8>,
        chunk_and_boundaries: Vec<(MerkleHash, u64)>,
        compression: CompressionScheme,
    ) -> Result<SerializedXorbObject, CoreError> {
        let mut writer = Cursor::new(Vec::new());
        let mut xorb = XorbObject::default();
        xorb.info.xorb_hash = *hash;
        xorb.info.num_chunks = chunk_and_boundaries.len() as u64;
        xorb.info.chunk_boundary_offsets = Vec::with_capacity(xorb.info.num_chunks as usize);
        xorb.info.chunk_hashes = chunk_and_boundaries.iter().map(|(h, _)| *h).collect();
        xorb.info.unpacked_chunk_offsets = chunk_and_boundaries.iter().map(|(_, b)| *b).collect();

        let mut raw_start_idx = 0u64;

        for boundary in &chunk_and_boundaries {
            let chunk_boundary = boundary.1;
            let chunk_raw_bytes = &data[raw_start_idx as usize..chunk_boundary as usize];
            let _chunk_written_bytes = serialize_chunk(chunk_raw_bytes, &mut writer, compression)?;
            xorb.info.chunk_boundary_offsets.push(writer.position());
            raw_start_idx = chunk_boundary;
        }

        xorb.info.fill_in_boundary_offsets();

        let footer_start = writer.stream_position()?;
        let info_length = xorb.info.serialize(&mut writer)?;
        xorb.info_length = info_length as u64;

        writer.write_all(&xorb.info_length.to_le_bytes())?;

        Ok(SerializedXorbObject {
            serialized_data: writer.into_inner(),
            hash: *hash,
            raw_num_bytes: data.len() as u64,
            num_chunks: chunk_and_boundaries.len(),
            footer_start: Some(footer_start),
        })
    }

    #[derive(Debug, Clone, Copy)]
    pub enum ChunkSize {
        Random(u32, u32),
        Fixed(u32),
    }

    impl std::fmt::Display for ChunkSize {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            match self {
                ChunkSize::Random(a, b) => write!(f, "[{a}, {b}]"),
                ChunkSize::Fixed(a) => write!(f, "{a}"),
            }
        }
    }

    pub fn build_raw_xorb(num_chunks: u32, chunk_size: ChunkSize) -> RawXorbData {
        let mut chunks: Vec<super::super::Chunk> = Vec::with_capacity(num_chunks as usize);

        for _idx in 0..num_chunks {
            let chunk_size: u32 = match chunk_size {
                ChunkSize::Random(_a, _b) => {
                    // Without rand, use a fixed size for random
                    1024
                }
                ChunkSize::Fixed(size) => size,
            };
            let bytes = vec![0u8; chunk_size as usize];
            let chunk_hash = compute_data_hash(&bytes);
            chunks.push(super::super::Chunk {
                hash: chunk_hash,
                data: bytes.into(),
            });
        }

        RawXorbData::from_chunks(&chunks, vec![0])
    }

    #[allow(clippy::type_complexity)]
    pub fn build_xorb_object(
        num_chunks: u64,
        chunk_size: ChunkSize,
        compression_scheme: CompressionScheme,
    ) -> Result<(XorbObject, Vec<u8>, Vec<u8>, Vec<(MerkleHash, u64)>), CoreError> {
        let mut c = XorbObject::default();
        let mut chunk_hashes = vec![];
        let mut writer = Cursor::new(vec![]);
        let mut chunks = vec![];
        let mut data_contents_raw = vec![];
        let mut raw_chunk_boundaries = vec![];

        for _idx in 0..num_chunks {
            let chunk_size: u32 = match chunk_size {
                ChunkSize::Random(_a, _b) => 1024,
                ChunkSize::Fixed(size) => size,
            };

            let bytes = vec![0u8; chunk_size as usize];
            let chunk_hash = compute_data_hash(&bytes);
            chunks.push((chunk_hash, bytes.len() as u64));

            data_contents_raw.extend_from_slice(&bytes);

            let _bytes_written = serialize_chunk(&bytes, &mut writer, compression_scheme)?;

            raw_chunk_boundaries.push((chunk_hash, data_contents_raw.len() as u64));
            chunk_hashes.push(chunk_hash);
        }

        c.info.num_chunks = chunk_hashes.len() as u64;
        // Recompute chunk boundaries from the writer data
        let mut accumulated = 0u64;
        let writer_data = writer.get_ref();
        let mut pos = 0;
        c.info.chunk_boundary_offsets.clear();
        for _ in 0..num_chunks {
            if pos + 8 > writer_data.len() {
                break;
            }
            let header_buf: [u8; 8] = writer_data[pos..pos + 8].try_into().map_err(
                |e: std::array::TryFromSliceError| {
                    CoreError::MalformedData(format!("failed to read chunk header: {e}"))
                },
            )?;
            let compressed_len =
                u32::from_le_bytes([header_buf[1], header_buf[2], header_buf[3], 0]);
            pos += 8 + compressed_len as usize;
            accumulated += 8 + compressed_len as u64;
            c.info.chunk_boundary_offsets.push(accumulated);
        }

        c.info.unpacked_chunk_offsets = raw_chunk_boundaries.iter().map(|(_, b)| *b).collect();
        c.info.chunk_hashes = chunk_hashes;

        c.info.xorb_hash = xorb_hash(&chunks);

        c.info.fill_in_boundary_offsets();
        c.info_length = c.info.serialized_length() as u64;

        Ok((
            c,
            writer.into_inner(),
            data_contents_raw,
            raw_chunk_boundaries,
        ))
    }
}
