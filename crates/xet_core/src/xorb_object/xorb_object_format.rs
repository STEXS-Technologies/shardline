use std::io::{Cursor, Read, Seek, SeekFrom, Write};
use std::mem::{size_of, size_of_val};
use std::sync::atomic::Ordering;

use serde::Serialize;
use tracing::warn;

use super::constants::{TARGET_CHUNK_SIZE, XORB_BLOCK_SIZE};
use super::xorb_chunk_format::{deserialize_chunk, deserialize_chunk_header, serialize_chunk, write_chunk_header};
use super::{CompressionScheme, XorbChunkHeader};
use crate::error::{CoreError, Validate};
use crate::merklehash::MerkleHash;
use crate::utils::serialization_utils::*;

pub type XorbObjectIdent = [u8; 7];
pub(crate) const XORB_OBJECT_FORMAT_IDENT: XorbObjectIdent = [b'X', b'E', b'T', b'B', b'L', b'O', b'B'];
pub(crate) const XORB_OBJECT_FORMAT_VERSION_V0: u8 = 0;
pub(crate) const XORB_OBJECT_FORMAT_IDENT_HASHES: XorbObjectIdent = [b'X', b'B', b'L', b'B', b'H', b'S', b'H'];
pub(crate) const XORB_OBJECT_FORMAT_IDENT_BOUNDARIES: XorbObjectIdent = [b'X', b'B', b'L', b'B', b'B', b'N', b'D'];
pub(crate) const XORB_OBJECT_FORMAT_VERSION: u8 = 1;
pub(crate) const XORB_OBJECT_FORMAT_HASHES_VERSION: u8 = 0;
pub(crate) const XORB_OBJECT_FORMAT_BOUNDARIES_VERSION_NO_UNPACKED_INFO: u8 = 0;
pub(crate) const XORB_OBJECT_FORMAT_BOUNDARIES_VERSION: u8 = 1;
const XORB_OBJECT_INFO_DEFAULT_LENGTH: u32 = 92;

#[inline]
fn prealloc_num_chunks(declared_size: usize) -> usize {
    let average_num_chunks_per_xorb: usize =
        XORB_BLOCK_SIZE.load(Ordering::Relaxed) as usize / TARGET_CHUNK_SIZE.load(Ordering::Relaxed) as usize;
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

#[allow(deprecated)]
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
        write_bytes(self.xorb_hash.as_bytes())?;
        write_bytes(&self.num_chunks.to_le_bytes())?;

        for offset in &self.chunk_boundary_offsets {
            write_bytes(&offset.to_le_bytes())?;
        }
        for hash in &self.chunk_hashes {
            write_bytes(hash.as_bytes())?;
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

        let mut chunk_boundary_offsets = Vec::with_capacity(prealloc_num_chunks(num_chunks as usize));
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
    pub chunk_boundary_offsets: Vec<u32>,
    pub unpacked_chunk_offsets: Vec<u32>,
    pub num_chunks: u32,
    pub hashes_section_offset_from_end: u32,
    pub boundary_section_offset_from_end: u32,
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
            + size_of::<u32>() * 5
            + size_of::<MerkleHash>() * self.chunk_hashes.len()
            + size_of_val(&self._buffer)
            + self.chunk_boundary_offsets.len() * size_of::<u32>()
            + self.unpacked_chunk_offsets.len() * size_of::<u32>()
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
        write_u32(w, self.num_chunks)?;

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
        write_u32(w, self.num_chunks)?;

        if self.num_chunks as usize != self.chunk_boundary_offsets.len() {
            return Err(CoreError::MalformedData(format!(
                "Chunk boundary offset vector not correct length on serialization. ({}, expected {})",
                self.chunk_boundary_offsets.len(),
                self.num_chunks
            )));
        }
        write_u32s(w, &self.chunk_boundary_offsets)?;

        if self.num_chunks as usize != self.unpacked_chunk_offsets.len() {
            return Err(CoreError::MalformedData(format!(
                "Unpacked chunk offset vector not correct length on serialization. ({}, expected {})",
                self.unpacked_chunk_offsets.len(),
                self.num_chunks
            )));
        }
        write_u32s(w, &self.unpacked_chunk_offsets)?;

        write_u32(w, self.num_chunks)?;
        write_u32(w, self.hashes_section_offset_from_end)?;
        write_u32(w, self.boundary_section_offset_from_end)?;

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
        } else if s.version != XORB_OBJECT_FORMAT_VERSION {
            return Err(CoreError::MalformedData(
                "Xorb Invalid Format Version".to_string(),
            ));
        }

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

        let num_chunks_2 = read_u32(r)?;

        s.chunk_hashes.reserve(prealloc_num_chunks(num_chunks_2 as usize));
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

        let num_chunks_3 = read_u32(r)?;
        if num_chunks_2 != num_chunks_3 {
            return Err(CoreError::MalformedData(
                "Xorb Invalid: inconsistent num_chunks between hashes and boundaries section."
                    .to_string(),
            ));
        }

        s.chunk_boundary_offsets.reserve(prealloc_num_chunks(num_chunks_3 as usize));
        for _ in 0..num_chunks_3 {
            s.chunk_boundary_offsets.push(read_u32(r)?);
        }

        s.unpacked_chunk_offsets.reserve(prealloc_num_chunks(num_chunks_3 as usize));
        for _ in 0..num_chunks_3 {
            s.unpacked_chunk_offsets.push(read_u32(r)?);
        }

        s.num_chunks = read_u32(r)?;
        if s.num_chunks != num_chunks_2 {
            return Err(CoreError::MalformedData(
                "Xorb Invalid: inconsistent num_chunks between metadata and hashes section."
                    .to_string(),
            ));
        }

        s.hashes_section_offset_from_end = read_u32(r)?;
        s.boundary_section_offset_from_end = read_u32(r)?;

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
            chunk_boundary_offsets: src.chunk_boundary_offsets,
            unpacked_chunk_offsets: Vec::new(),
            num_chunks: src.num_chunks,
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
            + size_of::<u32>()
            + self.chunk_boundary_offsets.len() * size_of::<u32>()
            + self.unpacked_chunk_offsets.len() * size_of::<u32>()
            + size_of_val(&self.num_chunks)
            + size_of_val(&self.hashes_section_offset_from_end)
            + size_of_val(&self.boundary_section_offset_from_end)
            + size_of_val(&self._buffer)) as u32;

        self.hashes_section_offset_from_end = (size_of_val(&self.ident_hash_section)
            + size_of_val(&self.hashes_version)
            + size_of::<u32>()
            + self.chunk_hashes.len() * size_of::<MerkleHash>())
            as u32
            + self.boundary_section_offset_from_end;
    }

    pub fn has_chunk_hashes(&self) -> bool {
        !self.chunk_hashes.is_empty()
    }
}

#[derive(Clone, PartialEq, Eq, Debug, Serialize)]
pub struct XorbObject {
    pub info: XorbObjectInfoV1,
    pub info_length: u32,
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
    pub fn get_info_length<R: Read + Seek>(reader: &mut R) -> Result<u32, CoreError> {
        reader.seek(SeekFrom::End(-(size_of::<u32>() as i64)))?;
        let mut info_length = [0u8; 4];
        reader.read_exact(&mut info_length)?;
        let info_length = u32::from_le_bytes(info_length);
        Ok(info_length)
    }

    pub fn deserialize<R: Read + Seek>(reader: &mut R) -> Result<Self, CoreError> {
        let info_length = Self::get_info_length(reader)?;
        reader.seek(SeekFrom::End(
            -(size_of::<u32>() as i64 + info_length as i64),
        ))?;
        let (info, total_bytes_read) = XorbObjectInfoV1::deserialize(reader)?;
        if total_bytes_read != info_length {
            return Err(CoreError::MalformedData("Xorb Info Format Error".to_string()));
        }
        Ok(Self { info, info_length })
    }

    pub fn serialize_given_info<W: Write>(
        w: &mut W,
        info: XorbObjectInfoV1,
    ) -> Result<(Self, usize), CoreError> {
        let mut total_written_bytes: usize = 0;
        let info_length = info.serialize(w)? as u32;
        total_written_bytes += info_length as usize;
        write_u32(w, info_length)?;
        total_written_bytes += size_of::<u32>();

        let xorb_obj = Self { info, info_length };
        Ok((xorb_obj, total_written_bytes))
    }

    pub fn from_info(info: XorbObjectInfoV1) -> Self {
        let info_length = info.serialized_length() as u32;
        Self { info, info_length }
    }

    pub fn validate_xorb_object<R: Read + Seek>(
        reader: &mut R,
        hash: &MerkleHash,
    ) -> Result<Option<XorbObject>, CoreError> {
        let Some(xorb) = XorbObject::deserialize(reader).ok_for_format_error()? else {
            return Ok(None);
        };

        let mut hash_chunks = Vec::new();
        let mut cumulative_compressed_length: u32 = 0;
        let mut unpacked_chunk_offset = 0;
        let mut start_offset = 0;

        for idx in 0..xorb.info.num_chunks {
            reader.seek(SeekFrom::Start(start_offset as u64))?;
            let Some((data, compressed_chunk_length, chunk_uncompressed_length)) =
                deserialize_chunk(reader).ok_for_format_error()?
            else {
                return Ok(None);
            };

            let chunk_hash = crate::merklehash::compute_data_hash(&data);
            hash_chunks.push((chunk_hash, chunk_uncompressed_length as u64));

            cumulative_compressed_length += compressed_chunk_length as u32;
            unpacked_chunk_offset += chunk_uncompressed_length;

            if *xorb.info.chunk_hashes.get(idx as usize).ok_or_else(|| CoreError::MalformedData("missing chunk hash".into()))? != chunk_hash {
                warn!("XORB Validation: Chunk hash does not match Info object.");
                return Ok(None);
            }

            let boundary = *xorb.info.chunk_boundary_offsets.get(idx as usize).ok_or_else(|| CoreError::MalformedData("missing chunk boundary offset".into()))?;
            if (start_offset + compressed_chunk_length as u32) != boundary {
                warn!("XORB Validation: Chunk boundary byte index does not match Info object.");
                return Ok(None);
            }

            start_offset = boundary;

            if xorb.info.boundaries_version == XORB_OBJECT_FORMAT_BOUNDARIES_VERSION
                && unpacked_chunk_offset
                    != *xorb.info.unpacked_chunk_offsets.get(idx as usize).ok_or_else(|| CoreError::MalformedData("missing unpacked chunk offset".into()))?
            {
                warn!(
                    "XORB Validation: Chunk unpacked byte offset does not match Info object."
                );
                return Ok(None);
            }
        }

        let cur_position = reader.stream_position()? as u32;
        let expected_position = cumulative_compressed_length;
        let expected_from_end_position = reader.seek(SeekFrom::End(0))? as u32
            - xorb.info_length
            - size_of::<u32>() as u32;
        if cur_position != expected_position || cur_position != expected_from_end_position {
            warn!("XORB Validation: Content bytes after known chunks in Info object.");
            return Ok(None);
        }

        let xorb_hash = crate::merklehash::xorb_hash(&hash_chunks);
        if xorb_hash != *hash || xorb_hash != xorb.info.xorb_hash {
            warn!(
                "XORB Validation: Computed hash does not match provided hash or Info hash."
            );
            return Ok(None);
        }

        Ok(Some(xorb))
    }

    pub fn get_contents_length(&self) -> Result<u32, CoreError> {
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

        if self.info.num_chunks != self.info.chunk_boundary_offsets.len() as u32
            || self.info.num_chunks != self.info.chunk_hashes.len() as u32
            || (self.info.boundaries_version == XORB_OBJECT_FORMAT_BOUNDARIES_VERSION
                && self.info.num_chunks != self.info.unpacked_chunk_offsets.len() as u32)
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
        let mut xorb_object_info = XorbObjectInfoV1::default();

        let hash = xorb.hash();
        xorb_object_info.xorb_hash = hash;
        let raw_num_bytes = xorb.num_bytes() as u64;
        let num_chunks = xorb.data.len();

        let chunks_and_boundaries = xorb.xorb_info.chunks_and_boundaries();

        xorb_object_info.num_chunks = chunks_and_boundaries.len() as u32;
        xorb_object_info.chunk_boundary_offsets = Vec::with_capacity(xorb_object_info.num_chunks as usize);
        xorb_object_info.chunk_hashes = chunks_and_boundaries.iter().map(|(hash, _)| *hash).collect();
        xorb_object_info.unpacked_chunk_offsets = chunks_and_boundaries
            .iter()
            .map(|(_, unpacked_chunk_boundary)| *unpacked_chunk_boundary)
            .collect();

        let size_upper_bound = xorb.num_bytes()
            + size_of::<XorbObjectInfoV1>()
            + (32 + 2 * size_of::<u32>() + size_of::<MerkleHash>() + size_of::<XorbChunkHeader>())
                * xorb.data.len();

        let mut serialized_data = Vec::with_capacity(size_upper_bound);

        for chunk in xorb.data {
            serialize_chunk(&chunk, &mut serialized_data, compression_scheme)?;
            xorb_object_info
                .chunk_boundary_offsets
                .push(serialized_data.len() as u32);
        }

        xorb_object_info.fill_in_boundary_offsets();

        let mut footer_start = None;
        if serialize_footer {
            footer_start = Some(serialized_data.len() as u64);
            XorbObject::serialize_given_info(&mut serialized_data, xorb_object_info)?;
        }

        Ok(Self {
            serialized_data,
            hash,
            raw_num_bytes,
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
    let mut chunk_hash_and_size: Vec<(MerkleHash, u64)> = Vec::new();
    let mut info = XorbObjectInfoV1::default();

    while (reader.position() as usize) < raw_data.len() {
        let chunk_header = match deserialize_chunk_header(&mut reader) {
            Ok(header) => header,
            Err(CoreError::ChunkHeaderParse) => break,
            Err(e) => return Err(e),
        };

        let compressed_len = chunk_header.get_compressed_length() as usize;
        let mut compressed_buf = vec![0u8; compressed_len];
        reader
            .read_exact(&mut compressed_buf)
            .map_err(|e| CoreError::MalformedData(format!("Failed to read chunk data: {e}")))?;

        let uncompressed_data = chunk_header
            .get_compression_scheme()?
            .decompress_from_slice(&compressed_buf)
            .map_err(|e| CoreError::MalformedData(format!("Failed to decompress chunk: {e}")))?;

        let chunk_hash = crate::merklehash::compute_data_hash(&uncompressed_data);
        chunk_hash_and_size.push((chunk_hash, uncompressed_data.len() as u64));

        info.chunk_hashes.push(chunk_hash);
        info.chunk_boundary_offsets.push(
            info.chunk_boundary_offsets.last().unwrap_or(&0)
                + (size_of::<XorbChunkHeader>() + compressed_len) as u32,
        );
        info.unpacked_chunk_offsets.push(
            info.unpacked_chunk_offsets.last().unwrap_or(&0) + uncompressed_data.len() as u32,
        );

        write_chunk_header(writer, &chunk_header)?;
        writer.write_all(&compressed_buf)?;
    }

    let computed_hash = crate::merklehash::xorb_hash(&chunk_hash_and_size);
    info.xorb_hash = computed_hash;
    info.num_chunks = chunk_hash_and_size.len() as u32;
    info.fill_in_boundary_offsets();

    let (xorb_obj, _) = XorbObject::serialize_given_info(writer, info)?;

    Ok((xorb_obj, computed_hash))
}

pub mod test_utils {
    use super::super::xorb_chunk_format::serialize_chunk;
    use super::*;
    use crate::merklehash::xorb_hash;
    use crate::xorb_object::RawXorbData;

    pub fn serialized_xorb_object_from_components(
        hash: &MerkleHash,
        data: Vec<u8>,
        chunk_and_boundaries: Vec<(MerkleHash, u32)>,
        compression: CompressionScheme,
    ) -> Result<SerializedXorbObject, CoreError> {
        let mut writer = Cursor::new(Vec::new());
        let mut xorb = XorbObject::default();
        xorb.info.xorb_hash = *hash;
        xorb.info.num_chunks = chunk_and_boundaries.len() as u32;
        xorb.info.chunk_boundary_offsets = Vec::with_capacity(xorb.info.num_chunks as usize);
        xorb.info.chunk_hashes = chunk_and_boundaries.iter().map(|(h, _)| *h).collect();
        xorb.info.unpacked_chunk_offsets = chunk_and_boundaries
            .iter()
            .map(|(_, b)| *b)
            .collect();

        let mut raw_start_idx = 0u32;

        for boundary in &chunk_and_boundaries {
            let chunk_boundary = boundary.1;
            let chunk_raw_bytes = &data[raw_start_idx as usize..chunk_boundary as usize];
            let _chunk_written_bytes =
                serialize_chunk(chunk_raw_bytes, &mut writer, compression)?;
            xorb.info.chunk_boundary_offsets.push(writer.position() as u32);
            raw_start_idx = chunk_boundary;
        }

        xorb.info.fill_in_boundary_offsets();

        let footer_start = writer.stream_position()?;
        let info_length = xorb.info.serialize(&mut writer)?;
        xorb.info_length = info_length as u32;

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
            let chunk_hash = crate::merklehash::compute_data_hash(&bytes);
            chunks.push(super::super::Chunk {
                hash: chunk_hash,
                data: bytes.into(),
            });
        }

        RawXorbData::from_chunks(&chunks, vec![0])
    }

    pub fn build_xorb_object(
        num_chunks: u32,
        chunk_size: ChunkSize,
        compression_scheme: CompressionScheme,
    ) -> Result<(XorbObject, Vec<u8>, Vec<u8>, Vec<(MerkleHash, u32)>), CoreError> {
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
            let chunk_hash = crate::merklehash::compute_data_hash(&bytes);
            chunks.push((chunk_hash, bytes.len() as u64));

            data_contents_raw.extend_from_slice(&bytes);

            let _bytes_written = serialize_chunk(&bytes, &mut writer, compression_scheme)?;

            raw_chunk_boundaries.push((chunk_hash, data_contents_raw.len() as u32));
            chunk_hashes.push(chunk_hash);
        }

        c.info.num_chunks = chunk_hashes.len() as u32;
        // Recompute chunk boundaries from the writer data
        let mut accumulated = 0u32;
        let writer_data = writer.get_ref();
        let mut pos = 0;
        c.info.chunk_boundary_offsets.clear();
        for _ in 0..num_chunks {
            if pos + 8 > writer_data.len() {
                break;
            }
            let header_buf: [u8; 8] = writer_data[pos..pos + 8]
                .try_into()
                .map_err(|_| CoreError::MalformedData("failed to read chunk header".into()))?;
            let compressed_len =
                u32::from_le_bytes([header_buf[1], header_buf[2], header_buf[3], 0]);
            pos += 8 + compressed_len as usize;
            accumulated += 8 + compressed_len;
            c.info.chunk_boundary_offsets.push(accumulated);
        }

        c.info.unpacked_chunk_offsets = raw_chunk_boundaries
            .iter()
            .map(|(_, b)| *b)
            .collect();
        c.info.chunk_hashes = chunk_hashes.clone();

        c.info.xorb_hash = xorb_hash(&chunks);

        c.info.fill_in_boundary_offsets();
        c.info_length = c.info.serialized_length() as u32;

        Ok((c, writer.into_inner(), data_contents_raw, raw_chunk_boundaries))
    }
}
