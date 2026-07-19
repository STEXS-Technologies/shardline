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
use crate::{error::{CoreError, Validate}, merklehash::{compute_data_hash, xorb_hash, MerkleHash}, utils::serialization_utils::*};

pub type XorbObjectIdent = [u8; 7];
pub(crate) const XORB_OBJECT_FORMAT_IDENT: XorbObjectIdent =
    [b'X', b'E', b'T', b'B', b'L', b'O', b'B'];
pub(crate) const XORB_OBJECT_FORMAT_VERSION_V0: u8 = 0;
pub(crate) const XORB_OBJECT_FORMAT_IDENT_HASHES: XorbObjectIdent =
    [b'X', b'B', b'L', b'B', b'H', b'S', b'H'];
pub(crate) const XORB_OBJECT_FORMAT_IDENT_BOUNDARIES: XorbObjectIdent =
    [b'X', b'B', b'L', b'B', b'B', b'N', b'D'];
pub(crate) const XORB_OBJECT_FORMAT_VERSION: u8 = 2;
pub(crate) const XORB_OBJECT_FORMAT_HASHES_VERSION: u8 = 0;
pub(crate) const XORB_OBJECT_FORMAT_BOUNDARIES_VERSION_NO_UNPACKED_INFO: u8 = 0;
pub(crate) const XORB_OBJECT_FORMAT_BOUNDARIES_VERSION: u8 = 1;
const XORB_OBJECT_INFO_DEFAULT_LENGTH: u64 = 92;

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

        let num_chunks_2 = if is_v1 {
            read_u32(r)? as u64
        } else {
            read_u64(r)?
        };

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

        let num_chunks_3 = if is_v1 {
            read_u32(r)? as u64
        } else {
            read_u64(r)?
        };
        if num_chunks_2 != num_chunks_3 {
            return Err(CoreError::MalformedData(
                "Xorb Invalid: inconsistent num_chunks between hashes and boundaries section."
                    .to_string(),
            ));
        }

        s.chunk_boundary_offsets
            .reserve(prealloc_num_chunks(num_chunks_3 as usize));
        for _ in 0..num_chunks_3 {
            s.chunk_boundary_offsets.push(if is_v1 {
                read_u32(r)? as u64
            } else {
                read_u64(r)?
            });
        }

        s.unpacked_chunk_offsets
            .reserve(prealloc_num_chunks(num_chunks_3 as usize));
        for _ in 0..num_chunks_3 {
            s.unpacked_chunk_offsets.push(if is_v1 {
                read_u32(r)? as u64
            } else {
                read_u64(r)?
            });
        }

        s.num_chunks = if is_v1 {
            read_u32(r)? as u64
        } else {
            read_u64(r)?
        };
        if s.num_chunks != num_chunks_2 {
            return Err(CoreError::MalformedData(
                "Xorb Invalid: inconsistent num_chunks between metadata and hashes section."
                    .to_string(),
            ));
        }

        s.hashes_section_offset_from_end = if is_v1 {
            read_u32(r)? as u64
        } else {
            read_u64(r)?
        };
        s.boundary_section_offset_from_end = if is_v1 {
            read_u32(r)? as u64
        } else {
            read_u64(r)?
        };

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
            chunk_boundary_offsets: src.chunk_boundary_offsets.into_iter().map(|o| o as u64).collect(),
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

impl Default for XorbObject {
    fn default() -> Self {
        Self {
            info: Default::default(),
            info_length: XORB_OBJECT_INFO_DEFAULT_LENGTH,
        }
    }
}

impl XorbObject {
    pub fn get_info_length<R: Read + Seek>(reader: &mut R) -> Result<u64, CoreError> {
        reader.seek(SeekFrom::End(-(size_of::<u64>() as i64)))?;
        let mut info_length = [0u8; 8];
        reader.read_exact(&mut info_length)?;
        let info_length = u64::from_le_bytes(info_length);
        Ok(info_length)
    }

    pub fn deserialize<R: Read + Seek>(reader: &mut R) -> Result<Self, CoreError> {
        let info_length = Self::get_info_length(reader)?;
        reader.seek(SeekFrom::End(
            -(size_of::<u64>() as i64 + info_length as i64),
        ))?;
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
        let mut total_written_bytes: usize = 0;
        let info_length = info.serialize(w)? as u64;
        total_written_bytes += info_length as usize;
        write_u64(w, info_length)?;
        total_written_bytes += size_of::<u64>();

        let xorb_obj = Self { info, info_length };
        Ok((xorb_obj, total_written_bytes))
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

        let mut hash_chunks = Vec::with_capacity(xorb.info.num_chunks as usize);
        let mut cumulative_compressed_length: u64 = 0;
        let mut unpacked_chunk_offset: u64 = 0;
        let mut start_offset: u64 = 0;

        for idx in 0..xorb.info.num_chunks {
            reader.seek(SeekFrom::Start(start_offset))?;
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
            if (start_offset + compressed_chunk_length as u64) != boundary {
                warn!("XORB Validation: Chunk boundary byte index does not match Info object.");
                return Ok(None);
            }

            start_offset = boundary;

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
        }

        let cur_position = reader.stream_position()?;
        let expected_position = cumulative_compressed_length;
        let expected_from_end_position =
            reader.seek(SeekFrom::End(0))? - xorb.info_length - size_of::<u64>() as u64;
        if cur_position != expected_position || cur_position != expected_from_end_position {
            warn!("XORB Validation: Content bytes after known chunks in Info object.");
            return Ok(None);
        }

        let xorb_hash = xorb_hash(&hash_chunks);
        if xorb_hash != *hash || xorb_hash != xorb.info.xorb_hash {
            warn!("XORB Validation: Computed hash does not match provided hash or Info hash.");
            return Ok(None);
        }

        Ok(Some(xorb))
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
        let mut xorb_object_info = XorbObjectInfoV1::default();

        let hash = xorb.hash();
        xorb_object_info.xorb_hash = hash;
        let raw_num_bytes = xorb.num_bytes() as u64;
        let num_chunks = xorb.data.len();

        xorb_object_info.num_chunks = xorb.data.len() as u64;
        xorb_object_info.chunk_boundary_offsets =
            Vec::with_capacity(xorb_object_info.num_chunks as usize);
        xorb_object_info.chunk_hashes = xorb
            .data
            .iter()
            .map(|chunk_data| compute_data_hash(chunk_data))
            .collect();
        xorb_object_info.unpacked_chunk_offsets = xorb
            .xorb_info
            .chunk_boundaries
            .iter()
            .map(|&b| b as u64)
            .collect();

        let size_upper_bound = xorb.num_bytes()
            + size_of::<XorbObjectInfoV1>()
            + (32 + 2 * size_of::<u64>() + size_of::<MerkleHash>() + size_of::<XorbChunkHeader>())
                * xorb.data.len();

        let mut serialized_data = Vec::with_capacity(size_upper_bound);

        for chunk in xorb.data {
            serialize_chunk(&chunk, &mut serialized_data, compression_scheme)?;
            xorb_object_info
                .chunk_boundary_offsets
                .push(serialized_data.len() as u64);
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
    let estimated_chunks = raw_data
        .len()
        .checked_div(TARGET_CHUNK_SIZE.load(Ordering::Relaxed) as usize)
        .unwrap_or(0);
    let mut chunk_hash_and_size: Vec<(MerkleHash, u64)> =
        Vec::with_capacity(estimated_chunks.max(16));
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

        let chunk_hash = compute_data_hash(&uncompressed_data);
        chunk_hash_and_size.push((chunk_hash, uncompressed_data.len() as u64));

        info.chunk_hashes.push(chunk_hash);
        info.chunk_boundary_offsets.push(
            info.chunk_boundary_offsets.last().unwrap_or(&0)
                + (size_of::<XorbChunkHeader>() + compressed_len) as u64,
        );
        info.unpacked_chunk_offsets.push(
            info.unpacked_chunk_offsets.last().unwrap_or(&0) + uncompressed_data.len() as u64,
        );

        write_chunk_header(writer, &chunk_header)?;
        writer.write_all(&compressed_buf)?;
    }

    let computed_hash = xorb_hash(&chunk_hash_and_size);
    info.xorb_hash = computed_hash;
    info.num_chunks = chunk_hash_and_size.len() as u64;
    info.fill_in_boundary_offsets();

    let (xorb_obj, _) = XorbObject::serialize_given_info(writer, info)?;

    Ok((xorb_obj, computed_hash))
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
        assert!(info.chunk_boundary_offsets.is_empty());
        assert!(info.chunk_hashes.is_empty());
    }

    #[test]
    fn v0_serialize_roundtrip() {
        let info = make_v0_info(2, b"v0test");
        let mut buf = Vec::new();
        #[allow(deprecated)]
        let written = info.serialize(&mut buf).unwrap();
        assert!(written > 0);
        let mut r = Cursor::new(&buf);
        #[allow(deprecated)]
        let (info2, _) = XorbObjectInfoV0::deserialize(&mut r).unwrap();
        assert_eq!(info2.xorb_hash, info.xorb_hash);
        assert_eq!(info2.num_chunks, 2);
        assert_eq!(info2.chunk_boundary_offsets, info.chunk_boundary_offsets);
    }

    #[allow(deprecated)]
    #[test]
    fn v0_deserialize_invalid_ident() {
        let mut buf = vec![0u8; 100];
        buf[..7].copy_from_slice(b"INVALID");
        assert!(XorbObjectInfoV0::deserialize(&mut Cursor::new(buf)).is_err());
    }

    #[allow(deprecated)]
    #[test]
    fn v0_deserialize_invalid_version() {
        let mut buf = vec![0u8; 100];
        buf[..7].copy_from_slice(b"XETBLOB");
        buf[7] = 99;
        assert!(XorbObjectInfoV0::deserialize(&mut Cursor::new(buf)).is_err());
    }

    #[allow(deprecated)]
    #[test]
    fn v0_deserialize_v0_direct() {
        let info = make_v0_info(3, b"v0dir");
        let mut buf = Vec::new();
        info.serialize(&mut buf).unwrap();
        // Skip ident+version (8 bytes)
        let mut r = Cursor::new(&buf[8..]);
        let (info2, _) = XorbObjectInfoV0::deserialize_v0(&mut r).unwrap();
        assert_eq!(info2.num_chunks, 3);
        assert_eq!(info2.chunk_hashes.len(), 3);
    }

    #[test]
    fn v0_zero_chunks() {
        let info = make_v0_info(0, b"zero");
        let mut buf = Vec::new();
        #[allow(deprecated)]
        let _written = info.serialize(&mut buf).unwrap();
        let mut r = Cursor::new(&buf);
        #[allow(deprecated)]
        let (info2, _) = XorbObjectInfoV0::deserialize(&mut r).unwrap();
        assert_eq!(info2.num_chunks, 0);
        assert!(info2.chunk_hashes.is_empty());
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
    fn v1_serialize_roundtrip() {
        let info = make_v1_info(2, b"v1ser");
        let mut buf = Vec::new();
        let written = info.serialize(&mut buf).unwrap();
        assert!(written > 0);
        let mut r = Cursor::new(&buf);
        let (info2, read) = XorbObjectInfoV1::deserialize(&mut r).unwrap();
        assert_eq!(read, written as u32);
        assert_eq!(info2.xorb_hash, info.xorb_hash);
        assert_eq!(info2.num_chunks, 2);
        assert_eq!(info2.chunk_hashes, info.chunk_hashes);
        assert_eq!(info2.chunk_boundary_offsets, info.chunk_boundary_offsets);
        assert_eq!(info2.unpacked_chunk_offsets, info.unpacked_chunk_offsets);
    }

    #[test]
    fn v1_serialize_zero_chunks() {
        let info = XorbObjectInfoV1::default();
        let mut buf = Vec::new();
        let _written = info.serialize(&mut buf).unwrap();
        let mut r = Cursor::new(&buf);
        let (info2, _) = XorbObjectInfoV1::deserialize(&mut r).unwrap();
        assert_eq!(info2.num_chunks, 0);
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
    fn v1_deserialize_invalid_ident() {
        let mut buf = vec![0u8; 200];
        buf[..7].copy_from_slice(b"BADIDNT");
        assert!(XorbObjectInfoV1::deserialize(&mut Cursor::new(buf)).is_err());
    }

    #[test]
    fn v1_deserialize_invalid_version() {
        let mut buf = vec![0u8; 200];
        buf[..7].copy_from_slice(b"XETBLOB");
        buf[7] = 5;
        assert!(XorbObjectInfoV1::deserialize(&mut Cursor::new(buf)).is_err());
    }

    #[test]
    fn v1_deserialize_v0_compat() {
        let v0 = make_v0_info(1, b"compat");
        let mut v0_buf = Vec::new();
        #[allow(deprecated)]
        v0.serialize(&mut v0_buf).unwrap();
        let mut r = Cursor::new(v0_buf);
        let (v1, _) = XorbObjectInfoV1::deserialize(&mut r).unwrap();
        assert_eq!(v1.version, 2);
        assert_eq!(v1.xorb_hash, v0.xorb_hash);
        assert_eq!(v1.num_chunks, 1);
    }

    #[test]
    fn v1_from_v0() {
        let v0 = make_v0_info(2, b"fromv0");
        let v1 = XorbObjectInfoV1::from_v0(v0.clone());
        assert_eq!(v1.version, 2);
        assert_eq!(v1.xorb_hash, v0.xorb_hash);
        assert_eq!(v1.num_chunks, 2);
        assert_eq!(v1.chunk_hashes.len(), 2);
        assert_eq!(v1.chunk_boundary_offsets.len(), 2);
        assert!(v1.unpacked_chunk_offsets.is_empty()); // v0 has no unpacked
    }

    #[test]
    fn v1_deserialize_wrong_hash_section_ident() {
        let info = make_v1_info(0, b"test");
        let mut buf = Vec::new();
        info.serialize(&mut buf).unwrap();
        // Corrupt hash section ident (offset 40 for n=0: 7+1+32=40)
        if buf.len() > 47 {
            buf[40..47].copy_from_slice(b"CORRUPT");
        }
        assert!(XorbObjectInfoV1::deserialize(&mut Cursor::new(buf)).is_err());
    }

    #[test]
    fn v1_deserialize_wrong_boundary_section_ident() {
        let info = make_v1_info(0, b"test");
        let mut buf = Vec::new();
        info.serialize(&mut buf).unwrap();
        // Boundary section ident after: ident(7)+ver(1)+hash(32)+hash_ident(7)+hash_ver(1)+nchunks(4)=52
        if buf.len() > 59 {
            buf[52..59].copy_from_slice(b"CORRUPT");
        }
        assert!(XorbObjectInfoV1::deserialize(&mut Cursor::new(buf)).is_err());
    }

    #[test]
    fn v1_deserialize_wrong_hashes_version() {
        let info = make_v1_info(0, b"test");
        let mut buf = Vec::new();
        info.serialize(&mut buf).unwrap();
        // hashes_version at offset 47: 7+1+32+7 = 47
        if buf.len() > 47 {
            buf[47] = 99;
        }
        assert!(XorbObjectInfoV1::deserialize(&mut Cursor::new(buf)).is_err());
    }

    #[test]
    fn v1_deserialize_wrong_boundaries_version() {
        let info = make_v1_info(0, b"test");
        let mut buf = Vec::new();
        info.serialize(&mut buf).unwrap();
        // boundaries_version at offset 63: 7+1+32+7+1+8+7 = 63
        if buf.len() > 63 {
            buf[63] = 99;
        }
        assert!(XorbObjectInfoV1::deserialize(&mut Cursor::new(buf)).is_err());
    }

    #[test]
    fn v1_deserialize_inconsistent_num_chunks() {
        let info = make_v1_info(1, b"test");
        let mut buf = Vec::new();
        info.serialize(&mut buf).unwrap();
        // Boundary section num_chunks at offset 92 for n=1
        if buf.len() > 95 {
            buf[92..96].copy_from_slice(&2u32.to_le_bytes());
        }
        assert!(XorbObjectInfoV1::deserialize(&mut Cursor::new(buf)).is_err());
    }

    #[test]
    fn v1_deserialize_inconsistent_final_num_chunks() {
        let info = make_v1_info(1, b"test");
        let mut buf = Vec::new();
        info.serialize(&mut buf).unwrap();
        // Final num_chunks at offset 120 for n=1
        assert!(buf.len() > 127);
        buf[120..128].copy_from_slice(&99u64.to_le_bytes());
        assert!(XorbObjectInfoV1::deserialize(&mut Cursor::new(buf)).is_err());
    }

    #[test]
    fn v1_deserialize_incorrect_hashes_section_offset() {
        let info = make_v1_info(1, b"test");
        let mut buf = Vec::new();
        info.serialize(&mut buf).unwrap();
        // hashes_section_offset at offset 128 for n=1
        assert!(buf.len() > 135);
        buf[128..136].copy_from_slice(&0u64.to_le_bytes());
        assert!(XorbObjectInfoV1::deserialize(&mut Cursor::new(buf)).is_err());
    }

    #[test]
    fn v1_deserialize_incorrect_boundary_section_offset() {
        let info = make_v1_info(1, b"test");
        let mut buf = Vec::new();
        info.serialize(&mut buf).unwrap();
        // boundary_section_offset at offset 136 for n=1
        assert!(buf.len() > 143);
        buf[136..144].copy_from_slice(&0u64.to_le_bytes());
        assert!(XorbObjectInfoV1::deserialize(&mut Cursor::new(buf)).is_err());
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
        let info = make_v1_info(1, b"frominfo");
        let len = info.serialized_length() as u64;
        let obj = XorbObject::from_info(info);
        assert_eq!(obj.info_length, len);
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
    fn validate_xorb_object_success_path() {
        use crate::xorb_object::xorb_format_test_utils::{ChunkSize, build_xorb_object};
        let (obj, chunk_data, _, _) =
            build_xorb_object(1, ChunkSize::Fixed(100), CompressionScheme::LZ4).unwrap();
        let mut buf = Vec::new();
        buf.extend_from_slice(&chunk_data);
        let info_len = obj.info.serialize(&mut buf).unwrap();
        buf.extend_from_slice(&(info_len as u64).to_le_bytes());
        let hash = obj.info.xorb_hash;
        let result = XorbObject::validate_xorb_object(&mut Cursor::new(&buf), &hash).unwrap();
        assert!(result.is_some());
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
            xorb.info
                .chunk_boundary_offsets
                .push(writer.position());
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
