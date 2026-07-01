use std::fmt::Debug;
use std::io::{Cursor, Read, Write};
use std::mem::size_of;

use bytes::Bytes;
use serde::Serialize;

use super::shard_file::MDB_FILE_INFO_ENTRY_SIZE;
use super::xorb_structs::{XorbChunkSequenceEntry, XorbChunkSequenceHeader};
use crate::merklehash::MerkleHash;
use crate::utils::serialization_utils::*;

pub const MDB_DEFAULT_FILE_FLAG: u32 = 0;
pub const MDB_FILE_FLAG_WITH_VERIFICATION: u32 = 1 << 31;
pub const MDB_FILE_FLAG_VERIFICATION_MASK: u32 = 1 << 31;
pub const MDB_FILE_FLAG_WITH_METADATA_EXT: u32 = 1 << 30;
pub const MDB_FILE_FLAG_METADATA_EXT_MASK: u32 = 1 << 30;

pub type Sha256 = MerkleHash;

#[derive(Clone, Debug, Default, PartialEq, Serialize)]
#[repr(C)]
pub struct FileDataSequenceHeader {
    pub file_hash: MerkleHash,
    pub file_flags: u32,
    pub num_entries: u32,
    pub _unused: u64,
}

impl FileDataSequenceHeader {
    pub fn new<I: TryInto<u32>>(
        file_hash: MerkleHash,
        num_entries: I,
        contains_verification: bool,
        contains_metadata_ext: bool,
    ) -> Self
    where
        <I as TryInto<u32>>::Error: Debug,
    {
        let verification_flag = if contains_verification {
            MDB_FILE_FLAG_WITH_VERIFICATION
        } else {
            Default::default()
        };
        let metadata_ext_flag = if contains_metadata_ext {
            MDB_FILE_FLAG_WITH_METADATA_EXT
        } else {
            Default::default()
        };
        let file_flags = MDB_DEFAULT_FILE_FLAG | verification_flag | metadata_ext_flag;
        Self {
            file_hash,
            file_flags,
            num_entries: num_entries.try_into().unwrap(),
            #[cfg(test)]
            _unused: 126846135456846514u64,
            #[cfg(not(test))]
            _unused: 0,
        }
    }

    pub fn bookend() -> Self {
        Self {
            file_hash: [!0u64; 4].into(),
            ..Default::default()
        }
    }

    pub fn is_bookend(&self) -> bool {
        self.file_hash == [!0u64; 4].into()
    }

    pub fn serialize<W: Write>(&self, writer: &mut W) -> Result<usize, std::io::Error> {
        let mut buf = [0u8; size_of::<Self>()];
        {
            let mut writer_cur = Cursor::new(&mut buf[..]);
            let writer = &mut writer_cur;
            write_hash(writer, &self.file_hash)?;
            write_u32(writer, self.file_flags)?;
            write_u32(writer, self.num_entries)?;
            write_u64(writer, self._unused)?;
        }
        writer.write_all(&buf[..])?;
        Ok(size_of::<FileDataSequenceHeader>())
    }

    pub fn deserialize<R: Read>(reader: &mut R) -> Result<Self, std::io::Error> {
        let mut v = [0u8; size_of::<Self>()];
        reader.read_exact(&mut v[..])?;
        let mut reader_curs = Cursor::new(&v);
        let reader = &mut reader_curs;
        Ok(Self {
            file_hash: read_hash(reader)?,
            file_flags: read_u32(reader)?,
            num_entries: read_u32(reader)?,
            _unused: read_u64(reader)?,
        })
    }

    pub fn contains_metadata_ext(&self) -> bool {
        (self.file_flags & MDB_FILE_FLAG_METADATA_EXT_MASK) != 0
    }

    pub fn contains_verification(&self) -> bool {
        (self.file_flags & MDB_FILE_FLAG_VERIFICATION_MASK) != 0
    }

    pub fn num_info_entry_following(&self) -> u32 {
        let num_metadata_ext = if self.contains_metadata_ext() { 1 } else { 0 };
        if self.contains_verification() {
            self.num_entries * 2 + num_metadata_ext
        } else {
            self.num_entries + num_metadata_ext
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Serialize)]
#[repr(C)]
pub struct FileDataSequenceEntry {
    pub xorb_hash: MerkleHash,
    pub xorb_flags: u32,
    pub unpacked_segment_bytes: u32,
    pub chunk_index_start: u32,
    pub chunk_index_end: u32,
}

impl FileDataSequenceEntry {
    pub fn new<I1: TryInto<u32>>(
        xorb_hash: MerkleHash,
        unpacked_segment_bytes: I1,
        chunk_index_start: I1,
        chunk_index_end: I1,
    ) -> Self
    where
        <I1 as TryInto<u32>>::Error: Debug,
    {
        Self {
            xorb_hash,
            xorb_flags: MDB_DEFAULT_FILE_FLAG,
            unpacked_segment_bytes: unpacked_segment_bytes.try_into().unwrap(),
            chunk_index_start: chunk_index_start.try_into().unwrap(),
            chunk_index_end: chunk_index_end.try_into().unwrap(),
        }
    }

    pub fn from_xorb_entries<I1: TryInto<u32>>(
        metadata: &XorbChunkSequenceHeader,
        chunks: &[XorbChunkSequenceEntry],
        chunk_index_start: I1,
        chunk_index_end: I1,
    ) -> Self
    where
        <I1 as TryInto<u32>>::Error: Debug,
    {
        if chunks.is_empty() {
            return Self::default();
        }
        Self {
            xorb_hash: metadata.xorb_hash,
            xorb_flags: metadata.xorb_flags,
            unpacked_segment_bytes: chunks.iter().map(|sb| sb.unpacked_segment_bytes).sum(),
            chunk_index_start: chunk_index_start.try_into().unwrap(),
            chunk_index_end: chunk_index_end.try_into().unwrap(),
        }
    }

    pub fn serialize<W: Write>(&self, writer: &mut W) -> Result<usize, std::io::Error> {
        let mut buf = [0u8; size_of::<Self>()];
        {
            let mut writer_cur = Cursor::new(&mut buf[..]);
            let writer = &mut writer_cur;
            write_hash(writer, &self.xorb_hash)?;
            write_u32(writer, self.xorb_flags)?;
            write_u32(writer, self.unpacked_segment_bytes)?;
            write_u32(writer, self.chunk_index_start)?;
            write_u32(writer, self.chunk_index_end)?;
        }
        writer.write_all(&buf[..])?;
        Ok(size_of::<FileDataSequenceEntry>())
    }

    pub fn deserialize<R: Read>(reader: &mut R) -> Result<Self, std::io::Error> {
        let mut v = [0u8; size_of::<FileDataSequenceEntry>()];
        reader.read_exact(&mut v[..])?;
        let mut reader_curs = Cursor::new(&v);
        let reader = &mut reader_curs;
        Ok(Self {
            xorb_hash: read_hash(reader)?,
            xorb_flags: read_u32(reader)?,
            unpacked_segment_bytes: read_u32(reader)?,
            chunk_index_start: read_u32(reader)?,
            chunk_index_end: read_u32(reader)?,
        })
    }
}

#[derive(Clone, Debug, Default, PartialEq, Serialize)]
pub struct FileVerificationEntry {
    pub range_hash: MerkleHash,
    pub _unused: [u64; 2],
}

impl FileVerificationEntry {
    pub fn new(range_hash: MerkleHash) -> Self {
        Self {
            range_hash,
            _unused: Default::default(),
        }
    }

    pub fn serialize<W: Write>(&self, writer: &mut W) -> Result<usize, std::io::Error> {
        let mut buf = [0u8; size_of::<Self>()];
        {
            let mut writer = Cursor::new(&mut buf[..]);
            write_hash(&mut writer, &self.range_hash)?;
            write_u64s(&mut writer, &self._unused)?;
        }
        writer.write_all(&buf)?;
        Ok(size_of::<Self>())
    }

    pub fn deserialize<R: Read>(reader: &mut R) -> Result<Self, std::io::Error> {
        let mut v = [0u8; size_of::<Self>()];
        reader.read_exact(&mut v[..])?;
        let mut reader_curs = Cursor::new(&v);
        let reader = &mut reader_curs;
        Ok(Self {
            range_hash: read_hash(reader)?,
            _unused: Default::default(),
        })
    }
}

#[derive(Clone, Debug, Default, PartialEq, Serialize)]
pub struct FileMetadataExt {
    pub sha256: Sha256,
    pub _unused: [u64; 2],
}

impl FileMetadataExt {
    pub fn new(sha256: Sha256) -> Self {
        Self {
            sha256,
            _unused: Default::default(),
        }
    }

    pub fn serialize<W: Write>(&self, writer: &mut W) -> Result<usize, std::io::Error> {
        let mut buf = [0u8; size_of::<Self>()];
        {
            let mut writer = Cursor::new(&mut buf[..]);
            write_hash(&mut writer, &self.sha256)?;
            write_u64s(&mut writer, &self._unused)?;
        }
        writer.write_all(&buf)?;
        Ok(size_of::<Self>())
    }

    pub fn deserialize<R: Read>(reader: &mut R) -> Result<Self, std::io::Error> {
        let mut v = [0u8; size_of::<Self>()];
        reader.read_exact(&mut v[..])?;
        let mut reader_curs = Cursor::new(&v);
        let reader = &mut reader_curs;
        Ok(Self {
            sha256: read_hash(reader)?,
            _unused: Default::default(),
        })
    }
}

#[derive(Clone, Debug, Default, PartialEq, Serialize)]
pub struct MDBFileInfo {
    pub metadata: FileDataSequenceHeader,
    pub segments: Vec<FileDataSequenceEntry>,
    pub verification: Vec<FileVerificationEntry>,
    pub metadata_ext: Option<FileMetadataExt>,
}

impl MDBFileInfo {
    pub fn num_bytes(&self) -> u64 {
        size_of::<FileDataSequenceHeader>() as u64
            + self.metadata.num_info_entry_following() as u64 * MDB_FILE_INFO_ENTRY_SIZE as u64
    }

    pub fn file_size(&self) -> u64 {
        self.segments
            .iter()
            .map(|fse| fse.unpacked_segment_bytes as u64)
            .sum()
    }

    pub fn serialize<W: Write>(&self, writer: &mut W) -> Result<usize, std::io::Error> {
        let mut bytes_written = 0;
        bytes_written += self.metadata.serialize(writer)?;
        for file_segment in self.segments.iter() {
            bytes_written += file_segment.serialize(writer)?;
        }
        if self.contains_verification() {
            for verification in self.verification.iter() {
                bytes_written += verification.serialize(writer)?;
            }
        }
        if let Some(metadata_ext) = self.metadata_ext.as_ref() {
            bytes_written += metadata_ext.serialize(writer)?;
        }
        Ok(bytes_written)
    }

    pub fn deserialize<R: Read>(reader: &mut R) -> Result<Option<Self>, std::io::Error> {
        let metadata = FileDataSequenceHeader::deserialize(reader)?;
        if metadata.is_bookend() {
            return Ok(None);
        }

        let num_entries = metadata.num_entries as usize;
        let mut segments = Vec::with_capacity(num_entries);
        for _ in 0..num_entries {
            segments.push(FileDataSequenceEntry::deserialize(reader)?);
        }

        let mut verification = Vec::with_capacity(num_entries);
        if metadata.contains_verification() {
            for _ in 0..num_entries {
                verification.push(FileVerificationEntry::deserialize(reader)?);
            }
        }
        let metadata_ext = metadata
            .contains_metadata_ext()
            .then(|| FileMetadataExt::deserialize(reader))
            .transpose()?;

        Ok(Some(Self {
            metadata,
            segments,
            verification,
            metadata_ext,
        }))
    }

    pub fn contains_verification(&self) -> bool {
        self.metadata.contains_verification()
    }

    pub fn contains_metadata_ext(&self) -> bool {
        self.metadata.contains_metadata_ext()
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct MDBFileInfoView {
    header: FileDataSequenceHeader,
    data: Bytes,
}

impl MDBFileInfoView {
    pub fn new(data: Bytes) -> std::io::Result<Self> {
        let header = FileDataSequenceHeader::deserialize(&mut Cursor::new(&data))?;
        Self::from_data_and_header(header, data)
    }

    pub fn from_data_and_header(
        header: FileDataSequenceHeader,
        data: Bytes,
    ) -> std::io::Result<Self> {
        let n = header.num_entries as usize;
        let contains_verification = header.contains_verification();
        let contains_metadata_ext = header.contains_metadata_ext();

        let n_structs = 1 + n
            + (if contains_verification { n } else { 0 })
            + (if contains_metadata_ext { 1 } else { 0 });

        if data.len() < n_structs * MDB_FILE_INFO_ENTRY_SIZE {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "Provided slice too small to read MDBFileInfoView",
            ));
        }

        Ok(Self { header, data })
    }

    pub fn header(&self) -> &FileDataSequenceHeader {
        &self.header
    }

    pub fn num_entries(&self) -> usize {
        self.header.num_entries as usize
    }

    pub fn file_hash(&self) -> MerkleHash {
        self.header.file_hash
    }

    pub fn file_flags(&self) -> u32 {
        self.header.file_flags
    }

    pub fn contains_metadata_ext(&self) -> bool {
        self.header.contains_metadata_ext()
    }

    pub fn contains_verification(&self) -> bool {
        self.header.contains_verification()
    }

    pub fn entry(&self, idx: usize) -> FileDataSequenceEntry {
        FileDataSequenceEntry::deserialize(&mut Cursor::new(
            &self.data[((1 + idx) * MDB_FILE_INFO_ENTRY_SIZE)..],
        ))
        .expect("bookkeeping error on data bounds for entry")
    }

    pub fn verification(&self, idx: usize) -> FileVerificationEntry {
        FileVerificationEntry::deserialize(&mut Cursor::new(
            &self.data[((1 + self.num_entries() + idx) * MDB_FILE_INFO_ENTRY_SIZE)..],
        ))
        .expect("bookkeeping error on data bounds for verification")
    }

    pub fn byte_size(&self, with_verification: bool) -> usize {
        let n = self.num_entries();
        let n_structs = 1 + n
            + (if with_verification && self.contains_verification() {
                n
            } else {
                0
            })
            + (if self.contains_metadata_ext() { 1 } else { 0 });
        n_structs * MDB_FILE_INFO_ENTRY_SIZE
    }

    pub fn bytes(&self) -> Bytes {
        self.data.clone()
    }
}

impl From<&MDBFileInfoView> for MDBFileInfo {
    fn from(view: &MDBFileInfoView) -> Self {
        let segments: Vec<FileDataSequenceEntry> =
            (0..view.num_entries()).map(|i| view.entry(i)).collect();
        let verification = if view.contains_verification() {
            (0..view.num_entries()).map(|i| view.verification(i)).collect()
        } else {
            vec![]
        };
        MDBFileInfo {
            metadata: FileDataSequenceHeader::new(
                view.file_hash(),
                segments.len(),
                view.contains_verification(),
                view.contains_metadata_ext(),
            ),
            segments,
            verification,
            metadata_ext: None,
        }
    }
}
