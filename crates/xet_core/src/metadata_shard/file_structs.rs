use std::io::{Cursor, Read, Write};
use std::mem::size_of;

use bytes::Bytes;
use serde::Serialize;

use super::shard_file::MDB_FILE_INFO_ENTRY_SIZE;
use super::xorb_structs::{XorbChunkSequenceEntry, XorbChunkSequenceHeader};
use crate::{merklehash::MerkleHash, utils::serialization_utils::*};

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
    pub num_entries: u64,
    pub _unused: u64,
    pub _pad: u64,
}

impl FileDataSequenceHeader {
    pub fn new(
        file_hash: MerkleHash,
        num_entries: u64,
        contains_verification: bool,
        contains_metadata_ext: bool,
    ) -> Self {
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
            num_entries,
            #[cfg(test)]
            _unused: 126846135456846514u64,
            #[cfg(not(test))]
            _unused: 0,
            _pad: 0,
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
            write_u64(writer, self.num_entries)?;
            write_u64(writer, self._unused)?;
            write_u64(writer, self._pad)?;
        }
        writer.write_all(&buf[..])?;
        Ok(size_of::<FileDataSequenceHeader>())
    }

    pub fn deserialize<R: Read>(reader: &mut R, _version: u64) -> Result<Self, std::io::Error> {
        let mut v = [0u8; size_of::<Self>()];
        reader.read_exact(&mut v[..])?;
        let mut reader_curs = Cursor::new(&v);
        let reader = &mut reader_curs;
        Ok(Self {
            file_hash: read_hash(reader)?,
            file_flags: read_u32(reader)?,
            num_entries: read_u64(reader)?,
            _unused: read_u64(reader)?,
            _pad: read_u64(reader)?,
        })
    }

    pub fn contains_metadata_ext(&self) -> bool {
        (self.file_flags & MDB_FILE_FLAG_METADATA_EXT_MASK) != 0
    }

    pub fn contains_verification(&self) -> bool {
        (self.file_flags & MDB_FILE_FLAG_VERIFICATION_MASK) != 0
    }

    pub fn num_info_entry_following(&self) -> u64 {
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
    pub unpacked_segment_bytes: u64,
    pub chunk_index_start: u64,
    pub chunk_index_end: u64,
}

impl FileDataSequenceEntry {
    pub fn new(
        xorb_hash: MerkleHash,
        unpacked_segment_bytes: u64,
        chunk_index_start: u64,
        chunk_index_end: u64,
    ) -> Self {
        Self {
            xorb_hash,
            xorb_flags: MDB_DEFAULT_FILE_FLAG,
            unpacked_segment_bytes,
            chunk_index_start,
            chunk_index_end,
        }
    }

    pub fn from_xorb_entries(
        metadata: &XorbChunkSequenceHeader,
        chunks: &[XorbChunkSequenceEntry],
        chunk_index_start: u64,
        chunk_index_end: u64,
    ) -> Self {
        if chunks.is_empty() {
            return Self::default();
        }
        Self {
            xorb_hash: metadata.xorb_hash,
            xorb_flags: metadata.xorb_flags,
            unpacked_segment_bytes: chunks.iter().map(|sb| sb.unpacked_segment_bytes).sum(),
            chunk_index_start,
            chunk_index_end,
        }
    }

    pub fn serialize<W: Write>(&self, writer: &mut W) -> Result<usize, std::io::Error> {
        let mut buf = [0u8; size_of::<Self>()];
        {
            let mut writer_cur = Cursor::new(&mut buf[..]);
            let writer = &mut writer_cur;
            write_hash(writer, &self.xorb_hash)?;
            write_u32(writer, self.xorb_flags)?;
            write_u64(writer, self.unpacked_segment_bytes)?;
            write_u64(writer, self.chunk_index_start)?;
            write_u64(writer, self.chunk_index_end)?;
        }
        writer.write_all(&buf[..])?;
        Ok(size_of::<FileDataSequenceEntry>())
    }

    pub fn deserialize<R: Read>(reader: &mut R, _version: u64) -> Result<Self, std::io::Error> {
        let mut v = [0u8; size_of::<FileDataSequenceEntry>()];
        reader.read_exact(&mut v[..])?;
        let mut reader_curs = Cursor::new(&v);
        let reader = &mut reader_curs;
        Ok(Self {
            xorb_hash: read_hash(reader)?,
            xorb_flags: read_u32(reader)?,
            unpacked_segment_bytes: read_u64(reader)?,
            chunk_index_start: read_u64(reader)?,
            chunk_index_end: read_u64(reader)?,
        })
    }
}

#[derive(Clone, Debug, Default, PartialEq, Serialize)]
pub struct FileVerificationEntry {
    pub range_hash: MerkleHash,
    pub _unused: [u64; 2],
    pub _pad: [u64; 2],
}

impl FileVerificationEntry {
    pub fn new(range_hash: MerkleHash) -> Self {
        Self {
            range_hash,
            _unused: Default::default(),
            _pad: Default::default(),
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

    pub fn deserialize<R: Read>(reader: &mut R, _version: u64) -> Result<Self, std::io::Error> {
        let mut v = [0u8; size_of::<Self>()];
        reader.read_exact(&mut v[..])?;
        let mut reader_curs = Cursor::new(&v);
        let reader = &mut reader_curs;
        let mut unused = [0u64; 2];
        let mut pad = [0u64; 2];
        Ok(Self {
            range_hash: read_hash(reader)?,
            _unused: {
                read_u64s(reader, &mut unused)?;
                unused
            },
            _pad: {
                read_u64s(reader, &mut pad)?;
                pad
            },
        })
    }
}

#[derive(Clone, Debug, Default, PartialEq, Serialize)]
pub struct FileMetadataExt {
    pub sha256: Sha256,
    pub _unused: [u64; 2],
    pub _pad: [u64; 2],
}

impl FileMetadataExt {
    pub fn new(sha256: Sha256) -> Self {
        Self {
            sha256,
            _unused: Default::default(),
            _pad: Default::default(),
        }
    }

    pub fn serialize<W: Write>(&self, writer: &mut W) -> Result<usize, std::io::Error> {
        let mut buf = [0u8; size_of::<Self>()];
        {
            let mut writer = Cursor::new(&mut buf[..]);
            write_hash(&mut writer, &self.sha256)?;
            write_u64s(&mut writer, &self._unused)?;
            write_u64s(&mut writer, &self._pad)?;
        }
        writer.write_all(&buf)?;
        Ok(size_of::<Self>())
    }

    pub fn deserialize<R: Read>(reader: &mut R, _version: u64) -> Result<Self, std::io::Error> {
        let mut v = [0u8; size_of::<Self>()];
        reader.read_exact(&mut v[..])?;
        let mut reader_curs = Cursor::new(&v);
        let reader = &mut reader_curs;
        let mut unused = [0u64; 2];
        let mut pad = [0u64; 2];
        Ok(Self {
            sha256: read_hash(reader)?,
            _unused: {
                read_u64s(reader, &mut unused)?;
                unused
            },
            _pad: {
                read_u64s(reader, &mut pad)?;
                pad
            },
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
            + self.metadata.num_info_entry_following() * MDB_FILE_INFO_ENTRY_SIZE as u64
    }

    pub fn file_size(&self) -> u64 {
        self.segments
            .iter()
            .map(|fse| fse.unpacked_segment_bytes)
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

    pub fn deserialize<R: Read>(reader: &mut R, version: u64) -> Result<Option<Self>, std::io::Error> {
        let metadata = FileDataSequenceHeader::deserialize(reader, version)?;
        if metadata.is_bookend() {
            return Ok(None);
        }

        let num_entries = metadata.num_entries as usize;
        let mut segments = Vec::with_capacity(num_entries);
        for _ in 0..num_entries {
            segments.push(FileDataSequenceEntry::deserialize(reader, version)?);
        }

        let mut verification = Vec::with_capacity(num_entries);
        if metadata.contains_verification() {
            for _ in 0..num_entries {
                verification.push(FileVerificationEntry::deserialize(reader, version)?);
            }
        }
        let metadata_ext = metadata
            .contains_metadata_ext()
            .then(|| FileMetadataExt::deserialize(reader, version))
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
        let header = FileDataSequenceHeader::deserialize(&mut Cursor::new(&data), 3)?;
        Self::from_data_and_header(header, data)
    }

    pub fn from_data_and_header(
        header: FileDataSequenceHeader,
        data: Bytes,
    ) -> std::io::Result<Self> {
        let n = header.num_entries as usize;
        let contains_verification = header.contains_verification();
        let contains_metadata_ext = header.contains_metadata_ext();

        let n_structs = 1
            + n
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
        FileDataSequenceEntry::deserialize(
            &mut Cursor::new(
                &self.data[((1 + idx) * MDB_FILE_INFO_ENTRY_SIZE)..],
            ),
            3,
        )
        .expect("bookkeeping error on data bounds for entry")
    }

    pub fn verification(&self, idx: usize) -> FileVerificationEntry {
        FileVerificationEntry::deserialize(
            &mut Cursor::new(
                &self.data[((1 + self.num_entries() + idx) * MDB_FILE_INFO_ENTRY_SIZE)..],
            ),
            3,
        )
        .expect("bookkeeping error on data bounds for verification")
    }

    pub fn byte_size(&self, with_verification: bool) -> usize {
        let n = self.num_entries();
        let n_structs = 1
            + n
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
            (0..view.num_entries())
                .map(|i| view.verification(i))
                .collect()
        } else {
            vec![]
        };
        MDBFileInfo {
            metadata: FileDataSequenceHeader::new(
                view.file_hash(),
                segments.len() as u64,
                view.contains_verification(),
                view.contains_metadata_ext(),
            ),
            segments,
            verification,
            metadata_ext: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use bytes::Bytes;

    use super::*;
    use crate::merklehash::compute_data_hash;

    // ======= FileDataSequenceHeader =======

    #[test]
    fn header_new_basic() {
        let h = FileDataSequenceHeader::new(compute_data_hash(b"f"), 3, false, false);
        assert_eq!(h.file_flags, 0);
        assert_eq!(h.num_entries, 3);
    }

    #[test]
    fn header_new_with_verification() {
        let h = FileDataSequenceHeader::new(compute_data_hash(b"f"), 1, true, false);
        assert!(h.contains_verification());
        assert!(!h.contains_metadata_ext());
    }

    #[test]
    fn header_new_with_metadata_ext() {
        let h = FileDataSequenceHeader::new(compute_data_hash(b"f"), 1, false, true);
        assert!(!h.contains_verification());
        assert!(h.contains_metadata_ext());
    }

    #[test]
    fn header_new_with_both() {
        let h = FileDataSequenceHeader::new(compute_data_hash(b"f"), 2, true, true);
        assert!(h.contains_verification());
        assert!(h.contains_metadata_ext());
    }

    #[test]
    fn header_new_with_neither() {
        let h = FileDataSequenceHeader::new(compute_data_hash(b"f"), 0, false, false);
        assert!(!h.contains_verification());
        assert!(!h.contains_metadata_ext());
    }

    #[test]
    fn header_bookend_and_is_bookend() {
        let b = FileDataSequenceHeader::bookend();
        assert!(b.is_bookend());
        let h = FileDataSequenceHeader::new(MerkleHash::default(), 0, false, false);
        assert!(!h.is_bookend());
    }

    #[test]
    fn header_serialize_roundtrip() {
        let h = FileDataSequenceHeader::new(compute_data_hash(b"f"), 5, true, false);
        let mut buf = Vec::new();
        h.serialize(&mut buf).unwrap();
        let mut r = Cursor::new(&buf);
        let h2 = FileDataSequenceHeader::deserialize(&mut r, 3).unwrap();
        assert_eq!(h.file_hash, h2.file_hash);
        assert_eq!(h.file_flags, h2.file_flags);
        assert_eq!(h.num_entries, h2.num_entries);
    }

    #[test]
    fn header_num_info_entry_following_various() {
        // No flags: just entries
        let h = FileDataSequenceHeader::new(MerkleHash::default(), 3, false, false);
        assert_eq!(h.num_info_entry_following(), 3);
        // Verification: entries * 2
        let h = FileDataSequenceHeader::new(MerkleHash::default(), 3, true, false);
        assert_eq!(h.num_info_entry_following(), 6);
        // Metadata ext: entries + 1
        let h = FileDataSequenceHeader::new(MerkleHash::default(), 3, false, true);
        assert_eq!(h.num_info_entry_following(), 4);
        // Both: entries * 2 + 1
        let h = FileDataSequenceHeader::new(MerkleHash::default(), 3, true, true);
        assert_eq!(h.num_info_entry_following(), 7);
        // Zero entries with both flags: 0 * 2 + 1 = 1
        let h = FileDataSequenceHeader::new(MerkleHash::default(), 0, true, true);
        assert_eq!(h.num_info_entry_following(), 1);
    }

    #[test]
    fn header_default() {
        let h = FileDataSequenceHeader::default();
        assert_eq!(h.num_entries, 0);
        assert_eq!(h.file_flags, 0);
    }

    // ======= FileDataSequenceEntry =======

    #[test]
    fn entry_new_basic() {
        let e = FileDataSequenceEntry::new(compute_data_hash(b"seg"), 1024, 0, 512);
        assert_eq!(e.unpacked_segment_bytes, 1024);
        assert_eq!(e.chunk_index_start, 0);
        assert_eq!(e.chunk_index_end, 512);
        assert_eq!(e.xorb_flags, 0);
    }

    #[test]
    fn entry_serialize_roundtrip() {
        let e = FileDataSequenceEntry::new(compute_data_hash(b"e"), 200, 10, 20);
        let mut buf = Vec::new();
        e.serialize(&mut buf).unwrap();
        let mut r = Cursor::new(&buf);
        let e2 = FileDataSequenceEntry::deserialize(&mut r, 3).unwrap();
        assert_eq!(e, e2);
    }

    #[test]
    fn entry_from_xorb_entries() {
        let hash = compute_data_hash(b"xorb");
        let metadata = XorbChunkSequenceHeader::new(hash, 2, 200);
        let chunks = vec![
            XorbChunkSequenceEntry::new(compute_data_hash(b"c1"), 100, 0),
            XorbChunkSequenceEntry::new(compute_data_hash(b"c2"), 100, 100),
        ];
        let e = FileDataSequenceEntry::from_xorb_entries(&metadata, &chunks, 0, 2);
        assert_eq!(e.xorb_hash, hash);
        assert_eq!(e.unpacked_segment_bytes, 200);
        assert_eq!(e.chunk_index_start, 0);
        assert_eq!(e.chunk_index_end, 2);
    }

    #[test]
    fn entry_from_xorb_entries_empty_chunks() {
        let metadata = XorbChunkSequenceHeader::new(MerkleHash::default(), 0, 0);
        let e = FileDataSequenceEntry::from_xorb_entries(&metadata, &[], 0, 0);
        assert_eq!(e, FileDataSequenceEntry::default());
    }

    #[test]
    fn entry_default() {
        let e = FileDataSequenceEntry::default();
        assert_eq!(e.xorb_hash, MerkleHash::default());
        assert_eq!(e.unpacked_segment_bytes, 0);
    }

    // ======= FileVerificationEntry =======

    #[test]
    fn verification_entry_new() {
        let v = FileVerificationEntry::new(compute_data_hash(b"v"));
        assert_eq!(v.range_hash, compute_data_hash(b"v"));
    }

    #[test]
    fn verification_entry_serialize_roundtrip() {
        let v = FileVerificationEntry::new(compute_data_hash(b"v1"));
        let mut buf = Vec::new();
        v.serialize(&mut buf).unwrap();
        let mut r = Cursor::new(&buf);
        let v2 = FileVerificationEntry::deserialize(&mut r, 3).unwrap();
        assert_eq!(v.range_hash, v2.range_hash);
    }

    #[test]
    fn verification_entry_default() {
        let v = FileVerificationEntry::default();
        assert_eq!(v.range_hash, MerkleHash::default());
    }

    // ======= FileMetadataExt =======

    #[test]
    fn metadata_ext_new() {
        let m = FileMetadataExt::new(compute_data_hash(b"sha256"));
        assert_eq!(m.sha256, compute_data_hash(b"sha256"));
    }

    #[test]
    fn metadata_ext_serialize_roundtrip() {
        let m = FileMetadataExt::new(compute_data_hash(b"ext"));
        let mut buf = Vec::new();
        m.serialize(&mut buf).unwrap();
        let mut r = Cursor::new(&buf);
        let m2 = FileMetadataExt::deserialize(&mut r, 3).unwrap();
        assert_eq!(m.sha256, m2.sha256);
    }

    #[test]
    fn metadata_ext_default() {
        let m = FileMetadataExt::default();
        assert_eq!(m.sha256, MerkleHash::default());
    }

    // ======= MDBFileInfo =======

    #[test]
    fn file_info_num_bytes() {
        let info = MDBFileInfo {
            metadata: FileDataSequenceHeader::new(MerkleHash::default(), 2, false, false),
            segments: vec![
                FileDataSequenceEntry::new(MerkleHash::default(), 100, 0, 50),
                FileDataSequenceEntry::new(MerkleHash::default(), 200, 50, 100),
            ],
            verification: vec![],
            metadata_ext: None,
        };
        assert!(info.num_bytes() > 0);
    }

    #[test]
    fn file_info_file_size() {
        let info = MDBFileInfo {
            metadata: FileDataSequenceHeader::new(MerkleHash::default(), 2, false, false),
            segments: vec![
                FileDataSequenceEntry::new(MerkleHash::default(), 100, 0, 50),
                FileDataSequenceEntry::new(MerkleHash::default(), 200, 50, 100),
            ],
            verification: vec![],
            metadata_ext: None,
        };
        assert_eq!(info.file_size(), 300);
    }

    #[test]
    fn file_info_serialize_roundtrip_no_flags() {
        let info = MDBFileInfo {
            metadata: FileDataSequenceHeader::new(compute_data_hash(b"f"), 2, false, false),
            segments: vec![
                FileDataSequenceEntry::new(MerkleHash::default(), 50, 0, 25),
                FileDataSequenceEntry::new(MerkleHash::default(), 75, 25, 50),
            ],
            verification: vec![],
            metadata_ext: None,
        };
        let mut buf = Vec::new();
        info.serialize(&mut buf).unwrap();
        let mut r = Cursor::new(&buf);
        let info2 = MDBFileInfo::deserialize(&mut r, 3).unwrap().unwrap();
        assert_eq!(info.metadata.file_hash, info2.metadata.file_hash);
        assert_eq!(info.segments.len(), info2.segments.len());
    }

    #[test]
    fn file_info_serialize_with_verification() {
        let info = MDBFileInfo {
            metadata: FileDataSequenceHeader::new(compute_data_hash(b"f"), 1, true, false),
            segments: vec![FileDataSequenceEntry::new(
                MerkleHash::default(),
                50, 0, 25,
            )],
            verification: vec![FileVerificationEntry::new(compute_data_hash(b"v"))],
            metadata_ext: None,
        };
        let mut buf = Vec::new();
        info.serialize(&mut buf).unwrap();
        let mut r = Cursor::new(&buf);
        let info2 = MDBFileInfo::deserialize(&mut r, 3).unwrap().unwrap();
        assert!(info2.contains_verification());
        assert_eq!(info2.verification.len(), 1);
    }

    #[test]
    fn file_info_serialize_with_metadata_ext() {
        let info = MDBFileInfo {
            metadata: FileDataSequenceHeader::new(compute_data_hash(b"f"), 1, false, true),
            segments: vec![FileDataSequenceEntry::new(
                MerkleHash::default(),
                50, 0, 25,
            )],
            verification: vec![],
            metadata_ext: Some(FileMetadataExt::new(compute_data_hash(b"ext"))),
        };
        let mut buf = Vec::new();
        info.serialize(&mut buf).unwrap();
        let mut r = Cursor::new(&buf);
        let info2 = MDBFileInfo::deserialize(&mut r, 3).unwrap().unwrap();
        assert!(info2.contains_metadata_ext());
        assert!(info2.metadata_ext.is_some());
    }

    #[test]
    fn file_info_serialize_with_both() {
        let info = MDBFileInfo {
            metadata: FileDataSequenceHeader::new(compute_data_hash(b"f"), 1, true, true),
            segments: vec![FileDataSequenceEntry::new(
                MerkleHash::default(),
                50, 0, 25,
            )],
            verification: vec![FileVerificationEntry::new(compute_data_hash(b"v"))],
            metadata_ext: Some(FileMetadataExt::new(compute_data_hash(b"ext"))),
        };
        let mut buf = Vec::new();
        info.serialize(&mut buf).unwrap();
        let mut r = Cursor::new(&buf);
        let info2 = MDBFileInfo::deserialize(&mut r, 3).unwrap().unwrap();
        assert!(info2.contains_verification());
        assert!(info2.contains_metadata_ext());
    }

    #[test]
    fn file_info_deserialize_bookend_returns_none() {
        let b = FileDataSequenceHeader::bookend();
        let mut buf = Vec::new();
        b.serialize(&mut buf).unwrap();
        let mut r = Cursor::new(&buf);
        assert!(MDBFileInfo::deserialize(&mut r, 3).unwrap().is_none());
    }

    #[test]
    fn file_info_num_bytes_with_verification_and_metadata_ext() {
        let info = MDBFileInfo {
            metadata: FileDataSequenceHeader::new(compute_data_hash(b"f"), 2, true, true),
            segments: vec![
                FileDataSequenceEntry::new(MerkleHash::default(), 50, 0, 25),
                FileDataSequenceEntry::new(MerkleHash::default(), 75, 25, 50),
            ],
            verification: vec![
                FileVerificationEntry::new(compute_data_hash(b"v1")),
                FileVerificationEntry::new(compute_data_hash(b"v2")),
            ],
            metadata_ext: Some(FileMetadataExt::new(compute_data_hash(b"ext"))),
        };
        let nbytes = info.num_bytes();
        assert!(nbytes > 0);
        assert_eq!(info.file_size(), 125);
    }

    #[test]
    fn file_info_default() {
        let info = MDBFileInfo::default();
        assert!(info.segments.is_empty());
        assert!(info.verification.is_empty());
        assert!(info.metadata_ext.is_none());
    }

    #[test]
    fn file_info_contains_methods() {
        let info = MDBFileInfo {
            metadata: FileDataSequenceHeader::new(compute_data_hash(b"f"), 1, true, true),
            segments: vec![FileDataSequenceEntry::new(
                MerkleHash::default(),
                50, 0, 25,
            )],
            verification: vec![FileVerificationEntry::new(compute_data_hash(b"v"))],
            metadata_ext: Some(FileMetadataExt::new(compute_data_hash(b"ext"))),
        };
        assert!(info.contains_verification());
        assert!(info.contains_metadata_ext());

        let info2 = MDBFileInfo {
            metadata: FileDataSequenceHeader::new(compute_data_hash(b"f"), 0, false, false),
            segments: vec![],
            verification: vec![],
            metadata_ext: None,
        };
        assert!(!info2.contains_verification());
        assert!(!info2.contains_metadata_ext());
    }

    // ======= MDBFileInfoView =======

    #[test]
    fn view_basic() {
        let header = FileDataSequenceHeader::new(compute_data_hash(b"f"), 1, false, false);
        let entry = FileDataSequenceEntry::new(compute_data_hash(b"e"), 100, 0, 50);
        let mut buf = Vec::new();
        header.serialize(&mut buf).unwrap();
        entry.serialize(&mut buf).unwrap();
        let view = MDBFileInfoView::new(Bytes::from(buf)).unwrap();
        assert_eq!(view.num_entries(), 1);
        assert_eq!(view.file_hash(), compute_data_hash(b"f"));
        assert_eq!(view.file_flags(), header.file_flags);
        assert!(!view.contains_verification());
        assert!(!view.contains_metadata_ext());
    }

    #[test]
    fn view_with_verification_and_metadata_ext() {
        let header = FileDataSequenceHeader::new(compute_data_hash(b"f"), 1, true, true);
        let entry = FileDataSequenceEntry::new(compute_data_hash(b"e"), 100, 0, 50);
        let ver = FileVerificationEntry::new(compute_data_hash(b"v"));
        let met = FileMetadataExt::new(compute_data_hash(b"m"));
        let mut buf = Vec::new();
        header.serialize(&mut buf).unwrap();
        entry.serialize(&mut buf).unwrap();
        ver.serialize(&mut buf).unwrap();
        met.serialize(&mut buf).unwrap();
        let expected_size = 4 * crate::metadata_shard::shard_format::MDB_FILE_INFO_ENTRY_SIZE;
        buf.resize(expected_size, 0);

        let view = MDBFileInfoView::new(Bytes::from(buf)).unwrap();
        assert!(view.contains_verification());
        assert!(view.contains_metadata_ext());
        assert_eq!(view.header().file_hash, header.file_hash);

        let e = view.entry(0);
        assert_eq!(e.unpacked_segment_bytes, 100);
        let v = view.verification(0);
        assert_eq!(v.range_hash, compute_data_hash(b"v"));
    }

    #[test]
    fn view_byte_size_variants() {
        let header = FileDataSequenceHeader::new(compute_data_hash(b"f"), 2, true, false);
        let e1 = FileDataSequenceEntry::new(MerkleHash::default(), 50, 0, 25);
        let e2 = FileDataSequenceEntry::new(MerkleHash::default(), 75, 25, 50);
        let v1 = FileVerificationEntry::new(compute_data_hash(b"v1"));
        let v2 = FileVerificationEntry::new(compute_data_hash(b"v2"));
        let mut buf = Vec::new();
        header.serialize(&mut buf).unwrap();
        e1.serialize(&mut buf).unwrap();
        e2.serialize(&mut buf).unwrap();
        v1.serialize(&mut buf).unwrap();
        v2.serialize(&mut buf).unwrap();
        let size = (1 + 2 + 2) * crate::metadata_shard::shard_format::MDB_FILE_INFO_ENTRY_SIZE;
        buf.resize(size, 0);

        let view = MDBFileInfoView::new(Bytes::from(buf)).unwrap();
        assert_eq!(
            view.byte_size(true),
            (1 + 2 + 2) * crate::metadata_shard::shard_format::MDB_FILE_INFO_ENTRY_SIZE
        );
        assert_eq!(
            view.byte_size(false),
            (1 + 2) * crate::metadata_shard::shard_format::MDB_FILE_INFO_ENTRY_SIZE
        );
    }

    #[test]
    fn view_from_data_too_small() {
        let header = FileDataSequenceHeader::new(MerkleHash::default(), 2, false, false);
        assert!(MDBFileInfoView::from_data_and_header(header, Bytes::from(vec![0u8; 5])).is_err());
    }

    #[test]
    fn view_from_data_too_small_with_flags() {
        let header = FileDataSequenceHeader::new(MerkleHash::default(), 1, true, true);
        assert!(MDBFileInfoView::from_data_and_header(header, Bytes::from(vec![0u8; 10])).is_err());
    }

    #[test]
    fn view_bytes_returns_clone() {
        let header = FileDataSequenceHeader::new(MerkleHash::default(), 0, false, false);
        let mut buf = Vec::new();
        header.serialize(&mut buf).unwrap();
        let view = MDBFileInfoView::new(Bytes::from(buf.clone())).unwrap();
        assert_eq!(view.bytes().to_vec(), buf);
    }

    #[test]
    fn view_into_mdb_file_info() {
        let header = FileDataSequenceHeader::new(compute_data_hash(b"f"), 1, false, false);
        let entry = FileDataSequenceEntry::new(compute_data_hash(b"e"), 100, 0, 50);
        let mut buf = Vec::new();
        header.serialize(&mut buf).unwrap();
        entry.serialize(&mut buf).unwrap();
        let view = MDBFileInfoView::new(Bytes::from(buf)).unwrap();
        let info: MDBFileInfo = (&view).into();
        assert_eq!(info.segments.len(), 1);
        assert_eq!(info.segments[0].unpacked_segment_bytes, 100);
    }

    #[test]
    fn view_into_mdb_file_info_with_verification() {
        let header = FileDataSequenceHeader::new(compute_data_hash(b"f"), 1, true, false);
        let entry = FileDataSequenceEntry::new(compute_data_hash(b"e"), 100, 0, 50);
        let ver = FileVerificationEntry::new(compute_data_hash(b"v"));
        let mut buf = Vec::new();
        header.serialize(&mut buf).unwrap();
        entry.serialize(&mut buf).unwrap();
        ver.serialize(&mut buf).unwrap();
        let size = (1 + 1 + 1) * crate::metadata_shard::shard_format::MDB_FILE_INFO_ENTRY_SIZE;
        buf.resize(size, 0);
        let view = MDBFileInfoView::new(Bytes::from(buf)).unwrap();
        let info: MDBFileInfo = (&view).into();
        assert_eq!(info.segments.len(), 1);
        assert_eq!(info.verification.len(), 1);
    }

    #[test]
    fn file_flag_constants() {
        assert_eq!(MDB_DEFAULT_FILE_FLAG, 0);
        assert_eq!(MDB_FILE_FLAG_WITH_VERIFICATION, 1u32 << 31);
        assert_eq!(MDB_FILE_FLAG_VERIFICATION_MASK, 1u32 << 31);
        assert_eq!(MDB_FILE_FLAG_WITH_METADATA_EXT, 1u32 << 30);
        assert_eq!(MDB_FILE_FLAG_METADATA_EXT_MASK, 1u32 << 30);
    }
}
