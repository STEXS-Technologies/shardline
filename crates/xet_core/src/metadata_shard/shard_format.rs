use std::io::{Read, Write};
use std::mem::size_of;

use super::file_structs::*;
use super::shard_in_memory::MDBInMemoryShard;
use super::xorb_structs::*;
use crate::error::{CoreError, Result};
use crate::merklehash::HMACKey;
use crate::utils::serialization_utils::*;

pub const MDB_FILE_INFO_ENTRY_SIZE: usize = size_of::<[u64; 4]>() + 4 * size_of::<u64>();

static_assertions::const_assert!(size_of::<[u64; 4]>() + 4 * size_of::<u64>() == size_of::<FileDataSequenceHeader>());
static_assertions::const_assert!(size_of::<[u64; 4]>() + 4 * size_of::<u64>() == size_of::<FileDataSequenceEntry>());
static_assertions::const_assert!(size_of::<[u64; 4]>() + 4 * size_of::<u64>() == size_of::<FileVerificationEntry>());
static_assertions::const_assert!(size_of::<[u64; 4]>() + 4 * size_of::<u64>() == size_of::<FileMetadataExt>());

const MDB_XORB_INFO_ENTRY_SIZE: usize = size_of::<[u64; 4]>() + 4 * size_of::<u64>();
static_assertions::const_assert!(MDB_XORB_INFO_ENTRY_SIZE == size_of::<XorbChunkSequenceHeader>());
static_assertions::const_assert!(MDB_XORB_INFO_ENTRY_SIZE == size_of::<XorbChunkSequenceEntry>());

const MDB_SHARD_FOOTER_SIZE: i64 = size_of::<MDBShardFileFooter>() as i64;
const MDB_SHARD_HEADER_VERSION: u64 = 3;
const MDB_SHARD_FOOTER_VERSION: u64 = 2;

const MDB_SHARD_HEADER_TAG: [u8; 32] = [
    b'H', b'F', b'R', b'e', b'p', b'o', b'M', b'e', b't', b'a', b'D', b'a', b't', b'a', 0, 85, 105,
    103, 69, 106, 123, 129, 87, 131, 165, 189, 217, 92, 205, 209, 74, 169,
];

#[derive(Clone, Debug, PartialEq)]
pub struct MDBShardFileHeader {
    pub tag: [u8; 32],
    pub version: u64,
    pub footer_size: u64,
}

impl Default for MDBShardFileHeader {
    fn default() -> Self {
        Self {
            tag: MDB_SHARD_HEADER_TAG,
            version: MDB_SHARD_HEADER_VERSION,
            footer_size: MDB_SHARD_FOOTER_SIZE as u64,
        }
    }
}

impl MDBShardFileHeader {
    pub fn serialize<W: Write>(&self, writer: &mut W) -> Result<usize> {
        writer.write_all(&MDB_SHARD_HEADER_TAG)?;
        write_u64(writer, self.version)?;
        write_u64(writer, self.footer_size)?;
        Ok(size_of::<MDBShardFileHeader>())
    }

    pub fn deserialize<R: Read>(reader: &mut R) -> Result<Self> {
        let mut tag = [0u8; 32];
        reader.read_exact(&mut tag)?;
        if tag != MDB_SHARD_HEADER_TAG {
            return Err(CoreError::ShardVersion(
                "File does not appear to be a valid Merkle DB Shard file (Wrong Magic Number)."
                    .to_owned(),
            ));
        }
        Ok(Self {
            tag,
            version: read_u64(reader)?,
            footer_size: read_u64(reader)?,
        })
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct MDBShardFileFooter {
    pub version: u64,
    pub file_info_offset: u64,
    pub xorb_info_offset: u64,
    pub file_lookup_offset: u64,
    pub file_lookup_num_entry: u64,
    pub xorb_lookup_offset: u64,
    pub xorb_lookup_num_entry: u64,
    pub chunk_lookup_offset: u64,
    pub chunk_lookup_num_entry: u64,
    pub chunk_hash_hmac_key: HMACKey,
    pub shard_creation_timestamp: u64,
    pub shard_key_expiry: u64,
    pub _buffer: [u64; 6],
    pub stored_bytes_on_disk: u64,
    pub materialized_bytes: u64,
    pub stored_bytes: u64,
    pub footer_offset: u64,
}

impl Default for MDBShardFileFooter {
    fn default() -> Self {
        Self {
            version: MDB_SHARD_FOOTER_VERSION,
            file_info_offset: 0,
            xorb_info_offset: 0,
            file_lookup_offset: 0,
            file_lookup_num_entry: 0,
            xorb_lookup_offset: 0,
            xorb_lookup_num_entry: 0,
            chunk_lookup_offset: 0,
            chunk_lookup_num_entry: 0,
            chunk_hash_hmac_key: HMACKey::default(),
            shard_creation_timestamp: 0,
            shard_key_expiry: u64::MAX,
            _buffer: [0u64; 6],
            stored_bytes_on_disk: 0,
            materialized_bytes: 0,
            stored_bytes: 0,
            footer_offset: 0,
        }
    }
}

impl MDBShardFileFooter {
    pub fn serialize<W: Write>(&self, writer: &mut W) -> Result<usize> {
        write_u64(writer, self.version)?;
        write_u64(writer, self.file_info_offset)?;
        write_u64(writer, self.xorb_info_offset)?;
        write_u64(writer, self.file_lookup_offset)?;
        write_u64(writer, self.file_lookup_num_entry)?;
        write_u64(writer, self.xorb_lookup_offset)?;
        write_u64(writer, self.xorb_lookup_num_entry)?;
        write_u64(writer, self.chunk_lookup_offset)?;
        write_u64(writer, self.chunk_lookup_num_entry)?;
        write_hash(writer, &self.chunk_hash_hmac_key)?;
        write_u64(writer, self.shard_creation_timestamp)?;
        write_u64(writer, self.shard_key_expiry)?;
        write_u64s(writer, &self._buffer)?;
        write_u64(writer, self.stored_bytes_on_disk)?;
        write_u64(writer, self.materialized_bytes)?;
        write_u64(writer, self.stored_bytes)?;
        write_u64(writer, self.footer_offset)?;
        Ok(size_of::<MDBShardFileFooter>())
    }

    pub fn deserialize<R: Read>(reader: &mut R) -> Result<Self> {
        let version = read_u64(reader)?;
        if version != MDB_SHARD_FOOTER_VERSION {
            return Err(CoreError::ShardVersion(format!(
                "Error: Expected footer version {MDB_SHARD_FOOTER_VERSION}, got {version}"
            )));
        }

        let mut obj = Self {
            version,
            file_info_offset: read_u64(reader)?,
            xorb_info_offset: read_u64(reader)?,
            file_lookup_offset: read_u64(reader)?,
            file_lookup_num_entry: read_u64(reader)?,
            xorb_lookup_offset: read_u64(reader)?,
            xorb_lookup_num_entry: read_u64(reader)?,
            chunk_lookup_offset: read_u64(reader)?,
            chunk_lookup_num_entry: read_u64(reader)?,
            chunk_hash_hmac_key: read_hash(reader)?,
            shard_creation_timestamp: read_u64(reader)?,
            shard_key_expiry: read_u64(reader)?,
            ..Default::default()
        };

        read_u64s(reader, &mut obj._buffer)?;
        obj.stored_bytes_on_disk = read_u64(reader)?;
        obj.materialized_bytes = read_u64(reader)?;
        obj.stored_bytes = read_u64(reader)?;
        obj.footer_offset = read_u64(reader)?;

        Ok(obj)
    }
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct MDBShardInfo {
    pub header: MDBShardFileHeader,
    pub footer: MDBShardFileFooter,
    pub file_infos: Vec<MDBFileInfo>,
    pub xorb_infos: Vec<MDBXorbInfo>,
}

impl MDBShardInfo {
    pub fn non_content_byte_size() -> u64 {
        (size_of::<MDBShardFileHeader>() + size_of::<MDBShardFileFooter>()) as u64
    }

    pub fn serialize_from<W: Write>(
        writer: &mut W,
        shard: &MDBInMemoryShard,
        _expiration: Option<std::time::Duration>,
    ) -> Result<()> {
        let header = MDBShardFileHeader::default();
        header.serialize(writer)?;

        let mut footer = MDBShardFileFooter::default();

        for (_, file_info) in shard.file_content.iter() {
            file_info.serialize(writer)?;
        }
        FileDataSequenceHeader::bookend().serialize(writer)?;

        for (_, xorb_info) in shard.xorb_content.iter() {
            xorb_info.serialize(writer)?;
        }
        XorbChunkSequenceHeader::bookend().serialize(writer)?;

        // Write footer (for backward compatibility with code that reads it)
        footer.file_info_offset = MDBShardFileHeader::default()
            .serialize(&mut std::io::sink())
            .unwrap_or(0) as u64;
        footer.xorb_info_offset = 0;
        footer.footer_offset = 0;
        footer.serialize(writer)?;

        Ok(())
    }

    pub fn load_from_reader<R: Read + std::io::Seek>(
        reader: &mut R,
    ) -> std::result::Result<Self, CoreError> {
        let header = MDBShardFileHeader::deserialize(reader)
            .map_err(|e| CoreError::MalformedData(format!("Failed to read header: {e}")))?;
        let version = header.version;

        let mut file_infos = Vec::new();
        loop {
            match MDBFileInfo::deserialize(reader, version) {
                Ok(Some(file_info)) => file_infos.push(file_info),
                Ok(None) => break,
                Err(e) => {
                    if file_infos.is_empty() {
                        return Err(CoreError::MalformedData(format!(
                            "Failed to read file info: {e}"
                        )));
                    }
                    break;
                }
            }
        }

        let mut xorb_infos = Vec::new();
        loop {
            match MDBXorbInfo::deserialize(reader, version) {
                Ok(Some(xorb_info)) => xorb_infos.push(xorb_info),
                Ok(None) => break,
                Err(e) => {
                    if xorb_infos.is_empty() {
                        return Err(CoreError::MalformedData(format!(
                            "Failed to read xorb info: {e}"
                        )));
                    }
                    break;
                }
            }
        }

        Ok(Self {
            header,
            footer: MDBShardFileFooter::default(),
            file_infos,
            xorb_infos,
        })
    }

    pub fn read_all_file_info_sections<R: Read>(
        &self,
        reader: &mut R,
        version: u64,
    ) -> std::result::Result<Vec<MDBFileInfo>, CoreError> {
        let mut file_infos = Vec::new();
        loop {
            match MDBFileInfo::deserialize(reader, version) {
                Ok(Some(file_info)) => file_infos.push(file_info),
                Ok(None) => break,
                Err(_) => break,
            }
        }
        Ok(file_infos)
    }

    pub fn read_all_xorb_blocks_full<R: Read>(
        &self,
        reader: &mut R,
        version: u64,
    ) -> std::result::Result<Vec<MDBXorbInfo>, CoreError> {
        let mut xorb_infos = Vec::new();
        loop {
            match MDBXorbInfo::deserialize(reader, version) {
                Ok(Some(xorb_info)) => xorb_infos.push(xorb_info),
                Ok(None) => break,
                Err(_) => break,
            }
        }
        Ok(xorb_infos)
    }

    pub fn num_file_entries(&self) -> usize {
        self.file_infos.len()
    }

    pub fn num_xorb_entries(&self) -> usize {
        self.xorb_infos.len()
    }

    pub fn total_num_chunks(&self) -> usize {
        self.xorb_infos.iter().map(|x| x.chunks.len()).sum()
    }

    pub fn materialized_bytes(&self) -> u64 {
        self.file_infos.iter().map(|f| f.file_size()).sum()
    }

    pub fn stored_bytes(&self) -> u64 {
        self.xorb_infos
            .iter()
            .map(|x| x.metadata.num_bytes_in_xorb)
            .sum()
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use super::*;
    use crate::merklehash::MerkleHash;
    use crate::merklehash::compute_data_hash;

    fn make_file_info(num_entries: u64) -> MDBFileInfo {
        let entries: Vec<FileDataSequenceEntry> = (0..num_entries)
            .map(|i| {
                FileDataSequenceEntry::new(compute_data_hash(b"x"), 100u64, i * 100, (i + 1) * 100)
            })
            .collect();
        MDBFileInfo {
            metadata: FileDataSequenceHeader::new(
                compute_data_hash(b"f"),
                num_entries,
                false,
                false,
            ),
            segments: entries,
            verification: vec![],
            metadata_ext: None,
        }
    }

    fn make_xorb_info(num_chunks: u64, bytes_per_chunk: u64) -> MDBXorbInfo {
        let entries: Vec<XorbChunkSequenceEntry> = (0..num_chunks)
            .map(|i| {
                XorbChunkSequenceEntry::new(
                    compute_data_hash(b"c"),
                    bytes_per_chunk,
                    i * bytes_per_chunk,
                )
            })
            .collect();
        MDBXorbInfo {
            metadata: XorbChunkSequenceHeader::new(
                compute_data_hash(b"x"),
                num_chunks,
                num_chunks * bytes_per_chunk,
            ),
            chunks: entries,
        }
    }

    fn serialize_to_buf(info: &MDBShardInfo) -> Vec<u8> {
        let mut buf = Vec::new();
        info.header.serialize(&mut buf).unwrap();
        for f in &info.file_infos {
            f.serialize(&mut buf).unwrap();
        }
        FileDataSequenceHeader::bookend()
            .serialize(&mut buf)
            .unwrap();
        for x in &info.xorb_infos {
            x.serialize(&mut buf).unwrap();
        }
        XorbChunkSequenceHeader::bookend()
            .serialize(&mut buf)
            .unwrap();
        info.footer.serialize(&mut buf).unwrap();
        buf
    }

    // ======= MDBShardFileHeader =======

    #[test]
    fn header_default() {
        let h = MDBShardFileHeader::default();
        assert_eq!(h.version, 3);
        assert_eq!(h.tag, MDB_SHARD_HEADER_TAG);
        assert!(h.footer_size > 0);
    }

    #[test]
    fn header_serialize_roundtrip() {
        let h = MDBShardFileHeader::default();
        let mut buf = Vec::new();
        h.serialize(&mut buf).unwrap();
        assert_eq!(buf.len(), size_of::<MDBShardFileHeader>());
        let mut r = Cursor::new(&buf);
        let h2 = MDBShardFileHeader::deserialize(&mut r).unwrap();
        assert_eq!(h, h2);
    }

    #[test]
    fn header_deserialize_wrong_magic() {
        let mut buf = vec![0u8; 48];
        let mut bad = [b'X'; 32];
        bad[..5].copy_from_slice(b"WRONG");
        buf[..32].copy_from_slice(&bad);
        buf[32..40].copy_from_slice(&2u64.to_le_bytes());
        buf[40..48].copy_from_slice(&48u64.to_le_bytes());
        assert!(MDBShardFileHeader::deserialize(&mut Cursor::new(&buf)).is_err());
    }

    // ======= MDBShardFileFooter =======

    #[test]
    fn footer_default() {
        let f = MDBShardFileFooter::default();
        assert_eq!(f.version, 2);
        assert_eq!(f.shard_key_expiry, u64::MAX);
    }

    #[test]
    fn footer_serialize_roundtrip() {
        let mut f = MDBShardFileFooter::default();
        f.file_info_offset = 100;
        f.xorb_info_offset = 200;
        f.stored_bytes = 1024;
        let mut buf = Vec::new();
        f.serialize(&mut buf).unwrap();
        assert_eq!(buf.len(), size_of::<MDBShardFileFooter>());
        let mut r = Cursor::new(&buf);
        let f2 = MDBShardFileFooter::deserialize(&mut r).unwrap();
        assert_eq!(f.version, f2.version);
        assert_eq!(f.file_info_offset, f2.file_info_offset);
        assert_eq!(f.xorb_info_offset, f2.xorb_info_offset);
        assert_eq!(f.stored_bytes, f2.stored_bytes);
    }

    #[test]
    fn footer_deserialize_wrong_version() {
        let mut buf = vec![0u8; 200];
        buf[0..8].copy_from_slice(&99u64.to_le_bytes());
        assert!(MDBShardFileFooter::deserialize(&mut Cursor::new(&buf)).is_err());
    }

    // ======= MDBShardInfo =======

    #[test]
    fn shard_info_non_content_byte_size() {
        assert_eq!(
            MDBShardInfo::non_content_byte_size(),
            (size_of::<MDBShardFileHeader>() + size_of::<MDBShardFileFooter>()) as u64
        );
    }

    #[test]
    fn shard_info_default_empty() {
        let info = MDBShardInfo::default();
        assert_eq!(info.num_file_entries(), 0);
        assert_eq!(info.num_xorb_entries(), 0);
        assert_eq!(info.total_num_chunks(), 0);
        assert_eq!(info.materialized_bytes(), 0);
        assert_eq!(info.stored_bytes(), 0);
    }

    #[test]
    fn shard_info_counts() {
        let info = MDBShardInfo {
            header: MDBShardFileHeader::default(),
            footer: MDBShardFileFooter::default(),
            file_infos: vec![make_file_info(1), make_file_info(0)],
            xorb_infos: vec![make_xorb_info(1, 100)],
        };
        assert_eq!(info.num_file_entries(), 2);
        assert_eq!(info.num_xorb_entries(), 1);
        assert_eq!(info.total_num_chunks(), 1);
        assert_eq!(info.stored_bytes(), 100);
        assert_eq!(info.materialized_bytes(), 100 + 0);
    }

    #[test]
    fn shard_info_materialized_bytes_multiple() {
        let info = MDBShardInfo {
            header: MDBShardFileHeader::default(),
            footer: MDBShardFileFooter::default(),
            file_infos: vec![
                MDBFileInfo {
                    metadata: FileDataSequenceHeader::new(
                        MerkleHash::default(),
                        1u64,
                        false,
                        false,
                    ),
                    segments: vec![FileDataSequenceEntry::new(
                        MerkleHash::default(),
                        500u64, 0u64, 0u64,
                    )],
                    verification: vec![],
                    metadata_ext: None,
                },
                MDBFileInfo {
                    metadata: FileDataSequenceHeader::new(
                        MerkleHash::default(),
                        1u64,
                        false,
                        false,
                    ),
                    segments: vec![FileDataSequenceEntry::new(
                        MerkleHash::default(),
                        300u64, 0u64, 0u64,
                    )],
                    verification: vec![],
                    metadata_ext: None,
                },
            ],
            xorb_infos: vec![],
        };
        assert_eq!(info.materialized_bytes(), 800);
    }

    // ======= read_all_file_info_sections =======

    #[test]
    fn read_all_file_info_sections_empty() {
        let mut buf = Vec::new();
        FileDataSequenceHeader::bookend()
            .serialize(&mut buf)
            .unwrap();
        let info = MDBShardInfo::default();
        let files = info
            .read_all_file_info_sections(&mut Cursor::new(&buf), 3).unwrap();
        assert!(files.is_empty());
    }

    #[test]
    fn read_all_file_info_sections_multiple() {
        let file1 = make_file_info(1);
        let file2 = make_file_info(0);
        let mut buf = Vec::new();
        file1.serialize(&mut buf).unwrap();
        file2.serialize(&mut buf).unwrap();
        FileDataSequenceHeader::bookend()
            .serialize(&mut buf)
            .unwrap();
        let info = MDBShardInfo::default();
        let files = info
            .read_all_file_info_sections(&mut Cursor::new(&buf), 3).unwrap();
        assert_eq!(files.len(), 2);
    }

    #[test]
    fn read_all_file_info_sections_breaks_on_partial_error() {
        let file1 = make_file_info(1);
        let mut buf = Vec::new();
        file1.serialize(&mut buf).unwrap();
        buf.extend_from_slice(b"CORRUPTED");
        let info = MDBShardInfo::default();
        let files = info
            .read_all_file_info_sections(&mut Cursor::new(&buf), 3).unwrap();
        assert_eq!(files.len(), 1);
    }

    // ======= read_all_xorb_blocks_full =======

    #[test]
    fn read_all_xorb_blocks_full_empty() {
        let mut buf = Vec::new();
        XorbChunkSequenceHeader::bookend()
            .serialize(&mut buf)
            .unwrap();
        let info = MDBShardInfo::default();
        let x = info
            .read_all_xorb_blocks_full(&mut Cursor::new(&buf), 3)
            .unwrap();
        assert!(x.is_empty());
    }

    #[test]
    fn read_all_xorb_blocks_full_multiple() {
        let x1 = make_xorb_info(1, 100);
        let x2 = make_xorb_info(0, 0);
        let mut buf = Vec::new();
        x1.serialize(&mut buf).unwrap();
        x2.serialize(&mut buf).unwrap();
        XorbChunkSequenceHeader::bookend()
            .serialize(&mut buf)
            .unwrap();
        let info = MDBShardInfo::default();
        let x = info
            .read_all_xorb_blocks_full(&mut Cursor::new(&buf), 3)
            .unwrap();
        assert_eq!(x.len(), 2);
        assert_eq!(x[0].chunks.len(), 1);
    }

    #[test]
    fn read_all_xorb_blocks_full_breaks_on_partial_error() {
        let x1 = make_xorb_info(1, 100);
        let mut buf = Vec::new();
        x1.serialize(&mut buf).unwrap();
        buf.extend_from_slice(b"CORRUPTED");
        let info = MDBShardInfo::default();
        let x = info
            .read_all_xorb_blocks_full(&mut Cursor::new(&buf), 3)
            .unwrap();
        assert_eq!(x.len(), 1);
    }

    // ======= load_from_reader =======

    #[test]
    fn load_from_reader_bad_header() {
        let buf = vec![0u8; 48];
        assert!(MDBShardInfo::load_from_reader(&mut Cursor::new(buf)).is_err());
    }

    #[test]
    fn load_from_reader_file_info_error_when_empty() {
        let mut buf = Vec::new();
        MDBShardFileHeader::default().serialize(&mut buf).unwrap();
        buf.extend_from_slice(b"GARBAGE");
        assert!(MDBShardInfo::load_from_reader(&mut Cursor::new(buf)).is_err());
    }

    #[test]
    fn load_from_reader_full_roundtrip() {
        let mut buf = Vec::new();
        MDBShardFileHeader::default().serialize(&mut buf).unwrap();
        let file = make_file_info(1);
        file.serialize(&mut buf).unwrap();
        FileDataSequenceHeader::bookend()
            .serialize(&mut buf)
            .unwrap();
        let xorb = make_xorb_info(1, 100);
        xorb.serialize(&mut buf).unwrap();
        XorbChunkSequenceHeader::bookend()
            .serialize(&mut buf)
            .unwrap();
        MDBShardFileFooter::default().serialize(&mut buf).unwrap();
        let info = MDBShardInfo::load_from_reader(&mut Cursor::new(&buf)).unwrap();
        assert_eq!(info.num_file_entries(), 1);
        assert_eq!(info.num_xorb_entries(), 1);
    }

    #[test]
    fn load_from_reader_xorb_info_error_when_empty() {
        let mut buf = Vec::new();
        MDBShardFileHeader::default().serialize(&mut buf).unwrap();
        FileDataSequenceHeader::bookend()
            .serialize(&mut buf)
            .unwrap();
        buf.extend_from_slice(b"GARBAGE");
        assert!(MDBShardInfo::load_from_reader(&mut Cursor::new(buf)).is_err());
    }

    #[test]
    fn load_from_reader_xorb_info_break_on_error_with_data() {
        let mut buf = Vec::new();
        MDBShardFileHeader::default().serialize(&mut buf).unwrap();
        FileDataSequenceHeader::bookend()
            .serialize(&mut buf)
            .unwrap();
        let xorb = make_xorb_info(1, 100);
        xorb.serialize(&mut buf).unwrap();
        buf.extend_from_slice(b"GARBAGE");
        let result = MDBShardInfo::load_from_reader(&mut Cursor::new(&buf));
        assert!(result.is_ok());
        assert_eq!(result.unwrap().num_xorb_entries(), 1);
    }

    // ======= serialize_from =======

    #[test]
    fn serialize_from_roundtrip() {
        let mut shard = crate::metadata_shard::shard_in_memory::MDBInMemoryShard::default();
        shard.add_xorb_block(make_xorb_info(0, 0)).unwrap();
        shard
            .add_file_reconstruction_info(make_file_info(0))
            .unwrap();
        let mut buf = Vec::new();
        MDBShardInfo::serialize_from(&mut buf, &shard, None).unwrap();
        assert!(!buf.is_empty());

        // Verify can be loaded back
        let info = MDBShardInfo::load_from_reader(&mut Cursor::new(&buf)).unwrap();
        assert_eq!(info.num_file_entries(), 1);
        assert_eq!(info.num_xorb_entries(), 1);
    }

    // ======= entry_size constants =======

    #[test]
    fn mdb_file_info_entry_size_matches_structs() {
        assert_eq!(
            MDB_FILE_INFO_ENTRY_SIZE,
            size_of::<FileDataSequenceHeader>()
        );
        assert_eq!(MDB_FILE_INFO_ENTRY_SIZE, size_of::<FileDataSequenceEntry>());
        assert_eq!(MDB_FILE_INFO_ENTRY_SIZE, size_of::<FileVerificationEntry>());
        assert_eq!(MDB_FILE_INFO_ENTRY_SIZE, size_of::<FileMetadataExt>());
    }

    #[test]
    fn load_from_reader_multiple_files_and_xorbs_uses_default_footer() {
        let mut buf = Vec::new();
        MDBShardFileHeader::default().serialize(&mut buf).unwrap();
        // Write 2 files
        make_file_info(1).serialize(&mut buf).unwrap();
        make_file_info(2).serialize(&mut buf).unwrap();
        FileDataSequenceHeader::bookend()
            .serialize(&mut buf)
            .unwrap();
        // Write 2 xorbs
        make_xorb_info(1, 100).serialize(&mut buf).unwrap();
        make_xorb_info(2, 50).serialize(&mut buf).unwrap();
        XorbChunkSequenceHeader::bookend()
            .serialize(&mut buf)
            .unwrap();

        let info = MDBShardInfo::load_from_reader(&mut Cursor::new(&buf)).unwrap();
        assert_eq!(info.num_file_entries(), 2);
        assert_eq!(info.num_xorb_entries(), 2);
        assert_eq!(info.total_num_chunks(), 3);
        // Footer is default (version=1), not from the buffer
        assert_eq!(info.footer.version, 2);
    }

    #[test]
    fn load_from_reader_xorb_info_break_on_error_with_data_already_exists() {
        let mut buf = Vec::new();
        MDBShardFileHeader::default().serialize(&mut buf).unwrap();
        FileDataSequenceHeader::bookend()
            .serialize(&mut buf)
            .unwrap();
        make_xorb_info(1, 100).serialize(&mut buf).unwrap();
        // Corrupted data after first xorb (xorb_infos not empty, so break)
        buf.extend_from_slice(b"BAD_XORB_DATA");
        let result = MDBShardInfo::load_from_reader(&mut Cursor::new(&buf));
        assert!(result.is_ok());
        assert_eq!(result.unwrap().num_xorb_entries(), 1);
    }

    #[test]
    fn read_all_file_info_sections_error_path() {
        let mut buf = Vec::new();
        // Write valid file info
        make_file_info(1).serialize(&mut buf).unwrap();
        // Then garbage (Err branch)
        buf.extend_from_slice(b"GARBAGE");
        let info = MDBShardInfo::default();
        let files = info
            .read_all_file_info_sections(&mut Cursor::new(&buf), 3).unwrap();
        assert_eq!(files.len(), 1);
    }

    #[test]
    fn read_all_xorb_blocks_full_error_path() {
        let mut buf = Vec::new();
        make_xorb_info(1, 100).serialize(&mut buf).unwrap();
        buf.extend_from_slice(b"GARBAGE");
        let info = MDBShardInfo::default();
        let x = info
            .read_all_xorb_blocks_full(&mut Cursor::new(&buf), 3)
            .unwrap();
        assert_eq!(x.len(), 1);
    }

    #[test]
    fn shard_info_stored_bytes_multiple_xorbs() {
        let info = MDBShardInfo {
            header: MDBShardFileHeader::default(),
            footer: MDBShardFileFooter::default(),
            file_infos: vec![],
            xorb_infos: vec![make_xorb_info(1, 100), make_xorb_info(2, 150)],
        };
        assert_eq!(info.stored_bytes(), 100 + 300);
    }

    #[test]
    fn serialize_from_with_multiple_files_and_xorbs() {
        fn make_xorb_info_v2(
            hash_data: &[u8],
            num_chunks: u64,
            bytes_per_chunk: u64,
        ) -> MDBXorbInfo {
            let entries: Vec<XorbChunkSequenceEntry> = (0..num_chunks)
                .map(|i| {
                    XorbChunkSequenceEntry::new(
                        compute_data_hash(b"c"),
                        bytes_per_chunk,
                        i * bytes_per_chunk,
                    )
                })
                .collect();
            MDBXorbInfo {
                metadata: XorbChunkSequenceHeader::new(
                    compute_data_hash(hash_data),
                    num_chunks,
                    num_chunks * bytes_per_chunk,
                ),
                chunks: entries,
            }
        }
        fn make_file_info_v2(hash_data: &[u8], num_entries: u64) -> MDBFileInfo {
            let entries: Vec<FileDataSequenceEntry> = (0..num_entries)
                .map(|i| {
                    FileDataSequenceEntry::new(
                        compute_data_hash(b"x"),
                        100u64,
                        i * 100,
                        (i + 1) * 100,
                    )
                })
                .collect();
            MDBFileInfo {
                metadata: FileDataSequenceHeader::new(
                    compute_data_hash(hash_data),
                    num_entries,
                    false,
                    false,
                ),
                segments: entries,
                verification: vec![],
                metadata_ext: None,
            }
        }
        let mut shard = crate::metadata_shard::shard_in_memory::MDBInMemoryShard::default();
        shard
            .add_xorb_block(make_xorb_info_v2(b"x1", 1, 100))
            .unwrap();
        shard
            .add_xorb_block(make_xorb_info_v2(b"x2", 0, 0))
            .unwrap();
        shard
            .add_file_reconstruction_info(make_file_info_v2(b"f1", 1))
            .unwrap();
        shard
            .add_file_reconstruction_info(make_file_info_v2(b"f2", 0))
            .unwrap();
        let mut buf = Vec::new();
        MDBShardInfo::serialize_from(&mut buf, &shard, None).unwrap();
        let info = MDBShardInfo::load_from_reader(&mut Cursor::new(&buf)).unwrap();
        assert_eq!(info.num_file_entries(), 2);
        assert_eq!(info.num_xorb_entries(), 2);
    }
}
