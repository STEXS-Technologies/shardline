use std::io::{Read, Write};
use std::mem::size_of;

use super::file_structs::*;
use super::shard_in_memory::MDBInMemoryShard;
use super::xorb_structs::*;
use crate::error::{CoreError, Result};
use crate::merklehash::HMACKey;
use crate::utils::serialization_utils::*;

pub const MDB_FILE_INFO_ENTRY_SIZE: usize = size_of::<[u64; 4]>() + 4 * size_of::<u32>();

static_assertions::const_assert!(MDB_FILE_INFO_ENTRY_SIZE == size_of::<FileDataSequenceHeader>());
static_assertions::const_assert!(MDB_FILE_INFO_ENTRY_SIZE == size_of::<FileDataSequenceEntry>());
static_assertions::const_assert!(MDB_FILE_INFO_ENTRY_SIZE == size_of::<FileVerificationEntry>());
static_assertions::const_assert!(MDB_FILE_INFO_ENTRY_SIZE == size_of::<FileMetadataExt>());

const MDB_XORB_INFO_ENTRY_SIZE: usize = size_of::<[u64; 4]>() + 4 * size_of::<u32>();
static_assertions::const_assert!(MDB_XORB_INFO_ENTRY_SIZE == size_of::<XorbChunkSequenceHeader>());
static_assertions::const_assert!(MDB_XORB_INFO_ENTRY_SIZE == size_of::<XorbChunkSequenceEntry>());

const MDB_SHARD_FOOTER_SIZE: i64 = size_of::<MDBShardFileFooter>() as i64;
const MDB_SHARD_HEADER_VERSION: u64 = 2;
const MDB_SHARD_FOOTER_VERSION: u64 = 1;

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

        let mut file_infos = Vec::new();
        for (_, file_info) in shard.file_content.iter() {
            file_infos.push(file_info.clone());
        }

        let mut xorb_infos = Vec::new();
        for (_, xorb_info) in shard.xorb_content.iter() {
            xorb_infos.push(xorb_info.as_ref().clone());
        }

        let mut footer = MDBShardFileFooter::default();

        for file_info in &file_infos {
            file_info.serialize(writer)?;
        }

        for xorb_info in &xorb_infos {
            xorb_info.serialize(writer)?;
        }

        // Write footer
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

        let mut file_infos = Vec::new();
        loop {
            match MDBFileInfo::deserialize(reader) {
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
            match MDBXorbInfo::deserialize(reader) {
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
    ) -> std::result::Result<Vec<MDBFileInfo>, CoreError> {
        let mut file_infos = Vec::new();
        loop {
            match MDBFileInfo::deserialize(reader) {
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
    ) -> std::result::Result<Vec<MDBXorbInfo>, CoreError> {
        let mut xorb_infos = Vec::new();
        loop {
            match MDBXorbInfo::deserialize(reader) {
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
            .map(|x| x.metadata.num_bytes_in_xorb as u64)
            .sum()
    }
}
