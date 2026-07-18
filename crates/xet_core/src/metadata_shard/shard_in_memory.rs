use std::collections::BTreeMap;
use std::mem::size_of;
use std::sync::Arc;

use super::file_structs::*;
use super::shard_format::MDBShardInfo;
use super::xorb_structs::*;
use crate::error::Result;
use crate::merklehash::MerkleHash;

#[derive(Clone, Default, Debug)]
pub struct MDBInMemoryShard {
    pub xorb_content: BTreeMap<MerkleHash, Arc<MDBXorbInfo>>,
    pub file_content: BTreeMap<MerkleHash, MDBFileInfo>,
    current_shard_file_size: u64,
}

impl MDBInMemoryShard {
    pub fn add_xorb_block(
        &mut self,
        xorb_block_contents: impl Into<Arc<MDBXorbInfo>>,
    ) -> Result<()> {
        let dest_content_v: Arc<MDBXorbInfo> = xorb_block_contents.into();
        self.xorb_content
            .insert(dest_content_v.metadata.xorb_hash, dest_content_v.clone());
        for _chunk in dest_content_v.chunks.iter() {
            self.current_shard_file_size += (size_of::<u64>() + 2 * size_of::<u32>()) as u64;
        }
        self.current_shard_file_size += dest_content_v.num_bytes();
        self.current_shard_file_size += (size_of::<u64>() + size_of::<u32>()) as u64;
        Ok(())
    }

    pub fn add_file_reconstruction_info(&mut self, file_info: MDBFileInfo) -> Result<()> {
        self.current_shard_file_size += file_info.num_bytes();
        self.current_shard_file_size += (size_of::<u64>() + size_of::<u32>()) as u64;
        self.file_content
            .insert(file_info.metadata.file_hash, file_info);
        Ok(())
    }

    pub fn num_xorb_entries(&self) -> usize {
        self.xorb_content.len()
    }

    pub fn num_file_entries(&self) -> usize {
        self.file_content.len()
    }

    pub fn materialized_bytes(&self) -> u64 {
        self.file_content.iter().fold(0u64, |acc, (_, file)| {
            acc + file
                .segments
                .iter()
                .fold(0u64, |acc, entry| acc + entry.unpacked_segment_bytes as u64)
        })
    }

    pub fn stored_bytes(&self) -> u64 {
        self.xorb_content.iter().fold(0u64, |acc, (_, xorb)| {
            acc + xorb.metadata.num_bytes_in_xorb as u64
        })
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        let mut buf = Vec::new();
        MDBShardInfo::serialize_from(&mut buf, self, None)?;
        Ok(buf)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::merklehash::compute_data_hash;
    use crate::metadata_shard::file_structs::FileDataSequenceEntry;
    use crate::metadata_shard::xorb_structs::{
        MDBXorbInfo, XorbChunkSequenceEntry, XorbChunkSequenceHeader,
    };

    fn make_file_info(num_entries: u32, unpacked: u32) -> MDBFileInfo {
        let entries = (0..num_entries)
            .map(|i| {
                FileDataSequenceEntry::new(
                    compute_data_hash(b"x"),
                    unpacked,
                    i * unpacked,
                    (i + 1) * unpacked,
                )
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

    fn make_xorb_info(num_chunks: u32, bytes_per_chunk: u32) -> MDBXorbInfo {
        let entries = (0..num_chunks)
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

    #[test]
    fn default_is_empty() {
        let shard = MDBInMemoryShard::default();
        assert_eq!(shard.num_xorb_entries(), 0);
        assert_eq!(shard.num_file_entries(), 0);
        assert_eq!(shard.materialized_bytes(), 0);
        assert_eq!(shard.stored_bytes(), 0);
    }

    #[test]
    fn add_single_xorb_block() {
        let mut shard = MDBInMemoryShard::default();
        let xorb = make_xorb_info(1, 200);
        shard.add_xorb_block(xorb).unwrap();
        assert_eq!(shard.num_xorb_entries(), 1);
        assert_eq!(shard.stored_bytes(), 200);
    }

    fn make_xorb_info_v2(hash_data: &[u8], num_chunks: u32, bytes_per_chunk: u32) -> MDBXorbInfo {
        let entries = (0..num_chunks)
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

    #[test]
    fn add_multiple_xorb_blocks() {
        let mut shard = MDBInMemoryShard::default();
        shard
            .add_xorb_block(make_xorb_info_v2(b"x1", 1, 100))
            .unwrap();
        shard
            .add_xorb_block(make_xorb_info_v2(b"x2", 2, 150))
            .unwrap();
        assert_eq!(shard.num_xorb_entries(), 2);
        assert_eq!(shard.stored_bytes(), 100 + 300);
    }

    #[test]
    fn add_xorb_block_zero_chunks() {
        let mut shard = MDBInMemoryShard::default();
        let xorb = make_xorb_info(0, 0);
        shard.add_xorb_block(xorb).unwrap();
        assert_eq!(shard.num_xorb_entries(), 1);
        assert_eq!(shard.stored_bytes(), 0);
    }

    #[test]
    fn add_single_file_reconstruction_info() {
        let mut shard = MDBInMemoryShard::default();
        let info = make_file_info(1, 100);
        shard.add_file_reconstruction_info(info).unwrap();
        assert_eq!(shard.num_file_entries(), 1);
        assert_eq!(shard.materialized_bytes(), 100);
    }

    #[test]
    fn add_multiple_files() {
        let mut shard = MDBInMemoryShard::default();
        let f1 = make_file_info(2, 50);
        let mut f2 = make_file_info(3, 30);
        f2.metadata.file_hash = compute_data_hash(b"f2"); // different hash
        shard.add_file_reconstruction_info(f1).unwrap();
        shard.add_file_reconstruction_info(f2).unwrap();
        assert_eq!(shard.num_file_entries(), 2);
        assert_eq!(shard.materialized_bytes(), 100 + 90);
    }

    #[test]
    fn add_xorb_and_file_then_to_bytes() {
        let mut shard = MDBInMemoryShard::default();
        shard.add_xorb_block(make_xorb_info(1, 100)).unwrap();
        shard
            .add_file_reconstruction_info(make_file_info(1, 50))
            .unwrap();
        let bytes = shard.to_bytes().unwrap();
        assert!(!bytes.is_empty());
        // Should contain header, xorb info, file info, bookends
        assert!(bytes.len() > 100);
    }

    #[test]
    fn add_xorb_block_via_arc_directly() {
        let mut shard = MDBInMemoryShard::default();
        let xorb = Arc::new(make_xorb_info(1, 50));
        shard.add_xorb_block(xorb).unwrap();
        assert_eq!(shard.num_xorb_entries(), 1);
    }

    #[test]
    fn add_file_with_zero_entries() {
        let mut shard = MDBInMemoryShard::default();
        let info = MDBFileInfo {
            metadata: FileDataSequenceHeader::new(compute_data_hash(b"f"), 0u32, false, false),
            segments: vec![],
            verification: vec![],
            metadata_ext: None,
        };
        shard.add_file_reconstruction_info(info).unwrap();
        assert_eq!(shard.num_file_entries(), 1);
        assert_eq!(shard.materialized_bytes(), 0);
    }

    #[test]
    fn materialized_bytes_with_multiple_segments() {
        let mut shard = MDBInMemoryShard::default();
        let info = MDBFileInfo {
            metadata: FileDataSequenceHeader::new(compute_data_hash(b"f"), 2u32, false, false),
            segments: vec![
                FileDataSequenceEntry::new(compute_data_hash(b"x1"), 100u32, 0u32, 50u32),
                FileDataSequenceEntry::new(compute_data_hash(b"x2"), 200u32, 50u32, 100u32),
            ],
            verification: vec![],
            metadata_ext: None,
        };
        shard.add_file_reconstruction_info(info).unwrap();
        assert_eq!(shard.materialized_bytes(), 300);
    }
}
