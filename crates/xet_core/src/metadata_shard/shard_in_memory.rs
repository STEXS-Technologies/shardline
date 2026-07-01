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
