use std::borrow::Cow;

use serde::Serialize;

use crate::merklehash::{MerkleHash, xorb_hash};

use super::chunk::Chunk;

/// XORB info containing chunk boundaries.
#[derive(Clone, Debug, Default, Serialize)]
pub struct XorbInfo {
    /// Cumulative unpacked byte offsets for each chunk boundary.
    pub chunk_boundaries: Vec<u32>,
}

impl XorbInfo {
    pub fn chunks_and_boundaries(&self) -> Vec<(MerkleHash, u32)> {
        self.chunk_boundaries
            .iter()
            .map(|&end| (MerkleHash::default(), end))
            .collect()
    }
}

/// Raw xorb data ready for serialization.
#[derive(Clone, Debug, Serialize)]
pub struct RawXorbData {
    pub data: Vec<Cow<'static, [u8]>>,
    pub xorb_info: XorbInfo,
    pub file_boundaries: Vec<usize>,
}

impl RawXorbData {
    pub fn from_chunks(chunks: &[Chunk], file_boundaries: Vec<usize>) -> Self {
        let data = chunks.iter().map(|c| c.data.clone()).collect();
        let chunk_boundaries: Vec<u32> = chunks
            .iter()
            .scan(0u32, |acc, chunk| {
                *acc += chunk.data.len() as u32;
                Some(*acc)
            })
            .collect();
        let xorb_info = XorbInfo { chunk_boundaries };
        Self {
            data,
            xorb_info,
            file_boundaries,
        }
    }

    pub fn hash(&self) -> MerkleHash {
        let chunks_and_sizes: Vec<(MerkleHash, u64)> = self
            .data
            .iter()
            .map(|d| (crate::merklehash::compute_data_hash(d), d.len() as u64))
            .collect();
        xorb_hash(&chunks_and_sizes)
    }

    pub fn num_bytes(&self) -> usize {
        self.data.iter().map(|d| d.len()).sum()
    }
}

pub mod test_utils {
    use super::*;

    pub fn raw_xorb_to_vec(xorb: &RawXorbData) -> Vec<u8> {
        let mut result = Vec::new();
        for chunk in &xorb.data {
            result.extend_from_slice(chunk);
        }
        result
    }
}
