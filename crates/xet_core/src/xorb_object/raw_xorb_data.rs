use std::borrow::Cow;

use serde::Serialize;

use crate::merklehash::{MerkleHash, compute_data_hash, xorb_hash};

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
            .map(|d| (compute_data_hash(d), d.len() as u64))
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::merklehash::compute_data_hash;
    use crate::xorb_object::Chunk;

    #[test]
    fn xorb_info_default_has_empty_boundaries() {
        let info = XorbInfo::default();
        assert!(info.chunk_boundaries.is_empty());
    }

    #[test]
    fn xorb_info_chunks_and_boundaries_empty() {
        let info = XorbInfo::default();
        assert!(info.chunks_and_boundaries().is_empty());
    }

    #[test]
    fn xorb_info_chunks_and_boundaries_with_data() {
        let info = XorbInfo {
            chunk_boundaries: vec![100, 250, 400],
        };
        let boundaries = info.chunks_and_boundaries();
        assert_eq!(boundaries.len(), 3);
        assert_eq!(boundaries[0].1, 100);
        assert_eq!(boundaries[1].1, 250);
        assert_eq!(boundaries[2].1, 400);
        // Hash is always default (all zeros)
        assert_eq!(boundaries[0].0, MerkleHash::default());
    }

    #[test]
    fn raw_xorb_data_from_chunks_basic() {
        let chunks = vec![
            Chunk {
                hash: compute_data_hash(b"c1"),
                data: vec![1u8; 100].into(),
            },
            Chunk {
                hash: compute_data_hash(b"c2"),
                data: vec![2u8; 200].into(),
            },
        ];
        let raw = RawXorbData::from_chunks(&chunks, vec![0, 100]);
        assert_eq!(raw.num_bytes(), 300);
        assert_eq!(raw.data.len(), 2);
        assert_eq!(raw.file_boundaries, vec![0, 100]);
        // Check chunk_boundaries are cumulative sizes
        assert_eq!(raw.xorb_info.chunk_boundaries, vec![100, 300]);
    }

    #[test]
    fn raw_xorb_data_from_single_chunk() {
        let chunks = vec![Chunk {
            hash: MerkleHash::default(),
            data: vec![0u8; 42].into(),
        }];
        let raw = RawXorbData::from_chunks(&chunks, vec![]);
        assert_eq!(raw.num_bytes(), 42);
        assert_eq!(raw.data.len(), 1);
        assert_eq!(raw.xorb_info.chunk_boundaries, vec![42]);
    }

    #[test]
    fn raw_xorb_data_hash_non_default() {
        let chunks = vec![Chunk {
            hash: compute_data_hash(b"c"),
            data: vec![0u8; 50].into(),
        }];
        let raw = RawXorbData::from_chunks(&chunks, vec![0]);
        assert_ne!(raw.hash(), MerkleHash::default());
    }

    #[test]
    fn raw_xorb_data_hash_deterministic() {
        let chunks = vec![Chunk {
            hash: compute_data_hash(b"c"),
            data: vec![7u8; 100].into(),
        }];
        let raw1 = RawXorbData::from_chunks(&chunks, vec![0]);
        let raw2 = RawXorbData::from_chunks(&chunks, vec![0]);
        assert_eq!(raw1.hash(), raw2.hash());
    }

    #[test]
    fn raw_xorb_data_num_bytes_multi_chunk() {
        let chunks = vec![
            Chunk {
                hash: MerkleHash::default(),
                data: vec![0u8; 10].into(),
            },
            Chunk {
                hash: MerkleHash::default(),
                data: vec![0u8; 20].into(),
            },
            Chunk {
                hash: MerkleHash::default(),
                data: vec![0u8; 30].into(),
            },
        ];
        let raw = RawXorbData::from_chunks(&chunks, vec![0]);
        assert_eq!(raw.num_bytes(), 60);
    }

    #[test]
    fn raw_xorb_data_file_boundaries_preserved() {
        let chunks = vec![
            Chunk {
                hash: MerkleHash::default(),
                data: vec![1u8; 50].into(),
            },
            Chunk {
                hash: MerkleHash::default(),
                data: vec![2u8; 50].into(),
            },
        ];
        let raw = RawXorbData::from_chunks(&chunks, vec![0, 1]);
        assert_eq!(raw.file_boundaries, vec![0, 1]);
    }

    #[test]
    fn raw_xorb_data_empty_chunks_list() {
        let raw = RawXorbData::from_chunks(&[], vec![]);
        assert_eq!(raw.num_bytes(), 0);
        assert!(raw.data.is_empty());
        assert!(raw.xorb_info.chunk_boundaries.is_empty());
        assert_eq!(raw.hash(), MerkleHash::default());
    }

    #[test]
    fn test_utils_raw_xorb_to_vec() {
        let chunks = vec![
            Chunk {
                hash: MerkleHash::default(),
                data: vec![1u8; 10].into(),
            },
            Chunk {
                hash: MerkleHash::default(),
                data: vec![2u8; 20].into(),
            },
        ];
        let raw = RawXorbData::from_chunks(&chunks, vec![]);
        let flat = test_utils::raw_xorb_to_vec(&raw);
        assert_eq!(flat, [vec![1u8; 10], vec![2u8; 20]].concat());
        assert_eq!(flat.len(), 30);
    }
}
