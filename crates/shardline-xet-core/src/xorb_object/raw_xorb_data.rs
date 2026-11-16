use std::borrow::Cow;

use serde::Serialize;

use crate::merklehash::{MerkleHash, aggregated_hashes::xorb_hash_from_owned, compute_data_hash};

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
        Self::from_owned_data(
            chunks.iter().map(|chunk| chunk.data.clone()).collect(),
            file_boundaries,
        )
    }

    /// Builds raw xorb data by taking ownership of the chunk payloads.
    ///
    /// Upload paths commonly own the raw chunk buffers until packing completes.
    /// Consuming those chunks avoids cloning every payload before compression.
    pub fn from_owned_data(data: Vec<Cow<'static, [u8]>>, file_boundaries: Vec<usize>) -> Self {
        let mut chunk_boundaries = Vec::with_capacity(data.len());
        let mut unpacked_bytes = 0_u32;
        for chunk in &data {
            unpacked_bytes += chunk.len() as u32;
            chunk_boundaries.push(unpacked_bytes);
        }
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
        xorb_hash_from_owned(chunks_and_sizes)
    }

    /// Computes the xorb hash and the per-chunk hashes in one pass.
    pub fn hash_with_chunk_hashes(&self) -> (MerkleHash, Vec<MerkleHash>) {
        let mut chunks_and_sizes = Vec::with_capacity(self.data.len());
        let mut chunk_hashes = Vec::with_capacity(self.data.len());
        for data in &self.data {
            let chunk_hash = compute_data_hash(data);
            chunks_and_sizes.push((chunk_hash, data.len() as u64));
            chunk_hashes.push(chunk_hash);
        }
        (xorb_hash_from_owned(chunks_and_sizes), chunk_hashes)
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
    use crate::merklehash::aggregated_hashes::xorb_hash;

    #[test]
    fn boundary_projection_preserves_every_offset() {
        assert!(XorbInfo::default().chunk_boundaries.is_empty());
        for offsets in [vec![], vec![100, 250, 400]] {
            let expected: Vec<_> = offsets
                .iter()
                .map(|&offset| (MerkleHash::default(), offset))
                .collect();
            assert_eq!(
                XorbInfo {
                    chunk_boundaries: offsets
                }
                .chunks_and_boundaries(),
                expected
            );
        }
    }

    #[test]
    fn raw_construction_preserves_payloads_boundaries_and_hashes() {
        for lengths in [
            vec![],
            vec![0],
            vec![42],
            vec![50],
            vec![100],
            vec![100, 200],
            vec![10, 20, 30],
            vec![50, 50],
            vec![5, 6],
        ] {
            let chunks: Vec<_> = lengths
                .iter()
                .enumerate()
                .map(|(index, &len)| Chunk {
                    // Construction and hashing must use payload bytes, not this cached hash.
                    hash: MerkleHash::default(),
                    data: match len {
                        5 => Cow::Borrowed(b"first"),
                        6 => Cow::Borrowed(b"second"),
                        _ => vec![index as u8 + 1; len].into(),
                    },
                })
                .collect();
            let expected_hashes: Vec<_> = chunks
                .iter()
                .map(|chunk| compute_data_hash(&chunk.data))
                .collect();
            let members: Vec<_> = expected_hashes
                .iter()
                .copied()
                .zip(lengths.iter().map(|&len| len as u64))
                .collect();
            let expected_hash = xorb_hash(&members);
            let mut total = 0;
            let expected_offsets: Vec<_> = lengths
                .iter()
                .map(|&len| {
                    total += len as u32;
                    total
                })
                .collect();
            let flattened: Vec<_> = chunks
                .iter()
                .flat_map(|chunk| chunk.data.iter().copied())
                .collect();
            let files = vec![0, 100, lengths.len()];
            let cloned = RawXorbData::from_chunks(&chunks, files.clone());
            let owned_data: Vec<_> = chunks.into_iter().map(|chunk| chunk.data).collect();
            let pointers: Vec<_> = owned_data.iter().map(|data| data.as_ptr()).collect();
            let owned = RawXorbData::from_owned_data(owned_data, files.clone());
            assert_eq!(
                owned
                    .data
                    .iter()
                    .map(|data| data.as_ptr())
                    .collect::<Vec<_>>(),
                pointers
            );
            for raw in [cloned, owned] {
                assert_eq!(raw.data.len(), lengths.len());
                assert_eq!(raw.num_bytes(), total as usize);
                assert_eq!(raw.file_boundaries, files);
                assert_eq!(raw.xorb_info.chunk_boundaries, expected_offsets);
                assert_eq!(test_utils::raw_xorb_to_vec(&raw), flattened);
                assert_eq!(raw.hash(), expected_hash);
                assert_eq!(
                    raw.hash_with_chunk_hashes(),
                    (expected_hash, expected_hashes.clone())
                );
            }
        }
    }
}
