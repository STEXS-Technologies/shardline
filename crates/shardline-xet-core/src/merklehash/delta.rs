//! Delta-upload windows and opaque upstream-compatible gap commitments.
//!
//! Callers must authenticate the complete chunk sequence against the file root
//! and authorize its repository before exposing this metadata.

use std::ops::Range;

use serde::Serialize;
use xet_core_structures::merklehash::{MerkleHash as WireHash, MerkleHashSubtree};

use super::MerkleHash;

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DeltaWindow {
    pub dirty_byte_range: [u64; 2],
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct FileChunkHashesResponse {
    pub total_chunks: usize,
    pub file_size: u64,
    pub windows: Vec<DeltaWindow>,
    pub hash_ranges: Vec<Option<MerkleHashSubtree>>,
    pub gap_verification: Vec<String>,
}

#[derive(Debug, thiserror::Error)]
#[error("invalid delta ranges or chunk layout")]
pub struct InvalidDelta;

/// Expand half-open dirty ranges to complete chunks. Segment ranges identify
/// the original file terms by chunk position, rather than by object identity.
pub fn file_chunk_hashes(
    chunks: &[(MerkleHash, u64)],
    segments: &[Range<usize>],
    dirty: &[Range<u64>],
) -> Result<FileChunkHashesResponse, InvalidDelta> {
    let mut boundaries = Vec::with_capacity(chunks.len() + 1);
    boundaries.push(0u64);
    for (_, length) in chunks {
        if *length == 0 {
            return Err(InvalidDelta);
        }
        boundaries.push(
            boundaries
                .last()
                .unwrap()
                .checked_add(*length)
                .ok_or(InvalidDelta)?,
        );
    }
    let file_size = *boundaries.last().unwrap();
    let mut next = 0;
    for segment in segments {
        if segment.start != next || segment.end <= next || segment.end > chunks.len() {
            return Err(InvalidDelta);
        }
        next = segment.end;
    }
    if next != chunks.len() {
        return Err(InvalidDelta);
    }
    let mut windows = Vec::<Range<usize>>::new();
    for range in dirty {
        if range.start >= range.end {
            return Err(InvalidDelta);
        }
        let end = range.end.min(file_size);
        if range.start >= end {
            continue;
        }
        let start_chunk = boundaries.partition_point(|offset| *offset <= range.start) - 1;
        let end_chunk = boundaries.partition_point(|offset| *offset < end);
        windows.push(start_chunk..end_chunk);
    }
    if windows.is_empty() {
        return Err(InvalidDelta);
    }
    windows.sort_unstable_by_key(|range| range.start);
    let mut merged = Vec::<Range<usize>>::new();
    for window in windows {
        if let Some(previous) = merged.last_mut()
            && window.start <= previous.end
        {
            previous.end = previous.end.max(window.end);
            continue;
        }
        merged.push(window);
    }
    let wire: Vec<_> = chunks
        .iter()
        .map(|(hash, size)| {
            let bytes: [u8; 32] = (*hash).into();
            (WireHash::from(bytes), *size)
        })
        .collect();
    let mut gaps = Vec::with_capacity(merged.len() + 1);
    let mut start = 0;
    for window in &merged {
        gaps.push(start..window.start);
        start = window.end;
    }
    gaps.push(start..chunks.len());
    let hash_ranges = gaps
        .iter()
        .map(|gap| {
            (!gap.is_empty()).then(|| {
                MerkleHashSubtree::from_chunks(
                    gap.start == 0,
                    &wire[gap.clone()],
                    gap.end == chunks.len(),
                )
            })
        })
        .collect();
    let gap_verification = segments
        .iter()
        .filter(|segment| {
            gaps.iter()
                .any(|gap| gap.start <= segment.start && segment.end <= gap.end)
        })
        .map(|segment| {
            let hashes: Vec<_> = wire[segment.clone()]
                .iter()
                .map(|(hash, _)| *hash)
                .collect();
            xet_core_structures::metadata_shard::chunk_verification::range_hash_from_chunks(&hashes)
                .hex()
        })
        .collect();
    Ok(FileChunkHashesResponse {
        total_chunks: chunks.len(),
        file_size,
        windows: merged
            .iter()
            .map(|window| DeltaWindow {
                dirty_byte_range: [boundaries[window.start], boundaries[window.end]],
            })
            .collect(),
        hash_ranges,
        gap_verification,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn chunks() -> Vec<(MerkleHash, u64)> {
        (0..80).map(|i| (MerkleHash::from([i; 32]), 100)).collect()
    }

    #[test]
    fn unchanged_gaps_and_replacement_merge_to_native_file_identity() {
        let chunks = chunks();
        let segments = vec![0..10, 10..20, 20..40, 40..80];
        let response = file_chunk_hashes(&chunks, &segments, &[1101..1299, 4500..4600]).unwrap();
        assert_eq!(response.windows[0].dirty_byte_range, [1100, 1300]);
        assert_eq!(response.windows[1].dirty_byte_range, [4500, 4600]);
        assert_eq!(response.hash_ranges.len(), 3);
        // Only the first and third original segments are entirely unchanged.
        assert_eq!(response.gap_verification.len(), 2);
        let mut edited = chunks.clone();
        edited[11].0 = MerkleHash::from([201; 32]);
        edited[12].0 = MerkleHash::from([202; 32]);
        edited[45].0 = MerkleHash::from([203; 32]);
        let replacement = |range: Range<usize>| {
            let nodes: Vec<_> = edited[range]
                .iter()
                .map(|(hash, size)| {
                    let bytes: [u8; 32] = (*hash).into();
                    (WireHash::from(bytes), *size)
                })
                .collect();
            MerkleHashSubtree::from_chunks(false, &nodes, false)
        };
        let mut gaps = response.hash_ranges.into_iter();
        let pieces = vec![
            gaps.next().unwrap().unwrap(),
            replacement(11..13),
            gaps.next().unwrap().unwrap(),
            replacement(45..46),
            gaps.next().unwrap().unwrap(),
        ];
        let root = MerkleHashSubtree::merge(&pieces)
            .unwrap()
            .final_hash()
            .unwrap();
        let expected: [u8; 32] = super::super::file_hash(&edited).into();
        assert_eq!(
            blake3::keyed_hash(&[0; 32], root.as_bytes()).as_bytes(),
            &expected
        );
    }

    #[test]
    fn overlap_clamping_and_empty_gap_contract() {
        let chunks = chunks();
        let segments = std::slice::from_ref(&(0..80)).to_vec();
        let response = file_chunk_hashes(&chunks, &segments, &[7999..9000, 0..10, 8..150]).unwrap();
        assert_eq!(response.windows.len(), 2);
        assert_eq!(response.windows[0].dirty_byte_range, [0, 200]);
        assert_eq!(response.windows[1].dirty_byte_range, [7900, 8000]);
        assert!(response.hash_ranges[0].is_none());
        assert!(response.hash_ranges[2].is_none());
        assert!(response.gap_verification.is_empty());
        assert!(
            file_chunk_hashes(&chunks, &segments, std::slice::from_ref(&(8000..9000))).is_err()
        );
        assert!(
            file_chunk_hashes(
                &chunks,
                std::slice::from_ref(&(1..80)),
                std::slice::from_ref(&(0..1))
            )
            .is_err()
        );
        assert!(file_chunk_hashes(&chunks, &segments, &[]).is_err());
    }
}
