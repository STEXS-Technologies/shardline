//! Content-defined chunking (M3a): the client CDC chunker.
//!
//! The client and server use the same boundary finder from `shardline-xet-core`,
//! so they split a file at exactly the same boundaries (`docs/SDX_PLAN.md` §4.4.2 / §9-M3).
//!
//! The streaming surface mirrors the upstream `xet-data` `Chunker`
//! (`xet-data-1.5.4/src/deduplication/chunking.rs`): the ingest loop feeds 8
//! MiB blocks via [`Chunker::next_block_bytes`], which emits zero-copy
//! [`Bytes`] slices of each block where possible and buffers a partial
//! trailing chunk in `chunkbuf` across block boundaries. [`Chunker::finish`]
//! flushes the final chunk.

use bytes::Bytes;
use shardline_xet_core::cdc::CdcChunker;
use xet_core_structures::merklehash::MerkleHash;

use crate::hash::compute_chunk_hash;

/// Default CDC target chunk size: 64 KiB (chunks range 8–128 KiB).
pub const DEFAULT_TARGET_CHUNK_SIZE: usize = 64 * 1024;

/// A single CDC chunk: the payload bytes plus a lazily computed content hash.
///
/// When produced from [`Chunker::next_block_bytes`], `data` is a zero-copy
/// slice of the fed block whenever the chunk lies wholly within one block.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Chunk {
    /// The chunk payload bytes.
    pub data: Bytes,
}

impl Chunk {
    /// Wraps `data` as a chunk.
    #[must_use]
    pub const fn new(data: Bytes) -> Self {
        Self { data }
    }

    /// Returns the chunk payload length in bytes.
    #[must_use]
    pub const fn len(&self) -> usize {
        self.data.len()
    }

    /// Returns `true` when the chunk has no payload bytes.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Returns the chunk payload as a byte slice.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.data
    }

    /// Returns the content hash of the chunk payload (BLAKE3 `DATA_KEY`, M0).
    #[must_use]
    pub fn hash(&self) -> MerkleHash {
        compute_chunk_hash(&self.data)
    }
}

/// Streaming content-defined chunker for the ingest path (M3a / M3b).
///
/// Feeds 8 MiB ingestion blocks via [`Chunker::next_block_bytes`]; a partial
/// trailing chunk is buffered in `chunkbuf` across block boundaries and
/// flushed by [`Chunker::finish`]. Boundary detection is byte-identical to the
/// server's `CdcChunker` (see module docs).
#[derive(Debug, Clone)]
pub struct Chunker {
    cdc: CdcChunker,
    chunkbuf: Vec<u8>,
}

impl Chunker {
    /// Creates a new chunker with the given target chunk size.
    ///
    /// The default is [`DEFAULT_TARGET_CHUNK_SIZE`] (64 KiB).
    ///
    /// # Panics
    ///
    /// Panics unless `target_chunk_size` is a power of two in 128..=1 GiB.
    #[must_use]
    pub fn new(target_chunk_size: usize) -> Self {
        Self {
            cdc: CdcChunker::new(target_chunk_size),
            chunkbuf: Vec::with_capacity(target_chunk_size.wrapping_mul(2)),
        }
    }

    /// Creates a chunker with the default 64 KiB target.
    #[must_use]
    pub fn default_target() -> Self {
        Self::new(DEFAULT_TARGET_CHUNK_SIZE)
    }

    /// Returns the minimum chunk size enforced by this chunker.
    #[must_use]
    pub const fn min_chunk(&self) -> usize {
        self.cdc.min_chunk()
    }

    /// Returns the maximum chunk size enforced by this chunker.
    #[must_use]
    pub const fn max_chunk(&self) -> usize {
        self.cdc.max_chunk()
    }

    /// Resets all state: discards any buffered partial chunk and resets the
    /// hash for a fresh chunk stream.
    pub fn reset(&mut self) {
        self.chunkbuf.clear();
        self.cdc.reset();
    }

    /// Finds the next chunk boundary in `data`, which must be the full
    /// accumulated pending buffer (batch mode).
    ///
    /// This mirrors the server's `CdcChunker::find_boundary` directly for
    /// batch processing. The streaming surface ([`Chunker::next_block_bytes`])
    /// maintains its own buffer; do not mix the two modes on one instance.
    pub fn find_boundary(&mut self, data: &[u8]) -> Option<usize> {
        self.cdc.find_boundary(data)
    }

    /// Processes one block of `data` and returns every complete chunk it
    /// yields.
    ///
    /// When `is_final` is `true`, all remaining buffered bytes (including any
    /// partial trailing chunk held across previous blocks) are emitted and the
    /// chunker resets for a fresh stream. When `false`, a partial trailing
    /// chunk is kept in `chunkbuf` for the next block.
    ///
    /// Chunks that lie wholly within `data` are emitted as zero-copy [`Bytes`]
    /// slices; a chunk spanning the buffered tail and `data` is copied.
    #[must_use]
    pub fn next_block_bytes(&mut self, data: &Bytes, is_final: bool) -> Vec<Chunk> {
        let mut chunks = Vec::new();

        let mut pos = 0;
        if !self.chunkbuf.is_empty() {
            // Complete only the pending chunk. Extra bytes read ahead of its
            // boundary are scanned from the original Bytes after the CDC reset.
            let prior_len = self.chunkbuf.len();
            let append_len = self.max_chunk().saturating_sub(prior_len).min(data.len());
            self.chunkbuf.reserve_exact(append_len);
            self.chunkbuf
                .extend_from_slice(data.get(..append_len).unwrap_or_default());
            if let Some(boundary) = self.cdc.find_boundary(&self.chunkbuf) {
                self.chunkbuf.truncate(boundary);
                chunks.push(Chunk::new(Bytes::from(std::mem::take(&mut self.chunkbuf))));
                pos = boundary.saturating_sub(prior_len);
            } else {
                // A full max_chunk buffer always has a forced boundary, so
                // this case consumed all data and retains a partial tail.
                pos = data.len();
            }
        }

        // Zero-copy fast path, including the rest of a block after a pending
        // chunk completes. Only a partial trailing chunk needs buffering.
        while pos < data.len() {
            let remaining = data.get(pos..).unwrap_or_default();
            match self.cdc.find_boundary(remaining) {
                Some(boundary) => {
                    chunks.push(Chunk::new(data.slice(pos..pos.wrapping_add(boundary))));
                    pos = pos.wrapping_add(boundary);
                }
                None => {
                    if is_final {
                        chunks.push(Chunk::new(data.slice(pos..)));
                    } else {
                        self.chunkbuf.reserve_exact(remaining.len());
                        self.chunkbuf.extend_from_slice(remaining);
                    }
                    break;
                }
            }
        }

        if is_final {
            if !self.chunkbuf.is_empty() {
                let tail = std::mem::take(&mut self.chunkbuf);
                chunks.push(Chunk::new(Bytes::from(tail)));
            }
            self.reset();
        }

        chunks
    }

    /// Processes one block of `data`, returning the next chunk (if any) and
    /// the number of bytes of `data` consumed.
    ///
    /// Mirrors upstream `xet-data` `Chunker::next`: when `Some(chunk)` is
    /// returned, feed `data[consumed..]` on the next call (the remainder is
    /// re-scanned from a fresh hash state, which is deterministic for CDC).
    /// When `None` is returned, all of `data` was buffered. `is_final`
    /// flushes any remaining buffered bytes as the final chunk.
    #[must_use]
    pub fn next(&mut self, data: &[u8], is_final: bool) -> (Option<Chunk>, usize) {
        let prior_len = self.chunkbuf.len();
        self.chunkbuf.extend_from_slice(data);

        let boundary = self.cdc.find_boundary(&self.chunkbuf);
        match boundary {
            Some(boundary) => {
                // combined[..boundary] is the chunk; combined[boundary..] is
                // the unconsumed remainder (== data[consumed..]), which the
                // caller re-feeds. The chunker reset its own hash/pending
                // state when the boundary was found.
                let mut combined = std::mem::take(&mut self.chunkbuf);
                let _remainder = combined.split_off(boundary);
                let consumed = boundary.saturating_sub(prior_len);
                (Some(Chunk::new(Bytes::from(combined))), consumed)
            }
            None => {
                if is_final {
                    let combined = std::mem::take(&mut self.chunkbuf);
                    self.reset();
                    if combined.is_empty() {
                        (None, data.len())
                    } else {
                        (Some(Chunk::new(Bytes::from(combined))), data.len())
                    }
                } else {
                    // Everything is buffered in chunkbuf.
                    (None, data.len())
                }
            }
        }
    }

    /// Processes one block of `data`, returning all chunks it yields.
    ///
    /// Mirrors upstream `xet-data` `Chunker::next_block`; equivalent to
    /// repeatedly calling [`Chunker::next`]. Prefer
    /// [`Chunker::next_block_bytes`] for the zero-copy ingest path.
    #[must_use]
    pub fn next_block(&mut self, data: &[u8], is_final: bool) -> Vec<Chunk> {
        let mut chunks = Vec::new();
        let mut pos = 0;
        loop {
            if pos >= data.len() {
                break;
            }
            let (chunk, consumed) = self.next(data.get(pos..).unwrap_or_default(), is_final);
            if let Some(chunk) = chunk {
                chunks.push(chunk);
            }
            pos = pos.saturating_add(consumed);
        }
        if is_final && let Some(tail) = self.finish() {
            chunks.push(tail);
        }
        chunks
    }

    /// Flushes any remaining buffered bytes as the final chunk and resets.
    #[must_use]
    pub fn finish(&mut self) -> Option<Chunk> {
        self.next(&[], true).0
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use shardline_server::upload_ingest::cdc::CdcChunker;

    use super::{Chunk, Chunker, DEFAULT_TARGET_CHUNK_SIZE};

    /// LCG pseudo-random data (the server's own test generator).
    fn lcg_data(len: usize, seed: u64) -> Vec<u8> {
        let mut data = Vec::with_capacity(len);
        let mut state = seed;
        while data.len() < len {
            state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
            data.extend_from_slice(&state.to_le_bytes());
        }
        data.truncate(len);
        data
    }

    /// SplitMix64 pseudo-random data (upstream xet-data test generator).
    fn splitmix_data(len: usize, seed: u64) -> Vec<u8> {
        let mut data = Vec::with_capacity(len.wrapping_add(7));
        let mut state = seed;
        while data.len() < len {
            state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
            let mut z = state;
            z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
            z ^= z >> 31;
            data.extend_from_slice(&z.to_le_bytes());
        }
        data.truncate(len);
        data
    }

    /// A repeated byte pattern (highly compressible / deterministic triggers).
    fn pattern_data(len: usize) -> Vec<u8> {
        (0..len)
            .map(|i| (i.wrapping_mul(7).wrapping_add(3) & 0xFF) as u8)
            .collect()
    }

    /// The server's batch boundary driver (its own `cdc.rs` test logic).
    fn server_batch_boundaries(data: &[u8], target: usize) -> Vec<usize> {
        let mut chunker = CdcChunker::new(target);
        let mut boundaries = Vec::new();
        let mut pos = 0;
        loop {
            let remaining = data.get(pos..).unwrap_or_default();
            if remaining.is_empty() {
                break;
            }
            match chunker.find_boundary(remaining) {
                Some(boundary) => {
                    pos = pos.wrapping_add(boundary);
                    chunker.reset();
                    boundaries.push(pos);
                }
                None => break,
            }
        }
        if pos < data.len() {
            boundaries.push(data.len());
        }
        boundaries
    }

    /// The server's streaming-frame boundary driver (its `cdc.rs` test logic).
    fn server_stream_boundaries(data: &[u8], target: usize, frame_size: usize) -> Vec<usize> {
        let mut chunker = CdcChunker::new(target);
        let mut boundaries = Vec::new();
        let mut accumulated = Vec::new();
        let mut start = 0;
        while start < data.len() {
            let end = start.wrapping_add(frame_size).min(data.len());
            accumulated.extend_from_slice(data.get(start..end).unwrap_or_default());
            loop {
                if accumulated.len() < chunker.min_chunk() {
                    break;
                }
                match chunker.find_boundary(&accumulated) {
                    Some(boundary) => {
                        let abs = boundaries
                            .last()
                            .copied()
                            .unwrap_or(0usize)
                            .wrapping_add(boundary);
                        boundaries.push(abs);
                        accumulated = accumulated.split_off(boundary);
                        chunker.reset();
                    }
                    None => break,
                }
            }
            start = end;
        }
        if !accumulated.is_empty() {
            let abs = boundaries
                .last()
                .copied()
                .unwrap_or(0usize)
                .wrapping_add(accumulated.len());
            boundaries.push(abs);
        }
        boundaries
    }

    /// My chunker streaming boundary driver (via `next_block_bytes`).
    fn my_stream_boundaries(data: &[u8], target: usize, frame_size: usize) -> Vec<usize> {
        let mut chunker = Chunker::new(target);
        let mut boundaries = Vec::new();
        let mut consumed = 0usize;
        let bytes = Bytes::copy_from_slice(data);
        let mut start = 0;
        while start < data.len() {
            let end = start.wrapping_add(frame_size).min(data.len());
            let block = bytes.slice(start..end);
            let chunks = chunker.next_block_bytes(&block, end == data.len());
            for chunk in chunks {
                consumed = consumed.wrapping_add(chunk.len());
                boundaries.push(consumed);
            }
            start = end;
        }
        boundaries
    }

    /// My chunker batch driver (whole data in one `next_block_bytes` call).
    fn my_batch_boundaries(data: &[u8], target: usize) -> Vec<usize> {
        let mut chunker = Chunker::new(target);
        let chunks = chunker.next_block_bytes(&Bytes::copy_from_slice(data), true);
        let mut boundaries = Vec::new();
        let mut consumed = 0usize;
        for chunk in chunks {
            consumed = consumed.wrapping_add(chunk.len());
            boundaries.push(consumed);
        }
        boundaries
    }

    /// My `next`/`next_block` streaming driver (mirrors upstream contract).
    fn my_next_block_boundaries(data: &[u8], target: usize, frame_size: usize) -> Vec<usize> {
        let mut chunker = Chunker::new(target);
        let mut boundaries = Vec::new();
        let mut consumed = 0usize;
        let mut start = 0;
        while start < data.len() {
            let end = start.wrapping_add(frame_size).min(data.len());
            let chunks =
                chunker.next_block(data.get(start..end).unwrap_or_default(), end == data.len());
            for chunk in chunks {
                consumed = consumed.wrapping_add(chunk.len());
                boundaries.push(consumed);
            }
            start = end;
        }
        boundaries
    }

    #[test]
    fn chunker_empty_final_frame_preserves_tail_and_resets() {
        for len in [0, 1, 16, 127, 1023, 131072] {
            let data = vec![59u8; len];
            let mut chunker = Chunker::default_target();
            let mut chunks = chunker.next_block(&data, false);
            chunks.extend(chunker.next_block(&[], true));
            assert_eq!(
                chunks
                    .iter()
                    .flat_map(|c| c.as_bytes().iter().copied())
                    .collect::<Vec<_>>(),
                data
            );
            assert!(chunker.finish().is_none());
            // A final flush starts a new independent stream.
            let next = chunker.next_block(b"fresh stream", true);
            assert_eq!(next.len(), 1);
            assert_eq!(next[0].as_bytes(), b"fresh stream");
        }
    }

    #[test]
    fn chunker_split_frames_and_empty_final_match_server_bytes_and_hashes() {
        for target in [128, 1024, DEFAULT_TARGET_CHUNK_SIZE] {
            for len in [0, 1, 17, target, target * 2, target * 3 + 17] {
                let data = splitmix_data(len, 42);
                let boundaries = server_batch_boundaries(&data, target);
                for frame in [1, 17, 257, 8192] {
                    let mut bytes_chunker = Chunker::new(target);
                    let mut compat_chunker = Chunker::new(target);
                    let bytes = Bytes::copy_from_slice(&data);
                    let mut actual = Vec::new();
                    let mut compat = Vec::new();
                    let mut pos = 0;
                    while pos < len {
                        let end = pos.saturating_add(frame).min(len);
                        actual
                            .extend(bytes_chunker.next_block_bytes(&bytes.slice(pos..end), false));
                        compat.extend(compat_chunker.next_block(&data[pos..end], false));
                        assert!(bytes_chunker.chunkbuf.len() <= bytes_chunker.max_chunk());
                        assert!(bytes_chunker.chunkbuf.capacity() <= bytes_chunker.max_chunk());
                        pos = end;
                    }
                    actual.extend(bytes_chunker.next_block_bytes(&Bytes::new(), true));
                    compat.extend(compat_chunker.next_block(&[], true));
                    for chunks in [&actual, &compat] {
                        assert_eq!(chunks.len(), boundaries.len());
                        let mut start = 0;
                        for (chunk, end) in chunks.iter().zip(&boundaries) {
                            assert_eq!(chunk.as_bytes(), &data[start..*end]);
                            assert_eq!(
                                chunk.hash(),
                                crate::hash::compute_chunk_hash(&data[start..*end])
                            );
                            start = *end;
                        }
                        assert_eq!(start, len);
                    }
                    assert!(bytes_chunker.finish().is_none());
                    assert!(compat_chunker.finish().is_none());
                }
            }
        }
    }

    #[test]
    fn chunker_pending_head_resumes_shared_bytes_for_large_block() {
        let bytes = Bytes::from(splitmix_data(1024 * 1024, 7));
        let mut chunker = Chunker::new(1024);
        assert!(
            chunker
                .next_block_bytes(&bytes.slice(..1), false)
                .is_empty()
        );
        let chunks = chunker.next_block_bytes(&bytes.slice(1..), false);
        assert!(chunks.len() > 2);
        let mut offset = 0;
        for (index, chunk) in chunks.iter().enumerate() {
            assert_eq!(chunk.as_bytes(), &bytes[offset..offset + chunk.len()]);
            if index > 0 {
                assert_eq!(chunk.as_bytes().as_ptr(), bytes.slice(offset..).as_ptr());
            }
            offset += chunk.len();
        }
        assert!(chunker.chunkbuf.len() <= chunker.max_chunk());
        assert!(chunker.chunkbuf.capacity() <= chunker.max_chunk());
        if let Some(tail) = chunker.finish() {
            assert_eq!(tail.as_bytes(), &bytes[offset..]);
            offset += tail.len();
        }
        assert_eq!(offset, bytes.len());
    }

    #[test]
    fn chunker_target_bounds_reject_invalid_sizes_before_allocation() {
        for size in [
            0,
            1,
            2,
            64,
            127,
            129,
            (1 << 30) + 1,
            1 << 31,
            1usize << (usize::BITS - 1),
        ] {
            assert!(
                std::panic::catch_unwind(|| Chunker::new(size)).is_err(),
                "size={size}"
            );
        }
        // Test all supported targets through allocation-free CDC construction.
        for exponent in 7..=30 {
            let size = 1 << exponent;
            let mut chunker = super::CdcChunker::new(size);
            assert_eq!(chunker.max_chunk(), size * 2);
            if let Some(boundary) = chunker.find_boundary(&[0; 512]) {
                assert!(boundary > 0 && boundary <= 512);
            }
        }
    }

    #[test]
    fn chunker_constructs_with_valid_size() {
        let chunker = Chunker::new(1024);
        assert_eq!(chunker.min_chunk(), 128);
        assert_eq!(chunker.max_chunk(), 2048);
        assert_eq!(Chunker::new(DEFAULT_TARGET_CHUNK_SIZE).max_chunk(), 131072);
        assert_eq!(Chunker::default_target().min_chunk(), 8192);
    }

    #[test]
    fn chunker_forced_boundary_at_max_chunk() {
        let mut chunker = Chunker::new(128);
        let data = vec![0u8; 500];
        let chunks = chunker.next_block_bytes(&Bytes::copy_from_slice(&data), true);
        assert!(!chunks.is_empty());
        for chunk in &chunks {
            assert!(chunk.len() >= 16 && chunk.len() <= 256);
        }
        assert_eq!(chunks.iter().map(Chunk::len).sum::<usize>(), data.len());
    }

    #[test]
    fn chunker_empty_input_no_chunk_until_final() {
        let mut chunker = Chunker::new(128);
        let none = chunker.next_block_bytes(&Bytes::new(), false);
        assert!(none.is_empty());
        let final_empty = chunker.next_block_bytes(&Bytes::new(), true);
        assert!(final_empty.is_empty());
        assert!(chunker.finish().is_none());
    }

    #[test]
    fn chunker_streaming_matches_batch() {
        let mut data = Vec::with_capacity(256 * 1024);
        data.extend_from_slice(&lcg_data(256 * 1024, 42));
        let target = 1024;
        let batch = my_batch_boundaries(&data, target);
        for frame_size in [1, 17, 64, 128, 512, 1024, 4096] {
            let stream = my_stream_boundaries(&data, target, frame_size);
            assert_eq!(stream, batch, "frame size {frame_size} differs from batch");
        }
    }

    #[test]
    fn chunker_next_contract_matches_batch() {
        let data = lcg_data(256 * 1024, 7);
        let target = 1024;
        let batch = my_batch_boundaries(&data, target);
        for frame_size in [1, 37, 255, 2048] {
            let boundaries = my_next_block_boundaries(&data, target, frame_size);
            assert_eq!(boundaries, batch, "next() frame size {frame_size} differs");
        }
    }

    #[test]
    fn chunker_boundaries_byte_identical_to_server_across_inputs_and_targets() {
        let cases: Vec<(&str, Vec<u8>)> = vec![
            ("lcg", lcg_data(256 * 1024, 42)),
            ("zeros", vec![0u8; 256 * 1024]),
            ("pattern", pattern_data(256 * 1024)),
            ("splitmix", splitmix_data(256 * 1024, 99)),
        ];
        for (name, data) in cases {
            for target in [1024usize, 4096, 65536] {
                let server_batch = server_batch_boundaries(&data, target);
                let server_stream = server_stream_boundaries(&data, target, 128);
                let my_batch = my_batch_boundaries(&data, target);
                let my_stream = my_stream_boundaries(&data, target, 128);
                assert_eq!(
                    my_batch, server_batch,
                    "{name} target {target}: batch boundaries differ from server batch"
                );
                assert_eq!(
                    my_stream, server_stream,
                    "{name} target {target}: streaming boundaries differ from server streaming"
                );
                assert_eq!(
                    server_stream, server_batch,
                    "{name} target {target}: server streaming must equal server batch"
                );
            }
        }
    }

    #[test]
    fn chunker_determinism_across_frame_sizes_matches_server() {
        let data = lcg_data(64 * 1024, 99);
        let target = 256;
        let ref_stream = server_stream_boundaries(&data, target, data.len());
        for frame_size in [1, 17, 32, 64, 128, 256, 512] {
            let mine = my_stream_boundaries(&data, target, frame_size);
            let server = server_stream_boundaries(&data, target, frame_size);
            assert_eq!(
                mine, ref_stream,
                "frame size {frame_size} produced different boundaries"
            );
            assert_eq!(
                server, ref_stream,
                "server frame size {frame_size} produced different boundaries"
            );
        }
    }

    #[test]
    fn chunker_matches_upstream_golden_random_boundaries() {
        // Golden boundary vector from upstream xet-data
        // `chunking.rs::test_correctness_1mb_random_data` (default 64 KiB
        // target). Pins byte-identity against the reference chunker that the
        // server mirrors.
        let data = splitmix_data(1_000_000, 0);
        assert_eq!(data[0], 175);
        assert_eq!(data[127], 132);
        assert_eq!(data[111111], 118);
        let boundaries = my_batch_boundaries(&data, DEFAULT_TARGET_CHUNK_SIZE);
        assert_eq!(
            boundaries,
            vec![
                84493, 134421, 144853, 243318, 271793, 336457, 467529, 494581, 582000, 596735,
                616815, 653164, 678202, 724510, 815591, 827760, 958832, 991092, 1000000
            ]
        );
    }

    #[test]
    fn chunker_matches_upstream_golden_const_boundaries() {
        // Golden vector from upstream `test_correctness_1mb_const_data`.
        let data = vec![59u8; 1_000_000];
        let boundaries = my_batch_boundaries(&data, DEFAULT_TARGET_CHUNK_SIZE);
        assert_eq!(
            boundaries,
            vec![
                131072, 262144, 393216, 524288, 655360, 786432, 917504, 1000000
            ]
        );
    }

    #[test]
    fn chunker_zero_copy_slices_match_original_bytes() {
        let data = lcg_data(512 * 1024, 3);
        let mut chunker = Chunker::new(DEFAULT_TARGET_CHUNK_SIZE);
        let bytes = Bytes::copy_from_slice(&data);
        let chunks = chunker.next_block_bytes(&bytes, true);
        let mut rebuilt = Vec::with_capacity(data.len());
        for chunk in &chunks {
            rebuilt.extend_from_slice(chunk.as_bytes());
        }
        assert_eq!(rebuilt, data);
        // Hashes are the content hashes of the exact payload bytes.
        for chunk in &chunks {
            assert_eq!(
                chunk.hash(),
                crate::hash::compute_chunk_hash(chunk.as_bytes())
            );
        }
    }
}
