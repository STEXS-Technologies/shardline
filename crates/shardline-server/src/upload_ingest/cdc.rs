use tracing::trace;

use gearhash::DEFAULT_TABLE as GEAR_TABLE;

/// Gear hash (FastCDC) content-defined chunker.
///
/// Uses the same algorithm and parameters as `xet-data::deduplication::Chunker`.
/// Scans for chunk boundaries using a rolling Gear hash, producing chunks
/// that are roughly `target_chunk_size` bytes on average, with
/// `min_chunk = target_chunk_size / 8` and `max_chunk = target_chunk_size * 2`.
pub struct CdcChunker {
    hash: u64,
    min_chunk: usize,
    max_chunk: usize,
    mask: u64,
    pending_len: usize,
}

/// Smallest supported CDC target (a power of two above the 64-byte hash window).
pub const MIN_TARGET_CHUNK_SIZE: usize = 128;
/// Largest supported CDC target; twice this value must fit the chunk length.
pub const MAX_TARGET_CHUNK_SIZE: usize = 1 << 30;

impl CdcChunker {
    /// Creates a new chunker with the given target chunk size.
    ///
    /// # Panics
    ///
    /// Panics unless `target_chunk_size` is a power of two in 128..=1 GiB.
    #[must_use]
    pub fn new(target_chunk_size: usize) -> Self {
        assert!(
            target_chunk_size.is_power_of_two()
                && (MIN_TARGET_CHUNK_SIZE..=MAX_TARGET_CHUNK_SIZE).contains(&target_chunk_size),
            "CDC target chunk size must be a power of two in 128..=1073741824 bytes"
        );

        let raw_mask = target_chunk_size.wrapping_sub(1) as u64;
        let mask = raw_mask << raw_mask.leading_zeros();

        Self {
            hash: 0,
            min_chunk: target_chunk_size / 8,
            max_chunk: target_chunk_size.wrapping_mul(2),
            mask,
            pending_len: 0,
        }
    }

    /// Returns the minimum chunk size enforced by this chunker.
    #[must_use]
    pub const fn min_chunk(&self) -> usize {
        self.min_chunk
    }

    /// Find the next chunk boundary in `data` starting from `self.pending_len`.
    ///
    /// `data` is the full accumulated pending buffer. Bytes `[0..self.pending_len]`
    /// have already been scanned; this method resumes scanning from `self.pending_len`.
    ///
    /// Returns `Some(boundary_offset)` where `boundary_offset` is the
    /// position within `data` where the chunk ends, or `None` if no
    /// boundary was found.
    ///
    /// Matches xet-data Chunker::next_boundary behavior exactly:
    /// - Skips first `min_chunk - 64` bytes without hashing (hash window warmup)
    /// - Resets hash to 0 after each boundary is found
    /// - Forces boundary at max_chunk if no natural boundary found
    pub fn find_boundary(&mut self, data: &[u8]) -> Option<usize> {
        let n_bytes = data.len();
        if n_bytes == 0 {
            return None;
        }

        let previous_len = self.pending_len;
        // Resume scanning from where we left off — bytes [0..previous_len]
        // were already scanned in prior calls and their hash contribution
        // is already in self.hash.
        let mut cur_index = previous_len;

        // Skip the minimum chunk zone. The hash has a window size of 64,
        // so skip only min_chunk - 64 - 1 bytes without hashing.
        if previous_len.wrapping_add(64) < self.min_chunk {
            let skip = self
                .min_chunk
                .saturating_sub(previous_len)
                .saturating_sub(64)
                .saturating_sub(1)
                .min(n_bytes.saturating_sub(previous_len));
            cur_index = cur_index.wrapping_add(skip);
        }

        // Don't scan past the maximum chunk boundary
        let read_end = n_bytes.min(self.max_chunk);

        // Warm the same hash window without allowing an early boundary, then
        // use gearhash's runtime-dispatched SIMD scan (scalar on other CPUs).
        // next_match counts consumed bytes, including the matching byte.
        let mut hasher = gearhash::Hasher::new(&GEAR_TABLE);
        hasher.set_hash(self.hash);
        let match_start = cur_index.max(self.min_chunk).min(read_end);
        if let Some(warmup) = data.get(cur_index..match_start) {
            hasher.update(warmup);
        }
        let boundary = data
            .get(match_start..read_end)
            .and_then(|remaining| hasher.next_match(remaining, self.mask));
        self.hash = hasher.get_hash();

        let boundary = boundary
            .map(|consumed| match_start.wrapping_add(consumed))
            .or_else(|| (read_end >= self.max_chunk).then_some(self.max_chunk));
        if let Some(boundary) = boundary {
            self.reset();
            trace!(
                boundary,
                chunk_size = boundary,
                forced = boundary >= self.max_chunk,
                "CDC boundary found"
            );
            Some(boundary)
        } else {
            self.pending_len = read_end;
            None
        }
    }

    /// Resets the internal hash state for scanning a new chunk.
    pub const fn reset(&mut self) {
        self.hash = 0;
        self.pending_len = 0;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scalar_reference(chunker: &mut CdcChunker, data: &[u8]) -> Option<usize> {
        let mut start = chunker.pending_len;
        if start.wrapping_add(64) < chunker.min_chunk {
            let skip = chunker
                .min_chunk
                .saturating_sub(start.checked_add(65)?)
                .min(data.len().saturating_sub(start));
            start = start.checked_add(skip)?;
        }
        let end = data.len().min(chunker.max_chunk);
        for (index, byte) in data.iter().enumerate().take(end).skip(start) {
            chunker.hash = (chunker.hash << 1).wrapping_add(GEAR_TABLE[usize::from(*byte)]);
            if index >= chunker.min_chunk && chunker.hash & chunker.mask == 0 {
                chunker.reset();
                return index.checked_add(1);
            }
        }
        if end == chunker.max_chunk {
            chunker.reset();
            return Some(end);
        }
        chunker.pending_len = end;
        None
    }

    #[test]
    fn accelerated_scan_preserves_scalar_boundaries_and_streaming_state() {
        let mut random = vec![0_u8; 256 * 1024];
        let mut seed = 42_u64;
        for byte in &mut random {
            seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
            *byte = (seed >> 32) as u8;
        }
        for data in [&random[..], &vec![0_u8; random.len()][..]] {
            for target in [128, 1024, 65536] {
                for frame in [1, 17, 128, 4096, data.len()] {
                    let mut accelerated = CdcChunker::new(target);
                    let mut reference = CdcChunker::new(target);
                    let mut start = 0;
                    for available in (frame..data.len()).step_by(frame).chain([data.len()]) {
                        loop {
                            let pending = &data[start..available];
                            let expected = scalar_reference(&mut reference, pending);
                            let actual = accelerated.find_boundary(pending);
                            assert_eq!(actual, expected, "target={target}, frame={frame}");
                            assert_eq!(accelerated.hash, reference.hash);
                            assert_eq!(accelerated.pending_len, reference.pending_len);
                            match actual {
                                Some(consumed) => start += consumed,
                                None => break,
                            }
                        }
                    }
                }
            }
        }
    }

    #[test]
    fn cdc_target_bounds_reject_invalid_sizes_in_all_builds() {
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
                std::panic::catch_unwind(|| CdcChunker::new(size)).is_err(),
                "size={size}"
            );
        }
        // Construction does not allocate target-sized storage.
        for exponent in 7..=30 {
            let size = 1 << exponent;
            let mut chunker = CdcChunker::new(size);
            assert_eq!(chunker.max_chunk, size * 2);
            if let Some(boundary) = chunker.find_boundary(&[0; 512]) {
                assert!(boundary > 0 && boundary <= 512);
            }
        }
    }

    #[test]
    fn cdc_chunker_creates_with_valid_size() {
        let chunker = CdcChunker::new(1024);
        assert_eq!(chunker.min_chunk, 128);
        assert_eq!(chunker.max_chunk, 2048);
    }

    #[test]
    fn cdc_chunker_returns_none_for_insufficient_data() {
        let mut chunker = CdcChunker::new(1024);
        // Less than min_chunk (128)
        let result = chunker.find_boundary(b"hello world");
        assert!(result.is_none());
    }

    #[test]
    fn cdc_chunker_finds_boundary_in_large_data() {
        let mut chunker = CdcChunker::new(1024);
        // Create 2KB of data that should trigger a boundary
        let data = b"AAAAAABBBBBBCCCCCCDDDDDDEEEEEEFFFFFGGGGGGHHHHHHIIIIIIJJJJJJKKKKKKLLLLLLMMMMMMNNNNNNOOOOOOPPPPPPQQQQQQRRRRRRSSSSSSTTTTTTUUUUUUVVVVVVWWWWWWXXXXXXYYYYYYZZZZZZ".repeat(32);
        let result = chunker.find_boundary(&data);
        assert!(result.is_some());
        let boundary = result.unwrap();
        assert!(boundary >= chunker.min_chunk());
        assert!(boundary <= data.len().min(chunker.max_chunk));
    }

    #[test]
    fn cdc_chunker_resets_hash_state() {
        let mut chunker = CdcChunker::new(1024);
        let data = b"AAAAAABBBBBBCCCCCCDDDDDDEEEEEEFFFFFGGGGGGHHHHHHIIIIIIJJJJJJKKKKKKLLLLLLMMMMMMNNNNNNOOOOOOPPPPPPQQQQQQRRRRRRSSSSSSTTTTTTUUUUUUVVVVVVWWWWWWXXXXXXYYYYYYZZZZZZ".repeat(32);

        // Find first boundary
        let boundary1 = chunker.find_boundary(&data).unwrap();
        chunker.reset();

        // Scan the remaining data
        let remaining = &data[boundary1..];
        let boundary2 = chunker.find_boundary(remaining);
        assert!(boundary2.is_some());
    }

    #[test]
    fn cdc_chunker_forced_boundary_at_max_chunk() {
        // With a very small target, max_chunk = 2 * target
        let mut chunker = CdcChunker::new(128);
        // Create data that exceeds max_chunk
        let data = vec![0u8; 500];
        let result = chunker.find_boundary(&data);
        assert!(result.is_some());
        let boundary = result.unwrap();
        // Boundary must be at least min_chunk (16) and at most max_chunk (256)
        assert!(boundary >= 16);
        assert!(boundary <= 256);
    }

    /// Verify that streaming CDC (small frames) produces the same chunk
    /// boundaries as batch CDC (entire data at once).
    ///
    /// This is the critical correctness property: chunk boundaries must
    /// be identical regardless of how data is fed in.
    #[test]
    fn cdc_streaming_matches_batch_boundaries() {
        // Generate pseudo-random data large enough for multiple chunks
        // With target=1024, min=128, max=2048 — need several KB
        let mut data = Vec::with_capacity(256 * 1024);
        let mut state: u64 = 42;
        while data.len() < 256 * 1024 {
            state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
            data.extend_from_slice(&state.to_le_bytes());
        }
        data.truncate(256 * 1024);

        let target = 1024;

        // --- Batch mode: feed entire data, collect boundaries ---
        let mut batch_chunker = CdcChunker::new(target);
        let mut batch_boundaries = Vec::new();
        let mut batch_pos = 0;
        loop {
            let remaining = &data[batch_pos..];
            if remaining.is_empty() {
                break;
            }
            match batch_chunker.find_boundary(remaining) {
                Some(b) => {
                    batch_pos += b;
                    batch_chunker.reset();
                    batch_boundaries.push(batch_pos);
                }
                None => break,
            }
        }
        // Flush remaining as final chunk
        if batch_pos < data.len() {
            batch_boundaries.push(data.len());
        }

        // --- Streaming mode: feed in small frames (128 bytes each) ---
        let mut stream_chunker = CdcChunker::new(target);
        let mut stream_boundaries = Vec::new();
        let mut accumulated = Vec::new();
        let frame_size = 128;

        for chunk_start in (0..data.len()).step_by(frame_size) {
            let end = (chunk_start + frame_size).min(data.len());
            accumulated.extend_from_slice(&data[chunk_start..end]);

            loop {
                if accumulated.len() < stream_chunker.min_chunk() {
                    break;
                }
                match stream_chunker.find_boundary(&accumulated) {
                    Some(b) => {
                        // Record absolute offset
                        let abs_offset = stream_boundaries.last().copied().unwrap_or(0) + b;
                        stream_boundaries.push(abs_offset);
                        // Remove consumed bytes from front
                        accumulated = accumulated.split_off(b);
                        stream_chunker.reset();
                    }
                    None => break,
                }
            }
        }
        // Flush remaining
        if !accumulated.is_empty() {
            let abs_offset = stream_boundaries.last().copied().unwrap_or(0) + accumulated.len();
            stream_boundaries.push(abs_offset);
        }

        // --- Compare ---
        assert_eq!(
            batch_boundaries,
            stream_boundaries,
            "Streaming CDC boundaries differ from batch!\n  batch:   {:?}\n  stream:  {:?}",
            batch_boundaries.iter().take(20).collect::<Vec<_>>(),
            stream_boundaries.iter().take(20).collect::<Vec<_>>(),
        );
        assert!(
            batch_boundaries.len() >= 5,
            "Expected multiple chunks, got {}",
            batch_boundaries.len()
        );
    }

    /// Verify CDC produces deterministic boundaries across different
    /// call patterns — matching xet-data's streaming contract.
    #[test]
    fn cdc_boundary_determinism_across_frame_sizes() {
        let mut data = Vec::with_capacity(64 * 1024);
        let mut state: u64 = 99;
        while data.len() < 64 * 1024 {
            state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
            data.extend_from_slice(&state.to_le_bytes());
        }
        data.truncate(64 * 1024);

        let target = 256;

        // Feed at various frame sizes and collect boundaries
        let collect_boundaries = |frame_size: usize| -> Vec<usize> {
            let mut chunker = CdcChunker::new(target);
            let mut boundaries = Vec::new();
            let mut accumulated = Vec::new();

            for chunk_start in (0..data.len()).step_by(frame_size) {
                let end = (chunk_start + frame_size).min(data.len());
                accumulated.extend_from_slice(&data[chunk_start..end]);

                loop {
                    if accumulated.len() < chunker.min_chunk() {
                        break;
                    }
                    match chunker.find_boundary(&accumulated) {
                        Some(b) => {
                            let abs = boundaries.last().copied().unwrap_or(0) + b;
                            boundaries.push(abs);
                            accumulated = accumulated.split_off(b);
                            chunker.reset();
                        }
                        None => break,
                    }
                }
            }
            if !accumulated.is_empty() {
                let abs = boundaries.last().copied().unwrap_or(0) + accumulated.len();
                boundaries.push(abs);
            }
            boundaries
        };

        let ref_boundaries = collect_boundaries(data.len()); // batch
        for frame_size in [1, 17, 32, 64, 128, 256, 512] {
            let boundaries = collect_boundaries(frame_size);
            assert_eq!(
                boundaries, ref_boundaries,
                "Frame size {} produced different boundaries",
                frame_size
            );
        }
    }
}
