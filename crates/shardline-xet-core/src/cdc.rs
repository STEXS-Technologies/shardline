//! Shared content-defined chunk boundaries for client and server ingestion.
use tracing::trace;

use gearhash::DEFAULT_TABLE as GEAR_TABLE;

/// Gear hash (FastCDC) content-defined chunker.
///
/// Uses the same algorithm and parameters as `xet-data::deduplication::Chunker`.
/// Scans for chunk boundaries using a rolling Gear hash, producing chunks
/// that are roughly `target_chunk_size` bytes on average, with
/// `min_chunk = target_chunk_size / 8` and `max_chunk = target_chunk_size * 2`.
#[derive(Debug, Clone)]
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

    /// Returns the maximum chunk size enforced by this chunker.
    #[must_use]
    pub const fn max_chunk(&self) -> usize {
        self.max_chunk
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

    fn streamed_boundaries(data: &[u8], target: usize, frame_size: usize) -> Vec<usize> {
        let mut chunker = CdcChunker::new(target);
        let mut pending = Vec::new();
        let mut boundaries = Vec::new();
        let mut consumed = 0;
        for frame in data.chunks(frame_size) {
            pending.extend_from_slice(frame);
            while pending.len() >= chunker.min_chunk() {
                let Some(boundary) = chunker.find_boundary(&pending) else {
                    break;
                };
                consumed += boundary;
                boundaries.push(consumed);
                pending = pending.split_off(boundary);
            }
        }
        if !pending.is_empty() {
            boundaries.push(data.len());
        }
        boundaries
    }

    #[test]
    fn streaming_boundaries_are_independent_of_frame_size() {
        for (length, mut seed, target, frames) in [
            (256 * 1024, 42_u64, 1024, &[128][..]),
            (64 * 1024, 99_u64, 256, &[1, 17, 32, 64, 128, 256, 512][..]),
        ] {
            let mut data = Vec::with_capacity(length);
            while data.len() < length {
                seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
                data.extend_from_slice(&seed.to_le_bytes());
            }
            data.truncate(length);
            let expected = streamed_boundaries(&data, target, data.len());
            assert!(expected.len() >= 5);
            for &frame in frames {
                assert_eq!(
                    streamed_boundaries(&data, target, frame),
                    expected,
                    "target={target}, frame={frame}"
                );
            }
        }
    }
}
