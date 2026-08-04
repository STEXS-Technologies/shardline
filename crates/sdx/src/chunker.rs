//! Content-defined chunking (M3a): the client CDC chunker.
//!
//! The boundary-finding algorithm is **byte-identical** to the shardline
//! server's `shardline_server::upload_ingest::cdc::CdcChunker` (same gear
//! table, mask, min/max, hash window, skip, forced-boundary, and `pending_len`
//! resume semantics), so chunking on the client and on the server split a file
//! at exactly the same boundaries (`docs/SDX_PLAN.md` §4.4.2 / §9-M3).
//!
//! The streaming surface mirrors the upstream `xet-data` `Chunker`
//! (`xet-data-1.5.4/src/deduplication/chunking.rs`): the ingest loop feeds 8
//! MiB blocks via [`Chunker::next_block_bytes`], which emits zero-copy
//! [`Bytes`] slices of each block where possible and buffers a partial
//! trailing chunk in `chunkbuf` across block boundaries. [`Chunker::finish`]
//! flushes the final chunk.

use bytes::Bytes;
use xet_core_structures::merklehash::MerkleHash;

use crate::hash::compute_chunk_hash;

/// Default CDC target chunk size: 64 KiB (chunks range 8–128 KiB).
pub const DEFAULT_TARGET_CHUNK_SIZE: usize = 64 * 1024;

/// Gear hash table, verbatim from the server's `cdc.rs` (and
/// `gearhash-0.1.3/src/table.rs`). The table is the byte-identity contract
/// with the server's chunker.
const GEAR_TABLE: [u64; 256] = [
    0xb088d3a9e840f559,
    0x5652c7f739ed20d6,
    0x45b28969898972ab,
    0x6b0a89d5b68ec777,
    0x368f573e8b7a31b7,
    0x1dc636dce936d94b,
    0x207a4c4e5554d5b6,
    0xa474b34628239acb,
    0x3b06a83e1ca3b912,
    0x90e78d6c2f02baf7,
    0xe1c92df7150d9a8a,
    0x8e95053a1086d3ad,
    0x5a2ef4f1b83a0722,
    0xa50fac949f807fae,
    0x0e7303eb80d8d681,
    0x99b07edc1570ad0f,
    0x689d2fb555fd3076,
    0x00005082119ea468,
    0xc4b08306a88fcc28,
    0x3eb0678af6374afd,
    0xf19f87ab86ad7436,
    0xf2129fbfbe6bc736,
    0x481149575c98a4ed,
    0x0000010695477bc5,
    0x1fba37801a9ceacc,
    0x3bf06fd663a49b6d,
    0x99687e9782e3874b,
    0x79a10673aa50d8e3,
    0xe4accf9e6211f420,
    0x2520e71f87579071,
    0x2bd5d3fd781a8a9b,
    0x00de4dcddd11c873,
    0xeaa9311c5a87392f,
    0xdb748eb617bc40ff,
    0xaf579a8df620bf6f,
    0x86a6e5da1b09c2b1,
    0xcc2fc30ac322a12e,
    0x355e2afec1f74267,
    0x2d99c8f4c021a47b,
    0xbade4b4a9404cfc3,
    0xf7b518721d707d69,
    0x3286b6587bf32c20,
    0x0000b68886af270c,
    0xa115d6e4db8a9079,
    0x484f7e9c97b2e199,
    0xccca7bb75713e301,
    0xbf2584a62bb0f160,
    0xade7e813625dbcc8,
    0x000070940d87955a,
    0x8ae69108139e626f,
    0xbd776ad72fde38a2,
    0xfb6b001fc2fcc0cf,
    0xc7a474b8e67bc427,
    0xbaf6f11610eb5d58,
    0x09cb1f5b6de770d1,
    0xb0b219e6977d4c47,
    0x00ccbc386ea7ad4a,
    0xcc849d0adf973f01,
    0x73a3ef7d016af770,
    0xc807d2d386bdbdfe,
    0x7f2ac9966c791730,
    0xd037a86bc6c504da,
    0xf3f17c661eaa609d,
    0xaca626b04daae687,
    0x755a99374f4a5b07,
    0x90837ee65b2caede,
    0x6ee8ad93fd560785,
    0x0000d9e11053edd8,
    0x9e063bb2d21cdbd7,
    0x07ab77f12a01d2b2,
    0xec550255e6641b44,
    0x78fb94a8449c14c6,
    0xc7510e1bc6c0f5f5,
    0x0000320b36e4cae3,
    0x827c33262c8b1a2d,
    0x14675f0b48ea4144,
    0x267bd3a6498deceb,
    0xf1916ff982f5035e,
    0x86221b7ff434fb88,
    0x9dbecee7386f49d8,
    0xea58f8cac80f8f4a,
    0x008d198692fc64d8,
    0x6d38704fbabf9a36,
    0xe032cb07d1e7be4c,
    0x228d21f6ad450890,
    0x635cb1bfc02589a5,
    0x4620a1739ca2ce71,
    0xa7e7dfe3aae5fb58,
    0x0c10ca932b3c0deb,
    0x2727fee884afed7b,
    0xa2df1c6df9e2ab1f,
    0x4dcdd1ac0774f523,
    0x000070ffad33e24e,
    0xa2ace87bc5977816,
    0x9892275ab4286049,
    0xc2861181ddf18959,
    0xbb9972a042483e19,
    0xef70cd3766513078,
    0x00000513abfc9864,
    0xc058b61858c94083,
    0x09e850859725e0de,
    0x9197fb3bf83e7d94,
    0x7e1e626d12b64bce,
    0x520c54507f7b57d1,
    0xbee1797174e22416,
    0x6fd9ac3222e95587,
    0x0023957c9adfbf3e,
    0xa01c7d7e234bbe15,
    0xaba2c758b8a38cbb,
    0x0d1fa0ceec3e2b30,
    0x0bb6a58b7e60b991,
    0x4333dd5b9fa26635,
    0xc2fd3b7d4001c1a3,
    0xfb41802454731127,
    0x65a56185a50d18cb,
    0xf67a02bd8784b54f,
    0x696f11dd67e65063,
    0x00002022fca814ab,
    0x8cd6be912db9d852,
    0x695189b6e9ae8a57,
    0xee9453b50ada0c28,
    0xd8fc5ea91a78845e,
    0xab86bf191a4aa767,
    0x0000c6b5c86415e5,
    0x267310178e08a22e,
    0xed2d101b078bca25,
    0x3b41ed84b226a8fb,
    0x13e622120f28dc06,
    0xa315f5ebfb706d26,
    0x8816c34e3301bace,
    0xe9395b9cbb71fdae,
    0x002ce9202e721648,
    0x4283db1d2bb3c91c,
    0xd77d461ad2b1a6a5,
    0xe2ec17e46eeb866b,
    0xb8e0be4039fbc47c,
    0xdea160c4d5299d04,
    0x7eec86c8d28c3634,
    0x2119ad129f98a399,
    0xa6ccf46b61a283ef,
    0x2c52cedef658c617,
    0x2db4871169acdd83,
    0x0000f0d6f39ecbe9,
    0x3dd5d8c98d2f9489,
    0x8a1872a22b01f584,
    0xf282a4c40e7b3cf2,
    0x8020ec2ccb1ba196,
    0x6693b6e09e59e313,
    0x0000ce19cc7c83eb,
    0x20cb5735f6479c3b,
    0x762ebf3759d75a5b,
    0x207bfe823d693975,
    0xd77dc112339cd9d5,
    0x9ba7834284627d03,
    0x217dc513e95f51e9,
    0xb27b1a29fc5e7816,
    0x00d5cd9831bb662d,
    0x71e39b806d75734c,
    0x7e572af006fb1a23,
    0xa2734f2f6ae91f85,
    0xbf82c6b5022cddf2,
    0x5c3beac60761a0de,
    0xcdc893bb47416998,
    0x6d1085615c187e01,
    0x77f8ae30ac277c5d,
    0x917c6b81122a2c91,
    0x5b75b699add16967,
    0x0000cf6ae79a069b,
    0xf3c40afa60de1104,
    0x2063127aa59167c3,
    0x621de62269d1894d,
    0xd188ac1de62b4726,
    0x107036e2154b673c,
    0x0000b85f28553a1d,
    0xf2ef4e4c18236f3d,
    0xd9d6de6611b9f602,
    0xa1fc7955fb47911c,
    0xeb85fd032f298dbd,
    0xbe27502fb3befae1,
    0xe3034251c4cd661e,
    0x441364d354071836,
    0x0082b36c75f2983e,
    0xb145910316fa66f0,
    0x021c069c9847caf7,
    0x2910dfc75a4b5221,
    0x735b353e1c57a8b5,
    0xce44312ce98ed96c,
    0xbc942e4506bdfa65,
    0xf05086a71257941b,
    0xfec3b215d351cead,
    0x00ae1055e0144202,
    0xf54b40846f42e454,
    0x00007fd9c8bcbcc8,
    0xbfbd9ef317de9bfe,
    0xa804302ff2854e12,
    0x39ce4957a5e5d8d4,
    0xffb9e2a45637ba84,
    0x55b9ad1d9ea0818b,
    0x00008acbf319178a,
    0x48e2bfc8d0fbfb38,
    0x8be39841e848b5e8,
    0x0e2712160696a08b,
    0xd51096e84b44242a,
    0x1101ba176792e13a,
    0xc22e770f4531689d,
    0x1689eff272bbc56c,
    0x00a92a197f5650ec,
    0xbc765990bda1784e,
    0xc61441e392fcb8ae,
    0x07e13a2ced31e4a0,
    0x92cbe984234e9d4d,
    0x8f4ff572bb7d8ac5,
    0x0b9670c00b963bd0,
    0x62955a581a03eb01,
    0x645f83e5ea000254,
    0x41fce516cd88f299,
    0xbbda9748da7a98cf,
    0x0000aab2fe4845fa,
    0x19761b069bf56555,
    0x8b8f5e8343b6ad56,
    0x3e5d1cfd144821d9,
    0xec5c1e2ca2b0cd8f,
    0xfaf7e0fea7fbb57f,
    0x000000d3ba12961b,
    0xda3f90178401b18e,
    0x70ff906de33a5feb,
    0x0527d5a7c06970e7,
    0x22d8e773607c13e9,
    0xc9ab70df643c3bac,
    0xeda4c6dc8abe12e3,
    0xecef1f410033e78a,
    0x0024c2b274ac72cb,
    0x06740d954fa900b4,
    0x1d7a299b323d6304,
    0xb3c37cb298cbead5,
    0xc986e3c76178739b,
    0x9fabea364b46f58a,
    0x6da214c5af85cc56,
    0x17a43ed8b7a38f84,
    0x6eccec511d9adbeb,
    0xf9cab30913335afb,
    0x4a5e60c5f415eed2,
    0x00006967503672b4,
    0x9da51d121454bb87,
    0x84321e13b9bbc816,
    0xfb3d6fb6ab2fdd8d,
    0x60305eed8e160a8d,
    0xcbbf4b14e9946ce8,
    0x00004f63381b10c3,
    0x07d5b7816fcc4e10,
    0xe5a536726a6a8155,
    0x57afb23447a07fdd,
    0x18f346f7abc9d394,
    0x636dc655d61ad33d,
    0xcc8bab4939f7f3f6,
    0x63c7a906c1dd187b,
];

/// FastCDC gear-hash boundary finder.
///
/// This is the shardline server's `CdcChunker` algorithm verbatim
/// (`shardline_server::upload_ingest::cdc::CdcChunker::find_boundary`): a
/// rolling gear hash with a 64-byte warmup window, a boundary triggered when
/// `i >= min_chunk && (hash & mask) == 0`, a forced boundary at `max_chunk`,
/// and `pending_len` resume semantics so scanning can span multiple buffer
/// feeds.
#[derive(Debug, Clone)]
struct CdcChunker {
    hash: u64,
    min_chunk: usize,
    max_chunk: usize,
    mask: u64,
    pending_len: usize,
}

impl CdcChunker {
    /// Creates a new chunker with the given target chunk size.
    ///
    /// `target_chunk_size` must be a power of two and greater than 64.
    #[must_use]
    fn new(target_chunk_size: usize) -> Self {
        debug_assert!(target_chunk_size.is_power_of_two());
        debug_assert!(target_chunk_size > 64);

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
    const fn min_chunk(&self) -> usize {
        self.min_chunk
    }

    /// Find the next chunk boundary in `data` starting from `self.pending_len`.
    ///
    /// `data` is the full accumulated pending buffer. Bytes `[0..pending_len]`
    /// have already been scanned; scanning resumes from `pending_len`.
    ///
    /// Returns `Some(boundary_offset)` where `boundary_offset` is the position
    /// within `data` where the chunk ends, or `None` if no boundary was found.
    ///
    /// Semantics match the server's `CdcChunker::find_boundary` exactly:
    /// - Skips the first `min_chunk - 64 - 1` bytes without hashing (hash
    ///   window warmup);
    /// - Resets the hash to 0 after each boundary is found;
    /// - Forces a boundary at `max_chunk` if no natural boundary is found.
    fn find_boundary(&mut self, data: &[u8]) -> Option<usize> {
        let n_bytes = data.len();
        if n_bytes == 0 {
            return None;
        }

        let previous_len = self.pending_len;
        // Resume scanning from where we left off — bytes [0..previous_len]
        // were already scanned in prior calls and their hash contribution is
        // already in self.hash.
        let mut cur_index = previous_len;
        let mut create_chunk = false;

        // Skip the minimum chunk zone. The hash has a window size of 64, so
        // skip only min_chunk - 64 - 1 bytes without hashing.
        if previous_len.wrapping_add(64) < self.min_chunk {
            let skip = self
                .min_chunk
                .saturating_sub(previous_len)
                .saturating_sub(64)
                .saturating_sub(1)
                .min(n_bytes.saturating_sub(previous_len));
            cur_index = cur_index.wrapping_add(skip);
        }

        // Don't scan past the maximum chunk boundary.
        let read_end = n_bytes.min(self.max_chunk);

        // Scan for a boundary using the Gear hash.
        let mut boundary_index = None;
        for i in cur_index..read_end {
            self.hash = (self.hash << 1).wrapping_add({
                let byte = *data.get(i).unwrap_or(&0);
                *GEAR_TABLE.get(byte as usize).unwrap_or(&0)
            });

            // Enforce the minimum chunk size — i is an absolute position in
            // the pending buffer, which equals the position from the current
            // chunk start (pending is always fresh after a boundary flush).
            if i >= self.min_chunk && (self.hash & self.mask) == 0 {
                boundary_index = Some(i);
                break;
            }
        }

        if let Some(i) = boundary_index {
            cur_index = i.wrapping_add(1);
            create_chunk = true;
        }

        // If no boundary found, advance cur_index to read_end so the
        // max-chunk check below uses the correct position.
        if !create_chunk {
            cur_index = read_end;
        }

        // Force a boundary at max_chunk.
        if cur_index >= self.max_chunk {
            cur_index = self.max_chunk;
            create_chunk = true;
        }

        if create_chunk {
            self.hash = 0; // Reset for the next chunk (matches server behavior).
            self.pending_len = 0;
            Some(cur_index)
        } else {
            // Mark all bytes up to read_end as scanned.
            self.pending_len = cur_index;
            None
        }
    }

    /// Resets the internal hash state for scanning a new chunk.
    const fn reset(&mut self) {
        self.hash = 0;
        self.pending_len = 0;
    }
}

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
    /// `target_chunk_size` must be a power of two and greater than 64; the
    /// default is [`DEFAULT_TARGET_CHUNK_SIZE`] (64 KiB).
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
        self.cdc.max_chunk
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

        if !self.chunkbuf.is_empty() {
            // A partial trailing chunk is buffered: append the block and scan
            // the combined buffer (server accumulate-then-scan semantics).
            self.chunkbuf.extend_from_slice(data);
            self.drain_buffer(&mut chunks);
        } else {
            // Zero-copy fast path: scan the block directly.
            let mut pos = 0;
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
                            self.chunkbuf.extend_from_slice(remaining);
                        }
                        break;
                    }
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
        if is_final {
            self.reset();
        }
        chunks
    }

    /// Flushes any remaining buffered bytes as the final chunk and resets.
    #[must_use]
    pub fn finish(&mut self) -> Option<Chunk> {
        self.next(&[], true).0
    }

    /// Emits every complete chunk from the internal buffer, keeping any
    /// remaining partial tail for the next block.
    fn drain_buffer(&mut self, chunks: &mut Vec<Chunk>) {
        while let Some(boundary) = self.cdc.find_boundary(&self.chunkbuf) {
            let tail = self.chunkbuf.split_off(boundary);
            let chunk_bytes = std::mem::replace(&mut self.chunkbuf, tail);
            chunks.push(Chunk::new(Bytes::from(chunk_bytes)));
            if self.chunkbuf.is_empty() {
                break;
            }
        }
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
