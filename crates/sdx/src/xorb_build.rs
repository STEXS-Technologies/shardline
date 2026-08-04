//! Xorb build / serialize primitives (M3a).
//!
//! [`build_xorb`] produces a fully serialized xorb (chunk payload + footer)
//! that is **byte-identical** to the shardline server's
//! `shardline_server::upload_ingest::xorb_packer::pack_chunks_into_xorb` on
//! the same chunk inputs, via the pinned upstream `xet-core-structures` crate
//! (this crate does **not** depend on the server's `shardline-xet-core` fork).
//!
//! The chunk payload stream (`XorbChunkHeader` + compressed data per chunk,
//! BG4+LZ4) is identical between the fork and upstream 1.5.2. The footer is
//! **not**: the shardline fork serializes format-v2 footers with `u64` scalar
//! and offset fields while upstream 1.5.2 emits format-v1 `u32` footers, so
//! [`build_xorb`] assembles the fork's v2 footer layout directly (the private
//! `append_footer` helper). The xorb hash (Merkle root over
//! `"{hex} : {size}\n"` lines) and the per-chunk content hashes are computed
//! with the upstream primitives, which M0 golden tests already pin
//! byte-identical to the fork.
//!
//! This module also provides the xorb cut-condition and body-limit safety
//! helpers the M3b upload path needs (`docs/SDX_PLAN.md` §4.4.2): cut at
//! 64 MiB / 8192 chunks on uncompressed data, and never let the serialized
//! size exceed shardline's 64 MiB request-body cap (serialized size adds
//! 8-byte chunk headers + footer blocks, so incompressible data can exceed the
//! uncompressed cut — the helpers keep the worst case under a configurable
//! safety cap, default [`SERIALIZED_XORB_SAFETY_CAP_BYTES`] = 60 MiB).

use bytes::Bytes;
use xet_core_structures::merklehash::MerkleHash;
use xet_core_structures::merklehash::xorb_hash;
use xet_core_structures::xorb_object::CompressionScheme;
use xet_core_structures::xorb_object::serialize_chunk;

use crate::error::SdxError;
use crate::hash::{compute_chunk_hash, xet_hash_hex_string};

/// Maximum uncompressed bytes per xorb: 64 MiB.
pub const MAX_XORB_BYTES: usize = 64 * 1024 * 1024;
/// Maximum chunk count per xorb: 8192.
pub const MAX_XORB_CHUNKS: usize = 8 * 1024;
/// Default serialized-size safety cap for xorb bodies (below shardline's
/// 64 MiB request-body limit, leaving room for chunk headers + footer).
pub const SERIALIZED_XORB_SAFETY_CAP_BYTES: u64 = 60 * 1024 * 1024;

/// Serialized chunk header size (version byte + 3-byte compressed length +
/// scheme byte + 3-byte uncompressed length).
const XORB_CHUNK_HEADER_BYTES: usize = 8;
/// Footer fixed overhead (fork format v2): ident(7) + version(1) + xorb
/// hash(32) + hashes ident(7) + hashes version(1) + num_chunks(8) + boundaries
/// ident(7) + boundaries version(1) + num_chunks(8) + num_chunks(8) + two
/// section offsets(16) + 16-byte buffer = 112, plus the trailing 8-byte
/// `info_length` field.
const XORB_FOOTER_FIXED_BYTES: usize = 120;
/// Footer overhead per chunk: 32-byte hash + 8-byte boundary + 8-byte
/// unpacked offset = 48.
const XORB_FOOTER_PER_CHUNK_BYTES: usize = 48;

/// `XETBLOB` footer ident.
const XORB_FORMAT_IDENT: [u8; 7] = *b"XETBLOB";
/// `XBLBHSH` hashes-section ident.
const XORB_HASHES_SECTION_IDENT: [u8; 7] = *b"XBLBHSH";
/// `XBLBBND` boundaries-section ident.
const XORB_BOUNDARIES_SECTION_IDENT: [u8; 7] = *b"XBLBBND";
/// Fork footer format version (shardline-xet-core `XORB_OBJECT_FORMAT_VERSION`).
const XORB_FORMAT_VERSION: u8 = 2;
/// Hashes-section version.
const XORB_HASHES_SECTION_VERSION: u8 = 0;
/// Boundaries-section version (with unpacked offsets).
const XORB_BOUNDARIES_SECTION_VERSION: u8 = 1;
/// Trailing footer extensibility buffer (excluded from the xorb hash).
const XORB_FOOTER_BUFFER_LEN: usize = 16;

/// Metadata for one chunk packed inside a xorb.
#[derive(Debug, Clone)]
pub struct XorbChunkEntry {
    /// Index of this chunk within the xorb (0-based).
    pub chunk_index: u32,
    /// Byte offset of this chunk in the original file (uncompressed).
    pub raw_offset: u64,
    /// Uncompressed chunk length.
    pub raw_length: u64,
    /// Byte offset of this chunk in the serialized xorb (including header).
    pub packed_offset: u64,
    /// Serialized length of this chunk (header + compressed payload).
    pub packed_length: u64,
    /// Content hash of the chunk (BLAKE3 `DATA_KEY`).
    pub hash: MerkleHash,
}

/// A fully packed xorb container.
#[derive(Debug, Clone)]
pub struct BuiltXorb {
    /// Complete serialized xorb bytes (chunk payload + footer).
    pub serialized: Vec<u8>,
    /// Xorb content hash in Xet CAS API hexadecimal format.
    pub xorb_hash_hex: String,
    /// Byte offset where the footer starts (the chunk payload length). Feed
    /// `serialized[..footer_start]` to the footer-less [`crate::xorb::XorbReader`].
    pub footer_start: usize,
    /// Per-chunk metadata within this xorb.
    pub chunk_entries: Vec<XorbChunkEntry>,
}

impl BuiltXorb {
    /// Returns the serialized chunk payload (footer excluded) as a fresh slice.
    ///
    /// This is the byte range the ranged transfer endpoint serves and the
    /// input to [`crate::xorb::XorbReader`].
    #[must_use]
    pub fn payload(&self) -> &[u8] {
        self.serialized.get(..self.footer_start).unwrap_or_default()
    }
}

/// Packs CDC chunks (raw bytes + file offset) into a serialized xorb with
/// BG4+LZ4 compression and a format-v2 footer, byte-identical to the server's
/// `pack_chunks_into_xorb`.
///
/// # Errors
///
/// Returns [`SdxError::XorbBuild`] when `chunks` is empty or a chunk fails to
/// serialize.
pub fn build_xorb(chunks: &[(Bytes, u64)]) -> Result<BuiltXorb, SdxError> {
    if chunks.is_empty() {
        return Err(SdxError::XorbBuild(
            "cannot build an xorb with no chunks".to_owned(),
        ));
    }

    // Serialize the chunk payload stream and collect footer metadata.
    let mut payload = Vec::new();
    let mut chunk_hashes = Vec::with_capacity(chunks.len());
    let mut chunk_boundary_offsets = Vec::with_capacity(chunks.len());
    let mut unpacked_chunk_offsets = Vec::with_capacity(chunks.len());
    let mut chunk_entries = Vec::with_capacity(chunks.len());
    let mut packed_offset: u64 = 0;
    let mut unpacked_total: u64 = 0;

    for (index, (data, raw_offset)) in chunks.iter().enumerate() {
        let hash = compute_chunk_hash(data);
        chunk_hashes.push(hash);

        let written = serialize_chunk(data, &mut payload, CompressionScheme::ByteGrouping4LZ4)
            .map_err(|error| {
                SdxError::XorbBuild(format!("chunk {index} serialization failed: {error}"))
            })?;
        let written = u64::try_from(written).unwrap_or(u64::MAX);

        chunk_entries.push(XorbChunkEntry {
            chunk_index: u32::try_from(index).unwrap_or(u32::MAX),
            raw_offset: *raw_offset,
            raw_length: u64::try_from(data.len()).unwrap_or(u64::MAX),
            packed_offset,
            packed_length: written,
            hash,
        });

        packed_offset = packed_offset.saturating_add(written);
        chunk_boundary_offsets.push(packed_offset);
        unpacked_total =
            unpacked_total.saturating_add(u64::try_from(data.len()).unwrap_or(u64::MAX));
        unpacked_chunk_offsets.push(unpacked_total);
    }

    // Xorb hash: Merkle root over (chunk hash, size) pairs.
    let xorb_hash = xorb_hash(
        &chunks
            .iter()
            .map(|(data, _)| {
                (
                    compute_chunk_hash(data),
                    u64::try_from(data.len()).unwrap_or(u64::MAX),
                )
            })
            .collect::<Vec<_>>(),
    );

    // Append the fork-format v2 footer.
    let footer = append_footer(
        xorb_hash,
        &chunk_hashes,
        &chunk_boundary_offsets,
        &unpacked_chunk_offsets,
    );
    let footer_start = payload.len();
    payload.extend_from_slice(&footer);

    Ok(BuiltXorb {
        serialized: payload,
        xorb_hash_hex: xet_hash_hex_string(xorb_hash),
        footer_start,
        chunk_entries,
    })
}

/// Appends the shardline format-v2 xorb footer to `out`.
///
/// Layout (LE scalars, matching `shardline-xet-core`'s
/// `XorbObjectInfoV1::serialize` + `XorbObject::serialize_given_info`):
///
/// ```text
/// "XETBLOB" | version=2 | xorb_hash(32) |
/// "XBLBHSH" | hashes_version=0 | num_chunks(u64) | chunk_hashes(32·n) |
/// "XBLBBND" | boundaries_version=1 | num_chunks(u64) |
///   chunk_boundary_offsets(u64·n) | unpacked_chunk_offsets(u64·n) |
/// num_chunks(u64) | hashes_section_offset_from_end(u64) |
///   boundary_section_offset_from_end(u64) | buffer(16) |
/// info_length(u64)
/// ```
fn append_footer(
    xorb_hash: MerkleHash,
    chunk_hashes: &[MerkleHash],
    chunk_boundary_offsets: &[u64],
    unpacked_chunk_offsets: &[u64],
) -> Vec<u8> {
    let num_chunks = u64::try_from(chunk_hashes.len()).unwrap_or(u64::MAX);

    let boundary_section_offset_from_end = (XORB_BOUNDARIES_SECTION_IDENT.len() as u64)
        .saturating_add(1)
        .saturating_add(8) // boundaries_version + num_chunks
        .saturating_add(
            8u64.saturating_mul(u64::try_from(chunk_boundary_offsets.len()).unwrap_or(u64::MAX)),
        )
        .saturating_add(
            8u64.saturating_mul(u64::try_from(unpacked_chunk_offsets.len()).unwrap_or(u64::MAX)),
        )
        .saturating_add(8) // num_chunks
        .saturating_add(8) // hashes_section_offset_from_end
        .saturating_add(8) // boundary_section_offset_from_end
        .saturating_add(XORB_FOOTER_BUFFER_LEN as u64);

    let hashes_section_offset_from_end = (XORB_HASHES_SECTION_IDENT.len() as u64)
        .saturating_add(1)
        .saturating_add(8) // hashes_version + num_chunks
        .saturating_add(32u64.saturating_mul(u64::try_from(chunk_hashes.len()).unwrap_or(u64::MAX)))
        .saturating_add(boundary_section_offset_from_end);

    let mut out = Vec::with_capacity(
        XORB_FOOTER_FIXED_BYTES
            .saturating_add(XORB_FOOTER_PER_CHUNK_BYTES.saturating_mul(chunk_hashes.len())),
    );

    out.extend_from_slice(&XORB_FORMAT_IDENT);
    out.push(XORB_FORMAT_VERSION);
    out.extend_from_slice(&merkle_bytes(xorb_hash));

    out.extend_from_slice(&XORB_HASHES_SECTION_IDENT);
    out.push(XORB_HASHES_SECTION_VERSION);
    out.extend_from_slice(&num_chunks.to_le_bytes());
    for hash in chunk_hashes {
        out.extend_from_slice(&merkle_bytes(*hash));
    }

    out.extend_from_slice(&XORB_BOUNDARIES_SECTION_IDENT);
    out.push(XORB_BOUNDARIES_SECTION_VERSION);
    out.extend_from_slice(&num_chunks.to_le_bytes());
    for offset in chunk_boundary_offsets {
        out.extend_from_slice(&offset.to_le_bytes());
    }
    for offset in unpacked_chunk_offsets {
        out.extend_from_slice(&offset.to_le_bytes());
    }

    out.extend_from_slice(&num_chunks.to_le_bytes());
    out.extend_from_slice(&hashes_section_offset_from_end.to_le_bytes());
    out.extend_from_slice(&boundary_section_offset_from_end.to_le_bytes());
    out.extend_from_slice(&[0u8; XORB_FOOTER_BUFFER_LEN]);

    // Trailing info length (u64), the last 8 bytes of a serialized xorb.
    let info_length = u64::try_from(out.len()).unwrap_or(u64::MAX);
    out.extend_from_slice(&info_length.to_le_bytes());
    out
}

fn merkle_bytes(hash: MerkleHash) -> [u8; 32] {
    hash.into()
}

/// Returns `true` when adding a chunk of `next_chunk_size` to a xorb holding
/// `new_data_size` uncompressed bytes across `chunk_count` chunks requires a
/// new xorb (mirror `docs/SDX_PLAN.md` §4.4.2 / upstream
/// `file_deduplication.rs` cut condition):
///
/// `new_data_size + next_chunk_size > 64 MiB || chunk_count + 1 > 8192`
#[must_use]
pub const fn xorb_cut_condition(
    new_data_size: usize,
    chunk_count: usize,
    next_chunk_size: usize,
) -> bool {
    new_data_size.saturating_add(next_chunk_size) > MAX_XORB_BYTES
        || chunk_count.saturating_add(1) > MAX_XORB_CHUNKS
}

/// Returns `true` when a serialized body of `serialized_len` bytes fits within
/// a `cap_bytes` body limit.
#[must_use]
pub const fn serialized_size_le(serialized_len: u64, cap_bytes: u64) -> bool {
    serialized_len <= cap_bytes
}

/// Returns the largest single chunk size that can be added to a xorb holding
/// `current_bytes` uncompressed across `current_chunks` chunks without
/// violating the 64 MiB / 8192 cut condition or the serialized-size safety
/// cap (worst case: 8-byte chunk headers + format-v2 footer overhead).
///
/// M3b should cut on the serialized size (or call this helper) so the upload
/// body never exceeds shardline's 64 MiB request-body cap
/// (`docs/SDX_PLAN.md` §4.4.2).
#[must_use]
pub fn xorb_max_addable_chunk(
    current_bytes: usize,
    current_chunks: usize,
    safety_cap_bytes: u64,
) -> usize {
    if current_chunks >= MAX_XORB_CHUNKS {
        return 0;
    }
    let bytes_budget = MAX_XORB_BYTES.saturating_sub(current_bytes);

    let next_chunk_count = current_chunks.saturating_add(1);
    let header_overhead = XORB_CHUNK_HEADER_BYTES.saturating_mul(next_chunk_count);
    let footer_overhead = XORB_FOOTER_FIXED_BYTES
        .saturating_add(XORB_FOOTER_PER_CHUNK_BYTES.saturating_mul(next_chunk_count));
    let overhead = header_overhead.saturating_add(footer_overhead);

    let serialized_budget = safety_cap_bytes
        .saturating_sub(u64::try_from(current_bytes).unwrap_or(u64::MAX))
        .saturating_sub(u64::try_from(overhead).unwrap_or(u64::MAX));

    let serialized_budget = usize::try_from(serialized_budget).unwrap_or(usize::MAX);
    bytes_budget.min(serialized_budget)
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use bytes::Bytes;

    use super::{
        BuiltXorb, MAX_XORB_BYTES, MAX_XORB_CHUNKS, SERIALIZED_XORB_SAFETY_CAP_BYTES,
        XORB_CHUNK_HEADER_BYTES, XORB_FOOTER_FIXED_BYTES, XORB_FOOTER_PER_CHUNK_BYTES, build_xorb,
        serialized_size_le, xorb_cut_condition, xorb_max_addable_chunk,
    };
    use crate::hash::parse_xet_hash_hex;
    use crate::xorb::XorbReader;

    /// LCG pseudo-random data.
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

    fn chunk_pairs(chunks: &[Vec<u8>]) -> Vec<(Bytes, u64)> {
        let mut offset = 0u64;
        chunks
            .iter()
            .map(|data| {
                let pair = (Bytes::copy_from_slice(data), offset);
                offset = offset.saturating_add(u64::try_from(data.len()).unwrap());
                pair
            })
            .collect()
    }

    /// Asserts byte-identity with the server packer on the same chunk inputs.
    fn assert_byte_identical(chunks: &[Vec<u8>]) {
        let pairs = chunk_pairs(chunks);
        let server = shardline_server::upload_ingest::xorb_packer::pack_chunks_into_xorb(
            &pairs
                .iter()
                .map(|(data, offset)| (data.to_vec(), *offset))
                .collect::<Vec<_>>(),
        )
        .unwrap();

        let built = build_xorb(&pairs).unwrap();

        assert_eq!(
            built.serialized, server.serialized,
            "serialized bytes differ from server packer"
        );
        assert_eq!(
            built.xorb_hash_hex, server.xorb_hash_hex,
            "xorb hash hex differs from server packer"
        );
        assert_eq!(
            built.chunk_entries.len(),
            server.chunk_entries.len(),
            "chunk entry count differs"
        );
        for (mine, theirs) in built.chunk_entries.iter().zip(server.chunk_entries.iter()) {
            assert_eq!(mine.chunk_index, theirs.chunk_index);
            assert_eq!(mine.raw_offset, theirs.raw_offset);
            assert_eq!(mine.raw_length, theirs.raw_length);
            assert_eq!(mine.packed_offset, u64::from(theirs.packed_offset));
            assert_eq!(mine.packed_length, u64::from(theirs.packed_length));
        }
        // Footer start equals the server's packed content length.
        let _ = parse_xet_hash_hex(&built.xorb_hash_hex).unwrap();
        assert_eq!(built.payload().len(), built.footer_start);
        assert!(!built.payload().is_empty());
        assert!(built.serialized.len() >= built.footer_start);
    }

    #[test]
    fn build_xorb_matches_server_single_chunk() {
        assert_byte_identical(&[b"hello world".to_vec()]);
        assert_byte_identical(&[b"A".repeat(2_000_000)]);
        assert_byte_identical(&[lcg_data(1024 * 1024, 1)]);
        assert_byte_identical(&[vec![0u8; 65536]]);
    }

    #[test]
    fn build_xorb_matches_server_multiple_chunks() {
        assert_byte_identical(&[b"hello ".to_vec(), b"world".to_vec(), b"!".to_vec()]);
        assert_byte_identical(&[vec![0xABu8; 65536], vec![0xCDu8; 65536], lcg_data(4096, 5)]);
    }

    #[test]
    fn build_xorb_matches_server_max_chunks() {
        // 8192 tiny chunks (the max per xorb).
        let chunks: Vec<Vec<u8>> = (0..MAX_XORB_CHUNKS)
            .map(|i| vec![(i & 0xFF) as u8; 64])
            .collect();
        assert_byte_identical(&chunks);
    }

    #[test]
    fn build_xorb_matches_server_binary_and_incompressible() {
        let chunks: Vec<Vec<u8>> = (0..256)
            .map(|i| (0..64).map(|j| ((i + j) & 0xFF) as u8).collect())
            .collect();
        assert_byte_identical(&chunks);
        assert_byte_identical(&(0..64).map(|i| lcg_data(65536, i)).collect::<Vec<_>>());
    }

    #[test]
    fn build_xorb_empty_errors() {
        assert!(build_xorb(&[]).is_err());
    }

    #[test]
    fn build_xorb_deterministic() {
        let chunks = chunk_pairs(&[
            b"deterministic a".to_vec(),
            lcg_data(8192, 9),
            b"deterministic b".to_vec(),
        ]);
        let first = build_xorb(&chunks).unwrap();
        let second = build_xorb(&chunks).unwrap();
        assert_eq!(first.serialized, second.serialized);
        assert_eq!(first.xorb_hash_hex, second.xorb_hash_hex);
    }

    #[test]
    fn build_xorb_hash_depends_on_chunk_order() {
        let chunks_a = chunk_pairs(&[b"first".to_vec(), b"second".to_vec(), b"third".to_vec()]);
        let chunks_b = chunk_pairs(&[b"first".to_vec(), b"third".to_vec(), b"second".to_vec()]);
        let built_a = build_xorb(&chunks_a).unwrap();
        let built_b = build_xorb(&chunks_b).unwrap();
        assert_ne!(built_a.serialized, built_b.serialized);
        assert_ne!(built_a.xorb_hash_hex, built_b.xorb_hash_hex);
    }

    #[test]
    fn build_xorb_roundtrips_through_sdx_xorb_reader() {
        let chunks = chunk_pairs(&[
            b"round-trip chunk one".to_vec(),
            lcg_data(100_000, 11),
            b"round-trip chunk three".to_vec(),
        ]);
        let built = build_xorb(&chunks).unwrap();
        let decoded = XorbReader::new(built.payload().to_vec())
            .decode_chunks()
            .unwrap();
        assert_eq!(decoded.len(), chunks.len());
        for (decoded_chunk, (expected, _)) in decoded.iter().zip(chunks.iter()) {
            assert_eq!(decoded_chunk.data, expected.as_ref());
        }
    }

    #[test]
    fn build_xorb_roundtrips_through_server_validation_path() {
        let chunks = chunk_pairs(&[
            b"server validation chunk".to_vec(),
            lcg_data(50_000, 21),
            b"tail".to_vec(),
        ]);
        let built = build_xorb(&chunks).unwrap();

        let expected: shardline_protocol::ShardlineHash = {
            let hash: xet_core_structures::merklehash::MerkleHash =
                parse_xet_hash_hex(&built.xorb_hash_hex).unwrap();
            let bytes: [u8; 32] = hash.into();
            shardline_protocol::ShardlineHash::from_bytes(bytes)
        };

        let mut cursor = Cursor::new(built.serialized.as_slice());
        let validated = shardline_server::validate_serialized_xorb(&mut cursor, expected).unwrap();
        let decoded =
            shardline_server::decode_serialized_xorb_chunks(&mut cursor, &validated).unwrap();
        assert_eq!(decoded.len(), chunks.len());
        for (decoded_chunk, (expected, _)) in decoded.iter().zip(chunks.iter()) {
            assert_eq!(decoded_chunk.data(), expected.as_ref());
        }
    }

    #[test]
    fn build_xorb_large_xorb_respects_serialized_safety_estimates() {
        // ~64 MiB of incompressible data across 1024 chunks. Serialized size
        // (LZ4 expansion + 8-byte headers + format-v2 footer) can exceed the
        // 60 MiB safety cap and even the 64 MiB hard body cap — exactly why
        // M3b must cut on the serialized size (plan §4.4.2).
        let chunks: Vec<Vec<u8>> = (0..1024).map(|i| lcg_data(65536, i)).collect();
        let pairs = chunk_pairs(&chunks);
        let built = build_xorb(&pairs).unwrap();
        let overhead = XORB_CHUNK_HEADER_BYTES.saturating_mul(1024).saturating_add(
            XORB_FOOTER_FIXED_BYTES
                .saturating_add(XORB_FOOTER_PER_CHUNK_BYTES.saturating_mul(1024)),
        );
        assert!(built.serialized.len() <= MAX_XORB_BYTES.saturating_add(overhead));
        assert!(
            built.serialized.len() > MAX_XORB_BYTES,
            "incompressible data at the uncompressed cut must exceed the 64 MiB body cap"
        );
        assert!(
            !serialized_size_le(
                u64::try_from(built.serialized.len()).unwrap_or(u64::MAX),
                SERIALIZED_XORB_SAFETY_CAP_BYTES
            ),
            "a 64 MiB uncompressed xorb must not be assumed safe"
        );

        // A build planned with `xorb_max_addable_chunk` (60 MiB cap) stays
        // under the cap even for incompressible data.
        let chunk_size = 65536usize;
        let mut total = 0usize;
        let mut count = 0usize;
        while xorb_max_addable_chunk(total, count, SERIALIZED_XORB_SAFETY_CAP_BYTES) >= chunk_size {
            total = total.saturating_add(chunk_size);
            count = count.saturating_add(1);
        }
        let planned: Vec<Vec<u8>> = (0..count).map(|i| lcg_data(chunk_size, i as u64)).collect();
        let built = build_xorb(&chunk_pairs(&planned)).unwrap();
        assert!(
            serialized_size_le(
                u64::try_from(built.serialized.len()).unwrap_or(u64::MAX),
                SERIALIZED_XORB_SAFETY_CAP_BYTES
            ),
            "helper-planned xorb of {} uncompressed bytes serialized to {} bytes, exceeding the 60 MiB cap",
            total,
            built.serialized.len()
        );
    }

    #[test]
    fn xorb_cut_condition_mirrors_plan() {
        assert!(!xorb_cut_condition(0, 0, 1024));
        assert!(xorb_cut_condition(MAX_XORB_BYTES, 0, 1));
        assert!(xorb_cut_condition(0, MAX_XORB_CHUNKS, 1));
        assert!(xorb_cut_condition(MAX_XORB_BYTES.saturating_sub(1), 0, 2));
        assert!(!xorb_cut_condition(MAX_XORB_BYTES.saturating_sub(1), 0, 1));
        assert!(!xorb_cut_condition(0, MAX_XORB_CHUNKS.saturating_sub(1), 1));
    }

    #[test]
    fn xorb_max_addable_chunk_respects_all_budgets() {
        // Empty xorb: the serialized budget binds first (headers + footer
        // overhead must fit under the cap).
        let cap_budget = xorb_max_addable_chunk(0, 0, SERIALIZED_XORB_SAFETY_CAP_BYTES);
        assert!(cap_budget <= SERIALIZED_XORB_SAFETY_CAP_BYTES as usize);
        assert!(cap_budget < MAX_XORB_BYTES);
        // With a 64 MiB cap the uncompressed budget binds, minus per-chunk
        // serialization overhead (header + footer for the one new chunk).
        let overhead = XORB_CHUNK_HEADER_BYTES
            .saturating_add(XORB_FOOTER_FIXED_BYTES)
            .saturating_add(XORB_FOOTER_PER_CHUNK_BYTES);
        let uncapped = xorb_max_addable_chunk(0, 0, MAX_XORB_BYTES as u64);
        assert_eq!(uncapped, MAX_XORB_BYTES.saturating_sub(overhead));
        // Chunk-count limit binds first.
        assert_eq!(xorb_max_addable_chunk(0, MAX_XORB_CHUNKS, u64::MAX), 0);
        assert!(xorb_max_addable_chunk(0, MAX_XORB_CHUNKS.saturating_sub(1), u64::MAX) > 0);
        // A nearly full xorb has a small budget.
        assert!(xorb_max_addable_chunk(MAX_XORB_BYTES.saturating_sub(1000), 0, u64::MAX) <= 1000);
    }

    #[test]
    fn built_xorb_payload_is_footer_excluded_slice() {
        let chunks = chunk_pairs(&[b"payload slice".to_vec()]);
        let built: BuiltXorb = build_xorb(&chunks).unwrap();
        assert_eq!(built.payload(), &built.serialized[..built.footer_start]);
        assert!(built.serialized[built.footer_start..].starts_with(b"XETBLOB"));
    }
}
