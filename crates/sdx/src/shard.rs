//! Serialize and parse Xet metadata shards — the manifest that maps files to
//! the xorb chunk ranges that contain their data.
//!
//! Shardline's server fork of `xet-core` serializes metadata shards with
//! **format version 3** and `u64` entry fields, while the pinned upstream
//! `xet-core-structures` 1.5.2 writes version 2 with `u32` fields (same
//! divergence class as the xorb footer in the write path). This module
//! assembles v3 upload shards and reads v2/v3 dedup shards with the shared
//! core header and entry readers (`docs/SDX_PLAN.md` §4.4.2 / §9-M3).
//!
//! Byte layout (little-endian scalars; v3 headers and entries occupy 64 bytes,
//! including four trailing padding bytes):
//!
//! ```text
//! MDBShardFileHeader:  tag(32) | version(u64 = 3) | footer_size(u64 = 200)
//!   per file:          FileDataSequenceHeader(64) | FileDataSequenceEntry(64) × n
//!   FileDataSequenceHeader bookend (all-ones file hash)
//!   per xorb:          XorbChunkSequenceHeader(64) | XorbChunkSequenceEntry(64) × n
//!   XorbChunkSequenceHeader bookend (all-ones xorb hash)
//! MDBShardFileFooter:  version(u64 = 2) + placeholder offsets (200 bytes total)
//! ```
//!
//! The server's `upload_shard` handler re-normalizes the uploaded bytes with
//! its own `MDBInMemoryShard::to_bytes()` before storing, so what matters for
//! upload is that the header version and entry layout are parseable and that
//! the file/xorb sections are semantically correct; the footer is informational.

use shardline_xet_core::metadata_shard::{
    file_structs::FileDataSequenceHeader,
    shard_format::MDBShardFileHeader,
    xorb_structs::{MAX_XORB_CHUNK_ENTRIES, XorbChunkSequenceEntry, XorbChunkSequenceHeader},
};
use xet_core_structures::merklehash::MerkleHash;

use crate::error::SdxError;

/// Shard header magic tag (`MDB_SHARD_HEADER_TAG`).
const SHARD_HEADER_TAG: [u8; 32] = [
    b'H', b'F', b'R', b'e', b'p', b'o', b'M', b'e', b't', b'a', b'D', b'a', b't', b'a', 0, 85, 105,
    103, 69, 106, 123, 129, 87, 131, 165, 189, 217, 92, 205, 209, 74, 169,
];
/// Shard header format version (fork `MDB_SHARD_HEADER_VERSION`).
const SHARD_HEADER_VERSION: u64 = 3;
/// Shard footer format version (fork `MDB_SHARD_FOOTER_VERSION`).
const SHARD_FOOTER_VERSION: u64 = 2;
/// Serialized header size: tag(32) + version(8) + footer_size(8).
const SHARD_HEADER_SIZE: u64 = 48;
/// Serialized footer size (u64 fields; see module docs).
const SHARD_FOOTER_SIZE: u64 = 200;
/// Serialized v3 file/xorb entry size (60 data bytes + 4 trailing padding bytes:
/// each `#[repr(C)]` header/entry has a `u32` field followed by `u64` fields, so
/// the fork's serializer writes a 64-byte buffer).
const SHARD_ENTRY_SIZE: usize = 64;
/// Trailing zero padding appended to each 60-byte entry to reach 64 bytes.
const SHARD_ENTRY_PADDING: [u8; 4] = [0u8; 4];
/// V2 shard entry size (48 bytes), for parsing legacy shards.
const SHARD_V2_ENTRY_SIZE: usize = 48;
/// Returns the bookend marker hash (all-ones), used to terminate a shard section.
fn bookend() -> MerkleHash {
    MerkleHash::from([0xFFu8; 32])
}
/// File entry flag bits. `MDB_DEFAULT_FILE_FLAG` only (no verification /
/// metadata-ext) for client-built shards.
const MDB_DEFAULT_FILE_FLAG: u32 = 0;
/// `MDB_CHUNK_WITH_GLOBAL_DEDUP_FLAG` — marks a chunk whose global-dedup
/// eligibility was already established (imported from a dedup hit).
pub(crate) const MDB_CHUNK_WITH_GLOBAL_DEDUP_FLAG: u32 = 1 << 31;
/// `MDB_DEFAULT_XORB_FLAG`.
const MDB_DEFAULT_XORB_FLAG: u32 = 0;

/// One file-data range (`FileDataSequenceEntry`): a contiguous range of chunks
/// within a single xorb that maps to a contiguous range of the file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShardSegment {
    /// Xorb the range points into.
    pub xorb_hash: MerkleHash,
    /// Uncompressed bytes this range contributes to the file.
    pub unpacked_segment_bytes: u64,
    /// First chunk index into the xorb (inclusive).
    pub chunk_index_start: u64,
    /// First chunk index past the range (exclusive).
    pub chunk_index_end: u64,
}

/// One file's reconstruction metadata (`MDBFileInfo` without verification /
/// metadata-ext sections).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShardFileEntry {
    /// File content hash (`merklehash::file_hash` over the chunk list).
    pub file_hash: MerkleHash,
    /// Ordered ranges covering the whole file.
    pub segments: Vec<ShardSegment>,
}

/// One chunk within a xorb (`XorbChunkSequenceEntry`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShardXorbChunk {
    /// Content hash of the chunk.
    pub chunk_hash: MerkleHash,
    /// Cumulative uncompressed offset of this chunk within the xorb.
    pub chunk_byte_range_start: u64,
    /// Uncompressed chunk length.
    pub unpacked_segment_bytes: u64,
    /// Chunk flags (global-dedup flag for imported chunks).
    pub flags: u32,
}

/// One xorb's chunk sequence (`MDBXorbInfo`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShardXorb {
    /// Xorb hash.
    pub xorb_hash: MerkleHash,
    /// Total uncompressed bytes in the xorb.
    pub num_bytes_in_xorb: u64,
    /// Chunk entries in xorb order.
    pub chunks: Vec<ShardXorbChunk>,
}

/// Serializes file and xorb sections into a fork-v3 shard byte buffer.
///
/// The output is parseable by the server's `upload_shard` handler, which
/// re-normalizes it before storage. Returns `None`-free `Vec<u8>`; the format
/// has no failure modes beyond memory.
#[must_use]
pub fn serialize_shard(files: &[ShardFileEntry], xorbs: &[ShardXorb]) -> Vec<u8> {
    serialize_shard_iter(files, xorbs)
}

pub(crate) fn serialize_shard_iter<'xorb>(
    files: &[ShardFileEntry],
    xorbs: impl IntoIterator<Item = &'xorb ShardXorb>,
) -> Vec<u8> {
    let mut out = Vec::new();

    // Header.
    out.extend_from_slice(&SHARD_HEADER_TAG);
    out.extend_from_slice(&SHARD_HEADER_VERSION.to_le_bytes());
    out.extend_from_slice(&SHARD_FOOTER_SIZE.to_le_bytes());

    // File sections.
    for file in files {
        append_file_header(&mut out, file.file_hash, file.segments.len());
        for segment in &file.segments {
            append_file_entry(&mut out, segment);
        }
    }
    append_file_header(&mut out, bookend(), 0);

    // Xorb sections.
    for xorb in xorbs {
        append_xorb_header(
            &mut out,
            xorb.xorb_hash,
            xorb.num_bytes_in_xorb,
            xorb.chunks.len(),
        );
        for chunk in &xorb.chunks {
            append_xorb_entry(&mut out, chunk);
        }
    }
    append_xorb_header(&mut out, bookend(), 0, 0);

    // Footer (placeholder offsets, matching the fork's `serialize_from`).
    append_footer(&mut out);

    out
}

/// Parses the xorb chunk tables from a serialized shard, skipping the file
/// sections.
///
/// Used to import the shard returned by a global-dedup hit.
///
/// # Examples
///
/// Build a shard with [`serialize_shard`] and parse the xorb sections back:
///
/// ```rust
/// use sdx::{MerkleHash, ShardXorb, ShardXorbChunk, serialize_shard};
/// use sdx::shard::parse_shard_xorbs;
///
/// # fn main() -> Result<(), sdx::SdxError> {
/// let xorbs = vec![ShardXorb {
///     xorb_hash: MerkleHash::from([1u8; 32]),
///     num_bytes_in_xorb: 16,
///     chunks: vec![ShardXorbChunk {
///         chunk_hash: MerkleHash::from([2u8; 32]),
///         chunk_byte_range_start: 0,
///         unpacked_segment_bytes: 16,
///         flags: 0,
///     }],
/// }];
///
/// let bytes = serialize_shard(&[], &xorbs);
/// let parsed = parse_shard_xorbs(&bytes)?;
/// assert_eq!(parsed, xorbs);
/// # Ok(())
/// # }
/// ```
///
/// # Errors
///
/// Returns [`SdxError::ShardParse`] when the body is not a valid shard or uses
/// an unsupported layout.
pub fn parse_shard_xorbs(body: &[u8]) -> Result<Vec<ShardXorb>, SdxError> {
    let mut reader = body;
    let shard_header = MDBShardFileHeader::deserialize(&mut reader).map_err(shard_parse)?;
    let version = shard_header.version;
    if !matches!(version, 2 | 3) {
        return Err(shard_parse("unsupported shard version"));
    }

    // Skip file tables without allocating their reconstruction metadata.
    loop {
        let file_header =
            FileDataSequenceHeader::deserialize(&mut reader, version).map_err(shard_parse)?;
        if file_header.is_bookend() {
            break;
        }
        let records = file_header.num_info_entry_following();
        skip_shard_bytes(&mut reader, records.saturating_mul(entry_size(version)))?;
    }

    let mut xorbs = Vec::new();
    loop {
        let xorb_header =
            XorbChunkSequenceHeader::deserialize(&mut reader, version).map_err(shard_parse)?;
        if xorb_header.is_bookend() {
            break;
        }
        let num_chunks = usize::try_from(xorb_header.num_entries).map_err(shard_parse)?;
        if num_chunks > MAX_XORB_CHUNK_ENTRIES {
            return Err(shard_parse("xorb chunk count exceeds maximum"));
        }
        let mut chunks = Vec::with_capacity(num_chunks);
        for _ in 0..num_chunks {
            let entry =
                XorbChunkSequenceEntry::deserialize(&mut reader, version).map_err(shard_parse)?;
            chunks.push(ShardXorbChunk {
                chunk_hash: <[u8; 32]>::from(entry.chunk_hash).into(),
                chunk_byte_range_start: entry.chunk_byte_range_start,
                unpacked_segment_bytes: entry.unpacked_segment_bytes,
                flags: entry.flags,
            });
        }
        xorbs.push(ShardXorb {
            xorb_hash: <[u8; 32]>::from(xorb_header.xorb_hash).into(),
            num_bytes_in_xorb: xorb_header.num_bytes_in_xorb,
            chunks,
        });
    }

    Ok(xorbs)
}

/// Looks up the xorb + chunk index containing `chunk_hash` in a parsed shard.
#[must_use]
pub fn find_chunk_in_xorbs<'shard>(
    xorbs: &'shard [ShardXorb],
    chunk_hash: &MerkleHash,
) -> Option<(&'shard ShardXorb, u32)> {
    xorbs.iter().find_map(|xorb| {
        xorb.chunks
            .iter()
            .position(|chunk| chunk.chunk_hash == *chunk_hash)
            .map(|index| (xorb, index as u32))
    })
}

// ── serialization helpers ──────────────────────────────────────────────────

fn append_file_header(out: &mut Vec<u8>, file_hash: MerkleHash, num_entries: usize) {
    out.extend_from_slice(&hash_bytes(file_hash));
    out.extend_from_slice(&MDB_DEFAULT_FILE_FLAG.to_le_bytes());
    out.extend_from_slice(&u64::try_from(num_entries).unwrap_or(u64::MAX).to_le_bytes());
    out.extend_from_slice(&0u64.to_le_bytes()); // _unused
    out.extend_from_slice(&0u64.to_le_bytes()); // _pad
    out.extend_from_slice(&SHARD_ENTRY_PADDING);
}

fn append_file_entry(out: &mut Vec<u8>, segment: &ShardSegment) {
    out.extend_from_slice(&hash_bytes(segment.xorb_hash));
    out.extend_from_slice(&MDB_DEFAULT_FILE_FLAG.to_le_bytes());
    out.extend_from_slice(&segment.unpacked_segment_bytes.to_le_bytes());
    out.extend_from_slice(&segment.chunk_index_start.to_le_bytes());
    out.extend_from_slice(&segment.chunk_index_end.to_le_bytes());
    out.extend_from_slice(&SHARD_ENTRY_PADDING);
}

fn append_xorb_header(
    out: &mut Vec<u8>,
    xorb_hash: MerkleHash,
    num_bytes: u64,
    num_entries: usize,
) {
    out.extend_from_slice(&hash_bytes(xorb_hash));
    out.extend_from_slice(&MDB_DEFAULT_XORB_FLAG.to_le_bytes());
    out.extend_from_slice(&u64::try_from(num_entries).unwrap_or(u64::MAX).to_le_bytes());
    out.extend_from_slice(&num_bytes.to_le_bytes());
    out.extend_from_slice(&0u64.to_le_bytes()); // num_bytes_on_disk
    out.extend_from_slice(&SHARD_ENTRY_PADDING);
}

fn append_xorb_entry(out: &mut Vec<u8>, chunk: &ShardXorbChunk) {
    out.extend_from_slice(&hash_bytes(chunk.chunk_hash));
    out.extend_from_slice(&chunk.chunk_byte_range_start.to_le_bytes());
    out.extend_from_slice(&chunk.unpacked_segment_bytes.to_le_bytes());
    out.extend_from_slice(&chunk.flags.to_le_bytes());
    out.extend_from_slice(&0u64.to_le_bytes()); // _unused
    out.extend_from_slice(&SHARD_ENTRY_PADDING);
}

/// Writes the fork's placeholder footer (`MDBShardFileFooter::default` with the
/// header size in `file_info_offset`), matching the fork's `serialize_from`.
fn append_footer(out: &mut Vec<u8>) {
    out.extend_from_slice(&SHARD_FOOTER_VERSION.to_le_bytes());
    out.extend_from_slice(&SHARD_HEADER_SIZE.to_le_bytes()); // file_info_offset
    out.extend_from_slice(&0u64.to_le_bytes()); // xorb_info_offset
    out.extend_from_slice(&0u64.to_le_bytes()); // file_lookup_offset
    out.extend_from_slice(&0u64.to_le_bytes()); // file_lookup_num_entry
    out.extend_from_slice(&0u64.to_le_bytes()); // xorb_lookup_offset
    out.extend_from_slice(&0u64.to_le_bytes()); // xorb_lookup_num_entry
    out.extend_from_slice(&0u64.to_le_bytes()); // chunk_lookup_offset
    out.extend_from_slice(&0u64.to_le_bytes()); // chunk_lookup_num_entry
    out.extend_from_slice(&[0u8; 32]); // chunk_hash_hmac_key
    out.extend_from_slice(&0u64.to_le_bytes()); // shard_creation_timestamp
    out.extend_from_slice(&u64::MAX.to_le_bytes()); // shard_key_expiry
    for _ in 0..6 {
        out.extend_from_slice(&0u64.to_le_bytes()); // _buffer
    }
    out.extend_from_slice(&0u64.to_le_bytes()); // stored_bytes_on_disk
    out.extend_from_slice(&0u64.to_le_bytes()); // materialized_bytes
    out.extend_from_slice(&0u64.to_le_bytes()); // stored_bytes
    out.extend_from_slice(&0u64.to_le_bytes()); // footer_offset
}

// ── parsing helpers ────────────────────────────────────────────────────────

/// Bytes per header/entry for a given shard version.
const fn entry_size(version: u64) -> u64 {
    if version < SHARD_HEADER_VERSION {
        SHARD_V2_ENTRY_SIZE as u64
    } else {
        SHARD_ENTRY_SIZE as u64
    }
}

fn hash_bytes(hash: MerkleHash) -> [u8; 32] {
    hash.into()
}

fn shard_parse(error: impl std::fmt::Display) -> SdxError {
    SdxError::ShardParse(error.to_string())
}

fn skip_shard_bytes(reader: &mut &[u8], len: u64) -> Result<(), SdxError> {
    let len = usize::try_from(len).map_err(shard_parse)?;
    *reader = reader
        .get(len..)
        .ok_or_else(|| shard_parse("unexpected end of shard body"))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use xet_core_structures::merklehash::{MerkleHash, compute_data_hash};

    use super::{
        MAX_XORB_CHUNK_ENTRIES, MDB_DEFAULT_XORB_FLAG, SHARD_ENTRY_PADDING, SHARD_ENTRY_SIZE,
        SHARD_FOOTER_SIZE, SHARD_HEADER_TAG, ShardFileEntry, ShardSegment, ShardXorb,
        ShardXorbChunk, bookend, find_chunk_in_xorbs, hash_bytes, parse_shard_xorbs,
        serialize_shard,
    };

    fn chunk(hash: MerkleHash, start: u64, len: u64) -> ShardXorbChunk {
        ShardXorbChunk {
            chunk_hash: hash,
            chunk_byte_range_start: start,
            unpacked_segment_bytes: len,
            flags: 0,
        }
    }

    #[test]
    fn shard_round_trips_file_and_xorb_sections() {
        let c1 = compute_data_hash(b"chunk one");
        let c2 = compute_data_hash(b"chunk two");
        let c3 = compute_data_hash(b"chunk three");
        let x1 = MerkleHash::from([1u8; 32]);
        let x2 = MerkleHash::from([2u8; 32]);
        let f1 = MerkleHash::from([3u8; 32]);

        let xorbs = vec![
            ShardXorb {
                xorb_hash: x1,
                num_bytes_in_xorb: 32,
                chunks: vec![chunk(c1, 0, 16), chunk(c2, 16, 16)],
            },
            ShardXorb {
                xorb_hash: x2,
                num_bytes_in_xorb: 16,
                chunks: vec![chunk(c3, 0, 16)],
            },
        ];
        let files = vec![ShardFileEntry {
            file_hash: f1,
            segments: vec![
                ShardSegment {
                    xorb_hash: x1,
                    unpacked_segment_bytes: 32,
                    chunk_index_start: 0,
                    chunk_index_end: 2,
                },
                ShardSegment {
                    xorb_hash: x2,
                    unpacked_segment_bytes: 16,
                    chunk_index_start: 0,
                    chunk_index_end: 1,
                },
            ],
        }];

        let bytes = serialize_shard(&files, &xorbs);
        // 48-byte header + 1 file (64 + 2*64) + bookend(64) + 2 xorbs
        // (64+2*64 + 64+1*64) + bookend(64) + 200-byte footer.
        assert_eq!(bytes.len(), 48 + 192 + 64 + 320 + 64 + 200);
        // Header tag + version 3.
        assert!(bytes.starts_with(b"HFRepoMetaData"));
        assert_eq!(bytes.get(32..40), Some(&3u64.to_le_bytes()[..]));

        let parsed = parse_shard_xorbs(&bytes).unwrap();
        assert_eq!(parsed, xorbs);
    }

    #[test]
    fn shard_bookends_are_all_ones() {
        let bytes = serialize_shard(&[], &[]);
        let parsed = parse_shard_xorbs(&bytes).unwrap();
        assert!(parsed.is_empty());
        assert!(bookend() == MerkleHash::from([0xFFu8; 32]));
    }

    #[test]
    fn shard_parser_finds_chunk_owner() {
        let c1 = compute_data_hash(b"needle");
        let c2 = compute_data_hash(b"other");
        let x1 = MerkleHash::from([7u8; 32]);
        let xorbs = vec![ShardXorb {
            xorb_hash: x1,
            num_bytes_in_xorb: 32,
            chunks: vec![chunk(c1, 0, 16), chunk(c2, 16, 16)],
        }];
        let bytes = serialize_shard(&[], &xorbs);
        let parsed = parse_shard_xorbs(&bytes).unwrap();
        let (xorb, index) = find_chunk_in_xorbs(&parsed, &c1).unwrap();
        assert_eq!(xorb.xorb_hash, x1);
        assert_eq!(index, 0);
        assert!(find_chunk_in_xorbs(&parsed, &compute_data_hash(b"absent")).is_none());
    }

    #[test]
    fn shard_parser_rejects_huge_xorb_chunk_count() {
        // A xorb section declaring more than the allocation cap must error
        // instead of attempting a huge `Vec::with_capacity`. The body carries
        // only the headers; the parser must reject the count before reading
        // entries or allocating.
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&SHARD_HEADER_TAG);
        bytes.extend_from_slice(&3u64.to_le_bytes());
        bytes.extend_from_slice(&SHARD_FOOTER_SIZE.to_le_bytes());
        // File section bookend (full SHARD_ENTRY_SIZE header).
        bytes.extend_from_slice(&hash_bytes(bookend()));
        bytes.extend_from_slice(&[0u8; SHARD_ENTRY_SIZE - 32]);
        // Xorb header declaring one entry past the cap.
        bytes.extend_from_slice(&hash_bytes(MerkleHash::from([5u8; 32])));
        bytes.extend_from_slice(&MDB_DEFAULT_XORB_FLAG.to_le_bytes());
        bytes.extend_from_slice(&(MAX_XORB_CHUNK_ENTRIES as u64 + 1).to_le_bytes());
        bytes.extend_from_slice(&0u64.to_le_bytes()); // num_bytes_in_xorb
        bytes.extend_from_slice(&0u64.to_le_bytes()); // num_bytes_on_disk
        bytes.extend_from_slice(&SHARD_ENTRY_PADDING);

        assert!(parse_shard_xorbs(&bytes).is_err());
    }

    #[test]
    fn shard_parser_reads_legacy_shard_emitted_by_upstream() {
        use xet_core_structures::metadata_shard::{
            file_structs::{
                FileDataSequenceEntry, FileDataSequenceHeader, FileMetadataExt,
                FileVerificationEntry, MDBFileInfo,
            },
            shard_in_memory::MDBInMemoryShard,
            xorb_structs::{MDBXorbInfo, XorbChunkSequenceEntry, XorbChunkSequenceHeader},
        };

        let xorb_hash = MerkleHash::from([7u8; 32]);
        let chunk_hash = compute_data_hash(b"legacy chunk");
        let mut shard = MDBInMemoryShard::default();
        shard
            .add_xorb_block(MDBXorbInfo {
                metadata: XorbChunkSequenceHeader::new(xorb_hash, 1, 16),
                chunks: vec![
                    XorbChunkSequenceEntry::new(chunk_hash, 16, 0).with_global_dedup_flag(true),
                ],
            })
            .unwrap();
        shard
            .add_file_reconstruction_info(MDBFileInfo {
                metadata: FileDataSequenceHeader::new(compute_data_hash(b"file"), 1, true, true),
                segments: vec![FileDataSequenceEntry::new(xorb_hash, 16, 0, 1)],
                verification: vec![FileVerificationEntry::new(chunk_hash)],
                metadata_ext: Some(FileMetadataExt::new(compute_data_hash(b"file"))),
            })
            .unwrap();
        let bytes = shard.to_bytes().unwrap();
        assert_eq!(bytes.get(32..40), Some(&2u64.to_le_bytes()[..]));
        let mut expected = chunk(chunk_hash, 0, 16);
        expected.flags = super::MDB_CHUNK_WITH_GLOBAL_DEDUP_FLAG;
        assert_eq!(
            parse_shard_xorbs(&bytes).unwrap(),
            vec![ShardXorb {
                xorb_hash,
                num_bytes_in_xorb: 16,
                chunks: vec![expected],
            }]
        );
    }

    #[test]
    fn shard_parser_skips_optional_file_records_in_both_layouts() {
        for version in [2_u64, 3] {
            let width = super::entry_size(version) as usize;
            for (flags, records_per_segment, extension) in [
                (0_u32, 1, 0),
                (1 << 31, 2, 0),
                (1 << 30, 1, 1),
                ((1 << 31) | (1 << 30), 2, 1),
            ] {
                for count in [0, 2] {
                    let mut bytes = Vec::from(SHARD_HEADER_TAG);
                    bytes.extend_from_slice(&version.to_le_bytes());
                    bytes.extend_from_slice(&SHARD_FOOTER_SIZE.to_le_bytes());
                    let header_start = bytes.len();
                    bytes.extend_from_slice(&[1; 32]);
                    bytes.extend_from_slice(&flags.to_le_bytes());
                    if version == 2 {
                        bytes.extend_from_slice(&(count as u32).to_le_bytes());
                    } else {
                        bytes.extend_from_slice(&(count as u64).to_le_bytes());
                    }
                    bytes.resize(header_start + width, 0);
                    bytes.resize(
                        bytes.len() + (count * records_per_segment + extension) * width,
                        0,
                    );
                    // File and xorb bookends follow the complete optional body.
                    for _ in 0..2 {
                        bytes.extend_from_slice(&[255; 32]);
                        bytes.resize(bytes.len() + width - 32, 0);
                    }
                    assert!(parse_shard_xorbs(&bytes).unwrap().is_empty());
                    for prefix in 0..bytes.len() {
                        assert!(parse_shard_xorbs(&bytes[..prefix]).is_err());
                    }
                    let count_end = if version == 2 { 40 } else { 44 };
                    bytes[header_start + 36..header_start + count_end].fill(255);
                    assert!(parse_shard_xorbs(&bytes).is_err());
                }
            }
        }
    }

    #[test]
    fn shard_parser_rejects_garbage() {
        assert!(parse_shard_xorbs(b"not a shard").is_err());
        assert!(parse_shard_xorbs(&[]).is_err());
        for version in [0u64, 1, 4, u64::MAX] {
            let mut bytes = serialize_shard(&[], &[]);
            bytes[32..40].copy_from_slice(&version.to_le_bytes());
            assert!(parse_shard_xorbs(&bytes).is_err(), "version {version}");
        }
    }

    #[test]
    fn shard_parser_rejects_truncated_sections() {
        let c1 = compute_data_hash(b"chunk");
        let x1 = MerkleHash::from([9u8; 32]);
        let xorbs = vec![ShardXorb {
            xorb_hash: x1,
            num_bytes_in_xorb: 8,
            chunks: vec![chunk(c1, 0, 8)],
        }];
        let bytes = serialize_shard(&[], &xorbs);
        // Truncate inside the xorb section (the parser ignores the footer, so
        // cut well before it).
        let truncated = bytes
            .get(..bytes.len().saturating_sub(203))
            .unwrap_or_default();
        assert!(parse_shard_xorbs(truncated).is_err());
    }
}
