//! Fork-format (v3) metadata-shard serialization and parsing (M3b).
//!
//! The shardline server's `shardline-xet-core` fork serializes metadata shards
//! with **format version 3** and `u64` entry fields, while the pinned upstream
//! `xet-core-structures` 1.5.2 writes version 2 with `u32` fields (same
//! divergence class as the xorb footer in M3a). sdx does not depend on the
//! fork, so this module assembles and parses the fork's v3 layout directly
//! (`docs/SDX_PLAN.md` §4.4.2 / §9-M3).
//!
//! Byte layout (all little-endian scalars, `MDB_FILE_INFO_ENTRY_SIZE` /
//! `MDB_XORB_INFO_ENTRY_SIZE` = 60 bytes for v3):
//!
//! ```text
//! MDBShardFileHeader:  tag(32) | version(u64 = 3) | footer_size(u64 = 200)
//!   per file:          FileDataSequenceHeader(60) | FileDataSequenceEntry(60) × n
//!   FileDataSequenceHeader bookend (all-ones file hash)
//!   per xorb:          XorbChunkSequenceHeader(60) | XorbChunkSequenceEntry(60) × n
//!   XorbChunkSequenceHeader bookend (all-ones xorb hash)
//! MDBShardFileFooter:  version(u64 = 2) + placeholder offsets (200 bytes total)
//! ```
//!
//! The server's `upload_shard` handler re-normalizes the uploaded bytes with
//! its own `MDBInMemoryShard::to_bytes()` before storing, so what matters for
//! upload is that the header version and entry layout are parseable and that
//! the file/xorb sections are semantically correct; the footer is informational.

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

/// Parses the xorb sections of a fork-format shard, skipping the file
/// sections. Used to import the shard returned by a global-dedup hit.
///
/// # Errors
///
/// Returns [`SdxError::ShardParse`] when the body is not a valid shard or uses
/// an unsupported layout.
pub fn parse_shard_xorbs(body: &[u8]) -> Result<Vec<ShardXorb>, SdxError> {
    let mut reader = BodyReader::new(body);

    // Header.
    let tag = reader.take(SHARD_HEADER_TAG.len())?;
    if tag != SHARD_HEADER_TAG {
        return Err(shard_parse("invalid shard magic tag"));
    }
    let version = reader.take_u64()?;
    let _footer_size = reader.take_u64()?;

    // File sections until the bookend.
    loop {
        let header = reader.take(SHARD_ENTRY_SIZE)?;
        let file_hash = read_hash(header)?;
        if file_hash == bookend() {
            break;
        }
        let num_entries = read_file_num_entries(header, version)?;
        let flags = read_flags(header)?;
        let entry_size = entry_size(version);
        reader.skip(num_entries.saturating_mul(entry_size))?;
        if flags & MDB_FILE_FLAG_WITH_VERIFICATION != 0 {
            reader.skip(num_entries.saturating_mul(entry_size))?;
        }
        if flags & MDB_FILE_FLAG_WITH_METADATA_EXT != 0 {
            reader.skip(entry_size)?;
        }
    }

    // Xorb sections until the bookend.
    let mut xorbs = Vec::new();
    loop {
        let header = reader.take(SHARD_ENTRY_SIZE)?;
        let xorb_hash = read_hash(header)?;
        if xorb_hash == bookend() {
            break;
        }
        let num_entries = read_xorb_num_entries(header, version)?;
        let num_bytes = read_xorb_num_bytes(header, version)?;
        let entry_len = entry_size(version);
        let mut chunks = Vec::with_capacity(usize::try_from(num_entries).unwrap_or(usize::MAX));
        for _ in 0..num_entries {
            let entry = reader.take(usize::try_from(entry_len).unwrap_or(usize::MAX))?;
            chunks.push(read_xorb_chunk(entry, version)?);
        }
        xorbs.push(ShardXorb {
            xorb_hash,
            num_bytes_in_xorb: num_bytes,
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

const MDB_FILE_FLAG_WITH_VERIFICATION: u32 = 1 << 31;
const MDB_FILE_FLAG_WITH_METADATA_EXT: u32 = 1 << 30;

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

fn read_hash(bytes: &[u8]) -> Result<MerkleHash, SdxError> {
    let raw: [u8; 32] = bytes
        .get(..32)
        .and_then(|slice| <[u8; 32]>::try_from(slice).ok())
        .ok_or_else(|| shard_parse("truncated hash in shard section"))?;
    Ok(MerkleHash::from(raw))
}

fn read_flags(bytes: &[u8]) -> Result<u32, SdxError> {
    let raw = bytes
        .get(32..36)
        .and_then(|slice| <[u8; 4]>::try_from(slice).ok())
        .ok_or_else(|| shard_parse("truncated flags in shard section"))?;
    Ok(u32::from_le_bytes(raw))
}

fn read_file_num_entries(bytes: &[u8], version: u64) -> Result<u64, SdxError> {
    if version < SHARD_HEADER_VERSION {
        Ok(u64::from(read_u32_at(bytes, 36)?))
    } else {
        read_u64_at(bytes, 36)
    }
}

fn read_xorb_num_entries(bytes: &[u8], version: u64) -> Result<u64, SdxError> {
    read_file_num_entries(bytes, version)
}

fn read_xorb_num_bytes(bytes: &[u8], version: u64) -> Result<u64, SdxError> {
    if version < SHARD_HEADER_VERSION {
        // v2: hash(32) + flags(4) + num_entries(4) + num_bytes(4).
        Ok(u64::from(read_u32_at(bytes, 40)?))
    } else {
        // v3: hash(32) + flags(4) + num_entries(8) + num_bytes(8).
        read_u64_at(bytes, 44)
    }
}

fn read_xorb_chunk(bytes: &[u8], version: u64) -> Result<ShardXorbChunk, SdxError> {
    let chunk_hash = read_hash(bytes)?;
    if version < SHARD_HEADER_VERSION {
        Ok(ShardXorbChunk {
            chunk_hash,
            chunk_byte_range_start: u64::from(read_u32_at(bytes, 32)?),
            unpacked_segment_bytes: u64::from(read_u32_at(bytes, 36)?),
            flags: read_u32_at(bytes, 40)?,
        })
    } else {
        Ok(ShardXorbChunk {
            chunk_hash,
            chunk_byte_range_start: read_u64_at(bytes, 32)?,
            unpacked_segment_bytes: read_u64_at(bytes, 40)?,
            flags: read_u32_at(bytes, 48)?,
        })
    }
}

fn read_u32_at(bytes: &[u8], offset: usize) -> Result<u32, SdxError> {
    let raw = bytes
        .get(offset..offset.saturating_add(4))
        .and_then(|slice| <[u8; 4]>::try_from(slice).ok())
        .ok_or_else(|| shard_parse("truncated u32 in shard section"))?;
    Ok(u32::from_le_bytes(raw))
}

fn read_u64_at(bytes: &[u8], offset: usize) -> Result<u64, SdxError> {
    let raw = bytes
        .get(offset..offset.saturating_add(8))
        .and_then(|slice| <[u8; 8]>::try_from(slice).ok())
        .ok_or_else(|| shard_parse("truncated u64 in shard section"))?;
    Ok(u64::from_le_bytes(raw))
}

fn hash_bytes(hash: MerkleHash) -> [u8; 32] {
    hash.into()
}

fn shard_parse(message: &str) -> SdxError {
    SdxError::ShardParse(message.to_owned())
}

/// Bounded reader over a byte slice.
struct BodyReader<'buf> {
    bytes: &'buf [u8],
    pos: usize,
}

impl<'buf> BodyReader<'buf> {
    const fn new(bytes: &'buf [u8]) -> Self {
        Self { bytes, pos: 0 }
    }

    fn take(&mut self, len: usize) -> Result<&'buf [u8], SdxError> {
        let end = self.pos.saturating_add(len);
        let slice = self
            .bytes
            .get(self.pos..end)
            .ok_or_else(|| shard_parse("unexpected end of shard body"))?;
        self.pos = end;
        Ok(slice)
    }

    fn skip(&mut self, len: u64) -> Result<(), SdxError> {
        let len = usize::try_from(len).unwrap_or(usize::MAX);
        let end = self.pos.saturating_add(len);
        if end > self.bytes.len() {
            return Err(shard_parse("unexpected end of shard body"));
        }
        self.pos = end;
        Ok(())
    }

    fn take_u64(&mut self) -> Result<u64, SdxError> {
        let raw = self.take(8)?;
        let raw: [u8; 8] = <[u8; 8]>::try_from(raw)
            .map_err(|_error| shard_parse("truncated u64 in shard section"))?;
        Ok(u64::from_le_bytes(raw))
    }
}

#[cfg(test)]
mod tests {
    use xet_core_structures::merklehash::{MerkleHash, compute_data_hash};

    use super::{
        ShardFileEntry, ShardSegment, ShardXorb, ShardXorbChunk, bookend, find_chunk_in_xorbs,
        parse_shard_xorbs, serialize_shard,
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
        // 48-byte header + 1 file (60 + 2*60) + bookend(60) + 2 xorbs
        // (60+2*60 + 60+1*60) + bookend(60) + 200-byte footer.
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
    fn shard_parser_rejects_garbage() {
        assert!(parse_shard_xorbs(b"not a shard").is_err());
        assert!(parse_shard_xorbs(&[]).is_err());
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
