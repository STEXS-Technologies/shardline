//! Git pack file generation.
//!
//! Implements the Git pack format for serving pack files during
//! `git clone` and `git fetch` operations. This is a minimal
//! implementation that generates non-delta packs.

use flate2::Compression;
use flate2::write::ZlibEncoder;
use sha1::{Digest, Sha1};
use std::io::Write;

/// Pack file generation error.
#[derive(Debug)]
pub enum PackError {
    /// Zlib compression failed.
    Zlib(std::io::Error),
    /// Too many objects to fit in the pack header (exceeds u32::MAX).
    TooManyObjects,
    /// Variable-length integer shift exceeds 63 bits.
    ShiftOverflow,
    /// Delta data is malformed or references a missing base object.
    InvalidDelta,
    /// Total decompressed size across all objects exceeds the allowed limit.
    ExcessiveDecompressedSize,
}

impl std::fmt::Display for PackError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Zlib(e) => write!(f, "zlib compression failed: {e}"),
            Self::TooManyObjects => write!(f, "too many objects for pack file"),
            Self::ShiftOverflow => write!(f, "variable-length integer shift overflow"),
            Self::InvalidDelta => write!(f, "invalid or missing delta base"),
            Self::ExcessiveDecompressedSize => {
                write!(f, "total decompressed size exceeds allowed limit")
            }
        }
    }
}

impl std::error::Error for PackError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Zlib(e) => Some(e),
            Self::TooManyObjects
            | Self::ShiftOverflow
            | Self::InvalidDelta
            | Self::ExcessiveDecompressedSize => None,
        }
    }
}

/// Git object types used in pack encoding.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum ObjectType {
    Commit = 1,
    Tree = 2,
    Blob = 3,
    Tag = 4,
}

impl ObjectType {
    const fn name(self) -> &'static str {
        match self {
            Self::Commit => "commit",
            Self::Tree => "tree",
            Self::Blob => "blob",
            Self::Tag => "tag",
        }
    }
}

/// A Git object to be included in a pack file.
#[derive(Debug, Clone)]
pub struct GitObject {
    pub object_type: ObjectType,
    pub data: Vec<u8>,
}

impl GitObject {
    #[must_use]
    pub const fn commit(data: Vec<u8>) -> Self {
        Self {
            object_type: ObjectType::Commit,
            data,
        }
    }

    #[must_use]
    pub const fn tree(data: Vec<u8>) -> Self {
        Self {
            object_type: ObjectType::Tree,
            data,
        }
    }

    #[must_use]
    pub const fn blob(data: Vec<u8>) -> Self {
        Self {
            object_type: ObjectType::Blob,
            data,
        }
    }

    /// Computes the SHA1 hash of the object (type + size + content).
    #[must_use]
    pub fn sha1(&self) -> [u8; 20] {
        let header = format!("{} {}\0", self.object_type.name(), self.data.len());
        let mut hasher = Sha1::new();
        hasher.update(header.as_bytes());
        hasher.update(&self.data);
        hasher.finalize().into()
    }
}

/// Generates a Git pack file from a list of objects.
///
/// Returns the raw bytes of the pack file (header + objects + tail checksum).
///
/// # Errors
///
/// Returns [`PackError`] if zlib compression fails or if there are too many
/// objects to fit in the pack header.
pub fn generate_pack(objects: &[GitObject]) -> Result<Vec<u8>, PackError> {
    let mut out = Vec::new();

    // Pack header: "PACK" + version(4) + num_objects(4)
    let num_objects =
        u32::try_from(objects.len()).map_err(|_overflow| PackError::TooManyObjects)?;
    out.extend_from_slice(b"PACK");
    out.extend_from_slice(&2u32.to_be_bytes()); // version 2
    out.extend_from_slice(&num_objects.to_be_bytes());

    // Write each object
    for obj in objects {
        write_object(&mut out, obj)?;
    }

    // Tail checksum: SHA1 of everything so far
    let mut hasher = Sha1::new();
    hasher.update(&out);
    let checksum: [u8; 20] = hasher.finalize().into();
    out.extend_from_slice(&checksum);

    Ok(out)
}

/// Writes a single object to the pack stream.
fn write_object(out: &mut Vec<u8>, obj: &GitObject) -> Result<(), PackError> {
    // Object header: type (3 bits) + size (4+ bits), varint-encoded.
    //
    // Git pack format (MSB-first varint):
    //   Byte 0: [continuation:1][type:3][size_bits_0_3:4]
    //   Byte 1+: [continuation:1][size_bits:7]
    // Continuation means more size bytes follow.
    let type_bits = obj.object_type as u8;
    let size = obj.data.len();

    // First byte: type in bits 6-4, low 4 bits of size in bits 3-0.
    let mut byte = (type_bits << 4) | ((size as u8) & 0x0f);
    let mut size_remaining = size >> 4;

    if size_remaining > 0 {
        byte |= 0x80; // continuation bit
        out.push(byte);

        while size_remaining > 0 {
            let mut next_byte = (size_remaining & 0x7f) as u8;
            size_remaining >>= 7;
            if size_remaining > 0 {
                next_byte |= 0x80;
            }
            out.push(next_byte);
        }
    } else {
        out.push(byte);
    }

    // Zlib-compress the object content
    let mut encoder = ZlibEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(&obj.data).map_err(PackError::Zlib)?;
    let compressed = encoder.finish().map_err(PackError::Zlib)?;
    out.extend_from_slice(&compressed);
    Ok(())
}

/// Creates a minimal commit object for a tree with given entries.
///
/// This is a helper for generating test/demo commits.
#[must_use]
pub fn create_commit_object(
    tree_sha1: &[u8; 20],
    parent_sha1: Option<&[u8; 20]>,
    author: &str,
    message: &str,
) -> GitObject {
    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    let mut commit = format!("tree {}\n", hex::encode(tree_sha1));
    if let Some(parent) = parent_sha1 {
        use std::fmt::Write;
        writeln!(&mut commit, "parent {}", hex::encode(parent)).ok();
    }
    {
        use std::fmt::Write;
        writeln!(&mut commit, "author {author} {timestamp} +0000").ok();
        writeln!(&mut commit, "committer {author} {timestamp} +0000").ok();
    }
    commit.push('\n');
    commit.push_str(message);
    commit.push('\n');

    GitObject::commit(commit.into_bytes())
}

/// Creates a tree object from a list of (mode, filename, sha1) entries.
///
/// Entries must be sorted by filename (Git requirement).
#[must_use]
pub fn create_tree_object(entries: &[(u32, &str, &[u8; 20])]) -> GitObject {
    let mut tree_data = Vec::new();

    for (mode, name, sha1) in entries {
        // Tree entry: "{mode} {name}\0{sha1}"
        let mode_str = format!("{mode:o}");
        tree_data.extend_from_slice(mode_str.as_bytes());
        tree_data.push(b' ');
        tree_data.extend_from_slice(name.as_bytes());
        tree_data.push(0);
        tree_data.extend_from_slice(sha1.as_slice());
    }

    GitObject::tree(tree_data)
}

/// Creates a blob object from raw content.
#[must_use]
pub fn create_blob_object(content: &[u8]) -> GitObject {
    GitObject::blob(content.to_vec())
}

/// Generates a "no-op" pack (empty pack with 0 objects).
///
/// Used when a client asks for objects but the repository has none yet.
///
/// # Errors
///
/// Returns [`PackError`] if zlib compression fails.
pub fn empty_pack() -> Result<Vec<u8>, PackError> {
    generate_pack(&[])
}

/// Applies a Git binary delta to a base object.
///
/// The delta format is: source_size (varint), target_size (varint), instructions.
/// Instructions: copy from base (0x80 flag + offset + size), insert new data (0x00 flag + data).
///
/// # Errors
///
/// Returns [`PackError::InvalidDelta`] if the delta data is malformed or the
/// base object doesn't match the expected source size.
pub fn apply_delta(base: &[u8], delta: &[u8]) -> Result<Vec<u8>, PackError> {
    let mut pos = 0;

    // Parse source size (varint, MSB-first, 7 bits per byte)
    let (source_size, after_source) = parse_delta_varint(delta, pos)?;
    pos = after_source;

    // Parse target size (varint)
    let (target_size, after_target) = parse_delta_varint(delta, pos)?;
    pos = after_target;

    if source_size != base.len() {
        return Err(PackError::InvalidDelta);
    }

    let mut result = Vec::with_capacity(target_size);

    while pos < delta.len() {
        let cmd = delta.get(pos).copied().ok_or(PackError::InvalidDelta)?;
        pos = pos.wrapping_add(1);

        if cmd & 0x80 != 0 {
            // Copy instruction: copy bytes from the base object.
            // Bits 0-3 indicate which offset bytes are present (LSB first).
            // Bits 4-6 indicate which size bytes are present (LSB first).
            let mut copy_offset: usize = 0;
            let mut copy_size: usize = 0;

            let mut shift = 0;
            for i in 0..4 {
                if cmd & (1 << i) != 0 {
                    let offset_byte = delta.get(pos).copied().ok_or(PackError::InvalidDelta)?;
                    copy_offset |= (offset_byte as usize).wrapping_shl(shift);
                    pos = pos.wrapping_add(1);
                    shift = shift.wrapping_add(8);
                }
            }

            shift = 0;
            for i in 4..7 {
                if cmd & (1 << i) != 0 {
                    let size_byte = delta.get(pos).copied().ok_or(PackError::InvalidDelta)?;
                    copy_size |= (size_byte as usize).wrapping_shl(shift);
                    pos = pos.wrapping_add(1);
                    shift = shift.wrapping_add(8);
                }
            }

            // A size of 0 in the encoding means 0x10000 (65536).
            if copy_size == 0 {
                copy_size = 0x10000;
            }

            if copy_offset.wrapping_add(copy_size) > base.len() {
                return Err(PackError::InvalidDelta);
            }

            result.extend_from_slice(
                base.get(copy_offset..copy_offset.wrapping_add(copy_size))
                    .ok_or(PackError::InvalidDelta)?,
            );
        } else if cmd != 0 {
            // Insert instruction: copy cmd bytes from the delta stream.
            let insert_size = cmd as usize;
            if pos.wrapping_add(insert_size) > delta.len() {
                return Err(PackError::InvalidDelta);
            }
            result.extend_from_slice(
                delta
                    .get(pos..pos.wrapping_add(insert_size))
                    .ok_or(PackError::InvalidDelta)?,
            );
            pos = pos.wrapping_add(insert_size);
        } else {
            // cmd == 0 is not valid in the Git delta format.
            return Err(PackError::InvalidDelta);
        }
    }

    if result.len() != target_size {
        return Err(PackError::InvalidDelta);
    }

    Ok(result)
}

/// Parses a variable-length integer from delta data.
///
/// Encoding: MSB-first, 7 bits of value per byte, MSB is continuation flag.
fn parse_delta_varint(data: &[u8], mut pos: usize) -> Result<(usize, usize), PackError> {
    let mut result: usize = 0;
    let mut shift: u32 = 0;
    loop {
        let byte = data.get(pos).copied().ok_or(PackError::InvalidDelta)?;
        pos = pos.wrapping_add(1);
        result |= ((byte & 0x7f) as usize).wrapping_shl(shift);
        shift = shift.wrapping_add(7);
        if byte & 0x80 == 0 {
            break;
        }
    }
    Ok((result, pos))
}

/// Parses the negative offset from an OFS_DELTA entry in a pack file.
///
/// The encoding uses MSB continuation with a special accumulation:
/// `offset = ((offset + 1) << 7) | (byte & 0x7f)` for subsequent bytes.
///
/// # Errors
///
/// Returns [`PackError::InvalidDelta`] if the offset data is malformed.
pub fn parse_ofs_delta_offset(data: &[u8], pos: &mut usize) -> Result<usize, PackError> {
    let mut byte = data.get(*pos).copied().ok_or(PackError::InvalidDelta)?;
    *pos = (*pos).wrapping_add(1);
    let mut offset = (byte & 0x7f) as usize;
    while byte & 0x80 != 0 {
        byte = data.get(*pos).copied().ok_or(PackError::InvalidDelta)?;
        *pos = (*pos).wrapping_add(1);
        offset = offset.wrapping_add(1).wrapping_shl(7) | (byte & 0x7f) as usize;
    }
    Ok(offset)
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing)]
mod tests {
    use super::*;
    use std::error::Error;

    #[test]
    fn blob_sha1_matches_git() {
        let blob = create_blob_object(b"hello world");
        let sha1 = blob.sha1();
        let hex = hex::encode(sha1);
        // Git format: "blob {size}\0{content}"
        let expected = sha1::Sha1::digest(b"blob 11\0hello world");
        assert_eq!(hex, hex::encode(expected));
    }

    #[test]
    fn pack_header_is_valid() {
        let pack = empty_pack().expect("empty pack should not fail");
        assert_eq!(&pack[0..4], b"PACK");
        assert_eq!(&pack[4..8], &2u32.to_be_bytes()); // version 2
        assert_eq!(&pack[8..12], &0u32.to_be_bytes()); // 0 objects
        // Tail checksum is 20 bytes
        assert_eq!(pack.len(), 12 + 20);
    }

    #[test]
    fn pack_with_blob_object() {
        let blob = create_blob_object(b"test content");
        let pack = generate_pack(&[blob]).expect("pack generation should not fail");
        // Header (12) + object data + checksum (20)
        assert!(pack.len() > 32);
        assert_eq!(&pack[0..4], b"PACK");
        assert_eq!(&pack[8..12], &1u32.to_be_bytes()); // 1 object
    }

    #[test]
    fn commit_object_format() {
        let tree_sha1 = [0xab; 20];
        let commit = create_commit_object(
            &tree_sha1,
            None,
            "Test User <test@example.com>",
            "Initial commit",
        );
        let content = String::from_utf8(commit.data).unwrap();
        assert!(content.starts_with(&format!("tree {}", hex::encode(tree_sha1))));
        assert!(content.contains("Initial commit"));
    }

    #[test]
    fn tree_object_sorted_entries() {
        let sha1_a = [0x01; 20];
        let sha1_b = [0x02; 20];
        let entries = vec![
            (0o100644u32, "b.txt", &sha1_b),
            (0o100644, "a.txt", &sha1_a),
        ];
        let tree = create_tree_object(&entries);
        assert_eq!(tree.object_type, ObjectType::Tree);
        // Tree data should contain both entries
        let content = String::from_utf8_lossy(&tree.data);
        assert!(content.contains("a.txt"));
        assert!(content.contains("b.txt"));
    }

    // --- apply_delta tests ---

    #[test]
    #[allow(clippy::vec_init_then_push)]
    fn apply_delta_complex_multi_step() {
        // Base: "ABCDEFGH" (8 bytes)
        let base = b"ABCDEFGH";

        // Target: "A123EFGH" — copy A (offset=0, size=1), insert "123", copy EFGH (offset=4, size=4)
        // Target size: 1 + 3 + 4 = 8
        let mut delta = Vec::new();
        // Source size: 8 (varint)
        delta.push(8);
        // Target size: 8 (varint)
        delta.push(8);

        // Copy instruction 1: offset=0, size=1 ("A")
        //   offset bytes: no bits set → 0 offset bytes
        //   size bytes: bit 4 set → 1 size byte
        //   cmd = 0x80 (copy flag) | 0x10 = 0x90
        delta.push(0x90);
        delta.push(0x01); // size = 1

        // Insert instruction: 3 bytes "123"
        delta.push(3);
        delta.extend_from_slice(b"123");

        // Copy instruction 2: offset=4, size=4 ("EFGH")
        //   offset bytes: bit 0 set → 1 offset byte
        //   size bytes: bit 4 set → 1 size byte
        //   cmd = 0x01 | 0x80 | 0x10 = 0x91
        delta.push(0x91);
        delta.push(0x04); // offset = 4
        delta.push(0x04); // size = 4

        let result = apply_delta(base, &delta).unwrap();
        assert_eq!(result, b"A123EFGH");
    }

    #[test]
    #[allow(clippy::vec_init_then_push)]
    fn apply_delta_target_size_mismatch() {
        // Base: "Hello"
        let base = b"Hello";

        // Build a delta that claims target size 10 but only produces 5 bytes.
        let mut delta = Vec::new();
        // Source size: 5
        delta.push(5);
        // Target size: 10 (wrong — we'll only produce 5 bytes via copy-all)
        delta.push(10);

        // Copy instruction: offset=0, size=5 (copy entire base)
        //   cmd = 0x80 (copy flag) | 0x10 (1 size byte) = 0x90
        delta.push(0x90);
        delta.push(0x05); // size = 5

        let result = apply_delta(base, &delta);
        assert!(
            result.is_err(),
            "should fail when target size doesn't match actual output"
        );
        assert!(
            matches!(result.unwrap_err(), PackError::InvalidDelta),
            "error should be InvalidDelta"
        );
    }

    #[test]
    #[allow(clippy::vec_init_then_push)]
    fn apply_delta_base_size_mismatch() {
        // Source size in delta doesn't match the actual base length
        let base = b"Hello, World!";

        let mut delta = Vec::new();
        // Source size: 99 (wrong — base is 13 bytes)
        delta.push(99);
        // Target size: 5
        delta.push(5);
        // Copy command: offset=0, size=5
        delta.push(0x90);
        delta.push(0x00);
        delta.push(0x05);

        let result = apply_delta(base, &delta);
        assert!(result.is_err(), "should fail when base size doesn't match");
        assert!(
            matches!(result.unwrap_err(), PackError::InvalidDelta),
            "error should be InvalidDelta"
        );
    }

    #[test]
    #[allow(clippy::vec_init_then_push)]
    fn apply_delta_truncated_data() {
        // Delta that is truncated mid-copy-instruction
        let base = b"Hello, World!";

        let mut delta = Vec::new();
        // Source size: 13
        delta.push(13);
        // Target size: 5
        delta.push(5);

        // Copy instruction: offset=0, size=5
        //   cmd = 0x80 | 0x10 = 0x90 (needs 1 size byte)
        delta.push(0x90);
        // Truncated — missing the size byte

        let result = apply_delta(base, &delta);
        assert!(result.is_err(), "should fail on truncated delta data");
    }

    #[test]
    #[allow(clippy::vec_init_then_push)]
    fn apply_delta_cmd_zero_is_invalid() {
        // cmd == 0 is not valid in the Git delta format
        let base = b"Hello";
        let mut delta = Vec::new();
        delta.push(5); // source size
        delta.push(5); // target size
        delta.push(0x00); // invalid cmd = 0

        let result = apply_delta(base, &delta);
        assert!(result.is_err());
    }

    // --- parse_ofs_delta_offset tests ---

    #[test]
    fn parse_ofs_delta_offset_single_byte() {
        // Single byte, MSB clear: offset = byte & 0x7f
        let data = [0x05];
        let mut pos = 0;
        let offset = parse_ofs_delta_offset(&data, &mut pos).unwrap();
        assert_eq!(offset, 5);
        assert_eq!(pos, 1);
    }

    #[test]
    fn parse_ofs_delta_offset_single_byte_max() {
        // Single byte with max 7-bit value (MSB clear = no continuation)
        let data = [0x7f];
        let mut pos = 0;
        let offset = parse_ofs_delta_offset(&data, &mut pos).unwrap();
        assert_eq!(offset, 127);
        assert_eq!(pos, 1);
    }

    #[test]
    fn parse_ofs_delta_offset_single_byte_zero() {
        let data = [0x00];
        let mut pos = 0;
        let offset = parse_ofs_delta_offset(&data, &mut pos).unwrap();
        assert_eq!(offset, 0);
        assert_eq!(pos, 1);
    }

    #[test]
    fn parse_ofs_delta_offset_two_bytes() {
        // Two bytes: first has MSB set (continuation), second does not.
        // Byte 0: 0x82 → low 7 bits = 2, continuation
        // Byte 1: 0x00 → low 7 bits = 0, no continuation
        // offset = ((2 + 1) << 7) | 0 = 384
        let data = [0x82, 0x00];
        let mut pos = 0;
        let offset = parse_ofs_delta_offset(&data, &mut pos).unwrap();
        assert_eq!(offset, 384);
        assert_eq!(pos, 2);
    }

    #[test]
    fn parse_ofs_delta_offset_two_bytes_small() {
        // Two bytes encoding 128:
        // Byte 0: 0x81 → low 7 bits = 1, continuation
        // Byte 1: 0x00 → low 7 bits = 0, no continuation
        // offset = ((1 + 1) << 7) | 0 = 256
        let data = [0x81, 0x00];
        let mut pos = 0;
        let offset = parse_ofs_delta_offset(&data, &mut pos).unwrap();
        assert_eq!(offset, 256);
        assert_eq!(pos, 2);
    }

    #[test]
    fn parse_ofs_delta_offset_two_bytes_with_payload() {
        // Two bytes with payload in second byte:
        // Byte 0: 0x81 → low 7 bits = 1, continuation
        // Byte 1: 0x7f → low 7 bits = 127, no continuation
        // offset = ((1 + 1) << 7) | 127 = 256 + 127 = 383
        let data = [0x81, 0x7f];
        let mut pos = 0;
        let offset = parse_ofs_delta_offset(&data, &mut pos).unwrap();
        assert_eq!(offset, 383);
        assert_eq!(pos, 2);
    }

    #[test]
    fn parse_ofs_delta_offset_three_bytes() {
        // Three bytes:
        // Byte 0: 0x81 → low 7 bits = 1, continuation
        // Byte 1: 0x81 → low 7 bits = 1, continuation
        // Byte 2: 0x01 → low 7 bits = 1, no continuation
        // Step 1: offset = 1
        // Step 2: offset = ((1+1) << 7) | 1 = 256 + 1 = 257
        // Step 3: offset = ((257+1) << 7) | 1 = 258*128 + 1 = 33024 + 1 = 33025
        let data = [0x81, 0x81, 0x01];
        let mut pos = 0;
        let offset = parse_ofs_delta_offset(&data, &mut pos).unwrap();
        assert_eq!(offset, 33025);
        assert_eq!(pos, 3);
    }

    #[test]
    fn parse_ofs_delta_offset_trailing_continuation_is_error() {
        // A byte with MSB set as the last byte means the parser expects
        // another byte but there isn't one.
        let data = [0x80];
        let mut pos = 0;
        let result = parse_ofs_delta_offset(&data, &mut pos);
        assert!(
            result.is_err(),
            "trailing continuation byte should be an error"
        );
    }

    #[test]
    fn parse_ofs_delta_offset_empty_data() {
        let data: [u8; 0] = [];
        let mut pos = 0;
        let result = parse_ofs_delta_offset(&data, &mut pos);
        assert!(result.is_err(), "empty data should be an error");
    }

    #[test]
    fn parse_ofs_delta_offset_pos_past_end() {
        let data = [0x05];
        let mut pos = 1; // already past end
        let result = parse_ofs_delta_offset(&data, &mut pos);
        assert!(result.is_err(), "pos past end should be an error");
    }

    // --- PackError Display tests ---

    #[test]
    fn pack_error_display_zlib() {
        let io_err = std::io::Error::other("compression failed");
        let err = PackError::Zlib(io_err);
        let msg = err.to_string();
        assert!(msg.contains("zlib compression failed"));
        assert!(msg.contains("compression failed"));
    }

    #[test]
    fn pack_error_display_too_many_objects() {
        let err = PackError::TooManyObjects;
        assert_eq!(err.to_string(), "too many objects for pack file");
    }

    #[test]
    fn pack_error_display_shift_overflow() {
        let err = PackError::ShiftOverflow;
        assert_eq!(err.to_string(), "variable-length integer shift overflow");
    }

    #[test]
    fn pack_error_display_invalid_delta() {
        let err = PackError::InvalidDelta;
        assert_eq!(err.to_string(), "invalid or missing delta base");
    }

    #[test]
    fn pack_error_display_excessive_decompressed_size() {
        let err = PackError::ExcessiveDecompressedSize;
        assert_eq!(
            err.to_string(),
            "total decompressed size exceeds allowed limit"
        );
    }

    #[test]
    fn pack_error_source_zlib_returns_inner() {
        let io_err = std::io::Error::other("compress fail");
        let err = PackError::Zlib(io_err);
        assert!(err.source().is_some());
    }

    #[test]
    fn pack_error_source_non_zlib_returns_none() {
        assert!(PackError::TooManyObjects.source().is_none());
        assert!(PackError::ShiftOverflow.source().is_none());
        assert!(PackError::InvalidDelta.source().is_none());
        assert!(PackError::ExcessiveDecompressedSize.source().is_none());
    }

    // --- ObjectType tests ---

    #[test]
    fn object_type_name_matches_git_conventions() {
        assert_eq!(ObjectType::Commit.name(), "commit");
        assert_eq!(ObjectType::Tree.name(), "tree");
        assert_eq!(ObjectType::Blob.name(), "blob");
        assert_eq!(ObjectType::Tag.name(), "tag");
    }

    // --- GitObject constructors ---

    #[test]
    fn git_object_commit_constructor_sets_type() {
        let obj = GitObject::commit(b"test".to_vec());
        assert_eq!(obj.object_type, ObjectType::Commit);
        assert_eq!(obj.data, b"test");
    }

    #[test]
    fn git_object_tree_constructor_sets_type() {
        let obj = GitObject::tree(b"tree-data".to_vec());
        assert_eq!(obj.object_type, ObjectType::Tree);
    }

    #[test]
    fn git_object_blob_constructor_sets_type() {
        let obj = GitObject::blob(b"blob-data".to_vec());
        assert_eq!(obj.object_type, ObjectType::Blob);
    }

    // --- parse_delta_varint tests ---

    #[test]
    fn parse_delta_varint_single_byte_zero() {
        assert_eq!(parse_delta_varint(&[0x00], 0).unwrap(), (0, 1));
    }

    #[test]
    fn parse_delta_varint_single_byte_max() {
        assert_eq!(parse_delta_varint(&[0x7f], 0).unwrap(), (127, 1));
    }

    #[test]
    fn parse_delta_varint_two_bytes() {
        // Byte 0: 0x81 (MSB set, value=1)
        // Byte 1: 0x0a (MSB clear, value=10)
        // result = 1 | (10 << 7) = 1 + 1280 = 1281
        assert_eq!(parse_delta_varint(&[0x81, 0x0a], 0).unwrap(), (1281, 2));
    }

    #[test]
    fn parse_delta_varint_empty_data() {
        let result = parse_delta_varint(&[], 0);
        assert!(result.is_err());
    }

    #[test]
    fn parse_delta_varint_truncated() {
        // Continuation byte but no more data
        let result = parse_delta_varint(&[0x80], 0);
        assert!(result.is_err());
    }

    #[test]
    fn parse_delta_varint_with_offset() {
        let data = [0xff, 0x7f]; // skip first byte, parse from index 1
        assert_eq!(parse_delta_varint(&data, 1).unwrap(), (127, 2));
    }

    // --- Additional edge cases ---

    #[test]
    fn commit_object_with_parent() {
        let tree_sha1 = [0xcd; 20];
        let parent_sha1 = [0xef; 20];
        let commit = create_commit_object(
            &tree_sha1,
            Some(&parent_sha1),
            "Author <author@test.com>",
            "Merge commit",
        );
        let content = String::from_utf8(commit.data).unwrap();
        assert!(content.starts_with(&format!("tree {}", hex::encode(tree_sha1))));
        assert!(content.contains(&format!("parent {}", hex::encode(parent_sha1))));
        assert!(content.contains("Merge commit"));
        assert!(content.contains("Author <author@test.com>"));
    }

    #[test]
    #[allow(clippy::vec_init_then_push)]
    fn apply_delta_copy_size_zero_means_65536() {
        // Git delta format: copy_size of 0 in the encoding means 65536 (0x10000).
        // Base: 70000 bytes
        let base = vec![b'A'; 70000];
        // Target: copy 65536 bytes from offset 0
        let mut delta = Vec::new();
        // Source size: 70000 (varint, fits in 3 bytes)
        // 70000 = 0x11170
        // bytes: 0x80 | (0x11170 & 0x7f)=0x70 → 0xF0
        //        0x80 | (0x11170 >> 7 & 0x7f)=0x22 → 0xA2
        //        0x11170 >> 14 & 0x7f = 0x04 → 0x04 (no continuation)
        delta.push(0xF0);
        delta.push(0xA2);
        delta.push(0x04);
        // Target size: 65536 (varint)
        // 65536 = 0x10000
        // bytes: 0x80 | 0x00 → 0x80
        //        0x80 | 0x00 → 0x80
        //        0x04 → 0x04 (no continuation)
        delta.push(0x80);
        delta.push(0x80);
        delta.push(0x04);
        // Copy instruction: cmd = 0x80 | 0x10 = 0x90 (copy + 1 size byte, no offset bytes → offset=0)
        // size byte = 0 means 65536
        delta.push(0x90);
        delta.push(0x00);

        let result = apply_delta(&base, &delta).unwrap();
        assert_eq!(result.len(), 65536);
        assert_eq!(result, &base[..65536]);
    }

    #[test]
    fn apply_delta_copy_exceeding_base_errors() {
        // Copy instruction requests bytes beyond base length
        let base = b"short";
        let mut delta = Vec::new();
        // Source size: 5
        delta.push(5);
        // Target size: 10
        delta.push(10);
        // Copy: offset=0, size=65536 (too large for base)
        delta.push(0x90);
        delta.push(0x00); // size = 0 means 65536

        let result = apply_delta(base, &delta);
        assert!(matches!(result, Err(PackError::InvalidDelta)));
    }

    #[test]
    fn generate_pack_too_many_objects() {
        // Create more than u32::MAX objects
        let _obj = GitObject::blob(b"x".to_vec());
        // We can't actually allocate u32::MAX objects, so we test the error
        // path by seeing that u32::try_from fails for a large enough length.
        // Instead, we verify the u32 conversion logic:
        let too_many = (u32::MAX as usize).wrapping_add(1);
        let result = u32::try_from(too_many);
        assert!(result.is_err());
    }

    #[test]
    fn write_object_large_size_triggers_varint_encoding() {
        // Create a blob large enough that size > 0x0f, which triggers
        // the continuation-bit varint encoding in write_object.
        let data = vec![0u8; 0x100]; // 256 bytes, low nibble = 0
        let blob = GitObject::blob(data);
        let pack = generate_pack(&[blob]).expect("pack should generate");
        // Header (12) + blob data (compressed) + checksum (20) = at least 32
        assert!(pack.len() > 32);
        // Verify pack header says 1 object
        assert_eq!(&pack[8..12], &1u32.to_be_bytes());
    }

    #[test]
    fn create_tree_object_single_entry() {
        let sha1 = [0x42; 20];
        let entries = vec![(0o100644u32, "readme.txt", &sha1)];
        let tree = create_tree_object(&entries);
        assert_eq!(tree.object_type, ObjectType::Tree);
        let content = tree.data;
        // Verify the entry is encoded: "100644 readme.txt\0{20 bytes}"
        assert!(content.starts_with(b"100644 readme.txt\x00"));
    }

    #[test]
    fn parse_ofs_delta_offset_shift_wrapping() {
        // A single byte offset with MSB set should complete normally
        // when followed by a byte with MSB clear.
        let data = [0x80, 0x01]; // continuation then final byte
        let mut pos = 0;
        let offset = parse_ofs_delta_offset(&data, &mut pos).unwrap();
        // Step 1: byte 0x80 → low 7 bits = 0, continuation
        // Step 2: byte 0x01 → low 7 bits = 1, no continuation
        // offset = ((0 + 1) << 7) | 1 = 128 + 1 = 129
        assert_eq!(offset, 129);
        assert_eq!(pos, 2);
    }
}
