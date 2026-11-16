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
#[derive(Debug, thiserror::Error)]
pub enum PackError {
    /// Pack header, entry size, or trailing bytes are malformed.
    #[error("invalid pack framing or object size")]
    InvalidPack,
    /// Pack SHA-1 trailer does not match its contents.
    #[error("invalid pack checksum")]
    InvalidChecksum,
    /// Zlib compression failed.
    #[error("zlib compression failed: {0}")]
    Zlib(#[source] std::io::Error),
    /// Too many objects to fit in the pack header (exceeds u32::MAX).
    #[error("too many objects for pack file")]
    TooManyObjects,
    /// Variable-length integer shift exceeds 63 bits.
    #[error("variable-length integer shift overflow")]
    ShiftOverflow,
    /// Delta data is malformed or references a missing base object.
    #[error("invalid or missing delta base")]
    InvalidDelta,
    /// Total decompressed size across all objects exceeds the allowed limit.
    #[error("total decompressed size exceeds allowed limit")]
    ExcessiveDecompressedSize,
}

/// Hard ceiling for a single delta target size, in bytes.
///
/// Git delta targets are capped by the same bound the pack parser uses for
/// the total decompressed size of all objects in a receive-pack
/// ([`crate::git::smart_http::pack_parse::MAX_DECOMPRESSED_TOTAL_BYTES`],
/// 512 MiB). We reuse that single shared constant as the ceiling for any one
/// delta target so a malicious delta can never force an unbounded allocation
/// before the `result.len() != target_size` check runs.
const MAX_DELTA_TARGET_SIZE: usize =
    crate::git::smart_http::pack_parse::MAX_DECOMPRESSED_TOTAL_BYTES;

/// Maximum number of bytes a delta varint may span.
///
/// Each byte carries 7 value bits, so 10 bytes covers up to 70 bits of
/// payload, which can never fit in a `usize`. Cap the decoder at 10 bytes
/// (and reject shifts ≥ 64) so the shift never overflows and parsing always
/// terminates.
const MAX_DELTA_VARINT_BYTES: usize = 10;

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
    }
    out.push(byte);

    while size_remaining > 0 {
        let mut next_byte = (size_remaining & 0x7f) as u8;
        size_remaining >>= 7;
        if size_remaining > 0 {
            next_byte |= 0x80;
        }
        out.push(next_byte);
    }

    // Zlib-compress the object content
    let mut encoder = ZlibEncoder::new(out, Compression::default());
    encoder.write_all(&obj.data).map_err(PackError::Zlib)?;
    encoder.finish().map_err(PackError::Zlib)?;
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
    apply_delta_with_limit(base, delta, MAX_DELTA_TARGET_SIZE)
}

pub(crate) fn apply_delta_with_limit(
    base: &[u8],
    delta: &[u8],
    limit: usize,
) -> Result<Vec<u8>, PackError> {
    let (source_size, pos) = parse_delta_varint(delta, 0)?;
    let (target_size, pos) = parse_delta_varint(delta, pos)?;

    if source_size != base.len() {
        return Err(PackError::InvalidDelta);
    }

    // Enforce a hard maximum on the target size before allocating, so a
    // malicious delta can never trigger an unbounded `with_capacity`.
    if target_size > limit {
        return Err(PackError::ExcessiveDecompressedSize);
    }

    let mut result = Vec::with_capacity(target_size);

    let mut instructions = delta.get(pos..).ok_or(PackError::InvalidDelta)?.iter();
    while let Some(&cmd) = instructions.next() {
        if cmd & 0x80 != 0 {
            // Copy instruction: copy bytes from the base object.
            // Bits 0-3 indicate which offset bytes are present (LSB first).
            // Bits 4-6 indicate which size bytes are present (LSB first).
            let mut copy_offset: usize = 0;
            let mut copy_size: usize = 0;

            for i in 0u32..4 {
                if cmd & (1 << i) != 0 {
                    let offset_byte = instructions
                        .next()
                        .copied()
                        .ok_or(PackError::InvalidDelta)?;
                    copy_offset |= (offset_byte as usize)
                        .checked_shl(i.saturating_mul(8))
                        .ok_or(PackError::InvalidDelta)?;
                }
            }

            for i in 4u32..7 {
                if cmd & (1 << i) != 0 {
                    let size_byte = instructions
                        .next()
                        .copied()
                        .ok_or(PackError::InvalidDelta)?;
                    copy_size |= (size_byte as usize)
                        .checked_shl(i.saturating_sub(4).saturating_mul(8))
                        .ok_or(PackError::InvalidDelta)?;
                }
            }

            // A size of 0 in the encoding means 0x10000 (65536).
            if copy_size == 0 {
                copy_size = 0x10000;
            }

            // A single copy instruction must never request more bytes than the
            // declared target size; reject it up front so a malicious delta
            // cannot drive unbounded growth through copy-size amplification.
            if copy_size > target_size {
                return Err(PackError::ExcessiveDecompressedSize);
            }

            let copy_end = copy_offset
                .checked_add(copy_size)
                .ok_or(PackError::InvalidDelta)?;
            let copied = base
                .get(copy_offset..copy_end)
                .ok_or(PackError::InvalidDelta)?;

            // Bound the running output size *before* extending the buffer. The
            // `with_capacity` cap alone is insufficient: repeated copies can
            // keep appending past `target_size` and allocate far beyond it
            // before the final `result.len() != target_size` check runs. Reject
            // as soon as the projected length would exceed `target_size`.
            let projected = result
                .len()
                .checked_add(copy_size)
                .ok_or(PackError::ExcessiveDecompressedSize)?;
            if projected > target_size {
                return Err(PackError::ExcessiveDecompressedSize);
            }

            result.extend_from_slice(copied);
        } else if cmd != 0 {
            // Insert instruction: copy cmd bytes from the delta stream.
            let insert_size = cmd as usize;
            let (inserted, rest) = instructions
                .as_slice()
                .split_at_checked(insert_size)
                .ok_or(PackError::InvalidDelta)?;
            // Bound the running output size before extending the buffer, as
            // with copy instructions above.
            let projected = result
                .len()
                .checked_add(insert_size)
                .ok_or(PackError::ExcessiveDecompressedSize)?;
            if projected > target_size {
                return Err(PackError::ExcessiveDecompressedSize);
            }
            result.extend_from_slice(inserted);
            instructions = rest.iter();
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
/// Encoding: little-endian, 7 bits of value per byte; MSB is the continuation flag.
fn parse_delta_varint(data: &[u8], mut pos: usize) -> Result<(usize, usize), PackError> {
    let mut result: usize = 0;
    for shift in (0..usize::BITS).step_by(7).take(MAX_DELTA_VARINT_BYTES) {
        let byte = data.get(pos).copied().ok_or(PackError::InvalidDelta)?;
        pos = pos.wrapping_add(1);
        let value = usize::from(byte & 0x7f);
        if value > (usize::MAX >> shift) {
            return Err(PackError::InvalidDelta);
        }
        result |= value << shift;
        if byte & 0x80 == 0 {
            return Ok((result, pos));
        }
    }
    Err(PackError::InvalidDelta)
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
    // The offset accumulator is bounds-checked with checked arithmetic; the
    // iteration budget uses a bounded counter (wrapping is unreachable —
    // the loop exits after at most MAX_DELTA_VARINT_BYTES iterations).
    let mut iterations = 0usize;
    while byte & 0x80 != 0 {
        iterations = iterations.wrapping_add(1);
        if iterations >= MAX_DELTA_VARINT_BYTES {
            return Err(PackError::InvalidDelta);
        }
        byte = data.get(*pos).copied().ok_or(PackError::InvalidDelta)?;
        *pos = (*pos).wrapping_add(1);
        offset = offset
            .checked_add(1)
            .and_then(|v| v.checked_mul(128))
            .ok_or(PackError::InvalidDelta)?;
        offset |= (byte & 0x7f) as usize;
    }
    Ok(offset)
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing)]
mod tests {
    use super::*;
    use std::error::Error;

    #[test]
    fn delta_sparse_copy_bytes_keep_their_bit_positions() {
        let base: Vec<u8> = (0..512)
            .map(|index| u8::try_from(index % 251).unwrap())
            .collect();
        // Offset byte 0 is omitted; byte 1 specifies offset256, not offset1.
        let sparse_offset = [0x80, 0x04, 1, 0x92, 1, 1];
        assert_eq!(apply_delta(&base, &sparse_offset).unwrap(), vec![base[256]]);
        // Size byte 0 is omitted; byte 1 specifies length256, not length1.
        let sparse_length = [0x80, 0x04, 0x80, 0x02, 0xa0, 1];
        assert_eq!(apply_delta(&base, &sparse_length).unwrap(), base[..256]);
    }

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

    #[test]
    fn ofs_delta_offsets_preserve_values_and_consume_only_the_encoding() {
        for (encoded, expected) in [
            (&[0x05][..], 5),
            (&[0x7f][..], 127),
            (&[0x00][..], 0),
            (&[0x82, 0x00][..], 384),
            (&[0x81, 0x00][..], 256),
            (&[0x81, 0x7f][..], 383),
            (&[0x81, 0x81, 0x01][..], 33025),
            (&[0x80, 0x01][..], 129),
        ] {
            for start in [0, 2] {
                let mut data = vec![0xff; start];
                data.extend_from_slice(encoded);
                data.extend_from_slice(b"trailing pack data");
                let mut pos = start;
                assert_eq!(parse_ofs_delta_offset(&data, &mut pos).unwrap(), expected);
                assert_eq!(pos, start.checked_add(encoded.len()).unwrap());
            }
        }
    }

    #[test]
    fn ofs_delta_offsets_reject_incomplete_encodings_without_skipping_bytes() {
        for (data, start, consumed) in [
            (&[][..], 0, 0),
            (&[0x80][..], 0, 1),
            (&[0x05][..], 1, 1),
            (&[0x05][..], usize::MAX, usize::MAX),
        ] {
            let mut pos = start;
            assert!(matches!(
                parse_ofs_delta_offset(data, &mut pos),
                Err(PackError::InvalidDelta)
            ));
            assert_eq!(pos, consumed);
        }
    }

    #[test]
    fn pack_errors_preserve_messages_and_source_chains() {
        for (error, expected) in [
            (
                PackError::InvalidPack,
                "invalid pack framing or object size",
            ),
            (PackError::InvalidChecksum, "invalid pack checksum"),
            (PackError::TooManyObjects, "too many objects for pack file"),
            (
                PackError::ShiftOverflow,
                "variable-length integer shift overflow",
            ),
            (PackError::InvalidDelta, "invalid or missing delta base"),
            (
                PackError::ExcessiveDecompressedSize,
                "total decompressed size exceeds allowed limit",
            ),
        ] {
            assert_eq!(error.to_string(), expected);
            assert!(error.source().is_none());
        }
        let error = PackError::Zlib(std::io::Error::other("compression failed"));
        assert_eq!(
            error.to_string(),
            "zlib compression failed: compression failed"
        );
        let source = error
            .source()
            .unwrap()
            .downcast_ref::<std::io::Error>()
            .unwrap();
        assert_eq!(source.kind(), std::io::ErrorKind::Other);
        assert_eq!(source.to_string(), "compression failed");
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

    #[test]
    fn delta_varints_preserve_values_and_stop_at_the_terminator() {
        for (encoded, start, expected) in [
            (&[0x00][..], 0, (0, 1)),
            (&[0x7f][..], 0, (127, 1)),
            (&[0x81, 0x0a][..], 0, (1281, 2)),
            (&[0xff, 0x7f][..], 1, (127, 2)),
            (&[0x01, 0xff][..], 0, (1, 1)),
        ] {
            assert_eq!(parse_delta_varint(encoded, start).unwrap(), expected);
        }
    }

    #[test]
    fn delta_varints_reject_missing_truncated_and_unterminated_encodings() {
        for (encoded, start) in [
            (&[][..], 0),
            (&[0x80][..], 0),
            (&[0x00][..], 1),
            (&[0x00][..], usize::MAX),
            (&[0x80; MAX_DELTA_VARINT_BYTES][..], 0),
        ] {
            assert!(matches!(
                parse_delta_varint(encoded, start),
                Err(PackError::InvalidDelta)
            ));
        }
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
        // Copy instruction requests bytes beyond base length (but within the
        // target size, so the base-bounds check is what rejects it).
        let base = b"short";
        let mut delta = Vec::new();
        // Source size: 5
        delta.push(5);
        // Target size: 10
        delta.push(10);
        // Copy: offset=0, size=8 (exceeds base length 5, within target 10)
        delta.push(0x90);
        delta.push(0x08); // size = 8

        let result = apply_delta(base, &delta);
        assert!(matches!(result, Err(PackError::InvalidDelta)));
    }

    #[test]
    #[allow(clippy::vec_init_then_push)]
    fn apply_delta_copy_sum_exceeding_target_errors() {
        // Regression test for unbounded-growth / OOM: a delta whose copy
        // instructions sum far beyond the declared target_size must error
        // (ExcessiveDecompressedSize) rather than grow the result Vec unbounded.
        // Base: 131072 bytes (0x80 0x80 0x08 varint).
        let base = vec![b'X'; 131072];
        let mut delta = Vec::new();
        // Source size: 131072 (varint: 0x80 0x80 0x08).
        delta.push(0x80);
        delta.push(0x80);
        delta.push(0x08);
        // Target size: 100000 (varint: 0xA0 0x8D 0x06).
        delta.push(0xA0);
        delta.push(0x8D);
        delta.push(0x06);
        // Two copy instructions, each emitting 65536 bytes from offset 0
        // (cmd 0x90 = copy flag + 1 size byte; size byte 0 => 65536).
        // Sum would be 131072 > target_size 100000.
        delta.push(0x90);
        delta.push(0x00);
        delta.push(0x90);
        delta.push(0x00);

        let result = apply_delta(&base, &delta);
        assert!(
            matches!(result, Err(PackError::ExcessiveDecompressedSize)),
            "expected ExcessiveDecompressedSize, got {result:?}"
        );
    }

    #[test]
    #[allow(clippy::vec_init_then_push)]
    fn apply_delta_single_copy_exceeding_target_errors() {
        // A single copy instruction that requests more bytes than the target
        // size must be rejected up front (copy-size amplification guard).
        let base = vec![b'Y'; 70000];
        let mut delta = Vec::new();
        // Source size: 70000 (varint: 0xF0 0xA2 0x04).
        delta.push(0xF0);
        delta.push(0xA2);
        delta.push(0x04);
        // Target size: 10.
        delta.push(10);
        // Copy: offset=0, size=65536 (size byte 0 => 65536) > target 10.
        delta.push(0x90);
        delta.push(0x00);

        let result = apply_delta(&base, &delta);
        assert!(
            matches!(result, Err(PackError::ExcessiveDecompressedSize)),
            "expected ExcessiveDecompressedSize, got {result:?}"
        );
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
    fn delta_varint_value_overflow_rejected() {
        let mut delta = vec![0x80; 9];
        delta.push(2); // 2 << 63 truncates even though the shift is in range.
        delta.push(0);
        assert!(apply_delta(&[], &delta).is_err());
    }

    #[test]
    fn ofs_delta_multiplication_overflow_rejected() {
        let mut bytes = [0xff; 10];
        bytes[9] = 0; // terminated encoding whose arithmetic exceeds usize.
        assert!(parse_ofs_delta_offset(&bytes, &mut 0).is_err());
    }

    #[test]
    fn delta_target_respects_remaining_pack_budget() {
        assert!(matches!(
            apply_delta_with_limit(b"abc", &[3, 4, 4, b'a', b'b', b'c', b'd'], 3),
            Err(PackError::ExcessiveDecompressedSize)
        ));
    }
}
