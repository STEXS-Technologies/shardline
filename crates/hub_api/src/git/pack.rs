//! Git pack file generation.
//!
//! Implements the Git pack format for serving pack files during
//! `git clone` and `git fetch` operations. This is a minimal
//! implementation that generates non-delta packs.

use flate2::write::ZlibEncoder;
use flate2::Compression;
use sha1::{Digest, Sha1};
use std::io::Write;

/// Pack file generation error.
#[derive(Debug)]
pub enum PackError {
    /// Zlib compression failed.
    Zlib(std::io::Error),
}

impl std::fmt::Display for PackError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Zlib(e) => write!(f, "zlib compression failed: {e}"),
        }
    }
}

impl std::error::Error for PackError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Zlib(e) => Some(e),
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
/// Returns [`PackError`] if zlib compression fails.
pub fn generate_pack(objects: &[GitObject]) -> Result<Vec<u8>, PackError> {
    let mut out = Vec::new();

    // Pack header: "PACK" + version(4) + num_objects(4)
    out.extend_from_slice(b"PACK");
    out.extend_from_slice(&2u32.to_be_bytes()); // version 2
    out.extend_from_slice(&(objects.len() as u32).to_be_bytes());

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
    encoder
        .write_all(&obj.data)
        .map_err(PackError::Zlib)?;
    let compressed = encoder.finish().map_err(PackError::Zlib)?;
    out.extend_from_slice(&compressed);
    Ok(())
}

/// Creates a minimal commit object for a tree with given entries.
///
/// This is a helper for generating test/demo commits.
#[must_use]
#[allow(clippy::expect_used)]
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
        writeln!(&mut commit, "parent {}", hex::encode(parent)).expect("write to String never fails");
    }
    {
        use std::fmt::Write;
        writeln!(&mut commit, "author {author} {timestamp} +0000").expect("write to String never fails");
        writeln!(&mut commit, "committer {author} {timestamp} +0000").expect("write to String never fails");
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

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing
)]
mod tests {
    use super::*;

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
        let commit = create_commit_object(&tree_sha1, None, "Test User <test@example.com>", "Initial commit");
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
}
