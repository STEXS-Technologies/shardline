//! Tree walking and commit parsing utilities.

use std::collections::HashMap;

use super::super::pack::{GitObject, ObjectType};
use super::error::SmartHttpError;
use shardline_index::hub::HubFileEntry;

/// Maximum depth for recursive tree walking to prevent stack overflow from
/// maliciously crafted pushes with deeply nested tree objects.
const MAX_TREE_DEPTH: usize = 128;

/// Parses a raw Git commit object and extracts tree SHA, parent SHA, and message.
///
/// Format: `"tree <sha>\nparent <sha>\nauthor ...\ncommitter ...\n\n<message>"`
#[doc(hidden)]
pub fn parse_commit_object(
    data: &[u8],
) -> Result<(String, Option<String>, String), SmartHttpError> {
    let text =
        std::str::from_utf8(data).map_err(|e| SmartHttpError::CommitEncoding(e.to_string()))?;

    let mut tree_sha = None;
    let mut parent_sha = None;

    // Split at the first blank line (separates headers from message).
    let (headers, message) = match text.split_once("\n\n") {
        Some((h, m)) => (h, m.trim()),
        None => (text, ""),
    };

    for line in headers.lines() {
        if let Some(sha) = line.strip_prefix("tree ") {
            tree_sha = Some(sha.trim().to_owned());
        } else if let Some(sha) = line.strip_prefix("parent ") {
            parent_sha = Some(sha.trim().to_owned());
        }
    }

    let tree = tree_sha.ok_or(SmartHttpError::CommitMissingTree)?;
    Ok((tree, parent_sha, message.to_owned()))
}

/// Walks a Git tree object recursively, collecting file entries.
///
/// Each tree entry is: `"<mode> <name>\0<20-byte-sha>"`.
/// Directories (mode `040000`) are recursed into.  Files (modes `100644`
/// and `100755`) are looked up in the object map and added as
/// [`HubFileEntry`] values.  LFS pointer blobs are detected by their
/// magic prefix and stored with `is_lfs: true`.
#[doc(hidden)]
pub fn walk_git_tree(
    tree_sha: &[u8; 20],
    objects: &HashMap<[u8; 20], &GitObject>,
    prefix: &str,
) -> Result<Vec<HubFileEntry>, SmartHttpError> {
    walk_git_tree_inner(tree_sha, objects, prefix, 0)
}

fn walk_git_tree_inner(
    tree_sha: &[u8; 20],
    objects: &HashMap<[u8; 20], &GitObject>,
    prefix: &str,
    depth: usize,
) -> Result<Vec<HubFileEntry>, SmartHttpError> {
    if depth > MAX_TREE_DEPTH {
        return Err(SmartHttpError::TreeDepthExceeded);
    }

    let tree_obj = objects
        .get(tree_sha)
        .ok_or_else(|| SmartHttpError::TreeObjectNotFound(hex::encode(tree_sha)))?;

    if tree_obj.object_type != ObjectType::Tree {
        return Err(SmartHttpError::ExpectedTreeObject(tree_obj.object_type));
    }

    let mut entries = Vec::new();
    let data = &tree_obj.data;
    let mut pos = 0;

    while pos < data.len() {
        // Parse mode (octal string until space).
        // SAFETY: While loop ensures pos < data.len(), so data.get(pos..) is Some.
        // .position() scans from pos for the first space byte. If found, space_pos
        // is relative to pos, so pos + space_pos < data.len().
        let tail = data
            .get(pos..)
            .ok_or(SmartHttpError::TreePositionOutOfBounds)?;
        let space_pos = tail
            .iter()
            .position(|&b| b == b' ')
            .ok_or(SmartHttpError::TreeMissingSpaceAfterMode)?;
        // SAFETY: space_pos found within data[pos..], so it fits within bounds.
        // Using .and_then chaining avoids the addition expression entirely.
        let mode_slice = data
            .get(pos..)
            .and_then(|s| s.get(..space_pos))
            .ok_or(SmartHttpError::TreeModeRangeOutOfBounds)?;
        let mode_str = std::str::from_utf8(mode_slice)
            .map_err(|e| SmartHttpError::TreeModeEncoding(e.to_string()))?;

        // Parse name (until null byte).
        // SAFETY: pos + space_pos < data.len() (proven above), so name_start <= data.len()
        let name_start = pos
            .checked_add(space_pos)
            .and_then(|p| p.checked_add(1))
            .ok_or(SmartHttpError::TreeArithmeticOverflow)?;
        // SAFETY: name_start <= data.len() so the slice is valid (empty if equal)
        let name_tail = data
            .get(name_start..)
            .ok_or(SmartHttpError::TreeNamePositionOutOfBounds)?;
        let null_pos = name_tail
            .iter()
            .position(|&b| b == 0)
            .ok_or(SmartHttpError::TreeMissingNullAfterName)?;
        // SAFETY: null_pos found within data[name_start..], so it fits within bounds.
        let name_slice = data
            .get(name_start..)
            .and_then(|s| s.get(..null_pos))
            .ok_or(SmartHttpError::TreeNameRangeOutOfBounds)?;
        let name = std::str::from_utf8(name_slice)
            .map_err(|e| SmartHttpError::TreeNameEncoding(e.to_string()))?;

        // Parse SHA (20 bytes after null).
        // SAFETY: name_start + null_pos < data.len() (proven above), so sha_start <= data.len()
        let sha_start = name_start
            .checked_add(null_pos)
            .and_then(|p| p.checked_add(1))
            .ok_or(SmartHttpError::TreeArithmeticOverflow)?;
        // SAFETY: sha_start + 20 <= data.len() checked below with checked_add
        let sha_end = sha_start
            .checked_add(20)
            .ok_or(SmartHttpError::TreeArithmeticOverflow)?;
        if sha_end > data.len() {
            return Err(SmartHttpError::TreeTruncatedSha);
        }
        let mut entry_sha = [0u8; 20];
        // SAFETY: sha_start + 20 <= data.len() checked above
        let sha_slice = data
            .get(sha_start..)
            .and_then(|s| s.get(..20))
            .ok_or(SmartHttpError::TreeShaRangeOutOfBounds)?;
        entry_sha.copy_from_slice(sha_slice);

        // SAFETY: sha_start + 20 <= data.len() (checked above) so next pos is valid or == len
        pos = sha_start
            .checked_add(20)
            .ok_or(SmartHttpError::TreeArithmeticOverflow)?;

        let full_path = if prefix.is_empty() {
            name.to_owned()
        } else {
            format!("{prefix}/{name}")
        };

        // Validate path components to prevent traversal via git tree entries.
        // Reject ".." and "." components, null bytes, and control characters.
        if name == ".." || name == "." || name.contains('\0') {
            return Err(SmartHttpError::TreeInvalidEntryName(name.to_owned()));
        }
        if name.bytes().any(|b| b < 0x20 || b == 0x7f) {
            return Err(SmartHttpError::TreeInvalidEntryName(name.to_owned()));
        }

        if mode_str == "40000" {
            // Directory — recurse into subtree.
            let next_depth = depth
                .checked_add(1)
                .ok_or(SmartHttpError::TreeDepthOverflow)?;
            let mut sub_entries = walk_git_tree_inner(&entry_sha, objects, &full_path, next_depth)?;
            entries.append(&mut sub_entries);
        } else if mode_str == "100644" || mode_str == "100755" {
            // Regular file.
            let blob_obj = objects
                .get(&entry_sha)
                .ok_or_else(|| SmartHttpError::BlobObjectNotFound(hex::encode(entry_sha)))?;

            if blob_obj.object_type != ObjectType::Blob {
                return Err(SmartHttpError::ExpectedBlobObject(blob_obj.object_type));
            }

            // Check if this is an LFS pointer.
            if blob_obj
                .data
                .starts_with(b"version https://git-lfs.github.com/spec/v1")
            {
                let text = std::str::from_utf8(&blob_obj.data)
                    .map_err(|e| SmartHttpError::LfsPointerEncoding(e.to_string()))?;
                let oid = parse_lfs_pointer_field(text, "oid")
                    .ok_or(SmartHttpError::LfsPointerMissingOid)?;
                let size_str = parse_lfs_pointer_field(text, "size")
                    .ok_or(SmartHttpError::LfsPointerMissingSize)?;
                let size: u64 = size_str
                    .parse::<u64>()
                    .map_err(|e: std::num::ParseIntError| {
                        SmartHttpError::LfsPointerSize(e.to_string())
                    })?;

                entries.push(HubFileEntry {
                    path: full_path,
                    size,
                    sha: oid,
                    is_lfs: true,
                });
            } else {
                // Inline file — compute content hash.
                let sha = {
                    let mut h = blake3::Hasher::new();
                    h.update(&blob_obj.data);
                    hex::encode(h.finalize().as_bytes())
                };
                entries.push(HubFileEntry {
                    path: full_path,
                    size: blob_obj.data.len() as u64,
                    sha,
                    is_lfs: false,
                });
            }
        }
        // Skip other entry types (symlinks 120000, submodules 160000).
    }

    Ok(entries)
}

/// Parses a field value from an LFS pointer.
///
/// LFS pointer format:
/// ```text
/// version https://git-lfs.github.com/spec/v1
/// oid sha256:<oid>
/// size <size>
/// ```
pub(super) fn parse_lfs_pointer_field(text: &str, field: &str) -> Option<String> {
    for line in text.lines() {
        if let Some(value) = line.strip_prefix(&format!("{field} ")) {
            // For "oid", strip the "sha256:" prefix.
            if field == "oid" {
                return value.strip_prefix("sha256:").map(|s| s.to_owned());
            }
            return Some(value.trim().to_owned());
        }
    }
    None
}
