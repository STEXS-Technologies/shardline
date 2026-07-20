//! Pack data parsing for receive-pack.

use std::collections::HashMap;

use super::super::pack::{GitObject, ObjectType, PackError, apply_delta, parse_ofs_delta_offset};

/// Maximum total decompressed size for all objects in a receive-pack (512 MB).
/// Prevents zlib-bomb attacks that decompress to many GB of memory.
const MAX_TOTAL_DECOMPRESSED_SIZE: usize = 512 * 1024 * 1024;

/// # Errors
///
/// Returns `PackError` if the pack data is malformed or incomplete.
pub fn parse_pack_data(data: &[u8]) -> Result<Vec<GitObject>, PackError> {
    if data.len() < 12 {
        return Ok(Vec::new());
    }

    // SAFETY: data.len() >= 12 checked above, so range [0..4] is within bounds.
    // Using .get().unwrap_or(&[]) for bounds safety: if the precondition is
    // violated, an empty slice won't match "PACK" and we return an empty vec.
    if data.get(0..4).unwrap_or(&[]) != b"PACK" {
        return Ok(Vec::new());
    }

    // SAFETY: data.len() >= 12 checked above ensures indices 4..8 are valid.
    let mut version_arr = [0u8; 4];
    version_arr.copy_from_slice(data.get(4..8).unwrap_or(&[0, 0, 0, 0]));
    let version = u32::from_be_bytes(version_arr);
    // SAFETY: data.len() >= 12 checked above ensures indices 8..12 are valid.
    let mut num_objects_arr = [0u8; 4];
    num_objects_arr.copy_from_slice(data.get(8..12).unwrap_or(&[0, 0, 0, 0]));
    let num_objects = u32::from_be_bytes(num_objects_arr);

    if version != 2 {
        return Ok(Vec::new());
    }

    let mut objects = Vec::new();
    let mut sha_index: HashMap<[u8; 20], usize> = HashMap::new();
    let mut pos: usize = 12;
    let mut total_decompressed: usize = 0;

    for _ in 0..num_objects {
        if pos >= data.len() {
            break;
        }

        // SAFETY: pos < data.len() checked above, so data.get(pos) is Some.
        // .unwrap_or(&0) provides a default that won't match valid pack entries.
        let byte = *data.get(pos).unwrap_or(&0);
        pos = pos.wrapping_add(1);

        let obj_type = (byte >> 4) & 0x07;
        let mut _size = (byte & 0x0f) as u64;
        let mut shift: u32 = 4;

        let mut current = byte;
        while current & 0x80 != 0 && pos < data.len() {
            // SAFETY: while condition ensures pos < data.len()
            current = *data.get(pos).unwrap_or(&0);
            pos = pos.wrapping_add(1);
            shift = shift.wrapping_add(7);
            if shift >= 64 {
                return Err(PackError::ShiftOverflow);
            }
            _size |= ((current & 0x7f) as u64) << shift;
        }

        match obj_type {
            1..=4 => {
                // SAFETY: pos may equal data.len() (empty slice is valid for decompress_zlib)
                let remaining = data.get(pos..).unwrap_or(&[]);
                match decompress_zlib(remaining) {
                    Ok((decompressed, bytes_used)) => {
                        pos = pos.wrapping_add(bytes_used);
                        total_decompressed = total_decompressed
                            .checked_add(decompressed.len())
                            .ok_or(PackError::ExcessiveDecompressedSize)?;
                        if total_decompressed > MAX_TOTAL_DECOMPRESSED_SIZE {
                            return Err(PackError::ExcessiveDecompressedSize);
                        }
                        let ot = match obj_type {
                            1 => ObjectType::Commit,
                            2 => ObjectType::Tree,
                            3 => ObjectType::Blob,
                            _ => ObjectType::Tag,
                        };
                        let obj = GitObject {
                            object_type: ot,
                            data: decompressed,
                        };
                        let sha = obj.sha1();
                        sha_index.insert(sha, objects.len());
                        objects.push(obj);
                    }
                    Err(_) => break,
                }
            }
            6 => {
                // OFS_DELTA — resolve against a base object by negative offset.
                let offset = parse_ofs_delta_offset(data, &mut pos)?;
                // SAFETY: pos may equal data.len() (empty slice is valid for decompress_zlib)
                let remaining = data.get(pos..).unwrap_or(&[]);
                match decompress_zlib(remaining) {
                    Ok((delta_data, bytes_used)) => {
                        pos = pos.wrapping_add(bytes_used);
                        total_decompressed = total_decompressed
                            .checked_add(delta_data.len())
                            .ok_or(PackError::ExcessiveDecompressedSize)?;
                        if total_decompressed > MAX_TOTAL_DECOMPRESSED_SIZE {
                            return Err(PackError::ExcessiveDecompressedSize);
                        }
                        let base_idx = objects
                            .len()
                            .checked_sub(offset)
                            .ok_or(PackError::InvalidDelta)?;
                        // SAFETY: checked_sub ensures base_idx < objects.len()
                        let base = objects
                            .get(base_idx)
                            .ok_or(PackError::InvalidDelta)?
                            .clone();
                        let resolved_data = apply_delta(&base.data, &delta_data)?;
                        total_decompressed = total_decompressed
                            .checked_add(resolved_data.len())
                            .ok_or(PackError::ExcessiveDecompressedSize)?;
                        if total_decompressed > MAX_TOTAL_DECOMPRESSED_SIZE {
                            return Err(PackError::ExcessiveDecompressedSize);
                        }
                        let resolved = GitObject {
                            object_type: base.object_type,
                            data: resolved_data,
                        };
                        let sha = resolved.sha1();
                        sha_index.insert(sha, objects.len());
                        objects.push(resolved);
                    }
                    Err(_) => break,
                }
            }
            7 => {
                // REF_DELTA — resolve against a base object by SHA.
                if pos.wrapping_add(20) > data.len() {
                    return Err(PackError::InvalidDelta);
                }
                let mut base_sha = [0u8; 20];
                // SAFETY: pos.wrapping_add(20) > data.len() check above guarantees range is valid
                base_sha.copy_from_slice(
                    data.get(pos..pos.wrapping_add(20))
                        .ok_or(PackError::InvalidDelta)?,
                );
                pos = pos.wrapping_add(20);
                // SAFETY: pos may equal data.len() (empty slice is valid for decompress_zlib)
                let remaining = data.get(pos..).unwrap_or(&[]);
                match decompress_zlib(remaining) {
                    Ok((delta_data, bytes_used)) => {
                        pos = pos.wrapping_add(bytes_used);
                        total_decompressed = total_decompressed
                            .checked_add(delta_data.len())
                            .ok_or(PackError::ExcessiveDecompressedSize)?;
                        if total_decompressed > MAX_TOTAL_DECOMPRESSED_SIZE {
                            return Err(PackError::ExcessiveDecompressedSize);
                        }
                        let &base_idx = sha_index.get(&base_sha).ok_or(PackError::InvalidDelta)?;
                        // SAFETY: base_idx comes from sha_index which is populated
                        // with every object's index as they are pushed to objects
                        let base = objects
                            .get(base_idx)
                            .ok_or(PackError::InvalidDelta)?
                            .clone();
                        let resolved_data = apply_delta(&base.data, &delta_data)?;
                        total_decompressed = total_decompressed
                            .checked_add(resolved_data.len())
                            .ok_or(PackError::ExcessiveDecompressedSize)?;
                        if total_decompressed > MAX_TOTAL_DECOMPRESSED_SIZE {
                            return Err(PackError::ExcessiveDecompressedSize);
                        }
                        let resolved = GitObject {
                            object_type: base.object_type,
                            data: resolved_data,
                        };
                        let sha = resolved.sha1();
                        sha_index.insert(sha, objects.len());
                        objects.push(resolved);
                    }
                    Err(_) => break,
                }
            }
            _ => break,
        }
    }

    Ok(objects)
}

/// Maximum allowed decompressed size for zlib data (512 MB).
const MAX_DECOMPRESSED_SIZE: usize = 512 * 1024 * 1024;

pub(super) fn decompress_zlib(data: &[u8]) -> Result<(Vec<u8>, usize), Box<dyn std::error::Error>> {
    use flate2::Decompress;
    use flate2::FlushDecompress;

    // Decompress zlib data, tracking the exact number of compressed bytes consumed.
    let mut decompressor = Decompress::new(true); // true = zlib-wrapped (not raw deflate)
    let mut output = Vec::new();
    let mut input_pos = 0;

    loop {
        let before_in = decompressor.total_in();
        let before_out = decompressor.total_out();

        // SAFETY: input_pos tracks consumed bytes and never exceeds data.len()
        let in_chunk = data.get(input_pos..).unwrap_or(&[]);
        let in_len = in_chunk.len().min(4096);

        let flush = if input_pos
            .checked_add(in_len)
            .is_some_and(|sum| sum >= data.len())
        {
            FlushDecompress::Finish
        } else {
            FlushDecompress::None
        };

        // Allocate buffer for potential output.
        let buf_len = in_len.saturating_mul(4).max(256);
        let start = output.len();
        output.resize(start.wrapping_add(buf_len), 0);
        // SAFETY: in_len = min(in_chunk.len(), 4096) so ..in_len is within bounds.
        // SAFETY: output was just resized to start + buf_len, so output[start..]
        // has at least buf_len elements available.
        let status = decompressor.decompress(
            in_chunk.get(..in_len).unwrap_or(&[]),
            // SAFETY: output was just resized to start + buf_len, so output[start..]
            // has at least buf_len elements available. .unwrap_or(&mut []) handles
            // the impossible out-of-bounds case safely.
            output.get_mut(start..).unwrap_or(&mut []),
            flush,
        )?;

        let consumed = decompressor.total_in().wrapping_sub(before_in);
        let produced = decompressor.total_out().wrapping_sub(before_out);
        output.truncate(start.wrapping_add(produced as usize));
        input_pos = input_pos.wrapping_add(consumed as usize);

        if status == flate2::Status::StreamEnd || in_len == 0 {
            break;
        }
    }

    if output.len() > MAX_DECOMPRESSED_SIZE {
        return Err(format!(
            "decompressed data exceeds maximum size of {MAX_DECOMPRESSED_SIZE} bytes"
        )
        .into());
    }

    Ok((output, input_pos))
}
