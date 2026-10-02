//! Strict, bounded pack parsing for receive-pack.

use super::super::pack::{
    GitObject, ObjectType, PackError, apply_delta_with_limit, parse_ofs_delta_offset,
};
use sha1::{Digest, Sha1};
use std::collections::{HashMap, VecDeque};

/// Aggregate ceiling for inflated entry bytes and resolved delta bytes.
pub(crate) const MAX_DECOMPRESSED_TOTAL_BYTES: usize = 512 * 1024 * 1024;
const MAX_PACK_OBJECTS: usize = 100_000;

/// Returns an error for invalid framing, checksum, sizes, or unresolved deltas.
/// # Errors
/// Returns `PackError` if the pack is malformed or exceeds its resource bounds.
pub fn parse_pack_data(data: &[u8]) -> Result<Vec<GitObject>, PackError> {
    parse_pack_data_with_bases(data, &HashMap::new())
}

#[derive(Clone, Copy)]
enum Base {
    Offset(usize),
    Sha([u8; 20]),
}

struct Entry {
    object_type: Option<ObjectType>,
    base: Option<Base>,
    data: Vec<u8>,
}

/// External bases are restricted to the authorized repository by the caller.
pub(super) fn parse_pack_data_with_bases(
    data: &[u8],
    bases: &HashMap<[u8; 20], &GitObject>,
) -> Result<Vec<GitObject>, PackError> {
    parse_pack_data_with_budget(data, bases, MAX_DECOMPRESSED_TOTAL_BYTES)
}

/// Bound retained inflation and resolved delta allocation by the caller's
/// remaining budget, including scoped archive object loads.
pub(super) fn parse_pack_data_with_budget(
    data: &[u8],
    bases: &HashMap<[u8; 20], &GitObject>,
    budget: usize,
) -> Result<Vec<GitObject>, PackError> {
    let budget = budget.min(MAX_DECOMPRESSED_TOTAL_BYTES);
    if data.len() < 32 || data.get(..4) != Some(b"PACK") {
        return Err(PackError::InvalidPack);
    }
    let body_end = data.len().checked_sub(20).ok_or(PackError::InvalidPack)?;
    if Sha1::digest(data.get(..body_end).ok_or(PackError::InvalidPack)?).as_slice()
        != data.get(body_end..).ok_or(PackError::InvalidPack)?
    {
        return Err(PackError::InvalidChecksum);
    }
    let body = data.get(..body_end).ok_or(PackError::InvalidPack)?;
    let version = u32::from_be_bytes(
        body.get(4..8)
            .ok_or(PackError::InvalidPack)?
            .try_into()
            .map_err(|_error| PackError::InvalidPack)?,
    );
    if !matches!(version, 2 | 3) {
        return Err(PackError::InvalidPack);
    }
    let count = u32::from_be_bytes(
        body.get(8..12)
            .ok_or(PackError::InvalidPack)?
            .try_into()
            .map_err(|_error| PackError::InvalidPack)?,
    ) as usize;
    // Each entry requires at least an object header and zlib stream. Bound
    // count before allocating bookkeeping, including a header-only attack.
    if count > MAX_PACK_OBJECTS {
        return Err(PackError::TooManyObjects);
    }
    let mut entries = Vec::new();
    let mut offsets = HashMap::new();
    let mut pos = 12;
    let mut total = 0usize;
    for index in 0..count {
        let start = pos;
        let mut byte = *body.get(pos).ok_or(PackError::InvalidPack)?;
        pos = pos.saturating_add(1);
        let kind = (byte >> 4) & 7;
        let mut size = usize::from(byte & 15);
        let mut shift = 4;
        while byte & 128 != 0 {
            byte = *body.get(pos).ok_or(PackError::InvalidPack)?;
            pos = pos.saturating_add(1);
            let value = usize::from(byte & 127);
            if shift >= usize::BITS || value > (usize::MAX >> shift) {
                return Err(PackError::ShiftOverflow);
            }
            size |= value << shift;
            shift = shift.saturating_add(7);
        }
        let (object_type, base) = match kind {
            1 => (Some(ObjectType::Commit), None),
            2 => (Some(ObjectType::Tree), None),
            3 => (Some(ObjectType::Blob), None),
            4 => (Some(ObjectType::Tag), None),
            6 => {
                let distance = parse_ofs_delta_offset(body, &mut pos)?;
                if distance == 0 {
                    return Err(PackError::InvalidDelta);
                }
                let offset = start.checked_sub(distance).ok_or(PackError::InvalidDelta)?;
                let base = *offsets.get(&offset).ok_or(PackError::InvalidDelta)?;
                (None, Some(Base::Offset(base)))
            }
            7 => {
                let end = pos.checked_add(20).ok_or(PackError::InvalidDelta)?;
                let sha = body
                    .get(pos..end)
                    .ok_or(PackError::InvalidDelta)?
                    .try_into()
                    .map_err(|_error| PackError::InvalidDelta)?;
                pos = end;
                (None, Some(Base::Sha(sha)))
            }
            _ => return Err(PackError::InvalidPack),
        };
        let remaining = budget
            .checked_sub(total)
            .ok_or(PackError::ExcessiveDecompressedSize)?;
        if size > remaining {
            return Err(PackError::ExcessiveDecompressedSize);
        }
        let (inflated, used) =
            decompress_zlib_bounded(body.get(pos..).ok_or(PackError::InvalidPack)?, size)?;
        if inflated.len() != size {
            return Err(PackError::InvalidPack);
        }
        total = total
            .checked_add(inflated.len())
            .ok_or(PackError::ExcessiveDecompressedSize)?;
        pos = pos.checked_add(used).ok_or(PackError::InvalidPack)?;
        offsets.insert(start, index);
        entries.push(Entry {
            object_type,
            base,
            data: inflated,
        });
    }
    if pos != body.len() {
        return Err(PackError::InvalidPack);
    }

    // Dependency queues resolve forward REF_DELTA and chains without recursion
    // or repeatedly scanning the entire pack for each newly available base.
    let mut ready = VecDeque::new();
    let mut by_offset: HashMap<usize, Vec<usize>> = HashMap::new();
    let mut by_sha: HashMap<[u8; 20], Vec<usize>> = HashMap::new();
    for (index, entry) in entries.iter().enumerate() {
        match entry.base {
            None => ready.push_back(index),
            Some(Base::Offset(base)) => by_offset.entry(base).or_default().push(index),
            Some(Base::Sha(sha)) if bases.contains_key(&sha) => ready.push_back(index),
            Some(Base::Sha(sha)) => by_sha.entry(sha).or_default().push(index),
        }
    }
    let mut resolved: Vec<Option<GitObject>> = (0..count).map(|_| None).collect();
    let mut sha_index = HashMap::new();
    while let Some(index) = ready.pop_front() {
        let entry = entries.get_mut(index).ok_or(PackError::InvalidDelta)?;
        let object = if let Some(base) = entry.base {
            let base = match base {
                Base::Offset(base_index) => resolved.get(base_index).and_then(Option::as_ref),
                Base::Sha(sha) => sha_index
                    .get(&sha)
                    .and_then(|base_index| resolved.get(*base_index))
                    .and_then(Option::as_ref)
                    .or_else(|| bases.get(&sha).copied()),
            }
            .ok_or(PackError::InvalidDelta)?;
            let resolved_data = apply_delta_with_limit(
                &base.data,
                &entry.data,
                budget
                    .checked_sub(total)
                    .ok_or(PackError::ExcessiveDecompressedSize)?,
            )?;
            total = total
                .checked_add(resolved_data.len())
                .ok_or(PackError::ExcessiveDecompressedSize)?;
            entry.data.clear();
            GitObject {
                object_type: base.object_type,
                data: resolved_data,
            }
        } else {
            GitObject {
                object_type: entry.object_type.ok_or(PackError::InvalidPack)?,
                data: std::mem::take(&mut entry.data),
            }
        };
        let sha = object.sha1();
        *resolved.get_mut(index).ok_or(PackError::InvalidDelta)? = Some(object);
        sha_index.insert(sha, index);
        if let Some(waiters) = by_offset.remove(&index) {
            ready.extend(waiters);
        }
        if let Some(waiters) = by_sha.remove(&sha) {
            ready.extend(waiters);
        }
    }
    resolved
        .into_iter()
        .map(|object| object.ok_or(PackError::InvalidDelta))
        .collect()
}

#[cfg(test)]
pub(super) fn decompress_zlib(data: &[u8]) -> Result<(Vec<u8>, usize), PackError> {
    decompress_zlib_bounded(data, MAX_DECOMPRESSED_TOTAL_BYTES)
}

fn decompress_zlib_bounded(data: &[u8], limit: usize) -> Result<(Vec<u8>, usize), PackError> {
    use flate2::{Decompress, FlushDecompress, Status};
    let mut decompressor = Decompress::new(true);
    let mut output = Vec::new();
    let mut input_pos = 0;
    loop {
        let before_in = decompressor.total_in();
        let before_out = decompressor.total_out();
        // Scratch storage is fixed: even a tiny declared limit cannot cause
        // expansion beyond the limit in the retained output allocation.
        let mut scratch = [0u8; 8192];
        let status = decompressor
            .decompress(
                data.get(input_pos..).ok_or(PackError::InvalidPack)?,
                &mut scratch,
                FlushDecompress::None,
            )
            .map_err(|_error| PackError::InvalidPack)?;
        let consumed = (decompressor.total_in().saturating_sub(before_in)) as usize;
        let produced = (decompressor.total_out().saturating_sub(before_out)) as usize;
        if produced > limit.saturating_sub(output.len()) {
            return Err(PackError::ExcessiveDecompressedSize);
        }
        output
            .try_reserve_exact(produced)
            .map_err(|_error| PackError::ExcessiveDecompressedSize)?;
        output.extend_from_slice(scratch.get(..produced).ok_or(PackError::InvalidPack)?);
        input_pos = input_pos
            .checked_add(consumed)
            .ok_or(PackError::InvalidPack)?;
        if status == Status::StreamEnd {
            return Ok((output, input_pos));
        }
        if consumed == 0 && produced == 0 {
            return Err(PackError::InvalidPack);
        }
    }
}
#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects
)]
mod strict_tests {
    use super::*;
    use crate::git::pack::{create_blob_object, generate_pack};
    use sha1::{Digest, Sha1};

    fn resign(pack: &mut [u8]) {
        let end = pack.len() - 20;
        let hash = Sha1::digest(&pack[..end]);
        pack[end..].copy_from_slice(&hash);
    }

    #[test]
    fn checksum_corruption_is_rejected() {
        let mut pack = generate_pack(&[create_blob_object(b"hello")]).unwrap();
        let end = pack.len() - 1;
        pack[end] ^= 1;
        assert!(parse_pack_data(&pack).is_err());
    }

    #[test]
    fn declared_object_size_mismatch_is_rejected() {
        let mut pack = generate_pack(&[create_blob_object(b"hello")]).unwrap();
        pack[12] = 0x34;
        resign(&mut pack);
        assert!(parse_pack_data(&pack).is_err());
    }

    #[test]
    fn missing_or_extra_trailer_is_rejected() {
        let pack = generate_pack(&[create_blob_object(b"hello")]).unwrap();
        assert!(parse_pack_data(&pack[..pack.len() - 20]).is_err());
        let mut extra = pack;
        extra.push(0);
        assert!(parse_pack_data(&extra).is_err());
    }
    fn compress(data: &[u8]) -> Vec<u8> {
        use std::io::Write;
        let mut encoder =
            flate2::write::ZlibEncoder::new(Vec::new(), flate2::Compression::default());
        encoder.write_all(data).unwrap();
        encoder.finish().unwrap()
    }

    fn finish(mut pack: Vec<u8>) -> Vec<u8> {
        pack.extend_from_slice(&Sha1::digest(&pack));
        pack
    }

    #[test]
    fn multibyte_size_matches_git_encoding() {
        for len in [16, 127, 128, 2048, 65536] {
            let bytes = vec![b'a'; len];
            let pack = generate_pack(&[create_blob_object(&bytes)]).unwrap();
            assert_eq!(parse_pack_data(&pack).unwrap()[0].data, bytes);
        }
    }

    #[test]
    fn forward_ref_delta_resolves_before_later_base() {
        let base = create_blob_object(b"abc");
        let mut pack = b"PACK".to_vec();
        pack.extend_from_slice(&2u32.to_be_bytes());
        pack.extend_from_slice(&2u32.to_be_bytes());
        let delta = [3, 4, 4, b'a', b'b', b'c', b'd'];
        pack.push(0x70 | delta.len() as u8);
        pack.extend_from_slice(&base.sha1());
        pack.extend_from_slice(&compress(&delta));
        pack.push(0x33);
        pack.extend_from_slice(&compress(&base.data));
        let parsed = parse_pack_data(&finish(pack)).unwrap();
        assert_eq!(parsed[0].data, b"abcd");
        assert_eq!(parsed[1].data, b"abc");
    }

    #[test]
    fn inflated_output_cannot_exceed_running_budget() {
        let compressed = compress(&vec![b'a'; 1024 * 1024]);
        assert!(matches!(
            decompress_zlib_bounded(&compressed, 16),
            Err(PackError::ExcessiveDecompressedSize)
        ));
        assert!(decompress_zlib_bounded(&compressed[..compressed.len() - 1], 1024 * 1024).is_err());
    }

    #[test]
    fn valid_checksum_does_not_hide_extra_object_bytes() {
        let mut pack = generate_pack(&[create_blob_object(b"a")]).unwrap();
        pack.truncate(pack.len() - 20);
        pack.push(0);
        assert!(matches!(
            parse_pack_data(&finish(pack)),
            Err(PackError::InvalidPack)
        ));
    }

    #[test]
    fn missing_base_and_oversized_count_are_rejected() {
        let mut pack = b"PACK".to_vec();
        pack.extend_from_slice(&2u32.to_be_bytes());
        pack.extend_from_slice(&(MAX_PACK_OBJECTS as u32 + 1).to_be_bytes());
        assert!(matches!(
            parse_pack_data(&finish(pack)),
            Err(PackError::TooManyObjects)
        ));
        let mut pack = b"PACK".to_vec();
        pack.extend_from_slice(&2u32.to_be_bytes());
        pack.extend_from_slice(&1u32.to_be_bytes());
        pack.push(0x73);
        pack.extend_from_slice(&[0; 20]);
        pack.extend_from_slice(&compress(&[0, 0, 0]));
        assert!(matches!(
            parse_pack_data(&finish(pack)),
            Err(PackError::InvalidDelta)
        ));
    }

    #[test]
    fn pack_version_three_is_supported() {
        let mut pack = generate_pack(&[]).unwrap();
        pack[7] = 3;
        resign(&mut pack);
        assert!(parse_pack_data(&pack).unwrap().is_empty());
    }
    #[test]
    fn caller_budget_limits_archive_inflation() {
        let pack = generate_pack(&[create_blob_object(&[b'a'; 8192])]).unwrap();
        assert!(matches!(
            parse_pack_data_with_budget(&pack, &HashMap::new(), 128),
            Err(PackError::ExcessiveDecompressedSize)
        ));
        assert_eq!(
            parse_pack_data_with_budget(&pack, &HashMap::new(), 8192).unwrap()[0]
                .data
                .len(),
            8192
        );
    }
    #[test]
    fn long_reverse_ref_delta_chain_resolves_iteratively() {
        let count = 1000u32;
        let mut pack = b"PACK".to_vec();
        pack.extend_from_slice(&2u32.to_be_bytes());
        pack.extend_from_slice(&(count + 1).to_be_bytes());
        for value in (1..=count).rev() {
            let parent = create_blob_object(&(value - 1).to_be_bytes());
            let mut delta = vec![4, 4, 4];
            delta.extend_from_slice(&value.to_be_bytes());
            pack.push(0x77);
            pack.extend_from_slice(&parent.sha1());
            pack.extend_from_slice(&compress(&delta));
        }
        pack.push(0x34);
        pack.extend_from_slice(&compress(&0u32.to_be_bytes()));
        let parsed = parse_pack_data(&finish(pack)).unwrap();
        assert_eq!(parsed.len(), count as usize + 1);
        assert_eq!(parsed[0].data, count.to_be_bytes());
        assert_eq!(parsed[count as usize].data, 0u32.to_be_bytes());
    }

    #[test]
    fn thin_delta_uses_only_supplied_base() {
        let base = create_blob_object(b"abc");
        let mut pack = b"PACK".to_vec();
        pack.extend_from_slice(&2u32.to_be_bytes());
        pack.extend_from_slice(&1u32.to_be_bytes());
        pack.push(0x77);
        pack.extend_from_slice(&base.sha1());
        pack.extend_from_slice(&compress(&[3, 4, 4, b'a', b'b', b'c', b'd']));
        let pack = finish(pack);
        assert!(parse_pack_data(&pack).is_err());
        let bases = HashMap::from([(base.sha1(), &base)]);
        assert_eq!(
            parse_pack_data_with_bases(&pack, &bases).unwrap()[0].data,
            b"abcd"
        );
    }
}
