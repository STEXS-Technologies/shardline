//! Read a native member from its fully authenticated source container.

use super::*;

/// Reads a decoded member without relying on a standalone chunk copy or trusting
/// derived descriptor offsets. The complete source is verified before any member
/// is returned. Missing containers or members return `None`; corruption is an error.
///
/// # Errors
/// Returns an error for invalid hashes, source I/O or failed container validation.
pub fn read_verified_xorb_chunk(
    store: &ServerObjectStore,
    xorb_hash: &str,
    chunk_hash: &str,
) -> Result<Option<Vec<u8>>, XetAdapterError> {
    let expected = parse_xet_hash_hex(xorb_hash)?;
    let member = parse_xet_hash_hex(chunk_hash)?;
    let key = xorb_object_key(xorb_hash)?;
    let Some((mut source, _)) = store.materialize_bounded_object_if_present(&key, 0)? else {
        return Ok(None);
    };
    let validated = validate_serialized_xorb(&mut source, expected)?;
    let Some(descriptor) = validated
        .chunks()
        .iter()
        .find(|chunk| chunk.hash() == member)
    else {
        return Ok(None);
    };
    use std::io::{Seek, SeekFrom};
    source.seek(SeekFrom::Start(descriptor.packed_start()))?;
    let (bytes, packed, unpacked) = shardline_xet_core::xorb_object::deserialize_chunk(&mut source)
        .map_err(|_error| XetAdapterError::InvalidSerializedXorb)?;
    // Recheck after the validation pass: local bytes can change between reads.
    if Some(u64::try_from(packed)?)
        != descriptor
            .packed_end()
            .checked_sub(descriptor.packed_start())
        || u64::from(unpacked) != descriptor.unpacked_len()
        || u64::try_from(bytes.len())? != descriptor.unpacked_len()
        || shardline_xet_core::merklehash::compute_data_hash(&bytes)
            != shardline_xet_core::merklehash::MerkleHash::from(*member.as_bytes())
    {
        return Err(XetAdapterError::InvalidSerializedXorb);
    }
    Ok(Some(bytes))
}

#[cfg(test)]
mod tests {
    use super::*;
    use shardline_storage::ObjectStore;
    use shardline_xet_core::{
        merklehash::{compute_data_hash, xorb_hash},
        xorb_object::{
            CompressionScheme, xorb_format_test_utils::serialized_xorb_object_from_components,
        },
    };

    #[test]
    fn native_member_read_authenticates_the_entire_source_without_chunk_copies() {
        let first = b"first source member";
        let second = b"requested source member";
        let leaves = [
            (compute_data_hash(first), first.len() as u64),
            (compute_data_hash(second), second.len() as u64),
        ];
        let hash = xorb_hash(&leaves);
        let bytes = serialized_xorb_object_from_components(
            &hash,
            [first.as_slice(), second.as_slice()].concat(),
            vec![
                (leaves[0].0, first.len() as u64),
                (leaves[1].0, (first.len() + second.len()) as u64),
            ],
            CompressionScheme::None,
        )
        .unwrap()
        .serialized_data;
        let root = tempfile::tempdir().unwrap();
        let objects = ServerObjectStore::local(root.path().join("objects")).unwrap();
        let key = xorb_object_key(&hash.hex()).unwrap();
        let member = leaves[1].0.hex();
        assert!(
            read_verified_xorb_chunk(&objects, &hash.hex(), &member)
                .unwrap()
                .is_none()
        );
        ObjectStore::put_if_absent(
            &objects,
            &key,
            ObjectBody::from_slice(&bytes),
            &ObjectIntegrity::new(chunk_hash(&bytes), bytes.len() as u64),
        )
        .unwrap();
        assert_eq!(
            read_verified_xorb_chunk(&objects, &hash.hex(), &member)
                .unwrap()
                .unwrap(),
            second
        );
        // Equal payload bytes do not make generic and native identities
        // interchangeable, even when discovery supplies a valid source Xorb.
        let generic_member = shardline_index::xet_hash_hex_string(chunk_hash(second));
        assert_ne!(generic_member, member);
        assert!(
            read_verified_xorb_chunk(&objects, &hash.hex(), &generic_member)
                .unwrap()
                .is_none()
        );
        assert!(
            read_verified_xorb_chunk(&objects, &hash.hex(), &compute_data_hash(b"absent").hex())
                .unwrap()
                .is_none()
        );
        // Corrupt an unselected member: reading only the requested member must
        // not hide source corruption elsewhere in the container.
        ObjectStore::delete_if_present(&objects, &key).unwrap();
        let mut corrupt = bytes;
        corrupt[8] ^= 1;
        ObjectStore::put_if_absent(
            &objects,
            &key,
            ObjectBody::from_slice(&corrupt),
            &ObjectIntegrity::new(chunk_hash(&corrupt), corrupt.len() as u64),
        )
        .unwrap();
        assert!(read_verified_xorb_chunk(&objects, &hash.hex(), &member).is_err());
    }
}
