use std::io::{Cursor, Write};

use proptest::prelude::*;

use crate::error::{CoreError, Validate};
use crate::merklehash::{
    DataHash, HMACKey, MerkleHash,
    aggregated_hashes::{file_hash, file_hash_with_salt, xorb_hash},
    compute_data_hash, compute_internal_node_hash,
    data_hash::{DataHashBytesParseError, DataHashHexParseError},
};
use crate::metadata_shard::{
    constants::{
        MDB_SHARD_EXPIRATION_BUFFER, MDB_SHARD_GLOBAL_DEDUP_CHUNK_MODULUS,
        MDB_SHARD_LOCAL_CACHE_EXPIRATION, hash_is_global_dedup_eligible,
    },
    file_structs::{
        FileDataSequenceEntry, FileDataSequenceHeader, FileMetadataExt, FileVerificationEntry,
        MDB_FILE_FLAG_WITH_METADATA_EXT, MDB_FILE_FLAG_WITH_VERIFICATION, MDBFileInfo,
        MDBFileInfoView,
    },
    shard_format::{MDBShardFileFooter, MDBShardFileHeader, MDBShardInfo},
    xorb_structs::{MDBXorbInfo, XorbChunkSequenceEntry, XorbChunkSequenceHeader},
};
use crate::utils::serialization_utils::{write_u32, write_u64};
use crate::xorb_object::{
    Chunk, CompressionScheme, RawXorbData,
    compression_scheme::lz4_compress_from_slice,
    xorb_chunk_format::{
        XORB_CHUNK_HEADER_LENGTH, XorbChunkHeader, deserialize_chunk, deserialize_chunk_header,
        deserialize_chunks_to_writer, serialize_chunk, write_chunk_header,
    },
    xorb_format_test_utils::{ChunkSize, build_raw_xorb, build_xorb_object},
    xorb_object_format::{
        SerializedXorbObject, XorbObject, XorbObjectInfoV0, XorbObjectInfoV1,
        reconstruct_xorb_with_footer,
    },
};

// ============================================================================
// DataHash / MerkleHash tests
// ============================================================================

#[test]
fn data_hash_default_is_all_zeros() {
    let h = DataHash::default();
    assert_eq!(*h, [0u64; 4]);
}

#[test]
fn data_hash_from_u64_array_roundtrip() {
    let arr = [1u64, 2, 3, 4];
    let h = DataHash::from(arr);
    assert_eq!(*h, arr);
}

#[test]
fn data_hash_from_u8_array_roundtrip() {
    let arr = [1u8; 32];
    let h = DataHash::from(arr);
    let back: [u8; 32] = h.into();
    assert_eq!(arr, back);
}

#[test]
fn data_hash_from_u8_ref_roundtrip() {
    let arr = [0xABu8; 32];
    let h = DataHash::from(&arr);
    let back: [u8; 32] = h.into();
    assert_eq!(arr, back);
}

#[test]
fn data_hash_from_slice_valid() {
    let bytes = vec![42u8; 32];
    let h = DataHash::from_slice(&bytes).unwrap();
    let back: [u8; 32] = h.into();
    assert_eq!(bytes.as_slice(), &back);
}

#[test]
fn data_hash_from_slice_wrong_length() {
    let bytes = vec![0u8; 31];
    assert!(DataHash::from_slice(&bytes).is_err());

    let bytes = vec![0u8; 33];
    assert!(DataHash::from_slice(&bytes).is_err());
}

#[test]
fn data_hash_try_from_slice() {
    let bytes = [7u8; 32];
    let h = DataHash::try_from(bytes.as_slice()).unwrap();
    let back: [u8; 32] = h.into();
    assert_eq!(bytes, back);

    let short = [0u8; 16];
    assert!(DataHash::try_from(short.as_slice()).is_err());
}

#[test]
fn data_hash_hex_roundtrip() {
    let arr = [0xDEADBEEFu64, 0xCAFEBABE, 0x12345678, 0x9ABCDEF0];
    let h = DataHash::from(arr);
    let hex_str = h.hex();
    assert_eq!(hex_str.len(), 64);
    let h2 = DataHash::from_hex(&hex_str).unwrap();
    assert_eq!(h, h2);
}

#[test]
fn data_hash_from_hex_invalid_length() {
    assert!(DataHash::from_hex("abc").is_err());
    assert!(DataHash::from_hex(&"a".repeat(63)).is_err());
    assert!(DataHash::from_hex(&"a".repeat(65)).is_err());
}

#[test]
fn data_hash_from_hex_invalid_chars() {
    assert!(DataHash::from_hex(&"g".repeat(64)).is_err());
    assert!(DataHash::from_hex(&"z".repeat(64)).is_err());
}

#[test]
fn data_hash_display_and_lower_hex() {
    let h = DataHash::from([1u64, 2, 3, 4]);
    let disp = format!("{h}");
    let hex = format!("{h:x}");
    assert_eq!(disp, hex);
    assert_eq!(disp.len(), 64);
}

#[test]
fn data_hash_debug() {
    let h = DataHash::from([0u64; 4]);
    let dbg = format!("{h:?}");
    assert_eq!(
        dbg,
        "0000000000000000000000000000000000000000000000000000000000000000"
    );
}

#[test]
fn data_hash_ord() {
    let a = DataHash::from([1u64, 0, 0, 0]);
    let b = DataHash::from([2u64, 0, 0, 0]);
    assert!(a < b);
    assert!(b > a);
    assert!(a <= a);
    assert!(a >= a);
}

#[test]
fn data_hash_eq() {
    let a = DataHash::from([1, 2, 3, 4]);
    let b = DataHash::from([1, 2, 3, 4]);
    let c = DataHash::from([1, 2, 3, 5]);
    assert_eq!(a, b);
    assert_ne!(a, c);
}

#[test]
fn data_hash_rem() {
    let h = DataHash::from([0, 0, 0, 10]);
    assert_eq!(h % 3, 1);
    assert_eq!(h % 5, 0);
    assert_eq!(h % 10, 0);
}

#[test]
fn data_hash_as_ref_and_into_vec() {
    let h = DataHash::from([1, 2, 3, 4]);
    let bytes_ref: [u8; 32] = h.into();
    assert_eq!(bytes_ref.len(), 32);
    let vec: Vec<u8> = h.into();
    assert_eq!(vec.len(), 32);
}

#[test]
fn data_hash_as_ref_data_hash() {
    let h = DataHash::from([1, 2, 3, 4]);
    let r: &DataHash = h.as_ref();
    assert_eq!(*r, h);
}

#[test]
fn compute_data_hash_consistent() {
    let data = b"hello world";
    let h1 = compute_data_hash(data);
    let h2 = compute_data_hash(data);
    assert_eq!(h1, h2);
}

#[test]
fn compute_data_hash_different_inputs() {
    let h1 = compute_data_hash(b"hello");
    let h2 = compute_data_hash(b"world");
    assert_ne!(h1, h2);
}

#[test]
fn compute_data_hash_empty() {
    let h = compute_data_hash(b"");
    assert_ne!(h, MerkleHash::default());
}

#[test]
fn compute_internal_node_hash_consistent() {
    let data = b"test internal node";
    let h1 = compute_internal_node_hash(data);
    let h2 = compute_internal_node_hash(data);
    assert_eq!(h1, h2);
}

#[test]
fn compute_internal_node_hash_different_from_data_hash() {
    let data = b"same data";
    let dh = compute_data_hash(data);
    let ih = compute_internal_node_hash(data);
    assert_ne!(dh, ih);
}

#[test]
fn merkle_hash_type_is_data_hash() {
    let h: MerkleHash = DataHash::from([1u64; 4]);
    assert_eq!(*h, [1u64; 4]);
}

#[test]
fn hmac_key_type_is_data_hash() {
    let k: HMACKey = DataHash::from([42u64; 4]);
    assert_eq!(*k, [42u64; 4]);
}

// ============================================================================
// DataHash property tests
// ============================================================================

proptest! {
    #[test]
    fn data_hash_never_all_zeros_for_non_empty(data in prop::collection::vec(1u8..=255, 1..1024)) {
        let h = compute_data_hash(&data);
        prop_assert_ne!(h, MerkleHash::default());
    }

    #[test]
    fn data_hash_hex_roundtrip_prop(arr in prop::array::uniform4(0u64..u64::MAX)) {
        let h = DataHash::from(arr);
        let hex = h.hex();
        let h2 = DataHash::from_hex(&hex).unwrap();
        prop_assert_eq!(h, h2);
    }

    #[test]
    fn data_hash_u8_roundtrip(arr in prop::array::uniform32(0u8..=255)) {
        let h = DataHash::from(arr);
        let back: [u8; 32] = h.into();
        prop_assert_eq!(arr, back);
    }
}

// ============================================================================
// Aggregated hashes tests
// ============================================================================

#[test]
fn xorb_hash_empty() {
    let h = xorb_hash(&[]);
    assert_eq!(h, MerkleHash::default());
}

#[test]
fn xorb_hash_single_chunk() {
    let chunks = vec![(compute_data_hash(b"a"), 1u64)];
    let h = xorb_hash(&chunks);
    assert_ne!(h, MerkleHash::default());
}

#[test]
fn xorb_hash_deterministic() {
    let chunks = vec![
        (compute_data_hash(b"a"), 1u64),
        (compute_data_hash(b"b"), 2u64),
    ];
    let h1 = xorb_hash(&chunks);
    let h2 = xorb_hash(&chunks);
    assert_eq!(h1, h2);
}

#[test]
fn xorb_hash_order_matters() {
    let a = (compute_data_hash(b"a"), 1u64);
    let b = (compute_data_hash(b"b"), 2u64);
    let h1 = xorb_hash(&[a, b]);
    let h2 = xorb_hash(&[b, a]);
    assert_ne!(h1, h2);
}

#[test]
fn file_hash_empty() {
    let h = file_hash(&[]);
    assert_eq!(h, MerkleHash::default());
}

#[test]
fn file_hash_deterministic() {
    let chunks = vec![(compute_data_hash(b"a"), 1u64)];
    assert_eq!(file_hash(&chunks), file_hash(&chunks));
}

#[test]
fn file_hash_with_salt_deterministic() {
    let chunks = vec![(compute_data_hash(b"a"), 1u64)];
    let salt = [1u8; 32];
    assert_eq!(
        file_hash_with_salt(&chunks, &salt),
        file_hash_with_salt(&chunks, &salt)
    );
}

#[test]
fn file_hash_different_from_xorb_hash() {
    let chunks = vec![
        (compute_data_hash(b"a"), 100u64),
        (compute_data_hash(b"b"), 200u64),
    ];
    assert_ne!(file_hash(&chunks), xorb_hash(&chunks));
}

#[test]
fn file_hash_with_salt_different_from_default() {
    let chunks = vec![(compute_data_hash(b"a"), 1u64)];
    let default = file_hash(&chunks);
    let salted = file_hash_with_salt(&chunks, &[1u8; 32]);
    assert_ne!(default, salted);
}

// ============================================================================
// XorbChunkHeader tests
// ============================================================================

#[test]
fn serialize_chunk_roundtrip() {
    let data = b"test chunk data for serialization";
    let mut writer = Cursor::new(Vec::new());
    let bytes_written = serialize_chunk(data, &mut writer, CompressionScheme::LZ4).unwrap();
    assert!(bytes_written > 0);

    writer.set_position(0);
    let (decompressed, compressed_size, uncompressed_size) =
        deserialize_chunk(&mut writer).unwrap();
    assert_eq!(decompressed, data);
    assert_eq!(uncompressed_size as usize, data.len());
    assert_eq!(compressed_size, bytes_written);
}

#[test]
fn serialize_chunk_none_compression() {
    let data = b"small";
    let mut writer = Cursor::new(Vec::new());
    let bytes_written = serialize_chunk(data, &mut writer, CompressionScheme::None).unwrap();
    assert_eq!(bytes_written, XORB_CHUNK_HEADER_LENGTH + data.len());

    writer.set_position(0);
    let (decompressed, _, _) = deserialize_chunk(&mut writer).unwrap();
    assert_eq!(decompressed, data);
}

#[test]
fn deserialize_chunk_header_errors_on_empty() {
    let mut reader = Cursor::new(Vec::new());
    assert!(deserialize_chunk_header(&mut reader).is_err());
}

// ============================================================================
// XorbObjectInfoV0 tests
// ============================================================================

#[test]
fn xorb_object_info_v0_serialize_roundtrip() {
    let mut info = XorbObjectInfoV0::default();
    info.xorb_hash = compute_data_hash(b"test");
    info.num_chunks = 1;
    info.chunk_boundary_offsets = vec![100];
    info.chunk_hashes = vec![compute_data_hash(b"chunk")];

    let mut buf = Vec::new();
    #[allow(deprecated)]
    let bytes_written = info.serialize(&mut buf).unwrap();
    assert!(bytes_written > 0);

    let mut cursor = Cursor::new(&buf);
    #[allow(deprecated)]
    let (deserialized, bytes_read) = XorbObjectInfoV0::deserialize(&mut cursor).unwrap();
    assert_eq!(bytes_read, bytes_written as u32);
    assert_eq!(deserialized.xorb_hash, info.xorb_hash);
    assert_eq!(deserialized.num_chunks, 1);
    assert_eq!(deserialized.chunk_boundary_offsets, vec![100]);
    assert_eq!(deserialized.chunk_hashes, info.chunk_hashes);
}

#[test]
fn xorb_object_info_v0_deserialize_v0_roundtrip() {
    let mut info = XorbObjectInfoV0::default();
    info.xorb_hash = compute_data_hash(b"v0");
    info.num_chunks = 2;
    info.chunk_boundary_offsets = vec![50, 100];
    info.chunk_hashes = vec![compute_data_hash(b"c1"), compute_data_hash(b"c2")];

    let mut buf = Vec::new();
    #[allow(deprecated)]
    info.serialize(&mut buf).unwrap();

    // Skip ident + version bytes (8 bytes) to call deserialize_v0 directly
    let mut cursor = Cursor::new(&buf[8..]);
    #[allow(deprecated)]
    let (deserialized, _) = XorbObjectInfoV0::deserialize_v0(&mut cursor).unwrap();
    assert_eq!(deserialized.xorb_hash, info.xorb_hash);
    assert_eq!(deserialized.num_chunks, 2);
    assert_eq!(deserialized.chunk_hashes, info.chunk_hashes);
}

// ============================================================================
// XorbObjectInfoV1 tests
// ============================================================================

#[test]
fn xorb_object_info_v1_serialize_roundtrip() {
    let mut info = XorbObjectInfoV1::default();
    info.xorb_hash = compute_data_hash(b"test_v1");
    info.num_chunks = 2;
    info.chunk_hashes = vec![compute_data_hash(b"c1"), compute_data_hash(b"c2")];
    info.chunk_boundary_offsets = vec![50, 120];
    info.unpacked_chunk_offsets = vec![48, 100];
    info.fill_in_boundary_offsets();

    let mut buf = Vec::new();
    let bytes_written = info.serialize(&mut buf).unwrap();
    assert!(bytes_written > 0);

    let mut cursor = Cursor::new(buf);
    let (deserialized, bytes_read) = XorbObjectInfoV1::deserialize(&mut cursor).unwrap();
    assert_eq!(bytes_read, bytes_written as u32);
    assert_eq!(deserialized.xorb_hash, info.xorb_hash);
    assert_eq!(deserialized.num_chunks, 2);
    assert_eq!(deserialized.chunk_hashes, info.chunk_hashes);
    assert_eq!(
        deserialized.chunk_boundary_offsets,
        info.chunk_boundary_offsets
    );
}

#[test]
fn xorb_object_info_v1_from_v0() {
    let mut v0 = XorbObjectInfoV0::default();
    v0.xorb_hash = compute_data_hash(b"v0data");
    v0.num_chunks = 1;
    v0.chunk_boundary_offsets = vec![100];
    v0.chunk_hashes = vec![compute_data_hash(b"chunk1")];

    let expected_hash = v0.xorb_hash;
    let v1 = XorbObjectInfoV1::from_v0(v0);
    assert_eq!(v1.version, 2);
    assert_eq!(v1.xorb_hash, expected_hash);
    assert_eq!(v1.num_chunks, 1);
    assert_eq!(v1.chunk_hashes.len(), 1);
}

#[test]
fn xorb_object_info_v1_deserialize_v0_compat() {
    // Build a v0 serialization, prefixed with XETBLOB + version 0
    let mut v0 = XorbObjectInfoV0::default();
    v0.xorb_hash = compute_data_hash(b"compat");
    v0.num_chunks = 1;
    v0.chunk_boundary_offsets = vec![80];
    v0.chunk_hashes = vec![compute_data_hash(b"chunk")];

    let mut v0_buf = Vec::new();
    #[allow(deprecated)]
    v0.serialize(&mut v0_buf).unwrap();

    // XorbObjectInfoV1::deserialize reads the ident + version first, then delegates
    let mut cursor = Cursor::new(v0_buf);
    let (v1, _) = XorbObjectInfoV1::deserialize(&mut cursor).unwrap();
    assert_eq!(v1.version, 2);
    assert_eq!(v1.xorb_hash, v0.xorb_hash);
    assert_eq!(v1.num_chunks, 1);
}

// ============================================================================
// XorbObject tests
// ============================================================================

#[test]
fn xorb_object_default() {
    let obj = XorbObject::default();
    assert_eq!(obj.info.version, 2);
    assert!(obj.info_length > 0);
}

#[test]
fn xorb_object_from_info() {
    let info = XorbObjectInfoV1::default();
    let len = info.serialized_length() as u64;
    let obj = XorbObject::from_info(info);
    assert_eq!(obj.info_length, len);
}

#[test]
fn xorb_object_serialize_given_info_roundtrip() {
    let mut info = XorbObjectInfoV1::default();
    info.xorb_hash = compute_data_hash(b"obj");
    info.num_chunks = 1;
    info.chunk_hashes = vec![compute_data_hash(b"c")];
    info.chunk_boundary_offsets = vec![100];
    info.unpacked_chunk_offsets = vec![50];
    info.fill_in_boundary_offsets();

    let mut buf = Vec::new();
    let (obj, written) = XorbObject::serialize_given_info(&mut buf, info.clone()).unwrap();
    assert!(written > 0);
    assert_eq!(obj.info.xorb_hash, info.xorb_hash);

    // Deserialize: get_info_length reads last 4 bytes, then reads info
    let mut cursor = Cursor::new(&buf);
    let deserialized = XorbObject::deserialize(&mut cursor).unwrap();
    assert_eq!(deserialized.info.xorb_hash, info.xorb_hash);
    assert_eq!(deserialized.info.num_chunks, info.num_chunks);
}

#[test]
fn xorb_object_get_info_length() {
    let mut info = XorbObjectInfoV1::default();
    info.xorb_hash = compute_data_hash(b"len");
    info.num_chunks = 0;
    info.fill_in_boundary_offsets();

    let mut buf = Vec::new();
    XorbObject::serialize_given_info(&mut buf, info).unwrap();

    let mut cursor = Cursor::new(&buf);
    let len = XorbObject::get_info_length(&mut cursor).unwrap();
    assert!(len > 0);
}

#[test]
fn xorb_object_oversized_footer_lengths_are_invalid_input() {
    for length in [
        i64::MAX as u64 - 7,
        i64::MAX as u64,
        i64::MAX as u64 + 1,
        u64::MAX,
    ] {
        let mut reader = Cursor::new(length.to_le_bytes());
        assert!(XorbObject::deserialize(&mut reader).is_err());
    }
}

#[test]
fn xorb_object_validation_rejects_forged_parsed_footer_length() {
    let (mut object, chunk_data, _, _) =
        build_xorb_object(1, ChunkSize::Fixed(100), CompressionScheme::LZ4).unwrap();
    let mut bytes = chunk_data;
    let info_length = object.info.serialize(&mut bytes).unwrap();
    bytes.extend_from_slice(&(info_length as u64).to_le_bytes());
    let hash = object.info.xorb_hash;
    object.info_length = u64::MAX;
    let result = XorbObject::validate_xorb_object_with_info(&mut Cursor::new(bytes), object, &hash);
    assert!(result.is_err() || result.unwrap().is_none());
}

#[test]
fn xorb_object_get_contents_length() {
    let mut info = XorbObjectInfoV1::default();
    info.xorb_hash = compute_data_hash(b"contents");
    info.num_chunks = 1;
    info.chunk_boundary_offsets = vec![42];
    info.chunk_hashes = vec![compute_data_hash(b"c")];
    info.unpacked_chunk_offsets = vec![42];
    info.fill_in_boundary_offsets();

    let obj = XorbObject::from_info(info);
    assert_eq!(obj.get_contents_length().unwrap(), 42);
}

#[test]
fn xorb_object_get_contents_length_no_chunks_errors() {
    let obj = XorbObject::default();
    assert!(obj.get_contents_length().is_err());
}

#[test]
fn xorb_object_validate_xorb_object_info_errors_no_chunks() {
    let mut info = XorbObjectInfoV1::default();
    info.xorb_hash = compute_data_hash(b"test");
    info.num_chunks = 0;
    let obj = XorbObject::from_info(info);
    // get_contents_length calls validate_xorb_object_info which fails on 0 chunks
    assert!(obj.get_contents_length().is_err());
}

// ============================================================================
// SerializedXorbObject tests
// ============================================================================

#[test]
fn serialized_xorb_object_from_components_roundtrip() {
    use crate::xorb_object::xorb_object_format::test_utils::serialized_xorb_object_from_components;

    let hash = compute_data_hash(b"test_hash");
    let data = vec![0u8; 1024];
    let chunk_boundaries = vec![
        (compute_data_hash(b"c1"), 512),
        (compute_data_hash(b"c2"), 1024),
    ];

    let serialized = serialized_xorb_object_from_components(
        &hash,
        data.clone(),
        chunk_boundaries,
        CompressionScheme::None,
    )
    .unwrap();

    assert_eq!(serialized.hash, hash);
    assert_eq!(serialized.raw_num_bytes, 1024);
    assert_eq!(serialized.num_chunks, 2);
    assert!(serialized.footer_start.is_some());

    // Deserialize
    let mut cursor = Cursor::new(&serialized.serialized_data);
    let obj = XorbObject::deserialize(&mut cursor).unwrap();
    assert_eq!(obj.info.xorb_hash, hash);
    assert_eq!(obj.info.num_chunks, 2);
}

// ============================================================================
// reconstruct_xorb_with_footer tests
// ============================================================================

#[test]
fn reconstruct_xorb_with_footer_roundtrip() {
    let data = b"hello world for reconstruction!";
    // Build a chunk with None compression for simplicity
    let mut chunk_writer = Cursor::new(Vec::<u8>::new());
    serialize_chunk(data, &mut chunk_writer, CompressionScheme::None).unwrap();
    let chunk_data = chunk_writer.into_inner();

    let mut output = Cursor::new(Vec::<u8>::new());
    let (xorb_obj, hash) = reconstruct_xorb_with_footer(&mut output, &chunk_data).unwrap();

    assert_ne!(hash, MerkleHash::default());
    assert_eq!(xorb_obj.info.num_chunks, 1);
    assert_eq!(xorb_obj.info.xorb_hash, hash);
}

// ============================================================================
// MDBFileInfo tests
// ============================================================================

#[test]
fn mdb_file_info_default() {
    let info = MDBFileInfo::default();
    assert_eq!(info.metadata, FileDataSequenceHeader::default());
    assert!(info.segments.is_empty());
    assert!(info.verification.is_empty());
    assert!(info.metadata_ext.is_none());
}

// ============================================================================
// constants tests
// ============================================================================

#[test]
fn global_dedup_chunk_modulus() {
    assert_eq!(MDB_SHARD_GLOBAL_DEDUP_CHUNK_MODULUS, 1024);
}

#[test]
fn hash_is_global_dedup_eligible_zero() {
    let h = MerkleHash::default(); // all zeros, 0 % 1024 == 0
    assert!(hash_is_global_dedup_eligible(&h));
}

#[test]
fn hash_is_global_dedup_eligible_nonzero() {
    // Build a hash where [3] = 5, so 5 % 1024 != 0
    let h = DataHash::from([0, 0, 0, 5]);
    assert!(!hash_is_global_dedup_eligible(&h));
}

#[test]
fn hash_is_global_dedup_eligible_exact_modulus() {
    let h = DataHash::from([0, 0, 0, MDB_SHARD_GLOBAL_DEDUP_CHUNK_MODULUS]);
    assert!(hash_is_global_dedup_eligible(&h));
}

#[test]
fn shard_expiration_buffer() {
    assert!(MDB_SHARD_EXPIRATION_BUFFER.as_secs() > 0);
    assert_eq!(MDB_SHARD_EXPIRATION_BUFFER.as_secs(), 7 * 24 * 3600);
}

#[test]
fn shard_local_cache_expiration() {
    assert!(MDB_SHARD_LOCAL_CACHE_EXPIRATION.as_secs() > 0);
    assert_eq!(
        MDB_SHARD_LOCAL_CACHE_EXPIRATION.as_secs(),
        3 * 7 * 24 * 3600
    );
}

// ============================================================================
// XorbObject build_raw_xorb tests
// ============================================================================

#[test]
fn build_raw_xorb_fixed() {
    let raw = build_raw_xorb(3, ChunkSize::Fixed(512));
    assert_eq!(raw.data.len(), 3);
    assert_eq!(raw.num_bytes(), 3 * 512);
}

#[test]
fn build_raw_xorb_to_vec() {
    let raw = build_raw_xorb(2, ChunkSize::Fixed(256));
    let vec = crate::xorb_object::raw_xorb_data::test_utils::raw_xorb_to_vec(&raw);
    assert_eq!(vec.len(), 512);
}

// ============================================================================
// XorbObject build_xorb_object tests
// ============================================================================

#[test]
fn build_xorb_object_and_serialize() {
    let (obj, _chunk_data, _raw_data, _boundaries) =
        build_xorb_object(2, ChunkSize::Fixed(512), CompressionScheme::None).unwrap();

    assert_eq!(obj.info.num_chunks, 2);
    assert_ne!(obj.info.xorb_hash, MerkleHash::default());
}

// ============================================================================
// MDBFileInfoView tests
// ============================================================================

#[test]
fn mdb_file_info_view_basic() {
    let header = FileDataSequenceHeader::new(compute_data_hash(b"f"), 1u64, false, false);
    let entry = FileDataSequenceEntry::new(compute_data_hash(b"e"), 100u64, 0u64, 50u64);

    let mut buf = Vec::new();
    header.serialize(&mut buf).unwrap();
    entry.serialize(&mut buf).unwrap();

    use crate::metadata_shard::shard_format::MDB_FILE_INFO_ENTRY_SIZE;
    let view =
        crate::metadata_shard::file_structs::MDBFileInfoView::new(bytes::Bytes::from(buf)).unwrap();
    assert_eq!(view.num_entries(), 1);
    assert_eq!(view.file_hash(), compute_data_hash(b"f"));
    let e = view.entry(0);
    assert_eq!(e.unpacked_segment_bytes, 100);
    assert_eq!(view.byte_size(false), (1 + 1) * MDB_FILE_INFO_ENTRY_SIZE);
}

#[test]
fn mdb_file_info_view_from_data_too_small() {
    let header = FileDataSequenceHeader::new(MerkleHash::default(), 2u64, false, false);
    assert!(
        crate::metadata_shard::file_structs::MDBFileInfoView::from_data_and_header(
            header,
            bytes::Bytes::from(vec![0u8; 5])
        )
        .is_err()
    );
}

#[test]
fn mdb_file_info_view_bytes() {
    let header = FileDataSequenceHeader::new(MerkleHash::default(), 0u64, false, false);
    let mut buf = Vec::new();
    header.serialize(&mut buf).unwrap();
    let view =
        crate::metadata_shard::file_structs::MDBFileInfoView::new(bytes::Bytes::from(buf)).unwrap();
    let _ = view.bytes();
}

#[test]
#[allow(non_snake_case)]
fn mdb_file_info_view_fromMDBFileInfoView_into_mdb_file_info() {
    let header = FileDataSequenceHeader::new(compute_data_hash(b"f"), 1u64, false, false);
    let entry = FileDataSequenceEntry::new(compute_data_hash(b"e"), 100u64, 0u64, 50u64);

    let mut buf = Vec::new();
    header.serialize(&mut buf).unwrap();
    entry.serialize(&mut buf).unwrap();

    let view =
        crate::metadata_shard::file_structs::MDBFileInfoView::new(bytes::Bytes::from(buf)).unwrap();
    let info: MDBFileInfo = (&view).into();
    assert_eq!(info.segments.len(), 1);
    assert_eq!(info.segments[0].unpacked_segment_bytes, 100);
}

// ============================================================================
// XorbObjectInfoV1 deserialize various error paths
// ============================================================================

#[test]
fn xorb_object_info_v1_deserialize_wrong_hash_section_ident() {
    let mut info = XorbObjectInfoV1::default();
    info.xorb_hash = compute_data_hash(b"test");
    info.num_chunks = 0;
    info.chunk_hashes = vec![];
    info.chunk_boundary_offsets = vec![];
    info.unpacked_chunk_offsets = vec![];
    info.fill_in_boundary_offsets();

    let mut buf = Vec::new();
    info.serialize(&mut buf).unwrap();

    // Corrupt the hash section ident (offset after ident+version+xorb_hash = 7+1+32 = 40)
    let hash_section_offset = 40;
    buf[hash_section_offset..hash_section_offset + 7].copy_from_slice(b"CORRUPT");

    let mut cursor = Cursor::new(buf);
    let result = XorbObjectInfoV1::deserialize(&mut cursor);
    assert!(result.is_err());
}

#[test]
fn xorb_object_info_v1_deserialize_wrong_boundary_section_ident() {
    let mut info = XorbObjectInfoV1::default();
    info.xorb_hash = compute_data_hash(b"test");
    info.num_chunks = 0;
    info.fill_in_boundary_offsets();

    let mut buf = Vec::new();
    info.serialize(&mut buf).unwrap();

    // Find boundary section ident offset
    // It comes after: ident(7) + version(1) + xorb_hash(32) + hash_section_ident(7) + hashes_version(1) + num_chunks(4)
    // = 52
    let boundary_offset = 52;
    if boundary_offset + 7 <= buf.len() {
        buf[boundary_offset..boundary_offset + 7].copy_from_slice(b"CORRUPT");
    }

    let mut cursor = Cursor::new(buf);
    let result = XorbObjectInfoV1::deserialize(&mut cursor);
    assert!(result.is_err());
}

// ============================================================================
// XorbObject validate with corrupted data
// ============================================================================

#[test]
fn xorb_object_validate_xorb_object_bad_footer() {
    let mut buf = vec![0u8; 100];
    // Write a valid-looking footer length
    let footer_len = 50u32;
    buf[96..100].copy_from_slice(&footer_len.to_le_bytes());
    let mut cursor = Cursor::new(buf);
    let result = XorbObject::validate_xorb_object(&mut cursor, &MerkleHash::default());
    assert!(result.is_err() || result.unwrap().is_none());
}

// ============================================================================
// Serialize chunk with auto compression
// ============================================================================

#[test]
fn serialize_chunk_auto_compression() {
    let data = vec![0u8; 1024];
    let mut writer = Cursor::new(Vec::new());
    let bytes_written = serialize_chunk(&data, &mut writer, CompressionScheme::Auto).unwrap();
    assert!(bytes_written > 0);

    writer.set_position(0);
    let (decompressed, _, _) = deserialize_chunk(&mut writer).unwrap();
    assert_eq!(decompressed, data);
}

// ============================================================================
// XorbObject format constants
// ============================================================================

#[test]
fn xorb_object_format_constants() {
    use crate::xorb_object::xorb_object_format::{
        XORB_OBJECT_FORMAT_BOUNDARIES_VERSION,
        XORB_OBJECT_FORMAT_BOUNDARIES_VERSION_NO_UNPACKED_INFO, XORB_OBJECT_FORMAT_HASHES_VERSION,
        XORB_OBJECT_FORMAT_IDENT, XORB_OBJECT_FORMAT_IDENT_BOUNDARIES,
        XORB_OBJECT_FORMAT_IDENT_HASHES, XORB_OBJECT_FORMAT_VERSION, XORB_OBJECT_FORMAT_VERSION_V0,
    };
    assert_eq!(XORB_OBJECT_FORMAT_IDENT, *b"XETBLOB");
    assert_eq!(XORB_OBJECT_FORMAT_IDENT_HASHES, *b"XBLBHSH");
    assert_eq!(XORB_OBJECT_FORMAT_IDENT_BOUNDARIES, *b"XBLBBND");
    assert_eq!(XORB_OBJECT_FORMAT_VERSION, 2);
    assert_eq!(XORB_OBJECT_FORMAT_VERSION_V0, 0);
    assert_eq!(XORB_OBJECT_FORMAT_HASHES_VERSION, 0);
    assert_eq!(XORB_OBJECT_FORMAT_BOUNDARIES_VERSION, 1);
    assert_eq!(XORB_OBJECT_FORMAT_BOUNDARIES_VERSION_NO_UNPACKED_INFO, 0);
}

// ============================================================================
// File data sequence constants
// ============================================================================

#[test]
fn file_flag_constants() {
    assert_eq!(
        crate::metadata_shard::file_structs::MDB_DEFAULT_FILE_FLAG,
        0
    );
    assert_eq!(
        crate::metadata_shard::file_structs::MDB_FILE_FLAG_WITH_VERIFICATION,
        1u32 << 31
    );
    assert_eq!(
        crate::metadata_shard::file_structs::MDB_FILE_FLAG_VERIFICATION_MASK,
        1u32 << 31
    );
    assert_eq!(
        crate::metadata_shard::file_structs::MDB_FILE_FLAG_WITH_METADATA_EXT,
        1u32 << 30
    );
    assert_eq!(
        crate::metadata_shard::file_structs::MDB_FILE_FLAG_METADATA_EXT_MASK,
        1u32 << 30
    );
}

// ============================================================================
// Property tests for serialization roundtrips
// ============================================================================

proptest! {
    #[test]
    fn file_data_sequence_header_roundtrip(
        file_hash in prop::array::uniform4(0u64..u64::MAX),
        num_entries in 0u64..100,
        flags in 0u32..=MDB_FILE_FLAG_WITH_VERIFICATION | MDB_FILE_FLAG_WITH_METADATA_EXT,
    ) {
        let header = FileDataSequenceHeader {
            file_hash: DataHash::from(file_hash),
            file_flags: flags,
            num_entries,
            _unused: 0,
            _pad: 0,
        };
        let mut buf = Vec::new();
        header.serialize(&mut buf).unwrap();
        let deserialized = FileDataSequenceHeader::deserialize(&mut Cursor::new(&buf), 3).unwrap();
        prop_assert_eq!(&header, &deserialized);
    }

    #[test]
    fn file_data_sequence_entry_roundtrip(
        hash in prop::array::uniform4(0u64..u64::MAX),
        unpacked in 0u64..1_000_000,
        start in 0u64..1_000_000,
        end in 0u64..1_000_000,
    ) {
        let entry = FileDataSequenceEntry {
            xorb_hash: DataHash::from(hash),
            xorb_flags: 0,
            unpacked_segment_bytes: unpacked,
            chunk_index_start: start,
            chunk_index_end: end,
        };
        let mut buf = Vec::new();
        entry.serialize(&mut buf).unwrap();
        let deserialized = FileDataSequenceEntry::deserialize(&mut Cursor::new(&buf), 3).unwrap();
        prop_assert_eq!(&entry, &deserialized);
    }

    #[test]
    fn xorb_chunk_sequence_header_roundtrip(
        hash in prop::array::uniform4(0u64..u64::MAX),
        num_entries in 0u64..100,
        bytes_in_xorb in 0u64..10_000_000,
        bytes_on_disk in 0u64..10_000_000,
    ) {
        let header = XorbChunkSequenceHeader {
            xorb_hash: DataHash::from(hash),
            xorb_flags: 0,
            num_entries,
            num_bytes_in_xorb: bytes_in_xorb,
            num_bytes_on_disk: bytes_on_disk,
        };
        let mut buf = Vec::new();
        header.serialize(&mut buf).unwrap();
        let deserialized = XorbChunkSequenceHeader::deserialize(&mut Cursor::new(&buf), 3).unwrap();
        prop_assert_eq!(&header, &deserialized);
    }

    #[test]
    fn xorb_chunk_sequence_entry_roundtrip(
        hash in prop::array::uniform4(0u64..u64::MAX),
        byte_range_start in 0u64..10_000_000,
        unpacked in 0u64..10_000_000,
        flags in 0u32..=u32::MAX,
    ) {
        let entry = XorbChunkSequenceEntry {
            chunk_hash: DataHash::from(hash),
            chunk_byte_range_start: byte_range_start,
            unpacked_segment_bytes: unpacked,
            flags,
            _unused: 0,
        };
        let mut buf = Vec::new();
        entry.serialize(&mut buf).unwrap();
        let deserialized = XorbChunkSequenceEntry::deserialize(&mut Cursor::new(&buf), 3).unwrap();
        prop_assert_eq!(&entry, &deserialized);
    }
}

// ============================================================================
// Property tests for additional binary format types
// ============================================================================

proptest! {
    #[test]
    fn mdb_shard_file_header_roundtrip(
        version in 0u64..100,
        footer_size in 0u64..10_000,
    ) {
        let header = MDBShardFileHeader {
            version,
            footer_size,
            ..Default::default()
        };
        let mut buf = Vec::new();
        header.serialize(&mut buf).unwrap();
        let deserialized = MDBShardFileHeader::deserialize(&mut Cursor::new(&buf)).unwrap();
        prop_assert_eq!(&header, &deserialized);
    }

    #[test]
    fn mdb_shard_file_footer_roundtrip(
        file_info_offset in 0u64..1_000_000_000u64,
        xorb_info_offset in 0u64..1_000_000_000u64,
        file_lookup_offset in 0u64..1_000_000_000u64,
        file_lookup_num_entry in 0u64..1_000_000u64,
        xorb_lookup_offset in 0u64..1_000_000_000u64,
        xorb_lookup_num_entry in 0u64..1_000_000u64,
        chunk_lookup_offset in 0u64..1_000_000_000u64,
        chunk_lookup_num_entry in 0u64..1_000_000u64,
        chunk_hash_hmac_key in prop::array::uniform4(0u64..u64::MAX),
        shard_creation_timestamp in 0u64..1_000_000_000_000u64,
        shard_key_expiry in 0u64..u64::MAX,
        buffer in prop::array::uniform6(0u64..u64::MAX),
        stored_bytes_on_disk in 0u64..1_000_000_000u64,
        materialized_bytes in 0u64..1_000_000_000u64,
        stored_bytes in 0u64..1_000_000_000u64,
        footer_offset in 0u64..1_000_000_000u64,
    ) {
        let footer = MDBShardFileFooter {
            file_info_offset,
            xorb_info_offset,
            file_lookup_offset,
            file_lookup_num_entry,
            xorb_lookup_offset,
            xorb_lookup_num_entry,
            chunk_lookup_offset,
            chunk_lookup_num_entry,
            chunk_hash_hmac_key: HMACKey::from(chunk_hash_hmac_key),
            shard_creation_timestamp,
            shard_key_expiry,
            _buffer: buffer,
            stored_bytes_on_disk,
            materialized_bytes,
            stored_bytes,
            footer_offset,
            ..Default::default()
        };
        let mut buf = Vec::new();
        footer.serialize(&mut buf).unwrap();
        let deserialized = MDBShardFileFooter::deserialize(&mut Cursor::new(&buf)).unwrap();
        prop_assert_eq!(&footer, &deserialized);
    }

    #[allow(deprecated, clippy::field_reassign_with_default)]
    #[test]
    fn xorb_object_info_v0_roundtrip(
        xorb_hash in prop::array::uniform4(0u64..u64::MAX),
        chunk_boundary_offsets in prop::collection::vec(0u32..1_000_000u32, 0..10),
        chunk_hashes_vec in prop::collection::vec(prop::array::uniform4(0u64..u64::MAX), 0..10),
    ) {
        let num_chunks = chunk_boundary_offsets.len().min(chunk_hashes_vec.len());
        let mut info = XorbObjectInfoV0::default();
        info.xorb_hash = MerkleHash::from(xorb_hash);
        info.num_chunks = num_chunks as u32;
        info.chunk_boundary_offsets = chunk_boundary_offsets[..num_chunks].to_vec();
        info.chunk_hashes = chunk_hashes_vec[..num_chunks].iter().map(|a| MerkleHash::from(*a)).collect();
        let mut buf = Vec::new();
        info.serialize(&mut buf).unwrap();
        let (deserialized, _) = XorbObjectInfoV0::deserialize(&mut Cursor::new(&buf)).unwrap();
        prop_assert_eq!(info, deserialized);
    }

    #[test]
    fn xorb_object_info_v1_roundtrip(
        xorb_hash in prop::array::uniform4(0u64..u64::MAX),
        chunk_hashes_vec in prop::collection::vec(prop::array::uniform4(0u64..u64::MAX), 0..10),
        chunk_boundary_offsets in prop::collection::vec(0u64..1_000_000u64, 0..10),
        unpacked_chunk_offsets in prop::collection::vec(0u64..1_000_000u64, 0..10),
    ) {
        let num_chunks = chunk_hashes_vec
            .len().min(chunk_boundary_offsets.len()).min(unpacked_chunk_offsets.len());
        let mut info = XorbObjectInfoV1::default();
        info.xorb_hash = MerkleHash::from(xorb_hash);
        info.num_chunks = num_chunks as u64;
        info.chunk_hashes = chunk_hashes_vec[..num_chunks].iter().map(|a| MerkleHash::from(*a)).collect();
        info.chunk_boundary_offsets = chunk_boundary_offsets[..num_chunks].to_vec();
        info.unpacked_chunk_offsets = unpacked_chunk_offsets[..num_chunks].to_vec();
        info.fill_in_boundary_offsets();
        let mut buf = Vec::new();
        info.serialize(&mut buf).unwrap();
        let (deserialized, _) = XorbObjectInfoV1::deserialize(&mut Cursor::new(&buf)).unwrap();
        prop_assert_eq!(info, deserialized);
    }

    #[test]
    fn xorb_chunk_header_roundtrip(
        compression_scheme in 0u8..3u8,
        compressed_length in 0u32..=0xFFFFFFu32,
        uncompressed_length in 0u32..=0xFFFFFFu32,
    ) {
        let scheme = CompressionScheme::try_from(compression_scheme).unwrap();
        let header = XorbChunkHeader::new(scheme, compressed_length, uncompressed_length);
        let mut buf = Vec::new();
        write_chunk_header(&mut buf, &header).unwrap();
        let deserialized = deserialize_chunk_header(&mut Cursor::new(&buf)).unwrap();
        prop_assert_eq!(header, deserialized);
    }
}

// ============================================================================
// deserialize_chunks_to_writer tests
// ============================================================================

#[test]
fn deserialize_chunks_to_writer_empty_input() {
    let mut reader = std::io::Cursor::new(Vec::new());
    let mut writer = Vec::new();
    let (compressed, indices) = deserialize_chunks_to_writer(&mut reader, &mut writer).unwrap();
    assert_eq!(compressed, 0);
    assert_eq!(indices, vec![0]);
}

#[test]
fn deserialize_chunks_to_writer_single_chunk() {
    let data = b"single chunk test data";
    let mut src = std::io::Cursor::new(Vec::new());
    serialize_chunk(data, &mut src, CompressionScheme::None).unwrap();
    let serialized = src.into_inner();

    let mut reader = std::io::Cursor::new(&serialized);
    let mut writer = Vec::new();
    let (compressed, indices) = deserialize_chunks_to_writer(&mut reader, &mut writer).unwrap();
    assert!(compressed > 0);
    assert_eq!(writer, data);
    assert_eq!(indices, vec![0, data.len() as u32]);
}

#[test]
fn deserialize_chunks_to_writer_multiple_chunks() {
    let chunk1 = b"first chunk data here";
    let chunk2 = b"second chunk data here";
    let mut src = std::io::Cursor::new(Vec::new());
    serialize_chunk(chunk1, &mut src, CompressionScheme::LZ4).unwrap();
    serialize_chunk(chunk2, &mut src, CompressionScheme::LZ4).unwrap();
    let serialized = src.into_inner();

    let mut reader = std::io::Cursor::new(&serialized);
    let mut writer = Vec::new();
    let (compressed, indices) = deserialize_chunks_to_writer(&mut reader, &mut writer).unwrap();
    assert!(compressed > 0);
    let expected = [chunk1.as_slice(), chunk2.as_slice()].concat();
    assert_eq!(writer, expected);
    assert_eq!(
        indices,
        vec![0, chunk1.len() as u32, (chunk1.len() + chunk2.len()) as u32]
    );
}

// ============================================================================
// deserialize_chunk decompressed length mismatch
// ============================================================================

#[test]
fn deserialize_chunk_rejects_uncompressed_length_mismatch() {
    // Manually construct a chunk with None compression header that lies
    // about the uncompressed length.
    let data = b"real data";
    let mut buf = Vec::new();
    // Write header with wrong uncompressed_length
    let header = XorbChunkHeader::new(CompressionScheme::None, data.len() as u32, 9999);
    buf.push(header.version);
    buf.extend_from_slice(&header.get_compressed_length().to_le_bytes()[0..3]);
    buf.push(header.get_compression_scheme().unwrap() as u8);
    buf.extend_from_slice(&9999u32.to_le_bytes()[0..3]); // lie
    buf.extend_from_slice(data);
    let mut reader = std::io::Cursor::new(buf);
    let result = crate::xorb_object::xorb_chunk_format::deserialize_chunk(&mut reader);
    assert!(result.is_err());
}

#[test]
fn deserialize_chunks_to_writer_partial_header_errors() {
    // Only provide 3 bytes (incomplete header)
    let partial_data = [1u8, 2, 3];
    let mut reader = std::io::Cursor::new(partial_data);
    let mut writer = Vec::new();
    let result = deserialize_chunks_to_writer(&mut reader, &mut writer);
    assert!(result.is_err());
}

// ============================================================================
// CompressionScheme decompress_from_reader tests
// ============================================================================

#[test]
fn compression_scheme_decompress_from_reader_empty() {
    let compressed = lz4_compress_from_slice(b"").unwrap();
    let mut reader = std::io::Cursor::new(&compressed);
    let mut writer = Vec::new();
    let bytes = CompressionScheme::LZ4
        .decompress_from_reader(&mut reader, &mut writer)
        .unwrap();
    assert_eq!(bytes, 0);
    assert!(writer.is_empty());
}

// ============================================================================
// XorbObject validate_xorb_object_info error paths
// ============================================================================

#[test]
fn xorb_object_validate_info_num_chunks_mismatch_with_boundaries() {
    let mut info = XorbObjectInfoV1::default();
    info.xorb_hash = compute_data_hash(b"valid_hash");
    info.num_chunks = 2;
    info.chunk_hashes = vec![compute_data_hash(b"c1"), compute_data_hash(b"c2")];
    info.chunk_boundary_offsets = vec![100]; // only 1, but num_chunks=2
    info.unpacked_chunk_offsets = vec![50, 150];
    let obj = XorbObject::from_info(info);
    assert!(obj.get_contents_length().is_err());
}

#[test]
fn xorb_object_validate_info_num_chunks_mismatch_with_hashes() {
    let mut info = XorbObjectInfoV1::default();
    info.xorb_hash = compute_data_hash(b"valid_hash");
    info.num_chunks = 2;
    info.chunk_hashes = vec![compute_data_hash(b"c1")]; // only 1
    info.chunk_boundary_offsets = vec![100, 200];
    info.unpacked_chunk_offsets = vec![50, 100];
    let mut buf = Vec::new();
    assert!(info.serialize(&mut buf).is_err());
}

#[test]
fn xorb_object_validate_info_missing_xorb_hash() {
    let mut info = XorbObjectInfoV1::default();
    info.num_chunks = 1;
    info.chunk_hashes = vec![compute_data_hash(b"c1")];
    info.chunk_boundary_offsets = vec![100];
    info.unpacked_chunk_offsets = vec![50];
    // xorb_hash stays default (all zeros)
    let obj = XorbObject::from_info(info);
    assert!(obj.get_contents_length().is_err());
}

#[test]
fn xorb_object_get_contents_length_uses_last_boundary() {
    let mut info = XorbObjectInfoV1::default();
    info.xorb_hash = compute_data_hash(b"content_len");
    info.num_chunks = 3;
    info.chunk_hashes = vec![
        compute_data_hash(b"c1"),
        compute_data_hash(b"c2"),
        compute_data_hash(b"c3"),
    ];
    info.chunk_boundary_offsets = vec![50, 120, 200];
    info.unpacked_chunk_offsets = vec![30, 80, 150];
    info.fill_in_boundary_offsets();
    let obj = XorbObject::from_info(info);
    assert_eq!(obj.get_contents_length().unwrap(), 200);
}

// ============================================================================
// MDBShardInfo read_all_file_info_sections / read_all_xorb_blocks_full tests
// ============================================================================

#[test]
fn mdb_shard_info_read_all_file_info_sections_empty() {
    // Just a bookend
    let bookend = FileDataSequenceHeader::bookend();
    let mut buf = Vec::new();
    bookend.serialize(&mut buf).unwrap();
    let info = MDBShardInfo::default();
    let mut cursor = std::io::Cursor::new(buf);
    let files = info.read_all_file_info_sections(&mut cursor, 3).unwrap();
    assert!(files.is_empty());
}

#[test]
fn mdb_shard_info_read_all_file_info_sections_multiple() {
    let file1 = MDBFileInfo {
        metadata: FileDataSequenceHeader::new(compute_data_hash(b"f1"), 1u64, false, false),
        segments: vec![FileDataSequenceEntry::new(
            MerkleHash::default(),
            100u64,
            0u64,
            50u64,
        )],
        verification: vec![],
        metadata_ext: None,
    };
    let file2 = MDBFileInfo {
        metadata: FileDataSequenceHeader::new(compute_data_hash(b"f2"), 0u64, false, false),
        segments: vec![],
        verification: vec![],
        metadata_ext: None,
    };
    let mut buf = Vec::new();
    file1.serialize(&mut buf).unwrap();
    file2.serialize(&mut buf).unwrap();
    FileDataSequenceHeader::bookend()
        .serialize(&mut buf)
        .unwrap();

    let info = MDBShardInfo::default();
    let mut cursor = std::io::Cursor::new(buf);
    let files = info.read_all_file_info_sections(&mut cursor, 3).unwrap();
    assert_eq!(files.len(), 2);
    assert_eq!(files[0].segments[0].unpacked_segment_bytes, 100);
}

#[test]
fn mdb_shard_info_read_all_xorb_blocks_full_empty() {
    let bookend = XorbChunkSequenceHeader::bookend();
    let mut buf = Vec::new();
    bookend.serialize(&mut buf).unwrap();
    let info = MDBShardInfo::default();
    let mut cursor = std::io::Cursor::new(buf);
    let xorb_infos = info.read_all_xorb_blocks_full(&mut cursor, 3).unwrap();
    assert!(xorb_infos.is_empty());
}

#[test]
fn mdb_shard_info_read_all_xorb_blocks_full_multiple() {
    let xorb1 = MDBXorbInfo {
        metadata: XorbChunkSequenceHeader::new(compute_data_hash(b"x1"), 1u64, 100u64),
        chunks: vec![XorbChunkSequenceEntry::new(
            compute_data_hash(b"c1"),
            100u64,
            0u64,
        )],
    };
    let xorb2 = MDBXorbInfo {
        metadata: XorbChunkSequenceHeader::new(compute_data_hash(b"x2"), 0u64, 0u64),
        chunks: vec![],
    };
    let mut buf = Vec::new();
    xorb1.serialize(&mut buf).unwrap();
    xorb2.serialize(&mut buf).unwrap();
    XorbChunkSequenceHeader::bookend()
        .serialize(&mut buf)
        .unwrap();

    let info = MDBShardInfo::default();
    let mut cursor = std::io::Cursor::new(buf);
    let xorb_infos = info.read_all_xorb_blocks_full(&mut cursor, 3).unwrap();
    assert_eq!(xorb_infos.len(), 2);
    assert_eq!(xorb_infos[0].chunks.len(), 1);
}

// ============================================================================
// MDBShardInfo read_all_file_info_sections / read_all_xorb_blocks_full partial error tests
// ============================================================================

#[test]
fn mdb_shard_info_read_all_file_info_sections_breaks_on_partial_error() {
    let file1 = MDBFileInfo {
        metadata: FileDataSequenceHeader::new(compute_data_hash(b"f1"), 1u64, false, false),
        segments: vec![FileDataSequenceEntry::new(
            MerkleHash::default(),
            100u64,
            0u64,
            50u64,
        )],
        verification: vec![],
        metadata_ext: None,
    };
    let mut buf = Vec::new();
    file1.serialize(&mut buf).unwrap();
    // Append corrupted data instead of a valid second entry
    buf.extend_from_slice(b"CORRUPTED DATA THAT WONT PARSE AS FILE INFO");

    let info = MDBShardInfo::default();
    let mut cursor = std::io::Cursor::new(buf);
    let files = info.read_all_file_info_sections(&mut cursor, 3).unwrap();
    assert_eq!(files.len(), 1, "should return the first valid file info");
    assert_eq!(files[0].segments[0].unpacked_segment_bytes, 100);
}

#[test]
fn mdb_shard_info_read_all_xorb_blocks_full_breaks_on_partial_error() {
    let xorb1 = MDBXorbInfo {
        metadata: XorbChunkSequenceHeader::new(compute_data_hash(b"x1"), 1u64, 100u64),
        chunks: vec![XorbChunkSequenceEntry::new(
            compute_data_hash(b"c1"),
            100u64,
            0u64,
        )],
    };
    let mut buf = Vec::new();
    xorb1.serialize(&mut buf).unwrap();
    // Append corrupted data instead of a valid second entry
    buf.extend_from_slice(b"CORRUPTED XOrb INFO THAT WONT PARSE");

    let info = MDBShardInfo::default();
    let mut cursor = std::io::Cursor::new(buf);
    let xorb_infos = info.read_all_xorb_blocks_full(&mut cursor, 3).unwrap();
    assert_eq!(
        xorb_infos.len(),
        1,
        "should return the first valid xorb info"
    );
    assert_eq!(xorb_infos[0].chunks.len(), 1);
}

// ============================================================================
// MDBFileInfoView accessor tests
// ============================================================================

#[test]
fn mdb_file_info_view_file_flags_and_contains() {
    // For a header with both flags and num_entries=1:
    // n_structs = 1 (header) + 1 (segments) + 1 (verification) + 1 (metadata_ext) = 4
    use crate::metadata_shard::shard_format::MDB_FILE_INFO_ENTRY_SIZE;
    let header = FileDataSequenceHeader::new(compute_data_hash(b"f"), 1u64, true, true);
    let entry = FileDataSequenceEntry::new(compute_data_hash(b"e"), 100u64, 0u64, 50u64);
    let ver = FileVerificationEntry::new(compute_data_hash(b"v"));
    let met = FileMetadataExt::new(compute_data_hash(b"m"));

    let mut buf = Vec::new();
    header.serialize(&mut buf).unwrap();
    entry.serialize(&mut buf).unwrap();
    ver.serialize(&mut buf).unwrap();
    met.serialize(&mut buf).unwrap();
    // Pad to full expected size
    let expected_size = 4 * MDB_FILE_INFO_ENTRY_SIZE;
    buf.resize(expected_size, 0);

    let view = MDBFileInfoView::new(bytes::Bytes::from(buf)).unwrap();
    assert_eq!(view.file_flags(), header.file_flags);
    assert!(view.contains_verification());
    assert!(view.contains_metadata_ext());

    // Test without any flags
    let header2 = FileDataSequenceHeader::new(compute_data_hash(b"g"), 0u64, false, false);
    let mut buf2 = Vec::new();
    header2.serialize(&mut buf2).unwrap();
    let view2 = MDBFileInfoView::new(bytes::Bytes::from(buf2)).unwrap();
    assert!(!view2.contains_verification());
    assert!(!view2.contains_metadata_ext());
}

#[test]
fn mdb_file_info_view_byte_size_with_verification() {
    let header = FileDataSequenceHeader::new(compute_data_hash(b"f"), 2u64, true, false);
    let e1 = FileDataSequenceEntry::new(MerkleHash::default(), 50u64, 0u64, 25u64);
    let e2 = FileDataSequenceEntry::new(MerkleHash::default(), 75u64, 25u64, 50u64);
    let v1 = FileVerificationEntry::new(compute_data_hash(b"v1"));
    let v2 = FileVerificationEntry::new(compute_data_hash(b"v2"));

    let mut buf = Vec::new();
    header.serialize(&mut buf).unwrap();
    e1.serialize(&mut buf).unwrap();
    e2.serialize(&mut buf).unwrap();
    v1.serialize(&mut buf).unwrap();
    v2.serialize(&mut buf).unwrap();

    use crate::metadata_shard::shard_format::MDB_FILE_INFO_ENTRY_SIZE;
    let view = MDBFileInfoView::new(bytes::Bytes::from(buf)).unwrap();
    assert_eq!(view.byte_size(true), (1 + 2 + 2) * MDB_FILE_INFO_ENTRY_SIZE);
    assert_eq!(view.byte_size(false), (1 + 2) * MDB_FILE_INFO_ENTRY_SIZE);
}

// ============================================================================
// DataHashBytesParseError / DataHashHexParseError tests
// ============================================================================

#[test]
fn data_hash_hex_parse_error_display() {
    let err = DataHashHexParseError;
    assert_eq!(err.to_string(), "Invalid hex input for DataHash");
}

#[test]
fn data_hash_hex_parse_error_from_parse_int() {
    let parse_err = "invalid".parse::<u64>().unwrap_err();
    let err: DataHashHexParseError = parse_err.into();
    assert_eq!(err.to_string(), "Invalid hex input for DataHash");
}

#[test]
fn data_hash_bytes_parse_error_display() {
    let err = DataHashBytesParseError;
    assert_eq!(err.to_string(), "Invalid bytes input for DataHash");
}

#[test]
fn data_hash_error_impl_source() {
    use std::error::Error;
    let err = DataHashHexParseError;
    assert!(Error::source(&err).is_none());
    let err = DataHashBytesParseError;
    assert!(Error::source(&err).is_none());
}

#[test]
fn data_hash_hash_impl() {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    let a = DataHash::from([1u64, 2, 3, 4]);
    let b = DataHash::from([1u64, 2, 3, 4]);
    let mut ha = DefaultHasher::new();
    let mut hb = DefaultHasher::new();
    a.hash(&mut ha);
    b.hash(&mut hb);
    assert_eq!(ha.finish(), hb.finish());
}

#[test]
fn data_hash_deref_mut() {
    let mut h = DataHash::from([0u64; 4]);
    h[0] = 42;
    assert_eq!(h[0], 42);
}

// ============================================================================
// xorb_hash / aggregated hashes — edge-case paths
// ============================================================================

#[test]
fn xorb_hash_three_chunks_no_mod_condition_triggers_end_fallback() {
    // Three hashes where NONE has h[3] % 4 == 0 → triggers line-20 fallback
    let h1 = MerkleHash::from([0u64, 0, 0, 1]); // 1 % 4 = 1
    let h2 = MerkleHash::from([0u64, 0, 0, 2]); // 2 % 4 = 2
    let h3 = MerkleHash::from([0u64, 0, 0, 3]); // 3 % 4 = 3
    let chunks = vec![(h1, 1), (h2, 2), (h3, 3)];
    let result = xorb_hash(&chunks);
    assert_ne!(result, MerkleHash::default());
}

#[test]
fn xorb_hash_three_chunks_with_mod_condition_triggers_early_return() {
    // Three hashes where the third (index 2) has h[3] % 4 == 0 → triggers early return
    let h1 = MerkleHash::from([0u64, 0, 0, 1]); // 1 % 4 = 1
    let h2 = MerkleHash::from([0u64, 0, 0, 2]); // 2 % 4 = 2
    let h3 = MerkleHash::from([0u64, 0, 0, 4]); // 4 % 4 = 0 → triggers early return
    let chunks = vec![(h1, 1), (h2, 2), (h3, 3)];
    let result = xorb_hash(&chunks);
    assert_ne!(result, MerkleHash::default());
}

#[test]
fn xorb_hash_many_chunks_exercises_multiple_merge_rounds() {
    // 10 chunks forces multiple merge rounds and all next_merge_cut paths
    let mut chunks = Vec::new();
    for i in 0u64..10 {
        let h = MerkleHash::from([i, i * 2, i * 3, i * 4]);
        chunks.push((h, i + 1));
    }
    let result = xorb_hash(&chunks);
    assert_ne!(result, MerkleHash::default());
}

#[test]
fn file_hash_with_salt_empty() {
    let salt = [0xABu8; 32];
    let result = file_hash_with_salt(&[], &salt);
    assert_eq!(result, MerkleHash::default());
}

// ============================================================================
// HashedWrite tests
// ============================================================================

#[test]
fn hashed_write_computes_hash() {
    use crate::merklehash::HashedWrite;
    let data = b"hashed write test data";
    let mut writer = HashedWrite::new(Vec::new());
    writer.write_all(data).unwrap();
    let hash = writer.hash();
    let expected = compute_data_hash(data);
    assert_eq!(hash, expected);
}

#[test]
fn hashed_write_into_inner() {
    use crate::merklehash::HashedWrite;
    let data = b"data for into_inner";
    let mut writer = HashedWrite::new(Vec::new());
    writer.write_all(data).unwrap();
    let inner = writer.into_inner();
    assert_eq!(inner, data);
}

#[test]
fn hashed_write_empty_input() {
    use crate::merklehash::HashedWrite;
    let writer = HashedWrite::new(Vec::new());
    let hash = writer.hash();
    let expected = compute_data_hash(b"");
    assert_eq!(hash, expected);
}

#[test]
fn hashed_write_flush() {
    use crate::merklehash::HashedWrite;
    let mut writer = HashedWrite::new(Vec::new());
    writer.write_all(b"flush test").unwrap();
    writer.flush().unwrap();
}

// ============================================================================
// XorbObject serialize_given_info with various sizes
// ============================================================================

#[test]
fn xorb_object_serialize_given_info_zero_chunks() {
    let info = XorbObjectInfoV1::default();
    let mut buf = Vec::new();
    let (obj, written) = XorbObject::serialize_given_info(&mut buf, info).unwrap();
    assert!(written > 0);
    assert_eq!(obj.info.num_chunks, 0);
}

// ============================================================================
// try_read_chunk_header edge cases
// ============================================================================

#[test]
fn try_read_chunk_header_empty_reader_returns_none() {
    let mut reader = std::io::Cursor::new(Vec::new());
    // We can't call try_read_chunk_header directly (it's private),
    // but deserialize_chunks_to_writer should handle this case
    let mut writer = Vec::new();
    let (compressed, indices) = deserialize_chunks_to_writer(&mut reader, &mut writer).unwrap();
    assert_eq!(compressed, 0);
    assert_eq!(indices, vec![0]);
}

// ============================================================================
// MDBFileInfoView header / verification accessors with verification data
// ============================================================================

#[test]
fn mdb_file_info_view_header_accessor() {
    use crate::metadata_shard::shard_format::MDB_FILE_INFO_ENTRY_SIZE;
    let header = FileDataSequenceHeader::new(compute_data_hash(b"hdr-test"), 2u64, true, false);
    let e1 = FileDataSequenceEntry::new(MerkleHash::default(), 50u64, 0u64, 25u64);
    let e2 = FileDataSequenceEntry::new(MerkleHash::default(), 75u64, 25u64, 50u64);
    let v1 = FileVerificationEntry::new(compute_data_hash(b"v1"));
    let v2 = FileVerificationEntry::new(compute_data_hash(b"v2"));

    let mut buf = Vec::new();
    header.serialize(&mut buf).unwrap();
    e1.serialize(&mut buf).unwrap();
    e2.serialize(&mut buf).unwrap();
    v1.serialize(&mut buf).unwrap();
    v2.serialize(&mut buf).unwrap();
    let expected_size = (1 + 2 + 2) * MDB_FILE_INFO_ENTRY_SIZE;
    buf.resize(expected_size, 0);

    let view = MDBFileInfoView::new(bytes::Bytes::from(buf)).unwrap();
    assert_eq!(view.header().file_hash, header.file_hash);
    assert_eq!(view.header().num_entries, header.num_entries);

    let v_entry = view.verification(0);
    assert_eq!(v_entry.range_hash, compute_data_hash(b"v1"));
    let v_entry2 = view.verification(1);
    assert_eq!(v_entry2.range_hash, compute_data_hash(b"v2"));
}

#[test]
fn mdb_file_info_view_into_with_verification() {
    use crate::metadata_shard::shard_format::MDB_FILE_INFO_ENTRY_SIZE;
    let header = FileDataSequenceHeader::new(compute_data_hash(b"f"), 1u64, true, false);
    let entry = FileDataSequenceEntry::new(compute_data_hash(b"e"), 100u64, 0u64, 50u64);
    let ver = FileVerificationEntry::new(compute_data_hash(b"v"));

    let mut buf = Vec::new();
    header.serialize(&mut buf).unwrap();
    entry.serialize(&mut buf).unwrap();
    ver.serialize(&mut buf).unwrap();
    let expected_size = (1 + 1 + 1) * MDB_FILE_INFO_ENTRY_SIZE;
    buf.resize(expected_size, 0);

    let view = MDBFileInfoView::new(bytes::Bytes::from(buf)).unwrap();
    let info: MDBFileInfo = (&view).into();
    assert_eq!(info.segments.len(), 1);
    assert_eq!(info.verification.len(), 1);
    assert_eq!(info.verification[0].range_hash, compute_data_hash(b"v"));
}

// ============================================================================
// aggregated_hashes — write_decimal_u64 with val=0, next_merge_cut edge cases
// ============================================================================

#[test]
fn xorb_hash_with_zero_size_triggers_write_decimal_zero_branch() {
    // write_decimal_u64 has a fast path for val==0 that writes '0' directly.
    // xorb_hash -> aggregated_node_hash -> merged_hash_of_sequence -> write_hash_entry -> write_decimal_u64.
    // Passing a chunk with size=0 triggers that branch.
    let chunks = vec![(MerkleHash::from([1u64, 0, 0, 0]), 0u64)];
    let result = xorb_hash(&chunks);
    assert_ne!(result, MerkleHash::default());
}

#[test]
fn xorb_hash_with_zero_and_nonzero_sizes_exercises_write_decimal_mixed() {
    // Mix zero and non-zero sizes to exercise write_decimal_u64 both branches.
    let chunks = vec![
        (MerkleHash::from([1u64, 0, 0, 0]), 0u64),
        (MerkleHash::from([2u64, 0, 0, 0]), 100u64),
        (MerkleHash::from([3u64, 0, 0, 0]), 0u64),
    ];
    let result = xorb_hash(&chunks);
    assert_ne!(result, MerkleHash::default());
}

#[test]
fn xorb_hash_exact_two_chunks_triggers_next_merge_cut_len_eq_2() {
    // next_merge_cut returns hashes.len() when len <= 2.
    let chunks = vec![
        (MerkleHash::from([0u64, 0, 0, 1]), 10u64),
        (MerkleHash::from([0u64, 0, 0, 2]), 20u64),
    ];
    let result = xorb_hash(&chunks);
    assert_ne!(result, MerkleHash::default());
}

#[test]
fn xorb_hash_mod_condition_at_various_positions_exercises_next_merge_cut() {
    // Test mod condition hits at different positions (index 2, 3, 4, etc.)
    for mod_pos in 2..=5 {
        let chunks: Vec<(MerkleHash, u64)> = (0..6)
            .map(|i| {
                // Make position `mod_pos` have hash[3] % 4 == 0
                let h = if i == mod_pos {
                    MerkleHash::from([0u64, 0, 0, 4]) // 4 % 4 == 0
                } else {
                    MerkleHash::from([0u64, 0, 0, (i + 1) as u64]) // avoid mod 4 == 0
                };
                (h, (i + 1) as u64)
            })
            .collect();
        let result = xorb_hash(&chunks);
        assert_ne!(result, MerkleHash::default(), "failed at mod_pos={mod_pos}");
    }
}

#[test]
fn xorb_hash_large_set_triggers_multiple_merge_passes() {
    // 12 chunks with sizes that force multiple merge passes.
    let mut chunks = Vec::new();
    for i in 0u64..12 {
        let h = MerkleHash::from([i, i * 3, i * 7, i * 11]);
        chunks.push((h, (i + 1) * 10));
    }
    let result = xorb_hash(&chunks);
    assert_ne!(result, MerkleHash::default());
}

// ============================================================================
// aggregated_hashes — file_hash_with_salt non-empty edge case
// ============================================================================

#[test]
fn file_hash_with_salt_non_empty_produces_different_hash_for_different_salts() {
    let chunks = vec![(MerkleHash::from([1u64, 2, 3, 4]), 100u64)];
    let salt1 = [1u8; 32];
    let salt2 = [2u8; 32];
    let h1 = file_hash_with_salt(&chunks, &salt1);
    let h2 = file_hash_with_salt(&chunks, &salt2);
    assert_ne!(h1, h2);
    assert_ne!(h1, MerkleHash::default());
    assert_ne!(h2, MerkleHash::default());
}

// ============================================================================
// data_hash — remaining untested methods
// ============================================================================

#[test]
fn data_hash_as_bytes_returns_correct_slice() {
    let h = DataHash::from([0x0102030405060708u64, 0, 0, 0]);
    let bytes: [u8; 32] = h.into();
    assert_eq!(bytes.len(), 32);
    // First 8 bytes should be little-endian of the first u64
    let first_u64 = u64::from_le_bytes(bytes[0..8].try_into().unwrap());
    assert_eq!(first_u64, 0x0102030405060708u64);
}

#[test]
fn data_hash_try_from_slice_exact_32() {
    let bytes = [0xABu8; 32];
    let h = DataHash::try_from(bytes.as_slice()).unwrap();
    let back: [u8; 32] = h.into();
    assert_eq!(bytes, back);
}

#[test]
fn data_hash_into_vec_u8() {
    let h = DataHash::from([0xDEADBEEFu64, 0xCAFEBABE, 0x12345678, 0x9ABCDEF0]);
    let vec: Vec<u8> = h.into();
    assert_eq!(vec.len(), 32);
    let back = DataHash::from_slice(&vec).unwrap();
    assert_eq!(back, h);
}

// ============================================================================
// XorbObjectInfoV1 — remaining deserialize error paths
// ============================================================================

#[test]
fn xorb_object_info_v1_deserialize_wrong_hashes_version() {
    let mut info = XorbObjectInfoV1::default();
    info.xorb_hash = compute_data_hash(b"test");
    info.num_chunks = 0;
    info.fill_in_boundary_offsets();
    let mut buf = Vec::new();
    info.serialize(&mut buf).unwrap();
    // Corrupt hashes_version byte (offset 47 for 0 chunks: 7+1+32+7 = 47)
    assert!(buf.len() > 47, "buffer too small: {}", buf.len());
    buf[47] = 99; // invalid hashes version
    let mut cursor = Cursor::new(buf);
    let result = XorbObjectInfoV1::deserialize(&mut cursor);
    assert!(result.is_err());
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("Hash Metadata Section")
    );
}

#[test]
fn xorb_object_info_v1_deserialize_wrong_boundaries_version() {
    let mut info = XorbObjectInfoV1::default();
    info.xorb_hash = compute_data_hash(b"test");
    info.num_chunks = 0;
    info.fill_in_boundary_offsets();
    let mut buf = Vec::new();
    info.serialize(&mut buf).unwrap();
    // boundaries_version offset for 0 chunks: 7+1+32+7+1+8+7 = 63
    assert!(buf.len() > 63);
    buf[63] = 99; // invalid boundaries version
    let mut cursor = Cursor::new(buf);
    let result = XorbObjectInfoV1::deserialize(&mut cursor);
    assert!(result.is_err());
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("Boundaries Metadata Section")
    );
}

#[test]
fn xorb_object_info_v1_deserialize_inconsistent_num_chunks_between_sections() {
    let mut info = XorbObjectInfoV1::default();
    info.xorb_hash = compute_data_hash(b"test");
    info.num_chunks = 1;
    info.chunk_hashes = vec![compute_data_hash(b"c1")];
    info.chunk_boundary_offsets = vec![100];
    info.unpacked_chunk_offsets = vec![50];
    info.fill_in_boundary_offsets();
    let mut buf = Vec::new();
    info.serialize(&mut buf).unwrap();
    // Change the boundary section num_chunks (second num_chunks, at offset 60 for n=1)
    // For n=1: 7+1+32+7+1+4+32+7+1 = offset 92... let me compute more carefully.
    // Actually let's just patch it via the helper approach: serialize with mismatched values.
    // Simpler: construct an info with one value, serialize, then corrupt the byte.
    // For 1 chunk with these values, the buffer layout is:
    // 0: ident(7) 7:ver(1) 8:xorb_hash(32) 40:hash_ident(7) 47:hash_ver(1) 48:nchunks(4)
    // 52:chunk_hash(32) 84:bnd_ident(7) 91:bnd_ver(1) 92:nchunks(4) 96:offsets(4)
    // 100:unpacked(4) 104:nchunks(4) 108:hdr_off(4) 112:bnd_off(4) 116:buf(16)
    // So boundary section num_chunks is at offset 92
    assert!(buf.len() > 92);
    buf[92..96].copy_from_slice(&2u32.to_le_bytes()); // mismatch: hash count 1 but boundary says 2
    let mut cursor = Cursor::new(buf);
    let result = XorbObjectInfoV1::deserialize(&mut cursor);
    assert!(result.is_err());
}

#[test]
fn xorb_object_info_v1_deserialize_inconsistent_num_chunks_final() {
    let mut info = XorbObjectInfoV1::default();
    info.xorb_hash = compute_data_hash(b"test");
    info.num_chunks = 1;
    info.chunk_hashes = vec![compute_data_hash(b"c1")];
    info.chunk_boundary_offsets = vec![100];
    info.unpacked_chunk_offsets = vec![50];
    info.fill_in_boundary_offsets();
    let mut buf = Vec::new();
    info.serialize(&mut buf).unwrap();
    // The final num_chunks is at offset 120 for n=1
    assert!(buf.len() > 127);
    buf[120..128].copy_from_slice(&99u64.to_le_bytes());
    let mut cursor = Cursor::new(buf);
    let result = XorbObjectInfoV1::deserialize(&mut cursor);
    assert!(result.is_err());
}

#[test]
fn xorb_object_info_v1_deserialize_incorrect_hashes_section_offset() {
    let mut info = XorbObjectInfoV1::default();
    info.xorb_hash = compute_data_hash(b"test");
    info.num_chunks = 1;
    info.chunk_hashes = vec![compute_data_hash(b"c1")];
    info.chunk_boundary_offsets = vec![100];
    info.unpacked_chunk_offsets = vec![50];
    info.fill_in_boundary_offsets();
    let mut buf = Vec::new();
    info.serialize(&mut buf).unwrap();
    // hashes_section_offset_from_end is at offset 128 for n=1
    assert!(buf.len() > 135);
    buf[128..136].copy_from_slice(&0u64.to_le_bytes()); // set to 0 -> incorrect
    let mut cursor = Cursor::new(buf);
    let result = XorbObjectInfoV1::deserialize(&mut cursor);
    assert!(result.is_err());
}

#[test]
fn xorb_object_info_v1_deserialize_incorrect_boundary_section_offset() {
    let mut info = XorbObjectInfoV1::default();
    info.xorb_hash = compute_data_hash(b"test");
    info.num_chunks = 1;
    info.chunk_hashes = vec![compute_data_hash(b"c1")];
    info.chunk_boundary_offsets = vec![100];
    info.unpacked_chunk_offsets = vec![50];
    info.fill_in_boundary_offsets();
    let mut buf = Vec::new();
    info.serialize(&mut buf).unwrap();
    // boundary_section_offset_from_end is at offset 136 for n=1
    assert!(buf.len() > 143);
    buf[136..144].copy_from_slice(&0u64.to_le_bytes()); // set to 0 -> incorrect
    let mut cursor = Cursor::new(buf);
    let result = XorbObjectInfoV1::deserialize(&mut cursor);
    assert!(result.is_err());
}

// ============================================================================
// XorbObjectInfoV1 — serialize edge case: zero chunks with explicit values
// ============================================================================

#[test]
fn xorb_object_info_v1_serialize_zero_chunks_succeeds() {
    let info = XorbObjectInfoV1::default(); // num_chunks=0
    let mut buf = Vec::new();
    let n = info.serialize(&mut buf).unwrap();
    assert!(n > 0);
    // Should deserialize successfully
    let mut cursor = Cursor::new(&buf);
    let (deser, _) = XorbObjectInfoV1::deserialize(&mut cursor).unwrap();
    assert_eq!(deser.num_chunks, 0);
}

// ============================================================================
// XorbObject validate_xorb_object — comprehensive error path tests
// ============================================================================

#[test]
fn xorb_object_validate_xorb_object_chunk_hash_mismatch() {
    // Build a valid xorb with one chunk
    let (obj, chunk_data, _, _) =
        build_xorb_object(1, ChunkSize::Fixed(100), CompressionScheme::None).unwrap();
    let _chunk_len = chunk_data.len();
    let mut buf = Vec::new();
    buf.extend_from_slice(&chunk_data);
    let info_offset = buf.len();
    let info_len = obj.info.serialize(&mut buf).unwrap();
    buf.extend_from_slice(&(info_len as u64).to_le_bytes());

    // Corrupt the chunk hash in info portion of buffer.
    // In the info serialization for n=1:
    //   ident(7) + ver(1) + xorb_hash(32) + hash_ident(7) + hash_ver(1) + nchunks(4) = 52
    // So chunk_hash[0] is at info_offset + 52
    let hash_offset = info_offset + 52;
    buf[hash_offset..hash_offset + 32].copy_from_slice(&[0u8; 32]);

    // Provide a hash that won't match the final computed xorb_hash either
    let wrong_hash = compute_data_hash(b"wrong_hash");
    let mut cursor = Cursor::new(&buf);
    let result = XorbObject::validate_xorb_object(&mut cursor, &wrong_hash).unwrap();
    // Since the stored chunk_hash is all zeros and the actual chunk hash differs,
    // the validation finds a chunk hash mismatch → returns Ok(None) without error
    assert!(result.is_none());
}

#[test]
fn xorb_object_validate_xorb_object_boundary_mismatch() {
    // Build a valid xorb with 1 chunk via build_xorb_object + serialization
    let (obj, chunk_data, _, _) =
        build_xorb_object(1, ChunkSize::Fixed(100), CompressionScheme::None).unwrap();
    let _chunk_data_len = chunk_data.len();
    let mut buf = Vec::new();
    buf.extend_from_slice(&chunk_data);
    let info_offset = buf.len();
    let info_len = obj.info.serialize(&mut buf).unwrap();
    buf.extend_from_slice(&(info_len as u64).to_le_bytes());
    let hash = obj.info.xorb_hash;

    // The boundary offset is stored in the info portion of the buffer (after chunk data).
    // In the info serialization for n=1:
    //   ident(7) + ver(1) + xorb_hash(32) + hash_ident(7) + hash_ver(1) + nchunks(4)
    //   + chunk_hash(32) + bnd_ident(7) + bnd_ver(1) + nchunks(4) + bnd_offset(4)
    // The bnd_offset is at info_offset + 96 (= 7+1+32+7+1+4+32+7+1+4).
    let bnd_offset_pos = info_offset + 96;
    if bnd_offset_pos + 4 <= buf.len() {
        buf[bnd_offset_pos..bnd_offset_pos + 4].copy_from_slice(&9999u32.to_le_bytes());
    }

    let mut cursor = Cursor::new(&buf);
    let result = XorbObject::validate_xorb_object(&mut cursor, &hash).unwrap();
    assert!(result.is_none());
}

#[test]
fn xorb_object_validate_xorb_object_content_bytes_mismatch_returns_none() {
    let (obj, chunk_data, _, _) =
        build_xorb_object(1, ChunkSize::Fixed(100), CompressionScheme::None).unwrap();
    let mut buf = Vec::new();
    buf.extend_from_slice(&chunk_data);
    // Append extra garbage bytes after chunk data but before footer
    buf.extend_from_slice(b"EXTRA_GARBAGE_BYTES");
    let info_len = obj.info.serialize(&mut buf).unwrap();
    buf.extend_from_slice(&(info_len as u64).to_le_bytes());

    let hash = obj.info.xorb_hash;
    let mut cursor = Cursor::new(&buf);
    let result = XorbObject::validate_xorb_object(&mut cursor, &hash).unwrap();
    assert!(result.is_none());
}

#[test]
fn xorb_object_validate_xorb_object_hash_mismatch() {
    let (obj, chunk_data, _, _) =
        build_xorb_object(1, ChunkSize::Fixed(100), CompressionScheme::None).unwrap();
    let mut buf = Vec::new();
    buf.extend_from_slice(&chunk_data);
    let info_len = obj.info.serialize(&mut buf).unwrap();
    buf.extend_from_slice(&(info_len as u64).to_le_bytes());

    let wrong_hash = compute_data_hash(b"completely_wrong_hash");
    let mut cursor = Cursor::new(&buf);
    let result = XorbObject::validate_xorb_object(&mut cursor, &wrong_hash).unwrap();
    assert!(result.is_none());
}

#[test]
fn xorb_object_validate_xorb_object_unpacked_offset_mismatch_v1() {
    // Build with v1 boundaries (the default), corrupt unpacked offset
    let (obj, chunk_data, _, _) =
        build_xorb_object(1, ChunkSize::Fixed(100), CompressionScheme::None).unwrap();
    let mut buf = Vec::new();
    buf.extend_from_slice(&chunk_data);
    let info_offset = buf.len();
    let info_len = obj.info.serialize(&mut buf).unwrap();
    buf.extend_from_slice(&(info_len as u64).to_le_bytes());
    let hash = obj.info.xorb_hash;

    // unpacked offset position = bnd_offset_pos + 4 = info_offset + 96 + 4 = info_offset + 100
    let unpacked_pos = info_offset + 100;
    if unpacked_pos + 4 <= buf.len() {
        buf[unpacked_pos..unpacked_pos + 4].copy_from_slice(&9999u32.to_le_bytes());
    }

    let mut cursor = Cursor::new(&buf);
    let result = XorbObject::validate_xorb_object(&mut cursor, &hash).unwrap();
    assert!(result.is_none());
}

// ============================================================================
// SerializedXorbObject::from_xorb_with_compression without footer
// ============================================================================

#[test]
fn serialized_xorb_object_from_xorb_without_footer() {
    let chunks = vec![
        Chunk {
            hash: compute_data_hash(b"c1"),
            data: vec![1u8; 50].into(),
        },
        Chunk {
            hash: compute_data_hash(b"c2"),
            data: vec![2u8; 100].into(),
        },
    ];
    let raw = RawXorbData::from_chunks(&chunks, vec![0, 50]);
    let serialized = SerializedXorbObject::from_xorb_with_compression(
        raw,
        CompressionScheme::None,
        false, // no footer serialized
    )
    .unwrap();
    assert!(serialized.footer_start.is_none());
    assert_eq!(serialized.num_chunks, 2);
    assert_ne!(serialized.hash, MerkleHash::default());
    assert!(serialized.raw_num_bytes > 0);
    assert!(!serialized.serialized_data.is_empty());
}

#[test]
fn serialized_xorb_object_from_xorb_with_footer_and_lz4() {
    let chunks = vec![Chunk {
        hash: compute_data_hash(b"x1"),
        data: vec![0xABu8; 200].into(),
    }];
    let raw = RawXorbData::from_chunks(&chunks, vec![0]);
    let serialized = SerializedXorbObject::from_xorb_with_compression(
        raw,
        CompressionScheme::LZ4,
        true, // with footer
    )
    .unwrap();
    assert!(serialized.footer_start.is_some());
    assert_eq!(serialized.num_chunks, 1);
}

// ============================================================================
// XorbObjectInfoV0 — remaining tests
// ============================================================================

#[test]
fn xorb_object_info_v0_serialize_with_multiple_chunks() {
    let mut info = XorbObjectInfoV0::default();
    info.xorb_hash = compute_data_hash(b"multi");
    info.num_chunks = 3;
    info.chunk_boundary_offsets = vec![100, 250, 400];
    info.chunk_hashes = vec![
        compute_data_hash(b"c1"),
        compute_data_hash(b"c2"),
        compute_data_hash(b"c3"),
    ];

    let mut buf = Vec::new();
    #[allow(deprecated)]
    let written = info.serialize(&mut buf).unwrap();
    assert!(written > 0);

    // Deserialize from scratch
    let mut cursor = Cursor::new(&buf);
    #[allow(deprecated)]
    let (deser, _) = XorbObjectInfoV0::deserialize(&mut cursor).unwrap();
    assert_eq!(deser.num_chunks, 3);
    assert_eq!(deser.chunk_boundary_offsets.len(), 3);
    assert_eq!(deser.chunk_hashes.len(), 3);
}

// ============================================================================
// deserialize_chunk — compressed length vs header decompressed length mismatch
// ============================================================================

#[test]
fn deserialize_chunk_rejects_uncompressed_length_mismatch_using_function() {
    // Use deserialize_chunk_to_writer with a bogus header that lies about
    // uncompressed length (0 bytes claimed but data present).
    let data = b"some data that decompresses fine";
    let mut compressed_buf = Vec::new();
    serialize_chunk(data, &mut compressed_buf, CompressionScheme::None).unwrap();

    // Now overwrite the header's uncompressed_length to be much larger
    // header is at bytes 0..8
    let wrong_len = 999999u32;
    compressed_buf[5..8].copy_from_slice(&wrong_len.to_le_bytes()[0..3]);

    let mut reader = Cursor::new(&compressed_buf);
    let result = deserialize_chunk(&mut reader);
    assert!(result.is_err());
}

// ============================================================================
// try_read_chunk_header — partial read edge case
// ============================================================================

#[test]
fn try_read_chunk_header_partial_read_triggers_read_exact_fallback() {
    // Provide exactly 5 bytes (less than full 8-byte header)
    let partial = [0u8, 1, 2, 3, 4];
    let mut reader = Cursor::new(&partial);
    let mut writer = Vec::new();
    let result = deserialize_chunks_to_writer(&mut reader, &mut writer);
    assert!(result.is_err());
}

// ============================================================================
// MDBShardInfo load_from_reader — error paths
// ============================================================================

#[test]
fn mdb_shard_info_load_from_reader_bad_header() {
    let buf = vec![0u8; 48]; // all zeros, wrong magic
    let mut cursor = Cursor::new(buf);
    let result = MDBShardInfo::load_from_reader(&mut cursor);
    assert!(result.is_err());
}

#[test]
fn mdb_shard_info_load_from_reader_file_info_error_when_empty() {
    // Valid header, then garbage (so file info read fails with empty)
    let mut buf = Vec::new();
    MDBShardFileHeader::default().serialize(&mut buf).unwrap();
    buf.extend_from_slice(b"GARBAGE DATA THAT IS NOT A VALID FILE INFO");
    let mut cursor = Cursor::new(buf);
    let result = MDBShardInfo::load_from_reader(&mut cursor);
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(err.to_string().contains("Failed to read file info"));
}

#[test]
fn mdb_shard_info_load_from_reader_file_info_break_on_error_with_data() {
    // File loop gets 2 valid files, then hit EOF (simulate truncated data).
    // The loop breaks (file_infos not empty). Then xorb loop gets EOF and also
    // breaks (xorb_infos empty -> returns error).
    let mut buf = Vec::new();
    MDBShardFileHeader::default().serialize(&mut buf).unwrap();
    let file = MDBFileInfo {
        metadata: FileDataSequenceHeader::new(compute_data_hash(b"f1"), 1u64, false, false),
        segments: vec![FileDataSequenceEntry::new(
            MerkleHash::default(),
            100u64,
            0u64,
            0u64,
        )],
        verification: vec![],
        metadata_ext: None,
    };
    file.serialize(&mut buf).unwrap();
    let file2 = MDBFileInfo {
        metadata: FileDataSequenceHeader::new(compute_data_hash(b"f2"), 0u64, false, false),
        segments: vec![],
        verification: vec![],
        metadata_ext: None,
    };
    file2.serialize(&mut buf).unwrap();
    // Truncate so next read fails (no more data)
    let mut cursor = Cursor::new(buf);
    let result = MDBShardInfo::load_from_reader(&mut cursor);
    // xorb_infos is empty and header read fails -> returns error
    assert!(result.is_err());
}

#[test]
fn mdb_shard_info_load_from_reader_xorb_info_error_when_empty() {
    let mut buf = Vec::new();
    MDBShardFileHeader::default().serialize(&mut buf).unwrap();
    // Write a valid file bookend
    FileDataSequenceHeader::bookend()
        .serialize(&mut buf)
        .unwrap();
    // Then garbage for xorb info section
    buf.extend_from_slice(b"GARBAGE XORB DATA");
    let mut cursor = Cursor::new(buf);
    let result = MDBShardInfo::load_from_reader(&mut cursor);
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(err.to_string().contains("Failed to read xorb info"));
}

#[test]
fn mdb_shard_info_load_from_reader_xorb_info_error_when_non_empty() {
    let mut buf = Vec::new();
    MDBShardFileHeader::default().serialize(&mut buf).unwrap();
    // Valid file bookend
    FileDataSequenceHeader::bookend()
        .serialize(&mut buf)
        .unwrap();
    // Valid xorb info
    let xorb = MDBXorbInfo {
        metadata: XorbChunkSequenceHeader::new(compute_data_hash(b"x"), 1u64, 100u64),
        chunks: vec![XorbChunkSequenceEntry::new(
            compute_data_hash(b"c"),
            100u64,
            0u64,
        )],
    };
    xorb.serialize(&mut buf).unwrap();
    // Garbage after valid xorb
    buf.extend_from_slice(b"GARBAGE");
    let mut cursor = Cursor::new(buf);
    let result = MDBShardInfo::load_from_reader(&mut cursor);
    assert!(result.is_ok());
    let info = result.unwrap();
    assert_eq!(info.num_xorb_entries(), 1);
}

#[test]
fn mdb_shard_info_load_from_reader_full_roundtrip() {
    let mut buf = Vec::new();
    MDBShardFileHeader::default().serialize(&mut buf).unwrap();

    let file = MDBFileInfo {
        metadata: FileDataSequenceHeader::new(compute_data_hash(b"f"), 1u64, false, false),
        segments: vec![FileDataSequenceEntry::new(
            MerkleHash::default(),
            100u64,
            0u64,
            50u64,
        )],
        verification: vec![],
        metadata_ext: None,
    };
    file.serialize(&mut buf).unwrap();
    FileDataSequenceHeader::bookend()
        .serialize(&mut buf)
        .unwrap();

    let xorb = MDBXorbInfo {
        metadata: XorbChunkSequenceHeader::new(compute_data_hash(b"x"), 1u64, 100u64),
        chunks: vec![XorbChunkSequenceEntry::new(
            compute_data_hash(b"c"),
            100u64,
            0u64,
        )],
    };
    xorb.serialize(&mut buf).unwrap();
    XorbChunkSequenceHeader::bookend()
        .serialize(&mut buf)
        .unwrap();

    // Footer (for roundtrip we don't need real footer values)
    MDBShardFileFooter::default().serialize(&mut buf).unwrap();

    let mut cursor = Cursor::new(&buf);
    let info = MDBShardInfo::load_from_reader(&mut cursor).unwrap();
    assert_eq!(info.num_file_entries(), 1);
    assert_eq!(info.num_xorb_entries(), 1);
    assert_eq!(info.file_infos[0].segments[0].unpacked_segment_bytes, 100);
}

// ============================================================================
// MDBShardInfo serialize_from — basic path
// ============================================================================

#[test]
fn mdb_shard_info_serialize_from_roundtrip() {
    let mut shard = crate::metadata_shard::shard_in_memory::MDBInMemoryShard::default();
    let xorb = MDBXorbInfo {
        metadata: XorbChunkSequenceHeader::new(compute_data_hash(b"x"), 0u64, 0u64),
        chunks: vec![],
    };
    shard.add_xorb_block(xorb).unwrap();
    let file = MDBFileInfo {
        metadata: FileDataSequenceHeader::new(compute_data_hash(b"f"), 0u64, false, false),
        segments: vec![],
        verification: vec![],
        metadata_ext: None,
    };
    shard.add_file_reconstruction_info(file).unwrap();

    let mut buf = Vec::new();
    MDBShardInfo::serialize_from(&mut buf, &shard, None).unwrap();
    assert!(!buf.is_empty());
}

// ============================================================================
// XorbStructs — remaining paths
// ============================================================================

#[test]
fn mdb_xorb_info_deserialize_empty_chunks() {
    let header = XorbChunkSequenceHeader::new(compute_data_hash(b"empty"), 0u64, 0u64);
    let mut buf = Vec::new();
    header.serialize(&mut buf).unwrap();
    let mut cursor = Cursor::new(&buf);
    let result = MDBXorbInfo::deserialize(&mut cursor, 3).unwrap();
    assert!(result.is_some());
    let info = result.unwrap();
    assert_eq!(info.chunks.len(), 0);
    assert_eq!(info.metadata.xorb_hash, header.xorb_hash);
}

// ============================================================================
// MDBFileInfo — num_bytes and file_size edge cases
// ============================================================================

#[test]
fn mdb_file_info_num_bytes_with_verification() {
    let info = MDBFileInfo {
        metadata: FileDataSequenceHeader::new(compute_data_hash(b"f"), 2u64, true, false),
        segments: vec![
            FileDataSequenceEntry::new(MerkleHash::default(), 50u64, 0u64, 25u64),
            FileDataSequenceEntry::new(MerkleHash::default(), 75u64, 25u64, 50u64),
        ],
        verification: vec![
            FileVerificationEntry::new(compute_data_hash(b"v1")),
            FileVerificationEntry::new(compute_data_hash(b"v2")),
        ],
        metadata_ext: None,
    };
    let nbytes = info.num_bytes();
    assert!(nbytes > 0);
    assert_eq!(info.file_size(), 125);
}

#[test]
fn mdb_file_info_num_bytes_with_metadata_ext() {
    let info = MDBFileInfo {
        metadata: FileDataSequenceHeader::new(compute_data_hash(b"f"), 1u64, false, true),
        segments: vec![FileDataSequenceEntry::new(
            MerkleHash::default(),
            100u64,
            0u64,
            50u64,
        )],
        verification: vec![],
        metadata_ext: Some(FileMetadataExt::new(compute_data_hash(b"ext"))),
    };
    let nbytes = info.num_bytes();
    assert!(nbytes > 0);
}

// ============================================================================
// MDBFileInfoView — from_data_and_header error path with insufficient size
// ============================================================================

#[test]
fn mdb_file_info_view_from_data_too_small_with_flags() {
    let header = FileDataSequenceHeader::new(MerkleHash::default(), 1u64, true, true);
    let result = MDBFileInfoView::from_data_and_header(header, bytes::Bytes::from(vec![0u8; 10]));
    assert!(result.is_err());
}

// ============================================================================
// HashedWrite — byte-level write and flush coverage
// ============================================================================

#[test]
fn hashed_write_multiple_writes() {
    use crate::merklehash::HashedWrite;
    let mut writer = HashedWrite::new(Vec::new());
    writer.write_all(b"part1").unwrap();
    writer.write_all(b"part2").unwrap();
    writer.write_all(b"part3").unwrap();
    let hash = writer.hash();
    let expected = compute_data_hash(b"part1part2part3");
    assert_eq!(hash, expected);
}

// ============================================================================
// Validate trait — ok_for_format_error with all variants
// ============================================================================

#[test]
fn validate_ok_for_format_error_io_passthrough() {
    let result: Result<u32, CoreError> = Err(CoreError::Io(std::io::Error::other("io error")));
    let err = result.ok_for_format_error().unwrap_err();
    assert!(matches!(err, CoreError::Io(_)));
}

#[test]
fn validate_ok_for_format_error_internal_passthrough() {
    let result: Result<u32, CoreError> = Err(CoreError::InternalError("bug".into()));
    let err = result.ok_for_format_error().unwrap_err();
    assert!(matches!(err, CoreError::InternalError(_)));
}

// ============================================================================
// XorbObject::deserialize with info_length mismatch
// ============================================================================

#[test]
fn xorb_object_deserialize_info_length_zero() {
    // Footer says info_length=0 → seek to end-4, read nothing, total_bytes_read(0) != info_length(0)?
    // Actually it depends. Let's check the behavior.
    let mut info = XorbObjectInfoV1::default();
    info.xorb_hash = compute_data_hash(b"test");
    let mut buf = Vec::new();
    info.serialize(&mut buf).unwrap();
    // Write footer with 0 length (truncated)
    write_u64(&mut buf, 0u64).unwrap();
    let mut cursor = Cursor::new(&buf);
    let result = XorbObject::deserialize(&mut cursor);
    assert!(result.is_err());
}

// ============================================================================
// test_utils coverage: build_raw_xorb / build_xorb_object with ChunkSize::Random
// ============================================================================

#[test]
fn build_raw_xorb_random_chunk_size() {
    let raw = build_raw_xorb(2, ChunkSize::Random(500, 1000));
    assert_eq!(raw.data.len(), 2);
    assert_eq!(raw.num_bytes(), 2 * 1024); // Random falls back to 1024
}

#[test]
fn build_xorb_object_random_chunk_size() {
    let (obj, chunk_data, raw_data, boundaries) =
        build_xorb_object(3, ChunkSize::Random(200, 800), CompressionScheme::LZ4).unwrap();
    assert_eq!(obj.info.num_chunks, 3);
    assert!(!chunk_data.is_empty());
    assert_eq!(raw_data.len(), 3 * 1024);
    assert_eq!(boundaries.len(), 3);
}

// ============================================================================
// validate_xorb_object — SUCCESS path (covers L574, L576)
// ============================================================================

#[test]
fn xorb_object_validate_xorb_object_success_path() {
    let (obj, chunk_data, _, _) =
        build_xorb_object(1, ChunkSize::Fixed(100), CompressionScheme::LZ4).unwrap();
    let mut buf = Vec::new();
    buf.extend_from_slice(&chunk_data);
    let info_len = obj.info.serialize(&mut buf).unwrap();
    buf.extend_from_slice(&(info_len as u64).to_le_bytes());

    let hash = obj.info.xorb_hash;
    let mut cursor = Cursor::new(&buf);
    let result = XorbObject::validate_xorb_object(&mut cursor, &hash).unwrap();
    assert!(result.is_some());
    let validated = result.unwrap();
    assert_eq!(validated.info.num_chunks, 1);
    assert_eq!(validated.info.xorb_hash, hash);
}

// ============================================================================
// validate_xorb_object — empty chunks error
// ============================================================================

#[test]
fn xorb_object_validate_xorb_object_empty_chunks_errors() {
    // Try validating with no chunk data at all (reader has only footer)
    let mut buf = Vec::new();
    let info = XorbObjectInfoV1::default(); // num_chunks=0, no xorb_hash
    let info_len = info.serialize(&mut buf).unwrap() as u32;
    write_u32(&mut buf, info_len).unwrap();
    let mut cursor = Cursor::new(&buf);
    let result = XorbObject::validate_xorb_object(&mut cursor, &MerkleHash::default());
    assert!(result.is_err() || result.unwrap().is_none());
}

// ============================================================================
// reconstruct_xorb_with_footer — edge cases
// ============================================================================

#[test]
fn reconstruct_xorb_with_footer_empty_data() {
    let mut output = Vec::new();
    let result = reconstruct_xorb_with_footer(&mut output, b"");
    assert!(result.is_ok());
    let (obj, hash) = result.unwrap();
    assert_eq!(obj.info.num_chunks, 0);
    assert_eq!(hash, MerkleHash::default());
}

#[test]
fn reconstruct_xorb_with_footer_xetblob_ident_breaks() {
    // Data starting with "XETBLOB" ident breaks the deserialize_chunk_header loop
    let mut output = Vec::new();
    let result = reconstruct_xorb_with_footer(&mut output, b"XETBLOBextra");
    // Should succeed with 0 chunks (header parse returns ChunkHeaderParse -> break)
    assert!(result.is_ok());
    let (obj, hash) = result.unwrap();
    assert_eq!(obj.info.num_chunks, 0);
    assert_eq!(hash, MerkleHash::default());
}

#[test]
fn reconstruct_xorb_with_footer_zero_compressed_length_errors() {
    // Create a chunk header with compressed_length=0, then try to read 0 bytes
    let mut chunk = Vec::new();
    let header = XorbChunkHeader::new(CompressionScheme::None, 0, 100);
    write_chunk_header(&mut chunk, &header).unwrap();
    // Don't add data (compressed_length=0 so no data follows)
    let mut output = Vec::new();
    let result = reconstruct_xorb_with_footer(&mut output, &chunk);
    // The header declares 100 uncompressed bytes but carries none: the declared
    // uncompressed length is enforced during decompression, so this malformed
    // chunk is rejected instead of being accepted as an empty chunk.
    assert!(result.is_err());
}

#[test]
fn reconstruct_xorb_with_footer_malformed_compressed_length_causes_read_error() {
    // Header says compressed_length=100 but only 5 bytes follow
    let mut chunk = Vec::new();
    let header = XorbChunkHeader::new(CompressionScheme::None, 100, 100);
    write_chunk_header(&mut chunk, &header).unwrap();
    chunk.extend_from_slice(b"SHORT"); // only 5 bytes, not 100
    let mut output = Vec::new();
    let result = reconstruct_xorb_with_footer(&mut output, &chunk);
    assert!(result.is_err());
}

// ============================================================================
// try_read_chunk_header — IO error on read (private fn tested via
// deserialize_chunks_to_writer with a reader that returns errors)
// ============================================================================

struct ErrorReader;

impl std::io::Read for ErrorReader {
    fn read(&mut self, _buf: &mut [u8]) -> std::io::Result<usize> {
        Err(std::io::Error::other("read error"))
    }
}

#[test]
fn deserialize_chunks_to_writer_io_error_propagates() {
    let mut reader = ErrorReader;
    let mut writer = Vec::new();
    let result = deserialize_chunks_to_writer(&mut reader, &mut writer);
    assert!(result.is_err());
    // Should be an IO error, not a header parse error
    let err = result.unwrap_err();
    assert!(matches!(err, CoreError::Io(_)));
}

// ============================================================================
// Chunk struct — basic sanity
// ============================================================================

#[test]
fn chunk_struct_basic() {
    let hash = compute_data_hash(b"test");
    let data: Vec<u8> = vec![1u8, 2, 3];
    let chunk = Chunk {
        hash,
        data: data.into(),
    };
    assert_eq!(chunk.hash, hash);
    assert_eq!(&*chunk.data, &[1u8, 2, 3]);
}
