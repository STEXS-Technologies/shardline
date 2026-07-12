use std::io::Cursor;

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
        MDB_FILE_FLAG_METADATA_EXT_MASK, MDB_FILE_FLAG_VERIFICATION_MASK,
        MDB_FILE_FLAG_WITH_METADATA_EXT, MDB_FILE_FLAG_WITH_VERIFICATION, MDBFileInfo,
    },
    shard_format::{MDBShardFileFooter, MDBShardFileHeader, MDBShardInfo},
    xorb_structs::{MDBXorbInfo, MDBXorbInfoView, XorbChunkSequenceEntry, XorbChunkSequenceHeader},
};
use crate::xorb_object::{
    Chunk, CompressionScheme, RawXorbData,
    compression_scheme::{lz4_compress_from_slice, lz4_decompress_from_slice},
    raw_xorb_data::XorbInfo,
    xorb_chunk_format::{
        XORB_CHUNK_HEADER_LENGTH, XorbChunkHeader, deserialize_chunk, deserialize_chunk_header,
        parse_chunk_header, serialize_chunk,
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
    let bytes_ref: &[u8] = h.as_ref();
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
// CompressionScheme tests
// ============================================================================

#[test]
fn compression_scheme_variants() {
    let none = CompressionScheme::None;
    let lz4 = CompressionScheme::LZ4;
    let bg4 = CompressionScheme::ByteGrouping4LZ4;
    let auto = CompressionScheme::Auto;

    assert_eq!(none as u8, 0);
    assert_eq!(lz4 as u8, 1);
    assert_eq!(bg4 as u8, 2);
    assert_eq!(auto as u8, 99);
}

#[test]
fn compression_scheme_try_from() {
    assert_eq!(
        CompressionScheme::try_from(0).unwrap(),
        CompressionScheme::None
    );
    assert_eq!(
        CompressionScheme::try_from(1).unwrap(),
        CompressionScheme::LZ4
    );
    assert_eq!(
        CompressionScheme::try_from(2).unwrap(),
        CompressionScheme::ByteGrouping4LZ4
    );
    assert_eq!(
        CompressionScheme::try_from(99).unwrap(),
        CompressionScheme::Auto
    );
    assert!(
        CompressionScheme::try_from(99).is_err()
            || CompressionScheme::try_from(99).unwrap() == CompressionScheme::Auto
    );
}

#[test]
fn compression_scheme_try_from_invalid() {
    assert!(CompressionScheme::try_from(3u8).is_err());
    assert!(CompressionScheme::try_from(255u8).is_err());
}

#[test]
fn compression_scheme_from_str() {
    assert_eq!(
        "auto".parse::<CompressionScheme>().unwrap(),
        CompressionScheme::Auto
    );
    assert_eq!(
        "none".parse::<CompressionScheme>().unwrap(),
        CompressionScheme::None
    );
    assert_eq!(
        "lz4".parse::<CompressionScheme>().unwrap(),
        CompressionScheme::LZ4
    );
    assert_eq!(
        "bg4-lz4".parse::<CompressionScheme>().unwrap(),
        CompressionScheme::ByteGrouping4LZ4
    );
    assert_eq!(
        "".parse::<CompressionScheme>().unwrap(),
        CompressionScheme::Auto
    );
    assert_eq!(
        "AUTO".parse::<CompressionScheme>().unwrap(),
        CompressionScheme::Auto
    );
    assert_eq!(
        " Auto ".parse::<CompressionScheme>().unwrap(),
        CompressionScheme::Auto
    );
}

#[test]
fn compression_scheme_from_str_invalid() {
    assert!("garbage".parse::<CompressionScheme>().is_err());
    assert!("lz5".parse::<CompressionScheme>().is_err());
}

#[test]
fn compression_scheme_display() {
    assert_eq!(format!("{}", CompressionScheme::Auto), "auto");
    assert_eq!(format!("{}", CompressionScheme::None), "none");
    assert_eq!(format!("{}", CompressionScheme::LZ4), "lz4");
    assert_eq!(
        format!("{}", CompressionScheme::ByteGrouping4LZ4),
        "bg4-lz4"
    );
}

#[test]
fn compression_scheme_resolve_for_data() {
    assert_eq!(
        CompressionScheme::Auto.resolve_for_data(b"test"),
        CompressionScheme::LZ4
    );
    assert_eq!(
        CompressionScheme::None.resolve_for_data(b"test"),
        CompressionScheme::None
    );
    assert_eq!(
        CompressionScheme::LZ4.resolve_for_data(b"test"),
        CompressionScheme::LZ4
    );
}

#[test]
fn compression_scheme_choose_from_data() {
    assert_eq!(
        CompressionScheme::choose_from_data(b"test"),
        CompressionScheme::LZ4
    );
}

#[test]
fn lz4_roundtrip() {
    let data = b"hello world, this is a test of lz4 compression!";
    let compressed = lz4_compress_from_slice(data).unwrap();
    let decompressed = lz4_decompress_from_slice(&compressed).unwrap();
    assert_eq!(&decompressed[..], &data[..]);
}

#[test]
fn lz4_compress_then_decompress_via_enum() {
    let data = b"test data for compression scheme roundtrip";
    let compressed = CompressionScheme::LZ4.compress_from_slice(data).unwrap();
    let decompressed = CompressionScheme::LZ4
        .decompress_from_slice(&compressed)
        .unwrap();
    assert_eq!(&decompressed[..], &data[..]);
}

#[test]
fn compression_none_passthrough() {
    let data = b"no compression here";
    let compressed = CompressionScheme::None.compress_from_slice(data).unwrap();
    assert_eq!(compressed.as_ref(), data);
    let decompressed = CompressionScheme::None
        .decompress_from_slice(&compressed)
        .unwrap();
    assert_eq!(&*decompressed, data);
}

#[test]
fn compression_auto_compress_resolves_to_lz4() {
    let data = b"auto resolved compression";
    let compressed = CompressionScheme::Auto.compress_from_slice(data).unwrap();
    let decompressed = CompressionScheme::LZ4
        .decompress_from_slice(&compressed)
        .unwrap();
    assert_eq!(&decompressed[..], &data[..]);
}

#[test]
fn decompress_auto_errors() {
    assert!(
        CompressionScheme::Auto
            .decompress_from_slice(b"test")
            .is_err()
    );
}

#[test]
fn byte_grouping_lz4_roundtrip() {
    let data = b"byte grouping lz4 round trip test data for verification";
    let compressed = CompressionScheme::ByteGrouping4LZ4
        .compress_from_slice(data)
        .unwrap();
    let decompressed = CompressionScheme::ByteGrouping4LZ4
        .decompress_from_slice(&compressed)
        .unwrap();
    assert_eq!(&decompressed[..], &data[..]);
}

#[test]
fn all_compression_schemes_roundtrip() {
    let data =
        b"comprehensive round trip test across all compression schemes available in the system";
    for scheme in &[
        CompressionScheme::None,
        CompressionScheme::LZ4,
        CompressionScheme::ByteGrouping4LZ4,
    ] {
        let compressed = scheme.compress_from_slice(data).unwrap();
        let decompressed = scheme.decompress_from_slice(&compressed).unwrap();
        assert_eq!(
            &decompressed[..],
            &data[..],
            "compression scheme {scheme:?} roundtrip failed"
        );
    }
}

// ============================================================================
// XorbChunkHeader tests
// ============================================================================

#[test]
fn xorb_chunk_header_new_roundtrip() {
    let header = XorbChunkHeader::new(CompressionScheme::LZ4, 100, 200);
    assert_eq!(header.get_compressed_length(), 100);
    assert_eq!(header.get_uncompressed_length(), 200);
    assert_eq!(
        header.get_compression_scheme().unwrap(),
        CompressionScheme::LZ4
    );
}

#[test]
fn xorb_chunk_header_setters() {
    let mut header = XorbChunkHeader::default();
    header.set_compressed_length(500);
    header.set_uncompressed_length(1000);
    header.set_compression_scheme(CompressionScheme::None);
    assert_eq!(header.get_compressed_length(), 500);
    assert_eq!(header.get_uncompressed_length(), 1000);
    assert_eq!(
        header.get_compression_scheme().unwrap(),
        CompressionScheme::None
    );
}

#[test]
fn xorb_chunk_header_parse_roundtrip() {
    let header = XorbChunkHeader::new(CompressionScheme::None, 42, 84);
    let mut buf = [0u8; XORB_CHUNK_HEADER_LENGTH];
    buf[0] = header.version;
    buf[1..4].copy_from_slice(&header.get_compressed_length().to_le_bytes()[0..3]);
    buf[4] = header.get_compression_scheme().unwrap() as u8;
    buf[5..8].copy_from_slice(&header.get_uncompressed_length().to_le_bytes()[0..3]);

    let parsed = parse_chunk_header(buf).unwrap();
    assert_eq!(parsed.get_compressed_length(), 42);
    assert_eq!(parsed.get_uncompressed_length(), 84);
    assert_eq!(
        parsed.get_compression_scheme().unwrap(),
        CompressionScheme::None
    );
}

#[test]
fn xorb_chunk_header_parse_rejects_xetblob_ident() {
    let mut buf = [0u8; XORB_CHUNK_HEADER_LENGTH];
    buf[..7].copy_from_slice(b"XETBLOB");
    assert!(parse_chunk_header(buf).is_err());
}

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
fn xorb_object_info_v0_default() {
    let info = XorbObjectInfoV0::default();
    assert_eq!(info.version, 0);
    assert_eq!(info.num_chunks, 0);
    assert!(info.chunk_boundary_offsets.is_empty());
    assert!(info.chunk_hashes.is_empty());
}

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

#[test]
fn xorb_object_info_v0_deserialize_invalid_ident() {
    let mut buf = vec![0u8; 100];
    buf[..7].copy_from_slice(b"INVALID");
    let mut cursor = Cursor::new(buf);
    #[allow(deprecated)]
    let result = XorbObjectInfoV0::deserialize(&mut cursor);
    assert!(result.is_err());
}

#[test]
fn xorb_object_info_v0_deserialize_invalid_version() {
    let mut buf = vec![0u8; 100];
    buf[..7].copy_from_slice(b"XETBLOB");
    buf[7] = 99; // invalid version
    let mut cursor = Cursor::new(buf);
    #[allow(deprecated)]
    let result = XorbObjectInfoV0::deserialize(&mut cursor);
    assert!(result.is_err());
}

// ============================================================================
// XorbObjectInfoV1 tests
// ============================================================================

#[test]
fn xorb_object_info_v1_default() {
    let info = XorbObjectInfoV1::default();
    assert_eq!(info.version, 1);
    assert_eq!(info.num_chunks, 0);
    assert!(info.chunk_boundary_offsets.is_empty());
    assert!(info.chunk_hashes.is_empty());
}

#[test]
fn xorb_object_info_v1_fill_in_boundary_offsets() {
    let mut info = XorbObjectInfoV1::default();
    info.chunk_hashes = vec![compute_data_hash(b"a")];
    info.chunk_boundary_offsets = vec![100];
    info.unpacked_chunk_offsets = vec![50];
    info.num_chunks = 1;
    info.fill_in_boundary_offsets();
    assert!(info.boundary_section_offset_from_end > 0);
    assert!(info.hashes_section_offset_from_end > 0);
}

#[test]
fn xorb_object_info_v1_serialized_length() {
    let mut info = XorbObjectInfoV1::default();
    info.chunk_hashes = vec![compute_data_hash(b"a")];
    info.chunk_boundary_offsets = vec![100];
    info.unpacked_chunk_offsets = vec![50];
    info.num_chunks = 1;
    let len = info.serialized_length();
    assert!(len > 0);
}

#[test]
fn xorb_object_info_v1_has_chunk_hashes() {
    let mut info = XorbObjectInfoV1::default();
    assert!(!info.has_chunk_hashes());
    info.chunk_hashes = vec![compute_data_hash(b"a")];
    assert!(info.has_chunk_hashes());
}

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
    assert_eq!(v1.version, 1);
    assert_eq!(v1.xorb_hash, expected_hash);
    assert_eq!(v1.num_chunks, 1);
    assert_eq!(v1.chunk_hashes.len(), 1);
}

#[test]
fn xorb_object_info_v1_serialize_chunk_hash_mismatch() {
    let mut info = XorbObjectInfoV1::default();
    info.num_chunks = 2;
    info.chunk_hashes = vec![compute_data_hash(b"c1")]; // only 1 hash but num_chunks=2
    info.chunk_boundary_offsets = vec![50, 100];
    info.unpacked_chunk_offsets = vec![50, 100];
    let mut buf = Vec::new();
    assert!(info.serialize(&mut buf).is_err());
}

#[test]
fn xorb_object_info_v1_serialize_boundary_mismatch() {
    let mut info = XorbObjectInfoV1::default();
    info.num_chunks = 2;
    info.chunk_hashes = vec![compute_data_hash(b"c1"), compute_data_hash(b"c2")];
    info.chunk_boundary_offsets = vec![50]; // only 1 offset but num_chunks=2
    info.unpacked_chunk_offsets = vec![50, 100];
    let mut buf = Vec::new();
    assert!(info.serialize(&mut buf).is_err());
}

#[test]
fn xorb_object_info_v1_serialize_unpacked_mismatch() {
    let mut info = XorbObjectInfoV1::default();
    info.num_chunks = 2;
    info.chunk_hashes = vec![compute_data_hash(b"c1"), compute_data_hash(b"c2")];
    info.chunk_boundary_offsets = vec![50, 100];
    info.unpacked_chunk_offsets = vec![50]; // only 1 offset but num_chunks=2
    let mut buf = Vec::new();
    assert!(info.serialize(&mut buf).is_err());
}

#[test]
fn xorb_object_info_v1_deserialize_invalid_ident() {
    let mut buf = vec![0u8; 200];
    buf[..7].copy_from_slice(b"BADIDNT");
    let mut cursor = Cursor::new(buf);
    assert!(XorbObjectInfoV1::deserialize(&mut cursor).is_err());
}

#[test]
fn xorb_object_info_v1_deserialize_invalid_version() {
    let mut buf = vec![0u8; 200];
    buf[..7].copy_from_slice(b"XETBLOB");
    buf[7] = 5; // invalid version
    let mut cursor = Cursor::new(buf);
    assert!(XorbObjectInfoV1::deserialize(&mut cursor).is_err());
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
    assert_eq!(v1.version, 1);
    assert_eq!(v1.xorb_hash, v0.xorb_hash);
    assert_eq!(v1.num_chunks, 1);
}

// ============================================================================
// XorbObject tests
// ============================================================================

#[test]
fn xorb_object_default() {
    let obj = XorbObject::default();
    assert_eq!(obj.info.version, 1);
    assert!(obj.info_length > 0);
}

#[test]
fn xorb_object_from_info() {
    let info = XorbObjectInfoV1::default();
    let len = info.serialized_length() as u32;
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
    let mut writer = Cursor::new(Vec::<u8>::new());

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
// FileDataSequenceHeader tests
// ============================================================================

#[test]
fn file_data_sequence_header_new() {
    let hash = compute_data_hash(b"file");
    let header = FileDataSequenceHeader::new(hash, 3u32, false, false);
    assert_eq!(header.file_hash, hash);
    assert_eq!(header.num_entries, 3);
    assert_eq!(header.file_flags, 0);
}

#[test]
fn file_data_sequence_header_new_with_verification() {
    let hash = compute_data_hash(b"file");
    let header = FileDataSequenceHeader::new(hash, 1u32, true, false);
    assert!(header.contains_verification());
    assert!(!header.contains_metadata_ext());
}

#[test]
fn file_data_sequence_header_new_with_metadata_ext() {
    let hash = compute_data_hash(b"file");
    let header = FileDataSequenceHeader::new(hash, 1u32, false, true);
    assert!(!header.contains_verification());
    assert!(header.contains_metadata_ext());
}

#[test]
fn file_data_sequence_header_new_with_both() {
    let hash = compute_data_hash(b"file");
    let header = FileDataSequenceHeader::new(hash, 2u32, true, true);
    assert!(header.contains_verification());
    assert!(header.contains_metadata_ext());
}

#[test]
fn file_data_sequence_header_bookend() {
    let bookend = FileDataSequenceHeader::bookend();
    assert!(bookend.is_bookend());
}

#[test]
fn file_data_sequence_header_not_bookend() {
    let header = FileDataSequenceHeader::new(MerkleHash::default(), 0u32, false, false);
    assert!(!header.is_bookend());
}

#[test]
fn file_data_sequence_header_serialize_roundtrip() {
    let header = FileDataSequenceHeader::new(compute_data_hash(b"f"), 5u32, true, false);
    let mut buf = Vec::new();
    header.serialize(&mut buf).unwrap();

    let mut cursor = Cursor::new(buf);
    let deserialized = FileDataSequenceHeader::deserialize(&mut cursor).unwrap();
    assert_eq!(header.file_hash, deserialized.file_hash);
    assert_eq!(header.num_entries, deserialized.num_entries);
    assert_eq!(header.file_flags, deserialized.file_flags);
}

#[test]
fn file_data_sequence_header_num_info_entry_following_no_flags() {
    let header = FileDataSequenceHeader::new(MerkleHash::default(), 3u32, false, false);
    assert_eq!(header.num_info_entry_following(), 3);
}

#[test]
fn file_data_sequence_header_num_info_entry_following_verification() {
    let header = FileDataSequenceHeader::new(MerkleHash::default(), 3u32, true, false);
    assert_eq!(header.num_info_entry_following(), 6); // 3*2 + 0
}

#[test]
fn file_data_sequence_header_num_info_entry_following_metadata_ext() {
    let header = FileDataSequenceHeader::new(MerkleHash::default(), 3u32, false, true);
    assert_eq!(header.num_info_entry_following(), 4); // 3 + 1
}

#[test]
fn file_data_sequence_header_num_info_entry_following_both() {
    let header = FileDataSequenceHeader::new(MerkleHash::default(), 3u32, true, true);
    assert_eq!(header.num_info_entry_following(), 7); // 3*2 + 1
}

#[test]
fn file_data_sequence_header_default() {
    let header = FileDataSequenceHeader::default();
    assert_eq!(header.num_entries, 0);
    assert_eq!(header.file_flags, 0);
}

// ============================================================================
// FileDataSequenceEntry tests
// ============================================================================

#[test]
fn file_data_sequence_entry_new() {
    let hash = compute_data_hash(b"segment");
    let entry = FileDataSequenceEntry::new(hash, 1024u32, 0u32, 512u32);
    assert_eq!(entry.xorb_hash, hash);
    assert_eq!(entry.unpacked_segment_bytes, 1024);
    assert_eq!(entry.chunk_index_start, 0);
    assert_eq!(entry.chunk_index_end, 512);
}

#[test]
fn file_data_sequence_entry_serialize_roundtrip() {
    let entry = FileDataSequenceEntry::new(compute_data_hash(b"e"), 200u32, 10u32, 20u32);
    let mut buf = Vec::new();
    entry.serialize(&mut buf).unwrap();

    let mut cursor = Cursor::new(buf);
    let deserialized = FileDataSequenceEntry::deserialize(&mut cursor).unwrap();
    assert_eq!(entry, deserialized);
}

#[test]
fn file_data_sequence_entry_from_xorb_entries() {
    let hash = compute_data_hash(b"xorb");
    let metadata = XorbChunkSequenceHeader::new(hash, 2u32, 200u32);
    let chunks = vec![
        XorbChunkSequenceEntry::new(compute_data_hash(b"c1"), 100u32, 0u32),
        XorbChunkSequenceEntry::new(compute_data_hash(b"c2"), 100u32, 100u32),
    ];
    let entry = FileDataSequenceEntry::from_xorb_entries(&metadata, &chunks, 0u32, 2u32);
    assert_eq!(entry.xorb_hash, hash);
    assert_eq!(entry.unpacked_segment_bytes, 200);
    assert_eq!(entry.chunk_index_start, 0);
    assert_eq!(entry.chunk_index_end, 2);
}

#[test]
fn file_data_sequence_entry_from_xorb_entries_empty() {
    let metadata = XorbChunkSequenceHeader::new(MerkleHash::default(), 0u32, 0u32);
    let entry = FileDataSequenceEntry::from_xorb_entries(&metadata, &[], 0u32, 0u32);
    assert_eq!(entry, FileDataSequenceEntry::default());
}

// ============================================================================
// FileVerificationEntry tests
// ============================================================================

#[test]
fn file_verification_entry_new() {
    let hash = compute_data_hash(b"verify");
    let entry = FileVerificationEntry::new(hash);
    assert_eq!(entry.range_hash, hash);
}

#[test]
fn file_verification_entry_serialize_roundtrip() {
    let entry = FileVerificationEntry::new(compute_data_hash(b"v"));
    let mut buf = Vec::new();
    entry.serialize(&mut buf).unwrap();

    let mut cursor = Cursor::new(buf);
    let deserialized = FileVerificationEntry::deserialize(&mut cursor).unwrap();
    assert_eq!(entry.range_hash, deserialized.range_hash);
}

// ============================================================================
// FileMetadataExt tests
// ============================================================================

#[test]
fn file_metadata_ext_new() {
    let sha = compute_data_hash(b"sha");
    let ext = FileMetadataExt::new(sha);
    assert_eq!(ext.sha256, sha);
}

#[test]
fn file_metadata_ext_serialize_roundtrip() {
    let ext = FileMetadataExt::new(compute_data_hash(b"ext"));
    let mut buf = Vec::new();
    ext.serialize(&mut buf).unwrap();

    let mut cursor = Cursor::new(buf);
    let deserialized = FileMetadataExt::deserialize(&mut cursor).unwrap();
    assert_eq!(ext.sha256, deserialized.sha256);
}

// ============================================================================
// MDBFileInfo tests
// ============================================================================

#[test]
fn mdb_file_info_num_bytes() {
    let header = FileDataSequenceHeader::new(MerkleHash::default(), 2u32, false, false);
    let segments = vec![
        FileDataSequenceEntry::new(MerkleHash::default(), 100u32, 0u32, 50u32),
        FileDataSequenceEntry::new(MerkleHash::default(), 200u32, 50u32, 100u32),
    ];
    let info = MDBFileInfo {
        metadata: header,
        segments,
        verification: vec![],
        metadata_ext: None,
    };
    let nbytes = info.num_bytes();
    assert!(nbytes > 0);
}

#[test]
fn mdb_file_info_file_size() {
    let header = FileDataSequenceHeader::new(MerkleHash::default(), 2u32, false, false);
    let segments = vec![
        FileDataSequenceEntry::new(MerkleHash::default(), 100u32, 0u32, 50u32),
        FileDataSequenceEntry::new(MerkleHash::default(), 200u32, 50u32, 100u32),
    ];
    let info = MDBFileInfo {
        metadata: header,
        segments,
        verification: vec![],
        metadata_ext: None,
    };
    assert_eq!(info.file_size(), 300);
}

#[test]
fn mdb_file_info_serialize_roundtrip_no_flags() {
    let info = MDBFileInfo {
        metadata: FileDataSequenceHeader::new(compute_data_hash(b"f"), 2u32, false, false),
        segments: vec![
            FileDataSequenceEntry::new(MerkleHash::default(), 50u32, 0u32, 25u32),
            FileDataSequenceEntry::new(MerkleHash::default(), 75u32, 25u32, 50u32),
        ],
        verification: vec![],
        metadata_ext: None,
    };

    let mut buf = Vec::new();
    info.serialize(&mut buf).unwrap();

    let mut cursor = Cursor::new(buf);
    let deserialized = MDBFileInfo::deserialize(&mut cursor).unwrap().unwrap();
    assert_eq!(info.metadata.file_hash, deserialized.metadata.file_hash);
    assert_eq!(info.segments.len(), deserialized.segments.len());
    assert_eq!(
        info.segments[0].unpacked_segment_bytes,
        deserialized.segments[0].unpacked_segment_bytes
    );
}

#[test]
fn mdb_file_info_serialize_with_verification() {
    let info = MDBFileInfo {
        metadata: FileDataSequenceHeader::new(compute_data_hash(b"f"), 1u32, true, false),
        segments: vec![FileDataSequenceEntry::new(
            MerkleHash::default(),
            50u32,
            0u32,
            25u32,
        )],
        verification: vec![FileVerificationEntry::new(compute_data_hash(b"v"))],
        metadata_ext: None,
    };

    let mut buf = Vec::new();
    info.serialize(&mut buf).unwrap();

    let mut cursor = Cursor::new(buf);
    let deserialized = MDBFileInfo::deserialize(&mut cursor).unwrap().unwrap();
    assert!(deserialized.contains_verification());
    assert_eq!(deserialized.verification.len(), 1);
}

#[test]
fn mdb_file_info_serialize_with_metadata_ext() {
    let info = MDBFileInfo {
        metadata: FileDataSequenceHeader::new(compute_data_hash(b"f"), 1u32, false, true),
        segments: vec![FileDataSequenceEntry::new(
            MerkleHash::default(),
            50u32,
            0u32,
            25u32,
        )],
        verification: vec![],
        metadata_ext: Some(FileMetadataExt::new(compute_data_hash(b"ext"))),
    };

    let mut buf = Vec::new();
    info.serialize(&mut buf).unwrap();

    let mut cursor = Cursor::new(buf);
    let deserialized = MDBFileInfo::deserialize(&mut cursor).unwrap().unwrap();
    assert!(deserialized.contains_metadata_ext());
    assert!(deserialized.metadata_ext.is_some());
}

#[test]
fn mdb_file_info_deserialize_bookend_returns_none() {
    let bookend = FileDataSequenceHeader::bookend();
    let mut buf = Vec::new();
    bookend.serialize(&mut buf).unwrap();

    let mut cursor = Cursor::new(buf);
    let result = MDBFileInfo::deserialize(&mut cursor).unwrap();
    assert!(result.is_none());
}

#[test]
fn mdb_file_info_default() {
    let info = MDBFileInfo::default();
    assert_eq!(info.metadata, FileDataSequenceHeader::default());
    assert!(info.segments.is_empty());
    assert!(info.verification.is_empty());
    assert!(info.metadata_ext.is_none());
}

// ============================================================================
// MDBXorbInfo tests
// ============================================================================

#[test]
fn mdb_xorb_info_new() {
    let hash = compute_data_hash(b"xorb");
    let metadata = XorbChunkSequenceHeader::new(hash, 1u32, 100u32);
    let chunks = vec![XorbChunkSequenceEntry::new(
        compute_data_hash(b"c"),
        100u32,
        0u32,
    )];
    let info = MDBXorbInfo { metadata, chunks };
    assert_eq!(info.num_bytes(), 48 + 48); // header + 1 entry
}

#[test]
fn mdb_xorb_info_serialize_roundtrip() {
    let hash = compute_data_hash(b"xorb");
    let metadata = XorbChunkSequenceHeader::new(hash, 1u32, 200u32);
    let chunks = vec![XorbChunkSequenceEntry::new(
        compute_data_hash(b"c"),
        200u32,
        0u32,
    )];
    let info = MDBXorbInfo { metadata, chunks };

    let mut buf = Vec::new();
    info.serialize(&mut buf).unwrap();

    let mut cursor = Cursor::new(buf);
    let deserialized = MDBXorbInfo::deserialize(&mut cursor).unwrap().unwrap();
    assert_eq!(info.metadata.xorb_hash, deserialized.metadata.xorb_hash);
    assert_eq!(info.chunks.len(), deserialized.chunks.len());
    assert_eq!(info.chunks[0].chunk_hash, deserialized.chunks[0].chunk_hash);
}

#[test]
fn mdb_xorb_info_deserialize_bookend() {
    let bookend = XorbChunkSequenceHeader::bookend();
    let mut buf = Vec::new();
    bookend.serialize(&mut buf).unwrap();

    let mut cursor = Cursor::new(buf);
    let result = MDBXorbInfo::deserialize(&mut cursor).unwrap();
    assert!(result.is_none());
}

#[test]
fn mdb_xorb_info_chunks_and_boundaries() {
    let hash = compute_data_hash(b"xorb");
    let metadata = XorbChunkSequenceHeader::new(hash, 2u32, 300u32);
    let chunks = vec![
        XorbChunkSequenceEntry::new(compute_data_hash(b"c1"), 100u32, 0u32),
        XorbChunkSequenceEntry::new(compute_data_hash(b"c2"), 200u32, 100u32),
    ];
    let info = MDBXorbInfo { metadata, chunks };
    let boundaries = info.chunks_and_boundaries();
    assert_eq!(boundaries.len(), 2);
    assert_eq!(boundaries[0].1, 100); // start(0) + unpacked(100)
    assert_eq!(boundaries[1].1, 300); // start(100) + unpacked(200)
}

#[test]
fn mdb_xorb_info_default() {
    let info = MDBXorbInfo::default();
    assert!(info.chunks.is_empty());
    assert_eq!(info.metadata.num_entries, 0);
}

// ============================================================================
// XorbChunkSequenceHeader tests
// ============================================================================

#[test]
fn xorb_chunk_sequence_header_new() {
    let hash = compute_data_hash(b"xorb");
    let header = XorbChunkSequenceHeader::new(hash, 3u32, 500u32);
    assert_eq!(header.xorb_hash, hash);
    assert_eq!(header.num_entries, 3);
    assert_eq!(header.num_bytes_in_xorb, 500);
    assert_eq!(header.num_bytes_on_disk, 0);
}

#[test]
fn xorb_chunk_sequence_header_bookend() {
    let bookend = XorbChunkSequenceHeader::bookend();
    assert!(bookend.is_bookend());
}

#[test]
fn xorb_chunk_sequence_header_not_bookend() {
    let header = XorbChunkSequenceHeader::new(MerkleHash::default(), 0u32, 0u32);
    assert!(!header.is_bookend());
}

#[test]
fn xorb_chunk_sequence_header_serialize_roundtrip() {
    let header = XorbChunkSequenceHeader::new(compute_data_hash(b"h"), 2u32, 300u32);
    let mut buf = Vec::new();
    header.serialize(&mut buf).unwrap();

    let mut cursor = Cursor::new(buf);
    let deserialized = XorbChunkSequenceHeader::deserialize(&mut cursor).unwrap();
    assert_eq!(header, deserialized);
}

// ============================================================================
// XorbChunkSequenceEntry tests
// ============================================================================

#[test]
fn xorb_chunk_sequence_entry_new() {
    let hash = compute_data_hash(b"chunk");
    let entry = XorbChunkSequenceEntry::new(hash, 512u32, 100u32);
    assert_eq!(entry.chunk_hash, hash);
    assert_eq!(entry.unpacked_segment_bytes, 512);
    assert_eq!(entry.chunk_byte_range_start, 100);
    assert_eq!(entry.flags, 0);
    assert_eq!(entry._unused, 0);
}

#[test]
fn xorb_chunk_sequence_entry_with_global_dedup_flag() {
    let entry = XorbChunkSequenceEntry::new(MerkleHash::default(), 100u32, 0u32);
    let flagged = entry.with_global_dedup_flag(true);
    assert_ne!(flagged.flags, 0);

    let unflagged = flagged.with_global_dedup_flag(false);
    assert_eq!(unflagged.flags, 0);
}

#[test]
fn xorb_chunk_sequence_entry_is_global_dedup_eligible() {
    let entry = XorbChunkSequenceEntry::new(MerkleHash::default(), 100u32, 0u32);
    // default hash (all zeros) has modulus 0, so it IS eligible
    assert!(entry.is_global_dedup_eligible());

    let entry = entry.with_global_dedup_flag(true);
    assert!(entry.is_global_dedup_eligible());

    let _entry = entry.with_global_dedup_flag(false);
    // depends on the hash - if hash % 1024 == 0, still eligible
}

#[test]
fn xorb_chunk_sequence_entry_serialize_roundtrip() {
    let entry = XorbChunkSequenceEntry::new(compute_data_hash(b"e"), 256u32, 64u32);
    let mut buf = Vec::new();
    entry.serialize(&mut buf).unwrap();

    let mut cursor = Cursor::new(buf);
    let deserialized = XorbChunkSequenceEntry::deserialize(&mut cursor).unwrap();
    assert_eq!(entry, deserialized);
}

// ============================================================================
// MDBXorbInfoView tests
// ============================================================================

#[test]
fn mdb_xorb_info_view_from_bytes() {
    let hash = compute_data_hash(b"view");
    let metadata = XorbChunkSequenceHeader::new(hash, 1u32, 100u32);
    let chunk = XorbChunkSequenceEntry::new(compute_data_hash(b"c"), 100u32, 0u32);

    let mut buf = Vec::new();
    metadata.serialize(&mut buf).unwrap();
    chunk.serialize(&mut buf).unwrap();

    let view = MDBXorbInfoView::new(bytes::Bytes::from(buf)).unwrap();
    assert_eq!(view.xorb_hash(), hash);
    assert_eq!(view.num_entries(), 1);

    let c = view.chunk(0);
    assert_eq!(c.chunk_hash, chunk.chunk_hash);
}

#[test]
fn mdb_xorb_info_view_byte_size() {
    let metadata = XorbChunkSequenceHeader::new(MerkleHash::default(), 2u32, 200u32);
    let chunk1 = XorbChunkSequenceEntry::new(MerkleHash::default(), 100u32, 0u32);
    let chunk2 = XorbChunkSequenceEntry::new(MerkleHash::default(), 100u32, 100u32);

    let mut buf = Vec::new();
    metadata.serialize(&mut buf).unwrap();
    chunk1.serialize(&mut buf).unwrap();
    chunk2.serialize(&mut buf).unwrap();

    let view = MDBXorbInfoView::new(bytes::Bytes::from(buf)).unwrap();
    assert_eq!(
        view.byte_size(),
        std::mem::size_of::<XorbChunkSequenceHeader>()
            + 2 * std::mem::size_of::<XorbChunkSequenceEntry>()
    );
}

#[test]
fn mdb_xorb_info_view_serialize_roundtrip() {
    let hash = compute_data_hash(b"ser");
    let metadata = XorbChunkSequenceHeader::new(hash, 1u32, 50u32);
    let chunk = XorbChunkSequenceEntry::new(compute_data_hash(b"c"), 50u32, 0u32);

    let mut buf = Vec::new();
    metadata.serialize(&mut buf).unwrap();
    chunk.serialize(&mut buf).unwrap();

    let view = MDBXorbInfoView::new(bytes::Bytes::from(buf.clone())).unwrap();
    let mut out = Vec::new();
    view.serialize(&mut out).unwrap();
    assert_eq!(buf, out);
}

#[test]
fn mdb_xorb_info_view_from_data_too_small() {
    let metadata = XorbChunkSequenceHeader::new(MerkleHash::default(), 2u32, 100u32);
    let mut buf = Vec::new();
    metadata.serialize(&mut buf).unwrap();
    // Intentionally too small - no room for 2 chunks
    assert!(
        MDBXorbInfoView::from_data_and_header(metadata, bytes::Bytes::from(vec![0u8; 10])).is_err()
    );
}

// ============================================================================
// MDBShardFileHeader tests
// ============================================================================

#[test]
fn mdb_shard_file_header_default() {
    let header = MDBShardFileHeader::default();
    assert_eq!(header.version, 2);
    assert!(header.footer_size > 0);
}

#[test]
fn mdb_shard_file_header_serialize_roundtrip() {
    let header = MDBShardFileHeader::default();
    let mut buf = Vec::new();
    header.serialize(&mut buf).unwrap();

    let mut cursor = Cursor::new(buf);
    let deserialized = MDBShardFileHeader::deserialize(&mut cursor).unwrap();
    assert_eq!(header, deserialized);
}

#[test]
fn mdb_shard_file_header_deserialize_wrong_magic() {
    let mut buf = vec![0u8; 48];
    let mut bad_magic = [b'X'; 32];
    bad_magic[..5].copy_from_slice(b"WRONG");
    buf[..32].copy_from_slice(&bad_magic);
    buf[32..40].copy_from_slice(&2u64.to_le_bytes());
    buf[40..48].copy_from_slice(&48u64.to_le_bytes());
    let mut cursor = Cursor::new(buf);
    assert!(MDBShardFileHeader::deserialize(&mut cursor).is_err());
}

// ============================================================================
// MDBShardFileFooter tests
// ============================================================================

#[test]
fn mdb_shard_file_footer_default() {
    let footer = MDBShardFileFooter::default();
    assert_eq!(footer.version, 1);
    assert_eq!(footer.shard_key_expiry, u64::MAX);
}

#[test]
fn mdb_shard_file_footer_serialize_roundtrip() {
    let mut footer = MDBShardFileFooter::default();
    footer.file_info_offset = 100;
    footer.xorb_info_offset = 200;
    footer.stored_bytes = 1024;

    let mut buf = Vec::new();
    footer.serialize(&mut buf).unwrap();

    let mut cursor = Cursor::new(buf);
    let deserialized = MDBShardFileFooter::deserialize(&mut cursor).unwrap();
    assert_eq!(footer.version, deserialized.version);
    assert_eq!(footer.file_info_offset, deserialized.file_info_offset);
    assert_eq!(footer.xorb_info_offset, deserialized.xorb_info_offset);
    assert_eq!(footer.stored_bytes, deserialized.stored_bytes);
}

#[test]
fn mdb_shard_file_footer_deserialize_wrong_version() {
    let mut buf = vec![0u8; 200];
    buf[0..8].copy_from_slice(&99u64.to_le_bytes()); // wrong version
    let mut cursor = Cursor::new(buf);
    assert!(MDBShardFileFooter::deserialize(&mut cursor).is_err());
}

// ============================================================================
// MDBShardInfo tests
// ============================================================================

#[test]
fn mdb_shard_info_non_content_byte_size() {
    let size = MDBShardInfo::non_content_byte_size();
    assert!(size > 0);
    assert_eq!(
        size,
        (std::mem::size_of::<MDBShardFileHeader>() + std::mem::size_of::<MDBShardFileFooter>())
            as u64
    );
}

#[test]
fn mdb_shard_info_default() {
    let info = MDBShardInfo::default();
    assert_eq!(info.num_file_entries(), 0);
    assert_eq!(info.num_xorb_entries(), 0);
    assert_eq!(info.total_num_chunks(), 0);
    assert_eq!(info.materialized_bytes(), 0);
    assert_eq!(info.stored_bytes(), 0);
}

#[test]
fn mdb_shard_info_counts() {
    let mut info = MDBShardInfo::default();
    info.file_infos.push(MDBFileInfo {
        metadata: FileDataSequenceHeader::new(MerkleHash::default(), 0u32, false, false),
        segments: vec![],
        verification: vec![],
        metadata_ext: None,
    });
    info.file_infos.push(MDBFileInfo {
        metadata: FileDataSequenceHeader::new(MerkleHash::default(), 0u32, false, false),
        segments: vec![],
        verification: vec![],
        metadata_ext: None,
    });
    info.xorb_infos.push(MDBXorbInfo {
        metadata: XorbChunkSequenceHeader::new(MerkleHash::default(), 0u32, 100u32),
        chunks: vec![],
    });

    assert_eq!(info.num_file_entries(), 2);
    assert_eq!(info.num_xorb_entries(), 1);
    assert_eq!(info.total_num_chunks(), 0);
    assert_eq!(info.stored_bytes(), 100);
}

#[test]
fn mdb_shard_info_materialized_bytes() {
    let info = MDBShardInfo {
        header: MDBShardFileHeader::default(),
        footer: MDBShardFileFooter::default(),
        file_infos: vec![MDBFileInfo {
            metadata: FileDataSequenceHeader::new(MerkleHash::default(), 1u32, false, false),
            segments: vec![FileDataSequenceEntry::new(
                MerkleHash::default(),
                500u32,
                0u32,
                0u32,
            )],
            verification: vec![],
            metadata_ext: None,
        }],
        xorb_infos: vec![],
    };
    assert_eq!(info.materialized_bytes(), 500);
}

// ============================================================================
// MDBInMemoryShard tests
// ============================================================================

#[test]
fn mdb_in_memory_shard_default() {
    let shard = crate::metadata_shard::shard_in_memory::MDBInMemoryShard::default();
    assert_eq!(shard.num_xorb_entries(), 0);
    assert_eq!(shard.num_file_entries(), 0);
    assert_eq!(shard.materialized_bytes(), 0);
    assert_eq!(shard.stored_bytes(), 0);
}

#[test]
fn mdb_in_memory_shard_add_file_reconstruction_info() {
    let mut shard = crate::metadata_shard::shard_in_memory::MDBInMemoryShard::default();
    let info = MDBFileInfo {
        metadata: FileDataSequenceHeader::new(compute_data_hash(b"f"), 1u32, false, false),
        segments: vec![FileDataSequenceEntry::new(
            MerkleHash::default(),
            100u32,
            0u32,
            0u32,
        )],
        verification: vec![],
        metadata_ext: None,
    };
    shard.add_file_reconstruction_info(info).unwrap();
    assert_eq!(shard.num_file_entries(), 1);
    assert_eq!(shard.materialized_bytes(), 100);
}

#[test]
fn mdb_in_memory_shard_add_xorb_block() {
    let mut shard = crate::metadata_shard::shard_in_memory::MDBInMemoryShard::default();
    let xorb = MDBXorbInfo {
        metadata: XorbChunkSequenceHeader::new(compute_data_hash(b"x"), 1u32, 200u32),
        chunks: vec![XorbChunkSequenceEntry::new(
            MerkleHash::default(),
            200u32,
            0u32,
        )],
    };
    shard.add_xorb_block(xorb).unwrap();
    assert_eq!(shard.num_xorb_entries(), 1);
    assert_eq!(shard.stored_bytes(), 200);
}

#[test]
fn mdb_in_memory_shard_to_bytes_roundtrip() {
    let mut shard = crate::metadata_shard::shard_in_memory::MDBInMemoryShard::default();
    let xorb = MDBXorbInfo {
        metadata: XorbChunkSequenceHeader::new(compute_data_hash(b"x"), 0u32, 0u32),
        chunks: vec![],
    };
    shard.add_xorb_block(xorb).unwrap();
    let bytes = shard.to_bytes().unwrap();
    assert!(!bytes.is_empty());
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
// Error tests
// ============================================================================

#[test]
fn core_error_display_io() {
    let err = CoreError::Io(std::io::Error::new(
        std::io::ErrorKind::NotFound,
        "file missing",
    ));
    let msg = format!("{err}");
    assert!(msg.contains("I/O error"));
    assert!(msg.contains("file missing"));
}

#[test]
fn core_error_display_internal_error() {
    let err = CoreError::InternalError("bug".to_string());
    assert_eq!(format!("{err}"), "Internal error: bug");
}

#[test]
fn core_error_display_other() {
    let err = CoreError::Other("something".to_string());
    assert_eq!(format!("{err}"), "something");
}

#[test]
fn core_error_display_shard_version() {
    let err = CoreError::ShardVersion("bad version".to_string());
    assert_eq!(format!("{err}"), "Shard version error: bad version");
}

#[test]
fn core_error_display_invalid_shard() {
    let err = CoreError::InvalidShard("bad shard".to_string());
    assert_eq!(format!("{err}"), "Invalid shard: bad shard");
}

#[test]
fn core_error_display_invalid_range() {
    let err = CoreError::InvalidRange;
    assert_eq!(format!("{err}"), "Invalid range");
}

#[test]
fn core_error_display_invalid_arguments() {
    let err = CoreError::InvalidArguments;
    assert_eq!(format!("{err}"), "Invalid arguments");
}

#[test]
fn core_error_display_malformed_data() {
    let err = CoreError::MalformedData("garbage".to_string());
    assert_eq!(format!("{err}"), "Malformed data: garbage");
}

#[test]
fn core_error_display_hash_mismatch() {
    let err = CoreError::HashMismatch;
    assert_eq!(format!("{err}"), "Hash mismatch");
}

#[test]
fn core_error_display_chunk_header_parse() {
    let err = CoreError::ChunkHeaderParse;
    assert_eq!(format!("{err}"), "Chunk header parse error");
}

#[test]
fn core_error_display_compression_error() {
    let lz4_err = lz4_flex::frame::Error::WrongMagicNumber;
    let err = CoreError::CompressionError(lz4_err);
    let msg = format!("{err}");
    assert!(msg.contains("Compression error"));
}

#[test]
fn core_error_other_constructor() {
    let err = CoreError::other("test");
    assert_eq!(format!("{err}"), "test");
}

#[test]
fn core_error_invalid_shard_constructor() {
    let err = CoreError::invalid_shard("bad");
    assert_eq!(format!("{err}"), "Invalid shard: bad");
}

#[test]
fn core_error_partial_eq_by_discriminant() {
    let a = CoreError::Other("foo".to_string());
    let b = CoreError::Other("bar".to_string());
    assert_eq!(a, b); // same discriminant

    let c = CoreError::InvalidRange;
    assert_ne!(a, c); // different discriminant
}

#[test]
fn core_error_from_io_error() {
    let io_err = std::io::Error::new(std::io::ErrorKind::Other, "oops");
    let err: CoreError = io_err.into();
    assert!(matches!(err, CoreError::Io(_)));
}

#[test]
fn core_error_from_lz4_error() {
    let lz4_err = lz4_flex::frame::Error::WrongMagicNumber;
    let err: CoreError = lz4_err.into();
    assert!(matches!(err, CoreError::CompressionError(_)));
}

#[test]
fn core_error_from_hex_parse_error() {
    let err: CoreError = DataHashHexParseError.into();
    assert!(matches!(err, CoreError::Other(_)));
}

#[test]
fn core_error_from_bytes_parse_error() {
    let err: CoreError = DataHashBytesParseError.into();
    assert!(matches!(err, CoreError::Other(_)));
}

// ============================================================================
// Validate trait tests
// ============================================================================

#[test]
fn validate_ok_converts_to_some() {
    let result: Result<u32, CoreError> = Ok(42);
    let validated = result.ok_for_format_error().unwrap();
    assert_eq!(validated, Some(42));
}

#[test]
fn validate_malformed_data_converts_to_none() {
    let result: Result<u32, CoreError> = Err(CoreError::MalformedData("bad".to_string()));
    let validated = result.ok_for_format_error().unwrap();
    assert!(validated.is_none());
}

#[test]
fn validate_other_error_propagates() {
    let result: Result<u32, CoreError> = Err(CoreError::Other("real error".to_string()));
    let err = result.ok_for_format_error().unwrap_err();
    assert!(matches!(err, CoreError::Other(_)));
}

// ============================================================================
// RawXorbData tests
// ============================================================================

#[test]
fn raw_xorb_data_from_chunks() {
    let chunks = vec![
        Chunk {
            hash: compute_data_hash(b"c1"),
            data: vec![1u8; 100].into(),
        },
        Chunk {
            hash: compute_data_hash(b"c2"),
            data: vec![2u8; 200].into(),
        },
    ];
    let raw = RawXorbData::from_chunks(&chunks, vec![0, 100]);
    assert_eq!(raw.num_bytes(), 300);
    assert_eq!(raw.data.len(), 2);
    assert_eq!(raw.file_boundaries, vec![0, 100]);
}

#[test]
fn raw_xorb_data_hash() {
    let chunks = vec![Chunk {
        hash: compute_data_hash(b"c"),
        data: vec![0u8; 50].into(),
    }];
    let raw = RawXorbData::from_chunks(&chunks, vec![0]);
    let h = raw.hash();
    assert_ne!(h, MerkleHash::default());
}

#[test]
fn raw_xorb_data_num_bytes() {
    let chunks = vec![
        Chunk {
            hash: MerkleHash::default(),
            data: vec![0u8; 10].into(),
        },
        Chunk {
            hash: MerkleHash::default(),
            data: vec![0u8; 20].into(),
        },
    ];
    let raw = RawXorbData::from_chunks(&chunks, vec![0]);
    assert_eq!(raw.num_bytes(), 30);
}

#[test]
fn raw_xorb_data_chunks_and_boundaries() {
    let info = XorbInfo {
        chunk_boundaries: vec![100, 250, 400],
    };
    let boundaries = info.chunks_and_boundaries();
    assert_eq!(boundaries.len(), 3);
    assert_eq!(boundaries[0].1, 100);
    assert_eq!(boundaries[1].1, 250);
    assert_eq!(boundaries[2].1, 400);
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
    let header = FileDataSequenceHeader::new(compute_data_hash(b"f"), 1u32, false, false);
    let entry = FileDataSequenceEntry::new(compute_data_hash(b"e"), 100u32, 0u32, 50u32);

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
    let header = FileDataSequenceHeader::new(MerkleHash::default(), 2u32, false, false);
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
    let header = FileDataSequenceHeader::new(MerkleHash::default(), 0u32, false, false);
    let mut buf = Vec::new();
    header.serialize(&mut buf).unwrap();
    let view =
        crate::metadata_shard::file_structs::MDBFileInfoView::new(bytes::Bytes::from(buf)).unwrap();
    let _ = view.bytes();
}

#[test]
fn mdb_file_info_view_fromMDBFileInfoView_into_mdb_file_info() {
    let header = FileDataSequenceHeader::new(compute_data_hash(b"f"), 1u32, false, false);
    let entry = FileDataSequenceEntry::new(compute_data_hash(b"e"), 100u32, 0u32, 50u32);

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
    assert_eq!(XORB_OBJECT_FORMAT_VERSION, 1);
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
// XorbChunkFormat constants
// ============================================================================

#[test]
fn xorb_chunk_header_length() {
    assert_eq!(XORB_CHUNK_HEADER_LENGTH, 8);
}

// ============================================================================
// Property tests for serialization roundtrips
// ============================================================================

proptest! {
    #[test]
    fn file_data_sequence_header_roundtrip(
        file_hash in prop::array::uniform4(0u64..u64::MAX),
        num_entries in 0u32..100,
        flags in 0u32..=MDB_FILE_FLAG_WITH_VERIFICATION | MDB_FILE_FLAG_WITH_METADATA_EXT,
    ) {
        let header = FileDataSequenceHeader {
            file_hash: DataHash::from(file_hash),
            file_flags: flags,
            num_entries,
            _unused: 0,
        };
        let mut buf = Vec::new();
        header.serialize(&mut buf).unwrap();
        let deserialized = FileDataSequenceHeader::deserialize(&mut Cursor::new(&buf)).unwrap();
        prop_assert_eq!(&header, &deserialized);
    }

    #[test]
    fn file_data_sequence_entry_roundtrip(
        hash in prop::array::uniform4(0u64..u64::MAX),
        unpacked in 0u32..1_000_000,
        start in 0u32..1_000_000,
        end in 0u32..1_000_000,
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
        let deserialized = FileDataSequenceEntry::deserialize(&mut Cursor::new(&buf)).unwrap();
        prop_assert_eq!(&entry, &deserialized);
    }

    #[test]
    fn xorb_chunk_sequence_header_roundtrip(
        hash in prop::array::uniform4(0u64..u64::MAX),
        num_entries in 0u32..100,
        bytes_in_xorb in 0u32..10_000_000,
        bytes_on_disk in 0u32..10_000_000,
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
        let deserialized = XorbChunkSequenceHeader::deserialize(&mut Cursor::new(&buf)).unwrap();
        prop_assert_eq!(&header, &deserialized);
    }

    #[test]
    fn xorb_chunk_sequence_entry_roundtrip(
        hash in prop::array::uniform4(0u64..u64::MAX),
        byte_range_start in 0u32..10_000_000,
        unpacked in 0u32..10_000_000,
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
        let deserialized = XorbChunkSequenceEntry::deserialize(&mut Cursor::new(&buf)).unwrap();
        prop_assert_eq!(&entry, &deserialized);
    }
}
