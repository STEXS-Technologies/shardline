use std::io::Write;

use shardline_xet_core::merklehash::aggregated_hashes::{
    file_hash, file_hash_with_salt, xorb_hash,
};
use shardline_xet_core::merklehash::data_hash::DataHash;
use shardline_xet_core::merklehash::{
    HashedWrite, MerkleHash, compute_data_hash, compute_internal_node_hash,
};

// ============================================================================
// End-to-end hashing: compute data hash → compute xorb hash → build merkle
// tree → verify hash consistency across different input sizes.
// ============================================================================

/// Data hash is consistent for the same input
#[test]
fn data_hash_consistency() {
    let inputs: Vec<&[u8]> = vec![
        b"",
        b"a",
        b"hello world",
        b"the quick brown fox jumps over the lazy dog",
        &[0u8; 1024],
        &[0xFFu8; 4096],
    ];

    for input in &inputs {
        let h1 = compute_data_hash(input);
        let h2 = compute_data_hash(input);
        assert_eq!(
            h1,
            h2,
            "data hash should be deterministic for input len={}",
            input.len()
        );
        if !input.is_empty() {
            assert_ne!(h1, MerkleHash::default(), "data should not hash to zero");
        }
    }
}

/// Different inputs produce different hashes
#[test]
fn data_hash_different_inputs() {
    assert_ne!(compute_data_hash(b"hello"), compute_data_hash(b"world"));
    assert_ne!(compute_data_hash(b"a"), compute_data_hash(b"aa"));
    assert_ne!(compute_data_hash(b""), compute_data_hash(b" "));
}

/// Internal node hash is distinct from data hash for same content
#[test]
fn internal_node_hash_vs_data_hash() {
    let inputs: Vec<&[u8]> = vec![b"", b"test", b"some data to hash", &[0u8; 256]];

    for input in &inputs {
        let dh = compute_data_hash(input);
        let ih = compute_internal_node_hash(input);
        assert_ne!(
            dh,
            ih,
            "internal node hash should differ from data hash for input len={}",
            input.len(),
        );
    }
}

/// Internal node hash is deterministic
#[test]
fn internal_node_hash_consistency() {
    let data = b"internal node test data";
    let h1 = compute_internal_node_hash(data);
    let h2 = compute_internal_node_hash(data);
    assert_eq!(h1, h2);
}

/// Xorb hash over real chunk hashes
#[test]
fn xorb_hash_with_real_data() {
    let chunks_data: Vec<&[u8]> = vec![
        b"first chunk content",
        b"second chunk with different data",
        b"third chunk data here",
        b"",
    ];

    let mut pairs = Vec::new();
    for chunk in &chunks_data {
        let hash = compute_data_hash(chunk);
        pairs.push((hash, chunk.len() as u64));
    }

    let xh = xorb_hash(&pairs);
    assert_ne!(xh, MerkleHash::default());

    // Verify determinism
    assert_eq!(xorb_hash(&pairs), xorb_hash(&pairs));

    // Order matters
    let mut reversed = pairs.clone();
    reversed.reverse();
    assert_ne!(
        xorb_hash(&pairs),
        xorb_hash(&reversed),
        "xorb hash should be order-sensitive"
    );
}

/// Xorb hash with various numbers of chunks
#[test]
fn xorb_hash_different_chunk_counts() {
    for &num_chunks in &[1usize, 2, 3, 4, 5, 8, 9, 12, 20] {
        let mut pairs = Vec::with_capacity(num_chunks);
        for i in 0..num_chunks {
            let data = vec![i as u8; 32];
            let hash = compute_data_hash(&data);
            pairs.push((hash, data.len() as u64));
        }
        let xh = xorb_hash(&pairs);
        assert_ne!(
            xh,
            MerkleHash::default(),
            "xorb hash should not be zero for {num_chunks} chunks"
        );
    }
}

/// Xorb hash with zero chunks produces default hash
#[test]
fn xorb_hash_empty() {
    assert_eq!(xorb_hash(&[]), MerkleHash::default());
}

/// File hash with real data
#[test]
fn file_hash_with_real_data() {
    let chunks_data: Vec<&[u8]> = vec![b"segment1", b"segment2", b"segment3"];

    let mut pairs = Vec::new();
    for chunk in &chunks_data {
        let hash = compute_data_hash(chunk);
        pairs.push((hash, chunk.len() as u64));
    }

    let fh = file_hash(&pairs);
    assert_ne!(fh, MerkleHash::default());

    // File hash differs from xorb hash for same data
    let xh = xorb_hash(&pairs);
    assert_ne!(fh, xh, "file hash should differ from xorb hash");
}

/// File hash with salt produces different results for different salts
#[test]
fn file_hash_with_salt_variants() {
    let chunks_data: Vec<&[u8]> = vec![b"data1", b"data2"];
    let mut pairs = Vec::new();
    for chunk in &chunks_data {
        let hash = compute_data_hash(chunk);
        pairs.push((hash, chunk.len() as u64));
    }

    let default_salt = [0u8; 32];
    let salt_a = [0xABu8; 32];
    let salt_b = [0xCDu8; 32];

    let fh_default = file_hash_with_salt(&pairs, &default_salt);
    let fh_a = file_hash_with_salt(&pairs, &salt_a);
    let fh_b = file_hash_with_salt(&pairs, &salt_b);

    assert_ne!(
        fh_default, fh_a,
        "different salts should produce different hashes"
    );
    assert_ne!(
        fh_a, fh_b,
        "different salts should produce different hashes"
    );
    assert_eq!(
        file_hash_with_salt(&pairs, &salt_a),
        file_hash_with_salt(&pairs, &salt_a)
    );
}

/// File hash with empty chunks returns default
#[test]
fn file_hash_empty() {
    assert_eq!(file_hash(&[]), MerkleHash::default());
    assert_eq!(
        file_hash_with_salt(&[], &[0xFFu8; 32]),
        MerkleHash::default(),
    );
}

/// HashedWrite end-to-end: compute hash while writing
#[test]
fn hashed_write_end_to_end() {
    let data = b"This is test data for HashedWrite integration";
    let mut writer = HashedWrite::new(Vec::new());
    writer.write_all(data).unwrap();
    let hash = writer.hash();
    let expected = compute_data_hash(data);
    assert_eq!(hash, expected);

    let inner = writer.into_inner();
    assert_eq!(inner, data);
}

/// HashedWrite with multiple writes
#[test]
fn hashed_write_multiple_writes() {
    let mut writer = HashedWrite::new(Vec::new());
    writer.write_all(b"part1").unwrap();
    writer.write_all(b"part2").unwrap();
    writer.write_all(b"part3").unwrap();
    writer.flush().unwrap();

    let combined = b"part1part2part3";
    assert_eq!(writer.hash(), compute_data_hash(combined));
    assert_eq!(writer.into_inner(), combined);
}

/// DataHash hex roundtrip for various values
#[test]
fn data_hash_hex_roundtrip_various() {
    let values = [
        [0u64, 0, 0, 0],
        [1u64, 2, 3, 4],
        [u64::MAX, u64::MAX, u64::MAX, u64::MAX],
        [0xDEADBEEF, 0xCAFEBABE, 0x12345678, 0x9ABCDEF0],
    ];

    for arr in &values {
        let h = DataHash::from(*arr);
        let hex = h.hex();
        assert_eq!(hex.len(), 64);
        let h2 = DataHash::from_hex(&hex).unwrap();
        assert_eq!(h, h2, "hex roundtrip failed for {arr:?}");
    }
}

/// DataHash from_slice with valid and invalid lengths
#[test]
fn data_hash_from_slice_various() {
    // Valid
    let valid = vec![0x42u8; 32];
    let h = DataHash::from_slice(&valid).unwrap();
    let back: [u8; 32] = h.into();
    assert_eq!(&valid, &back);

    // Invalid lengths
    for &len in &[0usize, 1, 31, 33, 100] {
        let bytes = vec![0u8; len];
        assert!(
            DataHash::from_slice(&bytes).is_err(),
            "should reject length {len}"
        );
    }
}

/// DataHash default is all zeros
#[test]
fn data_hash_default_is_zero() {
    assert_eq!(*DataHash::default(), [0u64; 4]);
}

/// DataHash ordering
#[test]
fn data_hash_ordering() {
    let small = DataHash::from([1u64, 0, 0, 0]);
    let medium = DataHash::from([2u64, 0, 0, 0]);
    let large = DataHash::from([3u64, 0, 0, 0]);

    assert!(small < medium);
    assert!(medium < large);
    assert!(large > small);
    assert_eq!(small, small);
}

/// DataHash Display produces hex
#[test]
fn data_hash_display_is_hex() {
    let h = DataHash::from([0xDEADBEEFu64, 0xCAFEBABE, 0x12345678, 0x9ABCDEF0]);
    let display = format!("{h}");
    assert_eq!(display.len(), 64);
    assert!(display.chars().all(|c| c.is_ascii_hexdigit()));
}

/// MerkleHash type alias matches DataHash
#[test]
fn merkle_hash_type_alias() {
    let h: MerkleHash = DataHash::from([42u64; 4]);
    assert_eq!(*h, [42u64; 4]);
}

/// Hash consistency across variable-sized data using xorb_hash tree
#[test]
fn cross_size_hash_consistency() {
    // Compute individual chunk hashes
    let chunks: Vec<Vec<u8>> = vec![
        vec![0u8; 64],
        vec![1u8; 128],
        vec![2u8; 256],
        vec![3u8; 512],
    ];

    let pairs: Vec<_> = chunks
        .iter()
        .map(|c| (compute_data_hash(c), c.len() as u64))
        .collect();

    let xh = xorb_hash(&pairs);
    assert_ne!(xh, MerkleHash::default());

    // If we merge the data and compute a single hash, it should differ
    let merged: Vec<u8> = chunks.iter().flat_map(|c| c.iter().copied()).collect();
    let merged_hash = compute_data_hash(&merged);
    assert_ne!(
        xh, merged_hash,
        "xorb hash should differ from hash of concatenated data"
    );
}
