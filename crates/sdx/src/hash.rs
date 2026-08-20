//! BLAKE3 keyed hashing and Xet CAS API hexadecimal conversion.
//!
//! The Xet frontend addresses content by 64 lowercase hexadecimal characters in
//! which each 8-byte group of the raw 32-byte digest is byte-reversed ("Xet CAS
//! API hexadecimal ordering"). This module mirrors the wire contract in
//! `shardline-index/src/xet_hash.rs` (`parse_xet_hash_hex` /
//! `xet_hash_hex_string`, `docs/SDX_PLAN.md` §7 item 1) and delegates the
//! keyed-hash primitives to the pinned upstream `xet-core-structures` crate
//! (`docs/SDX_PLAN.md` §4.1 / §11 Q3/Q6 — Merkle hash primitives are reused,
//! not reimplemented).
//!
//! The hash type is upstream [`xet_core_structures::merklehash::MerkleHash`]
//! (a `[u64; 4]`-backed 256-bit value) so that sdx's chunk hashes are directly
//! comparable with the reference client stack and the server's
//! `shardline-xet-core`.

use xet_core_structures::merklehash::MerkleHash;
use xet_core_structures::merklehash::compute_data_hash;
use xet_core_structures::metadata_shard::chunk_verification::range_hash_from_chunks;

use crate::error::XetHashParseError;

/// Number of hexadecimal characters in a Xet CAS API hash string.
const HASH_HEX_LENGTH: usize = 64;
/// Bytes per group reversed by the Xet CAS API hexadecimal ordering.
const XET_HASH_GROUP_BYTES: usize = 8;

/// Parses the reordered lowercase hash text used by Xet API paths and persisted
/// Xet metadata.
///
/// Mirrors `shardline-index`'s `parse_xet_hash_hex` exactly: the input must be
/// exactly 64 lowercase hexadecimal characters, and each 8-byte group is
/// reversed to recover the raw digest bytes.
///
/// # Errors
///
/// Returns [`XetHashParseError`] when the string has the wrong length or
/// contains a character outside lowercase hexadecimal.
pub fn parse_xet_hash_hex(value: &str) -> Result<MerkleHash, XetHashParseError> {
    if value.len() != HASH_HEX_LENGTH {
        return Err(XetHashParseError::InvalidLength);
    }

    if !value
        .bytes()
        .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
    {
        return Err(XetHashParseError::InvalidCharacter(
            "non-lowercase hexadecimal character".to_owned(),
        ));
    }

    let decoded =
        hex::decode(value).map_err(|e| XetHashParseError::InvalidCharacter(e.to_string()))?;
    let reordered = decoded
        .as_chunks::<XET_HASH_GROUP_BYTES>()
        .0
        .iter()
        .flat_map(|chunk| chunk.iter().rev().copied())
        .collect::<Vec<u8>>();
    let bytes = <[u8; 32]>::try_from(reordered).map_err(|vec| {
        let _ = vec;
        XetHashParseError::InvalidLength
    })?;

    Ok(MerkleHash::from(bytes))
}

/// Returns the reordered lowercase hash text used by Xet API paths and persisted
/// Xet metadata.
///
/// Equivalent to `MerkleHash::hex()` from the reference `xet-core-structures`
/// crate, which formats each 8-byte group little-endian (byte-reversed). This is
/// the canonical path encoding for Xet CAS API routes such as
/// `/v1/chunks/default/{hash}`.
#[must_use]
pub fn xet_hash_hex_string(hash: MerkleHash) -> String {
    let bytes: [u8; 32] = hash.into();
    let mut encoded = Vec::with_capacity(HASH_HEX_LENGTH);
    for chunk in bytes.as_chunks::<XET_HASH_GROUP_BYTES>().0 {
        for byte in chunk.iter().rev() {
            append_lower_hex_byte(&mut encoded, *byte);
        }
    }

    String::from_utf8(encoded).unwrap_or_default()
}

/// Computes the BLAKE3 keyed chunk hash (`DATA_KEY`) for a chunk's raw bytes.
///
/// Delegates to the pinned upstream `xet-core-structures` crate's
/// `compute_data_hash` (keyed with the same `DATA_KEY` that the server's
/// `shardline-xet-core` mirrors byte-identically) rather than reimplementing
/// the keyed hash.
#[must_use]
pub fn compute_chunk_hash(data: &[u8]) -> MerkleHash {
    compute_data_hash(data)
}

/// Computes the term-verification hash (`VERIFICATION_KEY`) over the
/// concatenated raw chunk-hash bytes of a term.
///
/// Delegates to the pinned upstream `xet-core-structures` crate's
/// `metadata_shard::chunk_verification::range_hash_from_chunks`, which
/// BLAKE3-keyed hashes the concatenation of each chunk's raw bytes with
/// `VERIFICATION_KEY` (`docs/SDX_PLAN.md` §7 item 11).
#[must_use]
pub fn compute_term_verification_hash(chunks: &[MerkleHash]) -> MerkleHash {
    range_hash_from_chunks(chunks)
}

fn append_lower_hex_byte(output: &mut Vec<u8>, byte: u8) {
    output.push(lower_hex_digit(byte >> 4));
    output.push(lower_hex_digit(byte & 0x0f));
}

const fn lower_hex_digit(nibble: u8) -> u8 {
    match nibble {
        0 => b'0',
        1 => b'1',
        2 => b'2',
        3 => b'3',
        4 => b'4',
        5 => b'5',
        6 => b'6',
        7 => b'7',
        8 => b'8',
        9 => b'9',
        10 => b'a',
        11 => b'b',
        12 => b'c',
        13 => b'd',
        14 => b'e',
        _ => b'f',
    }
}

#[cfg(test)]
mod tests {
    use xet_core_structures::merklehash::MerkleHash;

    use super::{
        compute_chunk_hash, compute_term_verification_hash, parse_xet_hash_hex, xet_hash_hex_string,
    };

    #[test]
    fn xet_hash_hex_uses_xet_byte_group_ordering() {
        let hash = MerkleHash::from([
            0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23,
            24, 25, 26, 27, 28, 29, 30, 31,
        ]);

        let xet_hex = xet_hash_hex_string(hash);

        assert_eq!(
            xet_hex,
            "07060504030201000f0e0d0c0b0a090817161514131211101f1e1d1c1b1a1918"
        );
        assert!(matches!(parse_xet_hash_hex(&xet_hex), Ok(h) if h == hash));
    }

    #[test]
    fn parse_xet_hash_hex_rejects_short_string() {
        assert!(matches!(
            parse_xet_hash_hex("abc"),
            Err(crate::error::XetHashParseError::InvalidLength)
        ));
    }

    #[test]
    fn parse_xet_hash_hex_rejects_long_string() {
        assert!(matches!(
            parse_xet_hash_hex(&"a".repeat(65)),
            Err(crate::error::XetHashParseError::InvalidLength)
        ));
    }

    #[test]
    fn parse_xet_hash_hex_rejects_uppercase() {
        let hex = "A".repeat(64);
        assert!(matches!(
            parse_xet_hash_hex(&hex),
            Err(crate::error::XetHashParseError::InvalidCharacter(_))
        ));
    }

    #[test]
    fn parse_xet_hash_hex_rejects_non_hex_characters() {
        let hex = format!("{}z{}", "a".repeat(32), "b".repeat(31));
        assert!(matches!(
            parse_xet_hash_hex(&hex),
            Err(crate::error::XetHashParseError::InvalidCharacter(_))
        ));
    }

    #[test]
    fn parse_xet_hash_hex_rejects_empty_string() {
        assert!(matches!(
            parse_xet_hash_hex(""),
            Err(crate::error::XetHashParseError::InvalidLength)
        ));
    }

    #[test]
    fn xet_hash_vectors_round_trip_each_xet_byte_group_independently() {
        let cases = [
            (
                [
                    0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xaa, 0xbb, 0xcc,
                    0xdd, 0xee, 0xff, 0x10, 0x32, 0x54, 0x76, 0x98, 0xba, 0xdc, 0xfe, 0x01, 0x23,
                    0x45, 0x67, 0x89, 0xab, 0xcd, 0xef,
                ],
                "7766554433221100ffeeddccbbaa9988fedcba9876543210efcdab8967452301",
            ),
            (
                [
                    0xff, 0xee, 0xdd, 0xcc, 0xbb, 0xaa, 0x99, 0x88, 0x77, 0x66, 0x55, 0x44, 0x33,
                    0x22, 0x11, 0x00, 0xef, 0xcd, 0xab, 0x89, 0x67, 0x45, 0x23, 0x01, 0xfe, 0xdc,
                    0xba, 0x98, 0x76, 0x54, 0x32, 0x10,
                ],
                "8899aabbccddeeff00112233445566770123456789abcdef1032547698badcfe",
            ),
        ];

        for (bytes, xet_hex) in cases {
            let hash = MerkleHash::from(bytes);

            assert_eq!(xet_hash_hex_string(hash), xet_hex);
            assert!(matches!(parse_xet_hash_hex(xet_hex), Ok(h) if h == hash));
        }
    }

    #[test]
    fn xet_hex_matches_upstream_hex_endianness_vector() {
        // Golden vector from xet-core-structures 1.5.2 `data_hash.rs`
        // `test_hash_hex_string_endianness`. Upstream `DataHash::hex()` output
        // is exactly the Xet CAS API ordering, so the raw bytes and the
        // expected string must round-trip through sdx's formatter/parser and
        // agree with the upstream `hex()` method.
        let raw: [u8; 32] = [
            22, 175, 58, 132, 4, 75, 131, 214, 190, 153, 138, 66, 226, 3, 153, 242, 204, 86, 80,
            234, 249, 153, 80, 99, 159, 80, 65, 138, 236, 231, 149, 78,
        ];
        let expected = "d6834b04843aaf16f29903e2428a99be635099f9ea5056cc4e95e7ec8a41509f";

        let hash = MerkleHash::from(raw);
        assert_eq!(xet_hash_hex_string(hash), expected);
        assert_eq!(hash.hex(), expected);
        assert!(matches!(parse_xet_hash_hex(expected), Ok(h) if h == hash));
    }

    #[test]
    fn chunk_hash_matches_shardline_xet_core_golden_digests() {
        // Byte-level golden digests for BLAKE3 keyed with DATA_KEY — the
        // constant shared byte-identically by shardline-xet-core
        // (`merklehash/data_hash.rs:214`) and the pinned xet-core-structures
        // 1.5.2 (`merklehash/data_hash.rs:288`). `compute_chunk_hash` delegates
        // to the upstream crate, so these pin the wire-level digest.
        let cases: [(&[u8], [u8; 32]); 3] = [
            (
                b"",
                [
                    0x10, 0x5f, 0x7e, 0x4e, 0x78, 0xcf, 0xf2, 0xe0, 0x5f, 0x9a, 0x0e, 0x15, 0xaf,
                    0x84, 0x4f, 0xc3, 0x15, 0xd9, 0xba, 0xde, 0x16, 0x42, 0x66, 0xf9, 0x67, 0x0f,
                    0x87, 0x49, 0x10, 0x74, 0x4d, 0x36,
                ],
            ),
            (
                b"hello",
                [
                    0x0b, 0x05, 0x98, 0xd9, 0x12, 0xba, 0x76, 0x90, 0x3e, 0x94, 0x0f, 0x77, 0xbf,
                    0xc8, 0x95, 0x9c, 0xb7, 0x82, 0x0a, 0x49, 0xc7, 0x55, 0xad, 0x6c, 0xc2, 0xc2,
                    0x05, 0x2a, 0x2a, 0x58, 0xf1, 0x6c,
                ],
            ),
            (
                b"the quick brown fox jumps over the lazy dog",
                [
                    0xb2, 0xbf, 0xcb, 0xc5, 0xc1, 0x0e, 0x7b, 0x76, 0x30, 0xba, 0x4c, 0xc9, 0x44,
                    0x62, 0xf4, 0x63, 0x37, 0xb0, 0x11, 0x68, 0x44, 0x9a, 0x14, 0x39, 0x07, 0xbc,
                    0x03, 0x28, 0xec, 0x4f, 0xae, 0x2a,
                ],
            ),
        ];

        for (data, expected) in cases {
            let digest = compute_chunk_hash(data);
            let bytes: [u8; 32] = digest.into();
            assert_eq!(bytes, expected);
            assert!(matches!(
                parse_xet_hash_hex(&xet_hash_hex_string(digest)),
                Ok(h) if h == digest
            ));
        }
    }

    #[test]
    fn chunk_hash_changes_with_input_and_is_not_default() {
        assert_eq!(compute_chunk_hash(b"hello"), compute_chunk_hash(b"hello"));
        assert_ne!(compute_chunk_hash(b"hello"), compute_chunk_hash(b"world"));
        assert_ne!(compute_chunk_hash(b""), MerkleHash::default());
    }

    #[test]
    fn term_verification_hash_matches_upstream_golden_digest() {
        // VERIFICATION_KEY (`xet-core-structures` 1.5.2
        // `metadata_shard/chunk_verification.rs:4`) over the concatenation of
        // raw chunk-hash bytes. Golden digest for a single all-0x01 chunk hash.
        let chunks = [MerkleHash::from([1u8; 32])];
        let digest = compute_term_verification_hash(&chunks);
        let bytes: [u8; 32] = digest.into();
        assert_eq!(
            bytes,
            [
                0x46, 0xc0, 0x68, 0x58, 0x17, 0xab, 0x60, 0x5f, 0x51, 0x65, 0xe5, 0x32, 0x19, 0xc7,
                0x35, 0x8d, 0x4b, 0x99, 0xa6, 0xc7, 0xd9, 0x51, 0x90, 0xa6, 0x60, 0xf2, 0x17, 0x3b,
                0x01, 0x80, 0x86, 0x5c,
            ]
        );
    }
}
