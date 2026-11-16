use std::borrow::Borrow;

use shardline_protocol::{HashParseError, ShardlineHash};

const XET_HASH_GROUP_BYTES: usize = 8;

/// Parses the reordered lowercase hash text used by Xet API paths and persisted Xet metadata.
///
/// # Errors
///
/// Returns [`HashParseError`] when the string has the wrong length, contains
/// non-lowercase hexadecimal characters, or cannot be decoded into 32 bytes.
pub fn parse_xet_hash_hex(value: &str) -> Result<ShardlineHash, HashParseError> {
    let mut bytes = *ShardlineHash::parse_hex(value)?.as_bytes();
    for chunk in bytes.as_chunks_mut::<XET_HASH_GROUP_BYTES>().0 {
        chunk.reverse();
    }

    Ok(ShardlineHash::from_bytes(bytes))
}

/// Returns the reordered lowercase hash text used by Xet API paths and persisted Xet metadata.
#[must_use]
pub fn xet_hash_hex_string(hash: impl Borrow<ShardlineHash>) -> String {
    let mut bytes = *hash.borrow().as_bytes();
    for chunk in bytes.as_chunks_mut::<XET_HASH_GROUP_BYTES>().0 {
        chunk.reverse();
    }
    hex::encode(bytes)
}

#[cfg(test)]
mod tests {
    use shardline_protocol::ShardlineHash;

    use super::{parse_xet_hash_hex, xet_hash_hex_string};

    #[test]
    fn xet_hash_hex_uses_xet_byte_group_ordering() {
        let hash = ShardlineHash::from_bytes([
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
    fn parser_rejects_wrong_lengths_and_noncanonical_characters() {
        for value in [String::new(), "abc".to_owned(), "a".repeat(65)] {
            assert!(
                matches!(
                    parse_xet_hash_hex(&value),
                    Err(shardline_protocol::HashParseError::InvalidLength)
                ),
                "{value:?}"
            );
        }
        for value in [
            "A".repeat(64),
            format!("{}z{}", "a".repeat(32), "b".repeat(31)),
        ] {
            assert!(
                matches!(
                    parse_xet_hash_hex(&value),
                    Err(shardline_protocol::HashParseError::InvalidCharacter(_))
                ),
                "{value:?}"
            );
        }
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
            let hash = ShardlineHash::from_bytes(bytes);

            assert_eq!(xet_hash_hex_string(hash), xet_hex);
            assert!(matches!(parse_xet_hash_hex(xet_hex), Ok(h) if h == hash));
        }
    }
}
