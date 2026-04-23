use serde::{Deserialize, Serialize};
use thiserror::Error;

const HASH_BYTE_LENGTH: usize = 32;
const HASH_API_HEX_LENGTH: usize = 64;
const HASH_API_GROUP_BYTES: usize = 8;

/// A 32-byte protocol hash.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ShardlineHash {
    bytes: [u8; HASH_BYTE_LENGTH],
}

impl ShardlineHash {
    /// Creates a hash from raw bytes.
    #[must_use]
    pub const fn from_bytes(bytes: [u8; HASH_BYTE_LENGTH]) -> Self {
        Self { bytes }
    }

    /// Returns the raw hash bytes.
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; HASH_BYTE_LENGTH] {
        &self.bytes
    }

    /// Parses a hash from the hexadecimal representation used by Xet CAS API paths.
    ///
    /// # Errors
    ///
    /// Returns [`HashParseError`] when the string has the wrong length, contains
    /// non-lowercase hexadecimal characters, or cannot be decoded into 32 bytes.
    pub fn parse_api_hex(value: &str) -> Result<Self, HashParseError> {
        if value.len() != HASH_API_HEX_LENGTH {
            return Err(HashParseError::InvalidLength);
        }

        if !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
        {
            return Err(HashParseError::InvalidCharacter);
        }

        let decoded = hex::decode(value).map_err(|_error| HashParseError::InvalidCharacter)?;
        let reordered = decoded
            .chunks_exact(HASH_API_GROUP_BYTES)
            .flat_map(|chunk| chunk.iter().rev().copied())
            .collect::<Vec<u8>>();
        let bytes = <[u8; HASH_BYTE_LENGTH]>::try_from(reordered)
            .map_err(|_error| HashParseError::InvalidLength)?;

        Ok(Self { bytes })
    }

    /// Returns the hexadecimal representation used by Xet CAS API paths.
    #[must_use]
    pub fn api_hex_string(&self) -> String {
        let mut encoded = Vec::with_capacity(HASH_API_HEX_LENGTH);
        for chunk in self.bytes.chunks_exact(HASH_API_GROUP_BYTES) {
            for byte in chunk.iter().rev() {
                append_lower_hex_byte(&mut encoded, *byte);
            }
        }

        String::from_utf8(encoded).unwrap_or_default()
    }
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

/// Hash parsing failure.
#[derive(Debug, Clone, Copy, Error, PartialEq, Eq)]
pub enum HashParseError {
    /// The hash string did not contain exactly 64 hexadecimal characters.
    #[error("hash must contain exactly 64 lowercase hexadecimal characters")]
    InvalidLength,
    /// The hash string contained a character outside lowercase hexadecimal.
    #[error("hash must use lowercase hexadecimal characters only")]
    InvalidCharacter,
}

#[cfg(test)]
mod tests {
    use super::{HashParseError, ShardlineHash};

    #[test]
    fn api_hash_hex_uses_xet_byte_group_ordering() {
        let hash = ShardlineHash::from_bytes([
            0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23,
            24, 25, 26, 27, 28, 29, 30, 31,
        ]);

        let api_hex = hash.api_hex_string();

        assert_eq!(
            api_hex,
            "07060504030201000f0e0d0c0b0a090817161514131211101f1e1d1c1b1a1918"
        );
        assert_eq!(ShardlineHash::parse_api_hex(&api_hex), Ok(hash));
    }

    #[test]
    fn api_hash_vectors_round_trip_each_xet_byte_group_independently() {
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

        for (bytes, api_hex) in cases {
            let hash = ShardlineHash::from_bytes(bytes);

            assert_eq!(hash.api_hex_string(), api_hex);
            assert_eq!(ShardlineHash::parse_api_hex(api_hex), Ok(hash));
        }
    }

    #[test]
    fn api_hash_rejects_uppercase_hex() {
        let hash = ShardlineHash::from_bytes([31; 32]);
        let invalid = hash.api_hex_string().replacen('f', "F", 1);
        let result = ShardlineHash::parse_api_hex(&invalid);

        assert_eq!(result, Err(HashParseError::InvalidCharacter));
    }

    #[test]
    fn api_hash_rejects_short_hex() {
        let result = ShardlineHash::parse_api_hex("abc");

        assert_eq!(result, Err(HashParseError::InvalidLength));
    }

    #[test]
    fn api_hash_rejects_long_hex() {
        let result = ShardlineHash::parse_api_hex(
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        );

        assert_eq!(result, Err(HashParseError::InvalidLength));
    }

    #[test]
    fn api_hash_rejects_non_hex_character() {
        let result = ShardlineHash::parse_api_hex(
            "zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz",
        );

        assert_eq!(result, Err(HashParseError::InvalidCharacter));
    }

    #[test]
    fn raw_bytes_are_preserved() {
        let bytes = [9; 32];
        let hash = ShardlineHash::from_bytes(bytes);

        assert_eq!(hash.as_bytes(), &bytes);
    }
}
