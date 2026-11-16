use serde::{Deserialize, Serialize};
use thiserror::Error;

const HASH_BYTE_LENGTH: usize = 32;
const HASH_HEX_LENGTH: usize = 64;

/// A 32-byte protocol hash.
///
/// # Examples
///
/// ```
/// use shardline_protocol::ShardlineHash;
///
/// let hash = ShardlineHash::from_bytes([1; 32]);
/// assert_eq!(hash.as_bytes(), &[1; 32]);
///
/// // The canonical text form round-trips.
/// let hex = hash.hex_string();
/// assert_eq!(ShardlineHash::parse_hex(&hex)?, hash);
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
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

    /// Parses a hash from canonical lowercase hexadecimal text.
    ///
    /// # Examples
    ///
    /// ```
    /// use shardline_protocol::ShardlineHash;
    ///
    /// let hex = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
    /// let hash = ShardlineHash::parse_hex(hex)?;
    /// assert_eq!(hash.hex_string(), hex);
    ///
    /// // Wrong-length or non-lowercase input is rejected.
    /// assert!(ShardlineHash::parse_hex("ABC").is_err());
    /// assert!(ShardlineHash::parse_hex("not-a-hash").is_err());
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Returns [`HashParseError`] when the string has the wrong length, contains
    /// non-lowercase hexadecimal characters, or cannot be decoded into 32 bytes.
    pub fn parse_hex(value: &str) -> Result<Self, HashParseError> {
        if value.len() != HASH_HEX_LENGTH {
            return Err(HashParseError::InvalidLength);
        }

        if !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
        {
            return Err(HashParseError::InvalidCharacter(
                "non-lowercase hexadecimal character".to_owned(),
            ));
        }

        let mut bytes = [0; HASH_BYTE_LENGTH];
        hex::decode_to_slice(value, &mut bytes)
            .map_err(|error| HashParseError::InvalidCharacter(error.to_string()))?;

        Ok(Self { bytes })
    }

    /// Returns canonical lowercase hexadecimal text.
    #[must_use]
    pub fn hex_string(&self) -> String {
        hex::encode(self.bytes)
    }
}

/// Hash parsing failure.
#[derive(Debug, Clone, Error)]
pub enum HashParseError {
    /// The hash string did not contain exactly 64 hexadecimal characters.
    #[error("hash must contain exactly 64 lowercase hexadecimal characters")]
    InvalidLength,
    /// The hash string contained a character outside lowercase hexadecimal, or hex decoding failed.
    #[error("invalid hash character: {0}")]
    InvalidCharacter(String),
}

#[cfg(test)]
mod tests {
    use super::{HashParseError, ShardlineHash};

    #[test]
    fn canonical_hash_vectors_round_trip() {
        let cases = [
            (
                std::array::from_fn(|index| u8::try_from(index).unwrap()),
                "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f",
            ),
            (
                [
                    0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xaa, 0xbb, 0xcc,
                    0xdd, 0xee, 0xff, 0x10, 0x32, 0x54, 0x76, 0x98, 0xba, 0xdc, 0xfe, 0x01, 0x23,
                    0x45, 0x67, 0x89, 0xab, 0xcd, 0xef,
                ],
                "00112233445566778899aabbccddeeff1032547698badcfe0123456789abcdef",
            ),
            (
                [
                    0xff, 0xee, 0xdd, 0xcc, 0xbb, 0xaa, 0x99, 0x88, 0x77, 0x66, 0x55, 0x44, 0x33,
                    0x22, 0x11, 0x00, 0xef, 0xcd, 0xab, 0x89, 0x67, 0x45, 0x23, 0x01, 0xfe, 0xdc,
                    0xba, 0x98, 0x76, 0x54, 0x32, 0x10,
                ],
                "ffeeddccbbaa99887766554433221100efcdab8967452301fedcba9876543210",
            ),
        ];

        for (bytes, hex) in cases {
            let hash = ShardlineHash::from_bytes(bytes);

            assert_eq!(hash.as_bytes(), &bytes);
            assert_eq!(hash.hex_string(), hex);
            let parsed = ShardlineHash::parse_hex(hex);
            assert_eq!(parsed.unwrap(), hash);
        }
    }

    #[test]
    fn canonical_hash_rejects_invalid_length_and_characters() {
        for value in [
            "abc".to_owned(),
            "a".repeat(66),
            format!("{}zz", "a".repeat(64)),
        ] {
            assert!(
                matches!(
                    ShardlineHash::parse_hex(&value),
                    Err(HashParseError::InvalidLength)
                ),
                "{value:?}"
            );
        }
        for value in [
            ShardlineHash::from_bytes([31; 32])
                .hex_string()
                .replacen('f', "F", 1),
            "z".repeat(64),
            format!("A{}", "a".repeat(63)),
            format!("{}F{}", "a".repeat(32), "a".repeat(31)),
        ] {
            assert!(
                matches!(
                    ShardlineHash::parse_hex(&value),
                    Err(HashParseError::InvalidCharacter(_))
                ),
                "{value:?}"
            );
        }
    }

    #[test]
    fn hash_from_bytes_as_bytes_roundtrip() {
        let cases = [
            [0u8; 32],
            [1u8; 32],
            [9u8; 32],
            [0xffu8; 32],
            [
                0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0, 0x01, 0x23, 0x45, 0x67, 0x89, 0xab,
                0xcd, 0xef, 0xfe, 0xdc, 0xba, 0x98, 0x76, 0x54, 0x32, 0x10, 0x00, 0x11, 0x22, 0x33,
                0x44, 0x55, 0x66, 0x77,
            ],
        ];
        for bytes in cases {
            let hash = ShardlineHash::from_bytes(bytes);
            assert_eq!(
                hash.as_bytes(),
                &bytes,
                "from_bytes/as_bytes roundtrip failed for {bytes:?}"
            );
        }
    }

    #[test]
    fn hash_parse_errors_preserve_messages() {
        for (error, expected) in [
            (
                HashParseError::InvalidLength,
                "hash must contain exactly 64 lowercase hexadecimal characters",
            ),
            (
                HashParseError::InvalidCharacter("bad char".to_owned()),
                "invalid hash character: bad char",
            ),
        ] {
            assert_eq!(error.to_string(), expected);
        }
    }
}
