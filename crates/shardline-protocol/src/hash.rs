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

        let decoded =
            hex::decode(value).map_err(|e| HashParseError::InvalidCharacter(e.to_string()))?;
        let bytes = <[u8; HASH_BYTE_LENGTH]>::try_from(decoded).map_err(|vec| {
            let _ = vec;
            HashParseError::InvalidLength
        })?;

        Ok(Self { bytes })
    }

    /// Returns canonical lowercase hexadecimal text.
    #[must_use]
    pub fn hex_string(&self) -> String {
        let mut encoded = Vec::with_capacity(HASH_HEX_LENGTH);
        for byte in self.bytes {
            append_lower_hex_byte(&mut encoded, byte);
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
    fn canonical_hash_hex_round_trips_raw_bytes() {
        let hash = ShardlineHash::from_bytes([
            0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23,
            24, 25, 26, 27, 28, 29, 30, 31,
        ]);

        let hex = hash.hex_string();

        assert_eq!(
            hex,
            "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f"
        );
        let parsed = ShardlineHash::parse_hex(&hex);
        assert_eq!(parsed.unwrap(), hash);
    }

    #[test]
    fn canonical_hash_vectors_round_trip() {
        let cases = [
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

            assert_eq!(hash.hex_string(), hex);
            let parsed = ShardlineHash::parse_hex(hex);
            assert_eq!(parsed.unwrap(), hash);
        }
    }

    #[test]
    fn canonical_hash_rejects_uppercase_hex() {
        let hash = ShardlineHash::from_bytes([31; 32]);
        let invalid = hash.hex_string().replacen('f', "F", 1);
        let result = ShardlineHash::parse_hex(&invalid);

        assert!(matches!(result, Err(HashParseError::InvalidCharacter(_))));
    }

    #[test]
    fn canonical_hash_rejects_short_hex() {
        let result = ShardlineHash::parse_hex("abc");

        assert!(matches!(result, Err(HashParseError::InvalidLength)));
    }

    #[test]
    fn canonical_hash_rejects_long_hex() {
        let result = ShardlineHash::parse_hex(
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        );

        assert!(matches!(result, Err(HashParseError::InvalidLength)));
    }

    #[test]
    fn canonical_hash_rejects_non_hex_character() {
        let result = ShardlineHash::parse_hex(
            "zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz",
        );

        assert!(matches!(result, Err(HashParseError::InvalidCharacter(_))));
    }

    #[test]
    fn raw_bytes_are_preserved() {
        let bytes = [9; 32];
        let hash = ShardlineHash::from_bytes(bytes);

        assert_eq!(hash.as_bytes(), &bytes);
    }

    #[test]
    fn hash_from_bytes_as_bytes_roundtrip() {
        let cases = [
            [0u8; 32],
            [1u8; 32],
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
    fn hash_parse_error_display_invalid_length() {
        let error = HashParseError::InvalidLength;
        let msg = error.to_string();
        assert!(!msg.is_empty());
        assert!(
            msg.contains("64"),
            "expected mention of 64 in display, got: {msg}"
        );
        assert!(
            msg.contains("lowercase"),
            "expected mention of lowercase in display, got: {msg}"
        );
    }

    #[test]
    fn hash_parse_error_display_invalid_character() {
        let error = HashParseError::InvalidCharacter("bad char".to_owned());
        let msg = error.to_string();
        assert!(!msg.is_empty());
        assert!(
            msg.contains("bad char"),
            "expected 'bad char' in display, got: {msg}"
        );
    }

    #[test]
    fn hash_copy_and_clone_work() {
        let hash = ShardlineHash::from_bytes([42; 32]);
        let cloned = hash;
        assert_eq!(hash, cloned);
        let copied = hash;
        assert_eq!(hash, copied);
    }

    #[test]
    fn hash_partial_eq_compares_bytes() {
        let a = ShardlineHash::from_bytes([1; 32]);
        let b = ShardlineHash::from_bytes([1; 32]);
        let c = ShardlineHash::from_bytes([2; 32]);
        assert_eq!(a, b);
        assert_ne!(a, c);
    }

    #[test]
    fn hash_parse_hex_rejects_garbage_after_valid_prefix() {
        let result = ShardlineHash::parse_hex(&format!("{}zz", "a".repeat(64)));
        assert!(matches!(result, Err(HashParseError::InvalidLength)));
    }

    #[test]
    fn hash_hex_string_all_byte_values() {
        let mut bytes = [0u8; 32];
        for (i, byte) in bytes.iter_mut().enumerate() {
            *byte = i as u8;
        }
        let hash = ShardlineHash::from_bytes(bytes);
        let hex = hash.hex_string();
        let expected = (0..32u8)
            .map(|b| format!("{b:02x}"))
            .collect::<Vec<_>>()
            .concat();
        assert_eq!(hex, expected);
    }

    #[test]
    fn hash_parse_hex_rejects_uppercase_at_start() {
        // Exactly 64 chars, first char uppercase => InvalidCharacter
        let mixed = format!("A{}", "a".repeat(63));
        let result = ShardlineHash::parse_hex(&mixed);
        assert!(matches!(result, Err(HashParseError::InvalidCharacter(_))));
    }

    #[test]
    fn hash_parse_hex_rejects_uppercase_in_middle() {
        // Exactly 64 chars, one char in middle uppercase => InvalidCharacter
        let mut bytes = "a".repeat(32);
        bytes.push('F');
        bytes.push_str(&"a".repeat(31));
        assert_eq!(bytes.len(), 64);
        let result = ShardlineHash::parse_hex(&bytes);
        assert!(matches!(result, Err(HashParseError::InvalidCharacter(_))));
    }

    #[test]
    fn hash_error_derive_impls() {
        // HashParseError implements Clone
        let _a = HashParseError::InvalidLength;
        let _b = HashParseError::InvalidCharacter("msg".to_owned());
    }
}
