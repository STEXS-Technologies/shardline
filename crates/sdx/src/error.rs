use thiserror::Error;

/// Failure to parse a 64-character Xet CAS API hexadecimal hash string.
#[derive(Debug, Clone, Error)]
pub enum XetHashParseError {
    /// The hash string did not contain exactly 64 hexadecimal characters.
    #[error("hash must contain exactly 64 lowercase hexadecimal characters")]
    InvalidLength,
    /// The hash string contained a character outside lowercase hexadecimal, or hex decoding failed.
    #[error("invalid hash character: {0}")]
    InvalidCharacter(String),
}
