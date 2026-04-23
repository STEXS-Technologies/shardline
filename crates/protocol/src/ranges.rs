use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Inclusive byte range.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ByteRange {
    start: u64,
    end_inclusive: u64,
}

impl ByteRange {
    /// Creates an inclusive byte range.
    ///
    /// # Errors
    ///
    /// Returns [`RangeError::Inverted`] when `end_inclusive` is smaller than `start`.
    pub const fn new(start: u64, end_inclusive: u64) -> Result<Self, RangeError> {
        if end_inclusive < start {
            return Err(RangeError::Inverted);
        }

        Ok(Self {
            start,
            end_inclusive,
        })
    }

    /// Returns the first byte offset in the range.
    #[must_use]
    pub const fn start(&self) -> u64 {
        self.start
    }

    /// Returns the inclusive final byte offset in the range.
    #[must_use]
    pub const fn end_inclusive(&self) -> u64 {
        self.end_inclusive
    }

    /// Returns the number of bytes in the range.
    #[must_use]
    pub const fn len(&self) -> Option<u64> {
        match self.end_inclusive.checked_sub(self.start) {
            Some(offset) => offset.checked_add(1),
            None => None,
        }
    }

    /// Returns false because validated inclusive byte ranges always contain at least one byte.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        false
    }
}

/// End-exclusive chunk index range.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChunkRange {
    start: u32,
    end_exclusive: u32,
}

impl ChunkRange {
    /// Creates an end-exclusive chunk index range.
    ///
    /// # Errors
    ///
    /// Returns [`RangeError::Empty`] when `end_exclusive` is equal to `start`.
    /// Returns [`RangeError::Inverted`] when `end_exclusive` is smaller than `start`.
    pub const fn new(start: u32, end_exclusive: u32) -> Result<Self, RangeError> {
        if end_exclusive < start {
            return Err(RangeError::Inverted);
        }

        if end_exclusive == start {
            return Err(RangeError::Empty);
        }

        Ok(Self {
            start,
            end_exclusive,
        })
    }

    /// Returns the first chunk index in the range.
    #[must_use]
    pub const fn start(self) -> u32 {
        self.start
    }

    /// Returns the end-exclusive chunk index.
    #[must_use]
    pub const fn end_exclusive(self) -> u32 {
        self.end_exclusive
    }
}

/// Range construction failure.
#[derive(Debug, Clone, Copy, Error, PartialEq, Eq)]
pub enum RangeError {
    /// The range end was smaller than the range start.
    #[error("range end must not be smaller than range start")]
    Inverted,
    /// The range contained no chunks.
    #[error("chunk range must contain at least one chunk")]
    Empty,
}

/// Reconstruction request range parse failure.
#[derive(Debug, Clone, Error, PartialEq, Eq)]
pub enum HttpRangeParseError {
    /// The header did not start with the expected unit token.
    #[error("range header must use bytes=<start>-<end> syntax")]
    MissingBytesUnit,
    /// The header contained unsupported or malformed syntax.
    #[error("range header must use bytes=<start>-<end> syntax")]
    InvalidSyntax,
    /// The numeric range could not be parsed.
    #[error("range header contained an invalid number")]
    InvalidNumber,
    /// The requested start exceeded the represented resource length.
    #[error("requested range is not satisfiable")]
    Unsatisfiable,
}

/// Parses a reconstruction `Range` header into an inclusive byte range.
///
/// The Xet reconstruction API uses `bytes=<start>-<end>` syntax with an inclusive end.
/// When the requested end exceeds the resource length, the returned range is clamped to
/// the last byte of the resource.
///
/// # Errors
///
/// Returns [`HttpRangeParseError::InvalidSyntax`] when the header uses unsupported
/// syntax, [`HttpRangeParseError::InvalidNumber`] when parsing fails, and
/// [`HttpRangeParseError::Unsatisfiable`] when the requested start exceeds the last byte
/// of the resource.
pub fn parse_http_byte_range(
    value: &str,
    resource_length: u64,
) -> Result<ByteRange, HttpRangeParseError> {
    let Some(raw_suffix) = value.strip_prefix("bytes=") else {
        return Err(HttpRangeParseError::MissingBytesUnit);
    };
    if raw_suffix.is_empty() {
        return Err(HttpRangeParseError::InvalidSyntax);
    }
    if raw_suffix.contains(',') {
        return Err(HttpRangeParseError::InvalidSyntax);
    }

    let mut parts = raw_suffix.splitn(2, '-');
    let Some(raw_start) = parts.next() else {
        return Err(HttpRangeParseError::InvalidSyntax);
    };
    let Some(raw_end) = parts.next() else {
        return Err(HttpRangeParseError::InvalidSyntax);
    };
    if raw_start.is_empty() {
        return Err(HttpRangeParseError::InvalidSyntax);
    }

    let start = raw_start
        .parse::<u64>()
        .map_err(|_error| HttpRangeParseError::InvalidNumber)?;
    if start >= resource_length {
        return Err(HttpRangeParseError::Unsatisfiable);
    }

    let last_byte = resource_length
        .checked_sub(1)
        .ok_or(HttpRangeParseError::Unsatisfiable)?;
    let parsed_end = if raw_end.is_empty() {
        last_byte
    } else {
        raw_end
            .parse::<u64>()
            .map_err(|_error| HttpRangeParseError::InvalidNumber)?
    };
    let end_inclusive = parsed_end.min(last_byte);

    ByteRange::new(start, end_inclusive).map_err(|_error| HttpRangeParseError::InvalidSyntax)
}

#[cfg(test)]
mod tests {
    use super::{ByteRange, ChunkRange, HttpRangeParseError, RangeError, parse_http_byte_range};

    #[test]
    fn byte_range_is_inclusive() {
        let range = ByteRange::new(10, 20);

        assert!(range.is_ok());
        if let Ok(value) = range {
            assert_eq!(value.start(), 10);
            assert_eq!(value.end_inclusive(), 20);
            assert_eq!(value.len(), Some(11));
            assert!(!value.is_empty());
        }
    }

    #[test]
    fn byte_range_rejects_inverted_input() {
        let range = ByteRange::new(20, 10);

        assert_eq!(range, Err(RangeError::Inverted));
    }

    #[test]
    fn byte_range_reports_unrepresentable_full_u64_length() {
        let range = ByteRange::new(0, u64::MAX);

        assert!(range.is_ok());
        if let Ok(value) = range {
            assert_eq!(value.len(), None);
        }
    }

    #[test]
    fn chunk_range_rejects_empty_ranges() {
        let range = ChunkRange::new(4, 4);

        assert_eq!(range, Err(RangeError::Empty));
    }

    #[test]
    fn chunk_range_rejects_inverted_ranges() {
        let range = ChunkRange::new(5, 4);

        assert_eq!(range, Err(RangeError::Inverted));
    }

    #[test]
    fn chunk_range_is_end_exclusive() {
        let range = ChunkRange::new(4, 9);

        assert!(range.is_ok());
        if let Ok(value) = range {
            assert_eq!(value.start(), 4);
            assert_eq!(value.end_exclusive(), 9);
        }
    }

    #[test]
    fn http_byte_range_parses_inclusive_range() {
        let parsed = parse_http_byte_range("bytes=10-20", 100);
        let expected = ByteRange::new(10, 20);

        assert!(expected.is_ok());
        assert_eq!(
            parsed,
            expected.map_err(|_error| HttpRangeParseError::InvalidSyntax)
        );
    }

    #[test]
    fn http_byte_range_clamps_open_or_oversized_end_to_resource() {
        let open_ended = parse_http_byte_range("bytes=10-", 25);
        let oversized = parse_http_byte_range("bytes=10-999", 25);
        let expected = ByteRange::new(10, 24);

        assert!(expected.is_ok());
        assert_eq!(
            open_ended,
            expected.map_err(|_error| HttpRangeParseError::InvalidSyntax)
        );
        assert_eq!(
            oversized,
            expected.map_err(|_error| HttpRangeParseError::InvalidSyntax)
        );
    }

    #[test]
    fn http_byte_range_accepts_single_byte_and_final_byte_vectors() {
        let first_byte = parse_http_byte_range("bytes=0-0", 10);
        let final_byte = parse_http_byte_range("bytes=9-9", 10);

        assert_eq!(
            first_byte,
            ByteRange::new(0, 0).map_err(|_error| HttpRangeParseError::InvalidSyntax)
        );
        assert_eq!(
            final_byte,
            ByteRange::new(9, 9).map_err(|_error| HttpRangeParseError::InvalidSyntax)
        );
    }

    #[test]
    fn http_byte_range_rejects_invalid_syntax() {
        assert_eq!(
            parse_http_byte_range("items=0-1", 10),
            Err(HttpRangeParseError::MissingBytesUnit)
        );
        assert_eq!(
            parse_http_byte_range("bytes=-10", 10),
            Err(HttpRangeParseError::InvalidSyntax)
        );
        assert_eq!(
            parse_http_byte_range("bytes=1-2,4-5", 10),
            Err(HttpRangeParseError::InvalidSyntax)
        );
        assert_eq!(
            parse_http_byte_range("bytes=2-1", 10),
            Err(HttpRangeParseError::InvalidSyntax)
        );
        assert_eq!(
            parse_http_byte_range("bytes= 1-2", 10),
            Err(HttpRangeParseError::InvalidNumber)
        );
        assert_eq!(
            parse_http_byte_range("bytes=1 -2", 10),
            Err(HttpRangeParseError::InvalidNumber)
        );
    }

    #[test]
    fn http_byte_range_rejects_suffix_and_multi_range_forms() {
        assert_eq!(
            parse_http_byte_range("bytes=-1", 10),
            Err(HttpRangeParseError::InvalidSyntax)
        );
        assert_eq!(
            parse_http_byte_range("bytes=0-0,1-1", 10),
            Err(HttpRangeParseError::InvalidSyntax)
        );
    }

    #[test]
    fn http_byte_range_rejects_unsatisfiable_start() {
        assert_eq!(
            parse_http_byte_range("bytes=10-20", 10),
            Err(HttpRangeParseError::Unsatisfiable)
        );
        assert_eq!(
            parse_http_byte_range("bytes=0-0", 0),
            Err(HttpRangeParseError::Unsatisfiable)
        );
    }
}
