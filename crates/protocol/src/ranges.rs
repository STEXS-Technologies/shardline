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
#[derive(Debug, Clone, Error)]
pub enum HttpRangeParseError {
    /// The header did not start with the expected unit token.
    #[error("range header must use bytes=<start>-<end> syntax")]
    MissingBytesUnit,
    /// The header contained unsupported or malformed syntax.
    #[error("range header must use bytes=<start>-<end> syntax: {0}")]
    InvalidSyntax(String),
    /// The numeric range could not be parsed.
    #[error("range header contained an invalid number: {0}")]
    InvalidNumber(String),
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
        return Err(HttpRangeParseError::InvalidSyntax(
            "empty range suffix".to_owned(),
        ));
    }
    if raw_suffix.contains(',') {
        return Err(HttpRangeParseError::InvalidSyntax(
            "multi-range not supported".to_owned(),
        ));
    }

    let mut parts = raw_suffix.splitn(2, '-');
    let Some(raw_start) = parts.next() else {
        return Err(HttpRangeParseError::InvalidSyntax(
            "missing range start".to_owned(),
        ));
    };
    let Some(raw_end) = parts.next() else {
        return Err(HttpRangeParseError::InvalidSyntax(
            "missing range end".to_owned(),
        ));
    };
    if raw_start.is_empty() {
        // Suffix range: bytes=-N (last N bytes)
        let suffix_len = raw_end
            .parse::<u64>()
            .map_err(|e| HttpRangeParseError::InvalidNumber(e.to_string()))?;
        if suffix_len == 0 {
            return Err(HttpRangeParseError::InvalidSyntax(
                "suffix length must be non-zero".to_owned(),
            ));
        }
        let start = resource_length.saturating_sub(suffix_len);
        let end = resource_length.saturating_sub(1);
        return ByteRange::new(start, end)
            .map_err(|err| HttpRangeParseError::InvalidSyntax(err.to_string()));
    }

    let start = raw_start
        .parse::<u64>()
        .map_err(|e| HttpRangeParseError::InvalidNumber(e.to_string()))?;
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
            .map_err(|e| HttpRangeParseError::InvalidNumber(e.to_string()))?
    };
    let end_inclusive = parsed_end.min(last_byte);

    ByteRange::new(start, end_inclusive)
        .map_err(|err| HttpRangeParseError::InvalidSyntax(err.to_string()))
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
        assert_eq!(parsed.unwrap(), expected.unwrap());
    }

    #[test]
    fn http_byte_range_clamps_open_or_oversized_end_to_resource() {
        let open_ended = parse_http_byte_range("bytes=10-", 25);
        let oversized = parse_http_byte_range("bytes=10-999", 25);
        let expected = ByteRange::new(10, 24);

        assert!(expected.is_ok());
        let expected = expected.unwrap();
        assert_eq!(open_ended.unwrap(), expected);
        assert_eq!(oversized.unwrap(), expected);
    }

    #[test]
    fn http_byte_range_rejects_invalid_syntax() {
        assert!(matches!(
            parse_http_byte_range("items=0-1", 10),
            Err(HttpRangeParseError::MissingBytesUnit)
        ));
        assert!(matches!(
            parse_http_byte_range("bytes=1-2,4-5", 10),
            Err(HttpRangeParseError::InvalidSyntax(_))
        ));
        assert!(matches!(
            parse_http_byte_range("bytes=2-1", 10),
            Err(HttpRangeParseError::InvalidSyntax(_))
        ));
        assert!(matches!(
            parse_http_byte_range("bytes= 1-2", 10),
            Err(HttpRangeParseError::InvalidNumber(_))
        ));
        assert!(matches!(
            parse_http_byte_range("bytes=1 -2", 10),
            Err(HttpRangeParseError::InvalidNumber(_))
        ));
    }

    #[test]
    fn http_byte_range_accepts_suffix_and_rejects_multi_range_forms() {
        let suffix = parse_http_byte_range("bytes=-1", 10);
        let expected = ByteRange::new(9, 9);
        assert!(expected.is_ok());
        assert_eq!(suffix.unwrap(), expected.unwrap());

        let suffix_large = parse_http_byte_range("bytes=-100", 50);
        let expected_large = ByteRange::new(0, 49);
        assert!(expected_large.is_ok());
        assert_eq!(suffix_large.unwrap(), expected_large.unwrap());

        assert!(matches!(
            parse_http_byte_range("bytes=0-0,1-1", 10),
            Err(HttpRangeParseError::InvalidSyntax(_))
        ));
    }

    #[test]
    fn http_byte_range_rejects_unsatisfiable_start() {
        assert!(matches!(
            parse_http_byte_range("bytes=10-20", 10),
            Err(HttpRangeParseError::Unsatisfiable)
        ));
        assert!(matches!(
            parse_http_byte_range("bytes=0-0", 0),
            Err(HttpRangeParseError::Unsatisfiable)
        ));
    }

    #[test]
    fn http_byte_range_rejects_empty_raw_suffix() {
        assert!(matches!(
            parse_http_byte_range("bytes=", 10),
            Err(HttpRangeParseError::InvalidSyntax(_))
        ));
    }

    #[test]
    fn http_byte_range_rejects_empty_start_no_end() {
        // "bytes=-" hits the suffix branch; empty suffix length fails parse as InvalidNumber
        assert!(matches!(
            parse_http_byte_range("bytes=-", 10),
            Err(HttpRangeParseError::InvalidNumber(_))
        ));
    }

    #[test]
    fn http_byte_range_rejects_suffix_zero() {
        assert!(matches!(
            parse_http_byte_range("bytes=-0", 10),
            Err(HttpRangeParseError::InvalidSyntax(_))
        ));
    }

    #[test]
    fn http_byte_range_suffix_larger_than_resource_clamps_to_start() {
        let result = parse_http_byte_range("bytes=-999", 100);
        let expected = ByteRange::new(0, 99);
        assert!(expected.is_ok());
        assert_eq!(result.unwrap(), expected.unwrap());
    }

    // --- error Display tests ---

    #[test]
    fn range_error_display_inverted() {
        let msg = RangeError::Inverted.to_string();
        assert!(!msg.is_empty());
        assert!(
            msg.contains("smaller"),
            "expected 'smaller' in display, got: {msg}"
        );
    }

    #[test]
    fn range_error_display_empty() {
        let msg = RangeError::Empty.to_string();
        assert!(!msg.is_empty());
        assert!(
            msg.contains("at least one chunk"),
            "expected 'at least one chunk' in display, got: {msg}"
        );
    }

    #[test]
    fn http_range_parse_error_display_all_variants() {
        let cases: &[(HttpRangeParseError, &str)] = &[
            (HttpRangeParseError::MissingBytesUnit, "syntax"),
            (HttpRangeParseError::InvalidSyntax("test".to_owned()), "syntax"),
            (HttpRangeParseError::InvalidNumber("test".to_owned()), "invalid number"),
            (HttpRangeParseError::Unsatisfiable, "satisfiable"),
        ];
        for (error, substring) in cases {
            let msg = error.to_string();
            assert!(!msg.is_empty(), "empty display for {error:?}");
            assert!(
                msg.contains(substring),
                "expected '{substring}' in '{msg}' from {error:?}"
            );
        }
    }

    // ── Additional ByteRange edge cases ──────────────────────────────────

    #[test]
    fn byte_range_single_byte() {
        let range = ByteRange::new(5, 5).unwrap();
        assert_eq!(range.start(), 5);
        assert_eq!(range.end_inclusive(), 5);
        assert_eq!(range.len(), Some(1));
        assert!(!range.is_empty());
    }

    #[test]
    fn byte_range_zero_length_range() {
        // Start == end is valid — covers 1 byte
        let range = ByteRange::new(0, 0).unwrap();
        assert_eq!(range.len(), Some(1));
    }

    #[test]
    fn byte_range_u64_max_start() {
        let range = ByteRange::new(u64::MAX, u64::MAX);
        assert!(range.is_ok());
        let range = range.unwrap();
        assert_eq!(range.len(), Some(1));
    }

    #[test]
    fn byte_range_clone_copy_consistency() {
        let range = ByteRange::new(10, 20).unwrap();
        let cloned = range;
        assert_eq!(range, cloned);
        assert_eq!(range.start(), cloned.start());
        assert_eq!(range.end_inclusive(), cloned.end_inclusive());
    }

    // ── Additional ChunkRange edge cases ─────────────────────────────────

    #[test]
    fn chunk_range_single_chunk() {
        let range = ChunkRange::new(3, 4).unwrap();
        assert_eq!(range.start(), 3);
        assert_eq!(range.end_exclusive(), 4);
    }

    #[test]
    fn chunk_range_u32_bounds() {
        let range = ChunkRange::new(0, u32::MAX).unwrap();
        assert_eq!(range.start(), 0);
        assert_eq!(range.end_exclusive(), u32::MAX);
    }

    #[test]
    fn chunk_range_clone_copy_consistency() {
        let range = ChunkRange::new(1, 5).unwrap();
        let cloned = range;
        assert_eq!(range, cloned);
        assert_eq!(range.start(), cloned.start());
    }

    // ── Additional parse_http_byte_range edge cases ──────────────────────

    #[test]
    fn http_byte_range_exact_resource_length_start_zero() {
        // Start=0 with resource_length=1 -> byte 0
        let result = parse_http_byte_range("bytes=0-0", 1).unwrap();
        assert_eq!(result.start(), 0);
        assert_eq!(result.end_inclusive(), 0);
    }

    #[test]
    fn http_byte_range_rejects_start_equals_resource_length() {
        // start == resource_length is unsatisfiable
        let result = parse_http_byte_range("bytes=5-10", 5);
        assert!(matches!(result, Err(HttpRangeParseError::Unsatisfiable)));
    }

    #[test]
    fn http_byte_range_zero_resource_length() {
        // resource_length=0 -> any range is unsatisfiable
        let result = parse_http_byte_range("bytes=0-0", 0);
        assert!(matches!(result, Err(HttpRangeParseError::Unsatisfiable)));
    }

    #[test]
    fn http_byte_range_suffix_zero_resource_length() {
        // resource_length=0, suffix "-0" -> no bytes available
        let result = parse_http_byte_range("bytes=-0", 0);
        assert!(matches!(result, Err(HttpRangeParseError::InvalidSyntax(_))));
    }

    #[test]
    fn http_byte_range_huge_numbers() {
        let result = parse_http_byte_range("bytes=99999999999999999999-100000000000000000000", 100);
        assert!(matches!(result, Err(HttpRangeParseError::InvalidNumber(_))));
    }

    // ── RangeError derive tests ──────────────────────────────────────────

    #[test]
    fn range_error_clone_copy_partial_eq() {
        let a = RangeError::Inverted;
        let b = RangeError::Empty;
        let a2 = a;
        assert_eq!(a, a2);
        assert_ne!(a, b);
    }

    #[test]
    fn http_range_parse_error_debug_non_empty() {
        let err = HttpRangeParseError::InvalidSyntax("test".to_owned());
        let debug = format!("{err:?}");
        assert!(!debug.is_empty());
    }
}
