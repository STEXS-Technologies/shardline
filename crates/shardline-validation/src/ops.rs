use thiserror::Error;

/// Maximum allowed stored file record metadata size in bytes.
pub const MAX_LOCAL_RECORD_METADATA_BYTES: u64 = 1_073_741_824;

/// Checked addition returning an error on overflow.
///
/// # Examples
///
/// ```
/// use shardline_validation::checked_add;
///
/// assert_eq!(checked_add(40, 2)?, 42);
/// assert!(checked_add(u64::MAX, 1).is_err());
/// # Ok::<(), shardline_validation::RebuildOverflowError>(())
/// ```
///
/// # Errors
///
/// Returns [`RebuildOverflowError`] when the addition overflows.
pub const fn checked_add(left: u64, right: u64) -> Result<u64, RebuildOverflowError> {
    match left.checked_add(right) {
        Some(value) => Ok(value),
        None => Err(RebuildOverflowError),
    }
}

/// Checked increment returning an error on overflow.
///
/// # Errors
///
/// Returns [`RebuildOverflowError`] when the increment overflows.
pub const fn checked_increment(value: u64) -> Result<u64, RebuildOverflowError> {
    checked_add(value, 1)
}

/// Arithmetic overflow during rebuild operations.
#[derive(Debug, Clone, Copy, Error)]
#[error("arithmetic overflow")]
pub struct RebuildOverflowError;

/// Returns the current Unix time in seconds, or an error if the system clock
/// is before the Unix epoch.
///
/// # Errors
///
/// Returns [`RebuildOverflowError`] when the system time is before the Unix
/// epoch.
pub fn unix_now_seconds_checked() -> Result<u64, RebuildOverflowError> {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .map_err(|_e| RebuildOverflowError)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn checked_arithmetic_preserves_values_and_rejects_overflow() {
        for (left, right, expected) in [(1, 2, Some(3)), (0, 0, Some(0)), (u64::MAX, 1, None)] {
            assert_eq!(checked_add(left, right).ok(), expected);
        }
        assert_eq!(checked_increment(0).unwrap(), 1);
        assert!(checked_increment(u64::MAX).is_err());
    }

    #[test]
    fn rebuild_overflow_error_display() {
        let msg = RebuildOverflowError.to_string();
        assert_eq!(msg, "arithmetic overflow");
    }

    #[test]
    fn unix_now_seconds_checked_returns_modern_timestamp() {
        let ts = unix_now_seconds_checked().unwrap();
        assert!(ts >= 1_700_000_000, "timestamp {ts} too small");
    }
}
