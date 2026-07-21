use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Returns the current Unix timestamp in seconds.
///
/// If the system clock reports a time before the Unix epoch, this returns `0`.
#[must_use]
pub fn unix_now_seconds_lossy() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::ZERO)
        .as_secs()
}

#[cfg(test)]
mod tests {
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn returns_modern_timestamp() {
        let ts = super::unix_now_seconds_lossy();
        assert!(
            ts >= 1_700_000_000,
            "timestamp {ts} is before year 2023; system clock may be wrong"
        );
    }

    #[test]
    fn not_in_the_future() {
        let ts = super::unix_now_seconds_lossy();
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        // Allow a small delta in case the clock ticks between the two calls
        assert!(
            ts <= now,
            "timestamp {ts} should not exceed current time {now}"
        );
    }
}
