use std::time::{SystemTime, UNIX_EPOCH};

use crate::ServerError;

pub(crate) fn unix_now_seconds_checked() -> Result<u64, ServerError> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .map_err(|_error| ServerError::Overflow)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unix_now_seconds_checked_returns_reasonable_value() {
        let result = unix_now_seconds_checked();
        assert!(result.is_ok());
        let seconds = result.unwrap();
        // Sanity: should be >= 2023 (1700000000) and not absurdly far in the future.
        assert!(
            seconds >= 1_700_000_000,
            "unix timestamp seems unreasonably low: {seconds}"
        );
        assert!(
            seconds <= 2_000_000_000,
            "unix timestamp seems unreasonably high: {seconds}"
        );
    }
}
