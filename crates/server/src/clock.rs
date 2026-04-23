use std::time::{SystemTime, UNIX_EPOCH};

use crate::ServerError;

pub(crate) fn unix_now_seconds_checked() -> Result<u64, ServerError> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .map_err(|_error| ServerError::Overflow)
}
