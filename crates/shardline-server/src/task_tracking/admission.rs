//! Automatic history capacity is observation policy, never execution authority.
use crate::ServerError;

pub(super) const AUTOMATIC_HISTORY_CAPACITY: u64 = 4096;

pub(super) fn automatic_publication_result(published: bool) -> Result<(), ServerError> {
    if published {
        Ok(())
    } else {
        Err(ServerError::Io(std::io::Error::other(
            "automatic task history admission unavailable: retained history is full, the manifest is missing, or the artifact exceeds 64 KiB; existing history is preserved",
        )))
    }
}
