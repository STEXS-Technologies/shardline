//! Automatic history capacity is observation policy, never execution authority.
use super::TaskEvidenceStore;
use crate::ServerError;

pub(super) const AUTOMATIC_HISTORY_CAPACITY: u64 = 4096;

impl TaskEvidenceStore {
    /// Observes optional recording capacity, independently of server readiness.
    /// A later publisher still checks admission atomically; this reserves no slot.
    pub(crate) fn history_notice(&self) -> Result<Option<&'static str>, ServerError> {
        let full = if let Some(metadata) = &self.postgres_metadata {
            metadata.admission_full(AUTOMATIC_HISTORY_CAPACITY)?
        } else if let Some(metadata) = &self.local_metadata {
            metadata.task_history_admission_full(AUTOMATIC_HISTORY_CAPACITY)?
        } else {
            return Ok(Some(
                "New task history is unavailable. Existing records may still be listed; an empty list does not prove maintenance did not run.",
            ));
        };
        Ok(full.then_some("Task history is full. Existing evidence is preserved; new maintenance can still run, but some runs may not be recorded. Verified history cleanup continues after maintenance. This notice does not describe server readiness."))
    }
}

pub(super) fn automatic_publication_result(published: bool) -> Result<(), ServerError> {
    if published {
        Ok(())
    } else {
        Err(ServerError::Io(std::io::Error::other(
            "automatic task history admission unavailable: retained history is full, the manifest is missing, or the artifact exceeds 64 KiB; existing history is preserved",
        )))
    }
}
