//! Cooperative cancellation of optional history housekeeping with router lifetime.
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

#[derive(Debug)]
pub(crate) struct HousekeepingGuard {
    stopped: Arc<AtomicBool>,
}

impl HousekeepingGuard {
    pub(crate) fn new() -> (Self, Arc<AtomicBool>) {
        let stopped = Arc::new(AtomicBool::new(false));
        (
            Self {
                stopped: stopped.clone(),
            },
            stopped,
        )
    }
}

impl Drop for HousekeepingGuard {
    fn drop(&mut self) {
        self.stopped.store(true, Ordering::Relaxed);
    }
}

impl super::TaskEvidenceStore {
    pub(crate) fn with_housekeeping_stop(mut self, stopped: Arc<AtomicBool>) -> Self {
        self.housekeeping_stop = Some(stopped);
        self
    }

    pub(super) fn check_housekeeping(&self) -> Result<(), crate::ServerError> {
        if self
            .housekeeping_stop
            .as_ref()
            .is_some_and(|stopped| stopped.load(Ordering::Relaxed))
        {
            return Err(crate::ServerError::Io(std::io::Error::new(
                std::io::ErrorKind::Interrupted,
                "history housekeeping stopped with router",
            )));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;
