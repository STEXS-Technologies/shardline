//! Startup workers must finish while the runtime driving their I/O is alive.
use crate::ServerError;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use tokio::task::JoinHandle;

/// Shutdown ownership for blocking startup maintenance in an embedded router.
/// Stop accepting requests and drop the router before calling `shutdown`, and
/// keep the Tokio runtime alive until it returns. Normal `serve` does this
/// automatically.
#[must_use = "Drain startup workers before shutting down their I/O runtime"]
pub struct RouterBackgroundTasks {
    stopped: Arc<AtomicBool>,
    workers: Vec<JoinHandle<()>>,
}

impl RouterBackgroundTasks {
    pub(super) const fn new(stopped: Arc<AtomicBool>) -> Self {
        Self {
            stopped,
            workers: Vec::new(),
        }
    }

    pub(super) fn register(&mut self, worker: JoinHandle<()>) {
        self.workers.push(worker);
    }

    pub(super) fn stopped_flag(&self) -> Arc<AtomicBool> {
        self.stopped.clone()
    }

    /// Stops optional housekeeping and waits for every admitted startup worker.
    /// Blocking work is not aborted: an in-flight durable update must settle
    /// while its database and timer runtime remain available.
    ///
    /// # Errors
    /// Returns a worker failure after all workers have been joined.
    pub async fn shutdown(self) -> Result<(), ServerError> {
        self.stopped.store(true, Ordering::Relaxed);
        let mut first_failure = None;
        for worker in self.workers {
            if let Err(error) = worker.await {
                first_failure.get_or_insert_with(|| ServerError::Io(std::io::Error::other(error)));
            }
        }
        first_failure.map_or(Ok(()), Err)
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;

    #[tokio::test]
    async fn shutdown_waits_for_inflight_blocking_work_without_aborting_it() {
        let stopped = Arc::new(AtomicBool::new(false));
        let mut background = RouterBackgroundTasks::new(stopped.clone());
        let (release, wait) = tokio::sync::oneshot::channel();
        let finished = Arc::new(AtomicBool::new(false));
        let worker_finished = finished.clone();
        background.register(tokio::task::spawn_blocking(move || {
            wait.blocking_recv().unwrap();
            worker_finished.store(true, Ordering::SeqCst);
        }));
        let shutdown = background.shutdown();
        tokio::pin!(shutdown);
        assert!(futures_util::poll!(&mut shutdown).is_pending());
        assert!(stopped.load(Ordering::SeqCst));
        assert!(!finished.load(Ordering::SeqCst));
        release.send(()).unwrap();
        shutdown.await.unwrap();
        assert!(finished.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn failed_worker_does_not_detach_later_workers() {
        let mut background = RouterBackgroundTasks::new(Arc::new(AtomicBool::new(false)));
        let failed = tokio::spawn(std::future::pending());
        failed.abort();
        background.register(failed);
        let (release, wait) = tokio::sync::oneshot::channel();
        background.register(tokio::task::spawn_blocking(move || {
            wait.blocking_recv().unwrap();
        }));
        let shutdown = background.shutdown();
        tokio::pin!(shutdown);
        assert!(futures_util::poll!(&mut shutdown).is_pending());
        release.send(()).unwrap();
        assert!(shutdown.await.is_err());
    }
}
