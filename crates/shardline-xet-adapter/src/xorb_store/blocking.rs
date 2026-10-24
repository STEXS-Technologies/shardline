//! Cancellation ownership for native file persistence queued on Tokio's pool.

use crate::XetAdapterError;

struct CancelQueuedWrite(tokio::task::AbortHandle);

impl Drop for CancelQueuedWrite {
    fn drop(&mut self) {
        // Tokio aborts a queued blocking job, but cannot interrupt one which
        // already started synchronous I/O. Its result remains unacknowledged
        // and the upload intent must be reconciled through the normal path.
        self.0.abort();
    }
}

pub(super) async fn cancellable_write<R: Send + 'static, T: Send + 'static>(
    resources: R,
    work: impl FnOnce() -> Result<T, XetAdapterError> + Send + 'static,
) -> Result<T, XetAdapterError> {
    let job = tokio::task::spawn_blocking(move || {
        let _resources = resources;
        work()
    });
    let _cancellation = CancelQueuedWrite(job.abort_handle());
    job.await
        .map_err(|error| XetAdapterError::Io(std::io::Error::other(error)))?
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        future::Future,
        sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        },
        task::{Context, Waker},
    };

    #[test]
    fn cancelled_queued_native_file_write_never_runs() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .max_blocking_threads(1)
            .build()
            .unwrap();
        runtime.block_on(async {
            let (entered, started) = tokio::sync::oneshot::channel();
            let (finish, wait) = std::sync::mpsc::channel();
            let occupied = tokio::task::spawn_blocking(move || {
                entered.send(()).unwrap();
                wait.recv().unwrap();
            });
            started.await.unwrap();
            let wrote = Arc::new(AtomicBool::new(false));
            let observed = Arc::clone(&wrote);
            let mut queued = Box::pin(cancellable_write((), move || {
                observed.store(true, Ordering::SeqCst);
                Ok(())
            }));
            let mut context = Context::from_waker(Waker::noop());
            assert!(queued.as_mut().poll(&mut context).is_pending());
            drop(queued);
            finish.send(()).unwrap();
            occupied.await.unwrap();
            // Drain the single blocking worker after the cancelled job. This
            // removes scheduler timing from the negative side-effect check.
            tokio::task::spawn_blocking(|| ()).await.unwrap();
            assert!(!wrote.load(Ordering::SeqCst), "cancelled queued write ran");
        });
    }

    #[test]
    fn cancellation_does_not_pretend_to_stop_running_synchronous_writes() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .max_blocking_threads(1)
            .build()
            .unwrap();
        runtime.block_on(async {
            let (entered, started) = tokio::sync::oneshot::channel();
            let (finish, wait) = std::sync::mpsc::channel();
            let wrote = Arc::new(AtomicBool::new(false));
            let observed = Arc::clone(&wrote);
            let mut running = Box::pin(cancellable_write((), move || {
                entered.send(()).unwrap();
                wait.recv().unwrap();
                observed.store(true, Ordering::SeqCst);
                Ok(())
            }));
            let mut context = Context::from_waker(Waker::noop());
            assert!(running.as_mut().poll(&mut context).is_pending());
            started.await.unwrap();
            drop(running);
            assert!(!wrote.load(Ordering::SeqCst));
            finish.send(()).unwrap();
            tokio::task::spawn_blocking(|| ()).await.unwrap();
            assert!(
                wrote.load(Ordering::SeqCst),
                "started write was still running"
            );
        });
    }

    #[test]
    fn running_write_retains_its_file_and_admission_after_caller_cancellation() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .max_blocking_threads(1)
            .build()
            .unwrap();
        runtime.block_on(async {
            let admission = Arc::new(tokio::sync::Semaphore::new(1));
            let permit = admission.clone().acquire_owned().await.unwrap();
            let file = tempfile::NamedTempFile::new().unwrap();
            let path = file.path().to_owned();
            let (entered, started) = tokio::sync::oneshot::channel();
            let (finish, wait) = std::sync::mpsc::channel();
            let mut running = Box::pin(cancellable_write((file, permit), move || {
                entered.send(()).unwrap();
                wait.recv().unwrap();
                Ok(())
            }));
            let mut context = Context::from_waker(Waker::noop());
            assert!(running.as_mut().poll(&mut context).is_pending());
            started.await.unwrap();
            drop(running);
            assert!(path.exists(), "running write lost its source file");
            assert!(
                admission.try_acquire().is_err(),
                "running work lost admission"
            );
            finish.send(()).unwrap();
            tokio::task::spawn_blocking(|| ()).await.unwrap();
            assert!(!path.exists(), "finished write leaked its source file");
            assert_eq!(admission.available_permits(), 1);
        });
    }
}
