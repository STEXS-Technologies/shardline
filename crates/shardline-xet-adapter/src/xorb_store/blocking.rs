//! Cancellation ownership for native file persistence queued on Tokio's pool.

use crate::XetAdapterError;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

struct CancelQueuedWrite {
    abort: tokio::task::AbortHandle,
    cancelled: Arc<AtomicBool>,
}

impl Drop for CancelQueuedWrite {
    fn drop(&mut self) {
        // Tokio aborts a queued blocking job, but cannot interrupt one which
        // already started synchronous I/O. Its result remains unacknowledged
        // and the upload intent must be reconciled through the normal path.
        self.cancelled.store(true, Ordering::Release);
        self.abort.abort();
    }
}

pub(super) async fn cancellable_write<R: Send + 'static, T: Send + 'static>(
    resources: R,
    work: impl FnOnce() -> Result<T, XetAdapterError> + Send + 'static,
) -> Result<T, XetAdapterError> {
    cancellable_read(resources, move |_| work()).await
}

/// Read-only work can stop between reads; started immutable writes retain
/// their existing completion/reconciliation semantics through cancellable_write.
pub(super) async fn cancellable_read<R: Send + 'static, T: Send + 'static>(
    resources: R,
    work: impl FnOnce(&dyn Fn() -> std::io::Result<()>) -> Result<T, XetAdapterError> + Send + 'static,
) -> Result<T, XetAdapterError> {
    let cancelled = Arc::new(AtomicBool::new(false));
    let observed = Arc::clone(&cancelled);
    let job = tokio::task::spawn_blocking(move || {
        let _resources = resources;
        work(&|| {
            if observed.load(Ordering::Acquire) {
                Err(std::io::ErrorKind::ConnectionAborted.into())
            } else {
                Ok(())
            }
        })
    });
    let _cancellation = CancelQueuedWrite {
        abort: job.abort_handle(),
        cancelled,
    };
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
    fn running_read_stops_at_its_next_checkpoint_and_releases_admission() {
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
            let (result, observed) = tokio::sync::oneshot::channel();
            let mut running = Box::pin(cancellable_read((file, permit), move |checkpoint| {
                checkpoint()?;
                entered.send(()).unwrap();
                wait.recv().unwrap();
                let error = checkpoint().unwrap_err();
                result.send(error.kind()).unwrap();
                Err::<(), _>(error.into())
            }));
            let mut context = Context::from_waker(Waker::noop());
            assert!(running.as_mut().poll(&mut context).is_pending());
            started.await.unwrap();
            drop(running);
            assert!(path.exists());
            assert_eq!(admission.available_permits(), 0);
            finish.send(()).unwrap();
            assert_eq!(
                observed.await.unwrap(),
                std::io::ErrorKind::ConnectionAborted
            );
            tokio::task::spawn_blocking(|| ()).await.unwrap();
            assert!(!path.exists());
            assert_eq!(admission.available_permits(), 1);
        });
    }

    #[test]
    fn cancelled_queued_file_preparation_never_reads_or_publishes_input() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .max_blocking_threads(1)
            .build()
            .unwrap();
        runtime.block_on(async {
            let root = tempfile::tempdir().unwrap();
            let objects = shardline_server_core::ServerObjectStore::local(root.path()).unwrap();
            let admission = Arc::new(tokio::sync::Semaphore::new(1));
            let permit = admission.clone().acquire_owned().await.unwrap();
            // Empty input would fail immediately if preparation still ran on
            // this async thread. Occupy the only worker before polling it.
            let file = tempfile::NamedTempFile::new().unwrap();
            let path = file.path().to_owned();
            let hash = "a".repeat(64);
            let (entered, started) = tokio::sync::oneshot::channel();
            let (finish, wait) = std::sync::mpsc::channel();
            let occupied = tokio::task::spawn_blocking(move || {
                entered.send(()).unwrap();
                wait.recv().unwrap();
            });
            started.await.unwrap();
            let mut queued = Box::pin(super::super::store_owned_xorb_file(
                &objects, &hash, file, permit,
            ));
            let mut context = Context::from_waker(Waker::noop());
            assert!(queued.as_mut().poll(&mut context).is_pending());
            assert!(path.exists());
            assert_eq!(admission.available_permits(), 0);
            drop(queued);
            finish.send(()).unwrap();
            occupied.await.unwrap();
            tokio::task::spawn_blocking(|| ()).await.unwrap();
            assert!(!path.exists());
            assert_eq!(admission.available_permits(), 1);
            let key = super::super::xorb_object_key(&hash).unwrap();
            assert!(
                shardline_storage::ObjectStore::metadata(&objects, &key)
                    .unwrap()
                    .is_none()
            );
        });
    }

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
