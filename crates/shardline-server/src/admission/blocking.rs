//! Shared admission for blocking upload and download work.
use super::BoundedPool;
use crate::ServerError;

// Cancel queued work when its caller disappears. Running work retains its
// permit until completion; aborting a future cannot stop synchronous I/O.
struct AbortBlockingJob(tokio::task::AbortHandle);
impl Drop for AbortBlockingJob {
    fn drop(&mut self) {
        self.0.abort();
    }
}

pub(crate) async fn blocking_work<T: Send + 'static>(
    pool: &BoundedPool,
    work: impl FnOnce() -> Result<T, ServerError> + Send + 'static,
) -> Result<T, ServerError> {
    let permit = pool
        .acquire()
        .await
        .map_err(|error| ServerError::Io(std::io::Error::other(error)))?;
    let job = tokio::task::spawn_blocking(move || {
        let _permit = permit;
        work()
    });
    let _abort = AbortBlockingJob(job.abort_handle());
    job.await
        .map_err(|error| ServerError::Io(std::io::Error::other(error)))?
}

/// Allocates request-owned state on the caller's thread after admission.
/// Admission remains charged until the completed result is claimed or dropped.
pub(crate) async fn blocking_work_with_setup<S: Send + 'static, T: Send + 'static>(
    pool: &BoundedPool,
    setup: impl FnOnce() -> S,
    work: impl FnOnce(S) -> Result<T, ServerError> + Send + 'static,
) -> Result<T, ServerError> {
    let permit = pool
        .acquire()
        .await
        .map_err(|error| ServerError::Io(std::io::Error::other(error)))?;
    let state = setup();
    let (result, _permit) = blocking_work_with_permit(permit, move || work(state)).await?;
    Ok(result)
}

/// Keeps already-admitted buffers charged through blocking work and its caller's
/// subsequent asynchronous storage. Cancellation cannot release running work.
pub(crate) async fn blocking_work_with_permit<T: Send + 'static>(
    permit: tokio::sync::OwnedSemaphorePermit,
    work: impl FnOnce() -> Result<T, ServerError> + Send + 'static,
) -> Result<(T, tokio::sync::OwnedSemaphorePermit), ServerError> {
    let job = tokio::task::spawn_blocking(move || (work(), permit));
    let _abort = AbortBlockingJob(job.abort_handle());
    let (result, retained_permit) = job
        .await
        .map_err(|error| ServerError::Io(std::io::Error::other(error)))?;
    Ok((result?, retained_permit))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        num::NonZeroUsize,
        sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        },
    };

    #[tokio::test]
    async fn cancelled_running_work_retains_its_existing_permit() {
        let pool = BoundedPool::new(NonZeroUsize::new(1).unwrap());
        let permit = pool.acquire().await.unwrap();
        let (started, running) = tokio::sync::oneshot::channel();
        let (finish, wait) = std::sync::mpsc::channel();
        let caller = tokio::spawn(blocking_work_with_permit(permit, move || {
            started.send(()).unwrap();
            wait.recv().unwrap();
            Ok(vec![0_u8; 1024])
        }));
        running.await.unwrap();
        caller.abort();
        assert!(caller.await.unwrap_err().is_cancelled());
        assert!(pool.try_acquire().is_none(), "running work remains charged");
        finish.send(()).unwrap();
        let _released = tokio::time::timeout(std::time::Duration::from_secs(5), pool.acquire())
            .await
            .expect("worker must release admission after finishing")
            .unwrap();
    }

    #[tokio::test]
    async fn cancelled_queued_setup_does_not_allocate_or_retain_admission() {
        let pool = BoundedPool::new(NonZeroUsize::new(1).unwrap());
        let held = pool.acquire().await.unwrap();
        let allocated = Arc::new(AtomicBool::new(false));
        let observed = Arc::clone(&allocated);
        let queued_pool = pool.clone();
        let (entered, started) = tokio::sync::oneshot::channel();
        let queued = tokio::spawn(async move {
            entered.send(()).unwrap();
            blocking_work_with_setup(
                &queued_pool,
                move || {
                    observed.store(true, Ordering::SeqCst);
                    vec![0_u8; 1024]
                },
                Ok,
            )
            .await
        });
        started.await.unwrap();
        queued.abort();
        assert!(queued.await.unwrap_err().is_cancelled());
        assert!(!allocated.load(Ordering::SeqCst));
        drop(held);
        assert!(pool.try_acquire().is_some());
    }
}
