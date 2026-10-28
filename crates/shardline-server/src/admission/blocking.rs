//! Shared admission for blocking upload and download work.
use super::BoundedPool;
use crate::ServerError;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

/// Request lifetime signal for synchronous work. Workers must check it between
/// bounded units of work; it cannot interrupt an already-issued storage call.
#[derive(Clone)]
pub(crate) struct WorkCancellation(Arc<AtomicBool>);

impl WorkCancellation {
    pub(crate) fn check(&self) -> Result<(), ServerError> {
        self.check_io().map_err(ServerError::Io)
    }

    pub(crate) fn check_io(&self) -> std::io::Result<()> {
        if self.0.load(Ordering::Acquire) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::Interrupted,
                "request cancelled during blocking verification",
            ));
        }
        Ok(())
    }
}

struct CancelWorkOnDrop(WorkCancellation);

impl Drop for CancelWorkOnDrop {
    fn drop(&mut self) {
        self.0.0.store(true, Ordering::Release);
    }
}

/// Cancellation is cooperative once running; admission remains owned by the job.
pub(crate) async fn cancellable_work_with_permit<T: Send + 'static>(
    permit: tokio::sync::OwnedSemaphorePermit,
    work: impl FnOnce(WorkCancellation) -> Result<T, ServerError> + Send + 'static,
) -> Result<(T, tokio::sync::OwnedSemaphorePermit), ServerError> {
    let cancellation = WorkCancellation(Arc::new(AtomicBool::new(false)));
    let _cancel = CancelWorkOnDrop(cancellation.clone());
    blocking_work_with_permit(permit, move || {
        cancellation.check()?;
        work(cancellation)
    })
    .await
}

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

/// Allocates on the caller's thread only after admission and cancels running
/// work cooperatively. Admission remains charged until completion or cleanup.
pub(crate) async fn cancellable_work_with_setup<S: Send + 'static, T: Send + 'static>(
    pool: &BoundedPool,
    setup: impl FnOnce() -> S,
    work: impl FnOnce(S, WorkCancellation) -> Result<T, ServerError> + Send + 'static,
) -> Result<T, ServerError> {
    let permit = pool
        .acquire()
        .await
        .map_err(|error| ServerError::Io(std::io::Error::other(error)))?;
    let state = setup();
    let (result, _permit) =
        cancellable_work_with_permit(permit, move |cancel| work(state, cancel)).await?;
    Ok(result)
}

/// Keeps already-admitted buffers charged through blocking work and its caller's
/// subsequent asynchronous storage. Cancellation cannot release running work.
/// The guard may be shared with transport-owned buffers; the last owner releases it.
pub(crate) async fn blocking_work_with_permit<T: Send + 'static, P: Send + 'static>(
    permit: P,
    work: impl FnOnce() -> Result<T, ServerError> + Send + 'static,
) -> Result<(T, P), ServerError> {
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
    async fn cancelled_running_worker_observes_signal_before_next_unit() {
        let pool = BoundedPool::new(NonZeroUsize::new(1).unwrap());
        let permit = pool.acquire().await.unwrap();
        let (started, running) = tokio::sync::oneshot::channel();
        let (resume, wait) = std::sync::mpsc::channel();
        let (stopped, observed) = tokio::sync::oneshot::channel();
        let caller = tokio::spawn(cancellable_work_with_permit(permit, move |cancel| {
            started.send(()).unwrap();
            wait.recv().unwrap();
            let result = cancel.check();
            stopped.send(result.is_err()).unwrap();
            result
        }));
        running.await.unwrap();
        caller.abort();
        assert!(caller.await.unwrap_err().is_cancelled());
        assert!(pool.try_acquire().is_none());
        resume.send(()).unwrap();
        assert!(
            tokio::time::timeout(std::time::Duration::from_secs(5), observed)
                .await
                .unwrap()
                .unwrap()
        );
        let _released = tokio::time::timeout(std::time::Duration::from_secs(5), pool.acquire())
            .await
            .unwrap()
            .unwrap();
    }

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
            cancellable_work_with_setup(
                &queued_pool,
                move || {
                    observed.store(true, Ordering::SeqCst);
                    vec![0_u8; 1024]
                },
                |state, _cancel| Ok(state),
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
