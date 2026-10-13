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
