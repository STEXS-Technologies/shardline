//! Cooperatively await writes on the store's existing transport runtime.
use std::{future::Future, io};

use tokio::{runtime::Runtime, task::AbortHandle};

use super::S3ObjectStoreError;
use crate::PutOutcome;

struct CancelWriteOnDrop(AbortHandle);

impl Drop for CancelWriteOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

pub(super) async fn run(
    runtime: &Runtime,
    operation: impl Future<Output = Result<PutOutcome, S3ObjectStoreError>> + Send + 'static,
) -> Result<PutOutcome, S3ObjectStoreError> {
    let task = runtime.spawn(operation);
    // Dropping the caller must abort the owned future, rather than leave a
    // detached operation running after its request has released ownership.
    let _cancellation = CancelWriteOnDrop(task.abort_handle());
    task.await
        .map_err(|error| S3ObjectStoreError::Runtime(io::Error::other(error)))?
}
