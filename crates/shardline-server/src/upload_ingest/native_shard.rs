//! Admitted read-only native verification; publication stays with its caller.

use super::NativeBody;
use crate::{
    ServerError,
    admission::{BoundedPool, cancellable_work_with_permit},
};
use shardline_protocol::RepositoryScope;
use shardline_server_core::{ServerObjectStore, ShardMetadataLimits};

pub(crate) async fn prepare_native_shard(
    staged: NativeBody,
    objects: ServerObjectStore,
    scope: Option<RepositoryScope>,
    limits: ShardMetadataLimits,
    pool: &BoundedPool,
) -> Result<
    (
        shardline_xet_adapter::PreparedShardUpload,
        crate::upload_identity::NativeUploadResources,
    ),
    ServerError,
> {
    let permit = pool
        .acquire()
        .await
        .map_err(|error| ServerError::Io(std::io::Error::other(error)))?;
    // Never wait for a second slot while holding the first: saturation keeps
    // the sequential path and cannot deadlock nested blocking admission.
    let digest_permit = std::thread::available_parallelism()
        .ok()
        .filter(|cpus| cpus.get() > 1)
        .and_then(|_| pool.try_acquire());
    let ((prepared, admission), permit) =
        cancellable_work_with_permit(permit, move |cancellation| {
            let NativeBody { bytes, admission } = staged;
            let mut reader = std::io::Cursor::new(bytes);
            let prepared = if let Some(digest_permit) = digest_permit {
                shardline_xet_adapter::prepare_uploaded_shard_with_digest_worker(
                    &mut reader,
                    &objects,
                    scope.as_ref(),
                    limits,
                    &|| cancellation.check_io(),
                    digest_permit,
                )?
            } else {
                shardline_xet_adapter::prepare_uploaded_shard_with_checkpoint(
                    &mut reader,
                    &objects,
                    scope.as_ref(),
                    limits,
                    &|| cancellation.check_io(),
                )?
            };
            cancellation.check()?;
            Ok((prepared, admission))
        })
        .await?;
    Ok((prepared, (admission, permit)))
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use std::{num::NonZeroUsize, sync::Arc, time::Duration};

    #[tokio::test]
    async fn rejected_shard_releases_all_slots_without_nested_admission_deadlock() {
        for capacity in [1, 2] {
            let pool = BoundedPool::new(NonZeroUsize::new(capacity).unwrap());
            let buffers = Arc::new(tokio::sync::Semaphore::new(1));
            let staged = NativeBody {
                bytes: Vec::new(),
                admission: buffers.clone().try_acquire_owned().unwrap(),
            };
            let result = tokio::time::timeout(
                Duration::from_secs(5),
                prepare_native_shard(
                    staged,
                    ServerObjectStore::Blackhole,
                    None,
                    shardline_server_core::DEFAULT_SHARD_METADATA_LIMITS,
                    &pool,
                ),
            )
            .await
            .unwrap();
            assert!(result.is_err());
            let permits: Vec<_> = (0..capacity).map(|_| pool.try_acquire().unwrap()).collect();
            assert!(pool.try_acquire().is_none());
            assert_eq!(buffers.available_permits(), 1);
            drop(permits);
        }
    }
}
