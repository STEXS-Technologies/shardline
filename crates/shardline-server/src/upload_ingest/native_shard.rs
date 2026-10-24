//! Admitted read-only native verification; publication stays with its caller.

use super::StagedRequestBody;
use crate::{
    ServerError,
    admission::{BoundedPool, cancellable_work_with_permit},
};
use shardline_protocol::RepositoryScope;
use shardline_server_core::{ServerObjectStore, ShardMetadataLimits};

pub(crate) async fn prepare_native_shard(
    staged: StagedRequestBody,
    objects: ServerObjectStore,
    scope: Option<RepositoryScope>,
    limits: ShardMetadataLimits,
    pool: &BoundedPool,
) -> Result<
    (
        shardline_xet_adapter::PreparedShardUpload,
        tokio::sync::OwnedSemaphorePermit,
    ),
    ServerError,
> {
    let permit = pool
        .acquire()
        .await
        .map_err(|error| ServerError::Io(std::io::Error::other(error)))?;
    cancellable_work_with_permit(permit, move |cancellation| {
        let prepared = match staged {
            StagedRequestBody::Memory { bytes, .. } => {
                shardline_xet_adapter::prepare_uploaded_shard_with_checkpoint(
                    &mut std::io::Cursor::new(bytes),
                    &objects,
                    scope.as_ref(),
                    limits,
                    &|| cancellation.check_io(),
                )?
            }
            StagedRequestBody::File { file, .. } => {
                let mut reader = std::fs::File::open(file.path())?;
                shardline_xet_adapter::prepare_uploaded_shard_with_checkpoint(
                    &mut reader,
                    &objects,
                    scope.as_ref(),
                    limits,
                    &|| cancellation.check_io(),
                )?
            }
        };
        cancellation.check()?;
        Ok(prepared)
    })
    .await
}
