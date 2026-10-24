//! Keep native upload resources attached to their final immutable write.

use super::StagedRequestBody;
use crate::{ServerError, xet_adapter::XorbUploadResponse};
use shardline_server_core::ServerObjectStore;
use tokio::sync::OwnedSemaphorePermit;

pub(crate) async fn store_admitted_xorb(
    objects: &ServerObjectStore,
    expected_hash: &str,
    staged: StagedRequestBody,
    permit: OwnedSemaphorePermit,
) -> Result<XorbUploadResponse, ServerError> {
    match staged {
        StagedRequestBody::Memory { bytes, .. } => {
            let stored = shardline_xet_adapter::store_xorb_bytes_with_resources(
                objects,
                expected_hash,
                &bytes,
                permit,
            )
            .await?;
            Ok(XorbUploadResponse {
                was_inserted: stored.was_inserted,
            })
        }
        StagedRequestBody::File { file, .. } => {
            let stored =
                shardline_xet_adapter::store_owned_xorb_file(objects, expected_hash, file, permit)
                    .await?;
            Ok(XorbUploadResponse {
                was_inserted: stored.was_inserted,
            })
        }
    }
}
