//! Keep native upload resources attached to their final immutable write.

use crate::{ServerError, upload_identity::PreparedNativeXorb, xet_adapter::XorbUploadResponse};
use shardline_server_core::ServerObjectStore;
use tokio::sync::OwnedSemaphorePermit;

pub(crate) async fn store_admitted_xorb(
    objects: &ServerObjectStore,
    staged: PreparedNativeXorb,
    permit: OwnedSemaphorePermit,
) -> Result<XorbUploadResponse, ServerError> {
    match staged {
        PreparedNativeXorb::Memory(prepared) => {
            let stored =
                shardline_xet_adapter::store_prepared_xorb_memory(objects, *prepared, permit)
                    .await?;
            Ok(XorbUploadResponse {
                was_inserted: stored.was_inserted,
            })
        }
        PreparedNativeXorb::File(file) => {
            let stored =
                shardline_xet_adapter::store_prepared_xorb_file(objects, *file, permit).await?;
            Ok(XorbUploadResponse {
                was_inserted: stored.was_inserted,
            })
        }
    }
}
