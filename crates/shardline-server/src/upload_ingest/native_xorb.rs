//! Keep native upload resources attached to their final immutable write.

use crate::{ServerError, upload_identity::PreparedNativeXorb, xet_adapter::XorbUploadResponse};
use shardline_server_core::ServerObjectStore;

pub(crate) async fn store_admitted_xorb(
    objects: &ServerObjectStore,
    staged: PreparedNativeXorb,
    permit: crate::upload_identity::NativeUploadResources,
) -> Result<XorbUploadResponse, ServerError> {
    let stored = shardline_xet_adapter::store_prepared_xorb_memory(objects, staged, permit).await?;
    Ok(XorbUploadResponse {
        was_inserted: stored.was_inserted,
    })
}
