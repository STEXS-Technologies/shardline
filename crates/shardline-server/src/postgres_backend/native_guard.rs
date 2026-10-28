//! Keep native storage fencing on the request's original ownership session.
use std::sync::Arc;

use shardline_index::{PostgresIndexStore, PostgresMetadataStoreError};
use sqlx::{Connection, PgConnection};

use crate::{
    ServerError, ServerObjectStore,
    maintenance_barrier::RequestSession,
    upload_identity::{NativeUploadResources, PreparedNativeXorb},
    upload_ingest::store_admitted_xorb,
    xet_adapter::{XorbUploadResponse, xorb_object_key},
};

pub(super) async fn store_xorb(
    index: &PostgresIndexStore,
    identity: &Arc<()>,
    objects: &ServerObjectStore,
    hash: &str,
    staged: PreparedNativeXorb,
    permit: NativeUploadResources,
) -> Result<XorbUploadResponse, ServerError> {
    let result = if let Some(mut owner) = RequestSession::connection_for(identity).await {
        let connection = owner.as_mut().ok_or(ServerError::StaleResourceFence)?;
        store_on_connection(connection, objects, hash, staged, permit).await
    } else {
        let mut connection = index
            .pool()
            .acquire()
            .await
            .map_err(PostgresMetadataStoreError::from)?;
        store_on_connection(&mut connection, objects, hash, staged, permit).await
    };
    // Release the borrowed owner before checking it. Never check out a new
    // session to recover lost ownership, including after storage failures.
    RequestSession::verify_for(identity).await?;
    result
}

async fn store_on_connection(
    connection: &mut PgConnection,
    objects: &ServerObjectStore,
    hash: &str,
    staged: PreparedNativeXorb,
    permit: NativeUploadResources,
) -> Result<XorbUploadResponse, ServerError> {
    let key = xorb_object_key(hash)?;
    let mut transaction = connection
        .begin()
        .await
        .map_err(PostgresMetadataStoreError::from)?;
    PostgresIndexStore::require_remote_delete_settled(&mut transaction, &key).await?;
    let stored = store_admitted_xorb(objects, staged, permit).await?;
    // COMMIT on this same physical session verifies it survived storage while
    // retaining the deletion guard throughout every immutable write.
    transaction
        .commit()
        .await
        .map_err(PostgresMetadataStoreError::from)?;
    Ok(stored)
}
