use shardline_index::PostgresIndexStore;
use shardline_server_core::{ServerObjectStore, remote_delete::GuardedRemoteDeleteError};
use shardline_storage::{DeleteOutcome, ObjectKey, ObjectStore};

use crate::GcError;

pub(crate) async fn delete_gc_object(
    objects: &ServerObjectStore,
    key: &ObjectKey,
    guard: Option<&PostgresIndexStore>,
) -> Result<DeleteOutcome, GcError> {
    if let Some(index) = guard {
        return shardline_server_core::remote_delete::delete_with_postgres_guard(
            index, objects, key,
        )
        .await
        .map_err(|error| match error {
            GuardedRemoteDeleteError::Metadata(error) => GcError::PostgresMetadata(error),
            GuardedRemoteDeleteError::ObjectStore(error) => GcError::ObjectStore(error),
        });
    }
    objects.delete_if_present(key).map_err(GcError::ObjectStore)
}
