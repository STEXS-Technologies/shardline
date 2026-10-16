//! Distinguish complete legacy page selection from budgeted retirement scans.
use crate::{ServerError, object_store::ServerObjectStoreError};

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum ScanPurpose {
    Pagination,
    Retirement,
}

pub(super) enum InventoryError {
    Stopped,
    Storage(ServerError),
}

impl From<ServerError> for InventoryError {
    fn from(error: ServerError) -> Self {
        Self::Storage(error)
    }
}

impl From<shardline_storage::LocalObjectStoreError> for InventoryError {
    fn from(error: shardline_storage::LocalObjectStoreError) -> Self {
        Self::Storage(ServerObjectStoreError::Local(error).into())
    }
}

impl From<shardline_storage::S3ObjectStoreError> for InventoryError {
    fn from(error: shardline_storage::S3ObjectStoreError) -> Self {
        Self::Storage(ServerObjectStoreError::S3(error).into())
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::super::TaskEvidenceStore;
    use crate::{ServerConfig, ServerError, ServerObjectStore};
    use shardline_storage::ObjectPrefix;
    use std::{num::NonZeroUsize, ops::ControlFlow};

    #[test]
    fn retirement_stops_legacy_visits_while_pagination_considers_every_item() {
        let root = tempfile::tempdir().unwrap();
        let store = TaskEvidenceStore::from_objects(ServerObjectStore::local(root.path()).unwrap());
        for number in 0..40 {
            store
                .publish(&format!("fixture/{number:03}"), b"original")
                .unwrap();
        }
        let prefix = ObjectPrefix::parse("_shardline_tasks/v1/fixture/").unwrap();
        let mut visited = 0;
        store
            .visit_retirement_prefix_until(&prefix, |_| {
                visited += 1;
                Ok(ControlFlow::Break(()))
            })
            .unwrap();
        assert_eq!(visited, 1);
        visited = 0;
        store
            .visit_prefix_until(&prefix, None, |_| {
                visited += 1;
                Ok(ControlFlow::Break(()))
            })
            .unwrap();
        assert_eq!(visited, 40);
        assert!(matches!(
            store.visit_retirement_prefix_until(&prefix, |_| Err(ServerError::InvalidPath)),
            Err(ServerError::InvalidPath)
        ));
    }

    #[test]
    fn retirement_break_in_metadata_skips_remaining_legacy_inventory() {
        let root = tempfile::tempdir().unwrap();
        let config = ServerConfig::new(
            "127.0.0.1:8080".parse().unwrap(),
            "http://localhost:8080".to_owned(),
            root.path().to_owned(),
            NonZeroUsize::new(4096).unwrap(),
        );
        let store = TaskEvidenceStore::from_config(&config).unwrap();
        store.publish("fixture/metadata", b"metadata").unwrap();
        let legacy = TaskEvidenceStore::from_objects(store.objects.clone());
        for number in 0..40 {
            legacy
                .publish(&format!("fixture/legacy-{number:03}"), b"original")
                .unwrap();
        }
        let prefix = ObjectPrefix::parse("_shardline_tasks/v1/fixture/").unwrap();
        let mut visited = 0;
        store
            .visit_retirement_prefix_until(&prefix, |_| {
                visited += 1;
                Ok(ControlFlow::Break(()))
            })
            .unwrap();
        assert_eq!(visited, 1);
        let mut preserved = 0;
        store
            .visit_prefix(&prefix, |_| {
                preserved += 1;
                Ok(())
            })
            .unwrap();
        assert_eq!(preserved, 41);
    }
}
