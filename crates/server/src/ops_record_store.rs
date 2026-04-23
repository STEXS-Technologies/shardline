use shardline_index::{LocalRecordStore, PostgresRecordStore, RecordStore};

/// Operation-time record-store classification.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum OpsRecordKind {
    Latest,
    Version,
}

/// Extra locator metadata needed by operator tooling.
pub(crate) trait OpsRecordStore: RecordStore {
    /// Renders a stable operator-facing location for one record locator.
    fn locator_display(&self, locator: &Self::Locator) -> String;

    /// Extracts the file identifier implied by a locator.
    fn locator_file_id(&self, locator: &Self::Locator, kind: OpsRecordKind) -> Option<String>;

    /// Extracts the immutable content hash implied by a version locator.
    fn locator_content_hash(&self, locator: &Self::Locator, kind: OpsRecordKind) -> Option<String>;
}

impl OpsRecordStore for LocalRecordStore {
    fn locator_display(&self, locator: &Self::Locator) -> String {
        locator.record_key().to_owned()
    }

    fn locator_file_id(&self, locator: &Self::Locator, _kind: OpsRecordKind) -> Option<String> {
        Some(locator.file_id().to_owned())
    }

    fn locator_content_hash(&self, locator: &Self::Locator, kind: OpsRecordKind) -> Option<String> {
        if kind != OpsRecordKind::Version {
            return None;
        }

        locator.content_hash().map(ToOwned::to_owned)
    }
}

impl OpsRecordStore for PostgresRecordStore {
    fn locator_display(&self, locator: &Self::Locator) -> String {
        locator.record_key().to_owned()
    }

    fn locator_file_id(&self, locator: &Self::Locator, _kind: OpsRecordKind) -> Option<String> {
        Some(locator.file_id().to_owned())
    }

    fn locator_content_hash(&self, locator: &Self::Locator, kind: OpsRecordKind) -> Option<String> {
        if kind != OpsRecordKind::Version {
            return None;
        }

        locator.content_hash().map(ToOwned::to_owned)
    }
}

#[cfg(test)]
mod tests {
    use shardline_index::{FileRecord, RecordStore};
    use shardline_protocol::{RepositoryProvider, RepositoryScope};

    use super::{LocalRecordStore, OpsRecordKind, OpsRecordStore};

    #[test]
    fn local_locator_helpers_extract_scoped_file_id_and_hash() {
        let scope =
            RepositoryScope::new(RepositoryProvider::GitHub, "octo", "assets", Some("main"));
        assert!(scope.is_ok());
        let Ok(scope) = scope else {
            return;
        };
        let store = LocalRecordStore::open("/var/lib/shardline".into());
        let record = FileRecord {
            file_id: "asset.bin".to_owned(),
            content_hash: "a".repeat(64),
            total_bytes: 0,
            chunk_size: 0,
            repository_scope: Some(scope),
            chunks: Vec::new(),
        };

        let latest = store.latest_record_locator(&record);
        let version = store.version_record_locator(&record);

        assert_eq!(
            store.locator_file_id(&latest, OpsRecordKind::Latest),
            Some("asset.bin".to_owned())
        );
        assert_eq!(
            store.locator_file_id(&version, OpsRecordKind::Version),
            Some("asset.bin".to_owned())
        );
        assert_eq!(
            store.locator_content_hash(&version, OpsRecordKind::Version),
            Some("a".repeat(64))
        );
    }
}
